// Copyright 2026 Valere Fedronic
//
// This file is part of matrix-rust-rtc.
//
// matrix-rust-rtc is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// matrix-rust-rtc is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

//! Frames across the FFI boundary.
//!
//! The delivery model (per-platform decision made with the SDK owner):
//!
//! - **Streams are async-pull**: objects with `async fn next()`, which
//!   Kotlin bridges to a `Flow` and Swift to an `AsyncStream`. Frame
//!   delivery is latest-frame-wins upstream, so a slow consumer drops
//!   frames instead of buffering.
//! - **Audio frames go by value** ([`FfiAudioFrame`], ~2 KB at 10 ms/48 kHz
//!   mono — the copy is noise at 100 calls/s), as little-endian bytes rather
//!   than a sample list: uniffi maps `Vec<u8>` to a `ByteArray`/`Data`, whereas
//!   a `Vec<i16>` becomes a boxed `List<Short>` — roughly 48 000 boxed objects a
//!   second at 48 kHz mono, which was the single biggest cost this API imposed
//!   on a host.
//! - **Video frames are objects** ([`VideoFrameRef`]) whose default read path
//!   is the zero-copy one: `plane_ptr` + `stride`, valid for as long as the
//!   object is referenced (lifetime is the object, not a manual handle) and
//!   readable and releasable from any thread. `data(plane)` returns a copy
//!   for hosts that cannot take a pointer, and is the slow path — at frame
//!   sizes the copies, and the garbage they make, cost more than the codec.
//! - **Capture goes the other way by value**: the host pushes PCM/I420 into
//!   a [`FfiLocalTrack`].

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::Mutex as TokioMutex;

use matrix_rtc_media::{AudioFrame, LocalTrackHandle, VideoFrame};

use super::MediaFfiError;
use super::types::FfiStreamKind;

/// A chunk of interleaved 16-bit PCM.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiAudioFrame {
    /// Interleaved samples as **little-endian `int16`** bytes:
    /// `samples_per_channel * num_channels * 2` long.
    ///
    /// Bytes rather than a list of samples because uniffi renders `Vec<u8>` as a
    /// `ByteArray` (Kotlin) / `Data` (Swift) — a single buffer — while a list of
    /// samples arrives boxed, one object per sample. On Kotlin, read with
    /// `ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer()`;
    /// `AudioTrack`/`AudioRecord` accept the byte form directly.
    pub data: Vec<u8>,
    pub sample_rate: u32,
    pub num_channels: u32,
    pub samples_per_channel: u32,
}

/// Little-endian `i16` bytes, the shape [`FfiAudioFrame::data`] carries.
fn pcm_to_le_bytes(samples: &[i16]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    bytes
}

/// Inverse of [`pcm_to_le_bytes`]. A trailing odd byte is dropped: it cannot be
/// half a sample, and truncating is better than shifting every later sample by
/// one byte, which turns the whole frame into noise.
fn pcm_from_le_bytes(bytes: &[u8]) -> Vec<i16> {
    let (pairs, _trailing) = bytes.as_chunks::<2>();
    pairs.iter().copied().map(i16::from_le_bytes).collect()
}

/// Frame rotation to apply before rendering.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum FfiVideoRotation {
    Deg0,
    Deg90,
    Deg180,
    Deg270,
}

impl From<matrix_rtc_media::VideoRotation> for FfiVideoRotation {
    fn from(rotation: matrix_rtc_media::VideoRotation) -> Self {
        use matrix_rtc_media::VideoRotation as Rotation;
        match rotation {
            Rotation::Deg0 => Self::Deg0,
            Rotation::Deg90 => Self::Deg90,
            Rotation::Deg180 => Self::Deg180,
            Rotation::Deg270 => Self::Deg270,
        }
    }
}

impl From<FfiVideoRotation> for matrix_rtc_media::VideoRotation {
    fn from(rotation: FfiVideoRotation) -> Self {
        match rotation {
            FfiVideoRotation::Deg0 => Self::Deg0,
            FfiVideoRotation::Deg90 => Self::Deg90,
            FfiVideoRotation::Deg180 => Self::Deg180,
            FfiVideoRotation::Deg270 => Self::Deg270,
        }
    }
}

/// One of the three I420 planes.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum FfiVideoPlane {
    Y,
    U,
    V,
}

/// A stream of received audio frames. `next()` suspends until the next
/// frame; `None` means the stream ended (track gone).
#[derive(uniffi::Object)]
pub struct AudioFrameStream {
    frames: TokioMutex<BoxStream<'static, AudioFrame>>,
}

impl AudioFrameStream {
    pub(super) fn new(frames: BoxStream<'static, AudioFrame>) -> Self {
        Self {
            frames: TokioMutex::new(frames),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl AudioFrameStream {
    pub async fn next(&self) -> Option<FfiAudioFrame> {
        let frame = self.frames.lock().await.next().await?;
        Some(FfiAudioFrame {
            data: pcm_to_le_bytes(&frame.data),
            sample_rate: frame.sample_rate,
            num_channels: frame.num_channels,
            samples_per_channel: frame.samples_per_channel,
        })
    }
}

/// A stream of received video frames (latest-frame-wins).
///
/// Holding a frame does not apply backpressure and does not stall the stream:
/// dropping happens upstream, in a queue of one, whether or not the host still
/// holds earlier frames. Each frame owns its own plane allocations — nothing
/// is recycled between them — so a host may hold several at once (a renderer
/// with a frame in flight and the next one arriving) and read them all
/// concurrently. The cost of holding is memory, and the price of holding too
/// long is a dropped frame, never a corrupted one.
#[derive(uniffi::Object)]
pub struct VideoFrameStream {
    frames: TokioMutex<BoxStream<'static, VideoFrame>>,
}

impl VideoFrameStream {
    pub(super) fn new(frames: BoxStream<'static, VideoFrame>) -> Self {
        Self {
            frames: TokioMutex::new(frames),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl VideoFrameStream {
    pub async fn next(&self) -> Option<Arc<VideoFrameRef>> {
        let frame = self.frames.lock().await.next().await?;
        Some(Arc::new(VideoFrameRef { frame }))
    }
}

/// A received I420 video frame.
///
/// [`Self::plane_ptr`] + [`Self::stride`] is the intended way to render one:
/// the planes are read in place, with no copy on either side of the boundary.
/// [`Self::data`] is the fallback for hosts that cannot take a raw pointer —
/// it copies the plane, and the bindings copy it again into a host array, so
/// it costs two full frame copies per plane where the pointer costs none.
///
/// The memory is immutable and thread-agnostic for the object's whole life;
/// [`Self::plane_ptr`] carries the exact contract.
#[derive(uniffi::Object)]
pub struct VideoFrameRef {
    frame: VideoFrame,
}

#[uniffi::export]
impl VideoFrameRef {
    pub fn width(&self) -> u32 {
        self.frame.buffer.width
    }

    pub fn height(&self) -> u32 {
        self.frame.buffer.height
    }

    pub fn rotation(&self) -> FfiVideoRotation {
        self.frame.rotation.into()
    }

    pub fn timestamp_us(&self) -> i64 {
        self.frame.timestamp_us
    }

    pub fn stride(&self, plane: FfiVideoPlane) -> u32 {
        match plane {
            FfiVideoPlane::Y => self.frame.buffer.stride_y,
            FfiVideoPlane::U => self.frame.buffer.stride_u,
            FfiVideoPlane::V => self.frame.buffer.stride_v,
        }
    }

    /// A copy of one plane's bytes, for hosts that cannot read a raw pointer.
    ///
    /// Prefer [`Self::plane_ptr`]: this clones the plane here and the bindings
    /// clone it again into a host-owned array — two full copies per plane, per
    /// frame, per stream, which at 720p30 on three streams is the difference
    /// between idle and a saturated core.
    pub fn data(&self, plane: FfiVideoPlane) -> Vec<u8> {
        match plane {
            FfiVideoPlane::Y => self.frame.buffer.data_y.clone(),
            FfiVideoPlane::U => self.frame.buffer.data_u.clone(),
            FfiVideoPlane::V => self.frame.buffer.data_v.clone(),
        }
    }

    /// The raw address of one plane's bytes, for zero-copy renderers (GL
    /// upload, `CVPixelBuffer` wrapping, libyuv via JNI, ...). Read
    /// [`Self::plane_len`] bytes, with [`Self::stride`] as the row pitch.
    ///
    /// # Lifetime
    ///
    /// The address is valid for exactly as long as *any* reference to this
    /// object is alive, and is stable across calls: the object owns the three
    /// plane allocations outright and nothing reallocates them. The stream
    /// does not retain the frame — once `next()` has handed it over, the host
    /// alone decides when the memory goes away, by releasing this object.
    ///
    /// # Threads
    ///
    /// There is no thread affinity in either direction:
    ///
    /// - **Reading.** Any thread, any number of threads at once, no
    ///   synchronisation needed. Nothing writes to the planes after the frame
    ///   is constructed — this type has no interior mutability and no method
    ///   taking `&mut self` — so the bytes are immutable for the object's
    ///   whole life. Posting the frame to a render thread is enough; the
    ///   handoff itself supplies the ordering.
    /// - **Releasing.** Any thread, not only the one `next()` returned on.
    ///   Dropping the object frees three plain heap allocations through the
    ///   global allocator; there is no thread-local state, no runtime, and no
    ///   JNI attach on that path.
    ///
    /// The one rule is ordering, not thread identity: **every read must
    /// happen before the release**, and the release must happen exactly once.
    /// Freeing the frame while a renderer is still reading through these
    /// pointers is a use-after-free, exactly as in C.
    ///
    /// The trap is a renderer that *retains*. If the pointers go to something
    /// that returns before it draws — a GL renderer posting to its own
    /// thread, a libwebrtc buffer wrapping this memory — then the scope that
    /// fetched the pointer is not the scope that may release. Drive the
    /// release from that consumer's own completion callback (on Android, the
    /// `releaseCallback` of the wrapping `JavaI420Buffer`), which is also the
    /// only thing that knows when the last read finished. Such a callback
    /// failing to fire leaks the frame rather than corrupting it — benign next
    /// to a use-after-free, but these are megabyte allocations.
    pub fn plane_ptr(&self, plane: FfiVideoPlane) -> u64 {
        let slice: &[u8] = match plane {
            FfiVideoPlane::Y => &self.frame.buffer.data_y,
            FfiVideoPlane::U => &self.frame.buffer.data_u,
            FfiVideoPlane::V => &self.frame.buffer.data_v,
        };
        slice.as_ptr() as u64
    }

    /// The size in bytes of one plane, bounding reads through
    /// [`Self::plane_ptr`].
    pub fn plane_len(&self, plane: FfiVideoPlane) -> u64 {
        let slice: &[u8] = match plane {
            FfiVideoPlane::Y => &self.frame.buffer.data_y,
            FfiVideoPlane::U => &self.frame.buffer.data_u,
            FfiVideoPlane::V => &self.frame.buffer.data_v,
        };
        slice.len() as u64
    }
}

/// A captured I420 frame the host pushes into a video publication.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiVideoFrameData {
    pub width: u32,
    pub height: u32,
    pub rotation: FfiVideoRotation,
    /// Capture timestamp in microseconds; `0` lets the transport stamp it.
    pub timestamp_us: i64,
    pub data_y: Vec<u8>,
    pub stride_y: u32,
    pub data_u: Vec<u8>,
    pub stride_u: u32,
    pub data_v: Vec<u8>,
    pub stride_v: u32,
}

/// A live local publication: push captured frames into it. Obtained from
/// [`MediaSession::publish`](super::MediaSession::publish).
///
/// This is only a handle: dropping it (Kotlin/Swift `close()`/`destroy()`)
/// releases the wrapper and does NOT retract the publication — peers keep
/// seeing the stream. Retract it with
/// [`MediaSession::unpublish`](super::MediaSession::unpublish); the capture
/// methods then fail with a transport error instead of crashing, so a
/// capture thread racing the unpublish is safe.
#[derive(uniffi::Object)]
pub struct FfiLocalTrack {
    inner: Arc<dyn LocalTrackHandle>,
}

impl FfiLocalTrack {
    pub(super) fn new(inner: Arc<dyn LocalTrackHandle>) -> Self {
        Self { inner }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiLocalTrack {
    pub fn kind(&self) -> FfiStreamKind {
        self.inner.kind().into()
    }

    /// Push captured PCM. Suspends for pacing (backpressure on the capture
    /// loop); errors once the publication is gone.
    pub async fn capture_audio(&self, frame: FfiAudioFrame) -> Result<(), MediaFfiError> {
        self.inner
            .capture_audio(AudioFrame {
                data: pcm_from_le_bytes(&frame.data),
                sample_rate: frame.sample_rate,
                num_channels: frame.num_channels,
                samples_per_channel: frame.samples_per_channel,
            })
            .await
            .map_err(|error| MediaFfiError::Transport(error.to_string()))
    }

    /// Push a captured I420 frame (latest-wins at the encoder, no
    /// backpressure).
    pub fn capture_video(&self, frame: FfiVideoFrameData) -> Result<(), MediaFfiError> {
        self.inner
            .capture_video(VideoFrame {
                buffer: matrix_rtc_media::I420Buffer {
                    width: frame.width,
                    height: frame.height,
                    data_y: frame.data_y,
                    stride_y: frame.stride_y,
                    data_u: frame.data_u,
                    stride_u: frame.stride_u,
                    data_v: frame.data_v,
                    stride_v: frame.stride_v,
                },
                rotation: frame.rotation.into(),
                timestamp_us: frame.timestamp_us,
            })
            .map_err(|error| MediaFfiError::Transport(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The byte encoding is a wire contract with the host: a host that reads it
    /// as little-endian `int16` must get back exactly what was captured. Getting
    /// the endianness or the pairing wrong does not fail — it produces noise.
    #[test]
    fn pcm_survives_a_round_trip_through_bytes() {
        let samples: Vec<i16> = vec![0, 1, -1, i16::MAX, i16::MIN, 1234, -4321];
        let bytes = pcm_to_le_bytes(&samples);

        assert_eq!(bytes.len(), samples.len() * 2, "two bytes per sample");
        assert_eq!(&bytes[..2], &[0x00, 0x00]);
        assert_eq!(&bytes[2..4], &[0x01, 0x00], "little-endian, low byte first");
        assert_eq!(pcm_from_le_bytes(&bytes), samples);
    }

    /// A host that hands us a half sample gets the whole samples and no panic.
    /// Shifting by the stray byte instead would silently turn the rest of the
    /// frame into noise.
    #[test]
    fn a_trailing_odd_byte_is_dropped_rather_than_shifting_the_frame() {
        let mut bytes = pcm_to_le_bytes(&[7, 8]);
        bytes.push(0xff);

        assert_eq!(pcm_from_le_bytes(&bytes), vec![7, 8]);
    }
}
