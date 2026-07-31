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
//!   mono — the copy is noise at 100 calls/s).
//! - **Video frames are objects** ([`VideoFrameRef`]) exposing both safe
//!   copies (`data_y`/`data_u`/`data_v`) and zero-copy plane pointers
//!   (`plane_ptr` + strides) for renderers that consume raw memory. The
//!   pointers are valid for as long as the object is referenced — lifetime
//!   is the object, not a manual handle.
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
    /// Interleaved samples, `samples_per_channel * num_channels` long.
    pub data: Vec<i16>,
    pub sample_rate: u32,
    pub num_channels: u32,
    pub samples_per_channel: u32,
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
            data: frame.data,
            sample_rate: frame.sample_rate,
            num_channels: frame.num_channels,
            samples_per_channel: frame.samples_per_channel,
        })
    }
}

/// A stream of received video frames (latest-frame-wins).
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
/// Renderers on a fast path read plane memory directly via [`Self::plane_ptr`]
/// and the strides — valid for as long as this object is referenced. The
/// `data_*` accessors return safe copies instead.
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

    /// A safe copy of one plane's bytes.
    pub fn data(&self, plane: FfiVideoPlane) -> Vec<u8> {
        match plane {
            FfiVideoPlane::Y => self.frame.buffer.data_y.clone(),
            FfiVideoPlane::U => self.frame.buffer.data_u.clone(),
            FfiVideoPlane::V => self.frame.buffer.data_v.clone(),
        }
    }

    /// The raw address of one plane's bytes, for zero-copy renderers
    /// (`CVPixelBuffer` wrapping, libyuv via JNI, ...).
    ///
    /// Valid only while this object is referenced — hold the frame for as
    /// long as the memory is read, then drop it.
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
                data: frame.data,
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
