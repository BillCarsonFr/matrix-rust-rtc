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

//! Publishing local media.
//!
//! Capture stays platform-side (camera, microphone, screen grabber): the
//! application pushes raw frames *into* the transport through a
//! [`LocalTrackHandle`], and the transport owns encoding, simulcast layering,
//! and pacing. Publications always go to the *own* focus — the one announced
//! in our membership's `transports`, where peers subscribe to us.

use async_trait::async_trait;

use crate::frame::{AudioFrame, VideoFrame};
use crate::participant::MediaStreamKind;
use crate::transport::TransportError;

/// PCM format the application will push into an audio publication.
#[derive(Clone, Copy, Debug)]
pub struct AudioSourceConfig {
    pub sample_rate: u32,
    pub num_channels: u32,
}

impl Default for AudioSourceConfig {
    fn default() -> Self {
        Self {
            sample_rate: 48_000,
            num_channels: 1,
        }
    }
}

/// Capture resolution of a video publication. Transports derive encoder and
/// simulcast layer settings from it.
#[derive(Clone, Copy, Debug)]
pub struct VideoSourceConfig {
    pub width: u32,
    pub height: u32,
}

/// What to publish.
#[derive(Clone, Debug)]
pub struct PublishOptions {
    /// Which stream this is (microphone, camera, screenshare, ...); peers see
    /// it under the same [`MediaStreamKind`].
    pub kind: MediaStreamKind,
    /// PCM format, for audio kinds.
    pub audio: Option<AudioSourceConfig>,
    /// Capture resolution, for video kinds.
    pub video: Option<VideoSourceConfig>,
    /// Publish multiple quality layers so receivers can pick per their
    /// [`MediaConstraints`](crate::constraints::MediaConstraints). Video only;
    /// transports without layered publishing ignore it.
    pub simulcast: bool,
}

impl PublishOptions {
    /// A microphone publication with the default PCM format.
    pub fn microphone() -> Self {
        Self {
            kind: MediaStreamKind::Microphone,
            audio: Some(AudioSourceConfig::default()),
            video: None,
            simulcast: false,
        }
    }

    /// A camera publication with simulcast enabled.
    pub fn camera(video: VideoSourceConfig) -> Self {
        Self {
            kind: MediaStreamKind::Camera,
            audio: None,
            video: Some(video),
            simulcast: true,
        }
    }

    /// A screenshare publication with simulcast enabled.
    pub fn screen_share(video: VideoSourceConfig) -> Self {
        Self {
            kind: MediaStreamKind::ScreenShare,
            audio: None,
            video: Some(video),
            simulcast: true,
        }
    }
}

/// A live local publication: push captured frames into it.
///
/// Handles are cheap `Arc`s. There is no explicit unpublish yet — the
/// publication ends with its connection (leave/close).
#[async_trait]
pub trait LocalTrackHandle: Send + Sync {
    fn kind(&self) -> MediaStreamKind;

    /// Push a chunk of captured PCM. Paced by the transport: resolves when
    /// the frame has been accepted, applying backpressure to the capture
    /// loop. Errors once the publication is gone.
    async fn capture_audio(&self, _frame: AudioFrame) -> Result<(), TransportError> {
        Err(TransportError::Unsupported(
            "this publication does not accept audio frames".into(),
        ))
    }

    /// Push a captured video frame. Delivery to the encoder is
    /// latest-frame-wins; there is no backpressure.
    fn capture_video(&self, _frame: VideoFrame) -> Result<(), TransportError> {
        Err(TransportError::Unsupported(
            "this publication does not accept video frames".into(),
        ))
    }
}
