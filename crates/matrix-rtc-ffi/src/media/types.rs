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

//! FFI DTOs mirroring the transport-agnostic media model, plus the
//! host-implemented OpenID token provider.

use async_trait::async_trait;

use super::MediaFfiError;

/// The kind of media stream (mirrors `matrix_rtc_media::MediaStreamKind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiStreamKind {
    Microphone,
    Camera,
    ScreenShare,
    ScreenShareAudio,
    Data,
}

impl From<FfiStreamKind> for matrix_rtc_media::MediaStreamKind {
    fn from(kind: FfiStreamKind) -> Self {
        match kind {
            FfiStreamKind::Microphone => Self::Microphone,
            FfiStreamKind::Camera => Self::Camera,
            FfiStreamKind::ScreenShare => Self::ScreenShare,
            FfiStreamKind::ScreenShareAudio => Self::ScreenShareAudio,
            FfiStreamKind::Data => Self::Data,
        }
    }
}

impl From<matrix_rtc_media::MediaStreamKind> for FfiStreamKind {
    fn from(kind: matrix_rtc_media::MediaStreamKind) -> Self {
        use matrix_rtc_media::MediaStreamKind as Kind;
        match kind {
            Kind::Microphone => Self::Microphone,
            Kind::Camera => Self::Camera,
            Kind::ScreenShare => Self::ScreenShare,
            Kind::ScreenShareAudio => Self::ScreenShareAudio,
            Kind::Data => Self::Data,
        }
    }
}

/// Live state of one stream of a participant.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiStreamState {
    pub kind: FfiStreamKind,
    pub muted: bool,
}

/// One joined membership of the call, with its current media streams.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiParticipant {
    /// `member.id` of the membership — unique per join, the roster key.
    pub member_id: String,
    pub user_id: String,
    pub device_id: Option<String>,
    pub is_local: bool,
    /// Whether any transport can reach this member's media.
    pub reachable: bool,
    pub streams: Vec<FfiStreamState>,
}

impl From<matrix_rtc_media::Participant> for FfiParticipant {
    fn from(participant: matrix_rtc_media::Participant) -> Self {
        Self {
            member_id: participant.member_id,
            user_id: participant.user_id,
            device_id: participant.device_id,
            is_local: participant.is_local,
            reachable: participant.reachable,
            streams: participant
                .streams
                .into_iter()
                .map(|stream| FfiStreamState {
                    kind: stream.kind.into(),
                    muted: stream.muted,
                })
                .collect(),
        }
    }
}

/// Why the call ended.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiEndedReason {
    /// We left deliberately.
    Left,
    /// The connection to our own focus closed and will not be
    /// re-established.
    ConnectionClosed { message: String },
}

/// An event on the unified call stream (mirrors
/// `matrix_rtc_media::CallEvent`). Consume via
/// [`MediaSession::next_event`](super::MediaSession::next_event).
#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiCallEvent {
    ParticipantJoined {
        member_id: String,
        user_id: String,
    },
    ParticipantLeft {
        member_id: String,
    },
    /// Frames for this stream can be obtained now (via `audio_stream` /
    /// `video_stream`).
    StreamStarted {
        member_id: String,
        kind: FfiStreamKind,
    },
    StreamStopped {
        member_id: String,
        kind: FfiStreamKind,
    },
    StreamMuted {
        member_id: String,
        kind: FfiStreamKind,
    },
    StreamUnmuted {
        member_id: String,
        kind: FfiStreamKind,
    },
    ActiveSpeakers {
        member_ids: Vec<String>,
    },
    /// This participant's media is decryptable from here on.
    KeyImported {
        member_id: String,
        key_index: u8,
    },
    /// A transport-level participant with no signalled membership; it gets
    /// no subscription. Diagnostics only.
    UnknownParticipant {
        identity: String,
    },
    /// Media health: `degraded` while any transport connection reconnects.
    MediaConnectionState {
        degraded: bool,
    },
    /// The call is over; no further events follow.
    Ended {
        reason: FfiEndedReason,
    },
}

impl From<matrix_rtc_media::CallEvent> for FfiCallEvent {
    fn from(event: matrix_rtc_media::CallEvent) -> Self {
        use matrix_rtc_media::CallEvent as Event;
        match event {
            Event::ParticipantJoined { member_id, user_id } => {
                Self::ParticipantJoined { member_id, user_id }
            }
            Event::ParticipantLeft { member_id } => Self::ParticipantLeft { member_id },
            Event::StreamStarted { member_id, kind } => Self::StreamStarted {
                member_id,
                kind: kind.into(),
            },
            Event::StreamStopped { member_id, kind } => Self::StreamStopped {
                member_id,
                kind: kind.into(),
            },
            Event::StreamMuted { member_id, kind } => Self::StreamMuted {
                member_id,
                kind: kind.into(),
            },
            Event::StreamUnmuted { member_id, kind } => Self::StreamUnmuted {
                member_id,
                kind: kind.into(),
            },
            Event::ActiveSpeakers { member_ids } => Self::ActiveSpeakers { member_ids },
            Event::KeyImported {
                member_id,
                key_index,
            } => Self::KeyImported {
                member_id,
                key_index,
            },
            Event::UnknownParticipant { identity } => Self::UnknownParticipant { identity },
            Event::MediaConnectionState { degraded } => Self::MediaConnectionState { degraded },
            Event::Ended { reason } => Self::Ended {
                reason: match reason {
                    matrix_rtc_media::EndedReason::Left => FfiEndedReason::Left,
                    matrix_rtc_media::EndedReason::ConnectionClosed { message } => {
                        FfiEndedReason::ConnectionClosed { message }
                    }
                },
            },
        }
    }
}

/// Coarse quality cap, for callers that don't know their render size.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum FfiQualityLimit {
    Low,
    Medium,
    High,
}

/// How much detail to receive for a video stream; the variants are mutually
/// exclusive. Prefer `Dimensions` — the renderer knows its surface size, the
/// server knows the publisher's layer ladder.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum FfiVideoDetail {
    Auto,
    Dimensions { width: u32, height: u32 },
    Quality { limit: FfiQualityLimit },
}

/// Subscription constraints for one stream of one participant (mirrors
/// `matrix_rtc_media::MediaConstraints` — see its docs for the semantics).
#[derive(Clone, Copy, Debug, uniffi::Record)]
pub struct FfiMediaConstraints {
    /// `false` releases the stream as fully as the transport supports; use
    /// for closed tiles, not scroll-by invisibility.
    pub enabled: bool,
    /// `false` pauses the stream (no data, instant resume).
    pub visible: bool,
    pub detail: FfiVideoDetail,
    /// Pause all video of this participant, keep audio.
    pub low_bandwidth: bool,
}

impl From<FfiMediaConstraints> for matrix_rtc_media::MediaConstraints {
    fn from(constraints: FfiMediaConstraints) -> Self {
        Self {
            enabled: constraints.enabled,
            visible: constraints.visible,
            detail: match constraints.detail {
                FfiVideoDetail::Auto => matrix_rtc_media::VideoDetail::Auto,
                FfiVideoDetail::Dimensions { width, height } => {
                    matrix_rtc_media::VideoDetail::Dimensions(matrix_rtc_media::Dimensions {
                        width,
                        height,
                    })
                }
                FfiVideoDetail::Quality { limit } => {
                    matrix_rtc_media::VideoDetail::Quality(match limit {
                        FfiQualityLimit::Low => matrix_rtc_media::QualityLimit::Low,
                        FfiQualityLimit::Medium => matrix_rtc_media::QualityLimit::Medium,
                        FfiQualityLimit::High => matrix_rtc_media::QualityLimit::High,
                    })
                }
            },
            low_bandwidth: constraints.low_bandwidth,
        }
    }
}

/// PCM format the host will push into an audio publication.
#[derive(Clone, Copy, Debug, uniffi::Record)]
pub struct FfiAudioSourceConfig {
    pub sample_rate: u32,
    pub num_channels: u32,
}

/// Capture resolution of a video publication.
#[derive(Clone, Copy, Debug, uniffi::Record)]
pub struct FfiVideoSourceConfig {
    pub width: u32,
    pub height: u32,
}

/// What to publish (mirrors `matrix_rtc_media::PublishOptions`).
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiPublishOptions {
    pub kind: FfiStreamKind,
    /// Required for audio kinds.
    pub audio: Option<FfiAudioSourceConfig>,
    /// Required for video kinds.
    pub video: Option<FfiVideoSourceConfig>,
    /// Publish multiple quality layers (video only).
    pub simulcast: bool,
}

impl From<FfiPublishOptions> for matrix_rtc_media::PublishOptions {
    fn from(options: FfiPublishOptions) -> Self {
        Self {
            kind: options.kind.into(),
            audio: options
                .audio
                .map(|audio| matrix_rtc_media::AudioSourceConfig {
                    sample_rate: audio.sample_rate,
                    num_channels: audio.num_channels,
                }),
            video: options
                .video
                .map(|video| matrix_rtc_media::VideoSourceConfig {
                    width: video.width,
                    height: video.height,
                }),
            simulcast: options.simulcast,
        }
    }
}

/// A Matrix OpenID token, as returned by
/// `POST /_matrix/client/v3/user/{userId}/openid/request_token`.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiOpenIdToken {
    pub access_token: String,
    pub token_type: String,
    pub matrix_server_name: String,
    pub expires_in_secs: u64,
}

/// Host-implemented source of Matrix OpenID tokens (MSC4195 token exchange).
///
/// Implement with the host's Matrix client; called whenever a focus
/// connection needs a fresh SFU JWT — including connections to *peers'*
/// foci, so expect more than one call per session.
///
/// (`async_trait` must sit *under* the uniffi attribute: uniffi parses the
/// original `async fn` tokens, `async_trait` then makes the trait
/// dyn-compatible for the Rust side.)
#[uniffi::export(with_foreign)]
#[async_trait]
pub trait OpenIdTokenProvider: Send + Sync {
    async fn get_open_id_token(&self) -> Result<FfiOpenIdToken, MediaFfiError>;
}

/// Adapts the host's provider to the transport's token source.
pub(super) struct TokenProviderAdapter(pub(super) std::sync::Arc<dyn OpenIdTokenProvider>);

#[async_trait]
impl matrix_rtc_livekit::OpenIdTokenSource for TokenProviderAdapter {
    async fn open_id_token(
        &self,
    ) -> Result<matrix_rtc_livekit::OpenIdToken, matrix_rtc_livekit::Error> {
        let token = self
            .0
            .get_open_id_token()
            .await
            .map_err(|error| matrix_rtc_livekit::Error::OpenIdToken(error.to_string()))?;
        Ok(matrix_rtc_livekit::OpenIdToken {
            access_token: token.access_token,
            token_type: token.token_type,
            matrix_server_name: token.matrix_server_name,
            expires_in: token.expires_in_secs,
        })
    }
}
