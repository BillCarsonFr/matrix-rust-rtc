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

//! Per-participant, per-stream subscription constraints.
//!
//! Applications describe *how they consume* a stream — is it rendered, at
//! what size, under what bandwidth budget — and transports translate the
//! resolved form into their own control surface. The declarative shape is
//! deliberately transport-free so a P2P or WebTransport backend can interpret
//! the same values with its own signalling.
//!
//! The [`CallEngine`](crate::engine::CallEngine) stores constraints and hands
//! transports only the folded [`ResolvedConstraints`]; transports interpret,
//! they never police. Whatever is asked for is a *cap*: transports and
//! servers still adapt downwards under congestion on their own.
//!
//! Two design points, learned from how SFUs actually behave:
//!
//! - **Not rendering has two strengths.** A stream that is merely
//!   off-screen ([`MediaConstraints::visible`] `= false`) is *paused* —
//!   still subscribed, no data flowing, resumes instantly when scrolled
//!   back. A stream the app does not want at all
//!   ([`MediaConstraints::enabled`] `= false`) may be released as fully as
//!   the transport supports — ideally unsubscribed (bandwidth **and**
//!   decoder freed, resume renegotiates), at minimum paused. The
//!   distinction is in the model so transports can honour it; large calls
//!   need it — a 20-tile grid cannot hold 20 warm video decoders on a
//!   phone.
//! - **Size beats quality as a hint.** The renderer knows its surface size
//!   in pixels; it does not know the publisher's simulcast layer ladder.
//!   [`VideoDetail::Dimensions`] lets the server pick the closest layer and
//!   is the preferred control (LiveKit has deprecated the quality field in
//!   its wire protocol in favour of dimensions). The two are mutually
//!   exclusive by construction.

use crate::participant::MediaStreamKind;

/// A rendered size in pixels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dimensions {
    pub width: u32,
    pub height: u32,
}

/// Coarse cap on the quality layer to receive, for callers that don't know
/// their render size. Prefer [`VideoDetail::Dimensions`] when the size is
/// known.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualityLimit {
    Low,
    Medium,
    High,
}

/// How much detail to receive for a video stream. The variants are mutually
/// exclusive — a size hint and a quality cap cannot both be in force.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VideoDetail {
    /// No preference: the transport/server delivers the best it can afford.
    #[default]
    Auto,
    /// The size of the surface this stream is rendered into; the transport
    /// picks the smallest sufficient layer. Preferred.
    Dimensions(Dimensions),
    /// An explicit layer cap, when no render size is known.
    Quality(QualityLimit),
}

/// What the application wants from one stream of one participant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MediaConstraints {
    /// Hard switch. `false` releases the stream as fully as the transport
    /// supports (ideally a full unsubscribe: bandwidth and decoder freed,
    /// resume renegotiates). Use for tiles the user closed, not for
    /// scroll-by invisibility.
    pub enabled: bool,
    /// Whether the stream is currently rendered on screen. `false` pauses
    /// the stream without unsubscribing, so it resumes instantly.
    pub visible: bool,
    /// How much detail to receive (video kinds only).
    pub detail: VideoDetail,
    /// Bandwidth-saver mode: pause all video of this participant, keep audio.
    pub low_bandwidth: bool,
}

impl Default for MediaConstraints {
    fn default() -> Self {
        Self {
            enabled: true,
            visible: true,
            detail: VideoDetail::Auto,
            low_bandwidth: false,
        }
    }
}

/// The demand state of one stream after folding the constraint fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamDemand {
    /// Subscribed, data flowing.
    Active,
    /// Subscribed but paused server-side: no data, instant resume.
    Paused,
    /// Released as fully as the transport supports: ideally unsubscribed
    /// (no bandwidth, no decoder, resume renegotiates), at minimum paused.
    Off,
}

/// The folded form handed to transports: one effective setting with the
/// interactions between the [`MediaConstraints`] fields already resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedConstraints {
    pub demand: StreamDemand,
    /// Detail preference; always [`VideoDetail::Auto`] for audio kinds.
    pub detail: VideoDetail,
}

impl MediaConstraints {
    /// Fold the constraint fields into the effective per-stream setting.
    ///
    /// `enabled = false` wins over everything (full unsubscribe);
    /// invisibility pauses; `low_bandwidth` pauses video kinds but leaves
    /// audio flowing.
    pub fn resolve(&self, kind: MediaStreamKind) -> ResolvedConstraints {
        let is_video = matches!(kind, MediaStreamKind::Camera | MediaStreamKind::ScreenShare);
        let demand = if !self.enabled {
            StreamDemand::Off
        } else if !self.visible || (self.low_bandwidth && is_video) {
            StreamDemand::Paused
        } else {
            StreamDemand::Active
        };
        ResolvedConstraints {
            demand,
            detail: if is_video {
                self.detail
            } else {
                VideoDetail::Auto
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_keep_streams_active() {
        let resolved = MediaConstraints::default().resolve(MediaStreamKind::Camera);
        assert_eq!(resolved.demand, StreamDemand::Active);
        assert_eq!(resolved.detail, VideoDetail::Auto);
    }

    #[test]
    fn disabled_turns_the_stream_off_and_wins_over_everything() {
        let constraints = MediaConstraints {
            enabled: false,
            visible: true,
            detail: VideoDetail::Quality(QualityLimit::High),
            low_bandwidth: false,
        };
        assert_eq!(
            constraints.resolve(MediaStreamKind::Camera).demand,
            StreamDemand::Off
        );
        assert_eq!(
            constraints.resolve(MediaStreamKind::Microphone).demand,
            StreamDemand::Off
        );
    }

    #[test]
    fn invisible_pauses_instead_of_unsubscribing() {
        let constraints = MediaConstraints {
            visible: false,
            ..Default::default()
        };
        assert_eq!(
            constraints.resolve(MediaStreamKind::Camera).demand,
            StreamDemand::Paused
        );
        assert_eq!(
            constraints.resolve(MediaStreamKind::Microphone).demand,
            StreamDemand::Paused
        );
    }

    #[test]
    fn low_bandwidth_pauses_video_but_keeps_audio() {
        let constraints = MediaConstraints {
            low_bandwidth: true,
            ..Default::default()
        };
        assert_eq!(
            constraints.resolve(MediaStreamKind::Camera).demand,
            StreamDemand::Paused
        );
        assert_eq!(
            constraints.resolve(MediaStreamKind::ScreenShare).demand,
            StreamDemand::Paused
        );
        assert_eq!(
            constraints.resolve(MediaStreamKind::Microphone).demand,
            StreamDemand::Active
        );
        assert_eq!(
            constraints
                .resolve(MediaStreamKind::ScreenShareAudio)
                .demand,
            StreamDemand::Active
        );
    }

    #[test]
    fn detail_only_applies_to_video_kinds() {
        let constraints = MediaConstraints {
            detail: VideoDetail::Dimensions(Dimensions {
                width: 320,
                height: 180,
            }),
            ..Default::default()
        };
        assert_eq!(
            constraints.resolve(MediaStreamKind::Camera).detail,
            VideoDetail::Dimensions(Dimensions {
                width: 320,
                height: 180,
            })
        );
        assert_eq!(
            constraints.resolve(MediaStreamKind::Microphone).detail,
            VideoDetail::Auto
        );
    }
}
