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
//! Applications describe *how they consume* a stream — is it rendered, at what
//! size, under what bandwidth budget — and transports translate the resolved
//! form into their own control surface (LiveKit: `set_enabled` /
//! `update_video_dimensions` / `set_video_quality`, which the SFU combines
//! with simulcast and dynacast). The declarative shape is deliberately
//! transport-free so a P2P or WebTransport backend can interpret the same
//! values with its own signalling.
//!
//! The [`CallEngine`](crate::engine::CallEngine) stores constraints and hands
//! transports only the folded [`ResolvedConstraints`]; transports interpret,
//! they never police.

use crate::participant::MediaStreamKind;

/// A rendered size in pixels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dimensions {
    pub width: u32,
    pub height: u32,
}

/// Cap on the simulcast quality layer to receive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualityLimit {
    Low,
    Medium,
    High,
}

/// What the application wants from one stream of one participant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MediaConstraints {
    /// Hard switch: `false` stops receiving the stream entirely.
    pub enabled: bool,
    /// Whether the stream is currently rendered on screen. Invisible streams
    /// are paused without unsubscribing, so they resume instantly.
    pub visible: bool,
    /// Size of the surface the stream is rendered into, used to pick the
    /// smallest sufficient simulcast layer.
    pub desired_dimensions: Option<Dimensions>,
    /// Explicit upper bound on the quality layer, on top of the size-derived
    /// choice.
    pub max_quality: Option<QualityLimit>,
    /// Bandwidth-saver mode: keep audio, drop all video of this participant.
    pub low_bandwidth: bool,
}

impl Default for MediaConstraints {
    fn default() -> Self {
        Self {
            enabled: true,
            visible: true,
            desired_dimensions: None,
            max_quality: None,
            low_bandwidth: false,
        }
    }
}

/// The folded form handed to transports: one effective setting with the
/// interactions between the [`MediaConstraints`] fields already resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedConstraints {
    /// Whether the stream should currently flow at all.
    pub active: bool,
    pub dimensions: Option<Dimensions>,
    pub max_quality: Option<QualityLimit>,
}

impl MediaConstraints {
    /// Fold the constraint fields into the effective per-stream setting.
    ///
    /// `low_bandwidth` suppresses video kinds but leaves audio flowing;
    /// `enabled`/`visible` gate every kind.
    pub fn resolve(&self, kind: MediaStreamKind) -> ResolvedConstraints {
        let is_video = matches!(kind, MediaStreamKind::Camera | MediaStreamKind::ScreenShare);
        let active = self.enabled && self.visible && !(self.low_bandwidth && is_video);
        ResolvedConstraints {
            active,
            dimensions: self.desired_dimensions,
            max_quality: self.max_quality,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_keep_streams_active() {
        let resolved = MediaConstraints::default().resolve(MediaStreamKind::Camera);
        assert!(resolved.active);
        assert_eq!(resolved.dimensions, None);
        assert_eq!(resolved.max_quality, None);
    }

    #[test]
    fn low_bandwidth_drops_video_but_keeps_audio() {
        let constraints = MediaConstraints {
            low_bandwidth: true,
            ..Default::default()
        };
        assert!(!constraints.resolve(MediaStreamKind::Camera).active);
        assert!(!constraints.resolve(MediaStreamKind::ScreenShare).active);
        assert!(constraints.resolve(MediaStreamKind::Microphone).active);
        assert!(
            constraints
                .resolve(MediaStreamKind::ScreenShareAudio)
                .active
        );
    }

    #[test]
    fn disabled_or_invisible_gates_everything() {
        for constraints in [
            MediaConstraints {
                enabled: false,
                ..Default::default()
            },
            MediaConstraints {
                visible: false,
                ..Default::default()
            },
        ] {
            assert!(!constraints.resolve(MediaStreamKind::Microphone).active);
            assert!(!constraints.resolve(MediaStreamKind::Camera).active);
        }
    }
}
