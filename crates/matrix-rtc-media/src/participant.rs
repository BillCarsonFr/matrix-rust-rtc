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

//! The participant roster as applications see it.
//!
//! A [`Participant`] is a MatrixRTC *membership* (one `m.rtc.member` join,
//! keyed by its unique `member_id`) enriched with live media state. The roster
//! is derived from the core's membership snapshots — signalling is the source
//! of truth for who is in the call; transports only attach media to entries
//! that already exist.

/// The kind of media stream a participant publishes.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum MediaStreamKind {
    Microphone,
    Camera,
    ScreenShare,
    ScreenShareAudio,
    Data,
}

/// Live state of one media stream of a participant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamState {
    pub kind: MediaStreamKind,
    /// Whether the publisher has muted the stream.
    pub muted: bool,
}

/// One joined membership of the call, with its current media streams.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Participant {
    /// `member.id` of the membership — unique per join, the roster key.
    pub member_id: String,
    /// Matrix user ID of the member.
    pub user_id: String,
    /// Device that sent (encrypted) the membership event, when attributable.
    pub device_id: Option<String>,
    /// Whether this is our own membership.
    pub is_local: bool,
    /// Whether any registered transport can reach this member's media. A
    /// member publishing only unsupported transports stays in the roster
    /// (signalling truth) but never gets streams.
    pub reachable: bool,
    /// Streams currently published by this participant, in arrival order.
    pub streams: Vec<StreamState>,
}
