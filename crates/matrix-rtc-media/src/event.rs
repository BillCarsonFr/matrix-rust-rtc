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

//! The unified call event stream.
//!
//! One stream merges what used to be two worlds: membership signalling from
//! `matrix-rtc-core` (who joined, who left, whose key arrived) and media
//! transport state (streams appearing, mute changes, connection health).
//! Events reference participants by `member_id` only — obtain frames through
//! [`CallEngine::remote_track`](crate::engine::CallEngine::remote_track).
//!
//! Events are plain data (`Clone + PartialEq`) so they can be broadcast to any
//! number of subscribers and later cross the FFI boundary unchanged.

use crate::participant::MediaStreamKind;

/// Why the call ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndedReason {
    /// We left the call deliberately.
    Left,
    /// The connection to our own focus (the one we publish on) closed and
    /// will not be re-established. Peer-focus connections closing do not end
    /// the call — they reconnect.
    ConnectionClosed {
        /// Transport-provided description (e.g. the LiveKit disconnect reason).
        message: String,
    },
}

/// An event on the unified call stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallEvent {
    /// A membership joined the call (including our own).
    ParticipantJoined { member_id: String, user_id: String },
    /// A membership left the call or expired.
    ParticipantLeft { member_id: String },
    /// A participant's stream became available; frames can be obtained now.
    StreamStarted {
        member_id: String,
        kind: MediaStreamKind,
    },
    /// A participant's stream went away.
    StreamStopped {
        member_id: String,
        kind: MediaStreamKind,
    },
    /// The publisher muted the stream (it stays subscribed).
    StreamMuted {
        member_id: String,
        kind: MediaStreamKind,
    },
    /// The publisher unmuted the stream.
    StreamUnmuted {
        member_id: String,
        kind: MediaStreamKind,
    },
    /// The set of currently speaking participants changed.
    ActiveSpeakers { member_ids: Vec<String> },
    /// A media decryption key for this participant was imported; their frames
    /// are decryptable from here on.
    KeyImported { member_id: String, key_index: u8 },
    /// A transport-level participant appeared that maps to no signalled
    /// membership. It gets no subscription (and could not be decrypted
    /// anyway); surfaced for diagnostics.
    UnknownParticipant { identity: String },
    /// Media connection health: `degraded` while a transport reconnects.
    MediaConnectionState { degraded: bool },
    /// The call is over; no further events follow.
    Ended { reason: EndedReason },
}
