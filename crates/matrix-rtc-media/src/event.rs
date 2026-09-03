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

use matrix_rtc_core::KeyRejection;

use crate::participant::MediaStreamKind;

/// Whether a participant's frames are encrypting and decrypting cleanly.
///
/// Reported per participant rather than per stream: the transport's frame
/// cryptor is keyed by participant identity, so a failure does not say which
/// of their tracks it came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameEncryptionState {
    /// Frames are being encrypted and decrypted normally.
    Ok,
    /// Frames are arriving with a key index we hold no key for — their media
    /// key has not reached us (or reached us under the wrong identity).
    MissingKey,
    /// We hold a key for the index the frames carry, but it does not decrypt
    /// them. The two sides disagree about the key material itself.
    DecryptionFailed,
    /// Our *outgoing* frames failed to encrypt, so peers receive nothing
    /// usable from us.
    EncryptionFailed,
    /// The transport's cryptor failed internally.
    InternalError,
}

impl FrameEncryptionState {
    /// Whether this state means media is not flowing usably.
    pub fn is_failure(&self) -> bool {
        !matches!(self, Self::Ok)
    }
}

/// What the media layer can say about a frame-encryption failure.
///
/// The state itself comes from the transport's frame cryptor, which reports
/// *that* it cannot decrypt but not why. This adds the half the media layer does
/// know — which keys it has actually installed for that participant — because the
/// two cases behind a `MissingKey` need completely different investigations:
/// a key that never arrived (or arrived under an identity the transport does not
/// use) is a signalling or identity problem, while frames carrying an index we
/// have not been given yet is a rotation still in flight.
///
/// Reasons a key was *refused* live in the core, which is the only layer that
/// sees the to-device message; those surface separately as
/// [`CallEvent::KeyDiscarded`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameEncryptionDiagnostic {
    /// The state is not a failure, so there is nothing to explain.
    NotApplicable,
    /// No media key has been installed for this participant under any index.
    /// Either none ever arrived, or it arrived under a different identity.
    NoKeyInstalled,
    /// Keys are installed at these indices, so the frames must be carrying a
    /// different one — or the material itself disagrees.
    KeysInstalled {
        /// Key indices installed for this participant, in the order they were
        /// imported.
        key_indices: Vec<u8>,
    },
}

/// One speaking member and how loud they are.
#[derive(Clone, Debug, PartialEq)]
pub struct SpeakingMember {
    pub member_id: String,
    /// `0.0` (silent) to `1.0` (loudest); `0.0` from transports that report no
    /// level.
    pub level: f32,
}

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
///
/// `PartialEq` but not `Eq`: an audio level is an `f32`.
#[derive(Clone, Debug, PartialEq)]
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
    ///
    /// Carries each speaker's audio level alongside their `member_id`: "who is
    /// talking" and "how loud" arrive together from the transport, and splitting
    /// them forces a host to meter the PCM itself for the second half.
    ActiveSpeakers { speakers: Vec<SpeakingMember> },
    /// A media decryption key for this participant was imported; their frames
    /// are decryptable from here on.
    KeyImported { member_id: String, key_index: u8 },
    /// Frame encryption state for a participant's media changed.
    ///
    /// Anything but [`FrameEncryptionState::Ok`] means their frames are not
    /// decoding — note that the RTP itself may be arriving perfectly well, so
    /// the receive path keeps producing frames (silence, or a frozen picture).
    /// Pair with
    /// [`CallEngine::receive_stats`](crate::engine::CallEngine::receive_stats)
    /// to tell a key failure from an empty network path.
    FrameEncryptionState {
        member_id: String,
        state: FrameEncryptionState,
        /// What the media layer knows about the failure — chiefly whether any key
        /// was installed for this participant at all.
        diagnostic: FrameEncryptionDiagnostic,
    },
    /// A media key for this participant was refused, so their frames will not
    /// decrypt. Carries the reason, which is otherwise only ever logged.
    ///
    /// Distinct from a key that never arrived: this one arrived and was rejected,
    /// which is a configuration or trust problem (an unverified device, say)
    /// rather than a delivery one.
    KeyDiscarded {
        /// The member the key claimed to be for.
        member_id: String,
        /// The rejected key's index, when the message got far enough to have one.
        key_index: Option<u8>,
        /// Who sent it, as far as decryption metadata could attribute it.
        sender_user_id: Option<String>,
        sender_device_id: Option<String>,
        /// Why it was refused. Typed rather than a message, so a host can act on
        /// it — `NotCrossSigned` is a "verify this device" prompt, not just a log
        /// line.
        reason: KeyRejection,
    },
    /// A participant raised their hand (Element Call's `m.reaction` annotation
    /// of their membership). Also set on the roster as
    /// [`Participant::hand_raised_at_ms`](crate::participant::Participant::hand_raised_at_ms).
    HandRaised {
        member_id: String,
        raised_at_ms: u64,
    },
    /// A participant lowered their hand, or left with it up.
    HandLowered { member_id: String },
    /// A participant sent an emoji reaction. Transient: show `emoji` for a few
    /// seconds (Element Call uses three) and play `sound` if reaction sounds
    /// are on. The SDK does no audio; `sound` is the base name of the asset
    /// to play (`clap`, `party`, …, or `generic` for an unknown `name`), or
    /// `None` for a silent reaction.
    Reaction {
        member_id: String,
        emoji: String,
        /// The reaction's `name` as sent, e.g. `clapping`. Empty if none.
        name: String,
        sound: Option<String>,
    },
    /// A transport-level participant appeared that maps to no signalled
    /// membership. It gets no subscription (and could not be decrypted
    /// anyway); surfaced for diagnostics.
    UnknownParticipant { identity: String },
    /// Media connection health: `degraded` while a transport reconnects.
    MediaConnectionState { degraded: bool },
    /// The call is over; no further events follow.
    Ended { reason: EndedReason },
}
