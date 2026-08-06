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

//! Media over the FFI: participants with observable frame streams.
//!
//! Behind the `media` cargo feature (default **off**: it pulls the LiveKit
//! client and therefore libwebrtc — roughly 8–15 MB per ABI). The slim
//! signalling-only artifact stays available without it.
//!
//! The shape mirrors the native `Call` facade, adapted to an FFI host that
//! owns its own Matrix stack:
//!
//! 1. The host drives the [`RtcSessionManagerHandle`](crate::RtcSessionManagerHandle)
//!    exactly as before (sticky events in, commands out) and `join`s the slot.
//! 2. [`connect_media_session`] then attaches media: it wires the E2EE key
//!    bridge into the core, starts the transport-agnostic `CallEngine`
//!    (which opens connections to every peer's focus — MSC4195 multi-SFU),
//!    and connects the own-focus SFU with per-participant frame encryption.
//!    It reads the `member.id` from that join rather than taking one from the
//!    host — our MSC4195 participant identity is derived from it, so the two
//!    must not be able to disagree.
//! 3. The host consumes [`MediaSession`]: the unified event stream
//!    (`next_event`, bridged to Kotlin `Flow` / Swift `AsyncStream`), the
//!    participant roster, per-stream constraints, frame streams
//!    (audio by value; video as objects with both safe copies and zero-copy
//!    plane pointers), and local publications it pushes captured frames into.
//! 4. Keys: outbound distribution already flows through the host's
//!    [`CommandSenderCallback`](crate::CommandSenderCallback)
//!    (`sendToDeviceMessage`); inbound, the host feeds decrypted
//!    `m.rtc.encryption_key` to-device messages to
//!    [`RtcSessionManagerHandle::receive_encryption_key`](crate::RtcSessionManagerHandle::receive_encryption_key).
//!
//! Everything media runs on a dedicated multithreaded tokio runtime
//! ([`runtime`]); the manager's `?Send` futures never touch it.

mod frames;
mod session;
mod types;

#[cfg(target_os = "android")]
mod android;

#[cfg(test)]
mod tests;

pub use frames::{
    AudioFrameStream, FfiAudioFrame, FfiLocalTrack, FfiVideoFrameData, FfiVideoPlane,
    FfiVideoRotation, VideoFrameRef, VideoFrameStream,
};
pub use session::{MediaSession, MediaSessionConfig, connect_media_session};
pub use types::{
    FfiAudioSourceConfig, FfiCallEvent, FfiEndedReason, FfiFrameEncryptionDiagnostic,
    FfiFrameEncryptionState, FfiKeyRejection, FfiMediaConstraints, FfiOpenIdToken, FfiParticipant,
    FfiPublishOptions, FfiQualityLimit, FfiReceiveStats, FfiSpeakingMember, FfiStreamKind,
    FfiStreamState, FfiVideoDetail, FfiVideoSourceConfig, OpenIdTokenProvider,
};

/// Errors produced by the media layer of the FFI.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum MediaFfiError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// Media transport failure (token exchange, SFU connection, publication).
    #[error("media transport error: {0}")]
    Transport(String),
    /// The slot is not joined (or its session is gone); join before
    /// connecting media.
    #[error("not joined: {0}")]
    NotJoined(String),
    /// The host's OpenID token provider failed.
    #[error("token acquisition failed: {0}")]
    Token(String),
    #[error("internal lock poisoned")]
    InternalLockPoisoned,
}

/// The multithreaded runtime every media task runs on (engine actor,
/// connection pool, SFU IO). Lazily created on first use.
///
/// Shared with the synchronous FFI entry points ([`crate::runtime`]) rather
/// than a second runtime of its own: one pool of worker threads instead of two
/// on a mobile device, and the entry points need a multi-threaded runtime for
/// exactly the same reason media does. Media's futures are spawned (so `Send`);
/// the entry points only ever block on theirs, which imposes no such bound.
pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    crate::runtime::runtime()
}
