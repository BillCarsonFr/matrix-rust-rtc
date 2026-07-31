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

//! Transport-agnostic MatrixRTC media model.
//!
//! [`matrix-rtc-core`] answers *who is in the call* (memberships, keys); a
//! transport crate (e.g. `matrix-rtc-livekit`) answers *how bytes flow*. This
//! crate sits between them and gives applications one vocabulary for both:
//!
//! - [`Participant`]s keyed by MatrixRTC `member_id`, each with media
//!   [`StreamState`]s (microphone, camera, screenshare, ...);
//! - frame streams ([`AudioFrame`], [`VideoFrame`]) obtained per participant
//!   through [`RemoteTrackHandle`], with no transport types on the surface;
//! - per-participant [`MediaConstraints`] (visibility, rendered size, quality
//!   cap) that transports translate into subscribe-side simulcast control;
//! - a unified [`CallEvent`] stream merging membership signalling and media
//!   transport state.
//!
//! The [`CallEngine`] ties these together: it watches the core's membership
//! snapshots, maps transport-level participant identities back to memberships,
//! and (from Phase 1) maintains one connection per focus so that MSC4195
//! multi-SFU calls look like a single flat participant set.
//!
//! Transports implement [`MediaTransport`]/[`TransportConnection`]; everything
//! in this crate is `Send`, deliberately on the other side of a channel
//! boundary from the core's `?Send` command futures (the only core input is
//! the `watch` membership snapshot channel, whose payload is plain data).
//!
//! [`matrix-rtc-core`]: matrix_rtc_core

pub mod constraints;
pub mod engine;
pub mod event;
pub mod frame;
pub mod local;
pub mod participant;
pub mod transport;

pub use constraints::{
    Dimensions, MediaConstraints, QualityLimit, ResolvedConstraints, StreamDemand, VideoDetail,
};
pub use engine::{CallEngine, EngineConfig, EngineHandle};
pub use event::{CallEvent, EndedReason};
pub use frame::{AudioFrame, I420Buffer, VideoFrame, VideoRotation};
pub use local::{AudioSourceConfig, LocalTrackHandle, PublishOptions, VideoSourceConfig};
pub use participant::{MediaStreamKind, Participant, StreamState};
pub use transport::{
    ConnectionContext, ConnectionEvent, MediaTransport, OwnMemberClaims, RemoteTrackHandle,
    TransportConnection, TransportError,
};
