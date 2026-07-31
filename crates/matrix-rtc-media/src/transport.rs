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

//! The contract a media transport implements.
//!
//! A transport (LiveKit SFU today; P2P or WebTransport pub/sub later) knows
//! how to reach the media a member advertises in the `transports.published`
//! field of their `m.rtc.member` event, and how to map between MatrixRTC
//! memberships and its own participant identities.
//!
//! Members whose [`MediaTransport::connection_key`] is equal share one
//! [`TransportConnection`] — for LiveKit that key is the
//! `livekit_service_url`, matching MSC4195's rule that members on the same
//! focus share a room while every other focus needs its own subscribe-side
//! connection.
//!
//! Everything here is `Send + Sync`; transports must never require the
//! engine's tasks to be pinned to a `LocalSet`.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures_core::stream::BoxStream;
use matrix_rtc_core::{JoinedMembership, RtcTransport};
use tokio::sync::mpsc;

use crate::constraints::ResolvedConstraints;
use crate::frame::{AudioFrame, VideoFrame};
use crate::local::{LocalTrackHandle, PublishOptions};
use crate::participant::MediaStreamKind;

/// Errors produced by media transports.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The transport cannot serve the requested connection.
    #[error("unsupported transport: {0}")]
    Unsupported(String),
    /// Establishing the connection failed (auth, network, ...).
    #[error("transport connect failed: {0}")]
    Connect(String),
    /// The connection is gone.
    #[error("transport connection closed: {0}")]
    Closed(String),
}

/// Our own membership identity, as transports need it for authentication and
/// identity derivation.
#[derive(Clone, Debug)]
pub struct OwnMemberClaims {
    /// `member.id` of our join, unique per join.
    pub member_id: String,
    pub user_id: String,
    pub device_id: String,
}

/// Call-scoped context handed to [`MediaTransport::connect`].
#[derive(Clone, Debug)]
pub struct ConnectionContext {
    pub room_id: String,
    pub slot_id: String,
    pub member: OwnMemberClaims,
}

/// Handle to one subscribed remote track: opens frame streams on demand.
///
/// Handles are cheap `Arc`s; a stream borrows nothing — dropping it stops
/// frame delivery for that consumer only.
pub trait RemoteTrackHandle: Send + Sync {
    fn kind(&self) -> MediaStreamKind;

    /// A stream of decoded audio frames, for audio tracks.
    fn audio_frames(&self) -> Option<BoxStream<'static, AudioFrame>> {
        None
    }

    /// A stream of decoded video frames, for video tracks. Delivery is
    /// latest-frame-wins: slow consumers drop frames instead of buffering.
    fn video_frames(&self) -> Option<BoxStream<'static, VideoFrame>> {
        None
    }
}

/// State changes of one transport connection, translated into the
/// transport-neutral vocabulary.
///
/// `identity` is the transport-level participant identity (for LiveKit the
/// MSC4195 pseudonymous identity); the engine maps it back to a `member_id`
/// via [`MediaTransport::remote_identity`].
pub enum ConnectionEvent {
    RemoteJoined {
        identity: String,
    },
    RemoteLeft {
        identity: String,
    },
    TrackAdded {
        identity: String,
        kind: MediaStreamKind,
        track: Arc<dyn RemoteTrackHandle>,
    },
    TrackRemoved {
        identity: String,
        kind: MediaStreamKind,
    },
    TrackMuted {
        identity: String,
        kind: MediaStreamKind,
    },
    TrackUnmuted {
        identity: String,
        kind: MediaStreamKind,
    },
    ActiveSpeakers {
        identities: Vec<String>,
    },
    /// The connection lost its transport link and is re-establishing it.
    Reconnecting,
    Reconnected,
    /// The connection ended and will not recover.
    Closed {
        message: String,
    },
}

impl fmt::Debug for ConnectionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RemoteJoined { identity } => f
                .debug_struct("RemoteJoined")
                .field("identity", identity)
                .finish(),
            Self::RemoteLeft { identity } => f
                .debug_struct("RemoteLeft")
                .field("identity", identity)
                .finish(),
            Self::TrackAdded { identity, kind, .. } => f
                .debug_struct("TrackAdded")
                .field("identity", identity)
                .field("kind", kind)
                .finish_non_exhaustive(),
            Self::TrackRemoved { identity, kind } => f
                .debug_struct("TrackRemoved")
                .field("identity", identity)
                .field("kind", kind)
                .finish(),
            Self::TrackMuted { identity, kind } => f
                .debug_struct("TrackMuted")
                .field("identity", identity)
                .field("kind", kind)
                .finish(),
            Self::TrackUnmuted { identity, kind } => f
                .debug_struct("TrackUnmuted")
                .field("identity", identity)
                .field("kind", kind)
                .finish(),
            Self::ActiveSpeakers { identities } => f
                .debug_struct("ActiveSpeakers")
                .field("identities", identities)
                .finish(),
            Self::Reconnecting => f.write_str("Reconnecting"),
            Self::Reconnected => f.write_str("Reconnected"),
            Self::Closed { message } => f.debug_struct("Closed").field("message", message).finish(),
        }
    }
}

/// A media transport backend (LiveKit today; P2P/WebTransport later).
#[async_trait]
pub trait MediaTransport: Send + Sync {
    /// Stable name of the backend, matching the MSC4143 transport `type`
    /// (e.g. `"livekit"`). Used for logging and `can_subscribe` matching.
    fn transport_type(&self) -> &'static str;

    /// The connection grouping key for a published transport, if this backend
    /// can serve it. Members with equal keys share one connection.
    fn connection_key(&self, transport: &RtcTransport) -> Option<String>;

    /// The identity this member will have on the media plane, used to
    /// reverse-map transport participants to memberships. `None` when the
    /// membership lacks what the derivation needs (e.g. no attributable
    /// sending device).
    fn remote_identity(&self, member: &JoinedMembership) -> Option<String>;

    /// Open the connection for `connection_key` and start translating its
    /// state into [`ConnectionEvent`]s. The returned receiver ends (after a
    /// final [`ConnectionEvent::Closed`], when the transport can tell) when
    /// the connection is gone.
    async fn connect(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> Result<
        (
            Box<dyn TransportConnection>,
            mpsc::UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    >;
}

/// A live connection to one focus.
#[async_trait]
pub trait TransportConnection: Send + Sync {
    /// The grouping key this connection serves.
    fn connection_key(&self) -> &str;

    /// Publish a local track on this connection (only ever called on the own
    /// focus). The default rejects publishing, for receive-only transports.
    async fn publish(
        &self,
        _options: PublishOptions,
    ) -> Result<Arc<dyn LocalTrackHandle>, TransportError> {
        Err(TransportError::Unsupported(
            "this transport cannot publish".into(),
        ))
    }

    /// Apply resolved subscription constraints for one of this connection's
    /// remote participants. Transports interpret — LiveKit maps to
    /// enabled/dimensions/quality (`UpdateTrackSettings`); a transport with
    /// no subscription control may ignore them (the default).
    async fn apply_constraints(
        &self,
        _identity: &str,
        _kind: MediaStreamKind,
        _resolved: ResolvedConstraints,
    ) -> Result<(), TransportError> {
        Ok(())
    }

    /// Close the connection. Idempotent.
    async fn close(&self) -> Result<(), TransportError>;
}
