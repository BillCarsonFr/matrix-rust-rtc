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

//! The FFI media session: the host-facing equivalent of the native `Call`
//! facade's media half, layered on a slot the host has already joined
//! through the [`RtcSessionManagerHandle`](crate::RtcSessionManagerHandle).

use std::sync::Arc;

use tokio::sync::Mutex as TokioMutex;
use tokio::sync::broadcast;

use matrix_rtc_core::RtcIdentityMapper;
use matrix_rtc_livekit::identity::pseudonymous_identity;
use matrix_rtc_livekit::{
    LiveKitMediaTransport, LiveKitTransportConnection, MediaKeyBridge, msc4195_key_provider,
};
use matrix_rtc_media::{
    CallEngine, CallEvent, ConnectionContext, EngineConfig, OwnMemberClaims,
    TransportConnection as _,
};

use super::frames::{AudioFrameStream, FfiLocalTrack, VideoFrameStream};
use super::types::{
    FfiCallEvent, FfiMediaConstraints, FfiParticipant, FfiPublishOptions, FfiReceiveStats,
    FfiStreamKind, OpenIdTokenProvider, TokenProviderAdapter,
};
use super::{MediaFfiError, runtime};
use crate::RtcSessionManagerHandle;

/// Identifies the joined slot to attach media to, and how to reach the SFU.
#[derive(Clone, Debug, uniffi::Record)]
pub struct MediaSessionConfig {
    pub room_id: String,
    pub slot_id: String,
    /// The `member.id` this device joined the slot with — the same value
    /// passed as `membership_id` in the join params.
    pub member_id: String,
    pub user_id: String,
    pub device_id: String,
    /// The MSC4195 authorisation-service URL of the focus we publish on —
    /// the same URL announced in our membership's transport. (Peers' foci
    /// are discovered from their memberships automatically.)
    pub livekit_service_url: String,
}

/// Attach media to a joined slot: wire frame-key signalling into the core,
/// start the engine (which connects to every peer's focus), and connect the
/// own-focus SFU with per-participant frame E2EE.
///
/// Preconditions: the manager has a command sender, the host feeds it sticky
/// events/room state, and `join` succeeded for this room/slot with
/// `membership_id = config.member_id`.
#[uniffi::export(async_runtime = "tokio")]
pub async fn connect_media_session(
    manager: Arc<RtcSessionManagerHandle>,
    config: MediaSessionConfig,
    token_provider: Arc<dyn OpenIdTokenProvider>,
) -> Result<Arc<MediaSession>, MediaFfiError> {
    // Everything media lives on the dedicated runtime; hopping onto it here
    // means every internally spawned task (engine actor, pool, IO) inherits
    // the right context regardless of which thread the FFI call came in on.
    runtime()
        .spawn(build_media_session(manager, config, token_provider))
        .await
        .map_err(|error| MediaFfiError::Transport(format!("media task panicked: {error}")))?
}

async fn build_media_session(
    manager: Arc<RtcSessionManagerHandle>,
    config: MediaSessionConfig,
    token_provider: Arc<dyn OpenIdTokenProvider>,
) -> Result<Arc<MediaSession>, MediaFfiError> {
    log::info!(
        "media: connecting [{}/{}] member={} user={} device={} focus={}",
        config.room_id,
        config.slot_id,
        config.member_id,
        config.user_id,
        config.device_id,
        config.livekit_service_url,
    );

    // Frame encryption: one shared KeyProvider feeds every SFU connection
    // (keys are indexed by pseudonymous identity, globally unique per
    // membership) and the bridge that imports keys the core signals.
    let provider = msc4195_key_provider();
    let bridge = Arc::new(MediaKeyBridge::with_provider(provider.clone()));

    // Wire the core's encryption manager to the bridge and to the MSC4195
    // identity derivation, and take the membership snapshot channel the
    // engine consumes. All sync — the manager mutex is never held across an
    // await.
    let memberships = {
        let mut mgr =
            crate::lock_mutex(&manager.inner).map_err(|_| MediaFfiError::InternalLockPoisoned)?;
        let memberships = mgr
            .subscribe_membership_snapshots(&config.room_id, &config.slot_id)
            .ok_or_else(|| {
                log::warn!(
                    "media: no session for [{}/{}] — join the slot before connecting media",
                    config.room_id,
                    config.slot_id,
                );
                MediaFfiError::NotJoined(format!(
                    "no session for {}/{} — join the slot first",
                    config.room_id, config.slot_id
                ))
            })?;
        // Mapper before handler: the replay below derives MSC4195 identities
        // through it, and installing it second would replay peer keys under the
        // raw `member_id` fallback — an identity the SFU never uses, which is
        // indistinguishable from importing nothing.
        let identity_mapper: RtcIdentityMapper =
            Arc::new(|user_id: &str, device_id: &str, member_id: &str| {
                pseudonymous_identity(user_id, device_id, member_id)
            });
        mgr.set_encryption_identity_mapper(&config.room_id, &config.slot_id, identity_mapper);

        if !mgr.set_encryption_signal_handler(&config.room_id, &config.slot_id, bridge.clone()) {
            log::warn!(
                "media: session [{}/{}] has no encryption manager — join the slot first",
                config.room_id,
                config.slot_id,
            );
            return Err(MediaFfiError::NotJoined(
                "the session has no encryption manager — join the slot first".into(),
            ));
        }

        // Keys signalled between `join` and now were stored but dropped —
        // nothing was listening. Without this, every participant whose key
        // arrived before media attached stays undecryptable until a rotation,
        // which only a membership change triggers.
        //
        // `block_on` rather than `.await`: this function is spawned, so its
        // future must be `Send`, and awaiting here would hold the manager's
        // `MutexGuard` across a yield point. The replay does not yield anyway —
        // it signals with `use_after_ms: 0`, which the bridge applies inline.
        crate::runtime::block_on(mgr.replay_encryption_keys(&config.room_id, &config.slot_id));

        memberships
    };

    let transport = Arc::new(LiveKitMediaTransport::new(
        reqwest::Client::new(),
        Arc::new(TokenProviderAdapter(token_provider)),
        provider,
    ));
    let ctx = ConnectionContext {
        room_id: config.room_id.clone(),
        slot_id: config.slot_id.clone(),
        member: OwnMemberClaims {
            member_id: config.member_id.clone(),
            user_id: config.user_id.clone(),
            device_id: config.device_id.clone(),
        },
    };
    let engine = CallEngine::new(
        EngineConfig {
            transports: vec![transport.clone()],
            own_member_id: config.member_id.clone(),
            ctx: ctx.clone(),
            own_connection_key: Some(config.livekit_service_url.clone()),
        },
        memberships,
    );

    // Imported media keys surface as `CallEvent::KeyImported`.
    let engine_handle = engine.handle();
    bridge.set_key_import_listener(Box::new(move |key| {
        engine_handle.notify_key_imported(key.rtc_backend_identity.clone(), key.key_index);
    }));

    // Own focus connects synchronously so a broken SFU fails this call
    // instead of surfacing later as a dead session.
    let (connection, connection_events) = transport
        .connect_livekit(&config.livekit_service_url, &ctx)
        .await
        .map_err(|error| {
            log::warn!(
                "media: own focus {} refused the connection: {error}",
                config.livekit_service_url,
            );
            MediaFfiError::Transport(error.to_string())
        })?;
    engine.adopt_own_connection(Box::new(connection.clone()), connection_events);

    let events = engine.subscribe_events();
    let own_identity = pseudonymous_identity(&config.user_id, &config.device_id, &config.member_id);

    log::info!("media: connected, local identity {own_identity}");

    Ok(Arc::new(MediaSession {
        engine,
        connection,
        _bridge: bridge,
        events: TokioMutex::new(events),
        own_identity,
    }))
}

/// A live media session on a joined slot: the participant roster, the
/// unified event stream, per-stream constraints, frame streams, and local
/// publications — with no transport types on the surface.
///
/// End it with [`MediaSession::disconnect`]; leaving the slot itself stays a
/// manager concern (`RtcSessionManagerHandle::leave`).
#[derive(uniffi::Object)]
pub struct MediaSession {
    engine: CallEngine,
    connection: LiveKitTransportConnection,
    /// Keeps the key bridge alive alongside the session for clarity; the
    /// core's encryption manager also holds it.
    _bridge: Arc<MediaKeyBridge>,
    events: TokioMutex<broadcast::Receiver<CallEvent>>,
    own_identity: String,
}

#[uniffi::export(async_runtime = "tokio")]
impl MediaSession {
    /// The next event on the unified call stream. Suspends until one
    /// arrives; `None` means the session is over. Bridge to a Kotlin `Flow`
    /// or Swift `AsyncStream` by looping.
    ///
    /// A consumer that falls very far behind may miss events (the internal
    /// buffer holds 256); resynchronise from [`Self::participants`].
    pub async fn next_event(&self) -> Option<FfiCallEvent> {
        let mut events = self.events.lock().await;
        loop {
            match events.recv().await {
                Ok(event) => return Some(event.into()),
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    log::warn!("call event consumer lagged; {missed} events dropped");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// The current participant roster (including ourselves).
    pub fn participants(&self) -> Vec<FfiParticipant> {
        self.engine
            .participants()
            .into_iter()
            .map(Into::into)
            .collect()
    }

    /// Our MSC4195 pseudonymous identity on the media plane (the JWT `sub`;
    /// peers import our media key under it).
    pub fn local_identity(&self) -> String {
        self.own_identity.clone()
    }

    /// Set the subscription constraints for one stream of one participant.
    /// Debounced and re-applied automatically when the stream (re)appears.
    pub fn set_constraints(
        &self,
        member_id: String,
        kind: FfiStreamKind,
        constraints: FfiMediaConstraints,
    ) {
        self.engine
            .set_constraints(member_id, kind.into(), constraints.into());
    }

    /// The audio frame stream of a participant's stream, once
    /// [`FfiCallEvent::StreamStarted`] announced it. Each call opens an
    /// independent stream.
    pub fn audio_stream(
        &self,
        member_id: String,
        kind: FfiStreamKind,
    ) -> Option<Arc<AudioFrameStream>> {
        let track = self.engine.remote_track(&member_id, kind.into())?;
        Some(Arc::new(AudioFrameStream::new(track.audio_frames()?)))
    }

    /// The video frame stream of a participant's stream (latest-frame-wins).
    pub fn video_stream(
        &self,
        member_id: String,
        kind: FfiStreamKind,
    ) -> Option<Arc<VideoFrameStream>> {
        let track = self.engine.remote_track(&member_id, kind.into())?;
        Some(Arc::new(VideoFrameStream::new(track.video_frames()?)))
    }

    /// Cumulative receive-side RTP counters for a participant's stream.
    ///
    /// `null` while that stream is not subscribed, or before the first RTCP
    /// report has arrived. This is the only way to tell "no RTP arriving" from
    /// "RTP arriving that does not decode": the receive path fabricates frames
    /// at a fixed cadence either way. See [`FfiReceiveStats`] for how to read
    /// the counters — they are totals, so sample twice and compare.
    pub async fn receive_stats(
        &self,
        member_id: String,
        kind: FfiStreamKind,
    ) -> Option<FfiReceiveStats> {
        self.engine
            .receive_stats(&member_id, kind.into())
            .await
            .map(Into::into)
    }

    /// Publish a local track on our focus; push captured frames into the
    /// returned handle.
    pub async fn publish(
        &self,
        options: FfiPublishOptions,
    ) -> Result<Arc<FfiLocalTrack>, MediaFfiError> {
        log::info!("media: publishing {options:?}");

        let handle = self.engine.publish(options.into()).await.map_err(|error| {
            log::warn!("media: publish failed: {error}");
            MediaFfiError::Transport(error.to_string())
        })?;
        Ok(Arc::new(FfiLocalTrack::new(handle)))
    }

    /// End the media session: emits `Ended { Left }`, closes every
    /// peer-focus connection, then the own-focus one. Leave the slot via the
    /// manager separately.
    ///
    /// (Named `disconnect` rather than `close`: uniffi already gives every
    /// Kotlin object an `AutoCloseable.close()` for handle disposal, and a
    /// suspend `close()` collides with it.)
    pub async fn disconnect(&self) -> Result<(), MediaFfiError> {
        log::info!("media: disconnecting");
        self.engine.shutdown().await;
        self.connection.close().await.map_err(|error| {
            log::warn!("media: own focus did not close cleanly: {error}");
            MediaFfiError::Transport(error.to_string())
        })
    }
}
