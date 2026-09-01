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

use matrix_rtc_bridge::compat::ElementCallCompat;
use matrix_rtc_livekit::{
    LiveKitMediaTransport, LiveKitTransportConnection, MediaKeyBridge, TokenEndpoint,
    identity_mapper, msc4195_key_provider, msc4195_media_key_bridge,
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
/// events/room state, and `join` succeeded for this room/slot. The `member.id`
/// comes from that join — the host neither chooses nor passes it.
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
        "media: connecting [{}/{}] user={} device={} focus={}",
        config.room_id,
        config.slot_id,
        config.user_id,
        config.device_id,
        config.livekit_service_url,
    );

    // Which MatrixRTC generation this room was joined for, read back from the
    // join rather than taken as a parameter: it decides the participant identity
    // and the token endpoint, and those disagreeing with the membership we
    // already published is not an error but a silence — peers sit in the roster
    // with no media, keys install under an identity the SFU never assigned, and
    // nothing logs a problem. See `crate::compat`.
    let compat = manager.element_call_compat_for(&config.room_id);
    if compat != ElementCallCompat::Off {
        log::info!(
            "media: [{}/{}] connecting in Element Call compatibility mode {compat:?}",
            config.room_id,
            config.slot_id,
        );
    }
    // Call it once and share the `Arc`: it has four uses here — the core's
    // encryption manager, the media transport, our own identity, and the key
    // ring — and they must not skew.
    let identity_mapper = identity_mapper(compat);

    // Frame encryption: one shared KeyProvider feeds every SFU connection
    // (keys are indexed by the participant identity, globally unique per
    // membership) and the bridge that imports keys the core signals.
    let provider = msc4195_key_provider();
    let bridge = Arc::new(msc4195_media_key_bridge(provider.clone()));

    // Wire the core's encryption manager to the bridge and to the MSC4195
    // identity derivation, and take the membership snapshot channel the engine
    // consumes.
    let (memberships, member_id) = {
        let mut mgr = manager.inner.lock().await;
        // Read the `member.id` from the join rather than taking one from the
        // host: it is what our MSC4195 participant identity is derived from, so
        // a value that disagrees with the published membership would put our
        // media on an identity no peer holds a key for.
        let member_id = mgr
            .own_member_id(&config.room_id, &config.slot_id)
            .ok_or_else(|| {
                log::warn!(
                    "media: [{}/{}] has not joined — join the slot before connecting media",
                    config.room_id,
                    config.slot_id,
                );
                MediaFfiError::NotJoined(format!(
                    "{}/{} has not joined — join the slot first",
                    config.room_id, config.slot_id
                ))
            })?;
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
        // Mapper before handler: the replay below derives identities through it,
        // and installing it second would replay peer keys under the raw
        // `member_id` fallback — an identity the SFU never uses, which is
        // indistinguishable from importing nothing.
        mgr.set_encryption_identity_mapper(
            &config.room_id,
            &config.slot_id,
            identity_mapper.clone(),
        );

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

        (memberships, member_id)
    };

    let transport = Arc::new(
        LiveKitMediaTransport::new(
            reqwest::Client::new(),
            Arc::new(TokenProviderAdapter(token_provider)),
            provider,
        )
        // The same mapper the core got, so our own identity, the peers' and the
        // key ring's all agree.
        .with_identity_mapper(identity_mapper.clone())
        .with_token_endpoint(match compat {
            // Pre-MSC4195 `/sfu/get`, which is also where that generation's
            // unhashed `{user}:{device}` identity comes from — the endpoint mints
            // the identity, so the two are one decision, not two.
            ElementCallCompat::StateEvents => TokenEndpoint::LegacyElementCall,
            _ => TokenEndpoint::Msc4195,
        }),
    );
    let ctx = ConnectionContext {
        room_id: config.room_id.clone(),
        slot_id: config.slot_id.clone(),
        member: OwnMemberClaims {
            member_id: member_id.clone(),
            user_id: config.user_id.clone(),
            device_id: config.device_id.clone(),
        },
    };
    let engine = CallEngine::new(
        EngineConfig {
            transports: vec![transport.clone()],
            own_member_id: member_id.clone(),
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

    // Refused keys surface as `FfiCallEvent::KeyDiscarded`. Without this the
    // reason a key was rejected never leaves the core, and the host sees only a
    // `MissingKey` it cannot distinguish from a key that never arrived.
    let engine_handle = engine.handle();
    bridge.set_key_discard_listener(Box::new(move |discarded| {
        engine_handle.notify_key_discarded(discarded);
    }));

    // Keys signalled between `join` and now were stored but dropped — nothing
    // was listening. Without this, every participant whose key arrived before
    // media attached stays undecryptable until a rotation, which only a
    // membership change triggers.
    //
    // Deliberately *after* the import listener, even though the handler has been
    // installed since the block above: replaying earlier still fixes decryption,
    // but silently — no `KeyImported` reaches the host for the very keys the host
    // is most likely to be missing, so a working call is indistinguishable from
    // the bug this replay exists to fix. Still before `connect_livekit`, so the
    // key ring is populated before the first frame can arrive.
    //
    {
        let mgr = manager.inner.lock().await;
        mgr.replay_encryption_keys(&config.room_id, &config.slot_id)
            .await;
    }

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
    let own_identity = identity_mapper(&config.user_id, &config.device_id, &member_id);

    // Move our sender onto each key we rotate to. Importing a key only fills the
    // provider's ring; the index our frames actually carry lives on the frame
    // cryptor. Without this we advertise a rotation to peers and carry on
    // encrypting with the previous key, so anyone joining after it decrypts
    // nothing — and the forward secrecy the rotation exists for is not delivered.
    let connection_for_keys = connection.clone();
    bridge.set_local_sender(
        own_identity.clone(),
        Box::new(move |key_index| connection_for_keys.set_local_key_index(key_index)),
    );
    // Adopt the index we are already on rather than assuming 0, and record it for
    // tracks published later.
    if let Some(own_key) = bridge.key_for(&own_identity) {
        connection.set_local_key_index(own_key.key_index);
    }

    log::info!("media: connected as member {member_id}, local identity {own_identity}");

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

    /// Our participant identity on the media plane (the JWT `sub`; peers import
    /// our media key under it).
    ///
    /// The MSC4195 pseudonymous hash, or — in
    /// [`FfiElementCallCompat::StateEvents`](crate::FfiElementCallCompat::StateEvents)
    /// — the plain `{user}:{device}` string that generation's authorisation
    /// service mints.
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
    /// returned handle. Retract it with [`MediaSession::unpublish`] — closing
    /// the returned handle does not.
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

    /// Mute or unmute one of our own publications.
    ///
    /// Peers are told, so their UI can show it — muting is not the same as
    /// simply not pushing frames, which looks to a peer like a stalled sender.
    /// Our own roster entry and the event stream are updated too
    /// (`StreamMuted`/`StreamUnmuted` against our `member_id`), so a host can
    /// render its own state from the same source it renders everyone else's
    /// instead of keeping a parallel copy.
    ///
    /// Errors if nothing of that kind is currently published.
    pub async fn set_local_muted(
        &self,
        kind: FfiStreamKind,
        muted: bool,
    ) -> Result<(), MediaFfiError> {
        log::info!("media: setting our own {kind:?} muted={muted}");

        self.engine
            .set_local_muted(kind.into(), muted)
            .await
            .map_err(|error| {
                log::warn!("media: local mute failed: {error}");
                MediaFfiError::Transport(error.to_string())
            })
    }

    /// Retract one of our own publications, so peers drop the stream instead
    /// of rendering an empty tile — what a stopped screen share needs, since
    /// unlike a camera a screen has no "off" state a mute could represent.
    ///
    /// Peers see the stream removed, and our own roster entry drops it
    /// (`StreamStopped` against our `member_id`). The `FfiLocalTrack` from
    /// [`MediaSession::publish`] is dead afterwards: `captureAudio` /
    /// `captureVideo` fail with a transport error (they never crash, so a
    /// capture thread still mid-call is safe — it should stop on the first
    /// error). `ScreenShare` and `ScreenShareAudio` are separate
    /// publications; unpublish each. Re-publishing the same kind later is a
    /// fresh [`MediaSession::publish`].
    ///
    /// Errors if nothing of that kind is currently published.
    pub async fn unpublish(&self, kind: FfiStreamKind) -> Result<(), MediaFfiError> {
        log::info!("media: unpublishing our own {kind:?}");

        self.engine.unpublish(kind.into()).await.map_err(|error| {
            log::warn!("media: unpublish failed: {error}");
            MediaFfiError::Transport(error.to_string())
        })
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
