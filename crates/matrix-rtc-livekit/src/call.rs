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

//! High-level "join a call" facade over the whole stack.
//!
//! [`Call::join`] wires together everything a MatrixRTC participant needs —
//! the [`RtcSessionManager`] with an SDK-backed command sender, the sticky
//! membership bridge, MSC4143 media-key signalling in both directions, the
//! MSC4195 token exchange, and an E2EE-enabled SFU connection driven through
//! the transport-agnostic [`matrix_rtc_media`] layer. [`Call::leave`] tears
//! all of it down in the right order.
//!
//! Consume the call through the unified stream
//! ([`Call::subscribe_call_events`]) and the [`Call::participants`] roster;
//! the raw LiveKit accessors ([`Call::events`], [`Call::session`]) remain for
//! the transition and will go away once frame streams cover their uses.
//!
//! Requires the `matrix-sdk` feature.
//!
//! # Runtime requirements
//!
//! The core's command sender is `?Send`, so the futures driving the session
//! are `!Send`: **[`Call::join`] must be called from within a
//! [`tokio::task::LocalSet`]** (it uses `spawn_local` internally) and panics
//! outside one. See `examples/join_and_record.rs` for the runtime skeleton.
//!
//! # Preconditions
//!
//! - the client is logged in and syncing (e.g. `matrix_sdk_ui::sync_service::SyncService`
//!   is running — under `unstable-msc4354` it auto-enables the sticky-events
//!   extension the membership bridge relies on);
//! - the user has joined `room`;
//! - the slot is open (an `m.rtc.slot` state event; see [`open_slot`]) —
//!   MSC4143 counts nobody as joined against a closed slot.

use std::sync::Arc;
use std::time::Duration;

use livekit::RoomEvent;
use matrix_sdk::deserialized_responses::{EncryptionInfo, VerificationLevel, VerificationState};
use matrix_sdk::event_handler::EventHandlerDropGuard;
use matrix_sdk::ruma::api::client::rtc::transports::v1 as rtc_transports;
use matrix_sdk::ruma::events::AnyToDeviceEvent;
use matrix_sdk::ruma::events::rtc::transport::RtcTransport as RumaRtcTransport;
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::{Client, Room};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Mutex, broadcast, watch};
use tokio::task::JoinHandle;

use matrix_rtc_core::{
    EncryptionConfig, JoinSessionParams, KeyOrigin, LiveKitTransport, ReceivedEncryptionKey,
    RtcIdentityMapper, RtcSessionManager, RtcTransport, SlotEncryption, generate_member_id,
};
use matrix_rtc_media::{
    CallEngine, CallEvent, ConnectionContext, EngineConfig, LocalTrackHandle, MediaConstraints,
    MediaStreamKind, OwnMemberClaims, Participant, PublishOptions, ReceiveStats, RemoteTrackHandle,
};

use crate::compat::{self, ElementCallDialect};
use crate::identity::pseudonymous_identity;
use crate::matrix_bridge::{SdkCommandSender, run_sticky_bridge};
use crate::session::LiveKitSession;
use crate::transport_impl::{LiveKitMediaTransport, LiveKitTransportConnection};
use crate::{MediaKeyBridge, msc4195_key_provider};

type Manager = Arc<Mutex<RtcSessionManager<SdkCommandSender>>>;

/// Errors produced when joining, operating, or leaving a [`Call`].
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// A Matrix client error (login state, room access, ...).
    #[error(transparent)]
    Sdk(#[from] matrix_sdk::Error),

    /// A LiveKit transport error (token exchange, SFU connection).
    #[error(transparent)]
    Transport(#[from] crate::Error),

    /// A media transport error surfaced through the media layer.
    #[error(transparent)]
    Media(#[from] matrix_rtc_media::TransportError),

    /// MatrixRTC signalling through the core failed (membership, slot, keys).
    #[error("MatrixRTC signalling failed: {0}")]
    Signalling(String),
}

fn signalling_error(error: impl std::fmt::Display) -> CallError {
    CallError::Signalling(error.to_string())
}

/// Options for [`Call::join`]. `CallOptions::default()` matches the common
/// case: the `m.call#ROOM` slot of the `m.call` application, transport
/// discovery via the homeserver, and the core's default encryption policy.
#[derive(Clone, Debug)]
pub struct CallOptions {
    /// MatrixRTC slot to join.
    pub slot_id: String,
    /// MatrixRTC application of the slot.
    pub application: String,
    /// LiveKit authorisation service URL to use when the homeserver does not
    /// advertise a LiveKit transport (MSC4143 `GET /rtc/transports`). Joining
    /// fails if discovery yields nothing and no fallback is set.
    pub livekit_service_url_fallback: Option<String>,
    /// Override for the core's media-key policy. `None` keeps the core's
    /// default, which requires key senders to be cross-signed (MSC4153) —
    /// only relax this for test setups whose users have no cross-signing.
    pub encryption_config: Option<EncryptionConfig>,
    /// How often to refresh the dead man's switch delayed leave.
    pub heartbeat_interval: Duration,
    /// HTTP client used for the token exchange with the authorisation
    /// service. Supply one to control TLS behaviour (e.g. self-signed dev
    /// certs); `None` builds a default client.
    pub http: Option<reqwest::Client>,
    /// Whether to subscribe to peers' media. `false` joins publish-only: the
    /// roster still fills from membership signalling, but no remote track is
    /// ever subscribed, so [`CallEvent::StreamStarted`] and
    /// [`Call::remote_track`] never produce anything. Only a load generator
    /// wants this.
    pub auto_subscribe: bool,
    /// Also speak the pre-2026 Element Call dialect on the way out, for calls
    /// with clients that have not caught up with the 2026 MSC4143 rewrite (the
    /// JS SDK, and so Element Call).
    ///
    /// Membership events stay MSC4143-valid — the legacy fields ride alongside
    /// — but media keys go out as `io.element.call.encryption_keys` *instead of*
    /// the spec type, since a to-device message has only one type. A call with
    /// this on therefore exchanges keys with legacy peers and not with
    /// spec-current ones.
    ///
    /// Reading the legacy dialect needs no flag and is always on. See
    /// [`crate::compat`], and delete all of it once Element Call catches up.
    pub legacy_element_call: bool,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            slot_id: "m.call#ROOM".to_owned(),
            application: "m.call".to_owned(),
            livekit_service_url_fallback: None,
            encryption_config: None,
            heartbeat_interval: Duration::from_secs(15),
            http: None,
            auto_subscribe: true,
            legacy_element_call: false,
        }
    }
}

/// Aborts the wrapped task when dropped, so a [`Call`] going out of scope
/// never leaks its background loops.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A media encryption key extracted from a peer's decrypted
/// `m.rtc.encryption_key` to-device message, carried from the (`Send`) event
/// handler to the (`!Send`) key pump over an mpsc channel.
struct ReceivedKey {
    origin: KeyOrigin,
    room_id: String,
    member_id: String,
    key_index: u8,
    key_b64: String,
}

/// A joined MatrixRTC call: live membership signalling plus an E2EE SFU
/// connection.
///
/// Obtained from [`Call::join`]; end it with [`Call::leave`]. Dropping a
/// `Call` without leaving stops the background tasks and the sync-side key
/// handler, but sends no leave event — peers then see this member disappear
/// only when the dead man's switch fires.
pub struct Call {
    manager: Manager,
    engine: CallEngine,
    connection: LiveKitTransportConnection,
    raw_events: UnboundedReceiver<RoomEvent>,
    bridge: Arc<MediaKeyBridge>,
    own_identity: String,
    membership_id: String,
    room_id: String,
    slot_id: String,
    heartbeat: AbortOnDrop,
    key_pump: AbortOnDrop,
    _sticky_bridge: AbortOnDrop,
    _key_handler: EventHandlerDropGuard,
    _legacy_key_handler: EventHandlerDropGuard,
}

impl Call {
    /// Join the MatrixRTC call on `room` and connect to the SFU with
    /// per-participant frame E2EE.
    ///
    /// Publishes this device's `m.rtc.member` membership as a sticky event,
    /// arms the dead man's switch delayed leave (kept alive by an internal
    /// heartbeat), starts distributing/ingesting media keys over Olm-encrypted
    /// to-device messages, discovers the LiveKit transport, and connects.
    ///
    /// Must run inside a [`tokio::task::LocalSet`]; see the module docs for
    /// this and the other preconditions.
    pub async fn join(room: &Room, options: CallOptions) -> Result<Call, CallError> {
        let client = room.client();
        let user_id = client
            .user_id()
            .ok_or_else(|| CallError::Signalling("client has no user id (not logged in)".into()))?
            .to_string();
        let device_id = client
            .device_id()
            .ok_or_else(|| CallError::Signalling("client has no device id (not logged in)".into()))?
            .to_string();
        let room_id = room.room_id().to_string();

        // The manager plus the bridge feeding it peer memberships.
        let command_sender = if options.legacy_element_call {
            log::warn!(
                "[{room_id}/{}] joining in pre-2026 Element Call compatibility mode: media keys \
                 go out as {} and will not reach spec-current peers",
                options.slot_id,
                compat::LEGACY_KEY_EVENT_TYPE,
            );
            SdkCommandSender::with_element_call_compat(
                client.clone(),
                ElementCallDialect::new(
                    user_id.clone(),
                    device_id.clone(),
                    options.slot_id.clone(),
                ),
            )
        } else {
            SdkCommandSender::new(client.clone())
        };
        let manager: Manager = Arc::new(Mutex::new(RtcSessionManager::with_command_sender(
            Arc::new(command_sender),
        )));
        let sticky_bridge = AbortOnDrop(tokio::task::spawn_local(run_sticky_bridge(
            room.clone(),
            manager.clone(),
        )));

        // Frame encryption: a single shared KeyProvider handle feeds both the
        // LiveKit room (which encrypts our frames and decrypts peers') and the
        // MediaKeyBridge (which imports every key the core signals). MSC4195
        // per-participant HKDF mode.
        let provider = msc4195_key_provider();
        let bridge = Arc::new(MediaKeyBridge::with_provider(provider.clone()));

        // Receive path: peers distribute their media keys as Olm-encrypted
        // `m.rtc.encryption_key` to-device messages. The SDK decrypts and
        // dispatches them to a handler that must stay `Send`; forward the key
        // bytes over a channel to a `spawn_local` pump that drives the `!Send`
        // manager. The drop guard unregisters the handler with the `Call`.
        let (key_tx, key_rx) = unbounded_channel::<ReceivedKey>();
        let handler = register_key_receiver(&client, key_tx.clone());
        let key_handler = client.event_handler_drop_guard(handler);
        // Peers that predate the 2026 rewrite send their keys under a different
        // type entirely, which ruma has no typed event for. Always registered:
        // reading the legacy dialect costs a string comparison and cannot
        // affect a spec-current call.
        let legacy_key_handler =
            client.event_handler_drop_guard(register_legacy_key_receiver(&client, key_tx));

        // MSC4143 requires a fresh `member.id` on every join, so this must not
        // be derived from the (stable) user and device IDs.
        let membership_id = generate_member_id();
        let own_identity = pseudonymous_identity(&user_id, &device_id, &membership_id);

        log::info!(
            "[{room_id}/{}] join: user={user_id} device={device_id} member={membership_id} \
             identity={own_identity}",
            options.slot_id,
        );

        let livekit =
            discover_livekit_transport(&client, options.livekit_service_url_fallback.as_deref())
                .await?;
        log::info!(
            "[{room_id}/{}] join: focus is {}",
            options.slot_id,
            livekit.livekit_service_url,
        );

        // Join the RTC session, then — still holding the manager lock so no
        // sticky update can interleave — wire the encryption manager to our
        // bridge and to the MSC4195 pseudonymous-identity derivation, and
        // take the membership snapshot channel the media engine consumes.
        let mut params = JoinSessionParams::new(
            user_id.clone(),
            device_id.clone(),
            room_id.clone(),
            options.slot_id.clone(),
            options.application.clone(),
            RtcTransport::LiveKit(livekit.clone()),
        );
        params.membership_id = Some(membership_id.clone());
        params.encryption_config = options.encryption_config.clone();
        let memberships = {
            let mut mgr = manager.lock().await;
            mgr.join(params).await.map_err(signalling_error)?;
            let identity_mapper: RtcIdentityMapper =
                Arc::new(|user_id: &str, device_id: &str, member_id: &str| {
                    pseudonymous_identity(user_id, device_id, member_id)
                });
            // The mapper goes in *before* the signal handler. Identities are
            // derived at signal time, so a key signalled in between would be
            // imported under the fallback `user:device` identity — one the SFU
            // never uses, which looks exactly like the key never arriving. The
            // join itself now drives the first distribution, so that window is no
            // longer theoretical.
            mgr.set_encryption_identity_mapper(&room_id, &options.slot_id, identity_mapper);
            if !mgr.set_encryption_signal_handler(&room_id, &options.slot_id, bridge.clone()) {
                log::warn!(
                    "[{room_id}/{}] join: the joined session has no encryption manager",
                    options.slot_id,
                );
                return Err(CallError::Signalling(
                    "failed to register encryption signal handler".into(),
                ));
            }
            mgr.subscribe_membership_snapshots(&room_id, &options.slot_id)
                .ok_or_else(|| {
                    CallError::Signalling("joined session is not tracked by the manager".into())
                })?
        };

        let key_pump = AbortOnDrop(spawn_key_pump(manager.clone(), key_rx));
        let heartbeat = AbortOnDrop(spawn_heartbeat(
            manager.clone(),
            room_id.clone(),
            options.slot_id.clone(),
            options.heartbeat_interval,
        ));

        // The media layer: a LiveKit transport sharing the E2EE key provider,
        // and the engine reconciling memberships with connection events. The
        // client is the OpenID token source for the MSC4195 token exchange.
        let http = match options.http {
            Some(http) => http,
            None => reqwest::Client::new(),
        };
        let transport = Arc::new(
            LiveKitMediaTransport::new(http, Arc::new(client.clone()), provider)
                .with_auto_subscribe(options.auto_subscribe),
        );
        let ctx = ConnectionContext {
            room_id: room_id.clone(),
            slot_id: options.slot_id.clone(),
            member: OwnMemberClaims {
                member_id: membership_id.clone(),
                user_id,
                device_id,
            },
        };
        // The engine owns connections to every peer focus (MSC4195 multi-SFU);
        // only the own focus is connected here, synchronously, so a failed
        // join can be reported (and signalled away) immediately.
        let engine = CallEngine::new(
            EngineConfig {
                transports: vec![transport.clone()],
                own_member_id: membership_id.clone(),
                ctx: ctx.clone(),
                own_connection_key: Some(livekit.livekit_service_url.clone()),
            },
            memberships,
        );

        // Imported media keys surface as `CallEvent::KeyImported`, and refused
        // ones as `CallEvent::KeyDiscarded` — the only way the reason a key was
        // rejected leaves the core.
        let engine_handle = engine.handle();
        bridge.set_key_import_listener(Box::new(move |key| {
            engine_handle.notify_key_imported(key.rtc_backend_identity.clone(), key.key_index);
        }));
        let engine_handle = engine.handle();
        bridge.set_key_discard_listener(Box::new(move |discarded| {
            engine_handle.notify_key_discarded(discarded);
        }));

        // Re-signal every key held so far, now that the listener above exists.
        //
        // Two things arrive before this point: our own first key, which `join`
        // distributes and signals, and any peer key the sticky bridge has already
        // pumped in. Both were applied to the key provider, but with no listener
        // installed neither produced a `KeyImported` — so the one path a host
        // cannot otherwise observe was also the one it most needed to see. The
        // replay is idempotent at the provider and honours whatever remains of a
        // rotation's `delayBeforeUse`.
        //
        // It runs before `connect_livekit` so the key ring is populated before the
        // first frame can arrive.
        if !manager
            .lock()
            .await
            .replay_encryption_keys(&room_id, &options.slot_id)
            .await
        {
            log::warn!(
                "[{room_id}/{}] join: could not replay held keys; peers may stay undecryptable \
                 until the next rotation",
                options.slot_id,
            );
        }

        log::info!(
            "[{room_id}/{}] join: connecting own focus {}",
            options.slot_id,
            livekit.livekit_service_url,
        );
        let (connection, connection_events) = match transport
            .connect_livekit(&livekit.livekit_service_url, &ctx)
            .await
        {
            Ok(connected) => connected,
            Err(error) => {
                log::warn!(
                    "[{room_id}/{}] join: own focus {} refused the connection ({error}); \
                     leaving the slot again",
                    options.slot_id,
                    livekit.livekit_service_url,
                );
                // We are signalled as joined but have no media path; leave so
                // peers don't wait on the dead man's switch to notice.
                drop(heartbeat);
                if let Err(leave_error) = manager
                    .lock()
                    .await
                    .leave(room_id, options.slot_id, Default::default())
                    .await
                {
                    log::warn!("leave after failed SFU connect also failed: {leave_error}");
                }
                return Err(error.into());
            }
        };
        engine.adopt_own_connection(Box::new(connection.clone()), connection_events);

        // Now that a room exists, let the bridge move our sender onto each key we
        // rotate to. Importing a key only fills the provider's ring — the index
        // our frames actually carry lives on the frame cryptor, and without this
        // we advertise a rotation to peers and keep encrypting with the previous
        // key. A peer joining after a rotation then holds only the new index and
        // decrypts nothing, and the forward secrecy the rotation exists for is
        // not delivered.
        //
        // Installed after `connect_livekit`, and after the replay above, so the
        // first key is already in the ring; the hook only ever *moves* the index.
        let connection_for_keys = connection.clone();
        bridge.set_local_sender(
            own_identity.clone(),
            Box::new(move |key_index| connection_for_keys.set_local_key_index(key_index)),
        );
        // Adopt whatever index we are already on, rather than assuming 0: a
        // rotation between `join` and here would otherwise be missed, and the
        // connection remembers the value for tracks published later (nothing is
        // published yet, so this only records it).
        if let Some(own_key) = bridge.key_for(&own_identity) {
            connection.set_local_key_index(own_key.key_index);
        }

        log::info!("[{room_id}/{}] join: complete", options.slot_id);

        // Transition-period raw stream; subscribed immediately after connect,
        // so only events racing the connect itself can be missed here.
        let raw_events = connection.session().room().subscribe();

        Ok(Call {
            manager,
            engine,
            connection,
            raw_events,
            bridge,
            own_identity,
            membership_id,
            room_id,
            slot_id: options.slot_id,
            heartbeat,
            key_pump,
            _sticky_bridge: sticky_bridge,
            _key_handler: key_handler,
            _legacy_key_handler: legacy_key_handler,
        })
    }

    /// The unified call event stream: membership changes, media streams
    /// starting/stopping, key imports, connection health, call end.
    ///
    /// This is the transport-agnostic replacement for [`Call::events`]. Any
    /// number of subscribers may exist; a subscriber that falls far behind
    /// observes a `Lagged` error and should resynchronise from
    /// [`Call::participants`].
    pub fn subscribe_call_events(&self) -> broadcast::Receiver<CallEvent> {
        self.engine.subscribe_events()
    }

    /// The current participant roster (including ourselves), derived from
    /// membership signalling and enriched with live media streams.
    pub fn participants(&self) -> Vec<Participant> {
        self.engine.participants()
    }

    /// Watch the participant roster; the receiver always holds the latest
    /// snapshot.
    pub fn subscribe_participants(&self) -> watch::Receiver<Vec<Participant>> {
        self.engine.subscribe_participants()
    }

    /// The frame-stream handle for a participant's subscribed stream, once
    /// [`CallEvent::StreamStarted`] announced it.
    pub fn remote_track(
        &self,
        member_id: &str,
        kind: MediaStreamKind,
    ) -> Option<Arc<dyn RemoteTrackHandle>> {
        self.engine.remote_track(member_id, kind)
    }

    /// Cumulative receive-side RTP counters for a participant's stream, or
    /// `None` while it is not subscribed / before the first RTCP report.
    ///
    /// The only way to distinguish "no RTP arriving" from "RTP arriving that
    /// does not decode": the receive path produces frames at a fixed cadence
    /// either way. See [`ReceiveStats`].
    pub async fn receive_stats(
        &self,
        member_id: &str,
        kind: MediaStreamKind,
    ) -> Option<ReceiveStats> {
        self.engine.receive_stats(member_id, kind).await
    }

    /// Publish a local track (microphone, camera, screenshare) on our focus;
    /// push captured frames into the returned handle.
    pub async fn publish(
        &self,
        options: PublishOptions,
    ) -> Result<Arc<dyn LocalTrackHandle>, CallError> {
        Ok(self.engine.publish(options).await?)
    }

    /// Set subscription constraints (visibility, rendered size, quality cap,
    /// low-bandwidth mode) for one stream of one participant. Applied after a
    /// short debounce and re-applied whenever the stream (re)appears.
    pub fn set_constraints(
        &self,
        member_id: &str,
        kind: MediaStreamKind,
        constraints: MediaConstraints,
    ) {
        self.engine.set_constraints(member_id, kind, constraints);
    }

    /// The media engine driving this call's roster and event stream.
    pub fn engine(&self) -> &CallEngine {
        &self.engine
    }

    /// The raw LiveKit room event stream (participants joining, tracks
    /// subscribed, disconnects, ...).
    ///
    /// Transition API: prefer [`Call::subscribe_call_events`]; this accessor
    /// goes away once frame-level consumers are served by [`Call::remote_track`].
    ///
    /// The stream ending (`recv()` returning `None`) means the call is over:
    /// the room closes its event channel on [`Call::leave`] and after any
    /// unrecoverable disconnect (server eviction, reconnects exhausted, ...) —
    /// in the latter case a [`RoomEvent::Disconnected`] carrying the reason is
    /// delivered first, so match it only if the reason matters. Transient
    /// network drops are resumed internally (`Reconnecting`/`Reconnected`
    /// events) and do not end the stream. There is no built-in deadline:
    /// waiting for an event that may never come (e.g. a track from a peer who
    /// never publishes) should be wrapped in a timeout by the caller.
    pub fn events(&mut self) -> &mut UnboundedReceiver<RoomEvent> {
        &mut self.raw_events
    }

    /// The connected SFU session (access the LiveKit room to publish, ...).
    ///
    /// Transition API: media access moves behind [`Call::remote_track`] and
    /// the upcoming publish surface.
    pub fn session(&self) -> &LiveKitSession {
        self.connection.session()
    }

    /// Our MSC4195 pseudonymous LiveKit identity (the JWT `sub`). Peers see
    /// this as our participant identity and import our media key under it.
    pub fn local_identity(&self) -> &str {
        &self.own_identity
    }

    /// The `m.rtc.member` membership id of this join.
    pub fn membership_id(&self) -> &str {
        &self.membership_id
    }

    /// Number of members (including ourselves) currently joined to the slot,
    /// as signalled over sticky membership events.
    pub async fn member_count(&self) -> usize {
        self.manager
            .lock()
            .await
            .member_count(&self.room_id, &self.slot_id)
            .unwrap_or(0)
    }

    /// Whether a media key for the given MSC4195 participant identity has been
    /// received and imported into this call's frame decryptor. See
    /// [`Call::local_identity`] for the identity peers know us by.
    pub fn imported_key_for(&self, identity: &str) -> bool {
        self.bridge.key_for(identity).is_some()
    }

    /// Leave the call cleanly: send the leave event (cancelling the delayed
    /// leave) and close the SFU connection.
    ///
    /// The heartbeat stops first so it cannot re-arm a delayed leave after
    /// `leave` cancels the current one. The SFU connection is closed even if
    /// the Matrix-side leave fails; the first error wins.
    pub async fn leave(self) -> Result<(), CallError> {
        let Call {
            manager,
            engine,
            connection,
            heartbeat,
            key_pump,
            room_id,
            slot_id,
            ..
        } = self;
        drop(heartbeat);
        drop(key_pump);

        // Step logs bracket every await so a wedged teardown pinpoints itself.
        log::debug!("[{room_id}] leave: sending matrix leave (membership + delayed-event cancel)");
        let leave_result = manager
            .lock()
            .await
            .leave(room_id.clone(), slot_id, Default::default())
            .await
            .map_err(signalling_error);
        log::debug!(
            "[{room_id}] leave: matrix leave {}; shutting down the media engine",
            if leave_result.is_ok() {
                "sent"
            } else {
                "FAILED"
            },
        );
        // Emits `CallEvent::Ended { reason: Left }` and closes every
        // peer-focus connection; the own-focus close below reports its result.
        engine.shutdown().await;
        log::debug!("[{room_id}] leave: media engine down; closing own SFU connection");
        use matrix_rtc_media::TransportConnection as _;
        let close_result = connection.close().await.map_err(CallError::from);
        log::debug!("[{room_id}] leave: complete");
        leave_result.and(close_result)
    }
}

/// Open a MatrixRTC slot in a room by publishing its `m.rtc.slot` state event.
///
/// Requires the power level for `m.rtc.slot` state (by default the room
/// creator). Passing `None` for `encryption` opens an unencrypted slot; calls
/// in encrypted rooms should use `m.per_member` slot encryption.
pub async fn open_slot(
    client: &Client,
    room_id: &str,
    slot_id: &str,
    application: &str,
    encryption: Option<SlotEncryption>,
) -> Result<(), CallError> {
    RtcSessionManager::with_command_sender(Arc::new(SdkCommandSender::new(client.clone())))
        .open_slot(
            room_id.to_owned(),
            slot_id.to_owned(),
            application.to_owned(),
            encryption,
        )
        .await
        .map_err(signalling_error)
}

/// Ask the homeserver which RTC transports it offers, and take the first
/// LiveKit one (MSC4143 returns them in descending order of preference).
///
/// Falls back to `fallback_url` when the homeserver does not implement the
/// endpoint or advertises no LiveKit transport; errors if there is no
/// fallback either.
pub async fn discover_livekit_transport(
    client: &Client,
    fallback_url: Option<&str>,
) -> Result<LiveKitTransport, CallError> {
    match client.send(rtc_transports::Request::new()).await {
        Ok(response) => {
            for transport in response.rtc_transports {
                if let RumaRtcTransport::LiveKit(livekit) = transport {
                    log::info!(
                        "homeserver offers a livekit transport at {}",
                        livekit.service_url
                    );
                    return Ok(LiveKitTransport {
                        livekit_service_url: livekit.service_url,
                    });
                }
            }
            log::info!("homeserver advertises no livekit transport; using the fallback URL");
        }
        Err(error) => {
            log::info!("transports endpoint unavailable ({error}); using the fallback URL");
        }
    }

    fallback_url
        .map(|url| LiveKitTransport {
            livekit_service_url: url.to_owned(),
        })
        .ok_or_else(|| {
            CallError::Signalling(
                "the homeserver advertises no livekit transport and no fallback URL is configured"
                    .into(),
            )
        })
}

/// Register a to-device handler that forwards decrypted
/// `m.rtc.encryption_key` events to the key pump.
///
/// The handler is `Send` (it only moves owned key data into a channel), which
/// `add_event_handler` requires; the `!Send` work happens in the pump.
fn register_key_receiver(
    client: &Client,
    key_tx: UnboundedSender<ReceivedKey>,
) -> matrix_sdk::event_handler::EventHandlerHandle {
    client.add_event_handler(
        move |event: AnyToDeviceEvent, encryption_info: Option<EncryptionInfo>| {
            let key_tx = key_tx.clone();
            async move {
                if let AnyToDeviceEvent::RtcEncryptionKey(event) = event {
                    let _ = key_tx.send(ReceivedKey {
                        origin: key_origin(encryption_info.as_ref()),
                        room_id: event.content.room_id.to_string(),
                        member_id: event.content.member_id,
                        key_index: event.content.media_key.index,
                        key_b64: event.content.media_key.key,
                    });
                }
            }
        },
    )
}

/// Register a to-device handler for media keys from peers that predate the 2026
/// MSC4143 rewrite (`io.element.call.encryption_keys`).
///
/// Takes the event raw rather than typed, for two reasons: ruma has no typed
/// event for the legacy type at all, and a typed handler silently never fires
/// when the content does not match ruma's model — a failure mode this crate has
/// already been bitten by once. The type is filtered here instead, so a
/// `Raw<AnyToDeviceEvent>` handler (which matches every to-device event) only
/// ever acts on the one type it is for.
///
/// Feeds the same channel as [`register_key_receiver`]; the core neither knows
/// nor cares which dialect a key arrived in. See [`crate::compat`].
fn register_legacy_key_receiver(
    client: &Client,
    key_tx: UnboundedSender<ReceivedKey>,
) -> matrix_sdk::event_handler::EventHandlerHandle {
    client.add_event_handler(
        move |event: Raw<AnyToDeviceEvent>, encryption_info: Option<EncryptionInfo>| {
            let key_tx = key_tx.clone();
            async move {
                if event.get_field::<String>("type").ok().flatten().as_deref()
                    != Some(compat::LEGACY_KEY_EVENT_TYPE)
                {
                    return;
                }

                let content = match event.get_field::<serde_json::Value>("content") {
                    Ok(Some(content)) => content,
                    _ => {
                        log::warn!(
                            "ignoring a {} to-device message with no content object",
                            compat::LEGACY_KEY_EVENT_TYPE,
                        );
                        return;
                    }
                };

                let Some(key) = compat::element_call::parse_key_message(&content) else {
                    log::warn!(
                        "ignoring a {} to-device message missing a required field; that peer's \
                         media will not decrypt",
                        compat::LEGACY_KEY_EVENT_TYPE,
                    );
                    return;
                };

                let _ = key_tx.send(ReceivedKey {
                    origin: key_origin(encryption_info.as_ref()),
                    room_id: key.room_id,
                    member_id: key.member_id,
                    key_index: key.key_index,
                    key_b64: key.key_b64,
                });
            }
        },
    )
}

/// Drain received peer keys into the (`!Send`) manager. Runs until the
/// channel closes or the task is aborted.
fn spawn_key_pump(manager: Manager, mut key_rx: UnboundedReceiver<ReceivedKey>) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(received) = key_rx.recv().await {
            if let Err(error) = manager
                .lock()
                .await
                .receive_encryption_key(ReceivedEncryptionKey {
                    origin: received.origin,
                    room_id: received.room_id,
                    member_id: received.member_id,
                    key_b64: received.key_b64,
                    key_index: received.key_index,
                })
                .await
            {
                log::warn!("failed to ingest received media key: {error}");
            }
        }
    })
}

/// Keep pushing the dead man's switch delayed leave back while joined.
fn spawn_heartbeat(
    manager: Manager,
    room_id: String,
    slot_id: String,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            manager.lock().await.heartbeat(&room_id, &slot_id).await;
        }
    })
}

/// Translate the SDK's decryption metadata into the core's [`KeyOrigin`].
///
/// `None` means the to-device message arrived unencrypted, which MSC4143 says
/// to discard — the core makes that call, this just reports it faithfully.
fn key_origin(info: Option<&EncryptionInfo>) -> KeyOrigin {
    let Some(info) = info else {
        return KeyOrigin::Cleartext;
    };

    // MSC4153 asks whether the sending device is cross-signed, not whether we
    // trust its owner: an unverified *identity* still signs its own devices.
    // States that leave the device unattributable count as not cross-signed.
    let sender_is_cross_signed = !matches!(
        info.verification_state,
        VerificationState::Unverified(
            VerificationLevel::UnsignedDevice
                | VerificationLevel::None(_)
                | VerificationLevel::MismatchedSender
        )
    );

    KeyOrigin::Encrypted {
        sender_user_id: info.sender.to_string(),
        sender_device_id: info.sender_device.as_ref().map(|d| d.to_string()),
        sender_is_cross_signed,
    }
}
