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
//! MSC4195 token exchange, and an E2EE-enabled SFU connection — and returns a
//! single handle exposing the LiveKit event stream. [`Call::leave`] tears all
//! of it down in the right order.
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
use matrix_sdk::{Client, Room};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

use matrix_rtc_core::{
    EncryptionConfig, JoinSessionParams, KeyOrigin, LiveKitTransport, ReceivedEncryptionKey,
    RtcIdentityMapper, RtcSessionManager, RtcTransport, SlotEncryption, generate_member_id,
};

use crate::identity::pseudonymous_identity;
use crate::matrix_bridge::{SdkCommandSender, run_sticky_bridge};
use crate::session::LiveKitSession;
use crate::{
    LiveKitConnection, LiveKitTransportConfig, MediaKeyBridge, MemberClaims, connect_e2ee,
    msc4195_key_provider,
};

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
    connection: LiveKitConnection,
    bridge: Arc<MediaKeyBridge>,
    own_identity: String,
    membership_id: String,
    room_id: String,
    slot_id: String,
    heartbeat: AbortOnDrop,
    key_pump: AbortOnDrop,
    _sticky_bridge: AbortOnDrop,
    _key_handler: EventHandlerDropGuard,
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
        let manager: Manager = Arc::new(Mutex::new(RtcSessionManager::with_command_sender(
            Arc::new(SdkCommandSender::new(client.clone())),
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
        let handler = register_key_receiver(&client, key_tx);
        let key_handler = client.event_handler_drop_guard(handler);

        // MSC4143 requires a fresh `member.id` on every join, so this must not
        // be derived from the (stable) user and device IDs.
        let membership_id = generate_member_id();
        let own_identity = pseudonymous_identity(&user_id, &device_id, &membership_id);

        let livekit =
            discover_livekit_transport(&client, options.livekit_service_url_fallback.as_deref())
                .await?;

        // Join the RTC session, then — still holding the manager lock so no
        // sticky update can interleave — wire the encryption manager to our
        // bridge and to the MSC4195 pseudonymous-identity derivation.
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
        {
            let mut mgr = manager.lock().await;
            mgr.join(params).await.map_err(signalling_error)?;
            let identity_mapper: RtcIdentityMapper =
                Arc::new(|user_id: &str, device_id: &str, member_id: &str| {
                    pseudonymous_identity(user_id, device_id, member_id)
                });
            if !mgr.set_encryption_signal_handler(&room_id, &options.slot_id, bridge.clone()) {
                return Err(CallError::Signalling(
                    "failed to register encryption signal handler".into(),
                ));
            }
            mgr.set_encryption_identity_mapper(&room_id, &options.slot_id, identity_mapper);
        }

        let key_pump = AbortOnDrop(spawn_key_pump(manager.clone(), key_rx));
        let heartbeat = AbortOnDrop(spawn_heartbeat(
            manager.clone(),
            room_id.clone(),
            options.slot_id.clone(),
            options.heartbeat_interval,
        ));

        // Token exchange + SFU connect (the client is the OpenID token source).
        let http = match options.http {
            Some(http) => http,
            None => reqwest::Client::new(),
        };
        let lk_config = LiveKitTransportConfig {
            livekit_service_url: livekit.livekit_service_url.clone(),
            room_id: room_id.clone(),
            slot_id: options.slot_id.clone(),
            member: MemberClaims {
                id: membership_id.clone(),
                claimed_user_id: user_id,
                claimed_device_id: device_id,
            },
        };
        let connection = match connect_e2ee(&http, &lk_config, &client, provider).await {
            Ok(connection) => connection,
            Err(error) => {
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

        Ok(Call {
            manager,
            connection,
            bridge,
            own_identity,
            membership_id,
            room_id,
            slot_id: options.slot_id,
            heartbeat,
            key_pump,
            _sticky_bridge: sticky_bridge,
            _key_handler: key_handler,
        })
    }

    /// The LiveKit room event stream (participants joining, tracks
    /// subscribed, disconnects, ...).
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
        &mut self.connection.events
    }

    /// The connected SFU session (access the LiveKit room to publish, ...).
    pub fn session(&self) -> &LiveKitSession {
        &self.connection.session
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
            connection,
            heartbeat,
            key_pump,
            room_id,
            slot_id,
            ..
        } = self;
        drop(heartbeat);
        drop(key_pump);

        let leave_result = manager
            .lock()
            .await
            .leave(room_id, slot_id, Default::default())
            .await
            .map_err(signalling_error);
        let close_result = connection.session.close().await.map_err(CallError::from);
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
