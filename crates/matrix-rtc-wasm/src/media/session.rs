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

//! The web media session: the participant roster and connection lifecycle over
//! livekit-js, layered on a slot the page has already joined through
//! [`WasmRtcSessionManager`].
//!
//! A port of the FFI's `connect_media_session` seam
//! (`matrix-rtc-ffi/src/media/session.rs`) — same wiring, same order — minus
//! everything media: frames, publishing, and constraints stay in livekit-js.
//! The shared `CallEngine` still owns roster reconciliation and the
//! multi-focus connection pool; its actor runs on the JS microtask queue.

use std::sync::Arc;

use js_sys::{Function, Reflect};
use matrix_rtc_bridge::compat::ElementCallCompat;
use matrix_rtc_livekit_proto::{TokenEndpoint, identity_mapper};
use matrix_rtc_media::keys::MediaKeyHandler;
use matrix_rtc_media::{
    CallEngine, CallEvent, ConnectionContext, EndedReason, EngineConfig, FrameEncryptionDiagnostic,
    FrameEncryptionState, OwnMemberClaims, Participant, TransportConnection as _,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};
use wasm_bindgen::prelude::*;

use super::transport::{JsFrameKeyRing, JsMediaTransport, JsTransportConnection, stream_kind_str};
use crate::WasmRtcSessionManager;

/// livekit-js's `ExternalE2EEKeyProvider` default `keyringSize`.
const LIVEKIT_JS_DEFAULT_KEY_RING_SIZE: u16 = 16;

/// Identifies the joined slot to attach media to, and how to reach the SFU.
#[derive(Debug, Deserialize)]
struct WasmMediaSessionConfig {
    room_id: String,
    slot_id: String,
    user_id: String,
    device_id: String,
    /// The MSC4195 authorisation-service URL of the focus we publish on —
    /// the same URL announced in our membership's transport. (Peers' foci
    /// are discovered from their memberships automatically.)
    livekit_service_url: String,
    /// livekit-js key-provider ring size, when configured away from its
    /// default of 16 (`keyringSize`). Keys at or past it are rejected.
    #[serde(default)]
    key_ring_size: Option<u16>,
    /// Element Call compatibility generation this room was joined for:
    /// `"off"` (default), `"sticky_events"`, or `"state_events"`. Decides the
    /// participant-identity derivation and the token endpoint, so it must
    /// match the membership the page published.
    #[serde(default)]
    element_call_compat: Option<String>,
}

/// Calls the delegate's `setLocalKeyIndex(index)`; the local-sender hook.
fn set_local_key_index(delegate: &JsValue, index: u8) {
    let method = Reflect::get(delegate, &JsValue::from_str("setLocalKeyIndex"))
        .ok()
        .filter(|method| !method.is_undefined());
    let Some(method) = method.and_then(|method| method.dyn_into::<Function>().ok()) else {
        log::warn!(
            "media: delegate has no setLocalKeyIndex — our key rotations will not change \
             what we encrypt with"
        );
        return;
    };
    if let Err(error) = method.call1(delegate, &JsValue::from(index)) {
        log::warn!("media: setLocalKeyIndex({index}) threw: {error:?}");
    }
}

#[wasm_bindgen]
impl WasmRtcSessionManager {
    /// Attach media to a joined slot: wire frame-key signalling into the core,
    /// start the engine (which connects to every peer's focus), and connect
    /// the own-focus livekit-js room.
    ///
    /// Preconditions: the manager has a command sender, the page feeds it
    /// sticky events/room state, and `join` succeeded for this room/slot. The
    /// `member.id` comes from that join — the page neither chooses nor passes
    /// it.
    ///
    /// `config` is `{ room_id, slot_id, user_id, device_id,
    /// livekit_service_url, key_ring_size?, element_call_compat? }`;
    /// `delegate` is the object driving livekit-js (see the module docs of
    /// the transport for its required methods). The delegate may additionally
    /// implement `onParticipants(roster)`, `onEvent(event)`, and
    /// `onSwitchComplete()` — the push half of the session, invoked from
    /// spawned pumps for the life of the call.
    #[wasm_bindgen(js_name = connectMedia)]
    pub async fn connect_media(
        &mut self,
        config: JsValue,
        delegate: JsValue,
    ) -> Result<WasmMediaSession, JsError> {
        let config: WasmMediaSessionConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|err| JsError::new(&format!("invalid media session config: {err}")))?;

        log::info!(
            "media: connecting [{}/{}] user={} device={} focus={}",
            config.room_id,
            config.slot_id,
            config.user_id,
            config.device_id,
            config.livekit_service_url,
        );

        // Which MatrixRTC generation this room was joined for, read back from
        // the join rather than trusted from this config: it decides the
        // participant identity and the token endpoint, and those disagreeing
        // with the membership we already published is not an error but a
        // silence — peers sit in the roster with no media, keys install under
        // an identity the SFU never assigned, and nothing logs a problem. The
        // config field is accepted only as a cross-check.
        let compat = self.element_call_compat_for(&config.room_id);
        if let Some(requested) = config.element_call_compat.as_deref() {
            let requested = crate::compat::parse_compat(Some(requested))?;
            if requested != compat {
                return Err(JsError::new(&format!(
                    "element_call_compat {requested:?} disagrees with the mode this room was \
                     joined in ({compat:?}); set the mode on join and drop it here",
                )));
            }
        }
        if compat != ElementCallCompat::Off {
            log::info!(
                "media: [{}/{}] connecting in Element Call compatibility mode {compat:?}",
                config.room_id,
                config.slot_id,
            );
        }
        // Call it once and share the `Arc`: it has four uses here — the core's
        // encryption manager, the media transport, our own identity, and the
        // key ring — and they must not skew.
        let mapper = identity_mapper(compat);

        // Frame encryption: livekit-js owns the key provider; the shared
        // handler forwards every signalled key into it through the delegate.
        let ring = JsFrameKeyRing::new(
            delegate.clone(),
            config
                .key_ring_size
                .unwrap_or(LIVEKIT_JS_DEFAULT_KEY_RING_SIZE),
        );
        // The ring and handler hold `JsValue`s, so they are `!Send` — shared
        // ownership on one thread. The `Arc`s are what the core's and the
        // engine's APIs take.
        #[expect(clippy::arc_with_non_send_sync)]
        let handler = Arc::new(MediaKeyHandler::with_ring(Arc::new(ring)));

        // Read the `member.id` from the join rather than taking one from the
        // page: it is what our MSC4195 participant identity is derived from,
        // so a value that disagrees with the published membership would put
        // our media on an identity no peer holds a key for.
        let member_id = self
            .inner
            .own_member_id(&config.room_id, &config.slot_id)
            .ok_or_else(|| {
                log::warn!(
                    "media: [{}/{}] has not joined — join the slot before connecting media",
                    config.room_id,
                    config.slot_id,
                );
                JsError::new("the slot has not joined — join it before connecting media")
            })?;
        let memberships = self
            .inner
            .subscribe_membership_snapshots(&config.room_id, &config.slot_id)
            .ok_or_else(|| {
                JsError::new("no session for the slot — join it before connecting media")
            })?;

        // Mapper before handler: the replay below derives identities through
        // it, and installing it second would replay peer keys under the raw
        // `member_id` fallback — an identity the SFU never uses, which is
        // indistinguishable from importing nothing.
        self.inner
            .set_encryption_identity_mapper(&config.room_id, &config.slot_id, mapper.clone());
        if !self.inner.set_encryption_signal_handler(
            &config.room_id,
            &config.slot_id,
            handler.clone(),
        ) {
            return Err(JsError::new(
                "the session has no encryption manager — join the slot first",
            ));
        }

        #[expect(clippy::arc_with_non_send_sync)]
        let transport = Arc::new(JsMediaTransport::new(
            delegate.clone(),
            mapper.clone(),
            match compat {
                // Pre-MSC4195 `/sfu/get`, which is also where that
                // generation's unhashed `{user}:{device}` identity comes from
                // — the endpoint mints the identity, so the two are one
                // decision, not two.
                ElementCallCompat::StateEvents => TokenEndpoint::LegacyElementCall,
                _ => TokenEndpoint::Msc4195,
            },
        ));
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

        // Imported media keys surface as `key_imported` events.
        let engine_handle = engine.handle();
        handler.set_key_import_listener(Box::new(move |key| {
            engine_handle.notify_key_imported(key.rtc_backend_identity.clone(), key.key_index);
        }));

        // Refused keys surface as `key_discarded`. Without this the reason a
        // key was rejected never leaves the core, and the page sees only a
        // `missing_key` it cannot distinguish from a key that never arrived.
        let engine_handle = engine.handle();
        handler.set_key_discard_listener(Box::new(move |discarded| {
            engine_handle.notify_key_discarded(discarded);
        }));

        // A key rotation coalesced into a `delayBeforeUse` window falls due
        // the instant the window closes, and the handler's timer is the only
        // thing that knows when that is. The core cannot be flushed from the
        // handler's task, so the moment is handed to JS (the
        // `onSwitchComplete` pump below), which calls `flushDueKeyRotation`.
        let (switch_tx, switch_rx) = watch::channel(0u64);
        handler.set_switch_complete_listener(Box::new(move || {
            switch_tx.send_modify(|count| *count += 1);
        }));

        // Keys signalled between `join` and now were stored but dropped —
        // nothing was listening. Without this, every participant whose key
        // arrived before media attached stays undecryptable until a rotation.
        // After the listeners (so `key_imported` reaches the page for exactly
        // the keys it is most likely to be missing), before the connect (so
        // the ring is populated before the first frame can arrive).
        self.inner
            .replay_encryption_keys(&config.room_id, &config.slot_id)
            .await;

        // Own focus connects synchronously so a broken SFU fails this call
        // instead of surfacing later as a dead session.
        let (connection, connection_events) = transport
            .connect_js(&config.livekit_service_url, &ctx)
            .await
            .map_err(|error| {
                log::warn!(
                    "media: own focus {} refused the connection: {error}",
                    config.livekit_service_url,
                );
                JsError::new(&error.to_string())
            })?;
        engine.adopt_own_connection(Box::new(connection.clone()), connection_events);

        let own_identity = mapper(&config.user_id, &config.device_id, &member_id);

        // Roster, event, and switch-complete delivery run as spawned pumps
        // owning their receivers, invoking the delegate's optional callbacks.
        // NOT as async session methods: a wasm-bindgen `&mut self` future
        // holds the object borrowed across its awaits, so a parked long-poll
        // would make every other session call throw ("recursive use of an
        // object"). The pumps borrow nothing from the session.
        if let Some(on_participants) = delegate_callback(&delegate, "onParticipants") {
            let mut participants_rx = engine.subscribe_participants();
            let mapper = mapper.clone();
            wasm_bindgen_futures::spawn_local(async move {
                while participants_rx.changed().await.is_ok() {
                    let roster: Vec<Participant> = participants_rx.borrow_and_update().clone();
                    let roster: Vec<WasmParticipant> = roster
                        .iter()
                        .map(|participant| to_wasm_participant(&mapper, participant))
                        .collect();
                    match serde_wasm_bindgen::to_value(&roster) {
                        Ok(roster) => {
                            let _ = on_participants.call1(&JsValue::NULL, &roster);
                        }
                        Err(error) => log::warn!("media: roster did not serialize: {error}"),
                    }
                }
            });
        }
        if let Some(on_event) = delegate_callback(&delegate, "onEvent") {
            let mut events = engine.subscribe_events();
            wasm_bindgen_futures::spawn_local(async move {
                loop {
                    match events.recv().await {
                        Ok(event) => {
                            let event = WasmCallEvent::from(event);
                            match serde_wasm_bindgen::to_value(&event) {
                                Ok(event) => {
                                    let _ = on_event.call1(&JsValue::NULL, &event);
                                }
                                Err(error) => {
                                    log::warn!("media: event did not serialize: {error}")
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(missed)) => {
                            log::warn!("media: event consumer lagged, {missed} event(s) dropped");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        }
        // Optional but recommended: the heartbeat also flushes due rotations,
        // so without this a coalesced rotation waits for the next beat instead
        // of happening at the instant it is owed. The callback should call
        // `manager.flushDueKeyRotation(roomId, slotId)`.
        if let Some(on_switch_complete) = delegate_callback(&delegate, "onSwitchComplete") {
            let mut switch_rx = switch_rx;
            wasm_bindgen_futures::spawn_local(async move {
                while switch_rx.changed().await.is_ok() {
                    let _ = on_switch_complete.call0(&JsValue::NULL);
                }
            });
        }

        // Move our sender onto each key we rotate to. Importing a key only
        // fills the ring; the index our frames actually carry lives on the
        // frame cryptor, which livekit-js owns — hence through the delegate.
        let delegate_for_keys = delegate.clone();
        handler.set_local_sender(
            own_identity.clone(),
            Box::new(move |key_index| set_local_key_index(&delegate_for_keys, key_index)),
        );
        // Adopt the index we are already on rather than assuming 0.
        if let Some(own_key) = handler.key_for(&own_identity) {
            set_local_key_index(&delegate, own_key.key_index);
        }

        log::info!("media: connected as member {member_id}, local identity {own_identity}");

        Ok(WasmMediaSession {
            engine,
            own_connection: connection,
            _handler: handler,
            identity_mapper: mapper,
            own_identity,
        })
    }
}

/// An optional delegate callback, by name.
fn delegate_callback(delegate: &JsValue, name: &str) -> Option<Function> {
    Reflect::get(delegate, &JsValue::from_str(name))
        .ok()
        .filter(|method| !method.is_undefined())
        .and_then(|method| method.dyn_into::<Function>().ok())
}

/// A live media session on a joined slot: the participant roster (with the
/// livekit-js identity of each entry). Media itself — tracks, publishing,
/// rendering — stays in livekit-js; join roster entries to
/// `room.getParticipantByIdentity(rtc_identity)`.
///
/// Roster changes, call events, and switch-complete moments arrive through
/// the delegate's `onParticipants` / `onEvent` / `onSwitchComplete`
/// callbacks, registered at [`WasmRtcSessionManager::connect_media`] time.
///
/// End it with [`WasmMediaSession::disconnect`]; leaving the slot itself stays
/// a manager concern ([`WasmRtcSessionManager::leave`]).
#[wasm_bindgen]
pub struct WasmMediaSession {
    engine: CallEngine,
    own_connection: JsTransportConnection,
    /// Keeps the key handler alive alongside the session for clarity; the
    /// core's encryption manager also holds it.
    _handler: Arc<MediaKeyHandler>,
    identity_mapper: matrix_rtc_core::RtcIdentityMapper,
    own_identity: String,
}

#[wasm_bindgen]
impl WasmMediaSession {
    /// The current roster, as an array of participants (see the crate docs for
    /// the JSON shape). `rtc_identity` is the livekit-js participant identity,
    /// when derivable.
    pub fn participants(&self) -> Result<JsValue, JsError> {
        let roster: Vec<WasmParticipant> = self
            .engine
            .participants()
            .iter()
            .map(|participant| to_wasm_participant(&self.identity_mapper, participant))
            .collect();
        serde_wasm_bindgen::to_value(&roster).map_err(|err| JsError::new(&err.to_string()))
    }

    /// Our own livekit-js participant identity (the MSC4195 pseudonymous
    /// identity, or the legacy `{user}:{device}` in that compatibility mode).
    #[wasm_bindgen(js_name = ownRtcIdentity)]
    pub fn own_rtc_identity(&self) -> String {
        self.own_identity.clone()
    }

    /// Shut the media session down: stop the engine (closing peer-focus
    /// connections) and close the own-focus room through the delegate.
    /// Leaving the slot is separate ([`WasmRtcSessionManager::leave`]).
    pub async fn disconnect(&mut self) -> Result<(), JsError> {
        self.engine.shutdown().await;
        self.own_connection
            .close()
            .await
            .map_err(|error| JsError::new(&error.to_string()))
    }
}

fn to_wasm_participant(
    mapper: &matrix_rtc_core::RtcIdentityMapper,
    participant: &Participant,
) -> WasmParticipant {
    // No attributable device, no identity — such a member also cannot be
    // reached on the media plane (same rule as `remote_identity`).
    let rtc_identity = participant
        .device_id
        .as_deref()
        .map(|device_id| mapper(&participant.user_id, device_id, &participant.member_id));
    WasmParticipant {
        member_id: participant.member_id.clone(),
        user_id: participant.user_id.clone(),
        device_id: participant.device_id.clone(),
        is_local: participant.is_local,
        reachable: participant.reachable,
        rtc_identity,
        streams: participant
            .streams
            .iter()
            .map(|stream| WasmStreamState {
                kind: stream_kind_str(stream.kind),
                muted: stream.muted,
            })
            .collect(),
    }
}

/// One published stream on a roster entry.
#[derive(Serialize)]
struct WasmStreamState {
    kind: &'static str,
    muted: bool,
}

/// A roster entry, as JS sees it.
#[derive(Serialize)]
struct WasmParticipant {
    member_id: String,
    user_id: String,
    device_id: Option<String>,
    is_local: bool,
    reachable: bool,
    /// The livekit-js participant identity, when derivable — the join key for
    /// `room.getParticipantByIdentity()`.
    rtc_identity: Option<String>,
    streams: Vec<WasmStreamState>,
}

/// A call event, as JS sees it: `{ type, ...fields }`, snake_case throughout
/// like the rest of this binding.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WasmCallEvent {
    ParticipantJoined {
        member_id: String,
        user_id: String,
    },
    ParticipantLeft {
        member_id: String,
    },
    StreamStarted {
        member_id: String,
        kind: &'static str,
    },
    StreamStopped {
        member_id: String,
        kind: &'static str,
    },
    StreamMuted {
        member_id: String,
        kind: &'static str,
    },
    StreamUnmuted {
        member_id: String,
        kind: &'static str,
    },
    ActiveSpeakers {
        speakers: Vec<WasmSpeaker>,
    },
    KeyImported {
        member_id: String,
        key_index: u8,
    },
    FrameEncryptionState {
        member_id: String,
        state: &'static str,
        /// Key indices installed for the member, when any are — the half of a
        /// failure diagnosis the media layer knows.
        installed_key_indices: Option<Vec<u8>>,
    },
    KeyDiscarded {
        member_id: String,
        key_index: Option<u8>,
        sender_user_id: Option<String>,
        sender_device_id: Option<String>,
        /// Machine-readable rejection: `cleartext | not_cross_signed |
        /// room_mismatch | sender_mismatch | unverifiable_device |
        /// device_mismatch`.
        reason_code: &'static str,
        /// Human-readable rejection, with the mismatch details.
        reason: String,
    },
    UnknownParticipant {
        identity: String,
    },
    MediaConnectionState {
        degraded: bool,
    },
    Ended {
        /// `left`, or the transport's disconnect description.
        reason: String,
    },
}

#[derive(Serialize)]
struct WasmSpeaker {
    member_id: String,
    level: f32,
}

fn encryption_state_str(state: FrameEncryptionState) -> &'static str {
    match state {
        FrameEncryptionState::Ok => "ok",
        FrameEncryptionState::MissingKey => "missing_key",
        FrameEncryptionState::DecryptionFailed => "decryption_failed",
        FrameEncryptionState::EncryptionFailed => "encryption_failed",
        FrameEncryptionState::InternalError => "internal_error",
    }
}

fn rejection_code(rejection: &matrix_rtc_core::KeyRejection) -> &'static str {
    use matrix_rtc_core::KeyRejection;
    match rejection {
        KeyRejection::Cleartext => "cleartext",
        KeyRejection::NotCrossSigned => "not_cross_signed",
        KeyRejection::RoomMismatch { .. } => "room_mismatch",
        KeyRejection::SenderMismatch { .. } => "sender_mismatch",
        KeyRejection::UnverifiableDevice => "unverifiable_device",
        KeyRejection::DeviceMismatch { .. } => "device_mismatch",
    }
}

impl From<CallEvent> for WasmCallEvent {
    fn from(event: CallEvent) -> Self {
        match event {
            CallEvent::ParticipantJoined { member_id, user_id } => {
                Self::ParticipantJoined { member_id, user_id }
            }
            CallEvent::ParticipantLeft { member_id } => Self::ParticipantLeft { member_id },
            CallEvent::StreamStarted { member_id, kind } => Self::StreamStarted {
                member_id,
                kind: stream_kind_str(kind),
            },
            CallEvent::StreamStopped { member_id, kind } => Self::StreamStopped {
                member_id,
                kind: stream_kind_str(kind),
            },
            CallEvent::StreamMuted { member_id, kind } => Self::StreamMuted {
                member_id,
                kind: stream_kind_str(kind),
            },
            CallEvent::StreamUnmuted { member_id, kind } => Self::StreamUnmuted {
                member_id,
                kind: stream_kind_str(kind),
            },
            CallEvent::ActiveSpeakers { speakers } => Self::ActiveSpeakers {
                speakers: speakers
                    .into_iter()
                    .map(|speaker| WasmSpeaker {
                        member_id: speaker.member_id,
                        level: speaker.level,
                    })
                    .collect(),
            },
            CallEvent::KeyImported {
                member_id,
                key_index,
            } => Self::KeyImported {
                member_id,
                key_index,
            },
            CallEvent::FrameEncryptionState {
                member_id,
                state,
                diagnostic,
            } => Self::FrameEncryptionState {
                member_id,
                state: encryption_state_str(state),
                installed_key_indices: match diagnostic {
                    FrameEncryptionDiagnostic::KeysInstalled { key_indices } => Some(key_indices),
                    FrameEncryptionDiagnostic::NoKeyInstalled => Some(Vec::new()),
                    FrameEncryptionDiagnostic::NotApplicable => None,
                },
            },
            CallEvent::KeyDiscarded {
                member_id,
                key_index,
                sender_user_id,
                sender_device_id,
                reason,
            } => Self::KeyDiscarded {
                member_id,
                key_index,
                sender_user_id,
                sender_device_id,
                reason_code: rejection_code(&reason),
                reason: reason.to_string(),
            },
            CallEvent::UnknownParticipant { identity } => Self::UnknownParticipant { identity },
            CallEvent::MediaConnectionState { degraded } => Self::MediaConnectionState { degraded },
            CallEvent::Ended { reason } => Self::Ended {
                reason: match reason {
                    EndedReason::Left => "left".to_owned(),
                    EndedReason::ConnectionClosed { message } => message,
                },
            },
        }
    }
}
