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

//! Native UniFFI bindings for the MatrixRTC core.
//!
//! This module defines UniFFI-facing DTOs and object wrappers, then converts
//! them into core DTOs so `matrix-rtc-core` stays decoupled from FFI-specific
//! types and binding-tooling concerns.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::watch;

use matrix_rtc_bridge::compat::ElementCallCompat;
use matrix_rtc_core::{
    EventConversionError, JoinedMembership as CoreJoinedMembership, RawStickyEvent,
    RtcSessionManager,
};
mod commands;
pub mod compat;
mod logging;
mod runtime;
pub use commands::{
    CommandSenderCallback, CommandSenderError, FfiCommandSender, FfiJoinSessionParams,
    FfiLeaveSessionParams, FfiNotificationType, FfiNotifyConfig, FfiToDeviceDelivery,
    FfiToDeviceRecipient, FfiTransportConfig,
};
pub use compat::{FfiElementCallCompat, LegacyStateMemberEvent, RawMemberEvent};
pub use logging::{
    RtcLogConfig, RtcLogLevel, RtcLogRecord, RtcLogSink, dropped_log_record_count, log_event,
    setup_logging,
};

/// Participants with observable frame streams, publishing, and constraints —
/// see the module docs. Pulls the LiveKit client (libwebrtc): default off.
#[cfg(feature = "media")]
pub mod media;

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum MatrixRtcFfiError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("internal lock poisoned")]
    InternalLockPoisoned,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct StickyEvent {
    pub room_id: String,
    pub sender: String,
    /// Device that sent the event, from its decryption metadata. MSC4143 has no
    /// self-asserted device field, so the host must supply this for key
    /// distribution to target a single device.
    pub sender_device_id: Option<String>,
    /// Whether the event arrived encrypted; MSC4143 requires member events to be
    /// encrypted in encrypted rooms. `None` if unknown — which is not the same
    /// as `false`, which would drop the member in an encrypted room.
    pub was_encrypted: Option<bool>,
    pub event_type: String,
    pub slot_id: String,
    pub sticky_key: String,
    pub application_type: Option<String>,
    pub member_id: Option<String>,
    /// MSC4143 `member.membership`: "join" or "leave".
    pub membership: Option<String>,
    pub leave_reason: Option<FfiLeaveReason>,
    /// The raw MSC4143 `content.transports` object as JSON (`{"published":
    /// [...], "can_subscribe": [...]}`), passed through untyped so
    /// transport-specific fields survive the boundary. Without it the member
    /// projects with no transports — media layers then treat them as
    /// unreachable.
    pub transports_json: Option<String>,
}

/// MSC4143 `leave_reason`: a machine-readable `code` plus an optional
/// human-readable `reason`.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiLeaveReason {
    pub code: String,
    pub reason: Option<String>,
}

impl From<FfiLeaveReason> for matrix_rtc_core::LeaveReason {
    fn from(value: FfiLeaveReason) -> Self {
        matrix_rtc_core::LeaveReason {
            code: matrix_rtc_core::LeaveCode::from_code(&value.code),
            reason: value.reason,
        }
    }
}

/// An `m.rtc.slot` state event, with its content as a JSON string.
///
/// The content is passed as JSON rather than a typed record so that
/// application- and mechanism-specific fields survive the FFI boundary.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SlotEvent {
    /// The event's state key, which is the slot id.
    pub slot_id: String,
    /// The raw `m.rtc.slot` content as JSON.
    pub content_json: String,
}

impl SlotEvent {
    fn into_core(self, room_id: &str) -> Result<matrix_rtc_core::RawSlotEvent, MatrixRtcFfiError> {
        let content = serde_json::from_str(&self.content_json).map_err(|error| {
            MatrixRtcFfiError::InvalidInput(format!("invalid m.rtc.slot content: {error}"))
        })?;

        Ok(matrix_rtc_core::RawSlotEvent {
            room_id: room_id.to_owned(),
            slot_id: self.slot_id,
            content,
        })
    }
}

/// The RTC encryption mechanism an `m.rtc.slot` prescribes for its members.
///
/// Its presence is what turns RTC encryption on for the slot; its absence turns
/// it off. MSC4143 requires it in an encrypted room and forbids it elsewhere, and
/// a slot that gets this wrong resolves *closed* for every client — which reads
/// as the call simply never starting.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiSlotEncryption {
    /// MSC4143 `m.per_member`: each member distributes its own media key. The
    /// only mechanism this SDK implements, and what an encrypted room wants.
    PerMember,
    /// A mechanism named by string. Opening a slot with one this SDK does not
    /// implement means it cannot join that slot itself; it is here so a host can
    /// still publish one rather than being unable to express it.
    Other { encryption_type: String },
}

impl From<FfiSlotEncryption> for matrix_rtc_core::SlotEncryption {
    fn from(value: FfiSlotEncryption) -> Self {
        matrix_rtc_core::SlotEncryption {
            encryption_type: match value {
                FfiSlotEncryption::PerMember => "m.per_member".to_owned(),
                FfiSlotEncryption::Other { encryption_type } => encryption_type,
            },
            extra: std::collections::BTreeMap::new(),
        }
    }
}

/// A decrypted `m.rtc.encryption_key` to-device message, fed into the core so
/// peers' media keys reach the encryption manager (and, with the `media`
/// feature, the frame decryptors).
///
/// The host's Matrix SDK receives and decrypts the to-device event; the
/// `sender_*` fields come from its decryption metadata, not the payload.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiReceivedEncryptionKey {
    /// The `room_id` carried in the message content.
    pub room_id: String,
    /// The `member_id` carried in the message content.
    pub member_id: String,
    /// The key material, encoded per the message's `format`.
    pub key_b64: String,
    /// The rolling key index (0-255).
    pub key_index: u8,
    /// Whether the to-device message arrived Olm-encrypted. MSC4143 requires
    /// it; the core discards cleartext keys.
    pub was_encrypted: bool,
    /// User the message was decrypted as coming from (required when
    /// `was_encrypted`).
    pub sender_user_id: Option<String>,
    /// Device the message was decrypted as coming from, when attributable.
    pub sender_device_id: Option<String>,
    /// Whether that device is cross-signed (MSC4153).
    pub sender_is_cross_signed: bool,
}

impl FfiReceivedEncryptionKey {
    fn into_core(self) -> Result<matrix_rtc_core::ReceivedEncryptionKey, MatrixRtcFfiError> {
        let origin = if self.was_encrypted {
            matrix_rtc_core::KeyOrigin::Encrypted {
                sender_user_id: self.sender_user_id.ok_or_else(|| {
                    MatrixRtcFfiError::InvalidInput(
                        "an encrypted key needs its sender_user_id".into(),
                    )
                })?,
                sender_device_id: self.sender_device_id,
                sender_is_cross_signed: self.sender_is_cross_signed,
            }
        } else {
            matrix_rtc_core::KeyOrigin::Cleartext
        };
        Ok(matrix_rtc_core::ReceivedEncryptionKey {
            origin,
            room_id: self.room_id,
            member_id: self.member_id,
            key_b64: self.key_b64,
            key_index: self.key_index,
        })
    }
}

/// A transport a member publishes media on (MSC4143 `transports.published`).
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiRtcTransport {
    /// MSC4195 LiveKit transport.
    LiveKit { livekit_service_url: String },
    /// A transport this SDK does not know; kept for forward compatibility.
    Unsupported { transport_type: String },
}

impl From<&matrix_rtc_core::RtcTransport> for FfiRtcTransport {
    fn from(transport: &matrix_rtc_core::RtcTransport) -> Self {
        match transport {
            matrix_rtc_core::RtcTransport::LiveKit(livekit) => FfiRtcTransport::LiveKit {
                livekit_service_url: livekit.livekit_service_url.clone(),
            },
            matrix_rtc_core::RtcTransport::Unsupported(unsupported) => {
                FfiRtcTransport::Unsupported {
                    transport_type: unsupported.transport_type.clone(),
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct JoinedMembership {
    pub room_id: String,
    pub slot_id: String,
    pub sender: String,
    pub sender_device_id: Option<String>,
    pub sticky_key: String,
    pub member_id: String,
    pub application: Option<String>,
    /// Transports this member publishes media on (MSC4143).
    pub transports: Vec<FfiRtcTransport>,
    /// Transport types this member can subscribe to (MSC4143).
    pub can_subscribe: Vec<String>,
}
#[derive(uniffi::Object)]
pub struct RtcSessionManagerHandle {
    /// An async mutex because every entry point is async and holds it across
    /// awaits into the host. `Arc` so a heartbeat driver can hold a `Weak` to it
    /// without keeping the manager alive past the handle.
    inner: Arc<TokioMutex<RtcSessionManager<FfiCommandSender>>>,
    /// One driver per joined session, keyed by `(room_id, slot_id)`. Dropping
    /// the entry stops its task. A `std::sync::Mutex` on purpose: it is only
    /// ever held for a map insert or remove, never across an await.
    heartbeats: Mutex<HashMap<(String, String), HeartbeatDriver>>,
    /// The command sender installed by
    /// [`set_command_sender`](Self::set_command_sender), kept so a join can
    /// register the outbound dialect it must render its sends in. The manager
    /// holds the same `Arc`.
    command_sender: Mutex<Option<Arc<FfiCommandSender>>>,
    /// Which Element Call generation each room was joined for, by room id.
    ///
    /// One reading, three consumers: the outbound dialect above, how an inbound
    /// legacy media key is bound to a membership, and the media session's SFU
    /// identity and token endpoint. The media layer looks it up here rather than
    /// being told it again, because the two disagreeing is not an error but a
    /// silence — a connected call in which nothing decrypts. Empty for every
    /// spec-current room. See [`crate::compat`].
    element_call_compat: Mutex<HashMap<String, ElementCallCompat>>,
}

/// How often the keep-alive is driven.
///
/// Three ticks inside the 30 s default delayed-leave timeout, so a skipped tick
/// (the manager was busy) or one slow round trip cannot let the switch fire.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Owns the task that drives one session's keep-alive.
struct HeartbeatDriver {
    /// Dropped to ask the task to stop; it observes the closed channel.
    _stop: tokio::sync::mpsc::Sender<()>,
}

/// Runs one session's keep-alive until the session ends or the handle goes away.
async fn run_heartbeat(
    manager: Weak<TokioMutex<RtcSessionManager<FfiCommandSender>>>,
    room_id: String,
    slot_id: String,
    interval: Duration,
    mut stop: tokio::sync::mpsc::Receiver<()>,
) {
    loop {
        tokio::select! {
            // The driver was dropped (leave, rejoin, or the handle died), so a
            // stop takes effect at once rather than at the end of the interval.
            _ = stop.recv() => break,
            _ = tokio::time::sleep(interval) => {}
        }

        let Some(manager) = manager.upgrade() else {
            log::debug!("[{room_id}/{slot_id}] heartbeat: manager gone, stopping");
            break;
        };

        // Skip rather than queue behind an in-flight FFI call: the next tick is
        // 10 s away and the dead man's switch has 30 s, so waiting our turn
        // behind a slow host would only make the beat later than it needs to be.
        let still_joined = match manager.try_lock() {
            Ok(mut guard) => guard.heartbeat(&room_id, &slot_id).await,
            Err(_) => {
                log::debug!("[{room_id}/{slot_id}] heartbeat: manager busy, skipping a tick");
                true
            }
        };

        // `false` means the session is gone or has left — `leave` takes the
        // membership machine, so a beat racing a leave is a no-op and lands
        // here. Nothing left to keep alive.
        if !still_joined {
            log::debug!("[{room_id}/{slot_id}] heartbeat: no longer joined, stopping");
            break;
        }
    }
}

struct SubscriptionState {
    receiver: watch::Receiver<Vec<CoreJoinedMembership>>,
    initial_pending: bool,
}

#[derive(uniffi::Object)]
pub struct MembershipSnapshotSubscription {
    state: Mutex<SubscriptionState>,
}

#[uniffi::export(async_runtime = "tokio")]
impl RtcSessionManagerHandle {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(TokioMutex::new(RtcSessionManager::new())),
            heartbeats: Mutex::new(HashMap::new()),
            command_sender: Mutex::new(None),
            element_call_compat: Mutex::new(HashMap::new()),
        })
    }

    /// Sets the command sender for this manager.
    ///
    /// This must be called before join/leave operations to enable sending events
    /// back to the Matrix room. The callback will be invoked by the core to send
    /// membership events (join, leave, keep-alive) for all sessions.
    ///
    /// # Arguments
    /// * `callback` - Native implementation of CommandSenderCallback
    pub async fn set_command_sender(
        &self,
        callback: Arc<dyn CommandSenderCallback>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!("manager: command sender installed");
        let command_sender = FfiCommandSender::new(callback);
        // The same `Arc` the manager gets: a join registers its outbound dialect
        // on it, and a sender the handle could not reach would render every
        // compat send spec-current with nothing to say so.
        match lock_mutex(&self.command_sender) {
            Ok(mut slot) => *slot = Some(command_sender.clone()),
            Err(error) => log::error!("manager: could not retain the command sender: {error}"),
        }
        let mut manager = self.inner.lock().await;
        manager.set_command_sender(command_sender);
        Ok(())
    }

    /// Apply the **complete** current sticky state for one room.
    ///
    /// Replaces, rather than merges: a member absent from `events` is gone, and
    /// an empty list clears the room.
    ///
    /// That is what makes expiry work. A member does not only leave by sending a
    /// leave event — an MSC4354 sticky entry lapses when its owner stops
    /// refreshing it, which is exactly what a crashed client does, and the lapse
    /// produces no event to feed in. A merging call would keep that member in
    /// the call for good and leave every host to diff snapshots itself.
    ///
    /// The usual flow is this call once with the SDK's current sticky map for
    /// the room, and again whenever it changes. It is the only way membership
    /// reaches the core, and safe to call as often as you like — re-asserting
    /// the full state is also how you resynchronise after a reconnect.
    pub async fn set_current_sticky_state(
        &self,
        room_id: String,
        events: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::debug!(
            "manager: [{room_id}] current sticky state in, {} events ({})",
            events.len(),
            describe_events(&events),
        );
        trace_sticky_events("current", &events);

        let mut manager = self.inner.lock().await;
        manager
            .set_current_sticky_state(&room_id, to_core_events(events))
            .await
            .map_err(map_conversion_error)
    }

    /// Applies a room's complete `m.rtc.slot` state.
    ///
    /// Calling this is what makes the MSC4143 open-slot condition apply to the
    /// room; until then it cannot be evaluated and is not enforced. Any slot in
    /// the room not present in `slots` is treated as closed, so always pass the
    /// full set — an empty list included.
    ///
    /// **Ignored for a room joined in [`FfiElementCallCompat::StateEvents`]**,
    /// and the join forgets anything already supplied: that generation predates
    /// `m.rtc.slot` entirely, so its rooms contain none, and "no slots" would
    /// resolve every session closed and project out every member including us.
    /// Feed slots unconditionally and let the mode decide — there is nothing for
    /// a host to special-case.
    pub async fn on_room_slots_received(
        &self,
        room_id: String,
        slots: Vec<SlotEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::debug!(
            "manager: [{room_id}] room slots in: {:?}",
            slots.iter().map(|slot| &slot.slot_id).collect::<Vec<_>>(),
        );

        if self
            .element_call_compat_for(&room_id)
            .reads_state_membership()
        {
            log::debug!(
                "manager: [{room_id}] ignoring the slot state: the room was joined in a \
                 MatrixRTC generation that predates m.rtc.slot",
            );
            return Ok(());
        }

        let mapped = slots
            .into_iter()
            .map(|slot| slot.into_core(&room_id))
            .collect::<Result<Vec<_>, _>>()
            .inspect_err(|err| log::warn!("manager: [{room_id}] rejected a slot event: {err}"))?;

        let mut manager = self.inner.lock().await;
        manager.on_room_slots_received(&room_id, mapped).await;
        Ok(())
    }

    /// Opens a slot, by publishing its `m.rtc.slot` state event.
    ///
    /// A room has no slot until somebody opens one, and a host that reports its
    /// slot state ([`Self::on_room_slots_received`]) will project every member
    /// out of a room that has none — so in a room where no other client opens
    /// slots, this is what makes a call possible at all. That includes every room
    /// whose other participant is Element Call: no generation of it publishes
    /// `m.rtc.slot`.
    ///
    /// `slot_id` must start with `{application_type}#` — MSC4143 makes the slot
    /// id the state key and requires that shape, and a slot that ignores it is
    /// one every client treats as closed. Rejected here rather than at the
    /// homeserver, which would accept it.
    ///
    /// `encryption` must be [`FfiSlotEncryption::PerMember`] in an encrypted room
    /// and `null` elsewhere; the mismatch resolves the slot closed for everyone.
    ///
    /// Publishing room state usually needs a raised power level, so a rejection
    /// by the homeserver surfaces here.
    ///
    /// Nothing to do with the compatibility modes: in
    /// [`FfiElementCallCompat::StateEvents`] the open-slot condition is not
    /// enforced at all, so a slot opened for such a room changes nothing.
    pub async fn open_slot(
        &self,
        room_id: String,
        slot_id: String,
        application_type: String,
        encryption: Option<FfiSlotEncryption>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!(
            "manager: [{room_id}/{slot_id}] opening slot: application={application_type} \
             encryption={encryption:?}",
        );

        let manager = self.inner.lock().await;
        manager
            .open_slot(
                room_id,
                slot_id,
                application_type,
                encryption.map(Into::into),
            )
            .await
            .map_err(|error| {
                log::warn!("manager: could not open the slot: {error}");
                MatrixRtcFfiError::InvalidInput(error.to_string())
            })
    }

    /// Closes a slot, by setting its `m.rtc.slot` status to `closed`.
    ///
    /// Every member of it becomes left as soon as clients apply the new state —
    /// this ends the call for everyone, not just for us. Leaving is
    /// [`Self::leave`].
    pub async fn close_slot(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!("manager: [{room_id}/{slot_id}] closing slot");

        let manager = self.inner.lock().await;
        manager.close_slot(room_id, slot_id).await.map_err(|error| {
            log::warn!("manager: could not close the slot: {error}");
            MatrixRtcFfiError::InvalidInput(error.to_string())
        })
    }

    /// Sets the users currently joined to a room.
    ///
    /// MSC4143 only counts a member event while its sender is still joined to
    /// the room; until this is called that condition is not enforced.
    pub async fn on_room_members_received(
        &self,
        room_id: String,
        joined_user_ids: Vec<String>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::debug!(
            "manager: [{room_id}] room members in: {} joined",
            joined_user_ids.len(),
        );
        log::trace!("manager: [{room_id}] joined users: {joined_user_ids:?}");

        let mut manager = self.inner.lock().await;
        manager
            .on_room_members_received(&room_id, joined_user_ids)
            .await;
        Ok(())
    }

    /// Reports whether a room is end-to-end encrypted.
    ///
    /// MSC4143 requires RTC encryption in encrypted rooms and forbids it
    /// elsewhere, so this changes how the room's slots resolve and whether
    /// cleartext member events count.
    pub async fn on_room_encryption_received(
        &self,
        room_id: String,
        encrypted: bool,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!("manager: [{room_id}] room encryption in: encrypted={encrypted}");

        let mut manager = self.inner.lock().await;
        manager
            .on_room_encryption_received(&room_id, encrypted)
            .await;
        Ok(())
    }

    /// Feeds a decrypted `m.rtc.encryption_key` to-device message to every
    /// session of its room. Call for each such message the host's sync
    /// delivers; without it peers' media never becomes decryptable.
    pub async fn receive_encryption_key(
        &self,
        key: FfiReceivedEncryptionKey,
    ) -> Result<(), MatrixRtcFfiError> {
        // Never the key material itself — only what decides whether it is
        // accepted. Its length is enough to spot a truncated or empty key.
        log::debug!(
            "manager: [{}] encryption key in: member={} index={} len={} encrypted={} sender={:?}/{:?} cross_signed={}",
            key.room_id,
            key.member_id,
            key.key_index,
            key.key_b64.len(),
            key.was_encrypted,
            key.sender_user_id,
            key.sender_device_id,
            key.sender_is_cross_signed,
        );

        let received = key.into_core()?;
        let manager = self.inner.lock().await;
        manager
            .receive_encryption_key(received)
            .await
            .map_err(|error| {
                log::warn!("manager: encryption key rejected: {error}");
                MatrixRtcFfiError::InvalidInput(error.to_string())
            })
    }

    /// A JSON dump of everything the manager and its sessions currently
    /// believe: sessions, room state per room, and every candidate member with
    /// the reason it is or is not projected as joined.
    ///
    /// For bug reports and for answering "what does Rust think the state is
    /// right now?" without a debugger. Contains no key material.
    pub async fn debug_snapshot(&self) -> Result<String, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager.debug_snapshot().to_string())
    }

    pub async fn session_count(&self) -> Result<u64, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager.session_count() as u64)
    }

    pub async fn member_count(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<u64>, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager
            .member_count(&room_id, &slot_id)
            .map(|count| count as u64))
    }

    /// Observe the joined roster of one session.
    ///
    /// Returns `None` if no session exists for `(room_id, slot_id)` — a session
    /// appears when the first member event for that slot is fed in, or when this
    /// manager joins it.
    ///
    /// This is the roster the media layer works from, and the counterpart to
    /// [`Self::member_count`], which answers only "how many". Polling the count
    /// once a second was the previous way to notice a change: the wrong shape for
    /// a value that moves a handful of times per call, and useless for learning
    /// *who* moved. A host that wants to be told a call has started watches this.
    ///
    /// The subscription yields the current roster on its first
    /// `nextSnapshot()` and then only on change, so a host can attach at any
    /// point without missing the state it attached to.
    pub async fn subscribe_membership_snapshots(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<Arc<MembershipSnapshotSubscription>>, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager
            .subscribe_membership_snapshots(&room_id, &slot_id)
            .map(|receiver| {
                Arc::new(MembershipSnapshotSubscription {
                    state: Mutex::new(SubscriptionState {
                        receiver,
                        initial_pending: true,
                    }),
                })
            }))
    }

    /// Joins a session, returning the `member.id` it joined as.
    ///
    /// The SDK generates that id; hosts do not supply one. MSC4143 requires a
    /// fresh `member.id` on every join, and reusing one is silently destructive:
    /// the MSC4195 participant identity is derived from it, so a repeat join
    /// keeps the identity peers already hold a key for while our key index
    /// restarts at 0 — every peer then decrypts our media with the previous
    /// call's key and never recovers.
    ///
    /// Hosts that need the id later (nothing in this API does — see
    /// [`connect_media_session`](crate::media::connect_media_session)) can read
    /// it back with [`Self::own_member_id`] instead of storing it, which cannot
    /// go stale across a rejoin.
    pub async fn join(&self, params: FfiJoinSessionParams) -> Result<String, MatrixRtcFfiError> {
        log::info!("manager: join requested {}", params.summary());

        // Kept for the keep-alive driver, which outlives `params`.
        let room_id = params.room_id.clone();
        let slot_id = params.slot_id.clone();
        let user_id = params.user_id.clone();
        let device_id = params.device_id.clone();
        let compat = compat::resolve(params.element_call_compat);

        let mut core_params = params.into_core().map_err(|e| {
            log::warn!("manager: join rejected before it started: {e}");
            MatrixRtcFfiError::InvalidInput(e.to_string())
        })?;
        // Not always a fresh id: see `compat::member_id` for the one generation
        // where a fresh one makes us mark ourselves departed on our own join.
        let member_id = compat::member_id(compat, &user_id, &device_id);
        core_params.membership_id = Some(member_id.clone());

        // Before the join, not after: the join itself sends the membership (and
        // arms the delayed leave), so a dialect registered afterwards would let
        // exactly the two events that announce us go out spec-current.
        self.set_element_call_compat(&room_id, &user_id, &device_id, &slot_id, compat);

        // Hold the guard across the join, rather than swapping a placeholder
        // `RtcSessionManager::new()` in and unlocking for the duration. That
        // pattern had two failure modes: any call landing during the join —
        // `member_count`, or a sticky snapshot — operated on the *placeholder*,
        // so events arriving in that window were applied to it and then thrown
        // away when the real manager was restored; and a panic mid-join dropped
        // the real manager during unwind, silently leaving the empty one behind
        // with no poison flag to signal it.
        //
        // Serialising instead is safe because no host callback re-enters a
        // handle: the command sender only sends events outward. If that ever
        // changes, this becomes a deadlock rather than lost state.
        let mut manager = self.inner.lock().await;

        // Slots fed before this join were fed before anything knew the room
        // belonged to a generation that has none, and "no slots" is what closes
        // a session and projects out every member. Later feeds are ignored by
        // `on_room_slots_received`; this undoes the earlier ones, and must run
        // before the join so the session is created already unenforced.
        if compat.reads_state_membership() {
            manager.forget_room_slots(&room_id).await;
        }

        let result = {
            manager.join(core_params).await.map_err(|e| {
                log::warn!("manager: join failed: {e}");
                MatrixRtcFfiError::InvalidInput(e.to_string())
            })
        };

        if result.is_ok() {
            log::info!("manager: join succeeded as {member_id}");
            drop(manager);
            self.start_heartbeat(room_id, slot_id);
        }

        result.map(|()| member_id)
    }

    /// Our `member.id` in one session, or `None` if there is no such session or
    /// it has not joined.
    ///
    /// Changes on every join (MSC4143), so read it when needed rather than
    /// caching what [`Self::join`] returned.
    pub async fn own_member_id(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<String>, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager.own_member_id(&room_id, &slot_id))
    }

    /// Restarts the keep-alive for one session: reschedules the delayed leave,
    /// and re-sends the membership if its sticky entry is halfway to expiring.
    ///
    /// **Hosts do not need to call this** — [`Self::join`] starts a driver that
    /// does it every 10 seconds, and [`Self::leave`] stops it. It is exported
    /// for hosts that would rather drive the keep-alive from their own scheduler
    /// (a foreground service, a workmanager job), and for tests.
    ///
    /// Returns `false` if there is no joined session for `(room_id, slot_id)`,
    /// which means there is nothing to keep alive.
    pub async fn heartbeat(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<bool, MatrixRtcFfiError> {
        let mut manager = self.inner.lock().await;
        Ok(manager.heartbeat(&room_id, &slot_id).await)
    }

    /// Starts (or replaces) the keep-alive driver for one session.
    fn start_heartbeat(&self, room_id: String, slot_id: String) {
        self.start_heartbeat_every(room_id, slot_id, HEARTBEAT_INTERVAL);
    }

    /// [`Self::start_heartbeat`] with the interval spelled out, so a test can
    /// beat faster than a session ships with.
    fn start_heartbeat_every(&self, room_id: String, slot_id: String, interval: Duration) {
        let (stop, stop_rx) = tokio::sync::mpsc::channel(1);
        let manager = Arc::downgrade(&self.inner);
        let key = (room_id.clone(), slot_id.clone());

        // On `runtime()` rather than a thread of its own: the body is a sleep
        // and an await on a mutex, and `tokio::time::sleep` needs a timer to
        // fire at all. Detached — it stops when the `stop` sender below is
        // dropped, or when the manager behind its `Weak` goes away.
        runtime::runtime().spawn(run_heartbeat(manager, room_id, slot_id, interval, stop_rx));

        log::info!(
            "manager: keep-alive driver started for [{}/{}] every {interval:?}",
            key.0,
            key.1,
        );
        // Replaces any previous driver for this session; dropping the old
        // sender stops its task.
        match lock_mutex(&self.heartbeats) {
            Ok(mut drivers) => {
                drivers.insert(key, HeartbeatDriver { _stop: stop });
            }
            Err(error) => log::error!("manager: could not register keep-alive: {error}"),
        }
    }

    /// Stops the keep-alive driver for one session, if any.
    fn stop_heartbeat(&self, room_id: &str, slot_id: &str) {
        let key = (room_id.to_owned(), slot_id.to_owned());
        match lock_mutex(&self.heartbeats) {
            Ok(mut drivers) => {
                if drivers.remove(&key).is_some() {
                    log::debug!("manager: keep-alive driver stopped for [{room_id}/{slot_id}]");
                }
            }
            Err(error) => log::error!("manager: could not stop the keep-alive: {error}"),
        }
    }

    pub async fn leave(
        &self,
        room_id: String,
        slot_id: String,
        params: FfiLeaveSessionParams,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!(
            "manager: leave requested [{room_id}/{slot_id}] reason={:?}",
            params.leave_reason,
        );

        let core_params = params.into_core();

        // Stop the keep-alive first, so it cannot re-arm a delayed leave after
        // the leave below cancels it. A beat already in flight is harmless: it
        // holds the manager lock we are about to take, and once `leave` has
        // taken the membership machine any later beat is a no-op.
        self.stop_heartbeat(&room_id, &slot_id);

        // Kept for the compat cleanup below, which happens after the leave has
        // been rendered in the dialect it is a leave *in*.
        let left_room = room_id.clone();

        // Held across the leave, for the reasons in `join` above. It matters
        // most here: losing the manager to a placeholder mid-leave would drop
        // the very state needed to depart, leaving the membership live until the
        // dead man's switch expired.
        let mut manager = self.inner.lock().await;

        let result = {
            manager
                .leave(room_id, slot_id, core_params)
                .await
                .map_err(|e| {
                    log::warn!("manager: leave failed: {e}");
                    MatrixRtcFfiError::InvalidInput(e.to_string())
                })
        };

        if result.is_ok() {
            log::info!("manager: leave succeeded");
            // Only now: the leave itself had to be rendered in the dialect it is
            // a leave *in*, or the peers we joined for would never see it.
            self.clear_element_call_compat(&left_room);
        }

        result
    }

    /// Apply the **complete** current membership for one room, from raw event
    /// content, across every MatrixRTC generation the room contains.
    ///
    /// The counterpart of [`Self::set_current_sticky_state`] for hosts talking to
    /// pre-2026 Element Call, and it replaces rather than merges for the same
    /// reason: a member does not only leave by sending a leave, and a lapsed
    /// entry produces no event to feed in.
    ///
    /// Both sources go in **one** call because both are replaced by it. Fed
    /// separately, each call would wipe the other's members and the roster would
    /// flicker between the two halves of the call. A room with no pre-sticky
    /// members passes an empty list — the usual case, and cheap.
    ///
    /// - `member_events` — the room's live `m.rtc.member` sticky events, content
    ///   verbatim. The pre-2026 sticky normalisation runs on every one of them
    ///   and is safe for spec-current content (it only ever fills in a modern
    ///   field that is absent), so a mixed room needs no sorting by the host.
    /// - `legacy_state_events` — the room's `org.matrix.msc3401.call.member`
    ///   **room state**, for [`FfiElementCallCompat::StateEvents`]. Translated to
    ///   MSC4143 memberships here, including the expiry that generation states in
    ///   its content rather than having the homeserver enforce.
    ///
    /// A sticky membership wins over a state one with the same key: an Element
    /// Call build mid-transition can write both, and the same human twice in the
    /// roster means two receive streams and two key exchanges for one peer.
    pub async fn set_current_membership(
        &self,
        room_id: String,
        member_events: Vec<RawMemberEvent>,
        legacy_state_events: Vec<LegacyStateMemberEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::debug!(
            "manager: [{room_id}] current membership in: {} sticky + {} pre-sticky state event(s)",
            member_events.len(),
            legacy_state_events.len(),
        );

        let current =
            compat::merge_current_membership(&room_id, member_events, legacy_state_events);

        let mut manager = self.inner.lock().await;
        manager
            .set_current_sticky_state(&room_id, current)
            .await
            .map_err(map_conversion_error)
    }

    /// Feeds a decrypted pre-2026 `io.element.call.encryption_keys` to-device
    /// message to every session of its room.
    ///
    /// The legacy counterpart of [`Self::receive_encryption_key`], taking the
    /// content raw because the two generations disagree about where the key, the
    /// index and the owning membership live. Without it a legacy peer's media
    /// never becomes decryptable, and it is the half of interop most easily
    /// forgotten: the roster fills in, everything looks joined, and every remote
    /// tile stays black.
    ///
    /// Feed **every** message of that type; which mode the room was joined in
    /// decides how the key is bound, and this call knows it already.
    ///
    /// `sender` is the to-device event's sender (homeserver-stamped, so the one
    /// identity in the event worth trusting), and `sender_device_id` the device
    /// Olm decryption attributed it to. A key that arrived in the clear is
    /// discarded by the core, as MSC4143 requires — pass `was_encrypted` honestly
    /// rather than smoothing it over.
    pub async fn receive_legacy_encryption_key(
        &self,
        sender: String,
        content_json: String,
        was_encrypted: bool,
        sender_device_id: Option<String>,
        sender_is_cross_signed: bool,
    ) -> Result<(), MatrixRtcFfiError> {
        let content: serde_json::Value = serde_json::from_str(&content_json).map_err(|error| {
            log::warn!("manager: a legacy encryption key is not JSON: {error}");
            MatrixRtcFfiError::InvalidInput(format!("invalid key message content: {error}"))
        })?;

        let room_id = content
            .get("room_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let compat = self.element_call_compat_for(&room_id);

        let Some(key) =
            compat::parse_legacy_key(compat, &sender, sender_device_id.as_deref(), &content)
        else {
            // Not an error the host can act on — the message is simply unusable —
            // but silence here reads as a key that never arrived, which is the
            // hardest interop failure to diagnose.
            log::warn!(
                "manager: [{room_id}] ignoring a legacy encryption key from {sender}: it is \
                 missing a required field, or names no device to bind it to. That peer's media \
                 will not decrypt.",
            );
            return Ok(());
        };

        log::debug!(
            "manager: [{}] legacy encryption key in: member={} index={} len={} encrypted={} \
             sender={sender}/{sender_device_id:?} cross_signed={sender_is_cross_signed}",
            key.room_id,
            key.member_id,
            key.key_index,
            key.key_b64.len(),
            was_encrypted,
        );

        let received = FfiReceivedEncryptionKey {
            room_id: key.room_id,
            member_id: key.member_id,
            key_b64: key.key_b64,
            key_index: key.key_index,
            was_encrypted,
            sender_user_id: Some(sender),
            sender_device_id,
            sender_is_cross_signed,
        }
        .into_core()?;

        let manager = self.inner.lock().await;
        manager
            .receive_encryption_key(received)
            .await
            .map_err(|error| {
                log::warn!("manager: legacy encryption key rejected: {error}");
                MatrixRtcFfiError::InvalidInput(error.to_string())
            })
    }
}

/// The compat bookkeeping a join installs and a leave clears.
///
/// Not exported: hosts choose the mode once, on
/// [`FfiJoinSessionParams::element_call_compat`], and everything else reads it
/// back from here. A second way to set it would be a second way for the four
/// derivations it feeds to disagree.
impl RtcSessionManagerHandle {
    /// Record the mode for `room_id` and install the outbound dialect it implies.
    fn set_element_call_compat(
        &self,
        room_id: &str,
        user_id: &str,
        device_id: &str,
        slot_id: &str,
        compat: ElementCallCompat,
    ) {
        match lock_mutex(&self.element_call_compat) {
            Ok(mut modes) => {
                if compat == ElementCallCompat::Off {
                    // A rejoin can turn compat off, and a stale entry would keep
                    // rendering our sends for a generation we are no longer
                    // talking to.
                    modes.remove(room_id);
                } else {
                    log::info!(
                        "manager: [{room_id}/{slot_id}] joining in Element Call compatibility \
                         mode {compat:?}",
                    );
                    modes.insert(room_id.to_owned(), compat);
                }
            }
            Err(error) => log::error!("manager: could not record the compat mode: {error}"),
        }

        let dialect = compat::outbound_dialect(compat, user_id, device_id, room_id, slot_id);
        match lock_mutex(&self.command_sender) {
            Ok(sender) => match sender.as_ref() {
                Some(sender) => sender.set_dialect(room_id, dialect),
                // Joining without a command sender fails in the core a moment
                // later; say which half was missing while it is still obvious.
                None if compat != ElementCallCompat::Off => log::warn!(
                    "manager: [{room_id}] compat mode {compat:?} was requested before a command \
                     sender was installed; sends cannot be rendered for it",
                ),
                None => {}
            },
            Err(error) => log::error!("manager: could not install the outbound dialect: {error}"),
        }
    }

    /// Forget `room_id`'s mode and dialect.
    ///
    /// Per room, so leaving one slot forgets it for the room's other sessions
    /// too. Same reasoning as the sender's dialect map: this exists to talk to a
    /// generation of Element Call with one call per room, and a room-keyed
    /// to-device key cannot be told two slots apart anyway.
    fn clear_element_call_compat(&self, room_id: &str) {
        if let Ok(mut modes) = lock_mutex(&self.element_call_compat) {
            modes.remove(room_id);
        }
        if let Ok(sender) = lock_mutex(&self.command_sender)
            && let Some(sender) = sender.as_ref()
        {
            sender.clear_dialect(room_id);
        }
    }

    /// The generation `room_id` was joined for, or [`ElementCallCompat::Off`].
    ///
    /// `pub(crate)` for the media layer, which derives its SFU identity and picks
    /// its token endpoint from this rather than from a second host-supplied
    /// value — see the field's docs.
    pub(crate) fn element_call_compat_for(&self, room_id: &str) -> ElementCallCompat {
        lock_mutex(&self.element_call_compat)
            .ok()
            .and_then(|modes| modes.get(room_id).copied())
            .unwrap_or_default()
    }
}

#[uniffi::export]
impl MembershipSnapshotSubscription {
    pub fn next_snapshot(&self) -> Result<Option<Vec<JoinedMembership>>, MatrixRtcFfiError> {
        let mut state = lock_mutex(&self.state)?;

        let snapshot = if state.initial_pending {
            state.initial_pending = false;
            Some(state.receiver.borrow().clone())
        } else {
            match state.receiver.has_changed() {
                Ok(true) => Some(state.receiver.borrow_and_update().clone()),
                Ok(false) | Err(_) => None,
            }
        };

        Ok(snapshot.map(|members| {
            members
                .into_iter()
                .map(to_ffi_joined_membership)
                .collect::<Vec<_>>()
        }))
    }
}

fn to_core_event(event: StickyEvent) -> matrix_rtc_core::RawStickyEvent {
    use matrix_rtc_core::{
        ApplicationInfo, MemberInfo, Membership, RawStickyEvent, RawStickyEventContent,
    };
    use std::collections::BTreeMap;

    // Map FFI types to MSC4143-compliant core types
    let application = ApplicationInfo {
        application_type: event.application_type,
        extra: BTreeMap::new(),
    };

    let member = MemberInfo {
        id: event.member_id,
        membership: event.membership.map(|m| match m.as_str() {
            "join" => Membership::Join,
            "leave" => Membership::Leave,
            _ => Membership::Unknown(m),
        }),
    };

    // A malformed transports object degrades that member to "no transports"
    // (media layers see them as unreachable) rather than failing the whole
    // sticky batch — it is host-supplied JSON, not protocol input we control.
    let transports = event.transports_json.and_then(|json| {
        serde_json::from_str(&json)
            .map_err(|error| {
                log::warn!("ignoring malformed transports JSON on sticky event: {error}");
            })
            .ok()
    });

    RawStickyEvent {
        room_id: event.room_id,
        sender: event.sender,
        origin: match event.was_encrypted {
            Some(true) => matrix_rtc_core::EventOrigin::encrypted(event.sender_device_id),
            Some(false) => matrix_rtc_core::EventOrigin::Cleartext,
            None => matrix_rtc_core::EventOrigin::Unknown,
        },
        event_type: event.event_type,
        content: RawStickyEventContent {
            slot_id: event.slot_id,
            sticky_key: event.sticky_key,
            application,
            member,
            transports,
            leave_reason: event.leave_reason.map(Into::into),
        },
    }
}

fn to_core_events(events: Vec<StickyEvent>) -> Vec<RawStickyEvent> {
    events.into_iter().map(to_core_event).collect()
}

fn to_ffi_joined_membership(member: CoreJoinedMembership) -> JoinedMembership {
    JoinedMembership {
        room_id: member.room_id,
        slot_id: member.slot_id,
        sender: member.sender,
        sender_device_id: member.origin.sender_device_id().map(str::to_owned),
        sticky_key: member.sticky_key,
        member_id: member.member_id,
        application: member.application,
        transports: member.transports.iter().map(Into::into).collect(),
        can_subscribe: member.can_subscribe,
    }
}

fn map_conversion_error(err: EventConversionError) -> MatrixRtcFfiError {
    log::warn!("rejected a sticky event batch: {err}");
    MatrixRtcFfiError::InvalidInput(err.to_string())
}

/// Compact `(room, slot)` census of a sticky batch: which sessions it touched,
/// and how many events each got.
fn describe_events(events: &[StickyEvent]) -> String {
    if events.is_empty() {
        return "none".to_owned();
    }

    let mut counts: Vec<(String, usize)> = Vec::new();
    for event in events {
        let key = format!("{}/{}", event.room_id, event.slot_id);
        match counts.iter_mut().find(|(seen, _)| *seen == key) {
            Some((_, count)) => *count += 1,
            None => counts.push((key, 1)),
        }
    }

    counts
        .iter()
        .map(|(key, count)| format!("{key} x{count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Per-event detail for a sticky batch, for when a member is dropped for a
/// reason the summary cannot show.
fn trace_sticky_events(kind: &str, events: &[StickyEvent]) {
    if !log::log_enabled!(log::Level::Trace) {
        return;
    }

    for event in events {
        log::trace!(
            "sticky {kind}: [{}/{}] type={} sender={} device={:?} sticky_key={} membership={:?} encrypted={:?} transports={}",
            event.room_id,
            event.slot_id,
            event.event_type,
            event.sender,
            event.sender_device_id,
            event.sticky_key,
            event.membership,
            event.was_encrypted,
            event.transports_json.is_some(),
        );
    }
}

/// Locks `mutex`, recovering from poisoning rather than propagating it.
///
/// A panic anywhere inside a handle method used to poison the handle for the
/// rest of the process: every later call — `member_count`, and critically
/// `leave` — returned [`MatrixRtcFfiError::InternalLockPoisoned`] forever, so
/// the host could not even depart the session and its membership stayed live
/// until the dead man's switch expired. One panic permanently disabling the
/// manager, including the ability to leave it, is worse than the panic.
///
/// So we take the guard anyway and clear the flag. The state behind it may have
/// been mid-mutation when the panic unwound, which is why this logs at error
/// level: it is a bug worth reporting, not a condition to handle silently.
///
/// Still returns `Result` — the signature is what ~20 call sites and the
/// `media` module expect, and it keeps room for a future fallible lock — but
/// the error path is now unreachable.
fn lock_mutex<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, MatrixRtcFfiError> {
    match mutex.lock() {
        Ok(guard) => Ok(guard),
        Err(poisoned) => {
            log::error!(
                "recovering a poisoned lock: an earlier call panicked and its state may be \
                 inconsistent. Please report this with the panic that preceded it."
            );
            mutex.clear_poison();
            Ok(poisoned.into_inner())
        }
    }
}

uniffi::setup_scaffolding!();

#[cfg(test)]
mod tests {
    use super::*;

    /// The manual entry point exists and reports honestly when there is nothing
    /// to keep alive, rather than pretending it beat something.
    #[tokio::test]
    async fn heartbeat_reports_no_session_when_not_joined() {
        let manager = RtcSessionManagerHandle::new();
        assert!(
            !manager
                .heartbeat("!room:example.org".to_owned(), "m.call#ROOM".to_owned())
                .await
                .expect("the call itself should succeed"),
        );
    }

    /// A driver is only registered by a successful join, and `leave` must clear
    /// it even for a session that was never joined — otherwise a failed join
    /// would leave a thread beating forever.
    #[test]
    fn stopping_an_unknown_heartbeat_is_harmless() {
        let manager = RtcSessionManagerHandle::new();
        manager.stop_heartbeat("!room:example.org", "m.call#ROOM");
        assert!(lock_mutex(&manager.heartbeats).unwrap().is_empty());
    }

    fn join_event() -> StickyEvent {
        StickyEvent {
            room_id: "!room:example.org".to_owned(),
            sender: "@alice:example.org".to_owned(),
            sender_device_id: Some("DEVICEID".to_owned()),
            was_encrypted: Some(true),
            event_type: "m.rtc.member".to_owned(),
            slot_id: "m.call#ROOM".to_owned(),
            sticky_key: "alice-device-a".to_owned(),
            application_type: Some("m.call".to_owned()),
            member_id: Some("alice-device-a".to_owned()),
            membership: Some("join".to_owned()),
            leave_reason: None,
            transports_json: Some(
                r#"{"published":[{"type":"livekit","livekit_service_url":"https://sfu.example.org"}],"can_subscribe":["livekit"]}"#
                    .to_owned(),
            ),
        }
    }

    #[tokio::test]
    async fn ffi_snapshot_entrypoint_accepts_join_event() {
        let manager = RtcSessionManagerHandle::new();

        let result = manager
            .set_current_sticky_state("!room:example.org".to_owned(), vec![join_event()])
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn transports_round_trip_through_the_ffi() {
        let manager = RtcSessionManagerHandle::new();
        manager
            .set_current_sticky_state("!room:example.org".to_owned(), vec![join_event()])
            .await
            .unwrap();
        let subscription = manager
            .subscribe_membership_snapshots(
                "!room:example.org".to_owned(),
                "m.call#ROOM".to_owned(),
            )
            .await
            .unwrap()
            .expect("the member event should have created the session");

        let joined = subscription.next_snapshot().unwrap().unwrap();
        assert_eq!(
            joined[0].transports,
            vec![FfiRtcTransport::LiveKit {
                livekit_service_url: "https://sfu.example.org".to_owned(),
            }]
        );
        assert_eq!(joined[0].can_subscribe, vec!["livekit".to_owned()]);
    }

    /// The roster is observable from the *manager* handle, which is the one a
    /// host drives and the one the media layer needs. Previously it existed only
    /// on `RtcSessionHandle`, so a host running the manager could see
    /// `memberCount` but never who was in the call — and polling a count once a
    /// second was the only way to notice a change.
    #[tokio::test]
    async fn the_manager_publishes_the_roster_of_a_session() {
        let manager = RtcSessionManagerHandle::new();
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        assert!(
            manager
                .subscribe_membership_snapshots(room_id.clone(), slot_id.clone())
                .await
                .unwrap()
                .is_none(),
            "no session for that slot yet, so nothing to observe"
        );

        manager
            .set_current_sticky_state(room_id.clone(), vec![join_event()])
            .await
            .expect("the current state should be accepted");

        let subscription = manager
            .subscribe_membership_snapshots(room_id, slot_id)
            .await
            .unwrap()
            .expect("the member event should have created the session");

        // Attaching late still yields the state attached to, not silence.
        let initial = subscription
            .next_snapshot()
            .unwrap()
            .expect("the first call reports the current roster");
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].sender, "@alice:example.org");

        assert_eq!(
            subscription.next_snapshot().unwrap(),
            None,
            "and then only on change"
        );
    }

    /// One room, three generations, one roster: a spec-current peer, a 2025
    /// Element Call peer, and a pre-sticky one carried in room state.
    ///
    /// The point of the single entry point. Fed as two calls, each would replace
    /// the other's members and the roster would flicker between the two halves of
    /// the call.
    #[tokio::test]
    async fn membership_from_every_generation_lands_in_one_roster() {
        let manager = RtcSessionManagerHandle::new();
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        let spec = RawMemberEvent {
            sender: "@alice:example.org".to_owned(),
            sender_device_id: Some("ALICEDEV".to_owned()),
            was_encrypted: Some(true),
            event_type: "m.rtc.member".to_owned(),
            content_json: serde_json::json!({
                "slot_id": "m.call#ROOM",
                "msc4354_sticky_key": "alice-a",
                "application": { "type": "m.call" },
                "member": { "id": "alice-a", "membership": "join" },
                "transports": {
                    "published": [{ "type": "livekit", "livekit_service_url": "https://sfu" }],
                    "can_subscribe": ["livekit"],
                },
            })
            .to_string(),
        };
        // No `membership`, no `transports` — that generation states neither.
        let legacy_sticky = RawMemberEvent {
            sender: "@bob:example.org".to_owned(),
            sender_device_id: Some("BOBDEV".to_owned()),
            was_encrypted: Some(true),
            event_type: "m.rtc.member".to_owned(),
            content_json: serde_json::json!({
                "slot_id": "m.call#ROOM",
                "msc4354_sticky_key": "bob-a",
                "application": { "type": "m.call" },
                "member": { "id": "bob-a", "user_id": "@bob:example.org", "device_id": "BOBDEV" },
                "rtc_transports": [{ "type": "livekit", "livekit_service_url": "https://sfu" }],
                "versions": [],
            })
            .to_string(),
        };
        let pre_sticky = LegacyStateMemberEvent {
            sender: "@carl:example.org".to_owned(),
            state_key: "_@carl:example.org_CARLDEV_m.call".to_owned(),
            origin_server_ts: matrix_rtc_bridge::compat::element_call_state::now_ms(),
            content_json: serde_json::json!({
                "application": "m.call",
                "call_id": "",
                "device_id": "CARLDEV",
                "expires": 14_400_000_u64,
                "membershipID": "@carl:example.org:CARLDEV",
                "foci_preferred": [{ "type": "livekit", "livekit_service_url": "https://sfu" }],
            })
            .to_string(),
        };

        manager
            .set_current_membership(room_id.clone(), vec![spec, legacy_sticky], vec![pre_sticky])
            .await
            .expect("the membership should be accepted");

        let subscription = manager
            .subscribe_membership_snapshots(room_id, slot_id)
            .await
            .unwrap()
            .expect("the member events should have created the session");
        let mut joined = subscription
            .next_snapshot()
            .unwrap()
            .expect("the first call reports the current roster");
        joined.sort_by(|a, b| a.sender.cmp(&b.sender));

        assert_eq!(
            joined
                .iter()
                .map(|member| member.sender.as_str())
                .collect::<Vec<_>>(),
            [
                "@alice:example.org",
                "@bob:example.org",
                "@carl:example.org"
            ],
        );
        // Every one of them must be bound to a device, or no media key can travel
        // in either direction and they are in the roster with nothing else.
        assert_eq!(
            joined
                .iter()
                .map(|member| member.sender_device_id.as_deref())
                .collect::<Vec<_>>(),
            [Some("ALICEDEV"), Some("BOBDEV"), Some("CARLDEV")],
        );
        // And to a transport, lifted out of whichever shape stated it.
        assert!(
            joined
                .iter()
                .all(|member| member.transports.len() == 1 && member.can_subscribe == ["livekit"]),
            "every generation's SFU must survive the translation: {joined:?}",
        );
    }

    /// A sticky membership wins over a state one for the same member: an Element
    /// Call build mid-transition can write both, and the same human twice in the
    /// roster means two receive streams and two key exchanges for one peer.
    #[tokio::test]
    async fn a_sticky_membership_supersedes_the_state_one_for_the_same_member() {
        let manager = RtcSessionManagerHandle::new();
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        let sticky = RawMemberEvent {
            sender: "@carl:example.org".to_owned(),
            sender_device_id: Some("CARLDEV".to_owned()),
            was_encrypted: Some(true),
            event_type: "m.rtc.member".to_owned(),
            content_json: serde_json::json!({
                "slot_id": "m.call#ROOM",
                "msc4354_sticky_key": "@carl:example.org:CARLDEV",
                "application": { "type": "m.call" },
                "member": { "id": "@carl:example.org:CARLDEV", "membership": "join" },
                "transports": {
                    "published": [{ "type": "livekit", "livekit_service_url": "https://sfu" }],
                    "can_subscribe": ["livekit"],
                },
            })
            .to_string(),
        };
        let same_member_in_state = LegacyStateMemberEvent {
            sender: "@carl:example.org".to_owned(),
            state_key: "_@carl:example.org_CARLDEV_m.call".to_owned(),
            origin_server_ts: matrix_rtc_bridge::compat::element_call_state::now_ms(),
            content_json: serde_json::json!({
                "application": "m.call",
                "device_id": "CARLDEV",
                "expires": 14_400_000_u64,
                "membershipID": "@carl:example.org:CARLDEV",
            })
            .to_string(),
        };

        manager
            .set_current_membership(room_id.clone(), vec![sticky], vec![same_member_in_state])
            .await
            .expect("the membership should be accepted");

        let joined = manager
            .subscribe_membership_snapshots(room_id, slot_id)
            .await
            .unwrap()
            .expect("a session")
            .next_snapshot()
            .unwrap()
            .expect("a roster");
        assert_eq!(joined.len(), 1, "carl should appear once: {joined:?}");
        // The sticky one, which is the one carrying a transport.
        assert_eq!(joined[0].transports.len(), 1);
    }

    /// A panic inside one handle method must not disable the handle forever.
    /// It used to: the poisoned mutex made every later call — `leave` included —
    /// return `InternalLockPoisoned`, so the host could not depart the session
    /// and its membership stayed live until the dead man's switch expired.
    #[test]
    fn a_poisoned_lock_is_recovered_rather_than_propagated() {
        let mutex = Mutex::new(0_u32);

        let panicked = std::panic::catch_unwind(|| {
            let mut guard = mutex.lock().unwrap();
            *guard = 1;
            panic!("simulates a panic while the guard is held");
        });
        assert!(panicked.is_err());
        assert!(mutex.is_poisoned(), "the mutex should now be poisoned");

        // The value the panicking call had written is still there, and the lock
        // is usable again.
        let guard = lock_mutex(&mutex).expect("a poisoned lock must still be usable");
        assert_eq!(*guard, 1);
        drop(guard);
        assert!(
            !mutex.is_poisoned(),
            "the poison flag should have been cleared"
        );
    }
}
