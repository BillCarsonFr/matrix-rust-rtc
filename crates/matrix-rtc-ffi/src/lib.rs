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

use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::watch;

use matrix_rtc_core::{
    CallMembershipEvent, EventConversionError, JoinedMembership as CoreJoinedMembership,
    RawStickyEvent, RawStickyEventUpdate, RtcSession, RtcSessionManager, StickyEventsUpdate,
};
mod commands;
pub use commands::{
    CommandSenderCallback, CommandSenderError, FfiCommandSender, FfiJoinSessionParams,
    FfiLeaveSessionParams, FfiTransportConfig,
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

#[derive(Clone, Debug, uniffi::Record)]
pub struct StickyEventUpdate {
    pub current: StickyEvent,
    pub previous: StickyEvent,
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
pub struct RtcSessionHandle {
    inner: Mutex<RtcSession<FfiCommandSender>>,
}

#[derive(uniffi::Object)]
pub struct RtcSessionManagerHandle {
    inner: Mutex<RtcSessionManager<FfiCommandSender>>,
}

struct SubscriptionState {
    receiver: watch::Receiver<Vec<CoreJoinedMembership>>,
    initial_pending: bool,
}

#[derive(uniffi::Object)]
pub struct MembershipSnapshotSubscription {
    state: Mutex<SubscriptionState>,
}

#[uniffi::export]
impl RtcSessionHandle {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(RtcSession::new()),
        })
    }

    /// Sets the command sender for this session.
    ///
    /// This must be called before join/leave operations to enable sending events
    /// back to the Matrix room. The callback will be invoked by the core to send
    /// membership events (join, leave, keep-alive).
    ///
    /// # Arguments
    /// * `callback` - Native implementation of CommandSenderCallback
    pub fn set_command_sender(
        &self,
        callback: Box<dyn CommandSenderCallback>,
    ) -> Result<(), MatrixRtcFfiError> {
        let command_sender = FfiCommandSender::new(Arc::from(callback));
        let mut session = lock_mutex(&self.inner)?;
        session.set_command_sender(command_sender);
        Ok(())
    }

    pub fn on_sticky_events_snapshot_received(
        &self,
        events: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        let parsed = to_core_membership_events(to_core_events(events))?;
        let mut session = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            session.initial_events(parsed).await;
            Ok(())
        })
    }

    pub fn on_sticky_events_update_received(
        &self,
        added: Vec<StickyEvent>,
        updated: Vec<StickyEventUpdate>,
        removed: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut membership_events = to_core_membership_events(to_core_events(added))?;

        let updated_events = to_core_membership_events(
            to_core_updates(updated)
                .into_iter()
                .map(|update| update.current)
                .collect(),
        )?;
        membership_events.extend(updated_events);

        let removed_events = to_core_left_membership_events(to_core_events(removed))?;
        membership_events.extend(removed_events);

        let mut session = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            session.handle_update(membership_events).await;
            Ok(())
        })
    }

    pub fn subscribe_membership_snapshots(
        &self,
    ) -> Result<Arc<MembershipSnapshotSubscription>, MatrixRtcFfiError> {
        let session = lock_mutex(&self.inner)?;
        let receiver = session.subscribe_membership_snapshots();

        Ok(Arc::new(MembershipSnapshotSubscription {
            state: Mutex::new(SubscriptionState {
                receiver,
                initial_pending: true,
            }),
        }))
    }

    pub fn join(&self, params: FfiJoinSessionParams) -> Result<(), MatrixRtcFfiError> {
        let core_params = params
            .into_core()
            .map_err(|e| MatrixRtcFfiError::InvalidInput(e.to_string()))?;

        // Take the session out of the mutex to avoid holding the guard across await
        let mut inner = lock_mutex(&self.inner)?;
        let mut session = std::mem::replace(&mut *inner, RtcSession::new());

        // Drop the lock before doing async work
        drop(inner);

        // Do the async join
        // For FFI, the command sender callbacks are synchronous, so the async
        // operations will complete immediately. We use a simple block_on.
        let result = futures::executor::block_on(async {
            session
                .join(core_params)
                .await
                .map_err(|e| MatrixRtcFfiError::InvalidInput(e.to_string()))
        });

        // Store the session back
        let mut inner = lock_mutex(&self.inner)?;
        *inner = session;

        result
    }

    pub fn leave(&self, params: FfiLeaveSessionParams) -> Result<(), MatrixRtcFfiError> {
        let _core_params = params.into_core();

        let _session = lock_mutex(&self.inner)?;

        // Note: This requires room_id and slot_id to be tracked in the session
        // For now, we need to handle this properly
        // This is a limitation that the session needs to track its room/slot
        Err(MatrixRtcFfiError::InvalidInput(
            "leave() on single session requires room_id and slot_id to be tracked by the session itself. Use RtcSessionManagerHandle::leave() instead.".to_string(),
        ))
    }
}

#[uniffi::export]
impl RtcSessionManagerHandle {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(RtcSessionManager::new()),
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
    pub fn set_command_sender(
        &self,
        callback: Box<dyn CommandSenderCallback>,
    ) -> Result<(), MatrixRtcFfiError> {
        let command_sender = FfiCommandSender::new(Arc::from(callback));
        let mut manager = lock_mutex(&self.inner)?;
        manager.set_command_sender(command_sender);
        Ok(())
    }

    pub fn initial_sticky_for_room(
        &self,
        room_id: String,
        events: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut manager = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            manager
                .initial_sticky_for_room(&room_id, to_core_events(events))
                .await
                .map_err(map_conversion_error)
        })
    }

    pub fn sticky_update_for_room(
        &self,
        room_id: String,
        added: Vec<StickyEvent>,
        updated: Vec<StickyEventUpdate>,
        removed: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        let update = StickyEventsUpdate {
            added: to_core_events(added),
            updated: to_core_updates(updated),
            removed: to_core_events(removed),
        };

        let mut manager = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            manager
                .sticky_update_for_room(&room_id, update)
                .await
                .map_err(map_conversion_error)
        })
    }

    pub fn on_sticky_events_snapshot_received(
        &self,
        events: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut manager = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            manager
                .on_sticky_events_snapshot_received(to_core_events(events))
                .await
                .map_err(map_conversion_error)
        })
    }

    pub fn on_sticky_events_update_received(
        &self,
        added: Vec<StickyEvent>,
        updated: Vec<StickyEventUpdate>,
        removed: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut manager = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            manager
                .on_sticky_events_update_received(StickyEventsUpdate {
                    added: to_core_events(added),
                    updated: to_core_updates(updated),
                    removed: to_core_events(removed),
                })
                .await
                .map_err(map_conversion_error)
        })
    }

    /// Applies a room's complete `m.rtc.slot` state.
    ///
    /// Calling this is what makes the MSC4143 open-slot condition apply to the
    /// room; until then it cannot be evaluated and is not enforced. Any slot in
    /// the room not present in `slots` is treated as closed, so always pass the
    /// full set — an empty list included.
    pub fn on_room_slots_received(
        &self,
        room_id: String,
        slots: Vec<SlotEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        let mapped = slots
            .into_iter()
            .map(|slot| slot.into_core(&room_id))
            .collect::<Result<Vec<_>, _>>()?;

        let mut manager = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            manager.on_room_slots_received(&room_id, mapped).await;
            Ok(())
        })
    }

    /// Sets the users currently joined to a room.
    ///
    /// MSC4143 only counts a member event while its sender is still joined to
    /// the room; until this is called that condition is not enforced.
    pub fn on_room_members_received(
        &self,
        room_id: String,
        joined_user_ids: Vec<String>,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut manager = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            manager
                .on_room_members_received(&room_id, joined_user_ids)
                .await;
            Ok(())
        })
    }

    /// Reports whether a room is end-to-end encrypted.
    ///
    /// MSC4143 requires RTC encryption in encrypted rooms and forbids it
    /// elsewhere, so this changes how the room's slots resolve and whether
    /// cleartext member events count.
    pub fn on_room_encryption_received(
        &self,
        room_id: String,
        encrypted: bool,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut manager = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            manager
                .on_room_encryption_received(&room_id, encrypted)
                .await;
            Ok(())
        })
    }

    /// Feeds a decrypted `m.rtc.encryption_key` to-device message to every
    /// session of its room. Call for each such message the host's sync
    /// delivers; without it peers' media never becomes decryptable.
    pub fn receive_encryption_key(
        &self,
        key: FfiReceivedEncryptionKey,
    ) -> Result<(), MatrixRtcFfiError> {
        let received = key.into_core()?;
        let manager = lock_mutex(&self.inner)?;
        futures::executor::block_on(async {
            manager
                .receive_encryption_key(received)
                .await
                .map_err(|error| MatrixRtcFfiError::InvalidInput(error.to_string()))
        })
    }

    pub fn session_count(&self) -> Result<u64, MatrixRtcFfiError> {
        let manager = lock_mutex(&self.inner)?;
        Ok(manager.session_count() as u64)
    }

    pub fn member_count(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<u64>, MatrixRtcFfiError> {
        let manager = lock_mutex(&self.inner)?;
        Ok(manager
            .member_count(&room_id, &slot_id)
            .map(|count| count as u64))
    }

    pub fn join(&self, params: FfiJoinSessionParams) -> Result<(), MatrixRtcFfiError> {
        let core_params = params
            .into_core()
            .map_err(|e| MatrixRtcFfiError::InvalidInput(e.to_string()))?;

        // Take the manager out of the mutex to avoid holding the guard across await
        let mut inner = lock_mutex(&self.inner)?;
        let mut manager = std::mem::replace(&mut *inner, RtcSessionManager::new());

        // Drop the lock before doing async work
        drop(inner);

        // Do the async join
        let result = futures::executor::block_on(async {
            manager
                .join(core_params)
                .await
                .map_err(|e| MatrixRtcFfiError::InvalidInput(e.to_string()))
        });

        // Store the manager back
        let mut inner = lock_mutex(&self.inner)?;
        *inner = manager;

        result
    }

    pub fn leave(
        &self,
        room_id: String,
        slot_id: String,
        params: FfiLeaveSessionParams,
    ) -> Result<(), MatrixRtcFfiError> {
        let core_params = params.into_core();

        // Take the manager out of the mutex to avoid holding the guard across await
        let mut inner = lock_mutex(&self.inner)?;
        let mut manager = std::mem::replace(&mut *inner, RtcSessionManager::new());

        // Drop the lock before doing async work
        drop(inner);

        // Do the async leave
        let result = futures::executor::block_on(async {
            manager
                .leave(room_id, slot_id, core_params)
                .await
                .map_err(|e| MatrixRtcFfiError::InvalidInput(e.to_string()))
        });

        // Store the manager back
        let mut inner = lock_mutex(&self.inner)?;
        *inner = manager;

        result
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

fn to_core_updates(updates: Vec<StickyEventUpdate>) -> Vec<RawStickyEventUpdate> {
    updates
        .into_iter()
        .map(|update| RawStickyEventUpdate {
            current: to_core_event(update.current),
            previous: to_core_event(update.previous),
        })
        .collect()
}

fn to_core_membership_events(
    events: Vec<RawStickyEvent>,
) -> Result<Vec<CallMembershipEvent>, MatrixRtcFfiError> {
    events.into_iter().try_fold(Vec::new(), |mut acc, event| {
        match event.try_into_call_membership_event() {
            Ok(event) => {
                acc.push(event);
                Ok(acc)
            }
            Err(EventConversionError::UnsupportedEventType { .. }) => Ok(acc),
            Err(err) => Err(map_conversion_error(err)),
        }
    })
}

fn to_core_left_membership_events(
    events: Vec<RawStickyEvent>,
) -> Result<Vec<CallMembershipEvent>, MatrixRtcFfiError> {
    events.into_iter().try_fold(Vec::new(), |mut acc, event| {
        match event.try_into_left_membership_event() {
            Ok(event) => {
                acc.push(event);
                Ok(acc)
            }
            Err(EventConversionError::UnsupportedEventType { .. }) => Ok(acc),
            Err(err) => Err(map_conversion_error(err)),
        }
    })
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
    MatrixRtcFfiError::InvalidInput(err.to_string())
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, MatrixRtcFfiError> {
    mutex
        .lock()
        .map_err(|_| MatrixRtcFfiError::InternalLockPoisoned)
}

uniffi::setup_scaffolding!();

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn ffi_session_snapshot_entrypoint_accepts_join_event() {
        let session = RtcSessionHandle::new();

        let result = session.on_sticky_events_snapshot_received(vec![join_event()]);

        assert!(result.is_ok());
    }

    #[test]
    fn transports_round_trip_through_the_ffi() {
        let session = RtcSessionHandle::new();
        let subscription = session.subscribe_membership_snapshots().unwrap();
        let _ = subscription.next_snapshot();

        session
            .on_sticky_events_snapshot_received(vec![join_event()])
            .unwrap();

        let joined = subscription.next_snapshot().unwrap().unwrap();
        assert_eq!(
            joined[0].transports,
            vec![FfiRtcTransport::LiveKit {
                livekit_service_url: "https://sfu.example.org".to_owned(),
            }]
        );
        assert_eq!(joined[0].can_subscribe, vec!["livekit".to_owned()]);
    }

    #[test]
    fn ffi_session_subscription_emits_initial_then_join_snapshot() {
        let session = RtcSessionHandle::new();
        let subscription = session.subscribe_membership_snapshots().unwrap();

        let initial = subscription.next_snapshot().unwrap();
        assert_eq!(initial, Some(Vec::new()));

        let snapshot_result = session.on_sticky_events_snapshot_received(vec![join_event()]);
        assert!(snapshot_result.is_ok());

        let joined = subscription.next_snapshot().unwrap().unwrap();
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].sender, "@alice:example.org");

        let no_update = subscription.next_snapshot().unwrap();
        assert_eq!(no_update, None);
    }
}
