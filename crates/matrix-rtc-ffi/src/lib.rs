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

use matrix_rtc_core::{
    CallMembershipEvent, EventConversionError, JoinedMembership as CoreJoinedMembership,
    RawStickyEvent, RawStickyEventUpdate, RtcSession, RtcSessionManager, StickyEventsUpdate,
};
use tokio::sync::watch;

mod commands;
pub use commands::{
    CommandSenderCallback, CommandSenderError, FfiCommandSender, FfiJoinSessionParams,
    FfiLeaveSessionParams, FfiTransportConfig,
};

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
    pub event_type: String,
    pub slot_id: String,
    pub sticky_key: String,
    pub application_type: Option<String>,
    pub member_id: Option<String>,
    pub disconnect_reason: Option<String>,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct StickyEventUpdate {
    pub current: StickyEvent,
    pub previous: StickyEvent,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct JoinedMembership {
    pub room_id: String,
    pub slot_id: String,
    pub sender: String,
    pub sticky_key: String,
    pub application: Option<String>,
}

#[derive(uniffi::Object)]
pub struct RtcSessionHandle {
    inner: Mutex<RtcSession>,
}

#[derive(uniffi::Object)]
pub struct RtcSessionManagerHandle {
    inner: Mutex<RtcSessionManager>,
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
        session.initial_events(parsed);
        Ok(())
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
        session.handle_update(membership_events);
        Ok(())
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
        manager
            .initial_sticky_for_room(&room_id, to_core_events(events))
            .map_err(map_conversion_error)
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
        manager
            .sticky_update_for_room(&room_id, update)
            .map_err(map_conversion_error)
    }

    pub fn on_sticky_events_snapshot_received(
        &self,
        events: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut manager = lock_mutex(&self.inner)?;
        manager
            .on_sticky_events_snapshot_received(to_core_events(events))
            .map_err(map_conversion_error)
    }

    pub fn on_sticky_events_update_received(
        &self,
        added: Vec<StickyEvent>,
        updated: Vec<StickyEventUpdate>,
        removed: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut manager = lock_mutex(&self.inner)?;
        manager
            .on_sticky_events_update_received(StickyEventsUpdate {
                added: to_core_events(added),
                updated: to_core_updates(updated),
                removed: to_core_events(removed),
            })
            .map_err(map_conversion_error)
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
        ApplicationInfo, DisconnectReason, MemberInfo, RawStickyEvent, RawStickyEventContent,
    };
    use std::collections::BTreeMap;

    // Map FFI types to MSC4143-compliant core types
    let application = ApplicationInfo {
        application_type: event.application_type,
        extra: BTreeMap::new(),
    };

    let member = MemberInfo {
        id: event.member_id,
        claimed_device_id: None,
        claimed_user_id: None,
    };

    let disconnect_reason = event.disconnect_reason.map(|reason| {
        // For now, map simple string to a basic disconnect reason
        // In a full implementation, this would parse the MSC4143 object
        DisconnectReason {
            class: None,
            reason: Some(reason),
            description: None,
        }
    });

    RawStickyEvent {
        room_id: event.room_id,
        sender: event.sender,
        event_type: event.event_type,
        content: RawStickyEventContent {
            slot_id: event.slot_id,
            sticky_key: event.sticky_key,
            application,
            member,
            versions: Vec::new(),
            disconnect_reason,
            m_relates_to: None,
            rtc_transports: None,
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
        sticky_key: member.sticky_key,
        application: member.application,
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
            event_type: "m.rtc.member".to_owned(),
            slot_id: "m.call#ROOM".to_owned(),
            sticky_key: "alice-device-a".to_owned(),
            application_type: Some("m.call".to_owned()),
            member_id: Some("alice-device-a".to_owned()),
            disconnect_reason: None,
        }
    }

    #[test]
    fn ffi_session_snapshot_entrypoint_accepts_join_event() {
        let session = RtcSessionHandle::new();

        let result = session.on_sticky_events_snapshot_received(vec![join_event()]);

        assert!(result.is_ok());
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
