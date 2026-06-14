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

//! Multi-session routing for MatrixRTC sticky events.
//!
//! The manager owns many `RtcSession` instances and dispatches room-scoped
//! sticky snapshots/updates to the right session by `(room_id, slot_id)`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::commands::RtcCommandSender;
use crate::error::{JoinError, LeaveError};
use crate::event::{
    EventConversionError, RawStickyEvent, RawStickyEventUpdate, StickyEventsUpdate,
};
use crate::join::{JoinSessionParams, LeaveSessionParams};
use crate::session::{CallMembershipEvent, RtcSession};

/// Holds and routes all active RTC sessions.
#[derive(Default)]
pub struct RtcSessionManager {
    sessions: HashMap<SessionKey, RtcSession>,
    /// Command sender for sending events to Matrix rooms.
    /// This is passed to sessions when they are created or when they need to send commands.
    command_sender: Option<Arc<dyn RtcCommandSender>>,
}

impl RtcSessionManager {
    // TODO(msc4143): add a manager-level lifecycle subscription API that emits
    // when sessions are created/removed (separate from per-session membership snapshots).
    /// Creates an empty session manager without a command sender.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty session manager with a command sender.
    pub fn with_command_sender(command_sender: Arc<dyn RtcCommandSender>) -> Self {
        Self {
            sessions: HashMap::new(),
            command_sender: Some(command_sender),
        }
    }

    /// Sets the command sender for this manager.
    pub fn set_command_sender(&mut self, command_sender: Arc<dyn RtcCommandSender>) {
        self.command_sender = Some(command_sender);
    }

    /// Returns true if this manager has a command sender configured.
    pub fn has_command_sender(&self) -> bool {
        self.command_sender.is_some()
    }

    /// Joins an RTC session with the given parameters.
    ///
    /// This will find or create the appropriate session for the given room_id and slot_id,
    /// and then call join on that session.
    ///
    /// # Arguments
    ///
    /// * `params` - The join parameters including user info, transport, etc.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the join completed successfully.
    /// Returns `Err(JoinError)` if validation fails, command sender not configured, or commands fail.
    pub async fn join(&mut self, params: JoinSessionParams) -> Result<(), JoinError> {
        let command_sender = self
            .command_sender
            .as_ref()
            .ok_or(JoinError::CommandError(
                crate::error::CommandError::from_message("no command sender configured"),
            ))?
            .clone();

        let key = SessionKey::new(params.room_id.clone(), params.slot_id.clone());

        // Get or create the session
        let session = self.sessions.entry(key).or_insert_with(|| {
            let mut session = RtcSession::new();
            session.set_command_sender(command_sender.clone());
            session
        });

        // If the session doesn't have a command sender yet, set it
        if !session.has_command_sender() {
            session.set_command_sender(command_sender);
        }

        session.join(params).await
    }

    /// Leaves an RTC session.
    ///
    /// This will find the appropriate session for the given room_id and slot_id,
    /// and then call leave on that session.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID of the session to leave
    /// * `slot_id` - The slot ID of the session to leave
    /// * `params` - The leave parameters including optional disconnect reason
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the leave completed successfully.
    /// Returns `Err(LeaveError)` if the session doesn't exist or other errors occur.
    pub async fn leave(
        &mut self,
        room_id: String,
        slot_id: String,
        params: LeaveSessionParams,
    ) -> Result<(), LeaveError> {
        let key = SessionKey::new(room_id, slot_id);
        let session = self.sessions.get_mut(&key).ok_or(LeaveError::CommandError(
            crate::error::CommandError::from_message("session not found"),
        ))?;

        session.leave(params).await
    }

    /// Applies an initial sticky snapshot for one room, typically from SDK `getStickyEvents`.
    pub fn initial_sticky_for_room(
        &mut self,
        room_id: &str,
        events: impl IntoIterator<Item = RawStickyEvent>,
    ) -> Result<(), EventConversionError> {
        let mut batches: HashMap<SessionKey, Vec<CallMembershipEvent>> = HashMap::new();

        for event in events {
            if event.room_id != room_id {
                continue;
            }

            let Some(event) = self.try_convert_membership_event(event)? else {
                continue;
            };

            let slot_id = match &event {
                CallMembershipEvent::Joined(joined) => joined.slot_id.clone(),
                CallMembershipEvent::Left(left) => left.slot_id.clone(),
            };

            let key = SessionKey::new(room_id.to_owned(), slot_id);
            batches.entry(key).or_default().push(event);
        }

        for (key, batch) in batches {
            self.session_for_key(key).initial_events(batch);
        }

        Ok(())
    }

    /// Applies a sticky diff batch for one room.
    pub fn sticky_update_for_room(
        &mut self,
        room_id: &str,
        update: StickyEventsUpdate,
    ) -> Result<(), EventConversionError> {
        let mut batches: HashMap<SessionKey, Vec<CallMembershipEvent>> = HashMap::new();

        for event in update.added {
            if event.room_id != room_id {
                continue;
            }

            let Some(event) = self.try_convert_membership_event(event)? else {
                continue;
            };

            let slot_id = match &event {
                CallMembershipEvent::Joined(joined) => joined.slot_id.clone(),
                CallMembershipEvent::Left(left) => left.slot_id.clone(),
            };

            let key = SessionKey::new(room_id.to_owned(), slot_id);
            batches.entry(key).or_default().push(event);
        }

        for changed in update.updated {
            let RawStickyEventUpdate { current, previous } = changed;

            if current.room_id != room_id {
                continue;
            }

            let Some(event) = self.try_convert_membership_event(current)? else {
                continue;
            };

            let slot_id = match &event {
                CallMembershipEvent::Joined(joined) => joined.slot_id.clone(),
                CallMembershipEvent::Left(left) => left.slot_id.clone(),
            };

            let key = SessionKey::new(room_id.to_owned(), slot_id);
            batches.entry(key).or_default().push(event);

            let _ = previous;
        }

        for event in update.removed {
            if event.room_id != room_id {
                continue;
            }

            let Some(event) = self.try_convert_removed_membership_event(event)? else {
                continue;
            };

            let slot_id = match &event {
                CallMembershipEvent::Joined(joined) => joined.slot_id.clone(),
                CallMembershipEvent::Left(left) => left.slot_id.clone(),
            };

            let key = SessionKey::new(room_id.to_owned(), slot_id);
            batches.entry(key).or_default().push(event);
        }

        for (key, batch) in batches {
            self.session_for_key(key).handle_update(batch);
        }

        Ok(())
    }

    /// Applies an initial sticky snapshot, grouped by room and slot.
    pub fn on_sticky_events_snapshot_received(
        &mut self,
        events: impl IntoIterator<Item = RawStickyEvent>,
    ) -> Result<(), EventConversionError> {
        let mut by_room: HashMap<String, Vec<RawStickyEvent>> = HashMap::new();

        for event in events {
            by_room
                .entry(event.room_id.clone())
                .or_default()
                .push(event);
        }

        for (room_id, batch) in by_room {
            self.initial_sticky_for_room(&room_id, batch)?;
        }

        Ok(())
    }

    /// Applies one incremental sticky diff batch, grouped by room and slot.
    pub fn on_sticky_events_update_received(
        &mut self,
        update: StickyEventsUpdate,
    ) -> Result<(), EventConversionError> {
        let mut by_room: HashMap<String, StickyEventsUpdate> = HashMap::new();

        for event in update.added {
            by_room
                .entry(event.room_id.clone())
                .or_default()
                .added
                .push(event);
        }

        for changed in update.updated {
            by_room
                .entry(changed.current.room_id.clone())
                .or_default()
                .updated
                .push(changed);
        }

        for event in update.removed {
            by_room
                .entry(event.room_id.clone())
                .or_default()
                .removed
                .push(event);
        }

        for (room_id, batch) in by_room {
            self.sticky_update_for_room(&room_id, batch)?;
        }

        Ok(())
    }

    /// Returns the number of tracked sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Returns the member count for one `(room_id, slot_id)` session.
    pub fn member_count(&self, room_id: &str, slot_id: &str) -> Option<usize> {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        self.sessions.get(&key).map(RtcSession::member_count)
    }

    fn session_for_key(&mut self, key: SessionKey) -> &mut RtcSession {
        self.sessions.entry(key).or_default()
    }

    fn try_convert_membership_event(
        &self,
        event: RawStickyEvent,
    ) -> Result<Option<CallMembershipEvent>, EventConversionError> {
        match event.try_into_call_membership_event() {
            Ok(event) => Ok(Some(event)),
            Err(EventConversionError::UnsupportedEventType { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn try_convert_removed_membership_event(
        &self,
        event: RawStickyEvent,
    ) -> Result<Option<CallMembershipEvent>, EventConversionError> {
        match event.try_into_left_membership_event() {
            Ok(event) => Ok(Some(event)),
            Err(EventConversionError::UnsupportedEventType { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionKey {
    room_id: String,
    slot_id: String,
}

impl SessionKey {
    fn new(room_id: String, slot_id: String) -> Self {
        Self { room_id, slot_id }
    }
}
