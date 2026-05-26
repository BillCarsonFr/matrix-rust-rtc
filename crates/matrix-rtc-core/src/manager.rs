// Copyright 2026 Valere Fedronic
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
// OF ANY KIND, either express or implied. See the License for the specific
// language governing permissions and limitations under the License.

//! Multi-session routing for MatrixRTC sticky events.
//!
//! The manager owns many `RtcSession` instances and dispatches room-scoped
//! sticky snapshots/updates to the right session by `(room_id, slot_id)`.

use std::collections::HashMap;

use crate::event::{
    EventConversionError, RawStickyEvent, RawStickyEventUpdate, StickyEventsUpdate,
};
use crate::session::{CallMembershipEvent, RtcSession};

/// Holds and routes all active RTC sessions.
#[derive(Default)]
pub struct RtcSessionManager {
    sessions: HashMap<SessionKey, RtcSession>,
}

impl RtcSessionManager {
    // TODO(msc4143): add a manager-level lifecycle subscription API that emits
    // when sessions are created/removed (separate from per-session membership snapshots).
    /// Creates an empty session manager.
    pub fn new() -> Self {
        Self::default()
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
