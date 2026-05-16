//! Multi-session routing for MatrixRTC sticky events.
//!
//! The manager owns many `MatrixRtcMachine` instances and dispatches room-scoped
//! sticky snapshots/updates to the right machine by `(room_id, slot_id)`.

use std::collections::HashMap;

use crate::event::{
    EventConversionError, RawStickyEvent, RawStickyEventUpdate, StickyEventsUpdate,
};
use crate::machine::MatrixRtcMachine;

/// Holds and routes all active RTC sessions.
#[derive(Default)]
pub struct RtcSessionManager {
    sessions: HashMap<SessionKey, MatrixRtcMachine>,
}

impl RtcSessionManager {
    /// Creates an empty session manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one sticky event routed to the appropriate session machine.
    pub fn on_sticky_event_received(
        &mut self,
        event: RawStickyEvent,
    ) -> Result<(), EventConversionError> {
        let room_id = event.room_id.clone();
        self.initial_sticky_for_room(&room_id, [event])
    }

    /// Applies an initial sticky snapshot for one room, typically from SDK `getStickyEvents`.
    pub fn initial_sticky_for_room(
        &mut self,
        room_id: &str,
        events: impl IntoIterator<Item = RawStickyEvent>,
    ) -> Result<(), EventConversionError> {
        let mut batches: HashMap<SessionKey, Vec<RawStickyEvent>> = HashMap::new();

        for event in events {
            if event.room_id != room_id {
                continue;
            }

            let key = SessionKey::new(room_id.to_owned(), event.content.slot_id.clone());
            batches.entry(key).or_default().push(event);
        }

        for (key, batch) in batches {
            self.machine_for_key(key).initial_events(batch)?;
        }

        Ok(())
    }

    /// Applies a sticky diff batch for one room.
    pub fn sticky_update_for_room(
        &mut self,
        room_id: &str,
        update: StickyEventsUpdate,
    ) -> Result<(), EventConversionError> {
        let mut batches: HashMap<SessionKey, StickyEventsUpdate> = HashMap::new();

        for event in update.added {
            if event.room_id != room_id {
                continue;
            }

            let key = SessionKey::new(room_id.to_owned(), event.content.slot_id.clone());
            batches.entry(key).or_default().added.push(event);
        }

        for changed in update.updated {
            let RawStickyEventUpdate { current, previous } = changed;

            if current.room_id != room_id {
                continue;
            }

            let key = SessionKey::new(room_id.to_owned(), current.content.slot_id.clone());
            batches
                .entry(key)
                .or_default()
                .updated
                .push(RawStickyEventUpdate { current, previous });
        }

        for event in update.removed {
            if event.room_id != room_id {
                continue;
            }

            let key = SessionKey::new(room_id.to_owned(), event.content.slot_id.clone());
            batches.entry(key).or_default().removed.push(event);
        }

        for (key, batch) in batches {
            self.machine_for_key(key).handle_update(batch)?;
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
        self.sessions.get(&key).map(MatrixRtcMachine::member_count)
    }

    fn machine_for_key(&mut self, key: SessionKey) -> &mut MatrixRtcMachine {
        self.sessions.entry(key).or_default()
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
