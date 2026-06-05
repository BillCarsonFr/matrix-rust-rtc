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

//! Core MatrixRTC domain crate.
//!
//! This crate keeps platform-agnostic RTC behavior and receives data through DTOs
//! (`RawStickyEvent`, `StickyEventsUpdate`). DTOs are used on purpose so the core
//! is decoupled from SDK-specific event types (JS SDK objects, FFI structs, etc.).

mod event;
mod manager;
mod session;

pub use event::{
    EventConversionError, RawStickyEvent, RawStickyEventContent, RawStickyEventUpdate,
    StickyEventsUpdate,
};
pub use manager::RtcSessionManager;
pub use session::{CallMembershipEvent, JoinedMembership, LeftMembership, RtcSession};

#[cfg(test)]
mod tests {
    use super::*;

    const ROOM_ID: &str = "!room:example.org";
    const EVENT_TYPE_RTC_MEMBER: &str = "m.rtc.member";

    fn sticky_event(
        sender: &str,
        slot_id: &str,
        sticky_key: &str,
        application_type: Option<&str>,
        member_id: Option<&str>,
        disconnect_reason: Option<&str>,
    ) -> RawStickyEvent {
        RawStickyEvent {
            room_id: ROOM_ID.to_owned(),
            sender: sender.to_owned(),
            event_type: EVENT_TYPE_RTC_MEMBER.to_owned(),
            content: RawStickyEventContent {
                slot_id: slot_id.to_owned(),
                sticky_key: sticky_key.to_owned(),
                application_type: application_type.map(str::to_owned),
                member_id: member_id.map(str::to_owned),
                disconnect_reason: disconnect_reason.map(str::to_owned),
            },
        }
    }

    fn joined_event(sender: &str, slot_id: &str, sticky_key: &str) -> RawStickyEvent {
        sticky_event(
            sender,
            slot_id,
            sticky_key,
            Some("m.call"),
            Some(sticky_key),
            None,
        )
    }

    fn left_event(sender: &str, slot_id: &str, sticky_key: &str) -> RawStickyEvent {
        sticky_event(sender, slot_id, sticky_key, None, None, Some("ice_failed"))
    }

    fn left_membership(sender: &str, slot_id: &str, sticky_key: &str) -> CallMembershipEvent {
        CallMembershipEvent::Left(LeftMembership {
            room_id: ROOM_ID.to_owned(),
            slot_id: slot_id.to_owned(),
            sender: sender.to_owned(),
            sticky_key: sticky_key.to_owned(),
            disconnect_reason: Some("ice_failed".to_owned()),
        })
    }

    #[test]
    fn manager_routes_snapshot_and_diff_update_membership() {
        let mut manager = RtcSessionManager::new();

        let joined = joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a");

        manager
            .initial_sticky_for_room(ROOM_ID, vec![joined.clone()])
            .unwrap();
        assert_eq!(manager.session_count(), 1);
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));

        let joined_updated = RawStickyEvent {
            content: RawStickyEventContent {
                slot_id: "m.call#ROOM".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application_type: Some("m.call".to_owned()),
                member_id: Some("alice-device-a".to_owned()),
                disconnect_reason: None,
            },
            ..joined.clone()
        };

        let left = left_event("@alice:example.org", "m.call#ROOM", "alice-device-a");

        manager
            .sticky_update_for_room(
                ROOM_ID,
                StickyEventsUpdate {
                    added: Vec::new(),
                    updated: vec![RawStickyEventUpdate {
                        current: joined_updated,
                        previous: joined,
                    }],
                    removed: vec![left],
                },
            )
            .unwrap();

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(0));
    }

    #[test]
    fn session_is_single_session_only() {
        let mut session = RtcSession::new();

        let event = joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a")
            .try_into_call_membership_event()
            .unwrap();

        session.initial_events(vec![event]);

        assert_eq!(session.member_count(), 1);
    }

    #[test]
    fn removed_events_clear_membership_even_if_content_looks_connected() {
        let mut session = RtcSession::new();

        let joined = joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a")
            .try_into_call_membership_event()
            .unwrap();

        session.initial_events(vec![joined]);

        session.handle_update(vec![left_membership(
            "@alice:example.org",
            "m.call#ROOM",
            "alice-device-a",
        )]);

        assert_eq!(session.member_count(), 0);
    }

    #[test]
    fn manager_groups_sessions_by_slot_within_room() {
        let mut manager = RtcSessionManager::new();

        let call_one = joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a");
        let call_two = joined_event("@bob:example.org", "m.call#SIDE", "bob-device-a");

        manager
            .initial_sticky_for_room(ROOM_ID, vec![call_one, call_two])
            .unwrap();

        assert_eq!(manager.session_count(), 2);
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));
        assert_eq!(manager.member_count(ROOM_ID, "m.call#SIDE"), Some(1));
    }

    #[test]
    fn session_membership_receiver_emits_initial_and_full_snapshots() {
        let mut session = RtcSession::new();
        let mut subscription = session.subscribe_membership_snapshots();

        let initial = subscription.borrow().clone();
        assert!(initial.is_empty());

        let joined = joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a")
            .try_into_call_membership_event()
            .unwrap();

        session.initial_events(vec![joined]);

        assert!(subscription.has_changed().unwrap());
        let after_join = subscription.borrow_and_update().clone();
        assert_eq!(after_join.len(), 1);
        assert_eq!(after_join[0].sender, "@alice:example.org");

        session.handle_update(vec![left_membership(
            "@alice:example.org",
            "m.call#ROOM",
            "alice-device-a",
        )]);

        assert!(subscription.has_changed().unwrap());
        let after_leave = subscription.borrow_and_update().clone();
        assert!(after_leave.is_empty());

        assert!(!subscription.has_changed().unwrap());
    }

    #[test]
    fn manager_accepts_stable_and_unstable_rtc_member_event_types() {
        let mut manager = RtcSessionManager::new();

        let stable = joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a");

        let unstable = RawStickyEvent {
            event_type: "org.matrix.msc4143.rtc.member".to_owned(),
            ..joined_event("@bob:example.org", "m.call#ROOM", "bob-device-a")
        };

        manager
            .initial_sticky_for_room(ROOM_ID, vec![stable, unstable])
            .unwrap();

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(2));
    }

    #[test]
    fn manager_ignores_non_membership_event_types() {
        let mut manager = RtcSessionManager::new();

        let event = RawStickyEvent {
            room_id: ROOM_ID.to_owned(),
            sender: "@alice:example.org".to_owned(),
            event_type: "m.not.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: "m.call#ROOM".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application_type: None,
                member_id: None,
                disconnect_reason: None,
            },
        };

        manager
            .initial_sticky_for_room(ROOM_ID, vec![event])
            .unwrap();

        assert_eq!(manager.session_count(), 0);
    }
}
