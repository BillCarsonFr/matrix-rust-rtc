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

        session
            .initial_events(vec![joined_event(
                "@alice:example.org",
                "m.call#ROOM",
                "alice-device-a",
            )])
            .unwrap();

        assert_eq!(session.member_count(), 1);
    }

    #[test]
    fn removed_events_clear_membership_even_if_content_looks_connected() {
        let mut session = RtcSession::new();

        session
            .initial_events(vec![joined_event(
                "@alice:example.org",
                "m.call#ROOM",
                "alice-device-a",
            )])
            .unwrap();

        session
            .handle_update(StickyEventsUpdate {
                added: Vec::new(),
                updated: Vec::new(),
                removed: vec![joined_event(
                    "@alice:example.org",
                    "m.call#ROOM",
                    "alice-device-a",
                )],
            })
            .unwrap();

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

        session
            .initial_events(vec![joined_event(
                "@alice:example.org",
                "m.call#ROOM",
                "alice-device-a",
            )])
            .unwrap();

        assert!(subscription.has_changed().unwrap());
        let after_join = subscription.borrow_and_update().clone();
        assert_eq!(after_join.len(), 1);
        assert_eq!(after_join[0].sender, "@alice:example.org");

        session
            .handle_update(StickyEventsUpdate {
                added: Vec::new(),
                updated: Vec::new(),
                removed: vec![joined_event(
                    "@alice:example.org",
                    "m.call#ROOM",
                    "alice-device-a",
                )],
            })
            .unwrap();

        assert!(subscription.has_changed().unwrap());
        let after_leave = subscription.borrow_and_update().clone();
        assert!(after_leave.is_empty());

        assert!(!subscription.has_changed().unwrap());
    }
}
