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

    #[test]
    fn manager_routes_snapshot_and_diff_update_membership() {
        let mut manager = RtcSessionManager::new();

        let joined = RawStickyEvent {
            room_id: "!room:example.org".to_owned(),
            sender: "@alice:example.org".to_owned(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: "m.call#ROOM".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application_type: Some("m.call".to_owned()),
                member_id: Some("alice-device-a".to_owned()),
                disconnect_reason: None,
            },
        };

        manager
            .initial_sticky_for_room("!room:example.org", vec![joined.clone()])
            .unwrap();
        assert_eq!(manager.session_count(), 1);
        assert_eq!(
            manager.member_count("!room:example.org", "m.call#ROOM"),
            Some(1)
        );

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

        let left = RawStickyEvent {
            room_id: "!room:example.org".to_owned(),
            sender: "@alice:example.org".to_owned(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: "m.call#ROOM".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application_type: None,
                member_id: None,
                disconnect_reason: Some("ice_failed".to_owned()),
            },
        };

        manager
            .sticky_update_for_room(
                "!room:example.org",
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

        assert_eq!(
            manager.member_count("!room:example.org", "m.call#ROOM"),
            Some(0)
        );
    }

    #[test]
    fn session_is_single_session_only() {
        let mut session = RtcSession::new();

        session
            .initial_events(vec![RawStickyEvent {
                room_id: "!room:example.org".to_owned(),
                sender: "@alice:example.org".to_owned(),
                event_type: "m.rtc.member".to_owned(),
                content: RawStickyEventContent {
                    slot_id: "m.call#ROOM".to_owned(),
                    sticky_key: "alice-device-a".to_owned(),
                    application_type: Some("m.call".to_owned()),
                    member_id: Some("alice-device-a".to_owned()),
                    disconnect_reason: None,
                },
            }])
            .unwrap();

        assert_eq!(session.member_count(), 1);
    }

    #[test]
    fn removed_events_clear_membership_even_if_content_looks_connected() {
        let mut session = RtcSession::new();

        session
            .initial_events(vec![RawStickyEvent {
                room_id: "!room:example.org".to_owned(),
                sender: "@alice:example.org".to_owned(),
                event_type: "m.rtc.member".to_owned(),
                content: RawStickyEventContent {
                    slot_id: "m.call#ROOM".to_owned(),
                    sticky_key: "alice-device-a".to_owned(),
                    application_type: Some("m.call".to_owned()),
                    member_id: Some("alice-device-a".to_owned()),
                    disconnect_reason: None,
                },
            }])
            .unwrap();

        session
            .handle_update(StickyEventsUpdate {
                added: Vec::new(),
                updated: Vec::new(),
                removed: vec![RawStickyEvent {
                    room_id: "!room:example.org".to_owned(),
                    sender: "@alice:example.org".to_owned(),
                    event_type: "m.rtc.member".to_owned(),
                    content: RawStickyEventContent {
                        slot_id: "m.call#ROOM".to_owned(),
                        sticky_key: "alice-device-a".to_owned(),
                        application_type: Some("m.call".to_owned()),
                        member_id: Some("alice-device-a".to_owned()),
                        disconnect_reason: None,
                    },
                }],
            })
            .unwrap();

        assert_eq!(session.member_count(), 0);
    }

    #[test]
    fn manager_groups_sessions_by_slot_within_room() {
        let mut manager = RtcSessionManager::new();

        let call_one = RawStickyEvent {
            room_id: "!room:example.org".to_owned(),
            sender: "@alice:example.org".to_owned(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: "m.call#ROOM".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application_type: Some("m.call".to_owned()),
                member_id: Some("alice-device-a".to_owned()),
                disconnect_reason: None,
            },
        };
        let call_two = RawStickyEvent {
            room_id: "!room:example.org".to_owned(),
            sender: "@bob:example.org".to_owned(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: "m.call#SIDE".to_owned(),
                sticky_key: "bob-device-a".to_owned(),
                application_type: Some("m.call".to_owned()),
                member_id: Some("bob-device-a".to_owned()),
                disconnect_reason: None,
            },
        };

        manager
            .initial_sticky_for_room("!room:example.org", vec![call_one, call_two])
            .unwrap();

        assert_eq!(manager.session_count(), 2);
        assert_eq!(
            manager.member_count("!room:example.org", "m.call#ROOM"),
            Some(1)
        );
        assert_eq!(
            manager.member_count("!room:example.org", "m.call#SIDE"),
            Some(1)
        );
    }
}
