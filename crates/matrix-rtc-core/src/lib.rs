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

mod commands;
mod encryption;
mod error;
mod event;
mod join;
mod manager;
mod own_membership;
mod session;
mod slot;
mod transport;
mod wire;

pub use commands::RtcCommandSender;
pub use encryption::types::{
    EncryptionConfig, InboundEncryptionKey, KeyMaterialSignal, KeyOrigin, KeyRejection,
    OutboundEncryptionKey, OutdatedKeyFilter, ParticipantDeviceInfo, ReceivedEncryptionKey,
};
pub use encryption::{
    EncryptionKeySignalHandler, EncryptionManager, KEY_MESSAGE_TYPE, RtcIdentityMapper,
};
pub use error::{CommandError, JoinError, LeaveError};
pub use event::{
    EventConversionError, EventOrigin, RawStickyEvent, RawStickyEventContent, RawStickyEventUpdate,
    StickyEventsUpdate,
};
pub use join::{JoinSessionParams, LeaveSessionParams, TransportIntent, generate_member_id};
pub use manager::RtcSessionManager;
pub use own_membership::{
    KeepAliveInfo, OwnMembershipMachine, OwnMembershipState, transport_to_json,
};
pub use session::{
    ApplicationInfo, CallMembershipEvent, JoinedMembership, LeaveCode, LeaveReason, LeftMembership,
    MemberInfo, Membership, RtcSession,
};
pub use slot::{
    EncryptionMechanism, OpenSlot, RawSlotEvent, RawSlotEventContent, RoomEncryption,
    SLOT_EVENT_TYPE, SlotEncryption, SlotState, SlotStatus,
};
pub use transport::{
    LiveKitTransport, MemberTransports, RawRtcTransport, RtcTransport, UnsupportedTransport,
};
pub use wire::wire_event_type;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::NoopCommandSender;
    use std::sync::Arc;

    const ROOM_ID: &str = "!room:example.org";
    const EVENT_TYPE_RTC_MEMBER: &str = "m.rtc.member";

    fn sticky_event(
        sender: &str,
        slot_id: &str,
        sticky_key: &str,
        application_type: Option<&str>,
        member: MemberInfo,
        leave_reason: Option<LeaveReason>,
    ) -> RawStickyEvent {
        RawStickyEvent {
            room_id: ROOM_ID.to_owned(),
            sender: sender.to_owned(),
            origin: EventOrigin::default(),
            event_type: EVENT_TYPE_RTC_MEMBER.to_owned(),
            content: RawStickyEventContent {
                slot_id: slot_id.to_owned(),
                sticky_key: sticky_key.to_owned(),
                application: ApplicationInfo {
                    application_type: application_type.map(str::to_owned),
                    extra: std::collections::BTreeMap::new(),
                },
                member,
                transports: None,
                leave_reason,
            },
        }
    }

    fn joined_event(sender: &str, slot_id: &str, sticky_key: &str) -> RawStickyEvent {
        sticky_event(
            sender,
            slot_id,
            sticky_key,
            Some("m.call"),
            MemberInfo {
                id: Some(sticky_key.to_owned()),
                membership: Some(Membership::Join),
            },
            None,
        )
    }

    #[allow(dead_code)]
    fn left_event(sender: &str, slot_id: &str, sticky_key: &str) -> RawStickyEvent {
        sticky_event(
            sender,
            slot_id,
            sticky_key,
            None,
            MemberInfo {
                id: Some(sticky_key.to_owned()),
                membership: Some(Membership::Leave),
            },
            Some(LeaveReason::new(LeaveCode::Leave)),
        )
    }

    fn slot_event(slot_id: &str, json: &str) -> RawSlotEvent {
        RawSlotEvent {
            room_id: ROOM_ID.to_owned(),
            slot_id: slot_id.to_owned(),
            content: serde_json::from_str(json).expect("slot content must parse"),
        }
    }

    fn open_call_slot() -> RawSlotEvent {
        slot_event(
            "m.call#ROOM",
            r#"{ "status": "open", "application": { "type": "m.call" } }"#,
        )
    }

    /// Restored after fixing the `RtcSessionManager::new()` recursion that used
    /// to overflow the stack (it was misattributed to watch channels).
    #[tokio::test]
    async fn manager_routes_snapshot_and_diff_update_membership() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();
        let joined = joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a");

        manager
            .initial_sticky_for_room(ROOM_ID, vec![joined.clone()])
            .await
            .unwrap();

        assert_eq!(manager.session_count(), 1);
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));

        let left = left_event("@alice:example.org", "m.call#ROOM", "alice-device-a");
        manager
            .sticky_update_for_room(
                ROOM_ID,
                StickyEventsUpdate {
                    added: Vec::new(),
                    updated: Vec::new(),
                    removed: vec![left],
                },
            )
            .await
            .unwrap();

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(0));
    }

    #[tokio::test]
    async fn manager_accepts_stable_and_unstable_rtc_member_event_types() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        let stable = joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a");
        let unstable = RawStickyEvent {
            event_type: "org.matrix.msc4143.rtc.member".to_owned(),
            ..joined_event("@bob:example.org", "m.call#ROOM", "bob-device-a")
        };

        manager
            .initial_sticky_for_room(ROOM_ID, vec![stable, unstable])
            .await
            .unwrap();

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(2));
    }

    #[tokio::test]
    async fn manager_ignores_non_membership_event_types() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        let event = RawStickyEvent {
            event_type: "m.not.rtc.member".to_owned(),
            ..joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a")
        };

        manager
            .initial_sticky_for_room(ROOM_ID, vec![event])
            .await
            .unwrap();

        assert_eq!(manager.session_count(), 0);
    }

    /// Until a host supplies room state the open-slot condition cannot be
    /// evaluated, so it is not enforced; otherwise every existing consumer would
    /// silently see an empty session.
    #[tokio::test]
    async fn members_are_joined_while_slot_state_is_unsupplied() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager
            .initial_sticky_for_room(
                ROOM_ID,
                vec![joined_event("@alice:example.org", "m.call#ROOM", "alice-a")],
            )
            .await
            .unwrap();

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));
        assert_eq!(manager.slot_state(ROOM_ID, "m.call#ROOM"), None);
    }

    /// MSC4143: a member event only counts as joined against an *open* slot.
    /// Supplying room state with no slot in it means the slot is closed.
    #[tokio::test]
    async fn members_are_left_when_no_slot_is_open() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager
            .initial_sticky_for_room(
                ROOM_ID,
                vec![joined_event("@alice:example.org", "m.call#ROOM", "alice-a")],
            )
            .await
            .unwrap();
        manager.on_room_slots_received(ROOM_ID, Vec::new()).await;

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(0));
        assert_eq!(
            manager.slot_state(ROOM_ID, "m.call#ROOM"),
            Some(SlotState::Closed)
        );
    }

    #[tokio::test]
    async fn members_are_joined_against_an_open_slot() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager
            .on_room_slots_received(ROOM_ID, vec![open_call_slot()])
            .await;
        manager
            .initial_sticky_for_room(
                ROOM_ID,
                vec![joined_event("@alice:example.org", "m.call#ROOM", "alice-a")],
            )
            .await
            .unwrap();

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));
    }

    /// "Clients MUST constantly react to and respect the latest state of the
    /// room": closing a slot mid-session leaves everyone in it, and reopening it
    /// brings back the members whose events are still sticky.
    #[tokio::test]
    async fn closing_and_reopening_a_slot_re_evaluates_members() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager
            .on_room_slots_received(ROOM_ID, vec![open_call_slot()])
            .await;
        manager
            .initial_sticky_for_room(
                ROOM_ID,
                vec![joined_event("@alice:example.org", "m.call#ROOM", "alice-a")],
            )
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));

        manager
            .on_room_slots_received(
                ROOM_ID,
                vec![slot_event("m.call#ROOM", r#"{ "status": "closed" }"#)],
            )
            .await;
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(0));

        manager
            .on_room_slots_received(ROOM_ID, vec![open_call_slot()])
            .await;
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));
    }

    /// Slot state that arrives before the session exists still governs it.
    #[tokio::test]
    async fn slot_state_applies_to_sessions_created_later() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager.on_room_slots_received(ROOM_ID, Vec::new()).await;
        manager
            .initial_sticky_for_room(
                ROOM_ID,
                vec![joined_event("@alice:example.org", "m.call#ROOM", "alice-a")],
            )
            .await
            .unwrap();

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(0));
    }

    /// A slot in one room says nothing about the same slot id in another.
    #[tokio::test]
    async fn slot_state_is_scoped_to_its_room() {
        const OTHER_ROOM: &str = "!other:example.org";
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager
            .on_room_slots_received(ROOM_ID, vec![open_call_slot()])
            .await;

        assert!(manager.slot_state(ROOM_ID, "m.call#ROOM").is_some());
        assert_eq!(manager.slot_state(OTHER_ROOM, "m.call#ROOM"), None);
    }

    /// MSC4143: a member event only counts while its sender is still joined to
    /// the room.
    #[tokio::test]
    async fn members_who_left_the_room_are_not_joined() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager
            .initial_sticky_for_room(
                ROOM_ID,
                vec![
                    joined_event("@alice:example.org", "m.call#ROOM", "alice-a"),
                    joined_event("@bob:example.org", "m.call#ROOM", "bob-a"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(2));

        // Bob is no longer in the room, though his member event is still sticky.
        manager
            .on_room_members_received(ROOM_ID, vec!["@alice:example.org".to_owned()])
            .await;

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));
    }

    /// MSC4143: in an encrypted room a member event that was not encrypted
    /// "MUST be considered left".
    #[tokio::test]
    async fn cleartext_member_events_are_left_in_an_encrypted_room() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        let encrypted = RawStickyEvent {
            origin: EventOrigin::encrypted(Some("ALICEDEV".to_owned())),
            ..joined_event("@alice:example.org", "m.call#ROOM", "alice-a")
        };
        let cleartext = RawStickyEvent {
            origin: EventOrigin::Cleartext,
            ..joined_event("@bob:example.org", "m.call#ROOM", "bob-a")
        };

        manager
            .initial_sticky_for_room(ROOM_ID, vec![encrypted, cleartext])
            .await
            .unwrap();
        // Nothing has reported the room's encryption yet, so neither is judged.
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(2));

        manager.on_room_encryption_received(ROOM_ID, true).await;
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));
    }

    /// An unencrypted room imposes no such requirement.
    #[tokio::test]
    async fn cleartext_member_events_are_fine_in_an_unencrypted_room() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        let cleartext = RawStickyEvent {
            origin: EventOrigin::Cleartext,
            ..joined_event("@bob:example.org", "m.call#ROOM", "bob-a")
        };
        manager
            .initial_sticky_for_room(ROOM_ID, vec![cleartext])
            .await
            .unwrap();
        manager.on_room_encryption_received(ROOM_ID, false).await;

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));
    }

    /// A slot with no encryption object is closed in an encrypted room, so its
    /// members are left even though everything else about them is valid.
    #[tokio::test]
    async fn unencrypted_slot_closes_in_an_encrypted_room() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager
            .on_room_slots_received(ROOM_ID, vec![open_call_slot()])
            .await;
        manager
            .initial_sticky_for_room(
                ROOM_ID,
                vec![joined_event("@alice:example.org", "m.call#ROOM", "alice-a")],
            )
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));

        manager.on_room_encryption_received(ROOM_ID, true).await;

        assert_eq!(
            manager.slot_state(ROOM_ID, "m.call#ROOM"),
            Some(SlotState::Closed)
        );
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(0));
    }

    /// Room encryption arriving after the slot re-resolves it, and vice versa;
    /// the manager keeps slots unresolved so either order works.
    #[tokio::test]
    async fn slot_resolution_reacts_to_room_encryption_in_either_order() {
        let encrypted_slot = || {
            slot_event(
                "m.call#ROOM",
                r#"{ "status": "open",
                     "application": { "type": "m.call" },
                     "encryption": { "type": "m.per_member" } }"#,
            )
        };

        // Encryption first, then the slot.
        let mut a: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();
        a.on_room_encryption_received(ROOM_ID, true).await;
        a.on_room_slots_received(ROOM_ID, vec![encrypted_slot()])
            .await;
        assert!(a.slot_state(ROOM_ID, "m.call#ROOM").unwrap().is_open());

        // Slot first, then encryption.
        let mut b: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();
        b.on_room_slots_received(ROOM_ID, vec![encrypted_slot()])
            .await;
        b.on_room_encryption_received(ROOM_ID, true).await;
        assert!(b.slot_state(ROOM_ID, "m.call#ROOM").unwrap().is_open());
    }

    /// The slot's `encryption` object — not local configuration — decides
    /// whether media keys are distributed. Exercised end to end: joining a slot
    /// that prescribes `m.per_member` in an encrypted room produces key
    /// to-device traffic to the other member.
    #[tokio::test]
    async fn slot_encryption_turns_key_distribution_on() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = RtcSessionManager::with_command_sender(sender.clone());

        manager.on_room_encryption_received(ROOM_ID, true).await;
        manager
            .on_room_slots_received(
                ROOM_ID,
                vec![slot_event(
                    "m.call#ROOM",
                    r#"{ "status": "open",
                         "application": { "type": "m.call" },
                         "encryption": { "type": "m.per_member" } }"#,
                )],
            )
            .await;

        // Local config asks for NO keys; the slot must override it upward.
        join_and_admit_a_peer(&mut manager, false).await;

        assert!(
            !sender.to_device_messages.lock().unwrap().is_empty(),
            "keys should be distributed when the slot prescribes a mechanism"
        );
    }

    /// Conversely, MSC4143 forbids RTC encryption in an unencrypted room, so no
    /// keys are distributed there however the client is configured.
    #[tokio::test]
    async fn absent_slot_encryption_turns_key_distribution_off() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = RtcSessionManager::with_command_sender(sender.clone());

        manager.on_room_encryption_received(ROOM_ID, false).await;
        manager
            .on_room_slots_received(ROOM_ID, vec![open_call_slot()])
            .await;

        // Local config asks for keys; the slot must override it downward.
        join_and_admit_a_peer(&mut manager, true).await;

        assert!(
            sender.to_device_messages.lock().unwrap().is_empty(),
            "no keys should be distributed when the slot prescribes no mechanism"
        );
    }

    /// Joins as alice with `local_manage_media_keys` as the caller's own
    /// preference, then lets bob in so a membership change triggers key
    /// distribution (if it is enabled at all).
    async fn join_and_admit_a_peer(
        manager: &mut RtcSessionManager<crate::commands::MockCommandSender>,
        local_manage_media_keys: bool,
    ) {
        let mut params = JoinSessionParams::new(
            "@alice:example.org".to_owned(),
            "ALICEDEV".to_owned(),
            ROOM_ID.to_owned(),
            "m.call#ROOM".to_owned(),
            "m.call".to_owned(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://example.com/jwt".to_owned(),
            }),
        );
        params.membership_id = Some("alice-a".to_owned());
        params.encryption_config = Some(EncryptionConfig {
            manage_media_keys: local_manage_media_keys,
            ..EncryptionConfig::default()
        });
        manager.join(params).await.expect("join should succeed");

        let bob = RawStickyEvent {
            origin: EventOrigin::encrypted(Some("BOBDEV".to_owned())),
            ..joined_event("@bob:example.org", "m.call#ROOM", "bob-a")
        };
        manager
            .sticky_update_for_room(
                ROOM_ID,
                StickyEventsUpdate {
                    added: vec![bob],
                    updated: Vec::new(),
                    removed: Vec::new(),
                },
            )
            .await
            .unwrap();
    }

    /// A member that only receives — a recorder, say — is a valid participant
    /// under MSC4143: `transports` carries no REQUIRED marker, so publishing
    /// nothing is a legitimate choice rather than a broken join.
    #[tokio::test]
    async fn a_receive_only_member_joins_and_publishes_nothing() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = RtcSessionManager::with_command_sender(sender.clone());

        let params = JoinSessionParams::with_transport_intent(
            "@recorder:example.org".to_owned(),
            "RECORDERDEV".to_owned(),
            ROOM_ID.to_owned(),
            "m.call#ROOM".to_owned(),
            "m.call".to_owned(),
            TransportIntent::ReceiveOnly {
                can_subscribe: vec!["livekit".to_owned()],
            },
        );
        manager.join(params).await.expect("join should succeed");

        let sticky = sender.sticky_events.lock().unwrap();
        let (_, _, content, _) = sticky.first().expect("a join should have been sent");

        // Nothing published, but peers are still told what it can receive on,
        // so they pick a transport it can actually hear.
        assert!(content.pointer("/transports/published/0").is_none());
        assert_eq!(
            content
                .pointer("/transports/can_subscribe/0")
                .and_then(|v| v.as_str()),
            Some("livekit")
        );
        assert_eq!(
            content
                .pointer("/member/membership")
                .and_then(|v| v.as_str()),
            Some("join")
        );
    }

    /// Stating nothing at all is legal too; the object is then omitted entirely
    /// rather than emitted empty.
    #[tokio::test]
    async fn a_receive_only_member_with_no_cue_omits_transports() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = RtcSessionManager::with_command_sender(sender.clone());

        let params = JoinSessionParams::with_transport_intent(
            "@recorder:example.org".to_owned(),
            "RECORDERDEV".to_owned(),
            ROOM_ID.to_owned(),
            "m.call#ROOM".to_owned(),
            "m.call".to_owned(),
            TransportIntent::ReceiveOnly {
                can_subscribe: Vec::new(),
            },
        );
        manager.join(params).await.expect("join should succeed");

        let sticky = sender.sticky_events.lock().unwrap();
        let (_, _, content, _) = sticky.first().expect("a join should have been sent");
        assert!(content.get("transports").is_none());
    }

    /// A publishing member advertises the transport the application chose, and
    /// declares it can receive on that type too.
    #[tokio::test]
    async fn a_publishing_member_advertises_its_transport() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = RtcSessionManager::with_command_sender(sender.clone());

        let params = JoinSessionParams::new(
            "@alice:example.org".to_owned(),
            "ALICEDEV".to_owned(),
            ROOM_ID.to_owned(),
            "m.call#ROOM".to_owned(),
            "m.call".to_owned(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://sfu.example.com/jwt".to_owned(),
            }),
        );
        manager.join(params).await.expect("join should succeed");

        let sticky = sender.sticky_events.lock().unwrap();
        let (_, _, content, _) = sticky.first().expect("a join should have been sent");
        assert_eq!(
            content
                .pointer("/transports/published/0/livekit_service_url")
                .and_then(|v| v.as_str()),
            Some("https://sfu.example.com/jwt")
        );
        assert_eq!(
            content
                .pointer("/transports/can_subscribe/0")
                .and_then(|v| v.as_str()),
            Some("livekit")
        );
    }

    #[test]
    fn joined_event_with_livekit_transport_is_parsed_correctly() {
        use crate::transport::{RawRtcTransport, RtcTransport};
        use std::collections::BTreeMap;

        let mut extra_fields = BTreeMap::new();
        extra_fields.insert(
            "livekit_service_url".to_owned(),
            serde_json::Value::String("https://example.com/livekit/jwt".to_owned()),
        );

        let event = RawStickyEvent {
            room_id: ROOM_ID.to_owned(),
            sender: "@alice:example.org".to_owned(),
            origin: EventOrigin::default(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: "m.call#ROOM".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application: ApplicationInfo {
                    application_type: Some("m.call".to_owned()),
                    extra: std::collections::BTreeMap::new(),
                },
                member: MemberInfo {
                    id: Some("alice-device-a".to_owned()),
                    membership: Some(Membership::Join),
                },
                transports: Some(MemberTransports::publishing(RawRtcTransport {
                    transport_type: "livekit".to_owned(),
                    extra_fields,
                })),
                leave_reason: None,
            },
        };

        let membership_event = event.try_into_call_membership_event().unwrap();

        match membership_event {
            CallMembershipEvent::Joined(joined) => {
                assert_eq!(joined.transports.len(), 1);
                match &joined.transports[0] {
                    RtcTransport::LiveKit(livekit) => {
                        assert_eq!(
                            livekit.livekit_service_url,
                            "https://example.com/livekit/jwt"
                        );
                    }
                    RtcTransport::Unsupported(_) => panic!("Expected LiveKit transport"),
                }
            }
            CallMembershipEvent::Left(_) => panic!("Expected Joined membership"),
        }
    }

    #[test]
    fn joined_event_with_unknown_transport_is_preserved_as_unsupported() {
        use crate::transport::{RawRtcTransport, RtcTransport};
        use std::collections::BTreeMap;

        let mut extra_fields = BTreeMap::new();
        extra_fields.insert(
            "custom_field".to_owned(),
            serde_json::Value::String("custom_value".to_owned()),
        );

        let event = RawStickyEvent {
            room_id: ROOM_ID.to_owned(),
            sender: "@alice:example.org".to_owned(),
            origin: EventOrigin::default(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: "m.call#ROOM".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application: ApplicationInfo {
                    application_type: Some("m.call".to_owned()),
                    extra: std::collections::BTreeMap::new(),
                },
                member: MemberInfo {
                    id: Some("alice-device-a".to_owned()),
                    membership: Some(Membership::Join),
                },
                transports: Some(MemberTransports::publishing(RawRtcTransport {
                    transport_type: "unknown_transport".to_owned(),
                    extra_fields,
                })),
                leave_reason: None,
            },
        };

        let membership_event = event.try_into_call_membership_event().unwrap();

        match membership_event {
            CallMembershipEvent::Joined(joined) => {
                assert_eq!(joined.transports.len(), 1);
                match &joined.transports[0] {
                    RtcTransport::Unsupported(unsupported) => {
                        assert_eq!(unsupported.transport_type, "unknown_transport");
                        assert!(unsupported.extra_fields.contains_key("custom_field"));
                    }
                    RtcTransport::LiveKit(_) => panic!("Expected Unsupported transport"),
                }
            }
            CallMembershipEvent::Left(_) => panic!("Expected Joined membership"),
        }
    }
}
