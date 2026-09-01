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
mod maybe_send;
mod notification;
mod own_membership;
mod session;
mod slot;
mod transport;
mod wire;

pub use commands::{RtcCommandSender, ToDeviceDelivery, ToDeviceRecipient};
pub use encryption::types::{
    EncryptionConfig, InboundEncryptionKey, KeyMaterialSignal, KeyOrigin, KeyRejection,
    OutboundEncryptionKey, OutdatedKeyFilter, ParticipantDeviceInfo, ReceivedEncryptionKey,
};
pub use encryption::{
    DiscardedKey, EncryptionKeySignalHandler, EncryptionManager, KEY_MESSAGE_TYPE, RtcClock,
    RtcIdentityMapper,
};
pub use error::{CommandError, JoinError, LeaveError};
pub use event::{
    EventConversionError, EventOrigin, RawStickyEvent, RawStickyEventContent, RawStickyEventUpdate,
    StickyEventsUpdate,
};
pub use join::{JoinSessionParams, LeaveSessionParams, TransportIntent, generate_member_id};
pub use manager::RtcSessionManager;
pub use maybe_send::MaybeSend;
pub use notification::{
    DEFAULT_RING_LIFETIME_MS, MAX_RING_LIFETIME_MS, Mentions, NOTIFICATION_EVENT_TYPE,
    NotificationType, NotifyConfig, build_notification_content, notification_sticky_duration_ms,
};
pub use own_membership::{
    DelayedLeaveSupport, KeepAliveInfo, MembershipTimings, OwnMembershipMachine,
    OwnMembershipState, transport_to_json,
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
                created_ts: None,
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

    /// An open slot prescribing MSC4143 per-member media keys, so joining it in
    /// an encrypted room turns key distribution on.
    fn encrypted_call_slot() -> RawSlotEvent {
        slot_event(
            "m.call#ROOM",
            r#"{ "status": "open",
                 "application": { "type": "m.call" },
                 "encryption": { "type": "m.per_member" } }"#,
        )
    }

    /// Joins as alice under an explicit `member_id`, so a leave/rejoin pair can
    /// be told apart in the assertions.
    async fn join_as(
        manager: &mut RtcSessionManager<crate::commands::MockCommandSender>,
        member_id: &str,
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
        params.membership_id = Some(member_id.to_owned());
        manager.join(params).await.expect("join should succeed");
    }

    /// Feeds the current sticky state containing one peer, the way a host does.
    async fn admit_peer(
        manager: &mut RtcSessionManager<crate::commands::MockCommandSender>,
        user_id: &str,
        device_id: &str,
        member_id: &str,
    ) {
        let event = RawStickyEvent {
            origin: EventOrigin::encrypted(Some(device_id.to_owned())),
            ..joined_event(user_id, "m.call#ROOM", member_id)
        };
        manager
            .set_current_sticky_state(ROOM_ID, vec![event])
            .await
            .unwrap();
    }

    async fn leave_call(manager: &mut RtcSessionManager<crate::commands::MockCommandSender>) {
        manager
            .leave(
                ROOM_ID.to_owned(),
                "m.call#ROOM".to_owned(),
                LeaveSessionParams::new(),
            )
            .await
            .expect("leave should succeed");
    }

    fn roster(
        manager: &RtcSessionManager<crate::commands::MockCommandSender>,
    ) -> Vec<crate::session::JoinedMembership> {
        manager
            .subscribe_membership_snapshots(ROOM_ID, "m.call#ROOM")
            .expect("the session should exist")
            .borrow()
            .clone()
    }

    async fn encrypted_call_manager(
        sender: Arc<crate::commands::MockCommandSender>,
    ) -> RtcSessionManager<crate::commands::MockCommandSender> {
        let mut manager = RtcSessionManager::with_command_sender(sender);
        manager.on_room_encryption_received(ROOM_ID, true).await;
        manager
            .on_room_slots_received(ROOM_ID, vec![encrypted_call_slot()])
            .await;
        manager
    }

    /// An MSC4354 sticky entry expires when its owner stops refreshing it — a
    /// crashed client — and the lapse produces no event at all. So the current
    /// state simply arrives smaller, and the member has to go: this call
    /// replaces rather than merges. Merging kept them in the call for good and
    /// pushed expiry detection onto every host.
    #[tokio::test]
    async fn a_member_whose_sticky_entry_expired_is_dropped() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();
        let alice = joined_event("@alice:example.org", "m.call#ROOM", "alice-a");
        let bob = joined_event("@bob:example.org", "m.call#ROOM", "bob-a");

        manager
            .set_current_sticky_state(ROOM_ID, vec![alice.clone(), bob])
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(2));

        // Bob's entry lapsed: no leave event, he is simply absent now.
        manager
            .set_current_sticky_state(ROOM_ID, vec![alice])
            .await
            .unwrap();
        assert_eq!(
            manager.member_count(ROOM_ID, "m.call#ROOM"),
            Some(1),
            "an expired entry must leave the call, with no leave event to feed in"
        );

        // And an empty state empties the room, rather than being a no-op.
        manager
            .set_current_sticky_state(ROOM_ID, Vec::new())
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(0));
    }

    /// A slot whose last member expired contributes no events at all, so it
    /// vanishes from the payload entirely. It still has to be cleared, or the
    /// replace has a hole exactly where the ghost would be.
    #[tokio::test]
    async fn a_slot_missing_from_the_current_state_is_cleared_too() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();
        let in_call = joined_event("@alice:example.org", "m.call#ROOM", "alice-a");
        let in_other = joined_event("@bob:example.org", "m.call#OTHER", "bob-a");

        manager
            .set_current_sticky_state(ROOM_ID, vec![in_call.clone(), in_other])
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#OTHER"), Some(1));

        // Only the first slot is represented now; the second must empty.
        manager
            .set_current_sticky_state(ROOM_ID, vec![in_call])
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));
        assert_eq!(
            manager.member_count(ROOM_ID, "m.call#OTHER"),
            Some(0),
            "a slot absent from the current state is empty, not untouched"
        );
    }

    /// A second call in the same process must distribute a key just as the first
    /// one did.
    ///
    /// The session survives `leave()` on purpose — it is keyed by `(room, slot)`
    /// and a host may still want the roster after hanging up — so the second
    /// join starts with the first call's roster already in place. Nothing else
    /// changes: the incumbent's membership is byte-identical across our leave and
    /// rejoin, so there is no roster transition for the second call to ride on.
    /// If distribution only ever happens on a membership *change*, the second
    /// call silently never distributes and the incumbent is left at
    /// `MISSING_KEY` — which is what an Android integration hit, four runs out of
    /// four.
    #[tokio::test]
    async fn a_rejoin_in_the_same_process_distributes_a_key_to_the_incumbent() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = encrypted_call_manager(sender.clone()).await;

        // First call: bob arrives after we joined, so the roster changes while we
        // hold a key and distribution is triggered.
        join_as(&mut manager, "alice-a").await;
        admit_peer(&mut manager, "@bob:example.org", "BOBDEV", "bob-a").await;
        assert!(
            !sender
                .to_device_messages_for("@bob:example.org", "BOBDEV")
                .is_empty(),
            "first call should have distributed a key to the incumbent"
        );

        leave_call(&mut manager).await;
        sender.to_device_messages.lock().unwrap().clear();

        // Second call: deliberately no further sticky events. Bob has not moved.
        join_as(&mut manager, "alice-b").await;

        let sent = sender.to_device_messages_for("@bob:example.org", "BOBDEV");
        assert!(
            !sent.is_empty(),
            "the second call in the same process distributed no key to the incumbent, so its \
             media cannot be decrypted"
        );
        assert_eq!(
            sent.len(),
            1,
            "the incumbent should be handed the key once, not once per code path \
             that noticed the join"
        );
        let (_, content) = &sent[0];
        assert_eq!(
            content.pointer("/member_id").and_then(|v| v.as_str()),
            Some("alice-b"),
            "the key must be advertised under the member id of the current join"
        );
        assert_eq!(
            content.pointer("/media_key/index").and_then(|v| v.as_u64()),
            Some(0),
            "a fresh join starts a fresh key index"
        );
    }

    /// Joins as alice, asking for an MSC4075 notification.
    async fn join_and_notify(
        manager: &mut RtcSessionManager<crate::commands::MockCommandSender>,
        member_id: &str,
        notify: NotifyConfig,
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
        params.membership_id = Some(member_id.to_owned());
        params.notify = Some(notify);
        manager.join(params).await.expect("join should succeed");
    }

    /// Every sticky send of the notification type, as `(content, duration_ms)`.
    fn notifications_sent(
        sender: &crate::commands::MockCommandSender,
    ) -> Vec<(serde_json::Value, u64)> {
        sender
            .sticky_events
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, event_type, _, _)| event_type == NOTIFICATION_EVENT_TYPE)
            .map(|(_, _, content, duration_ms)| (content.clone(), *duration_ms))
            .collect()
    }

    /// Starting a call is what summons the room, and MSC4075 ties the
    /// notification to the membership that justifies it — so the event id the
    /// join's sticky send reported has to come back out as the relation target.
    #[tokio::test]
    async fn starting_a_call_notifies_the_room() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = encrypted_call_manager(sender.clone()).await;

        let mut notify = NotifyConfig::ring();
        notify.intent = Some("video".to_owned());
        join_and_notify(&mut manager, "alice-a", notify).await;

        let sent = notifications_sent(&sender);
        assert_eq!(sent.len(), 1, "the call starter should notify exactly once");
        let (content, duration_ms) = &sent[0];

        let member_event_id = sender
            .sticky_events
            .lock()
            .unwrap()
            .iter()
            .position(|(_, event_type, _, _)| event_type == "m.rtc.member")
            .map(|index| format!("$sticky-{}", index + 1))
            .expect("the join sends a membership event");
        assert_eq!(
            content.pointer("/m.relates_to/event_id").unwrap(),
            &serde_json::json!(member_event_id),
            "the relation must name our own membership event"
        );
        assert_eq!(
            content.pointer("/m.relates_to/rel_type").unwrap(),
            "m.reference"
        );
        assert_eq!(content.pointer("/application/type").unwrap(), "m.call");
        assert_eq!(
            content.pointer("/application/notification_type").unwrap(),
            "ring"
        );
        assert_eq!(
            content.pointer("/application/m.call.intent").unwrap(),
            "video"
        );
        assert_eq!(
            content.pointer("/application/device_id").unwrap(),
            "ALICEDEV"
        );
        assert_eq!(
            *duration_ms,
            2 * DEFAULT_RING_LIFETIME_MS,
            "MSC4075: the sticky entry must outlive the ring so acknowledgements can extend it"
        );
    }

    /// "Is anyone already here?" must not be answered `yes` by our own
    /// membership.
    ///
    /// The host feeds the room's whole sticky map, and once the homeserver has
    /// echoed our membership back that map contains *us*. A count that includes
    /// it concludes somebody else started the call and stays silent — so the
    /// caller hits "call" and nobody's phone rings.
    #[tokio::test]
    async fn our_own_membership_does_not_count_as_somebody_else() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = encrypted_call_manager(sender.clone()).await;

        // The echo of our own membership, under the very id we are about to
        // join with.
        admit_peer(&mut manager, "@alice:example.org", "ALICEDEV", "alice-a").await;
        join_and_notify(&mut manager, "alice-a", NotifyConfig::ring()).await;

        assert_eq!(
            notifications_sent(&sender).len(),
            1,
            "the only membership in the session is our own, so we are the one starting the call"
        );
    }

    /// A participation of ours from an earlier call in this process must not
    /// count either — including when the host reported no sending device for
    /// it.
    ///
    /// A session outlives `leave()` and keeps its candidates, so the previous
    /// call's membership is still there on a rejoin. It is normally dropped as
    /// `SupersededOwnParticipation`, but that rule needs the sending device, and
    /// an unencrypted room supplies none. Left in, it silences every subsequent
    /// call in the process until the app restarts.
    #[tokio::test]
    async fn a_stale_participation_of_ours_does_not_count_either() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = RtcSessionManager::with_command_sender(sender.clone());
        manager
            .on_room_slots_received(ROOM_ID, vec![open_call_slot()])
            .await;

        // Our own device, one call ago, with no device reported — exactly what
        // an unencrypted room yields.
        manager
            .set_current_sticky_state(
                ROOM_ID,
                vec![joined_event(
                    "@alice:example.org",
                    "m.call#ROOM",
                    "alice-old",
                )],
            )
            .await
            .unwrap();

        join_and_notify(&mut manager, "alice-new", NotifyConfig::ring()).await;

        assert_eq!(
            notifications_sent(&sender).len(),
            1,
            "the session held nothing but our own previous participation"
        );
    }

    /// The other edge of the same rule: our own user on a *different* device is
    /// an ordinary peer, and one already in the call started it.
    #[tokio::test]
    async fn another_device_of_ours_already_in_the_call_counts() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = encrypted_call_manager(sender.clone()).await;

        admit_peer(
            &mut manager,
            "@alice:example.org",
            "ALICELAPTOP",
            "alice-laptop",
        )
        .await;
        join_and_notify(&mut manager, "alice-a", NotifyConfig::ring()).await;

        assert!(
            notifications_sent(&sender).is_empty(),
            "our laptop was already in the call, so our phone is joining, not starting"
        );
    }

    /// Joining a call someone else started must not ring the room a second
    /// time, even if the host asked for a notification.
    #[tokio::test]
    async fn joining_an_occupied_session_notifies_nobody() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = encrypted_call_manager(sender.clone()).await;

        admit_peer(&mut manager, "@bob:example.org", "BOBDEV", "bob-a").await;
        join_and_notify(&mut manager, "alice-a", NotifyConfig::ring()).await;

        assert!(
            notifications_sent(&sender).is_empty(),
            "bob was already in the session, so he started the call, not us"
        );
    }

    #[tokio::test]
    async fn joining_quietly_notifies_nobody() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = encrypted_call_manager(sender.clone()).await;

        join_as(&mut manager, "alice-a").await;

        assert!(notifications_sent(&sender).is_empty());
    }

    /// MSC4143 requires a fresh `member.id` per join, so the previous
    /// participation of this very device is not a peer — it is us, one call ago.
    /// Leaving it in the roster gives the media layer a phantom member to open a
    /// receive stream for and to expect a key from.
    #[tokio::test]
    async fn a_rejoin_does_not_advertise_the_previous_participation() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = encrypted_call_manager(sender.clone()).await;

        join_as(&mut manager, "alice-a").await;
        // The homeserver echoes our own membership back through the sticky map.
        admit_peer(&mut manager, "@alice:example.org", "ALICEDEV", "alice-a").await;
        leave_call(&mut manager).await;

        join_as(&mut manager, "alice-b").await;

        let member_ids: Vec<_> = roster(&manager)
            .into_iter()
            .map(|membership| membership.member_id)
            .collect();
        assert!(
            !member_ids.iter().any(|id| id == "alice-a"),
            "our superseded participation is still in the roster: {member_ids:?}"
        );
    }

    /// The session outliving a leave is the contract, not an accident: a host
    /// that hangs up may still render "3 people are in this call", and the media
    /// session is torn down separately with no ordering guarantee. Pinned so a
    /// future "just drop the session on leave" refactor has to argue with a test.
    #[tokio::test]
    async fn a_left_session_still_publishes_the_peer_roster() {
        let sender = Arc::new(crate::commands::MockCommandSender::new());
        let mut manager = encrypted_call_manager(sender.clone()).await;

        join_as(&mut manager, "alice-a").await;
        admit_peer(&mut manager, "@bob:example.org", "BOBDEV", "bob-a").await;
        leave_call(&mut manager).await;

        assert_eq!(manager.session_count(), 1, "the session should survive");
        assert!(
            roster(&manager)
                .iter()
                .any(|membership| membership.member_id == "bob-a"),
            "the peer roster should survive our own departure"
        );
        assert_eq!(
            manager.member_count(ROOM_ID, "m.call#ROOM"),
            Some(1),
            "the incumbent is still in the call"
        );
        assert_eq!(
            manager.own_member_id(ROOM_ID, "m.call#ROOM"),
            None,
            "we are no longer joined, so we have no member id"
        );
    }

    /// Restored after fixing the `RtcSessionManager::new()` recursion that used
    /// to overflow the stack (it was misattributed to watch channels).
    #[tokio::test]
    async fn manager_routes_snapshot_and_diff_update_membership() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();
        let joined = joined_event("@alice:example.org", "m.call#ROOM", "alice-device-a");

        manager
            .set_current_sticky_state(ROOM_ID, vec![joined.clone()])
            .await
            .unwrap();

        assert_eq!(manager.session_count(), 1);
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));

        // A departure reaches the core as a leave-shaped sticky replacing the
        // join under the same key — and, once that lapses too, as plain absence.
        let left = left_event("@alice:example.org", "m.call#ROOM", "alice-device-a");
        manager
            .set_current_sticky_state(ROOM_ID, vec![left])
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
            .set_current_sticky_state(ROOM_ID, vec![stable, unstable])
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
            .set_current_sticky_state(ROOM_ID, vec![event])
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
            .set_current_sticky_state(
                ROOM_ID,
                vec![joined_event("@alice:example.org", "m.call#ROOM", "alice-a")],
            )
            .await
            .unwrap();

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(1));
        assert_eq!(manager.slot_state(ROOM_ID, "m.call#ROOM"), None);
    }

    /// Re-applying the same sticky state must publish nothing at all.
    ///
    /// `set_current_sticky_state` rebuilds the candidate set from scratch, and
    /// used to refresh after *each* event in the batch. The roster was therefore
    /// republished on the way up — one member, then two, then three — so the
    /// first publication of every tick looked like everyone but one participant
    /// leaving. The encryption manager believed it and rotated the key, once per
    /// sticky tick per session, each rotation sending to every remaining member.
    /// In a ten-device call that was a rotation every few seconds; the cost is
    /// quadratic in participants.
    ///
    /// The sticky bridge re-sends the full live set on every tick, so "identical
    /// input publishes nothing" is the property that matters.
    #[tokio::test]
    async fn re_applying_the_same_sticky_state_publishes_nothing() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();
        let members = || {
            vec![
                joined_event("@alice:example.org", "m.call#ROOM", "alice-a"),
                joined_event("@bob:example.org", "m.call#ROOM", "bob-a"),
                joined_event("@carol:example.org", "m.call#ROOM", "carol-a"),
            ]
        };

        manager
            .set_current_sticky_state(ROOM_ID, members())
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(3));

        let mut snapshots = manager
            .subscribe_membership_snapshots(ROOM_ID, "m.call#ROOM")
            .expect("the session exists");
        snapshots.borrow_and_update();

        manager
            .set_current_sticky_state(ROOM_ID, members())
            .await
            .unwrap();

        assert!(
            !snapshots.has_changed().unwrap(),
            "an unchanged sticky state must not republish the roster; every \
             republication is a membership diff the encryption manager acts on",
        );
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(3));
    }

    /// MSC4143: a member event only counts as joined against an *open* slot.
    /// Supplying room state with no slot in it means the slot is closed.
    #[tokio::test]
    async fn members_are_left_when_no_slot_is_open() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager
            .set_current_sticky_state(
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
            .set_current_sticky_state(
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
            .set_current_sticky_state(
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
            .set_current_sticky_state(
                ROOM_ID,
                vec![joined_event("@alice:example.org", "m.call#ROOM", "alice-a")],
            )
            .await
            .unwrap();

        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(0));
    }

    /// The way back from "no open slots" to "not my business".
    ///
    /// A room of a MatrixRTC generation older than `m.rtc.slot` contains none, so
    /// a host that fed slot state before learning that must be able to take it
    /// back — otherwise every member of that room, itself included, stays
    /// projected out for the rest of the process.
    #[tokio::test]
    async fn forgetting_a_room_s_slots_stops_the_condition_being_enforced() {
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        // No slot in the room: everyone is projected out.
        manager.on_room_slots_received(ROOM_ID, Vec::new()).await;
        manager
            .set_current_sticky_state(
                ROOM_ID,
                vec![joined_event("@alice:example.org", "m.call#ROOM", "alice-a")],
            )
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#ROOM"), Some(0));

        manager.forget_room_slots(ROOM_ID).await;
        assert_eq!(
            manager.member_count(ROOM_ID, "m.call#ROOM"),
            Some(1),
            "an unenforced condition must not keep a live member out",
        );
        assert_eq!(
            manager.slot_state(ROOM_ID, "m.call#ROOM"),
            None,
            "the slot is unknown again, not open",
        );

        // And a session created afterwards is born unenforced too, rather than
        // inheriting the forgotten "no slots".
        manager
            .set_current_sticky_state(
                ROOM_ID,
                vec![
                    joined_event("@alice:example.org", "m.call#ROOM", "alice-a"),
                    joined_event("@bob:example.org", "m.call#OTHER", "bob-a"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM_ID, "m.call#OTHER"), Some(1));
    }

    /// Forgetting one room's slots leaves every other room's alone.
    #[tokio::test]
    async fn forgetting_slots_is_scoped_to_its_room() {
        const OTHER_ROOM: &str = "!other:example.org";
        let mut manager: RtcSessionManager<NoopCommandSender> = RtcSessionManager::new();

        manager
            .on_room_slots_received(ROOM_ID, vec![open_call_slot()])
            .await;
        manager
            .on_room_slots_received(
                OTHER_ROOM,
                vec![RawSlotEvent {
                    room_id: OTHER_ROOM.to_owned(),
                    ..open_call_slot()
                }],
            )
            .await;

        manager.forget_room_slots(ROOM_ID).await;

        assert_eq!(manager.slot_state(ROOM_ID, "m.call#ROOM"), None);
        assert!(
            manager
                .slot_state(OTHER_ROOM, "m.call#ROOM")
                .is_some_and(|state| state.is_open()),
        );
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
            .set_current_sticky_state(
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
            .set_current_sticky_state(ROOM_ID, vec![encrypted, cleartext])
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
            .set_current_sticky_state(ROOM_ID, vec![cleartext])
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
            .set_current_sticky_state(
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
            .set_current_sticky_state(ROOM_ID, vec![bob])
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
                created_ts: None,
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
                created_ts: None,
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
