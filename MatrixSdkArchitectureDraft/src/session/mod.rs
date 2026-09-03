//! Pure projection of Matrix events into session state.
//!
//! The session is the **single converter** from raw Matrix events to the
//! [`Member`] representation. Every event type is converted in place, in
//! `session::convert` (one file per generation):
//!
//! | event type | converted to |
//! |---|---|
//! | `m.rtc.member` (sticky; modern + 2025 dialect) | member candidate |
//! | `org.matrix.msc3401.call.member` (state; only with `StateEvents` compat) | member candidate, one per event |
//! | `m.rtc.slot` (state) | slot condition |
//! | `m.room.member` (state) | room-membership condition |
//! | `m.room.encryption` (state) | encryption condition |
//!
//! Unknown types are ignored; room-state conditions stay unenforced until
//! the first event of their type arrives (or the seed supplies them).
//!
//! The session consumes **raw** events, so it runs its own MSC4354 sticky
//! map (`sticky.rs`: expiry, conflict resolution, removals) following the
//! rules matrix-rust-sdk uses — which makes the static path order-independent.
//!
//! Two entry points share that logic:
//!
//! - [`Session`] — **live**: constructed with the [`RoomEventsDriver`]
//!   slice, it seeds itself (`read_state` / `read_events`) and consumes the
//!   driver's streams; consumers read it only through [`SessionSnapshot`]s
//!   (current value or `watch` subscription).
//! - [`compute_sessions_from_events`] — **static**: a pure function over
//!   already-synced events returning plain [`SessionSnapshot`]s. Nothing
//!   subscribes, nothing updates — you need to pass a driver (i.e. build a
//!   `Session`) for that.
//!
//! [`RoomEventsDriver`]: crate::driver::RoomEventsDriver

pub(crate) mod convert;
pub(crate) mod dispatch;
mod live;
pub(crate) mod slot;
pub(crate) mod state;
pub(crate) mod sticky;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test_support;

pub use convert::LEGACY_SLOT_ID;
pub use live::Session;

use crate::executor;
use crate::types::{Member, RawMatrixEvent, RtcTransport};
use state::RoomState;
use std::collections::HashMap;

/// Pre-2026 Element Call interop, selected per call.
///
/// Consumed in two places: the session's *read* side (`StateEvents` enables
/// the MSC3401 converter; the permissive 2025-dialect read is always on) and
/// `own_membership`'s *write* side (the dialect our own events are rendered
/// in — the opt-in half, since it changes what other clients see).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum ElementCallCompat {
    #[default]
    Off,
    /// 2025 format: already MSC4354-sticky, different member-content fields.
    StickyEvents,
    /// Pre-MSC4354: `org.matrix.msc3401.call.member` room state,
    /// `{user}:{device}` identities, `/sfu/get` tokens. Not additive — such
    /// a call is visible to that generation and nobody else.
    StateEvents,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SessionConfig {
    pub compat: ElementCallCompat,
}

/// Resolved `m.rtc.slot` state, per MSC4143: open requires `status: "open"`
/// plus an application whose type agrees with the state key; anything else
/// (missing application, unknown status, empty content) is closed.
#[derive(Clone, Debug, PartialEq)]
pub enum SlotState {
    Open(OpenSlot),
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OpenSlot {
    pub application_type: String,
    /// What the slot declared.
    pub encryption: Option<SlotEncryption>,
    /// What was negotiated against the room's encryption state (`None` when
    /// the declaration was dropped, e.g. mechanism declared in an
    /// unencrypted room).
    pub mechanism: Option<EncryptionMechanism>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SlotEncryption {
    pub mechanism_type: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EncryptionMechanism {
    /// `m.per_member` — the only implemented mechanism.
    PerMember,
    /// Closes the slot in an encrypted room (we cannot take part compliantly).
    Unsupported(String),
}

/// Why a candidate member event is not in the joined projection.
///
/// Two conditions are scoped by the candidate's generation:
/// `SlotClosed` never applies to MSC3401 candidates (that generation has no
/// slot concept), and `UnencryptedInEncryptedRoom` never applies to them
/// either (state events are unencrypted by nature; MSC4143 requires
/// encryption only of *sticky* member events).
///
/// `Expired` appears exactly once: in the snapshot published by the
/// transition that dropped the candidate. The static path never reports it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum JoinExclusionReason {
    SlotClosed,
    UnencryptedInEncryptedRoom,
    SenderNotInRoom,
    Expired,
}

/// The one read surface of a session — a plain, cloneable value carrying
/// the joined projection plus its metadata and conveniences. Produced live
/// by [`Session`] and statically by [`compute_sessions_from_events`]; the
/// matrix-rust-sdk `room_info` computation populates its fields from these
/// on every room update.
///
/// Published only on change (`PartialEq`), so consumers may treat every
/// `changed()` as a real roster/slot change. `members` is sorted by
/// `(user_id, member_id)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionSnapshot {
    pub room_id: String,
    pub slot_id: String,
    /// The joined projection.
    pub members: Vec<Member>,
    /// Union of all members' published transports.
    pub transports: Vec<RtcTransport>,
    /// Candidates excluded from the projection, with reasons — the
    /// load-bearing diagnostics (a member silently vanishing is the hardest
    /// failure to debug from outside).
    pub excluded_candidates: Vec<(Member, JoinExclusionReason)>,
    /// `None` while no slot state was supplied (condition unenforced); always
    /// `None` for the legacy slot.
    pub slot_state: Option<SlotState>,
    /// The slot-prescribed encryption decision (overrides local config at
    /// join time); `None` while unknown.
    pub negotiated_encryption: Option<bool>,
    /// `origin_server_ts` of the earliest joined membership, while active.
    pub start_ts: Option<u64>,
    /// From the open slot (or the memberships' application).
    pub application_type: Option<String>,
    /// `true` once the live session finished seeding from the driver's
    /// `read_*` calls (even after read failures); always `true` on the static
    /// path. `own_membership` mirrors it as `has_fetched_initial_member_list`.
    pub seeded: bool,
}

impl SessionSnapshot {
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub fn is_active(&self) -> bool {
        !self.members.is_empty()
    }
}

/// Static, cheap session computation for room-list / room-header info —
/// call it on every room change, no manager, no driver, nothing live.
///
/// Takes *all* of a room's relevant events in one slice (sticky and state;
/// the same dispatch a live [`Session`] runs; many rooms at once is fine) and
/// groups by `(room_id, slot_id)` — MSC3401 candidates land in
/// [`LEGACY_SLOT_ID`]. Returns one snapshot per `(room, slot)` that has either
/// a slot state event or at least one joined candidate, so a closed and
/// empty slot is still reported (room_info can tell "closed" from "never had
/// a call"). Sorted by `(room_id, slot_id)`.
///
/// Events without a `room_id` are skipped; malformed events never poison the
/// rest of the batch. The receive time for MSC4354 expiry is the call time,
/// and origins that are `Unknown` leave the encrypted-room rule unenforced.
pub fn compute_sessions_from_events(
    events: &[RawMatrixEvent],
    config: &SessionConfig,
) -> Vec<SessionSnapshot> {
    let now = executor::now_ms();
    let mut rooms: HashMap<String, RoomState> = HashMap::new();
    for event in events {
        let Some(room_id) = dispatch::room_id(event) else {
            log::trace!("compute_sessions_from_events: event without a room_id skipped");
            continue;
        };
        let ingest = dispatch::classify(event, config, now);
        rooms
            .entry(room_id.to_owned())
            .or_insert_with(|| RoomState::for_static(room_id))
            .ingest(ingest, now);
    }
    let mut snapshots: Vec<SessionSnapshot> = rooms
        .values()
        .flat_map(|state| {
            state
                .slot_ids()
                .into_iter()
                .map(|slot_id| SessionSnapshot {
                    seeded: true,
                    ..state.project(&slot_id)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    snapshots.sort_by(|a, b| (&a.room_id, &a.slot_id).cmp(&(&b.room_id, &b.slot_id)));
    log::debug!(
        "compute_sessions_from_events: {} event(s) in {} room(s) -> {} session(s)",
        events.len(),
        rooms.len(),
        snapshots.len()
    );
    snapshots
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::test_support::*;
    use super::*;
    use crate::types::EventOrigin;
    use serde_json::{Value, json};

    fn now() -> u64 {
        executor::now_ms()
    }

    fn compute(events: Vec<RawMatrixEvent>, compat: ElementCallCompat) -> Vec<SessionSnapshot> {
        compute_sessions_from_events(&events, &SessionConfig { compat })
    }

    fn keys(snapshots: &[SessionSnapshot]) -> Vec<(String, String, usize)> {
        snapshots
            .iter()
            .map(|s| (s.room_id.clone(), s.slot_id.clone(), s.member_count()))
            .collect()
    }

    fn encrypted(device: &str) -> EventOrigin {
        EventOrigin::Encrypted {
            sender_device_id: Some(device.to_owned()),
        }
    }

    fn in_slot(mut event: Value, slot_id: &str) -> Value {
        event["content"]["slot_id"] = json!(slot_id);
        event
    }

    #[test]
    fn groups_by_room_and_slot() {
        let t = now();
        let events = vec![
            raw(member_join_event("@a:x", "m-a", t), encrypted("A")),
            raw(
                in_slot(member_join_event("@b:x", "m-b", t), "m.whiteboard#ROOM"),
                encrypted("B"),
            ),
            raw(
                in_room(member_join_event("@c:x", "m-c", t), "!other:x"),
                encrypted("C"),
            ),
            raw(
                in_room(
                    in_slot(member_join_event("@d:x", "m-d", t), "m.whiteboard#ROOM"),
                    "!other:x",
                ),
                encrypted("D"),
            ),
            raw(
                in_room(member_join_event("@e:x", "m-e", t), "!other:x"),
                encrypted("E"),
            ),
            // MSC3401 lands in the legacy slot only with StateEvents.
            raw(
                msc3401_member_event("@f:x", "DEV", t, t),
                EventOrigin::Unknown,
            ),
        ];
        let snapshots = compute(events.clone(), ElementCallCompat::Off);
        assert_eq!(
            keys(&snapshots),
            vec![
                ("!other:x".into(), "m.call#ROOM".into(), 2),
                ("!other:x".into(), "m.whiteboard#ROOM".into(), 1),
                (ROOM_ID.into(), "m.call#ROOM".into(), 1),
                (ROOM_ID.into(), "m.whiteboard#ROOM".into(), 1),
            ]
        );
        let snapshots = compute(events, ElementCallCompat::StateEvents);
        assert_eq!(snapshots.len(), 5);
        let legacy = snapshots
            .iter()
            .find(|s| s.slot_id == LEGACY_SLOT_ID)
            .unwrap();
        assert_eq!(legacy.room_id, ROOM_ID);
        assert_eq!(legacy.members[0].member_id, "@f:x:DEV");
        assert_eq!(
            legacy.members[0].transports.published[0].properties["livekit_service_url"],
            LK_SERVICE_URL
        );
    }

    #[test]
    fn a_closed_empty_slot_is_reported_and_a_room_with_neither_is_not() {
        let t = now();
        let snapshots = compute(
            vec![raw(slot_closed_event(t), EventOrigin::Unknown)],
            ElementCallCompat::Off,
        );
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].slot_state, Some(SlotState::Closed));
        assert!(!snapshots[0].is_active());
        assert_eq!(snapshots[0].negotiated_encryption, Some(false));

        let nothing = compute(
            vec![
                raw(room_encryption_event(t), EventOrigin::Unknown),
                raw(room_member_event("@a:x", "join", t), EventOrigin::Unknown),
                raw(member_leave_event("@a:x", "m-a", t), encrypted("A")),
            ],
            ElementCallCompat::Off,
        );
        assert!(nothing.is_empty());
    }

    #[test]
    fn malformed_events_never_poison_the_batch() {
        let t = now();
        let snapshots = compute(
            vec![
                raw(
                    json!({ "type": "m.rtc.member", "room_id": ROOM_ID }),
                    EventOrigin::Unknown,
                ),
                raw(
                    json!({ "type": "m.rtc.member", "room_id": ROOM_ID, "sender": "@x:x", "content": "nope" }),
                    EventOrigin::Unknown,
                ),
                raw(
                    json!({ "type": "m.rtc.slot", "room_id": ROOM_ID, "sender": "@x:x", "content": { "status": 7 } }),
                    EventOrigin::Unknown,
                ),
                raw(json!(42), EventOrigin::Unknown),
                raw(
                    json!({ "type": "m.rtc.member", "sender": "@x:x", "content": {} }),
                    EventOrigin::Unknown,
                ),
                raw(member_join_event("@a:x", "m-a", t), encrypted("A")),
                raw(
                    json!({ "type": "com.example.thing", "room_id": ROOM_ID, "sender": "@x:x", "content": {} }),
                    EventOrigin::Unknown,
                ),
            ],
            ElementCallCompat::Off,
        );
        assert_eq!(keys(&snapshots), vec![(ROOM_ID.into(), SLOT_ID.into(), 1)]);
    }

    #[test]
    fn order_independent_and_idempotent() {
        let t = now();
        let events = vec![
            raw(member_join_event("@a:x", "m-a", t - 1_000), encrypted("A")),
            raw(member_join_event("@a:x", "m-a", t - 500), encrypted("A")), // refresh
            raw(member_join_event("@b:x", "m-b", t - 2_000), encrypted("B")),
            raw(
                member_bare_leave_event("@b:x", "m-b", t - 1_000),
                encrypted("B"),
            ),
            raw(member_join_event("@c:x", "m-c", t), EventOrigin::Cleartext),
            raw(room_encryption_event(t), EventOrigin::Unknown),
            raw(
                slot_event(
                    SLOT_ID,
                    json!({ "status": "open", "application": { "type": "m.call" }, "encryption": { "type": "m.per_member" } }),
                    t,
                ),
                EventOrigin::Unknown,
            ),
            raw(room_member_event("@a:x", "join", t), EventOrigin::Unknown),
            raw(room_member_event("@c:x", "join", t), EventOrigin::Unknown),
            raw(
                msc3401_member_event("@d:x", "DEV", t - 3_000, t - 3_000),
                EventOrigin::Unknown,
            ),
        ];
        let reference = compute(events.clone(), ElementCallCompat::StateEvents);
        assert_eq!(
            compute(events.clone(), ElementCallCompat::StateEvents),
            reference,
            "idempotent"
        );
        assert_eq!(reference.len(), 2);
        let main = reference.iter().find(|s| s.slot_id == SLOT_ID).unwrap();
        assert_eq!(
            main.members
                .iter()
                .map(|m| m.member_id.as_str())
                .collect::<Vec<_>>(),
            vec!["m-a"]
        );
        assert_eq!(
            main.excluded_candidates[0].1,
            JoinExclusionReason::UnencryptedInEncryptedRoom
        );
        assert_eq!(main.negotiated_encryption, Some(true));

        let mut seed: u64 = 0x1234_5678_9ABC_DEF0;
        for _ in 0..20 {
            let mut shuffled = events.clone();
            for i in (1..shuffled.len()).rev() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let j = (seed >> 33) as usize % (i + 1);
                shuffled.swap(i, j);
            }
            assert_eq!(compute(shuffled, ElementCallCompat::StateEvents), reference);
        }
    }

    #[test]
    fn unknown_origins_leave_the_encrypted_room_rule_unenforced() {
        let t = now();
        let snapshots = compute(
            vec![
                raw(room_encryption_event(t), EventOrigin::Unknown),
                raw(member_join_event("@a:x", "m-a", t), EventOrigin::Unknown),
            ],
            ElementCallCompat::Off,
        );
        // Documented trade: the member count may overshoot.
        assert_eq!(snapshots[0].member_count(), 1);
    }

    #[test]
    fn a_mock_driver_shaped_join_yields_one_member_with_the_lk_transport() {
        let mut event: Value = serde_json::from_str(MOCK_DRIVER_JOIN_FIXTURE).unwrap();
        event["origin_server_ts"] = json!(now());
        let snapshots = compute(vec![raw(event, encrypted("RDEV"))], ElementCallCompat::Off);
        assert_eq!(snapshots.len(), 1);
        let s = &snapshots[0];
        assert_eq!(s.member_count(), 1);
        assert_eq!(s.members[0].member_id, "m-1");
        assert_eq!(s.transports[0].transport_type, "livekit");
        assert_eq!(
            s.transports[0].properties["livekit_service_url"],
            LK_SERVICE_URL
        );
        assert_eq!(s.slot_state, None, "no slot event in the slice");
    }

    /// Phase 6: one pass, nothing quadratic. 500 rooms × 5 events, well
    /// under a second even unoptimized.
    #[test]
    fn static_path_cost_is_linear_ish() {
        let t = now();
        let mut events = Vec::new();
        for room in 0..500 {
            let room_id = format!("!room{room}:x");
            events.push(raw(in_room(slot_event(SLOT_ID, json!({ "status": "open", "application": { "type": "m.call" }, "encryption": { "type": "m.per_member" } }), t), &room_id), EventOrigin::Unknown));
            events.push(raw(
                in_room(room_encryption_event(t), &room_id),
                EventOrigin::Unknown,
            ));
            for member in 0..3 {
                events.push(raw(
                    in_room(
                        member_join_event(
                            &format!("@u{member}:x"),
                            &format!("m-{room}-{member}"),
                            t,
                        ),
                        &room_id,
                    ),
                    encrypted("D"),
                ));
            }
        }
        let start = std::time::Instant::now();
        let snapshots = compute(events, ElementCallCompat::Off);
        let elapsed = start.elapsed();
        assert_eq!(snapshots.len(), 500);
        assert!(snapshots.iter().all(|s| s.member_count() == 3));
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "took {elapsed:?}"
        );
    }
}
