//! `RoomState`: everything the session knows about one room, and the
//! projection of it into a [`SessionSnapshot`] per slot.
//!
//! Pure: no I/O, no clock except the `now` argument. The live `Session`
//! holds one and projects a single slot; the static path holds one per room
//! and projects every `(room, slot)`.
//!
//! Room-state conditions are *opt-in*: "no slot state has been supplied" and
//! "slot state supplied but this slot has none" are different things (the
//! first leaves the condition unenforced, the second closes the slot), and
//! the same goes for the room's joined members.

use super::convert::{CandidateSource, LEGACY_SLOT_ID, MemberCandidate, msc3401};
use super::dispatch::Ingest;
use super::slot::RawSlot;
use super::sticky::{self, Outcome};
use super::{JoinExclusionReason, SessionSnapshot, SlotState};
use crate::types::RtcTransport;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap, HashSet};

/// Whether a transition changed the state. A hint only: publication is
/// decided by comparing projected snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Changed {
    Yes,
    No,
}

impl Changed {
    fn from_bool(changed: bool) -> Self {
        if changed { Self::Yes } else { Self::No }
    }
}

pub(crate) struct RoomState {
    room_id: String,
    /// MSC4143 candidates, keyed per MSC4354.
    sticky: sticky::Map<MemberCandidate>,
    /// MSC3401 candidates by state key (`StateEvents` compat only).
    legacy: HashMap<String, MemberCandidate>,
    slots: HashMap<String, RawSlot>,
    slot_state_supplied: bool,
    room_encryption: Option<bool>,
    /// `None` = unenforced.
    room_members: Option<HashSet<String>>,
    /// Static path: the first `m.room.member` event supplies the set. Live:
    /// only the seed does — a single live event is not the room's roster.
    infer_room_members: bool,
    /// Candidates the last `expire` dropped, reported once as `Expired`.
    expired: Vec<MemberCandidate>,
}

impl RoomState {
    /// For the live session: room members are enforced only after the seed
    /// supplied them ([`Self::supply_room_members`]).
    pub(crate) fn for_live(room_id: &str) -> Self {
        Self::new(room_id, false)
    }

    /// For the static path: the slice is all the host has, so the first
    /// `m.room.member` event in it turns the condition on.
    pub(crate) fn for_static(room_id: &str) -> Self {
        Self::new(room_id, true)
    }

    fn new(room_id: &str, infer_room_members: bool) -> Self {
        Self {
            room_id: room_id.to_owned(),
            sticky: sticky::Map::new(),
            legacy: HashMap::new(),
            slots: HashMap::new(),
            slot_state_supplied: false,
            room_encryption: None,
            room_members: None,
            infer_room_members,
            expired: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn room_id(&self) -> &str {
        &self.room_id
    }

    /// Slot state is known for this room (even if no slot event exists):
    /// slots without an event resolve `Closed` from now on.
    pub(crate) fn supply_slot_state(&mut self) {
        if !self.slot_state_supplied {
            log::debug!(
                "[{}] slot state supplied; the open-slot condition is enforced",
                self.room_id
            );
        }
        self.slot_state_supplied = true;
    }

    /// The room's joined members are known (possibly none yet).
    pub(crate) fn supply_room_members(&mut self) {
        if self.room_members.is_none() {
            log::debug!(
                "[{}] room members supplied; the sender-in-room condition is enforced",
                self.room_id
            );
            self.room_members = Some(HashSet::new());
        }
    }

    /// Forget the previous transition's `Expired` report. Call once at the
    /// start of every transition (a batch of ingests plus one `expire`).
    pub(crate) fn start_transition(&mut self) {
        self.expired.clear();
    }

    /// Apply one classified event. `now` is the receive time.
    pub(crate) fn ingest(&mut self, ingest: Ingest, now: u64) -> Changed {
        let room = self.room_id.clone();
        match ingest {
            Ingest::Member {
                key,
                event_id,
                candidate,
            } => {
                log::debug!(
                    "[{room}] m.rtc.member {key}: {:?} in slot '{}' (expires_at={:?})",
                    candidate.membership,
                    candidate.slot_id,
                    candidate.expires_at
                );
                let end_time = candidate.expires_at;
                Changed::from_bool(
                    self.sticky
                        .upsert(key, end_time, &event_id, Some(candidate), now)
                        == Outcome::Changed,
                )
            }
            Ingest::MemberRemoval {
                key,
                event_id,
                expires_at,
            } => {
                log::debug!("[{room}] m.rtc.member {key}: removal");
                Changed::from_bool(
                    self.sticky.upsert(key, expires_at, &event_id, None, now) == Outcome::Changed,
                )
            }
            Ingest::LegacyMember {
                state_key,
                candidate,
            } => {
                if candidate.expires_at.is_some_and(|at| at <= now) {
                    log::debug!(
                        "[{room}] msc3401 member {state_key} already expired on arrival ({}ms ago); ignored",
                        now.saturating_sub(candidate.expires_at.unwrap_or(0))
                    );
                    return Changed::No;
                }
                let changed = self.legacy.get(&state_key) != Some(&candidate);
                if changed {
                    log::debug!(
                        "[{room}] msc3401 member {state_key}: {} (expires_at={:?})",
                        candidate.member.member_id,
                        candidate.expires_at
                    );
                    self.legacy.insert(state_key, candidate);
                } else {
                    log::trace!("[{room}] msc3401 member {state_key} unchanged");
                }
                Changed::from_bool(changed)
            }
            Ingest::LegacyMemberRemoval { state_key } => {
                let removed = self.legacy.remove(&state_key).is_some();
                if removed {
                    log::debug!("[{room}] msc3401 member {state_key} left");
                }
                Changed::from_bool(removed)
            }
            Ingest::Slot { slot_id, slot } => {
                self.supply_slot_state();
                let changed = self.slots.get(&slot_id) != Some(&slot);
                if changed {
                    log::info!(
                        "[{room}] m.rtc.slot '{slot_id}' -> {}",
                        if slot.resolve(self.room_encryption).is_open() {
                            "Open"
                        } else {
                            "Closed"
                        }
                    );
                    self.slots.insert(slot_id, slot);
                }
                Changed::from_bool(changed)
            }
            Ingest::RoomMember { user_id, joined } => match &mut self.room_members {
                Some(members) => {
                    let changed = if joined {
                        members.insert(user_id.clone())
                    } else {
                        members.remove(&user_id)
                    };
                    if changed {
                        log::debug!("[{room}] room member {user_id}: joined={joined}");
                    }
                    Changed::from_bool(changed)
                }
                None if self.infer_room_members => {
                    self.supply_room_members();
                    self.ingest(Ingest::RoomMember { user_id, joined }, now)
                }
                None => {
                    log::trace!(
                        "[{room}] room member {user_id} ignored: room members not supplied"
                    );
                    Changed::No
                }
            },
            Ingest::RoomEncryption => {
                let changed = self.room_encryption != Some(true);
                if changed {
                    log::info!(
                        "[{room}] room encryption: {:?} -> Some(true)",
                        self.room_encryption
                    );
                    self.room_encryption = Some(true);
                }
                Changed::from_bool(changed)
            }
            Ingest::Ignored(reason) => {
                log::trace!("[{room}] ignored event: {reason}");
                Changed::No
            }
        }
    }

    /// Drop every candidate whose `expires_at <= now`. The dropped ones are
    /// reported as `Expired` by the projection of this transition only.
    pub(crate) fn expire(&mut self, now: u64) -> Changed {
        let mut dropped = self.sticky.expire(now);
        let due: Vec<String> = self
            .legacy
            .iter()
            .filter(|(_, c)| c.expires_at.is_some_and(|at| at <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for state_key in due {
            if let Some(candidate) = self.legacy.remove(&state_key) {
                log::debug!("[{}] msc3401 member {state_key} expired", self.room_id);
                dropped.push(candidate);
            }
        }
        let changed = dropped.iter().any(MemberCandidate::is_join);
        self.expired.extend(dropped);
        Changed::from_bool(changed)
    }

    /// The earliest expiry over both maps.
    pub(crate) fn next_expiry(&self) -> Option<u64> {
        let legacy = self.legacy.values().filter_map(|c| c.expires_at).min();
        match (self.sticky.next_expiry(), legacy) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Every slot that has either a slot state event or at least one joined
    /// candidate (see plan §1.7).
    pub(crate) fn slot_ids(&self) -> BTreeSet<String> {
        let mut ids: BTreeSet<String> = self.slots.keys().cloned().collect();
        ids.extend(
            self.sticky
                .values()
                .filter(|c| c.is_join())
                .map(|c| c.slot_id.clone()),
        );
        if !self.legacy.is_empty() {
            ids.insert(LEGACY_SLOT_ID.to_owned());
        }
        ids
    }

    /// The resolved state of `slot_id`; `None` while slot state is
    /// unsupplied, and always `None` for the legacy slot (that generation
    /// has no slot concept).
    pub(crate) fn slot_state(&self, slot_id: &str) -> Option<SlotState> {
        if slot_id == LEGACY_SLOT_ID || !self.slot_state_supplied {
            return None;
        }
        Some(
            self.slots
                .get(slot_id)
                .map(|s| s.resolve(self.room_encryption))
                .unwrap_or(SlotState::Closed),
        )
    }

    /// Whether RTC data should be encrypted, per the slot and the room:
    /// `None` while nothing was supplied to negotiate from, otherwise
    /// whether the slot is open with a mechanism this client supports.
    pub(crate) fn negotiated_encryption(&self, slot_id: &str) -> Option<bool> {
        self.slot_state(slot_id).map(|state| {
            state
                .open()
                .and_then(|s| s.mechanism.as_ref())
                .is_some_and(|m| m.is_supported())
        })
    }

    /// The MSC4143 join conditions that depend on room state.
    fn condition(&self, candidate: &MemberCandidate) -> Option<JoinExclusionReason> {
        if candidate.source == CandidateSource::Msc4143 {
            if self
                .slot_state(&candidate.slot_id)
                .is_some_and(|s| !s.is_open())
            {
                return Some(JoinExclusionReason::SlotClosed);
            }
            // In an encrypted room `m.rtc.member` events MUST be encrypted, and
            // one that is not "MUST be considered left". An event whose
            // encryption the host did not report is not judged.
            if self.room_encryption == Some(true) && candidate.origin.was_encrypted() == Some(false)
            {
                return Some(JoinExclusionReason::UnencryptedInEncryptedRoom);
            }
        }
        match &self.room_members {
            Some(members) if !members.contains(&candidate.member.user_id) => {
                Some(JoinExclusionReason::SenderNotInRoom)
            }
            _ => None,
        }
    }

    /// Project one slot.
    pub(crate) fn project(&self, slot_id: &str) -> SessionSnapshot {
        let mut joined: Vec<MemberCandidate> = Vec::new();
        let mut excluded: Vec<(MemberCandidate, JoinExclusionReason)> = Vec::new();

        for candidate in self
            .sticky
            .values()
            .filter(|c| c.slot_id == slot_id && c.is_join())
        {
            match self.condition(candidate) {
                None => joined.push(candidate.clone()),
                Some(reason) => excluded.push((candidate.clone(), reason)),
            }
        }

        if slot_id == LEGACY_SLOT_ID {
            let mut survivors: Vec<MemberCandidate> = Vec::new();
            for candidate in self.legacy.values() {
                match self.condition(candidate) {
                    None => survivors.push(candidate.clone()),
                    Some(reason) => excluded.push((candidate.clone(), reason)),
                }
            }
            // Cross-member: which SFU a legacy member publishes on depends on
            // the roster, so it is resolved over the survivors here.
            survivors.sort_by(|a, b| legacy_order(a).cmp(&legacy_order(b)));
            msc3401::assign_transports(&mut survivors);
            joined.extend(survivors);
        }

        for candidate in self
            .expired
            .iter()
            .filter(|c| c.slot_id == slot_id && c.is_join())
        {
            excluded.push((candidate.clone(), JoinExclusionReason::Expired));
        }

        // HashMap iteration is unordered; the snapshot must not be.
        joined.sort_by(|a, b| member_order(a).cmp(&member_order(b)));
        excluded.sort_by(|a, b| member_order(&a.0).cmp(&member_order(&b.0)));

        let slot_state = self.slot_state(slot_id);
        let negotiated_encryption = self.negotiated_encryption(slot_id);
        let start_ts = joined.iter().map(start_of).min();
        let application_type = slot_state
            .as_ref()
            .and_then(SlotState::open)
            .map(|open| open.application_type.clone())
            .or_else(|| {
                joined
                    .iter()
                    .find_map(|c| c.member.application_type.clone())
            });

        let mut transports: Vec<RtcTransport> = Vec::new();
        for transport in joined
            .iter()
            .flat_map(|c| c.member.transports.published.iter())
        {
            if !transports.contains(transport) {
                transports.push(transport.clone());
            }
        }

        SessionSnapshot {
            room_id: self.room_id.clone(),
            slot_id: slot_id.to_owned(),
            members: joined.into_iter().map(|c| c.member).collect(),
            transports,
            excluded_candidates: excluded
                .into_iter()
                .map(|(c, reason)| (c.member, reason))
                .collect(),
            slot_state,
            negotiated_encryption,
            start_ts,
            application_type,
            seeded: false,
        }
    }

    /// Everything this room state believes about `slot_id`, as JSON, for bug
    /// reports. Includes the *candidates* and why each is in or out — the
    /// joined set alone cannot explain an absence.
    pub(crate) fn debug_json(&self, slot_id: &str) -> Value {
        let describe = |candidate: &MemberCandidate, map_end_time: Option<u64>| {
            json!({
                "sender": candidate.member.user_id,
                "member_id": candidate.member.member_id,
                "slot_id": candidate.slot_id,
                "source": format!("{:?}", candidate.source),
                "membership": format!("{:?}", candidate.membership),
                "device_id": candidate.member.device_id,
                "device_attribution": format!("{:?}", candidate.member.device_attribution),
                "was_encrypted": candidate.origin.was_encrypted(),
                "expires_at": map_end_time.or(candidate.expires_at),
                "legacy_call_id": candidate.legacy.as_ref().map(|l| l.call_id.clone()),
                "state_key": candidate.legacy.as_ref().map(|l| l.state_key.clone()),
                "condition": match (candidate.is_join(), self.condition(candidate)) {
                    (false, _) => "Left".to_owned(),
                    (true, None) => "Joined".to_owned(),
                    (true, Some(reason)) => format!("{reason:?}"),
                },
            })
        };
        let mut candidates: Vec<Value> = self
            .sticky
            .iter()
            .map(|(key, end_time, value)| match value {
                Some(candidate) => describe(candidate, Some(end_time)),
                None => json!({ "sender": key.sender, "sticky_key": key.sticky_key, "expires_at": end_time, "condition": "Removed" }),
            })
            .collect();
        candidates.extend(self.legacy.values().map(|c| describe(c, None)));
        candidates.sort_by_key(|c| c.to_string());

        let snapshot = self.project(slot_id);
        json!({
            "room_id": self.room_id,
            "slot_id": slot_id,
            "slot": match self.slot_state(slot_id) {
                None => "Unsupplied".to_owned(),
                Some(state) if state.is_open() => "Open".to_owned(),
                Some(_) => "Closed".to_owned(),
            },
            "slot_state_supplied": self.slot_state_supplied,
            "known_slots": self.slots.keys().collect::<BTreeSet<_>>(),
            "room_encryption": self.room_encryption,
            "room_members_known": self.room_members.as_ref().map(HashSet::len),
            "negotiated_encryption": self.negotiated_encryption(slot_id),
            "joined_count": snapshot.members.len(),
            "joined": snapshot.members.iter().map(|m| m.member_id.clone()).collect::<Vec<_>>(),
            "excluded": snapshot
                .excluded_candidates
                .iter()
                .map(|(m, reason)| json!({ "member_id": m.member_id, "reason": format!("{reason:?}") }))
                .collect::<Vec<_>>(),
            "sticky_live": self.sticky.live_len(),
            "legacy_count": self.legacy.len(),
            "next_expiry": self.next_expiry(),
            "candidates": candidates,
        })
    }
}

fn member_order(candidate: &MemberCandidate) -> (&str, &str) {
    (&candidate.member.user_id, &candidate.member.member_id)
}

fn legacy_order(candidate: &MemberCandidate) -> (u64, &str) {
    match &candidate.legacy {
        Some(legacy) => (legacy.joined_at, &legacy.state_key),
        None => (candidate.origin_server_ts, ""),
    }
}

/// When a joined candidate's participation began: `origin_server_ts`, or the
/// dialect's `joined_at` for MSC3401 (a re-sent state event has a later
/// `origin_server_ts` but the same join).
fn start_of(candidate: &MemberCandidate) -> u64 {
    candidate
        .legacy
        .as_ref()
        .map(|l| l.joined_at)
        .unwrap_or(candidate.origin_server_ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::dispatch::classify;
    use crate::session::test_support::*;
    use crate::session::{ElementCallCompat, SessionConfig};
    use crate::types::EventOrigin;
    use serde_json::json;

    const NOW: u64 = 1_700_000_000_000;

    fn live() -> RoomState {
        RoomState::for_live(ROOM_ID)
    }

    fn apply_in(
        state: &mut RoomState,
        event: Value,
        origin: EventOrigin,
        compat: ElementCallCompat,
        now: u64,
    ) -> Changed {
        let ingest = classify(&raw(event, origin), &SessionConfig { compat }, now);
        state.ingest(ingest, now)
    }

    fn apply(state: &mut RoomState, event: Value, origin: EventOrigin) -> Changed {
        apply_in(state, event, origin, ElementCallCompat::Off, NOW)
    }

    fn encrypted(device: &str) -> EventOrigin {
        EventOrigin::Encrypted {
            sender_device_id: Some(device.to_owned()),
        }
    }

    fn joined(state: &RoomState) -> Vec<String> {
        state
            .project(SLOT_ID)
            .members
            .iter()
            .map(|m| m.member_id.clone())
            .collect()
    }

    fn excluded(state: &RoomState) -> Vec<(String, JoinExclusionReason)> {
        state
            .project(SLOT_ID)
            .excluded_candidates
            .iter()
            .map(|(m, r)| (m.member_id.clone(), *r))
            .collect()
    }

    // -- slot condition -------------------------------------------------------

    #[test]
    fn members_join_while_slot_state_is_unsupplied() {
        let mut s = live();
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        assert_eq!(joined(&s), vec!["m-a"]);
        let snap = s.project(SLOT_ID);
        assert_eq!(snap.slot_state, None);
        assert_eq!(snap.negotiated_encryption, None);
        assert_eq!(
            snap.application_type.as_deref(),
            Some("m.call"),
            "from the member when there is no slot"
        );
    }

    #[test]
    fn slot_closing_leaves_and_reopening_restores() {
        let mut s = live();
        apply(&mut s, slot_open_event(NOW), EventOrigin::Cleartext);
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        assert_eq!(joined(&s), vec!["m-a"]);
        assert!(s.project(SLOT_ID).slot_state.as_ref().unwrap().is_open());

        assert_eq!(
            apply(&mut s, slot_closed_event(NOW), EventOrigin::Cleartext),
            Changed::Yes
        );
        assert!(joined(&s).is_empty());
        assert_eq!(
            excluded(&s),
            vec![("m-a".to_owned(), JoinExclusionReason::SlotClosed)]
        );

        apply(&mut s, slot_open_event(NOW), EventOrigin::Cleartext);
        assert_eq!(joined(&s), vec!["m-a"], "candidates survive a closed slot");
    }

    #[test]
    fn slot_state_supplied_but_absent_for_this_slot_is_closed() {
        let mut s = live();
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        // A slot event for *another* slot supplies slot state for the room.
        apply(
            &mut s,
            slot_event(
                "m.whiteboard#ROOM",
                json!({ "status": "open", "application": { "type": "m.whiteboard" } }),
                NOW,
            ),
            EventOrigin::Cleartext,
        );
        assert_eq!(s.project(SLOT_ID).slot_state, Some(SlotState::Closed));
        assert!(joined(&s).is_empty());
        // The seed can supply it explicitly too.
        let mut s = live();
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        s.supply_slot_state();
        assert_eq!(s.project(SLOT_ID).slot_state, Some(SlotState::Closed));
        assert_eq!(s.project(SLOT_ID).negotiated_encryption, Some(false));
    }

    #[test]
    fn slot_condition_never_applies_to_msc3401_candidates() {
        let mut s = live();
        apply_in(
            &mut s,
            slot_closed_event(NOW),
            EventOrigin::Cleartext,
            ElementCallCompat::StateEvents,
            NOW,
        );
        apply_in(
            &mut s,
            msc3401_member_event("@a:x", "DEV", NOW - 1_000, NOW - 1_000),
            EventOrigin::Unknown,
            ElementCallCompat::StateEvents,
            NOW,
        );
        let legacy = s.project(LEGACY_SLOT_ID);
        assert_eq!(legacy.members.len(), 1);
        assert_eq!(
            legacy.slot_state, None,
            "the legacy slot never has a slot state"
        );
        assert_eq!(legacy.negotiated_encryption, None);
        assert_eq!(legacy.application_type.as_deref(), Some("m.call"));
    }

    // -- encryption condition -------------------------------------------------

    #[test]
    fn cleartext_members_are_excluded_only_in_encrypted_rooms() {
        let mut s = live();
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            EventOrigin::Cleartext,
        );
        apply(
            &mut s,
            member_join_event("@b:x", "m-b", NOW),
            EventOrigin::Unknown,
        );
        apply(
            &mut s,
            member_join_event("@c:x", "m-c", NOW),
            encrypted("C"),
        );
        assert_eq!(joined(&s), vec!["m-a", "m-b", "m-c"]);

        assert_eq!(
            apply(&mut s, room_encryption_event(NOW), EventOrigin::Cleartext),
            Changed::Yes
        );
        assert_eq!(
            joined(&s),
            vec!["m-b", "m-c"],
            "Unknown origin is never judged"
        );
        assert_eq!(
            excluded(&s),
            vec![(
                "m-a".to_owned(),
                JoinExclusionReason::UnencryptedInEncryptedRoom
            )]
        );
    }

    #[test]
    fn slot_resolution_reacts_to_room_encryption_in_either_order() {
        // Encryption after the slot.
        let mut s = live();
        apply(&mut s, slot_open_event(NOW), EventOrigin::Cleartext);
        assert!(s.project(SLOT_ID).slot_state.unwrap().is_open());
        apply(&mut s, room_encryption_event(NOW), EventOrigin::Cleartext);
        assert_eq!(
            s.project(SLOT_ID).slot_state,
            Some(SlotState::Closed),
            "unencrypted slot closes in an encrypted room"
        );
        // Encryption before the slot.
        let mut s = live();
        apply(&mut s, room_encryption_event(NOW), EventOrigin::Cleartext);
        apply(&mut s, slot_open_event(NOW), EventOrigin::Cleartext);
        assert_eq!(s.project(SLOT_ID).slot_state, Some(SlotState::Closed));
        apply(
            &mut s,
            slot_event(
                SLOT_ID,
                json!({ "status": "open", "application": { "type": "m.call" }, "encryption": { "type": "m.per_member" } }),
                NOW,
            ),
            EventOrigin::Cleartext,
        );
        assert!(s.project(SLOT_ID).slot_state.unwrap().is_open());
    }

    #[test]
    fn negotiated_encryption_follows_the_slot() {
        let mut s = live();
        assert_eq!(s.project(SLOT_ID).negotiated_encryption, None);
        apply(&mut s, slot_open_event(NOW), EventOrigin::Cleartext);
        assert_eq!(
            s.project(SLOT_ID).negotiated_encryption,
            Some(false),
            "no encryption object"
        );
        apply(
            &mut s,
            slot_event(
                SLOT_ID,
                json!({ "status": "open", "application": { "type": "m.call" }, "encryption": { "type": "m.per_member" } }),
                NOW,
            ),
            EventOrigin::Cleartext,
        );
        assert_eq!(s.project(SLOT_ID).negotiated_encryption, Some(true));
        apply(&mut s, slot_closed_event(NOW), EventOrigin::Cleartext);
        assert_eq!(
            s.project(SLOT_ID).negotiated_encryption,
            Some(false),
            "closed"
        );
    }

    #[test]
    fn unencrypted_in_encrypted_room_never_applies_to_msc3401_candidates() {
        let mut s = live();
        apply_in(
            &mut s,
            room_encryption_event(NOW),
            EventOrigin::Cleartext,
            ElementCallCompat::StateEvents,
            NOW,
        );
        apply_in(
            &mut s,
            msc3401_member_event("@a:x", "DEV", NOW - 1_000, NOW - 1_000),
            EventOrigin::Cleartext,
            ElementCallCompat::StateEvents,
            NOW,
        );
        let legacy = s.project(LEGACY_SLOT_ID);
        assert_eq!(legacy.members.len(), 1);
        assert!(legacy.excluded_candidates.is_empty());
    }

    // -- room membership condition -------------------------------------------

    #[test]
    fn room_membership_is_unenforced_until_supplied_then_tracks_joins_and_leaves() {
        let mut s = live();
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        // Live: a single m.room.member event does not turn the condition on.
        assert_eq!(
            apply(
                &mut s,
                room_member_event("@z:x", "join", NOW),
                EventOrigin::Cleartext
            ),
            Changed::No
        );
        assert_eq!(joined(&s), vec!["m-a"]);

        s.supply_room_members();
        assert!(
            joined(&s).is_empty(),
            "supplied and empty: nobody is in the room"
        );
        assert_eq!(
            excluded(&s),
            vec![("m-a".to_owned(), JoinExclusionReason::SenderNotInRoom)]
        );
        apply(
            &mut s,
            room_member_event("@a:x", "join", NOW),
            EventOrigin::Cleartext,
        );
        assert_eq!(joined(&s), vec!["m-a"]);
        apply(
            &mut s,
            room_member_event("@a:x", "leave", NOW),
            EventOrigin::Cleartext,
        );
        assert!(joined(&s).is_empty());
        apply(
            &mut s,
            room_member_event("@a:x", "join", NOW),
            EventOrigin::Cleartext,
        );
        assert_eq!(joined(&s), vec!["m-a"], "rejoin restores");
        assert_eq!(
            apply(
                &mut s,
                room_member_event("@unrelated:x", "leave", NOW),
                EventOrigin::Cleartext
            ),
            Changed::No
        );
        assert_eq!(joined(&s), vec!["m-a"]);
    }

    #[test]
    fn the_static_path_infers_room_members_from_the_slice() {
        let mut s = RoomState::for_static(ROOM_ID);
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        apply(
            &mut s,
            member_join_event("@b:x", "m-b", NOW),
            encrypted("B"),
        );
        apply(
            &mut s,
            room_member_event("@a:x", "join", NOW),
            EventOrigin::Cleartext,
        );
        assert_eq!(joined(&s), vec!["m-a"]);
        assert_eq!(
            excluded(&s),
            vec![("m-b".to_owned(), JoinExclusionReason::SenderNotInRoom)]
        );
    }

    // -- multiple members per device (plan 1.2) -------------------------------

    #[test]
    fn two_members_from_one_device_are_both_joined_and_leave_independently() {
        let mut s = live();
        apply(
            &mut s,
            member_join_event("@a:x", "m-player", NOW),
            encrypted("A"),
        );
        apply(
            &mut s,
            member_join_event("@a:x", "m-moderator", NOW),
            encrypted("A"),
        );
        assert_eq!(joined(&s), vec!["m-moderator", "m-player"]);
        apply(
            &mut s,
            member_leave_event("@a:x", "m-player", NOW + 1),
            encrypted("A"),
        );
        assert_eq!(joined(&s), vec!["m-moderator"]);
    }

    // -- change detection ------------------------------------------------------

    #[test]
    fn identical_events_change_nothing_and_a_batch_is_one_transition() {
        let mut s = live();
        let ev = member_join_event("@a:x", "m-a", NOW);
        assert_eq!(apply(&mut s, ev.clone(), encrypted("A")), Changed::Yes);
        assert_eq!(apply(&mut s, ev.clone(), encrypted("A")), Changed::No);
        assert_eq!(
            apply(&mut s, slot_open_event(NOW), EventOrigin::Cleartext),
            Changed::Yes
        );
        assert_eq!(
            apply(&mut s, slot_open_event(NOW), EventOrigin::Cleartext),
            Changed::No
        );
        assert_eq!(
            apply(&mut s, room_encryption_event(NOW), EventOrigin::Cleartext),
            Changed::Yes
        );
        assert_eq!(
            apply(&mut s, room_encryption_event(NOW), EventOrigin::Cleartext),
            Changed::No
        );
        assert_eq!(
            s.project(SLOT_ID),
            s.project(SLOT_ID),
            "projection is deterministic"
        );

        // Six joins ingested, one projection: the intermediate rosters are
        // never observable because nothing projects between ingests.
        let mut s = live();
        for i in 0..6 {
            apply(
                &mut s,
                member_join_event(&format!("@u{i}:x"), &format!("m-{i}"), NOW),
                encrypted("D"),
            );
        }
        assert_eq!(s.project(SLOT_ID).members.len(), 6);
    }

    #[test]
    fn removal_leave_and_expiry_remove_the_candidate() {
        let mut s = live();
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        assert_eq!(
            apply(
                &mut s,
                member_bare_leave_event("@a:x", "m-a", NOW + 1),
                encrypted("A")
            ),
            Changed::Yes
        );
        assert!(joined(&s).is_empty());
        assert!(
            excluded(&s).is_empty(),
            "a withdrawn member is not excluded, it is gone"
        );

        let mut s = live();
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        assert_eq!(
            apply(
                &mut s,
                member_leave_event("@a:x", "m-a", NOW + 1),
                encrypted("A")
            ),
            Changed::Yes
        );
        assert!(joined(&s).is_empty());

        // Expiry surfaces once as Expired, then is gone.
        let mut s = live();
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        assert_eq!(s.next_expiry(), Some(NOW + 240_000));
        s.start_transition();
        assert_eq!(s.expire(NOW + 240_000 - 1), Changed::No);
        assert_eq!(joined(&s), vec!["m-a"]);
        s.start_transition();
        assert_eq!(s.expire(NOW + 240_000), Changed::Yes);
        assert!(joined(&s).is_empty());
        assert_eq!(
            excluded(&s),
            vec![("m-a".to_owned(), JoinExclusionReason::Expired)]
        );
        s.start_transition();
        assert!(excluded(&s).is_empty());
        assert_eq!(s.next_expiry(), None);
    }

    #[test]
    fn legacy_expiry_shares_the_timer_and_expired_on_arrival_is_ignored() {
        let mut s = live();
        let compat = ElementCallCompat::StateEvents;
        // expires 4h after joined_at
        apply_in(
            &mut s,
            msc3401_member_event("@a:x", "DEV", NOW, NOW),
            EventOrigin::Unknown,
            compat,
            NOW,
        );
        apply_in(
            &mut s,
            member_join_event("@b:x", "m-b", NOW),
            encrypted("B"),
            compat,
            NOW,
        );
        assert_eq!(
            s.next_expiry(),
            Some(NOW + 240_000),
            "the sticky one is earlier"
        );
        s.start_transition();
        s.expire(NOW + 240_000);
        assert_eq!(s.next_expiry(), Some(NOW + 4 * 3_600_000));
        s.start_transition();
        assert_eq!(s.expire(NOW + 4 * 3_600_000), Changed::Yes);
        let legacy = s.project(LEGACY_SLOT_ID);
        assert!(legacy.members.is_empty());
        assert_eq!(
            legacy.excluded_candidates[0].1,
            JoinExclusionReason::Expired
        );

        let mut s = live();
        let old = msc3401_member_event("@a:x", "DEV", NOW - 5 * 3_600_000, NOW - 5 * 3_600_000);
        assert_eq!(
            apply_in(&mut s, old, EventOrigin::Unknown, compat, NOW),
            Changed::No
        );
        assert!(s.project(LEGACY_SLOT_ID).members.is_empty());
        assert!(s.slot_ids().is_empty());
    }

    // -- snapshot metadata ------------------------------------------------------

    #[test]
    fn snapshot_metadata() {
        let mut s = live();
        let empty = s.project(SLOT_ID);
        assert_eq!(empty.member_count(), 0);
        assert!(!empty.is_active());
        assert_eq!(empty.start_ts, None);
        assert_eq!(empty.application_type, None);

        apply(
            &mut s,
            member_join_event_with("@a:x", "m-a", NOW + 500, "https://lk-a"),
            encrypted("A"),
        );
        apply(
            &mut s,
            member_join_event_with("@b:x", "m-b", NOW + 100, "https://lk-b"),
            encrypted("B"),
        );
        apply(
            &mut s,
            member_join_event_with("@c:x", "m-c", NOW + 900, "https://lk-a"),
            encrypted("C"),
        );
        let snap = s.project(SLOT_ID);
        assert_eq!(snap.member_count(), 3);
        assert!(snap.is_active());
        assert_eq!(
            snap.start_ts,
            Some(NOW + 100),
            "earliest joined origin_server_ts"
        );
        assert_eq!(snap.application_type.as_deref(), Some("m.call"));
        let urls: Vec<&str> = snap
            .transports
            .iter()
            .map(|t| t.properties["livekit_service_url"].as_str().unwrap())
            .collect();
        assert_eq!(
            urls,
            vec!["https://lk-a", "https://lk-b"],
            "union without duplicates"
        );
        assert_eq!(
            snap.members[0].transports.published[0].properties["livekit_service_url"],
            "https://lk-a"
        );

        // application_type prefers the open slot's (encrypted, since the
        // room becomes encrypted below and an unencrypted slot would close)
        apply(
            &mut s,
            slot_event(
                SLOT_ID,
                json!({ "status": "open", "application": { "type": "m.call" }, "encryption": { "type": "m.per_member" } }),
                NOW,
            ),
            EventOrigin::Cleartext,
        );
        assert_eq!(
            s.project(SLOT_ID).application_type.as_deref(),
            Some("m.call")
        );

        // excluded carries every excluded candidate with its reason
        apply(&mut s, room_encryption_event(NOW), EventOrigin::Cleartext);
        apply(
            &mut s,
            member_join_event("@d:x", "m-d", NOW),
            EventOrigin::Cleartext,
        );
        s.supply_room_members();
        for user in ["@a:x", "@b:x", "@d:x"] {
            apply(
                &mut s,
                room_member_event(user, "join", NOW),
                EventOrigin::Cleartext,
            );
        }
        let snap = s.project(SLOT_ID);
        assert_eq!(
            snap.excluded_candidates
                .iter()
                .map(|(m, r)| (m.member_id.as_str(), *r))
                .collect::<Vec<_>>(),
            vec![
                ("m-c", JoinExclusionReason::SenderNotInRoom),
                ("m-d", JoinExclusionReason::UnencryptedInEncryptedRoom)
            ]
        );
    }

    #[test]
    fn debug_json_carries_the_verdicts() {
        let mut s = live();
        apply(&mut s, slot_open_event(NOW), EventOrigin::Cleartext);
        apply(&mut s, room_encryption_event(NOW), EventOrigin::Cleartext);
        apply(
            &mut s,
            member_join_event("@a:x", "m-a", NOW),
            encrypted("A"),
        );
        apply(
            &mut s,
            member_join_event("@b:x", "m-b", NOW),
            EventOrigin::Cleartext,
        );
        s.supply_room_members();
        apply(
            &mut s,
            room_member_event("@a:x", "join", NOW),
            EventOrigin::Cleartext,
        );
        apply(
            &mut s,
            room_member_event("@b:x", "join", NOW),
            EventOrigin::Cleartext,
        );
        let debug = s.debug_json(SLOT_ID);
        assert_eq!(
            debug["slot"], "Closed",
            "unencrypted slot in an encrypted room"
        );
        assert_eq!(debug["room_encryption"], true);
        assert_eq!(debug["room_members_known"], 2);
        assert_eq!(debug["negotiated_encryption"], false);
        assert_eq!(debug["joined"], json!([]));
        let conditions: Vec<&str> = debug["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["condition"].as_str().unwrap())
            .collect();
        assert_eq!(conditions.len(), 2);
        assert!(conditions.iter().all(|c| *c == "SlotClosed"));
    }
}
