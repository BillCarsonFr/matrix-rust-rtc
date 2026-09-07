//! Legacy `org.matrix.msc3401.call.member` room-state memberships ->
//! candidates. Only runs with `ElementCallCompat::StateEvents`. Delete-by
//! date: this whole file goes with that generation (plus one dispatch arm
//! and the `legacy` map in `state.rs`).
//!
//! State key `_{user}_{device}_{application}{call_id}`. It is **not parsed**:
//! everything it encodes is also in the content or in the event's own
//! homeserver-stamped `sender`. It is the `RoomState.legacy` key, appears in
//! logs and breaks focus ties.
//!
//! | | Element Call, pre-sticky | MSC4143 today |
//! |---|---|---|
//! | carrier | `org.matrix.msc3401.call.member` state event | `m.rtc.member` sticky event |
//! | session | `application: "m.call"` (a string) + `call_id` | `slot_id: "m.call#ROOM"` |
//! | member id | `membershipID` | `member.id` |
//! | device | `device_id` in the content, self-asserted | the device that encrypted the event |
//! | join | any non-empty content | `member.membership: "join"` |
//! | leave | content becomes `{}` | `membership: "leave"` |
//! | lifetime | `created_ts + expires`, checked by every reader | the homeserver's sticky TTL |
//! | transport | `foci_preferred` + `focus_active.focus_selection` | `transports.{published,can_subscribe}` |
//!
//! Two things are resolved here because nothing downstream can: **expiry**
//! (the deadline is in the content, so the candidate carries `expires_at`
//! and the session's expiry timer covers it) and **the active focus**
//! (`oldest_membership` makes a member's SFU a property of the whole
//! roster, so [`assign_transports`] runs over the surviving candidates at
//! projection time).
//!
//! # Not handled: the `memberships` array
//!
//! The generation before this one put a `memberships[]` array (all devices
//! of a user) into a single state event. That form is **not supported and
//! must not be implemented**: content carrying `memberships` yields nothing,
//! so it can neither be mis-parsed as a per-device membership nor sneak back
//! in as a feature.

use super::{CandidateMembership, CandidateSource, LegacyDetails, MemberCandidate};
use crate::session::dispatch;
use crate::types::{DeviceAttribution, Member, MemberTransports, RawMatrixEvent};
use serde_json::Value;

/// MSC3401 has no slot concept; its candidates land in this well-known slot
/// id (the MSC3401 room call is the one with the empty call id).
pub const LEGACY_SLOT_ID: &str = "";

/// The `call_id` sentinel an MSC4143 slot id uses for the room-wide session,
/// where this dialect uses an empty string.
const ROOM_CALL_ID: &str = "ROOM";

/// The lifetime to assume when a content states no `expires`: four hours,
/// the JS SDK's own default.
const DEFAULT_EXPIRES_MS: u64 = 4 * 60 * 60 * 1000;

/// What one state event contributes.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Msc3401Conversion {
    Candidate {
        state_key: String,
        candidate: MemberCandidate,
    },
    /// Empty content: this generation's leave.
    Removal { state_key: String },
}

/// The LiveKit participant identity this generation uses: the plain
/// `{user_id}:{device_id}` string. It is also the legacy `membershipID`
/// fallback, so in this mode a member's id and its SFU identity coincide.
pub(crate) fn participant_identity(user_id: &str, device_id: &str) -> String {
    format!("{user_id}:{device_id}")
}

/// Convert one per-device state membership into at most one candidate.
///
/// Dropped, each with a log line except the leave: empty content (`{}`) is
/// the leave → `Removal`; a `memberships` array; `application` that is not a
/// non-empty string (an object is the *modern* shape and not this dialect);
/// a missing or empty `device_id` (nothing could be bound to it and no media
/// key could travel in either direction). Expiry is *not* judged here: the
/// candidate carries `expires_at` and the session refuses one already past
/// it on arrival, exactly like the sticky map.
pub(crate) fn member_candidate(event: &RawMatrixEvent) -> Option<Msc3401Conversion> {
    let sender = dispatch::sender(event)?;
    let state_key = dispatch::state_key(event)?.to_owned();
    let object = dispatch::content(event)?.as_object()?;

    if object.is_empty() {
        return Some(Msc3401Conversion::Removal { state_key });
    }

    if object.contains_key("memberships") {
        log::debug!(
            "ignoring a `memberships` array from {sender} ({state_key}): that is two Element Call \
             generations back and is not supported"
        );
        return None;
    }

    let Some(application_type) = object
        .get("application")
        .and_then(Value::as_str)
        .filter(|a| !a.is_empty())
    else {
        log::debug!(
            "ignoring a pre-sticky membership from {sender} ({state_key}): it names no application"
        );
        return None;
    };

    let Some(device_id) = object
        .get("device_id")
        .and_then(Value::as_str)
        .filter(|d| !d.is_empty())
    else {
        log::debug!(
            "ignoring a pre-sticky membership from {sender} ({state_key}): it names no device, so \
             nothing could be bound to it and no media key could travel in either direction"
        );
        return None;
    };

    let origin_server_ts = dispatch::origin_server_ts(event).unwrap_or(0);
    // A refresh keeps `created_ts` and gains a later `origin_server_ts`, so
    // the minimum is the join instant; a first join has no `created_ts`, so
    // it is `origin_server_ts`. The minimum also stops a peer with a fast
    // clock from looking alive longer than it is.
    let joined_at = match object.get("created_ts").and_then(Value::as_u64) {
        Some(created_ts) => created_ts.min(origin_server_ts),
        None => origin_server_ts,
    };
    let expires = object
        .get("expires")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_EXPIRES_MS);
    let expires_at = joined_at.saturating_add(expires);

    let call_id = object
        .get("call_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let legacy_call_id = format!(
        "{application_type}#{}",
        if call_id.is_empty() {
            ROOM_CALL_ID
        } else {
            call_id
        }
    );

    // `membershipID` is what Element Call addresses its media keys to; the
    // fallback is the JS SDK's own, `{user}:{device}`.
    let member_id = object
        .get("membershipID")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| participant_identity(sender, device_id));

    let candidate = MemberCandidate {
        member: Member {
            member_id,
            user_id: sender.to_owned(),
            device_id: Some(device_id.to_owned()),
            device_attribution: DeviceAttribution::Claimed,
            membership_ts: Some(joined_at),
            display_name: None,
            avatar_url: None,
            intent: object
                .get("m.call.intent")
                .and_then(Value::as_str)
                .map(str::to_owned),
            application_type: Some(application_type.to_owned()),
            // Resolved across members at projection time (`assign_transports`).
            transports: MemberTransports::default(),
        },
        source: CandidateSource::Msc3401State,
        membership: CandidateMembership::Join,
        origin: event.origin.clone(),
        expires_at: Some(expires_at),
        slot_id: LEGACY_SLOT_ID.to_owned(),
        origin_server_ts,
        leave_reason: None,
        legacy: Some(LegacyDetails {
            call_id: legacy_call_id,
            state_key: state_key.clone(),
            joined_at,
            own_focus: object
                .get("foci_preferred")
                .and_then(Value::as_array)
                .and_then(|foci| foci.first())
                .cloned(),
            prefers_own_focus: object
                .get("focus_active")
                .and_then(|focus| focus.get("focus_selection"))
                .and_then(Value::as_str)
                == Some("multi_sfu"),
        }),
    };

    Some(Msc3401Conversion::Candidate {
        state_key,
        candidate,
    })
}

/// Resolve the focus each surviving legacy candidate publishes on and write
/// it into `member.transports`.
///
/// `oldest_membership` — the historical default — points every member at the
/// first preferred focus of the oldest membership of the *same legacy call*
/// (`joined_at` ascending, ties by `state_key` so two clients reading the
/// same room agree). `multi_sfu` means each member keeps its own. Either
/// way, if the chosen side names no focus the other is used rather than
/// declaring a member who publishes nowhere; with no focus anywhere the
/// member appears with empty transports.
///
/// Runs over the *survivors* only — expired and excluded candidates have
/// already been removed — so a long-dead membership cannot pin the room to
/// an SFU nobody is on any more.
pub(crate) fn assign_transports(candidates: &mut [MemberCandidate]) {
    let resolved: Vec<MemberTransports> = candidates
        .iter()
        .map(|candidate| {
            resolve_focus(candidate, candidates)
                .and_then(super::msc4143::parse_transport)
                .map(|transport| MemberTransports {
                    can_subscribe: vec![transport.transport_type.clone()],
                    published: vec![transport],
                })
                .unwrap_or_default()
        })
        .collect();
    for (candidate, transports) in candidates.iter_mut().zip(resolved) {
        candidate.member.transports = transports;
    }
}

fn resolve_focus<'a>(member: &'a MemberCandidate, all: &'a [MemberCandidate]) -> Option<&'a Value> {
    let details = member.legacy.as_ref()?;
    if details.prefers_own_focus && details.own_focus.is_some() {
        return details.own_focus.as_ref();
    }
    let oldest = all
        .iter()
        .filter_map(|candidate| candidate.legacy.as_ref())
        .filter(|candidate| candidate.call_id == details.call_id)
        .min_by(|a, b| {
            a.joined_at
                .cmp(&b.joined_at)
                .then_with(|| a.state_key.cmp(&b.state_key))
        })
        .and_then(|oldest| oldest.own_focus.as_ref());
    oldest.or(details.own_focus.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EventOrigin;
    use serde_json::json;

    /// A join as observed from Element Call on the JS SDK, pre-sticky.
    const EC_JOIN: &str = r#"{
        "application": "m.call",
        "call_id": "",
        "scope": "m.room",
        "device_id": "V5cP8FErcB",
        "membershipID": "@alice:example.io:V5cP8FErcB",
        "expires": 14400000,
        "created_ts": 1749000000000,
        "m.call.intent": "video",
        "focus_active": { "type": "livekit", "focus_selection": "multi_sfu" },
        "foci_preferred": [
            {
                "type": "livekit",
                "livekit_alias": "!room:example.io",
                "livekit_service_url": "https://mrtc.example.io/livekit/jwt"
            }
        ]
    }"#;

    const CREATED: u64 = 1_749_000_000_000;
    /// Ten minutes after `EC_JOIN`'s `created_ts`.
    const NOW: u64 = 1_749_000_600_000;

    fn content(json: &str) -> Value {
        serde_json::from_str(json).unwrap()
    }

    fn event_at(state_key: &str, origin_server_ts: u64, content: Value) -> RawMatrixEvent {
        RawMatrixEvent {
            event: json!({
                "type": "org.matrix.msc3401.call.member",
                "sender": "@alice:example.io",
                "state_key": state_key,
                "event_id": format!("$ev-{state_key}"),
                "room_id": "!room:example.io",
                "origin_server_ts": origin_server_ts,
                "content": content,
            }),
            origin: EventOrigin::Unknown,
        }
    }

    fn one(json: &str) -> Option<Msc3401Conversion> {
        member_candidate(&event_at(
            "_@alice:example.io_V5cP8FErcB_m.call",
            CREATED,
            content(json),
        ))
    }

    fn candidate(json: &str) -> MemberCandidate {
        match one(json) {
            Some(Msc3401Conversion::Candidate { candidate, .. }) => candidate,
            other => panic!("expected a candidate, got {other:?}"),
        }
    }

    /// `EC_JOIN` with `patch` applied to the top level; a `Value::Null` removes.
    fn join_with(patch: &[(&str, Value)]) -> Value {
        let mut value = content(EC_JOIN);
        let object = value.as_object_mut().unwrap();
        for (key, replacement) in patch {
            if replacement.is_null() {
                object.remove(*key);
            } else {
                object.insert((*key).to_owned(), replacement.clone());
            }
        }
        value
    }

    fn candidate_of(
        state_key: &str,
        origin_server_ts: u64,
        value: Value,
    ) -> Option<MemberCandidate> {
        match member_candidate(&event_at(state_key, origin_server_ts, value)) {
            Some(Msc3401Conversion::Candidate { candidate, .. }) => Some(candidate),
            _ => None,
        }
    }

    #[test]
    fn a_join_becomes_a_candidate_with_the_exact_member_fields() {
        let c = candidate(EC_JOIN);
        assert_eq!(c.source, CandidateSource::Msc3401State);
        assert!(c.is_join());
        assert_eq!(c.slot_id, LEGACY_SLOT_ID);
        let legacy = c.legacy.as_ref().unwrap();
        assert_eq!(legacy.call_id, "m.call#ROOM");
        assert_eq!(legacy.state_key, "_@alice:example.io_V5cP8FErcB_m.call");
        assert_eq!(legacy.joined_at, CREATED);
        assert!(legacy.prefers_own_focus);
        assert_eq!(c.member.member_id, "@alice:example.io:V5cP8FErcB");
        assert_eq!(c.member.user_id, "@alice:example.io");
        assert_eq!(c.member.application_type.as_deref(), Some("m.call"));
        assert_eq!(c.member.intent.as_deref(), Some("video"));
        assert_eq!(c.member.device_id.as_deref(), Some("V5cP8FErcB"));
        assert_eq!(c.member.device_attribution, DeviceAttribution::Claimed);
        assert_eq!(c.member.membership_ts, Some(CREATED));
        assert_eq!(c.expires_at, Some(CREATED + 14_400_000));
        assert_eq!(c.origin, EventOrigin::Unknown);
        // Transports are assigned at projection time.
        assert!(c.member.transports.published.is_empty());
    }

    #[test]
    fn the_empty_call_id_becomes_the_room_sentinel() {
        assert_eq!(candidate(EC_JOIN).legacy.unwrap().call_id, "m.call#ROOM");
        let named =
            candidate_of("a", CREATED, join_with(&[("call_id", json!("standup"))])).unwrap();
        assert_eq!(named.legacy.unwrap().call_id, "m.call#standup");
    }

    #[test]
    fn an_empty_content_is_a_removal() {
        assert_eq!(
            one("{}"),
            Some(Msc3401Conversion::Removal {
                state_key: "_@alice:example.io_V5cP8FErcB_m.call".into()
            })
        );
    }

    /// Guards the out-of-scope decision: a `memberships[]` array produces
    /// nothing, even when its entries are well-formed.
    #[test]
    fn a_memberships_array_yields_no_candidate() {
        let array = r#"{ "memberships": [ {
            "application": "m.call", "call_id": "", "device_id": "V5cP8FErcB",
            "expires": 14400000, "created_ts": 1749000000000,
            "foci_preferred": [{ "type": "livekit", "livekit_service_url": "https://x" }]
        } ] }"#;
        assert!(one(array).is_none());
    }

    #[test]
    fn a_membership_with_no_device_id_is_dropped() {
        assert!(candidate_of("a", CREATED, join_with(&[("device_id", Value::Null)])).is_none());
        assert!(candidate_of("a", CREATED, join_with(&[("device_id", json!(""))])).is_none());
    }

    #[test]
    fn a_membership_with_no_application_is_dropped() {
        assert!(candidate_of("a", CREATED, join_with(&[("application", Value::Null)])).is_none());
        // An object is the *modern* shape and not this dialect.
        assert!(
            candidate_of(
                "a",
                CREATED,
                join_with(&[("application", json!({ "type": "m.call" }))])
            )
            .is_none()
        );
    }

    /// The deadline itself counts as expired (`expires_at <= now`); the
    /// session applies that rule, this pins the value it applies it to.
    #[test]
    fn the_expiry_deadline_is_created_ts_plus_expires() {
        let c = candidate(EC_JOIN);
        let deadline = CREATED + 14_400_000;
        assert_eq!(c.expires_at, Some(deadline));
        assert!(!is_expired(&c, deadline - 1));
        assert!(is_expired(&c, deadline));
    }

    fn is_expired(c: &MemberCandidate, now: u64) -> bool {
        c.expires_at.is_some_and(|at| at <= now)
    }

    /// The first join of a session states no `created_ts` — the client cannot
    /// know it yet — so the deadline is measured from the event itself.
    #[test]
    fn a_first_join_expires_from_origin_server_ts() {
        let first_join = join_with(&[("created_ts", Value::Null)]);
        let old = candidate_of("a", 1_000, first_join.clone()).unwrap();
        assert!(is_expired(&old, NOW));
        let fresh = candidate_of("a", NOW - 1_000, first_join).unwrap();
        assert!(!is_expired(&fresh, NOW));
        assert_eq!(fresh.legacy.unwrap().joined_at, NOW - 1_000);
    }

    #[test]
    fn a_membership_with_no_expires_gets_four_hours() {
        let c = candidate_of("a", CREATED, join_with(&[("expires", Value::Null)])).unwrap();
        assert_eq!(c.expires_at, Some(CREATED + DEFAULT_EXPIRES_MS));
    }

    /// A peer with a fast clock must not get to outlive its own event.
    #[test]
    fn a_created_ts_after_the_event_is_clamped_to_the_event() {
        let skewed = join_with(&[("created_ts", json!(NOW + 60_000))]);
        let c = candidate_of("a", 1_000, skewed).unwrap();
        assert_eq!(c.legacy.as_ref().unwrap().joined_at, 1_000);
        assert!(is_expired(&c, NOW));
    }

    #[test]
    fn the_membership_id_falls_back_to_user_and_device() {
        let c = candidate_of("a", CREATED, join_with(&[("membershipID", Value::Null)])).unwrap();
        assert_eq!(c.member.member_id, "@alice:example.io:V5cP8FErcB");
        assert_eq!(
            c.member.member_id,
            participant_identity("@alice:example.io", "V5cP8FErcB")
        );
    }

    // -- focus resolution ----------------------------------------------------

    const JOINED_EARLIER: u64 = NOW - 600_000;
    const JOINED_LATER: u64 = NOW - 300_000;

    fn focus(url: &str) -> Value {
        json!({ "type": "livekit", "livekit_service_url": url })
    }

    /// One member, with `created_ts` and `origin_server_ts` agreeing.
    fn focus_member(
        state_key: &str,
        joined_at: u64,
        selection: &str,
        url: &str,
    ) -> MemberCandidate {
        let content = join_with(&[
            ("created_ts", json!(joined_at)),
            (
                "focus_active",
                json!({ "type": "livekit", "focus_selection": selection }),
            ),
            ("foci_preferred", json!([focus(url)])),
        ]);
        candidate_of(state_key, joined_at, content).unwrap()
    }

    fn published_url(c: &MemberCandidate) -> Option<&str> {
        c.member
            .transports
            .published
            .first()?
            .properties
            .get("livekit_service_url")?
            .as_str()
    }

    fn resolved(mut members: Vec<MemberCandidate>) -> Vec<MemberCandidate> {
        assign_transports(&mut members);
        members
    }

    /// Fed newest-first, so a naive "first in the list wins" would fail.
    #[test]
    fn oldest_membership_uses_the_oldest_members_focus() {
        let members = resolved(vec![
            focus_member("b", JOINED_LATER, "oldest_membership", "https://new"),
            focus_member("a", JOINED_EARLIER, "oldest_membership", "https://old"),
        ]);
        for member in &members {
            assert_eq!(published_url(member), Some("https://old"));
            assert_eq!(
                member.member.transports.can_subscribe,
                vec!["livekit".to_owned()]
            );
        }
    }

    #[test]
    fn oldest_membership_is_resolved_per_legacy_call() {
        let room_wide = focus_member("a", JOINED_LATER, "oldest_membership", "https://room");
        let mut standup = focus_member("b", JOINED_EARLIER, "oldest_membership", "https://standup");
        standup.legacy.as_mut().unwrap().call_id = "m.call#standup".into();
        let members = resolved(vec![room_wide, standup]);
        // The older `standup` membership must not lend its focus to the
        // room-wide session it is not part of.
        assert_eq!(published_url(&members[0]), Some("https://room"));
        assert_eq!(published_url(&members[1]), Some("https://standup"));
    }

    #[test]
    fn multi_sfu_keeps_each_members_own_focus() {
        let members = resolved(vec![
            focus_member("a", JOINED_EARLIER, "multi_sfu", "https://one"),
            focus_member("b", JOINED_LATER, "multi_sfu", "https://two"),
        ]);
        assert_eq!(published_url(&members[0]), Some("https://one"));
        assert_eq!(published_url(&members[1]), Some("https://two"));
    }

    /// A member we never try to reach is worse than one we reach on the wrong
    /// SFU, so an empty `foci_preferred` borrows rather than publishing nowhere.
    #[test]
    fn a_member_with_no_focus_of_its_own_borrows_one() {
        let mut without = focus_member("b", JOINED_LATER, "multi_sfu", "https://unused");
        without.legacy.as_mut().unwrap().own_focus = None;
        let members = resolved(vec![
            focus_member("a", JOINED_EARLIER, "multi_sfu", "https://one"),
            without,
        ]);
        assert_eq!(published_url(&members[1]), Some("https://one"));
    }

    #[test]
    fn a_membership_with_no_focus_anywhere_still_appears() {
        let mut without = candidate(EC_JOIN);
        without.legacy.as_mut().unwrap().own_focus = None;
        let members = resolved(vec![without]);
        assert_eq!(members.len(), 1);
        assert!(members[0].member.transports.published.is_empty());
        assert!(members[0].member.transports.can_subscribe.is_empty());
    }

    /// Two clients reading the same room must reach the same answer,
    /// whatever order the state arrived in — hence the `state_key` tie-break.
    #[test]
    fn the_translation_is_deterministic_under_reordering() {
        let a = focus_member("a", JOINED_EARLIER, "oldest_membership", "https://a");
        let b = focus_member("b", JOINED_EARLIER, "oldest_membership", "https://b");
        let forwards = resolved(vec![a.clone(), b.clone()]);
        let backwards = resolved(vec![b, a]);
        assert_eq!(published_url(&forwards[0]), published_url(&backwards[1]));
        assert_eq!(published_url(&forwards[0]), Some("https://a"));
    }

    /// The focus is passed through untouched: `livekit_alias` is not ours to
    /// interpret and rides along in `properties`.
    #[test]
    fn the_focus_object_is_passed_through_verbatim() {
        let members = resolved(vec![candidate(EC_JOIN)]);
        let transport = &members[0].member.transports.published[0];
        assert_eq!(transport.transport_type, "livekit");
        assert_eq!(transport.properties["livekit_alias"], "!room:example.io");
        assert_eq!(
            transport.properties["livekit_service_url"],
            "https://mrtc.example.io/livekit/jwt"
        );
    }
}
