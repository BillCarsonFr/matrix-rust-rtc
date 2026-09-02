//! Raw event accessors and the one dispatch funnel.
//!
//! [`RawMatrixEvent::event`] is the *full* event object as the host SDK holds
//! it — `type`, `sender`, `event_id`, `room_id`, `origin_server_ts`,
//! `content`, optional `state_key`, optional top-level `msc4354_sticky`,
//! optional `unsigned`. For encrypted events it is the *decrypted* event
//! (the host resolves `m.room.encrypted` before handing it over) and
//! `origin` carries the decryption metadata. `read_state` / `read_events`
//! return the same shape. Nothing else in the crate parses room-event JSON,
//! so every accessor lives here — and this is the ONLY file that knows the
//! stable and unstable event-type strings.

use crate::session::convert::msc3401::{self, Msc3401Conversion};
use crate::session::convert::msc4143::{self, Msc4143Conversion};
use crate::session::convert::MemberCandidate;
use crate::session::slot::RawSlot;
use crate::session::sticky::{self, StickyKey};
use crate::session::{ElementCallCompat, SessionConfig};
use crate::types::RawMatrixEvent;
use serde_json::Value;

/// Stable first: what the map keys and every log line use.
pub(crate) const MEMBER_EVENT_TYPES: [&str; 2] = ["m.rtc.member", "org.matrix.msc4143.rtc.member"];
pub(crate) const SLOT_EVENT_TYPES: [&str; 2] = ["m.rtc.slot", "org.matrix.msc4143.rtc.slot"];
pub(crate) const LEGACY_MEMBER_EVENT_TYPE: &str = "org.matrix.msc3401.call.member";
pub(crate) const ROOM_MEMBER_EVENT_TYPE: &str = "m.room.member";
pub(crate) const ROOM_ENCRYPTION_EVENT_TYPE: &str = "m.room.encryption";

// -- accessors ---------------------------------------------------------------

pub(crate) fn event_type(event: &RawMatrixEvent) -> Option<&str> {
    event.event.get("type")?.as_str()
}

pub(crate) fn sender(event: &RawMatrixEvent) -> Option<&str> {
    event.event.get("sender")?.as_str().filter(|s| !s.is_empty())
}

pub(crate) fn state_key(event: &RawMatrixEvent) -> Option<&str> {
    event.event.get("state_key")?.as_str()
}

pub(crate) fn event_id(event: &RawMatrixEvent) -> Option<&str> {
    event.event.get("event_id")?.as_str()
}

pub(crate) fn room_id(event: &RawMatrixEvent) -> Option<&str> {
    event.event.get("room_id")?.as_str().filter(|s| !s.is_empty())
}

pub(crate) fn origin_server_ts(event: &RawMatrixEvent) -> Option<u64> {
    event.event.get("origin_server_ts")?.as_u64()
}

/// The `content` object; `None` when absent or not an object.
pub(crate) fn content(event: &RawMatrixEvent) -> Option<&Value> {
    event.event.get("content").filter(|c| c.is_object())
}

/// MSC4354 `duration_ms`, from the top-level `msc4354_sticky` block
/// (`sticky` accepted too).
pub(crate) fn sticky_duration_ms(event: &RawMatrixEvent) -> Option<u64> {
    event
        .event
        .get("msc4354_sticky")
        .or_else(|| event.event.get("sticky"))?
        .get("duration_ms")?
        .as_u64()
}

/// The server-computed remaining lifetime, when the host passed `unsigned`.
pub(crate) fn sticky_ttl_ms(event: &RawMatrixEvent) -> Option<u64> {
    event.event.get("unsigned")?.get("msc4354_sticky_duration_ttl_ms")?.as_u64()
}

// -- dispatch ----------------------------------------------------------------

/// What one raw event means to a [`RoomState`](crate::session::state::RoomState).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Ingest {
    /// `m.rtc.member` (either type): a join or leave candidate. The
    /// candidate's `expires_at` is the MSC4354 `end_time`, `None` when the
    /// event carried no sticky metadata (the map refuses it).
    Member { key: StickyKey, event_id: String, candidate: MemberCandidate },
    /// MSC4354 removal: content = sticky key only.
    MemberRemoval { key: StickyKey, event_id: String, expires_at: Option<u64> },
    /// `org.matrix.msc3401.call.member` (`StateEvents` only), one per event.
    LegacyMember { state_key: String, candidate: MemberCandidate },
    /// Same type, empty content.
    LegacyMemberRemoval { state_key: String },
    /// `m.rtc.slot` (either type).
    Slot { slot_id: String, slot: RawSlot },
    /// `m.room.member`; `joined` is `membership == "join"`.
    RoomMember { user_id: String, joined: bool },
    /// `m.room.encryption` naming an algorithm. Empty content is `Ignored`:
    /// a room cannot be un-encrypted, so nothing is inferred from it.
    RoomEncryption,
    /// Logged at trace, never an error that aborts a batch.
    Ignored(&'static str),
}

/// Classify one raw event. `now` is the receive time (the MSC4354
/// `received_ts`); the static path passes its call time.
pub(crate) fn classify(event: &RawMatrixEvent, config: &SessionConfig, now: u64) -> Ingest {
    let Some(event_type) = event_type(event) else {
        return Ingest::Ignored("event without a type");
    };
    if sender(event).is_none() {
        return Ingest::Ignored("event without a sender");
    }
    if content(event).is_none() {
        return Ingest::Ignored("event without a content object");
    }

    if MEMBER_EVENT_TYPES.contains(&event_type) {
        return classify_member(event, now);
    }
    if SLOT_EVENT_TYPES.contains(&event_type) {
        let Some(slot_id) = state_key(event) else {
            return Ingest::Ignored("m.rtc.slot without a state_key");
        };
        let slot = RawSlot::parse(slot_id, content(event).unwrap_or(&Value::Null));
        return Ingest::Slot { slot_id: slot_id.to_owned(), slot };
    }
    match event_type {
        ROOM_MEMBER_EVENT_TYPE => {
            let Some(user_id) = state_key(event).filter(|k| !k.is_empty()) else {
                return Ingest::Ignored("m.room.member without a state_key");
            };
            let joined = content(event)
                .and_then(|c| c.get("membership"))
                .and_then(Value::as_str)
                == Some("join");
            Ingest::RoomMember { user_id: user_id.to_owned(), joined }
        }
        ROOM_ENCRYPTION_EVENT_TYPE => {
            let has_algorithm =
                content(event).and_then(|c| c.get("algorithm")).and_then(Value::as_str).is_some();
            if has_algorithm {
                Ingest::RoomEncryption
            } else {
                Ingest::Ignored("m.room.encryption without an algorithm")
            }
        }
        LEGACY_MEMBER_EVENT_TYPE => {
            if config.compat != ElementCallCompat::StateEvents {
                return Ingest::Ignored("msc3401 member event outside StateEvents compat");
            }
            match msc3401::member_candidate(event) {
                Some(Msc3401Conversion::Candidate { state_key, candidate }) => {
                    Ingest::LegacyMember { state_key, candidate }
                }
                Some(Msc3401Conversion::Removal { state_key }) => {
                    Ingest::LegacyMemberRemoval { state_key }
                }
                None => Ingest::Ignored("msc3401 member event that is no membership"),
            }
        }
        _ => Ingest::Ignored("unrelated event type"),
    }
}

fn classify_member(event: &RawMatrixEvent, now: u64) -> Ingest {
    let Some(conversion) = msc4143::member_candidate(event) else {
        return Ingest::Ignored("m.rtc.member that is no membership");
    };
    let sender = sender(event).unwrap_or_default().to_owned();
    let event_id = event_id(event).unwrap_or_default().to_owned();
    let expires_at = sticky::end_time(
        origin_server_ts(event).unwrap_or(now),
        sticky_duration_ms(event),
        sticky_ttl_ms(event),
        now,
    );
    // Both type spellings key the same entry: one member, one map slot.
    let key = |sticky_key: String| StickyKey {
        sender,
        event_type: MEMBER_EVENT_TYPES[0].to_owned(),
        sticky_key,
    };
    match conversion {
        Msc4143Conversion::Candidate { sticky_key, mut candidate } => {
            candidate.expires_at = expires_at;
            Ingest::Member { key: key(sticky_key), event_id, candidate }
        }
        Msc4143Conversion::Removal { sticky_key } => {
            Ingest::MemberRemoval { key: key(sticky_key), event_id, expires_at }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::convert::CandidateMembership;
    use crate::session::test_support::*;
    use crate::types::EventOrigin;
    use serde_json::json;

    const NOW: u64 = 1_700_000_000_000;

    fn classify_now(value: Value, compat: ElementCallCompat) -> Ingest {
        classify(&raw(value, EventOrigin::Unknown), &SessionConfig { compat }, NOW)
    }

    #[test]
    fn classifies_both_member_and_slot_spellings() {
        for t in MEMBER_EVENT_TYPES {
            let mut ev = member_join_event("@a:x", "m-1", NOW);
            ev["type"] = json!(t);
            match classify_now(ev, ElementCallCompat::Off) {
                Ingest::Member { key, candidate, .. } => {
                    assert_eq!(key.event_type, "m.rtc.member");
                    assert_eq!(key.sender, "@a:x");
                    assert_eq!(key.sticky_key, "m-1");
                    assert_eq!(candidate.membership, CandidateMembership::Join);
                    assert_eq!(candidate.expires_at, Some(NOW + 240_000));
                }
                other => panic!("{other:?}"),
            }
        }
        for t in SLOT_EVENT_TYPES {
            let mut ev = slot_open_event(NOW);
            ev["type"] = json!(t);
            assert!(matches!(classify_now(ev, ElementCallCompat::Off), Ingest::Slot { slot_id, .. } if slot_id == SLOT_ID));
        }
    }

    #[test]
    fn classifies_room_member_and_encryption() {
        assert_eq!(
            classify_now(room_member_event("@a:x", "join", NOW), ElementCallCompat::Off),
            Ingest::RoomMember { user_id: "@a:x".into(), joined: true }
        );
        for membership in ["leave", "ban", "invite"] {
            assert_eq!(
                classify_now(room_member_event("@a:x", membership, NOW), ElementCallCompat::Off),
                Ingest::RoomMember { user_id: "@a:x".into(), joined: false }
            );
        }
        let mut missing = room_member_event("@a:x", "join", NOW);
        missing["content"] = json!({});
        assert_eq!(
            classify_now(missing, ElementCallCompat::Off),
            Ingest::RoomMember { user_id: "@a:x".into(), joined: false }
        );
        assert_eq!(classify_now(room_encryption_event(NOW), ElementCallCompat::Off), Ingest::RoomEncryption);
        // Empty content is not "unencrypted": a room cannot be un-encrypted.
        let mut empty = room_encryption_event(NOW);
        empty["content"] = json!({});
        assert!(matches!(classify_now(empty, ElementCallCompat::Off), Ingest::Ignored(_)));
    }

    #[test]
    fn msc3401_is_ignored_outside_state_events_compat() {
        let ev = msc3401_member_event("@a:x", "DEV", NOW, NOW);
        assert!(matches!(classify_now(ev.clone(), ElementCallCompat::Off), Ingest::Ignored(_)));
        assert!(matches!(classify_now(ev.clone(), ElementCallCompat::StickyEvents), Ingest::Ignored(_)));
        assert!(matches!(classify_now(ev, ElementCallCompat::StateEvents), Ingest::LegacyMember { .. }));
        let mut leave = msc3401_member_event("@a:x", "DEV", NOW, NOW);
        leave["content"] = json!({});
        assert!(matches!(
            classify_now(leave, ElementCallCompat::StateEvents),
            Ingest::LegacyMemberRemoval { state_key } if state_key == "_@a:x_DEV_m.call"
        ));
    }

    #[test]
    fn unknown_and_malformed_events_are_ignored_not_errors() {
        assert!(matches!(classify_now(json!({ "type": "m.room.message", "sender": "@a:x", "content": {} }), ElementCallCompat::Off), Ingest::Ignored(_)));
        assert!(matches!(classify_now(json!({ "sender": "@a:x", "content": {} }), ElementCallCompat::Off), Ingest::Ignored(_)));
        assert!(matches!(classify_now(json!({ "type": "m.rtc.member", "content": {} }), ElementCallCompat::Off), Ingest::Ignored(_)));
        assert!(matches!(classify_now(json!({ "type": "m.rtc.member", "sender": "@a:x" }), ElementCallCompat::Off), Ingest::Ignored(_)));
        assert!(matches!(classify_now(json!({ "type": "m.rtc.member", "sender": "@a:x", "content": 5 }), ElementCallCompat::Off), Ingest::Ignored(_)));
        assert!(matches!(classify_now(json!("not even an object"), ElementCallCompat::Off), Ingest::Ignored(_)));
    }

    #[test]
    fn sticky_metadata_is_read_from_every_spelling() {
        let base = member_join_event("@a:x", "m-1", NOW - 10_000);
        let expires = |ev: Value| match classify_now(ev, ElementCallCompat::Off) {
            Ingest::Member { candidate, .. } => candidate.expires_at,
            other => panic!("{other:?}"),
        };
        // msc4354_sticky.duration_ms: min(ots, now) + duration
        assert_eq!(expires(base.clone()), Some(NOW - 10_000 + 240_000));
        // sticky.duration_ms
        let mut alt = base.clone();
        let sticky = alt.as_object_mut().unwrap().remove("msc4354_sticky").unwrap();
        alt["sticky"] = sticky;
        assert_eq!(expires(alt), Some(NOW - 10_000 + 240_000));
        // unsigned ttl preferred
        let mut ttl = base.clone();
        ttl["unsigned"] = json!({ "msc4354_sticky_duration_ttl_ms": 5_000 });
        assert_eq!(expires(ttl), Some(NOW + 5_000));
        // no metadata at all: still a candidate, expires_at None
        let mut none = base.clone();
        none.as_object_mut().unwrap().remove("msc4354_sticky");
        assert_eq!(expires(none), None);
    }

    #[test]
    fn a_bare_sticky_key_is_a_removal_with_the_events_end_time() {
        let ev = member_bare_leave_event("@a:x", "m-1", NOW);
        match classify_now(ev, ElementCallCompat::Off) {
            Ingest::MemberRemoval { key, expires_at, .. } => {
                assert_eq!(key.sticky_key, "m-1");
                assert_eq!(expires_at, Some(NOW + 240_000));
            }
            other => panic!("{other:?}"),
        }
    }

    /// The TS mock's `memberJoinEvent` output, copied verbatim: the two
    /// builders must stay in sync.
    #[test]
    fn a_mock_driver_shaped_join_parses() {
        let event: Value = serde_json::from_str(MOCK_DRIVER_JOIN_FIXTURE).unwrap();
        match classify_now(event, ElementCallCompat::Off) {
            Ingest::Member { candidate, .. } => {
                assert!(candidate.is_join());
                assert_eq!(candidate.member.member_id, "m-1");
                assert_eq!(candidate.member.transports.published[0].properties["livekit_service_url"], LK_SERVICE_URL);
            }
            other => panic!("{other:?}"),
        }
    }
}
