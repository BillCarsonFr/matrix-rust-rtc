//! `m.rtc.member` sticky events (stable and unstable types) -> candidates,
//! spec-shaped (MSC4143) content only.

use super::{CandidateMembership, CandidateSource, MemberCandidate};
use crate::session::dispatch;
use crate::types::{
    DeviceAttribution, EventOrigin, LeaveReason, Member, MemberTransports, RawMatrixEvent,
    RtcTransport,
};
use serde_json::{Map, Value};

/// What one `m.rtc.member` event contributes.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Msc4143Conversion {
    /// A membership (join or leave) under `sticky_key`.
    Candidate {
        sticky_key: String,
        candidate: MemberCandidate,
    },
    /// MSC4354 removal: the content carries nothing but the sticky key.
    Removal { sticky_key: String },
}

/// Convert one sticky member event. `None` for content that is no
/// membership at all (not an object, no sticky key, no `member` object).
///
/// Per MSC4143 the event counts as a join only when `member.membership` is
/// `join`, `member.id` is set and `application.type` names a type; anything
/// else — including a membership value this client does not know — is a
/// leave. The remaining join conditions (open slot, sender in the room,
/// encrypted in an encrypted room, still sticky) are applied by the session.
pub(crate) fn member_candidate(event: &RawMatrixEvent) -> Option<Msc4143Conversion> {
    let sender = dispatch::sender(event)?;
    let content = dispatch::content(event)?.clone();
    let object = content.as_object()?;

    let sticky_key = sticky_key_of(object);

    let Some(slot_id) = object.get("slot_id") else {
        return match sticky_key {
            // A content that is only a sticky key is how MSC4354 withdraws an
            // entry (and how the 2025 dialect leaves). No slot id is needed:
            // the map key alone says which entry goes.
            Some(sticky_key) => Some(Msc4143Conversion::Removal { sticky_key }),
            None => {
                log::debug!(
                    "m.rtc.member from {sender} has neither slot_id nor sticky key; ignored"
                );
                None
            }
        };
    };
    let Some(slot_id) = slot_id.as_str().filter(|s| !s.is_empty()) else {
        log::debug!("m.rtc.member from {sender} has a non-string or empty slot_id; ignored");
        return None;
    };
    let Some(sticky_key) = sticky_key else {
        log::debug!("m.rtc.member from {sender} carries no sticky key; ignored");
        return None;
    };
    let Some(member) = object.get("member").and_then(Value::as_object) else {
        log::debug!("m.rtc.member from {sender} has no member object; ignored");
        return None;
    };

    let member_id = member
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty());
    let membership = member.get("membership").and_then(Value::as_str);
    let application = object.get("application").and_then(Value::as_object);
    let application_type = application
        .and_then(|a| a.get("type"))
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty());

    // `join` also requires an application type: MSC4143 makes the application
    // object REQUIRED on a joining member event.
    let joined = membership == Some("join") && member_id.is_some() && application_type.is_some();
    if let Some(unknown) = membership.filter(|m| *m != "join" && *m != "leave") {
        log::debug!("m.rtc.member from {sender}: unknown membership '{unknown}', treated as leave");
    }
    if let Some(id) = member_id
        && id != sticky_key
    {
        // MSC4143 requires sticky_key == member.id. The sticky key decides
        // map identity regardless; the mismatch is worth surfacing.
        log::warn!("m.rtc.member from {sender} has sticky_key '{sticky_key}' != member.id '{id}'");
    }

    let (device_id, device_attribution) = attribute_device(&event.origin);

    let intent = application
        .and_then(|a| a.get("m.call.intent"))
        .and_then(Value::as_str)
        .map(str::to_owned);

    let transports = if joined {
        object
            .get("transports")
            .map(parse_transports)
            .unwrap_or_default()
    } else {
        MemberTransports::default()
    };

    let leave_reason = object.get("leave_reason").and_then(|reason| {
        Some(LeaveReason {
            code: reason.get("code")?.as_str()?.to_owned(),
            reason: reason
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    });

    let candidate = MemberCandidate {
        member: Member {
            // `member.id` identifies the participation (media keys bind to
            // it); the sticky key identifies the map entry. They are equal
            // in every compliant event; a leave may omit `member.id`.
            member_id: member_id.unwrap_or(&sticky_key).to_owned(),
            user_id: sender.to_owned(),
            device_id,
            device_attribution,
            membership_ts: None,
            display_name: None,
            avatar_url: None,
            event_id: dispatch::event_id(event).map(str::to_owned),
            intent,
            application_type: application_type.map(str::to_owned),
            transports,
        },
        source: CandidateSource::Msc4143,
        membership: if joined {
            CandidateMembership::Join
        } else {
            CandidateMembership::Leave
        },
        origin: event.origin.clone(),
        // The sticky layer fills this from the event's sticky metadata.
        expires_at: None,
        slot_id: slot_id.to_owned(),
        origin_server_ts: dispatch::origin_server_ts(event).unwrap_or(0),
        leave_reason,
        legacy: None,
    };

    Some(Msc4143Conversion::Candidate {
        sticky_key,
        candidate,
    })
}

/// The device a member is bound to, and how much that binding is worth: only
/// decryption metadata can name one for a sticky member event (MSC4143
/// content carries no device), so it is `Verified` or nothing.
fn attribute_device(origin: &EventOrigin) -> (Option<String>, DeviceAttribution) {
    match origin {
        EventOrigin::Encrypted {
            sender_device_id: Some(device_id),
        } => (Some(device_id.clone()), DeviceAttribution::Verified),
        _ => (None, DeviceAttribution::Unknown),
    }
}

/// The sticky key under either spelling (unstable first).
fn sticky_key_of(object: &Map<String, Value>) -> Option<String> {
    object
        .get("msc4354_sticky_key")
        .or_else(|| object.get("sticky_key"))
        .and_then(Value::as_str)
        .filter(|k| !k.is_empty())
        .map(str::to_owned)
}

/// `transports: { published: [ {type, ...} ], can_subscribe: [type] }`.
///
/// Every transport type is preserved: `type` becomes `transport_type` and the
/// rest of the object (the type-specific fields) rides along as `properties`.
/// Elements without a string `type` are skipped, as are non-string
/// `can_subscribe` entries.
fn parse_transports(transports: &Value) -> MemberTransports {
    let published = transports
        .get("published")
        .and_then(Value::as_array)
        .map(|entries| entries.iter().filter_map(parse_transport).collect())
        .unwrap_or_default();
    let can_subscribe = transports
        .get("can_subscribe")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    MemberTransports {
        published,
        can_subscribe,
    }
}

/// One `published[..]` entry. Shared with the MSC3401 converter, whose
/// focus objects have the same `{type, ...}` shape.
pub(crate) fn parse_transport(entry: &Value) -> Option<RtcTransport> {
    let object = entry.as_object()?;
    let Some(transport_type) = object.get("type").and_then(Value::as_str) else {
        log::debug!("published transport without a string type skipped: {entry}");
        return None;
    };
    let mut properties = object.clone();
    properties.remove("type");
    Some(RtcTransport {
        transport_type: transport_type.to_owned(),
        properties: Value::Object(properties),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A spec-shaped join event as another client would send it on the wire.
    const JOIN_JSON: &str = r#"{
        "slot_id": "m.call#ROOM",
        "member": { "id": "xyzABCDEF0123", "membership": "join" },
        "application": { "type": "m.call", "m.call.voice_only": true, "m.call.intent": "video" },
        "transports": {
            "published": [
                { "type": "livekit", "livekit_service_url": "https://sfu.example.com/jwt" }
            ],
            "can_subscribe": ["livekit"]
        },
        "msc4354_sticky_key": "xyzABCDEF0123"
    }"#;

    fn event_with(content: Value, origin: EventOrigin) -> RawMatrixEvent {
        RawMatrixEvent {
            event: json!({
                "type": "m.rtc.member",
                "sender": "@alice:example.org",
                "event_id": "$ev1",
                "room_id": "!room:example.org",
                "origin_server_ts": 1_700_000_000_000u64,
                "msc4354_sticky": { "duration_ms": 240_000 },
                "content": content,
            }),
            origin,
        }
    }

    fn encrypted(device: &str) -> EventOrigin {
        EventOrigin::Encrypted {
            sender_device_id: Some(device.to_owned()),
        }
    }

    fn convert(json: &str) -> Option<Msc4143Conversion> {
        member_candidate(&event_with(
            serde_json::from_str(json).unwrap(),
            encrypted("DEVICEID"),
        ))
    }

    fn candidate(json: &str) -> MemberCandidate {
        match convert(json) {
            Some(Msc4143Conversion::Candidate { candidate, .. }) => candidate,
            other => panic!("expected a candidate, got {other:?}"),
        }
    }

    #[test]
    fn spec_shaped_join_parses() {
        let c = candidate(JOIN_JSON);
        assert!(c.is_join());
        assert_eq!(c.source, CandidateSource::Msc4143);
        assert_eq!(c.slot_id, "m.call#ROOM");
        assert_eq!(c.member.member_id, "xyzABCDEF0123");
        assert_eq!(c.member.user_id, "@alice:example.org");
        assert_eq!(c.member.application_type.as_deref(), Some("m.call"));
        assert_eq!(c.member.intent.as_deref(), Some("video"));
        assert_eq!(c.member.device_id.as_deref(), Some("DEVICEID"));
        assert_eq!(c.member.device_attribution, DeviceAttribution::Verified);
        assert_eq!(
            c.member.transports.can_subscribe,
            vec!["livekit".to_owned()]
        );
        assert_eq!(c.member.transports.published.len(), 1);
        let lk = &c.member.transports.published[0];
        assert_eq!(lk.transport_type, "livekit");
        assert_eq!(
            lk.properties["livekit_service_url"],
            "https://sfu.example.com/jwt"
        );
        assert!(
            lk.properties.get("type").is_none(),
            "type lives in transport_type"
        );
        assert_eq!(c.origin_server_ts, 1_700_000_000_000);
        assert_eq!(c.expires_at, None, "filled by the sticky layer");
        assert_eq!(c.member.membership_ts, None);
    }

    /// `member.membership` is authoritative: fully join-shaped content is
    /// still a leave if it says so.
    #[test]
    fn membership_leave_wins_over_join_shaped_content() {
        let json = JOIN_JSON.replace(r#""membership": "join""#, r#""membership": "leave""#);
        let c = candidate(&json);
        assert_eq!(c.membership, CandidateMembership::Leave);
        assert_eq!(c.member.member_id, "xyzABCDEF0123");
        assert!(c.member.transports.published.is_empty());
    }

    /// A membership value from a future spec revision must not fail the
    /// parse; it simply doesn't count as joined.
    #[test]
    fn unknown_membership_parses_and_counts_as_left() {
        let json = JOIN_JSON.replace(r#""membership": "join""#, r#""membership": "lurking""#);
        assert_eq!(candidate(&json).membership, CandidateMembership::Leave);
    }

    /// MSC4143 makes `application` REQUIRED on a joining member event.
    #[test]
    fn join_without_application_type_is_not_a_join() {
        let json = JOIN_JSON.replace(
            r#""application": { "type": "m.call", "m.call.voice_only": true, "m.call.intent": "video" },"#,
            "",
        );
        assert_eq!(candidate(&json).membership, CandidateMembership::Leave);
    }

    #[test]
    fn sticky_key_is_accepted_under_both_spellings() {
        let stable = JOIN_JSON.replace("msc4354_sticky_key", "sticky_key");
        match convert(&stable) {
            Some(Msc4143Conversion::Candidate { sticky_key, .. }) => {
                assert_eq!(sticky_key, "xyzABCDEF0123")
            }
            other => panic!("{other:?}"),
        }
    }

    /// The sticky key decides the map entry; a differing `member.id` still
    /// identifies the participation (and is logged).
    #[test]
    fn mismatched_sticky_key_keeps_both_identities() {
        let json = JOIN_JSON.replace(
            r#""msc4354_sticky_key": "xyzABCDEF0123""#,
            r#""msc4354_sticky_key": "other""#,
        );
        match convert(&json) {
            Some(Msc4143Conversion::Candidate {
                sticky_key,
                candidate,
            }) => {
                assert_eq!(sticky_key, "other");
                assert_eq!(candidate.member.member_id, "xyzABCDEF0123");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn unknown_transport_is_preserved_as_is() {
        let json = JOIN_JSON.replace(
            r#"{ "type": "livekit", "livekit_service_url": "https://sfu.example.com/jwt" }"#,
            r#"{ "type": "com.example.sfu", "endpoint": "wss://x", "nested": { "a": 1 } }, { "no_type": true }"#,
        );
        let c = candidate(&json);
        assert_eq!(
            c.member.transports.published.len(),
            1,
            "typeless entries are skipped"
        );
        let t = &c.member.transports.published[0];
        assert_eq!(t.transport_type, "com.example.sfu");
        assert_eq!(
            t.properties,
            json!({ "endpoint": "wss://x", "nested": { "a": 1 } })
        );
    }

    #[test]
    fn receive_only_member_keeps_can_subscribe() {
        let json = JOIN_JSON.replace(
            r#""published": [
                { "type": "livekit", "livekit_service_url": "https://sfu.example.com/jwt" }
            ],"#,
            r#""published": [],"#,
        );
        let c = candidate(&json);
        assert!(c.is_join());
        assert!(c.member.transports.published.is_empty());
        assert_eq!(
            c.member.transports.can_subscribe,
            vec!["livekit".to_owned()]
        );
    }

    #[test]
    fn content_not_an_object_or_member_not_an_object_is_none() {
        assert!(convert(r#""just a string""#).is_none());
        assert!(
            convert(r#"{ "slot_id": "m.call#ROOM", "msc4354_sticky_key": "k", "member": "nope" }"#)
                .is_none()
        );
        assert!(convert(r#"{ "slot_id": "m.call#ROOM", "msc4354_sticky_key": "k" }"#).is_none());
        // No sticky key at all → not a membership.
        assert!(convert(r#"{ "slot_id": "m.call#ROOM", "member": { "id": "abc" } }"#).is_none());
    }

    #[test]
    fn leave_carries_its_reason() {
        let c = candidate(
            r#"{ "slot_id": "m.call#ROOM",
                 "member": { "id": "abc", "membership": "leave" },
                 "leave_reason": { "code": "slot_closed", "reason": "bye" },
                 "msc4354_sticky_key": "abc" }"#,
        );
        assert_eq!(c.membership, CandidateMembership::Leave);
        assert_eq!(
            c.leave_reason,
            Some(LeaveReason {
                code: "slot_closed".into(),
                reason: Some("bye".into())
            })
        );
        // Transport- and application-defined codes survive as-is.
        let c = candidate(
            r#"{ "slot_id": "m.call#ROOM", "member": { "id": "abc", "membership": "leave" },
                 "leave_reason": { "code": "ice_failed" }, "msc4354_sticky_key": "abc" }"#,
        );
        assert_eq!(c.leave_reason.unwrap().code, "ice_failed");
    }

    #[test]
    fn origin_passes_through_untouched() {
        let content: Value = serde_json::from_str(JOIN_JSON).unwrap();
        for origin in [
            encrypted("D"),
            EventOrigin::Encrypted {
                sender_device_id: None,
            },
            EventOrigin::Cleartext,
            EventOrigin::Unknown,
        ] {
            match member_candidate(&event_with(content.clone(), origin.clone())) {
                Some(Msc4143Conversion::Candidate { candidate, .. }) => {
                    assert_eq!(candidate.origin, origin)
                }
                other => panic!("{other:?}"),
            }
        }
    }

    // -- removals ------------------------------------------------------------

    #[test]
    fn bare_sticky_key_leave_is_a_removal() {
        assert_eq!(
            convert(r#"{ "msc4354_sticky_key": "41065006-4d3e-49ab-8c7a-3c8471ef6bec" }"#),
            Some(Msc4143Conversion::Removal {
                sticky_key: "41065006-4d3e-49ab-8c7a-3c8471ef6bec".into()
            })
        );
        // Under the stable spelling too.
        assert_eq!(
            convert(r#"{ "sticky_key": "abc" }"#),
            Some(Msc4143Conversion::Removal {
                sticky_key: "abc".into()
            })
        );
    }

    /// Content that is neither a leave nor a membership is simply not one.
    #[test]
    fn content_missing_everything_is_left_to_fail_normally() {
        assert!(convert(r#"{ "member": { "id": "abc" } }"#).is_none());
    }
}
