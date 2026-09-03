//! `ElementCallCompat::StickyEvents` write side: render our spec content in
//! the 2025 Element Call sticky dialect. Delete with that generation. The
//! read side lives in `session::convert::msc4143`.

use super::OwnIdentity;
use super::wire::is_leave;
use serde_json::{Map, Value};

/// A join is rewritten **additively** (every spec field stays; the legacy
/// ones are added beside it); a leave is replaced **wholesale** by a bare
/// sticky key — Element Call has no `membership` field and rejects a leave
/// without `rtc_transports`, while padding one would read as "joined,
/// publishing nothing". A spec-current peer cannot parse a bare key either
/// and treats it as departed, so the outcome is the same.
pub(crate) fn rewrite(spec: &Value, own: &OwnIdentity) -> Value {
    if is_leave(spec) {
        return match spec.get("msc4354_sticky_key") {
            Some(key) => serde_json::json!({ "msc4354_sticky_key": key }),
            None => spec.clone(),
        };
    }
    let mut content = spec.clone();
    let Some(object) = content.as_object_mut() else {
        return content;
    };
    if !object.contains_key("slot_id") {
        return content;
    }
    if !object.contains_key("rtc_transports")
        && let Some(published) = object
            .get("transports")
            .and_then(|t| t.get("published"))
            .and_then(Value::as_array)
            .filter(|p| !p.is_empty())
            .cloned()
    {
        object.insert("rtc_transports".to_owned(), Value::Array(published));
    }
    object
        .entry("versions")
        .or_insert_with(|| Value::Array(Vec::new()));
    let member = object
        .entry("member")
        .or_insert_with(|| Value::Object(Map::new()));
    if let Some(member) = member.as_object_mut() {
        member
            .entry("user_id")
            .or_insert_with(|| Value::String(own.user_id.clone()));
        member
            .entry("device_id")
            .or_insert_with(|| Value::String(own.device_id.clone()));
    }
    content
}

#[cfg(test)]
mod tests {
    use super::super::wire::{join_content, leave_content};
    use super::*;
    use crate::types::{LeaveReason, RtcTransport, TransportIntent};
    use serde_json::json;

    fn own() -> OwnIdentity {
        OwnIdentity {
            user_id: "@me:x".into(),
            device_id: "DEV".into(),
        }
    }

    fn join() -> Value {
        join_content(
            "m.call#ROOM",
            "m-1",
            "m.call",
            None,
            &TransportIntent::Publish(RtcTransport {
                transport_type: "livekit".into(),
                properties: json!({ "livekit_service_url": "https://lk" }),
            }),
        )
    }

    #[test]
    fn legacy_join_is_additive_and_keeps_every_spec_field() {
        let spec = join();
        let legacy = rewrite(&spec, &own());
        for (key, value) in spec.as_object().unwrap() {
            if key == "member" {
                assert_eq!(legacy["member"]["id"], value["id"]);
                assert_eq!(legacy["member"]["membership"], value["membership"]);
            } else {
                assert_eq!(&legacy[key], value, "{key}");
            }
        }
    }

    #[test]
    fn legacy_join_mirrors_published_into_rtc_transports() {
        let legacy = rewrite(&join(), &own());
        assert_eq!(
            legacy["rtc_transports"],
            json!([{ "type": "livekit", "livekit_service_url": "https://lk" }])
        );
    }

    #[test]
    fn legacy_join_adds_versions_user_id_and_device_id_only_when_absent() {
        let legacy = rewrite(&join(), &own());
        assert_eq!(legacy["versions"], json!([]));
        assert_eq!(legacy["member"]["user_id"], "@me:x");
        assert_eq!(legacy["member"]["device_id"], "DEV");
        let mut pre = join();
        pre["member"]["device_id"] = json!("OTHER");
        pre["versions"] = json!(["1"]);
        let legacy = rewrite(&pre, &own());
        assert_eq!(legacy["member"]["device_id"], "OTHER");
        assert_eq!(legacy["versions"], json!(["1"]));
    }

    #[test]
    fn receive_only_join_gets_no_rtc_transports() {
        let spec = join_content(
            "s",
            "m",
            "m.call",
            None,
            &TransportIntent::ReceiveOnly {
                can_subscribe: vec!["livekit".into()],
            },
        );
        assert!(rewrite(&spec, &own()).get("rtc_transports").is_none());
    }

    #[test]
    fn legacy_leave_is_a_bare_sticky_key() {
        let leave = leave_content("m.call#ROOM", "m-1", &LeaveReason::leave());
        assert_eq!(
            rewrite(&leave, &own()),
            json!({ "msc4354_sticky_key": "m-1" })
        );
        let delayed = leave_content("m.call#ROOM", "m-1", &LeaveReason::delayed_leave());
        assert_eq!(
            rewrite(&delayed, &own()),
            json!({ "msc4354_sticky_key": "m-1" })
        );
    }
}
