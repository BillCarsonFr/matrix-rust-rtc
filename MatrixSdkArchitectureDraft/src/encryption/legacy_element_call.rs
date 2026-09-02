//! The pre-2026 Element Call key message, `io.element.call.encryption_keys`
//! (`ElementCallCompat::StateEvents`). Delete-by date: this file plus the
//! two dispatch arms in [`super::matrix_encryption_event`].
//!
//! Differences to MSC4143: the type; `keys` (an object today, historically an
//! array — highest index wins); `member.id` plus a `claimed_device_id` and
//! a top-level `device_id` (the dialect derives identities as
//! `{user}:{device}`); `call_id` + `session` derived from the slot id; a
//! wall-clock `sent_ts` which we drop on read (receipt time is what the
//! outdated filter uses); 16-byte keys.

use super::MediaKey;
use super::matrix_encryption_event::{ParsedKeyMessage, WireError, decode_key, encode_key};
use serde_json::{Value, json};

pub const LEGACY_KEY_EVENT_TYPE: &str = "io.element.call.encryption_keys";

/// `{application}#{call_id}` -> `(application, call_id)`; `#ROOM` and a
/// missing separator both mean the room-scoped call (empty call id).
pub(crate) fn legacy_session(slot_id: &str) -> (&str, &str) {
    match slot_id.split_once('#') {
        Some((application, "ROOM")) => (application, ""),
        Some((application, call_id)) => (application, call_id),
        None => (slot_id, ""),
    }
}

pub(crate) fn participant_identity(user_id: &str, device_id: &str) -> String {
    format!("{user_id}:{device_id}")
}

pub(crate) fn build_content(
    room_id: &str,
    slot_id: &str,
    own_member_id: &str,
    own_device_id: &str,
    key: &MediaKey,
    now_ms: u64,
) -> Value {
    let (application, call_id) = legacy_session(slot_id);
    json!({
        "keys": { "index": key.index, "key": encode_key(&key.key) },
        "member": { "id": own_member_id, "claimed_device_id": own_device_id },
        "device_id": own_device_id,
        "room_id": room_id,
        "call_id": call_id,
        "sent_ts": now_ms,
        "session": { "application": application, "call_id": call_id, "scope": "m.room" },
    })
}

pub(crate) fn parse_content(sender: &str, content: &Value) -> Result<ParsedKeyMessage, WireError> {
    let room_id = content
        .get("room_id")
        .and_then(Value::as_str)
        .ok_or(WireError::MissingField("room_id"))?;
    let keys = content.get("keys").ok_or(WireError::MissingField("keys"))?;
    // Object today, array historically: take the entry with the highest index.
    let entry = match keys {
        Value::Array(entries) => entries
            .iter()
            .filter(|e| e.get("index").and_then(Value::as_u64).is_some())
            .max_by_key(|e| e["index"].as_u64().unwrap_or(0))
            .ok_or(WireError::MissingField("keys[]"))?,
        other => other,
    };
    let index = entry
        .get("index")
        .and_then(Value::as_u64)
        .ok_or(WireError::MissingField("keys.index"))?;
    let index = u8::try_from(index).map_err(|_| WireError::InvalidIndex)?;
    let key = entry
        .get("key")
        .and_then(Value::as_str)
        .ok_or(WireError::MissingField("keys.key"))?;
    let member_id = match content
        .pointer("/member/id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        Some(id) => id.to_owned(),
        None => {
            let device_id = content
                .get("device_id")
                .and_then(Value::as_str)
                .ok_or(WireError::MissingField("device_id"))?;
            participant_identity(sender, device_id)
        }
    };
    Ok(ParsedKeyMessage {
        room_id: room_id.to_owned(),
        member_id,
        index,
        key: decode_key(key)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_content_carries_member_device_call_id_session_and_sent_ts() {
        let key = MediaKey { key: vec![1u8; 16], index: 2, creation_ts_ms: 5 };
        let c = build_content("!r", "m.call#abc", "@u:x:DEV", "DEV", &key, 1234);
        assert_eq!(c["keys"]["index"], 2);
        assert_eq!(c["member"]["id"], "@u:x:DEV");
        assert_eq!(c["member"]["claimed_device_id"], "DEV");
        assert_eq!(c["device_id"], "DEV");
        assert_eq!(c["call_id"], "abc");
        assert_eq!(c["sent_ts"], 1234);
        assert_eq!(c["session"]["application"], "m.call");
        assert_eq!(c["session"]["scope"], "m.room");
        assert_eq!(legacy_session("m.call#ROOM"), ("m.call", ""));
        assert_eq!(legacy_session("m.call"), ("m.call", ""));
    }

    #[test]
    fn legacy_keys_object_and_array_forms_both_parse_highest_index_wins() {
        let object = json!({
            "room_id": "!r", "device_id": "D", "member": { "id": "M" },
            "keys": { "index": 4, "key": encode_key(&[9u8; 16]) }
        });
        assert_eq!(parse_content("@u:x", &object).unwrap().index, 4);
        let array = json!({
            "room_id": "!r", "device_id": "D", "member": { "id": "M" },
            "keys": [
                { "index": 1, "key": encode_key(&[1u8; 16]) },
                { "index": 6, "key": encode_key(&[6u8; 16]) }
            ]
        });
        let p = parse_content("@u:x", &array).unwrap();
        assert_eq!((p.index, p.key), (6, vec![6u8; 16]));
    }

    #[test]
    fn legacy_member_id_falls_back_to_user_colon_device() {
        let c = json!({
            "room_id": "!r", "device_id": "D",
            "keys": { "index": 0, "key": encode_key(&[1u8; 16]) }
        });
        assert_eq!(parse_content("@u:x", &c).unwrap().member_id, "@u:x:D");
    }
}
