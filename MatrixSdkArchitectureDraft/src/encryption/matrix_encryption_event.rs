//! `m.rtc.encryption_key` on the wire (MSC4143): build the outbound content,
//! parse inbound content, and the base64 conventions. The legacy Element
//! Call dialect lives in [`super::legacy_element_call`] and is reached from
//! here through one dispatch arm each way.

use crate::driver::ToDeviceMessage;
use crate::encryption::{KeyOrigin, MediaKey, ReceivedEncryptionKey};
use crate::session::ElementCallCompat;
use crate::types::EventOrigin;
use base64::Engine;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use serde_json::{Value, json};

pub const KEY_EVENT_TYPE: &str = "m.rtc.encryption_key";
pub const KEY_EVENT_TYPE_UNSTABLE: &str = "org.matrix.msc4143.rtc.encryption_key";

/// MSC4143 `format` of the `media_key` — 0: raw key bytes, base64.
pub const KEY_FORMAT: u64 = 0;

/// Both spellings inbound, plus the legacy dialect's type.
pub fn is_key_event_type(event_type: &str) -> bool {
    event_type == KEY_EVENT_TYPE
        || event_type == KEY_EVENT_TYPE_UNSTABLE
        || event_type == super::legacy_element_call::LEGACY_KEY_EVENT_TYPE
}

/// The event type our key messages go out as, per compat mode.
pub fn outbound_event_type(compat: ElementCallCompat) -> &'static str {
    match compat {
        ElementCallCompat::StateEvents => super::legacy_element_call::LEGACY_KEY_EVENT_TYPE,
        // Deployed clients read the unstable spelling.
        ElementCallCompat::Off | ElementCallCompat::StickyEvents => {
            crate::types::wire_event_type(KEY_EVENT_TYPE)
        }
    }
}

/// Padded standard base64 out (what matrix-js-sdk emits) ...
pub fn encode_key(key: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(key)
}

/// ... and padding-indifferent in: the shipped Rust core says "unpadded" in a
/// comment while writing padded, so accept both.
pub fn decode_key(s: &str) -> Result<Vec<u8>, WireError> {
    const LENIENT: GeneralPurpose = GeneralPurpose::new(
        &alphabet::STANDARD,
        GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
    );
    LENIENT
        .decode(s.trim_end_matches('='))
        .map_err(|e| WireError::InvalidBase64(e.to_string()))
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum WireError {
    #[error("missing or malformed field `{0}`")]
    MissingField(&'static str),
    #[error("key index must be 0..=255")]
    InvalidIndex,
    #[error("key is not base64: {0}")]
    InvalidBase64(String),
    #[error("not a media key event type: {0}")]
    NotAKeyEvent(String),
}

/// Content of one key message, dialect-independent.
#[derive(Clone, Debug, PartialEq)]
pub struct ParsedKeyMessage {
    pub room_id: String,
    pub member_id: String,
    pub index: u8,
    pub key: Vec<u8>,
}

/// Outbound content for `key`, in the requested dialect.
pub fn build_content(
    compat: ElementCallCompat,
    room_id: &str,
    slot_id: &str,
    own_member_id: &str,
    own_device_id: &str,
    key: &MediaKey,
    now_ms: u64,
) -> Value {
    match compat {
        ElementCallCompat::StateEvents => super::legacy_element_call::build_content(
            room_id,
            slot_id,
            own_member_id,
            own_device_id,
            key,
            now_ms,
        ),
        ElementCallCompat::Off | ElementCallCompat::StickyEvents => json!({
            "room_id": room_id,
            "member_id": own_member_id,
            "media_key": { "index": key.index, "key": encode_key(&key.key) },
            "format": KEY_FORMAT,
        }),
    }
}

/// Parse MSC4143 content (`room_id`, `member_id`, `media_key.{index,key}`).
/// `format` is not checked: 0 is the only format and absent means 0.
pub fn parse_content(content: &Value) -> Result<ParsedKeyMessage, WireError> {
    let room_id = content
        .get("room_id")
        .and_then(Value::as_str)
        .ok_or(WireError::MissingField("room_id"))?;
    let member_id = content
        .get("member_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or(WireError::MissingField("member_id"))?;
    let media_key = content
        .get("media_key")
        .ok_or(WireError::MissingField("media_key"))?;
    let index = media_key
        .get("index")
        .and_then(Value::as_u64)
        .ok_or(WireError::MissingField("media_key.index"))?;
    let index = u8::try_from(index).map_err(|_| WireError::InvalidIndex)?;
    let key = media_key
        .get("key")
        .and_then(Value::as_str)
        .ok_or(WireError::MissingField("media_key.key"))?;
    Ok(ParsedKeyMessage {
        room_id: room_id.to_owned(),
        member_id: member_id.to_owned(),
        index,
        key: decode_key(key)?,
    })
}

/// One inbound to-device message -> a key awaiting verification. Dispatches
/// on the event type; the origin is mapped from the host's decryption
/// metadata, never from the payload.
pub fn to_received(msg: &ToDeviceMessage) -> Result<ReceivedEncryptionKey, WireError> {
    let parsed = if msg.event_type == super::legacy_element_call::LEGACY_KEY_EVENT_TYPE {
        super::legacy_element_call::parse_content(&msg.sender, &msg.content)?
    } else if msg.event_type == KEY_EVENT_TYPE || msg.event_type == KEY_EVENT_TYPE_UNSTABLE {
        parse_content(&msg.content)?
    } else {
        return Err(WireError::NotAKeyEvent(msg.event_type.clone()));
    };
    let origin = match &msg.origin {
        EventOrigin::Encrypted { sender_device_id } => KeyOrigin::Encrypted {
            sender_device_id: sender_device_id.clone(),
            sender_cross_signed: msg.sender_cross_signed,
        },
        EventOrigin::Cleartext => KeyOrigin::Cleartext,
        EventOrigin::Unknown => KeyOrigin::Unknown,
    };
    Ok(ReceivedEncryptionKey {
        room_id: parsed.room_id,
        member_id: parsed.member_id,
        sender_user_id: msg.sender.clone(),
        origin,
        key: parsed.key,
        index: parsed.index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> MediaKey {
        MediaKey {
            key: vec![7u8; 32],
            index: 3,
            creation_ts_ms: 1,
        }
    }

    #[test]
    fn outbound_content_declares_format_0_and_no_version() {
        let c = build_content(
            ElementCallCompat::Off,
            "!r",
            "m.call#",
            "M1",
            "DEV",
            &key(),
            0,
        );
        assert_eq!(c["format"], 0);
        assert!(c.get("version").is_none());
        assert_eq!(c["room_id"], "!r");
        assert_eq!(c["member_id"], "M1");
        assert_eq!(c["media_key"]["index"], 3);
        assert_eq!(c["media_key"]["key"], encode_key(&[7u8; 32]));
        assert_eq!(parse_content(&c).unwrap().key, vec![7u8; 32]);
    }

    #[test]
    fn outbound_type_is_the_unstable_spelling() {
        assert_eq!(
            outbound_event_type(ElementCallCompat::Off),
            KEY_EVENT_TYPE_UNSTABLE
        );
        assert_eq!(
            outbound_event_type(ElementCallCompat::StateEvents),
            super::super::legacy_element_call::LEGACY_KEY_EVENT_TYPE
        );
    }

    #[test]
    fn stable_and_unstable_event_types_parse_inbound() {
        for t in [KEY_EVENT_TYPE, KEY_EVENT_TYPE_UNSTABLE] {
            let msg = ToDeviceMessage {
                event_type: t.into(),
                sender: "@a:x".into(),
                content: build_content(ElementCallCompat::Off, "!r", "", "M1", "D", &key(), 0),
                origin: EventOrigin::Encrypted {
                    sender_device_id: Some("D".into()),
                },
                sender_cross_signed: Some(true),
            };
            let r = to_received(&msg).unwrap();
            assert_eq!(r.member_id, "M1");
            assert_eq!(r.index, 3);
            assert!(matches!(r.origin, KeyOrigin::Encrypted { .. }));
        }
        assert!(is_key_event_type(KEY_EVENT_TYPE) && is_key_event_type(KEY_EVENT_TYPE_UNSTABLE));
        assert!(!is_key_event_type("m.room.message"));
    }

    #[test]
    fn unpadded_and_padded_base64_both_decode() {
        let bytes = vec![1u8; 16];
        let padded = encode_key(&bytes);
        assert!(padded.ends_with('='));
        let unpadded = padded.trim_end_matches('=');
        assert_eq!(decode_key(&padded).unwrap(), bytes);
        assert_eq!(decode_key(unpadded).unwrap(), bytes);
        assert!(decode_key("***").is_err());
    }

    #[test]
    fn index_above_255_is_rejected() {
        let mut c = build_content(ElementCallCompat::Off, "!r", "", "M1", "D", &key(), 0);
        c["media_key"]["index"] = json!(256);
        assert_eq!(parse_content(&c), Err(WireError::InvalidIndex));
    }
}
