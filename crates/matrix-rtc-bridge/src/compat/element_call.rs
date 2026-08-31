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

//! The pre-2026 Element Call wire format, translated to and from current
//! MSC4143.
//!
//! Deliberately depends on nothing but `serde_json`: no `matrix-sdk` type, no
//! `matrix-rtc-core` type. Everything is a plain JSON-in/JSON-out function, so
//! the whole dialect is unit-testable without a homeserver and deleting it
//! cannot break a signature anywhere else.
//!
//! # The format, field by field
//!
//! | | Element Call today | MSC4143 today |
//! |---|---|---|
//! | member | `{user_id, device_id, id}` | `{id, membership}` |
//! | leave | content is `{msc4354_sticky_key}` alone | full content, `membership: "leave"` + `leave_reason` |
//! | transports | flat `rtc_transports` array | `transports.{published,can_subscribe}` |
//! | extras | `versions`, `m.relation` | gone from the spec |
//! | key type | `io.element.call.encryption_keys` | `m.rtc.encryption_key` |
//! | key content | `{keys, member, room_id, session, sent_ts}` | `{room_id, member_id, media_key, format}` |
//!
//! # The device id, and why it is load-bearing here
//!
//! Element Call runs as a widget, and the widget API gives it no access to
//! decryption metadata. It therefore cannot learn the device that sent a member
//! event, which is where MSC4143 (since the 2026 rewrite) says a member's device
//! comes from. Two consequences shape this module:
//!
//! - It cannot address a to-device key to us unless our member event *states*
//!   our device, so [`ElementCallDialect::rewrite_member_content`] puts
//!   `member.user_id` / `member.device_id` back on the wire. Without them,
//!   Element Call has no way to send us its media key and its media never
//!   decrypts for us.
//! - Its own member events carry the same self-asserted `member.device_id`.
//!   Where we cannot do better — a member event that reached us unencrypted —
//!   [`claimed_device_id`] surfaces it so the core has *some* device to bind the
//!   member to. That claim is never preferred over an authenticated device; see
//!   `sdk::snapshot`.

// `std::time::SystemTime::now()` panics on wasm32-unknown-unknown; web-time's
// is the same API over `Date.now()`.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use web_time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};

/// The pre-2026 to-device message type carrying media keys.
pub const LEGACY_KEY_EVENT_TYPE: &str = "io.element.call.encryption_keys";

/// The core's event type for a membership, before the bridge maps it to the
/// unstable id that goes on the wire.
const MEMBER_EVENT_TYPE: &str = "m.rtc.member";

/// The event types the core may hand us for a media key, stable and unstable.
const KEY_EVENT_TYPES: [&str; 2] = [
    "m.rtc.encryption_key",
    "org.matrix.msc4143.rtc.encryption_key",
];

/// The core's event type for an MSC4075 notification, before the bridge maps it
/// to the unstable id that goes on the wire.
const NOTIFICATION_EVENT_TYPE: &str = "m.rtc.notification";

/// Wall-clock milliseconds since the Unix epoch, for the legacy `sent_ts`.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Whether a normalised member content can be parsed as MSC4143 content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberContent {
    /// The content is MSC4143-shaped (either already, or after normalisation)
    /// and should be parsed as usual.
    Usable,
    /// A pre-2026 leave: the content is nothing but a sticky key.
    ///
    /// There is no slot id in it, so it cannot be attributed to a session and
    /// there is nothing useful to hand the core. Dropping it is the right
    /// outcome and not a loss: the bridge replaces the whole live sticky set on
    /// every tick, so an entry that contributes no event is exactly an entry
    /// whose member has left.
    BareLeave,
}

/// Rewrite a pre-2026 `m.rtc.member` content in place so the rest of the stack
/// only ever sees current MSC4143.
///
/// Safe to run on every inbound member event, which is why the bridge does not
/// gate it on a flag: every rule below fires only when the modern field is
/// absent *and* its legacy counterpart is present, so spec-shaped content comes
/// out untouched.
///
/// What it does:
///
/// - infers `member.membership = "join"` when the member object states no
///   membership but the content is join-shaped (it names an application);
/// - lifts a flat `rtc_transports` array into `transports.published`, declaring
///   the same transport types under `can_subscribe` — an Element Call client
///   publishing on LiveKit can receive LiveKit;
/// - recognises the bare-sticky-key leave and reports it as
///   [`MemberContent::BareLeave`].
///
/// `versions` and `m.relation` need no handling: they are simply ignored on
/// deserialization.
pub fn normalize_member_content(content: &mut Value) -> MemberContent {
    let Some(object) = content.as_object_mut() else {
        // Not an object at all. Let the normal parse fail and report it.
        return MemberContent::Usable;
    };

    if !object.contains_key("slot_id") {
        let has_sticky_key =
            object.contains_key("msc4354_sticky_key") || object.contains_key("sticky_key");
        if has_sticky_key {
            return MemberContent::BareLeave;
        }
        // Missing a slot id for some other reason; the normal parse error is a
        // better diagnostic than anything we could invent here.
        return MemberContent::Usable;
    }

    lift_rtc_transports(object);
    infer_membership(object);

    MemberContent::Usable
}

/// `rtc_transports: [...]` → `transports: {published: [...], can_subscribe: [...]}`.
fn lift_rtc_transports(object: &mut Map<String, Value>) {
    if object.contains_key("transports") {
        return;
    }
    let Some(published) = object
        .get("rtc_transports")
        .and_then(Value::as_array)
        .cloned()
    else {
        return;
    };
    if published.is_empty() {
        return;
    }

    let mut can_subscribe: Vec<Value> = Vec::new();
    for transport in &published {
        let Some(transport_type) = transport.get("type").and_then(Value::as_str) else {
            continue;
        };
        let entry = Value::String(transport_type.to_owned());
        if !can_subscribe.contains(&entry) {
            can_subscribe.push(entry);
        }
    }

    object.insert(
        "transports".to_owned(),
        json!({ "published": published, "can_subscribe": can_subscribe }),
    );
}

/// Give a legacy member object the `membership` MSC4143 now requires.
///
/// Only `join` is ever inferred. A legacy content that does not look like a join
/// is left without a membership, which the core already reads as left — writing
/// `"leave"` in would be inventing a statement the sender never made.
fn infer_membership(object: &mut Map<String, Value>) {
    let names_application = object
        .get("application")
        .and_then(|application| application.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|application_type| !application_type.is_empty());
    if !names_application {
        return;
    }

    let Some(member) = object.get_mut("member").and_then(Value::as_object_mut) else {
        return;
    };
    if member.contains_key("membership") {
        return;
    }
    let names_member = member
        .get("id")
        .and_then(Value::as_str)
        .is_some_and(|id| !id.is_empty());
    if !names_member {
        return;
    }

    member.insert("membership".to_owned(), Value::String("join".to_owned()));
}

/// Whether this content states `member.membership = "leave"`.
///
/// Shared with [`super::element_call_state`]: both legacy generations signal a
/// departure by replacing the content wholesale, so both need to recognise the
/// spec leave they are translating *from*.
pub(crate) fn is_leave(content: &Value) -> bool {
    content
        .get("member")
        .and_then(|member| member.get("membership"))
        .and_then(Value::as_str)
        == Some("leave")
}

/// The sticky key this content carries, under either spelling.
fn sticky_key_of(content: &Value) -> Option<&str> {
    content
        .get("msc4354_sticky_key")
        .or_else(|| content.get("sticky_key"))
        .and_then(Value::as_str)
}

/// The device a member event claims to come from (`member.device_id`).
///
/// Self-asserted and unauthenticated — MSC4143 removed the field for exactly
/// that reason. Use it only where there is no authenticated device to be had,
/// and never in preference to one.
pub fn claimed_device_id(content: &Value) -> Option<String> {
    content
        .get("member")?
        .get("device_id")?
        .as_str()
        .filter(|device_id| !device_id.is_empty())
        .map(str::to_owned)
}

/// The device a legacy key message claims to come from, under either spelling.
///
/// A fallback for when Olm decryption produced no sending device. Self-asserted,
/// so it is never preferred over an authenticated one.
pub fn claimed_key_device_id(content: &Value) -> Option<String> {
    content
        .get("device_id")
        .or_else(|| {
            content
                .get("member")
                .and_then(|member| member.get("claimed_device_id"))
        })?
        .as_str()
        .filter(|device_id| !device_id.is_empty())
        .map(str::to_owned)
}

/// A media key lifted out of a legacy `io.element.call.encryption_keys`
/// to-device message.
///
/// Only the fields the core needs. `sent_ts` is deliberately dropped: the core
/// stamps inbound keys with their receipt time, and a sender-supplied timestamp
/// is not something we would want to order keys by anyway.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyKeyMessage {
    /// Room the key is scoped to.
    pub room_id: String,
    /// The sender's `member.id`, which is how the core binds a key to a
    /// membership.
    pub member_id: String,
    /// The key material, base64.
    pub key_b64: String,
    /// The rolling key index.
    pub key_index: u8,
}

/// Parse a legacy media-key to-device message.
///
/// Returns `None` when a required field is missing, which for this message
/// means it cannot be used at all.
///
/// `sender` is the to-device event's sender, needed only for the pre-sticky
/// generation: that one has no `member` object, and binds the key to
/// `{sender}:{content.device_id}` — the same string
/// [`element_call_state::participant_identity`](super::element_call_state::participant_identity)
/// builds, and the same one the inbound state translation uses as `member.id`. If
/// the two ever diverged the key would be filed against a membership that does
/// not exist, so they share the one function rather than the one format string.
///
/// Both `keys` spellings are accepted: an object (2025) or an array of them
/// (pre-sticky), in which case the **highest** index wins, that being the most
/// recent key the sender is offering.
///
/// Note the key material is 16 bytes here where MSC4143 uses 32. Nothing needs
/// to convert it: both sides feed the raw bytes to the same LiveKit HKDF, so
/// they only have to agree, and the core already accepts either length.
pub fn parse_key_message(sender: &str, content: &Value) -> Option<LegacyKeyMessage> {
    let keys = match content.get("keys")? {
        Value::Array(entries) => entries
            .iter()
            .filter(|entry| entry.get("key").is_some())
            .max_by_key(|entry| entry.get("index").and_then(Value::as_u64).unwrap_or(0))?,
        object => object,
    };

    let member_id = match content.get("member").and_then(|member| member.get("id")) {
        Some(Value::String(member_id)) if !member_id.is_empty() => member_id.clone(),
        _ => {
            let device_id = content.get("device_id")?.as_str()?;
            super::element_call_state::participant_identity(sender, device_id)
        }
    };

    Some(LegacyKeyMessage {
        room_id: content.get("room_id")?.as_str()?.to_owned(),
        member_id,
        key_b64: keys.get("key")?.as_str()?.to_owned(),
        key_index: u8::try_from(keys.get("index")?.as_u64()?).ok()?,
    })
}

/// The outbound half: rewrites what we send so a pre-2026 Element Call can read
/// it.
///
/// Opt-in per call ([`CallOptions::element_call_compat`](crate::CallOptions::element_call_compat)),
/// because unlike the inbound normalisation this changes what every peer sees.
///
/// Member events are rewritten **additively** — the MSC4143 fields all stay put
/// and the legacy ones are added alongside — so one event on the wire is
/// readable by both dialects and a spec-current peer sees exactly what it would
/// have seen anyway. Key messages cannot work that way: the message type is one
/// or the other, so in this mode keys go out in the legacy dialect only.
#[derive(Clone, Debug)]
pub struct ElementCallDialect {
    own_user_id: String,
    own_device_id: String,
    slot_id: String,
}

impl ElementCallDialect {
    /// Builds the dialect for our own membership in `slot_id`.
    ///
    /// The user and device ids are the ones we publish in `member`; the slot id
    /// is needed to reconstruct the legacy `session` object on key messages.
    pub fn new(
        own_user_id: impl Into<String>,
        own_device_id: impl Into<String>,
        slot_id: impl Into<String>,
    ) -> Self {
        Self {
            own_user_id: own_user_id.into(),
            own_device_id: own_device_id.into(),
            slot_id: slot_id.into(),
        }
    }

    /// Whether `event_type` is a membership, and so needs the legacy aliases.
    pub fn is_member_event(event_type: &str) -> bool {
        event_type == MEMBER_EVENT_TYPE || event_type == "org.matrix.msc4143.rtc.member"
    }

    /// Rewrite an MSC4143 `m.rtc.member` content into something a pre-2026
    /// Element Call can read, in place.
    ///
    /// A **join** is rewritten additively: every MSC4143 field stays and the
    /// legacy ones are added beside it, so one event serves both dialects.
    /// `member.user_id` / `member.device_id` are what let Element Call address a
    /// to-device key to us at all (see the module docs); `rtc_transports` is
    /// where it looks for our SFU; `versions` it expects to exist.
    ///
    /// A **leave** cannot be additive. Element Call has no `membership` field —
    /// it signals departure by sending content holding nothing but the sticky
    /// key — and its validator requires `rtc_transports` on anything it does
    /// parse. Handed our spec leave it rejects the event outright; padded with
    /// an empty `rtc_transports` it would read us as *joined* and publishing
    /// nothing, which is a worse ghost than the one we are trying to clear. So a
    /// leave is replaced wholesale with the legacy shape.
    ///
    /// That costs a spec-current peer nothing that matters: it cannot parse a
    /// bare sticky key either, and a member event it cannot parse is one it
    /// already treats as departed. Same outcome, which is what makes this safe
    /// to do in a mode that is explicitly for talking to Element Call.
    pub fn rewrite_member_content(&self, content: &mut Value) {
        if is_leave(content)
            && let Some(sticky_key) = sticky_key_of(content).map(str::to_owned)
        {
            *content = json!({ "msc4354_sticky_key": sticky_key });
            return;
        }

        let Some(object) = content.as_object_mut() else {
            return;
        };

        // Already the legacy leave (or otherwise not a membership we can dress
        // up). Adding a `member` object here would turn a departure back into
        // something that looks like a membership.
        if !object.contains_key("slot_id") {
            return;
        }

        // Mirror `transports.published` back into the flat array. A leave
        // carries no transports, and then neither does the alias.
        if !object.contains_key("rtc_transports")
            && let Some(published) = object
                .get("transports")
                .and_then(|transports| transports.get("published"))
                .and_then(Value::as_array)
                .filter(|published| !published.is_empty())
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
                .or_insert_with(|| Value::String(self.own_user_id.clone()));
            member
                .entry("device_id")
                .or_insert_with(|| Value::String(self.own_device_id.clone()));
        }
    }

    /// Translate an outbound media-key to-device message into the legacy type
    /// and shape.
    ///
    /// Returns `None` for any other message type, which the caller then sends
    /// untouched.
    pub fn rewrite_key_message(
        &self,
        message_type: &str,
        content: &Value,
    ) -> Option<(String, Value)> {
        rewrite_key_message(&self.own_device_id, &self.slot_id, message_type, content)
    }
}

/// Translate an outbound media-key to-device message into the legacy type and
/// shape.
///
/// Returns `None` for any other message type, which the caller then sends
/// untouched.
///
/// Shared verbatim by both legacy generations. They differ only in what
/// `member_id` the core happens to be carrying, and in the state dialect that id
/// is already the legacy `membershipID`, so it comes out correct with no special
/// case.
///
/// `keys` is an **object**, for both. An earlier version of this made it an
/// array for the pre-sticky dialect on the assumption that the two generations
/// disagreed about it; a captured key message from a real pre-sticky Element Call
/// settled it — that build sends `keys` as an object with a `member.id` beside
/// it, exactly like its successor. The key-message format simply did not change
/// when the membership carrier did.
///
/// The remaining fields are the union of both observed shapes — a top-level
/// `device_id` and `call_id` beside the `member` and `session` objects. Those do
/// not collide, so a reader of either generation finds what it needs and ignores
/// the rest.
pub fn rewrite_key_message(
    own_device_id: &str,
    slot_id: &str,
    message_type: &str,
    content: &Value,
) -> Option<(String, Value)> {
    if !KEY_EVENT_TYPES.contains(&message_type) {
        return None;
    }

    let media_key = content.get("media_key")?;
    let index = media_key.get("index")?;
    let key = media_key.get("key")?;
    let (application, call_id) = legacy_session(slot_id);

    let legacy = json!({
        "keys": { "index": index, "key": key },
        "member": {
            "id": content.get("member_id")?,
            // Self-asserted, and the only device id Element Call can work
            // with — it cannot read decryption metadata.
            "claimed_device_id": own_device_id,
        },
        // The pre-sticky generation has no `member` object: it binds the key to
        // `{sender}:{content.device_id}` and looks for the device here.
        "device_id": own_device_id,
        "room_id": content.get("room_id")?,
        "call_id": call_id,
        "sent_ts": now_ms(),
        "session": {
            "application": application,
            "call_id": call_id,
            "scope": "m.room",
        },
    });

    Some((LEGACY_KEY_EVENT_TYPE.to_owned(), legacy))
}

/// Flatten an MSC4075 notification into the shape Element Call sends and reads.
///
/// Returns `None` for any other event type, which the caller then sends
/// untouched.
///
/// Element Call predates the `application` block: it puts `notification_type`,
/// `sender_ts`, `lifetime` and `m.call.intent` straight in the content and sends
/// no `m.text` at all. The event type is unchanged — both generations already
/// agree on `org.matrix.msc4075.rtc.notification` — so only the content moves.
///
/// `device_id` is dropped rather than hoisted: it exists for ring
/// acknowledgements, which this generation neither sends nor reads, and a
/// top-level `device_id` means something else entirely in its key messages.
pub fn flatten_notification_content(event_type: &str, content: &Value) -> Option<Value> {
    if event_type != NOTIFICATION_EVENT_TYPE {
        return None;
    }

    let application = content.get("application")?.as_object()?;
    let mut flat = Map::new();
    if let Some(mentions) = content.get("m.mentions") {
        flat.insert("m.mentions".to_owned(), mentions.clone());
    }
    for field in [
        "notification_type",
        "sender_ts",
        "lifetime",
        "m.call.intent",
    ] {
        if let Some(value) = application.get(field) {
            flat.insert(field.to_owned(), value.clone());
        }
    }
    if let Some(relates_to) = content.get("m.relates_to") {
        flat.insert("m.relates_to".to_owned(), relates_to.clone());
    }

    Some(Value::Object(flat))
}

/// Reconstruct the legacy `{application, call_id}` pair from a slot id.
///
/// MSC4143 folded the old `{application, call_id, scope}` triple into a single
/// slot id of the form `<application>#<call_id>`, where the sentinel `ROOM` is
/// the room-scoped session that used to be an empty `call_id`.
///
/// A free function, and shared with [`super::element_call_state`], because that
/// generation needs the same split in three places — the member content, the
/// state key and the key message's `session` object — and a second copy of the
/// sentinel rule is a second chance to get it wrong. Getting it wrong puts a
/// peer in a session we are not in, and the roster is silently empty.
pub(crate) fn legacy_session(slot_id: &str) -> (&str, &str) {
    match slot_id.split_once('#') {
        Some((application, "ROOM")) => (application, ""),
        Some((application, call_id)) => (application, call_id),
        // No separator: treat the whole thing as the application, which is
        // the room-scoped case again.
        None => (slot_id, ""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A join exactly as observed from Element Call on the JS SDK.
    const LEGACY_JOIN: &str = r#"{
        "application": { "type": "m.call", "m.call.intent": "video" },
        "slot_id": "m.call#ROOM",
        "rtc_transports": [
            {
                "type": "livekit",
                "livekit_service_url": "https://mrtc.example.io"
            }
        ],
        "member": {
            "device_id": "V5cP8FErcB",
            "user_id": "@alice:example.io",
            "id": "41065006-4d3e-49ab-8c7a-3c8471ef6bec"
        },
        "versions": [],
        "msc4354_sticky_key": "41065006-4d3e-49ab-8c7a-3c8471ef6bec"
    }"#;

    /// The matching leave: content is the sticky key and nothing else.
    const LEGACY_LEAVE: &str =
        r#"{ "msc4354_sticky_key": "41065006-4d3e-49ab-8c7a-3c8471ef6bec" }"#;

    /// A current-spec join, which must survive normalisation untouched.
    const SPEC_JOIN: &str = r#"{
        "slot_id": "m.call#ROOM",
        "member": { "id": "xyzABCDEF0123", "membership": "join" },
        "application": { "type": "m.call" },
        "transports": {
            "published": [{ "type": "livekit", "livekit_service_url": "https://sfu.example.com" }],
            "can_subscribe": ["livekit"]
        },
        "msc4354_sticky_key": "xyzABCDEF0123"
    }"#;

    fn normalized(json: &str) -> (MemberContent, Value) {
        let mut value: Value = serde_json::from_str(json).expect("valid json");
        let outcome = normalize_member_content(&mut value);
        (outcome, value)
    }

    #[test]
    fn legacy_join_gains_a_membership_and_typed_transports() {
        let (outcome, value) = normalized(LEGACY_JOIN);

        assert_eq!(outcome, MemberContent::Usable);
        assert_eq!(value.pointer("/member/membership").unwrap(), "join");
        assert_eq!(
            value
                .pointer("/transports/published/0/livekit_service_url")
                .unwrap(),
            "https://mrtc.example.io"
        );
        assert_eq!(
            value.pointer("/transports/can_subscribe/0").unwrap(),
            "livekit"
        );
        // The legacy fields stay where they were; nothing downstream reads them.
        assert!(value.get("rtc_transports").is_some());
    }

    /// The whole point of the inbound half being unconditional: it must be a
    /// no-op on spec-shaped content.
    #[test]
    fn spec_shaped_join_is_untouched() {
        let before: Value = serde_json::from_str(SPEC_JOIN).unwrap();
        let (outcome, after) = normalized(SPEC_JOIN);

        assert_eq!(outcome, MemberContent::Usable);
        assert_eq!(before, after);
    }

    /// A spec leave states `membership: "leave"`, and no amount of legacy
    /// inference may promote it to a join.
    #[test]
    fn spec_leave_is_untouched() {
        let json = SPEC_JOIN.replace(r#""membership": "join""#, r#""membership": "leave""#);
        let before: Value = serde_json::from_str(&json).unwrap();
        let (_, after) = normalized(&json);

        assert_eq!(before, after);
    }

    #[test]
    fn bare_sticky_key_leave_is_reported_for_dropping() {
        let (outcome, _) = normalized(LEGACY_LEAVE);
        assert_eq!(outcome, MemberContent::BareLeave);

        // Under the stable spelling too.
        let (outcome, _) = normalized(r#"{ "sticky_key": "abc" }"#);
        assert_eq!(outcome, MemberContent::BareLeave);
    }

    /// Content that is neither a legacy leave nor parseable must not be
    /// disguised as one — the normal parse error is the better diagnostic.
    #[test]
    fn content_missing_everything_is_left_to_fail_normally() {
        let (outcome, _) = normalized(r#"{ "member": { "id": "abc" } }"#);
        assert_eq!(outcome, MemberContent::Usable);
    }

    /// A join is only inferred when the content names an application, which is
    /// what MSC4143 requires of a join anyway.
    #[test]
    fn membership_is_not_inferred_without_an_application() {
        let json = LEGACY_JOIN.replace(
            r#""application": { "type": "m.call", "m.call.intent": "video" },"#,
            "",
        );
        let (_, value) = normalized(&json);
        assert!(value.pointer("/member/membership").is_none());
    }

    #[test]
    fn claimed_device_is_read_only_when_stated() {
        let (_, value) = normalized(LEGACY_JOIN);
        assert_eq!(claimed_device_id(&value).as_deref(), Some("V5cP8FErcB"));

        let (_, spec) = normalized(SPEC_JOIN);
        assert_eq!(claimed_device_id(&spec), None);
    }

    #[test]
    fn legacy_key_message_parses() {
        let content: Value = serde_json::from_str(
            r#"{
                "keys": { "index": 1, "key": "36/SXoTd/H/DnPU1NNzS1g==" },
                "member": { "claimed_device_id": "RwkMOenWJa", "id": "526e16aa" },
                "room_id": "!room:example.io",
                "sent_ts": 1786096055586,
                "session": { "application": "m.call", "call_id": "", "scope": "m.room" }
            }"#,
        )
        .unwrap();

        assert_eq!(
            parse_key_message("@alice:example.io", &content),
            Some(LegacyKeyMessage {
                room_id: "!room:example.io".to_owned(),
                member_id: "526e16aa".to_owned(),
                key_b64: "36/SXoTd/H/DnPU1NNzS1g==".to_owned(),
                key_index: 1,
            })
        );
    }

    #[test]
    fn a_key_message_missing_a_field_is_rejected() {
        let content = json!({ "keys": { "index": 0 }, "room_id": "!room:example.io" });
        assert_eq!(parse_key_message("@alice:example.io", &content), None);
    }

    /// The pre-sticky generation's shape: `keys` is an array and there is no
    /// `member` object, so the member id has to be built from the sender and the
    /// device the content states.
    #[test]
    fn a_pre_sticky_key_message_binds_to_the_sender_and_device() {
        let content = json!({
            "keys": [
                { "index": 0, "key": "b2xkZXIga2V5IG1hdGVyaWFs" },
                { "index": 2, "key": "36/SXoTd/H/DnPU1NNzS1g==" },
                { "index": 1, "key": "bWlkZGxlIGtleSBtYXRlcmlhbA==" },
            ],
            "device_id": "V5cP8FErcB",
            "call_id": "",
            "room_id": "!room:example.io",
            "sent_ts": 1786096055586u64,
        });

        assert_eq!(
            parse_key_message("@alice:example.io", &content),
            Some(LegacyKeyMessage {
                room_id: "!room:example.io".to_owned(),
                // The same string the inbound state translation uses as
                // `member.id`; if these diverged the key would be filed against
                // a membership that does not exist.
                member_id: "@alice:example.io:V5cP8FErcB".to_owned(),
                // The highest index wins: that is the most recent key on offer.
                key_b64: "36/SXoTd/H/DnPU1NNzS1g==".to_owned(),
                key_index: 2,
            })
        );
    }

    /// An explicit `member.id` always wins over the sender-derived fallback,
    /// even when the content also states a device.
    #[test]
    fn an_explicit_member_id_beats_the_fallback() {
        let content = json!({
            "keys": { "index": 0, "key": "36/SXoTd/H/DnPU1NNzS1g==" },
            "member": { "id": "526e16aa" },
            "device_id": "V5cP8FErcB",
            "room_id": "!room:example.io",
        });

        assert_eq!(
            parse_key_message("@alice:example.io", &content)
                .expect("parses")
                .member_id,
            "526e16aa"
        );
    }

    fn dialect() -> ElementCallDialect {
        ElementCallDialect::new("@bob:example.io", "BOBDEVICE", "m.call#ROOM")
    }

    /// Additive: every MSC4143 field survives, so one event serves both
    /// dialects.
    #[test]
    fn outbound_join_keeps_the_spec_fields_and_gains_the_legacy_ones() {
        let mut content: Value = serde_json::from_str(SPEC_JOIN).unwrap();
        dialect().rewrite_member_content(&mut content);

        assert_eq!(content.pointer("/member/membership").unwrap(), "join");
        assert_eq!(content.pointer("/member/id").unwrap(), "xyzABCDEF0123");
        assert_eq!(
            content.pointer("/transports/can_subscribe/0").unwrap(),
            "livekit"
        );

        assert_eq!(
            content.pointer("/member/user_id").unwrap(),
            "@bob:example.io"
        );
        assert_eq!(content.pointer("/member/device_id").unwrap(), "BOBDEVICE");
        assert_eq!(
            content
                .pointer("/rtc_transports/0/livekit_service_url")
                .unwrap(),
            "https://sfu.example.com"
        );
        assert_eq!(content.get("versions").unwrap(), &json!([]));
    }

    /// A leave becomes the legacy shape outright: content holding nothing but
    /// the sticky key, exactly what Element Call itself sends.
    ///
    /// Keeping `membership: "leave"` would be worse than useless — Element Call
    /// has no such field, its validator would reject the event for the missing
    /// `rtc_transports`, and padding that in would make us read as *joined*.
    #[test]
    fn outbound_leave_becomes_a_bare_sticky_key() {
        let mut content = json!({
            "slot_id": "m.call#ROOM",
            "member": { "id": "abc", "membership": "leave" },
            "leave_reason": { "code": "leave" },
            "msc4354_sticky_key": "abc"
        });
        dialect().rewrite_member_content(&mut content);

        assert_eq!(content, json!({ "msc4354_sticky_key": "abc" }));
    }

    /// The delayed leave (the dead man's switch) travels the same path, so it
    /// must come out the same way rather than carrying a `leave_reason` Element
    /// Call would choke on.
    #[test]
    fn outbound_delayed_leave_becomes_a_bare_sticky_key_too() {
        let mut content = json!({
            "slot_id": "m.call#ROOM",
            "member": { "id": "abc", "membership": "leave" },
            "leave_reason": {
                "code": "delayed_leave",
                "reason": "Dead man's switch: client failed to heartbeat"
            },
            "msc4354_sticky_key": "abc"
        });
        dialect().rewrite_member_content(&mut content);

        assert_eq!(content, json!({ "msc4354_sticky_key": "abc" }));
    }

    /// A leave we cannot name a sticky key for is left alone: an empty object
    /// would be a departure nobody can attribute.
    #[test]
    fn a_leave_without_a_sticky_key_is_not_replaced() {
        let mut content = json!({
            "slot_id": "m.call#ROOM",
            "member": { "id": "abc", "membership": "leave" }
        });
        dialect().rewrite_member_content(&mut content);

        assert_eq!(content.pointer("/member/membership").unwrap(), "leave");
    }

    #[test]
    fn outbound_aliases_are_idempotent() {
        let mut once: Value = serde_json::from_str(SPEC_JOIN).unwrap();
        dialect().rewrite_member_content(&mut once);
        let mut twice = once.clone();
        dialect().rewrite_member_content(&mut twice);

        assert_eq!(once, twice);
    }

    /// Idempotent for leaves too. A second pass sees a bare sticky key with no
    /// `slot_id` and must leave it be — growing a `member` object there would
    /// turn the departure back into something membership-shaped.
    #[test]
    fn a_rewritten_leave_survives_a_second_pass() {
        let mut content = json!({
            "slot_id": "m.call#ROOM",
            "member": { "id": "abc", "membership": "leave" },
            "msc4354_sticky_key": "abc"
        });
        dialect().rewrite_member_content(&mut content);
        let once = content.clone();
        dialect().rewrite_member_content(&mut content);

        assert_eq!(content, once);
        assert!(content.get("member").is_none());
    }

    #[test]
    fn outbound_key_message_takes_the_legacy_type_and_shape() {
        let content = json!({
            "room_id": "!room:example.io",
            "member_id": "our-member-id",
            "media_key": { "index": 3, "key": "aaaa" },
            "format": 0
        });

        let (message_type, legacy) = dialect()
            .rewrite_key_message("m.rtc.encryption_key", &content)
            .expect("a key message must be rewritten");

        assert_eq!(message_type, LEGACY_KEY_EVENT_TYPE);
        assert_eq!(legacy.pointer("/keys/index").unwrap(), 3);
        assert_eq!(legacy.pointer("/keys/key").unwrap(), "aaaa");
        assert_eq!(legacy.pointer("/member/id").unwrap(), "our-member-id");
        assert_eq!(
            legacy.pointer("/member/claimed_device_id").unwrap(),
            "BOBDEVICE"
        );
        assert_eq!(legacy.pointer("/room_id").unwrap(), "!room:example.io");
        assert_eq!(legacy.pointer("/session/application").unwrap(), "m.call");
        assert_eq!(legacy.pointer("/session/call_id").unwrap(), "");
        assert_eq!(legacy.pointer("/session/scope").unwrap(), "m.room");
        assert!(legacy.get("sent_ts").is_some());
    }

    /// Stripped of `application` and `m.text`, what the core sends must equal
    /// what a real Element Call sends — captured verbatim in
    /// `skills/e2e-testing/references/Alice-Bob-Call-Events.md`, where Bob
    /// starts the call and Alice, joining second, sends nothing.
    #[test]
    fn a_notification_is_flattened_into_what_element_call_sends() {
        // As `matrix_rtc_core::build_notification_content` writes it: the MSC's
        // `application` block plus the top-level copies every deployed receiver
        // actually reads.
        let spec = json!({
            "application": {
                "type": "m.call",
                "notification_type": "notification",
                "sender_ts": 1_779_030_731_419u64,
                "lifetime": 30000,
                "device_id": "WDQHAPEYDK",
                "m.call.intent": "video",
            },
            "notification_type": "notification",
            "sender_ts": 1_779_030_731_419u64,
            "lifetime": 30000,
            "m.call.intent": "video",
            "m.text": [{ "body": "Session started by @bob:synapse.othersite.m.localhost" }],
            "m.mentions": { "user_ids": [], "room": true },
            "m.relates_to": {
                "rel_type": "m.reference",
                "event_id": "$_ErcrEWx3Hj77_wScF-U4e9aS6cVi37RvFUeq12BiaI",
            },
        });

        let flat = flatten_notification_content("m.rtc.notification", &spec)
            .expect("a notification must be flattened");

        assert_eq!(
            flat,
            json!({
                "m.mentions": { "user_ids": [], "room": true },
                "notification_type": "notification",
                "sender_ts": 1_779_030_731_419u64,
                "lifetime": 30000,
                "m.call.intent": "video",
                "m.relates_to": {
                    "event_id": "$_ErcrEWx3Hj77_wScF-U4e9aS6cVi37RvFUeq12BiaI",
                    "rel_type": "m.reference",
                },
            })
        );
    }

    /// A call with no stated intent must not grow an `m.call.intent: null`.
    #[test]
    fn a_flattened_notification_omits_an_absent_intent() {
        let spec = json!({
            "application": {
                "type": "m.call",
                "notification_type": "ring",
                "sender_ts": 1u64,
                "lifetime": 30000,
                "device_id": "BOBDEVICE",
            },
            "m.mentions": { "user_ids": [], "room": true },
            "m.relates_to": { "rel_type": "m.reference", "event_id": "$member" },
        });

        let flat = flatten_notification_content("m.rtc.notification", &spec).unwrap();

        assert!(flat.get("m.call.intent").is_none());
        assert!(flat.get("application").is_none());
        assert!(flat.get("device_id").is_none());
    }

    /// Anything else through the same sender must come out untouched.
    #[test]
    fn other_event_types_are_not_flattened() {
        assert!(flatten_notification_content("m.rtc.member", &json!({})).is_none());
    }

    /// The dialect decides; only the content moves, never the event type.
    #[test]
    fn only_the_legacy_dialects_flatten_a_notification() {
        let spec = json!({
            "application": { "type": "m.call", "notification_type": "ring" },
            "m.mentions": { "user_ids": [], "room": true },
        });

        assert_eq!(
            crate::compat::OutboundDialect::None
                .rewrite_notification("m.rtc.notification", spec.clone()),
            spec,
            "a spec-current call sends the spec shape"
        );
        assert!(
            crate::compat::OutboundDialect::Sticky(dialect())
                .rewrite_notification("m.rtc.notification", spec)
                .get("application")
                .is_none()
        );
    }

    /// The unstable id is what the core actually sends today.
    #[test]
    fn the_unstable_key_type_is_rewritten_too() {
        let content = json!({
            "room_id": "!room:example.io",
            "member_id": "our-member-id",
            "media_key": { "index": 0, "key": "aaaa" }
        });
        assert!(
            dialect()
                .rewrite_key_message("org.matrix.msc4143.rtc.encryption_key", &content)
                .is_some()
        );
    }

    /// Anything that is not a media key passes through the sender untouched.
    #[test]
    fn other_to_device_types_are_not_rewritten() {
        assert!(
            dialect()
                .rewrite_key_message("m.room_key.withheld", &json!({}))
                .is_none()
        );
    }

    #[test]
    fn a_named_call_id_survives_the_slot_id_split() {
        assert_eq!(legacy_session("m.call#standup"), ("m.call", "standup"));
        assert_eq!(legacy_session("m.call#ROOM"), ("m.call", ""));
        // No separator at all is the room-scoped case again.
        assert_eq!(legacy_session("m.call"), ("m.call", ""));
    }
}
