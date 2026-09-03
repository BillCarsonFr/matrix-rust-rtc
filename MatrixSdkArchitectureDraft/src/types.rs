//! Shared domain types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A joined session member, projected from a valid `m.rtc.member` candidate.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Member {
    /// MSC4143 `member.id` — generated fresh per join, never derived from
    /// user/device ids.
    pub member_id: String,
    /// The event sender.
    pub user_id: String,
    /// The sending device. Verified (decryption metadata) or claimed
    /// (MSC3401 state) — see `device_attribution`.
    pub device_id: Option<String>,
    /// How `device_id` was established; decides how strictly inbound media
    /// keys are checked against it (see `encryption`).
    pub device_attribution: DeviceAttribution,
    /// `origin_server_ts` of the event that started this participation.
    /// `None` under native MSC4143, whose `member_id` is fresh per join. The
    /// MSC3401 converter sets it: that dialect reuses `{user}:{device}` as
    /// the member id across joins, and the timestamp is what tells a
    /// leave-and-rejoin apart from an unchanged membership.
    pub membership_ts: Option<u64>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    /// Application-level intent (e.g. what EX shows in the room header).
    pub intent: Option<String>,
    /// `application.type` of the membership (MSC4143 requires it on a join;
    /// the MSC3401 converter fills it from the state event's `application`
    /// string). Lets a slot-less session still report an application type.
    pub application_type: Option<String>,
    /// What this member publishes and can subscribe to. `connections` groups
    /// members by their published transports' connection key.
    pub transports: MemberTransports,
}

/// How a member's `device_id` was established.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum DeviceAttribution {
    /// From the sticky event's decryption metadata: the Olm sender device.
    Verified,
    /// Self-asserted (MSC3401 state key / content). A claim *narrows* what
    /// inbound keys are accepted from (they must still arrive Olm-encrypted
    /// from exactly that device); it never widens it.
    Claimed,
    /// The host reported nothing about the member event's origin. Checks
    /// that depend on it are skipped, not failed.
    #[default]
    Unknown,
}

/// How an inbound event reached us, from the host SDK's decryption metadata.
///
/// One value carries both "was it encrypted" and "which device sent it",
/// because both come from the same metadata: a cleartext event cannot carry a
/// sending device. `Unknown` means the host reported nothing — rules that
/// need this are skipped, not failed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EventOrigin {
    Encrypted { sender_device_id: Option<String> },
    Cleartext,
    Unknown,
}

impl EventOrigin {
    /// Whether the event arrived encrypted; `None` when the host did not say.
    pub fn was_encrypted(&self) -> Option<bool> {
        match self {
            Self::Encrypted { .. } => Some(true),
            Self::Cleartext => Some(false),
            Self::Unknown => None,
        }
    }

    /// The sending device, when decryption attributed one.
    pub fn sender_device_id(&self) -> Option<&str> {
        match self {
            Self::Encrypted { sender_device_id } => sender_device_id.as_deref(),
            Self::Cleartext | Self::Unknown => None,
        }
    }
}

/// An inbound Matrix event plus its origin.
///
/// `event` is the **full** event object as the host SDK holds it, untranslated:
/// `type`, `sender`, `event_id`, `room_id`, `origin_server_ts`, `content`,
/// optional `state_key`, optional top-level `msc4354_sticky`
/// (`{ duration_ms }`, MSC4354) and optional `unsigned`
/// (`msc4354_sticky_duration_ttl_ms` is read when present). For encrypted
/// events this is the *decrypted* event — the host resolves `m.room.encrypted`
/// before handing it over — and `origin` carries the decryption metadata.
/// `read_state` / `read_events` return the same shape. The accessors live in
/// `session::dispatch`; nothing else in the crate parses room-event JSON.
#[derive(Clone, Debug)]
pub struct RawMatrixEvent {
    pub event: Value,
    pub origin: EventOrigin,
}

/// One MSC4143 transport description (`transports.published[..]`).
/// `properties` keeps the type-specific fields (for LiveKit:
/// `livekit_service_url`, which doubles as the connection key).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RtcTransport {
    pub transport_type: String,
    pub properties: Value,
}

/// MSC4143 `member.transports`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MemberTransports {
    pub published: Vec<RtcTransport>,
    pub can_subscribe: Vec<String>,
}

/// What we intend to publish when joining. Receive-only members (recorders,
/// observers) are valid participants; `can_subscribe` still tells others
/// which transport to publish on so we can hear them.
#[derive(Clone, Debug, PartialEq)]
pub enum TransportIntent {
    Publish(RtcTransport),
    ReceiveOnly { can_subscribe: Vec<String> },
}

/// MSC4143 `leave_reason`: `code` machine-readable, `reason` human-readable.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaveReason {
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl LeaveReason {
    /// A voluntary leave (`leave`).
    pub const LEAVE: &'static str = "leave";
    /// The dead man's switch fired (`delayed_leave`).
    pub const DELAYED_LEAVE: &'static str = "delayed_leave";
    /// The slot was closed under us (`slot_closed`).
    pub const SLOT_CLOSED: &'static str = "slot_closed";

    pub fn new(code: impl Into<String>, reason: Option<String>) -> Self {
        Self {
            code: code.into(),
            reason,
        }
    }

    pub fn leave() -> Self {
        Self::new(Self::LEAVE, None)
    }

    pub fn delayed_leave() -> Self {
        Self::new(
            Self::DELAYED_LEAVE,
            Some("Dead man's switch: client failed to heartbeat".to_owned()),
        )
    }

    pub fn slot_closed() -> Self {
        Self::new(Self::SLOT_CLOSED, None)
    }
}

/// Generate a fresh MSC4143 member id for a new join: 16 random bytes, hex.
///
/// `getrandom` (with the `js` feature, see Cargo.toml) is the one source of
/// randomness that works on every target this crate builds for.
pub fn generate_member_id() -> String {
    let mut bytes = [0u8; 16];
    if let Err(error) = getrandom::getrandom(&mut bytes) {
        // Practically unreachable; fall back to the clock so a join still
        // gets a per-join id rather than panicking inside a constructor.
        log::error!("getrandom failed ({error}); deriving a member id from the clock");
        let now = crate::executor::now_ms().to_be_bytes();
        bytes[..8].copy_from_slice(&now);
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Translate a stable event type to what the deployed ecosystem expects on
/// the wire (`m.rtc.member` -> `org.matrix.msc4143.rtc.member`, ...).
/// Inbound, both spellings are accepted everywhere.
pub fn wire_event_type(event_type: &str) -> &str {
    match event_type {
        "m.rtc.member" => "org.matrix.msc4143.rtc.member",
        "m.rtc.slot" => "org.matrix.msc4143.rtc.slot",
        "m.rtc.encryption_key" => "org.matrix.msc4143.rtc.encryption_key",
        "m.rtc.notification" => "org.matrix.msc4075.rtc.notification",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_event_type_maps_the_types_the_crate_sends() {
        assert_eq!(
            wire_event_type("m.rtc.member"),
            "org.matrix.msc4143.rtc.member"
        );
        assert_eq!(wire_event_type("m.rtc.slot"), "org.matrix.msc4143.rtc.slot");
        assert_eq!(
            wire_event_type("m.rtc.encryption_key"),
            "org.matrix.msc4143.rtc.encryption_key"
        );
        // MSC4075, not MSC4143: notifications are their own proposal.
        assert_eq!(
            wire_event_type("m.rtc.notification"),
            "org.matrix.msc4075.rtc.notification"
        );
    }

    #[test]
    fn wire_event_type_leaves_unknown_types_alone() {
        assert_eq!(wire_event_type("com.example.custom"), "com.example.custom");
        assert_eq!(wire_event_type("m.room.member"), "m.room.member");
    }

    /// Already-unstable ids must survive a round through the table.
    #[test]
    fn wire_event_type_is_idempotent() {
        for stable in [
            "m.rtc.member",
            "m.rtc.slot",
            "m.rtc.encryption_key",
            "m.rtc.notification",
        ] {
            let once = wire_event_type(stable);
            assert_eq!(wire_event_type(once), once);
        }
    }

    #[test]
    fn member_ids_are_fresh_and_hex() {
        let a = generate_member_id();
        let b = generate_member_id();
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "two joins must never share a member id");
    }
}
