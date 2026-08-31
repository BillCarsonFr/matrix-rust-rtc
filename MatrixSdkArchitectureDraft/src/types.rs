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
    /// The sending device, from decryption metadata (never self-asserted).
    pub device_id: Option<String>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    /// Application-level intent (e.g. what EX shows in the room header).
    pub intent: Option<String>,
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

/// An inbound Matrix event plus its origin. `event` is the raw JSON the host
/// SDK holds (sticky member event, state event, ...), untranslated.
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
#[derive(Clone, Debug)]
pub enum TransportIntent {
    Publish(RtcTransport),
    ReceiveOnly { can_subscribe: Vec<String> },
}

/// MSC4143 `leave_reason`: `code` machine-readable, `reason` human-readable.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaveReason {
    pub code: String,
    pub reason: Option<String>,
}

/// Generate a fresh MSC4143 member id for a new join.
pub fn generate_member_id() -> String {
    todo!()
}

/// Translate a stable event type to what the deployed ecosystem expects on
/// the wire (`m.rtc.member` -> `org.matrix.msc4143.rtc.member`, ...).
/// Inbound, both spellings are accepted everywhere.
pub fn wire_event_type(event_type: &str) -> &str {
    todo!()
}
