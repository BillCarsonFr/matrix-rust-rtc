//! The single binding surface (feature `uniffi`): Swift and Kotlin via
//! uniffi-bindgen, React Native and web/wasm via uniffi-bindgen-react-native.
//! The generated API — and therefore the documented API — is identical on
//! every platform.
//!
//! FFI DTOs stay local to this module (JSON payloads as strings) and convert
//! into the crate's types at the boundary. Signalling is low-rate by design
//! (the media plane is host-owned), so nothing high-frequency crosses this
//! boundary. [`MatrixDriverCallback`] is the driver seam as an async
//! *foreign* trait — a matrix-rust-sdk-backed driver on mobile, a
//! matrix-js-sdk-backed one on web, implement the same contract.

#[cfg(feature = "runtime-probe")]
pub mod runtime_probe;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::connections::{
    ConnectionData, ConnectionProblem, ConnectionProblemKind, ConnectionWithMembers,
};
use crate::driver::{
    DelegatedDelayedLeaveRequest, DriverError, LivekitTokenRequest, LivekitTokenResponse,
    OwnMembershipDriver, RoomEventsDriver, SendEventResponse, StateKeySelector, ToDeviceDelivery,
    ToDeviceDriver, ToDeviceMessage, ToDeviceRecipient, ToDeviceSendDriver, TokenDriver,
};
use crate::encryption::{KeyMap, KeyRejection, MediaKey, MediaKeyChange, MediaKeyState};
use crate::own_membership::{
    self, DelayedLeaveOutcome, JoinError, JoinParams, LeaveError, OwnIdentity,
};
use crate::participation::{
    Component, DisconnectCause, Impairment, MembershipState, ParticipationConfig,
    ParticipationManager, SessionMembership, Severity, SlotError, Status,
};
use crate::session::{
    self, ElementCallCompat, JoinExclusionReason, SessionConfig, SessionRead, SessionSnapshot,
};
use crate::types::{
    DeviceAttribution, EventOrigin, LeaveReason, Member, RawMatrixEvent, RtcTransport,
    TransportIntent,
};
use serde_json::Value;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// Errors across the FFI, both directions. A foreign driver maps its
/// homeserver errors onto these: `Rejected` = 403 `M_FORBIDDEN`,
/// `Unsupported` = 404 `M_UNRECOGNIZED` — both read as "this homeserver will
/// never do delayed events" by the own-membership machine.
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum RtcError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// A homeserver HTTP failure — transient as far as this crate knows.
    #[error("http error: {0}")]
    Http(String),
    /// Anything the driver could not classify.
    #[error("driver error: {0}")]
    Driver(String),
    #[error("rejected: {0}")]
    Rejected(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// `M_LIMIT_EXCEEDED`: back off for `retry_after_ms` before retrying.
    #[error("rate limited")]
    RateLimited { retry_after_ms: Option<u64> },
    /// The manager behind the call has stopped; build a new one.
    #[error("the manager has stopped")]
    Stopped,
    #[error("already joined")]
    AlreadyJoined,
    #[error("not joined")]
    NotJoined,
    #[error("slot closed")]
    SlotClosed,
    /// The homeserver advertises no usable RTC transport — a configuration
    /// problem; retrying will not help.
    #[error("no usable RTC transport: {0}")]
    NoTransport(String),
    /// A transport exists but its token could not be minted — auth or
    /// network; retrying may help.
    #[error("the transport refused to mint a token: {0}")]
    TokenRefused(String),
    /// A crate precondition, not a caller mistake.
    #[error("the encryption machine could not be built: {0}")]
    EncryptionSetup(String),
}

impl From<RtcError> for DriverError {
    fn from(error: RtcError) -> Self {
        match error {
            RtcError::InvalidInput(message) | RtcError::Driver(message) => {
                DriverError::Other(message)
            }
            RtcError::Http(message) => DriverError::Http(message),
            RtcError::Rejected(message) => DriverError::Unauthorized(message),
            RtcError::Unsupported(message) => DriverError::Unsupported(message),
            RtcError::RateLimited { retry_after_ms } => DriverError::RateLimited { retry_after_ms },
            RtcError::Stopped => DriverError::Stopped,
            other => DriverError::Other(other.to_string()),
        }
    }
}

impl From<DriverError> for RtcError {
    fn from(error: DriverError) -> Self {
        match error {
            DriverError::Unauthorized(message) => RtcError::Rejected(message),
            DriverError::Unsupported(message) => RtcError::Unsupported(message),
            DriverError::RateLimited { retry_after_ms } => RtcError::RateLimited { retry_after_ms },
            DriverError::Stopped => RtcError::Stopped,
            DriverError::Http(message) => RtcError::Http(message),
            DriverError::Other(message) => RtcError::Driver(message),
        }
    }
}

impl From<JoinError> for RtcError {
    fn from(error: JoinError) -> Self {
        match error {
            JoinError::AlreadyJoined => RtcError::AlreadyJoined,
            JoinError::InvalidParams(message) => RtcError::InvalidInput(message),
            JoinError::SlotClosed => RtcError::SlotClosed,
            JoinError::NoTransport(e) => RtcError::NoTransport(e.to_string()),
            JoinError::TokenRefused(e) => RtcError::TokenRefused(e.to_string()),
            JoinError::EncryptionSetup(e) => RtcError::EncryptionSetup(e.to_string()),
            JoinError::Driver(e) => e.into(),
        }
    }
}

impl From<SlotError> for RtcError {
    fn from(error: SlotError) -> Self {
        match error {
            SlotError::InvalidSlotId(message) => RtcError::InvalidInput(message),
            SlotError::Driver(e) => e.into(),
        }
    }
}

impl From<LeaveError> for RtcError {
    fn from(error: LeaveError) -> Self {
        match error {
            LeaveError::NotJoined => RtcError::NotJoined,
            LeaveError::Driver(e) => e.into(),
        }
    }
}

#[derive(PartialEq, Clone, Debug, uniffi::Enum)]
pub enum FfiDeviceAttribution {
    Verified,
    Claimed,
    Unknown,
}

impl From<DeviceAttribution> for FfiDeviceAttribution {
    fn from(attribution: DeviceAttribution) -> Self {
        match attribution {
            DeviceAttribution::Verified => Self::Verified,
            DeviceAttribution::Claimed => Self::Claimed,
            DeviceAttribution::Unknown => Self::Unknown,
        }
    }
}

#[derive(PartialEq, Clone, Debug, uniffi::Record)]
pub struct FfiMember {
    pub member_id: String,
    pub user_id: String,
    pub device_id: Option<String>,
    pub device_attribution: FfiDeviceAttribution,
    /// `origin_server_ts` of the event that started this participation, where
    /// the dialect needs it to tell joins apart (MSC3401 compat).
    pub membership_ts: Option<u64>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub intent: Option<String>,
    pub application_type: Option<String>,
    /// The transports this member publishes on.
    pub published_transports: Vec<FfiRtcTransport>,
    pub can_subscribe: Vec<String>,
}

impl From<&Member> for FfiMember {
    fn from(member: &Member) -> Self {
        Self {
            member_id: member.member_id.clone(),
            user_id: member.user_id.clone(),
            device_id: member.device_id.clone(),
            device_attribution: member.device_attribution.into(),
            membership_ts: member.membership_ts,
            display_name: member.display_name.clone(),
            avatar_url: member.avatar_url.clone(),
            intent: member.intent.clone(),
            application_type: member.application_type.clone(),
            published_transports: member
                .transports
                .published
                .iter()
                .map(FfiRtcTransport::from)
                .collect(),
            can_subscribe: member.transports.can_subscribe.clone(),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiConnectionData {
    /// The connection key (`livekit_service_url`) `FfiMembership::connections`
    /// refers to.
    pub service_url: String,
    /// The SFU websocket URL to connect to.
    pub ws_url: String,
    pub jwt_token: String,
    /// From the JWT's `exp`; `None` when it carries none. A failed re-mint
    /// keeps the old token (a host may still be connected on it), so check
    /// this rather than discovering staleness through a failed connect.
    pub expires_at_ts: Option<u64>,
}

impl From<&ConnectionData> for FfiConnectionData {
    fn from(c: &ConnectionData) -> Self {
        Self {
            service_url: c.service_url.clone(),
            ws_url: c.ws_url.clone(),
            jwt_token: c.jwt_token.clone(),
            expires_at_ts: c.expires_at_ts,
        }
    }
}

/// A connection the session needs that the host cannot currently use.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiConnectionProblem {
    pub service_url: String,
    /// Members whose media is unavailable because of it.
    pub member_ids: Vec<String>,
    pub kind: FfiConnectionProblemKind,
    pub last_error: String,
    /// When the next mint is due; `0` = at the next beat.
    pub retry_at_ts: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, uniffi::Enum)]
pub enum FfiConnectionProblemKind {
    /// Wanted, never minted — absent from `connections()` entirely.
    NoToken,
    /// Present in `connections()` but its JWT is past `exp`.
    TokenExpired,
}

impl From<&ConnectionProblem> for FfiConnectionProblem {
    fn from(p: &ConnectionProblem) -> Self {
        Self {
            service_url: p.service_url.clone(),
            member_ids: p.member_ids.clone(),
            kind: match p.kind {
                ConnectionProblemKind::NoToken => FfiConnectionProblemKind::NoToken,
                ConnectionProblemKind::TokenExpired => FfiConnectionProblemKind::TokenExpired,
            },
            last_error: p.last_error.clone(),
            retry_at_ts: p.retry_at_ts,
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiConnectionWithMembers {
    pub connection: FfiConnectionData,
    pub members: Vec<FfiMember>,
}

impl From<&ConnectionWithMembers> for FfiConnectionWithMembers {
    fn from(c: &ConnectionWithMembers) -> Self {
        Self {
            connection: (&c.connection).into(),
            members: c.members.iter().map(FfiMember::from).collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, uniffi::Enum)]
pub enum FfiMembershipState {
    /// In the session's joined projection.
    Joined,
    /// Left the session but still holds a not-yet-rotated copy of our media
    /// key — render as "leaving / may still be listening" until rotation
    /// settles.
    LeftWithKeys,
}

/// One entry of the membership list — everything needed to render a tile
/// and later attach its media from the LK rooms.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiMembership {
    pub member: FfiMember,
    pub state: FfiMembershipState,
    /// `ws_url`s of the connections this member publishes on — the LK
    /// room(s) carrying their media. Empty for receive-only members and
    /// `LeftWithKeys` entries.
    pub connections: Vec<String>,
    /// Participant identity inside those LK rooms (MSC4195 pseudonymous
    /// hash; `{user}:{device}` in legacy compat mode).
    pub transport_identity: Option<String>,
    /// Whether this member and we can hear each other. `None` when the call
    /// does not manage media keys, for our own tile, and while not joined.
    pub media_key: Option<FfiMediaKeyState>,
}

/// Who can hear whom, for one tile. Two independent booleans: they fail for
/// different reasons and a UI renders them in different places.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiMediaKeyState {
    /// They hold our current key: they can decrypt us.
    pub holds_our_key: bool,
    /// We hold theirs: we can decrypt them.
    pub have_their_key: bool,
    /// Why their most recent key was discarded, while we still lack one.
    pub rejection: Option<FfiKeyRejection>,
}

impl From<&MediaKeyState> for FfiMediaKeyState {
    fn from(k: &MediaKeyState) -> Self {
        Self {
            holds_our_key: k.holds_our_key,
            have_their_key: k.have_their_key,
            rejection: k.rejection.as_ref().map(FfiKeyRejection::from),
        }
    }
}

/// Why an inbound media key was discarded — the answer to "why can't I hear
/// them?", which the crate computes and used to drop on the floor.
#[derive(Clone, Copy, Debug, PartialEq, uniffi::Enum)]
pub enum FfiKeyRejection {
    Cleartext,
    UnknownOrigin,
    SenderMismatch,
    DeviceMismatch,
    UnattributableMember,
    NotCrossSigned,
    WrongRoom,
    Outdated,
    NotManagingKeys,
}

impl From<&KeyRejection> for FfiKeyRejection {
    fn from(r: &KeyRejection) -> Self {
        match r {
            KeyRejection::Cleartext => Self::Cleartext,
            KeyRejection::UnknownOrigin => Self::UnknownOrigin,
            KeyRejection::SenderMismatch => Self::SenderMismatch,
            KeyRejection::DeviceMismatch => Self::DeviceMismatch,
            KeyRejection::UnattributableMember => Self::UnattributableMember,
            KeyRejection::NotCrossSigned => Self::NotCrossSigned,
            KeyRejection::WrongRoom => Self::WrongRoom,
            KeyRejection::Outdated => Self::Outdated,
            KeyRejection::NotManagingKeys => Self::NotManagingKeys,
        }
    }
}

impl From<&SessionMembership> for FfiMembership {
    fn from(m: &SessionMembership) -> Self {
        Self {
            member: (&m.member).into(),
            state: match m.state {
                MembershipState::Joined => FfiMembershipState::Joined,
                MembershipState::LeftWithKeys => FfiMembershipState::LeftWithKeys,
            },
            connections: m.connections.clone(),
            transport_identity: m.transport_identity.clone(),
            media_key: m.media_key.as_ref().map(FfiMediaKeyState::from),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiMediaKey {
    pub member_id: String,
    pub key: Vec<u8>,
    pub index: u8,
    pub creation_ts_ms: u64,
}

impl FfiMediaKey {
    fn new(member_id: &str, key: &MediaKey) -> Self {
        Self {
            member_id: member_id.to_owned(),
            key: key.key.clone(),
            index: key.index,
            creation_ts_ms: key.creation_ts_ms,
        }
    }
}

/// One entry per `(member, index)`, sorted by member id then index.
fn ffi_key_map(map: &KeyMap) -> Vec<FfiMediaKey> {
    let mut keys: Vec<FfiMediaKey> = map
        .iter()
        .flat_map(|(member_id, ring)| ring.iter().map(move |key| FfiMediaKey::new(member_id, key)))
        .collect();
    keys.sort_by(|a, b| (&a.member_id, a.index).cmp(&(&b.member_id, b.index)));
    keys
}

#[derive(PartialEq, Clone, Debug, uniffi::Record)]
pub struct FfiRtcTransport {
    pub transport_type: String,
    /// Type-specific fields as a JSON string (LiveKit: `livekit_service_url`).
    pub properties_json: String,
}

impl From<&RtcTransport> for FfiRtcTransport {
    fn from(t: &RtcTransport) -> Self {
        Self {
            transport_type: t.transport_type.clone(),
            properties_json: t.properties.to_string(),
        }
    }
}

impl TryFrom<FfiRtcTransport> for RtcTransport {
    type Error = RtcError;

    fn try_from(t: FfiRtcTransport) -> Result<Self, RtcError> {
        let properties: Value = if t.properties_json.trim().is_empty() {
            Value::Object(Default::default())
        } else {
            serde_json::from_str(&t.properties_json).map_err(|e| {
                RtcError::InvalidInput(format!("transport properties are not JSON: {e}"))
            })?
        };
        Ok(RtcTransport {
            transport_type: t.transport_type,
            properties,
        })
    }
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiTransportIntent {
    Publish { transport: FfiRtcTransport },
    ReceiveOnly { can_subscribe: Vec<String> },
}

impl TryFrom<FfiTransportIntent> for TransportIntent {
    type Error = RtcError;

    fn try_from(intent: FfiTransportIntent) -> Result<Self, RtcError> {
        Ok(match intent {
            FfiTransportIntent::Publish { transport } => {
                TransportIntent::Publish(transport.try_into()?)
            }
            FfiTransportIntent::ReceiveOnly { can_subscribe } => {
                TransportIntent::ReceiveOnly { can_subscribe }
            }
        })
    }
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiEventOrigin {
    Encrypted { sender_device_id: Option<String> },
    Cleartext,
    Unknown,
}

impl From<FfiEventOrigin> for EventOrigin {
    fn from(origin: FfiEventOrigin) -> Self {
        match origin {
            FfiEventOrigin::Encrypted { sender_device_id } => {
                EventOrigin::Encrypted { sender_device_id }
            }
            FfiEventOrigin::Cleartext => EventOrigin::Cleartext,
            FfiEventOrigin::Unknown => EventOrigin::Unknown,
        }
    }
}

/// Pre-2026 Element Call interop, selected per call (session read side +
/// own-membership write side).
#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiElementCallCompat {
    Off,
    StickyEvents,
    StateEvents,
}

impl From<FfiElementCallCompat> for ElementCallCompat {
    fn from(compat: FfiElementCallCompat) -> Self {
        match compat {
            FfiElementCallCompat::Off => ElementCallCompat::Off,
            FfiElementCallCompat::StickyEvents => ElementCallCompat::StickyEvents,
            FfiElementCallCompat::StateEvents => ElementCallCompat::StateEvents,
        }
    }
}

/// See `driver::LivekitTokenRequest`: the driver fetches the OpenID token
/// itself and posts `{ room_id, slot_id, openid_token, member }` to
/// `{url}/get_token` — or, with `legacy_sfu_get`, `{ room: room_id,
/// openid_token, device_id }` to `{url}/sfu/get`.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiLivekitTokenRequest {
    pub url: String,
    pub room_id: String,
    pub slot_id: String,
    /// MSC4195 member claims `{ id, claimed_user_id, claimed_device_id }`.
    pub member_json: String,
    pub legacy_sfu_get: bool,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiLivekitToken {
    pub jwt: String,
    /// The SFU websocket URL from the response, when it returned one.
    pub url: Option<String>,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiSendEventResponse {
    pub event_id: Option<String>,
    pub delay_id: Option<String>,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiToDeviceRecipient {
    pub user_id: String,
    pub device_id: String,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiToDeviceDelivery {
    pub recipient: FfiToDeviceRecipient,
    pub error: Option<String>,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiJoinParams {
    pub application_type: String,
    /// `application["m.call.intent"]`.
    pub intent: Option<String>,
    pub sticky_duration_ms: u64,
    pub keep_alive_timeout_ms: u64,
    /// Lifetime when the homeserver refuses delayed events (default 5 min).
    pub degraded_lifetime_ms: Option<u64>,
    pub delegate_delayed_leave: bool,
}

impl From<FfiJoinParams> for JoinParams {
    fn from(p: FfiJoinParams) -> Self {
        JoinParams {
            application_type: p.application_type,
            intent: p.intent,
            sticky_duration_ms: p.sticky_duration_ms,
            keep_alive_timeout_ms: p.keep_alive_timeout_ms,
            degraded_lifetime_ms: p.degraded_lifetime_ms,
            delegate_delayed_leave: p.delegate_delayed_leave,
        }
    }
}

/// Join progress, step by step.
#[derive(Clone, Copy, Debug, PartialEq, uniffi::Record)]
pub struct FfiJoinProgress {
    pub has_fetched_transports: bool,
    pub has_fetched_initial_member_list: bool,
    pub has_created_transport_token: bool,
    pub has_sent_delayed_leave_event: bool,
    pub has_sent_member_join_event: bool,
    pub has_delegated_delayed_event: bool,
    pub has_started_heartbeat: bool,
}

impl From<&own_membership::JoinStatus> for FfiJoinProgress {
    fn from(j: &own_membership::JoinStatus) -> Self {
        Self {
            has_fetched_transports: j.has_fetched_transports,
            has_fetched_initial_member_list: j.has_fetched_initial_member_list,
            has_created_transport_token: j.has_created_transport_token,
            has_sent_delayed_leave_event: j.has_sent_delayed_leave_event,
            has_sent_member_join_event: j.has_sent_member_join_event,
            has_delegated_delayed_event: j.has_delegated_delayed_event,
            has_started_heartbeat: j.has_started_heartbeat,
        }
    }
}

/// The media-key exchange as a whole.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum FfiEncryptionStatus {
    Joining {
        has_distributed_initial_keys: bool,
        has_received_all_member_keys: bool,
    },
    Connected {
        /// Members who left but still hold the key our media is encrypted
        /// with — "possibly still listening".
        left_members_with_keys: Vec<FfiMember>,
        fully_settled: bool,
        last_rotation_ts: u64,
    },
}

impl From<&crate::encryption::Status> for FfiEncryptionStatus {
    fn from(status: &crate::encryption::Status) -> Self {
        match status {
            crate::encryption::Status::Joining {
                has_distributed_initial_keys,
                has_received_all_member_keys,
            } => Self::Joining {
                has_distributed_initial_keys: *has_distributed_initial_keys,
                has_received_all_member_keys: *has_received_all_member_keys,
            },
            crate::encryption::Status::Connected {
                left_members_with_keys,
                fully_settled,
                last_rotation_ts,
            } => Self::Connected {
                left_members_with_keys: left_members_with_keys
                    .iter()
                    .map(FfiMember::from)
                    .collect(),
                fully_settled: *fully_settled,
                last_rotation_ts: *last_rotation_ts,
            },
        }
    }
}

/// The dead man's switch that clears our membership if this client dies.
/// Mutually exclusive states of one mechanism.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum FfiKeepAlive {
    /// Armed, and we restart it ourselves.
    Armed {
        delay_ms: u64,
        last_restart_ts: u64,
        /// When the homeserver publishes our leave if no restart lands.
        fires_at_ts: u64,
    },
    /// Handed to the SFU (MSC4195): we no longer restart it, so a frozen
    /// `last_restart_ts` is expected here rather than a fault.
    Delegated {
        delegated_at_ts: u64,
        earliest_fire_ts: u64,
    },
    /// Armed, but restarts are failing — we drop out at `fires_at_ts`
    /// unless one succeeds.
    RestartFailing {
        since_ts: u64,
        fires_at_ts: u64,
        last_error: String,
    },
    /// Its delay elapsed with no successful restart: we are probably out
    /// already. A replacement is being armed.
    Expired { since_ts: u64 },
    /// None armed. `permanent` = this homeserver refuses delayed events for
    /// good; otherwise we re-probe at `next_probe_ts` (`Some(0)` = next beat).
    Unavailable {
        permanent: bool,
        next_probe_ts: Option<u64>,
    },
}

impl From<&own_membership::KeepAlive> for FfiKeepAlive {
    fn from(k: &own_membership::KeepAlive) -> Self {
        match k {
            own_membership::KeepAlive::Armed {
                delay_ms,
                last_restart_ts,
                fires_at_ts,
            } => Self::Armed {
                delay_ms: *delay_ms,
                last_restart_ts: *last_restart_ts,
                fires_at_ts: *fires_at_ts,
            },
            own_membership::KeepAlive::Delegated {
                delegated_at_ts,
                earliest_fire_ts,
            } => Self::Delegated {
                delegated_at_ts: *delegated_at_ts,
                earliest_fire_ts: *earliest_fire_ts,
            },
            own_membership::KeepAlive::RestartFailing {
                since_ts,
                fires_at_ts,
                last_error,
            } => Self::RestartFailing {
                since_ts: *since_ts,
                fires_at_ts: *fires_at_ts,
                last_error: last_error.clone(),
            },
            own_membership::KeepAlive::Expired { since_ts } => Self::Expired {
                since_ts: *since_ts,
            },
            own_membership::KeepAlive::Unavailable {
                permanent,
                next_probe_ts,
            } => Self::Unavailable {
                permanent: *permanent,
                next_probe_ts: *next_probe_ts,
            },
        }
    }
}

/// Our sticky membership event on the server (MSC4354).
#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct FfiMembershipPublication {
    pub lifetime_ms: u64,
    pub last_published_ts: u64,
    /// `last_published_ts + lifetime_ms` — when the server drops us if no
    /// refresh lands.
    pub expires_at_ts: u64,
    pub refresh_failing_since_ts: Option<u64>,
    pub last_refresh_error: Option<String>,
}

impl From<&own_membership::MembershipPublication> for FfiMembershipPublication {
    fn from(m: &own_membership::MembershipPublication) -> Self {
        Self {
            lifetime_ms: m.lifetime_ms,
            last_published_ts: m.last_published_ts,
            expires_at_ts: m.expires_at_ts,
            refresh_failing_since_ts: m.refresh_failing_since_ts,
            last_refresh_error: m.last_refresh_error.clone(),
        }
    }
}

/// Whether the session projects our own membership — i.e. whether anybody
/// can see us.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum FfiRosterPresence {
    /// Sent, echo not back yet. Not a fault.
    AwaitingEcho,
    Present,
    /// It was in the roster and is gone.
    Missing {
        since_ts: u64,
        republished_at_ts: Option<u64>,
    },
    /// On the server, but the session refuses to project it. The self-heal
    /// deliberately does not re-send here.
    Excluded {
        reason: FfiJoinExclusionReason,
    },
}

impl From<&own_membership::RosterPresence> for FfiRosterPresence {
    fn from(r: &own_membership::RosterPresence) -> Self {
        match r {
            own_membership::RosterPresence::AwaitingEcho => Self::AwaitingEcho,
            own_membership::RosterPresence::Present => Self::Present,
            own_membership::RosterPresence::Missing {
                since_ts,
                republished_at_ts,
            } => Self::Missing {
                since_ts: *since_ts,
                republished_at_ts: *republished_at_ts,
            },
            own_membership::RosterPresence::Excluded { reason } => Self::Excluded {
                reason: (*reason).into(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, uniffi::Enum)]
pub enum FfiJoinExclusionReason {
    SlotClosed,
    UnencryptedInEncryptedRoom,
    SenderNotInRoom,
    Expired,
}

impl From<JoinExclusionReason> for FfiJoinExclusionReason {
    fn from(reason: JoinExclusionReason) -> Self {
        match reason {
            JoinExclusionReason::SlotClosed => Self::SlotClosed,
            JoinExclusionReason::UnencryptedInEncryptedRoom => Self::UnencryptedInEncryptedRoom,
            JoinExclusionReason::SenderNotInRoom => Self::SenderNotInRoom,
            JoinExclusionReason::Expired => Self::Expired,
        }
    }
}

/// One of the room-state / timeline reads the session's seed makes. A read
/// that failed leaves its condition *unknown*, which is not the same as the
/// condition being absent.
#[derive(Clone, Copy, Debug, PartialEq, uniffi::Enum)]
pub enum FfiSessionRead {
    Slot,
    RoomEncryption,
    RoomMembers,
    MemberEvents,
}

impl From<SessionRead> for FfiSessionRead {
    fn from(read: SessionRead) -> Self {
        match read {
            SessionRead::Slot => Self::Slot,
            SessionRead::RoomEncryption => Self::RoomEncryption,
            SessionRead::RoomMembers => Self::RoomMembers,
            SessionRead::MemberEvents => Self::MemberEvents,
        }
    }
}

/// How severe an [`FfiImpairment`] is, and therefore where to render it.
#[derive(Clone, Copy, Debug, PartialEq, uniffi::Enum)]
pub enum FfiSeverity {
    /// We are, or are about to be, out of the call — or peers cannot use
    /// our media.
    Critical,
    /// Degraded but functioning; a crash or a timeout would now hurt.
    Degraded,
    /// Worth surfacing in diagnostics, not in the call UI.
    Notice,
}

impl From<Severity> for FfiSeverity {
    fn from(severity: Severity) -> Self {
        match severity {
            Severity::Critical => Self::Critical,
            Severity::Degraded => Self::Degraded,
            Severity::Notice => Self::Notice,
        }
    }
}

/// A condition that is true right now and that the crate is still working
/// on. Every variant clears by itself when the underlying operation
/// succeeds — an impairment is never terminal; anything terminal ends the
/// participation and appears as [`FfiDisconnectCause`] instead.
///
/// A host that renders one warning banner can read this list and nothing
/// else; the structured status above is for anything that needs the details.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum FfiImpairment {
    KeepAliveRestartFailing {
        since_ts: u64,
        fires_at_ts: u64,
        last_error: String,
    },
    KeepAliveExpired {
        since_ts: u64,
    },
    KeepAliveUnavailable {
        permanent: bool,
        membership_expires_at_ts: u64,
    },
    MembershipRefreshFailing {
        since_ts: u64,
        expires_at_ts: u64,
        last_error: String,
    },
    OwnMembershipMissing {
        since_ts: u64,
        republished_at_ts: Option<u64>,
    },
    OwnMembershipExcluded {
        reason: FfiJoinExclusionReason,
    },
    MediaKeyNotDelivered {
        member_ids: Vec<String>,
    },
    MediaKeyNotReceived {
        member_ids: Vec<String>,
    },
    MediaKeyRejected {
        member_id: String,
        sender_user_id: String,
        reason: FfiKeyRejection,
        at_ts: u64,
    },
    ConnectionUnavailable {
        service_url: String,
        member_ids: Vec<String>,
        last_error: String,
        retry_at_ts: u64,
    },
    ConnectionTokenExpired {
        service_url: String,
        expired_at_ts: u64,
        last_error: String,
    },
    SessionStateUnread {
        reads: Vec<FfiSessionRead>,
    },
    JoinedBeforeSeed {
        at_ts: u64,
    },
}

impl From<&Impairment> for FfiImpairment {
    fn from(i: &Impairment) -> Self {
        match i {
            Impairment::KeepAliveRestartFailing {
                since_ts,
                fires_at_ts,
                last_error,
            } => Self::KeepAliveRestartFailing {
                since_ts: *since_ts,
                fires_at_ts: *fires_at_ts,
                last_error: last_error.clone(),
            },
            Impairment::KeepAliveExpired { since_ts } => Self::KeepAliveExpired {
                since_ts: *since_ts,
            },
            Impairment::KeepAliveUnavailable {
                permanent,
                membership_expires_at_ts,
            } => Self::KeepAliveUnavailable {
                permanent: *permanent,
                membership_expires_at_ts: *membership_expires_at_ts,
            },
            Impairment::MembershipRefreshFailing {
                since_ts,
                expires_at_ts,
                last_error,
            } => Self::MembershipRefreshFailing {
                since_ts: *since_ts,
                expires_at_ts: *expires_at_ts,
                last_error: last_error.clone(),
            },
            Impairment::OwnMembershipMissing {
                since_ts,
                republished_at_ts,
            } => Self::OwnMembershipMissing {
                since_ts: *since_ts,
                republished_at_ts: *republished_at_ts,
            },
            Impairment::OwnMembershipExcluded { reason } => Self::OwnMembershipExcluded {
                reason: (*reason).into(),
            },
            Impairment::MediaKeyNotDelivered { member_ids } => Self::MediaKeyNotDelivered {
                member_ids: member_ids.clone(),
            },
            Impairment::MediaKeyNotReceived { member_ids } => Self::MediaKeyNotReceived {
                member_ids: member_ids.clone(),
            },
            Impairment::MediaKeyRejected {
                member_id,
                sender_user_id,
                reason,
                at_ts,
            } => Self::MediaKeyRejected {
                member_id: member_id.clone(),
                sender_user_id: sender_user_id.clone(),
                reason: reason.into(),
                at_ts: *at_ts,
            },
            Impairment::ConnectionUnavailable {
                service_url,
                member_ids,
                last_error,
                retry_at_ts,
            } => Self::ConnectionUnavailable {
                service_url: service_url.clone(),
                member_ids: member_ids.clone(),
                last_error: last_error.clone(),
                retry_at_ts: *retry_at_ts,
            },
            Impairment::ConnectionTokenExpired {
                service_url,
                expired_at_ts,
                last_error,
            } => Self::ConnectionTokenExpired {
                service_url: service_url.clone(),
                expired_at_ts: *expired_at_ts,
                last_error: last_error.clone(),
            },
            Impairment::SessionStateUnread { reads } => Self::SessionStateUnread {
                reads: reads.iter().copied().map(FfiSessionRead::from).collect(),
            },
            Impairment::JoinedBeforeSeed { at_ts } => Self::JoinedBeforeSeed { at_ts: *at_ts },
        }
    }
}

/// The severity of one impairment, so a host can sort or filter without
/// re-deriving the table. (`impairments` already arrives sorted, most severe
/// first.)
#[uniffi::export]
pub fn impairment_severity(impairment: FfiImpairment) -> FfiSeverity {
    // Mirrors `participation::Impairment::severity`.
    match impairment {
        FfiImpairment::KeepAliveExpired { .. }
        | FfiImpairment::OwnMembershipMissing { .. }
        | FfiImpairment::OwnMembershipExcluded { .. }
        | FfiImpairment::KeepAliveRestartFailing { .. }
        | FfiImpairment::MembershipRefreshFailing { .. }
        | FfiImpairment::ConnectionTokenExpired { .. }
        | FfiImpairment::ConnectionUnavailable { .. } => FfiSeverity::Critical,
        FfiImpairment::MediaKeyNotDelivered { .. }
        | FfiImpairment::MediaKeyNotReceived { .. }
        | FfiImpairment::MediaKeyRejected { .. }
        | FfiImpairment::KeepAliveUnavailable { .. }
        | FfiImpairment::SessionStateUnread { .. } => FfiSeverity::Degraded,
        FfiImpairment::JoinedBeforeSeed { .. } => FfiSeverity::Notice,
    }
}

/// Why a join failed. Typed so a host can decide what to offer next:
/// `NoTransport` is a configuration problem, `TokenRefused` may be worth a
/// retry.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum FfiJoinError {
    AlreadyJoined,
    InvalidParams { message: String },
    SlotClosed,
    NoTransport { message: String },
    TokenRefused { message: String },
    EncryptionSetup { message: String },
    Driver { message: String },
}

impl From<&JoinError> for FfiJoinError {
    fn from(error: &JoinError) -> Self {
        match error {
            JoinError::AlreadyJoined => Self::AlreadyJoined,
            JoinError::InvalidParams(message) => Self::InvalidParams {
                message: message.clone(),
            },
            JoinError::SlotClosed => Self::SlotClosed,
            JoinError::NoTransport(e) => Self::NoTransport {
                message: e.to_string(),
            },
            JoinError::TokenRefused(e) => Self::TokenRefused {
                message: e.to_string(),
            },
            JoinError::EncryptionSetup(e) => Self::EncryptionSetup {
                message: e.to_string(),
            },
            JoinError::Driver(e) => Self::Driver {
                message: e.to_string(),
            },
        }
    }
}

/// Which pump stopped, for [`FfiDisconnectCause::ManagerStopped`].
#[derive(Clone, Copy, Debug, PartialEq, uniffi::Enum)]
pub enum FfiComponent {
    Session,
    OwnMembership,
    Connections,
    Encryption,
    Participation,
}

impl From<Component> for FfiComponent {
    fn from(component: Component) -> Self {
        match component {
            Component::Session => Self::Session,
            Component::OwnMembership => Self::OwnMembership,
            Component::Connections => Self::Connections,
            Component::Encryption => Self::Encryption,
            Component::Participation => Self::Participation,
        }
    }
}

/// Why we are not in a call. Terminal by construction: unlike an impairment,
/// none of these clears on its own.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum FfiDisconnectCause {
    /// No join has been attempted on this manager.
    NeverJoined,
    /// The host called `leave()`.
    LeftByHost {
        code: Option<String>,
        reason: Option<String>,
    },
    /// The slot was closed under us and the machine left on its own.
    SlotClosed,
    /// `join()` failed; the participation never started. `progress` says how
    /// far it got.
    JoinFailed {
        at_ts: u64,
        progress: FfiJoinProgress,
        error: FfiJoinError,
    },
    /// A pump stopped. The manager is dead and will not recover; build a new
    /// one.
    ManagerStopped { component: FfiComponent },
}

impl From<&DisconnectCause> for FfiDisconnectCause {
    fn from(cause: &DisconnectCause) -> Self {
        match cause {
            DisconnectCause::NeverJoined => Self::NeverJoined,
            DisconnectCause::LeftByHost { reason } => Self::LeftByHost {
                code: reason.as_ref().map(|r| r.code.clone()),
                reason: reason.as_ref().and_then(|r| r.reason.clone()),
            },
            DisconnectCause::SlotClosed => Self::SlotClosed,
            DisconnectCause::JoinFailed {
                at_ts,
                progress,
                error,
            } => Self::JoinFailed {
                at_ts: *at_ts,
                progress: progress.into(),
                error: error.into(),
            },
            DisconnectCause::ManagerStopped { component } => Self::ManagerStopped {
                component: (*component).into(),
            },
        }
    }
}

/// What became of the dead man's switch when we left. A failed cancel still
/// leaves us out of the call — the delay is itself a leave — but a stray
/// delayed event of ours may land afterwards.
#[derive(Clone, Copy, Debug, PartialEq, uniffi::Enum)]
pub enum FfiDelayedLeaveOutcome {
    Cancelled,
    MayStillFire,
}

impl From<DelayedLeaveOutcome> for FfiDelayedLeaveOutcome {
    fn from(outcome: DelayedLeaveOutcome) -> Self {
        match outcome {
            DelayedLeaveOutcome::Cancelled => Self::Cancelled,
            DelayedLeaveOutcome::MayStillFire => Self::MayStillFire,
        }
    }
}

/// The participation status, in full.
///
/// This used to be four opaque variants with everything else hidden behind
/// `debug_snapshot`'s unversioned JSON — which is a diagnostics dump, not a
/// UI contract. Every field a host needs is now typed.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum FfiStatus {
    Disconnected {
        cause: FfiDisconnectCause,
    },
    Joining {
        own_membership: FfiJoinProgress,
        encryption: FfiEncryptionStatus,
        /// Live problems already visible during the join.
        impairments: Vec<FfiImpairment>,
    },
    Connected {
        keep_alive: FfiKeepAlive,
        membership: FfiMembershipPublication,
        roster: FfiRosterPresence,
        encryption: FfiEncryptionStatus,
        /// Everything currently wrong, most severe first.
        impairments: Vec<FfiImpairment>,
    },
    Leaving {
        leave_event_sent: bool,
        /// `None` until the leave reaches that step (or when none was armed).
        delayed_leave: Option<FfiDelayedLeaveOutcome>,
        impairments: Vec<FfiImpairment>,
    },
}

fn ffi_impairments(impairments: &[Impairment]) -> Vec<FfiImpairment> {
    impairments.iter().map(FfiImpairment::from).collect()
}

impl From<&Status> for FfiStatus {
    fn from(status: &Status) -> Self {
        match status {
            Status::Disconnected(cause) => Self::Disconnected {
                cause: cause.into(),
            },
            Status::Joining(s) => Self::Joining {
                own_membership: (&s.own_membership).into(),
                encryption: (&s.encryption).into(),
                impairments: ffi_impairments(&s.impairments),
            },
            Status::Connected(s) => Self::Connected {
                keep_alive: (&s.own_membership.keep_alive).into(),
                membership: (&s.own_membership.membership).into(),
                roster: (&s.own_membership.roster).into(),
                encryption: (&s.encryption).into(),
                impairments: ffi_impairments(&s.impairments),
            },
            Status::Leaving(s) => Self::Leaving {
                leave_event_sent: s.own_membership.leave_event_sent,
                delayed_leave: s
                    .own_membership
                    .delayed_leave
                    .map(FfiDelayedLeaveOutcome::from),
                impairments: ffi_impairments(&s.impairments),
            },
        }
    }
}

/// One session as a plain value — what [`compute_sessions_from_events`]
/// returns. The convenience data the room-list / header / lobby / room_info
/// computation needs is precomputed into fields (records carry no methods).
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiSessionSnapshot {
    pub room_id: String,
    pub slot_id: String,
    pub members: Vec<FfiMember>,
    pub member_count: u32,
    pub is_active: bool,
    /// `origin_server_ts` of the earliest joined membership, while active.
    pub start_ts: Option<u64>,
    pub application_type: Option<String>,
    /// `None` while no slot state was supplied (condition unenforced).
    pub slot_open: Option<bool>,
    /// The slot-prescribed encryption decision; `None` while unknown.
    ///
    /// Read it together with `failed_reads`: `None` with an empty
    /// `failed_reads` means "this call is not encrypted", `None` with
    /// `Slot` in it means "we could not find out" — the difference between
    /// rendering an open padlock and rendering nothing.
    pub encrypted: Option<bool>,
    /// `true` once the live session finished seeding (even after read
    /// failures); always `true` for a statically computed snapshot.
    pub seeded: bool,
    /// Seed reads that failed, so an absent value can be told from an
    /// unknown one. Empty is the healthy case, and an entry disappears once
    /// a live state update supplies that value.
    pub failed_reads: Vec<FfiSessionRead>,
    /// Member events that landed but are not in the joined projection, with
    /// the reason — the load-bearing diagnostics for "why can nobody see
    /// me?". Find your own entry with `own_member_id()`.
    pub excluded_candidates: Vec<FfiExcludedCandidate>,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct FfiExcludedCandidate {
    pub member: FfiMember,
    pub reason: FfiJoinExclusionReason,
}

impl From<&SessionSnapshot> for FfiSessionSnapshot {
    fn from(snapshot: &SessionSnapshot) -> Self {
        Self {
            room_id: snapshot.room_id.clone(),
            slot_id: snapshot.slot_id.clone(),
            members: snapshot.members.iter().map(FfiMember::from).collect(),
            member_count: snapshot.member_count() as u32,
            is_active: snapshot.is_active(),
            start_ts: snapshot.start_ts,
            application_type: snapshot.application_type.clone(),
            slot_open: snapshot.slot_state.as_ref().map(|s| s.is_open()),
            encrypted: snapshot.negotiated_encryption,
            seeded: snapshot.seeded,
            failed_reads: snapshot
                .failed_reads
                .iter()
                .copied()
                .map(FfiSessionRead::from)
                .collect(),
            excluded_candidates: snapshot
                .excluded_candidates
                .iter()
                .map(|(member, reason)| FfiExcludedCandidate {
                    member: member.into(),
                    reason: (*reason).into(),
                })
                .collect(),
        }
    }
}

/// Parse one host-supplied event JSON string into the crate's raw event.
fn parse_raw_event(event_json: &str, origin: EventOrigin) -> Result<RawMatrixEvent, RtcError> {
    let event: Value = serde_json::from_str(event_json)
        .map_err(|error| RtcError::InvalidInput(format!("event is not valid JSON: {error}")))?;
    Ok(RawMatrixEvent { event, origin })
}

/// Parse a batch, skipping (and logging) strings that are not JSON — one
/// malformed entry must not poison the rest.
fn parse_raw_events(events_json: &[String], origin: EventOrigin) -> Vec<RawMatrixEvent> {
    events_json
        .iter()
        .filter_map(|json| match parse_raw_event(json, origin.clone()) {
            Ok(event) => Some(event),
            Err(error) => {
                log::warn!("skipping an event the host handed over: {error}");
                None
            }
        })
        .collect()
}

/// Fan-out of one host-emitted stream to any number of Rust subscribers —
/// the Rust side of "single-sink semantics". `emit` returns `false` once
/// every subscriber that ever existed is gone, which is the drop-guard
/// signal for the host to unhook its handler.
struct FanOut<T> {
    subscribers: Mutex<Vec<UnboundedSender<T>>>,
    ever_subscribed: AtomicBool,
}

impl<T: Clone> FanOut<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            subscribers: Mutex::new(Vec::new()),
            ever_subscribed: AtomicBool::new(false),
        })
    }

    fn subscribe(&self) -> UnboundedReceiver<T> {
        let (tx, rx) = unbounded_channel();
        self.subscribers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(tx);
        self.ever_subscribed.store(true, Ordering::Release);
        rx
    }

    fn emit(&self, item: T) -> bool {
        let mut subscribers = self.subscribers.lock().unwrap_or_else(|e| e.into_inner());
        subscribers.retain(|tx| tx.send(item.clone()).is_ok());
        !subscribers.is_empty() || !self.ever_subscribed.load(Ordering::Acquire)
    }
}

/// One end of a driver event stream: Rust-exported objects handed to the
/// foreign driver through the `subscribe_*` methods. The host calls `emit`
/// from its own event handlers (e.g. matrix-js-sdk listeners); `false` means
/// the Rust side dropped the stream — unhook the handler (this plays the
/// role of matrix-rust-sdk's `EventHandlerDropGuard`).
#[derive(uniffi::Object)]
pub struct RoomEventSink {
    fan_out: Arc<FanOut<RawMatrixEvent>>,
}

#[uniffi::export]
impl RoomEventSink {
    /// Any room event — sticky or state; the session dispatches on type.
    /// `event_json` is the full event object (see `session::dispatch`), the
    /// *decrypted* one for encrypted events, with `origin` carrying the
    /// decryption metadata. Returns `false` once no consumer is left.
    pub fn emit(&self, event_json: String, origin: FfiEventOrigin) -> bool {
        match parse_raw_event(&event_json, origin.into()) {
            Ok(event) => self.fan_out.emit(event),
            Err(error) => {
                // Not a drop signal: the stream is alive, the input was bad.
                log::warn!("RoomEventSink::emit ignored an event: {error}");
                true
            }
        }
    }
}

#[derive(uniffi::Object)]
pub struct ToDeviceSink {
    fan_out: Arc<FanOut<ToDeviceMessage>>,
}

#[uniffi::export]
impl ToDeviceSink {
    /// A decrypted to-device message with its origin metadata.
    /// `sender_cross_signed` is the MSC4153 verdict on the sending device
    /// (`None` = the host cannot tell, treated as not signed: media keys
    /// from such devices are rejected unless configured otherwise).
    pub fn emit(
        &self,
        event_type: String,
        sender: String,
        content_json: String,
        origin: FfiEventOrigin,
        sender_cross_signed: Option<bool>,
    ) -> bool {
        let content: Value = match serde_json::from_str(&content_json) {
            Ok(content) => content,
            Err(error) => {
                log::warn!(
                    "ToDeviceSink::emit ignored a {event_type} from {sender}: not valid JSON ({error})"
                );
                return true;
            }
        };
        self.fan_out.emit(ToDeviceMessage {
            event_type,
            sender,
            content,
            origin: origin.into(),
            sender_cross_signed,
        })
    }
}

#[derive(uniffi::Object)]
pub struct StateUpdateSink {
    fan_out: Arc<FanOut<Vec<RawMatrixEvent>>>,
}

#[uniffi::export]
impl StateUpdateSink {
    /// A batch of changed room-state events (applied atomically by the
    /// session: one snapshot per batch). State events are never encrypted,
    /// so their origin is `Cleartext`.
    pub fn emit(&self, events_json: Vec<String>) -> bool {
        self.fan_out
            .emit(parse_raw_events(&events_json, EventOrigin::Cleartext))
    }
}

/// The host-implemented Matrix driver: the same seam as
/// [`crate::driver::MatrixDriver`], flattened for FFI. matrix-rust-sdk hosts
/// adapt their `MatrixDriver`; a matrix-js-sdk host implements this directly.
///
/// Inbound events are a driver job too: the `subscribe_*` methods hand the
/// host a sink to emit into. Each is called synchronously, **exactly once**,
/// during [`FfiMatrixDriver`] construction — store the sink and hook the
/// actual event handlers whenever convenient afterwards. Single-sink
/// semantics: fan-out to multiple managers happens on the Rust side.
///
/// (`async_trait` must sit *under* the uniffi attribute: uniffi parses the
/// original `async fn` tokens.)
#[uniffi::export(with_foreign)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait MatrixDriverCallback: Send + Sync {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        duration_ms: u64,
    ) -> Result<FfiSendEventResponse, RtcError>;

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content_json: String,
    ) -> Result<FfiSendEventResponse, RtcError>;

    /// Returns the MSC4140 delay id (not an event id). `sticky_duration_ms`
    /// makes it a delayed *sticky* event (MSC4354 + MSC4140); ignore it if
    /// the host SDK cannot express both yet.
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        delay_ms: u64,
        sticky_duration_ms: Option<u64>,
    ) -> Result<String, RtcError>;

    /// Delayed state event — compat only (`StateEvents`).
    async fn send_delayed_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content_json: String,
        delay_ms: u64,
    ) -> Result<String, RtcError>;

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), RtcError>;

    async fn cancel_delayed_event(&self, room_id: String, delay_id: String)
    -> Result<(), RtcError>;

    async fn delegate_livekit_delayed_leave(
        &self,
        room_id: String,
        slot_id: String,
        member_json: String,
        delay_id: String,
    ) -> Result<(), RtcError>;

    async fn send_to_device(
        &self,
        recipients: Vec<FfiToDeviceRecipient>,
        event_type: String,
        content_json: String,
    ) -> Result<Vec<FfiToDeviceDelivery>, RtcError>;

    /// `GET /_matrix/client/v1/rtc/transports`, with well-known fallback.
    async fn get_rtc_transports(&self) -> Result<Vec<FfiRtcTransport>, RtcError>;

    /// MSC4195 token exchange, OpenID token included (see
    /// [`FfiLivekitTokenRequest`]).
    async fn get_livekit_token(
        &self,
        request: FfiLivekitTokenRequest,
    ) -> Result<FfiLivekitToken, RtcError>;

    /// Latest `limit` timeline events of `event_type`, as JSON strings.
    async fn read_events(
        &self,
        event_type: String,
        state_key: Option<String>,
        limit: u32,
    ) -> Result<Vec<String>, RtcError>;

    /// Current state entries of `event_type` (`state_key: None` = any).
    async fn read_state(
        &self,
        event_type: String,
        state_key: Option<String>,
    ) -> Result<Vec<String>, RtcError>;

    /// Live room events (sticky member events, slot/state changes). Store
    /// the sink; call `emit` from the host SDK's event handlers.
    fn subscribe_room_events(&self, sink: Arc<RoomEventSink>);

    /// Decrypted inbound to-device events with origin metadata.
    fn subscribe_to_device_events(&self, sink: Arc<ToDeviceSink>);

    /// Room-state update batches (what notices a slot closing promptly).
    fn subscribe_state_updates(&self, sink: Arc<StateUpdateSink>);
}

/// The FFI driver object — the one place a foreign [`MatrixDriverCallback`]
/// becomes a [`crate::driver::MatrixDriver`]. Room-scoped and room-lived,
/// like matrix-rust-sdk's widget `MatrixDriver`: construct it once per room
/// and share it across managers (one room can hold several slots).
///
/// It is both the FFI handle *and* the adapter: the trait impls below
/// translate JSON-string payloads and FFI records into the crate's driver
/// types. The foreign `subscribe_*` handshake happens in [`Self::new`], each
/// sink feeding one inbound channel; the Rust trait's `subscribe_*` methods
/// (fresh receiver per call) are served by fanning those channels out
/// internally — so any number of managers consume a foreign driver exactly
/// like a native one.
#[derive(uniffi::Object)]
pub struct FfiMatrixDriver {
    callback: Arc<dyn MatrixDriverCallback>,
    room_events: Arc<FanOut<RawMatrixEvent>>,
    to_device: Arc<FanOut<ToDeviceMessage>>,
    state_updates: Arc<FanOut<Vec<RawMatrixEvent>>>,
}

#[uniffi::export]
impl FfiMatrixDriver {
    /// Performs the `subscribe_*` handshake with the foreign driver
    /// (synchronously, exactly once).
    #[uniffi::constructor]
    pub fn new(callback: Arc<dyn MatrixDriverCallback>) -> Arc<Self> {
        let room_events = FanOut::new();
        let to_device = FanOut::new();
        let state_updates = FanOut::new();
        callback.subscribe_room_events(Arc::new(RoomEventSink {
            fan_out: room_events.clone(),
        }));
        callback.subscribe_to_device_events(Arc::new(ToDeviceSink {
            fan_out: to_device.clone(),
        }));
        callback.subscribe_state_updates(Arc::new(StateUpdateSink {
            fan_out: state_updates.clone(),
        }));
        Arc::new(Self {
            callback,
            room_events,
            to_device,
            state_updates,
        })
    }
}

fn ffi_state_key(selector: StateKeySelector) -> Option<String> {
    match selector {
        StateKeySelector::Key(key) => Some(key),
        StateKeySelector::Any => None,
    }
}

fn ffi_send_response(r: FfiSendEventResponse) -> SendEventResponse {
    SendEventResponse {
        event_id: r.event_id,
        delay_id: r.delay_id,
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl OwnMembershipDriver for FfiMatrixDriver {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<SendEventResponse, DriverError> {
        Ok(ffi_send_response(
            self.callback
                .send_sticky_event(room_id, event_type, content.to_string(), duration_ms)
                .await?,
        ))
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<SendEventResponse, DriverError> {
        Ok(ffi_send_response(
            self.callback
                .send_state_event(room_id, event_type, state_key, content.to_string())
                .await?,
        ))
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
        sticky_duration_ms: Option<u64>,
    ) -> Result<String, DriverError> {
        Ok(self
            .callback
            .send_delayed_event(
                room_id,
                event_type,
                content.to_string(),
                delay_ms,
                sticky_duration_ms,
            )
            .await?)
    }

    async fn send_delayed_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, DriverError> {
        Ok(self
            .callback
            .send_delayed_state_event(
                room_id,
                event_type,
                state_key,
                content.to_string(),
                delay_ms,
            )
            .await?)
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), DriverError> {
        Ok(self
            .callback
            .restart_delayed_event(room_id, delay_id)
            .await?)
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), DriverError> {
        Ok(self
            .callback
            .cancel_delayed_event(room_id, delay_id)
            .await?)
    }

    async fn delegate_livekit_delayed_leave(
        &self,
        request: DelegatedDelayedLeaveRequest,
    ) -> Result<(), DriverError> {
        Ok(self
            .callback
            .delegate_livekit_delayed_leave(
                request.room_id,
                request.slot_id,
                request.member.to_string(),
                request.delay_id,
            )
            .await?)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl ToDeviceSendDriver for FfiMatrixDriver {
    async fn send_to_device(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        event_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, DriverError> {
        let recipients = recipients
            .into_iter()
            .map(|r| FfiToDeviceRecipient {
                user_id: r.user_id,
                device_id: r.device_id,
            })
            .collect();
        let deliveries = self
            .callback
            .send_to_device(recipients, event_type, content.to_string())
            .await?;
        Ok(deliveries
            .into_iter()
            .map(|d| ToDeviceDelivery {
                recipient: ToDeviceRecipient {
                    user_id: d.recipient.user_id,
                    device_id: d.recipient.device_id,
                },
                error: d.error,
            })
            .collect())
    }
}

impl ToDeviceDriver for FfiMatrixDriver {
    fn subscribe_to_device_events(&self) -> UnboundedReceiver<ToDeviceMessage> {
        self.to_device.subscribe()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl RoomEventsDriver for FfiMatrixDriver {
    /// Events come back as JSON strings without decryption metadata, so
    /// their origin is `Unknown` (origin-dependent conditions are skipped
    /// for seeded members).
    async fn read_events(
        &self,
        event_type: String,
        state_key: Option<StateKeySelector>,
        limit: u32,
    ) -> Result<Vec<RawMatrixEvent>, DriverError> {
        let events = self
            .callback
            .read_events(event_type, state_key.and_then(ffi_state_key), limit)
            .await?;
        Ok(parse_raw_events(&events, EventOrigin::Unknown))
    }

    async fn read_state(
        &self,
        event_type: String,
        state_key: StateKeySelector,
    ) -> Result<Vec<RawMatrixEvent>, DriverError> {
        let events = self
            .callback
            .read_state(event_type, ffi_state_key(state_key))
            .await?;
        Ok(parse_raw_events(&events, EventOrigin::Cleartext))
    }

    fn subscribe_room_events(&self) -> UnboundedReceiver<RawMatrixEvent> {
        self.room_events.subscribe()
    }

    fn subscribe_state_updates(&self) -> UnboundedReceiver<Vec<RawMatrixEvent>> {
        self.state_updates.subscribe()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenDriver for FfiMatrixDriver {
    async fn get_rtc_transports(&self) -> Result<Vec<RtcTransport>, DriverError> {
        let transports = self.callback.get_rtc_transports().await?;
        Ok(transports
            .into_iter()
            .filter_map(|t| match RtcTransport::try_from(t) {
                Ok(t) => Some(t),
                Err(e) => {
                    log::warn!("skipping a transport the host handed over: {e}");
                    None
                }
            })
            .collect())
    }

    async fn get_livekit_token(
        &self,
        request: LivekitTokenRequest,
    ) -> Result<LivekitTokenResponse, DriverError> {
        let token = self
            .callback
            .get_livekit_token(FfiLivekitTokenRequest {
                url: request.url,
                room_id: request.room_id,
                slot_id: request.slot_id,
                member_json: request.member.to_string(),
                legacy_sfu_get: request.legacy_sfu_get,
            })
            .await?;
        Ok(LivekitTokenResponse {
            jwt: token.jwt,
            url: token.url,
        })
    }
}

#[uniffi::export(with_foreign)]
pub trait MembershipsListener: Send + Sync {
    /// One tile per membership; use the connections output to acquire each
    /// tile's media (`membership.connections` -> LK room,
    /// `membership.transport_identity` -> participant).
    fn on_memberships_change(&self, memberships: Vec<FfiMembership>);
}

#[uniffi::export(with_foreign)]
pub trait ConnectionsListener: Send + Sync {
    fn on_connections_change(&self, connections: Vec<FfiConnectionWithMembers>);
}

#[uniffi::export(with_foreign)]
pub trait KeyMapListener: Send + Sync {
    /// The full map plus the one key that changed — route `change` to the LK
    /// room(s) of that member.
    fn on_key_map_change(&self, key_map: Vec<FfiMediaKey>, change: FfiMediaKey);
}

#[uniffi::export(with_foreign)]
pub trait StatusListener: Send + Sync {
    fn on_status_change(&self, status: FfiStatus);
}

#[uniffi::export(with_foreign)]
pub trait KeyRejectedListener: Send + Sync {
    /// An inbound media key was discarded: `member_id` names whose, `reason`
    /// says why.
    ///
    /// **Secondary** to `FfiMembership::media_key.rejection` and
    /// `FfiImpairment::MediaKeyRejected`, which a UI attaching late still
    /// finds; this is for logging and telemetry.
    fn on_key_rejected(&self, member_id: String, reason: FfiKeyRejection);
}

/// FFI wrapper around [`ParticipationManager`].
#[derive(uniffi::Object)]
pub struct FfiParticipationManager {
    inner: ParticipationManager,
}

// No async runtime on wasm: uniffi-bindgen-react-native drives futures via
// the JS event loop (`wasm-unstable-single-threaded`).
#[cfg_attr(not(target_arch = "wasm32"), uniffi::export(async_runtime = "tokio"))]
#[cfg_attr(target_arch = "wasm32", uniffi::export)]
impl FfiParticipationManager {
    /// One manager per `(room, slot)`; `user_id`/`device_id` are who we
    /// publish as. Any number of managers may share one driver.
    #[uniffi::constructor]
    pub fn new(
        room_id: String,
        slot_id: String,
        user_id: String,
        device_id: String,
        driver: Arc<FfiMatrixDriver>,
        compat: FfiElementCallCompat,
    ) -> Arc<Self> {
        let config = ParticipationConfig {
            compat: compat.into(),
            ..ParticipationConfig::default()
        };
        let driver: Arc<dyn crate::driver::MatrixDriver> = driver;
        Arc::new(Self {
            inner: ParticipationManager::new(
                room_id,
                slot_id,
                OwnIdentity { user_id, device_id },
                driver,
                config,
            ),
        })
    }

    pub async fn join(
        &self,
        intent: FfiTransportIntent,
        params: FfiJoinParams,
    ) -> Result<(), RtcError> {
        Ok(self.inner.join(intent.try_into()?, params.into()).await?)
    }

    /// `code` defaults to MSC4143's plain `leave`.
    pub async fn leave(
        &self,
        code: Option<String>,
        reason: Option<String>,
    ) -> Result<(), RtcError> {
        let leave_reason = code.map(|code| LeaveReason::new(code, reason));
        Ok(self.inner.leave(leave_reason).await?)
    }

    /// Open this manager's slot (`m.per_member` encryption when `encrypted`).
    /// Our member id for this participation; `None` while not joined.
    ///
    /// Every self-referential check needs it — am I in the roster
    /// (`session().excluded_candidates`), which LiveKit participant is me
    /// (`own_membership().transport_identity`). Matching on
    /// `(user_id, device_id)` is not a substitute: one device may hold
    /// several RTC members, and a rejoin mints a fresh id.
    pub fn own_member_id(&self) -> Option<String> {
        self.inner.own_member_id()
    }

    /// Our own entry in `memberships()`, when the session projects it.
    pub fn own_membership(&self) -> Option<FfiMembership> {
        self.inner
            .own_membership()
            .as_ref()
            .map(FfiMembership::from)
    }

    /// Wanted connections the host cannot currently use, with the members
    /// whose media is affected. Empty is the healthy case.
    pub fn connection_problems(&self) -> Vec<FfiConnectionProblem> {
        self.inner
            .connection_problems()
            .iter()
            .map(FfiConnectionProblem::from)
            .collect()
    }

    pub async fn open_slot(
        &self,
        application_type: String,
        encrypted: bool,
    ) -> Result<(), RtcError> {
        Ok(self.inner.open_slot(&application_type, encrypted).await?)
    }

    pub async fn close_slot(&self) -> Result<(), RtcError> {
        Ok(self.inner.close_slot().await?)
    }

    /// The live session snapshot (slot open?, encrypted?, members, start
    /// time) — the same record `compute_sessions_from_events` returns.
    pub fn session(&self) -> FfiSessionSnapshot {
        (&self.inner.session()).into()
    }

    /// The session's joined projection plus left members still holding our
    /// keys (see [`FfiMembershipState`]).
    pub fn memberships(&self) -> Vec<FfiMembership> {
        self.inner
            .memberships()
            .iter()
            .map(FfiMembership::from)
            .collect()
    }

    pub fn connections(&self) -> Vec<FfiConnectionWithMembers> {
        self.inner
            .connections()
            .iter()
            .map(FfiConnectionWithMembers::from)
            .collect()
    }

    pub fn key_map(&self) -> Vec<FfiMediaKey> {
        ffi_key_map(&self.inner.key_map())
    }

    pub fn status(&self) -> FfiStatus {
        (&self.inner.status()).into()
    }

    pub fn set_memberships_listener(&self, listener: Arc<dyn MembershipsListener>) {
        self.inner
            .on_memberships_change(Box::new(move |memberships| {
                listener
                    .on_memberships_change(memberships.iter().map(FfiMembership::from).collect())
            }));
    }

    pub fn set_connections_listener(&self, listener: Arc<dyn ConnectionsListener>) {
        self.inner
            .on_connections_change(Box::new(move |connections| {
                listener.on_connections_change(
                    connections
                        .iter()
                        .map(FfiConnectionWithMembers::from)
                        .collect(),
                )
            }));
    }

    pub fn set_key_map_listener(&self, listener: Arc<dyn KeyMapListener>) {
        self.inner
            .on_key_map_change(Box::new(move |map: &KeyMap, change: &MediaKeyChange| {
                listener.on_key_map_change(
                    ffi_key_map(map),
                    FfiMediaKey::new(&change.member_id, &change.key),
                )
            }));
    }

    pub fn set_status_listener(&self, listener: Arc<dyn StatusListener>) {
        self.inner.on_status_change(Box::new(move |status| {
            listener.on_status_change(status.into())
        }));
    }

    pub fn set_key_rejected_listener(&self, listener: Arc<dyn KeyRejectedListener>) {
        self.inner
            .on_key_rejected(Box::new(move |member_id: &str, reason: &KeyRejection| {
                listener.on_key_rejected(member_id.to_owned(), reason.into())
            }));
    }

    /// Diagnostics JSON: an unversioned dump for bug reports, **not** a UI
    /// contract. Everything a UI needs is typed on `status()`,
    /// `session()`, `memberships()` and `connection_problems()`.
    pub fn debug_snapshot(&self) -> String {
        self.inner.debug_snapshot().to_string()
    }
}

/// Static session computation for room-list / header info: values, not
/// subscriptions — call it on every room update and populate room_info from
/// the snapshot fields. Takes all relevant events in one list (sticky and
/// state, many rooms at once — the dispatch groups by room and slot).
/// Origins are unknown here, so origin-dependent conditions stay
/// unenforced — fine for room-info purposes.
#[uniffi::export]
pub fn compute_sessions_from_events(
    events_json: Vec<String>,
    compat: FfiElementCallCompat,
) -> Result<Vec<FfiSessionSnapshot>, RtcError> {
    let events = events_json
        .iter()
        .map(|json| parse_raw_event(json, EventOrigin::Unknown))
        .collect::<Result<Vec<_>, _>>()?;
    let config = SessionConfig {
        compat: compat.into(),
    };
    Ok(session::compute_sessions_from_events(&events, &config)
        .iter()
        .map(FfiSessionSnapshot::from)
        .collect())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn fan_out_reports_false_only_once_every_subscriber_is_gone() {
        let fan_out: Arc<FanOut<u32>> = FanOut::new();
        // No consumer yet: the stream is not "dropped", just unobserved.
        assert!(fan_out.emit(1));
        let mut a = fan_out.subscribe();
        let mut b = fan_out.subscribe();
        assert!(fan_out.emit(2));
        assert_eq!(a.try_recv().unwrap(), 2);
        assert_eq!(b.try_recv().unwrap(), 2);
        drop(a);
        assert!(fan_out.emit(3), "one subscriber left");
        assert_eq!(b.try_recv().unwrap(), 3);
        drop(b);
        assert!(!fan_out.emit(4), "the last subscriber is gone: unhook");
    }

    #[test]
    fn room_event_sink_parses_and_bad_json_is_not_a_drop_signal() {
        let fan_out = FanOut::new();
        let sink = RoomEventSink {
            fan_out: fan_out.clone(),
        };
        let mut rx = fan_out.subscribe();
        assert!(sink.emit("not json".into(), FfiEventOrigin::Unknown));
        assert!(rx.try_recv().is_err());
        assert!(sink.emit(
            r#"{ "type": "m.rtc.member", "sender": "@a:x", "content": {} }"#.into(),
            FfiEventOrigin::Encrypted {
                sender_device_id: Some("DEV".into())
            },
        ));
        let event = rx.try_recv().unwrap();
        assert_eq!(event.event["type"], "m.rtc.member");
        assert_eq!(
            event.origin,
            EventOrigin::Encrypted {
                sender_device_id: Some("DEV".into())
            }
        );
    }

    #[test]
    fn state_update_sink_skips_malformed_entries_and_keeps_the_batch() {
        let fan_out = FanOut::new();
        let sink = StateUpdateSink {
            fan_out: fan_out.clone(),
        };
        let mut rx = fan_out.subscribe();
        assert!(sink.emit(vec![
            "{}".into(),
            "garbage".into(),
            r#"{ "type": "m.rtc.slot" }"#.into()
        ]));
        let batch = rx.try_recv().unwrap();
        assert_eq!(batch.len(), 2);
        assert!(batch.iter().all(|e| e.origin == EventOrigin::Cleartext));
    }

    #[test]
    fn compute_sessions_from_events_maps_to_the_ffi_record() {
        let now = crate::executor::now_ms();
        let join = format!(
            r#"{{ "type": "m.rtc.member", "sender": "@remote:example.org", "event_id": "$1",
                  "room_id": "!room:example.org", "origin_server_ts": {now},
                  "msc4354_sticky": {{ "duration_ms": 240000 }},
                  "content": {{ "slot_id": "m.call#ROOM", "msc4354_sticky_key": "m-1",
                                "member": {{ "id": "m-1", "membership": "join" }},
                                "application": {{ "type": "m.call" }},
                                "transports": {{ "published": [{{ "type": "livekit", "livekit_service_url": "https://lk" }}], "can_subscribe": ["livekit"] }} }} }}"#
        );
        let slot = format!(
            r#"{{ "type": "m.rtc.slot", "sender": "@admin:example.org", "state_key": "m.call#ROOM",
                  "room_id": "!room:example.org", "origin_server_ts": {now},
                  "content": {{ "status": "open", "application": {{ "type": "m.call" }} }} }}"#
        );
        let snapshots =
            compute_sessions_from_events(vec![join, slot], FfiElementCallCompat::Off).unwrap();
        assert_eq!(snapshots.len(), 1);
        let s = &snapshots[0];
        assert_eq!(s.member_count, 1);
        assert!(s.is_active);
        assert_eq!(s.slot_open, Some(true));
        assert_eq!(s.encrypted, Some(false));
        assert_eq!(s.application_type.as_deref(), Some("m.call"));
        assert_eq!(s.members[0].member_id, "m-1");
        assert!(
            matches!(
                s.members[0].device_attribution,
                FfiDeviceAttribution::Unknown
            ),
            "origins are unknown on this path"
        );

        assert!(matches!(
            compute_sessions_from_events(vec!["nope".into()], FfiElementCallCompat::Off),
            Err(RtcError::InvalidInput(_))
        ));
    }
}
