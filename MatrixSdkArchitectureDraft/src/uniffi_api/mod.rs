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

use crate::driver::{
    DelegatedDelayedLeaveRequest, DriverError, LivekitTokenRequest, LivekitTokenResponse,
    OpenIdToken, OwnMembershipDriver, RoomEventsDriver, SendEventResponse, StateKeySelector,
    ToDeviceDelivery, ToDeviceDriver, ToDeviceMessage, ToDeviceRecipient, ToDeviceSendDriver,
    TokenDriver,
};
use crate::participation::ParticipationManager;
use crate::session::{self, ElementCallCompat, SessionConfig, SessionSnapshot};
use crate::types::{DeviceAttribution, EventOrigin, Member, RawMatrixEvent, RtcTransport};
use serde_json::Value;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum RtcError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("driver error: {0}")]
    Driver(String),
    #[error("rejected: {0}")]
    Rejected(String),
}

impl From<RtcError> for DriverError {
    fn from(error: RtcError) -> Self {
        match error {
            RtcError::InvalidInput(message) => DriverError::Other(message),
            RtcError::Driver(message) => DriverError::Other(message),
            RtcError::Rejected(message) => DriverError::Unauthorized(message),
        }
    }
}

#[derive(Clone, Debug, uniffi::Enum)]
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

#[derive(Clone, Debug, uniffi::Record)]
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
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiConnectionData {
    pub jwt_token: String,
    pub ws_url: String,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiConnectionWithMembers {
    pub connection: FfiConnectionData,
    pub members: Vec<FfiMember>,
}

#[derive(Clone, Debug, uniffi::Enum)]
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
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiMediaKey {
    pub member_id: String,
    pub key: Vec<u8>,
    pub index: u8,
    pub creation_ts_ms: u64,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiRtcTransport {
    pub transport_type: String,
    /// Type-specific fields as a JSON string (LiveKit: `livekit_service_url`).
    pub properties_json: String,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiTransportIntent {
    Publish { transport: FfiRtcTransport },
    ReceiveOnly { can_subscribe: Vec<String> },
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
            FfiEventOrigin::Encrypted { sender_device_id } => EventOrigin::Encrypted { sender_device_id },
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

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiOpenIdToken {
    pub access_token: String,
    pub token_type: String,
    pub matrix_server_name: String,
    pub expires_in_ms: u64,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiLivekitTokenRequest {
    pub server_name: Option<String>,
    pub url: String,
    pub room_id: String,
    pub slot_id: String,
    /// Forwarded verbatim (MSC4533).
    pub member_json: String,
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
    pub sticky_duration_ms: u64,
    pub keep_alive_timeout_ms: u64,
    pub delegate_delayed_leave: bool,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiStatus {
    Disconnected,
    Joining,
    Connected,
    Leaving,
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
    pub encrypted: Option<bool>,
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
        Arc::new(Self { subscribers: Mutex::new(Vec::new()), ever_subscribed: AtomicBool::new(false) })
    }

    fn subscribe(&self) -> UnboundedReceiver<T> {
        let (tx, rx) = unbounded_channel();
        self.subscribers.lock().unwrap_or_else(|e| e.into_inner()).push(tx);
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
    pub fn emit(
        &self,
        event_type: String,
        sender: String,
        content_json: String,
        origin: FfiEventOrigin,
    ) -> bool {
        let content: Value = match serde_json::from_str(&content_json) {
            Ok(content) => content,
            Err(error) => {
                log::warn!("ToDeviceSink::emit ignored a {event_type} from {sender}: not valid JSON ({error})");
                return true;
            }
        };
        self.fan_out.emit(ToDeviceMessage {
            event_type,
            sender,
            content,
            origin: origin.into(),
            sender_cross_signed: None,
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
        self.fan_out.emit(parse_raw_events(&events_json, EventOrigin::Cleartext))
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

    /// Returns the MSC4140 delay id (not an event id).
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        delay_ms: u64,
    ) -> Result<String, RtcError>;

    async fn restart_delayed_event(&self, room_id: String, delay_id: String)
    -> Result<(), RtcError>;

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

    async fn get_open_id(&self) -> Result<FfiOpenIdToken, RtcError>;

    async fn get_rtc_transports(&self) -> Result<Vec<FfiRtcTransport>, RtcError>;

    async fn get_livekit_token(&self, request: FfiLivekitTokenRequest)
    -> Result<String, RtcError>;

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
        callback.subscribe_room_events(Arc::new(RoomEventSink { fan_out: room_events.clone() }));
        callback.subscribe_to_device_events(Arc::new(ToDeviceSink { fan_out: to_device.clone() }));
        callback.subscribe_state_updates(Arc::new(StateUpdateSink { fan_out: state_updates.clone() }));
        Arc::new(Self { callback, room_events, to_device, state_updates })
    }
}

fn ffi_state_key(selector: StateKeySelector) -> Option<String> {
    match selector {
        StateKeySelector::Key(key) => Some(key),
        StateKeySelector::Any => None,
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
        todo!()
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<SendEventResponse, DriverError> {
        todo!()
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, DriverError> {
        todo!()
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), DriverError> {
        todo!()
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), DriverError> {
        todo!()
    }

    async fn delegate_livekit_delayed_leave(
        &self,
        request: DelegatedDelayedLeaveRequest,
    ) -> Result<(), DriverError> {
        todo!()
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
        todo!()
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
        let events = self.callback.read_state(event_type, ffi_state_key(state_key)).await?;
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
    async fn get_open_id(&self) -> Result<OpenIdToken, DriverError> {
        todo!()
    }

    async fn get_rtc_transports(&self) -> Result<Vec<RtcTransport>, DriverError> {
        todo!()
    }

    async fn get_livekit_token(
        &self,
        request: LivekitTokenRequest,
    ) -> Result<LivekitTokenResponse, DriverError> {
        todo!()
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
    fn on_key_map_change(&self, key_map: Vec<FfiMediaKey>);
}

#[uniffi::export(with_foreign)]
pub trait StatusListener: Send + Sync {
    fn on_status_change(&self, status: FfiStatus);
}

/// FFI wrapper around [`ParticipationManager`].
#[derive(uniffi::Object)]
pub struct FfiParticipationManager {
    inner: std::sync::Mutex<Option<ParticipationManager>>,
}

// No async runtime on wasm: uniffi-bindgen-react-native drives futures via
// the JS event loop (`wasm-unstable-single-threaded`).
#[cfg_attr(not(target_arch = "wasm32"), uniffi::export(async_runtime = "tokio"))]
#[cfg_attr(target_arch = "wasm32", uniffi::export)]
impl FfiParticipationManager {
    #[uniffi::constructor]
    pub fn new(
        room_id: String,
        slot_id: String,
        driver: Arc<FfiMatrixDriver>,
        compat: FfiElementCallCompat,
    ) -> Arc<Self> {
        todo!()
    }

    pub async fn join(
        &self,
        intent: FfiTransportIntent,
        params: FfiJoinParams,
    ) -> Result<(), RtcError> {
        todo!()
    }

    pub async fn leave(&self, code: Option<String>, reason: Option<String>) -> Result<(), RtcError> {
        todo!()
    }

    /// The session's joined projection plus left members still holding our
    /// keys (see [`FfiMembershipState`]).
    pub fn memberships(&self) -> Vec<FfiMembership> {
        todo!()
    }

    pub fn connections(&self) -> Vec<FfiConnectionWithMembers> {
        todo!()
    }

    pub fn key_map(&self) -> Vec<FfiMediaKey> {
        todo!()
    }

    pub fn status(&self) -> FfiStatus {
        todo!()
    }

    pub fn set_memberships_listener(&self, listener: Arc<dyn MembershipsListener>) {
        todo!()
    }

    pub fn set_connections_listener(&self, listener: Arc<dyn ConnectionsListener>) {
        todo!()
    }

    pub fn set_key_map_listener(&self, listener: Arc<dyn KeyMapListener>) {
        todo!()
    }

    pub fn set_status_listener(&self, listener: Arc<dyn StatusListener>) {
        todo!()
    }

    /// Diagnostics JSON (state + per-candidate join verdicts).
    pub fn debug_snapshot(&self) -> String {
        todo!()
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
    let config = SessionConfig { compat: compat.into() };
    Ok(session::compute_sessions_from_events(&events, &config).iter().map(FfiSessionSnapshot::from).collect())
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
        let sink = RoomEventSink { fan_out: fan_out.clone() };
        let mut rx = fan_out.subscribe();
        assert!(sink.emit("not json".into(), FfiEventOrigin::Unknown));
        assert!(rx.try_recv().is_err());
        assert!(sink.emit(
            r#"{ "type": "m.rtc.member", "sender": "@a:x", "content": {} }"#.into(),
            FfiEventOrigin::Encrypted { sender_device_id: Some("DEV".into()) },
        ));
        let event = rx.try_recv().unwrap();
        assert_eq!(event.event["type"], "m.rtc.member");
        assert_eq!(event.origin, EventOrigin::Encrypted { sender_device_id: Some("DEV".into()) });
    }

    #[test]
    fn state_update_sink_skips_malformed_entries_and_keeps_the_batch() {
        let fan_out = FanOut::new();
        let sink = StateUpdateSink { fan_out: fan_out.clone() };
        let mut rx = fan_out.subscribe();
        assert!(sink.emit(vec!["{}".into(), "garbage".into(), r#"{ "type": "m.rtc.slot" }"#.into()]));
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
        let snapshots = compute_sessions_from_events(vec![join, slot], FfiElementCallCompat::Off).unwrap();
        assert_eq!(snapshots.len(), 1);
        let s = &snapshots[0];
        assert_eq!(s.member_count, 1);
        assert!(s.is_active);
        assert_eq!(s.slot_open, Some(true));
        assert_eq!(s.encrypted, Some(false));
        assert_eq!(s.application_type.as_deref(), Some("m.call"));
        assert_eq!(s.members[0].member_id, "m-1");
        assert!(matches!(s.members[0].device_attribution, FfiDeviceAttribution::Unknown), "origins are unknown on this path");

        assert!(matches!(
            compute_sessions_from_events(vec!["nope".into()], FfiElementCallCompat::Off),
            Err(RtcError::InvalidInput(_))
        ));
    }
}
