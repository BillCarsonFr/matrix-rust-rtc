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

use std::sync::Arc;

use crate::driver::{
    DelegatedDelayedLeaveRequest, DriverError, LivekitTokenRequest, LivekitTokenResponse,
    OpenIdToken, OwnMembershipDriver, RoomEventsDriver, SendEventResponse, StateKeySelector,
    ToDeviceDelivery, ToDeviceDriver, ToDeviceMessage, ToDeviceRecipient, ToDeviceSendDriver,
    TokenDriver,
};
use crate::participation::ParticipationManager;
use crate::types::{RawMatrixEvent, RtcTransport};
use serde_json::Value;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum RtcError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("driver error: {0}")]
    Driver(String),
    #[error("rejected: {0}")]
    Rejected(String),
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiMember {
    pub member_id: String,
    pub user_id: String,
    pub device_id: Option<String>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub intent: Option<String>,
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

/// Pre-2026 Element Call interop, selected per call (session read side +
/// own-membership write side).
#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiElementCallCompat {
    Off,
    StickyEvents,
    StateEvents,
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

/// One end of a driver event stream: Rust-exported objects handed to the
/// foreign driver through the `subscribe_*` methods. The host calls `emit`
/// from its own event handlers (e.g. matrix-js-sdk listeners); `false` means
/// the Rust side dropped the stream — unhook the handler (this plays the
/// role of matrix-rust-sdk's `EventHandlerDropGuard`).
#[derive(uniffi::Object)]
pub struct RoomEventSink {
    tx: UnboundedSender<RawMatrixEvent>,
}

#[uniffi::export]
impl RoomEventSink {
    /// Any room event — sticky or state; the session dispatches on type.
    pub fn emit(&self, event_json: String, origin: FfiEventOrigin) -> bool {
        todo!()
    }
}

#[derive(uniffi::Object)]
pub struct ToDeviceSink {
    tx: UnboundedSender<ToDeviceMessage>,
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
        todo!()
    }
}

#[derive(uniffi::Object)]
pub struct StateUpdateSink {
    tx: UnboundedSender<Vec<RawMatrixEvent>>,
}

#[uniffi::export]
impl StateUpdateSink {
    /// A batch of changed room-state events.
    pub fn emit(&self, events_json: Vec<String>) -> bool {
        todo!()
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
}

#[uniffi::export]
impl FfiMatrixDriver {
    /// Performs the `subscribe_*` handshake with the foreign driver
    /// (synchronously, exactly once).
    #[uniffi::constructor]
    pub fn new(callback: Arc<dyn MatrixDriverCallback>) -> Arc<Self> {
        todo!()
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
        todo!()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl RoomEventsDriver for FfiMatrixDriver {
    async fn read_events(
        &self,
        event_type: String,
        state_key: Option<StateKeySelector>,
        limit: u32,
    ) -> Result<Vec<RawMatrixEvent>, DriverError> {
        todo!()
    }

    async fn read_state(
        &self,
        event_type: String,
        state_key: StateKeySelector,
    ) -> Result<Vec<RawMatrixEvent>, DriverError> {
        todo!()
    }

    fn subscribe_room_events(&self) -> UnboundedReceiver<RawMatrixEvent> {
        todo!()
    }

    fn subscribe_state_updates(&self) -> UnboundedReceiver<Vec<RawMatrixEvent>> {
        todo!()
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
    todo!()
}
