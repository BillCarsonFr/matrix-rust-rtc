//! The only Matrix I/O boundary.
//!
//! Shaped after matrix-rust-sdk's widget `MatrixDriver` so that
//! implementation drops in unchanged (a thin adapter maps ruma types to the
//! JSON `Value`s here). Split into capability traits so each manager receives
//! no more access than it needs; [`MatrixDriver`] is the sum, with a blanket
//! impl.
//!
//! Futures are `Send` on native (uniffi spawns them) and `?Send` on wasm
//! (they wrap JS promises) — implement with the same pair of `cfg_attr`s.

use crate::types::{EventOrigin, RawMatrixEvent, RtcTransport};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Clone, Debug, thiserror::Error)]
pub enum DriverError {
    #[error("http error: {0}")]
    Http(String),
    #[error("not authorized: {0}")]
    Unauthorized(String),
    #[error("unsupported by homeserver: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Other(String),
}

/// Matrix OpenID token (what a transport exchanges for its own credentials).
#[derive(Clone, Debug)]
pub struct OpenIdToken {
    pub access_token: String,
    pub token_type: String,
    pub matrix_server_name: String,
    pub expires_in_ms: u64,
}

/// MSC4195 `POST /rtc/livekit/get_token`. `member` is forwarded verbatim
/// (MSC4533): the homeserver authorises it against actual room membership.
#[derive(Clone, Debug)]
pub struct LivekitTokenRequest {
    pub server_name: Option<String>,
    pub url: String,
    pub room_id: String,
    pub slot_id: String,
    pub member: Value,
}

#[derive(Clone, Debug)]
pub struct LivekitTokenResponse {
    pub jwt: String,
}

/// MSC4195 `POST /rtc/livekit/delegate_delayed_leave` — hands the dead man's
/// switch to the SFU.
#[derive(Clone, Debug)]
pub struct DelegatedDelayedLeaveRequest {
    pub room_id: String,
    pub slot_id: String,
    pub member: Value,
    pub delay_id: String,
}

#[derive(Clone, Debug, Default)]
pub struct SendEventResponse {
    pub event_id: Option<String>,
    /// Set instead of `event_id` for delayed sends (MSC4140 delay id).
    pub delay_id: Option<String>,
}

#[derive(Clone, Debug)]
pub enum StateKeySelector {
    Key(String),
    Any,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToDeviceRecipient {
    pub user_id: String,
    /// Always one specific device, never a `*` fan-out — media keys go to
    /// the device that published the membership.
    pub device_id: String,
}

/// Per-recipient outcome: one unreachable device must not silence the others,
/// and only recipients that actually got the key may be recorded as served.
#[derive(Clone, Debug)]
pub struct ToDeviceDelivery {
    pub recipient: ToDeviceRecipient,
    pub error: Option<String>,
}

/// A decrypted inbound to-device message with its origin metadata.
#[derive(Clone, Debug)]
pub struct ToDeviceMessage {
    pub event_type: String,
    pub sender: String,
    pub content: Value,
    pub origin: EventOrigin,
    /// Whether the sending device is cross-signed (MSC4153), when known.
    pub sender_cross_signed: Option<bool>,
}

/// Outbound events for the own-membership machine (and slot administration).
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait OwnMembershipDriver: Send + Sync {
    /// Send a sticky event (MSC4354). `duration_ms` must reach the server
    /// verbatim: the machine re-sends before it elapses.
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<SendEventResponse, DriverError>;

    /// Send a room state event (`m.rtc.slot` open/close).
    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<SendEventResponse, DriverError>;

    /// Schedule a delayed event (MSC4140); returns the *delay id*.
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, DriverError>;

    /// MSC4140 `restart` — the keep-alive primitive. Never emulate with
    /// cancel+reschedule: that leaves a window with no delayed leave armed.
    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), DriverError>;

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), DriverError>;

    /// MSC4195: delegate the delayed leave to the SFU.
    async fn delegate_livekit_delayed_leave(
        &self,
        request: DelegatedDelayedLeaveRequest,
    ) -> Result<(), DriverError>;
}

/// Outbound to-device messages (key distribution).
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait ToDeviceSendDriver: Send + Sync {
    /// Send one (Olm-encrypted) message to a set of devices, reporting the
    /// outcome per recipient. `Err` means the batch was not attempted.
    async fn send_to_device(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        event_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, DriverError>;
}

/// Adds the inbound to-device stream.
pub trait ToDeviceDriver: ToDeviceSendDriver {
    /// Decrypted inbound to-device events with origin metadata. Dropping the
    /// receiver stops forwarding.
    fn subscribe_to_device_events(&self) -> UnboundedReceiver<ToDeviceMessage>;
}

/// Room event/state reads and live streams.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RoomEventsDriver: Send + Sync {
    /// Latest `limit` timeline events of `event_type`.
    async fn read_events(
        &self,
        event_type: String,
        state_key: Option<StateKeySelector>,
        limit: u32,
    ) -> Result<Vec<RawMatrixEvent>, DriverError>;

    /// Current room state entries matching the selection.
    async fn read_state(
        &self,
        event_type: String,
        state_key: StateKeySelector,
    ) -> Result<Vec<RawMatrixEvent>, DriverError>;

    /// Live room events (sticky member events arrive here), each with its
    /// [`EventOrigin`] from decryption metadata.
    fn subscribe_room_events(&self) -> UnboundedReceiver<RawMatrixEvent>;

    /// Live room-state updates (slot changes, room members, encryption) —
    /// what lets a slot closing in an idle room be noticed promptly.
    fn subscribe_state_updates(&self) -> UnboundedReceiver<Vec<RawMatrixEvent>>;
}

/// Tokens and transport discovery.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait TokenDriver: Send + Sync {
    async fn get_open_id(&self) -> Result<OpenIdToken, DriverError>;

    /// `GET /_matrix/client/v1/rtc/transports`, with well-known fallback.
    async fn get_rtc_transports(&self) -> Result<Vec<RtcTransport>, DriverError>;

    /// MSC4195 LiveKit token exchange (body forwarded verbatim, MSC4533).
    async fn get_livekit_token(
        &self,
        request: LivekitTokenRequest,
    ) -> Result<LivekitTokenResponse, DriverError>;
}

/// The full driver — what matrix-rust-sdk's widget `MatrixDriver` (behind a
/// thin adapter) implements, and what `participation::Manager` takes.
pub trait MatrixDriver:
    OwnMembershipDriver + ToDeviceDriver + RoomEventsDriver + TokenDriver
{
}

impl<T> MatrixDriver for T where
    T: OwnMembershipDriver + ToDeviceDriver + RoomEventsDriver + TokenDriver
{
}
