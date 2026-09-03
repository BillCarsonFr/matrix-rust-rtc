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

/// What a driver call can fail with.
///
/// The taxonomy exists so a caller can decide *what to do next* without
/// string matching: [`Unauthorized`](Self::Unauthorized) and
/// [`Unsupported`](Self::Unsupported) are permanent for this homeserver (the
/// own-membership machine reads them as "delayed events will never work
/// here", `machine::classify_refusal`), [`RateLimited`](Self::RateLimited) is
/// explicitly transient and carries the server's own back-off, and
/// [`Stopped`](Self::Stopped) means the manager behind the call is gone —
/// nothing will ever answer, and no retry helps.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum DriverError {
    #[error("http error: {0}")]
    Http(String),
    #[error("not authorized: {0}")]
    Unauthorized(String),
    #[error("unsupported by homeserver: {0}")]
    Unsupported(String),
    /// `M_LIMIT_EXCEEDED`. Its own variant because it is the one error a host
    /// must *not* retry immediately: `retry_after_ms` is the server's
    /// `retry_after_ms` when it supplied one.
    #[error("rate limited{}", .retry_after_ms.map(|ms| format!(" (retry after {ms}ms)")).unwrap_or_default())]
    RateLimited { retry_after_ms: Option<u64> },
    /// The manager that owns this call has stopped; it will never answer.
    /// Surfaces to the host as
    /// `DisconnectCause::ManagerStopped` (`participation`).
    #[error("the manager has stopped")]
    Stopped,
    #[error("{0}")]
    Other(String),
}

/// MSC4195 `POST {url}/get_token`. The adapter obtains the OpenID token
/// itself (that exchange is part of *how* a LiveKit token is fetched and never
/// needed elsewhere) and posts `{ room_id, slot_id, openid_token, member }`.
/// `member` is the MSC4195 claims object `{ id, claimed_user_id,
/// claimed_device_id }`, forwarded verbatim.
///
/// With `legacy_sfu_get` the pre-MSC4195 `POST {url}/sfu/get` is used
/// instead, body `{ room: room_id, openid_token, device_id }` (the
/// `ElementCallCompat::StateEvents` generation — delete with it). `room` must
/// equal the `livekit_alias` `own_membership` writes, which is the room id.
#[derive(Clone, Debug, PartialEq)]
pub struct LivekitTokenRequest {
    /// The transport's `livekit_service_url`.
    pub url: String,
    pub room_id: String,
    pub slot_id: String,
    pub member: Value,
    pub legacy_sfu_get: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LivekitTokenResponse {
    pub jwt: String,
    /// The SFU websocket URL to connect to (MSC4195 response `url`); `None`
    /// when the service returned none — the transport URL is used then.
    pub url: Option<String>,
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

    /// Send a room state event: the facade's slot administration
    /// (`m.rtc.slot` open/close) and the MSC3401 compat dialect's membership.
    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<SendEventResponse, DriverError>;

    /// Schedule a delayed event (MSC4140); returns the *delay id*.
    ///
    /// `sticky_duration_ms` makes it a delayed **sticky** event (MSC4354 +
    /// MSC4140 combined): the delayed leave must be sticky to clear our
    /// sticky-map entry when it fires. An adapter that cannot express both
    /// may ignore it (the ghost window then rides on the sticky TTL).
    /// Transitional `Option`: once every adapter can, it becomes mandatory.
    ///
    /// `DriverError::Unsupported` / `Unauthorized` are read as "this
    /// homeserver will never do delayed events" (404 `M_UNRECOGNIZED`, 403
    /// `M_FORBIDDEN`); anything else is retried later.
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
        sticky_duration_ms: Option<u64>,
    ) -> Result<String, DriverError>;

    /// Delayed **state** event — compat only (`ElementCallCompat::StateEvents`:
    /// the delayed leave is an empty state event). Delete with that generation.
    async fn send_delayed_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
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
    /// `GET /_matrix/client/v1/rtc/transports`, with well-known fallback.
    async fn get_rtc_transports(&self) -> Result<Vec<RtcTransport>, DriverError>;

    /// MSC4195 LiveKit token exchange, OpenID token included (see
    /// [`LivekitTokenRequest`]).
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
