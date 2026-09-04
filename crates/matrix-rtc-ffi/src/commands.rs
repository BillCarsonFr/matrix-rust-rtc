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

//! FFI binding implementation of the command sender interface.
//!
//! This module provides UniFFI-facing types and the `FfiCommandSender` that implements
//! `RtcCommandSender` by delegating to native callbacks.
//!
//! DTOs are used to decouple core logic from FFI-specific types.

use matrix_rtc_bridge::compat::{MemberEventRoute, OutboundDialect};
use matrix_rtc_core::{CommandError, RtcCommandSender, wire_event_type};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::compat::FfiElementCallCompat;

/// The sole conversion from a core event-type string to the wire type.
///
/// The core speaks the stable MSC4143 ids; the host SDK behind these callbacks
/// (typically the matrix-rust-sdk bindings, whose `sendRaw` puts the string on
/// the wire verbatim) has no ruma alias table to normalise them, so every send
/// leaving this binding goes through [`matrix_rtc_core::wire_event_type`].
/// Without it a membership is published as `m.rtc.member` and no peer sees it.
fn wire_type(event_type: String) -> String {
    wire_event_type(&event_type).to_owned()
}

/// Error type for command sender callback operations.
///
/// This is used as the error type for the CommandSenderCallback trait to ensure
/// UniFFI can properly generate bindings for it.
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum CommandSenderError {
    /// Serialization error when converting content to JSON
    #[error("could not serialize the event content: {0}")]
    SerializationError(String),
    /// Error from the native SDK when sending the event
    #[error("the send failed: {0}")]
    SendError(String),
    /// The homeserver will never accept this request, so there is no point
    /// retrying it.
    ///
    /// Throw this instead of `SendError` from the four delayed-event callbacks
    /// when the homeserver has no MSC4140 support — a `404 M_UNRECOGNIZED`, or
    /// matrix.org's `403 M_FORBIDDEN "Sending delayed events has been
    /// disallowed"`. The SDK then stops arming a dead man's switch for the rest
    /// of the session and keeps the membership alive by its lifetime alone,
    /// rather than re-probing a homeserver that has already answered.
    ///
    /// Optional: a plain `SendError` degrades the same way, just with periodic
    /// retries. Nothing breaks if you never throw this.
    #[error("the homeserver does not support this: {0}")]
    NotSupported(String),
}

impl From<CommandSenderError> for matrix_rtc_core::CommandError {
    fn from(err: CommandSenderError) -> Self {
        match err {
            CommandSenderError::SerializationError(e) => {
                matrix_rtc_core::CommandError::SerializationError(e)
            }
            CommandSenderError::SendError(e) => matrix_rtc_core::CommandError::SendError(e),
            CommandSenderError::NotSupported(e) => {
                matrix_rtc_core::CommandError::DelayedEventsNotSupported(e)
            }
        }
    }
}

/// FFI-friendly transport configuration for join operations.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiTransportConfig {
    /// Transport type (e.g., "livekit")
    pub r#type: String,
    /// LiveKit service URL (required for livekit transport)
    pub livekit_service_url: Option<String>,
}

/// FFI-friendly encryption configuration.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiEncryptionConfig {
    /// Time to wait (ms) before using a newly distributed key (default: 5000ms).
    pub delay_before_use_ms: Option<u64>,
    /// Grace period (ms) for key rotation (default: 10000ms).
    pub key_rotation_grace_period_ms: Option<u64>,
    /// Longest a key may be used before it is replaced regardless of membership
    /// (default: 5400000ms, 1h30). Bounds how much of a long call one recovered
    /// key can decrypt.
    #[uniffi(default = None)]
    pub max_key_lifetime_ms: Option<u64>,
    /// Whether to manage media keys (default: true).
    pub manage_media_keys: Option<bool>,
    /// Whether to discard keys from devices that are not cross-signed
    /// (default: true, per MSC4153).
    pub require_cross_signed_sender: Option<bool>,
}

/// What kind of notification an MSC4075 call notification asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiNotificationType {
    /// Ring audibly, for `lifetime_ms`.
    Ring,
    /// Show a visual indication only.
    Notification,
}

/// FFI-friendly MSC4075 notification request, for a join that *starts* a call.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiNotifyConfig {
    /// Ring, or notify silently.
    pub notification_type: FfiNotificationType,
    /// MSC4196 `m.call.intent`, e.g. "audio" or "video". Omitted when unset.
    #[uniffi(default = None)]
    pub intent: Option<String>,
    /// How long the ring stays valid, in milliseconds (default: 30000, capped
    /// at 120000 because that is what receivers honour).
    #[uniffi(default = None)]
    pub lifetime_ms: Option<u64>,
    /// Users named individually in `m.mentions`. Usually empty.
    #[uniffi(default = [])]
    pub mention_user_ids: Vec<String>,
    /// Whether the whole room is targeted (default: true, which is what a call
    /// in a room means). Note that the room's power levels may gate this.
    #[uniffi(default = true)]
    pub mention_room: bool,
}

impl From<FfiNotifyConfig> for matrix_rtc_core::NotifyConfig {
    fn from(value: FfiNotifyConfig) -> Self {
        matrix_rtc_core::NotifyConfig {
            notification_type: match value.notification_type {
                FfiNotificationType::Ring => matrix_rtc_core::NotificationType::Ring,
                FfiNotificationType::Notification => {
                    matrix_rtc_core::NotificationType::Notification
                }
            },
            intent: value.intent,
            lifetime_ms: value.lifetime_ms,
            mentions: matrix_rtc_core::Mentions {
                user_ids: value.mention_user_ids,
                room: value.mention_room,
            },
        }
    }
}

/// FFI-friendly join session parameters.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiJoinSessionParams {
    /// Matrix user ID (e.g., "@alice:example.org")
    pub user_id: String,
    /// Device ID
    pub device_id: String,
    /// Room ID
    pub room_id: String,
    /// Slot ID (e.g., "m.call#ROOM")
    pub slot_id: String,
    /// Application type (e.g., "m.call")
    pub application: String,
    /// The transport to publish on. `None` joins without publishing — valid per
    /// MSC4143, and what a recorder or other observer wants.
    pub transport: Option<FfiTransportConfig>,
    /// Transport types this member can receive on. Only read when `transport`
    /// is `None`; a publishing member advertises its own transport's type.
    pub can_subscribe: Vec<String>,
    /// Optional keep-alive timeout in milliseconds (default: 30000).
    ///
    /// Arms the delayed leave (the dead man's switch for a client that dies).
    pub keep_alive_timeout_ms: Option<u64>,
    /// Optional sticky-map lifetime for our membership, in milliseconds
    /// (default: 3600000).
    ///
    /// A different clock from `keep_alive_timeout_ms`: this is how long the
    /// homeserver keeps the membership at all. The SDK re-sends the membership
    /// at half this interval, so shortening it buys nothing but traffic.
    pub sticky_duration_ms: Option<u64>,
    /// Optional membership lifetime to fall back to on a homeserver that
    /// refuses MSC4140 delayed events, in milliseconds (default: 300000).
    ///
    /// Only ever used on such a homeserver, where nothing clears a crashed
    /// client's membership except this lifetime running out. Raising it trades a
    /// slower cleanup for less signalling; the SDK re-sends the membership at
    /// half this interval, so 300000 means a membership event every 2½ minutes.
    /// Do not lower it below 300000 — MSC4354 says a sticky duration "SHOULD NOT
    /// be set to below 5 minutes", because a server whose clock runs behind
    /// expires sticky events early and a shorter duration cannot absorb that.
    #[uniffi(default = None)]
    pub degraded_lifetime_ms: Option<u64>,
    /// Optional encryption configuration
    pub encryption_config: Option<FfiEncryptionConfig>,
    /// Render this session for an older MatrixRTC generation, to interoperate
    /// with Element Call builds that predate the 2026 MSC4143 rewrite. Unset (or
    /// `Off`) is spec-current, and is what every non-interop join wants.
    ///
    /// This is one decision, not a wire-format flag: it also fixes the
    /// `member.id` we join with, how an inbound media key is bound, the SFU
    /// participant identity and the token endpoint. It is remembered for the room
    /// until the next join, so the media session picks it up on its own rather
    /// than being told again — those two disagreeing produces no error, just a
    /// connected call in which nothing decrypts.
    ///
    /// Reading the 2025 sticky dialect needs no mode and is always on. The other
    /// halves of interop are host obligations; see [`crate::compat`] for the list
    /// per mode.
    #[uniffi(default = None)]
    pub element_call_compat: Option<FfiElementCallCompat>,
    /// Ask for an MSC4075 notification to be sent with this join, so other
    /// devices in the room ring or show an incoming call.
    ///
    /// Unset — the default — joins quietly, which is what joining a call
    /// someone else started does. Set it only when the user is *starting* the
    /// call: the SDK still suppresses the notification if anybody is already in
    /// the session, but the intent to summon anyone at all is yours to state.
    #[uniffi(default = None)]
    pub notify: Option<FfiNotifyConfig>,
    /// How this session handles Element Call reactions and the raised hand.
    /// Unset is enabled with Element Call's three-second window.
    #[uniffi(default = None)]
    pub reactions: Option<FfiReactionsConfig>,
}

/// FFI-friendly reactions configuration (mirrors
/// `matrix_rtc_core::ReactionsConfig`).
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiReactionsConfig {
    /// Whether reactions are handled at all. Off, inbound reactions and raised
    /// hands are ignored and sending fails.
    #[uniffi(default = true)]
    pub enabled: bool,
    /// How long a received reaction counts as active, in milliseconds
    /// (default 3000): repeats from the same member inside it are dropped, and
    /// a host should keep the emoji on screen this long.
    #[uniffi(default = 3000)]
    pub active_window_ms: u64,
    /// The least time between two reactions we send, in milliseconds (default
    /// 3000). A send inside it fails without reaching the homeserver.
    #[uniffi(default = 3000)]
    pub send_cooldown_ms: u64,
}

impl From<FfiReactionsConfig> for matrix_rtc_core::ReactionsConfig {
    fn from(value: FfiReactionsConfig) -> Self {
        matrix_rtc_core::ReactionsConfig {
            enabled: value.enabled,
            active_window_ms: value.active_window_ms,
            send_cooldown_ms: value.send_cooldown_ms,
        }
    }
}

/// FFI-friendly leave session parameters.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiLeaveSessionParams {
    /// Optional MSC4143 leave reason. Defaults to `code = "leave"` when unset.
    pub leave_reason: Option<crate::FfiLeaveReason>,
}

/// Conversion from FFI transport config to core transport type.
impl FfiTransportConfig {
    pub fn into_core(self) -> Result<matrix_rtc_core::RtcTransport, matrix_rtc_core::CommandError> {
        use matrix_rtc_core::{LiveKitTransport, RtcTransport, UnsupportedTransport};
        use std::collections::BTreeMap;

        let mut extra_fields = BTreeMap::new();

        match self.r#type.as_str() {
            "livekit" => {
                let url = self.livekit_service_url.ok_or_else(|| {
                    matrix_rtc_core::CommandError::SendError(
                        "livekit transport requires livekit_service_url".to_string(),
                    )
                })?;
                Ok(RtcTransport::LiveKit(LiveKitTransport {
                    livekit_service_url: url,
                }))
            }
            _ => {
                if let Some(url) = self.livekit_service_url {
                    extra_fields.insert("livekit_service_url".to_string(), url.into());
                }
                Ok(RtcTransport::Unsupported(UnsupportedTransport {
                    transport_type: self.r#type,
                    extra_fields,
                }))
            }
        }
    }
}

impl From<FfiEncryptionConfig> for matrix_rtc_core::EncryptionConfig {
    fn from(value: FfiEncryptionConfig) -> Self {
        matrix_rtc_core::EncryptionConfig {
            delay_before_use_ms: value.delay_before_use_ms.unwrap_or(5000),
            key_rotation_grace_period_ms: value.key_rotation_grace_period_ms.unwrap_or(10000),
            max_key_lifetime_ms: value.max_key_lifetime_ms.unwrap_or(90 * 60 * 1000),
            manage_media_keys: value.manage_media_keys.unwrap_or(true),
            require_cross_signed_sender: value.require_cross_signed_sender.unwrap_or(true),
        }
    }
}

/// Conversion from FFI join params to core join params.
impl FfiJoinSessionParams {
    /// One-line description for logs. Covers everything that decides whether a
    /// join is accepted and how the member is projected — including the
    /// transport intent, which is the field integrators most often get wrong.
    pub(crate) fn summary(&self) -> String {
        let transport = match &self.transport {
            Some(transport) => format!(
                "publish:{}{}",
                transport.r#type,
                transport
                    .livekit_service_url
                    .as_deref()
                    .map(|url| format!("@{url}"))
                    .unwrap_or_default(),
            ),
            None => format!("receive_only:{:?}", self.can_subscribe),
        };

        format!(
            "[{}/{}] user={} device={} application={} transport={} keep_alive={:?}ms \
             encryption={} element_call_compat={:?} notify={:?}",
            self.room_id,
            self.slot_id,
            self.user_id,
            self.device_id,
            self.application,
            transport,
            self.keep_alive_timeout_ms,
            self.encryption_config.is_some(),
            self.element_call_compat.unwrap_or_default(),
            self.notify.as_ref().map(|notify| notify.notification_type),
        )
    }

    pub fn into_core(
        self,
    ) -> Result<matrix_rtc_core::JoinSessionParams, matrix_rtc_core::CommandError> {
        let transport = match self.transport {
            Some(transport) => matrix_rtc_core::TransportIntent::Publish(transport.into_core()?),
            None => matrix_rtc_core::TransportIntent::ReceiveOnly {
                can_subscribe: self.can_subscribe,
            },
        };
        let encryption_config = self.encryption_config.map(Into::into);
        Ok(matrix_rtc_core::JoinSessionParams {
            user_id: self.user_id,
            device_id: self.device_id,
            // Filled in by the join entry points, which generate a fresh id per
            // join and return it: a host-chosen `member.id` reused across joins
            // keeps the MSC4195 participant identity stable while the key index
            // restarts at 0, so peers decrypt new media with a stale key and
            // never recover. The SDK owns it so that is not expressible.
            membership_id: None,
            room_id: self.room_id,
            slot_id: self.slot_id,
            application: self.application,
            transport,
            keep_alive_timeout_ms: self.keep_alive_timeout_ms,
            sticky_duration_ms: self.sticky_duration_ms,
            degraded_lifetime_ms: self.degraded_lifetime_ms,
            encryption_config,
            notify: self.notify.map(Into::into),
            reactions: self.reactions.map(Into::into),
        })
    }
}

/// Conversion from FFI leave params to core leave params.
impl FfiLeaveSessionParams {
    pub fn into_core(self) -> matrix_rtc_core::LeaveSessionParams {
        matrix_rtc_core::LeaveSessionParams {
            leave_reason: self.leave_reason.map(Into::into),
        }
    }
}

/// Callback interface for native code to implement command sending.
///
/// This interface is implemented by the native layer (Kotlin, Swift, C++, etc.)
/// to provide the actual Matrix SDK integration for sending events.
///
/// The native implementation must guarantee:
/// - **Delivery**: Events will be delivered or an error will be reported
/// - **Ordering**: Events will be sent in the order they are received
/// Host-implemented outbound sends.
///
/// Async: every corresponding matrix-rust-sdk call is async, and a synchronous
/// callback forced hosts to bridge that themselves (`runBlocking` on a dedicated
/// dispatcher, on Android). uniffi renders these as `suspend` in Kotlin and
/// `async` in Swift.
///
/// (`async_trait` must sit *under* the uniffi attribute: uniffi parses the
/// original `async fn` tokens, `async_trait` then makes the trait
/// dyn-compatible for the Rust side.)
#[uniffi::export(with_foreign)]
#[async_trait]
pub trait CommandSenderCallback: Send + Sync {
    /// Called when a sticky event needs to be sent.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The wire event type (e.g., "org.matrix.msc4143.rtc.member");
    ///   send it verbatim, it is already translated for you
    /// * `content_json` - The event content as a JSON string
    /// * `duration_ms` - How long the homeserver should keep this entry in the
    ///   sticky map. Pass it through verbatim (matrix-rust-sdk:
    ///   `.with_sticky_duration_ms(durationMs)`); do NOT substitute a value of
    ///   your own. The SDK re-sends the membership before this elapses to stay
    ///   in the call, so a shorter lifetime here silently drops the membership
    ///   mid-call and a longer one leaves a ghost behind.
    ///
    ///   It is a `u64` here and a `u32` in matrix-rust-sdk's
    ///   `withStickyDurationMs`, so the value narrows on the way down. Clamp
    ///   rather than truncate: a bare cast turns an over-large duration into a
    ///   near-instant expiry, which reads as the membership vanishing for no
    ///   reason. The SDK clamps its own resolved value to one hour
    ///   (`MAX_STICKY_DURATION_MS`) before it ever reaches you, so in practice
    ///   the value always fits — the clamp is for hosts that pass their own.
    ///
    /// # Returns
    /// The event id the homeserver assigned — matrix-rust-sdk: the `eventId` on
    /// the send response. Every Matrix send responds with one, so this is never
    /// optional; the SDK needs it to relate an MSC4075 call notification to the
    /// membership event that justifies it.
    ///
    /// Throw a CommandSenderError on failure.
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        duration_ms: u64,
    ) -> Result<String, CommandSenderError>;

    /// Called when a delayed event needs to be scheduled.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type
    /// * `content_json` - The event content as a JSON string
    /// * `delay_ms` - Delay in milliseconds before the event is sent
    ///
    /// # Returns
    /// Return Ok(delay_id) with the MSC4140 delay ID on success, or Err on
    /// failure. That id — not an event id — is what `restartDelayedEvent` and
    /// `cancelDelayedEvent` take.
    ///
    /// Throwing does **not** fail the join. MSC4140 is optional and plenty of
    /// homeservers refuse it (matrix.org: `403 M_FORBIDDEN "Sending delayed
    /// events has been disallowed"`), which costs only the speed of the cleanup
    /// when a client dies, not the call — so the SDK joins anyway and shortens
    /// the membership lifetime instead. Throw
    /// [`CommandSenderError::NotSupported`] rather than `SendError` when the
    /// homeserver has said so, and the SDK will stop asking.
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        delay_ms: u64,
    ) -> Result<String, CommandSenderError>;

    /// Called when a state event needs to be sent.
    ///
    /// Used for `m.rtc.slot`, and — in
    /// [`FfiElementCallCompat::StateEvents`](crate::FfiElementCallCompat::StateEvents)
    /// — for the membership itself, as `org.matrix.msc3401.call.member`. Sending
    /// room state usually needs a raised power level, so return an error if the
    /// homeserver rejects it.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The wire event type (e.g., "org.matrix.msc4143.rtc.slot")
    /// * `state_key` - The state key (for a slot, the slot id)
    /// * `content_json` - The event content as a JSON string
    ///
    /// # Returns
    /// The event id the homeserver assigned, on the same terms as
    /// `sendStickyEvent`. Nothing reads it for a slot; it is required because
    /// in `StateEvents` compat mode the *membership* comes through here, and
    /// there it is what an MSC4075 notification relates to.
    ///
    /// Throw a CommandSenderError on failure.
    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content_json: String,
    ) -> Result<String, CommandSenderError>;

    /// Called when a delayed **state** event needs to be scheduled.
    ///
    /// Only ever called in
    /// [`FfiElementCallCompat::StateEvents`](crate::FfiElementCallCompat::StateEvents),
    /// where the membership is room state and so its dead man's switch has to be
    /// too. Implement it by throwing
    /// [`CommandSenderError::SendError`] in every other mode if you like — the
    /// SDK will not reach it.
    ///
    /// Worth knowing what it buys, because it is more than the sticky path gets:
    /// a delayed sticky leave clears nothing from the sticky map, so crash
    /// cleanup there rides on the entry's TTL. A delayed state event with `{}`
    /// content genuinely empties our membership, which is what MSC4140 was built
    /// for and what this generation of Element Call relies on — room state has
    /// no TTL to lapse.
    ///
    /// So a homeserver that refuses this one is the worst case for a ghost, and
    /// the reason the membership's own `expires` is shortened when it does.
    /// Throwing still does not fail the join; see [`Self::send_delayed_event`].
    ///
    /// matrix-rust-sdk: the MSC4140 delayed **state** send
    /// (`PUT /rooms/{roomId}/state/{eventType}/{stateKey}?org.matrix.msc4140.delay=…`).
    /// Note the state key, which the message-like `sendDelayedEvent` has no room
    /// for — that is the whole reason this is a second method.
    ///
    /// # Returns
    /// Return Ok(delay_id) with the MSC4140 delay ID on success, or Err on
    /// failure. The id is what `restartDelayedEvent` and `cancelDelayedEvent`
    /// take, exactly as for the message-like variant.
    async fn send_delayed_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content_json: String,
        delay_ms: u64,
    ) -> Result<String, CommandSenderError>;

    /// Called periodically to restart a scheduled delayed event's timer.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `delay_id` - The MSC4140 delay ID returned by `sendDelayedEvent`. Not
    ///   an event id: a delayed event has no event id until it fires. Passing
    ///   one here fails silently at the server and surfaces minutes later as the
    ///   dead man's switch retiring a live membership.
    ///
    /// # Implementation
    /// matrix-rust-sdk: `update_delayed_event` with `UpdateAction::Restart`.
    /// This is MSC4140's "heartbeat ping" — it resets the delay's timer to now
    /// plus its original delay, without changing its ID.
    ///
    /// Do NOT emulate it by cancelling and re-scheduling. That leaves the call
    /// unprotected in between, burns the server's `max_scheduled` quota, and a
    /// failed cancel leaks a delay which then fires — and because the sticky map
    /// resolves conflicts by *last to expire*, that leave out-expires the live
    /// membership and shows the user as having left a call they are still in.
    ///
    /// # Returns
    /// Return Ok(()) on success, or Err with a CommandSenderError on failure.
    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandSenderError>;

    /// Called when a previously scheduled delayed event needs to be canceled.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `delay_id` - The MSC4140 delay ID returned by `sendDelayedEvent`, as
    ///   for `restartDelayedEvent`.
    ///
    /// # Returns
    /// Return Ok(()) on success, or Err with a CommandSenderError on failure.
    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandSenderError>;

    /// Called when one to-device message must go to a set of devices (MSC4143).
    ///
    /// The same `content_json` goes to every recipient. Each is one specific
    /// device — send to exactly that device and never widen to the user's other
    /// devices; the SDK never asks for a fan-out.
    ///
    /// # Returns
    ///
    /// One [`FfiToDeviceDelivery`] per recipient, saying whether it was
    /// delivered. **A recipient you report as delivered is recorded as holding
    /// the media key and is never re-sent to**, so reporting a failure as a
    /// success costs that member the rest of the call; mark it failed instead
    /// and the next rollout retries them. A recipient you omit is treated as
    /// undelivered.
    ///
    /// Return `Err` only when the batch could not be attempted at all — then
    /// every recipient is treated as unserved. Do not throw for a single bad
    /// recipient: that abandons the send for every recipient after it.
    ///
    /// matrix-rust-sdk's own `sendToDeviceMessage` already takes a recipient
    /// list, so this maps onto one call.
    async fn send_to_device_message(
        &self,
        recipients: Vec<FfiToDeviceRecipient>,
        message_type: String,
        content_json: String,
    ) -> Result<Vec<FfiToDeviceDelivery>, CommandSenderError>;

    /// Called when a plain room event — message-like, neither sticky nor state
    /// — needs to be sent.
    ///
    /// Used for Element Call reactions (`io.element.call.reaction`) and the
    /// raised-hand `m.reaction` annotation. In an encrypted room the event must
    /// go out encrypted like any other message, which the SDK's ordinary room
    /// send (matrix-rust-sdk: `room.send(...)` / `sendRaw`) does on its own.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type, to send verbatim
    /// * `content_json` - The event content as a JSON string
    ///
    /// # Returns
    /// The event id the homeserver assigned, on the same terms as
    /// `sendStickyEvent`. A raised hand is lowered by redacting this very
    /// event, so the id must come back.
    ///
    /// Throw a CommandSenderError on failure.
    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
    ) -> Result<String, CommandSenderError>;

    /// Called when one of our own room events needs to be redacted.
    ///
    /// Only used to lower a raised hand: Element Call has no "hand lowered"
    /// event, the annotation is redacted instead.
    ///
    /// # Arguments
    /// * `room_id` - The room the event was sent to
    /// * `event_id` - The event to redact, as returned by `sendRoomEvent`
    /// * `reason` - Optional reason to put in the redaction (matrix-rust-sdk:
    ///   `room.redact(eventId, reason, null)`)
    ///
    /// Throw a CommandSenderError on failure.
    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), CommandSenderError>;
}

/// One device a to-device message is addressed to.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiToDeviceRecipient {
    pub user_id: String,
    pub device_id: String,
}

/// What became of one recipient of a to-device send.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiToDeviceDelivery {
    pub user_id: String,
    pub device_id: String,
    /// `null` when delivered; otherwise why not. The reason is surfaced to the
    /// host's logs, not interpreted.
    pub error: Option<String>,
}

/// FFI-friendly command sender that wraps a native callback implementation.
///
/// This struct is created by the FFI layer and passed to core to enable
/// sending commands back to the native Matrix SDK.
///
/// Note: The native callbacks are invoked synchronously during the send_* calls.
/// The callbacks passed to the core's RtcCommandSender methods are invoked immediately
/// based on the native callback's return value.
pub struct FfiCommandSender {
    callback: Arc<dyn CommandSenderCallback>,
    /// The outbound dialect each room's session speaks, registered by
    /// [`RtcSessionManagerHandle::join`](crate::RtcSessionManagerHandle::join)
    /// and empty for every spec-current room — which is the common case, and
    /// costs one uncontended lock and a miss.
    ///
    /// Keyed by room rather than by `(room, slot)` because a to-device media key
    /// names only its room: the core's key content carries `room_id` and nothing
    /// that identifies a slot. Two slots of one room joined in different modes is
    /// therefore not expressible, which is no loss — the mode exists to talk to a
    /// generation of Element Call that has one call per room.
    ///
    /// A `std::sync::Mutex` on purpose: it is only ever held to clone a dialect
    /// out or to insert one, never across an await.
    dialects: Mutex<HashMap<String, OutboundDialect>>,
}

impl FfiCommandSender {
    /// Creates a new FfiCommandSender with the given native callback implementation.
    ///
    /// Returns an `Arc<FfiCommandSender>` for thread-safe sharing with the core.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(callback: Arc<dyn CommandSenderCallback>) -> Arc<FfiCommandSender> {
        Arc::new(Self {
            callback,
            dialects: Mutex::new(HashMap::new()),
        })
    }

    /// Make every later send for `room_id` speak `dialect`.
    ///
    /// Replaces any previous one: a rejoin in a different mode is the whole point
    /// of setting this per join.
    pub(crate) fn set_dialect(&self, room_id: &str, dialect: OutboundDialect) {
        match self.dialects.lock() {
            Ok(mut dialects) => {
                dialects.insert(room_id.to_owned(), dialect);
            }
            // Only reachable if a previous holder panicked while holding it,
            // which for a map insert means the process is already in trouble.
            // Carrying on spec-current is the safe reading of "we don't know".
            Err(error) => log::error!("could not register the outbound dialect: {error}"),
        }
    }

    /// Forget `room_id`'s dialect, after a leave has been rendered in it.
    pub(crate) fn clear_dialect(&self, room_id: &str) {
        if let Ok(mut dialects) = self.dialects.lock() {
            dialects.remove(room_id);
        }
    }

    /// The dialect for `room_id`, or [`OutboundDialect::None`] — an unregistered
    /// room is a spec-current one.
    fn dialect(&self, room_id: &str) -> OutboundDialect {
        self.dialects
            .lock()
            .ok()
            .and_then(|dialects| dialects.get(room_id).cloned())
            .unwrap_or(OutboundDialect::None)
    }

    /// The dialect a to-device message is rendered in.
    ///
    /// A media key names its room inside the content, which is the only routing
    /// information a to-device send has — the signature carries recipients, not a
    /// room.
    fn dialect_for_content(&self, content: &Value) -> OutboundDialect {
        content
            .get("room_id")
            .and_then(Value::as_str)
            .map(|room_id| self.dialect(room_id))
            .unwrap_or(OutboundDialect::None)
    }
}

use async_trait::async_trait;

/// Logs what the host callback made of an outbound command.
///
/// Every command leaving the SDK goes through here, so "the core decided to
/// send X" and "the host accepted/rejected X" are always adjacent in the log —
/// which is what separates an SDK bug from a host-integration bug.
fn log_command<T>(what: &str, outcome: Result<T, CommandError>) -> Result<T, CommandError> {
    match &outcome {
        Ok(_) => log::debug!("command sent: {what}"),
        Err(error) => log::warn!("command failed: {what}: {error}"),
    }
    outcome
}

/// Kept at `trace`: the content of a to-device message is key material.
fn trace_command_content(what: &str, content_json: &str) {
    log::trace!("command sending: {what} content={content_json}");
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl RtcCommandSender for FfiCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        // Anything that is not a membership routes through untouched; only its
        // carrier follows the mode — the pre-sticky generation has no sticky
        // map, so there it becomes an ordinary room event. See
        // `matrix_rtc_bridge::compat`.
        let dialect = self.dialect(&room_id);
        let content = dialect.rewrite_notification(&event_type, content);
        match dialect.route_member_event(event_type, content, Some(duration_ms)) {
            MemberEventRoute::Sticky {
                event_type,
                content,
            } => {
                let content_json = serde_json::to_string(&content)
                    .map_err(|e| CommandError::SerializationError(e.to_string()))?;

                let wire_event_type = wire_type(event_type);
                let what = format!("sticky [{room_id}] type={wire_event_type}");
                trace_command_content(&what, &content_json);

                log_command(
                    &what,
                    self.callback
                        .send_sticky_event(room_id, wire_event_type, content_json, duration_ms)
                        .await
                        .map_err(CommandError::from),
                )
            }
            // The notification for a `StateEvents` room: a plain room event, so
            // a host whose SDK has no MSC4354 is never asked for a sticky send.
            // `duration_ms` has no meaning for it. Through `wire_type`, unlike
            // the state arm below: this type *is* the core's.
            MemberEventRoute::Room {
                event_type,
                content,
            } => {
                let content_json = serde_json::to_string(&content)
                    .map_err(|e| CommandError::SerializationError(e.to_string()))?;

                let wire_event_type = wire_type(event_type);
                let what = format!("room event [{room_id}] type={wire_event_type} (pre-sticky)");
                trace_command_content(&what, &content_json);

                log_command(
                    &what,
                    self.callback
                        .send_room_event(room_id, wire_event_type, content_json)
                        .await
                        .map_err(CommandError::from),
                )
            }
            // `duration_ms` is dropped on purpose: room state has no TTL, and in
            // this dialect the lifetime is stated inside the content instead
            // (`created_ts` + `expires`). The core's periodic re-send still fires
            // and is harmless — the dialect pins `created_ts`, so the content is
            // byte-identical and moves nothing a peer reads.
            //
            // The type is already the legacy wire id, so it does *not* go through
            // `wire_type`: that table is the core's own alias map, and this type
            // is not the core's.
            MemberEventRoute::State {
                event_type,
                state_key,
                content,
            } => {
                let content_json = serde_json::to_string(&content)
                    .map_err(|e| CommandError::SerializationError(e.to_string()))?;

                let what = format!("state [{room_id}] type={event_type} state_key={state_key}");
                trace_command_content(&what, &content_json);

                log_command(
                    &what,
                    self.callback
                        .send_state_event(room_id, event_type.to_owned(), state_key, content_json)
                        .await
                        .map_err(CommandError::from),
                )
            }
        }
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        // The delayed leave is a member event like any other, and a peer that
        // cannot read it is a peer we stay visible to forever — so it goes
        // through the same routing as the join it is paired with. No lifetime:
        // its legacy content is `{}`, which has nowhere to carry a deadline.
        match self
            .dialect(&room_id)
            .route_member_event(event_type, content, None)
        {
            // One arm for both carriers: a delayed event is a plain send in
            // every generation.
            MemberEventRoute::Sticky {
                event_type,
                content,
            }
            | MemberEventRoute::Room {
                event_type,
                content,
            } => {
                let content_json = serde_json::to_string(&content)
                    .map_err(|e| CommandError::SerializationError(e.to_string()))?;

                let wire_event_type = wire_type(event_type);
                let what = format!("delayed [{room_id}] type={wire_event_type} delay={delay_ms}ms");
                trace_command_content(&what, &content_json);

                log_command(
                    &what,
                    self.callback
                        .send_delayed_event(room_id, wire_event_type, content_json, delay_ms)
                        .await
                        .map_err(CommandError::from),
                )
            }
            MemberEventRoute::State {
                event_type,
                state_key,
                content,
            } => {
                let content_json = serde_json::to_string(&content)
                    .map_err(|e| CommandError::SerializationError(e.to_string()))?;

                let what = format!(
                    "delayed state [{room_id}] type={event_type} state_key={state_key} \
                     delay={delay_ms}ms"
                );
                trace_command_content(&what, &content_json);

                log_command(
                    &what,
                    self.callback
                        .send_delayed_state_event(
                            room_id,
                            event_type.to_owned(),
                            state_key,
                            content_json,
                            delay_ms,
                        )
                        .await
                        .map_err(CommandError::from),
                )
            }
        }
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        let what = format!("restart delayed [{room_id}] delay_id={delay_id}");
        log_command(
            &what,
            self.callback
                .restart_delayed_event(room_id, delay_id)
                .await
                .map_err(CommandError::from),
        )
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        let what = format!("cancel delayed [{room_id}] delay_id={delay_id}");

        log_command(
            &what,
            self.callback
                .cancel_delayed_event(room_id, delay_id)
                .await
                .map_err(CommandError::from),
        )
    }

    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let content_json = serde_json::to_string(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        // Verbatim, not through `wire_type`: a reaction is not a MatrixRTC type
        // and has no unstable alias in that table.
        let what = format!("room event [{room_id}] type={event_type}");
        trace_command_content(&what, &content_json);

        log_command(
            &what,
            self.callback
                .send_room_event(room_id, event_type, content_json)
                .await
                .map_err(CommandError::from),
        )
    }

    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), CommandError> {
        let what = format!("redact [{room_id}] event_id={event_id}");
        log_command(
            &what,
            self.callback
                .redact_event(room_id, event_id, reason)
                .await
                .map_err(CommandError::from),
        )
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<matrix_rtc_core::ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<matrix_rtc_core::ToDeviceDelivery>, CommandError> {
        // Unlike a member event, a to-device message cannot carry both dialects
        // at once — the type is one or the other — so in compat mode the media
        // key goes out in the legacy dialect alone, and is exchanged with legacy
        // peers rather than spec-current ones.
        let (message_type, content) = self
            .dialect_for_content(&content)
            .rewrite_key_message(&message_type, &content)
            .unwrap_or((message_type, content));

        let content_json = serde_json::to_string(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        let wire_message_type = wire_type(message_type);
        let what = format!(
            "to-device type={wire_message_type} to {} recipient(s)",
            recipients.len(),
        );
        trace_command_content(&what, &content_json);

        let ffi_recipients: Vec<FfiToDeviceRecipient> = recipients
            .iter()
            .map(|recipient| FfiToDeviceRecipient {
                user_id: recipient.user_id.clone(),
                device_id: recipient.device_id.clone(),
            })
            .collect();

        let deliveries = log_command(
            &what,
            self.callback
                .send_to_device_message(ffi_recipients, wire_message_type, content_json)
                .await
                .map_err(CommandError::from),
        )?;

        Ok(deliveries
            .into_iter()
            .map(|delivery| {
                let recipient =
                    matrix_rtc_core::ToDeviceRecipient::new(delivery.user_id, delivery.device_id);
                match delivery.error {
                    Some(error) => matrix_rtc_core::ToDeviceDelivery::failed(recipient, error),
                    None => matrix_rtc_core::ToDeviceDelivery::sent(recipient),
                }
            })
            .collect())
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let content_json = serde_json::to_string(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        let wire_event_type = wire_type(event_type);
        let what = format!("state [{room_id}] type={wire_event_type} state_key={state_key}");
        trace_command_content(&what, &content_json);

        log_command(
            &what,
            self.callback
                .send_state_event(room_id, wire_event_type, state_key, content_json)
                .await
                .map_err(CommandError::from),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Mock callback that records the event type each send was given, so tests
    /// can assert on what a native host would actually put on the wire.
    ///
    /// To-device sends are recorded in full — recipient and content — because
    /// "which member was handed which key index" is the question most key
    /// distribution bugs turn on, and the event type alone cannot answer it.
    #[derive(Default)]
    struct MockCommandSenderCallback {
        sent_types: Mutex<Vec<String>>,
        sticky_duration_ms: Mutex<Option<u64>>,
        to_device: Mutex<Vec<ToDeviceSend>>,
        /// Every sticky and state send in full, for the compat tests: which
        /// carrier a membership took, and under what content, is the whole
        /// question there and the event type alone cannot answer it.
        sends: Mutex<Vec<Send>>,
        /// Model a homeserver with MSC4140 switched off, the way matrix.org
        /// answers: every delayed send is refused.
        refuse_delayed: std::sync::atomic::AtomicBool,
    }

    #[derive(Clone)]
    struct ToDeviceSend {
        user_id: String,
        device_id: String,
        content: serde_json::Value,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Carrier {
        Sticky,
        State,
        DelayedSticky,
        DelayedState,
        Room,
    }

    #[derive(Clone, Debug)]
    struct Send {
        carrier: Carrier,
        event_type: String,
        state_key: Option<String>,
        content: serde_json::Value,
    }

    impl MockCommandSenderCallback {
        fn record(&self, event_type: &str) {
            self.sent_types.lock().unwrap().push(event_type.to_owned());
        }

        fn record_send(
            &self,
            carrier: Carrier,
            event_type: &str,
            state_key: Option<String>,
            content_json: &str,
        ) {
            self.sends.lock().unwrap().push(Send {
                carrier,
                event_type: event_type.to_owned(),
                state_key,
                content: serde_json::from_str(content_json).unwrap_or(serde_json::Value::Null),
            });
        }

        fn sends(&self) -> Vec<Send> {
            self.sends.lock().unwrap().clone()
        }

        fn sent_types(&self) -> Vec<String> {
            self.sent_types.lock().unwrap().clone()
        }

        fn to_device_for(&self, user_id: &str, device_id: &str) -> Vec<serde_json::Value> {
            self.to_device
                .lock()
                .unwrap()
                .iter()
                .filter(|send| send.user_id == user_id && send.device_id == device_id)
                .map(|send| send.content.clone())
                .collect()
        }

        fn clear_to_device(&self) {
            self.to_device.lock().unwrap().clear();
        }

        /// matrix.org's answer to a delayed send, verbatim.
        fn delayed_refusal(&self) -> Option<CommandSenderError> {
            self.refuse_delayed
                .load(std::sync::atomic::Ordering::Relaxed)
                .then(|| {
                    CommandSenderError::NotSupported(
                        "Sending delayed events has been disallowed".to_owned(),
                    )
                })
        }
    }

    #[async_trait]
    impl CommandSenderCallback for MockCommandSenderCallback {
        async fn send_sticky_event(
            &self,
            _room_id: String,
            event_type: String,
            content_json: String,
            duration_ms: u64,
        ) -> Result<String, CommandSenderError> {
            self.record(&event_type);
            self.record_send(Carrier::Sticky, &event_type, None, &content_json);
            *self.sticky_duration_ms.lock().unwrap() = Some(duration_ms);
            Ok("$sticky".to_owned())
        }

        async fn send_delayed_event(
            &self,
            room_id: String,
            event_type: String,
            content_json: String,
            _delay_ms: u64,
        ) -> Result<String, CommandSenderError> {
            if let Some(refusal) = self.delayed_refusal() {
                return Err(refusal);
            }
            self.record(&event_type);
            self.record_send(Carrier::DelayedSticky, &event_type, None, &content_json);
            Ok(format!("event-{}-{}", room_id, event_type))
        }

        async fn send_state_event(
            &self,
            _room_id: String,
            event_type: String,
            state_key: String,
            content_json: String,
        ) -> Result<String, CommandSenderError> {
            self.record(&event_type);
            self.record_send(Carrier::State, &event_type, Some(state_key), &content_json);
            Ok("$state".to_owned())
        }

        async fn send_delayed_state_event(
            &self,
            room_id: String,
            event_type: String,
            state_key: String,
            content_json: String,
            _delay_ms: u64,
        ) -> Result<String, CommandSenderError> {
            if let Some(refusal) = self.delayed_refusal() {
                return Err(refusal);
            }
            self.record(&event_type);
            self.record_send(
                Carrier::DelayedState,
                &event_type,
                Some(state_key),
                &content_json,
            );
            Ok(format!("delayed-state-{}-{}", room_id, event_type))
        }

        async fn restart_delayed_event(
            &self,
            _room_id: String,
            _delay_id: String,
        ) -> Result<(), CommandSenderError> {
            self.record("restart_delayed_event");
            Ok(())
        }

        async fn cancel_delayed_event(
            &self,
            _room_id: String,
            _delay_id: String,
        ) -> Result<(), CommandSenderError> {
            Ok(())
        }

        async fn send_room_event(
            &self,
            _room_id: String,
            event_type: String,
            content_json: String,
        ) -> Result<String, CommandSenderError> {
            self.record(&event_type);
            self.record_send(Carrier::Room, &event_type, None, &content_json);
            Ok("$room-event".to_owned())
        }

        async fn redact_event(
            &self,
            _room_id: String,
            _event_id: String,
            _reason: Option<String>,
        ) -> Result<(), CommandSenderError> {
            self.record("m.room.redaction");
            Ok(())
        }

        async fn send_to_device_message(
            &self,
            recipients: Vec<FfiToDeviceRecipient>,
            message_type: String,
            content_json: String,
        ) -> Result<Vec<FfiToDeviceDelivery>, CommandSenderError> {
            self.record(&message_type);
            let content: serde_json::Value =
                serde_json::from_str(&content_json).unwrap_or(serde_json::Value::Null);
            let mut guard = self.to_device.lock().unwrap();
            let deliveries = recipients
                .into_iter()
                .map(|recipient| {
                    guard.push(ToDeviceSend {
                        user_id: recipient.user_id.clone(),
                        device_id: recipient.device_id.clone(),
                        content: content.clone(),
                    });
                    FfiToDeviceDelivery {
                        user_id: recipient.user_id,
                        device_id: recipient.device_id,
                        error: None,
                    }
                })
                .collect();
            Ok(deliveries)
        }
    }

    /// Lets a test keep hold of the mock after handing it to
    /// `set_command_sender`, which takes ownership of a `Box`.
    #[async_trait]
    impl CommandSenderCallback for Arc<MockCommandSenderCallback> {
        async fn send_sticky_event(
            &self,
            room_id: String,
            event_type: String,
            content_json: String,
            duration_ms: u64,
        ) -> Result<String, CommandSenderError> {
            (**self)
                .send_sticky_event(room_id, event_type, content_json, duration_ms)
                .await
        }

        async fn send_delayed_event(
            &self,
            room_id: String,
            event_type: String,
            content_json: String,
            delay_ms: u64,
        ) -> Result<String, CommandSenderError> {
            (**self)
                .send_delayed_event(room_id, event_type, content_json, delay_ms)
                .await
        }

        async fn send_state_event(
            &self,
            room_id: String,
            event_type: String,
            state_key: String,
            content_json: String,
        ) -> Result<String, CommandSenderError> {
            (**self)
                .send_state_event(room_id, event_type, state_key, content_json)
                .await
        }

        async fn send_delayed_state_event(
            &self,
            room_id: String,
            event_type: String,
            state_key: String,
            content_json: String,
            delay_ms: u64,
        ) -> Result<String, CommandSenderError> {
            (**self)
                .send_delayed_state_event(room_id, event_type, state_key, content_json, delay_ms)
                .await
        }

        async fn restart_delayed_event(
            &self,
            room_id: String,
            delay_id: String,
        ) -> Result<(), CommandSenderError> {
            (**self).restart_delayed_event(room_id, delay_id).await
        }

        async fn cancel_delayed_event(
            &self,
            room_id: String,
            delay_id: String,
        ) -> Result<(), CommandSenderError> {
            (**self).cancel_delayed_event(room_id, delay_id).await
        }

        async fn send_room_event(
            &self,
            room_id: String,
            event_type: String,
            content_json: String,
        ) -> Result<String, CommandSenderError> {
            (**self)
                .send_room_event(room_id, event_type, content_json)
                .await
        }

        async fn redact_event(
            &self,
            room_id: String,
            event_id: String,
            reason: Option<String>,
        ) -> Result<(), CommandSenderError> {
            (**self).redact_event(room_id, event_id, reason).await
        }

        async fn send_to_device_message(
            &self,
            recipients: Vec<FfiToDeviceRecipient>,
            message_type: String,
            content_json: String,
        ) -> Result<Vec<FfiToDeviceDelivery>, CommandSenderError> {
            (**self)
                .send_to_device_message(recipients, message_type, content_json)
                .await
        }
    }

    fn mock_sender() -> (Arc<MockCommandSenderCallback>, Arc<FfiCommandSender>) {
        let mock = Arc::new(MockCommandSenderCallback::default());
        let sender = FfiCommandSender::new(mock.clone() as Arc<dyn CommandSenderCallback>);
        (mock, sender)
    }

    #[tokio::test]
    async fn test_ffi_command_sender_sends_sticky_event() {
        let (mock, command_sender) = mock_sender();

        let result = command_sender
            .send_sticky_event(
                "!room:example.org".to_string(),
                "m.rtc.member".to_string(),
                serde_json::json!({
                    "slot_id": "m.call#ROOM",
                    "sticky_key": "alice-device-a"
                }),
                90_000,
            )
            .await;

        assert!(result.is_ok());
        // The host must see the unstable id: peers do not match on `m.rtc.member`.
        assert_eq!(mock.sent_types(), ["org.matrix.msc4143.rtc.member"]);
        // The sticky lifetime must reach the host unchanged: the core schedules
        // its refresh against exactly this number.
        assert_eq!(*mock.sticky_duration_ms.lock().unwrap(), Some(90_000));
    }

    #[tokio::test]
    async fn test_ffi_command_sender_sends_delayed_event() {
        let (mock, command_sender) = mock_sender();

        let result = command_sender
            .send_delayed_event(
                "!room:example.org".to_string(),
                "m.rtc.member".to_string(),
                serde_json::json!({
                    "slot_id": "m.call#ROOM",
                    "sticky_key": "alice-device-a"
                }),
                30000,
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "event-!room:example.org-org.matrix.msc4143.rtc.member"
        );
        assert_eq!(mock.sent_types(), ["org.matrix.msc4143.rtc.member"]);
    }

    /// The delayed leave must carry the same wire id as the join sticky it
    /// cancels out, and the slot state event the same id other clients read.
    #[tokio::test]
    async fn test_ffi_command_sender_translates_every_outbound_type() {
        let (mock, command_sender) = mock_sender();

        command_sender
            .send_state_event(
                "!room:example.org".to_string(),
                matrix_rtc_core::SLOT_EVENT_TYPE.to_string(),
                "m.call#ROOM".to_string(),
                serde_json::json!({ "status": "open" }),
            )
            .await
            .unwrap();
        command_sender
            .send_to_device_message(
                vec![matrix_rtc_core::ToDeviceRecipient::new(
                    "@bob:example.org",
                    "device456",
                )],
                matrix_rtc_core::KEY_MESSAGE_TYPE.to_string(),
                serde_json::json!({ "key": "…" }),
            )
            .await
            .unwrap();

        assert_eq!(
            mock.sent_types(),
            [
                "org.matrix.msc4143.rtc.slot",
                "org.matrix.msc4143.rtc.encryption_key",
            ]
        );
    }

    /// A host's own event types are none of this binding's business.
    #[test]
    fn test_unknown_event_types_pass_through() {
        assert_eq!(
            wire_type("com.example.custom".to_owned()),
            "com.example.custom"
        );
    }

    fn join_params() -> FfiJoinSessionParams {
        FfiJoinSessionParams {
            user_id: "@alice:example.org".to_owned(),
            device_id: "DEVICE".to_owned(),
            room_id: "!room:example.org".to_owned(),
            slot_id: "m.call#ROOM".to_owned(),
            application: "m.call".to_owned(),
            transport: Some(FfiTransportConfig {
                r#type: "livekit".to_owned(),
                livekit_service_url: Some("https://sfu.example.org".to_owned()),
            }),
            can_subscribe: Vec::new(),
            keep_alive_timeout_ms: None,
            sticky_duration_ms: None,
            degraded_lifetime_ms: None,
            encryption_config: None,
            element_call_compat: None,
            notify: None,
            reactions: None,
        }
    }

    /// The SDK owns the `member.id`, and a rejoin must not reuse the previous
    /// one. Reuse keeps the MSC4195 participant identity stable while our key
    /// index restarts at 0, so every peer decrypts the new call's media with the
    /// old call's key and never recovers — which is why hosts cannot supply one.
    #[tokio::test]
    async fn every_join_gets_a_fresh_member_id() {
        let manager = crate::RtcSessionManagerHandle::new();
        manager
            .set_command_sender(Arc::new(MockCommandSenderCallback::default()))
            .await
            .expect("the mock sender should be accepted");
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        let first = manager.join(join_params()).await.expect("first join");
        assert_eq!(
            manager
                .own_member_id(room_id.clone(), slot_id.clone())
                .await
                .expect("the call itself should succeed"),
            Some(first.clone()),
            "the id returned by join must be the one the membership was published under",
        );

        manager
            .leave(
                room_id.clone(),
                slot_id.clone(),
                FfiLeaveSessionParams { leave_reason: None },
            )
            .await
            .expect("leave");
        let second = manager.join(join_params()).await.expect("rejoin");

        assert_ne!(first, second, "a rejoin must not reuse the member id");
        assert_eq!(
            manager
                .own_member_id(room_id, slot_id)
                .await
                .expect("the call itself should succeed"),
            Some(second),
        );
    }

    /// The keep-alive driver a join starts has to actually beat.
    ///
    /// It once spawned `run_heartbeat` on a bare OS thread — the closure built
    /// the future and returned it, nothing polled it, and the thread exited.
    /// That logs identically to a working driver and registers identically in
    /// `heartbeats`, so the assertion has to be on what reached the command
    /// sender: no `restart_delayed_event` means the dead man's switch fires
    /// mid-call.
    #[tokio::test]
    async fn the_keep_alive_driver_restarts_the_delayed_leave() {
        let mock = Arc::new(MockCommandSenderCallback::default());
        let manager = crate::RtcSessionManagerHandle::new();
        manager
            .set_command_sender(Arc::new(mock.clone()))
            .await
            .expect("the mock sender should be accepted");

        manager.join(join_params()).await.expect("join");
        // Replaces the driver the join just registered with one fast enough to
        // watch. Same code path, only the interval differs.
        manager.start_heartbeat_every(
            "!room:example.org".to_owned(),
            "m.call#ROOM".to_owned(),
            Duration::from_millis(50),
        );

        tokio::time::sleep(Duration::from_millis(300)).await;

        let beats = mock
            .sent_types()
            .into_iter()
            .filter(|sent| sent == "restart_delayed_event")
            .count();
        assert!(
            beats >= 2,
            "the driver should have restarted the delayed leave several times over 300ms at a \
             50ms interval, saw {beats}",
        );
    }

    /// A driver must not outlive the session it beats for: a beat landing after
    /// the leave has cancelled the delayed event would re-arm a leave nobody
    /// cancels.
    #[tokio::test]
    async fn leaving_stops_the_keep_alive_driver() {
        let mock = Arc::new(MockCommandSenderCallback::default());
        let manager = crate::RtcSessionManagerHandle::new();
        manager
            .set_command_sender(Arc::new(mock.clone()))
            .await
            .expect("the mock sender should be accepted");
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        manager.join(join_params()).await.expect("join");
        manager.start_heartbeat_every(room_id.clone(), slot_id.clone(), Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(150)).await;

        manager
            .leave(
                room_id,
                slot_id,
                crate::FfiLeaveSessionParams { leave_reason: None },
            )
            .await
            .expect("leave");
        let after_leave = mock
            .sent_types()
            .into_iter()
            .filter(|sent| sent == "restart_delayed_event")
            .count();

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            mock.sent_types()
                .into_iter()
                .filter(|sent| sent == "restart_delayed_event")
                .count(),
            after_leave,
            "no beat should reach the sender once the session has left",
        );
    }

    fn joined_sticky(user_id: &str, device_id: &str, member_id: &str) -> crate::StickyEvent {
        crate::StickyEvent {
            room_id: "!room:example.org".to_owned(),
            event_id: None,
            sender: user_id.to_owned(),
            sender_device_id: Some(device_id.to_owned()),
            was_encrypted: Some(true),
            event_type: "m.rtc.member".to_owned(),
            slot_id: "m.call#ROOM".to_owned(),
            sticky_key: member_id.to_owned(),
            application_type: Some("m.call".to_owned()),
            member_id: Some(member_id.to_owned()),
            membership: Some("join".to_owned()),
            leave_reason: None,
            transports_json: None,
        }
    }

    /// The same second-call regression as
    /// `matrix_rtc_core::tests::a_rejoin_in_the_same_process_distributes_a_key_to_the_incumbent`,
    /// but driven through the handle an FFI host actually holds — the path where
    /// an Android integration hit it. Kept at this layer too because the host
    /// reaches key distribution only via `join`/`leave` on the manager handle,
    /// and a fix that works in the core but not through the handle is no fix.
    #[tokio::test]
    async fn a_rejoin_distributes_keys_without_new_sticky_events() {
        let mock = Arc::new(MockCommandSenderCallback::default());
        let manager = crate::RtcSessionManagerHandle::new();
        manager
            .set_command_sender(Arc::new(mock.clone()))
            .await
            .expect("the mock sender should be accepted");
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        manager
            .on_room_encryption_received(room_id.clone(), true)
            .await
            .expect("room encryption");
        manager
            .on_room_slots_received(
                room_id.clone(),
                vec![crate::SlotEvent {
                    slot_id: slot_id.clone(),
                    content_json: r#"{ "status": "open",
                                       "application": { "type": "m.call" },
                                       "encryption": { "type": "m.per_member" } }"#
                        .to_owned(),
                }],
            )
            .await
            .expect("room slots");

        // First call: bob arrives after we joined, so there is a roster change.
        manager.join(join_params()).await.expect("first join");
        manager
            .set_current_sticky_state(
                room_id.clone(),
                vec![joined_sticky("@bob:example.org", "BOBDEV", "bob-a")],
            )
            .await
            .expect("bob's membership");
        assert!(
            !mock.to_device_for("@bob:example.org", "BOBDEV").is_empty(),
            "first call should have distributed a key to the incumbent"
        );

        manager
            .leave(
                room_id.clone(),
                slot_id.clone(),
                FfiLeaveSessionParams { leave_reason: None },
            )
            .await
            .expect("leave");
        mock.clear_to_device();

        // Second call: no new sticky events at all. Bob has not moved.
        let second = manager.join(join_params()).await.expect("rejoin");

        let sent = mock.to_device_for("@bob:example.org", "BOBDEV");
        assert!(
            !sent.is_empty(),
            "the second call distributed no key to the incumbent"
        );
        assert_eq!(
            sent[0].pointer("/member_id").and_then(|v| v.as_str()),
            Some(second.as_str()),
            "the key must be advertised under the member id this join published",
        );
    }

    /// A host can open the slot its own call needs — the reason this is exported
    /// at all is that no generation of Element Call publishes one, so in an
    /// interop room nobody else will.
    #[tokio::test]
    async fn opening_and_closing_a_slot_publishes_the_state_a_peer_reads() {
        let mock = Arc::new(MockCommandSenderCallback::default());
        let manager = crate::RtcSessionManagerHandle::new();
        manager
            .set_command_sender(Arc::new(mock.clone()))
            .await
            .expect("the mock sender should be accepted");

        manager
            .open_slot(
                "!room:example.org".to_owned(),
                "m.call#ROOM".to_owned(),
                "m.call".to_owned(),
                Some(crate::FfiSlotEncryption::PerMember),
            )
            .await
            .expect("open");
        manager
            .close_slot("!room:example.org".to_owned(), "m.call#ROOM".to_owned())
            .await
            .expect("close");

        let sends = mock.sends();
        assert_eq!(sends.len(), 2);
        // The unstable id, and the slot id as the state key: a peer matches on
        // both, and `m.rtc.slot` on the wire is a slot nobody sees.
        assert_eq!(sends[0].event_type, "org.matrix.msc4143.rtc.slot");
        assert_eq!(sends[0].state_key.as_deref(), Some("m.call#ROOM"));
        assert_eq!(
            sends[0].content,
            serde_json::json!({
                "status": "open",
                "application": { "type": "m.call" },
                "encryption": { "type": "m.per_member" },
            }),
        );
        assert_eq!(
            sends[1].content.get("status").unwrap(),
            &serde_json::json!("closed"),
        );
    }

    /// MSC4143 makes the slot id the state key and requires it to start with
    /// `{application}#`. A homeserver would accept anything; every client would
    /// then treat the slot as closed, which is indistinguishable from the call
    /// never starting.
    #[tokio::test]
    async fn a_slot_id_that_contradicts_its_application_is_refused() {
        let mock = Arc::new(MockCommandSenderCallback::default());
        let manager = crate::RtcSessionManagerHandle::new();
        manager
            .set_command_sender(Arc::new(mock.clone()))
            .await
            .expect("the mock sender should be accepted");

        let result = manager
            .open_slot(
                "!room:example.org".to_owned(),
                "m.call#ROOM".to_owned(),
                "m.something.else".to_owned(),
                None,
            )
            .await;

        assert!(result.is_err());
        assert!(mock.sends().is_empty(), "nothing should have been sent");
    }

    // --- Element Call compatibility ------------------------------------------
    //
    // The dialects themselves are tested exhaustively in
    // `matrix_rtc_bridge::compat`. What is tested here is the binding: that a
    // mode chosen on `join` reaches every send it has to, including the two the
    // join itself makes, and that it reaches no other room.

    fn compat_join(compat: FfiElementCallCompat) -> FfiJoinSessionParams {
        FfiJoinSessionParams {
            element_call_compat: Some(compat),
            keep_alive_timeout_ms: Some(30_000),
            ..join_params()
        }
    }

    async fn compat_manager() -> (
        Arc<MockCommandSenderCallback>,
        Arc<crate::RtcSessionManagerHandle>,
    ) {
        let mock = Arc::new(MockCommandSenderCallback::default());
        let manager = crate::RtcSessionManagerHandle::new();
        manager
            .set_command_sender(Arc::new(mock.clone()))
            .await
            .expect("the mock sender should be accepted");
        (mock, manager)
    }

    /// The 2025 dialect is additive, and has to be: one event on the wire serves
    /// both readers, so joining in this mode costs a spec-current peer nothing.
    #[tokio::test]
    async fn a_sticky_compat_join_is_readable_by_both_generations() {
        let (mock, manager) = compat_manager().await;

        let member_id = manager
            .join(compat_join(FfiElementCallCompat::StickyEvents))
            .await
            .expect("join");

        let membership = mock
            .sends()
            .into_iter()
            .find(|send| send.carrier == Carrier::Sticky)
            .expect("the membership should still be a sticky event");
        assert_eq!(membership.event_type, "org.matrix.msc4143.rtc.member");
        assert_eq!(
            membership.content.pointer("/member/id").unwrap(),
            &serde_json::json!(member_id),
            "the MSC4143 fields must survive the rewrite",
        );
        // Without these two, Element Call cannot learn our device and so cannot
        // address a media key to us at all: it runs as a widget, and the widget
        // API gives it no decryption metadata to read one from.
        assert_eq!(
            membership.content.pointer("/member/user_id").unwrap(),
            &serde_json::json!("@alice:example.org"),
        );
        assert_eq!(
            membership.content.pointer("/member/device_id").unwrap(),
            &serde_json::json!("DEVICE"),
        );
        // Where that generation looks for our SFU.
        assert_eq!(
            membership
                .content
                .pointer("/rtc_transports/0/livekit_service_url")
                .unwrap(),
            &serde_json::json!("https://sfu.example.org"),
        );
    }

    /// The pre-sticky dialect changes the *carrier*, so nothing about this join
    /// is a sticky event — and the member id stops being random, because in that
    /// generation it is also the `membershipID` and the SFU identity.
    #[tokio::test]
    async fn a_pre_sticky_join_publishes_room_state() {
        let (mock, manager) = compat_manager().await;

        let member_id = manager
            .join(compat_join(FfiElementCallCompat::StateEvents))
            .await
            .expect("join");
        assert_eq!(member_id, "@alice:example.org:DEVICE");

        let sends = mock.sends();
        assert!(
            sends.iter().all(|send| send.carrier != Carrier::Sticky),
            "a pre-sticky membership must not also go out as a sticky event",
        );

        let membership = sends
            .iter()
            .find(|send| send.carrier == Carrier::State)
            .expect("the membership should be room state");
        assert_eq!(membership.event_type, "org.matrix.msc3401.call.member");
        // The leading underscore is not decoration: Synapse rejects a state key
        // that looks like a user id from anyone but that user.
        assert_eq!(
            membership.state_key.as_deref(),
            Some("_@alice:example.org_DEVICE_m.call"),
        );
        assert_eq!(
            membership.content.get("membershipID").unwrap(),
            &serde_json::json!(member_id),
        );
        assert_eq!(
            membership.content.get("device_id").unwrap(),
            &serde_json::json!("DEVICE"),
        );

        // The dead man's switch, which in this generation genuinely empties the
        // membership — room state has no TTL to lapse, so without it a crashed
        // client is a permanent ghost in the call.
        let delayed = sends
            .iter()
            .find(|send| send.carrier == Carrier::DelayedState)
            .expect("the delayed leave should be a delayed state event");
        assert_eq!(delayed.state_key, membership.state_key);
        assert_eq!(
            delayed.content,
            serde_json::json!({}),
            "an empty content is this dialect's leave",
        );
    }

    /// The end-to-end shape of the matrix.org failure: Element X, a pre-sticky
    /// Element Call room, and a homeserver answering
    /// `403 M_FORBIDDEN "Sending delayed events has been disallowed"` to the
    /// delayed state event. That used to fail the join outright — no call at
    /// all, on a homeserver where the call works fine.
    #[tokio::test]
    async fn a_pre_sticky_join_survives_a_homeserver_without_delayed_events() {
        let (mock, manager) = compat_manager().await;
        mock.refuse_delayed
            .store(true, std::sync::atomic::Ordering::Relaxed);

        manager
            .join(compat_join(FfiElementCallCompat::StateEvents))
            .await
            .expect("a refused delayed leave must not fail the join");

        let sends = mock.sends();
        assert!(
            sends
                .iter()
                .all(|send| send.carrier != Carrier::DelayedState),
            "nothing was armed, because the homeserver refused",
        );

        // The membership itself still goes out, and still states a deadline —
        // which here is the only thing that will ever clear it, since there is
        // no delayed `{}` to empty the state event when this client dies.
        let membership = sends
            .iter()
            .find(|send| send.carrier == Carrier::State)
            .expect("the membership should still be published as room state");
        let expires = membership
            .content
            .get("expires")
            .and_then(serde_json::Value::as_u64)
            .expect("a pre-sticky membership always states its lifetime");
        assert!(expires > 0);
    }

    /// A pre-sticky room has no `m.rtc.slot` — the concept postdates that
    /// generation — so the host feeding its room state truthfully says "no slots",
    /// which would resolve the session closed and project out every member, us
    /// included. The mode has to absorb that; it is not something a host should
    /// have to special-case, and a host that gets it wrong sees an empty call
    /// with nothing in the log to explain it.
    ///
    /// Both orderings, because both happen: slot state usually arrives with sync
    /// (before the user joins) and keeps arriving afterwards.
    #[tokio::test]
    async fn a_pre_sticky_join_stops_slot_state_emptying_the_call() {
        let (_mock, manager) = compat_manager().await;
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());
        let peer = || {
            vec![joined_sticky(
                "@carl:example.org",
                "CARLDEV",
                "@carl:example.org:CARLDEV",
            )]
        };

        // Fed before the join, when nothing yet knows the room's generation.
        manager
            .on_room_slots_received(room_id.clone(), Vec::new())
            .await
            .expect("room slots");
        manager
            .set_current_sticky_state(room_id.clone(), peer())
            .await
            .expect("carl's membership");
        assert_eq!(
            manager
                .member_count(room_id.clone(), slot_id.clone())
                .await
                .unwrap(),
            Some(0),
            "with slot state supplied and no slot in it, the session is closed",
        );

        manager
            .join(compat_join(FfiElementCallCompat::StateEvents))
            .await
            .expect("join");
        manager
            .set_current_sticky_state(room_id.clone(), peer())
            .await
            .expect("carl's membership");
        assert_eq!(
            manager
                .member_count(room_id.clone(), slot_id.clone())
                .await
                .unwrap(),
            Some(1),
            "joining in this mode must take back the slot state fed earlier",
        );

        // And the host keeps feeding it, every sync, for the rest of the call.
        manager
            .on_room_slots_received(room_id.clone(), Vec::new())
            .await
            .expect("room slots");
        assert_eq!(
            manager.member_count(room_id, slot_id).await.unwrap(),
            Some(1),
            "later slot updates must be ignored too",
        );
    }

    /// A mode is per join, not per process: a later spec-current join in the same
    /// room must not inherit the dialect a previous one installed.
    #[tokio::test]
    async fn a_leave_forgets_the_dialect() {
        let (mock, manager) = compat_manager().await;
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        manager
            .join(compat_join(FfiElementCallCompat::StateEvents))
            .await
            .expect("join");
        manager
            .leave(
                room_id.clone(),
                slot_id.clone(),
                FfiLeaveSessionParams { leave_reason: None },
            )
            .await
            .expect("leave");

        // The leave itself still had to be rendered in the dialect it is a leave
        // *in*, or the peers we joined for would never see us go.
        assert!(
            mock.sends()
                .iter()
                .filter(|send| send.carrier == Carrier::State)
                .any(|send| send.content == serde_json::json!({})),
            "the leave should have emptied our state membership",
        );

        manager.join(join_params()).await.expect("rejoin");
        assert!(
            mock.sends()
                .iter()
                .any(|send| send.carrier == Carrier::Sticky
                    && send.event_type == "org.matrix.msc4143.rtc.member"),
            "a spec-current rejoin must go back to a sticky membership",
        );
    }

    /// The pre-sticky wire has no sticky map, so in that mode the notification
    /// is an ordinary room event — a host whose SDK has no MSC4354 is never
    /// asked for a sticky send. In every other mode it stays sticky.
    #[tokio::test]
    async fn a_notification_goes_out_as_a_room_event_in_the_state_dialect() {
        let (mock, sender) = mock_sender();
        sender.set_dialect(
            "!legacy:example.org",
            crate::compat::outbound_dialect(
                matrix_rtc_bridge::compat::ElementCallCompat::StateEvents,
                "@alice:example.org",
                "DEVICE",
                "!legacy:example.org",
                "m.call#ROOM",
            ),
        );
        let notification = serde_json::json!({
            "application": { "type": "m.call", "notification_type": "ring" },
            "m.mentions": { "user_ids": [], "room": true },
        });

        for room_id in ["!legacy:example.org", "!modern:example.org"] {
            sender
                .send_sticky_event(
                    room_id.to_owned(),
                    "m.rtc.notification".to_owned(),
                    notification.clone(),
                    30_000,
                )
                .await
                .unwrap();
        }

        let carriers: Vec<Carrier> = mock.sends().into_iter().map(|send| send.carrier).collect();
        assert_eq!(carriers, [Carrier::Room, Carrier::Sticky]);
        // Both under the wire id the core's alias map resolves to.
        assert_eq!(
            mock.sent_types(),
            [
                "org.matrix.msc4075.rtc.notification",
                "org.matrix.msc4075.rtc.notification"
            ]
        );
    }

    /// A to-device message names its room only inside the content, so that is
    /// what decides the dialect — and it must decide it for that room alone.
    #[tokio::test]
    async fn a_media_key_takes_the_dialect_of_its_own_room() {
        let (mock, sender) = mock_sender();
        sender.set_dialect(
            "!legacy:example.org",
            crate::compat::outbound_dialect(
                matrix_rtc_bridge::compat::ElementCallCompat::StickyEvents,
                "@alice:example.org",
                "DEVICE",
                "!legacy:example.org",
                "m.call#ROOM",
            ),
        );

        let key = |room_id: &str| {
            serde_json::json!({
                "room_id": room_id,
                "member_id": "MEMBER",
                "media_key": { "index": 0, "key": "AAAA" },
            })
        };
        let recipients = vec![matrix_rtc_core::ToDeviceRecipient::new(
            "@bob:example.org",
            "BOBDEV",
        )];

        sender
            .send_to_device_message(
                recipients.clone(),
                matrix_rtc_core::KEY_MESSAGE_TYPE.to_owned(),
                key("!legacy:example.org"),
            )
            .await
            .unwrap();
        sender
            .send_to_device_message(
                recipients,
                matrix_rtc_core::KEY_MESSAGE_TYPE.to_owned(),
                key("!modern:example.org"),
            )
            .await
            .unwrap();

        assert_eq!(
            mock.sent_types(),
            [
                // A to-device message has one type, so a key is sent in one
                // dialect or the other — never both.
                "io.element.call.encryption_keys",
                "org.matrix.msc4143.rtc.encryption_key",
            ],
        );
        let sent = mock.to_device_for("@bob:example.org", "BOBDEV");
        assert_eq!(
            sent[0].pointer("/keys/key").unwrap(),
            &serde_json::json!("AAAA"),
            "the legacy key message states its key under `keys`",
        );
        assert_eq!(
            sent[0].pointer("/member/id").unwrap(),
            &serde_json::json!("MEMBER"),
        );
        assert_eq!(
            sent[1].pointer("/media_key/key").unwrap(),
            &serde_json::json!("AAAA"),
            "the unregistered room must be left spec-current",
        );
    }
}
