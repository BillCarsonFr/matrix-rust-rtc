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

use matrix_rtc_core::{CommandError, RtcCommandSender, wire_event_type};
use serde_json::Value;
use std::sync::Arc;

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
#[derive(Debug, Clone, uniffi::Error)]
pub enum CommandSenderError {
    /// Serialization error when converting content to JSON
    SerializationError(String),
    /// Error from the native SDK when sending the event
    SendError(String),
}

impl From<CommandSenderError> for matrix_rtc_core::CommandError {
    fn from(err: CommandSenderError) -> Self {
        match err {
            CommandSenderError::SerializationError(e) => {
                matrix_rtc_core::CommandError::SerializationError(e)
            }
            CommandSenderError::SendError(e) => matrix_rtc_core::CommandError::SendError(e),
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
    /// Whether to manage media keys (default: true).
    pub manage_media_keys: Option<bool>,
    /// Whether to discard keys from devices that are not cross-signed
    /// (default: true, per MSC4153).
    pub require_cross_signed_sender: Option<bool>,
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
    /// Optional encryption configuration
    pub encryption_config: Option<FfiEncryptionConfig>,
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
            "[{}/{}] user={} device={} application={} transport={} keep_alive={:?}ms encryption={}",
            self.room_id,
            self.slot_id,
            self.user_id,
            self.device_id,
            self.application,
            transport,
            self.keep_alive_timeout_ms,
            self.encryption_config.is_some(),
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
            encryption_config,
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
#[uniffi::export(callback_interface)]
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
    /// # Returns
    /// Return Ok(()) on success, or Err with a CommandSenderError on failure.
    fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        duration_ms: u64,
    ) -> Result<(), CommandSenderError>;

    /// Called when a delayed event needs to be scheduled.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type
    /// * `content_json` - The event content as a JSON string
    /// * `delay_ms` - Delay in milliseconds before the event is sent
    ///
    /// # Returns
    /// Return Ok(event_id) with the scheduled event ID on success, or Err on failure.
    fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        delay_ms: u64,
    ) -> Result<String, CommandSenderError>;

    /// Called when a state event needs to be sent.
    ///
    /// Used for `m.rtc.slot`. Sending room state usually needs a raised power
    /// level, so return an error if the homeserver rejects it.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The wire event type (e.g., "org.matrix.msc4143.rtc.slot")
    /// * `state_key` - The state key (for a slot, the slot id)
    /// * `content_json` - The event content as a JSON string
    ///
    /// # Returns
    /// Return Ok(()) on success, or Err with a CommandSenderError on failure.
    fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content_json: String,
    ) -> Result<(), CommandSenderError>;

    /// Called periodically to restart a scheduled delayed event's timer.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `event_id` - The delay ID returned by send_delayed_event
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
    fn restart_delayed_event(
        &self,
        room_id: String,
        event_id: String,
    ) -> Result<(), CommandSenderError>;

    /// Called when a previously scheduled delayed event needs to be canceled.
    ///
    /// # Arguments
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `event_id` - The event ID returned by send_delayed_event
    ///
    /// # Returns
    /// Return Ok(()) on success, or Err with a CommandSenderError on failure.
    fn cancel_delayed_event(
        &self,
        room_id: String,
        event_id: String,
    ) -> Result<(), CommandSenderError>;

    /// Called when a to-device message needs to be sent (MSC4143).
    ///
    /// # Arguments
    /// * `user_id` - The target user ID
    /// * `device_id` - The target device ID. Always one device — send to exactly
    ///   that device and do not widen it to the user's other devices; the SDK
    ///   never asks for a fan-out.
    /// * `message_type` - The message type (e.g., "org.matrix.msc4143.rtc.encryption_key")
    /// * `content_json` - The message content as a JSON string
    ///
    /// # Returns
    /// Return Ok(()) on success, or Err with a CommandSenderError on failure.
    fn send_to_device_message(
        &self,
        user_id: String,
        device_id: String,
        message_type: String,
        content_json: String,
    ) -> Result<(), CommandSenderError>;
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
}

impl FfiCommandSender {
    /// Creates a new FfiCommandSender with the given native callback implementation.
    ///
    /// Returns an `Arc<FfiCommandSender>` for thread-safe sharing with the core.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(callback: Arc<dyn CommandSenderCallback>) -> Arc<FfiCommandSender> {
        Arc::new(Self { callback })
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

#[async_trait(?Send)]
impl RtcCommandSender for FfiCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<(), CommandError> {
        let content_json = serde_json::to_string(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        let wire_event_type = wire_type(event_type);
        let what = format!("sticky [{room_id}] type={wire_event_type}");
        trace_command_content(&what, &content_json);

        log_command(
            &what,
            self.callback
                .send_sticky_event(room_id, wire_event_type, content_json, duration_ms)
                .map_err(CommandError::from),
        )
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        let content_json = serde_json::to_string(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        let wire_event_type = wire_type(event_type);
        let what = format!("delayed [{room_id}] type={wire_event_type} delay={delay_ms}ms");
        trace_command_content(&what, &content_json);

        log_command(
            &what,
            self.callback
                .send_delayed_event(room_id, wire_event_type, content_json, delay_ms)
                .map_err(CommandError::from),
        )
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        event_id: String,
    ) -> Result<(), CommandError> {
        let what = format!("restart delayed [{room_id}] id={event_id}");
        log_command(
            &what,
            self.callback
                .restart_delayed_event(room_id, event_id)
                .map_err(CommandError::from),
        )
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        event_id: String,
    ) -> Result<(), CommandError> {
        let what = format!("cancel delayed [{room_id}] event_id={event_id}");

        log_command(
            &what,
            self.callback
                .cancel_delayed_event(room_id, event_id)
                .map_err(CommandError::from),
        )
    }

    async fn send_to_device_message(
        &self,
        user_id: String,
        device_id: String,
        message_type: String,
        content: Value,
    ) -> Result<(), CommandError> {
        let content_json = serde_json::to_string(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        let wire_message_type = wire_type(message_type);
        let what = format!("to-device {user_id}/{device_id} type={wire_message_type}");
        trace_command_content(&what, &content_json);

        log_command(
            &what,
            self.callback
                .send_to_device_message(user_id, device_id, wire_message_type, content_json)
                .map_err(CommandError::from),
        )
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<(), CommandError> {
        let content_json = serde_json::to_string(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        let wire_event_type = wire_type(event_type);
        let what = format!("state [{room_id}] type={wire_event_type} state_key={state_key}");
        trace_command_content(&what, &content_json);

        log_command(
            &what,
            self.callback
                .send_state_event(room_id, wire_event_type, state_key, content_json)
                .map_err(CommandError::from),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

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
    }

    #[derive(Clone)]
    struct ToDeviceSend {
        user_id: String,
        device_id: String,
        content: serde_json::Value,
    }

    impl MockCommandSenderCallback {
        fn record(&self, event_type: &str) {
            self.sent_types.lock().unwrap().push(event_type.to_owned());
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
    }

    impl CommandSenderCallback for MockCommandSenderCallback {
        fn send_sticky_event(
            &self,
            _room_id: String,
            event_type: String,
            _content_json: String,
            duration_ms: u64,
        ) -> Result<(), CommandSenderError> {
            self.record(&event_type);
            *self.sticky_duration_ms.lock().unwrap() = Some(duration_ms);
            Ok(())
        }

        fn send_delayed_event(
            &self,
            room_id: String,
            event_type: String,
            _content_json: String,
            _delay_ms: u64,
        ) -> Result<String, CommandSenderError> {
            self.record(&event_type);
            Ok(format!("event-{}-{}", room_id, event_type))
        }

        fn send_state_event(
            &self,
            _room_id: String,
            event_type: String,
            _state_key: String,
            _content_json: String,
        ) -> Result<(), CommandSenderError> {
            self.record(&event_type);
            Ok(())
        }

        fn restart_delayed_event(
            &self,
            _room_id: String,
            _event_id: String,
        ) -> Result<(), CommandSenderError> {
            self.record("restart_delayed_event");
            Ok(())
        }

        fn cancel_delayed_event(
            &self,
            _room_id: String,
            _event_id: String,
        ) -> Result<(), CommandSenderError> {
            Ok(())
        }

        fn send_to_device_message(
            &self,
            user_id: String,
            device_id: String,
            message_type: String,
            content_json: String,
        ) -> Result<(), CommandSenderError> {
            self.record(&message_type);
            self.to_device.lock().unwrap().push(ToDeviceSend {
                user_id,
                device_id,
                content: serde_json::from_str(&content_json).unwrap_or(serde_json::Value::Null),
            });
            Ok(())
        }
    }

    /// Lets a test keep hold of the mock after handing it to
    /// `set_command_sender`, which takes ownership of a `Box`.
    impl CommandSenderCallback for Arc<MockCommandSenderCallback> {
        fn send_sticky_event(
            &self,
            room_id: String,
            event_type: String,
            content_json: String,
            duration_ms: u64,
        ) -> Result<(), CommandSenderError> {
            (**self).send_sticky_event(room_id, event_type, content_json, duration_ms)
        }

        fn send_delayed_event(
            &self,
            room_id: String,
            event_type: String,
            content_json: String,
            delay_ms: u64,
        ) -> Result<String, CommandSenderError> {
            (**self).send_delayed_event(room_id, event_type, content_json, delay_ms)
        }

        fn send_state_event(
            &self,
            room_id: String,
            event_type: String,
            state_key: String,
            content_json: String,
        ) -> Result<(), CommandSenderError> {
            (**self).send_state_event(room_id, event_type, state_key, content_json)
        }

        fn restart_delayed_event(
            &self,
            room_id: String,
            event_id: String,
        ) -> Result<(), CommandSenderError> {
            (**self).restart_delayed_event(room_id, event_id)
        }

        fn cancel_delayed_event(
            &self,
            room_id: String,
            event_id: String,
        ) -> Result<(), CommandSenderError> {
            (**self).cancel_delayed_event(room_id, event_id)
        }

        fn send_to_device_message(
            &self,
            user_id: String,
            device_id: String,
            message_type: String,
            content_json: String,
        ) -> Result<(), CommandSenderError> {
            (**self).send_to_device_message(user_id, device_id, message_type, content_json)
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
                "@bob:example.org".to_string(),
                "device456".to_string(),
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
            encryption_config: None,
        }
    }

    /// The SDK owns the `member.id`, and a rejoin must not reuse the previous
    /// one. Reuse keeps the MSC4195 participant identity stable while our key
    /// index restarts at 0, so every peer decrypts the new call's media with the
    /// old call's key and never recovers — which is why hosts cannot supply one.
    #[test]
    fn every_join_gets_a_fresh_member_id() {
        let manager = crate::RtcSessionManagerHandle::new();
        manager
            .set_command_sender(Box::new(MockCommandSenderCallback::default()))
            .expect("the mock sender should be accepted");
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        let first = manager.join(join_params()).expect("first join");
        assert_eq!(
            manager
                .own_member_id(room_id.clone(), slot_id.clone())
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
            .expect("leave");
        let second = manager.join(join_params()).expect("rejoin");

        assert_ne!(first, second, "a rejoin must not reuse the member id");
        assert_eq!(
            manager
                .own_member_id(room_id, slot_id)
                .expect("the call itself should succeed"),
            Some(second),
        );
    }

    fn joined_sticky(user_id: &str, device_id: &str, member_id: &str) -> crate::StickyEvent {
        crate::StickyEvent {
            room_id: "!room:example.org".to_owned(),
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
    #[test]
    fn a_rejoin_distributes_keys_without_new_sticky_events() {
        let mock = Arc::new(MockCommandSenderCallback::default());
        let manager = crate::RtcSessionManagerHandle::new();
        manager
            .set_command_sender(Box::new(mock.clone()))
            .expect("the mock sender should be accepted");
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        manager
            .on_room_encryption_received(room_id.clone(), true)
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
            .expect("room slots");

        // First call: bob arrives after we joined, so there is a roster change.
        manager.join(join_params()).expect("first join");
        manager
            .on_sticky_events_update_received(
                vec![joined_sticky("@bob:example.org", "BOBDEV", "bob-a")],
                Vec::new(),
                Vec::new(),
            )
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
            .expect("leave");
        mock.clear_to_device();

        // Second call: no new sticky events at all. Bob has not moved.
        let second = manager.join(join_params()).expect("rejoin");

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
}
