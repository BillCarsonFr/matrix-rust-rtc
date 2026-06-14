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

use matrix_rtc_core::{CommandError, RtcCommandSender};
use serde_json::Value;
use std::sync::Arc;

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

/// FFI-friendly join session parameters.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiJoinSessionParams {
    /// Matrix user ID (e.g., "@alice:example.org")
    pub user_id: String,
    /// Device ID
    pub device_id: String,
    /// Optional sticky key, defaults to "{user_id}-{device_id}"
    pub membership_id: Option<String>,
    /// Room ID
    pub room_id: String,
    /// Slot ID (e.g., "m.call#ROOM")
    pub slot_id: String,
    /// Application type (e.g., "m.call")
    pub application: String,
    /// Transport configuration
    pub transport: FfiTransportConfig,
    /// Optional keep-alive timeout in milliseconds (default: 30000)
    pub keep_alive_timeout_ms: Option<u64>,
}

/// FFI-friendly leave session parameters.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiLeaveSessionParams {
    /// Optional reason for leaving (e.g., "user_left", "ice_failed")
    pub disconnect_reason: Option<String>,
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

/// Conversion from FFI join params to core join params.
impl FfiJoinSessionParams {
    pub fn into_core(
        self,
    ) -> Result<matrix_rtc_core::JoinSessionParams, matrix_rtc_core::CommandError> {
        let transport = self.transport.into_core()?;
        Ok(matrix_rtc_core::JoinSessionParams {
            user_id: self.user_id,
            device_id: self.device_id,
            membership_id: self.membership_id,
            room_id: self.room_id,
            slot_id: self.slot_id,
            application: self.application,
            transport,
            keep_alive_timeout_ms: self.keep_alive_timeout_ms,
        })
    }
}

/// Conversion from FFI leave params to core leave params.
impl FfiLeaveSessionParams {
    pub fn into_core(self) -> matrix_rtc_core::LeaveSessionParams {
        matrix_rtc_core::LeaveSessionParams {
            disconnect_reason: self.disconnect_reason,
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
    /// * `event_type` - The event type (e.g., "m.rtc.member")
    /// * `content_json` - The event content as a JSON string
    ///
    /// # Returns
    /// Return Ok(()) on success, or Err with a CommandSenderError on failure.
    fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
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
    /// Returns an `Arc<dyn RtcCommandSender>` for thread-safe sharing with the core.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(callback: Arc<dyn CommandSenderCallback>) -> Arc<dyn RtcCommandSender> {
        Arc::new(Self { callback })
    }
}

use async_trait::async_trait;

#[async_trait(?Send)]
impl RtcCommandSender for FfiCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<(), CommandError> {
        let content_json = serde_json::to_string(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        self.callback
            .send_sticky_event(room_id, event_type, content_json)
            .map_err(CommandError::from)?;
        Ok(())
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

        self.callback
            .send_delayed_event(room_id, event_type, content_json, delay_ms)
            .map_err(CommandError::from)
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        event_id: String,
    ) -> Result<(), CommandError> {
        self.callback
            .cancel_delayed_event(room_id, event_id)
            .map_err(CommandError::from)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // Mock implementation of CommandSenderCallback for testing
    struct MockCommandSenderCallback;

    impl CommandSenderCallback for MockCommandSenderCallback {
        fn send_sticky_event(
            &self,
            room_id: String,
            event_type: String,
            content_json: String,
        ) -> Result<(), CommandSenderError> {
            println!(
                "Mock send_sticky_event: room={}, type={}, content={}",
                room_id, event_type, content_json
            );
            Ok(())
        }

        fn send_delayed_event(
            &self,
            room_id: String,
            event_type: String,
            content_json: String,
            delay_ms: u64,
        ) -> Result<String, CommandSenderError> {
            println!(
                "Mock send_delayed_event: room={}, type={}, delay={}ms, content={}",
                room_id, event_type, delay_ms, content_json
            );
            Ok(format!("event-{}-{}", room_id, event_type))
        }

        fn cancel_delayed_event(
            &self,
            room_id: String,
            event_id: String,
        ) -> Result<(), CommandSenderError> {
            println!(
                "Mock cancel_delayed_event: room={}, event_id={}",
                room_id, event_id
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_ffi_command_sender_sends_sticky_event() {
        let mock_callback = MockCommandSenderCallback;
        let callback: Arc<dyn CommandSenderCallback> = Arc::new(mock_callback);
        let command_sender = FfiCommandSender::new(callback);

        let result = command_sender
            .send_sticky_event(
                "!room:example.org".to_string(),
                "m.rtc.member".to_string(),
                serde_json::json!({
                    "slot_id": "m.call#ROOM",
                    "sticky_key": "alice-device-a"
                }),
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_ffi_command_sender_sends_delayed_event() {
        let mock_callback = MockCommandSenderCallback;
        let callback: Arc<dyn CommandSenderCallback> = Arc::new(mock_callback);
        let command_sender = FfiCommandSender::new(callback);

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
        assert_eq!(result.unwrap(), "event-!room:example.org-m.rtc.member");
    }
}
