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

//! Command execution interface for MatrixRTC.
//!
//! This module provides the `RtcCommandSender` trait that allows the core crate
//! to send commands (events) to the Matrix room through the client SDK.
//! The client layer is responsible for actual delivery and guarantees ordering.
//!
//! Commands are async to allow the core to await completion, particularly
//! for the dead man's switch pattern where we need to verify delayed event
//! scheduling before sending join events.

use async_trait::async_trait;
use serde_json::Value;

use crate::error::CommandError;

/// Trait for sending Matrix events from the core crate to the client SDK.
///
/// Implementations of this trait are provided by the binding layers (WASM, FFI)
/// and delegate to the respective platform's Matrix client SDK.
///
/// The client layer is expected to provide:
/// - **Retry strategy**: For handling shaky connections or 429 rate limiting
///
/// Methods are async to allow awaiting completion and proper error handling.
/// Note: The `?Send` bound is used to support platforms like WASM where futures
/// may not be `Send` (e.g., when wrapping JavaScript Promises).
#[async_trait(?Send)]
pub trait RtcCommandSender: Send + Sync {
    /// Send a sticky event to a Matrix room.
    ///
    /// The event will be sent as a sticky event (using the appropriate MSC or
    /// stable event type). Returns Ok(()) on success or an error on failure.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type (e.g., "m.rtc.member")
    /// * `content` - The event content as a JSON value
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<(), CommandError>;

    /// Send a delayed event to a Matrix room.
    ///
    /// The event will be scheduled to be sent after the specified delay.
    /// Returns Ok(event_id) with the scheduled event's ID on success, or an error on failure.
    ///
    /// This is used for implementing the keep-alive mechanism where a delayed
    /// cleanup event is scheduled and periodically restarted.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type
    /// * `content` - The event content as a JSON value
    /// * `delay_ms` - Delay in milliseconds before the event is sent
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError>;

    /// Cancel a previously scheduled delayed event.
    ///
    /// This prevents the delayed event from being sent if it hasn't already been
    /// sent. Returns Ok(()) on success or an error on failure.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `event_id` - The event ID returned by `send_delayed_event`
    async fn cancel_delayed_event(
        &self,
        room_id: String,
        event_id: String,
    ) -> Result<(), CommandError>;

    /// Send a to-device message to a specific device.
    ///
    /// This is used for encryption key distribution as specified in MSC4143.
    /// To-device messages are encrypted using Olm and delivered directly to the
    /// target device(s).
    ///
    /// # Arguments
    ///
    /// * `user_id` - The target user ID
    /// * `device_id` - The target device ID (use "*" for all devices of the user)
    /// * `message_type` - The message type (e.g., "org.matrix.msc4143.rtc.encryption_key")
    /// * `content` - The message content as a JSON value
    ///
    /// # MSC4143 Compliance
    ///
    /// MSC4143 specifies that encryption keys MUST be sent via encrypted to-device
    /// messages. Keys sent in cleartext SHOULD be discarded by recipients.
    async fn send_to_device_message(
        &self,
        user_id: String,
        device_id: String,
        message_type: String,
        content: Value,
    ) -> Result<(), CommandError>;
}

/// A no-op implementation of `RtcCommandSender` for testing purposes.
///
/// This implementation immediately returns success, useful for
/// unit tests that don't need to verify command execution behavior.
#[cfg(test)]
pub struct NoopCommandSender;

#[cfg(test)]
#[async_trait(?Send)]
impl RtcCommandSender for NoopCommandSender {
    async fn send_sticky_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
    ) -> Result<(), CommandError> {
        Ok(())
    }

    async fn send_delayed_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
        _delay_ms: u64,
    ) -> Result<String, CommandError> {
        Ok("mock-event-id".to_string())
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        _event_id: String,
    ) -> Result<(), CommandError> {
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        _user_id: String,
        _device_id: String,
        _message_type: String,
        _content: Value,
    ) -> Result<(), CommandError> {
        Ok(())
    }
}

/// A mock implementation of `RtcCommandSender` that captures sent events for testing.
///
/// Useful for verifying that the core sends the correct events.
#[cfg(test)]
#[derive(Default)]
pub struct MockCommandSender {
    pub sticky_events: std::sync::Mutex<Vec<(String, String, Value)>>,
    pub delayed_events: std::sync::Mutex<Vec<(String, String, Value, u64)>>,
    pub cancelled_events: std::sync::Mutex<Vec<(String, String)>>,
    pub to_device_messages: std::sync::Mutex<Vec<(String, String, String, Value)>>,
}

#[cfg(test)]
impl MockCommandSender {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn last_sticky_event(&self) -> Option<(String, String, Value)> {
        self.sticky_events.lock().unwrap().last().cloned()
    }

    #[allow(dead_code)]
    pub fn last_delayed_event(&self) -> Option<(String, String, Value, u64)> {
        self.delayed_events.lock().unwrap().last().cloned()
    }

    #[allow(dead_code)]
    pub fn last_to_device_message(&self) -> Option<(String, String, String, Value)> {
        self.to_device_messages.lock().unwrap().last().cloned()
    }

    #[allow(dead_code)]
    pub fn to_device_messages_for(&self, user_id: &str, device_id: &str) -> Vec<(String, Value)> {
        self.to_device_messages
            .lock()
            .unwrap()
            .iter()
            .filter(|(u, d, _, _)| u == user_id && d == device_id)
            .map(|(_, _, t, c)| (t.clone(), c.clone()))
            .collect()
    }
}

#[cfg(test)]
#[async_trait(?Send)]
impl RtcCommandSender for MockCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<(), CommandError> {
        self.sticky_events
            .lock()
            .unwrap()
            .push((room_id, event_type, content));
        Ok(())
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        self.delayed_events.lock().unwrap().push((
            room_id.clone(),
            event_type.clone(),
            content,
            delay_ms,
        ));
        Ok(format!("delayed-{}-{}", room_id, event_type))
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        event_id: String,
    ) -> Result<(), CommandError> {
        self.cancelled_events
            .lock()
            .unwrap()
            .push((room_id, event_id));
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        user_id: String,
        device_id: String,
        message_type: String,
        content: Value,
    ) -> Result<(), CommandError> {
        self.to_device_messages
            .lock()
            .unwrap()
            .push((user_id, device_id, message_type, content));
        Ok(())
    }
}
