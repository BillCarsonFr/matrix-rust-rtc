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
//! Commands use callbacks to notify the core of success or failure, enabling
//! state management (e.g., tracking delayed event IDs for keep-alive).

use serde_json::Value;

use crate::error::CommandError;

/// Callback type for command completion notifications.
///
/// The callback receives a `Result` indicating success or failure of the command.
pub type CommandCallback = Box<dyn FnOnce(Result<(), CommandError>) + Send + Sync>;

/// Callback type for commands that return a value (like event IDs).
///
/// Used by `send_delayed_event` to return the scheduled event's ID.
pub type SendEventCallback = Box<dyn FnOnce(Result<String, CommandError>) + Send + Sync>;

/// Trait for sending Matrix events from the core crate to the client SDK.
///
/// Implementations of this trait are provided by the binding layers (WASM, FFI)
/// and delegate to the respective platform's Matrix client SDK.
///
/// The client layer is expected to guarantee:
/// - **Delivery**: Events will be delivered or an error will be reported
/// - **Ordering**: Events will be sent in the order they are received
///
/// This allows the core to use simple fire-and-forget semantics with callbacks.
pub trait RtcCommandSender: Send + Sync {
    /// Send a sticky event to a Matrix room.
    ///
    /// The event will be sent as a sticky event (using the appropriate MSC or
    /// stable event type). The callback is invoked when the client SDK confirms
    /// delivery or reports an error.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type (e.g., "m.rtc.member")
    /// * `content` - The event content as a JSON value
    /// * `callback` - Called when the operation completes
    fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        callback: CommandCallback,
    );

    /// Send a delayed event to a Matrix room.
    ///
    /// The event will be scheduled to be sent after the specified delay.
    /// The callback receives the event ID that can be used to cancel the event later.
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
    /// * `callback` - Called with the event ID on success, or error on failure
    fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
        callback: SendEventCallback,
    );

    /// Cancel a previously scheduled delayed event.
    ///
    /// This prevents the delayed event from being sent if it hasn't already been
    /// sent. The callback is invoked when the cancellation is confirmed or fails.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `event_id` - The event ID returned by `send_delayed_event`
    /// * `callback` - Called when the operation completes
    fn cancel_delayed_event(&self, room_id: String, event_id: String, callback: CommandCallback);
}

/// A no-op implementation of `RtcCommandSender` for testing purposes.
///
/// This implementation immediately invokes callbacks with success, useful for
/// unit tests that don't need to verify command execution behavior.
#[cfg(test)]
pub struct NoopCommandSender;

#[cfg(test)]
impl RtcCommandSender for NoopCommandSender {
    fn send_sticky_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
        callback: CommandCallback,
    ) {
        callback(Ok(()));
    }

    fn send_delayed_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
        _delay_ms: u64,
        callback: SendEventCallback,
    ) {
        callback(Ok("mock-event-id".to_string()));
    }

    fn cancel_delayed_event(&self, _room_id: String, _event_id: String, callback: CommandCallback) {
        callback(Ok(()));
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
}

#[cfg(test)]
impl RtcCommandSender for MockCommandSender {
    fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        callback: CommandCallback,
    ) {
        self.sticky_events
            .lock()
            .unwrap()
            .push((room_id, event_type, content));
        callback(Ok(()));
    }

    fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
        callback: SendEventCallback,
    ) {
        self.delayed_events.lock().unwrap().push((
            room_id.clone(),
            event_type.clone(),
            content,
            delay_ms,
        ));
        callback(Ok(format!("delayed-{}-{}", room_id, event_type)));
    }

    fn cancel_delayed_event(&self, room_id: String, event_id: String, callback: CommandCallback) {
        self.cancelled_events
            .lock()
            .unwrap()
            .push((room_id, event_id));
        callback(Ok(()));
    }
}
