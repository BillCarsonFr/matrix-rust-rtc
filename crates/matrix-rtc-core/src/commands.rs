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
use crate::maybe_send::MaybeSend;

/// One device a to-device message is addressed to.
///
/// Always one specific device rather than a `*` wildcard: media keys go to the
/// device that published the membership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToDeviceRecipient {
    pub user_id: String,
    pub device_id: String,
}

impl ToDeviceRecipient {
    pub fn new(user_id: impl Into<String>, device_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            device_id: device_id.into(),
        }
    }
}

/// What became of one recipient of a to-device send.
///
/// The distinction matters beyond reporting: a recipient recorded as served is
/// taken to hold the key and is never re-sent to, so a failure mistaken for a
/// success costs that member the rest of the call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToDeviceDelivery {
    pub recipient: ToDeviceRecipient,
    /// `None` when the message was accepted for this recipient; otherwise why
    /// it was not.
    pub error: Option<String>,
}

impl ToDeviceDelivery {
    /// The message was accepted for this recipient.
    pub fn sent(recipient: ToDeviceRecipient) -> Self {
        Self {
            recipient,
            error: None,
        }
    }

    /// The message could not be delivered to this recipient.
    pub fn failed(recipient: ToDeviceRecipient, error: impl Into<String>) -> Self {
        Self {
            recipient,
            error: Some(error.into()),
        }
    }

    pub fn is_sent(&self) -> bool {
        self.error.is_none()
    }
}

/// Trait for sending Matrix events from the core crate to the client SDK.
///
/// Implementations of this trait are provided by the binding layers (WASM, FFI)
/// and delegate to the respective platform's Matrix client SDK.
///
/// The client layer is expected to provide:
/// - **Retry strategy**: For handling shaky connections or 429 rate limiting
///
/// Methods are async so the core can await completion and handle errors.
///
/// The futures are `Send` everywhere except `wasm32`, where they cannot be: a
/// JS-backed future (`JsFuture` around a `Promise`) is not `Send`, and the
/// browser is single-threaded anyway. Every implementation of this trait needs
/// the same pair of `cfg_attr`s, or it will not satisfy the trait on one of the
/// two targets.
///
/// Native being `Send` is not incidental — it is what lets a uniffi async
/// export return one of these futures, since uniffi spawns them.
///
/// The implementing type is held to the same split by [`MaybeSend`]: `Send +
/// Sync` on native, unconstrained on `wasm32`, where a sender holds a `JsValue`.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RtcCommandSender: MaybeSend {
    /// Send a sticky event to a Matrix room.
    ///
    /// The event will be sent as a sticky event (using the appropriate MSC or
    /// stable event type).
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type (e.g., "m.rtc.member")
    /// * `content` - The event content as a JSON value
    /// * `duration_ms` - How long the server should keep this entry in the
    ///   sticky map. Implementations MUST pass this through rather than
    ///   choosing their own: the caller re-sends the event before this elapses,
    ///   so a different lifetime here silently breaks that refresh.
    ///
    /// # Returns
    ///
    /// The event id the homeserver assigned. Every Matrix send responds with
    /// one, so an implementation that cannot produce it is broken rather than
    /// merely terse — hence no `Option`. The core needs it for MSC4075, which
    /// requires an `m.reference` relation from a notification to the member
    /// event that justifies it.
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError>;

    /// Send a delayed event to a Matrix room.
    ///
    /// The event will be scheduled to be sent after the specified delay.
    ///
    /// Returns the MSC4140 **delay id** on success — the handle used to restart
    /// or cancel the scheduled send. It is not an event id: the event has none
    /// until it actually fires.
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

    /// Restart a previously scheduled delayed event's timer (MSC4140's
    /// `restart` action — "heartbeat ping").
    ///
    /// Resets the scheduled send time to now plus the *original* delay, leaving
    /// the delay id and everything else about the event untouched. This is the
    /// keep-alive primitive: one request, and no moment at which no delayed
    /// leave is armed.
    ///
    /// Do NOT emulate this with cancel-then-reschedule. That leaves a window
    /// with nothing armed, burns the server's `max_scheduled` quota, and a
    /// failed cancel leaks a delay that will fire and mark us as departed while
    /// we are still in the call.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `delay_id` - The MSC4140 delay id returned by `send_delayed_event`.
    ///   Not an event id: the delayed event has no event id until it fires.
    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError>;

    /// Cancel a previously scheduled delayed event.
    ///
    /// This prevents the delayed event from being sent if it hasn't already been
    /// sent. Returns Ok(()) on success or an error on failure.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `delay_id` - The MSC4140 delay id returned by `send_delayed_event`.
    ///   Not an event id: the delayed event has no event id until it fires.
    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError>;

    /// Send one to-device message to a set of devices, reporting the outcome
    /// per recipient.
    ///
    /// Used for encryption key distribution (MSC4143). The same `content` goes
    /// to every recipient; each is one specific device, never a `*` fan-out —
    /// media keys go to the device that published the membership, and widening
    /// would hand the key to devices outside the call (and, for our own user, to
    /// this very device, which Olm cannot encrypt to).
    ///
    /// # Why per recipient
    ///
    /// One unreachable device must not silence the others, and the caller has to
    /// know *which* ones were served: a recipient reported as delivered is
    /// recorded as holding the key and never retried, so reporting a failure as
    /// success costs that member the call. Returning a result per recipient lets
    /// the caller record only what actually landed and re-send the rest on the
    /// next rollout.
    ///
    /// An `Err` return means the batch could not be attempted at all; the caller
    /// treats every recipient as unserved.
    ///
    /// # Arguments
    ///
    /// * `recipients` - The target devices
    /// * `message_type` - The message type (e.g., "org.matrix.msc4143.rtc.encryption_key")
    /// * `content` - The message content as a JSON value
    ///
    /// # MSC4143 Compliance
    ///
    /// MSC4143 specifies that encryption keys MUST be sent via encrypted to-device
    /// messages. Keys sent in cleartext SHOULD be discarded by recipients.
    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError>;

    /// Send a state event to a Matrix room.
    ///
    /// Used for `m.rtc.slot`, the only MatrixRTC event that lives in room state.
    /// Sending it usually requires a power level the average member does not
    /// have, so implementations should surface an authorization failure as an
    /// error rather than swallowing it.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type (e.g., "m.rtc.slot")
    /// * `state_key` - The state key (for a slot, the slot id)
    /// * `content` - The event content as a JSON value
    ///
    /// # Returns
    ///
    /// The event id the homeserver assigned, on the same terms as
    /// [`send_sticky_event`](Self::send_sticky_event). No caller of a *slot*
    /// send reads it; it is here because the pre-MSC4354 Element Call dialect
    /// routes the membership through this method, and there the id is what a
    /// notification relates to.
    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError>;
}

/// A no-op implementation of `RtcCommandSender` for testing purposes.
///
/// This implementation immediately returns success, useful for
/// unit tests that don't need to verify command execution behavior.
#[cfg(test)]
pub struct NoopCommandSender;

#[cfg(test)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl RtcCommandSender for NoopCommandSender {
    async fn send_sticky_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
        _duration_ms: u64,
    ) -> Result<String, CommandError> {
        Ok("$mock-sticky-event".to_string())
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

    async fn restart_delayed_event(
        &self,
        _room_id: String,
        _delay_id: String,
    ) -> Result<(), CommandError> {
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        _delay_id: String,
    ) -> Result<(), CommandError> {
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        _message_type: String,
        _content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect())
    }

    async fn send_state_event(
        &self,
        _room_id: String,
        _event_type: String,
        _state_key: String,
        _content: Value,
    ) -> Result<String, CommandError> {
        Ok("$mock-state-event".to_string())
    }
}

/// A mock implementation of `RtcCommandSender` that captures sent events for testing.
///
/// Useful for verifying that the core sends the correct events.
#[cfg(test)]
#[derive(Default)]
pub struct MockCommandSender {
    pub sticky_events: std::sync::Mutex<Vec<(String, String, Value, u64)>>,
    pub delayed_events: std::sync::Mutex<Vec<(String, String, Value, u64)>>,
    pub restarted_events: std::sync::Mutex<Vec<(String, String)>>,
    pub cancelled_events: std::sync::Mutex<Vec<(String, String)>>,
    pub to_device_messages: std::sync::Mutex<Vec<(String, String, String, Value)>>,
    pub state_events: std::sync::Mutex<Vec<(String, String, String, Value)>>,
}

#[cfg(test)]
impl MockCommandSender {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn last_sticky_event(&self) -> Option<(String, String, Value, u64)> {
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
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl RtcCommandSender for MockCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        let mut guard = self.sticky_events.lock().unwrap();
        guard.push((room_id, event_type, content, duration_ms));
        // Numbered by send order, so a test can name the event a relation is
        // expected to point at.
        Ok(format!("$sticky-{}", guard.len()))
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

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.restarted_events
            .lock()
            .unwrap()
            .push((room_id, delay_id));
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.cancelled_events
            .lock()
            .unwrap()
            .push((room_id, delay_id));
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        let mut guard = self.to_device_messages.lock().unwrap();
        for recipient in &recipients {
            guard.push((
                recipient.user_id.clone(),
                recipient.device_id.clone(),
                message_type.clone(),
                content.clone(),
            ));
        }
        Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect())
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let mut guard = self.state_events.lock().unwrap();
        guard.push((room_id, event_type, state_key, content));
        Ok(format!("$state-{}", guard.len()))
    }
}
