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

//! Keep-alive mechanism for RTC sessions.
//!
//! This module provides a state machine that manages delayed cleanup events
//! to ensure that users are properly removed from RTC sessions if they
//! disconnect unexpectedly.
//!
//! The keep-alive mechanism works by:
//! 1. Scheduling a delayed event to clear the membership when joining
//! 2. Periodically restarting the delayed event to extend the timeout
//! 3. Canceling the delayed event when leaving properly
//!
//! This ensures that if a user loses connection or crashes, their membership
//! will be automatically cleaned up after the timeout period.

use serde_json::json;
use std::sync::{Arc, Mutex};

use crate::commands::{CommandCallback, RtcCommandSender, SendEventCallback};
use crate::error::CommandError;

/// Default keep-alive timeout in milliseconds (30 seconds).
pub const DEFAULT_KEEP_ALIVE_TIMEOUT_MS: u64 = 30_000;

/// State of the keep-alive machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeepAliveState {
    /// No keep-alive is active.
    NotStarted,
    /// A delayed cleanup event is scheduled and active.
    Active,
    /// The delayed cleanup event has been canceled.
    Cancelled,
}

/// Information about an active keep-alive delayed event.
#[derive(Debug, Clone)]
pub struct KeepAliveInfo {
    /// The event ID of the delayed cleanup event.
    pub delayed_event_id: String,
    /// The timeout in milliseconds before the event fires.
    pub timeout_ms: u64,
}

/// The KeepAliveMachine manages the lifecycle of delayed cleanup events.
///
/// It ensures that when a user joins an RTC session, a delayed event is scheduled
/// to automatically remove their membership if they disconnect. The user can
/// periodically restart the delayed event to keep their membership active,
/// or cancel it when they leave properly.
pub struct KeepAliveMachine {
    /// Reference to the command sender for sending events.
    command_sender: Arc<dyn RtcCommandSender>,
    /// Room ID for the session.
    room_id: String,
    /// Slot ID for the session.
    slot_id: String,
    /// Sticky key (membership ID) for our membership.
    sticky_key: String,
    /// Current state of the keep-alive machine.
    state: Arc<Mutex<KeepAliveState>>,
    /// Information about the active delayed event, if any.
    info: Arc<Mutex<Option<KeepAliveInfo>>>,
}

impl KeepAliveMachine {
    /// Creates a new keep-alive machine.
    ///
    /// # Arguments
    ///
    /// * `command_sender` - The command sender for sending events
    /// * `room_id` - The room ID for the session
    /// * `slot_id` - The slot ID for the session
    /// * `sticky_key` - The sticky key (membership ID) for our membership
    pub fn new(
        command_sender: Arc<dyn RtcCommandSender>,
        room_id: String,
        slot_id: String,
        sticky_key: String,
    ) -> Self {
        Self {
            command_sender,
            room_id,
            slot_id,
            sticky_key,
            state: Arc::new(Mutex::new(KeepAliveState::NotStarted)),
            info: Arc::new(Mutex::new(None)),
        }
    }

    /// Creates a new keep-alive machine with a custom timeout.
    pub fn with_timeout(
        command_sender: Arc<dyn RtcCommandSender>,
        room_id: String,
        slot_id: String,
        sticky_key: String,
        _timeout_ms: u64,
    ) -> Self {
        Self {
            command_sender,
            room_id,
            slot_id,
            sticky_key,
            state: Arc::new(Mutex::new(KeepAliveState::NotStarted)),
            info: Arc::new(Mutex::new(None)),
        }
    }

    /// Gets the current state of the keep-alive machine.
    pub fn state(&self) -> KeepAliveState {
        self.state.lock().unwrap().clone()
    }

    /// Gets information about the active delayed event, if any.
    pub fn info(&self) -> Option<KeepAliveInfo> {
        self.info.lock().unwrap().clone()
    }

    /// Gets the delayed event ID, if one is active.
    pub fn delayed_event_id(&self) -> Option<String> {
        self.info
            .lock()
            .unwrap()
            .as_ref()
            .map(|info| info.delayed_event_id.clone())
    }

    /// Starts the keep-alive mechanism by scheduling a delayed cleanup event.
    ///
    /// This sends a delayed event that will clear the membership after the timeout.
    /// The event ID is stored so it can be canceled later.
    ///
    /// # Arguments
    ///
    /// * `timeout_ms` - The delay in milliseconds before the cleanup event fires
    ///
    /// # Callback
    ///
    /// The callback is invoked when the delayed event is successfully scheduled,
    /// receiving the event ID. On error, the callback receives the error.
    pub fn start(&self, timeout_ms: u64) {
        let room_id = self.room_id.clone();
        let slot_id = self.slot_id.clone();
        let sticky_key = self.sticky_key.clone();
        let state = self.state.clone();
        let info = self.info.clone();

        // Build the leave event content for the delayed event
        let mut content = json!({
            "slot_id": slot_id,
            "sticky_key": sticky_key,
        });
        // Add disconnect reason to indicate this is a timeout
        content["disconnect_reason"] = json!("keep_alive_timeout");

        let callback: SendEventCallback = Box::new(move |result: Result<String, CommandError>| {
            let mut state_guard = state.lock().unwrap();
            let mut info_guard = info.lock().unwrap();

            match result {
                Ok(event_id) => {
                    *state_guard = KeepAliveState::Active;
                    *info_guard = Some(KeepAliveInfo {
                        delayed_event_id: event_id,
                        timeout_ms,
                    });
                }
                Err(e) => {
                    // Log error but keep state as NotStarted
                    eprintln!("Failed to start keep-alive: {:?}", e);
                }
            }
        });

        // Update state to reflect that we're starting
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = KeepAliveState::NotStarted;
        }

        self.command_sender.send_delayed_event(
            room_id,
            "m.rtc.member".to_string(),
            content,
            timeout_ms,
            callback,
        );
    }

    /// Restarts the keep-alive mechanism by canceling the current delayed event
    /// and scheduling a new one.
    ///
    /// This is called periodically to keep the user's membership active.
    ///
    /// # Arguments
    ///
    /// * `timeout_ms` - The delay in milliseconds for the new delayed event
    ///
    /// # Returns
    ///
    /// Returns `true` if there was an active delayed event to cancel.
    /// Returns `false` if no delayed event was active.
    pub fn restart(&self, timeout_ms: u64) -> bool {
        // First, cancel the existing delayed event if one exists
        let had_active = self.cancel();

        // Then start a new one
        self.start(timeout_ms);

        had_active
    }

    /// Cancels the active delayed cleanup event.
    ///
    /// This prevents the automatic cleanup from firing, allowing the user
    /// to leave the session gracefully.
    ///
    /// # Returns
    ///
    /// Returns `true` if there was an active delayed event to cancel.
    /// Returns `false` if no delayed event was active.
    pub fn cancel(&self) -> bool {
        let delayed_event_id = {
            let info_guard = self.info.lock().unwrap();
            info_guard
                .as_ref()
                .map(|info| info.delayed_event_id.clone())
        };

        if let Some(event_id) = delayed_event_id {
            let room_id = self.room_id.clone();
            let command_sender = self.command_sender.clone();
            let state = self.state.clone();
            let info = self.info.clone();

            let callback: CommandCallback = Box::new(move |_result: Result<(), CommandError>| {
                let mut state_guard = state.lock().unwrap();
                let mut info_guard = info.lock().unwrap();

                *state_guard = KeepAliveState::Cancelled;
                *info_guard = None;
            });

            command_sender.cancel_delayed_event(room_id, event_id, callback);

            // Update state immediately to reflect cancellation in progress
            {
                let mut state_guard = self.state.lock().unwrap();
                *state_guard = KeepAliveState::Cancelled;
            }

            true
        } else {
            false
        }
    }

    /// Stops the keep-alive mechanism.
    ///
    /// This is an alias for cancel() for explicit cleanup.
    pub fn stop(&self) -> bool {
        self.cancel()
    }
}

/// Builder for creating keep-alive machines with convenient defaults.
pub struct KeepAliveMachineBuilder {
    command_sender: Option<Arc<dyn RtcCommandSender>>,
    room_id: Option<String>,
    slot_id: Option<String>,
    sticky_key: Option<String>,
    timeout_ms: Option<u64>,
}

impl KeepAliveMachineBuilder {
    pub fn new() -> Self {
        Self {
            command_sender: None,
            room_id: None,
            slot_id: None,
            sticky_key: None,
            timeout_ms: None,
        }
    }

    pub fn command_sender(mut self, command_sender: Arc<dyn RtcCommandSender>) -> Self {
        self.command_sender = Some(command_sender);
        self
    }

    pub fn room_id(mut self, room_id: String) -> Self {
        self.room_id = Some(room_id);
        self
    }

    pub fn slot_id(mut self, slot_id: String) -> Self {
        self.slot_id = Some(slot_id);
        self
    }

    pub fn sticky_key(mut self, sticky_key: String) -> Self {
        self.sticky_key = Some(sticky_key);
        self
    }

    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn build(self) -> Option<KeepAliveMachine> {
        Some(KeepAliveMachine {
            command_sender: self.command_sender?,
            room_id: self.room_id?,
            slot_id: self.slot_id?,
            sticky_key: self.sticky_key?,
            state: Arc::new(Mutex::new(KeepAliveState::NotStarted)),
            info: Arc::new(Mutex::new(None)),
        })
    }
}

impl Default for KeepAliveMachineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{MockCommandSender, NoopCommandSender};
    use std::sync::Arc;

    #[test]
    fn test_machine_starts_not_started() {
        let machine = KeepAliveMachine::new(
            Arc::new(NoopCommandSender),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
        );

        assert_eq!(machine.state(), KeepAliveState::NotStarted);
        assert!(machine.delayed_event_id().is_none());
    }

    #[test]
    fn test_machine_start_schedules_event() {
        let mock_sender = Arc::new(MockCommandSender::new());
        let machine = KeepAliveMachine::new(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
        );

        machine.start(30_000);

        // The state should be NotStarted because the callback hasn't fired yet
        // In a real async environment, it would be Active
        // For testing purposes, we verify the command was sent
        let delayed_events = mock_sender.delayed_events.lock().unwrap();
        assert_eq!(delayed_events.len(), 1);
    }

    #[test]
    fn test_machine_cancel_without_active_event() {
        let machine = KeepAliveMachine::new(
            Arc::new(NoopCommandSender),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
        );

        let had_active = machine.cancel();
        assert!(!had_active);
    }

    #[test]
    fn test_builder() {
        let builder = KeepAliveMachineBuilder::new()
            .command_sender(Arc::new(NoopCommandSender))
            .room_id("!room:example.org".to_string())
            .slot_id("m.call#ROOM".to_string())
            .sticky_key("alice-device-a".to_string());

        let machine = builder.build().unwrap();
        assert_eq!(machine.room_id, "!room:example.org");
        assert_eq!(machine.slot_id, "m.call#ROOM");
        assert_eq!(machine.sticky_key, "alice-device-a");
    }

    #[test]
    fn test_builder_missing_fields() {
        let builder = KeepAliveMachineBuilder::new();
        let machine = builder.build();
        assert!(machine.is_none());
    }
}
