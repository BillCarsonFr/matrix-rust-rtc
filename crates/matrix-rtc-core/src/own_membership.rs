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
// You should have received a copy of the GNU Affero General License
// along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

//! Own membership management for RTC sessions.
//!
//! This module provides the `OwnMembershipMachine` that manages the lifecycle of the
//! current user's own membership in an RTC session, including the dead man's switch
//! keep-alive mechanism.
//!
//! The dead man's switch strategy works as follows:
//! 1. **Schedule delayed leave FIRST** - This is the safety net. If the client dies at any
//!    point, the delayed leave will fire and clean up our membership.
//! 2. **Send join membership** - Send the sticky join event to announce our presence.
//! 3. **Heartbeat** - Periodically restart the delayed leave to extend the timeout.
//!
//! This ensures that if the client crashes or loses connection, the delayed leave will
//! automatically clean up after the timeout period, preventing ghost memberships.

use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

use crate::commands::RtcCommandSender;
use crate::error::CommandError;
use crate::session::{ApplicationInfo, DisconnectReason, MemberInfo};
use crate::transport::RtcTransport;

/// Default keep-alive timeout in milliseconds (30 seconds).
pub const DEFAULT_KEEP_ALIVE_TIMEOUT_MS: u64 = 30_000;

/// Converts an RtcTransport to a JSON value for event content.
pub fn transport_to_json(transport: &RtcTransport) -> Value {
    match transport {
        RtcTransport::LiveKit(livekit) => {
            json!({
                "type": "livekit",
                "livekit_service_url": livekit.livekit_service_url
            })
        }
        RtcTransport::Unsupported(unsupported) => {
            let mut obj = json!({
                "type": unsupported.transport_type
            });
            for (key, value) in &unsupported.extra_fields {
                obj[key] = value.clone();
            }
            obj
        }
    }
}

/// State of the own membership machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnMembershipState {
    /// Not joined, no keep-alive active.
    NotJoined,
    /// Join is in progress (join event sent, waiting for confirmation).
    Joining,
    /// Successfully joined with active keep-alive.
    Joined,
    /// Leave is in progress.
    Leaving,
    /// Successfully left, keep-alive canceled.
    Left,
}

/// Information about the active keep-alive delayed event.
#[derive(Debug, Clone)]
pub struct KeepAliveInfo {
    /// The event ID of the delayed cleanup event.
    pub delayed_event_id: String,
    /// The timeout in milliseconds before the event fires.
    #[allow(dead_code)]
    pub timeout_ms: u64,
}

/// The OwnMembershipMachine manages the lifecycle of our own membership in an RTC session.
///
/// It implements the dead man's switch strategy:
/// 1. Schedule delayed leave membership event (safety net)
/// 2. Send join membership sticky event
/// 3. Heartbeat (every n seconds) -> restarts the delayed leave
///
/// The delayed leave is scheduled FIRST because it's safer - if the client dies at any
/// point, worst case we're cleaning up our membership.
///
/// This machine is responsible for:
/// - Managing our membership state (joined/left)
/// - Sending join/leave events via the command sender
/// - Managing the keep-alive delayed event lifecycle
/// - Storing and retrieving the delayed event ID from callbacks
pub struct OwnMembershipMachine<T: RtcCommandSender> {
    /// Reference to the command sender for sending events.
    command_sender: Arc<T>,
    /// Room ID for the session.
    room_id: String,
    /// Slot ID for the session.
    slot_id: String,
    /// Sticky key (membership ID) for our membership.
    sticky_key: String,
    /// Matrix user ID (for member.claimed_user_id in MSC4143).
    user_id: String,
    /// Device ID (for member.claimed_device_id in MSC4143).
    device_id: String,
    /// Application type (for application.type in MSC4143, e.g., "m.call").
    application_type: String,
    /// Current state of the membership machine.
    state: Arc<Mutex<OwnMembershipState>>,
    /// Information about the active delayed event, if any.
    keep_alive_info: Arc<Mutex<Option<KeepAliveInfo>>>,
    /// The keep-alive timeout in milliseconds.
    keep_alive_timeout_ms: u64,
}

impl<T: RtcCommandSender + 'static> OwnMembershipMachine<T> {
    /// Creates a new own membership machine.
    ///
    /// # Arguments
    ///
    /// * `command_sender` - The command sender for sending events
    /// * `room_id` - The room ID for the session
    /// * `slot_id` - The slot ID for the session
    /// * `sticky_key` - The sticky key (membership ID) for our membership
    /// * `user_id` - Matrix user ID (for MSC4143 member.claimed_user_id)
    /// * `device_id` - Device ID (for MSC4143 member.claimed_device_id)
    /// * `application_type` - Application type (for MSC4143 application.type, e.g., "m.call")
    /// * `keep_alive_timeout_ms` - The keep-alive timeout in milliseconds (default: 30000)
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        command_sender: Arc<T>,
        room_id: String,
        slot_id: String,
        sticky_key: String,
        user_id: String,
        device_id: String,
        application_type: String,
        keep_alive_timeout_ms: u64,
    ) -> Self {
        Self {
            command_sender,
            room_id,
            slot_id,
            sticky_key,
            user_id,
            device_id,
            application_type,
            state: Arc::new(Mutex::new(OwnMembershipState::NotJoined)),
            keep_alive_info: Arc::new(Mutex::new(None)),
            keep_alive_timeout_ms,
        }
    }

    /// Creates a new own membership machine with the default keep-alive timeout.
    pub fn with_default_timeout(
        command_sender: Arc<T>,
        room_id: String,
        slot_id: String,
        sticky_key: String,
        user_id: String,
        device_id: String,
        application_type: String,
    ) -> Self {
        Self::new(
            command_sender,
            room_id,
            slot_id,
            sticky_key,
            user_id,
            device_id,
            application_type,
            DEFAULT_KEEP_ALIVE_TIMEOUT_MS,
        )
    }

    /// Gets the current state of the membership machine.
    pub fn state(&self) -> OwnMembershipState {
        self.state.lock().unwrap().clone()
    }

    /// Gets the sticky key (membership ID) for our membership.
    pub fn sticky_key(&self) -> &str {
        &self.sticky_key
    }

    /// Gets the room ID.
    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    /// Gets the slot ID.
    pub fn slot_id(&self) -> &str {
        &self.slot_id
    }

    /// Gets the delayed event ID, if one is active.
    pub fn delayed_event_id(&self) -> Option<String> {
        self.keep_alive_info
            .lock()
            .unwrap()
            .as_ref()
            .map(|info| info.delayed_event_id.clone())
    }

    /// Joins the session by implementing the dead man's switch strategy.
    ///
    /// This method:
    /// 1. Schedules a delayed leave event FIRST (safety net) - **awaited for completion**
    /// 2. Sends the join membership event - **awaited for completion**
    /// 3. The delayed leave will be restarted by heartbeat calls
    ///
    /// The async design ensures that we verify the delayed leave is successfully scheduled
    /// before sending the join event, providing a proper safety net for the dead man's switch.
    ///
    /// # Arguments
    ///
    /// * `transport_json` - Optional JSON representing the transport configuration
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if both the delayed leave and join were scheduled/sent successfully.
    /// Returns an error if either operation fails.
    pub async fn join(
        &self,
        transport_json: Option<serde_json::Value>,
    ) -> Result<(), CommandError> {
        let room_id = self.room_id.clone();
        let slot_id = self.slot_id.clone();
        let sticky_key = self.sticky_key.clone();
        let user_id = self.user_id.clone();
        let device_id = self.device_id.clone();
        let application_type = self.application_type.clone();
        let keep_alive_timeout_ms = self.keep_alive_timeout_ms;

        // Update state to Joining first
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = OwnMembershipState::Joining;
        }

        // Step 1: Schedule delayed leave event FIRST (safety net)
        // If client dies at any point, worst case we're cleaning up.
        // **We await this to ensure the delayed event is scheduled before proceeding.**
        // MSC4143: disconnect_reason must be an object with class and reason fields
        let delayed_content = self.build_delayed_leave_content(&slot_id, &sticky_key);

        log::info!(
            "[{}] Scheduling delayed leave event (dead man's switch safety net)",
            room_id
        );

        // Schedule the delayed leave (Step 1 of dead man's switch)
        // This returns the event_id on success
        let delayed_event_id = self
            .command_sender
            .send_delayed_event(
                room_id.clone(),
                "m.rtc.member".to_string(),
                delayed_content,
                keep_alive_timeout_ms,
            )
            .await?;

        // Store the delayed event ID for later cancellation
        {
            let mut info_guard = self.keep_alive_info.lock().unwrap();
            *info_guard = Some(KeepAliveInfo {
                delayed_event_id,
                timeout_ms: keep_alive_timeout_ms,
            });
        }

        // Step 2: Send join membership event
        // **Only sent after delayed leave is confirmed scheduled.**
        let join_content = self.build_join_content(
            &slot_id,
            &sticky_key,
            &user_id,
            &device_id,
            &application_type,
            transport_json,
        );

        log::info!(
            "[{}] Sending join membership event (step 2 of dead man's switch)",
            room_id
        );

        // Send the join event (Step 2 of dead man's switch)
        self.command_sender
            .send_sticky_event(room_id.clone(), "m.rtc.member".to_string(), join_content)
            .await?;

        // Both steps completed successfully, transition to Joined state
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = OwnMembershipState::Joined;
        }

        log::info!(
            "[{}] Successfully joined with dead man's switch armed",
            room_id
        );

        Ok(())
    }

    /// Builds MSC4143-compliant content for a delayed leave event (dead man's switch).
    fn build_delayed_leave_content(&self, slot_id: &str, sticky_key: &str) -> Value {
        // Build typed disconnect reason
        let disconnect_reason = DisconnectReason {
            class: Some("server_error".to_string()),
            reason: Some("keep_alive_timeout".to_string()),
            description: Some("Dead man's switch: client failed to heartbeat".to_string()),
        };
        let disconnect_reason_value =
            serde_json::to_value(disconnect_reason).unwrap_or_else(|_| {
                json!({
                    "class": "server_error",
                    "reason": "keep_alive_timeout",
                    "description": "Dead man's switch: client failed to heartbeat"
                })
            });

        // Build the final content JSON with typed disconnect_reason
        json!({
            "slot_id": slot_id,
            "sticky_key": sticky_key,
            "disconnect_reason": disconnect_reason_value
        })
    }

    /// Builds MSC4143-compliant content for a join membership event.
    ///
    /// Uses typed structs for MSC4143 fields (application, member) but keeps transport
    /// as pre-serialized Value since it comes from transport_to_json().
    fn build_join_content(
        &self,
        slot_id: &str,
        sticky_key: &str,
        user_id: &str,
        device_id: &str,
        application_type: &str,
        transport_json: Option<Value>,
    ) -> Value {
        // Build typed application info and serialize it to ensure proper structure
        let application = ApplicationInfo {
            application_type: Some(application_type.to_string()),
            extra: std::collections::BTreeMap::new(),
        };
        let application_value =
            serde_json::to_value(application).unwrap_or_else(|_| json!({"type": application_type}));

        // Build typed member info and serialize it
        let member = MemberInfo {
            id: Some(sticky_key.to_string()),
            claimed_user_id: Some(user_id.to_string()),
            claimed_device_id: Some(device_id.to_string()),
        };
        let member_value = serde_json::to_value(member).unwrap_or_else(|_| {
            json!({
                "id": sticky_key,
                "claimed_user_id": user_id,
                "claimed_device_id": device_id
            })
        });

        // Build the final content JSON with typed fields
        let mut content = json!({
            "slot_id": slot_id,
            "sticky_key": sticky_key,
            "application": application_value,
            "member": member_value,
            "versions": ["v0"]
        });

        // Add transport if provided (already pre-serialized)
        if let Some(transport) = transport_json {
            content["rtc_transports"] = serde_json::Value::Array(vec![transport]);
        }

        content
    }

    /// Leaves the session by sending a leave event and canceling the keep-alive.
    ///
    /// This method:
    /// 1. Sends a leave membership event
    /// 2. Cancels the active delayed leave event (if any)
    ///
    /// Both operations are awaited to ensure proper cleanup.
    pub async fn leave(&self, disconnect_reason: Option<String>) -> Result<(), CommandError> {
        let room_id = self.room_id.clone();
        let slot_id = self.slot_id.clone();
        let sticky_key = self.sticky_key.clone();

        // Build MSC4143-compliant leave content
        // disconnect_reason must be an object with class and reason fields
        let leave_content = if let Some(reason) = disconnect_reason {
            // Map simple string reason to MSC4143 disconnect_reason object
            let (class, reason_str) = self.map_reason_to_msc4143(&reason);

            // Clone the strings for use in fallback
            let class_clone = class.clone();
            let reason_str_clone = reason_str.clone();
            let reason_clone = reason.clone();

            // Build typed disconnect reason and serialize it
            let disconnect_reason = DisconnectReason {
                class: Some(class),
                reason: Some(reason_str),
                description: Some(reason),
            };
            let disconnect_reason_value =
                serde_json::to_value(disconnect_reason).unwrap_or_else(|_| {
                    json!({
                        "class": class_clone,
                        "reason": reason_str_clone,
                        "description": reason_clone
                    })
                });

            json!({
                "slot_id": slot_id,
                "sticky_key": sticky_key,
                "disconnect_reason": disconnect_reason_value
            })
        } else {
            json!({
                "slot_id": slot_id,
                "sticky_key": sticky_key,
            })
        };

        // Update state to Leaving
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = OwnMembershipState::Leaving;
        }

        log::info!("[{}] Sending leave membership event", room_id);

        // Send leave event
        self.command_sender
            .send_sticky_event(room_id.clone(), "m.rtc.member".to_string(), leave_content)
            .await?;

        // Cancel the delayed leave event if one exists
        if let Some(event_id) = self.delayed_event_id() {
            log::debug!("[{}] Canceling delayed leave event: {}", room_id, event_id);
            self.command_sender
                .cancel_delayed_event(room_id.clone(), event_id.clone())
                .await?;

            // Clear the stored event ID
            {
                let mut info_guard = self.keep_alive_info.lock().unwrap();
                *info_guard = None;
            }
            log::debug!("[{}] Delayed leave event canceled", room_id);
        }

        // Transition to Left state
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = OwnMembershipState::Left;
        }

        log::info!("[{}] Successfully left session", room_id);

        Ok(())
    }

    /// Maps a simple disconnect reason string to MSC4143 class and reason.
    ///
    /// This is a helper for backward compatibility with code that passes simple string reasons.
    fn map_reason_to_msc4143(&self, reason: &str) -> (String, String) {
        // Map common reason strings to MSC4143 class/reason pairs
        match reason {
            // User actions
            "user_left" | "hangup" | "user_hung_up" => {
                ("user_action".to_string(), "hangup".to_string())
            }
            "switch_device" => ("user_action".to_string(), "switch_device".to_string()),

            // Client errors
            "ice_failed" | "media_error" | "transport_failure" | "encryption_error" => {
                ("client_error".to_string(), reason.to_string())
            }

            // Server errors
            "keep_alive_timeout" | "network_error" => {
                ("server_error".to_string(), reason.to_string())
            }

            // Default: treat as user action
            _ => ("user_action".to_string(), reason.to_string()),
        }
    }

    /// Restarts the keep-alive by canceling the current delayed leave and scheduling a new one.
    ///
    /// This is called periodically (heartbeat) to keep our membership active.
    /// The strategy is to always have a delayed leave scheduled, and restart it periodically.
    ///
    /// This method is fire-and-forget (doesn't return Result) because heartbeat failures
    /// should not break the application - we'll retry on the next heartbeat.
    pub async fn heartbeat(&self) {
        let room_id = self.room_id.clone();
        log::debug!("[{}] Heartbeat: restarting keep-alive", room_id);

        // First, cancel the existing delayed event if one exists
        if let Some(event_id) = self.delayed_event_id() {
            match self
                .command_sender
                .cancel_delayed_event(room_id.clone(), event_id.clone())
                .await
            {
                Ok(_) => {
                    log::debug!("[{}] Previous delayed leave canceled", room_id);
                }
                Err(e) => {
                    log::warn!("[{}] Failed to cancel delayed leave: {:?}", room_id, e);
                    // Continue anyway - we'll try to schedule a new one
                }
            }

            // Clear the stored event ID after attempting cancellation
            {
                let mut info_guard = self.keep_alive_info.lock().unwrap();
                *info_guard = None;
            }
        }

        // Then schedule a new delayed leave
        if let Err(e) = self.schedule_delayed_leave().await {
            log::warn!(
                "[{}] Failed to schedule new delayed leave: {:?}",
                room_id,
                e
            );
        }
    }

    /// Schedules a delayed leave event to clean up our membership if we disconnect.
    ///
    /// This is used internally by join() and heartbeat().
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the delayed event was scheduled successfully.
    /// Returns an error if scheduling failed.
    async fn schedule_delayed_leave(&self) -> Result<(), CommandError> {
        let room_id = self.room_id.clone();
        let slot_id = self.slot_id.clone();
        let sticky_key = self.sticky_key.clone();
        let keep_alive_timeout_ms = self.keep_alive_timeout_ms;

        log::debug!(
            "[{}] Scheduling new delayed leave (timeout: {}ms)",
            room_id,
            keep_alive_timeout_ms
        );

        // Use MSC4143-compliant content
        let delayed_content = self.build_delayed_leave_content(&slot_id, &sticky_key);

        // Schedule the delayed event and await its completion
        let delayed_event_id = self
            .command_sender
            .send_delayed_event(
                room_id.clone(),
                "m.rtc.member".to_string(),
                delayed_content,
                keep_alive_timeout_ms,
            )
            .await?;

        // Store the event ID
        {
            let mut info_guard = self.keep_alive_info.lock().unwrap();
            *info_guard = Some(KeepAliveInfo {
                delayed_event_id,
                timeout_ms: keep_alive_timeout_ms,
            });
        }

        log::debug!("[{}] Delayed leave scheduled successfully", room_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{MockCommandSender, NoopCommandSender};

    const USER_ID: &str = "@alice:example.org";
    const DEVICE_ID: &str = "device123";
    const APPLICATION_TYPE: &str = "m.call";

    #[test]
    fn test_machine_starts_not_joined() {
        let machine = OwnMembershipMachine::with_default_timeout(
            Arc::new(NoopCommandSender),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            APPLICATION_TYPE.to_string(),
        );

        assert_eq!(machine.state(), OwnMembershipState::NotJoined);
        assert!(machine.delayed_event_id().is_none());
    }

    #[test]
    fn test_machine_room_id() {
        let machine = OwnMembershipMachine::with_default_timeout(
            Arc::new(NoopCommandSender),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            APPLICATION_TYPE.to_string(),
        );

        assert_eq!(machine.room_id(), "!room:example.org");
    }

    #[test]
    fn test_machine_slot_id() {
        let machine = OwnMembershipMachine::with_default_timeout(
            Arc::new(NoopCommandSender),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            APPLICATION_TYPE.to_string(),
        );

        assert_eq!(machine.slot_id(), "m.call#ROOM");
    }

    #[test]
    fn test_machine_sticky_key() {
        let machine = OwnMembershipMachine::with_default_timeout(
            Arc::new(NoopCommandSender),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            APPLICATION_TYPE.to_string(),
        );

        assert_eq!(machine.sticky_key(), "alice-device-a");
    }

    #[tokio::test]
    async fn test_machine_join_schedules_delayed_leave() {
        let mock_sender = Arc::new(MockCommandSender::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            APPLICATION_TYPE.to_string(),
        );

        machine.join(None).await.expect("join should succeed");

        // Check that delayed events were scheduled
        let delayed_events = mock_sender.delayed_events.lock().unwrap();
        assert_eq!(delayed_events.len(), 1);

        // The first delayed event should be the leave (dead man's switch)
        let (room_id, event_type, content, _delay) = &delayed_events[0];
        assert_eq!(room_id, "!room:example.org");
        assert_eq!(event_type, "m.rtc.member");

        // Check that the content has the disconnect_reason
        assert!(content.get("disconnect_reason").is_some());
    }

    #[tokio::test]
    async fn test_machine_join_sends_join_event() {
        let mock_sender = Arc::new(MockCommandSender::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            APPLICATION_TYPE.to_string(),
        );

        machine.join(None).await.expect("join should succeed");

        // Check that sticky events were sent
        let sticky_events = mock_sender.sticky_events.lock().unwrap();
        assert_eq!(sticky_events.len(), 1);

        // The sticky event should be the join
        let (room_id, event_type, content) = &sticky_events[0];
        assert_eq!(room_id, "!room:example.org");
        assert_eq!(event_type, "m.rtc.member");
        assert_eq!(
            content.get("slot_id").and_then(|v| v.as_str()),
            Some("m.call#ROOM")
        );
    }

    #[tokio::test]
    async fn test_machine_join_with_transport() {
        let mock_sender = Arc::new(MockCommandSender::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            APPLICATION_TYPE.to_string(),
        );

        let transport = json!({
            "type": "livekit",
            "livekit_service_url": "https://example.com"
        });

        machine
            .join(Some(transport))
            .await
            .expect("join should succeed");

        // Check that the join event includes the transport
        let sticky_events = mock_sender.sticky_events.lock().unwrap();
        assert_eq!(sticky_events.len(), 1);

        let (_, _, content) = &sticky_events[0];
        assert!(content.get("rtc_transports").is_some());
    }

    #[tokio::test]
    async fn test_machine_leave_sends_leave_event() {
        let mock_sender = Arc::new(MockCommandSender::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            APPLICATION_TYPE.to_string(),
        );

        machine
            .leave(Some("user_left".to_string()))
            .await
            .expect("leave should succeed");

        // Check that leave event was sent
        let sticky_events = mock_sender.sticky_events.lock().unwrap();
        assert_eq!(sticky_events.len(), 1);

        let (room_id, event_type, content) = &sticky_events[0];
        assert_eq!(room_id, "!room:example.org");
        assert_eq!(event_type, "m.rtc.member");

        // MSC4143: disconnect_reason is now an object, not a string
        let disconnect_reason = content
            .get("disconnect_reason")
            .expect("disconnect_reason should be present");
        assert!(disconnect_reason.get("class").is_some());
        assert!(disconnect_reason.get("reason").is_some());
    }

    #[tokio::test]
    async fn test_machine_heartbeat_restarts_delayed_leave() {
        let mock_sender = Arc::new(MockCommandSender::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "alice-device-a".to_string(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            APPLICATION_TYPE.to_string(),
        );

        // Join to start the initial delayed leave
        machine.join(None).await.expect("join should succeed");

        // Get initial delayed event count
        let initial_count = {
            let delayed_events = mock_sender.delayed_events.lock().unwrap();
            delayed_events.len()
        };

        // Heartbeat should cancel and reschedule
        machine.heartbeat().await;

        // Check that another delayed event was scheduled
        let new_count = {
            let delayed_events = mock_sender.delayed_events.lock().unwrap();
            delayed_events.len()
        };

        // Should have scheduled at least one more (the cancel may or may not have been processed)
        assert!(new_count > initial_count);
    }
}
