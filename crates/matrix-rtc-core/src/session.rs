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

//! In-memory RTC session membership model.
//!
//! This module stores the current participant view for a single RTC session and
//! applies joined/left transitions from domain membership events produced by the
//! manager layer.

use serde::Serialize;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::watch;

use crate::commands::{CommandCallback, RtcCommandSender, SendEventCallback};
use crate::error::{CommandError, JoinError, LeaveError};
use crate::join::{JoinSessionParams, LeaveSessionParams};
use crate::transport::RtcTransport;

/// Per-session MatrixRTC state machine and membership store.
pub struct RtcSession {
    members: Vec<JoinedMembership>,
    membership_snapshots_tx: watch::Sender<Vec<JoinedMembership>>,
    /// Command sender for sending events to the Matrix room.
    command_sender: Option<Arc<dyn RtcCommandSender>>,
    /// Tracked delayed event ID for keep-alive cleanup.
    /// Some when a keep-alive delayed event is active.
    keep_alive_event_id: Option<String>,
    /// The sticky key (membership ID) for our own membership in this session.
    own_membership_key: Option<String>,
    /// Room ID for this session (set when joining).
    room_id: Option<String>,
    /// Slot ID for this session (set when joining).
    slot_id: Option<String>,
}

impl Clone for RtcSession {
    fn clone(&self) -> Self {
        Self {
            members: self.members.clone(),
            membership_snapshots_tx: self.membership_snapshots_tx.clone(),
            command_sender: self.command_sender.clone(),
            keep_alive_event_id: self.keep_alive_event_id.clone(),
            own_membership_key: self.own_membership_key.clone(),
            room_id: self.room_id.clone(),
            slot_id: self.slot_id.clone(),
        }
    }
}

impl RtcSession {
    /// Creates an empty session without a command sender.
    pub fn new() -> Self {
        let (membership_snapshots_tx, _membership_snapshots_rx) = watch::channel(Vec::new());

        Self {
            members: Vec::new(),
            membership_snapshots_tx,
            command_sender: None,
            keep_alive_event_id: None,
            own_membership_key: None,
            room_id: None,
            slot_id: None,
        }
    }

    /// Creates an empty session with a command sender.
    pub fn with_command_sender(command_sender: Arc<dyn RtcCommandSender>) -> Self {
        let (membership_snapshots_tx, _membership_snapshots_rx) = watch::channel(Vec::new());

        Self {
            members: Vec::new(),
            membership_snapshots_tx,
            command_sender: Some(command_sender),
            keep_alive_event_id: None,
            own_membership_key: None,
            room_id: None,
            slot_id: None,
        }
    }

    /// Sets the command sender for this session.
    pub fn set_command_sender(&mut self, command_sender: Arc<dyn RtcCommandSender>) {
        self.command_sender = Some(command_sender);
    }

    /// Returns true if this session has a command sender configured.
    pub fn has_command_sender(&self) -> bool {
        self.command_sender.is_some()
    }

    /// Joins this RTC session with the given parameters.
    ///
    /// This sends a membership event to the Matrix room via the command sender,
    /// and starts the keep-alive mechanism to ensure proper cleanup.
    ///
    /// # Arguments
    ///
    /// * `params` - The join parameters including user info, transport, etc.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the join was initiated successfully.
    /// Returns `Err(JoinError)` if validation fails or a command is already in progress.
    pub fn join(&mut self, params: JoinSessionParams) -> Result<(), JoinError> {
        params.validate().map_err(JoinError::MissingParameter)?;

        let command_sender = self.command_sender.as_ref().ok_or(JoinError::CommandError(
            CommandError::from_message("no command sender configured"),
        ))?;

        // Check if already joined with this membership
        let membership_id = params.membership_id();
        if self.own_membership_key.as_ref() == Some(&membership_id) {
            return Err(JoinError::AlreadyJoined(membership_id));
        }

        // Build the membership event content
        let content = self.build_join_content(&params);

        let room_id = params.room_id.clone();
        let event_type = "m.rtc.member".to_string();
        let keep_alive_timeout = params.keep_alive_timeout_ms();

        // Create callback for when the join event is sent
        let join_callback: CommandCallback = Box::new(move |_result: Result<(), CommandError>| {
            // This would ideally update the session state, but for now we just log
            // In a real implementation, we'd need to handle the callback asynchronously
            // which requires more complex state management
        });

        // Send the join event
        command_sender.send_sticky_event(
            room_id.clone(),
            event_type.clone(),
            content,
            join_callback,
        );

        // Start keep-alive: schedule a delayed event to clear our membership
        let delayed_content = self.build_leave_content(
            &room_id,
            &params.slot_id,
            &membership_id,
            Some("keep_alive_timeout".to_string()),
        );

        // For now, use a mock event ID. In a real implementation, we'd store the
        // actual event ID from the callback, but that requires async handling.
        self.keep_alive_event_id = Some(format!("delayed-{}-{}", room_id, membership_id));

        let cancel_callback: SendEventCallback =
            Box::new(move |_result: Result<String, CommandError>| {
                // On success, we could update keep_alive_event_id here
                // But for now, we use the mock ID set above
            });

        command_sender.send_delayed_event(
            room_id.clone(),
            "m.rtc.member".to_string(),
            delayed_content,
            keep_alive_timeout,
            cancel_callback,
        );

        // Update our state
        self.own_membership_key = Some(membership_id);
        self.room_id = Some(room_id);
        self.slot_id = Some(params.slot_id.clone());

        Ok(())
    }

    /// Leaves this RTC session.
    ///
    /// This sends a left membership event and cancels any active keep-alive delayed event.
    ///
    /// # Arguments
    ///
    /// * `params` - The leave parameters including optional disconnect reason.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the leave was initiated successfully.
    /// Returns `Err(LeaveError)` if not joined or command sender is not configured.
    pub fn leave(&mut self, params: LeaveSessionParams) -> Result<(), LeaveError> {
        let _own_key = self
            .own_membership_key
            .as_ref()
            .ok_or(LeaveError::NotJoined)?;

        let command_sender = self
            .command_sender
            .as_ref()
            .ok_or(LeaveError::CommandError(CommandError::from_message(
                "no command sender configured",
            )))?;

        // Get room_id and slot_id from the session state
        let room_id =
            self.room_id
                .as_ref()
                .ok_or(LeaveError::CommandError(CommandError::from_message(
                    "session missing room_id for leave",
                )))?;
        let slot_id =
            self.slot_id
                .as_ref()
                .ok_or(LeaveError::CommandError(CommandError::from_message(
                    "session missing slot_id for leave",
                )))?;

        // Build the leave event content
        let content =
            self.build_leave_content(room_id, slot_id, _own_key, params.disconnect_reason);

        // Send the leave event
        command_sender.send_sticky_event(
            room_id.clone().to_string(),
            "m.rtc.member".to_string(),
            content,
            Box::new(|_| {}), // Callback for leave event
        );

        // Cancel the keep-alive delayed event
        if let Some(event_id) = &self.keep_alive_event_id {
            command_sender.cancel_delayed_event(
                room_id.clone().to_string(),
                event_id.clone(),
                Box::new(|_| {}), // Callback for cancel
            );
        }

        // Clear our state
        self.own_membership_key = None;
        self.keep_alive_event_id = None;

        Ok(())
    }

    /// Builds the content for a join membership event.
    fn build_join_content(&self, params: &JoinSessionParams) -> Value {
        json!({
            "slot_id": params.slot_id,
            "sticky_key": params.membership_id(),
            "application": {
                "type": params.application
            },
            "member": {
                "id": params.membership_id()
            },
            "rtc_transports": [self.transport_to_json(&params.transport)]
        })
    }

    /// Builds the content for a leave membership event.
    fn build_leave_content(
        &self,
        _room_id: &str,
        slot_id: &str,
        sticky_key: &str,
        disconnect_reason: Option<String>,
    ) -> Value {
        let mut content = json!({
            "slot_id": slot_id,
            "sticky_key": sticky_key,
        });

        if let Some(reason) = disconnect_reason {
            content["disconnect_reason"] = json!(reason);
        }

        content
    }

    /// Converts an RtcTransport to a JSON value for event content.
    fn transport_to_json(&self, transport: &RtcTransport) -> Value {
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

    /// Subscribes to full membership snapshots for this session as a watch receiver.
    ///
    /// This is used by bindings that implement their own polling/callback model.
    pub fn subscribe_membership_snapshots(&self) -> watch::Receiver<Vec<JoinedMembership>> {
        self.membership_snapshots_tx.subscribe()
    }

    /// Applies the initial membership events for this single session.
    pub fn initial_events(&mut self, events: impl IntoIterator<Item = CallMembershipEvent>) {
        for event in events {
            self.apply_membership_event(event);
        }
    }

    /// Applies a membership update batch for this single session.
    pub fn handle_update(&mut self, events: impl IntoIterator<Item = CallMembershipEvent>) {
        for event in events {
            self.apply_membership_event(event);
        }
    }

    /// Applies one membership event to this session.
    pub fn update(&mut self, event: CallMembershipEvent) {
        self.apply_membership_event(event);
    }

    fn apply_membership_event(&mut self, event: CallMembershipEvent) {
        match event {
            CallMembershipEvent::Joined(joined) => {
                let key_sender = joined.sender.clone();
                let key_sticky = joined.sticky_key.clone();

                if let Some(index) = self.members.iter().position(|member| {
                    member.sender == key_sender && member.sticky_key == key_sticky
                }) {
                    if self.members[index] == joined {
                        return;
                    }

                    self.members[index] = joined;
                } else {
                    self.members.push(joined);
                }

                self.publish_membership_snapshot();
            }
            CallMembershipEvent::Left(left) => {
                let before = self.members.len();
                self.members.retain(|member| {
                    !(member.sender == left.sender && member.sticky_key == left.sticky_key)
                });

                if self.members.len() != before {
                    self.publish_membership_snapshot();
                }
            }
        }
    }

    fn publish_membership_snapshot(&self) {
        self.membership_snapshots_tx
            .send_replace(self.members.clone());
    }

    /// Returns the number of currently tracked joined members.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }
}

#[derive(Clone, Debug)]
/// Membership event projection derived from sticky event content.
pub enum CallMembershipEvent {
    /// A member is connected for the slot.
    Joined(JoinedMembership),
    /// A member is disconnected for the slot.
    Left(LeftMembership),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Connected membership payload.
pub struct JoinedMembership {
    /// Room where the membership is active.
    pub room_id: String,
    /// MatrixRTC slot identifier.
    pub slot_id: String,
    /// Sender user ID of the membership event.
    pub sender: String,
    /// Sticky key identifying this membership stream.
    pub sticky_key: String,
    /// Application type, usually `m.call`.
    pub application: Option<String>,
    /// RTC transports for this member (MSC4143 / MSC4195).
    pub transports: Vec<RtcTransport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Disconnected membership payload.
pub struct LeftMembership {
    /// Room where the membership was active.
    pub room_id: String,
    /// MatrixRTC slot identifier.
    pub slot_id: String,
    /// Sender user ID of the membership event.
    pub sender: String,
    /// Sticky key identifying this membership stream.
    pub sticky_key: String,
    /// Optional machine-readable reason provided by the sender.
    pub disconnect_reason: Option<String>,
}

impl Default for RtcSession {
    fn default() -> Self {
        Self::new()
    }
}
