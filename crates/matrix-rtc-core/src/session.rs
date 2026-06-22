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

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::watch;

use crate::commands::RtcCommandSender;
use crate::encryption::EncryptionManager;
use crate::error::{CommandError, JoinError, LeaveError};
use crate::join::{JoinSessionParams, LeaveSessionParams};
use crate::own_membership::{OwnMembershipMachine, transport_to_json};
use crate::transport::RtcTransport;

#[allow(unused_imports)]
use log::*;

/// Per-session MatrixRTC state machine and membership store.
pub struct RtcSession<T: RtcCommandSender> {
    members: Vec<JoinedMembership>,
    membership_snapshots_tx: watch::Sender<Vec<JoinedMembership>>,
    /// Command sender for sending events to the Matrix room.
    command_sender: Option<Arc<T>>,
    /// Machine for managing our own membership lifecycle (join/leave/keep-alive).
    own_membership_machine: Option<OwnMembershipMachine<T>>,
    /// Encryption manager for key distribution and management.
    encryption_manager: Option<EncryptionManager<T>>,
}

impl<T: RtcCommandSender> Clone for RtcSession<T> {
    fn clone(&self) -> Self {
        Self {
            members: self.members.clone(),
            membership_snapshots_tx: self.membership_snapshots_tx.clone(),
            command_sender: self.command_sender.clone(),
            own_membership_machine: None, // Don't clone the machine - it's not cloneable
            encryption_manager: None,     // Don't clone the encryption manager
        }
    }
}

impl<T: RtcCommandSender + 'static> RtcSession<T> {
    /// Creates an empty session without a command sender.
    pub fn new() -> Self {
        let (membership_snapshots_tx, _membership_snapshots_rx) = watch::channel(Vec::new());

        Self {
            members: Vec::new(),
            membership_snapshots_tx,
            command_sender: None,
            own_membership_machine: None,
            encryption_manager: None,
        }
    }

    /// Creates an empty session with a command sender.
    pub fn with_command_sender(command_sender: Arc<T>) -> Self {
        let (membership_snapshots_tx, _membership_snapshots_rx) = watch::channel(Vec::new());

        Self {
            members: Vec::new(),
            membership_snapshots_tx,
            command_sender: Some(command_sender),
            own_membership_machine: None,
            encryption_manager: None,
        }
    }

    /// Sets the command sender for this session.
    pub fn set_command_sender(&mut self, command_sender: Arc<T>) {
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
    /// The dead man's switch strategy is used:
    /// 1. Schedule delayed leave event FIRST (safety net) - **awaited**
    /// 2. Send join membership event - **awaited**
    /// 3. Heartbeat will restart the delayed leave periodically
    ///
    /// The async design ensures the delayed leave is scheduled before the join event is sent.
    ///
    /// # Arguments
    ///
    /// * `params` - The join parameters including user info, transport, etc.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the join completed successfully.
    /// Returns `Err(JoinError)` if validation fails, command sender not configured, or commands fail.
    pub async fn join(&mut self, params: JoinSessionParams) -> Result<(), JoinError> {
        params.validate().map_err(JoinError::MissingParameter)?;

        let command_sender = self.command_sender.as_ref().ok_or(JoinError::CommandError(
            CommandError::from_message("no command sender configured"),
        ))?;

        let membership_id = params.membership_id();

        // Check if already joined with this membership
        if self
            .own_membership_machine
            .as_ref()
            .is_some_and(|machine| machine.sticky_key() == membership_id)
        {
            return Err(JoinError::AlreadyJoined(membership_id));
        }

        // Create the own membership machine
        let transport_json = transport_to_json(&params.transport);
        let machine = OwnMembershipMachine::new(
            command_sender.clone(),
            params.room_id.clone(),
            params.slot_id.clone(),
            membership_id.clone(),
            params.user_id.clone(),
            params.device_id.clone(),
            params.application.clone(),
            params.keep_alive_timeout_ms(),
        );

        // Use the machine to join (async, awaits both delayed leave scheduling and join event)
        machine.join(Some(transport_json)).await?;

        // Store the machine
        self.own_membership_machine = Some(machine);

        // Create the encryption manager
        // We need a closure that can access self.members
        // Since we can't capture self by reference in an Arc closure, we'll use a different approach
        // For now, we'll create a simple closure that clones the members vector
        let get_memberships_for_encryption = {
            let members_tx = self.membership_snapshots_tx.clone();
            move || members_tx.borrow().clone()
        };

        let encryption_config = params.encryption_config();
        let mut encryption_manager = EncryptionManager::new(
            command_sender.clone(),
            params.user_id.clone(),
            params.device_id.clone(),
            membership_id.clone(),
            params.room_id.clone(),
            params.slot_id.clone(),
            get_memberships_for_encryption,
        );
        encryption_manager.set_config(encryption_config);

        // Start the encryption manager (creates first key)
        encryption_manager.join().await.map_err(|e| {
            JoinError::CommandError(CommandError::from_message(format!(
                "failed to start encryption manager: {:?}",
                e
            )))
        })?;

        // Store the encryption manager
        self.encryption_manager = Some(encryption_manager);

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
    /// Returns `Ok(())` if the leave completed successfully.
    /// Returns `Err(LeaveError)` if not joined, command sender is not configured, or commands fail.
    pub async fn leave(&mut self, params: LeaveSessionParams) -> Result<(), LeaveError> {
        // Check if we have a membership machine (i.e., we've joined)
        let machine = self
            .own_membership_machine
            .take()
            .ok_or(LeaveError::NotJoined)?;

        // Use the machine to leave (async, awaits both leave event and delayed event cancellation)
        machine.leave(params.disconnect_reason.clone()).await?;

        // Clean up the encryption manager
        if let Some(encryption_manager) = self.encryption_manager.take() {
            encryption_manager.leave();
        }

        Ok(())
    }

    /// Performs a heartbeat to restart the keep-alive delayed leave event.
    ///
    /// This should be called periodically (e.g., every 15-20 seconds) to keep the
    /// membership active. The dead man's switch strategy ensures that if the
    /// client stops sending heartbeats, the delayed leave will fire and clean up.
    ///
    /// # Returns
    ///
    /// Returns `true` if the heartbeat was processed successfully.
    /// Returns `false` if not joined (no membership machine active).
    pub async fn heartbeat(&mut self) -> bool {
        if let Some(machine) = self.own_membership_machine.as_ref() {
            machine.heartbeat().await;
            true
        } else {
            false
        }
    }

    /// Returns the number of currently tracked joined members.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Subscribes to full membership snapshots for this session as a watch receiver.
    ///
    /// This is used by bindings that implement their own polling/callback model.
    pub fn subscribe_membership_snapshots(&self) -> watch::Receiver<Vec<JoinedMembership>> {
        self.membership_snapshots_tx.subscribe()
    }

    /// Applies the initial membership events for this single session.
    pub async fn initial_events(&mut self, events: impl IntoIterator<Item = CallMembershipEvent>) {
        for event in events {
            self.apply_membership_event(event).await;
        }
    }

    /// Applies a membership update batch for this single session.
    pub async fn handle_update(&mut self, events: impl IntoIterator<Item = CallMembershipEvent>) {
        for event in events {
            self.apply_membership_event(event).await;
        }
    }

    /// Applies one membership event to this session.
    pub async fn update(&mut self, event: CallMembershipEvent) {
        self.apply_membership_event(event).await;
    }

    async fn apply_membership_event(&mut self, event: CallMembershipEvent) {
        let membership_changed = match event {
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
                true
            }
            CallMembershipEvent::Left(left) => {
                let before = self.members.len();
                self.members.retain(|member| {
                    !(member.sender == left.sender && member.sticky_key == left.sticky_key)
                });

                if self.members.len() != before {
                    self.publish_membership_snapshot();
                    true
                } else {
                    false
                }
            }
        };

        // Notify encryption manager of membership changes
        if membership_changed && let Some(ref encryption_manager) = self.encryption_manager {
            // Notify the encryption manager directly (async)
            let _ = encryption_manager.on_memberships_update().await;
        }
    }

    fn publish_membership_snapshot(&self) {
        self.membership_snapshots_tx
            .send_replace(self.members.clone());
    }
}

/// MSC4143: Relates-to reference for event continuity
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelatesTo {
    #[serde(rename = "rel_type")]
    pub relation_type: String,
    pub event_id: String,
}

/// MSC4143: Member object
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberInfo {
    /// UUID to distinguish multiple participations
    pub id: Option<String>,
    /// Matrix device identifier
    pub claimed_device_id: Option<String>,
    /// Matrix user ID
    pub claimed_user_id: Option<String>,
}

/// MSC4143: Application info
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationInfo {
    #[serde(rename = "type")]
    pub application_type: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// MSC4143: Disconnect reason
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisconnectReason {
    /// High-level category (e.g., "user_action", "server_error", "client_error")
    pub class: Option<String>,
    /// Machine-readable identifier (e.g., "hangup", "ice_failed")
    pub reason: Option<String>,
    /// Optional human-readable explanation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug)]
/// Membership event projection derived from sticky event content.
pub enum CallMembershipEvent {
    /// A member is connected for the slot.
    Joined(JoinedMembership),
    /// A member is disconnected for the slot.
    Left(LeftMembership),
}

/// MSC4143: Connected membership payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinedMembership {
    /// Room where the membership is active.
    pub room_id: String,
    /// MatrixRTC slot identifier.
    pub slot_id: String,
    /// Sender user ID of the membership event.
    pub sender: String,
    /// Sticky key identifying this membership stream.
    pub sticky_key: String,
    /// Application info (MSC4143).
    pub application: Option<String>,
    /// Member info (MSC4143).
    pub member: MemberInfo,
    /// Protocol versions (MSC4143).
    pub versions: Vec<String>,
    /// Optional relates-to reference (MSC4143).
    pub m_relates_to: Option<RelatesTo>,
    /// RTC transports for this member (MSC4143 / MSC4195).
    pub transports: Vec<RtcTransport>,
    /// Timestamp (ms) when this membership was created (MSC4143).
    pub created_ts: Option<u64>,
}

/// MSC4143: Disconnected membership payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeftMembership {
    /// Room where the membership was active.
    pub room_id: String,
    /// MatrixRTC slot identifier.
    pub slot_id: String,
    /// Sender user ID of the membership event.
    pub sender: String,
    /// Sticky key identifying this membership stream.
    pub sticky_key: String,
    /// Optional disconnect reason (MSC4143 compliant object).
    pub disconnect_reason: Option<DisconnectReason>,
    /// Optional relates-to reference (MSC4143).
    pub m_relates_to: Option<RelatesTo>,
}

impl<T: RtcCommandSender + 'static> Default for RtcSession<T> {
    fn default() -> Self {
        Self::new()
    }
}
