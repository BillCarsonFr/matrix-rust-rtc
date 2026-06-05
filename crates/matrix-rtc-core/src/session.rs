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
use tokio::sync::watch;

#[derive(Clone, Debug)]
/// Per-session MatrixRTC state machine and membership store.
pub struct RtcSession {
    members: Vec<JoinedMembership>,
    membership_snapshots_tx: watch::Sender<Vec<JoinedMembership>>,
}

impl RtcSession {
    /// Creates an empty session.
    pub fn new() -> Self {
        let (membership_snapshots_tx, _membership_snapshots_rx) = watch::channel(Vec::new());

        Self {
            members: Vec::new(),
            membership_snapshots_tx,
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
