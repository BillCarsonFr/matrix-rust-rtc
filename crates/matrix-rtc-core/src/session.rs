//! In-memory RTC session membership model.
//!
//! This module stores the current participant view per `(room_id, slot_id)` and
//! applies joined/left transitions derived from sticky event DTO conversion.

use crate::event::{EventConversionError, RawStickyEvent, StickyEventsUpdate};

#[derive(Clone, Debug, Default)]
/// Per-session MatrixRTC state machine and membership store.
pub struct RtcSession {
    members: Vec<JoinedMembership>,
}

impl RtcSession {
    /// Creates an empty session.
    pub fn new() -> Self {
        Self {
            members: Vec::new(),
        }
    }

    /// Applies the initial sticky events for this single session.
    pub fn initial_events(
        &mut self,
        events: impl IntoIterator<Item = RawStickyEvent>,
    ) -> Result<(), EventConversionError> {
        for event in events {
            self.apply_membership_event(event.try_into_call_membership_event()?);
        }

        Ok(())
    }

    /// Applies a sticky update batch for this single session.
    pub fn handle_update(
        &mut self,
        update: StickyEventsUpdate,
    ) -> Result<(), EventConversionError> {
        for event in update.added {
            self.apply_membership_event(event.try_into_call_membership_event()?);
        }

        for changed in update.updated {
            self.apply_membership_event(changed.current.try_into_call_membership_event()?);
        }

        for event in update.removed {
            self.apply_membership_event(event.try_into_left_membership_event()?);
        }

        Ok(())
    }

    /// Applies one membership event to this session.
    pub fn update(&mut self, event: CallMembershipEvent) {
        self.apply_membership_event(event);
    }

    fn apply_membership_event(&mut self, event: CallMembershipEvent) {
        match event {
            CallMembershipEvent::Joined(joined) => {
                self.members.retain(|member| {
                    !(member.sender == joined.sender && member.sticky_key == joined.sticky_key)
                });
                self.members.push(joined);
            }
            CallMembershipEvent::Left(left) => {
                self.members.retain(|member| {
                    !(member.sender == left.sender && member.sticky_key == left.sticky_key)
                });
            }
        }
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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
