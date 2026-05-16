//! In-memory RTC session membership model.
//!
//! This module stores the current participant view per `(room_id, slot_id)` and
//! applies joined/left transitions derived from sticky event DTO conversion.

#[derive(Clone, Debug, Default)]
/// In-memory view of members currently associated with one RTC session.
pub struct RtcSession {
    members: Vec<JoinedMembership>,
}

impl RtcSession {
    /// Creates an empty session membership view.
    pub fn new() -> Self {
        Self {
            members: Vec::new(),
        }
    }

    /// Applies one membership event to this session.
    pub fn update(&mut self, event: CallMembershipEvent) {
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
