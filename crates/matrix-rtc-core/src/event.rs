//! Event DTOs and conversion logic.
//!
//! These structs are transport-oriented DTOs: they carry sticky event data from host
//! SDK layers into the core without exposing host SDK types here.
//! Conversion then interprets DTO content as MatrixRTC membership events.

use crate::session::{CallMembershipEvent, JoinedMembership, LeftMembership};
use thiserror::Error;

#[derive(Clone, Debug)]
/// Minimal sticky event DTO received from host SDK layers.
pub struct RawStickyEvent {
    /// Room where the event belongs.
    pub room_id: String,
    /// Sender user ID of the event.
    pub sender: String,
    /// Matrix event type, e.g. `m.rtc.member`.
    pub event_type: String,
    /// Event content subset needed by the core.
    pub content: RawStickyEventContent,
}

#[derive(Clone, Debug)]
/// Content DTO extracted from a sticky Matrix event.
pub struct RawStickyEventContent {
    /// MatrixRTC slot identifier.
    pub slot_id: String,
    /// Sticky-map key associated with the sender/device membership.
    pub sticky_key: String,
    /// Application type from `content.application.type`.
    pub application_type: Option<String>,
    /// Member identifier from `content.member.id`.
    pub member_id: Option<String>,
    /// Optional disconnect reason for disconnected membership updates.
    pub disconnect_reason: Option<String>,
    // TODO(msc4143, msc4195): model `rtc_transports` from m.rtc.member once
    // transport selection/LiveKit integration is implemented.
}

#[derive(Clone, Debug)]
/// Update payload for one sticky key where `current` supersedes `previous`.
pub struct RawStickyEventUpdate {
    /// New sticky event value.
    pub current: RawStickyEvent,
    /// Previous sticky event value.
    pub previous: RawStickyEvent,
}

#[derive(Clone, Debug, Default)]
/// SDK-provided sticky diff batches.
pub struct StickyEventsUpdate {
    /// New keys that had no predecessor.
    pub added: Vec<RawStickyEvent>,
    /// Keys that replaced an existing value.
    pub updated: Vec<RawStickyEventUpdate>,
    /// Keys removed from the sticky map (usually by expiry).
    pub removed: Vec<RawStickyEvent>,
}

#[derive(Debug, Error, Eq, PartialEq)]
/// Conversion errors while mapping transport DTOs into domain membership events.
pub enum EventConversionError {
    #[error("unsupported event type '{found}' (expected m.rtc.member)")]
    UnsupportedEventType { found: String },
    #[error("missing required field '{field}'")]
    MissingField { field: &'static str },
}

impl RawStickyEvent {
    /// Converts a raw sticky DTO into a domain membership event.
    ///
    /// The event is interpreted as connected when connect content is present;
    /// otherwise it is treated as disconnected, per the current skeleton behavior.
    pub fn try_into_call_membership_event(
        self,
    ) -> Result<CallMembershipEvent, EventConversionError> {
        if self.event_type != "m.rtc.member" && self.event_type != "org.matrix.msc4143.rtc.member" {
            return Err(EventConversionError::UnsupportedEventType {
                found: self.event_type,
            });
        }

        if self.content.slot_id.is_empty() {
            return Err(EventConversionError::MissingField { field: "slot_id" });
        }

        if self.content.sticky_key.is_empty() {
            return Err(EventConversionError::MissingField {
                field: "sticky_key",
            });
        }

        let event = if self.content.is_valid_connect_content() {
            CallMembershipEvent::Joined(JoinedMembership {
                room_id: self.room_id,
                slot_id: self.content.slot_id,
                sender: self.sender,
                sticky_key: self.content.sticky_key,
                application: self.content.application_type,
            })
        } else {
            CallMembershipEvent::Left(LeftMembership {
                room_id: self.room_id,
                slot_id: self.content.slot_id,
                sender: self.sender,
                sticky_key: self.content.sticky_key,
                disconnect_reason: self.content.disconnect_reason,
            })
        };

        Ok(event)
    }

    /// Converts a sticky DTO into a disconnected membership event.
    ///
    /// This is used for sticky removals/expiry, where the event should always
    /// be interpreted as a left membership regardless of its content shape.
    pub fn try_into_left_membership_event(
        self,
    ) -> Result<CallMembershipEvent, EventConversionError> {
        if self.event_type != "m.rtc.member" && self.event_type != "org.matrix.msc4143.rtc.member" {
            return Err(EventConversionError::UnsupportedEventType {
                found: self.event_type,
            });
        }

        if self.content.slot_id.is_empty() {
            return Err(EventConversionError::MissingField { field: "slot_id" });
        }

        if self.content.sticky_key.is_empty() {
            return Err(EventConversionError::MissingField {
                field: "sticky_key",
            });
        }

        Ok(CallMembershipEvent::Left(LeftMembership {
            room_id: self.room_id,
            slot_id: self.content.slot_id,
            sender: self.sender,
            sticky_key: self.content.sticky_key,
            disconnect_reason: self.content.disconnect_reason,
        }))
    }
}

impl RawStickyEventContent {
    fn is_valid_connect_content(&self) -> bool {
        has_non_empty(&self.application_type) && has_non_empty(&self.member_id)
    }
}

fn has_non_empty(value: &Option<String>) -> bool {
    value.as_deref().is_some_and(|v| !v.is_empty())
}
