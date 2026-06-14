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

//! Event DTOs and conversion logic.
//!
//! These structs are transport-oriented DTOs: they carry sticky event data from host
//! SDK layers into the core without exposing host SDK types here.
//! Conversion then interprets DTO content as MatrixRTC membership events.

use serde::Serialize;

use crate::session::{CallMembershipEvent, JoinedMembership, LeftMembership};
use crate::transport::RawRtcTransport;
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

#[derive(Clone, Debug, Serialize)]
/// Content DTO extracted from a sticky Matrix event (MSC4143 compliant).
pub struct RawStickyEventContent {
    /// MatrixRTC slot identifier.
    pub slot_id: String,
    /// Sticky-map key associated with the sender/device membership.
    pub sticky_key: String,
    /// Application info from `content.application` (MSC4143).
    pub application: crate::session::ApplicationInfo,
    /// Member info from `content.member` (MSC4143).
    pub member: crate::session::MemberInfo,
    /// Protocol versions from `content.versions` (MSC4143).
    pub versions: Vec<String>,
    /// Optional disconnect reason for disconnected membership updates (MSC4143).
    pub disconnect_reason: Option<crate::session::DisconnectReason>,
    /// Optional relates-to reference (MSC4143).
    pub m_relates_to: Option<crate::session::RelatesTo>,
    /// RTC transports from `content.rtc_transports` (MSC4143 / MSC4195).
    pub rtc_transports: Option<Vec<RawRtcTransport>>,
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
    /// The event is interpreted as connected when connect content is present (has member.id);
    /// otherwise it is treated as disconnected, per MSC4143.
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

        let transports = self
            .content
            .rtc_transports
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|t| t.into_typed())
            .collect();

        let event = if self.content.is_connected() {
            // Build connected membership from MSC4143 fields
            let application_type = self
                .content
                .application
                .application_type
                .clone()
                .unwrap_or_default();

            CallMembershipEvent::Joined(JoinedMembership {
                room_id: self.room_id,
                slot_id: self.content.slot_id,
                sender: self.sender,
                sticky_key: self.content.sticky_key,
                application: Some(application_type),
                member: self.content.member.clone(),
                versions: self.content.versions.clone(),
                m_relates_to: self.content.m_relates_to.clone(),
                transports,
            })
        } else {
            // Build disconnected membership from MSC4143 fields
            CallMembershipEvent::Left(LeftMembership {
                room_id: self.room_id,
                slot_id: self.content.slot_id,
                sender: self.sender,
                sticky_key: self.content.sticky_key,
                disconnect_reason: self.content.disconnect_reason.clone(),
                m_relates_to: self.content.m_relates_to.clone(),
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
            disconnect_reason: self.content.disconnect_reason.clone(),
            m_relates_to: self.content.m_relates_to.clone(),
        }))
    }
}

impl RawStickyEventContent {
    /// MSC4143: An event is "connected" if it has member.id and application.type
    fn is_connected(&self) -> bool {
        self.member.id.as_deref().is_some_and(|v| !v.is_empty())
            && self
                .application
                .application_type
                .as_deref()
                .is_some_and(|v| !v.is_empty())
    }
}
