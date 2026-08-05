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

use serde::{Deserialize, Serialize};

use crate::session::{
    ApplicationInfo, CallMembershipEvent, JoinedMembership, LeaveReason, LeftMembership,
    MemberInfo, Membership,
};
use crate::transport::MemberTransports;
use thiserror::Error;

/// How an event reached us, and what its decryption metadata says.
///
/// MSC4143 asks two things of this: member events MUST be encrypted in
/// encrypted rooms, and a member's device is identified by the device that
/// encrypted their member event rather than by any field in the content. Both
/// come from the same place, so they are modelled as one value — a cleartext
/// event cannot carry a sending device, and that is unrepresentable here.
///
/// This mirrors [`KeyOrigin`], which does the same job for to-device messages.
///
/// [`KeyOrigin`]: crate::KeyOrigin
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventOrigin {
    /// The host did not report how the event arrived. Rules that depend on it
    /// are not applied.
    #[default]
    Unknown,
    /// The event arrived in the clear.
    Cleartext,
    /// The event arrived encrypted and was decrypted.
    Encrypted {
        /// The sending device, as decryption attributed it.
        ///
        /// Optional only because the SDK types it that way. Modern Olm messages
        /// carry the sender's device keys, so a decrypted event should always
        /// resolve to a device; an absent one means something is off rather than
        /// being a normal case to design around.
        sender_device_id: Option<String>,
    },
}

impl EventOrigin {
    /// Builds an origin for an event the host decrypted.
    pub fn encrypted(sender_device_id: Option<String>) -> Self {
        Self::Encrypted { sender_device_id }
    }

    /// The sending device, if the event was encrypted and attributable.
    pub fn sender_device_id(&self) -> Option<&str> {
        match self {
            Self::Encrypted { sender_device_id } => sender_device_id.as_deref(),
            Self::Unknown | Self::Cleartext => None,
        }
    }

    /// Whether the event arrived encrypted; `None` when the host did not say.
    pub fn was_encrypted(&self) -> Option<bool> {
        match self {
            Self::Unknown => None,
            Self::Cleartext => Some(false),
            Self::Encrypted { .. } => Some(true),
        }
    }
}

#[derive(Clone, Debug)]
/// Minimal sticky event DTO received from host SDK layers.
pub struct RawStickyEvent {
    /// Room where the event belongs.
    pub room_id: String,
    /// Sender user ID of the event.
    pub sender: String,
    /// How the event reached us, including the sending device.
    pub origin: EventOrigin,
    /// Matrix event type, e.g. `m.rtc.member`.
    pub event_type: String,
    /// Event content subset needed by the core.
    pub content: RawStickyEventContent,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
/// Content DTO extracted from a sticky Matrix event (MSC4143 compliant).
///
/// `Deserialize` lets host SDK layers parse an `m.rtc.member` event's `content`
/// object straight into this DTO (see `matrix-rtc-livekit`'s `matrix_bridge`).
/// Only `slot_id` and `sticky_key` are required; a leave event carries just
/// those plus `member` and `leave_reason`, so the rest default.
pub struct RawStickyEventContent {
    /// MatrixRTC slot identifier.
    pub slot_id: String,
    /// Sticky-map key associated with this membership; equal to `member.id`.
    ///
    /// Sent under the unstable MSC4354 name, and accepted under either. The
    /// stable spelling is what MSC4354 lands as, and matrix-rust-sdk already
    /// reads both — being stricter than the SDK here means a peer that has moved
    /// to `sticky_key` fails to deserialize, and the member event is then dropped
    /// whole: that participant simply never appears in the call.
    #[serde(rename = "msc4354_sticky_key", alias = "sticky_key")]
    pub sticky_key: String,
    /// Member info from `content.member` (MSC4143).
    #[serde(default, skip_serializing_if = "MemberInfo::is_empty")]
    pub member: MemberInfo,
    /// Application info from `content.application` (MSC4143).
    #[serde(default, skip_serializing_if = "ApplicationInfo::is_empty")]
    pub application: ApplicationInfo,
    /// Published / subscribable transports from `content.transports` (MSC4143).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transports: Option<MemberTransports>,
    /// Optional leave reason, for `membership = leave` (MSC4143).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leave_reason: Option<LeaveReason>,
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
    /// Validates the shared preconditions of both conversions.
    fn check_convertible(&self) -> Result<(), EventConversionError> {
        if self.event_type != "m.rtc.member" && self.event_type != "org.matrix.msc4143.rtc.member" {
            return Err(EventConversionError::UnsupportedEventType {
                found: self.event_type.clone(),
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

        Ok(())
    }

    /// Converts a raw sticky DTO into a domain membership event.
    ///
    /// Per MSC4143 the event counts as joined only when `member.membership` is
    /// `join`, `member.id` is set and the application object names a type.
    /// Anything else — including a content shape this client cannot make sense
    /// of — is treated as left.
    ///
    /// The remaining MSC4143 join conditions (an open `m.rtc.slot`, the sender
    /// still being joined to the room, and the event still being sticky) depend
    /// on state this layer does not see; they are applied by the session.
    pub fn try_into_call_membership_event(
        self,
    ) -> Result<CallMembershipEvent, EventConversionError> {
        self.check_convertible()?;

        let application_type = self.content.application.application_type.clone();
        let member_id = self.content.member.id.clone();

        // `join` also requires an application type: MSC4143 makes the application
        // object REQUIRED on a joining member event.
        let joined = self.content.member.is_join()
            && application_type.as_deref().is_some_and(|t| !t.is_empty());

        if !joined {
            return Ok(CallMembershipEvent::Left(self.into_left(member_id)));
        }

        let member_id = member_id.expect("is_join() guarantees a non-empty member.id");

        if member_id != self.content.sticky_key {
            // MSC4143 requires sticky_key == member.id. Honour the sticky_key for
            // map identity regardless, but the mismatch is worth surfacing.
            log::warn!(
                "m.rtc.member from {} has sticky_key '{}' != member.id '{}'",
                self.sender,
                self.content.sticky_key,
                member_id,
            );
        }

        let transports = self.content.transports.unwrap_or_default();

        Ok(CallMembershipEvent::Joined(JoinedMembership {
            room_id: self.room_id,
            slot_id: self.content.slot_id,
            sender: self.sender,
            origin: self.origin,
            sticky_key: self.content.sticky_key,
            member_id,
            application: application_type,
            transports: transports
                .published
                .into_iter()
                .map(|t| t.into_typed())
                .collect(),
            can_subscribe: transports.can_subscribe,
        }))
    }

    /// Converts a sticky DTO into a left membership event.
    ///
    /// This is used for sticky removals/expiry, where the event should always
    /// be interpreted as a left membership regardless of its content shape.
    pub fn try_into_left_membership_event(
        self,
    ) -> Result<CallMembershipEvent, EventConversionError> {
        self.check_convertible()?;
        let member_id = self.content.member.id.clone();
        Ok(CallMembershipEvent::Left(self.into_left(member_id)))
    }

    fn into_left(self, member_id: Option<String>) -> LeftMembership {
        LeftMembership {
            room_id: self.room_id,
            slot_id: self.content.slot_id,
            sender: self.sender,
            sticky_key: self.content.sticky_key,
            member_id,
            leave_reason: self.content.leave_reason,
        }
    }
}

impl RawStickyEventContent {
    /// Builds MSC4143-compliant content for a join membership event.
    ///
    /// This is the single source of truth for the outgoing `m.rtc.member` join wire
    /// format: the same struct governs both serialization here and deserialization of
    /// incoming events, so field names (e.g. the `msc4354_sticky_key` rename) live in
    /// exactly one place.
    ///
    /// `member_id` doubles as the sticky key, as MSC4143 requires.
    pub(crate) fn for_join(
        slot_id: String,
        member_id: String,
        application_type: String,
        transports: MemberTransports,
    ) -> Self {
        Self {
            slot_id,
            application: ApplicationInfo {
                application_type: Some(application_type),
                ..ApplicationInfo::default()
            },
            member: MemberInfo {
                id: Some(member_id.clone()),
                membership: Some(Membership::Join),
            },
            sticky_key: member_id,
            // Skipped entirely when this member neither publishes nor states
            // anything it can receive.
            transports: (!transports.is_empty()).then_some(transports),
            leave_reason: None,
        }
    }

    /// Builds MSC4143-compliant content for a leave membership event.
    ///
    /// Only `slot_id`, `member` and the optional `leave_reason` are emitted; the
    /// join-only fields are left empty and skipped during serialization.
    pub(crate) fn for_leave(
        slot_id: String,
        member_id: String,
        leave_reason: Option<LeaveReason>,
    ) -> Self {
        Self {
            slot_id,
            member: MemberInfo {
                id: Some(member_id.clone()),
                membership: Some(Membership::Leave),
            },
            sticky_key: member_id,
            application: ApplicationInfo::default(),
            transports: None,
            leave_reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::LeaveCode;
    use crate::transport::RawRtcTransport;
    use crate::transport::RtcTransport;

    /// A spec-shaped join event as another client would send it on the wire.
    const JOIN_JSON: &str = r#"{
        "slot_id": "m.call#ROOM",
        "member": { "id": "xyzABCDEF0123", "membership": "join" },
        "application": { "type": "m.call", "m.call.voice_only": true },
        "transports": {
            "published": [
                { "type": "livekit", "livekit_service_url": "https://sfu.example.com/jwt" }
            ],
            "can_subscribe": ["livekit"]
        },
        "msc4354_sticky_key": "xyzABCDEF0123"
    }"#;

    fn event(content: RawStickyEventContent) -> RawStickyEvent {
        RawStickyEvent {
            room_id: "!room:example.org".to_owned(),
            sender: "@alice:example.org".to_owned(),
            origin: EventOrigin::encrypted(Some("DEVICEID".to_owned())),
            event_type: "m.rtc.member".to_owned(),
            content,
        }
    }

    fn parse(json: &str) -> CallMembershipEvent {
        event(serde_json::from_str(json).expect("content must parse"))
            .try_into_call_membership_event()
            .expect("conversion must succeed")
    }

    /// The two facts MSC4143 needs from decryption — was it encrypted, and by
    /// which device — travel together, so a cleartext event cannot claim a
    /// sending device.
    #[test]
    fn origin_reports_encryption_and_device_together() {
        let attributed = EventOrigin::encrypted(Some("DEVICEID".to_owned()));
        assert_eq!(attributed.was_encrypted(), Some(true));
        assert_eq!(attributed.sender_device_id(), Some("DEVICEID"));

        // Encrypted but unattributable: the SDK could not name the device.
        let unattributed = EventOrigin::encrypted(None);
        assert_eq!(unattributed.was_encrypted(), Some(true));
        assert_eq!(unattributed.sender_device_id(), None);

        assert_eq!(EventOrigin::Cleartext.was_encrypted(), Some(false));
        assert_eq!(EventOrigin::Cleartext.sender_device_id(), None);

        // Unreported is distinct from cleartext: the rules that depend on it
        // are skipped rather than failed.
        assert_eq!(EventOrigin::Unknown.was_encrypted(), None);
        assert_eq!(EventOrigin::default(), EventOrigin::Unknown);
    }

    #[test]
    fn spec_shaped_join_event_parses() {
        match parse(JOIN_JSON) {
            CallMembershipEvent::Joined(joined) => {
                assert_eq!(joined.member_id, "xyzABCDEF0123");
                assert_eq!(joined.sticky_key, "xyzABCDEF0123");
                assert_eq!(joined.application.as_deref(), Some("m.call"));
                assert_eq!(joined.can_subscribe, vec!["livekit".to_owned()]);
                assert_eq!(joined.origin.sender_device_id(), Some("DEVICEID"));
                match &joined.transports[..] {
                    [RtcTransport::LiveKit(livekit)] => {
                        assert_eq!(livekit.livekit_service_url, "https://sfu.example.com/jwt");
                    }
                    other => panic!("expected one livekit transport, got {other:?}"),
                }
            }
            CallMembershipEvent::Left(_) => panic!("expected a joined membership"),
        }
    }

    /// `member.membership` is authoritative: fully join-shaped content (a
    /// `member.id`, an application, transports) is still a leave if it says so.
    #[test]
    fn membership_leave_wins_over_join_shaped_content() {
        let json = JOIN_JSON.replace(r#""membership": "join""#, r#""membership": "leave""#);
        match parse(&json) {
            CallMembershipEvent::Left(left) => {
                assert_eq!(left.member_id.as_deref(), Some("xyzABCDEF0123"));
            }
            CallMembershipEvent::Joined(_) => panic!("membership=leave must not count as joined"),
        }
    }

    /// A membership value from a future spec revision must not fail the parse;
    /// it simply doesn't count as joined.
    #[test]
    fn unknown_membership_parses_and_counts_as_left() {
        let json = JOIN_JSON.replace(r#""membership": "join""#, r#""membership": "lurking""#);
        let content: RawStickyEventContent =
            serde_json::from_str(&json).expect("unknown membership must still parse");
        assert_eq!(
            content.member.membership,
            Some(Membership::Unknown("lurking".to_owned()))
        );
        assert!(matches!(
            event(content).try_into_call_membership_event().unwrap(),
            CallMembershipEvent::Left(_)
        ));
    }

    /// MSC4143 makes `application` REQUIRED on a joining member event.
    #[test]
    fn join_without_application_is_left() {
        let json = JOIN_JSON.replace(
            r#""application": { "type": "m.call", "m.call.voice_only": true },"#,
            "",
        );
        assert!(matches!(parse(&json), CallMembershipEvent::Left(_)));
    }

    #[test]
    fn leave_reason_parses_generic_and_custom_codes() {
        let leave = |code: &str| {
            let json = format!(
                r#"{{ "slot_id": "m.call#ROOM",
                      "member": {{ "id": "abc", "membership": "leave" }},
                      "leave_reason": {{ "code": "{code}", "reason": "bye" }},
                      "msc4354_sticky_key": "abc" }}"#
            );
            match parse(&json) {
                CallMembershipEvent::Left(left) => left.leave_reason.expect("leave_reason"),
                CallMembershipEvent::Joined(_) => panic!("expected left"),
            }
        };

        assert_eq!(leave("slot_closed").code, LeaveCode::SlotClosed);
        assert_eq!(leave("delayed_leave").code, LeaveCode::DelayedLeave);
        assert_eq!(leave("leave").reason.as_deref(), Some("bye"));
        // Transport- and application-defined codes survive as-is.
        assert_eq!(
            leave("ice_failed").code,
            LeaveCode::Other("ice_failed".to_owned())
        );
    }

    /// A sticky removal is a leave whatever the content says, since the entry is
    /// no longer live.
    #[test]
    fn removal_of_join_content_is_left() {
        let content: RawStickyEventContent = serde_json::from_str(JOIN_JSON).unwrap();
        assert!(matches!(
            event(content).try_into_left_membership_event().unwrap(),
            CallMembershipEvent::Left(_)
        ));
    }

    /// What we send must be what we can read back.
    #[test]
    fn built_join_content_round_trips() {
        let built = RawStickyEventContent::for_join(
            "m.call#ROOM".to_owned(),
            "xyzABCDEF0123".to_owned(),
            "m.call".to_owned(),
            MemberTransports::publishing(RawRtcTransport {
                transport_type: "livekit".to_owned(),
                extra_fields: Default::default(),
            }),
        );

        let json = serde_json::to_value(&built).unwrap();
        assert_eq!(json.pointer("/member/membership").unwrap(), "join");
        assert_eq!(
            json.pointer("/msc4354_sticky_key").unwrap(),
            "xyzABCDEF0123"
        );

        let parsed: RawStickyEventContent = serde_json::from_value(json).unwrap();
        assert!(parsed.member.is_join());
        assert_eq!(
            parsed.transports.unwrap().can_subscribe,
            vec!["livekit".to_owned()]
        );
    }
}
