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

//! Pre-2026 Element Call interoperability, exposed to FFI hosts.
//!
//! The translation itself lives in [`matrix_rtc_bridge::compat`] and its
//! host-agnostic funnels in [`matrix_rtc_bridge::compat::ingest`], shared with
//! the wasm binding and the Rust-native path; nothing here re-implements a
//! dialect. What this module owns is the *uniffi* binding: an FFI-shaped mode
//! enum and the JSON-string record shapes uniffi can carry.
//!
//! # Why the host cannot do this itself
//!
//! [`crate::StickyEvent`] is a typed record: the host parses an `m.rtc.member`
//! content and hands over the fields. That works for a spec-current peer and
//! cannot work for a legacy one — a pre-2026 content states its transports in a
//! different place, its membership nowhere at all, and a pre-sticky one is not
//! even a sticky event. Re-implementing those rules in Kotlin and Swift would be
//! more copies of the trickiest code in this repository, so the raw content
//! crosses the boundary instead ([`RawMemberEvent`],
//! [`LegacyStateMemberEvent`]) and Rust does the parsing.
//!
//! # What a host must do differently in each mode
//!
//! - [`FfiElementCallCompat::Off`] — nothing. The existing typed ingestion and
//!   `m.rtc.member` sends are exactly right.
//! - [`FfiElementCallCompat::StickyEvents`] — feed membership through
//!   [`RtcSessionManagerHandle::set_current_membership`], and feed
//!   `io.element.call.encryption_keys` to-device messages through
//!   [`RtcSessionManagerHandle::receive_legacy_encryption_key`]. Both are needed
//!   to *read* that generation; the outbound half needs no host change.
//! - [`FfiElementCallCompat::StateEvents`] — the above, plus: pass the room's
//!   `org.matrix.msc3401.call.member` **state** as the second argument of
//!   `set_current_membership`, and implement
//!   [`CommandSenderCallback::send_delayed_state_event`](crate::CommandSenderCallback::send_delayed_state_event).
//!
//! Slots need no host handling in any mode. That generation predates
//! `m.rtc.slot`, so its rooms contain none and the truthful "no slots" a host
//! feeds would resolve the session closed and project out every member, us
//! included. Joining in [`FfiElementCallCompat::StateEvents`] forgets whatever
//! slot state was already supplied for the room (slot state usually arrives with
//! sync, before the user joins) and
//! [`RtcSessionManagerHandle::on_room_slots_received`] ignores later updates for
//! it, leaving the open-slot condition unenforced — "unknowable", which is the
//! honest answer, rather than "closed".
//!
//! The 2025 dialect is different, and deliberately: those rooms are otherwise
//! spec-shaped, a slot in one is meaningful, and the condition stays enforced. In
//! practice Element Call of that vintage publishes no `m.rtc.slot` either, so a
//! host that feeds slot state needs someone to open the slot — the native tools
//! do it themselves (`--open-slot`). A host that never calls
//! [`RtcSessionManagerHandle::on_room_slots_received`] at all is unaffected: the
//! condition is unenforced until it does.
//!
//! [`RtcSessionManagerHandle::set_current_membership`]: crate::RtcSessionManagerHandle::set_current_membership
//! [`RtcSessionManagerHandle::receive_legacy_encryption_key`]: crate::RtcSessionManagerHandle::receive_legacy_encryption_key
//! [`RtcSessionManagerHandle::on_room_slots_received`]: crate::RtcSessionManagerHandle::on_room_slots_received

use matrix_rtc_bridge::compat::ingest;
use matrix_rtc_bridge::compat::{ElementCallCompat, OutboundDialect, element_call};
use matrix_rtc_core::RawStickyEvent;
use serde_json::Value;

/// Which MatrixRTC generation a session speaks, for interoperating with Element
/// Call builds that predate the 2026 MSC4143 rewrite.
///
/// Chosen per join ([`FfiJoinSessionParams::element_call_compat`]) and remembered
/// for the room, because it decides more than the wire format of one event: the
/// `member.id` we join with, how an inbound media key is bound to a membership,
/// the SFU participant identity, and which authorisation-service endpoint mints
/// our token. Those must agree or the call connects and nothing decrypts.
///
/// Scaffolding, and meant to be deleted once Element Call catches up. See
/// [`matrix_rtc_bridge::compat`].
///
/// [`FfiJoinSessionParams::element_call_compat`]: crate::FfiJoinSessionParams::element_call_compat
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum FfiElementCallCompat {
    /// Current MSC4143 + MSC4354 only. The default, and the only mode that
    /// interoperates with spec-current peers.
    #[default]
    Off,
    /// Element Call as of 2025: MSC4354 sticky events carrying the pre-2026
    /// field names alongside the spec ones.
    ///
    /// Joins stay MSC4143-valid, so a spec-current peer still reads us. Leaves
    /// and media keys cannot be additive — a leave becomes the legacy
    /// bare-sticky-key content and keys go out as
    /// `io.element.call.encryption_keys` *instead of* the spec type — so in this
    /// mode keys are exchanged with legacy peers and not with spec-current ones.
    StickyEvents,
    /// Element Call before MSC4354: membership as `org.matrix.msc3401.call.member`
    /// **room state**, plain `{user}:{device}` SFU identities, and the
    /// pre-MSC4195 `/sfu/get` token endpoint.
    ///
    /// Nothing about this mode is additive: a call joined this way is visible to
    /// that generation of Element Call and to nobody else.
    StateEvents,
}

impl From<FfiElementCallCompat> for ElementCallCompat {
    fn from(value: FfiElementCallCompat) -> Self {
        match value {
            FfiElementCallCompat::Off => Self::Off,
            FfiElementCallCompat::StickyEvents => Self::StickyEvents,
            FfiElementCallCompat::StateEvents => Self::StateEvents,
        }
    }
}

/// An absent mode is [`ElementCallCompat::Off`]: hosts that predate this option,
/// and every host not talking to Element Call, leave the field unset.
pub(crate) fn resolve(compat: Option<FfiElementCallCompat>) -> ElementCallCompat {
    compat.unwrap_or_default().into()
}

/// See [`ingest::outbound_dialect`].
pub(crate) fn outbound_dialect(
    compat: ElementCallCompat,
    user_id: &str,
    device_id: &str,
    room_id: &str,
    slot_id: &str,
) -> OutboundDialect {
    ingest::outbound_dialect(compat, user_id, device_id, room_id, slot_id)
}

/// See [`ingest::member_id`].
pub(crate) fn member_id(compat: ElementCallCompat, user_id: &str, device_id: &str) -> String {
    ingest::member_id(compat, user_id, device_id)
}

/// One `m.rtc.member` sticky event, with its content as raw JSON.
///
/// The uniffi carrier for [`ingest::RawMemberEventIn`]; see there for the
/// semantics of every field.
#[derive(Clone, Debug, uniffi::Record)]
pub struct RawMemberEvent {
    /// The event's id. Element Call relates reactions and the raised hand to
    /// it; a member fed without one cannot be reacted for. Supply it.
    #[uniffi(default = None)]
    pub event_id: Option<String>,
    /// Sender user ID of the event.
    pub sender: String,
    /// Device that sent the event, from its decryption metadata.
    ///
    /// Ranked above the device a legacy content merely claims, and the only one
    /// that satisfies MSC4143's encrypted-room rule.
    pub sender_device_id: Option<String>,
    /// Whether the event arrived encrypted. `None` if the host did not say,
    /// which is not the same as `false` — that would drop the member in an
    /// encrypted room.
    pub was_encrypted: Option<bool>,
    /// The wire event type, e.g. `org.matrix.msc4143.rtc.member`.
    pub event_type: String,
    /// The event's whole `content` object as JSON.
    pub content_json: String,
}

impl RawMemberEvent {
    /// Parse the JSON-string carrier into the shared ingest shape, or explain
    /// in the log why the member will not appear.
    fn into_ingest(self, room_id: &str) -> Option<ingest::RawMemberEventIn> {
        let content: Value = serde_json::from_str(&self.content_json)
            .inspect_err(|error| {
                log::warn!(
                    "[{room_id}] ignoring a {} from {} whose content is not JSON: {error}. That \
                     member will not appear in the call.",
                    self.event_type,
                    self.sender,
                );
            })
            .ok()?;
        Some(ingest::RawMemberEventIn {
            event_id: self.event_id,
            sender: self.sender,
            sender_device_id: self.sender_device_id,
            was_encrypted: self.was_encrypted,
            event_type: self.event_type,
            content,
        })
    }
}

/// One `org.matrix.msc3401.call.member` **room state** event: a membership from
/// the Element Call generation that predates MSC4354.
///
/// The uniffi carrier for [`ingest::LegacyStateMemberEventIn`]; see there for
/// the semantics of every field. Only fed in
/// [`FfiElementCallCompat::StateEvents`] mode.
#[derive(Clone, Debug, uniffi::Record)]
pub struct LegacyStateMemberEvent {
    /// The event's id, carried through to the translated membership so
    /// reactions can relate to it. Supply it.
    #[uniffi(default = None)]
    pub event_id: Option<String>,
    /// The event's `sender`. Homeserver-authenticated, and the only trustworthy
    /// identity in the whole event.
    pub sender: String,
    /// The event's `state_key`.
    pub state_key: String,
    /// The event's `origin_server_ts`. Load-bearing, not decoration: it is the
    /// deadline base for a content with no `created_ts`, and the clamp for one
    /// that states an implausible future `created_ts`. A membership fed with `0`
    /// here is read as long expired and its member never appears.
    pub origin_server_ts: u64,
    /// The event's whole `content` object as JSON. An empty object (`{}`) is
    /// this generation's leave.
    pub content_json: String,
}

impl LegacyStateMemberEvent {
    fn into_ingest(self, room_id: &str) -> Option<ingest::LegacyStateMemberEventIn> {
        let content: Value = serde_json::from_str(&self.content_json)
            .inspect_err(|error| {
                log::warn!(
                    "[{room_id}] ignoring a {} from {} ({}) whose content is not JSON: {error}",
                    matrix_rtc_bridge::compat::STATE_MEMBER_EVENT_TYPE,
                    self.sender,
                    self.state_key,
                );
            })
            .ok()?;
        Some(ingest::LegacyStateMemberEventIn {
            event_id: self.event_id,
            sender: self.sender,
            state_key: self.state_key,
            origin_server_ts: self.origin_server_ts,
            content,
        })
    }
}

/// See [`ingest::merge_current_membership`].
pub(crate) fn merge_current_membership(
    room_id: &str,
    member_events: Vec<RawMemberEvent>,
    legacy_state_events: Vec<LegacyStateMemberEvent>,
) -> Vec<RawStickyEvent> {
    ingest::merge_current_membership(
        room_id,
        member_events
            .into_iter()
            .filter_map(|event| event.into_ingest(room_id))
            .collect(),
        legacy_state_events
            .into_iter()
            .filter_map(|event| event.into_ingest(room_id))
            .collect(),
    )
}

/// See [`ingest::parse_legacy_key`].
pub(crate) fn parse_legacy_key(
    compat: ElementCallCompat,
    sender: &str,
    sender_device_id: Option<&str>,
    content: &Value,
) -> Option<element_call::LegacyKeyMessage> {
    ingest::parse_legacy_key(compat, sender, sender_device_id, content)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The dialect logic itself is tested where it lives
    /// (`matrix_rtc_bridge::compat::ingest`); what is this crate's own is the
    /// JSON-string carrier, so pin that a bad string drops the one event, not
    /// the batch.
    #[test]
    fn a_non_json_content_drops_the_event_not_the_batch() {
        let good = RawMemberEvent {
            event_id: None,
            sender: "@alice:example.org".to_owned(),
            sender_device_id: Some("ALICEDEVICE".to_owned()),
            was_encrypted: Some(true),
            event_type: "org.matrix.msc4143.rtc.member".to_owned(),
            content_json: serde_json::json!({
                "slot_id": "m.call#ROOM",
                "msc4354_sticky_key": "MEMBER",
                "application": { "type": "m.call" },
                "member": { "id": "MEMBER", "membership": "join" },
            })
            .to_string(),
        };
        let bad = RawMemberEvent {
            content_json: "not json".to_owned(),
            ..good.clone()
        };

        let merged = merge_current_membership("!room:example.org", vec![bad, good], vec![]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].content.sticky_key, "MEMBER");
    }
}
