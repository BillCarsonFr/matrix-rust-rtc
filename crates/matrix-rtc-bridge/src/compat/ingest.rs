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

//! Raw-event ingestion for hosts, across every MatrixRTC generation.
//!
//! A host binding (uniffi, wasm) cannot feed a legacy membership through the
//! typed sticky ingestion: a pre-2026 content states its transports in a
//! different place, its membership nowhere at all, and a pre-sticky one is not
//! even a sticky event. So raw content crosses the boundary
//! ([`RawMemberEventIn`], [`LegacyStateMemberEventIn`]) and this module does
//! the parsing — once, here, rather than per binding: re-implementing these
//! rules in Kotlin, Swift, *and* JavaScript would be that many copies of the
//! trickiest code in this repository.
//!
//! The dialect translation itself lives in the sibling modules
//! ([`element_call`], [`element_call_state`]) and is shared verbatim with the
//! Rust-native path; this module owns only the host-shaped funnels around it,
//! and the two places a mode changes an identifier rather than a field
//! ([`member_id`], [`outbound_dialect`]).

use matrix_rtc_core::{EventOrigin, RawStickyEvent, RawStickyEventContent};
use serde_json::Value;

use super::{
    ElementCallCompat, ElementCallDialect, ElementCallStateDialect, MemberContent, OutboundDialect,
    element_call, element_call_state,
};

/// One `m.rtc.member` sticky event, with its content raw.
///
/// The raw counterpart of a binding's typed sticky record, and the only shape
/// that can carry a pre-2026 membership: the legacy normalisation runs on the
/// JSON, before anything typed sees it. Safe for spec-current events too —
/// every normalisation rule fires only where the modern field is absent and
/// its legacy counterpart is present — so a host that has both formats in one
/// room feeds them all through here.
#[derive(Clone, Debug)]
pub struct RawMemberEventIn {
    /// Sender user ID of the event.
    pub sender: String,
    /// Device that sent the event, from its decryption metadata.
    ///
    /// Ranked above the device a legacy content merely claims, and the only
    /// one that satisfies MSC4143's encrypted-room rule.
    pub sender_device_id: Option<String>,
    /// Whether the event arrived encrypted. `None` if the host did not say,
    /// which is not the same as `false` — that would drop the member in an
    /// encrypted room.
    pub was_encrypted: Option<bool>,
    /// The wire event type, e.g. `org.matrix.msc4143.rtc.member`.
    pub event_type: String,
    /// The event's whole `content` object.
    pub content: Value,
}

/// One `org.matrix.msc3401.call.member` **room state** event: a membership
/// from the Element Call generation that predates MSC4354.
///
/// Not a sticky event and not typed as one: it has no sticky key, its lifetime
/// is stated in the content rather than enforced by the homeserver, and it is
/// translated into an MSC4143 membership before the core sees it. Only fed in
/// [`ElementCallCompat::StateEvents`] mode.
#[derive(Clone, Debug)]
pub struct LegacyStateMemberEventIn {
    /// The event's `sender`. Homeserver-authenticated, and the only
    /// trustworthy identity in the whole event.
    pub sender: String,
    /// The event's `state_key`.
    pub state_key: String,
    /// The event's `origin_server_ts`. Load-bearing, not decoration: it is the
    /// deadline base for a content with no `created_ts`, and the clamp for one
    /// that states an implausible future `created_ts`. A membership fed with
    /// `0` here is read as long expired and its member never appears.
    pub origin_server_ts: u64,
    /// The event's whole `content` object. An empty object (`{}`) is this
    /// generation's leave.
    pub content: Value,
}

/// Build the outbound dialect for one joined session.
///
/// Everything it needs is known at join time, which is why the mode is a join
/// parameter rather than a manager-wide setting: the sticky dialect re-states
/// our own user and device on the wire, and the state dialect derives a state
/// key and a `livekit_alias` from the room.
pub fn outbound_dialect(
    compat: ElementCallCompat,
    user_id: &str,
    device_id: &str,
    room_id: &str,
    slot_id: &str,
) -> OutboundDialect {
    match compat {
        ElementCallCompat::Off => OutboundDialect::None,
        ElementCallCompat::StickyEvents => {
            OutboundDialect::Sticky(ElementCallDialect::new(user_id, device_id, slot_id))
        }
        ElementCallCompat::StateEvents => OutboundDialect::State(ElementCallStateDialect::new(
            user_id, device_id, room_id, slot_id,
        )),
    }
}

/// The `member.id` to join a session with.
///
/// MSC4143 requires a fresh id per join, and the SDK generates one — except in
/// the pre-sticky mode, where the member id *is* the legacy `membershipID`,
/// which *is* the SFU participant identity, and that generation's
/// authorisation service defines it as `{user}:{device}`. A random id there
/// would leave our own state event, echoed back through sync, failing the
/// core's `SupersededOwnParticipation` check: we would mark ourselves departed
/// on our own join.
pub fn member_id(compat: ElementCallCompat, user_id: &str, device_id: &str) -> String {
    match compat {
        ElementCallCompat::StateEvents => {
            element_call_state::participant_identity(user_id, device_id)
        }
        _ => matrix_rtc_core::generate_member_id(),
    }
}

/// Normalise and parse one raw member event, or explain in the log why it
/// contributes no membership.
///
/// Mirrors [`crate::sdk`]'s snapshot handling, which does the same job for the
/// Rust-native path — including the origin ranking, which is the subtle half.
pub fn to_core_member_event(room_id: &str, event: RawMemberEventIn) -> Option<RawStickyEvent> {
    let mut value = event.content;

    if element_call::normalize_member_content(&mut value) == MemberContent::BareLeave {
        // A pre-2026 leave: the content is a sticky key and nothing else, so
        // there is no slot to file it under. Dropping it *is* the leave — the
        // membership set is applied whole, and a member who contributes no event
        // is a member who is gone.
        log::debug!(
            "[{room_id}] pre-2026 leave from {}; member is gone",
            event.sender
        );
        return None;
    }

    let claimed_device = element_call::claimed_device_id(&value);

    let content: RawStickyEventContent = match serde_json::from_value(value) {
        Ok(content) => content,
        Err(error) => {
            log::warn!(
                "[{room_id}] ignoring an unparseable {} from {}: {error}. That member will not \
                 appear in the call.",
                event.event_type,
                event.sender,
            );
            return None;
        }
    };

    // A device the event merely *claims* is the last resort, never a preference:
    // it is consulted only where decryption produced none, and it is what makes a
    // pre-2026 Element Call peer usable at all — that client runs as a widget,
    // whose API gives it no decryption metadata, so a self-asserted device is the
    // only one it can either state or read. See `EventOrigin::Claimed`.
    let claimed = || claimed_device.clone().map(EventOrigin::claimed);
    let origin = match event.was_encrypted {
        Some(true) => match event.sender_device_id {
            Some(device_id) => EventOrigin::encrypted(Some(device_id)),
            // Olm messages carry the sender's device keys, so a decrypted event
            // should always name one. Worth saying out loud: without a device
            // this member's media keys are rejected in both directions.
            None => {
                log::warn!(
                    "[{room_id}] a decrypted {} from {} resolved to no sending device",
                    event.event_type,
                    event.sender,
                );
                claimed().unwrap_or_else(|| EventOrigin::encrypted(None))
            }
        },
        Some(false) => claimed().unwrap_or(EventOrigin::Cleartext),
        None => claimed().unwrap_or(EventOrigin::Unknown),
    };

    Some(RawStickyEvent {
        room_id: room_id.to_owned(),
        sender: event.sender,
        origin,
        event_type: event.event_type,
        content,
    })
}

/// Translate a room's whole `org.matrix.msc3401.call.member` state into core
/// memberships.
///
/// Takes the batch rather than one event: expiry needs one clock reading for the
/// whole set, and `focus_selection: "oldest_membership"` resolves a member's SFU
/// against the oldest membership of the same slot.
pub fn to_core_state_memberships(
    room_id: &str,
    events: Vec<LegacyStateMemberEventIn>,
) -> Vec<RawStickyEvent> {
    let parsed: Vec<element_call_state::StateMemberEvent> = events
        .into_iter()
        .filter_map(|event| {
            let content = serde_json::from_value(event.content)
                .inspect_err(|error| {
                    log::warn!(
                        "[{room_id}] ignoring a {} from {} ({}) whose content does not parse: \
                         {error}",
                        element_call_state::STATE_MEMBER_EVENT_TYPE,
                        event.sender,
                        event.state_key,
                    );
                })
                .ok()?;
            Some(element_call_state::StateMemberEvent {
                sender: event.sender,
                state_key: event.state_key,
                origin_server_ts: event.origin_server_ts,
                content,
            })
        })
        .collect();

    let translated =
        element_call_state::translate_state_memberships(&parsed, element_call_state::now_ms());
    log::debug!(
        "[{room_id}] {} {} state event(s) translated to {} live membership(s)",
        parsed.len(),
        element_call_state::STATE_MEMBER_EVENT_TYPE,
        translated.len(),
    );

    translated
        .into_iter()
        .filter_map(|membership| {
            let content: RawStickyEventContent = match serde_json::from_value(membership.content) {
                Ok(content) => content,
                Err(error) => {
                    log::warn!(
                        "[{room_id}] a translated pre-sticky membership from {} does not parse as \
                         MSC4143 content ({error}). That is a bug in the translation, not in the \
                         peer; that member will not appear in the call.",
                        membership.sender,
                    );
                    return None;
                }
            };
            Some(RawStickyEvent {
                room_id: room_id.to_owned(),
                sender: membership.sender,
                // The self-asserted device, which is all such a peer can state
                // and all we can read — a state event carries no decryption
                // metadata. Ranked below an authenticated device everywhere it
                // matters (it never satisfies the encrypted-room rule), but it is
                // what lets a media key travel in either direction at all.
                origin: EventOrigin::claimed(membership.claimed_device_id),
                // Not the type that was on the wire. What the core accepts is an
                // MSC4143 membership, and after the translation that is exactly
                // what this is; carrying the MSC3401 type here would only make
                // the core reject it.
                event_type: "m.rtc.member".to_owned(),
                content,
            })
        })
        .collect()
}

/// A room's complete current membership from both sources, ready for
/// `set_current_sticky_state`.
///
/// Both go in **one** call because both are replaced by it: fed separately,
/// each would wipe the other's members and the roster would flicker between
/// the two halves of the call. A sticky membership wins over a state one with
/// the same key — an Element Call build mid-transition can write both, and the
/// same human twice in the roster means two receive streams and two key
/// exchanges for one peer.
pub fn merge_current_membership(
    room_id: &str,
    member_events: Vec<RawMemberEventIn>,
    legacy_state_events: Vec<LegacyStateMemberEventIn>,
) -> Vec<RawStickyEvent> {
    let mut current: Vec<RawStickyEvent> = member_events
        .into_iter()
        .filter_map(|event| to_core_member_event(room_id, event))
        .collect();
    let from_sticky = current.len();

    if !legacy_state_events.is_empty() {
        let seen: Vec<String> = current
            .iter()
            .map(|event| event.content.sticky_key.clone())
            .collect();
        current.extend(
            to_core_state_memberships(room_id, legacy_state_events)
                .into_iter()
                .filter(|event| !seen.contains(&event.content.sticky_key)),
        );
    }

    log::debug!(
        "[{room_id}] {from_sticky} sticky + {} pre-sticky membership(s) applied",
        current.len() - from_sticky,
    );
    current
}

/// A media key lifted out of a legacy `io.element.call.encryption_keys`
/// to-device message, bound to the membership the current mode files it under.
///
/// Returns `None` when the message is missing a field the core needs, or when
/// there is no device to bind it to — both mean the key cannot be used, and both
/// are logged where they happen.
pub fn parse_legacy_key(
    compat: ElementCallCompat,
    sender: &str,
    sender_device_id: Option<&str>,
    content: &Value,
) -> Option<element_call::LegacyKeyMessage> {
    let mut key = element_call::parse_key_message(sender, content)?;

    // In the pre-sticky generation the `member.id` a key message carries is
    // Element Call's own per-session UUID, and it appears in *no* field of the
    // membership state event — so binding the key by it can never match, and the
    // key sits buffered while that peer's media stays undecryptable.
    //
    // Everything in that generation is keyed on `{user}:{device}` — the SFU
    // identity, our translated `member_id`, the `membershipID` — so bind on that
    // instead. The device comes from Olm decryption where possible, so both
    // halves are authenticated rather than self-asserted.
    if compat == ElementCallCompat::StateEvents {
        let device_id = sender_device_id
            .map(str::to_owned)
            .or_else(|| element_call::claimed_key_device_id(content))?;
        key.member_id = element_call_state::participant_identity(sender, &device_id);
    }

    Some(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(content: serde_json::Value) -> RawMemberEventIn {
        RawMemberEventIn {
            sender: "@alice:example.org".to_owned(),
            sender_device_id: Some("ALICEDEVICE".to_owned()),
            was_encrypted: Some(true),
            event_type: "org.matrix.msc4143.rtc.member".to_owned(),
            content,
        }
    }

    #[test]
    fn normalises_a_pre_2026_membership() {
        let event = raw(serde_json::json!({
            "slot_id": "m.call#ROOM",
            "msc4354_sticky_key": "MEMBER",
            "application": { "type": "m.call" },
            "member": { "id": "MEMBER", "user_id": "@alice:example.org", "device_id": "ALICEDEVICE" },
            "rtc_transports": [{ "type": "livekit", "livekit_service_url": "https://sfu" }],
        }));

        let converted = to_core_member_event("!room:example.org", event).expect("a membership");
        assert_eq!(converted.content.member.id.as_deref(), Some("MEMBER"));
        // Inferred: that generation has no `membership` field at all.
        assert_eq!(
            converted.content.member.membership,
            Some(matrix_rtc_core::Membership::Join),
        );
        // Lifted out of the flat `rtc_transports` array.
        let transports = converted.content.transports.expect("transports");
        assert_eq!(transports.published.len(), 1);
        assert_eq!(transports.can_subscribe, vec!["livekit".to_owned()]);
    }

    #[test]
    fn drops_a_pre_2026_leave() {
        let event = raw(serde_json::json!({ "msc4354_sticky_key": "MEMBER" }));
        assert!(to_core_member_event("!room:example.org", event).is_none());
    }

    #[test]
    fn prefers_the_decrypted_device_over_the_claimed_one() {
        let event = raw(serde_json::json!({
            "slot_id": "m.call#ROOM",
            "msc4354_sticky_key": "MEMBER",
            "application": { "type": "m.call" },
            "member": { "id": "MEMBER", "device_id": "CLAIMED" },
        }));

        let converted = to_core_member_event("!room:example.org", event).expect("a membership");
        assert_eq!(converted.origin.sender_device_id(), Some("ALICEDEVICE"));
        assert_eq!(converted.origin.was_encrypted(), Some(true));
    }

    #[test]
    fn falls_back_to_the_claimed_device_when_nothing_decrypted() {
        let mut event = raw(serde_json::json!({
            "slot_id": "m.call#ROOM",
            "msc4354_sticky_key": "MEMBER",
            "application": { "type": "m.call" },
            "member": { "id": "MEMBER", "device_id": "CLAIMED" },
        }));
        event.was_encrypted = None;
        event.sender_device_id = None;

        let converted = to_core_member_event("!room:example.org", event).expect("a membership");
        assert_eq!(converted.origin.sender_device_id(), Some("CLAIMED"));
        // A claim says nothing about encryption, so it never satisfies the
        // encrypted-room rule.
        assert_eq!(converted.origin.was_encrypted(), None);
    }

    #[test]
    fn translates_pre_sticky_room_state() {
        let now = element_call_state::now_ms();
        let events = vec![LegacyStateMemberEventIn {
            sender: "@alice:example.org".to_owned(),
            state_key: "_@alice:example.org_ALICEDEVICE_m.call".to_owned(),
            origin_server_ts: now,
            content: serde_json::json!({
                "application": "m.call",
                "call_id": "",
                "device_id": "ALICEDEVICE",
                "expires": 14_400_000_u64,
                "membershipID": "@alice:example.org:ALICEDEVICE",
                "foci_preferred": [{
                    "type": "livekit",
                    "livekit_service_url": "https://sfu",
                    "livekit_alias": "!room:example.org",
                }],
            }),
        }];

        let converted = to_core_state_memberships("!room:example.org", events);
        assert_eq!(converted.len(), 1);
        assert_eq!(converted[0].content.slot_id, "m.call#ROOM");
        assert_eq!(
            converted[0].content.member.id.as_deref(),
            Some("@alice:example.org:ALICEDEVICE"),
        );
        assert_eq!(converted[0].origin.sender_device_id(), Some("ALICEDEVICE"));
    }

    #[test]
    fn drops_an_expired_pre_sticky_membership() {
        let events = vec![LegacyStateMemberEventIn {
            sender: "@alice:example.org".to_owned(),
            state_key: "_@alice:example.org_ALICEDEVICE_m.call".to_owned(),
            origin_server_ts: 1,
            content: serde_json::json!({
                "application": "m.call",
                "device_id": "ALICEDEVICE",
                "expires": 1000_u64,
            }),
        }];

        assert!(to_core_state_memberships("!room:example.org", events).is_empty());
    }

    /// The merge is replace-not-merge per source, sticky winning on a shared
    /// key: the same human twice in the roster means two receive streams and
    /// two key exchanges for one peer.
    #[test]
    fn a_sticky_membership_wins_over_the_state_one_with_the_same_key() {
        let now = element_call_state::now_ms();
        let sticky = raw(serde_json::json!({
            "slot_id": "m.call#ROOM",
            "msc4354_sticky_key": "@alice:example.org:ALICEDEVICE",
            "application": { "type": "m.call" },
            "member": { "id": "@alice:example.org:ALICEDEVICE" },
        }));
        let state = LegacyStateMemberEventIn {
            sender: "@alice:example.org".to_owned(),
            state_key: "_@alice:example.org_ALICEDEVICE_m.call".to_owned(),
            origin_server_ts: now,
            content: serde_json::json!({
                "application": "m.call",
                "device_id": "ALICEDEVICE",
                "expires": 14_400_000_u64,
                "membershipID": "@alice:example.org:ALICEDEVICE",
            }),
        };

        let merged = merge_current_membership("!room:example.org", vec![sticky], vec![state]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].origin.was_encrypted(), Some(true));
    }

    #[test]
    fn binds_a_pre_sticky_key_by_user_and_device() {
        let content = serde_json::json!({
            "keys": { "index": 0, "key": "AAAA" },
            // The per-session UUID that matches no membership field.
            "member": { "id": "ef8adf45-0000-0000-0000-000000000000" },
            "device_id": "ALICEDEVICE",
            "room_id": "!room:example.org",
        });

        let sticky = parse_legacy_key(
            ElementCallCompat::StickyEvents,
            "@alice:example.org",
            Some("ALICEDEVICE"),
            &content,
        )
        .expect("a key");
        assert_eq!(sticky.member_id, "ef8adf45-0000-0000-0000-000000000000");

        let state = parse_legacy_key(
            ElementCallCompat::StateEvents,
            "@alice:example.org",
            Some("ALICEDEVICE"),
            &content,
        )
        .expect("a key");
        assert_eq!(state.member_id, "@alice:example.org:ALICEDEVICE");
        assert_eq!(state.key_index, 0);
        assert_eq!(state.room_id, "!room:example.org");
    }

    #[test]
    fn refuses_a_pre_sticky_key_with_no_device_to_bind_it_to() {
        let content = serde_json::json!({
            "keys": { "index": 0, "key": "AAAA" },
            "member": { "id": "ef8adf45" },
            "room_id": "!room:example.org",
        });

        assert!(
            parse_legacy_key(
                ElementCallCompat::StateEvents,
                "@alice:example.org",
                None,
                &content,
            )
            .is_none()
        );
    }
}
