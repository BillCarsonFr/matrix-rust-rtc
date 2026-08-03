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

//! MatrixRTC slots (`m.rtc.slot`).
//!
//! A slot is the room-state half of MatrixRTC: it says which application may run
//! at a given slot id and whether that slot is currently open. Membership is
//! only meaningful against an open slot, so this module's output feeds the
//! MSC4143 join conditions applied in [`crate::session`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::session::ApplicationInfo;

/// Event type for MatrixRTC slots (MSC4143).
///
/// This is the stable id. The core never rewrites it: translating to whatever
/// id the homeserver actually speaks (today
/// `org.matrix.msc4143.rtc.slot`) is a host-layer concern, and only the
/// `matrix-sdk`-backed bridge does it — the wasm and FFI bindings pass event
/// type strings through untouched, so those hosts choose their own id.
pub const SLOT_EVENT_TYPE: &str = "m.rtc.slot";

/// MSC4143 `content.status` of an `m.rtc.slot` event.
///
/// Unknown values parse into [`SlotStatus::Unknown`] instead of failing, and
/// resolve to a closed slot: a status this client does not understand is not
/// one it may treat as open.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatus {
    /// The slot is open for members to join.
    Open,
    /// The slot is closed.
    Closed,
    /// A status this client does not understand; treated as closed.
    #[serde(untagged)]
    Unknown(String),
}

/// Whether the room hosting a slot is end-to-end encrypted.
///
/// MSC4143 ties RTC encryption to room encryption in both directions, so this
/// is an input to resolving a slot. `Unknown` means no host has said, in which
/// case neither rule is enforced — the same opt-in shape as the other room-state
/// conditions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RoomEncryption {
    /// No host has reported the room's encryption state.
    #[default]
    Unknown,
    /// The room is end-to-end encrypted, so RTC encryption is REQUIRED.
    Encrypted,
    /// The room is not encrypted, so RTC encryption MUST NOT be used.
    Unencrypted,
}

/// A slot's negotiated RTC encryption mechanism.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncryptionMechanism {
    /// `m.per_member`: every member maintains its own sender key.
    PerMember,
    /// A mechanism this client does not implement.
    Unsupported(String),
}

impl EncryptionMechanism {
    /// Recognises a slot's `encryption.type`.
    pub fn from_type(encryption_type: &str) -> Self {
        match encryption_type {
            "m.per_member" | "org.matrix.msc4143.per_member" => Self::PerMember,
            other => Self::Unsupported(other.to_owned()),
        }
    }

    /// Whether this client can take part in a slot using this mechanism.
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::PerMember)
    }
}

/// MSC4143 `content.encryption` of an `m.rtc.slot` event.
///
/// Its presence is what enables RTC encryption for the slot; its absence
/// disables it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotEncryption {
    /// The encryption mechanism identifier, e.g. `m.per_member`.
    #[serde(rename = "type")]
    pub encryption_type: String,
    /// Mechanism-specific settings.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl SlotEncryption {
    /// The mechanism this object names.
    pub fn mechanism(&self) -> EncryptionMechanism {
        EncryptionMechanism::from_type(&self.encryption_type)
    }
}

/// Content of an `m.rtc.slot` state event, as it appears on the wire.
///
/// Every field is optional here so that a malformed or partial event still
/// parses; [`RawSlotEvent::resolve`] is what decides whether it describes an
/// open slot.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RawSlotEventContent {
    /// The slot's status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SlotStatus>,
    /// The application that may run in this slot.
    #[serde(default, skip_serializing_if = "ApplicationInfo::is_empty")]
    pub application: ApplicationInfo,
    /// The encryption mechanism for this slot, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<SlotEncryption>,
}

/// An `m.rtc.slot` state event received from a host SDK layer.
#[derive(Clone, Debug)]
pub struct RawSlotEvent {
    /// Room the slot belongs to.
    pub room_id: String,
    /// The event's `state_key`, which is the slot id.
    pub slot_id: String,
    /// The event content.
    pub content: RawSlotEventContent,
}

/// An open slot's configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenSlot {
    /// The application type that may run here.
    pub application_type: String,
    /// Application-specific settings from the slot event.
    pub application_extra: BTreeMap<String, serde_json::Value>,
    /// The slot's encryption object as declared, if any.
    pub encryption: Option<SlotEncryption>,
    /// The mechanism members of this slot must use, after applying the room's
    /// encryption state. `None` means RTC data is not encrypted.
    ///
    /// This can differ from `encryption`: a slot that declares a mechanism in an
    /// unencrypted room resolves to `None`, because MSC4143 forbids using RTC
    /// encryption there.
    pub mechanism: Option<EncryptionMechanism>,
}

/// The resolved state of a slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotState {
    /// The slot is open with this configuration.
    Open(OpenSlot),
    /// The slot is closed, or its event does not describe a valid open slot.
    Closed,
}

impl SlotState {
    /// The open configuration, if this slot is open.
    pub fn open(&self) -> Option<&OpenSlot> {
        match self {
            Self::Open(slot) => Some(slot),
            Self::Closed => None,
        }
    }

    /// Whether members may be considered joined to this slot.
    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open(_))
    }
}

impl RawSlotEvent {
    /// Resolves the event into a slot state.
    ///
    /// MSC4143: a slot is open only when `status = "open"` with a valid
    /// application object; "any slot that doesn't fulfill these requirements is
    /// closed". The application `type` must also align with the `state_key`,
    /// which is checked by *forming* the documented `{type}#` prefix and
    /// comparing — the grammar must not be used to parse a slot id apart.
    ///
    /// `room_encryption` decides the encryption half:
    ///
    /// - In an encrypted room, a slot MUST carry an `encryption` object, so one
    ///   without it is closed. A mechanism this client cannot implement also
    ///   closes the slot: encryption is required there, and joining without it
    ///   would break the same requirement.
    /// - In an unencrypted room, RTC encryption MUST NOT be used, so any
    ///   declared mechanism is dropped rather than honoured.
    /// - When the room's state is unknown, neither rule is applied and the
    ///   declared mechanism is taken at face value.
    pub fn resolve(&self, room_encryption: RoomEncryption) -> SlotState {
        if self.content.status != Some(SlotStatus::Open) {
            log::debug!(
                "m.rtc.slot '{}' resolves closed: status={:?}",
                self.slot_id,
                self.content.status,
            );
            return SlotState::Closed;
        }

        let Some(application_type) = self
            .content
            .application
            .application_type
            .as_deref()
            .filter(|t| !t.is_empty())
        else {
            log::warn!(
                "m.rtc.slot '{}' is open but declares no application type; treating it as closed",
                self.slot_id,
            );
            return SlotState::Closed;
        };

        if !self.slot_id.starts_with(&format!("{application_type}#")) {
            log::warn!(
                "m.rtc.slot '{}' declares application type '{}', which does not match its \
                 state key; treating the slot as closed",
                self.slot_id,
                application_type,
            );
            return SlotState::Closed;
        }

        let declared = self
            .content
            .encryption
            .as_ref()
            .map(SlotEncryption::mechanism);

        let mechanism = match room_encryption {
            RoomEncryption::Encrypted => match declared {
                Some(mechanism) if mechanism.is_supported() => Some(mechanism),
                Some(EncryptionMechanism::Unsupported(name)) => {
                    log::warn!(
                        "m.rtc.slot '{}' requires encryption mechanism '{}', which this client \
                         does not implement; treating the slot as closed",
                        self.slot_id,
                        name,
                    );
                    return SlotState::Closed;
                }
                // Unreachable in practice (`is_supported` covers every known
                // mechanism), kept so adding one cannot silently fall through.
                Some(mechanism) => Some(mechanism),
                None => {
                    log::warn!(
                        "m.rtc.slot '{}' is in an encrypted room but declares no encryption \
                         object; treating the slot as closed",
                        self.slot_id,
                    );
                    return SlotState::Closed;
                }
            },
            RoomEncryption::Unencrypted => {
                if declared.is_some() {
                    log::warn!(
                        "m.rtc.slot '{}' declares encryption in an unencrypted room; MSC4143 \
                         forbids RTC encryption there, so it will not be used",
                        self.slot_id,
                    );
                }
                None
            }
            RoomEncryption::Unknown => declared,
        };

        log::debug!(
            "m.rtc.slot '{}' resolves open: application={application_type} \
             room_encryption={room_encryption:?} mechanism={mechanism:?}",
            self.slot_id,
        );

        SlotState::Open(OpenSlot {
            application_type: application_type.to_owned(),
            application_extra: self.content.application.extra.clone(),
            encryption: self.content.encryption.clone(),
            mechanism,
        })
    }
}

impl RawSlotEventContent {
    /// Builds content that opens a slot for `application_type`.
    pub fn for_open(application_type: String, encryption: Option<SlotEncryption>) -> Self {
        Self {
            status: Some(SlotStatus::Open),
            application: ApplicationInfo {
                application_type: Some(application_type),
                ..ApplicationInfo::default()
            },
            encryption,
        }
    }

    /// Builds content that closes a slot.
    ///
    /// MSC4143 allows the `application` / `encryption` objects to be kept on a
    /// closed slot to make reopening easier, so callers may retain them; this
    /// helper emits the minimal form.
    pub fn for_close() -> Self {
        Self {
            status: Some(SlotStatus::Closed),
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(json: &str) -> SlotState {
        slot_in(json, RoomEncryption::Unknown)
    }

    fn slot_in(json: &str, room_encryption: RoomEncryption) -> SlotState {
        RawSlotEvent {
            room_id: "!room:example.org".to_owned(),
            slot_id: "m.call#ROOM".to_owned(),
            content: serde_json::from_str(json).expect("content must parse"),
        }
        .resolve(room_encryption)
    }

    const OPEN_ENCRYPTED: &str = r#"{ "status": "open",
        "application": { "type": "m.call" },
        "encryption": { "type": "m.per_member" } }"#;
    const OPEN_PLAIN: &str = r#"{ "status": "open", "application": { "type": "m.call" } }"#;

    #[test]
    fn open_slot_resolves_with_its_application_and_encryption() {
        let state = slot(
            r#"{ "status": "open",
                 "application": { "type": "m.call", "m.call.voice_only": true },
                 "encryption": { "type": "m.per_member" } }"#,
        );

        let open = state.open().expect("slot should be open");
        assert_eq!(open.application_type, "m.call");
        assert_eq!(
            open.application_extra.get("m.call.voice_only"),
            Some(&serde_json::Value::Bool(true))
        );
        assert_eq!(
            open.encryption.as_ref().map(|e| e.encryption_type.as_str()),
            Some("m.per_member")
        );
    }

    #[test]
    fn absent_encryption_means_encryption_disabled() {
        let state = slot(r#"{ "status": "open", "application": { "type": "m.call" } }"#);
        assert!(state.open().expect("open").encryption.is_none());
    }

    #[test]
    fn closed_status_resolves_closed() {
        assert_eq!(
            slot(r#"{ "status": "closed", "application": { "type": "m.call" } }"#),
            SlotState::Closed
        );
    }

    /// "Any slot that doesn't fulfill these requirements is closed" — an open
    /// status with no application is not a valid open slot.
    #[test]
    fn open_without_application_resolves_closed() {
        assert_eq!(slot(r#"{ "status": "open" }"#), SlotState::Closed);
    }

    /// A status from a future revision must parse, and must not be treated as
    /// open.
    #[test]
    fn unknown_status_parses_and_resolves_closed() {
        assert_eq!(
            slot(r#"{ "status": "draining", "application": { "type": "m.call" } }"#),
            SlotState::Closed
        );
    }

    /// An empty content object is how a slot event looked before `status`
    /// existed; it is not an open slot.
    #[test]
    fn empty_content_resolves_closed() {
        assert_eq!(slot("{}"), SlotState::Closed);
    }

    /// The application type has to agree with the slot id, or a slot could
    /// admit members of an application it does not name.
    #[test]
    fn application_type_must_align_with_the_slot_id() {
        assert_eq!(
            slot(r#"{ "status": "open", "application": { "type": "m.whiteboard" } }"#),
            SlotState::Closed
        );
    }

    /// The check is a prefix comparison, so an application type that merely
    /// starts the same does not count.
    #[test]
    fn application_type_prefix_must_end_at_the_separator() {
        let state = RawSlotEvent {
            room_id: "!room:example.org".to_owned(),
            slot_id: "m.callisto#ROOM".to_owned(),
            content: serde_json::from_str(
                r#"{ "status": "open", "application": { "type": "m.call" } }"#,
            )
            .unwrap(),
        }
        .resolve(RoomEncryption::Unknown);

        assert_eq!(state, SlotState::Closed);
    }

    /// MSC4143: "`m.rtc.slot` events MUST contain an `encryption` object when
    /// sent in an encrypted room ... slot events that violate these conditions
    /// MUST be considered closed".
    #[test]
    fn slot_without_encryption_is_closed_in_an_encrypted_room() {
        assert_eq!(
            slot_in(OPEN_PLAIN, RoomEncryption::Encrypted),
            SlotState::Closed
        );
    }

    #[test]
    fn slot_with_per_member_is_open_in_an_encrypted_room() {
        let state = slot_in(OPEN_ENCRYPTED, RoomEncryption::Encrypted);
        assert_eq!(
            state.open().expect("open").mechanism,
            Some(EncryptionMechanism::PerMember)
        );
    }

    /// Encryption is required here, and we cannot provide it with a mechanism we
    /// do not implement, so taking part at all would break the requirement.
    #[test]
    fn slot_with_unsupported_mechanism_is_closed_in_an_encrypted_room() {
        let json = OPEN_ENCRYPTED.replace("m.per_member", "com.example.quantum");
        assert_eq!(slot_in(&json, RoomEncryption::Encrypted), SlotState::Closed);
    }

    /// MSC4143: "MatrixRTC encryption MUST NOT be used in unencrypted rooms."
    /// The slot stays open; the mechanism is dropped.
    #[test]
    fn declared_encryption_is_not_used_in_an_unencrypted_room() {
        let state = slot_in(OPEN_ENCRYPTED, RoomEncryption::Unencrypted);
        let open = state.open().expect("slot should stay open");
        assert!(open.mechanism.is_none());
        // The declaration is still reported, so callers can see the mismatch.
        assert!(open.encryption.is_some());
    }

    #[test]
    fn plain_slot_in_an_unencrypted_room_is_open_without_encryption() {
        let state = slot_in(OPEN_PLAIN, RoomEncryption::Unencrypted);
        assert!(state.open().expect("open").mechanism.is_none());
    }

    /// Until a host reports the room's encryption state, neither rule applies.
    #[test]
    fn unknown_room_encryption_takes_the_slot_at_face_value() {
        assert!(slot_in(OPEN_PLAIN, RoomEncryption::Unknown).is_open());
        assert_eq!(
            slot_in(OPEN_ENCRYPTED, RoomEncryption::Unknown)
                .open()
                .expect("open")
                .mechanism,
            Some(EncryptionMechanism::PerMember)
        );
    }

    #[test]
    fn unstable_per_member_id_is_recognised() {
        let json = OPEN_ENCRYPTED.replace("m.per_member", "org.matrix.msc4143.per_member");
        assert_eq!(
            slot_in(&json, RoomEncryption::Encrypted)
                .open()
                .expect("open")
                .mechanism,
            Some(EncryptionMechanism::PerMember)
        );
    }

    #[test]
    fn built_open_content_round_trips() {
        let content = RawSlotEventContent::for_open(
            "m.call".to_owned(),
            Some(SlotEncryption {
                encryption_type: "m.per_member".to_owned(),
                extra: BTreeMap::new(),
            }),
        );

        let json = serde_json::to_value(&content).unwrap();
        assert_eq!(json.pointer("/status").unwrap(), "open");
        assert_eq!(json.pointer("/application/type").unwrap(), "m.call");
        assert_eq!(json.pointer("/encryption/type").unwrap(), "m.per_member");

        let parsed: RawSlotEventContent = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.status, Some(SlotStatus::Open));
    }

    #[test]
    fn built_close_content_has_no_application() {
        let json = serde_json::to_value(RawSlotEventContent::for_close()).unwrap();
        assert_eq!(json.pointer("/status").unwrap(), "closed");
        assert!(json.get("application").is_none());
    }
}
