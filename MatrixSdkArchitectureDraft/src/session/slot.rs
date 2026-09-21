//! `m.rtc.slot` parsing and resolution (MSC4143).
//!
//! A slot is the room-state half of MatrixRTC: it says which application may
//! run at a given slot id and whether that slot is currently open.
//! Membership is only meaningful against an open slot, so this module's
//! output feeds the join conditions applied in `state.rs`.

use super::{EncryptionMechanism, OpenSlot, SlotEncryption, SlotState};
use serde::Deserialize;
use serde_json::Value;

impl EncryptionMechanism {
    /// Recognises a slot's `encryption.type`.
    pub fn from_type(mechanism_type: &str) -> Self {
        match mechanism_type {
            "m.per_member" | "org.matrix.msc4143.per_member" => Self::PerMember,
            other => Self::Unsupported(other.to_owned()),
        }
    }

    /// Whether this client can take part in a slot using this mechanism.
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::PerMember)
    }
}

impl SlotState {
    pub fn open(&self) -> Option<&OpenSlot> {
        match self {
            Self::Open(slot) => Some(slot),
            Self::Closed => None,
        }
    }

    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open(_))
    }
}

/// MSC4143 `content.status`. Unknown values parse into `Unknown` instead of
/// failing, and resolve to a closed slot.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SlotStatus {
    Open,
    Closed,
    #[serde(untagged)]
    Unknown(String),
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
struct SlotApplication {
    #[serde(rename = "type", default)]
    application_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
struct SlotEncryptionContent {
    #[serde(rename = "type")]
    mechanism_type: String,
}

/// Content of an `m.rtc.slot` state event, as it appears on the wire. Every
/// field is optional so that a partial event still parses; [`RawSlot::resolve`]
/// decides whether it describes an open slot.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
struct RawSlotContent {
    #[serde(default)]
    status: Option<SlotStatus>,
    #[serde(default)]
    application: Option<SlotApplication>,
    #[serde(default)]
    encryption: Option<SlotEncryptionContent>,
}

/// One `m.rtc.slot` state event, kept unresolved because resolving it also
/// depends on the room's encryption state, which can arrive later or change.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawSlot {
    pub slot_id: String,
    content: RawSlotContent,
}

impl RawSlot {
    /// Parse a slot event's content. Malformed content (wrong field types)
    /// is logged and treated as empty, which resolves closed.
    pub(crate) fn parse(slot_id: &str, content: &Value) -> Self {
        let content = match serde_json::from_value::<RawSlotContent>(content.clone()) {
            Ok(content) => content,
            Err(error) => {
                log::warn!(
                    "m.rtc.slot '{slot_id}' has malformed content ({error}); treating it as closed"
                );
                RawSlotContent::default()
            }
        };
        Self {
            slot_id: slot_id.to_owned(),
            content,
        }
    }

    /// Resolve into a slot state.
    ///
    /// MSC4143: a slot is open only when `status = "open"` with a valid
    /// application object whose `type` agrees with the state key (checked by
    /// *forming* the `{type}#` prefix — the grammar is never used to parse a
    /// slot id apart); "any slot that doesn't fulfill these requirements is
    /// closed".
    ///
    /// `room_encryption` decides the encryption half:
    /// - `Some(true)`: a slot MUST carry an `encryption` object, so one
    ///   without it is closed; a mechanism this client cannot implement also
    ///   closes it (joining without encryption would break the requirement).
    /// - `Some(false)`: RTC encryption MUST NOT be used, so a declared
    ///   mechanism is dropped rather than honoured (still reported).
    /// - `None`: neither rule is applied; the declaration is taken at face
    ///   value.
    pub(crate) fn resolve(&self, room_encryption: Option<bool>) -> SlotState {
        if self.content.status != Some(SlotStatus::Open) {
            log::debug!(
                "m.rtc.slot '{}' resolves closed: status={:?}",
                self.slot_id,
                self.content.status
            );
            return SlotState::Closed;
        }

        let Some(application_type) = self
            .content
            .application
            .as_ref()
            .and_then(|a| a.application_type.as_deref())
            .filter(|t| !t.is_empty())
        else {
            log::warn!(
                "m.rtc.slot '{}' is open but declares no application type; treating it as closed",
                self.slot_id
            );
            return SlotState::Closed;
        };

        if !self.slot_id.starts_with(&format!("{application_type}#")) {
            log::warn!(
                "m.rtc.slot '{}' declares application type '{application_type}', which does not \
                 match its state key; treating the slot as closed",
                self.slot_id
            );
            return SlotState::Closed;
        }

        let declared = self.content.encryption.as_ref().map(|e| SlotEncryption {
            mechanism_type: e.mechanism_type.clone(),
        });
        let declared_mechanism = declared
            .as_ref()
            .map(|e| EncryptionMechanism::from_type(&e.mechanism_type));

        let mechanism = match room_encryption {
            Some(true) => match declared_mechanism {
                Some(mechanism) if mechanism.is_supported() => Some(mechanism),
                Some(EncryptionMechanism::Unsupported(name)) => {
                    log::warn!(
                        "m.rtc.slot '{}' requires encryption mechanism '{name}', which this client \
                         does not implement; treating the slot as closed",
                        self.slot_id
                    );
                    return SlotState::Closed;
                }
                // Unreachable in practice (`is_supported` covers every known
                // mechanism), kept so adding one cannot silently fall through.
                Some(mechanism) => Some(mechanism),
                None => {
                    log::warn!(
                        "m.rtc.slot '{}' is in an encrypted room but declares no encryption object; \
                         treating the slot as closed",
                        self.slot_id
                    );
                    return SlotState::Closed;
                }
            },
            Some(false) => {
                if declared_mechanism.is_some() {
                    log::warn!(
                        "m.rtc.slot '{}' declares encryption in an unencrypted room; MSC4143 forbids \
                         RTC encryption there, so it will not be used",
                        self.slot_id
                    );
                }
                None
            }
            None => declared_mechanism,
        };

        log::debug!(
            "m.rtc.slot '{}' resolves open: application={application_type} \
             room_encryption={room_encryption:?} mechanism={mechanism:?}",
            self.slot_id
        );

        SlotState::Open(OpenSlot {
            application_type: application_type.to_owned(),
            encryption: declared,
            mechanism,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(json: &str) -> SlotState {
        slot_in(json, None)
    }

    fn slot_in(json: &str, room_encryption: Option<bool>) -> SlotState {
        RawSlot::parse("m.call#ROOM", &serde_json::from_str(json).unwrap()).resolve(room_encryption)
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
            open.encryption.as_ref().map(|e| e.mechanism_type.as_str()),
            Some("m.per_member")
        );
        assert_eq!(open.mechanism, Some(EncryptionMechanism::PerMember));
    }

    #[test]
    fn absent_encryption_means_encryption_disabled() {
        let state = slot(OPEN_PLAIN);
        assert!(state.open().expect("open").encryption.is_none());
        assert!(state.open().expect("open").mechanism.is_none());
    }

    #[test]
    fn closed_status_resolves_closed() {
        assert_eq!(
            slot(r#"{ "status": "closed", "application": { "type": "m.call" } }"#),
            SlotState::Closed
        );
    }

    /// An open status with no application is not a valid open slot.
    #[test]
    fn open_without_application_resolves_closed() {
        assert_eq!(slot(r#"{ "status": "open" }"#), SlotState::Closed);
    }

    /// A status from a future revision must parse, and must not be open.
    #[test]
    fn unknown_status_parses_and_resolves_closed() {
        assert_eq!(
            slot(r#"{ "status": "draining", "application": { "type": "m.call" } }"#),
            SlotState::Closed
        );
    }

    #[test]
    fn empty_and_malformed_content_resolve_closed() {
        assert_eq!(slot("{}"), SlotState::Closed);
        assert_eq!(
            slot(r#"{ "status": 5, "application": [] }"#),
            SlotState::Closed
        );
        assert_eq!(slot(r#""a string""#), SlotState::Closed);
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

    /// A prefix comparison, so an application type that merely starts the
    /// same does not count.
    #[test]
    fn application_type_prefix_must_end_at_the_separator() {
        let state = RawSlot::parse(
            "m.callisto#ROOM",
            &serde_json::from_str(OPEN_PLAIN).unwrap(),
        )
        .resolve(None);
        assert_eq!(state, SlotState::Closed);
    }

    /// MSC4143: slots in encrypted rooms MUST carry an `encryption` object.
    #[test]
    fn slot_without_encryption_is_closed_in_an_encrypted_room() {
        assert_eq!(slot_in(OPEN_PLAIN, Some(true)), SlotState::Closed);
    }

    #[test]
    fn slot_with_per_member_is_open_in_an_encrypted_room() {
        let state = slot_in(OPEN_ENCRYPTED, Some(true));
        assert_eq!(
            state.open().expect("open").mechanism,
            Some(EncryptionMechanism::PerMember)
        );
    }

    #[test]
    fn slot_with_unsupported_mechanism_is_closed_in_an_encrypted_room() {
        let json = OPEN_ENCRYPTED.replace("m.per_member", "com.example.quantum");
        assert_eq!(slot_in(&json, Some(true)), SlotState::Closed);
    }

    /// MSC4143: "MatrixRTC encryption MUST NOT be used in unencrypted rooms."
    /// The slot stays open; the mechanism is dropped.
    #[test]
    fn declared_encryption_is_not_used_in_an_unencrypted_room() {
        let state = slot_in(OPEN_ENCRYPTED, Some(false));
        let open = state.open().expect("slot should stay open");
        assert!(open.mechanism.is_none());
        // The declaration is still reported, so callers can see the mismatch.
        assert!(open.encryption.is_some());
    }

    #[test]
    fn plain_slot_in_an_unencrypted_room_is_open_without_encryption() {
        assert!(
            slot_in(OPEN_PLAIN, Some(false))
                .open()
                .expect("open")
                .mechanism
                .is_none()
        );
    }

    /// Until a host reports the room's encryption state, neither rule applies.
    #[test]
    fn unknown_room_encryption_takes_the_slot_at_face_value() {
        assert!(slot_in(OPEN_PLAIN, None).is_open());
        assert_eq!(
            slot_in(OPEN_ENCRYPTED, None)
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
            slot_in(&json, Some(true)).open().expect("open").mechanism,
            Some(EncryptionMechanism::PerMember)
        );
    }
}
