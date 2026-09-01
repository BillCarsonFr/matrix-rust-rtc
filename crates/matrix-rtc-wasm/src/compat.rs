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

//! Pre-2026 Element Call interoperability, exposed to web hosts.
//!
//! The translation lives in [`matrix_rtc_bridge::compat`] and its host-agnostic
//! funnels in [`matrix_rtc_bridge::compat::ingest`], shared with the FFI and
//! the Rust-native path; nothing here re-implements a dialect. This module owns
//! only the JS-shaped carriers — raw event payloads deserialized straight from
//! JS objects — and the mode-string vocabulary
//! (`"off" | "sticky_events" | "state_events"`) the page uses everywhere.
//!
//! What a page must do differently per mode mirrors the FFI
//! (`matrix-rtc-ffi/src/compat.rs` module docs): in `sticky_events`, feed
//! membership through `setCurrentMembership` and legacy
//! `io.element.call.encryption_keys` to-device messages through
//! `receiveLegacyEncryptionKey`; in `state_events`, additionally pass the
//! room's `org.matrix.msc3401.call.member` state as `setCurrentMembership`'s
//! third argument and implement `sendDelayedStateEvent` on the client object.

use matrix_rtc_bridge::compat::{ElementCallCompat, ingest};
use serde::Deserialize;
use serde_json::Value;
use wasm_bindgen::JsError;

/// The page's mode vocabulary, shared by `join` and `connectMedia` so the two
/// can never disagree by spelling.
pub(crate) fn parse_compat(value: Option<&str>) -> Result<ElementCallCompat, JsError> {
    Ok(match value {
        None | Some("off") => ElementCallCompat::Off,
        Some("sticky_events") => ElementCallCompat::StickyEvents,
        Some("state_events") => ElementCallCompat::StateEvents,
        Some(other) => {
            return Err(JsError::new(&format!(
                "unknown element_call_compat {other:?}: expected off | sticky_events | state_events",
            )));
        }
    })
}

/// One `m.rtc.member` sticky event with its content raw, as JS hands it in:
/// `{ sender, sender_device_id?, was_encrypted?, type, content }`.
///
/// The raw counterpart of the typed sticky ingestion, and the only shape that
/// can carry a pre-2026 membership. Safe for spec-current events too — the
/// normalisation only ever fills in a modern field that is absent — so a page
/// feeds every membership through here regardless of generation.
#[derive(Debug, Deserialize)]
pub(crate) struct WasmRawMemberEvent {
    sender: String,
    #[serde(default)]
    sender_device_id: Option<String>,
    #[serde(default)]
    was_encrypted: Option<bool>,
    #[serde(rename = "type")]
    event_type: String,
    /// The event's whole `content` object, verbatim.
    content: Value,
}

impl From<WasmRawMemberEvent> for ingest::RawMemberEventIn {
    fn from(event: WasmRawMemberEvent) -> Self {
        Self {
            sender: event.sender,
            sender_device_id: event.sender_device_id,
            was_encrypted: event.was_encrypted,
            event_type: event.event_type,
            content: event.content,
        }
    }
}

/// One pre-MSC4354 `org.matrix.msc3401.call.member` **room state** event:
/// `{ sender, state_key, origin_server_ts, content }`. Only fed in
/// `state_events` mode; `origin_server_ts` is load-bearing (expiry base), not
/// decoration.
#[derive(Debug, Deserialize)]
pub(crate) struct WasmLegacyStateMemberEvent {
    sender: String,
    state_key: String,
    origin_server_ts: u64,
    content: Value,
}

impl From<WasmLegacyStateMemberEvent> for ingest::LegacyStateMemberEventIn {
    fn from(event: WasmLegacyStateMemberEvent) -> Self {
        Self {
            sender: event.sender,
            state_key: event.state_key,
            origin_server_ts: event.origin_server_ts,
            content: event.content,
        }
    }
}

/// A decrypted legacy `io.element.call.encryption_keys` to-device message, as
/// JS hands it in: `{ sender, content, was_encrypted, sender_device_id?,
/// sender_is_cross_signed? }`. The content stays raw because the generations
/// disagree about where the key, the index, and the owning membership live.
#[derive(Debug, Deserialize)]
pub(crate) struct WasmLegacyKeyMessage {
    pub sender: String,
    pub content: Value,
    pub was_encrypted: bool,
    #[serde(default)]
    pub sender_device_id: Option<String>,
    #[serde(default)]
    pub sender_is_cross_signed: bool,
}
