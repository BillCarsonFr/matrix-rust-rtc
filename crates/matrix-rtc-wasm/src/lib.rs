//! WebAssembly bindings for the MatrixRTC core.
//!
//! This layer accepts JS-shaped sticky events and maps them into core DTOs.
//! Keeping this conversion here lets the core remain independent from wasm/JS types.

use matrix_rtc_core::{
    RawStickyEvent, RawStickyEventContent, RawStickyEventUpdate, RtcSessionManager,
    StickyEventsUpdate,
};
use serde::Deserialize;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
/// WebAssembly-facing wrapper around `RtcSessionManager`.
pub struct WasmRtcSessionManager {
    inner: RtcSessionManager,
}

#[wasm_bindgen]
impl WasmRtcSessionManager {
    #[wasm_bindgen(constructor)]
    /// Creates an empty session manager instance for JS consumers.
    pub fn new() -> Self {
        Self {
            inner: RtcSessionManager::new(),
        }
    }

    /// Applies the initial sticky snapshot for one room from a JS iterable/array payload.
    pub fn on_sticky_events_snapshot_received(
        &mut self,
        room_id: String,
        events: JsValue,
    ) -> Result<(), JsError> {
        let input: Vec<WasmStickyEvent> = serde_wasm_bindgen::from_value(events)
            .map_err(|err| JsError::new(&format!("invalid sticky snapshot payload: {err}")))?;

        let mapped: Vec<RawStickyEvent> = input.into_iter().map(Into::into).collect();

        self.inner
            .initial_sticky_for_room(&room_id, mapped)
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Applies one sticky diff payload for one room from JS (`added`, `updated`, `removed`).
    pub fn on_sticky_events_update_received(
        &mut self,
        room_id: String,
        update: JsValue,
    ) -> Result<(), JsError> {
        let input: WasmStickyEventsUpdate = serde_wasm_bindgen::from_value(update)
            .map_err(|err| JsError::new(&format!("invalid sticky event payload: {err}")))?;

        let mapped = StickyEventsUpdate {
            added: input.added.into_iter().map(Into::into).collect(),
            updated: input
                .updated
                .into_iter()
                .map(|item| RawStickyEventUpdate {
                    current: item.current.into(),
                    previous: item.previous.into(),
                })
                .collect(),
            removed: input.removed.into_iter().map(Into::into).collect(),
        };

        self.inner
            .sticky_update_for_room(&room_id, mapped)
            .map_err(|err| JsError::new(&err.to_string()))
    }
}

impl Default for WasmRtcSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
struct WasmStickyEvent {
    room_id: String,
    sender: String,
    #[serde(rename = "type")]
    event_type: String,
    content: WasmStickyEventContent,
}

#[derive(Debug, Deserialize)]
struct WasmStickyEventContent {
    slot_id: String,
    sticky_key: String,
    application: Option<WasmApplication>,
    member: Option<WasmMember>,
    disconnect_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WasmApplication {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Debug, Deserialize)]
struct WasmMember {
    id: String,
}

#[derive(Debug, Deserialize)]
struct WasmStickyEventUpdate {
    current: WasmStickyEvent,
    previous: WasmStickyEvent,
}

#[derive(Debug, Deserialize)]
struct WasmStickyEventsUpdate {
    added: Vec<WasmStickyEvent>,
    updated: Vec<WasmStickyEventUpdate>,
    removed: Vec<WasmStickyEvent>,
}

impl From<WasmStickyEvent> for RawStickyEvent {
    fn from(value: WasmStickyEvent) -> Self {
        RawStickyEvent {
            room_id: value.room_id,
            sender: value.sender,
            event_type: value.event_type,
            content: RawStickyEventContent {
                slot_id: value.content.slot_id,
                sticky_key: value.content.sticky_key,
                application_type: value.content.application.map(|app| app.kind),
                member_id: value.content.member.map(|member| member.id),
                disconnect_reason: value.content.disconnect_reason,
            },
        }
    }
}
