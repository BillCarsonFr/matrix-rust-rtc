//! WebAssembly bindings for the MatrixRTC core.
//!
//! This layer accepts JS-shaped sticky events and maps them into core DTOs.
//! Keeping this conversion here lets the core remain independent from wasm/JS types.

use matrix_rtc_core::{
    EventConversionError, JoinedMembership, RawStickyEvent, RawStickyEventContent,
    RawStickyEventUpdate, RtcSession, RtcSessionManager, StickyEventsUpdate,
};
use serde::Deserialize;
use tokio::sync::watch;
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

    /// Returns the number of active sessions currently tracked by the manager.
    pub fn session_count(&self) -> u32 {
        self.inner.session_count() as u32
    }

    /// Returns the number of joined members for one `(room_id, slot_id)` session.
    pub fn member_count(&self, room_id: String, slot_id: String) -> Option<u32> {
        self.inner
            .member_count(&room_id, &slot_id)
            .map(|count| count as u32)
    }
}

impl Default for WasmRtcSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
/// WebAssembly-facing single-session API.
pub struct WasmRtcSession {
    inner: RtcSession,
}

#[wasm_bindgen]
impl WasmRtcSession {
    #[wasm_bindgen(constructor)]
    /// Creates an empty RTC session instance.
    pub fn new() -> Self {
        Self {
            inner: RtcSession::new(),
        }
    }

    /// Applies initial sticky events for this single session.
    pub fn on_sticky_events_snapshot_received(&mut self, events: JsValue) -> Result<(), JsError> {
        let input: Vec<WasmStickyEvent> = serde_wasm_bindgen::from_value(events)
            .map_err(|err| JsError::new(&format!("invalid sticky snapshot payload: {err}")))?;

        let mut membership_events = Vec::new();
        for event in input.into_iter() {
            let event = RawStickyEvent::from(event);
            match event.try_into_call_membership_event() {
                Ok(event) => membership_events.push(event),
                Err(EventConversionError::UnsupportedEventType { .. }) => continue,
                Err(err) => return Err(JsError::new(&err.to_string())),
            }
        }

        self.inner.initial_events(membership_events);

        Ok(())
    }

    /// Applies one sticky diff payload for this single session.
    pub fn on_sticky_events_update_received(&mut self, update: JsValue) -> Result<(), JsError> {
        let input: WasmStickyEventsUpdate = serde_wasm_bindgen::from_value(update)
            .map_err(|err| JsError::new(&format!("invalid sticky event payload: {err}")))?;

        let mut membership_events = Vec::new();

        for event in input.added {
            match RawStickyEvent::from(event).try_into_call_membership_event() {
                Ok(event) => membership_events.push(event),
                Err(EventConversionError::UnsupportedEventType { .. }) => continue,
                Err(err) => return Err(JsError::new(&err.to_string())),
            }
        }

        for item in input.updated {
            match RawStickyEvent::from(item.current).try_into_call_membership_event() {
                Ok(event) => membership_events.push(event),
                Err(EventConversionError::UnsupportedEventType { .. }) => continue,
                Err(err) => return Err(JsError::new(&err.to_string())),
            }
        }

        for event in input.removed {
            match RawStickyEvent::from(event).try_into_left_membership_event() {
                Ok(event) => membership_events.push(event),
                Err(EventConversionError::UnsupportedEventType { .. }) => continue,
                Err(err) => return Err(JsError::new(&err.to_string())),
            }
        }

        self.inner.handle_update(membership_events);

        Ok(())
    }

    /// Subscribes to full membership snapshots for this session.
    pub fn subscribe_membership_snapshots(&self) -> WasmMembershipSnapshotSubscription {
        WasmMembershipSnapshotSubscription {
            inner: self.inner.subscribe_membership_snapshots(),
            initial_pending: true,
        }
    }
}

impl Default for WasmRtcSession {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
/// Poll-based subscription for session membership snapshots.
pub struct WasmMembershipSnapshotSubscription {
    inner: watch::Receiver<Vec<JoinedMembership>>,
    initial_pending: bool,
}

#[wasm_bindgen]
impl WasmMembershipSnapshotSubscription {
    /// Returns the next full snapshot if available, or `null` if unchanged.
    pub fn next_snapshot(&mut self) -> Result<JsValue, JsError> {
        if self.initial_pending {
            self.initial_pending = false;
            return serde_wasm_bindgen::to_value(&self.inner.borrow().clone())
                .map_err(|err| JsError::new(&format!("failed to serialize snapshot: {err}")));
        }

        match self.inner.has_changed() {
            Ok(true) => serde_wasm_bindgen::to_value(&self.inner.borrow_and_update().clone())
                .map_err(|err| JsError::new(&format!("failed to serialize snapshot: {err}"))),
            Ok(false) | Err(_) => Ok(JsValue::NULL),
        }
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

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::Value;
    use wasm_bindgen_test::*;

    #[derive(Serialize)]
    struct TestStickyEvent {
        room_id: String,
        sender: String,
        #[serde(rename = "type")]
        event_type: String,
        content: TestStickyEventContent,
    }

    #[derive(Serialize)]
    struct TestStickyEventContent {
        slot_id: String,
        sticky_key: String,
        application: Option<TestApplication>,
        member: Option<TestMember>,
        disconnect_reason: Option<String>,
    }

    #[derive(Serialize)]
    struct TestApplication {
        #[serde(rename = "type")]
        kind: String,
    }

    #[derive(Serialize)]
    struct TestMember {
        id: String,
    }

    fn joined_event() -> TestStickyEvent {
        TestStickyEvent {
            room_id: "!room:example.org".to_owned(),
            sender: "@alice:example.org".to_owned(),
            event_type: "m.rtc.member".to_owned(),
            content: TestStickyEventContent {
                slot_id: "m.call#ROOM".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application: Some(TestApplication {
                    kind: "m.call".to_owned(),
                }),
                member: Some(TestMember {
                    id: "alice-device-a".to_owned(),
                }),
                disconnect_reason: None,
            },
        }
    }

    #[wasm_bindgen_test]
    fn next_snapshot_returns_current_snapshot_on_first_poll() {
        let mut session = WasmRtcSession::new();
        let events = serde_wasm_bindgen::to_value(&vec![joined_event()]).unwrap();
        session.on_sticky_events_snapshot_received(events).unwrap();

        let mut subscription = session.subscribe_membership_snapshots();
        let first = subscription.next_snapshot().unwrap();
        let parsed: Vec<Value> = serde_wasm_bindgen::from_value(first).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["sender"], "@alice:example.org");
    }

    #[wasm_bindgen_test]
    fn next_snapshot_returns_null_when_unchanged() {
        let session = WasmRtcSession::new();
        let mut subscription = session.subscribe_membership_snapshots();

        let first = subscription.next_snapshot().unwrap();
        let parsed: Vec<Value> = serde_wasm_bindgen::from_value(first).unwrap();
        assert!(parsed.is_empty());

        let second = subscription.next_snapshot().unwrap();
        assert!(second.is_null());
    }
}
