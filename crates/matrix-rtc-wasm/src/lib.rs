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

//! WebAssembly bindings for the MatrixRTC core.
//!
//! This layer accepts JS-shaped sticky events and maps them into core DTOs.
//! Keeping this conversion here lets the core remain independent from wasm/JS types.

use matrix_rtc_core::{
    EventConversionError, JoinSessionParams, JoinedMembership, LeaveSessionParams, RawRtcTransport,
    RawStickyEvent, RawStickyEventUpdate, RtcCommandSender, RtcSession, RtcSessionManager,
    RtcTransport, StickyEventsUpdate,
};

mod commands;
pub use commands::JsCommandSender;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::watch;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
/// WebAssembly-facing wrapper around `RtcSessionManager`.
pub struct WasmRtcSessionManager {
    inner: RtcSessionManager,
    /// Command sender for sending events to Matrix rooms
    command_sender: Option<Arc<dyn RtcCommandSender>>,
}

#[wasm_bindgen]
impl WasmRtcSessionManager {
    #[wasm_bindgen(constructor)]
    /// Creates an empty session manager instance for JS consumers.
    pub fn new() -> Self {
        Self {
            inner: RtcSessionManager::new(),
            command_sender: None,
        }
    }

    /// Sets up the command sender for this manager with a Matrix client.
    ///
    /// This must be called before join/leave operations.
    /// The client must implement methods: sendStickyEvent, sendDelayedEvent, cancelDelayedEvent.
    pub fn setup_command_sender(&mut self, client: JsValue) {
        let command_sender: Arc<dyn matrix_rtc_core::RtcCommandSender> =
            Arc::new(JsCommandSender::new(client));
        self.inner.set_command_sender(command_sender.clone());
        self.command_sender = Some(command_sender);
    }

    /// Returns true if a command sender has been set up.
    pub fn has_command_sender(&self) -> bool {
        self.command_sender.is_some()
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

    /// Joins an RTC session with the given parameters.
    ///
    /// This sends a membership event to join the call and starts the keep-alive mechanism.
    ///
    /// # Arguments
    ///
    /// * `params` - JSON object containing join parameters:
    ///   - `user_id`: Matrix user ID (e.g., "@alice:example.org")
    ///   - `device_id`: Device ID
    ///   - `membership_id`: Optional sticky key, defaults to "{user_id}-{device_id}"
    ///   - `room_id`: Room ID
    ///   - `slot_id`: Slot ID (e.g., "m.call#ROOM")
    ///   - `application`: Application type (e.g., "m.call")
    ///   - `transport`: Transport configuration object
    ///   - `keep_alive_timeout_ms`: Optional keep-alive timeout in milliseconds (default: 30000)
    pub async fn join(&mut self, params: JsValue) -> Result<(), JsError> {
        let params: WasmJoinSessionParams = serde_wasm_bindgen::from_value(params)
            .map_err(|err| JsError::new(&format!("invalid join params: {err}")))?;

        let core_params = params.into_core()?;

        self.inner
            .join(core_params)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Leaves an RTC session.
    ///
    /// This sends a left membership event and cancels the keep-alive mechanism.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID of the session to leave
    /// * `slot_id` - The slot ID of the session to leave
    /// * `params` - Optional JSON object containing leave parameters:
    ///   - `disconnect_reason`: Optional reason for leaving (e.g., "user_left", "ice_failed")
    pub async fn leave(
        &mut self,
        room_id: String,
        slot_id: String,
        params: JsValue,
    ) -> Result<(), JsError> {
        let params: WasmLeaveSessionParams = serde_wasm_bindgen::from_value(params)
            .map_err(|err| JsError::new(&format!("invalid leave params: {err}")))?;

        let core_params = params.into_core();

        self.inner
            .leave(room_id, slot_id, core_params)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }
}

impl Default for WasmRtcSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// WASM-friendly join session parameters.
#[derive(Debug, Deserialize)]
pub struct WasmJoinSessionParams {
    pub user_id: String,
    pub device_id: String,
    #[serde(default)]
    pub membership_id: Option<String>,
    pub room_id: String,
    pub slot_id: String,
    pub application: String,
    pub transport: WasmTransportConfig,
    #[serde(default)]
    pub keep_alive_timeout_ms: Option<u64>,
}

impl WasmJoinSessionParams {
    pub fn into_core(self) -> Result<JoinSessionParams, JsError> {
        let transport = self.transport.into_core()?;
        Ok(JoinSessionParams {
            user_id: self.user_id,
            device_id: self.device_id,
            membership_id: self.membership_id,
            room_id: self.room_id,
            slot_id: self.slot_id,
            application: self.application,
            transport,
            keep_alive_timeout_ms: self.keep_alive_timeout_ms,
        })
    }
}

/// WASM-friendly transport configuration.
#[derive(Debug, Deserialize)]
pub struct WasmTransportConfig {
    #[serde(rename = "type")]
    pub transport_type: String,
    #[serde(default)]
    pub livekit_service_url: Option<String>,
    #[serde(flatten)]
    pub extra_fields: std::collections::BTreeMap<String, serde_json::Value>,
}

impl WasmTransportConfig {
    pub fn into_core(self) -> Result<RtcTransport, JsError> {
        match self.transport_type.as_str() {
            "livekit" => {
                let url = self.livekit_service_url.ok_or_else(|| {
                    JsError::new("livekit transport requires livekit_service_url")
                })?;
                Ok(RtcTransport::LiveKit(matrix_rtc_core::LiveKitTransport {
                    livekit_service_url: url,
                }))
            }
            _ => {
                let mut extra_fields = self.extra_fields;
                // Add any known fields from the transport config
                if let Some(url) = self.livekit_service_url {
                    extra_fields.insert(
                        "livekit_service_url".to_string(),
                        serde_json::Value::String(url),
                    );
                }
                Ok(RtcTransport::Unsupported(
                    matrix_rtc_core::UnsupportedTransport {
                        transport_type: self.transport_type,
                        extra_fields,
                    },
                ))
            }
        }
    }
}

/// WASM-friendly leave session parameters.
#[derive(Debug, Deserialize, Default)]
pub struct WasmLeaveSessionParams {
    #[serde(default)]
    pub disconnect_reason: Option<String>,
}

impl WasmLeaveSessionParams {
    pub fn into_core(self) -> LeaveSessionParams {
        LeaveSessionParams {
            disconnect_reason: self.disconnect_reason,
        }
    }
}

#[wasm_bindgen]
/// WebAssembly-facing single-session API.
pub struct WasmRtcSession {
    inner: RtcSession,
    /// Command sender for sending events to Matrix rooms
    command_sender: Option<Arc<dyn RtcCommandSender>>,
}

#[wasm_bindgen]
impl WasmRtcSession {
    #[wasm_bindgen(constructor)]
    /// Creates an empty RTC session instance.
    pub fn new() -> Self {
        Self {
            inner: RtcSession::new(),
            command_sender: None,
        }
    }

    /// Sets up the command sender for this session.
    ///
    /// Sets up the command sender for this session with a Matrix client.
    ///
    /// This must be called before join/leave operations.
    /// The client must implement methods: sendStickyEvent, sendDelayedEvent, cancelDelayedEvent.
    pub fn setup_command_sender(&mut self, client: JsValue) {
        let command_sender: Arc<dyn matrix_rtc_core::RtcCommandSender> =
            Arc::new(JsCommandSender::new(client));
        self.inner.set_command_sender(command_sender.clone());
        self.command_sender = Some(command_sender);
    }

    /// Returns true if a command sender has been set up.
    pub fn has_command_sender(&self) -> bool {
        self.command_sender.is_some()
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

    /// Joins this RTC session with the given parameters.
    ///
    /// This sends a membership event to join the call and starts the keep-alive mechanism.
    ///
    /// # Arguments
    ///
    /// * `params` - JSON object containing join parameters (same as WasmRtcSessionManager::join)
    pub async fn join(&mut self, params: JsValue) -> Result<(), JsError> {
        let params: WasmJoinSessionParams = serde_wasm_bindgen::from_value(params)
            .map_err(|err| JsError::new(&format!("invalid join params: {err}")))?;

        let core_params = params.into_core()?;

        self.inner
            .join(core_params)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Leaves this RTC session.
    ///
    /// This sends a left membership event and cancels the keep-alive mechanism.
    ///
    /// # Arguments
    ///
    /// * `params` - Optional JSON object containing leave parameters (same as WasmRtcSessionManager::leave)
    pub fn leave(&mut self, params: JsValue) -> Result<(), JsError> {
        let _params: WasmLeaveSessionParams = serde_wasm_bindgen::from_value(params)
            .map_err(|err| JsError::new(&format!("invalid leave params: {err}")))?;

        // Note: This requires room_id and slot_id to be tracked in the session
        // For now, we return an error if they're not available
        // This is a limitation that should be addressed in the core crate
        Err(JsError::new(
            "leave() on single session requires room_id and slot_id to be tracked",
        ))
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
    #[serde(default)]
    rtc_transports: Option<Vec<WasmRawRtcTransport>>,
}

#[derive(Debug, Deserialize)]
struct WasmRawRtcTransport {
    #[serde(rename = "type")]
    transport_type: String,
    #[serde(flatten)]
    extra_fields: std::collections::BTreeMap<String, serde_json::Value>,
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

impl From<WasmRawRtcTransport> for RawRtcTransport {
    fn from(value: WasmRawRtcTransport) -> Self {
        RawRtcTransport {
            transport_type: value.transport_type,
            extra_fields: value.extra_fields,
        }
    }
}

impl From<WasmStickyEvent> for RawStickyEvent {
    fn from(value: WasmStickyEvent) -> Self {
        RawStickyEvent {
            room_id: value.room_id,
            sender: value.sender,
            event_type: value.event_type,
            content: matrix_rtc_core::RawStickyEventContent {
                slot_id: value.content.slot_id,
                sticky_key: value.content.sticky_key,
                application: matrix_rtc_core::ApplicationInfo {
                    application_type: value.content.application.map(|app| app.kind),
                    extra: std::collections::BTreeMap::new(),
                },
                member: matrix_rtc_core::MemberInfo {
                    id: value.content.member.map(|member| member.id),
                    claimed_device_id: None,
                    claimed_user_id: None,
                },
                versions: Vec::new(),
                disconnect_reason: value.content.disconnect_reason.map(|reason| {
                    matrix_rtc_core::DisconnectReason {
                        class: None,
                        reason: Some(reason),
                        description: None,
                    }
                }),
                m_relates_to: None,
                rtc_transports: value
                    .content
                    .rtc_transports
                    .map(|v| v.into_iter().map(Into::into).collect()),
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
