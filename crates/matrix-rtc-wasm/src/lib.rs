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
//!
//! # Only built for `wasm32`
//!
//! The crate body is `cfg`-gated to `wasm32` because it cannot compile anywhere
//! else: its futures wrap JS promises and are therefore `!Send`, while
//! `matrix-rtc-core`'s command traits are `Send` on every target *but* wasm32
//! (see [`matrix_rtc_core::RtcCommandSender`]). On other targets this compiles
//! to an empty crate so a workspace-wide `cargo check`/`clippy` still passes.
//!
//! The consequence is that a host-target build no longer type-checks this crate.
//! Check it explicitly:
//!
//! ```sh
//! cargo check -p matrix-rtc-wasm --target wasm32-unknown-unknown
//! ```
#![cfg(target_arch = "wasm32")]

use matrix_rtc_core::{
    EncryptionConfig, EventConversionError, JoinSessionParams, JoinedMembership,
    LeaveSessionParams, Mentions, NotificationType, NotifyConfig, RawRtcTransport, RawSlotEvent,
    RawStickyEvent, RtcSession, RtcSessionManager, RtcTransport,
};

mod commands;
mod logging;
pub use commands::JsCommandSender;
pub use logging::{init_logging, log_event};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::watch;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
/// WebAssembly-facing wrapper around `RtcSessionManager`.
pub struct WasmRtcSessionManager {
    inner: RtcSessionManager<JsCommandSender>,
    /// Command sender for sending events to Matrix rooms
    command_sender: Option<Arc<JsCommandSender>>,
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
    /// The client must implement methods: sendStickyEvent(roomId, type, content, durationMs),
    /// sendDelayedEvent, restartDelayedEvent, cancelDelayedEvent.
    pub fn setup_command_sender(&mut self, client: JsValue) {
        log::info!("manager: command sender installed");
        // `Rc` is not an option: the core's `set_command_sender` takes `Arc<T>`
        // on every target. `expect` so this goes away by itself if that changes.
        #[expect(clippy::arc_with_non_send_sync)]
        let command_sender: Arc<JsCommandSender> = Arc::new(JsCommandSender::new(client));
        self.inner.set_command_sender(command_sender.clone());
        self.command_sender = Some(command_sender);
    }

    /// Returns true if a command sender has been set up.
    pub fn has_command_sender(&self) -> bool {
        self.command_sender.is_some()
    }

    /// Applies the **complete** current sticky state for one room, from a JS
    /// iterable/array payload. Replaces rather than merges: a member absent from
    /// `events` is gone, and an empty list clears the room.
    pub async fn set_current_sticky_state(
        &mut self,
        room_id: String,
        events: JsValue,
    ) -> Result<(), JsError> {
        let input: Vec<WasmStickyEvent> =
            serde_wasm_bindgen::from_value(events).map_err(|err| {
                log::warn!("manager: [{room_id}] invalid sticky snapshot payload: {err}");
                JsError::new(&format!("invalid sticky snapshot payload: {err}"))
            })?;

        log::debug!(
            "manager: [{room_id}] initial sticky in, {} event(s)",
            input.len(),
        );

        let mapped: Vec<RawStickyEvent> = input.into_iter().map(Into::into).collect();

        self.inner
            .set_current_sticky_state(&room_id, mapped)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Applies a room's complete `m.rtc.slot` state.
    ///
    /// Calling this is what makes the MSC4143 open-slot condition apply to the
    /// room: until then it cannot be evaluated and is not enforced. Any slot in
    /// the room *not* present in `slots` is treated as closed, so always pass
    /// the full set — an empty array included.
    ///
    /// Each entry is `{ slot_id, content }`, where `content` is the raw
    /// `m.rtc.slot` content.
    pub async fn on_room_slots_received(
        &mut self,
        room_id: String,
        slots: JsValue,
    ) -> Result<(), JsError> {
        let input: Vec<WasmSlotEvent> = serde_wasm_bindgen::from_value(slots).map_err(|err| {
            log::warn!("manager: [{room_id}] invalid slot payload: {err}");
            JsError::new(&format!("invalid slot payload: {err}"))
        })?;

        log::debug!(
            "manager: [{room_id}] room slots in: {:?}",
            input.iter().map(|slot| &slot.slot_id).collect::<Vec<_>>(),
        );

        let mapped: Vec<RawSlotEvent> = input
            .into_iter()
            .map(|slot| RawSlotEvent {
                room_id: room_id.clone(),
                slot_id: slot.slot_id,
                content: slot.content,
            })
            .collect();

        self.inner.on_room_slots_received(&room_id, mapped).await;
        Ok(())
    }

    /// Sets the users currently joined to a room.
    ///
    /// MSC4143 only counts a member event while its sender is still joined to
    /// the room; until this is called that condition is not enforced.
    pub async fn on_room_members_received(&mut self, room_id: String, joined_user_ids: JsValue) {
        let members: Vec<String> = serde_wasm_bindgen::from_value(joined_user_ids)
            .inspect_err(|err| {
                log::warn!(
                    "manager: [{room_id}] unreadable joined-members payload ({err}); \
                     treating the room as empty",
                )
            })
            .unwrap_or_default();

        log::debug!(
            "manager: [{room_id}] room members in: {} joined",
            members.len(),
        );

        self.inner.on_room_members_received(&room_id, members).await;
    }

    /// Reports whether a room is end-to-end encrypted.
    ///
    /// MSC4143 requires RTC encryption in encrypted rooms and forbids it
    /// elsewhere, so this changes how the room's slots resolve and whether
    /// cleartext member events count.
    pub async fn on_room_encryption_received(&mut self, room_id: String, encrypted: bool) {
        log::info!("manager: [{room_id}] room encryption in: encrypted={encrypted}");
        self.inner
            .on_room_encryption_received(&room_id, encrypted)
            .await;
    }

    /// A JSON dump of everything the manager and its sessions currently
    /// believe, including every candidate member and the reason it is or is not
    /// projected as joined. For bug reports; contains no key material.
    #[wasm_bindgen(js_name = debugSnapshot)]
    pub fn debug_snapshot(&self) -> String {
        self.inner.debug_snapshot().to_string()
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
    ///   - `room_id`: Room ID
    ///   - `slot_id`: Slot ID (e.g., "m.call#ROOM")
    ///   - `application`: Application type (e.g., "m.call")
    ///   - `transport`: Transport configuration object
    ///   - `keep_alive_timeout_ms`: Optional keep-alive timeout in milliseconds (default: 30000)
    ///   - `sticky_duration_ms`: Optional sticky-map lifetime for our membership in
    ///     milliseconds (default: 3600000); the SDK re-sends the membership at half this
    ///   - `degraded_lifetime_ms`: Optional membership lifetime to fall back to on a
    ///     homeserver that refuses MSC4140 delayed events (default: 300000, and not to
    ///     be set below that — see MSC4354 on clock skew). Note that nothing refreshes
    ///     it here: this binding exposes no heartbeat, so a degraded join lapses when
    ///     this elapses.
    ///
    /// Resolves to the `member.id` this join used. The SDK generates it: MSC4143
    /// requires a fresh one per join, and reusing one keeps the media-plane
    /// participant identity stable while the key index restarts at 0, so peers
    /// would decrypt new media with the previous call's key.
    pub async fn join(&mut self, params: JsValue) -> Result<String, JsError> {
        let params: WasmJoinSessionParams =
            serde_wasm_bindgen::from_value(params).map_err(|err| {
                log::warn!("manager: invalid join params: {err}");
                JsError::new(&format!("invalid join params: {err}"))
            })?;

        log::info!(
            "manager: join requested [{}/{}] user={} device={} application={}",
            params.room_id,
            params.slot_id,
            params.user_id,
            params.device_id,
            params.application,
        );

        let mut core_params = params.into_core()?;
        let member_id = matrix_rtc_core::generate_member_id();
        core_params.membership_id = Some(member_id.clone());

        let result = self
            .inner
            .join(core_params)
            .await
            .map_err(|err| JsError::new(&err.to_string()));

        match &result {
            Ok(()) => log::info!("manager: join succeeded as {member_id}"),
            Err(_) => log::warn!("manager: join failed"),
        }

        result.map(|()| member_id)
    }

    /// Our `member.id` in one session, or `undefined` if there is no such
    /// session or it has not joined. Changes on every join, so read it when
    /// needed rather than caching what `join` returned.
    #[wasm_bindgen(js_name = ownMemberId)]
    pub fn own_member_id(&self, room_id: String, slot_id: String) -> Option<String> {
        self.inner.own_member_id(&room_id, &slot_id)
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
    ///   - `leave_reason`: Optional MSC4143 leave reason, an object of
    ///     `{ code, reason }` — e.g. `{ code: "leave" }` for an intentional
    ///     hang-up, or `{ code: "ice_failed", reason: "no candidates" }` for a
    ///     transport-defined cause. Defaults to `{ code: "leave" }`.
    pub async fn leave(
        &mut self,
        room_id: String,
        slot_id: String,
        params: JsValue,
    ) -> Result<(), JsError> {
        let params: WasmLeaveSessionParams = serde_wasm_bindgen::from_value(params)
            .map_err(|err| JsError::new(&format!("invalid leave params: {err}")))?;

        log::info!(
            "manager: leave requested [{room_id}/{slot_id}] reason={:?}",
            params.leave_reason,
        );

        let core_params = params.into_core();

        let result = self
            .inner
            .leave(room_id, slot_id, core_params)
            .await
            .map_err(|err| JsError::new(&err.to_string()));

        match &result {
            Ok(()) => log::info!("manager: leave succeeded"),
            Err(_) => log::warn!("manager: leave failed"),
        }

        result
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
    pub room_id: String,
    pub slot_id: String,
    pub application: String,
    /// The transport to publish on. Omit to join without publishing — valid per
    /// MSC4143, and what a recorder or other observer wants.
    #[serde(default)]
    pub transport: Option<WasmTransportConfig>,
    /// Transport types this member can receive on. Only read when `transport`
    /// is omitted; a publishing member advertises its own transport's type.
    #[serde(default)]
    pub can_subscribe: Vec<String>,
    #[serde(default)]
    pub keep_alive_timeout_ms: Option<u64>,
    pub sticky_duration_ms: Option<u64>,
    #[serde(default)]
    pub degraded_lifetime_ms: Option<u64>,
    #[serde(default)]
    pub encryption_config: Option<WasmEncryptionConfig>,
    /// Ask for an MSC4075 notification to be sent with this join, so other
    /// devices in the room ring or show an incoming call.
    ///
    /// Omit — the default — to join quietly, which is what joining a call
    /// someone else started does. Pass it only when the user is *starting* the
    /// call: the SDK still suppresses the notification if anybody is already in
    /// the session, but the intent to summon anyone at all is the caller's to
    /// state.
    #[serde(default)]
    pub notify: Option<WasmNotifyConfig>,
}

/// WASM-friendly MSC4075 notification request.
#[derive(Debug, Deserialize)]
pub struct WasmNotifyConfig {
    /// `"ring"` or `"notification"`.
    pub notification_type: String,
    /// MSC4196 `m.call.intent`, e.g. `"audio"` or `"video"`.
    #[serde(default)]
    pub intent: Option<String>,
    /// How long the ring stays valid, in milliseconds (default: 30000, capped
    /// at 120000 because that is what receivers honour).
    #[serde(default)]
    pub lifetime_ms: Option<u64>,
    /// Users named individually in `m.mentions`. Usually empty.
    #[serde(default)]
    pub mention_user_ids: Vec<String>,
    /// Whether the whole room is targeted (default: true).
    #[serde(default = "default_mention_room")]
    pub mention_room: bool,
}

fn default_mention_room() -> bool {
    true
}

impl WasmNotifyConfig {
    fn into_core(self) -> Result<NotifyConfig, JsError> {
        let notification_type = match self.notification_type.as_str() {
            "ring" => NotificationType::Ring,
            "notification" => NotificationType::Notification,
            other => {
                return Err(JsError::new(&format!(
                    "notification_type must be \"ring\" or \"notification\", got {other:?}"
                )));
            }
        };
        Ok(NotifyConfig {
            notification_type,
            intent: self.intent,
            lifetime_ms: self.lifetime_ms,
            mentions: Mentions {
                user_ids: self.mention_user_ids,
                room: self.mention_room,
            },
        })
    }
}

/// WASM-friendly encryption configuration.
#[derive(Debug, Default, Deserialize)]
pub struct WasmEncryptionConfig {
    #[serde(default = "default_delay_before_use_ms")]
    pub delay_before_use_ms: u64,
    #[serde(default = "default_key_rotation_grace_period_ms")]
    pub key_rotation_grace_period_ms: u64,
    /// Longest a key may be used before it is replaced regardless of membership
    /// (default: 5400000ms, 1h30).
    #[serde(default = "default_max_key_lifetime_ms")]
    pub max_key_lifetime_ms: u64,
    #[serde(default = "default_manage_media_keys")]
    pub manage_media_keys: bool,
    /// Whether to discard keys from devices that are not cross-signed
    /// (default: true, per MSC4153).
    #[serde(default = "default_require_cross_signed_sender")]
    pub require_cross_signed_sender: bool,
}

fn default_delay_before_use_ms() -> u64 {
    5000
}
fn default_key_rotation_grace_period_ms() -> u64 {
    10000
}
fn default_max_key_lifetime_ms() -> u64 {
    90 * 60 * 1000
}
fn default_manage_media_keys() -> bool {
    true
}
fn default_require_cross_signed_sender() -> bool {
    true
}

impl From<WasmEncryptionConfig> for EncryptionConfig {
    fn from(value: WasmEncryptionConfig) -> Self {
        EncryptionConfig {
            delay_before_use_ms: value.delay_before_use_ms,
            key_rotation_grace_period_ms: value.key_rotation_grace_period_ms,
            max_key_lifetime_ms: value.max_key_lifetime_ms,
            manage_media_keys: value.manage_media_keys,
            require_cross_signed_sender: value.require_cross_signed_sender,
        }
    }
}

impl WasmJoinSessionParams {
    pub fn into_core(self) -> Result<JoinSessionParams, JsError> {
        let transport = self.transport.map(|t| t.into_core()).transpose()?;
        let encryption_config = self.encryption_config.map(Into::into);
        Ok(JoinSessionParams {
            user_id: self.user_id,
            device_id: self.device_id,
            // Filled in by the join entry points, which generate a fresh id per
            // join and return it. A caller-chosen `member.id` reused across
            // joins would keep the MSC4195 participant identity stable while our
            // key index restarts at 0, so peers decrypt new media with the
            // previous call's key.
            membership_id: None,
            room_id: self.room_id,
            slot_id: self.slot_id,
            application: self.application,
            transport: match transport {
                Some(transport) => matrix_rtc_core::TransportIntent::Publish(transport),
                None => matrix_rtc_core::TransportIntent::ReceiveOnly {
                    can_subscribe: self.can_subscribe,
                },
            },
            keep_alive_timeout_ms: self.keep_alive_timeout_ms,
            sticky_duration_ms: self.sticky_duration_ms,
            degraded_lifetime_ms: self.degraded_lifetime_ms,
            encryption_config,
            notify: self.notify.map(WasmNotifyConfig::into_core).transpose()?,
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
    pub leave_reason: Option<WasmLeaveReason>,
}

/// MSC4143 `leave_reason`: a machine-readable `code` plus an optional
/// human-readable `reason`.
#[derive(Debug, Deserialize)]
pub struct WasmLeaveReason {
    pub code: String,
    #[serde(default)]
    pub reason: Option<String>,
}

impl From<WasmLeaveReason> for matrix_rtc_core::LeaveReason {
    fn from(value: WasmLeaveReason) -> Self {
        matrix_rtc_core::LeaveReason {
            code: matrix_rtc_core::LeaveCode::from_code(&value.code),
            reason: value.reason,
        }
    }
}

impl WasmLeaveSessionParams {
    pub fn into_core(self) -> LeaveSessionParams {
        LeaveSessionParams {
            leave_reason: self.leave_reason.map(Into::into),
        }
    }
}

#[wasm_bindgen]
/// WebAssembly-facing single-session API.
pub struct WasmRtcSession {
    inner: RtcSession<JsCommandSender>,
    /// Command sender for sending events to Matrix rooms
    command_sender: Option<Arc<JsCommandSender>>,
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
    /// The client must implement methods: sendStickyEvent(roomId, type, content, durationMs),
    /// sendDelayedEvent, restartDelayedEvent, cancelDelayedEvent.
    pub fn setup_command_sender(&mut self, client: JsValue) {
        // `Rc` is not an option: the core's `set_command_sender` takes `Arc<T>`
        // on every target. `expect` so this goes away by itself if that changes.
        #[expect(clippy::arc_with_non_send_sync)]
        let command_sender: Arc<JsCommandSender> = Arc::new(JsCommandSender::new(client));
        self.inner.set_command_sender(command_sender.clone());
        self.command_sender = Some(command_sender);
    }

    /// Returns true if a command sender has been set up.
    pub fn has_command_sender(&self) -> bool {
        self.command_sender.is_some()
    }

    /// Applies the complete current sticky state for this single session,
    /// replacing what it held: a member absent from `events` is gone.
    pub async fn set_current_sticky_state(&mut self, events: JsValue) -> Result<(), JsError> {
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

        self.inner.set_current_state(membership_events).await;

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
    ///
    /// Resolves to the SDK-generated `member.id` this join used.
    pub async fn join(&mut self, params: JsValue) -> Result<String, JsError> {
        let params: WasmJoinSessionParams = serde_wasm_bindgen::from_value(params)
            .map_err(|err| JsError::new(&format!("invalid join params: {err}")))?;

        let mut core_params = params.into_core()?;
        let member_id = matrix_rtc_core::generate_member_id();
        core_params.membership_id = Some(member_id.clone());

        self.inner
            .join(core_params)
            .await
            .map(|()| member_id)
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Our `member.id` for the current join, or `undefined` while not joined.
    #[wasm_bindgen(js_name = ownMemberId)]
    pub fn own_member_id(&self) -> Option<String> {
        self.inner.own_member_id().map(str::to_owned)
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
    /// Device that sent the event, from its decryption metadata. MSC4143 has no
    /// self-asserted device field, so the host must supply this.
    #[serde(default)]
    sender_device_id: Option<String>,
    /// Whether the event arrived encrypted; MSC4143 requires member events to be
    /// encrypted in encrypted rooms. Omit if unknown — omitting it is not the
    /// same as `false`, which would drop the member in an encrypted room.
    #[serde(default)]
    was_encrypted: Option<bool>,
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
    #[serde(default)]
    transports: Option<WasmTransports>,
    #[serde(default)]
    leave_reason: Option<WasmLeaveReason>,
}

#[derive(Debug, Deserialize)]
struct WasmSlotEvent {
    slot_id: String,
    #[serde(default)]
    content: matrix_rtc_core::RawSlotEventContent,
}

#[derive(Debug, Deserialize)]
struct WasmTransports {
    #[serde(default)]
    published: Vec<WasmRawRtcTransport>,
    #[serde(default)]
    can_subscribe: Vec<String>,
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
    /// MSC4143 `member.membership`: "join" or "leave".
    #[serde(default)]
    membership: Option<String>,
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
        let member = value
            .content
            .member
            .map(|member| matrix_rtc_core::MemberInfo {
                id: Some(member.id),
                membership: member.membership.map(|m| match m.as_str() {
                    "join" => matrix_rtc_core::Membership::Join,
                    "leave" => matrix_rtc_core::Membership::Leave,
                    _ => matrix_rtc_core::Membership::Unknown(m),
                }),
            })
            .unwrap_or_default();

        RawStickyEvent {
            room_id: value.room_id,
            sender: value.sender,
            origin: match value.was_encrypted {
                Some(true) => matrix_rtc_core::EventOrigin::encrypted(value.sender_device_id),
                Some(false) => matrix_rtc_core::EventOrigin::Cleartext,
                None => matrix_rtc_core::EventOrigin::Unknown,
            },
            event_type: value.event_type,
            content: matrix_rtc_core::RawStickyEventContent {
                slot_id: value.content.slot_id,
                sticky_key: value.content.sticky_key,
                application: matrix_rtc_core::ApplicationInfo {
                    application_type: value.content.application.map(|app| app.kind),
                    extra: std::collections::BTreeMap::new(),
                },
                member,
                transports: value
                    .content
                    .transports
                    .map(|t| matrix_rtc_core::MemberTransports {
                        published: t.published.into_iter().map(Into::into).collect(),
                        can_subscribe: t.can_subscribe,
                    }),
                leave_reason: value.content.leave_reason.map(Into::into),
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
    }

    #[derive(Serialize)]
    struct TestApplication {
        #[serde(rename = "type")]
        kind: String,
    }

    #[derive(Serialize)]
    struct TestMember {
        id: String,
        membership: String,
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
                    membership: "join".to_owned(),
                }),
            },
        }
    }

    #[wasm_bindgen_test]
    async fn next_snapshot_returns_current_snapshot_on_first_poll() {
        let mut session = WasmRtcSession::new();
        let events = serde_wasm_bindgen::to_value(&vec![joined_event()]).unwrap();
        session.set_current_sticky_state(events).await.unwrap();

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
