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

use matrix_rtc_bridge::compat::{ElementCallCompat, ingest};
use matrix_rtc_core::{
    EncryptionConfig, EventConversionError, JoinSessionParams, JoinedMembership, KeyOrigin,
    LeaveSessionParams, Mentions, NotificationType, NotifyConfig, RawRtcTransport, RawSlotEvent,
    RawStickyEvent, ReceivedEncryptionKey, RtcSession, RtcSessionManager, RtcTransport,
    SlotEncryption,
};

mod commands;
mod compat;
mod logging;
mod media;
mod ts_types;
pub use commands::JsCommandSender;
pub use logging::{init_logging, log_event};
pub use media::{WasmConnectionEventSink, WasmMediaSession};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::watch;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
/// WebAssembly-facing wrapper around `RtcSessionManager`.
pub struct WasmRtcSessionManager {
    inner: RtcSessionManager<JsCommandSender>,
    /// Command sender for sending events to Matrix rooms
    command_sender: Option<Arc<JsCommandSender>>,
    /// The Element Call compat mode each room was joined in, installed by
    /// [`Self::join`] and cleared by [`Self::leave`]. Pages choose the mode
    /// once, on the join; everything else — key binding, the media session's
    /// identities and token endpoint — reads it from here so the pieces cannot
    /// skew.
    element_call_compat: std::collections::HashMap<String, ElementCallCompat>,
}

#[wasm_bindgen]
impl WasmRtcSessionManager {
    #[wasm_bindgen(constructor)]
    /// Creates an empty session manager instance for JS consumers.
    pub fn new() -> Self {
        Self {
            inner: RtcSessionManager::new(),
            command_sender: None,
            element_call_compat: std::collections::HashMap::new(),
        }
    }

    /// Sets up the command sender for this manager with a Matrix client.
    ///
    /// This must be called before join/leave operations.
    /// The client must implement methods: sendStickyEvent(roomId, type, content, durationMs),
    /// sendDelayedEvent, restartDelayedEvent, cancelDelayedEvent.
    pub fn setup_command_sender(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "MatrixClientHost")] client: JsValue,
    ) {
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
        #[wasm_bindgen(unchecked_param_type = "StickyEventIn[]")] events: JsValue,
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
        #[wasm_bindgen(unchecked_param_type = "SlotEventIn[]")] slots: JsValue,
    ) -> Result<(), JsError> {
        let input: Vec<WasmSlotEvent> = serde_wasm_bindgen::from_value(slots).map_err(|err| {
            log::warn!("manager: [{room_id}] invalid slot payload: {err}");
            JsError::new(&format!("invalid slot payload: {err}"))
        })?;

        log::debug!(
            "manager: [{room_id}] room slots in: {:?}",
            input.iter().map(|slot| &slot.slot_id).collect::<Vec<_>>(),
        );

        // A room joined in the pre-sticky generation predates `m.rtc.slot`
        // entirely: its truthful "no slots" would resolve the session closed
        // and project out every member, us included. The join forgot earlier
        // slot state; this keeps later feeds from re-closing it. Pages feed
        // slots unconditionally and the mode decides — same as the FFI.
        if self
            .element_call_compat_for(&room_id)
            .reads_state_membership()
        {
            log::debug!(
                "manager: [{room_id}] ignoring the slot state: the room was joined in a \
                 MatrixRTC generation that predates m.rtc.slot",
            );
            return Ok(());
        }

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
    pub async fn on_room_members_received(
        &mut self,
        room_id: String,
        #[wasm_bindgen(unchecked_param_type = "string[]")] joined_user_ids: JsValue,
    ) {
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
    ///     be set below that — see MSC4354 on clock skew). Refreshing it is the page's
    ///     job: this binding starts no driver, so call [`Self::heartbeat`] on an
    ///     interval while joined.
    ///
    /// Resolves to the `member.id` this join used. The SDK generates it: MSC4143
    /// requires a fresh one per join, and reusing one keeps the media-plane
    /// participant identity stable while the key index restarts at 0, so peers
    /// would decrypt new media with the previous call's key.
    pub async fn join(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "JoinParamsIn")] params: JsValue,
    ) -> Result<String, JsError> {
        let params: WasmJoinSessionParams =
            serde_wasm_bindgen::from_value(params).map_err(|err| {
                log::warn!("manager: invalid join params: {err}");
                JsError::new(&format!("invalid join params: {err}"))
            })?;
        let mode = compat::parse_compat(params.element_call_compat.as_deref())?;

        log::info!(
            "manager: join requested [{}/{}] user={} device={} application={} compat={mode:?}",
            params.room_id,
            params.slot_id,
            params.user_id,
            params.device_id,
            params.application,
        );

        // Kept past `into_core`, for the compat bookkeeping below.
        let room_id = params.room_id.clone();
        let slot_id = params.slot_id.clone();
        let user_id = params.user_id.clone();
        let device_id = params.device_id.clone();

        let mut core_params = params.into_core()?;
        // Not always a fresh id: see `ingest::member_id` for the one generation
        // where a fresh one makes us mark ourselves departed on our own join.
        let member_id = ingest::member_id(mode, &user_id, &device_id);
        core_params.membership_id = Some(member_id.clone());

        // Before the join, not after: the join itself sends the membership
        // (and arms the delayed leave), so a dialect registered afterwards
        // would let exactly the two events that announce us go out
        // spec-current.
        self.element_call_compat.insert(room_id.clone(), mode);
        if let Some(sender) = &self.command_sender {
            sender.set_dialect(
                &room_id,
                ingest::outbound_dialect(mode, &user_id, &device_id, &room_id, &slot_id),
            );
        }
        // Slots fed before this join were fed before anything knew the room
        // belonged to a generation that has none, and "no slots" is what
        // closes a session and projects out every member. Must run before the
        // join so the session is created already unenforced.
        if mode.reads_state_membership() {
            self.inner.forget_room_slots(&room_id).await;
        }

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

    /// The event id of our current membership event in one session, or
    /// `undefined` if there is no such session or it has not joined. Moves on
    /// every sticky refresh, so read it at the moment of use.
    #[wasm_bindgen(js_name = ownMembershipEventId)]
    pub fn own_membership_event_id(&self, room_id: String, slot_id: String) -> Option<String> {
        self.inner.own_membership_event_id(&room_id, &slot_id)
    }

    // ---- Reactions and raised hands ----
    //
    // Three host obligations, for every room with a joined session (the
    // `matrix-js-sdk-host` module does all three): forward every decrypted
    // `io.element.call.reaction` and `m.reaction` event through
    // `onRoomTimelineEvents` and every redaction through `onEventRedacted`;
    // after each membership feed, answer `pendingRelationLookups` with each
    // event's `/relations` through `onRelationsReceived`; and supply
    // `event_id` on every sticky event fed in.

    /// Feeds message-like room events to every session of `room_id`:
    /// `[{ room_id, event_id, sender, sender_device_id?, was_encrypted?, type,
    /// origin_server_ts, content }]`. Only `io.element.call.reaction` and
    /// `m.reaction` are read; forward without filtering.
    #[wasm_bindgen(js_name = onRoomTimelineEvents)]
    pub fn on_room_timeline_events(
        &mut self,
        room_id: String,
        #[wasm_bindgen(unchecked_param_type = "TimelineEventIn[]")] events: JsValue,
    ) -> Result<(), JsError> {
        let events = parse_timeline_events(&room_id, events)?;
        self.inner.on_room_timeline_events(&room_id, &events);
        Ok(())
    }

    /// Lowers whichever hand the redacted `event_id` raised, in every session
    /// of `room_id`. Feed every redaction of the room.
    #[wasm_bindgen(js_name = onEventRedacted)]
    pub fn on_event_redacted(&mut self, room_id: String, event_id: String) {
        self.inner.on_event_redacted(&room_id, &event_id);
    }

    /// Membership events in `room_id` whose annotations have not been fetched
    /// yet, as `RelationLookup[]`. Answer each with
    /// `client.relations(roomId, membership_event_id, "m.annotation", "m.reaction")`
    /// through `onRelationsReceived`.
    #[wasm_bindgen(js_name = pendingRelationLookups, unchecked_return_type = "RelationLookup[]")]
    pub fn pending_relation_lookups(&self, room_id: String) -> Result<JsValue, JsError> {
        let lookups = self.inner.pending_relation_lookups(&room_id);
        serde_wasm_bindgen::to_value(&lookups).map_err(|err| JsError::new(&err.to_string()))
    }

    /// Feeds the annotations of one membership event back (same shape as
    /// `onRoomTimelineEvents`; `[]` when there were none).
    #[wasm_bindgen(js_name = onRelationsReceived)]
    pub fn on_relations_received(
        &mut self,
        room_id: String,
        target_event_id: String,
        #[wasm_bindgen(unchecked_param_type = "TimelineEventIn[]")] events: JsValue,
    ) -> Result<(), JsError> {
        let events = parse_timeline_events(&room_id, events)?;
        self.inner
            .on_relations_received(&room_id, &target_event_id, &events);
        Ok(())
    }

    /// Sends an Element Call emoji reaction from one session; `name` is what
    /// peers pick a sound by (see `reactionCatalog()`). Resolves with the
    /// event id; rejects inside the send cooldown.
    #[wasm_bindgen(js_name = sendReaction)]
    pub async fn send_reaction(
        &mut self,
        room_id: String,
        slot_id: String,
        emoji: String,
        name: String,
    ) -> Result<String, JsError> {
        self.inner
            .send_reaction(&room_id, &slot_id, &emoji, &name)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Raises our hand in one session. Idempotent while it is up.
    #[wasm_bindgen(js_name = raiseHand)]
    pub async fn raise_hand(&mut self, room_id: String, slot_id: String) -> Result<(), JsError> {
        self.inner
            .raise_hand(&room_id, &slot_id)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Lowers our hand in one session. A no-op when it is down.
    #[wasm_bindgen(js_name = lowerHand)]
    pub async fn lower_hand(&mut self, room_id: String, slot_id: String) -> Result<(), JsError> {
        self.inner
            .lower_hand(&room_id, &slot_id)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// The raised hands of one session, oldest first, as `RaisedHand[]`;
    /// empty if there is no such session.
    #[wasm_bindgen(js_name = raisedHands, unchecked_return_type = "RaisedHand[]")]
    pub fn raised_hands(&self, room_id: String, slot_id: String) -> Result<JsValue, JsError> {
        let hands = self
            .inner
            .raised_hands(&room_id, &slot_id)
            .unwrap_or_default();
        serde_wasm_bindgen::to_value(&hands).map_err(|err| JsError::new(&err.to_string()))
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
        #[wasm_bindgen(unchecked_param_type = "LeaveParamsIn")] params: JsValue,
    ) -> Result<(), JsError> {
        let params: WasmLeaveSessionParams = serde_wasm_bindgen::from_value(params)
            .map_err(|err| JsError::new(&format!("invalid leave params: {err}")))?;

        log::info!(
            "manager: leave requested [{room_id}/{slot_id}] reason={:?}",
            params.leave_reason,
        );

        let core_params = params.into_core();

        let left_room = room_id.clone();
        let result = self
            .inner
            .leave(room_id, slot_id, core_params)
            .await
            .map_err(|err| JsError::new(&err.to_string()));

        match &result {
            Ok(()) => {
                log::info!("manager: leave succeeded");
                // After the leave, which had to be rendered in the room's
                // dialect. Cleared on success only: a failed leave may retry
                // and still needs it.
                self.element_call_compat.remove(&left_room);
                if let Some(sender) = &self.command_sender {
                    sender.clear_dialect(&left_room);
                }
            }
            Err(_) => log::warn!("manager: leave failed"),
        }

        result
    }

    /// Restarts the keep-alive for one session: reschedules the delayed leave,
    /// and re-sends the membership if its sticky entry is halfway to expiring.
    /// Also flushes a key rotation that has come due.
    ///
    /// The core arms no timers and this binding starts no driver — **the page
    /// must call this on an interval while joined** (`setInterval`,
    /// [`HEARTBEAT_INTERVAL_MS`]), or the dead man's switch fires and peers see
    /// us depart mid-call.
    ///
    /// Resolves to `false` if there is no joined session for
    /// `(room_id, slot_id)`, which means there is nothing to keep alive.
    pub async fn heartbeat(&mut self, room_id: String, slot_id: String) -> bool {
        self.inner.heartbeat(&room_id, &slot_id).await
    }

    /// When the session's next key rotation falls due, in epoch milliseconds,
    /// or `undefined` when none is owed. Diagnostics: the rotation itself is
    /// performed by [`Self::heartbeat`] and by the media layer's
    /// switch-complete signal, not by polling this.
    #[wasm_bindgen(js_name = keyRotationDueAtMs)]
    pub fn key_rotation_due_at_ms(&self, room_id: String, slot_id: String) -> Option<f64> {
        self.inner
            .key_rotation_due_at_ms(&room_id, &slot_id)
            .map(|at| at as f64)
    }

    /// Performs the session's key rotation if one has come due; a no-op
    /// otherwise. Resolves to whether a rotation ran.
    #[wasm_bindgen(js_name = flushDueKeyRotation)]
    pub async fn flush_due_key_rotation(&mut self, room_id: String, slot_id: String) -> bool {
        self.inner.flush_due_key_rotation(&room_id, &slot_id).await
    }

    /// Feeds a decrypted `m.rtc.encryption_key` to-device message to every
    /// session of its room. Call for each such message the host's sync
    /// delivers; without it peers' media never becomes decryptable.
    ///
    /// `key` is `{ room_id, member_id, key_b64, key_index, was_encrypted,
    /// sender_user_id?, sender_device_id?, sender_is_cross_signed? }` — the
    /// `sender_*` fields come from the host's Olm decryption metadata, not from
    /// the payload.
    #[wasm_bindgen(js_name = receiveEncryptionKey)]
    pub async fn receive_encryption_key(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ReceivedKeyIn")] key: JsValue,
    ) -> Result<(), JsError> {
        let key: WasmReceivedEncryptionKey = serde_wasm_bindgen::from_value(key)
            .map_err(|err| JsError::new(&format!("invalid encryption key payload: {err}")))?;

        // Never the key material itself — only what decides whether it is
        // accepted. Its length is enough to spot a truncated or empty key.
        log::debug!(
            "manager: [{}] encryption key in: member={} index={} len={} encrypted={} \
             cross_signed={}",
            key.room_id,
            key.member_id,
            key.key_index,
            key.key_b64.len(),
            key.was_encrypted,
            key.sender_is_cross_signed,
        );

        let received = key.into_core()?;
        self.inner
            .receive_encryption_key(received)
            .await
            .map_err(|err| {
                log::warn!("manager: encryption key rejected: {err}");
                JsError::new(&err.to_string())
            })
    }

    /// Applies the **complete** current membership for one room, from raw
    /// event content, across every MatrixRTC generation the room contains.
    ///
    /// The counterpart of [`Self::set_current_sticky_state`] for pages talking
    /// to pre-2026 Element Call, and it replaces rather than merges for the
    /// same reason: a member does not only leave by sending a leave, and a
    /// lapsed entry produces no event to feed in.
    ///
    /// - `member_events` — the room's live `m.rtc.member` sticky events:
    ///   `[{ sender, sender_device_id?, was_encrypted?, type, content }]` with
    ///   `content` verbatim. The pre-2026 normalisation runs on every one and
    ///   is safe for spec-current content, so a mixed room needs no sorting.
    /// - `legacy_state_events` — the room's `org.matrix.msc3401.call.member`
    ///   **room state**, for the `state_events` mode:
    ///   `[{ sender, state_key, origin_server_ts, content }]`. Pass `[]`
    ///   otherwise — both sources go in one call because both are replaced by
    ///   it.
    #[wasm_bindgen(js_name = setCurrentMembership)]
    pub async fn set_current_membership(
        &mut self,
        room_id: String,
        #[wasm_bindgen(unchecked_param_type = "RawMemberEventIn[]")] member_events: JsValue,
        #[wasm_bindgen(unchecked_param_type = "LegacyStateMemberEventIn[]")]
        legacy_state_events: JsValue,
    ) -> Result<(), JsError> {
        let member_events: Vec<compat::WasmRawMemberEvent> =
            serde_wasm_bindgen::from_value(member_events).map_err(|err| {
                log::warn!("manager: [{room_id}] invalid membership payload: {err}");
                JsError::new(&format!("invalid membership payload: {err}"))
            })?;
        let legacy_state_events: Vec<compat::WasmLegacyStateMemberEvent> =
            serde_wasm_bindgen::from_value(legacy_state_events).map_err(|err| {
                log::warn!("manager: [{room_id}] invalid legacy state payload: {err}");
                JsError::new(&format!("invalid legacy state payload: {err}"))
            })?;

        log::debug!(
            "manager: [{room_id}] current membership in: {} sticky + {} pre-sticky state event(s)",
            member_events.len(),
            legacy_state_events.len(),
        );

        let current = ingest::merge_current_membership(
            &room_id,
            member_events.into_iter().map(Into::into).collect(),
            legacy_state_events.into_iter().map(Into::into).collect(),
        );

        self.inner
            .set_current_sticky_state(&room_id, current)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Feeds a decrypted pre-2026 `io.element.call.encryption_keys` to-device
    /// message to every session of its room.
    ///
    /// The legacy counterpart of [`Self::receive_encryption_key`], taking the
    /// content raw because the two generations disagree about where the key,
    /// the index and the owning membership live. Without it a legacy peer's
    /// media never becomes decryptable — the roster fills in, everything looks
    /// joined, and every remote tile stays black.
    ///
    /// `payload` is `{ sender, content, was_encrypted, sender_device_id?,
    /// sender_is_cross_signed? }` — the `sender_*` fields from Olm decryption
    /// metadata, `was_encrypted` honest (a cleartext key is discarded by the
    /// core, as MSC4143 requires). The mode the room was joined in decides how
    /// the key is bound; this call knows it already.
    #[wasm_bindgen(js_name = receiveLegacyEncryptionKey)]
    pub async fn receive_legacy_encryption_key(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "LegacyKeyIn")] payload: JsValue,
    ) -> Result<(), JsError> {
        let payload: compat::WasmLegacyKeyMessage = serde_wasm_bindgen::from_value(payload)
            .map_err(|err| JsError::new(&format!("invalid legacy key payload: {err}")))?;

        let room_id = payload
            .content
            .get("room_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let mode = self.element_call_compat_for(&room_id);

        let Some(key) = ingest::parse_legacy_key(
            mode,
            &payload.sender,
            payload.sender_device_id.as_deref(),
            &payload.content,
        ) else {
            // Not an error the page can act on — the message is simply
            // unusable — but silence here reads as a key that never arrived,
            // which is the hardest interop failure to diagnose.
            log::warn!(
                "manager: [{room_id}] ignoring a legacy encryption key from {}: it is missing a \
                 required field, or names no device to bind it to. That peer's media will not \
                 decrypt.",
                payload.sender,
            );
            return Ok(());
        };

        log::debug!(
            "manager: [{}] legacy encryption key in: member={} index={} len={} encrypted={} \
             cross_signed={}",
            key.room_id,
            key.member_id,
            key.key_index,
            key.key_b64.len(),
            payload.was_encrypted,
            payload.sender_is_cross_signed,
        );

        let received = WasmReceivedEncryptionKey {
            room_id: key.room_id,
            member_id: key.member_id,
            key_b64: key.key_b64,
            key_index: key.key_index,
            was_encrypted: payload.was_encrypted,
            sender_user_id: Some(payload.sender),
            sender_device_id: payload.sender_device_id,
            sender_is_cross_signed: payload.sender_is_cross_signed,
        }
        .into_core()?;

        self.inner
            .receive_encryption_key(received)
            .await
            .map_err(|err| {
                log::warn!("manager: legacy encryption key rejected: {err}");
                JsError::new(&err.to_string())
            })
    }

    /// Opens a slot by publishing its `m.rtc.slot` state event.
    ///
    /// `slot_id` must start with `{application_type}#` (MSC4143 makes the slot
    /// id the state key and requires that shape). `encryption` is the raw
    /// MSC4143 `content.encryption` object — `{ type: "m.per_member" }` in an
    /// encrypted room, `null`/`undefined` elsewhere; the mismatch resolves the
    /// slot closed for everyone. Publishing room state usually needs a raised
    /// power level, so a rejection by the homeserver surfaces here.
    #[wasm_bindgen(js_name = openSlot)]
    pub async fn open_slot(
        &mut self,
        room_id: String,
        slot_id: String,
        application_type: String,
        #[wasm_bindgen(unchecked_param_type = "SlotEncryptionIn | null | undefined")]
        encryption: JsValue,
    ) -> Result<(), JsError> {
        let encryption: Option<SlotEncryption> = serde_wasm_bindgen::from_value(encryption)
            .map_err(|err| JsError::new(&format!("invalid slot encryption payload: {err}")))?;

        log::info!(
            "manager: [{room_id}/{slot_id}] opening slot: application={application_type} \
             encryption={encryption:?}",
        );

        self.inner
            .open_slot(room_id, slot_id, application_type, encryption)
            .await
            .map_err(|err| {
                log::warn!("manager: could not open the slot: {err}");
                JsError::new(&err.to_string())
            })
    }

    /// Closes a slot, by setting its `m.rtc.slot` status to `closed`.
    ///
    /// Every member of it becomes left as soon as clients apply the new state —
    /// this ends the call for everyone, not just for us. Leaving is
    /// [`Self::leave`].
    #[wasm_bindgen(js_name = closeSlot)]
    pub async fn close_slot(&mut self, room_id: String, slot_id: String) -> Result<(), JsError> {
        log::info!("manager: [{room_id}/{slot_id}] closing slot");

        self.inner
            .close_slot(room_id, slot_id)
            .await
            .map_err(|err| {
                log::warn!("manager: could not close the slot: {err}");
                JsError::new(&err.to_string())
            })
    }
}

/// How often a page should call [`WasmRtcSessionManager::heartbeat`] while
/// joined. Matches the FFI's keep-alive driver interval.
/// One message-like room event for the reactions intake, as JS hands it in.
#[derive(Debug, Deserialize)]
struct WasmTimelineEvent {
    #[serde(default)]
    room_id: Option<String>,
    event_id: String,
    sender: String,
    #[serde(default)]
    sender_device_id: Option<String>,
    #[serde(default)]
    was_encrypted: Option<bool>,
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    origin_server_ts: u64,
    #[serde(default)]
    content: serde_json::Value,
}

fn parse_timeline_events(
    room_id: &str,
    events: JsValue,
) -> Result<Vec<matrix_rtc_core::RawTimelineEvent>, JsError> {
    let events: Vec<WasmTimelineEvent> = serde_wasm_bindgen::from_value(events).map_err(|err| {
        log::warn!("manager: [{room_id}] invalid timeline event payload: {err}");
        JsError::new(&format!("invalid timeline event payload: {err}"))
    })?;
    Ok(events
        .into_iter()
        .map(|event| matrix_rtc_core::RawTimelineEvent {
            room_id: event.room_id.unwrap_or_else(|| room_id.to_owned()),
            event_id: event.event_id,
            sender: event.sender,
            origin: match event.was_encrypted {
                Some(true) => matrix_rtc_core::EventOrigin::encrypted(event.sender_device_id),
                Some(false) => matrix_rtc_core::EventOrigin::Cleartext,
                None => matrix_rtc_core::EventOrigin::Unknown,
            },
            event_type: event.event_type,
            origin_server_ts: event.origin_server_ts,
            content: event.content,
        })
        .collect())
}

/// Element Call's reaction catalogue, as `ReactionKind[]` in the order its
/// picker shows them. The `sound` of each entry is the base name of the asset
/// to bundle and play; `reactionSoundFor` resolves a received `name`.
#[wasm_bindgen(js_name = reactionCatalog, unchecked_return_type = "ReactionKind[]")]
pub fn reaction_catalog() -> Result<JsValue, JsError> {
    #[derive(Serialize)]
    struct Kind {
        name: &'static str,
        emoji: &'static str,
        sound: Option<&'static str>,
    }
    let catalogue: Vec<Kind> = matrix_rtc_core::KNOWN_REACTIONS
        .iter()
        .map(|kind| Kind {
            name: kind.name,
            emoji: kind.emoji,
            sound: kind.sound,
        })
        .collect();
    serde_wasm_bindgen::to_value(&catalogue).map_err(|err| JsError::new(&err.to_string()))
}

/// The sound asset to play for a reaction `name`: a catalogue entry's sound,
/// `"generic"` for a name outside the catalogue, or `undefined` for a silent
/// one.
#[wasm_bindgen(js_name = reactionSoundFor)]
pub fn reaction_sound_for(name: String) -> Option<String> {
    matrix_rtc_core::sound_for(&name)
        .asset_name()
        .map(str::to_owned)
}

#[wasm_bindgen(js_name = HEARTBEAT_INTERVAL_MS)]
pub fn heartbeat_interval_ms() -> u32 {
    10_000
}

/// A decrypted `m.rtc.encryption_key` to-device message, as JS hands it in.
#[derive(Debug, Deserialize)]
struct WasmReceivedEncryptionKey {
    /// The `room_id` carried in the message content.
    room_id: String,
    /// The `member_id` carried in the message content.
    member_id: String,
    /// The key material, encoded per the message's `format`.
    key_b64: String,
    /// The rolling key index (0-255).
    key_index: u8,
    /// Whether the to-device message arrived Olm-encrypted. MSC4143 requires
    /// it; the core discards cleartext keys.
    was_encrypted: bool,
    /// User the message was decrypted as coming from (required when
    /// `was_encrypted`).
    #[serde(default)]
    sender_user_id: Option<String>,
    /// Device the message was decrypted as coming from, when attributable.
    #[serde(default)]
    sender_device_id: Option<String>,
    /// Whether that device is cross-signed (MSC4153).
    #[serde(default)]
    sender_is_cross_signed: bool,
}

impl WasmReceivedEncryptionKey {
    fn into_core(self) -> Result<ReceivedEncryptionKey, JsError> {
        let origin = if self.was_encrypted {
            KeyOrigin::Encrypted {
                sender_user_id: self
                    .sender_user_id
                    .ok_or_else(|| JsError::new("an encrypted key needs its sender_user_id"))?,
                sender_device_id: self.sender_device_id,
                sender_is_cross_signed: self.sender_is_cross_signed,
            }
        } else {
            KeyOrigin::Cleartext
        };
        Ok(ReceivedEncryptionKey {
            origin,
            room_id: self.room_id,
            member_id: self.member_id,
            key_b64: self.key_b64,
            key_index: self.key_index,
        })
    }
}

impl WasmRtcSessionManager {
    /// The mode `room_id` was joined in; an unregistered room is spec-current.
    pub(crate) fn element_call_compat_for(&self, room_id: &str) -> ElementCallCompat {
        self.element_call_compat
            .get(room_id)
            .cloned()
            .unwrap_or_default()
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
    /// Element Call compatibility generation to speak in this room:
    /// `"off"` (the default), `"sticky_events"`, or `"state_events"`.
    ///
    /// Chosen per join and remembered for the room, because it decides more
    /// than the wire format of one event: the `member.id` we join with, how an
    /// inbound media key is bound, the SFU participant identity, and the token
    /// endpoint. See the FFI's `compat` module docs for what a host must do
    /// differently per mode; on the web the same applies with
    /// `setCurrentMembership` / `receiveLegacyEncryptionKey` and the client
    /// object's `sendDelayedStateEvent`.
    #[serde(default)]
    pub element_call_compat: Option<String>,
    /// How this session handles Element Call reactions and the raised hand.
    /// Omitted is enabled with Element Call's three-second window.
    #[serde(default)]
    pub reactions: Option<WasmReactionsConfig>,
}

/// WASM-friendly reactions configuration (mirrors the core's
/// `ReactionsConfig`; every field defaults).
#[derive(Debug, Deserialize)]
pub struct WasmReactionsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_reaction_window_ms")]
    pub active_window_ms: u64,
    #[serde(default = "default_reaction_window_ms")]
    pub send_cooldown_ms: u64,
}

fn default_true() -> bool {
    true
}

fn default_reaction_window_ms() -> u64 {
    matrix_rtc_core::DEFAULT_REACTION_ACTIVE_MS
}

impl From<WasmReactionsConfig> for matrix_rtc_core::ReactionsConfig {
    fn from(value: WasmReactionsConfig) -> Self {
        matrix_rtc_core::ReactionsConfig {
            enabled: value.enabled,
            active_window_ms: value.active_window_ms,
            send_cooldown_ms: value.send_cooldown_ms,
        }
    }
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
            reactions: self.reactions.map(Into::into),
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
    pub fn setup_command_sender(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "MatrixClientHost")] client: JsValue,
    ) {
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
    pub async fn set_current_sticky_state(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "StickyEventIn[]")] events: JsValue,
    ) -> Result<(), JsError> {
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
    pub async fn join(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "JoinParamsIn")] params: JsValue,
    ) -> Result<String, JsError> {
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
    pub fn leave(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "LeaveParamsIn")] params: JsValue,
    ) -> Result<(), JsError> {
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
    #[wasm_bindgen(unchecked_return_type = "MembershipSnapshot[] | null")]
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
    /// The event's id; reactions and the raised hand relate to it.
    #[serde(default)]
    event_id: Option<String>,
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
    /// On the wire this is the MSC4354 unstable id `msc4354_sticky_key` (what
    /// every peer — this SDK included — currently writes); the plain spelling
    /// is what the MSC lands as. Two fields rather than a serde `alias`:
    /// captured Element Call events carry BOTH, which an alias rejects as a
    /// duplicate. The unstable spelling wins when they disagree.
    #[serde(default)]
    msc4354_sticky_key: Option<String>,
    #[serde(default)]
    sticky_key: Option<String>,
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
            event_id: value.event_id,
            sender: value.sender,
            origin: match value.was_encrypted {
                Some(true) => matrix_rtc_core::EventOrigin::encrypted(value.sender_device_id),
                Some(false) => matrix_rtc_core::EventOrigin::Cleartext,
                None => matrix_rtc_core::EventOrigin::Unknown,
            },
            event_type: value.event_type,
            content: matrix_rtc_core::RawStickyEventContent {
                slot_id: value.content.slot_id,
                // Empty when the event carries neither spelling; the core's
                // validation then drops the event with a logged reason rather
                // than this conversion failing the whole snapshot.
                sticky_key: value
                    .content
                    .msc4354_sticky_key
                    .or(value.content.sticky_key)
                    .unwrap_or_default(),
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
                created_ts: None,
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

    /// A JS mock of the Matrix client the manager's command sender dispatches
    /// on, answering every method with a resolved Promise.
    fn mock_client() -> JsValue {
        let client = js_sys::Object::new();
        let set = |name: &str, args: &str, body: &str| {
            let function = js_sys::Function::new_with_args(args, body);
            js_sys::Reflect::set(&client, &JsValue::from_str(name), &function).unwrap();
        };
        // Contents must arrive as plain objects: a real Matrix client
        // `JSON.stringify`s them, and an ES `Map` (the default
        // serde-wasm-bindgen output for serde maps) stringifies to `{}`.
        set(
            "sendStickyEvent",
            "roomId,eventType,content,durationMs",
            "if (JSON.stringify(content) === '{}' || content.member === undefined) \
                 return Promise.reject(new Error('content is not a plain object')); \
             return Promise.resolve({ event_id: '$sticky' });",
        );
        set(
            "sendDelayedEvent",
            "roomId,eventType,content,delayMs",
            "if (JSON.stringify(content) === '{}') \
                 return Promise.reject(new Error('content is not a plain object')); \
             return Promise.resolve('delay-id');",
        );
        set(
            "restartDelayedEvent",
            "roomId,delayId",
            "return Promise.resolve();",
        );
        set(
            "cancelDelayedEvent",
            "roomId,delayId",
            "return Promise.resolve();",
        );
        set(
            "sendStateEvent",
            "roomId,eventType,stateKey,content",
            "return Promise.resolve({ event_id: '$state' });",
        );
        client.into()
    }

    /// A full join on the wasm target. Beyond the join flow itself, this pins
    /// the clock: the join path reads wall-clock time, which on
    /// wasm32-unknown-unknown must come from `Date.now()` (web-time) —
    /// `std::time::SystemTime::now()` traps there.
    #[wasm_bindgen_test]
    async fn a_join_succeeds_on_wasm() {
        #[derive(Serialize)]
        struct TestJoinParams {
            user_id: &'static str,
            device_id: &'static str,
            room_id: &'static str,
            slot_id: &'static str,
            application: &'static str,
        }

        let mut manager = WasmRtcSessionManager::new();
        manager.setup_command_sender(mock_client());

        let params = serde_wasm_bindgen::to_value(&TestJoinParams {
            user_id: "@alice:example.org",
            device_id: "DEVICEID",
            room_id: "!room:example.org",
            slot_id: "m.call#ROOM",
            application: "m.call",
        })
        .unwrap();

        let member_id = manager.join(params).await.expect("join should succeed");
        assert!(!member_id.is_empty());
        assert_eq!(
            manager.own_member_id("!room:example.org".to_owned(), "m.call#ROOM".to_owned()),
            Some(member_id),
        );

        // And the keep-alive can run: this is the other wall-clock reader.
        assert!(
            manager
                .heartbeat("!room:example.org".to_owned(), "m.call#ROOM".to_owned())
                .await
        );
    }

    /// An Element Call 2025 membership as captured on the wire
    /// (`skills/e2e-testing/references/Alice-Bob-Call-Events.md`): flat
    /// `rtc_transports`, `member` with `user_id`/`device_id` and no
    /// `membership` field, `versions: []`. The raw funnel must normalise it
    /// into a counted member.
    #[wasm_bindgen_test]
    async fn set_current_membership_reads_the_ec_2025_wire_shape() {
        #[derive(Serialize)]
        struct Empty {}

        // The JSON-compatible serializer, so the fixture is plain objects —
        // what a real page passes (the default serializer would make ES Maps).
        let raw = serde_json::json!([{
            "sender": "@bob:synapse.othersite.m.localhost",
            "sender_device_id": "WDQHAPEYDK",
            "was_encrypted": true,
            "type": "org.matrix.msc4143.rtc.member",
            "content": {
                "application": { "type": "m.call", "m.call.intent": "video" },
                "slot_id": "m.call#ROOM",
                "rtc_transports": [{
                    "type": "livekit",
                    "livekit_service_url": "https://matrix-rtc.othersite.m.localhost/livekit/jwt",
                }],
                "member": {
                    "device_id": "WDQHAPEYDK",
                    "user_id": "@bob:synapse.othersite.m.localhost",
                    "id": "bcab799f-abae-4d38-bf1b-77238346349a",
                },
                "versions": [],
                "msc4354_sticky_key": "bcab799f-abae-4d38-bf1b-77238346349a",
            },
        }])
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .unwrap();
        let empty = serde_wasm_bindgen::to_value(&Vec::<Empty>::new()).unwrap();

        let mut manager = WasmRtcSessionManager::new();
        manager
            .set_current_membership("!room:example.org".to_owned(), raw, empty)
            .await
            .expect("the captured shape should ingest");
        assert_eq!(
            manager.member_count("!room:example.org".to_owned(), "m.call#ROOM".to_owned()),
            Some(1),
        );
    }

    /// The sticky dialect's outbound rewrite, through the real command path: a
    /// join in `sticky_events` mode must put the EC-2025 mirror fields on the
    /// wire, or that generation cannot see us.
    #[wasm_bindgen_test]
    async fn a_sticky_compat_join_mirrors_the_legacy_fields() {
        let client = js_sys::Object::new();
        let set = |name: &str, args: &str, body: &str| {
            let function = js_sys::Function::new_with_args(args, body);
            js_sys::Reflect::set(&client, &JsValue::from_str(name), &function).unwrap();
        };
        // Reject a membership missing any of the legacy mirror fields.
        set(
            "sendStickyEvent",
            "roomId,eventType,content,durationMs",
            "if (!Array.isArray(content.rtc_transports) \
                 || !Array.isArray(content.versions) \
                 || !content.member || content.member.user_id === undefined \
                 || content.member.device_id === undefined) \
                 return Promise.reject(new Error('legacy mirror fields missing: ' + JSON.stringify(content))); \
             return Promise.resolve({ event_id: '$sticky' });",
        );
        set(
            "sendDelayedEvent",
            "roomId,eventType,content,delayMs",
            "return Promise.resolve('delay-id');",
        );
        set(
            "restartDelayedEvent",
            "roomId,delayId",
            "return Promise.resolve();",
        );
        set(
            "cancelDelayedEvent",
            "roomId,delayId",
            "return Promise.resolve();",
        );

        #[derive(Serialize)]
        struct TestTransport {
            #[serde(rename = "type")]
            kind: &'static str,
            livekit_service_url: &'static str,
        }
        #[derive(Serialize)]
        struct TestJoinParams {
            user_id: &'static str,
            device_id: &'static str,
            room_id: &'static str,
            slot_id: &'static str,
            application: &'static str,
            transport: TestTransport,
            element_call_compat: &'static str,
        }

        let mut manager = WasmRtcSessionManager::new();
        manager.setup_command_sender(client.into());
        let params = serde_wasm_bindgen::to_value(&TestJoinParams {
            user_id: "@alice:example.org",
            device_id: "ALICEDEVICE",
            room_id: "!room:example.org",
            slot_id: "m.call#ROOM",
            application: "m.call",
            transport: TestTransport {
                kind: "livekit",
                livekit_service_url: "https://sfu.example.org/livekit/jwt",
            },
            element_call_compat: "sticky_events",
        })
        .unwrap();

        manager
            .join(params)
            .await
            .expect("the rewritten membership should satisfy the strict mock");
    }

    /// The `sticky_events` binding of a legacy key: bound by the `member.id`
    /// the message carries, fed through the raw payload contract.
    #[wasm_bindgen_test]
    async fn receive_legacy_encryption_key_accepts_the_documented_payload() {
        #[derive(Serialize)]
        struct TestLegacyKey {
            sender: &'static str,
            content: serde_json::Value,
            was_encrypted: bool,
            sender_device_id: &'static str,
            sender_is_cross_signed: bool,
        }

        let mut manager = WasmRtcSessionManager::new();
        // No session exists: the key is parsed, then dropped — not an error.
        let payload = serde_wasm_bindgen::to_value(&TestLegacyKey {
            sender: "@peer:example.org",
            content: serde_json::json!({
                "keys": [{ "index": 0, "key": "AAAA" }],
                "member": { "id": "bcab799f-abae-4d38-bf1b-77238346349a" },
                "device_id": "PEERDEVICE",
                "room_id": "!room:example.org",
            }),
            was_encrypted: true,
            sender_device_id: "PEERDEVICE",
            sender_is_cross_signed: true,
        })
        .unwrap();
        assert!(manager.receive_legacy_encryption_key(payload).await.is_ok());
    }

    #[wasm_bindgen_test]
    async fn heartbeat_without_a_session_reports_nothing_to_keep_alive() {
        let mut manager = WasmRtcSessionManager::new();
        assert!(
            !manager
                .heartbeat("!room:example.org".to_owned(), "m.call#ROOM".to_owned())
                .await
        );
    }

    /// Pins the JS payload contract of `receiveEncryptionKey`: snake_case
    /// fields, optional `sender_*`, and the encrypted-needs-a-sender rule.
    #[wasm_bindgen_test]
    async fn receive_encryption_key_accepts_the_documented_payload() {
        // A plain JS object, as a page would pass. `serde_json::json!` is no
        // substitute here: serde-wasm-bindgen turns its maps into ES `Map`s.
        #[derive(Serialize)]
        struct TestKey {
            room_id: &'static str,
            member_id: &'static str,
            key_b64: &'static str,
            key_index: u8,
            was_encrypted: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            sender_user_id: Option<&'static str>,
        }

        let key = TestKey {
            room_id: "!room:example.org",
            member_id: "alice-device-a",
            key_b64: "AAAA",
            key_index: 3,
            was_encrypted: true,
            sender_user_id: Some("@alice:example.org"),
        };

        let mut manager = WasmRtcSessionManager::new();
        // No session exists for the room: the key is dropped, not an error.
        let payload = serde_wasm_bindgen::to_value(&key).unwrap();
        assert!(manager.receive_encryption_key(payload).await.is_ok());

        let missing_sender = serde_wasm_bindgen::to_value(&TestKey {
            sender_user_id: None,
            ..key
        })
        .unwrap();
        assert!(
            manager
                .receive_encryption_key(missing_sender)
                .await
                .is_err(),
            "an encrypted key without its decrypted sender must be refused"
        );
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
