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

//! Multi-session routing for MatrixRTC sticky events.
//!
//! The manager owns many `RtcSession` instances and dispatches room-scoped
//! sticky snapshots/updates to the right session by `(room_id, slot_id)`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::watch;

use crate::commands::RtcCommandSender;
use crate::encryption::types::ReceivedEncryptionKey;
use crate::encryption::{EncryptionKeySignalHandler, RtcIdentityMapper};
use crate::error::{CommandError, JoinError, LeaveError};
use crate::event::{EventConversionError, RawStickyEvent};
use crate::join::{JoinSessionParams, LeaveSessionParams};
use crate::session::{CallMembershipEvent, JoinedMembership, RtcSession};
use crate::slot::{
    RawSlotEvent, RawSlotEventContent, RoomEncryption, SLOT_EVENT_TYPE, SlotEncryption, SlotState,
};

/// Holds and routes all active RTC sessions.
pub struct RtcSessionManager<T: RtcCommandSender> {
    sessions: HashMap<SessionKey, RtcSession<T>>,
    /// Command sender for sending events to Matrix rooms.
    /// This is passed to sessions when they are created or when they need to send commands.
    command_sender: Option<Arc<T>>,
    /// Slot events per `(room, slot)`, kept unresolved because resolving them
    /// also depends on the room's encryption state, which can arrive later or
    /// change. Held here as well as on the sessions so that state arriving
    /// before a session exists still applies when one is created.
    slots: HashMap<SessionKey, RawSlotEvent>,
    /// Rooms whose `m.rtc.slot` state has been supplied. A slot in one of these
    /// rooms with no entry in `slots` is closed, as opposed to unknown.
    rooms_with_slot_state: HashSet<String>,
    /// Joined room members per room, for rooms where the host supplies them.
    room_members: HashMap<String, HashSet<String>>,
    /// Room encryption per room, for rooms where the host reports it.
    room_encryption: HashMap<String, RoomEncryption>,
}

impl<T: RtcCommandSender + 'static> Default for RtcSessionManager<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: RtcCommandSender + 'static> RtcSessionManager<T> {
    // TODO(msc4143): add a manager-level lifecycle subscription API that emits
    // when sessions are created/removed (separate from per-session membership snapshots).
    /// Creates an empty session manager without a command sender.
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            command_sender: None,
            slots: HashMap::new(),
            rooms_with_slot_state: HashSet::new(),
            room_members: HashMap::new(),
            room_encryption: HashMap::new(),
        }
    }

    /// Creates an empty session manager with a command sender.
    pub fn with_command_sender(command_sender: Arc<T>) -> Self {
        Self {
            sessions: HashMap::new(),
            command_sender: Some(command_sender),
            slots: HashMap::new(),
            rooms_with_slot_state: HashSet::new(),
            room_members: HashMap::new(),
            room_encryption: HashMap::new(),
        }
    }

    /// Sets the command sender for this manager.
    pub fn set_command_sender(&mut self, command_sender: Arc<T>) {
        self.command_sender = Some(command_sender);
    }

    /// Returns true if this manager has a command sender configured.
    pub fn has_command_sender(&self) -> bool {
        self.command_sender.is_some()
    }

    /// Joins an RTC session with the given parameters.
    ///
    /// This will find or create the appropriate session for the given room_id and slot_id,
    /// and then call join on that session.
    ///
    /// # Arguments
    ///
    /// * `params` - The join parameters including user info, transport, etc.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the join completed successfully.
    /// Returns `Err(JoinError)` if validation fails, command sender not configured, or commands fail.
    pub async fn join(&mut self, params: JoinSessionParams) -> Result<(), JoinError> {
        let command_sender = self
            .command_sender
            .as_ref()
            .ok_or_else(|| {
                log::warn!(
                    "[{}/{}] join rejected: the manager has no command sender",
                    params.room_id,
                    params.slot_id,
                );
                JoinError::CommandError(crate::error::CommandError::from_message(
                    "no command sender configured",
                ))
            })?
            .clone();

        let key = SessionKey::new(params.room_id.clone(), params.slot_id.clone());
        let session = self.session_for_key(key);

        // If the session doesn't have a command sender yet, set it
        if !session.has_command_sender() {
            session.set_command_sender(command_sender);
        }

        session.join(params).await
    }

    /// Leaves an RTC session.
    ///
    /// This will find the appropriate session for the given room_id and slot_id,
    /// and then call leave on that session.
    ///
    /// The session is **kept**, not removed: it is keyed by `(room_id, slot_id)`
    /// and stays usable for a later join in the same process. See
    /// [`RtcSession::leave`] for why, and for what it does and does not clear —
    /// a rejoin therefore starts with the previous call's roster in place, which
    /// [`RtcSession::join`] accounts for.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID of the session to leave
    /// * `slot_id` - The slot ID of the session to leave
    /// * `params` - The leave parameters including the optional MSC4143 leave reason
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the leave completed successfully.
    /// Returns `Err(LeaveError)` if the session doesn't exist or other errors occur.
    pub async fn leave(
        &mut self,
        room_id: String,
        slot_id: String,
        params: LeaveSessionParams,
    ) -> Result<(), LeaveError> {
        let key = SessionKey::new(room_id, slot_id);
        let session = self.sessions.get_mut(&key).ok_or_else(|| {
            log::warn!("[{key}] leave rejected: no such session");
            LeaveError::CommandError(crate::error::CommandError::from_message(
                "session not found",
            ))
        })?;

        session.leave(params).await
    }

    /// Applies the **complete** current sticky state for one room.
    ///
    /// Replaces, rather than merges: a member absent from `events` is gone.
    ///
    /// That is what lets a host hand over what it has without doing any
    /// resolution of its own. An MSC4354 sticky entry lapses when its owner
    /// stops refreshing it — a crashed client — and the entry then simply
    /// disappears from the map. A host that only ever sees the current state
    /// (which is what matrix-sdk-ffi delivers: it collapses the SDK's delta to a
    /// snapshot before it crosses the boundary) has no way to say "this one
    /// expired" beyond its absence. Diffing here means every host does not have
    /// to.
    ///
    /// This is the only way membership reaches the core; there is deliberately
    /// no delta entry point. A delta carried nothing extra — the core flattens
    /// every removal to a plain leave, and an explicit leave arrives inside the
    /// current state anyway, as a leave-shaped sticky replacing the join under
    /// the same key.
    ///
    /// Safe to call repeatedly: re-assert the full state whenever the host's
    /// sticky map changes. Passing an empty list clears the room.
    ///
    /// # One call, at most one key rotation
    ///
    /// Everything the state says is applied before the roster is republished, so
    /// a change of any size costs at most one rotation — three people hanging up
    /// together mint one key between them, not three. Key rotation has no
    /// debounce of its own and cannot have one (the core owns no timer), so this
    /// batching is the only thing standing between a busy call and a key per
    /// event.
    ///
    /// Two consequences for hosts: pass the state **whole**, and do not fan one
    /// snapshot out into several calls. Splitting it is not merely slower — each
    /// call is a complete state, so a partial one reads as everybody missing from
    /// it having left, which rotates the key and re-sends it to every remaining
    /// member.
    pub async fn set_current_sticky_state(
        &mut self,
        room_id: &str,
        events: impl IntoIterator<Item = RawStickyEvent>,
    ) -> Result<(), EventConversionError> {
        let mut batches: HashMap<SessionKey, Vec<CallMembershipEvent>> = HashMap::new();

        for event in events {
            if event.room_id != room_id {
                continue;
            }

            let Some(event) = self.try_convert_membership_event(event)? else {
                continue;
            };

            let slot_id = match &event {
                CallMembershipEvent::Joined(joined) => joined.slot_id.clone(),
                CallMembershipEvent::Left(left) => left.slot_id.clone(),
            };

            let key = SessionKey::new(room_id.to_owned(), slot_id);
            batches.entry(key).or_default().push(event);
        }

        // Every slot we already track in this room, not just the ones named in
        // `events`: a slot whose last member expired disappears from the payload
        // entirely, and leaving it untouched is precisely the ghost this call
        // exists to prevent. Such a slot gets an empty set, which clears it.
        let known_slots: Vec<SessionKey> = self
            .sessions
            .keys()
            .filter(|key| key.room_id == room_id)
            .cloned()
            .collect();
        for key in known_slots {
            batches.entry(key).or_default();
        }

        log::debug!(
            "[{room_id}] current sticky state routed to {} session(s): {}",
            batches.len(),
            describe_batches(&batches),
        );

        for (key, batch) in batches {
            self.session_for_key(key).set_current_state(batch).await;
        }

        Ok(())
    }

    /// Restarts the keep-alive delayed-leave for one `(room_id, slot_id)` session.
    ///
    /// Call periodically (e.g. every 15 s) while joined so the dead man's switch
    /// timer keeps getting pushed back. Returns `false` if no such session is
    /// joined.
    pub async fn heartbeat(&mut self, room_id: &str, slot_id: &str) -> bool {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        match self.sessions.get_mut(&key) {
            Some(session) => session.heartbeat().await,
            None => false,
        }
    }

    /// Returns the number of tracked sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Returns the member count for one `(room_id, slot_id)` session.
    pub fn member_count(&self, room_id: &str, slot_id: &str) -> Option<usize> {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        self.sessions.get(&key).map(RtcSession::member_count)
    }

    /// Subscribes to membership snapshots of one `(room_id, slot_id)` session
    /// (see [`RtcSession::subscribe_membership_snapshots`]), or `None` if the
    /// session does not exist.
    pub fn subscribe_membership_snapshots(
        &self,
        room_id: &str,
        slot_id: &str,
    ) -> Option<watch::Receiver<Vec<JoinedMembership>>> {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        self.sessions
            .get(&key)
            .map(RtcSession::subscribe_membership_snapshots)
    }

    /// Registers a media key signal handler for one `(room_id, slot_id)`
    /// session. Returns `false` if the session does not exist or has not joined.
    pub fn set_encryption_signal_handler(
        &mut self,
        room_id: &str,
        slot_id: &str,
        handler: Arc<dyn EncryptionKeySignalHandler>,
    ) -> bool {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        self.sessions
            .get_mut(&key)
            .is_some_and(|session| session.set_encryption_signal_handler(handler))
    }

    /// Our `member.id` in one `(room_id, slot_id)` session, or `None` if there
    /// is no such session or it has not joined.
    ///
    /// See [`RtcSession::own_member_id`]: it changes on every join, so read it
    /// rather than cache it.
    pub fn own_member_id(&self, room_id: &str, slot_id: &str) -> Option<String> {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        self.sessions
            .get(&key)
            .and_then(|session| session.own_member_id())
            .map(str::to_owned)
    }

    /// Re-signals every key one session already holds to its signal handler.
    ///
    /// Call after installing both the handler and the identity mapper: keys
    /// that arrived before the handler existed were stored but never signalled.
    /// Returns `false` if the session does not exist or has not joined.
    pub async fn replay_encryption_keys(&self, room_id: &str, slot_id: &str) -> bool {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        match self.sessions.get(&key) {
            Some(session) => session.replay_encryption_keys().await,
            None => false,
        }
    }

    /// When a coalesced key rotation falls due for one session, if one is owed.
    ///
    /// A consumer with a scheduler drives [`Self::flush_due_key_rotation`] from
    /// this. The deadline is the end of the current key's freshness, which is
    /// *later* than the end of its `delayBeforeUse` — so a consumer that only
    /// reacts to a key coming into use will find nothing due yet and has to come
    /// back at this instant.
    pub fn key_rotation_due_at_ms(&self, room_id: &str, slot_id: &str) -> Option<u64> {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        self.sessions
            .get(&key)
            .and_then(|session| session.key_rotation_due_at_ms())
    }

    /// Performs a key rotation that was coalesced into a fresh key's window, if one
    /// is owed and the window has closed.
    ///
    /// Membership changes arriving while a rotation is still propagating do not
    /// each mint a key — they are answered by one rotation at the end of the
    /// window (see `EncryptionManager::flush_due_rotation`). Nothing inside the
    /// core can perform it: it holds no timer, so the consumer that *does* enforce
    /// `delayBeforeUse` is the one positioned to call this the moment the window
    /// ends. `matrix-rtc-livekit`'s `MediaKeyBridge` drives it from the same
    /// scheduled wake-up that installs the key.
    ///
    /// A consumer that does not is not left broken, only late: [`Self::heartbeat`]
    /// performs any owed rotation too, so it lands within one heartbeat instead.
    ///
    /// Cheap and idempotent — a no-op unless a rotation is actually due. Returns
    /// `false` if the session does not exist or has not joined.
    pub async fn flush_due_key_rotation(&self, room_id: &str, slot_id: &str) -> bool {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        match self.sessions.get(&key) {
            Some(session) => session.flush_due_key_rotation().await,
            None => false,
        }
    }

    /// Installs the RTC-backend identity mapper for one `(room_id, slot_id)`
    /// session. Returns `false` if the session does not exist or has not joined.
    pub fn set_encryption_identity_mapper(
        &mut self,
        room_id: &str,
        slot_id: &str,
        mapper: RtcIdentityMapper,
    ) -> bool {
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        self.sessions
            .get_mut(&key)
            .is_some_and(|session| session.set_encryption_identity_mapper(mapper))
    }

    /// Routes a media encryption key received from a peer into every session in
    /// the room it names.
    ///
    /// The MSC4143 key to-device content carries no `slot_id`, so the key is
    /// fanned out to all sessions of the room. This is exact for the common
    /// single-slot-per-room case; multi-slot rooms would receive the key in
    /// every slot (harmless — unmatched keys are buffered/ignored).
    pub async fn receive_encryption_key(
        &self,
        received: ReceivedEncryptionKey,
    ) -> Result<(), CommandError> {
        for (key, session) in self.sessions.iter() {
            if key.room_id == received.room_id {
                session.receive_encryption_key(received.clone()).await?;
            }
        }
        Ok(())
    }

    /// Applies the `m.rtc.slot` state of a room.
    ///
    /// `slots` must be the room's complete set of `m.rtc.slot` state events:
    /// any slot in this room *not* named by it is taken to be closed. Calling
    /// this is what switches the room from "slot state unknown" (where the
    /// MSC4143 open-slot condition cannot be evaluated, so is not enforced) to
    /// enforcing it, so hosts should call it with whatever they have — an empty
    /// list included — as soon as room state is available, and again on every
    /// change.
    pub async fn on_room_slots_received(
        &mut self,
        room_id: &str,
        slots: impl IntoIterator<Item = RawSlotEvent>,
    ) {
        self.rooms_with_slot_state.insert(room_id.to_owned());

        // Replace this room's slots wholesale; a slot that vanished from room
        // state is closed, not merely stale.
        self.slots.retain(|key, _| key.room_id != room_id);
        let mut accepted = 0;
        for slot in slots {
            if slot.room_id != room_id {
                log::warn!(
                    "[{room_id}] ignoring a slot event for another room ({})",
                    slot.room_id,
                );
                continue;
            }
            let key = SessionKey::new(room_id.to_owned(), slot.slot_id.clone());
            self.slots.insert(key, slot);
            accepted += 1;
        }
        log::debug!("[{room_id}] room slot state replaced with {accepted} slot(s)");

        self.push_slot_state(room_id).await;
    }

    /// Forgets a room's `m.rtc.slot` state, so the open-slot condition stops
    /// being enforced there.
    ///
    /// The way back from [`Self::on_room_slots_received`], and the only one: that
    /// call is otherwise irreversible, because an empty slot list means "no open
    /// slots" rather than "I have nothing to say". This is the second statement,
    /// and it restores the [`SlotKnowledge::Unsupplied`] a room starts in.
    ///
    /// It exists for rooms where the condition is not merely unknown but
    /// *inapplicable* — a MatrixRTC generation older than `m.rtc.slot` itself, in
    /// which no client publishes one and reporting "no slots" would resolve every
    /// session closed and project every member out, the caller included. A host
    /// that simply has not fetched the state yet should say nothing at all rather
    /// than call this.
    ///
    /// [`SlotKnowledge::Unsupplied`]: crate::SlotKnowledge::Unsupplied
    pub async fn forget_room_slots(&mut self, room_id: &str) {
        let known = self.rooms_with_slot_state.remove(room_id);
        self.slots.retain(|key, _| key.room_id != room_id);
        if !known {
            return;
        }

        log::info!("[{room_id}] slot state forgotten; the open-slot condition is not enforced");
        for (key, session) in self.sessions.iter_mut() {
            if key.room_id != room_id {
                continue;
            }
            session.forget_slot_state().await;
        }
    }

    /// Reports whether a room is end-to-end encrypted.
    ///
    /// MSC4143 requires RTC encryption in encrypted rooms and forbids it
    /// elsewhere, so this changes how the room's slots resolve. Until a host
    /// calls it neither rule is applied.
    pub async fn on_room_encryption_received(&mut self, room_id: &str, encrypted: bool) {
        let encryption = if encrypted {
            RoomEncryption::Encrypted
        } else {
            RoomEncryption::Unencrypted
        };

        if self.room_encryption.insert(room_id.to_owned(), encryption) == Some(encryption) {
            return;
        }

        // Slots already held for this room resolve differently now.
        self.push_slot_state(room_id).await;

        for (key, session) in self.sessions.iter_mut() {
            if key.room_id != room_id {
                continue;
            }
            session.set_room_encryption(encryption).await;
        }
    }

    /// Resolves this room's slots against its current encryption state and
    /// pushes the result to every session in the room.
    async fn push_slot_state(&mut self, room_id: &str) {
        if !self.rooms_with_slot_state.contains(room_id) {
            log::debug!(
                "[{room_id}] no slot state supplied yet; the open-slot condition stays unenforced",
            );
            return;
        }

        let encryption = self.room_encryption_for(room_id);
        let resolved: HashMap<SessionKey, SlotState> = self
            .slots
            .iter()
            .filter(|(key, _)| key.room_id == room_id)
            .map(|(key, slot)| (key.clone(), slot.resolve(encryption)))
            .collect();

        log::debug!(
            "[{room_id}] slots resolved against encryption={encryption:?}: {}",
            resolved
                .iter()
                .map(|(key, state)| format!(
                    "{}={}",
                    key.slot_id,
                    if state.is_open() { "Open" } else { "Closed" },
                ))
                .collect::<Vec<_>>()
                .join(", "),
        );

        for (key, session) in self.sessions.iter_mut() {
            if key.room_id != room_id {
                continue;
            }
            let state = resolved.get(key).cloned().unwrap_or(SlotState::Closed);
            session.set_slot_state(state).await;
        }
    }

    fn room_encryption_for(&self, room_id: &str) -> RoomEncryption {
        self.room_encryption
            .get(room_id)
            .copied()
            .unwrap_or_default()
    }

    /// Sets the users currently joined to a room.
    ///
    /// MSC4143 only counts a member event as joined while its sender is still
    /// joined to the room. Until a host calls this, that condition is not
    /// enforced.
    pub async fn on_room_members_received(
        &mut self,
        room_id: &str,
        joined_user_ids: impl IntoIterator<Item = String>,
    ) {
        let members: HashSet<String> = joined_user_ids.into_iter().collect();
        self.room_members
            .insert(room_id.to_owned(), members.clone());

        for (key, session) in self.sessions.iter_mut() {
            if key.room_id != room_id {
                continue;
            }
            session.set_room_members(members.clone()).await;
        }
    }

    /// Opens a slot by sending an `m.rtc.slot` state event.
    ///
    /// The slot id doubles as the state key and MSC4143 requires it to start
    /// with `{application_type}#`, so that is checked here rather than letting
    /// the homeserver accept a slot every client will treat as closed.
    ///
    /// Sending room state usually needs a raised power level; a rejection
    /// surfaces as [`CommandError`].
    pub async fn open_slot(
        &self,
        room_id: String,
        slot_id: String,
        application_type: String,
        encryption: Option<SlotEncryption>,
    ) -> Result<(), CommandError> {
        if !slot_id.starts_with(&format!("{application_type}#")) {
            return Err(CommandError::from_message(format!(
                "slot id '{slot_id}' does not match application type '{application_type}': \
                 MSC4143 requires the state key to be '{{application_type}}#{{slot}}'"
            )));
        }

        let content = RawSlotEventContent::for_open(application_type, encryption);
        self.send_slot_state(room_id, slot_id, content).await
    }

    /// Closes a slot by setting its `m.rtc.slot` status to `closed`.
    ///
    /// Members of the slot become left as soon as the new state is applied.
    pub async fn close_slot(&self, room_id: String, slot_id: String) -> Result<(), CommandError> {
        self.send_slot_state(room_id, slot_id, RawSlotEventContent::for_close())
            .await
    }

    async fn send_slot_state(
        &self,
        room_id: String,
        slot_id: String,
        content: RawSlotEventContent,
    ) -> Result<(), CommandError> {
        let command_sender = self
            .command_sender
            .as_ref()
            .ok_or_else(|| CommandError::from_message("no command sender configured"))?;

        let content =
            serde_json::to_value(content).expect("m.rtc.slot content is always serializable");

        command_sender
            .send_state_event(room_id, SLOT_EVENT_TYPE.to_owned(), slot_id, content)
            .await
    }

    /// Everything the manager and its sessions believe, as JSON.
    ///
    /// Meant to be attached to a bug report or dumped to the log when a roster
    /// looks wrong: it answers "which sessions exist, what room state do they
    /// have, and why is each candidate in or out" in one shot. Contains no key
    /// material.
    pub fn debug_snapshot(&self) -> serde_json::Value {
        let sessions: serde_json::Map<String, serde_json::Value> = self
            .sessions
            .iter()
            .map(|(key, session)| (key.to_string(), session.debug_snapshot()))
            .collect();

        serde_json::json!({
            "has_command_sender": self.command_sender.is_some(),
            "session_count": self.sessions.len(),
            "rooms_with_slot_state": self.rooms_with_slot_state,
            "known_slots": self.slots.keys().map(SessionKey::to_string).collect::<Vec<_>>(),
            "room_encryption": self
                .room_encryption
                .iter()
                .map(|(room_id, encryption)| (room_id.clone(), format!("{encryption:?}").into()))
                .collect::<serde_json::Map<String, serde_json::Value>>(),
            "room_members": self
                .room_members
                .iter()
                .map(|(room_id, members)| (room_id.clone(), members.len().into()))
                .collect::<serde_json::Map<String, serde_json::Value>>(),
            "sessions": sessions,
        })
    }

    /// The resolved state of a slot, if its room's state has been supplied.
    pub fn slot_state(&self, room_id: &str, slot_id: &str) -> Option<SlotState> {
        if !self.rooms_with_slot_state.contains(room_id) {
            return None;
        }
        let key = SessionKey::new(room_id.to_owned(), slot_id.to_owned());
        Some(
            self.slots
                .get(&key)
                .map(|slot| slot.resolve(self.room_encryption_for(room_id)))
                .unwrap_or(SlotState::Closed),
        )
    }

    /// Returns the session for `key`, creating it if needed.
    ///
    /// A newly created session is seeded with whatever room state the manager
    /// already holds, so slot state that arrived before the session existed
    /// still governs it. Seeding is synchronous because a session with no
    /// members has nothing to republish.
    fn session_for_key(&mut self, key: SessionKey) -> &mut RtcSession<T> {
        let encryption = self.room_encryption_for(&key.room_id);
        let slot = self.rooms_with_slot_state.contains(&key.room_id).then(|| {
            self.slots
                .get(&key)
                .map(|slot| slot.resolve(encryption))
                .unwrap_or(SlotState::Closed)
        });
        let room_members = self.room_members.get(&key.room_id).cloned();
        let command_sender = self.command_sender.clone();
        let log_tag = key.to_string();

        self.sessions.entry(key).or_insert_with(|| {
            log::info!(
                "session created [{log_tag}] seeded with slot={} members={:?} encryption={encryption:?}",
                match &slot {
                    Some(state) if state.is_open() => "Open",
                    Some(_) => "Closed",
                    None => "Unsupplied",
                },
                room_members.as_ref().map(HashSet::len),
            );

            let mut session = match command_sender {
                Some(sender) => RtcSession::with_command_sender(sender),
                None => RtcSession::new(),
            };
            session.set_log_tag(log_tag);
            if let Some(slot) = slot {
                session.seed_slot_state(slot);
            }
            if let Some(room_members) = room_members {
                session.seed_room_members(room_members);
            }
            session.seed_room_encryption(encryption);
            session
        })
    }

    fn try_convert_membership_event(
        &self,
        event: RawStickyEvent,
    ) -> Result<Option<CallMembershipEvent>, EventConversionError> {
        match event.try_into_call_membership_event() {
            Ok(event) => Ok(Some(event)),
            // Not an RTC member event at all — the host feeds us its whole
            // sticky map, so this is routine.
            Err(EventConversionError::UnsupportedEventType { .. }) => Ok(None),
            Err(err) => {
                log::warn!("dropping a malformed sticky member event: {err}");
                Err(err)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionKey {
    room_id: String,
    slot_id: String,
}

impl SessionKey {
    fn new(room_id: String, slot_id: String) -> Self {
        Self { room_id, slot_id }
    }
}

/// `slot_id xN` per session, for the one-line routing summary.
///
/// Which slots a room's sticky events landed in is the first thing to check
/// when a roster looks wrong: a typo in `slot_id` silently creates a second,
/// empty session rather than failing.
fn describe_batches(batches: &HashMap<SessionKey, Vec<CallMembershipEvent>>) -> String {
    if batches.is_empty() {
        return "none".to_owned();
    }

    batches
        .iter()
        .map(|(key, batch)| format!("{} x{}", key.slot_id, batch.len()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `room_id/slot_id` — the correlation key every session-scoped log line is
/// prefixed with.
impl std::fmt::Display for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.room_id, self.slot_id)
    }
}
