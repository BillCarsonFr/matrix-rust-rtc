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

//! In-memory RTC session membership model.
//!
//! This module stores the current participant view for a single RTC session and
//! applies joined/left transitions from domain membership events produced by the
//! manager layer.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::sync::watch;

use crate::commands::RtcCommandSender;
use crate::encryption::types::ReceivedEncryptionKey;
use crate::encryption::{EncryptionKeySignalHandler, EncryptionManager, RtcIdentityMapper};
use crate::error::{CommandError, JoinError, LeaveError};
use crate::event::EventOrigin;
use crate::join::{JoinSessionParams, LeaveSessionParams, TransportIntent};
use crate::own_membership::{OwnMembershipMachine, transport_to_json};
use crate::slot::{RoomEncryption, SlotState};
use crate::transport::{MemberTransports, RtcTransport};

#[allow(unused_imports)]
use log::*;

/// What a session knows about the room state governing its slot.
///
/// MSC4143 makes an open `m.rtc.slot` a precondition for anyone being joined,
/// so "no open slot" and "nobody has told us about the slot" have to be
/// distinguished: the first means every member is left, the second means the
/// condition cannot be evaluated yet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SlotKnowledge {
    /// No room state has been supplied for this room, so the slot condition is
    /// not enforced. Hosts opt in by calling
    /// [`RtcSessionManager::on_room_slots_received`].
    ///
    /// [`RtcSessionManager::on_room_slots_received`]: crate::RtcSessionManager::on_room_slots_received
    #[default]
    Unsupplied,
    /// Room state has been supplied; this is the slot's resolved state. A slot
    /// with no state event in the room resolves to [`SlotState::Closed`].
    Known(SlotState),
}

fn sticky_keys(members: &[JoinedMembership]) -> Vec<&str> {
    members
        .iter()
        .map(|member| member.sticky_key.as_str())
        .collect()
}

fn difference<'a>(from: &[&'a str], without: &[&str]) -> Vec<&'a str> {
    from.iter()
        .filter(|key| !without.contains(key))
        .copied()
        .collect()
}

fn describe_exclusions(excluded: &[(&str, JoinCondition)]) -> String {
    excluded
        .iter()
        .map(|(sticky_key, condition)| format!("{sticky_key}={condition:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Log tag for a session created outside a
/// [`RtcSessionManager`](crate::RtcSessionManager), which is the only thing
/// that knows the `(room, slot)` a session belongs to.
const UNATTRIBUTED_LOG_TAG: &str = "-/-";

/// Why a candidate member event is, or is not, projected as joined.
///
/// A plain `bool` here made the most confusing failure in the whole SDK
/// invisible: a member vanishing from the roster because of room state they
/// have nothing to do with. Carrying the reason costs nothing and turns that
/// into one readable log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinCondition {
    /// Every MSC4143 condition this session can evaluate is satisfied.
    Joined,
    /// The slot is closed, so nobody is joined to it.
    SlotClosed,
    /// MSC4143 requires `m.rtc.member` to be encrypted in an encrypted room,
    /// and one that is not "MUST be considered left".
    UnencryptedInEncryptedRoom,
    /// The sender is no longer joined to the room.
    SenderNotInRoom,
    /// A previous participation of *this* device, still sticky under an older
    /// `member_id`. MSC4143 mints a fresh `member.id` per join, so this is us one
    /// call ago, not a peer.
    SupersededOwnParticipation,
}

impl JoinCondition {
    fn is_joined(self) -> bool {
        matches!(self, JoinCondition::Joined)
    }
}

/// Who we are in the session's current join, for as long as we are joined.
///
/// Kept next to the membership machine so the join conditions can recognise our
/// own superseded participations. The `member_id` alone is not enough: telling a
/// stale participation of *this* device apart from another device of the same
/// user needs the device too.
#[derive(Clone, Debug)]
struct OwnParticipation {
    user_id: String,
    device_id: String,
    member_id: String,
}

/// Per-session MatrixRTC state machine and membership store.
pub struct RtcSession<T: RtcCommandSender> {
    /// Member events that are join-shaped and still sticky. These are
    /// candidates only: the remaining MSC4143 join conditions depend on room
    /// state, which can change under them at any time.
    candidates: Vec<JoinedMembership>,
    /// The candidates that currently satisfy every join condition. This is what
    /// gets published and what the encryption manager distributes keys to.
    members: Vec<JoinedMembership>,
    /// The slot this session's members are joined to.
    slot: SlotKnowledge,
    /// Users currently joined to the room, when the host supplies them; `None`
    /// leaves the room-membership condition unenforced.
    room_members: Option<HashSet<String>>,
    /// Whether the room is encrypted, which decides whether member events are
    /// required to be encrypted.
    room_encryption: RoomEncryption,
    membership_snapshots_tx: watch::Sender<Vec<JoinedMembership>>,
    /// Command sender for sending events to the Matrix room.
    command_sender: Option<Arc<T>>,
    /// Machine for managing our own membership lifecycle (join/leave/keep-alive).
    own_membership_machine: Option<OwnMembershipMachine<T>>,
    /// Identity of the current join, or `None` while not joined.
    own_participation: Option<OwnParticipation>,
    /// Encryption manager for key distribution and management.
    encryption_manager: Option<EncryptionManager<T>>,
    /// `room_id/slot_id`, prefixed to this session's log lines.
    ///
    /// A session does not otherwise know which slot it belongs to — the manager
    /// holds that in its key — so without this every line from a multi-call
    /// client is unattributable. Pre-formatted because it is used on paths that
    /// run per event.
    log_tag: String,
}

impl<T: RtcCommandSender> Clone for RtcSession<T> {
    fn clone(&self) -> Self {
        Self {
            candidates: self.candidates.clone(),
            members: self.members.clone(),
            slot: self.slot.clone(),
            room_members: self.room_members.clone(),
            room_encryption: self.room_encryption,
            membership_snapshots_tx: self.membership_snapshots_tx.clone(),
            command_sender: self.command_sender.clone(),
            own_membership_machine: None, // Don't clone the machine - it's not cloneable
            encryption_manager: None,     // Don't clone the encryption manager
            own_participation: None,      // A clone holds no machine, so it is not joined
            log_tag: self.log_tag.clone(),
        }
    }
}

impl<T: RtcCommandSender + 'static> RtcSession<T> {
    /// Creates an empty session without a command sender.
    pub fn new() -> Self {
        let (membership_snapshots_tx, _membership_snapshots_rx) = watch::channel(Vec::new());

        Self {
            candidates: Vec::new(),
            members: Vec::new(),
            slot: SlotKnowledge::default(),
            room_members: None,
            room_encryption: RoomEncryption::default(),
            membership_snapshots_tx,
            command_sender: None,
            own_membership_machine: None,
            encryption_manager: None,
            own_participation: None,
            log_tag: UNATTRIBUTED_LOG_TAG.to_owned(),
        }
    }

    /// Creates an empty session with a command sender.
    pub fn with_command_sender(command_sender: Arc<T>) -> Self {
        let (membership_snapshots_tx, _membership_snapshots_rx) = watch::channel(Vec::new());

        Self {
            candidates: Vec::new(),
            members: Vec::new(),
            slot: SlotKnowledge::default(),
            room_members: None,
            room_encryption: RoomEncryption::default(),
            membership_snapshots_tx,
            command_sender: Some(command_sender),
            own_membership_machine: None,
            encryption_manager: None,
            own_participation: None,
            log_tag: UNATTRIBUTED_LOG_TAG.to_owned(),
        }
    }

    /// Names this session in its log lines, as `room_id/slot_id`.
    ///
    /// Called by [`RtcSessionManager`](crate::RtcSessionManager) on creation; a
    /// standalone [`RtcSession`] keeps the placeholder tag.
    pub(crate) fn set_log_tag(&mut self, log_tag: String) {
        self.log_tag = log_tag;
    }

    /// Sets the command sender for this session.
    pub fn set_command_sender(&mut self, command_sender: Arc<T>) {
        self.command_sender = Some(command_sender);
    }

    /// Returns true if this session has a command sender configured.
    pub fn has_command_sender(&self) -> bool {
        self.command_sender.is_some()
    }

    /// Registers a handler that receives media key material signalled by this
    /// session's encryption manager.
    ///
    /// Returns `false` if the session has not joined yet (no encryption manager
    /// exists). Call after [`RtcSession::join`].
    pub fn set_encryption_signal_handler(
        &mut self,
        handler: Arc<dyn EncryptionKeySignalHandler>,
    ) -> bool {
        match &mut self.encryption_manager {
            Some(manager) => {
                manager.set_signal_handler(handler);
                true
            }
            None => false,
        }
    }

    /// Re-signals every key already held to the installed signal handler.
    ///
    /// Call after both the handler and the identity mapper are in place; see
    /// [`EncryptionManager::replay_keys_to_handler`]. Returns `false` if the
    /// session has no encryption manager.
    pub async fn replay_encryption_keys(&self) -> bool {
        match &self.encryption_manager {
            Some(manager) => {
                manager.replay_keys_to_handler().await;
                true
            }
            None => false,
        }
    }

    /// When a coalesced key rotation falls due, if one is owed.
    ///
    /// `None` while nothing is owed, or when not joined.
    pub fn key_rotation_due_at_ms(&self) -> Option<u64> {
        self.encryption_manager
            .as_ref()
            .and_then(|manager| manager.rotation_due_at_ms())
    }

    /// Performs a key rotation coalesced into a fresh key's window, if one is owed
    /// and due.
    ///
    /// See `RtcSessionManager::flush_due_key_rotation` for why this is the
    /// consumer's to call. Returns `false` if the session has not joined.
    pub async fn flush_due_key_rotation(&self) -> bool {
        match &self.encryption_manager {
            Some(manager) => {
                if let Err(error) = manager.flush_due_rotation().await {
                    log::warn!(
                        "[{}] a deferred key rotation failed: {error:?}",
                        self.log_tag,
                    );
                }
                true
            }
            None => false,
        }
    }

    /// Installs the identity mapper used to derive the RTC-backend participant
    /// identity carried in signalled key material (see [`RtcIdentityMapper`]).
    ///
    /// Returns `false` if the session has not joined yet. Call after
    /// [`RtcSession::join`].
    pub fn set_encryption_identity_mapper(&mut self, mapper: RtcIdentityMapper) -> bool {
        match &mut self.encryption_manager {
            Some(manager) => {
                manager.set_identity_mapper(mapper);
                true
            }
            None => false,
        }
    }

    /// Feeds a media encryption key received from a peer (MSC4143 to-device
    /// message) into this session's encryption manager.
    ///
    /// The key is only used if it passes the MSC4143 checks; see
    /// [`EncryptionManager::receive_key`]. A no-op if the session has not
    /// joined yet (no encryption manager).
    pub async fn receive_encryption_key(
        &self,
        received: ReceivedEncryptionKey,
    ) -> Result<(), CommandError> {
        match &self.encryption_manager {
            Some(manager) => manager.receive_key(received).await,
            None => Ok(()),
        }
    }

    /// Joins this RTC session with the given parameters.
    ///
    /// This sends a membership event to the Matrix room via the command sender,
    /// and starts the keep-alive mechanism to ensure proper cleanup.
    ///
    /// The dead man's switch strategy is used:
    /// 1. Schedule delayed leave event FIRST (safety net) - **awaited**
    /// 2. Send join membership event - **awaited**
    /// 3. Heartbeat will restart the delayed leave periodically
    ///
    /// The async design ensures the delayed leave is scheduled before the join event is sent.
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
        params.validate().map_err(|missing| {
            log::warn!("[{}] join rejected: missing {missing}", self.log_tag);
            JoinError::MissingParameter(missing)
        })?;

        let command_sender = self.command_sender.as_ref().ok_or_else(|| {
            log::warn!(
                "[{}] join rejected: no command sender configured — the host must call \
                 set_command_sender before joining",
                self.log_tag,
            );
            JoinError::CommandError(CommandError::from_message("no command sender configured"))
        })?;

        let membership_id = params.membership_id();

        // Check if already joined with this membership
        if self
            .own_membership_machine
            .as_ref()
            .is_some_and(|machine| machine.sticky_key() == membership_id)
        {
            log::warn!("[{}] already joined as {membership_id}", self.log_tag);
            return Err(JoinError::AlreadyJoined(membership_id));
        }

        log::info!(
            "[{}] joining as {membership_id} ({}/{}, transport {:?})",
            self.log_tag,
            params.user_id,
            params.device_id,
            params.transport,
        );

        let transports = match &params.transport {
            TransportIntent::Publish(transport) => MemberTransports::publishing(
                serde_json::from_value(transport_to_json(transport))
                    .expect("a transport always serializes to an object with a type"),
            ),
            TransportIntent::ReceiveOnly { can_subscribe } => MemberTransports {
                published: Vec::new(),
                can_subscribe: can_subscribe.clone(),
            },
        };
        let machine = OwnMembershipMachine::new(
            command_sender.clone(),
            params.room_id.clone(),
            params.slot_id.clone(),
            membership_id.clone(),
            params.application.clone(),
            params.keep_alive_timeout_ms(),
            params.sticky_duration_ms(),
        );

        // Use the machine to join (async, awaits both delayed leave scheduling and join event)
        machine.join(transports).await?;

        // Store the machine
        self.own_membership_machine = Some(machine);
        self.own_participation = Some(OwnParticipation {
            user_id: params.user_id.clone(),
            device_id: params.device_id.clone(),
            member_id: membership_id.clone(),
        });

        // Name ourselves in the log tag from here on. Until now `room/slot` was
        // enough, because one process meant one session per slot. A host that
        // runs several participations of the same slot in one process — the load
        // generator does, with ten — otherwise emits every session's lines under
        // an identical prefix, and the interleaving cannot be untangled even in
        // principle: "membership changed" from one device sits next to
        // "candidate added" from another, and any conclusion drawn is a guess.
        self.log_tag = format!("{}/{}", self.log_tag, params.device_id);

        // Create the encryption manager
        // We need a closure that can access self.members
        // Since we can't capture self by reference in an Arc closure, we'll use a different approach
        // For now, we'll create a simple closure that clones the members vector
        let get_memberships_for_encryption = {
            let members_tx = self.membership_snapshots_tx.clone();
            move || members_tx.borrow().clone()
        };

        let mut encryption_config = params.encryption_config();
        if let Some(negotiated) = self.negotiated_encryption() {
            // The slot decides whether RTC data is encrypted; the local flag only
            // applies where no slot state has been supplied to negotiate from.
            encryption_config.manage_media_keys = negotiated;
        }

        let mut encryption_manager = EncryptionManager::new(
            command_sender.clone(),
            params.user_id.clone(),
            params.device_id.clone(),
            membership_id.clone(),
            params.room_id.clone(),
            params.slot_id.clone(),
            get_memberships_for_encryption,
        );
        let manage_media_keys = encryption_config.manage_media_keys;
        encryption_manager.set_config(encryption_config);

        // Start the encryption manager (creates first key)
        encryption_manager.join().await.map_err(|e| {
            log::warn!(
                "[{}] encryption manager failed to start: {e:?}",
                self.log_tag
            );
            JoinError::CommandError(CommandError::from_message(format!(
                "failed to start encryption manager: {:?}",
                e
            )))
        })?;

        // Store the encryption manager
        self.encryption_manager = Some(encryption_manager);

        // Re-evaluate the roster: we now have an `own_participation`, so any
        // still-sticky participation of this device from a previous call stops
        // counting as a member. This also publishes the roster the media layer's
        // engine will boot from.
        self.refresh().await;

        // Drive the first distribution from the join itself, unconditionally.
        //
        // `on_memberships_update` is otherwise only reachable through
        // `refresh()`, which returns early when the roster is unchanged — and a
        // session outlives a `leave()`, so a second join in the same process
        // starts with the previous call's roster already in place and has no
        // change to ride on. Without this, the first call in a process
        // distributes its key (the peer's arrival moved the roster) and every
        // later one silently distributes nothing, leaving peers at
        // `MISSING_KEY` for the whole call.
        //
        // A failure here is logged rather than propagated: an unreachable peer
        // must not fail the join, exactly as it does not fail a rollout
        // triggered by a membership change.
        if let Some(encryption_manager) = self.encryption_manager.as_ref()
            && let Err(error) = encryption_manager.on_memberships_update().await
        {
            log::warn!(
                "[{}] the join's first key distribution failed: {error:?}",
                self.log_tag,
            );
        }

        log::info!(
            "[{}] joined as {membership_id} (media keys {})",
            self.log_tag,
            if manage_media_keys { "managed" } else { "off" },
        );

        Ok(())
    }

    /// Leaves this RTC session.
    ///
    /// This sends a left membership event and cancels any active keep-alive delayed event.
    ///
    /// # What survives
    ///
    /// Only *own-participation* state is dropped: the membership machine, the
    /// encryption manager and our identity. Room state (`slot`, `room_members`,
    /// `room_encryption`) and peer `candidates` are kept, and the session itself
    /// stays registered with its manager. Three reasons, all load-bearing:
    ///
    /// - Hosts feed sticky *deltas*. An unchanged peer membership is never
    ///   re-delivered, so a session reset to pristine would rejoin into an empty
    ///   roster and never learn about the members already in the call.
    /// - A host that has hung up may still want the roster — "3 people are in
    ///   this call" outlives our own participation.
    /// - The media session is torn down separately, with no ordering guarantee
    ///   relative to this call. Dropping the membership channel here would make a
    ///   still-running engine stop tracking membership mid-call.
    ///
    /// Because the session outlives a leave, [`Self::join`] must not depend on the
    /// roster changing after it returns — it drives the first key distribution
    /// itself.
    ///
    /// # Arguments
    ///
    /// * `params` - The leave parameters including optional disconnect reason.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the leave completed successfully.
    /// Returns `Err(LeaveError)` if not joined, command sender is not configured, or commands fail.
    pub async fn leave(&mut self, params: LeaveSessionParams) -> Result<(), LeaveError> {
        // Check if we have a membership machine (i.e., we've joined)
        let machine = self.own_membership_machine.take().ok_or_else(|| {
            log::warn!("[{}] leave rejected: not joined", self.log_tag);
            LeaveError::NotJoined
        })?;

        log::info!(
            "[{}] leaving as {} (reason {:?})",
            self.log_tag,
            machine.sticky_key(),
            params.leave_reason,
        );

        // Use the machine to leave (async, awaits both leave event and delayed event cancellation)
        machine
            .leave(params.leave_reason.clone())
            .await
            .inspect_err(|error| log::warn!("[{}] leave failed: {error}", self.log_tag))?;

        // Clean up the encryption manager
        if let Some(encryption_manager) = self.encryption_manager.take() {
            encryption_manager.leave();
        }
        self.own_participation = None;

        // Republish the roster now that we are no longer part of it, rather than
        // waiting for the host's next sticky delta. Room state and peer
        // candidates are deliberately kept: they are room truth a host feeding
        // deltas will never re-deliver, and a host that has hung up may still
        // want to see who is left in the call.
        self.refresh().await;

        log::info!("[{}] left", self.log_tag);

        Ok(())
    }

    /// Performs a heartbeat to restart the keep-alive delayed leave event.
    ///
    /// This should be called periodically (e.g., every 15-20 seconds) to keep the
    /// membership active. The dead man's switch strategy ensures that if the
    /// client stops sending heartbeats, the delayed leave will fire and clean up.
    ///
    /// # Returns
    ///
    /// Returns `true` if the heartbeat was processed successfully.
    /// Returns `false` if not joined (no membership machine active).
    pub async fn heartbeat(&mut self) -> bool {
        if let Some(machine) = self.own_membership_machine.as_ref() {
            log::trace!("[{}] heartbeat", self.log_tag);
            machine.heartbeat().await;

            // A rotation coalesced into a key's `delayBeforeUse` window needs
            // somebody to come back for it once the window closes, and in a call
            // whose roster has gone quiet nothing else will. This tick is the only
            // periodic one the core is given, so it doubles as that collector: the
            // rotation lands within one heartbeat of falling due rather than
            // waiting for the next membership change. A consumer that wants it on
            // time can drive `EncryptionManager::flush_due_rotation` from
            // `rotation_due_at_ms()` instead.
            if let Some(encryption_manager) = self.encryption_manager.as_ref()
                && let Err(error) = encryption_manager.flush_due_rotation().await
            {
                log::warn!(
                    "[{}] a deferred key rotation failed: {error:?}",
                    self.log_tag,
                );
            }
            true
        } else {
            log::debug!("[{}] heartbeat ignored: not joined", self.log_tag);
            false
        }
    }

    /// Returns the number of currently tracked joined members.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Our `member.id` for the current join, or `None` while not joined.
    ///
    /// MSC4143 requires a fresh one on every join, so this changes across a
    /// leave/rejoin and must not be cached by the caller. Consumers that need
    /// it — the media layer derives its MSC4195 participant identity from it —
    /// should read it here rather than supply one, so it cannot drift from the
    /// value the membership was actually published under.
    pub fn own_member_id(&self) -> Option<&str> {
        self.own_membership_machine
            .as_ref()
            .map(|machine| machine.sticky_key())
    }

    /// Subscribes to full membership snapshots for this session as a watch receiver.
    ///
    /// This is used by bindings that implement their own polling/callback model.
    pub fn subscribe_membership_snapshots(&self) -> watch::Receiver<Vec<JoinedMembership>> {
        self.membership_snapshots_tx.subscribe()
    }

    /// Applies the initial membership events for this single session.
    pub async fn set_current_state(
        &mut self,
        events: impl IntoIterator<Item = CallMembershipEvent>,
    ) {
        // Replace, do not merge: `events` is the complete set for this slot, so
        // a candidate missing from it is gone.
        //
        // The distinction is not academic. A member does not only leave by
        // sending a leave event — an MSC4354 sticky entry expires when its owner
        // stops refreshing it, which is exactly what a crashed client does, and
        // an entry lapsing produces no event at all. Merging would leave that
        // member in the call for good, and force every host to diff snapshots
        // itself to notice.
        let previous = std::mem::take(&mut self.candidates);
        for event in events {
            // Deliberately *not* `apply_membership_event`: this loop rebuilds
            // the whole candidate set, and publishing after each event would
            // announce every partial roster on the way — starting with a
            // one-member one, which reads as everybody else leaving. Refresh
            // happens once, below.
            self.record_membership_event(event);
        }

        let dropped: Vec<&str> = previous
            .iter()
            .filter(|before| {
                !self
                    .candidates
                    .iter()
                    .any(|now| now.sender == before.sender && now.sticky_key == before.sticky_key)
            })
            .map(|candidate| candidate.sticky_key.as_str())
            .collect();
        if !dropped.is_empty() {
            log::info!(
                "[{}] {} candidate(s) gone from the current sticky state (expired or \
                 withdrawn): {dropped:?}",
                self.log_tag,
                dropped.len(),
            );
        }

        // `apply_membership_event` refreshes only when it changed something, so
        // a purely shrinking update would otherwise never republish.
        self.refresh().await;
    }

    /// Applies one membership event to this session.
    pub async fn update(&mut self, event: CallMembershipEvent) {
        self.apply_membership_event(event).await;
    }

    /// Applies one membership event and publishes the result.
    async fn apply_membership_event(&mut self, event: CallMembershipEvent) {
        if self.record_membership_event(event) {
            self.refresh().await;
        }
    }

    /// Records one membership event against `candidates`, reporting whether it
    /// changed anything. **Publishes nothing.**
    ///
    /// Separate from [`apply_membership_event`](Self::apply_membership_event)
    /// because a batch must not publish per event. [`set_current_state`] rebuilds
    /// the candidate set from scratch, so refreshing inside the loop announces
    /// every intermediate state as though it were real: the first event of a
    /// six-member snapshot publishes a *one*-member roster, which the encryption
    /// manager reads as five members leaving and answers with a key rotation.
    /// The five are re-added an instant later, so the roster ends up correct and
    /// the rotation is pure waste — once per sticky tick, per session, and with
    /// a to-device send to every remaining member. That is quadratic in the
    /// participant count and was rotating keys every few seconds in a ten-device
    /// call.
    ///
    /// [`set_current_state`]: Self::set_current_state
    fn record_membership_event(&mut self, event: CallMembershipEvent) -> bool {
        match event {
            CallMembershipEvent::Joined(joined) => {
                let existing = self.candidates.iter().position(|candidate| {
                    candidate.sender == joined.sender && candidate.sticky_key == joined.sticky_key
                });

                match existing {
                    Some(index) if self.candidates[index] == joined => {
                        log::trace!(
                            "[{}] member event unchanged, ignored: {}/{}",
                            self.log_tag,
                            joined.sender,
                            joined.sticky_key,
                        );
                        return false;
                    }
                    Some(index) => {
                        log::debug!(
                            "[{}] candidate updated: {}/{}",
                            self.log_tag,
                            joined.sender,
                            joined.sticky_key,
                        );
                        self.candidates[index] = joined;
                    }
                    None => {
                        log::debug!(
                            "[{}] candidate added: {}/{}",
                            self.log_tag,
                            joined.sender,
                            joined.sticky_key,
                        );
                        self.candidates.push(joined);
                    }
                }
            }
            CallMembershipEvent::Left(left) => {
                let before = self.candidates.len();
                self.candidates.retain(|candidate| {
                    !(candidate.sender == left.sender && candidate.sticky_key == left.sticky_key)
                });

                if self.candidates.len() == before {
                    log::trace!(
                        "[{}] leave for an unknown candidate, ignored: {}/{}",
                        self.log_tag,
                        left.sender,
                        left.sticky_key,
                    );
                    return false;
                }

                log::debug!(
                    "[{}] candidate removed: {}/{}",
                    self.log_tag,
                    left.sender,
                    left.sticky_key,
                );
            }
        }

        true
    }

    /// Whether `candidate` satisfies the MSC4143 join conditions that depend on
    /// room state, and if not, which one it fails.
    ///
    /// The other two conditions are handled before this point: `membership =
    /// join` when the event is converted, and stickiness by the host's sticky
    /// map, whose removals arrive as leaves.
    fn join_condition(&self, candidate: &JoinedMembership) -> JoinCondition {
        match &self.slot {
            // Nothing has told us about the slot, so this condition cannot be
            // evaluated; enforcing it would drop every member.
            SlotKnowledge::Unsupplied => {}
            SlotKnowledge::Known(state) if state.is_open() => {}
            SlotKnowledge::Known(_) => return JoinCondition::SlotClosed,
        }

        // MSC4143: in an encrypted room `m.rtc.member` events MUST be
        // encrypted, and one that is not "MUST be considered left". An event
        // whose encryption the host did not report is not judged.
        if self.room_encryption == RoomEncryption::Encrypted
            && candidate.origin.was_encrypted() == Some(false)
        {
            return JoinCondition::UnencryptedInEncryptedRoom;
        }

        // A previous participation of this very device is us, one call ago —
        // MSC4143 mints a fresh `member.id` per join, and the old one stays
        // sticky until the homeserver expires it. Left in, it becomes a phantom
        // member: the media layer opens a receive stream for it and waits for a
        // key that will never come, and it forces a key rotation whose only
        // recipient is unreachable (a device cannot send itself a to-device
        // message). The encryption manager already filters it out of its
        // recipients; this is the same rule applied one level up, where the
        // published roster is decided.
        //
        // The device must match, not just the user: other devices of our own
        // user are ordinary peers. A candidate whose sending device the host did
        // not report therefore cannot be judged, and stays.
        if let Some(own) = &self.own_participation
            && candidate.sender == own.user_id
            && candidate.origin.sender_device_id() == Some(own.device_id.as_str())
            && candidate.member_id != own.member_id
        {
            return JoinCondition::SupersededOwnParticipation;
        }

        match &self.room_members {
            Some(joined) if !joined.contains(&candidate.sender) => JoinCondition::SenderNotInRoom,
            _ => JoinCondition::Joined,
        }
    }

    /// Recomputes the joined set and publishes it if it changed.
    ///
    /// Every input to the join conditions routes through here, so a slot
    /// closing or a member leaving the room takes effect the same way a leave
    /// event does — including telling the encryption manager to stop sharing
    /// keys with whoever dropped out.
    async fn refresh(&mut self) {
        let mut members = Vec::with_capacity(self.candidates.len());
        let mut excluded: Vec<(&str, JoinCondition)> = Vec::new();

        for candidate in &self.candidates {
            let condition = self.join_condition(candidate);
            if condition.is_joined() {
                members.push(candidate.clone());
            } else {
                excluded.push((candidate.sticky_key.as_str(), condition));
            }
        }

        if members == self.members {
            if !excluded.is_empty() {
                log::debug!(
                    "[{}] membership unchanged, {} candidate(s) still excluded: {}",
                    self.log_tag,
                    excluded.len(),
                    describe_exclusions(&excluded),
                );
            }
            return;
        }

        let joined = sticky_keys(&members);
        let previous = sticky_keys(&self.members);
        log::info!(
            "[{}] membership changed: {} -> {} joined (+{:?} -{:?}) of {} candidate(s)",
            self.log_tag,
            self.members.len(),
            members.len(),
            difference(&joined, &previous),
            difference(&previous, &joined),
            self.candidates.len(),
        );
        if !excluded.is_empty() {
            log::debug!(
                "[{}] {} candidate(s) excluded: {}",
                self.log_tag,
                excluded.len(),
                describe_exclusions(&excluded),
            );
        }

        self.members = members;
        self.membership_snapshots_tx
            .send_replace(self.members.clone());

        if let Some(ref encryption_manager) = self.encryption_manager {
            let _ = encryption_manager.on_memberships_update().await;
        }
    }

    /// Applies the room state governing this session's slot.
    ///
    /// Closing a slot leaves every member of it, and reopening it restores the
    /// ones whose member events are still sticky — MSC4143 requires clients to
    /// track the latest room state at all times, not just at join.
    pub async fn set_slot_state(&mut self, state: SlotState) {
        if self.slot == SlotKnowledge::Known(state.clone()) {
            return;
        }
        log::info!(
            "[{}] slot state: {:?} -> {}",
            self.log_tag,
            self.slot,
            if state.is_open() { "Open" } else { "Closed" },
        );
        self.slot = SlotKnowledge::Known(state);
        self.refresh().await;
    }

    /// Returns this session's slot to [`SlotKnowledge::Unsupplied`], so the
    /// open-slot condition stops being enforced.
    ///
    /// Not the same as being told the slot is open: "unknowable" and "open" agree
    /// on today's outcome and disagree about what a later `m.rtc.slot` means. The
    /// caller is saying it can no longer speak for this room's slots, not that it
    /// has seen one.
    pub(crate) async fn forget_slot_state(&mut self) {
        if self.slot == SlotKnowledge::Unsupplied {
            return;
        }
        log::info!(
            "[{}] slot state: {:?} -> Unsupplied (no longer enforced)",
            self.log_tag,
            self.slot,
        );
        self.slot = SlotKnowledge::Unsupplied;
        self.refresh().await;
    }

    /// Sets the slot state on a session that has no members yet.
    ///
    /// Used when a session is created after its room state was already known;
    /// [`RtcSession::set_slot_state`] is the one to use once it is live.
    pub(crate) fn seed_slot_state(&mut self, state: SlotState) {
        debug_assert!(self.candidates.is_empty(), "seeding a populated session");
        self.slot = SlotKnowledge::Known(state);
    }

    /// Sets the room members on a session that has no members yet.
    pub(crate) fn seed_room_members(&mut self, room_members: HashSet<String>) {
        debug_assert!(self.candidates.is_empty(), "seeding a populated session");
        self.room_members = Some(room_members);
    }

    /// Sets the room encryption state on a session that has no members yet.
    pub(crate) fn seed_room_encryption(&mut self, room_encryption: RoomEncryption) {
        debug_assert!(self.candidates.is_empty(), "seeding a populated session");
        self.room_encryption = room_encryption;
    }

    /// Sets whether the room is encrypted.
    ///
    /// In an encrypted room, members whose `m.rtc.member` event arrived in the
    /// clear stop counting as joined.
    pub async fn set_room_encryption(&mut self, room_encryption: RoomEncryption) {
        if self.room_encryption == room_encryption {
            return;
        }
        log::info!(
            "[{}] room encryption: {:?} -> {room_encryption:?}",
            self.log_tag,
            self.room_encryption,
        );
        self.room_encryption = room_encryption;
        self.refresh().await;
    }

    /// Whether RTC data should be encrypted, per the slot and the room.
    ///
    /// `None` means there is nothing to negotiate from — no room state has been
    /// supplied — and the caller's own configuration stands. Otherwise this is
    /// authoritative: MSC4143 prescribes the mechanism through the slot's
    /// `encryption` object, and forbids encryption outright in unencrypted
    /// rooms, so a local preference cannot override either way.
    ///
    /// NOTE: this is read when the session joins. A slot whose mechanism changes
    /// mid-session is not renegotiated; the dangerous direction (the slot
    /// closing) is already covered, since that leaves every member and so stops
    /// key distribution.
    pub fn negotiated_encryption(&self) -> Option<bool> {
        let SlotKnowledge::Known(state) = &self.slot else {
            return None;
        };

        Some(
            state
                .open()
                .and_then(|slot| slot.mechanism.as_ref())
                .is_some_and(|mechanism| mechanism.is_supported()),
        )
    }

    /// Everything this session believes, as JSON, for bug reports.
    ///
    /// Logs tell you what happened; this tells you where things ended up, which
    /// is the other half of diagnosing "the roster is wrong". Deliberately
    /// includes the *candidates* and why each is excluded — the joined set
    /// alone cannot explain an absence.
    pub fn debug_snapshot(&self) -> serde_json::Value {
        let candidates: Vec<serde_json::Value> = self
            .candidates
            .iter()
            .map(|candidate| {
                serde_json::json!({
                    "sender": candidate.sender,
                    "sticky_key": candidate.sticky_key,
                    "member_id": candidate.member_id,
                    "was_encrypted": candidate.origin.was_encrypted(),
                    "condition": format!("{:?}", self.join_condition(candidate)),
                })
            })
            .collect();

        serde_json::json!({
            "slot": match &self.slot {
                SlotKnowledge::Unsupplied => "Unsupplied".to_owned(),
                SlotKnowledge::Known(state) if state.is_open() => "Open".to_owned(),
                SlotKnowledge::Known(_) => "Closed".to_owned(),
            },
            "room_encryption": format!("{:?}", self.room_encryption),
            "room_members_known": self.room_members.as_ref().map(HashSet::len),
            "negotiated_encryption": self.negotiated_encryption(),
            "joined_count": self.members.len(),
            "joined": sticky_keys(&self.members),
            "candidates": candidates,
            "has_command_sender": self.command_sender.is_some(),
            "own_membership": self
                .own_membership_machine
                .as_ref()
                .map(|machine| machine.sticky_key()),
            "has_encryption_manager": self.encryption_manager.is_some(),
        })
    }

    /// The slot state this session is applying, if any has been supplied.
    pub fn slot_state(&self) -> Option<&SlotState> {
        match &self.slot {
            SlotKnowledge::Known(state) => Some(state),
            SlotKnowledge::Unsupplied => None,
        }
    }

    /// Sets the users currently joined to the room.
    ///
    /// Until this is called the room-membership condition is not enforced. A
    /// member who leaves the room stops being joined to the slot even if their
    /// member event is still sticky.
    pub async fn set_room_members(&mut self, room_members: HashSet<String>) {
        if self.room_members.as_ref() == Some(&room_members) {
            return;
        }
        log::debug!(
            "[{}] room members: {:?} -> {} joined",
            self.log_tag,
            self.room_members.as_ref().map(HashSet::len),
            room_members.len(),
        );
        self.room_members = Some(room_members);
        self.refresh().await;
    }
}

/// MSC4143: the intended membership status carried in `content.member.membership`.
///
/// Unknown values round-trip through [`Membership::Unknown`] rather than failing
/// the whole event: a member event using a status from a future spec revision
/// still has to parse, and simply doesn't count as joined.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Membership {
    /// The member intends to be joined to the slot.
    Join,
    /// The member has left the slot.
    Leave,
    /// A status this client does not understand; treated as left.
    #[serde(untagged)]
    Unknown(String),
}

/// MSC4143: Member object (`content.member`).
///
/// Note that the pre-2026 `claimed_user_id` / `claimed_device_id` fields are gone:
/// the sending user is authenticated by the event's `sender`, and the sending
/// device by the event's decryption metadata (see
/// [`JoinedMembership::sender_device_id`]).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberInfo {
    /// Identifier distinguishing this participation, unique per join.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The intended membership status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub membership: Option<Membership>,
}

impl MemberInfo {
    /// True when no member fields are set (used to skip serialization).
    pub fn is_empty(&self) -> bool {
        self.id.is_none() && self.membership.is_none()
    }

    /// True when this member object declares `membership = join` with a usable id.
    pub fn is_join(&self) -> bool {
        matches!(self.membership, Some(Membership::Join))
            && self.id.as_deref().is_some_and(|id| !id.is_empty())
    }
}

/// MSC4143: Application info
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationInfo {
    #[serde(rename = "type")]
    pub application_type: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl ApplicationInfo {
    /// True when no application fields are set (used to skip serialization for leave events).
    pub fn is_empty(&self) -> bool {
        self.application_type.is_none() && self.extra.is_empty()
    }
}

/// MSC4143: the generic `leave_reason.code` values defined by the core proposal.
///
/// Applications and transports may define further codes, which land in
/// [`LeaveCode::Other`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaveCode {
    /// The member left intentionally (e.g. by hanging up a call).
    Leave,
    /// The member left through a scheduled delayed leave event.
    DelayedLeave,
    /// The member left because the slot was closed mid-session.
    SlotClosed,
    /// A code defined outside this proposal.
    #[serde(untagged)]
    Other(String),
}

impl LeaveCode {
    /// Parses a wire `leave_reason.code`, keeping application- and
    /// transport-defined codes intact as [`LeaveCode::Other`].
    ///
    /// The bindings use this so every entry point agrees on the mapping.
    pub fn from_code(code: &str) -> Self {
        match code {
            "leave" => Self::Leave,
            "delayed_leave" => Self::DelayedLeave,
            "slot_closed" => Self::SlotClosed,
            other => Self::Other(other.to_owned()),
        }
    }
}

/// MSC4143: Leave reason (`content.leave_reason`).
///
/// Replaces the earlier `disconnect_reason` object. `code` is the machine-readable
/// identifier and `reason` the optional human-readable explanation — note that in
/// the old shape `reason` was the machine-readable half.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaveReason {
    /// Identifier for the specific leave cause.
    pub code: LeaveCode,
    /// Optional human-readable explanation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl LeaveReason {
    /// A leave reason carrying just a code.
    pub fn new(code: LeaveCode) -> Self {
        Self { code, reason: None }
    }

    /// A leave reason with a human-readable explanation.
    pub fn with_reason(code: LeaveCode, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: Some(reason.into()),
        }
    }
}

#[derive(Clone, Debug)]
/// Membership event projection derived from sticky event content.
pub enum CallMembershipEvent {
    /// A member is connected for the slot.
    Joined(JoinedMembership),
    /// A member is disconnected for the slot.
    Left(LeftMembership),
}

/// MSC4143: Joined membership payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinedMembership {
    /// Room where the membership is active.
    pub room_id: String,
    /// MatrixRTC slot identifier.
    pub slot_id: String,
    /// Sender user ID of the membership event.
    pub sender: String,
    /// How the member event reached us.
    ///
    /// MSC4143 removed the self-asserted `member.claimed_device_id`, so the
    /// sending device comes from here — key distribution targets "the devices
    /// that were used to encrypt these member events".
    pub origin: EventOrigin,
    /// Sticky key identifying this membership stream (equal to `member_id`).
    pub sticky_key: String,
    /// `member.id` — identifies this participation, unique per join.
    pub member_id: String,
    /// Application type from `content.application.type`.
    pub application: Option<String>,
    /// Transports this member publishes on (`content.transports.published`).
    pub transports: Vec<RtcTransport>,
    /// Transport types this member can subscribe to
    /// (`content.transports.can_subscribe`).
    pub can_subscribe: Vec<String>,
}

/// MSC4143: Left membership payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeftMembership {
    /// Room where the membership was active.
    pub room_id: String,
    /// MatrixRTC slot identifier.
    pub slot_id: String,
    /// Sender user ID of the membership event.
    pub sender: String,
    /// Sticky key identifying this membership stream.
    pub sticky_key: String,
    /// `member.id`, when the event carried one. Absent on malformed events and
    /// on sticky removals, which are treated as leaves regardless of content.
    pub member_id: Option<String>,
    /// Optional leave reason (MSC4143).
    pub leave_reason: Option<LeaveReason>,
}

impl<T: RtcCommandSender + 'static> Default for RtcSession<T> {
    fn default() -> Self {
        Self::new()
    }
}
