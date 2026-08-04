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
// GNU Affero General License for more details.
//
// You should have received a copy of the GNU Affero General License
// along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

//! Encryption manager for Matrix RTC sessions.
//!
//! This module provides key management for encrypted RTC sessions, implementing
//! the key distribution architecture described in [MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143).
//!
//! # Architecture
//!
//! The encryption manager is responsible for:
//!
//! 1. **Key Generation**: Creating secure random 32-byte keys for media encryption
//! 2. **Key Distribution**: Sending keys to other participants via to-device messages (MSC4143)
//! 3. **Key Storage**: Maintaining inbound keys from other participants
//! 4. **Key Rotation**: Rotating keys when participants join/leave with grace period support
//! 5. **Signaling**: Notifying the application layer when new key material is available
//!
//! # Key Distribution Strategy (MSC4143)
//!
//! To handle rapid join/leave scenarios efficiently:
//!
//! - When a participant **leaves** OR **membership changes**: Always rotate the key
//!   (all remaining participants get the new key)
//! - When **new joiners** arrive and the current key is young (< `key_rotation_grace_period_ms`):
//!   Reuse the current key, send only to the new participant(s)
//! - When **new joiners** arrive and the current key is old:
//!   Rotate the key (all participants get the new key)
//!
//! This prevents expensive key rotations when users quickly join in a row.
//!
//! # Key Usage Delay (MSC4143: delayBeforeUse)
//!
//! When a new key is distributed, it is NOT immediately used for encryption:
//! peers need `delay_before_use_ms` to receive it first. The first key is an
//! exception — it is signaled immediately on the first `on_memberships_update()`
//! call to ensure the transport is listening.
//!
//! This module does not *wait*, it only says how long to wait: the delay travels
//! to the consumer as [`KeyMaterialSignal::use_after_ms`], and the media layer
//! schedules activation. That keeps the core free of any timer — it holds no
//! reactor dependency at all, which is what lets a synchronous FFI host drive it
//! from a plain thread. Enforcing the delay is therefore a consumer obligation;
//! `matrix-rtc-livekit`'s `MediaKeyBridge` is the reference implementation.
//!
//! # Outdated Key Filtering
//!
//! In scenarios where participants quickly join/leave/join, keys might arrive
//! out of order. The `OutdatedKeyFilter` detects and drops outdated keys to
//! prevent decryption issues. If we receive a key at index N with timestamp T2,
//! then a key at the same index N with timestamp T1 < T2, the older key is dropped.
//!
//! # Integration with Application Layer
//!
//! The encryption manager signals new key material to the application layer via
//! the `EncryptionKeySignalHandler` trait. The application is responsible for:
//!
//! 1. Receiving the raw key bytes
//! 2. Applying key derivation/stretching as needed (e.g., using HKDF)
//! 3. Using the derived keys with the media encryption layer
//!
//! The raw bytes provided by the signal handler are the direct key material
//! that should be used with the application's key derivation function.
//!
//! # Example Usage
//!
//! ```no_run
//! use matrix_rtc_core::{CommandError, EncryptionConfig, EncryptionManager, JoinedMembership, KeyMaterialSignal, KeyOrigin, ReceivedEncryptionKey, RtcCommandSender};
//! use async_trait::async_trait;
//! use std::sync::Arc;
//! use base64::{Engine as _, engine::general_purpose};
//!
//! // Implement RtcCommandSender for your platform
//! struct MyCommandSender;
//!
//! #[async_trait(?Send)]
//! impl RtcCommandSender for MyCommandSender {
//!     async fn send_sticky_event(&self, _room_id: String, _event_type: String, _content: serde_json::Value, _duration_ms: u64) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//!     async fn send_delayed_event(&self, _room_id: String, _event_type: String, _content: serde_json::Value, _delay_ms: u64) -> Result<String, CommandError> {
//!         Ok(String::new())
//!     }
//!     async fn cancel_delayed_event(&self, _room_id: String, _event_id: String) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//!     async fn send_to_device_message(&self, _user_id: String, _device_id: String, _message_type: String, _content: serde_json::Value) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//!     async fn send_state_event(&self, _room_id: String, _event_type: String, _state_key: String, _content: serde_json::Value) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//! }
//!
//! // Create an encryption manager
//! let command_sender = Arc::new(MyCommandSender);
//! let get_memberships = || vec![];
//!
//! let mut manager = EncryptionManager::new(
//!     command_sender,
//!     "@alice:example.org".to_string(),
//!     "device123".to_string(),
//!     "xyzABCDEF0123".to_string(),  // member_id
//!     "!room:example.org".to_string(),
//!     "m.call#ROOM".to_string(),
//!     get_memberships,
//! );
//!
//! // Configure (optional)
//! manager.set_config(EncryptionConfig {
//!     delay_before_use_ms: 5000,
//!     key_rotation_grace_period_ms: 10000,
//!     manage_media_keys: true,
//!     require_cross_signed_sender: true,
//! });
//!
//! // Join the session (creates first key)
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! manager.join().await.unwrap();
//!
//! // Handle received keys (from to-device messages). `origin` carries the Olm
//! // decryption metadata, which is what the MSC4143 checks are made against.
//! manager.receive_key(ReceivedEncryptionKey {
//!     origin: KeyOrigin::Encrypted {
//!         sender_user_id: "@bob:example.org".to_string(),
//!         sender_device_id: Some("device456".to_string()),
//!         sender_is_cross_signed: true,
//!     },
//!     room_id: "!room:example.org".to_string(),
//!     member_id: "bob-member-id".to_string(),
//!     key_b64: general_purpose::STANDARD.encode(vec![1u8; 32]),
//!     key_index: 0,
//! }).await.unwrap();
//!
//! // Get current keys for application layer
//! let keys = manager.get_encryption_keys();
//! # });
//! ```

pub mod types;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use serde_json::json;
use types::*;

/// Closure type for getting current memberships, wrapped for thread-safety.
type GetMembershipsFn = Arc<Mutex<Box<dyn Fn() -> Vec<JoinedMembership> + Send>>>;

use crate::commands::RtcCommandSender;
use crate::error::CommandError;
use crate::event::EventOrigin;
use crate::session::JoinedMembership;

/// Message type for to-device encryption key distribution (MSC4143).
///
/// - Stable: `m.rtc.encryption_key`
/// - Unstable: `org.matrix.msc4143.rtc.encryption_key`
///
/// We use the unstable prefix for now as MSC4143 is still in draft.
pub const KEY_MESSAGE_TYPE: &str = "org.matrix.msc4143.rtc.encryption_key";

/// Trait for handlers that receive key material signals.
///
/// Implementations receive notifications when new key material is available
/// for use by the media layer.
#[async_trait(?Send)]
pub trait EncryptionKeySignalHandler: Send + Sync {
    /// Called when new key material is available for a participant.
    ///
    /// The application layer should use these raw bytes with key derivation
    /// to produce the actual encryption keys needed for media encryption/decryption.
    ///
    /// # Arguments
    /// * `signal` - Contains the raw key bytes, key index, and RTC backend identity
    async fn on_new_key_material(&self, signal: KeyMaterialSignal);
}

/// Maps a `(user_id, device_id, member_id)` triple to the RTC-backend
/// participant identity that the media layer addresses keys by.
///
/// `matrix-rtc-core` stays agnostic about how that identity is derived: a
/// consumer (e.g. the LiveKit transport, which uses the MSC4195 pseudonymous
/// identity) injects the derivation via [`EncryptionManager::set_identity_mapper`].
/// When no mapper is set, the manager falls back to its default identity
/// (`user_id:device_id` for the own key, `member_id` for inbound keys).
pub type RtcIdentityMapper = Arc<dyn Fn(&str, &str, &str) -> String + Send + Sync>;

/// The EncryptionManager manages encryption keys for an RTC session.
///
/// This implementation follows the architecture of the JS SDK's RTCEncryptionManager
/// but is implemented in a Rust-idiomatic way and complies with MSC4143.
///
/// # Responsibilities
///
/// - Generate and manage outbound encryption keys (32 secure random bytes)
/// - Distribute keys to participants via to-device messages (MSC4143)
/// - Receive and store inbound keys from other participants
/// - Signal new key material to the application layer
/// - Handle key rotation with grace period support
/// - Filter outdated keys to prevent decryption issues
pub struct EncryptionManager<T: RtcCommandSender> {
    /// Command sender for sending to-device messages
    command_sender: Arc<T>,

    /// Our own user ID (e.g., "@alice:example.org")
    own_user_id: String,

    /// Our own member ID from the `m.rtc.member` event (MSC4143)
    own_member_id: String,

    /// Our device ID
    own_device_id: String,

    /// Function to get current memberships (joined participants)
    /// Wrapped in Arc<Mutex<...>> to allow cloning and Send even if the closure is not Sync.
    get_memberships: GetMembershipsFn,

    /// Room ID for this session
    room_id: String,

    /// Slot ID for this session
    slot_id: String,

    /// Current outbound key (None if not joined)
    outbound_key: Arc<RwLock<Option<OutboundEncryptionKey>>>,

    /// Inbound keys from other participants, keyed by member_id
    inbound_keys: Arc<RwLock<HashMap<String, Vec<InboundEncryptionKey>>>>,

    /// Configuration
    config: EncryptionConfig,

    /// Handler for key material signals
    signal_handler: Option<Arc<dyn EncryptionKeySignalHandler>>,

    /// Optional consumer-supplied mapper from `(user_id, device_id, member_id)`
    /// to the RTC-backend participant identity (see [`RtcIdentityMapper`]).
    identity_mapper: Option<RtcIdentityMapper>,

    /// Track if key distribution is in progress
    key_distribution_in_progress: Arc<Mutex<bool>>,

    /// Track if a new distribution is needed after current completes
    need_new_distribution: Arc<Mutex<bool>>,

    /// Next key index to use (wraps at 256)
    next_key_index: Arc<Mutex<u8>>,

    /// Filter for detecting outdated keys
    key_buffer: Arc<Mutex<OutdatedKeyFilter>>,

    /// Keys that arrived before their membership was known (waiting for RTC membership)
    keys_without_membership: Arc<Mutex<Vec<PendingInboundKey>>>,
}

impl<T: RtcCommandSender + 'static> EncryptionManager<T> {
    /// Creates a new EncryptionManager.
    ///
    /// # Arguments
    /// * `command_sender` - For sending to-device messages
    /// * `own_user_id` - Our Matrix user ID (for RTC backend identity)
    /// * `own_device_id` - Our device ID (for RTC backend identity)
    /// * `own_member_id` - Our `member.id` from the `m.rtc.member` event (MSC4143)
    /// * `room_id` - The room ID for this session
    /// * `slot_id` - The slot ID for this session
    /// * `get_memberships` - Function to get current joined memberships
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        command_sender: Arc<T>,
        own_user_id: String,
        own_device_id: String,
        own_member_id: String,
        room_id: String,
        slot_id: String,
        get_memberships: impl Fn() -> Vec<JoinedMembership> + Send + 'static,
    ) -> Self {
        Self {
            command_sender,
            own_user_id,
            own_member_id,
            own_device_id,
            get_memberships: Arc::new(Mutex::new(Box::new(get_memberships))),
            room_id,
            slot_id,
            outbound_key: Arc::new(RwLock::new(None)),
            inbound_keys: Arc::new(RwLock::new(HashMap::new())),
            config: EncryptionConfig::default(),
            signal_handler: None,
            identity_mapper: None,
            key_distribution_in_progress: Arc::new(Mutex::new(false)),
            need_new_distribution: Arc::new(Mutex::new(false)),
            next_key_index: Arc::new(Mutex::new(0)),
            key_buffer: Arc::new(Mutex::new(OutdatedKeyFilter::default())),
            keys_without_membership: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Sets the configuration.
    pub fn set_config(&mut self, config: EncryptionConfig) {
        self.config = config;
    }

    /// Sets the handler for key material signals.
    ///
    /// Keys that arrived before this was installed were stored but not
    /// signalled — see [`Self::replay_keys_to_handler`], which the consumer
    /// should call once the identity mapper is in place too.
    pub fn set_signal_handler(&mut self, handler: Arc<dyn EncryptionKeySignalHandler>) {
        self.signal_handler = Some(handler);
    }

    /// Re-signals every key we already hold to the current handler.
    ///
    /// Signals are dropped when no handler is installed, and a key is otherwise
    /// only re-signalled by a rotation — which needs a membership change. So a
    /// handler attached after keys arrived (the normal case: media connects some
    /// time after the slot is joined) would never hear about them, and the
    /// transport would fail to decrypt those participants for the rest of the
    /// call.
    ///
    /// Install the identity mapper **before** calling this. Identities are
    /// derived here exactly as the live signal paths derive them, so replaying
    /// without the mapper imports keys under fallback identities the transport
    /// never sees — which looks identical to not replaying at all.
    pub async fn replay_keys_to_handler(&self) {
        let Some(handler) = self.signal_handler.clone() else {
            log::debug!("no signal handler to replay keys to");
            return;
        };

        let mut signals: Vec<KeyMaterialSignal> = Vec::new();

        // Our own outbound key, under the same identity `signal_key_to_app`
        // would use.
        if let Some(outbound) = self.get_outbound_key() {
            let rtc_backend_identity = match &self.identity_mapper {
                Some(mapper) => mapper(&self.own_user_id, &self.own_device_id, &self.own_member_id),
                None => self.get_own_rtc_backend_identity(),
            };
            signals.push(KeyMaterialSignal {
                key: outbound.key.clone(),
                key_index: outbound.key_index,
                rtc_backend_identity,
                // Already-held keys: whatever `delayBeforeUse` applied to them
                // elapsed long ago, so delaying again would only stall media.
                use_after_ms: 0,
            });
        }

        // Peer keys, under the identity derived from their membership — the
        // same derivation `add_key_to_participant` performs.
        let memberships = {
            let guard = self.get_memberships.lock().unwrap();
            guard()
        };
        for (member_id, keys) in self.get_all_inbound_keys() {
            let Some(membership) = memberships
                .iter()
                .find(|membership| membership.member_id == member_id)
            else {
                // No membership means no identity to derive; the key stays
                // stored and will be signalled if their membership shows up.
                log::debug!("not replaying keys for {member_id}: no known membership");
                continue;
            };
            let rtc_backend_identity = match &self.identity_mapper {
                Some(mapper) => mapper(
                    &membership.sender,
                    membership.origin.sender_device_id().unwrap_or(""),
                    &membership.member_id,
                ),
                None => membership.member_id.clone(),
            };
            for key in keys {
                signals.push(KeyMaterialSignal {
                    key: key.key.clone(),
                    key_index: key.key_index,
                    rtc_backend_identity: rtc_backend_identity.clone(),
                    use_after_ms: 0,
                });
            }
        }

        if signals.is_empty() {
            return;
        }

        log::info!("replaying {} held key(s) to the new handler", signals.len());
        for signal in signals {
            handler.on_new_key_material(signal).await;
        }
    }

    /// Sets the identity mapper used to derive the RTC-backend participant
    /// identity carried in [`KeyMaterialSignal`]s (see [`RtcIdentityMapper`]).
    pub fn set_identity_mapper(&mut self, mapper: RtcIdentityMapper) {
        self.identity_mapper = Some(mapper);
    }

    /// Gets the current timestamp in milliseconds.
    fn timestamp_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Generates a new secure random 32-byte key.
    ///
    /// Uses cryptographically secure random number generation via `OsRng`.
    pub fn generate_random_key(&self) -> Vec<u8> {
        use rand::RngCore;
        use rand_core::OsRng;

        let mut key = vec![0u8; 32];
        OsRng.fill_bytes(&mut key);
        key
    }

    /// Gets the next key index (0-255, wraps around).
    pub fn next_key_index(&self) -> u8 {
        let mut index = self.next_key_index.lock().unwrap();
        let result = *index;
        *index = index.wrapping_add(1);
        result
    }

    /// Gets our RTC backend identity.
    ///
    /// For now, uses the simple format: "user_id:device_id"
    /// In the future, this can be extended to use hashed identities as per MSC4143.
    pub fn get_own_rtc_backend_identity(&self) -> String {
        format!("{}:{}", self.own_user_id, self.own_device_id)
    }

    /// Gets our user ID.
    pub fn own_user_id(&self) -> &str {
        &self.own_user_id
    }

    /// Called when joining a session.
    ///
    /// Creates the first outbound key but does NOT signal it yet.
    /// The first key is signaled on the first `on_memberships_update()` call
    /// to ensure the transport is listening (as per JS SDK behavior).
    pub async fn join(&self) -> Result<(), CommandError> {
        log::info!(
            "[{}/{}] EncryptionManager joining",
            self.room_id,
            self.slot_id
        );

        if !self.config.manage_media_keys {
            log::debug!(
                "[{}/{}] Media keys management disabled",
                self.room_id,
                self.slot_id
            );
            return Ok(());
        }

        // Create the first outbound key
        let first_key = OutboundEncryptionKey {
            key: self.generate_random_key(),
            key_index: 0,
            creation_ts: self.timestamp_ms(),
            shared_with: Vec::new(),
        };

        // Store it
        *self.outbound_key.write().unwrap() = Some(first_key.clone());
        *self.next_key_index.lock().unwrap() = 1; // Next will be 1

        log::debug!(
            "[{}/{}] First outbound key created with index {}",
            self.room_id,
            self.slot_id,
            0
        );

        Ok(())
    }

    /// Called when leaving a session.
    ///
    /// Cleans up all state.
    pub fn leave(&self) {
        log::info!(
            "[{}/{}] EncryptionManager leaving",
            self.room_id,
            self.slot_id
        );

        *self.outbound_key.write().unwrap() = None;
        *self.inbound_keys.write().unwrap() = HashMap::new();
        *self.next_key_index.lock().unwrap() = 0;

        let mut buffer = self.key_buffer.lock().unwrap();
        buffer.buffer.clear();

        *self.keys_without_membership.lock().unwrap() = Vec::new();

        log::debug!(
            "[{}/{}] EncryptionManager state cleaned up",
            self.room_id,
            self.slot_id
        );
    }

    /// Called when memberships change (join/leave events).
    ///
    /// This triggers key distribution and signals the first key if not already done.
    pub async fn on_memberships_update(&self) -> Result<(), CommandError> {
        if !self.config.manage_media_keys {
            return Ok(());
        }

        // Check if we have keys waiting for membership
        self.check_keys_without_membership().await;

        // Check if we have an outbound key (i.e., we've joined)
        {
            let guard = self.outbound_key.read().unwrap();
            if guard.is_none() {
                // No outbound key yet, nothing to distribute
                return Ok(());
            }
        }

        // Signal the first key immediately on first membership update
        // (as per JS SDK test: "Set up my key asap even if no key distribution is needed")
        let should_signal = {
            let guard = self.outbound_key.read().unwrap();
            guard.as_ref().is_some_and(|key| key.shared_with.is_empty())
        };

        if should_signal {
            // Clone the key to avoid holding the lock across await
            let key_to_signal = {
                let guard = self.outbound_key.read().unwrap();
                guard.clone()
            };
            if let Some(key) = key_to_signal {
                // The first key: signalled eagerly and usable at once, so the
                // transport has a key ring before any media flows.
                self.signal_key_to_app(&key, 0).await;
            }
        }

        // Ensure key distribution
        self.ensure_key_distribution().await
    }

    /// Checks keys that arrived before their membership was known.
    async fn check_keys_without_membership(&self) {
        let keys_to_process = {
            let mut waiting = self.keys_without_membership.lock().unwrap();
            if waiting.is_empty() {
                return;
            }
            std::mem::take(&mut *waiting)
        };

        let known_memberships = {
            let guard = self.get_memberships.lock().unwrap();
            (guard)()
        };

        for pending in keys_to_process {
            let membership = known_memberships
                .iter()
                .find(|m| m.member_id == pending.key.member_id);

            match membership {
                // The membership is now known, so the MSC4143 sender/device
                // check that could not run at receive time runs here.
                Some(membership) => {
                    self.accept_verified_key(pending.key, &pending.origin, membership)
                        .await
                }
                None => self.keys_without_membership.lock().unwrap().push(pending),
            }
        }
    }

    /// Ensures key distribution happens (with coalescing).
    ///
    /// If a distribution is already in progress, this will schedule a new
    /// distribution to start immediately after the current one completes.
    /// This coalesces multiple rapid membership changes into a single follow-up distribution.
    pub async fn ensure_key_distribution(&self) -> Result<(), CommandError> {
        if !self.config.manage_media_keys {
            return Ok(());
        }

        let in_progress = {
            let mut guard = self.key_distribution_in_progress.lock().unwrap();
            if *guard {
                // Mark that we need a new distribution after current completes
                log::debug!(
                    "[{}/{}] Key distribution in progress, scheduling follow-up",
                    self.room_id,
                    self.slot_id
                );
                *self.need_new_distribution.lock().unwrap() = true;
                return Ok(());
            }
            *guard = true;
            true
        };

        if !in_progress {
            return Ok(());
        }

        let result = self.rollout_outbound_key().await;

        // Check if we need another distribution
        let needs_followup = {
            let mut need_new = self.need_new_distribution.lock().unwrap();
            if *need_new {
                *need_new = false;
                true
            } else {
                false
            }
        };

        if needs_followup {
            log::debug!(
                "[{}/{}] Starting follow-up distribution",
                self.room_id,
                self.slot_id
            );
            // Recursively call ensure_key_distribution using Box::pin
            let _ = Box::pin(self.ensure_key_distribution()).await;
        }

        *self.key_distribution_in_progress.lock().unwrap() = false;

        if let Err(e) = result {
            log::error!(
                "[{}/{}] Failed to rollout key: {:?}",
                self.room_id,
                self.slot_id,
                e
            );
        }

        Ok(())
    }

    /// Creates and distributes a new outbound key if needed.
    ///
    /// This implements the key rotation strategy from MSC4143:
    /// - If someone left OR membership timestamp changed: Always rotate key
    /// - If new joiners AND current key is young: Reuse current key (only send to new joiners)
    /// - If new joiners AND current key is old: Rotate key (send to all)
    async fn rollout_outbound_key(&self) -> Result<(), CommandError> {
        let current_memberships = (self.get_memberships.lock().unwrap())();

        // Build list of current participants (excluding ourselves)
        let current_participants: Vec<ParticipantDeviceInfo> = current_memberships
            .iter()
            .filter(|m| {
                // Exclude ourselves - we don't send keys to ourselves
                m.member_id != self.own_member_id
            })
            .map(|m| ParticipantDeviceInfo {
                user_id: m.sender.clone(),
                device_id: m.origin.sender_device_id().unwrap_or_default().to_owned(),
                member_id: m.member_id.clone(),
            })
            .collect();

        let current_key = {
            let guard = self.outbound_key.read().unwrap();
            guard.clone()
        };

        if current_key.is_none() {
            log::warn!(
                "[{}/{}] No outbound key available, cannot distribute",
                self.room_id,
                self.slot_id
            );
            return Ok(());
        }

        let current_key = current_key.unwrap();
        let already_shared_with = current_key.shared_with.clone();

        // Find participants who left (were previously shared with but are no longer present)
        let left: Vec<&ParticipantDeviceInfo> = already_shared_with
            .iter()
            .filter(|x| {
                !current_participants
                    .iter()
                    .any(|o| o.member_id == x.member_id)
            })
            .collect();

        // Find new participants (present now but not previously shared with)
        let joined: Vec<&ParticipantDeviceInfo> = current_participants
            .iter()
            .filter(|x| {
                !already_shared_with
                    .iter()
                    .any(|o| o.member_id == x.member_id)
            })
            .collect();

        // Check if any membership timestamps changed (user rotated their device/fingerprint)
        // This requires tracking timestamps, which we'll add to ParticipantDeviceInfo later
        // For now, we'll check if the membership has changed by comparing with shared_with
        let any_membership_changed = current_participants.iter().any(|x| {
            already_shared_with.iter().any(|o| {
                o.user_id == x.user_id && o.device_id == x.device_id && o.member_id != x.member_id
            })
        });

        let to_distribute_to: Vec<ParticipantDeviceInfo>;
        let mut use_new_key = false;
        let outbound_key_to_use: OutboundEncryptionKey;

        if !left.is_empty() || any_membership_changed {
            // Someone left or membership changed, we need to rotate the key
            log::info!(
                "[{}/{}] Key rotation needed: {} left, membership changed: {}",
                self.room_id,
                self.slot_id,
                left.len(),
                any_membership_changed
            );
            use_new_key = true;
            to_distribute_to = current_participants.clone();
            outbound_key_to_use = self.create_new_outbound_key();
        } else if !joined.is_empty() {
            // New joiners
            let now = self.timestamp_ms();
            let key_age = now.saturating_sub(current_key.creation_ts);

            if key_age < self.config.key_rotation_grace_period_ms {
                // Current key is still fresh, just distribute to new joiners
                log::debug!(
                    "[{}/{}] New joiners detected, but key is recent enough (age:{}ms < {}ms), keeping it",
                    self.room_id,
                    self.slot_id,
                    key_age,
                    self.config.key_rotation_grace_period_ms
                );
                to_distribute_to = joined.into_iter().cloned().collect();
                outbound_key_to_use = current_key;
            } else {
                // Key is too old, rotate it
                log::debug!(
                    "[{}/{}] New joiners detected, but key is old (age:{}ms >= {}ms), rotating",
                    self.room_id,
                    self.slot_id,
                    key_age,
                    self.config.key_rotation_grace_period_ms
                );
                use_new_key = true;
                to_distribute_to = current_participants.clone();
                outbound_key_to_use = self.create_new_outbound_key();
            }
        } else {
            // No changes, nothing to do
            log::debug!(
                "[{}/{}] No membership changes, no distribution needed",
                self.room_id,
                self.slot_id
            );
            return Ok(());
        }

        // Send keys via to-device messages
        let key_b64 = general_purpose::STANDARD.encode(&outbound_key_to_use.key);

        for participant in &to_distribute_to {
            self.send_key_to_participant(
                &key_b64,
                outbound_key_to_use.key_index,
                &participant.member_id,
            )
            .await?;
        }

        // Update or store the outbound key
        if use_new_key {
            {
                let mut guard = self.outbound_key.write().unwrap();
                let mut new_key = outbound_key_to_use.clone();
                new_key.shared_with = to_distribute_to.clone();
                *guard = Some(new_key);
            }

            // Signal the new key immediately, telling the consumer how long to
            // wait before encrypting with it (delayBeforeUse). The wait used to
            // happen here, as a `tokio::time::sleep` — but this runs on the
            // caller's task, which for the FFI is a *synchronous* host call, so
            // it blocked a host thread for the whole delay (and panicked
            // outright when that thread had no runtime installed). Timing
            // belongs where a scheduler already exists: the media layer.
            //
            // The first key carries no delay — it is signalled on the first
            // `on_memberships_update()` so the transport is listening.
            log::trace!(
                "[{}/{}] Signalling key index {}, usable in {}ms",
                self.room_id,
                self.slot_id,
                outbound_key_to_use.key_index,
                self.config.delay_before_use_ms
            );
            self.signal_key_to_app(&outbound_key_to_use, self.config.delay_before_use_ms)
                .await;
        } else {
            // Reusing existing key, just update shared_with
            {
                let mut guard = self.outbound_key.write().unwrap();
                if let Some(ref mut key) = *guard {
                    for recipient in &to_distribute_to {
                        if !key
                            .shared_with
                            .iter()
                            .any(|x| x.member_id == recipient.member_id)
                        {
                            key.shared_with.push(recipient.clone());
                        }
                    }
                }
            }
        }

        log::trace!(
            "[{}/{}] Key index:{} sent to {}",
            self.room_id,
            self.slot_id,
            outbound_key_to_use.key_index,
            to_distribute_to
                .iter()
                .map(|p| p.member_id.clone())
                .collect::<Vec<_>>()
                .join(",")
        );

        Ok(())
    }

    /// Creates a new outbound key.
    fn create_new_outbound_key(&self) -> OutboundEncryptionKey {
        OutboundEncryptionKey {
            key: self.generate_random_key(),
            key_index: self.next_key_index(),
            creation_ts: self.timestamp_ms(),
            shared_with: Vec::new(),
        }
    }

    /// Sends a key to a specific participant via to-device message (MSC4143 format).
    async fn send_key_to_participant(
        &self,
        key_b64: &str,
        index: u8,
        target_member_id: &str,
    ) -> Result<(), CommandError> {
        // Build the to-device message content (MSC4143 format)
        let content = json!({
            "room_id": self.room_id,
            "member_id": self.own_member_id,
            "media_key": {
                "index": index,
                "key": key_b64
            },
            // MSC4143 `format`: 0 means the raw key bytes, unpadded-base64 encoded.
            "format": 0
        });

        log::trace!(
            "[{}/{}] Sending key index {} to member {}",
            self.room_id,
            self.slot_id,
            index,
            target_member_id
        );

        // Find the target participant in our membership list
        let memberships = (self.get_memberships.lock().unwrap())();
        let target = memberships.iter().find(|m| m.member_id == target_member_id);

        match target {
            Some(membership) => {
                // Send to the specific user and device
                let target_user_id = &membership.sender;
                // MSC4143: key material goes to the device that encrypted the
                // member event. Falling back to "*" (every device of that user)
                // keeps cleartext-room sessions working; Olm still encrypts
                // per-device, so this widens delivery, not readership.
                let target_device_id = membership.origin.sender_device_id().unwrap_or("*");

                log::debug!(
                    "[{}/{}] Sending key to user={}, device={}",
                    self.room_id,
                    self.slot_id,
                    target_user_id,
                    target_device_id
                );

                self.command_sender
                    .send_to_device_message(
                        target_user_id.clone(),
                        target_device_id.to_string(),
                        KEY_MESSAGE_TYPE.to_string(),
                        content,
                    )
                    .await
            }
            None => {
                log::warn!(
                    "[{}/{}] Cannot send key to member {}: no matching membership found",
                    self.room_id,
                    self.slot_id,
                    target_member_id
                );
                // Buffer the key for when membership arrives
                // For now, just return Ok - the key will be sent when membership is known
                Ok(())
            }
        }
    }

    /// Signals a key to the application layer.
    /// Signals our own outbound key to the application layer.
    ///
    /// `use_after_ms` is the MSC4143 `delayBeforeUse` the consumer must observe
    /// before encrypting with it; `0` for the first key, which is signalled
    /// eagerly so the transport is listening.
    async fn signal_key_to_app(&self, key: &OutboundEncryptionKey, use_after_ms: u64) {
        if let Some(handler) = &self.signal_handler {
            let rtc_backend_id = match &self.identity_mapper {
                Some(mapper) => mapper(&self.own_user_id, &self.own_device_id, &self.own_member_id),
                None => self.get_own_rtc_backend_identity(),
            };
            let signal = KeyMaterialSignal {
                key: key.key.clone(),
                key_index: key.key_index,
                rtc_backend_identity: rtc_backend_id,
                use_after_ms,
            };

            // Signal to app (async, fire-and-forget)
            // Note: We don't use tokio::spawn here because the handler's future might not be Send
            let handler_clone = handler.clone();
            let _ = handler_clone.on_new_key_material(signal).await;
        }
    }

    /// Checks an inbound key against the sender's `m.rtc.member` event.
    ///
    /// MSC4143: having matched the message's `member_id` to a member event,
    /// "clients verify that the sender and device that was used to send the
    /// member event match the sender and device of the to-device message.
    /// Otherwise the message MUST be discarded."
    fn verify_against_membership(
        origin: &KeyOrigin,
        membership: &JoinedMembership,
    ) -> Result<(), KeyRejection> {
        let KeyOrigin::Encrypted {
            sender_user_id,
            sender_device_id,
            ..
        } = origin
        else {
            return Err(KeyRejection::Cleartext);
        };

        if sender_user_id != &membership.sender {
            return Err(KeyRejection::SenderMismatch {
                expected: membership.sender.clone(),
                actual: sender_user_id.clone(),
            });
        }

        let expected_device = match &membership.origin {
            EventOrigin::Encrypted {
                sender_device_id: Some(device),
            } => device.as_str(),

            // The host never said how the member event arrived, so there is
            // nothing to check against and nothing to accuse it of — the same
            // stance the rest of the design takes on unreported facts.
            EventOrigin::Unknown => return Ok(()),

            // Either the member event was encrypted but could not be attributed
            // to a device — which should not happen, since Olm messages carry
            // the sender's device keys — or it arrived in the clear. Both leave
            // no device to bind this key to, and MSC4143 makes that match a
            // MUST, so the check has not been satisfied. This is a backstop
            // rather than a path traffic is expected to take: skipping it would
            // quietly downgrade the requirement to a user-only match.
            EventOrigin::Encrypted {
                sender_device_id: None,
            }
            | EventOrigin::Cleartext => return Err(KeyRejection::UnverifiableDevice),
        };

        if sender_device_id.as_deref() != Some(expected_device) {
            return Err(KeyRejection::DeviceMismatch {
                expected: expected_device.to_owned(),
                actual: sender_device_id.clone(),
            });
        }

        Ok(())
    }

    /// Checks the parts of an inbound key that do not depend on the membership.
    fn verify_origin(&self, key: &ReceivedEncryptionKey) -> Result<(), KeyRejection> {
        if key.room_id != self.room_id {
            return Err(KeyRejection::RoomMismatch {
                claimed: key.room_id.clone(),
            });
        }

        match &key.origin {
            KeyOrigin::Cleartext => Err(KeyRejection::Cleartext),
            KeyOrigin::Encrypted {
                sender_is_cross_signed,
                ..
            } if self.config.require_cross_signed_sender && !sender_is_cross_signed => {
                Err(KeyRejection::NotCrossSigned)
            }
            KeyOrigin::Encrypted { .. } => Ok(()),
        }
    }

    /// Receives an encryption key from a to-device message.
    ///
    /// This is called when we receive a to-device message with type
    /// `org.matrix.msc4143.rtc.encryption_key`.
    ///
    /// Keys that fail the MSC4143 checks are discarded, and keys whose member
    /// event has not arrived yet are buffered together with their provenance so
    /// they can be checked once it does — a key is never signalled to the
    /// application before it has been verified.
    pub async fn receive_key(&self, received: ReceivedEncryptionKey) -> Result<(), CommandError> {
        if let Err(rejection) = self.verify_origin(&received) {
            log::warn!(
                "[{}/{}] Discarding key for member {}: {}",
                self.room_id,
                self.slot_id,
                received.member_id,
                rejection
            );
            return Ok(());
        }

        let key_bytes = general_purpose::STANDARD
            .decode(&received.key_b64)
            .map_err(|e| CommandError::SendError(format!("Failed to decode key: {}", e)))?;

        if key_bytes.len() != 32 {
            log::warn!(
                "[{}/{}] Received key with unexpected length: {} (expected 32)",
                self.room_id,
                self.slot_id,
                key_bytes.len()
            );
        }

        let inbound_key = InboundEncryptionKey {
            key: key_bytes,
            key_index: received.key_index,
            member_id: received.member_id.clone(),
            creation_ts: self.timestamp_ms(),
        };

        // Check if we know about this membership
        let known_memberships = {
            let guard = self.get_memberships.lock().unwrap();
            (guard)()
        };
        let membership = known_memberships
            .iter()
            .find(|m| m.member_id == received.member_id);

        match membership {
            Some(membership) => {
                self.accept_verified_key(inbound_key, &received.origin, membership)
                    .await
            }
            None => {
                log::debug!(
                    "[{}/{}] No matching RTC membership for key from member {}, buffering",
                    self.room_id,
                    self.slot_id,
                    received.member_id
                );
                self.keys_without_membership
                    .lock()
                    .unwrap()
                    .push(PendingInboundKey {
                        key: inbound_key,
                        origin: received.origin,
                    });
            }
        }

        Ok(())
    }

    /// Verifies a key against its member event and, if it passes, stores and
    /// signals it.
    ///
    /// The outdated-key filter is only consulted for keys that got this far, so
    /// a rejected key cannot poison the filter and suppress the genuine key at
    /// the same index.
    async fn accept_verified_key(
        &self,
        key: InboundEncryptionKey,
        origin: &KeyOrigin,
        membership: &JoinedMembership,
    ) {
        if let Err(rejection) = Self::verify_against_membership(origin, membership) {
            log::warn!(
                "[{}/{}] Discarding key for member {}: {}",
                self.room_id,
                self.slot_id,
                key.member_id,
                rejection
            );
            return;
        }

        let outdated = {
            let mut guard = self.key_buffer.lock().unwrap();
            guard.check_and_add(key.member_id.clone(), key.key_index, key.creation_ts)
        };

        if outdated {
            log::info!(
                "[{}/{}] Received outdated key from member {}, index {}, dropping",
                self.room_id,
                self.slot_id,
                key.member_id,
                key.key_index
            );
            return;
        }

        self.add_key_to_participant(key, membership).await;
    }

    /// Adds a key to a participant.
    async fn add_key_to_participant(
        &self,
        key: InboundEncryptionKey,
        membership: &JoinedMembership,
    ) {
        // Compute the RTC backend identity for this participant. When a mapper is
        // installed (e.g. the LiveKit transport's MSC4195 pseudonymous identity),
        // derive it from the membership's user/device/member triple; otherwise
        // fall back to the raw member_id (or `sender:device`).
        let rtc_backend_id = match &self.identity_mapper {
            Some(mapper) => mapper(
                &membership.sender,
                membership.origin.sender_device_id().unwrap_or(""),
                &membership.member_id,
            ),
            None => membership.member_id.clone(),
        };

        // Store the key
        let map_key = key.member_id.clone();
        {
            let mut guard = self.inbound_keys.write().unwrap();
            guard.entry(map_key).or_default().push(key.clone());
        }

        // Signal to application
        self.signal_inbound_key_to_app(key, rtc_backend_id).await;
    }

    /// Signals an inbound key to the application layer.
    async fn signal_inbound_key_to_app(
        &self,
        key: InboundEncryptionKey,
        rtc_backend_identity: String,
    ) {
        if let Some(handler) = &self.signal_handler {
            let signal = KeyMaterialSignal {
                key: key.key.clone(),
                key_index: key.key_index,
                rtc_backend_identity,
                // Inbound keys are only ever used to decrypt; delaying one would
                // just drop the sender's frames until it elapsed.
                use_after_ms: 0,
            };

            // Note: We don't use tokio::spawn here because the handler's future might not be Send
            let handler_clone = handler.clone();
            let _ = handler_clone.on_new_key_material(signal).await;
        }
    }

    /// Gets all inbound keys for a specific participant by member_id.
    pub fn get_inbound_keys(&self, member_id: &str) -> Vec<InboundEncryptionKey> {
        let inbound_keys = self.inbound_keys.read().unwrap();
        inbound_keys.get(member_id).cloned().unwrap_or_default()
    }

    /// Gets the current outbound key.
    pub fn get_outbound_key(&self) -> Option<OutboundEncryptionKey> {
        self.outbound_key.read().unwrap().clone()
    }

    /// Gets all stored inbound keys.
    pub fn get_all_inbound_keys(&self) -> HashMap<String, Vec<InboundEncryptionKey>> {
        self.inbound_keys.read().unwrap().clone()
    }

    /// Gets encryption keys for the application layer.
    ///
    /// Returns a map of member_id to their key rings (multiple keys per participant
    /// for rotation support).
    pub fn get_encryption_keys(&self) -> HashMap<String, Vec<KeyMaterialSignal>> {
        let mut result: HashMap<String, Vec<KeyMaterialSignal>> = HashMap::new();

        // Add outbound key
        if let Some(outbound) = self.get_outbound_key() {
            let rtc_backend_id = self.get_own_rtc_backend_identity();
            let signal = KeyMaterialSignal {
                key: outbound.key.clone(),
                key_index: outbound.key_index,
                rtc_backend_identity: rtc_backend_id,
                // A snapshot of keys already held, not a new-key notification:
                // whatever delay applied to them has long since been observed.
                use_after_ms: 0,
            };
            result
                .entry(self.own_member_id.clone())
                .or_default()
                .push(signal);
        }

        // Add inbound keys
        for (member_id, keys) in self.get_all_inbound_keys() {
            let member_id_clone = member_id.clone();
            for key in keys {
                // We need to compute the backend identity
                // For simplicity, use the member_id as identity
                let signal = KeyMaterialSignal {
                    key: key.key.clone(),
                    key_index: key.key_index,
                    rtc_backend_identity: member_id_clone.clone(),
                    use_after_ms: 0,
                };
                result
                    .entry(member_id_clone.clone())
                    .or_default()
                    .push(signal);
            }
        }

        result
    }
}

impl<T: RtcCommandSender + 'static> Clone for EncryptionManager<T> {
    fn clone(&self) -> Self {
        Self {
            command_sender: self.command_sender.clone(),
            own_user_id: self.own_user_id.clone(),
            own_member_id: self.own_member_id.clone(),
            own_device_id: self.own_device_id.clone(),
            get_memberships: self.get_memberships.clone(),
            room_id: self.room_id.clone(),
            slot_id: self.slot_id.clone(),
            outbound_key: self.outbound_key.clone(),
            inbound_keys: self.inbound_keys.clone(),
            config: self.config.clone(),
            signal_handler: self.signal_handler.clone(),
            identity_mapper: self.identity_mapper.clone(),
            key_distribution_in_progress: self.key_distribution_in_progress.clone(),
            need_new_distribution: self.need_new_distribution.clone(),
            next_key_index: self.next_key_index.clone(),
            key_buffer: self.key_buffer.clone(),
            keys_without_membership: self.keys_without_membership.clone(),
        }
    }
}

impl<T: RtcCommandSender + 'static> EncryptionManager<T> {
    /// Creates an Arc-wrapped clone of self.
    pub fn clone_arc(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{MockCommandSender, NoopCommandSender};
    use crate::event::EventOrigin;
    use crate::session::JoinedMembership;
    use std::sync::Arc;

    const ROOM_ID: &str = "!room:example.org";
    const SLOT_ID: &str = "m.call#ROOM";
    const USER_ID: &str = "@alice:example.org";
    const DEVICE_ID: &str = "device123";
    const MEMBER_ID: &str = "alice-device123-uuid";

    fn bob_membership() -> JoinedMembership {
        JoinedMembership {
            room_id: ROOM_ID.to_string(),
            slot_id: SLOT_ID.to_string(),
            sender: "@bob:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("device456".to_string())),
            sticky_key: "bob-device456-uuid".to_string(),
            member_id: "bob-device456-uuid".to_string(),
            application: Some("m.call".to_string()),
            transports: Vec::new(),
            can_subscribe: Vec::new(),
        }
    }

    fn bob_key(key: Vec<u8>, index: u8) -> ReceivedEncryptionKey {
        ReceivedEncryptionKey {
            origin: KeyOrigin::Encrypted {
                sender_user_id: "@bob:example.org".to_string(),
                sender_device_id: Some("device456".to_string()),
                sender_is_cross_signed: true,
            },
            room_id: ROOM_ID.to_string(),
            member_id: "bob-device456-uuid".to_string(),
            key_b64: general_purpose::STANDARD.encode(key),
            key_index: index,
        }
    }

    fn create_mock_get_memberships(
        participants: Vec<JoinedMembership>,
    ) -> impl Fn() -> Vec<JoinedMembership> + Send + Sync + 'static {
        move || participants.clone()
    }

    /// Records what the media layer would have been told.
    #[derive(Default)]
    struct RecordingHandler {
        signals: Mutex<Vec<KeyMaterialSignal>>,
    }

    impl RecordingHandler {
        fn identities(&self) -> Vec<String> {
            self.signals
                .lock()
                .unwrap()
                .iter()
                .map(|signal| signal.rtc_backend_identity.clone())
                .collect()
        }
    }

    #[async_trait(?Send)]
    impl EncryptionKeySignalHandler for RecordingHandler {
        async fn on_new_key_material(&self, signal: KeyMaterialSignal) {
            self.signals.lock().unwrap().push(signal);
        }
    }

    /// Media attaches after the slot is joined, so keys routinely arrive with no
    /// handler installed. They must not be lost: nothing re-signals them until a
    /// rotation, which needs a membership change.
    #[tokio::test]
    async fn keys_held_before_a_handler_exists_are_replayed_on_attach() {
        let mock_sender = Arc::new(NoopCommandSender);
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            mock_sender,
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        // Join and receive a peer key with nothing listening.
        manager.join().await.expect("join should succeed");
        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("bob's key should be accepted");

        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        assert!(
            handler.signals.lock().unwrap().is_empty(),
            "installing a handler must not itself signal"
        );

        manager.replay_keys_to_handler().await;

        let signals = handler.signals.lock().unwrap();
        assert_eq!(signals.len(), 2, "our own key and bob's");
        // Replayed keys are already in use by peers; delaying them again would
        // only stall media that could be decrypted now.
        assert!(signals.iter().all(|signal| signal.use_after_ms == 0));
        assert!(signals.iter().any(|signal| signal.key == vec![7u8; 32]));
    }

    /// The identity is the whole point of the replay: importing a key under one
    /// the transport never uses is indistinguishable from not importing it.
    #[tokio::test]
    async fn replayed_keys_use_the_installed_identity_mapper() {
        let mock_sender = Arc::new(NoopCommandSender);
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            mock_sender,
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        manager.join().await.expect("join should succeed");
        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("bob's key should be accepted");

        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        manager.set_identity_mapper(Arc::new(
            |user_id: &str, device_id: &str, member_id: &str| {
                format!("mapped:{user_id}/{device_id}/{member_id}")
            },
        ));

        manager.replay_keys_to_handler().await;

        let identities = handler.identities();
        assert!(
            identities.contains(&format!("mapped:{USER_ID}/{DEVICE_ID}/{MEMBER_ID}")),
            "our own key must replay under the mapped identity, got {identities:?}"
        );
        assert!(
            identities
                .contains(&"mapped:@bob:example.org/device456/bob-device456-uuid".to_string()),
            "bob's key must replay under his mapped identity, not his member_id, got {identities:?}"
        );
    }

    #[tokio::test]
    async fn replaying_without_a_handler_is_a_no_op() {
        let mock_sender = Arc::new(NoopCommandSender);
        let get_memberships = create_mock_get_memberships(vec![]);
        let manager = EncryptionManager::new(
            mock_sender,
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        manager.join().await.expect("join should succeed");

        // Must not panic on the missing handler.
        manager.replay_keys_to_handler().await;
    }

    #[tokio::test]
    async fn test_manager_join_creates_first_key() {
        let mock_sender = Arc::new(MockCommandSender::new());
        let get_memberships = create_mock_get_memberships(vec![]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager.join().await.expect("join should succeed");

        // Check that first key was created
        let outbound_key = manager.get_outbound_key();
        assert!(outbound_key.is_some());

        let key = outbound_key.unwrap();
        assert_eq!(key.key_index, 0);
        assert_eq!(key.key.len(), 32);
        assert!(key.creation_ts > 0);
    }

    #[tokio::test]
    async fn test_key_index_increments() {
        let mock_sender = Arc::new(NoopCommandSender);
        let get_memberships = create_mock_get_memberships(vec![]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager.join().await.expect("join should succeed");

        // First key should have index 0
        assert_eq!(manager.get_outbound_key().unwrap().key_index, 0);

        // Create a new key (simulating rotation)
        let new_key = manager.create_new_outbound_key();
        assert_eq!(new_key.key_index, 1);

        let another_key = manager.create_new_outbound_key();
        assert_eq!(another_key.key_index, 2);
    }

    #[tokio::test]
    async fn test_key_index_wraps() {
        let mock_sender = Arc::new(NoopCommandSender);
        let get_memberships = create_mock_get_memberships(vec![]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        // Create 256 keys, next should wrap to 0
        for _ in 0..256 {
            manager.create_new_outbound_key();
        }

        let key = manager.create_new_outbound_key();
        assert_eq!(key.key_index, 0);
    }

    #[tokio::test]
    async fn test_receive_key_stores_inbound() {
        let mock_sender = Arc::new(NoopCommandSender);
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager
            .receive_key(bob_key(vec![1u8; 32], 0))
            .await
            .expect("receive_key should succeed");

        // Check that key was stored
        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key_index, 0);
        assert_eq!(keys[0].key, vec![1u8; 32]);
    }

    #[tokio::test]
    async fn test_receive_outdated_key_is_dropped() {
        let mock_sender = Arc::new(NoopCommandSender);
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager
            .receive_key(bob_key(vec![1u8; 32], 1))
            .await
            .expect("receive_key should succeed");

        // A second key at the same index, no newer than the first, is outdated.
        manager
            .receive_key(bob_key(vec![2u8; 32], 1))
            .await
            .expect("receive_key should succeed");

        // Only the first key should be stored
        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key_index, 1);
        assert_eq!(keys[0].key, vec![1u8; 32]);
    }

    /// Helper: a manager with Bob joined, using the default (strict) config.
    fn manager_with_bob() -> EncryptionManager<NoopCommandSender> {
        EncryptionManager::new(
            Arc::new(NoopCommandSender),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            create_mock_get_memberships(vec![bob_membership()]),
        )
    }

    async fn assert_discarded(
        manager: &EncryptionManager<NoopCommandSender>,
        key: ReceivedEncryptionKey,
    ) {
        let member_id = key.member_id.clone();
        manager
            .receive_key(key)
            .await
            .expect("a discarded key is not an error");
        assert!(
            manager.get_inbound_keys(&member_id).is_empty(),
            "key should have been discarded"
        );
    }

    /// MSC4143: "clients SHOULD discard any m.rtc.encryption_key events that
    /// were sent in cleartext" — an unencrypted message has no authenticated
    /// sender, so nothing can be checked against the member event.
    #[tokio::test]
    async fn cleartext_key_is_discarded() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Cleartext;
        assert_discarded(&manager, key).await;
    }

    /// MSC4143 MUST: the to-device sender has to match the sender of the member
    /// event it claims. Otherwise any room member could publish keys as anyone.
    #[tokio::test]
    async fn key_from_wrong_user_is_discarded() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@mallory:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_cross_signed: true,
        };
        assert_discarded(&manager, key).await;
    }

    /// MSC4143 MUST: same for the device — another of Bob's own devices cannot
    /// speak for the device that published the membership.
    #[tokio::test]
    async fn key_from_wrong_device_is_discarded() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("someOtherDevice".to_string()),
            sender_is_cross_signed: true,
        };
        assert_discarded(&manager, key).await;
    }

    /// A member event that was encrypted but could not be attributed to a device
    /// gives nothing to bind the key to, so the MSC4143 device match cannot be
    /// satisfied. Accepting anyway would downgrade it to a user-only check,
    /// which any of that user's devices could then pass.
    #[tokio::test]
    async fn key_is_discarded_when_the_member_event_has_no_attributable_device() {
        let unattributable = JoinedMembership {
            origin: EventOrigin::encrypted(None),
            ..bob_membership()
        };
        let manager = EncryptionManager::new(
            Arc::new(NoopCommandSender),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            create_mock_get_memberships(vec![unattributable]),
        );

        assert_discarded(&manager, bob_key(vec![1u8; 32], 0)).await;
    }

    /// Same for a member event known to have arrived in the clear.
    #[tokio::test]
    async fn key_is_discarded_when_the_member_event_was_cleartext() {
        let cleartext = JoinedMembership {
            origin: EventOrigin::Cleartext,
            ..bob_membership()
        };
        let manager = EncryptionManager::new(
            Arc::new(NoopCommandSender),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            create_mock_get_memberships(vec![cleartext]),
        );

        assert_discarded(&manager, bob_key(vec![1u8; 32], 0)).await;
    }

    /// But an unreported origin is not an accusation: hosts that do not supply
    /// decryption metadata keep working, as everywhere else in the design.
    #[tokio::test]
    async fn key_is_accepted_when_the_member_events_origin_is_unreported() {
        let unreported = JoinedMembership {
            origin: EventOrigin::Unknown,
            ..bob_membership()
        };
        let manager = EncryptionManager::new(
            Arc::new(NoopCommandSender),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            create_mock_get_memberships(vec![unreported]),
        );

        manager
            .receive_key(bob_key(vec![1u8; 32], 0))
            .await
            .expect("should succeed");
        assert_eq!(manager.get_inbound_keys("bob-device456-uuid").len(), 1);
    }

    /// MSC4153: keys from devices that are not cross-signed are discarded when
    /// the client is configured to exclude insecure devices.
    #[tokio::test]
    async fn key_from_non_cross_signed_device_is_discarded_when_required() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_cross_signed: false,
        };
        assert_discarded(&manager, key).await;
    }

    /// ...and accepted when the deployment has opted out of that requirement.
    #[tokio::test]
    async fn key_from_non_cross_signed_device_is_accepted_when_not_required() {
        let mut manager = manager_with_bob();
        manager.set_config(EncryptionConfig {
            require_cross_signed_sender: false,
            ..EncryptionConfig::default()
        });

        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_cross_signed: false,
        };
        manager.receive_key(key).await.expect("should succeed");

        assert_eq!(manager.get_inbound_keys("bob-device456-uuid").len(), 1);
    }

    /// The manager fans keys out to every session in a room, so a key naming a
    /// different room is not ours to hold.
    #[tokio::test]
    async fn key_for_another_room_is_discarded() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.room_id = "!other:example.org".to_string();
        assert_discarded(&manager, key).await;
    }

    /// A key arriving before its member event is held, then verified once the
    /// membership shows up — the check cannot simply be skipped for these.
    #[tokio::test]
    async fn buffered_key_is_verified_when_membership_arrives() {
        let memberships = Arc::new(Mutex::new(Vec::new()));
        let manager = {
            let memberships = memberships.clone();
            EncryptionManager::new(
                Arc::new(NoopCommandSender),
                USER_ID.to_string(),
                DEVICE_ID.to_string(),
                MEMBER_ID.to_string(),
                ROOM_ID.to_string(),
                SLOT_ID.to_string(),
                move || memberships.lock().unwrap().clone(),
            )
        };

        // Two keys claiming Bob's membership: one genuinely from Bob's device,
        // one from an impostor. Neither can be checked yet.
        let mut impostor = bob_key(vec![9u8; 32], 0);
        impostor.origin = KeyOrigin::Encrypted {
            sender_user_id: "@mallory:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_cross_signed: true,
        };
        manager.receive_key(impostor).await.unwrap();
        manager
            .receive_key(bob_key(vec![1u8; 32], 1))
            .await
            .unwrap();
        assert!(manager.get_inbound_keys("bob-device456-uuid").is_empty());

        // Bob's membership arrives; the buffer is drained and checked.
        *memberships.lock().unwrap() = vec![bob_membership()];
        manager.on_memberships_update().await.unwrap();

        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1, "only the genuine key should survive");
        assert_eq!(keys[0].key, vec![1u8; 32]);
    }

    /// A discarded key must not occupy its (member, index) slot in the
    /// outdated-key filter, or an impostor could suppress the genuine key.
    #[tokio::test]
    async fn discarded_key_does_not_block_the_genuine_one() {
        let manager = manager_with_bob();

        let mut impostor = bob_key(vec![9u8; 32], 0);
        impostor.origin = KeyOrigin::Encrypted {
            sender_user_id: "@mallory:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_cross_signed: true,
        };
        manager.receive_key(impostor).await.unwrap();

        manager
            .receive_key(bob_key(vec![1u8; 32], 0))
            .await
            .unwrap();

        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key, vec![1u8; 32]);
    }

    /// The to-device payload states its encoding in `format`, which MSC4143
    /// defines as a number; `0` is raw bytes, base64 encoded.
    #[tokio::test]
    async fn distributed_key_declares_msc4143_format() {
        let sender = Arc::new(MockCommandSender::new());
        let manager = EncryptionManager::new(
            sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            create_mock_get_memberships(vec![bob_membership()]),
        );

        manager.join().await.unwrap();
        manager.on_memberships_update().await.unwrap();

        let messages = sender.to_device_messages.lock().unwrap();
        let (_, _, _, content) = messages.first().expect("a key should have been sent");
        assert_eq!(content.get("format").and_then(|v| v.as_u64()), Some(0));
        assert!(content.get("version").is_none());
    }

    #[tokio::test]
    async fn test_leave_cleans_up() {
        let mock_sender = Arc::new(NoopCommandSender);
        let get_memberships = create_mock_get_memberships(vec![]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager.join().await.expect("join should succeed");

        // Verify we have state
        assert!(manager.get_outbound_key().is_some());

        // Leave
        manager.leave();

        // Verify state is cleaned up
        assert!(manager.get_outbound_key().is_none());
        assert!(manager.get_all_inbound_keys().is_empty());
    }
}
