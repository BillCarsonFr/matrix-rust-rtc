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
//! When a new key is distributed, it is NOT immediately used for encryption.
//! Instead, we wait `delay_before_use_ms` to ensure it has been delivered to all
//! participants. The first key is an exception - it is signaled immediately on the
//! first `on_memberships_update()` call to ensure the transport is listening.
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
//! use matrix_rtc_core::{CommandError, EncryptionConfig, EncryptionManager, JoinedMembership, KeyMaterialSignal, RtcCommandSender};
//! use async_trait::async_trait;
//! use std::sync::Arc;
//! use base64::{Engine as _, engine::general_purpose};
//!
//! // Implement RtcCommandSender for your platform
//! struct MyCommandSender;
//!
//! #[async_trait(?Send)]
//! impl RtcCommandSender for MyCommandSender {
//!     async fn send_sticky_event(&self, _room_id: String, _event_type: String, _content: serde_json::Value) -> Result<(), CommandError> {
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
//! });
//!
//! // Join the session (creates first key)
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! manager.join().await.unwrap();
//!
//! // Handle received keys (from to-device messages)
//! manager.receive_key(
//!     "@bob:example.org".to_string(),
//!     "device456".to_string(),
//!     general_purpose::STANDARD.encode(vec![1u8; 32]),
//!     0,
//!     "bob-member-id".to_string(),
//!     "!room:example.org".to_string(),
//! ).await.unwrap();
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

    /// Track if key distribution is in progress
    key_distribution_in_progress: Arc<Mutex<bool>>,

    /// Track if a new distribution is needed after current completes
    need_new_distribution: Arc<Mutex<bool>>,

    /// Next key index to use (wraps at 256)
    next_key_index: Arc<Mutex<u8>>,

    /// Filter for detecting outdated keys
    key_buffer: Arc<Mutex<OutdatedKeyFilter>>,

    /// Keys that arrived before their membership was known (waiting for RTC membership)
    keys_without_membership: Arc<Mutex<Vec<InboundEncryptionKey>>>,
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
    pub fn set_signal_handler(&mut self, handler: Arc<dyn EncryptionKeySignalHandler>) {
        self.signal_handler = Some(handler);
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
            "[{}:{}] EncryptionManager joining",
            self.room_id,
            self.slot_id
        );

        if !self.config.manage_media_keys {
            log::debug!(
                "[{}:{}] Media keys management disabled",
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
            "[{}:{}] First outbound key created with index {}",
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
            "[{}:{}] EncryptionManager leaving",
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
            "[{}:{}] EncryptionManager state cleaned up",
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
                self.signal_key_to_app(&key).await;
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

        for key in keys_to_process {
            // Find membership matching the member_id
            // For now, we search by member_id in the memberships
            // In practice, JoinedMembership should have a member_id field
            let full_membership = known_memberships
                .iter()
                .find(|m| m.member.id.as_deref() == Some(&key.member_id));

            if let Some(membership) = full_membership {
                // We now have the membership, add the key properly
                self.add_key_to_participant(key, membership).await;
            } else {
                // Still no membership, put it back
                {
                    let mut guard = self.keys_without_membership.lock().unwrap();
                    guard.push(key);
                }
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
                    "[{}:{}] Key distribution in progress, scheduling follow-up",
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
                "[{}:{}] Starting follow-up distribution",
                self.room_id,
                self.slot_id
            );
            // Recursively call ensure_key_distribution using Box::pin
            let _ = Box::pin(self.ensure_key_distribution()).await;
        }

        *self.key_distribution_in_progress.lock().unwrap() = false;

        if let Err(e) = result {
            log::error!(
                "[{}:{}] Failed to rollout key: {:?}",
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
                m.member.id.as_deref() != Some(&self.own_member_id)
            })
            .map(|m| ParticipantDeviceInfo {
                user_id: m.sender.clone(),
                device_id: m.member.claimed_device_id.clone().unwrap_or_default(),
                member_id: m.member.id.clone().unwrap_or_default(),
            })
            .collect();

        let current_key = {
            let guard = self.outbound_key.read().unwrap();
            guard.clone()
        };

        if current_key.is_none() {
            log::warn!(
                "[{}:{}] No outbound key available, cannot distribute",
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
                "[{}:{}] Key rotation needed: {} left, membership changed: {}",
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
                    "[{}:{}] New joiners detected, but key is recent enough (age:{}ms < {}ms), keeping it",
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
                    "[{}:{}] New joiners detected, but key is old (age:{}ms >= {}ms), rotating",
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
                "[{}:{}] No membership changes, no distribution needed",
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

            // Wait before using this key (delayBeforeUse)
            // First key is already signaled, so we only delay for subsequent keys
            log::trace!(
                "[{}:{}] Delaying use of key index {} for {}ms",
                self.room_id,
                self.slot_id,
                outbound_key_to_use.key_index,
                self.config.delay_before_use_ms
            );
            tokio::time::sleep(std::time::Duration::from_millis(
                self.config.delay_before_use_ms,
            ))
            .await;

            // Signal the new key to the application
            self.signal_key_to_app(&outbound_key_to_use).await;
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
            "[{}:{}] Key index:{} sent to {}",
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
            "version": "0"
        });

        log::trace!(
            "[{}:{}] Sending key index {} to member {}",
            self.room_id,
            self.slot_id,
            index,
            target_member_id
        );

        // Find the target participant in our membership list
        let memberships = (self.get_memberships.lock().unwrap())();
        let target = memberships
            .iter()
            .find(|m| m.member.id.as_deref() == Some(target_member_id));

        match target {
            Some(membership) => {
                // Send to the specific user and device
                let target_user_id = &membership.sender;
                let target_device_id = membership
                    .member
                    .claimed_device_id
                    .as_deref()
                    .unwrap_or("*");

                log::debug!(
                    "[{}:{}] Sending key to user={}, device={}",
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
                    "[{}:{}] Cannot send key to member {}: no matching membership found",
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
    async fn signal_key_to_app(&self, key: &OutboundEncryptionKey) {
        if let Some(handler) = &self.signal_handler {
            let rtc_backend_id = self.get_own_rtc_backend_identity();
            let signal = KeyMaterialSignal {
                key: key.key.clone(),
                key_index: key.key_index,
                rtc_backend_identity: rtc_backend_id,
            };

            // Signal to app (async, fire-and-forget)
            // Note: We don't use tokio::spawn here because the handler's future might not be Send
            let handler_clone = handler.clone();
            let _ = handler_clone.on_new_key_material(signal).await;
        }
    }

    /// Receives an encryption key from a to-device message.
    ///
    /// This is called when we receive a to-device message with type
    /// `org.matrix.msc4143.rtc.encryption_key`.
    ///
    /// # Arguments
    /// * `sender_user_id` - User ID of the sender (from Olm decryption metadata)
    /// * `sender_device_id` - Device ID of the sender (from Olm decryption metadata)
    /// * `key_b64` - Base64-encoded key bytes
    /// * `key_index` - Key index (0-255)
    /// * `member_id` - The `member_id` from the message content
    /// * `room_id` - The `room_id` from the message content
    pub async fn receive_key(
        &self,
        _sender_user_id: String,
        _sender_device_id: String,
        key_b64: String,
        key_index: u8,
        member_id: String,
        _room_id: String,
    ) -> Result<(), CommandError> {
        // Verify the room_id matches our session
        // (We could add this check if we want to be strict)

        let key_bytes = general_purpose::STANDARD
            .decode(key_b64)
            .map_err(|e| CommandError::SendError(format!("Failed to decode key: {}", e)))?;

        if key_bytes.len() != 32 {
            log::warn!(
                "[{}:{}] Received key with unexpected length: {} (expected 32)",
                self.room_id,
                self.slot_id,
                key_bytes.len()
            );
        }

        let now = self.timestamp_ms();

        // Check if key is outdated using the filter
        let outdated = {
            let mut guard = self.key_buffer.lock().unwrap();
            guard.check_and_add(member_id.clone(), key_index, now)
        };

        if outdated {
            log::info!(
                "[{}:{}] Received outdated key from member {}, index {}, dropping",
                self.room_id,
                self.slot_id,
                member_id,
                key_index
            );
            return Ok(());
        }

        // Create the inbound key
        let inbound_key = InboundEncryptionKey {
            key: key_bytes,
            key_index,
            member_id: member_id.clone(),
            creation_ts: now,
        };

        // Check if we know about this membership
        let known_memberships = {
            let guard = self.get_memberships.lock().unwrap();
            (guard)()
        };
        let full_membership = known_memberships
            .iter()
            .find(|m| m.member.id.as_deref() == Some(&member_id));

        if let Some(membership) = full_membership {
            // We have the membership, add the key
            self.add_key_to_participant(inbound_key, membership).await;
        } else {
            // No membership yet, buffer the key
            log::debug!(
                "[{}:{}] No matching RTC membership for key from member {}, buffering",
                self.room_id,
                self.slot_id,
                member_id
            );
            self.keys_without_membership
                .lock()
                .unwrap()
                .push(inbound_key);
        }

        Ok(())
    }

    /// Adds a key to a participant.
    async fn add_key_to_participant(
        &self,
        key: InboundEncryptionKey,
        membership: &JoinedMembership,
    ) {
        // Compute RTC backend identity for this participant
        // For now, use member_id as the identity
        // In the future, this could be hashed as per MSC4143
        let rtc_backend_id = membership.member.id.clone().unwrap_or_else(|| {
            format!(
                "{}:{}",
                membership.sender,
                membership.member.claimed_device_id.as_deref().unwrap_or("")
            )
        });

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
    use crate::session::{JoinedMembership, MemberInfo};
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
            sticky_key: "bob-device456-uuid".to_string(),
            application: Some("m.call".to_string()),
            member: MemberInfo {
                id: Some("bob-device456-uuid".to_string()),
                claimed_device_id: Some("device456".to_string()),
                claimed_user_id: Some("@bob:example.org".to_string()),
            },
            versions: vec!["v0".to_string()],
            m_relates_to: None,
            transports: Vec::new(),
            created_ts: Some(2000),
        }
    }

    fn create_mock_get_memberships(
        participants: Vec<JoinedMembership>,
    ) -> impl Fn() -> Vec<JoinedMembership> + Send + Sync + 'static {
        move || participants.clone()
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

        // Receive a key from Bob
        let key_b64 = general_purpose::STANDARD.encode(vec![1u8; 32]);
        manager
            .receive_key(
                "@bob:example.org".to_string(),
                "device456".to_string(),
                key_b64,
                0,
                "bob-device456-uuid".to_string(),
                ROOM_ID.to_string(),
            )
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

        // Receive a key with index 1 and ts=2000
        let key_b64 = general_purpose::STANDARD.encode(vec![1u8; 32]);
        manager
            .receive_key(
                "@bob:example.org".to_string(),
                "device456".to_string(),
                key_b64,
                1,
                "bob-device456-uuid".to_string(),
                ROOM_ID.to_string(),
            )
            .await
            .expect("receive_key should succeed");

        // Try to receive an older key with index 1 and ts=1000 (should be dropped)
        let old_key_b64 = general_purpose::STANDARD.encode(vec![2u8; 32]);
        manager
            .receive_key(
                "@bob:example.org".to_string(),
                "device456".to_string(),
                old_key_b64,
                1,
                "bob-device456-uuid".to_string(),
                ROOM_ID.to_string(),
            )
            .await
            .expect("receive_key should succeed");

        // Only the first key should be stored
        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key_index, 1);
        assert_eq!(keys[0].key, vec![1u8; 32]);
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
