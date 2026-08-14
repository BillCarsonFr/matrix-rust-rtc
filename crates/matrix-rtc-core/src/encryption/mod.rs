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
//! - When a participant **leaves** OR **membership changes**: Rotate the key
//!   (all remaining participants get the new key)
//! - When **new joiners** arrive and the current key is young (< `key_rotation_grace_period_ms`):
//!   Reuse the current key, send only to the new participant(s)
//! - When **new joiners** arrive and the current key is old:
//!   Rotate the key (all participants get the new key)
//!
//! This prevents expensive key rotations when users quickly join in a row.
//!
//! # Deferring a departure's rotation
//!
//! A burst of departures is the mirror image of a burst of joins, and by default
//! it is not collapsed: each membership update carrying a leave mints and
//! distributes its own key. `leave_rotation_grace_period_ms` (default `0`) opts
//! into collapsing it, by the same key-age test the joiner path uses — the first
//! departure rotates, and the ones landing inside the fresh key's window are
//! answered together when it closes.
//!
//! The deferral is deliberately **state-based**, not a timer: the departed
//! members stay in the key's `shared_with` until a rotation clears them, so every
//! rollout re-derives that a rotation is owed. Only the deadline is stored, and
//! [`EncryptionManager::rotation_due`] exposes it so the session can drive the
//! rollout on a state push where the roster did not move. Nothing here schedules
//! anything, for the same reason `delayBeforeUse` does not (below).
//!
//! The trade is forward secrecy on leave: while a rotation is held back we keep
//! encrypting with a key the departed member holds.
//!
//! # Suspending rotation in a large call
//!
//! Both grace periods only collapse bursts; a rotation still costs one to-device
//! message per other member, and the membership changes that trigger one arrive
//! more often the larger the call is. `key_rotation_participant_limit` (default
//! `30`) puts a ceiling on that: at or above that many participants — ourselves
//! included — the key index stops moving for the rest of the call, and no
//! departure rotates it.
//!
//! Distribution continues. An arriving member is sent the current key and
//! decrypts immediately, and because every member already present holds it, that
//! one message is the whole cost of a join. A member who leaves is dropped from
//! the key's `shared_with` right away even though nothing rotates — otherwise a
//! return under the same member id would read as no change at all and never be
//! served — and the rotation they are owed is remembered separately, so the first
//! rollout back under the limit pays for every departure that happened meanwhile
//! in one go.
//!
//! The price is forward secrecy: over the limit, a member who left keeps a key
//! that stays live until the call shrinks again.
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
//! use matrix_rtc_core::{CommandError, EncryptionConfig, EncryptionManager, JoinedMembership, KeyMaterialSignal, KeyOrigin, ReceivedEncryptionKey, RtcCommandSender, ToDeviceDelivery, ToDeviceRecipient};
//! use async_trait::async_trait;
//! use std::sync::Arc;
//! use base64::{Engine as _, engine::general_purpose};
//!
//! // Implement RtcCommandSender for your platform
//! struct MyCommandSender;
//!
//! #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
//! #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
//! impl RtcCommandSender for MyCommandSender {
//!     async fn send_sticky_event(&self, _room_id: String, _event_type: String, _content: serde_json::Value, _duration_ms: u64) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//!     async fn send_delayed_event(&self, _room_id: String, _event_type: String, _content: serde_json::Value, _delay_ms: u64) -> Result<String, CommandError> {
//!         Ok(String::new())
//!     }
//!     async fn restart_delayed_event(&self, _room_id: String, _event_id: String) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//!     async fn cancel_delayed_event(&self, _room_id: String, _event_id: String) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//!     async fn send_to_device_message(&self, recipients: Vec<ToDeviceRecipient>, _message_type: String, _content: serde_json::Value) -> Result<Vec<ToDeviceDelivery>, CommandError> {
//!         Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect())
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
//!     leave_rotation_grace_period_ms: 0,
//!     key_rotation_participant_limit: 30,
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
//! // Keys reach the media layer through the signal handler installed above.
//! // A handler attached after keys arrived (the normal case — media connects
//! // some time after the slot is joined) calls `replay_keys_to_handler()` to
//! // receive the ones it missed.
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

use crate::commands::{RtcCommandSender, ToDeviceRecipient};
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
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait EncryptionKeySignalHandler: Send + Sync {
    /// Called when new key material is available for a participant.
    ///
    /// The application layer should use these raw bytes with key derivation
    /// to produce the actual encryption keys needed for media encryption/decryption.
    ///
    /// # Arguments
    /// * `signal` - Contains the raw key bytes, key index, and RTC backend identity
    async fn on_new_key_material(&self, signal: KeyMaterialSignal);

    /// Called when a key was received but refused, with the reason.
    ///
    /// The media layer can only report *that* a participant's frames will not
    /// decrypt; the reason is visible here and nowhere else, because this is the
    /// only layer that sees the to-device message and its provenance. A refused
    /// key and a key that never arrived look identical downstream, yet one is a
    /// trust or configuration problem and the other a delivery one.
    ///
    /// Defaulted to a no-op so existing handlers need not change.
    async fn on_key_discarded(&self, discarded: DiscardedKey) {
        let _ = discarded;
    }
}

/// A media key that was received and refused, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscardedKey {
    /// The member the key claimed to be for.
    pub member_id: String,
    /// The key's index, when the message got far enough for it to be meaningful.
    pub key_index: Option<u8>,
    /// The user the message was attributed to, if it was attributable at all.
    pub sender_user_id: Option<String>,
    /// The device it was attributed to, if known.
    pub sender_device_id: Option<String>,
    /// Why it was refused.
    pub reason: KeyRejection,
}

impl DiscardedKey {
    fn new(
        member_id: &str,
        key_index: Option<u8>,
        origin: &KeyOrigin,
        reason: KeyRejection,
    ) -> Self {
        let (sender_user_id, sender_device_id) = match origin {
            KeyOrigin::Encrypted {
                sender_user_id,
                sender_device_id,
                ..
            } => (Some(sender_user_id.clone()), sender_device_id.clone()),
            KeyOrigin::Cleartext => (None, None),
        };
        Self {
            member_id: member_id.to_owned(),
            key_index,
            sender_user_id,
            sender_device_id,
            reason,
        }
    }
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

    /// Our outbound key as last signalled to the app, so the same rotation is
    /// not installed twice and a replay can honour the remaining
    /// `delayBeforeUse`.
    ///
    /// The eager first-key signal used to be gated on `shared_with.is_empty()`,
    /// which is also the state a rollout leaves behind when there was nobody to
    /// distribute to — so in a solo call every membership update re-signalled the
    /// same index, and each one reached the transport as a fresh key import.
    signalled_key: Arc<Mutex<Option<SignalledKey>>>,

    /// When a departure-triggered rotation we held back becomes due, in epoch
    /// milliseconds. `None` when we owe no rotation.
    ///
    /// Only a deadline is stored, never the departures themselves: who left is
    /// re-derived on every rollout from the roster against the current key's
    /// `shared_with`, and the deferring path leaves `shared_with` alone, so a
    /// departed member stays in it and keeps reading as "left" until a rotation
    /// actually drops it. The deadline is the one thing that cannot be derived —
    /// it says whether the wait is over.
    deferred_rotation_due_ms: Arc<Mutex<Option<u64>>>,

    /// Whether a member left while rotation was suspended by
    /// [`EncryptionConfig::key_rotation_participant_limit`], so a rotation is owed
    /// the moment the call is small enough to afford one.
    ///
    /// This one *cannot* be re-derived, which is why it is stored. A suspended
    /// rollout drops the departed from the key's `shared_with` — see there for
    /// why a returning member is invisible otherwise — and that is the same
    /// record every other path reads a departure from. Keeping the debt here is
    /// what stops the leaver's key from staying live for the rest of the call.
    departure_owed_from_suspension: Arc<Mutex<bool>>,
}

/// What we last told the app about our own key, and when.
///
/// The timestamp is what makes a replay honest: a rotation signalled with a
/// `delayBeforeUse` has an install pending in the media layer, so replaying it at
/// `0` would install that index a second time *and* start encrypting with it
/// before peers have had their delay.
#[derive(Clone, Copy, Debug)]
struct SignalledKey {
    key_index: u8,
    /// When it was signalled, in epoch milliseconds.
    at_ms: u64,
    /// The `delayBeforeUse` it was signalled with.
    use_after_ms: u64,
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
            signalled_key: Arc::new(Mutex::new(None)),
            deferred_rotation_due_ms: Arc::new(Mutex::new(None)),
            departure_owed_from_suspension: Arc::new(Mutex::new(false)),
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
            // Replay with whatever is *left* of this key's `delayBeforeUse`,
            // which is almost always nothing:
            //
            // - Never signalled (the join key, or a rotation made before any
            //   handler existed): this replay is its first signal, and it must be
            //   usable at once or we cannot encrypt at all.
            // - Signalled and its delay elapsed: in use, so delaying again would
            //   only stall media that could flow now.
            // - Signalled and its delay still running: a rotation with an install
            //   already pending in the media layer. Replaying at `0` would
            //   install that index a second time *and* bypass MSC4143's
            //   `delayBeforeUse`, so we would start encrypting with a key peers
            //   have not installed yet.
            let use_after_ms = self
                .signalled_key
                .lock()
                .unwrap()
                .filter(|signalled| signalled.key_index == outbound.key_index)
                .map_or(0, |signalled| {
                    let elapsed = self.timestamp_ms().saturating_sub(signalled.at_ms);
                    signalled.use_after_ms.saturating_sub(elapsed)
                });
            signals.push(KeyMaterialSignal {
                key: outbound.key.clone(),
                key_index: outbound.key_index,
                rtc_backend_identity,
                use_after_ms,
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
        *self.signalled_key.lock().unwrap() = None;
        // A rotation owed to a call we are no longer in is owed to nobody, and a
        // session outlives its `leave()`: leaving the deadline behind would have
        // the next join's first unchanged-roster refresh roll out a key for
        // departures from the previous call.
        *self.deferred_rotation_due_ms.lock().unwrap() = None;
        *self.departure_owed_from_suspension.lock().unwrap() = false;

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

        // Signal our current key as soon as we have one, so the transport has a
        // key ring before any media flows — even when there is nobody to
        // distribute to yet.
        //
        // Gated on the index we last signalled, not on `shared_with.is_empty()`:
        // a rollout with no recipients also leaves `shared_with` empty, so the
        // old gate re-signalled the same index on every membership update. The
        // key material is identical, but each signal reaches the transport as a
        // fresh import of that index — which is the duplicate
        // `key index N imported` an Android integration observed.
        let should_signal = {
            let key = self.outbound_key.read().unwrap();
            let signalled = self.signalled_key.lock().unwrap();
            match key.as_ref() {
                Some(key) => signalled.map(|signalled| signalled.key_index) != Some(key.key_index),
                None => false,
            }
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

    /// Whether a rotation held back by
    /// [`EncryptionConfig::leave_rotation_grace_period_ms`] has come due.
    ///
    /// The deferral has no timer of its own — see the field docs for why the core
    /// owns no schedulers — so the consumer has to offer it a chance to run.
    /// Sessions already call [`Self::on_memberships_update`] whenever the roster
    /// moves; this is what tells them to call it when the roster *hasn't* moved,
    /// which is precisely the situation a deferred rotation is waiting in.
    ///
    /// Cheap and non-blocking: one mutex and a clock read, no roster diffing. Safe
    /// to consult on every state push.
    pub fn rotation_due(&self) -> bool {
        self.deferred_rotation_due_ms
            .lock()
            .unwrap()
            .is_some_and(|due_ms| self.timestamp_ms() >= due_ms)
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

        {
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
        }

        // Loop rather than recurse. The recursive version called itself *before*
        // clearing `key_distribution_in_progress`, so the follow-up took the
        // "already in progress" branch above, re-set `need_new_distribution` and
        // returned without rolling anything out — the coalesced distribution was
        // dropped every time, and the flag stayed latched.
        loop {
            if let Err(error) = self.rollout_outbound_key().await {
                log::error!(
                    "[{}/{}] Failed to rollout key: {error:?}",
                    self.room_id,
                    self.slot_id,
                );
            }

            let needs_followup = std::mem::take(&mut *self.need_new_distribution.lock().unwrap());
            if !needs_followup {
                break;
            }
            log::debug!(
                "[{}/{}] Starting follow-up distribution",
                self.room_id,
                self.slot_id
            );
        }

        *self.key_distribution_in_progress.lock().unwrap() = false;

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

        // Build the list of recipients: every membership but this device's.
        //
        // Matching on `member_id` alone is not enough. It is fresh on every join
        // (MSC4143), so a stale membership of *our own* device — we died without
        // leaving and rejoined inside its sticky lifetime — reads as a peer, and
        // we would send our own key to ourselves. Olm has no session with the
        // sending device, so that send fails; and because a repeat of our own
        // user+device under a new `member_id` also trips `any_membership_changed`
        // below, the ghost forces a rotation *and* breaks it, for as long as it
        // stays visible.
        //
        // Other devices of our own user are ordinary recipients and stay.
        let current_participants: Vec<ParticipantDeviceInfo> = current_memberships
            .iter()
            .filter(|m| {
                let is_this_join = m.member_id == self.own_member_id;
                let is_this_device = m.sender == self.own_user_id
                    && m.origin.sender_device_id() == Some(self.own_device_id.as_str());
                !is_this_join && !is_this_device
            })
            .map(|m| ParticipantDeviceInfo {
                user_id: m.sender.clone(),
                device_id: m.origin.sender_device_id().unwrap_or_default().to_owned(),
                member_id: m.member_id.clone(),
            })
            .collect();

        // Rotation is the part of key management that does not scale with the
        // roster: a new key has to reach every other member, so one costs O(N)
        // to-device messages in a call where the membership changes that trigger
        // them also arrive at a rate growing with N. Serving an arriving member
        // the key we already hold costs exactly one message at any size.
        //
        // So past `key_rotation_participant_limit` we keep the cheap half and drop
        // the expensive one: the key index stays put for as long as the call is
        // large, departures stop costing anything, and joiners are still served.
        // Everyone already present holds that key, so a large call converges on
        // one message per join and nothing else.
        //
        // The price is forward secrecy — see the config docs. Nothing about the
        // suspension is recorded: departed members stay in `shared_with` (only a
        // rotation clears it), so the first rollout after the call drops back
        // under the limit re-derives all of them from the diff above and retires
        // them in one rotation.
        let participant_count = current_participants.len() + 1;
        let rotation_suspended = participant_count >= self.config.key_rotation_participant_limit;

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

        // Does anyone actually still holding the outgoing key stand to be
        // disrupted by us switching away from it?
        //
        // `delayBeforeUse` buys the *recipients of a new key* time to install it
        // while we keep encrypting with the previous one, so members already in
        // the call never see a gap. That trade only pays when someone present can
        // decrypt what we are currently stamping. When nobody can — the call sat
        // empty and the key we hold was distributed to nobody, or every member who
        // had it has left — holding on protects no one and costs the arriving
        // member the entire delay, during which they have no key that works at
        // all. Switching at once then leaves only the unavoidable wait for their
        // own to-device delivery.
        let outgoing_key_is_live = current_participants.iter().any(|participant| {
            already_shared_with
                .iter()
                .any(|holder| holder.member_id == participant.member_id)
        });

        let key_age = self.timestamp_ms().saturating_sub(current_key.creation_ts);

        // Departures cost a new key. Whether we mint it *now* is the question a
        // burst makes expensive: N updates each carrying a departure produce N
        // rotations, each a to-device send to everyone still here.
        //
        // `leave_rotation_grace_period_ms` collapses them by measuring the same
        // key age the joiner path measures. The first departure of a burst meets
        // an old key and rotates at once; the rest meet the key it just minted,
        // and are held until it leaves its window — at which point one rotation
        // retires every one of them. Nothing about *which* members left is
        // recorded: they stay in `shared_with` (only a rotation clears it), so the
        // diff above re-derives them on every rollout until one happens.
        //
        // The trade is explicit in the config docs: a held-back rotation is time
        // in which a departed member can still decrypt us. Default `0` keeps the
        // rotate-immediately behaviour.
        let departures_now = !left.is_empty() || any_membership_changed;

        // Over the limit a departure buys no rotation — but it must still be
        // *recorded*, and `shared_with` is the only record of who holds the key.
        // Left alone it would keep naming the leaver for the rest of the call,
        // and then a member coming back under the same id reads as neither an
        // arrival nor a departure and is never handed the key they no longer
        // have. Member ids are fresh per join in MSC4143, but not everywhere: the
        // pre-2026 Element Call generation derives them from user+device (see
        // `compat`), and a sticky entry that lapses and is re-sent looks
        // identical either way.
        //
        // So drop them as holders here, and remember the rotation they are owed
        // in `departure_owed_from_suspension` — by the time we are under the limit
        // and can afford it, `left` no longer names anyone.
        if rotation_suspended && departures_now {
            *self.departure_owed_from_suspension.lock().unwrap() = true;

            let departed: Vec<&str> = left
                .iter()
                .map(|participant| participant.member_id.as_str())
                .collect();
            if !departed.is_empty() {
                let mut guard = self.outbound_key.write().unwrap();
                if let Some(ref mut key) = *guard {
                    key.shared_with
                        .retain(|holder| !departed.contains(&holder.member_id.as_str()));
                }
            }
        }

        // A departure we could not answer while suspended is still owed once the
        // call is small enough, where nothing in the roster diff remembers it.
        let departures = departures_now
            || (!rotation_suspended && *self.departure_owed_from_suspension.lock().unwrap());

        // Over the participant limit no departure rotates at all, so there is
        // nothing for the grace period to hold back either.
        let defer_departures = departures
            && !rotation_suspended
            && key_age < self.config.leave_rotation_grace_period_ms;

        {
            let mut due = self.deferred_rotation_due_ms.lock().unwrap();
            if defer_departures {
                // Keyed off the key's own creation, so re-deriving it on every
                // subsequent rollout in the burst lands on the same deadline
                // instead of pushing it further out each time.
                let deadline = current_key.creation_ts + self.config.leave_rotation_grace_period_ms;
                if *due != Some(deadline) {
                    log::info!(
                        "[{}/{}] Deferring rotation for {} departure(s) (membership changed: \
                         {}): key index {} is {}ms old, holding it until {}ms",
                        self.room_id,
                        self.slot_id,
                        left.len(),
                        any_membership_changed,
                        current_key.key_index,
                        key_age,
                        self.config.leave_rotation_grace_period_ms,
                    );
                }
                *due = Some(deadline);
            } else if !departures || rotation_suspended {
                // Nobody is owed a rotation any more. Reached when a rotation for
                // another reason (a joiner past the grace period) already retired
                // the departed members — or when we are over the participant limit,
                // where a standing deadline would have `rotation_due` ask for a
                // rollout on every state push that we would then decline to act on.
                *due = None;
            }
        }

        let to_distribute_to: Vec<ParticipantDeviceInfo>;
        let mut use_new_key = false;
        let outbound_key_to_use: OutboundEncryptionKey;

        if departures && !defer_departures && !rotation_suspended {
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
            if rotation_suspended {
                // Over the limit the key is not rotated for anything, however old
                // it is: the joiners get it, and the members who already hold it
                // get nothing.
                log::debug!(
                    "[{}/{}] {} participants (rotation limit {}): serving key index {} to {} joiner(s) without rotating",
                    self.room_id,
                    self.slot_id,
                    participant_count,
                    self.config.key_rotation_participant_limit,
                    current_key.key_index,
                    joined.len(),
                );
                to_distribute_to = joined.into_iter().cloned().collect();
                outbound_key_to_use = current_key;
            } else if key_age < self.config.key_rotation_grace_period_ms {
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
        } else if defer_departures {
            // Departures, held back, and nobody new to serve the current key to.
            // The rotation stays owed; `rotation_due` is what brings us back once
            // the window closes, since no further membership change need ever come.
            log::debug!(
                "[{}/{}] Departure rotation still deferred and no new joiners, \
                 nothing to distribute",
                self.room_id,
                self.slot_id
            );
            return Ok(());
        } else if departures && rotation_suspended {
            // Members left a call that is over the limit, and nobody arrived. The
            // key they hold stays live — that is the trade — and no message goes
            // out. `shared_with` still names them, so the first rollout once the
            // call is small enough again rotates them out.
            log::debug!(
                "[{}/{}] {} participants (rotation limit {}): {} departure(s) not rotating key index {}",
                self.room_id,
                self.slot_id,
                participant_count,
                self.config.key_rotation_participant_limit,
                left.len(),
                current_key.key_index,
            );
            return Ok(());
        } else {
            // No changes, nothing to do
            log::debug!(
                "[{}/{}] No membership changes, no distribution needed",
                self.room_id,
                self.slot_id
            );
            return Ok(());
        }

        // Send the key to every recipient in one call, and record only the
        // recipients it actually reached.
        //
        // This used to be a loop of single-recipient sends whose failures were
        // logged and then forgotten: `shared_with` was set to the full list
        // regardless, so a member the send never reached was treated as holding
        // the key and never re-sent to — they could not decrypt us for the rest
        // of the call. Recording only the served recipients makes the next
        // rollout see the rest as newly joined and try again.
        let key_b64 = general_purpose::STANDARD.encode(&outbound_key_to_use.key);
        let served = self
            .send_key_to_participants(&key_b64, outbound_key_to_use.key_index, &to_distribute_to)
            .await;

        let unreached: Vec<&str> = to_distribute_to
            .iter()
            .map(|participant| participant.member_id.as_str())
            .filter(|member_id| {
                !served
                    .iter()
                    .any(|participant| participant.member_id == *member_id)
            })
            .collect();

        // One line per rollout saying how it went as a whole: per-recipient
        // errors are easy to miss in a busy log, and "distributed to 1 of 3" is
        // the shape of the problem.
        if unreached.is_empty() {
            log::info!(
                "[{}/{}] distributed key index {} to {} member(s)",
                self.room_id,
                self.slot_id,
                outbound_key_to_use.key_index,
                to_distribute_to.len(),
            );
        } else {
            log::warn!(
                "[{}/{}] distributed key index {} to {} of {} member(s); not delivered to {:?}, \
                 who cannot decrypt our media until the next rollout retries them",
                self.room_id,
                self.slot_id,
                outbound_key_to_use.key_index,
                served.len(),
                to_distribute_to.len(),
                unreached,
            );
        }

        // Update or store the outbound key
        if use_new_key {
            {
                let mut guard = self.outbound_key.write().unwrap();
                let mut new_key = outbound_key_to_use.clone();
                new_key.shared_with = served.clone();
                *guard = Some(new_key);
            }

            // Whatever the reason for this rotation, it retired every member who
            // had left: the new key's `shared_with` holds only who we just served.
            // Clearing here rather than only on the departure branch covers the
            // case where a joiner past `key_rotation_grace_period_ms` rotates
            // while a departure was still being held back.
            *self.deferred_rotation_due_ms.lock().unwrap() = None;
            // Including a departure that happened while we were over the limit:
            // this rotation is the payment for it.
            *self.departure_owed_from_suspension.lock().unwrap() = false;

            // Signal the new key immediately, telling the consumer how long to
            // wait before encrypting with it (delayBeforeUse). The wait used to
            // happen here, as a `tokio::time::sleep` — but this runs on the
            // caller's task, which for the FFI is a *synchronous* host call, so
            // it blocked a host thread for the whole delay (and panicked
            // outright when that thread had no runtime installed). Timing
            // belongs where a scheduler already exists: the media layer.
            //
            // The delay is skipped when the key we are leaving behind is not live
            // for anyone present (see `outgoing_key_is_live`): there is no
            // continuity to protect, and waiting would only extend how long the
            // arriving member has nothing it can decrypt.
            //
            // The first key carries no delay either — it is signalled on the first
            // `on_memberships_update()` so the transport is listening.
            let use_after_ms = if outgoing_key_is_live {
                self.config.delay_before_use_ms
            } else {
                0
            };
            log::debug!(
                "[{}/{}] Signalling key index {}, usable in {}ms ({})",
                self.room_id,
                self.slot_id,
                outbound_key_to_use.key_index,
                use_after_ms,
                if outgoing_key_is_live {
                    "a member present still holds the key we are replacing"
                } else {
                    "nobody present holds the key we are replacing"
                },
            );
            self.signal_key_to_app(&outbound_key_to_use, use_after_ms)
                .await;
        } else {
            // Reusing the existing key: record only the recipients we reached,
            // so the rest are retried on the next rollout.
            {
                let mut guard = self.outbound_key.write().unwrap();
                if let Some(ref mut key) = *guard {
                    for recipient in &served {
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

    /// Sends the key to every reachable participant, returning those the send
    /// actually reached.
    ///
    /// Recipients whose membership names no sending device are unreachable and
    /// are dropped before the call — MSC4143 puts key material on the device that
    /// encrypted the member event, and there is deliberately no `*` fallback
    /// (that hands the key to devices outside the call, and for our own user to
    /// this very device, which Olm cannot encrypt to).
    async fn send_key_to_participants(
        &self,
        key_b64: &str,
        index: u8,
        targets: &[ParticipantDeviceInfo],
    ) -> Vec<ParticipantDeviceInfo> {
        if targets.is_empty() {
            return Vec::new();
        }

        // The same content for every recipient: it names *our* member id and key,
        // not theirs.
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

        let memberships = (self.get_memberships.lock().unwrap())();
        let mut recipients = Vec::with_capacity(targets.len());
        let mut addressed = Vec::with_capacity(targets.len());

        for target in targets {
            let Some(membership) = memberships.iter().find(|m| m.member_id == target.member_id)
            else {
                log::warn!(
                    "[{}/{}] cannot send key to member {}: no matching membership found",
                    self.room_id,
                    self.slot_id,
                    target.member_id,
                );
                continue;
            };
            let user_id = &membership.sender;
            let Some(device_id) = membership.origin.sender_device_id() else {
                log::warn!(
                    "[{}/{}] cannot send key to member {}: its member event names no sending \
                     device, and we will not broadcast the key to every device of {user_id}. \
                     Their media will not decrypt for us.",
                    self.room_id,
                    self.slot_id,
                    target.member_id,
                );
                continue;
            };

            // Recipient *and* index, on one line: an Android integration had to
            // add exactly this to discover that the core was addressing a key to
            // the device it was running on. Whether a member was addressed at
            // all, and with which index, is not answerable from any other line.
            log::debug!(
                "[{}/{}] sending key index {index} for member {} to {user_id}/{device_id}{}",
                self.room_id,
                self.slot_id,
                target.member_id,
                if user_id == &self.own_user_id && device_id == self.own_device_id {
                    " (ourselves — a homeserver will drop this)"
                } else {
                    ""
                },
            );

            recipients.push(ToDeviceRecipient::new(user_id.clone(), device_id));
            addressed.push(target.clone());
        }

        if recipients.is_empty() {
            return Vec::new();
        }

        let deliveries = match self
            .command_sender
            .send_to_device_message(recipients, KEY_MESSAGE_TYPE.to_string(), content)
            .await
        {
            Ok(deliveries) => deliveries,
            Err(error) => {
                // The batch never left; nobody was served, so nobody is recorded
                // as holding the key and the next rollout retries all of them.
                log::error!(
                    "[{}/{}] could not send key index {index} to any of {} recipient(s): {error}",
                    self.room_id,
                    self.slot_id,
                    addressed.len(),
                );
                return Vec::new();
            }
        };

        // Match results back by recipient rather than by position: an
        // implementation is free to reorder or to answer for only some of them,
        // and treating an unanswered recipient as served is the failure this
        // whole shape exists to prevent.
        addressed
            .into_iter()
            .filter(|target| {
                let delivered = deliveries.iter().find(|delivery| {
                    delivery.recipient.user_id == target.user_id
                        && delivery.recipient.device_id == target.device_id
                });
                match delivered {
                    Some(delivery) if delivery.is_sent() => true,
                    Some(delivery) => {
                        log::error!(
                            "[{}/{}] key index {index} was not delivered to member {} ({}/{}): {}",
                            self.room_id,
                            self.slot_id,
                            target.member_id,
                            target.user_id,
                            target.device_id,
                            delivery.error.as_deref().unwrap_or("no reason given"),
                        );
                        false
                    }
                    None => {
                        log::error!(
                            "[{}/{}] the host reported no outcome for member {} ({}/{}); treating \
                             key index {index} as undelivered so it is retried",
                            self.room_id,
                            self.slot_id,
                            target.member_id,
                            target.user_id,
                            target.device_id,
                        );
                        false
                    }
                }
            })
            .collect()
    }

    /// Signals a key to the application layer.
    /// Reports a refused key to the handler, so the reason leaves the core.
    async fn signal_discarded_key(&self, discarded: DiscardedKey) {
        if let Some(handler) = &self.signal_handler {
            handler.clone().on_key_discarded(discarded).await;
        }
    }

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
            *self.signalled_key.lock().unwrap() = Some(SignalledKey {
                key_index: key.key_index,
                at_ms: self.timestamp_ms(),
                use_after_ms,
            });
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

            // A device the member event only claims (see `EventOrigin::Claimed`
            // — the pre-2026 Element Call path). Checking against it is weaker
            // than MSC4143 wants, but not vacuous: the key still has to arrive
            // Olm-encrypted from that exact device, so a forged claim only ever
            // names a device its author cannot send as.
            EventOrigin::Claimed { device_id } => device_id.as_str(),

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
        // What the host claimed, logged before we decide anything about it, so a
        // discard can be read against the message that caused it.
        match &received.origin {
            KeyOrigin::Encrypted {
                sender_user_id,
                sender_device_id,
                sender_is_cross_signed,
            } => log::debug!(
                "[{}/{}] key index {} for member {} from {sender_user_id}/{} \
                 cross_signed={sender_is_cross_signed}",
                self.room_id,
                self.slot_id,
                received.key_index,
                received.member_id,
                sender_device_id.as_deref().unwrap_or("<unknown>"),
            ),
            KeyOrigin::Cleartext => log::debug!(
                "[{}/{}] key index {} for member {} arrived in cleartext",
                self.room_id,
                self.slot_id,
                received.key_index,
                received.member_id,
            ),
        }

        if let Err(rejection) = self.verify_origin(&received) {
            log::warn!(
                "[{}/{}] Discarding key index {} for member {}: {}",
                self.room_id,
                self.slot_id,
                received.key_index,
                received.member_id,
                rejection
            );
            self.signal_discarded_key(DiscardedKey::new(
                &received.member_id,
                Some(received.key_index),
                &received.origin,
                rejection,
            ))
            .await;
            return Ok(());
        }

        let key_bytes = general_purpose::STANDARD
            .decode(&received.key_b64)
            .map_err(|e| CommandError::SendError(format!("Failed to decode key: {}", e)))?;

        if key_bytes.len() != 32 && key_bytes.len() != 16 {
            log::warn!(
                "[{}/{}] Received key with unexpected length: {} (expected 32 or 16)",
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
                "[{}/{}] Discarding key index {} for member {}: {}",
                self.room_id,
                self.slot_id,
                key.key_index,
                key.member_id,
                rejection
            );
            self.signal_discarded_key(DiscardedKey::new(
                &key.member_id,
                Some(key.key_index),
                origin,
                rejection,
            ))
            .await;
            return;
        }

        let outdated = {
            let mut guard = self.key_buffer.lock().unwrap();
            // Entries older than the filter's TTL can no longer make anything
            // look outdated, so drop them here rather than keeping one per
            // (member, index) for the life of the session. This is the only place
            // keys are added, so it is the only place the buffer can grow.
            guard.cleanup(key.creation_ts);
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

        // Store the key: at most one entry per index, holding the newest material
        // we have seen for it.
        //
        // Both halves matter. A re-delivered *identical* key — the sender
        // re-distributing after a membership change, or two sticky updates racing
        // — must not be stored or signalled again: `OutdatedKeyFilter` cannot
        // catch it, because with no sender timestamp on the wire `creation_ts` is
        // stamped at receive time and a re-delivery never looks stale. Appending
        // instead grew a duplicate per delivery, signalled each as a fresh
        // import, and made every later replay re-signal all the copies.
        //
        // A *different* key at an index we already hold is a rekey, and the newer
        // material is what the sender is encrypting with, so it replaces the old
        // one rather than joining it. Keeping both would leave the replay order
        // deciding which one the transport ends up installed with — and replaying
        // the older one last silently downgrades us to a key the sender has
        // stopped using.
        let map_key = key.member_id.clone();
        {
            let mut guard = self.inbound_keys.write().unwrap();
            let held = guard.entry(map_key).or_default();
            match held
                .iter_mut()
                .find(|existing| existing.key_index == key.key_index)
            {
                Some(existing) if existing.key == key.key => {
                    log::trace!(
                        "[{}/{}] key index {} for member {} is already held; not re-importing it",
                        self.room_id,
                        self.slot_id,
                        key.key_index,
                        key.member_id,
                    );
                    return;
                }
                Some(existing) => {
                    log::debug!(
                        "[{}/{}] member {} rekeyed index {}; replacing the key we held",
                        self.room_id,
                        self.slot_id,
                        key.member_id,
                        key.key_index,
                    );
                    *existing = key.clone();
                }
                None => held.push(key.clone()),
            }
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
            signalled_key: self.signalled_key.clone(),
            deferred_rotation_due_ms: self.deferred_rotation_due_ms.clone(),
            departure_owed_from_suspension: self.departure_owed_from_suspension.clone(),
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
    use crate::commands::{MockCommandSender, NoopCommandSender, ToDeviceDelivery};
    use crate::event::EventOrigin;
    use crate::session::JoinedMembership;
    use std::sync::Arc;
    use std::time::Duration;

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

    /// Who a rollout actually addressed, in order.
    fn to_device_recipients(sender: &MockCommandSender) -> Vec<(String, String)> {
        sender
            .to_device_messages
            .lock()
            .unwrap()
            .iter()
            .map(|(user_id, device_id, _, _)| (user_id.clone(), device_id.clone()))
            .collect()
    }

    /// Records what the media layer would have been told.
    #[derive(Default)]
    struct RecordingHandler {
        signals: Mutex<Vec<KeyMaterialSignal>>,
        discarded: Mutex<Vec<DiscardedKey>>,
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

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl EncryptionKeySignalHandler for RecordingHandler {
        async fn on_new_key_material(&self, signal: KeyMaterialSignal) {
            self.signals.lock().unwrap().push(signal);
        }

        async fn on_key_discarded(&self, discarded: DiscardedKey) {
            self.discarded.lock().unwrap().push(discarded);
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

    /// Builds a manager whose memberships come from a list a test can mutate,
    /// so a departure or arrival can be staged between rollouts.
    fn manager_over(
        sender: Arc<MockCommandSender>,
        memberships: Arc<Mutex<Vec<JoinedMembership>>>,
    ) -> EncryptionManager<MockCommandSender> {
        EncryptionManager::new(
            sender,
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            move || memberships.lock().unwrap().clone(),
        )
    }

    /// One membership update per arriving peer, but only ever one signal per key
    /// index. Each signal reaches the transport as a key *import*, so a repeat is
    /// a duplicate `set_key` on the frame cryptor for material it already has.
    #[tokio::test]
    async fn a_key_index_is_signalled_once_however_often_memberships_update() {
        let memberships = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager_over(Arc::new(MockCommandSender::new()), memberships.clone());
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        manager.join().await.expect("join should succeed");

        // Alone in the call: a rollout with no recipients leaves `shared_with`
        // empty, which used to be read as "not signalled yet".
        for _ in 0..3 {
            manager
                .on_memberships_update()
                .await
                .expect("update should succeed");
        }

        let signals = handler.signals.lock().unwrap();
        assert_eq!(
            signals.len(),
            1,
            "the same key index was signalled {} times: {:?}",
            signals.len(),
            signals.iter().map(|s| s.key_index).collect::<Vec<_>>(),
        );
        assert_eq!(signals[0].key_index, 0);
        assert_eq!(
            signals[0].use_after_ms, 0,
            "the first key must be usable at once, or nothing can be encrypted"
        );
    }

    /// A departure rotates the key even with nobody left to send it to, and that
    /// eagerness is the point: the next arrival is then handed a key that is
    /// already the one we are stamping frames with, so they decrypt immediately.
    ///
    /// Deferring the rotation until someone arrives looks tidier — no index burnt
    /// on a key no peer will hold — but it is worse. The arrival would be handed a
    /// freshly rotated key while we keep stamping the *previous* one for the whole
    /// of its `delayBeforeUse`, and they hold nothing that decrypts it. Rotating
    /// on departure instead lets the grace-period reuse path below hand them the
    /// live key.
    ///
    /// Forward secrecy still holds: the departed member's key is retired at once,
    /// and the key the newcomer receives only ever encrypted media from after the
    /// call emptied.
    #[tokio::test]
    async fn a_departure_rotates_so_the_next_arrival_gets_the_live_key() {
        let sender = Arc::new(MockCommandSender::new());
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        manager.join().await.expect("join should succeed");

        // Bob is in the call and gets index 0.
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        let index_bob_got = sender
            .last_to_device_message()
            .expect("bob should have been sent a key")
            .3
            .pointer("/media_key/index")
            .and_then(|index| index.as_u64())
            .expect("the key message carries an index");

        // Bob leaves. Nobody is left.
        memberships.lock().unwrap().clear();
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_ne!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(index_bob_got as u8),
            "the departed member's key must be retired, or media sent after they \
             left stays readable to them"
        );

        // Carol arrives while that key is still inside its grace period, so it is
        // reused rather than rotated again — and it is exactly what we encrypt
        // with, so she decrypts from her first frame.
        let carol = JoinedMembership {
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("CAROLDEV".to_string())),
            sticky_key: "carol-a".to_string(),
            member_id: "carol-a".to_string(),
            ..bob_membership()
        };
        *memberships.lock().unwrap() = vec![carol];
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        let sent_to_carol = sender
            .to_device_messages_for("@carol:example.org", "CAROLDEV")
            .into_iter()
            .filter_map(|(_, content)| {
                content
                    .pointer("/media_key/index")
                    .and_then(|index| index.as_u64())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            sent_to_carol.len(),
            1,
            "carol should be handed exactly one key index, got {sent_to_carol:?}"
        );
        assert_ne!(
            sent_to_carol[0], index_bob_got,
            "the key bob held must not be reused for carol"
        );
        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(sent_to_carol[0] as u8),
            "we must encrypt with exactly the index carol was handed"
        );
    }

    /// A peer under a given user, device and member id.
    fn peer(user_id: &str, device_id: &str, member_id: &str) -> JoinedMembership {
        JoinedMembership {
            sender: user_id.to_string(),
            origin: EventOrigin::encrypted(Some(device_id.to_string())),
            sticky_key: member_id.to_string(),
            member_id: member_id.to_string(),
            ..bob_membership()
        }
    }

    /// Every key index a rollout put on the wire, in order.
    fn sent_key_indices(sender: &MockCommandSender) -> Vec<u64> {
        sender
            .to_device_messages
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, _, _, content)| {
                content
                    .pointer("/media_key/index")
                    .and_then(|index| index.as_u64())
            })
            .collect()
    }

    /// A burst of departures — a partition dropping a group of devices, a sticky
    /// batch expiring — arrives as one membership update per leaver. Rotating on
    /// each costs a fresh key and a to-device send to everyone still present, per
    /// update, and it is the shape `leave_rotation_grace_period_ms` exists to
    /// collapse: one rotation retires the whole burst.
    #[tokio::test]
    async fn departures_inside_the_leave_grace_period_collapse_into_one_rotation() {
        const GRACE_MS: u64 = 400;

        let sender = Arc::new(MockCommandSender::new());
        let memberships = Arc::new(Mutex::new(vec![
            peer("@bob:example.org", "BOBDEV", "bob-a"),
            peer("@carol:example.org", "CAROLDEV", "carol-a"),
            peer("@dave:example.org", "DAVEDEV", "dave-a"),
        ]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        manager.set_config(EncryptionConfig {
            leave_rotation_grace_period_ms: GRACE_MS,
            ..EncryptionConfig::default()
        });
        manager.join().await.expect("join should succeed");

        // All three hold index 0.
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(sent_key_indices(&sender), vec![0, 0, 0]);
        sender.to_device_messages.lock().unwrap().clear();

        // Bob then carol drop out, each in its own update, both inside the window
        // of the key they were just handed.
        for departed in ["bob-a", "carol-a"] {
            memberships
                .lock()
                .unwrap()
                .retain(|member| member.member_id != departed);
            manager
                .on_memberships_update()
                .await
                .expect("update should succeed");
        }

        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(0),
            "both departures are inside the window, so the key must not have moved yet"
        );
        assert!(
            sent_key_indices(&sender).is_empty(),
            "a deferred rotation must put nothing on the wire, got {:?}",
            sent_key_indices(&sender),
        );
        assert!(
            !manager.rotation_due(),
            "the window has not closed, so nothing is owed yet"
        );

        // The window closes. No further membership change is coming — the last
        // departure can be the last thing that happens in a call — so what brings
        // us back is the consumer asking whether a rotation has come due.
        tokio::time::sleep(Duration::from_millis(GRACE_MS + 150)).await;
        assert!(
            manager.rotation_due(),
            "the deferred rotation should be due once the window closes"
        );

        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(1),
            "two departures must cost exactly one rotation, not one each"
        );
        assert_eq!(
            sent_key_indices(&sender),
            vec![1],
            "only dave is left to receive the rotated key"
        );
        assert!(
            !manager.rotation_due(),
            "the rotation settled the debt, or every later state push rolls out again"
        );
    }

    /// Over `key_rotation_participant_limit` the key index stops moving: a
    /// departure costs nothing, and an arrival costs exactly one message — the
    /// current key — with no rotation for the members who already hold it.
    #[tokio::test]
    async fn over_the_participant_limit_a_joiner_gets_the_current_key_and_nobody_rotates() {
        let sender = Arc::new(MockCommandSender::new());
        let memberships = Arc::new(Mutex::new(vec![
            peer("@bob:example.org", "BOBDEV", "bob-a"),
            peer("@carol:example.org", "CAROLDEV", "carol-a"),
        ]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        // Three participants — bob, carol and us — is the limit exactly, so one
        // more arrival suspends rotation. No joiner grace period, so anything
        // below the limit would rotate on every arrival.
        manager.set_config(EncryptionConfig {
            key_rotation_participant_limit: 4,
            key_rotation_grace_period_ms: 0,
            ..EncryptionConfig::default()
        });
        manager.join().await.expect("join should succeed");

        // Under the limit with no joiner grace period: bob and carol arriving
        // rotates index 0 out, and both are handed index 1.
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(sent_key_indices(&sender), vec![1, 1]);
        sender.to_device_messages.lock().unwrap().clear();

        // Dave takes the call to the limit. He is served the key we already hold,
        // and bob and carol — who hold it too — hear nothing.
        memberships
            .lock()
            .unwrap()
            .push(peer("@dave:example.org", "DAVEDEV", "dave-a"));
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(1),
            "an arrival over the limit must not rotate, however old the key is"
        );
        assert_eq!(
            sent_key_indices(&sender),
            vec![1],
            "the joiner gets exactly one message, carrying the live key index"
        );
        assert_eq!(
            to_device_recipients(&sender),
            vec![("@dave:example.org".to_string(), "DAVEDEV".to_string())],
            "the members who already hold that key must not be re-sent it"
        );
        sender.to_device_messages.lock().unwrap().clear();

        // Carol leaves while the call is still at the limit, replaced by erin.
        // Normally a departure always rotates; here the key stands, so erin is
        // handed the same index carol keeps.
        memberships
            .lock()
            .unwrap()
            .retain(|member| member.member_id != "carol-a");
        memberships
            .lock()
            .unwrap()
            .push(peer("@erin:example.org", "ERINDEV", "erin-a"));
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(1),
            "a departure over the limit must not cost a rotation"
        );
        assert_eq!(
            sent_key_indices(&sender),
            vec![1],
            "only the arriving member is served, and with the standing key"
        );
        assert!(
            !manager.rotation_due(),
            "a suspended rotation must not ask the consumer for rollouts it will decline"
        );
    }

    /// A member who leaves and comes back while the call is over the limit must
    /// be handed the key again — they no longer have it.
    ///
    /// Nothing rotates over the limit, and `shared_with` is otherwise only ever
    /// cleared by a rotation, so the leaver would stay recorded as a holder of the
    /// live key. Under a member id that is fresh per join that merely leaks a
    /// ghost; under one derived from user+device (pre-2026 Element Call), or a
    /// sticky entry that lapses and is re-sent, the return is invisible and the
    /// member spends the rest of the call unable to decrypt us.
    #[tokio::test]
    async fn a_return_under_the_same_member_id_over_the_limit_is_served_again() {
        let sender = Arc::new(MockCommandSender::new());
        let memberships = Arc::new(Mutex::new(vec![
            peer("@bob:example.org", "BOBDEV", "bob-a"),
            peer("@carol:example.org", "CAROLDEV", "carol-a"),
            peer("@dave:example.org", "DAVEDEV", "dave-a"),
            peer("@erin:example.org", "ERINDEV", "erin-a"),
        ]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        // Five participants including us, so a single departure leaves us over
        // the limit and nothing rotates.
        manager.set_config(EncryptionConfig {
            key_rotation_participant_limit: 4,
            ..EncryptionConfig::default()
        });
        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        sender.to_device_messages.lock().unwrap().clear();

        memberships
            .lock()
            .unwrap()
            .retain(|member| member.member_id != "dave-a");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        memberships
            .lock()
            .unwrap()
            .push(peer("@dave:example.org", "DAVEDEV", "dave-a"));
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            to_device_recipients(&sender),
            vec![("@dave:example.org".to_string(), "DAVEDEV".to_string())],
            "the returning member must be handed the key, and nobody else disturbed"
        );
        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(0),
            "and serving them must still not cost a rotation"
        );
    }

    /// The departure that happened while we were over the limit is still owed a
    /// rotation once the call can afford one — even though the leaver was dropped
    /// from `shared_with` at the time, so the roster diff no longer remembers it.
    #[tokio::test]
    async fn a_departure_over_the_limit_is_rotated_out_once_the_call_shrinks() {
        let sender = Arc::new(MockCommandSender::new());
        let memberships = Arc::new(Mutex::new(vec![
            peer("@bob:example.org", "BOBDEV", "bob-a"),
            peer("@carol:example.org", "CAROLDEV", "carol-a"),
            peer("@dave:example.org", "DAVEDEV", "dave-a"),
            peer("@erin:example.org", "ERINDEV", "erin-a"),
        ]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        manager.set_config(EncryptionConfig {
            key_rotation_participant_limit: 4,
            ..EncryptionConfig::default()
        });
        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        sender.to_device_messages.lock().unwrap().clear();

        // Erin leaves, still over the limit: no rotation, and she stops counting
        // as a holder of the key.
        memberships
            .lock()
            .unwrap()
            .retain(|member| member.member_id != "erin-a");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(0),
            "a departure over the limit must not rotate"
        );

        // Dave leaves too, taking us under the limit. Erin's departure is paid
        // for here, by the same rotation as dave's.
        memberships
            .lock()
            .unwrap()
            .retain(|member| member.member_id != "dave-a");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(1),
            "the key both leavers hold must be retired once we are under the limit"
        );
        assert_eq!(
            to_device_recipients(&sender),
            vec![
                ("@bob:example.org".to_string(), "BOBDEV".to_string()),
                ("@carol:example.org".to_string(), "CAROLDEV".to_string()),
            ],
            "and the members still present must get the rotated key"
        );
    }

    /// Dropping back under the limit resumes rotation, and since the suspension
    /// recorded nothing, the departures accumulated during it are retired by a
    /// single rotation that reaches everyone still present.
    #[tokio::test]
    async fn falling_back_under_the_participant_limit_retires_the_departed_in_one_rotation() {
        let sender = Arc::new(MockCommandSender::new());
        let memberships = Arc::new(Mutex::new(vec![
            peer("@bob:example.org", "BOBDEV", "bob-a"),
            peer("@carol:example.org", "CAROLDEV", "carol-a"),
            peer("@dave:example.org", "DAVEDEV", "dave-a"),
        ]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        manager.set_config(EncryptionConfig {
            key_rotation_participant_limit: 4,
            ..EncryptionConfig::default()
        });
        manager.join().await.expect("join should succeed");

        // Four participants including us: at the limit, so all three hold index 0
        // and it stays there.
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(sent_key_indices(&sender), vec![0, 0, 0]);
        sender.to_device_messages.lock().unwrap().clear();

        // Carol and dave leave in one update, taking the call under the limit.
        memberships
            .lock()
            .unwrap()
            .retain(|member| member.member_id == "bob-a");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(1),
            "resuming must retire the key the departed members hold, exactly once"
        );
        assert_eq!(
            sent_key_indices(&sender),
            vec![1],
            "the rotated key goes to whoever is left, and to nobody twice"
        );
        assert_eq!(
            to_device_recipients(&sender),
            vec![("@bob:example.org".to_string(), "BOBDEV".to_string())],
        );
    }

    /// The window is measured against the age of the current key, exactly as the
    /// joiner path measures it. So it only ever holds back a rotation for a key we
    /// just minted — which is what a burst looks like — and the ordinary case of
    /// one member leaving a call that has been running for a while pays no
    /// latency, and no extra time in which the leaver can still decrypt us.
    #[tokio::test]
    async fn a_departure_from_a_key_past_the_window_rotates_without_waiting() {
        const GRACE_MS: u64 = 150;

        let sender = Arc::new(MockCommandSender::new());
        let memberships = Arc::new(Mutex::new(vec![
            peer("@bob:example.org", "BOBDEV", "bob-a"),
            peer("@dave:example.org", "DAVEDEV", "dave-a"),
        ]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        manager.set_config(EncryptionConfig {
            leave_rotation_grace_period_ms: GRACE_MS,
            ..EncryptionConfig::default()
        });
        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        // The key ages past the window before anyone leaves.
        tokio::time::sleep(Duration::from_millis(GRACE_MS + 100)).await;
        sender.to_device_messages.lock().unwrap().clear();

        memberships
            .lock()
            .unwrap()
            .retain(|member| member.member_id != "bob-a");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(1),
            "a departure from a key older than the window must rotate on the spot"
        );
        assert_eq!(
            sent_key_indices(&sender),
            vec![1],
            "and the rotated key goes to whoever is left"
        );
        assert!(!manager.rotation_due(), "nothing was deferred");
    }

    /// A session outlives its `leave()`, so a rotation owed to a call we have hung
    /// up on must not survive into the next one — the first unchanged-roster push
    /// after rejoining would otherwise roll out a key for departures from a call
    /// that is over.
    #[tokio::test]
    async fn leaving_the_call_drops_a_deferred_rotation() {
        const GRACE_MS: u64 = 150;

        let sender = Arc::new(MockCommandSender::new());
        let memberships = Arc::new(Mutex::new(vec![
            peer("@bob:example.org", "BOBDEV", "bob-a"),
            peer("@dave:example.org", "DAVEDEV", "dave-a"),
        ]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        manager.set_config(EncryptionConfig {
            leave_rotation_grace_period_ms: GRACE_MS,
            ..EncryptionConfig::default()
        });
        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        memberships
            .lock()
            .unwrap()
            .retain(|member| member.member_id != "bob-a");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        tokio::time::sleep(Duration::from_millis(GRACE_MS + 100)).await;
        assert!(manager.rotation_due(), "the deferral should be pending");

        manager.leave();
        assert!(
            !manager.rotation_due(),
            "a rotation owed to a call we left is owed to nobody"
        );
    }

    /// `delayBeforeUse` is there to keep members who hold the outgoing key
    /// decrypting while its replacement propagates. When the call has sat empty,
    /// the key we hold went to nobody, so there is no continuity to protect — and
    /// waiting would leave the arriving member, who holds nothing at all, blind
    /// for the whole delay instead of just for their own key's delivery.
    #[tokio::test]
    async fn a_rotation_nobody_can_decrypt_is_usable_at_once() {
        let sender = Arc::new(MockCommandSender::new());
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        // No grace period, so an arrival always rotates rather than reusing.
        manager.set_config(EncryptionConfig {
            key_rotation_grace_period_ms: 0,
            ..EncryptionConfig::default()
        });
        manager.join().await.expect("join should succeed");

        // Bob is here and holds our key.
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        // Carol joins alongside him. Bob can decrypt what we are stamping, so the
        // rotation must wait for her to install it before we switch.
        let carol = JoinedMembership {
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("CAROLDEV".to_string())),
            sticky_key: "carol-a".to_string(),
            member_id: "carol-a".to_string(),
            ..bob_membership()
        };
        memberships.lock().unwrap().push(carol.clone());
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(
            handler
                .signals
                .lock()
                .unwrap()
                .last()
                .expect("the rotation should have been signalled")
                .use_after_ms,
            EncryptionConfig::default().delay_before_use_ms,
            "bob still holds the outgoing key, so switching away from it early \
             would cut him off"
        );

        // Now everyone leaves and, later, carol arrives alone. The key we hold
        // reached nobody who is present, so there is nothing to keep alive.
        memberships.lock().unwrap().clear();
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        *memberships.lock().unwrap() = vec![carol];
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            handler
                .signals
                .lock()
                .unwrap()
                .last()
                .expect("the rotation should have been signalled")
                .use_after_ms,
            0,
            "nobody present holds the outgoing key, so waiting only keeps the \
             arriving member undecryptable for longer"
        );
    }

    /// MSC4143 sends the same key to a member whenever their membership event
    /// changes, so a receiver sees repeats. Storing each one grows the key list
    /// without bound and re-imports identical material; `OutdatedKeyFilter`
    /// cannot catch it, because with no sender timestamp on the wire a
    /// re-delivery always looks newer than what we hold.
    #[tokio::test]
    async fn an_identical_key_redelivery_is_not_reimported() {
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            Arc::new(NoopCommandSender),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());

        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("bob's key should be accepted");
        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("the redelivery should be accepted too");

        assert_eq!(
            handler.signals.lock().unwrap().len(),
            1,
            "an identical key was imported twice"
        );
        assert_eq!(
            manager.get_inbound_keys("bob-device456-uuid").len(),
            1,
            "an identical key was stored twice"
        );
    }

    /// A *different* key at the same index is a real rotation and must still get
    /// through — the dedupe keys on the material, not the index alone.
    #[tokio::test]
    async fn a_different_key_at_the_same_index_is_still_imported() {
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            Arc::new(NoopCommandSender),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());

        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("bob's key should be accepted");
        manager
            .receive_key(bob_key(vec![9u8; 32], 0))
            .await
            .expect("bob's replacement key should be accepted");

        assert_eq!(handler.signals.lock().unwrap().len(), 2);
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

    /// A peer rekeying an index we already hold must win: their newer key is what
    /// they are encrypting with, so keeping the first one guarantees their media
    /// never decrypts.
    ///
    /// This used to assert the opposite — that the second key was "outdated" and
    /// dropped — which only held because both receives landed in the same
    /// millisecond. MSC4143 puts no creation timestamp on the wire, so
    /// `OutdatedKeyFilter` stamps receive time and cannot tell a stale key from a
    /// fresh one; treating an equal timestamp as stale threw away the good key.
    #[tokio::test]
    async fn a_rekey_at_the_same_index_replaces_the_key_we_held() {
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
        manager
            .receive_key(bob_key(vec![2u8; 32], 1))
            .await
            .expect("receive_key should succeed");

        // One entry per index, holding the newest material — so a later replay
        // cannot reinstall the superseded key.
        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1, "an index should not accumulate keys");
        assert_eq!(keys[0].key_index, 1);
        assert_eq!(keys[0].key, vec![2u8; 32], "the rekey should have won");
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

    /// A discarded key must reach the host with its reason attached. Downstream,
    /// a refused key and one that never arrived both surface as `MISSING_KEY`, so
    /// without this the host cannot tell a trust problem from a delivery one —
    /// the reason was warn-logged inside the core and went nowhere.
    #[tokio::test]
    async fn a_refused_key_is_reported_with_its_reason() {
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            Arc::new(NoopCommandSender),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());

        // MSC4153: bob's device is not cross-signed, so his key is refused.
        let mut key = bob_key(vec![1u8; 32], 3);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_cross_signed: false,
        };
        manager
            .receive_key(key)
            .await
            .expect("a discarded key is not an error");

        let discarded = handler.discarded.lock().unwrap();
        assert_eq!(
            *discarded,
            vec![DiscardedKey {
                member_id: "bob-device456-uuid".to_string(),
                key_index: Some(3),
                sender_user_id: Some("@bob:example.org".to_string()),
                sender_device_id: Some("device456".to_string()),
                reason: KeyRejection::NotCrossSigned,
            }],
        );
        assert!(
            handler.signals.lock().unwrap().is_empty(),
            "a refused key must not also be signalled as usable"
        );
    }

    /// The other rejection stage — the key passed the origin checks but does not
    /// match the member event it claims — must report too, and say who sent it.
    #[tokio::test]
    async fn a_key_from_the_wrong_device_is_reported_with_its_reason() {
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            Arc::new(NoopCommandSender),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());

        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("SOMEOTHERDEV".to_string()),
            sender_is_cross_signed: true,
        };
        manager
            .receive_key(key)
            .await
            .expect("a discarded key is not an error");

        let discarded = handler.discarded.lock().unwrap();
        assert_eq!(
            discarded.len(),
            1,
            "the rejection should have been reported"
        );
        assert_eq!(
            discarded[0].sender_device_id.as_deref(),
            Some("SOMEOTHERDEV"),
            "the host needs the device that actually sent it, to act on this",
        );
        assert!(matches!(
            discarded[0].reason,
            KeyRejection::DeviceMismatch { .. }
        ));
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

    /// A member event that only *claims* its device still binds the key to that
    /// device — the pre-2026 Element Call path, where no authenticated device
    /// exists to be had. The claim narrows rather than widens: the key must
    /// still arrive from the named device.
    #[tokio::test]
    async fn key_is_checked_against_a_claimed_device() {
        let claimed = |device_id: &str| {
            EncryptionManager::new(
                Arc::new(NoopCommandSender),
                USER_ID.to_string(),
                DEVICE_ID.to_string(),
                MEMBER_ID.to_string(),
                ROOM_ID.to_string(),
                SLOT_ID.to_string(),
                create_mock_get_memberships(vec![JoinedMembership {
                    origin: EventOrigin::claimed(device_id),
                    ..bob_membership()
                }]),
            )
        };

        // Bob's key arrives Olm-encrypted from the device his member event
        // named, which is exactly the match MSC4143 asks for.
        let manager = claimed("device456");
        manager
            .receive_key(bob_key(vec![1u8; 32], 0))
            .await
            .expect("should succeed");
        assert_eq!(manager.get_inbound_keys("bob-device456-uuid").len(), 1);

        // A claim naming some other device is not a licence to accept anything:
        // the key came from `device456`, so it does not match.
        assert_discarded(&claimed("someOtherDevice"), bob_key(vec![1u8; 32], 0)).await;
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

    /// A membership of our own device under a *different* `member_id` — the
    /// ghost we leave behind by dying without leaving and rejoining inside its
    /// sticky lifetime. It must not be treated as a recipient: Olm has no
    /// session with the sending device, so the send fails, and the same
    /// user+device under a new id also forces a rotation — which the failure
    /// would then abandon.
    #[tokio::test]
    async fn our_own_stale_membership_is_never_a_key_recipient() {
        let ghost = JoinedMembership {
            member_id: "alice-device123-previous-join".to_string(),
            sticky_key: "alice-device123-previous-join".to_string(),
            sender: USER_ID.to_string(),
            origin: EventOrigin::encrypted(Some(DEVICE_ID.to_string())),
            ..bob_membership()
        };
        let mock_sender = Arc::new(MockCommandSender::new());
        let get_memberships = create_mock_get_memberships(vec![ghost, bob_membership()]);
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
        manager
            .on_memberships_update()
            .await
            .expect("distribution should succeed");

        assert!(
            to_device_recipients(&mock_sender)
                .iter()
                .all(|(user_id, device_id)| !(user_id == USER_ID && device_id == DEVICE_ID)),
            "we must never send our own media key to our own device, got {:?}",
            to_device_recipients(&mock_sender),
        );
        assert_eq!(
            to_device_recipients(&mock_sender),
            vec![("@bob:example.org".to_string(), "device456".to_string())],
            "bob is the only real recipient",
        );
    }

    /// Another device of our own user is an ordinary recipient — only *this*
    /// device is excluded.
    #[tokio::test]
    async fn another_device_of_our_own_user_still_receives_the_key() {
        let other_device = JoinedMembership {
            member_id: "alice-tablet-uuid".to_string(),
            sticky_key: "alice-tablet-uuid".to_string(),
            sender: USER_ID.to_string(),
            origin: EventOrigin::encrypted(Some("TABLET".to_string())),
            ..bob_membership()
        };
        let mock_sender = Arc::new(MockCommandSender::new());
        let get_memberships = create_mock_get_memberships(vec![other_device]);
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
        manager
            .on_memberships_update()
            .await
            .expect("distribution should succeed");

        assert_eq!(
            to_device_recipients(&mock_sender),
            vec![(USER_ID.to_string(), "TABLET".to_string())],
        );
    }

    /// No `"*"` fallback: a membership that names no sending device is
    /// unreachable, and broadcasting the key to every device of that user would
    /// hand it to devices that are not in the call.
    #[tokio::test]
    async fn a_membership_with_no_sending_device_is_skipped_not_broadcast() {
        let deviceless = JoinedMembership {
            origin: EventOrigin::encrypted(None),
            ..bob_membership()
        };
        let mock_sender = Arc::new(MockCommandSender::new());
        let get_memberships = create_mock_get_memberships(vec![deviceless]);
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
        manager
            .on_memberships_update()
            .await
            .expect("distribution should succeed");

        assert!(
            to_device_recipients(&mock_sender).is_empty(),
            "expected no send at all, got {:?}",
            to_device_recipients(&mock_sender),
        );
    }

    /// One unreachable recipient must not cost the others their key, must not
    /// abandon the rotation (the key still has to be stored and signalled) —
    /// and must not be recorded as holding the key, or it is never retried and
    /// that member cannot decrypt us for the rest of the call.
    #[tokio::test]
    async fn a_failed_recipient_does_not_abort_the_rollout() {
        struct FailsFirstRecipient {
            attempted: Mutex<Vec<String>>,
        }

        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        impl RtcCommandSender for FailsFirstRecipient {
            async fn send_sticky_event(
                &self,
                _room_id: String,
                _event_type: String,
                _content: serde_json::Value,
                _duration_ms: u64,
            ) -> Result<(), CommandError> {
                Ok(())
            }
            async fn send_delayed_event(
                &self,
                _room_id: String,
                _event_type: String,
                _content: serde_json::Value,
                _delay_ms: u64,
            ) -> Result<String, CommandError> {
                Ok("$delay".to_string())
            }
            async fn restart_delayed_event(
                &self,
                _room_id: String,
                _delay_id: String,
            ) -> Result<(), CommandError> {
                Ok(())
            }
            async fn cancel_delayed_event(
                &self,
                _room_id: String,
                _delay_id: String,
            ) -> Result<(), CommandError> {
                Ok(())
            }
            async fn send_to_device_message(
                &self,
                recipients: Vec<ToDeviceRecipient>,
                _message_type: String,
                _content: serde_json::Value,
            ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
                let mut attempted = self.attempted.lock().unwrap();
                Ok(recipients
                    .into_iter()
                    .map(|recipient| {
                        attempted.push(recipient.user_id.clone());
                        if recipient.user_id == "@bob:example.org" {
                            ToDeviceDelivery::failed(recipient, "no olm session")
                        } else {
                            ToDeviceDelivery::sent(recipient)
                        }
                    })
                    .collect())
            }
            async fn send_state_event(
                &self,
                _room_id: String,
                _event_type: String,
                _state_key: String,
                _content: serde_json::Value,
            ) -> Result<(), CommandError> {
                Ok(())
            }
        }

        let carol = JoinedMembership {
            member_id: "carol-device789-uuid".to_string(),
            sticky_key: "carol-device789-uuid".to_string(),
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("device789".to_string())),
            ..bob_membership()
        };
        let sender = Arc::new(FailsFirstRecipient {
            attempted: Mutex::new(Vec::new()),
        });
        let handler = Arc::new(RecordingHandler::default());
        let get_memberships = create_mock_get_memberships(vec![bob_membership(), carol]);
        let mut manager = EncryptionManager::new(
            sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        manager.set_signal_handler(handler.clone());

        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("a failed recipient must not fail the rollout");

        assert_eq!(
            *sender.attempted.lock().unwrap(),
            vec!["@bob:example.org", "@carol:example.org"],
            "carol must still be attempted after bob failed",
        );
        assert!(
            manager.get_outbound_key().is_some(),
            "the key must still be stored, or we rotate again next update and never converge",
        );
        assert!(
            !handler.signals.lock().unwrap().is_empty(),
            "the key must still reach the media layer, or we encrypt with nothing",
        );

        let shared_with: Vec<String> = manager
            .get_outbound_key()
            .expect("the key is stored")
            .shared_with
            .into_iter()
            .map(|participant| participant.user_id)
            .collect();
        assert_eq!(
            shared_with,
            vec!["@carol:example.org"],
            "only the recipient the send actually reached may be recorded as holding \
             the key; recording bob would mean never retrying him",
        );

        // Next rollout: bob is not in `shared_with`, so he counts as newly
        // joined and is addressed again.
        sender.attempted.lock().unwrap().clear();
        manager
            .on_memberships_update()
            .await
            .expect("the retry rollout should succeed");
        assert_eq!(
            *sender.attempted.lock().unwrap(),
            vec!["@bob:example.org"],
            "the unreached recipient should be retried, and the served one left alone",
        );
    }
}
