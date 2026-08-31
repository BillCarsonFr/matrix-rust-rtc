//! Per-member media key exchange over to-device messages (MSC4143).
//!
//! [`SendMachine`] owns the outbound half: it chunks the call into time
//! intervals; session changes inside an interval trigger a (rate-limited)
//! key rotation. [`Machine`] owns the `SendMachine` plus every key received
//! over time (the [`KeyMap`] a host feeds into its LiveKit key provider).

use crate::driver::{DriverError, ToDeviceDriver, ToDeviceSendDriver};
use crate::session::SessionSnapshot;
use crate::types::Member;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

/// Key material + index, ready for `room.set_key_for_participant`.
#[derive(Clone, Debug, PartialEq)]
pub struct MediaKey {
    pub key: Vec<u8>,
    pub index: u8,
    pub creation_ts_ms: u64,
}

/// member id -> current media key. The host-facing output of this module.
pub type KeyMap = HashMap<String, MediaKey>;

/// How an inbound key message arrived, from Olm decryption metadata.
#[derive(Clone, Debug, PartialEq)]
pub enum KeyOrigin {
    EncryptedToDevice { sender_device_id: String },
    Cleartext,
    Unknown,
}

/// A decoded inbound `m.rtc.encryption_key` message, pre-verification.
#[derive(Clone, Debug)]
pub struct ReceivedEncryptionKey {
    pub room_id: String,
    /// The member event this key names.
    pub member_id: String,
    pub sender_user_id: String,
    pub origin: KeyOrigin,
    pub key: Vec<u8>,
    pub index: u8,
}

/// Result of accepting a key that passed verification.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum KeyOutcome {
    /// Stored and signalled through the key map.
    Stored,
    /// Its member event has not arrived yet: buffered *with its origin* and
    /// verified when the membership shows up — deferred, never skipped.
    Buffered,
}

/// Why an inbound key was discarded. Rejected keys never occupy a
/// `(member, index)` slot, so a bogus key cannot suppress the genuine one.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum KeyRejection {
    #[error("key arrived in cleartext")]
    Cleartext,
    #[error("to-device sender does not match the member event sender")]
    SenderMismatch,
    #[error("sending device does not match the member event's device")]
    DeviceMismatch,
    #[error("member event names no device to check against")]
    UnattributableMember,
    #[error("sender device is not cross-signed (MSC4153)")]
    NotCrossSigned,
    #[error("key names another room")]
    WrongRoom,
    #[error("outdated (member, index) replay")]
    Outdated,
}

#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    /// Reject keys from non-cross-signed devices (MSC4153). Default on.
    pub require_cross_signed_sender: bool,
    /// Local default; overridden by the slot's negotiated encryption.
    pub manage_media_keys: bool,
}

#[derive(Clone, Debug)]
pub struct SendMachineConfig {
    /// Length of the rotation-check interval the call is chunked into.
    pub rotation_interval_ms: u64,
    /// Share the *current* key with a joiner inside the interval (they can
    /// decrypt up to `last_rotation_ts`) instead of forcing a rotation.
    pub share_current_key_with_joiners: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct JoinStatus {
    pub has_distributed_initial_keys: bool,
    pub has_received_all_member_keys: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ConnectedStatus {
    /// Members who left but still hold a not-yet-rotated key — display as
    /// "possibly still listening".
    pub left_members_with_keys: Vec<Member>,
    /// Every current member holds only current keys.
    pub fully_settled: bool,
    /// Upper bound on what a new joiner could decrypt retroactively; a UI
    /// threshold, not for direct rendering.
    pub last_rotation_ts: u64,
}

pub type OwnKeyCallback = Box<dyn Fn(&MediaKey) + Send + Sync>;
pub type KeyMapCallback = Box<dyn Fn(&KeyMap) + Send + Sync>;

/// Outbound key distribution and rotation.
pub struct SendMachine {
    driver: Arc<dyn ToDeviceSendDriver>,
    session: watch::Receiver<SessionSnapshot>,
    config: SendMachineConfig,
    on_key_for_own_member_change: OwnKeyCallback,
    next_rotation_check_ts: Option<u64>,
}

impl SendMachine {
    pub fn new(
        driver: Arc<dyn ToDeviceSendDriver>,
        session: watch::Receiver<SessionSnapshot>,
        config: SendMachineConfig,
        on_key_for_own_member_change: OwnKeyCallback,
    ) -> Self {
        todo!()
    }

    /// Distribute the current key to every joined member's device that has
    /// not received it; per-recipient results decide who is recorded as
    /// served (failures are retried on the next rollout).
    async fn ensure_key_distribution(&self) -> Result<(), DriverError> {
        todo!()
    }

    /// When the next deferred rotation is due (for host scheduling).
    fn rotation_due_at_ms(&self) -> Option<u64> {
        todo!()
    }

    /// Perform a due rotation now, if any.
    async fn flush_due_rotation(&self) -> Result<(), DriverError> {
        todo!()
    }
}

/// The full encryption machine: owns the [`SendMachine`], verifies and
/// stores inbound keys, and emits the merged [`KeyMap`].
pub struct Machine {
    send_machine: SendMachine,
    key_map: KeyMap,
    on_key_map_change: KeyMapCallback,
}

impl Machine {
    pub fn new(
        driver: Arc<dyn ToDeviceDriver>,
        session: watch::Receiver<SessionSnapshot>,
        own_member: Member,
        config: EncryptionConfig,
        send_config: SendMachineConfig,
        on_key_map_change: KeyMapCallback,
    ) -> Self {
        todo!()
    }

    /// Verify and ingest one inbound key (see [`KeyRejection`] for the rules
    /// carried over from the current implementation).
    async fn receive_key(
        &self,
        key: ReceivedEncryptionKey,
    ) -> Result<KeyOutcome, KeyRejection> {
        todo!()
    }

    pub fn key_map(&self) -> &KeyMap {
        todo!()
    }

    /// Re-emit the current key map (for hosts attaching a media session late).
    pub fn replay_key_map(&self) {
        todo!()
    }

    pub fn join_status(&self) -> JoinStatus {
        todo!()
    }

    pub fn connected_status(&self) -> ConnectedStatus {
        todo!()
    }
}
