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
// You should have received a copy of the GNU Affero General Public License
// along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

//! Types for the encryption module.
//!
//! This module defines the data structures used for managing encryption keys
//! in Matrix RTC sessions as specified in MSC4143.

use std::collections::HashMap;

/// Unique identifier for a participant's device in an RTC session.
///
/// MSC4143 uses `member_id` as the globally unique identifier for a participation
/// instance. This struct tracks the user, device, and member IDs for a participant.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ParticipantDeviceInfo {
    /// Matrix user ID of the participant
    pub user_id: String,
    /// Device ID of the participant
    pub device_id: String,
    /// The `member.id` from the `m.rtc.member` event (MSC4143)
    /// This is globally unique per member instance
    pub member_id: String,
}

impl ParticipantDeviceInfo {
    /// Creates a map key for this participant using the member_id.
    ///
    /// MSC4143 uses member_id as the primary key for participants.
    pub fn map_key(&self) -> String {
        self.member_id.clone()
    }
}

/// Authenticated provenance of an inbound `m.rtc.encryption_key` to-device
/// message.
///
/// MSC4143 requires the recipient to check the message against the sender's
/// `m.rtc.member` event, so the core needs to know who actually sent it. That
/// is only knowable from Olm decryption metadata, which the host supplies —
/// nothing in the message content is trustworthy for this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyOrigin {
    /// The message arrived Olm-encrypted; these values come from the
    /// decryption metadata, not from the payload.
    Encrypted {
        /// User the message was decrypted as coming from.
        sender_user_id: String,
        /// Device the message was decrypted as coming from, when the host
        /// could determine it.
        sender_device_id: Option<String>,
        /// Whether that device is cross-signed (MSC4153).
        sender_is_cross_signed: bool,
    },
    /// The message arrived in the clear, so it has no authenticated sender.
    Cleartext,
}

/// Why an inbound `m.rtc.encryption_key` message was discarded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyRejection {
    /// Sent in cleartext, so the sender cannot be authenticated (MSC4143).
    Cleartext,
    /// The sending device is not cross-signed (MSC4153).
    NotCrossSigned,
    /// The message names a different room than this session's.
    RoomMismatch {
        /// The `room_id` carried in the message.
        claimed: String,
    },
    /// The sender does not match the one on the member event it claims.
    SenderMismatch {
        /// The user the member event was sent by.
        expected: String,
        /// The user that actually sent the key.
        actual: String,
    },
    /// The member event names no sending device, so the required match cannot
    /// be performed at all. Not expected in practice for an encrypted member
    /// event — Olm messages carry the sender's device keys.
    UnverifiableDevice,
    /// The sending device does not match the one that sent the member event.
    DeviceMismatch {
        /// The device the member event was encrypted by.
        expected: String,
        /// The device that actually sent the key, if known.
        actual: Option<String>,
    },
}

impl std::fmt::Display for KeyRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cleartext => write!(f, "sent in cleartext"),
            Self::NotCrossSigned => write!(f, "sending device is not cross-signed"),
            Self::UnverifiableDevice => write!(
                f,
                "the member event names no sending device to check this against"
            ),
            Self::RoomMismatch { claimed } => write!(f, "claims a different room ({claimed})"),
            Self::SenderMismatch { expected, actual } => {
                write!(
                    f,
                    "sent by {actual}, but the member event is from {expected}"
                )
            }
            Self::DeviceMismatch { expected, actual } => write!(
                f,
                "sent by device {}, but the member event came from {expected}",
                actual.as_deref().unwrap_or("<unknown>")
            ),
        }
    }
}

/// An inbound `m.rtc.encryption_key` message, with the provenance needed to
/// decide whether to trust it.
#[derive(Clone, Debug)]
pub struct ReceivedEncryptionKey {
    /// How the message reached us, and from whom.
    pub origin: KeyOrigin,
    /// The `room_id` carried in the message content.
    pub room_id: String,
    /// The `member_id` carried in the message content, naming the sender's
    /// `m.rtc.member` event.
    pub member_id: String,
    /// The key material, encoded per the message's `format`.
    pub key_b64: String,
    /// The rolling key index (0-255).
    pub key_index: u8,
}

/// An inbound key whose `m.rtc.member` event has not arrived yet, held with the
/// provenance needed to verify it once the membership shows up.
#[derive(Clone, Debug)]
pub(crate) struct PendingInboundKey {
    pub key: InboundEncryptionKey,
    pub origin: KeyOrigin,
}

/// An inbound encryption key received from another participant.
///
/// These keys are used to decrypt media streams from other participants.
/// They are received via to-device messages of type `m.rtc.encryption_key` (MSC4143).
#[derive(Clone, Debug)]
pub struct InboundEncryptionKey {
    /// Raw key bytes (32 bytes for AES-256)
    pub key: Vec<u8>,
    /// Key index (0-255), used in S-Frame headers
    pub key_index: u8,
    /// The `member_id` from the sender's `m.rtc.member` event
    pub member_id: String,
    /// Timestamp (ms) when this key was created by the sender
    pub creation_ts: u64,
}

/// An outbound encryption key for encrypting our own media.
///
/// This key is distributed to other participants via to-device messages
/// and used to encrypt media streams we send to the transport layer.
#[derive(Clone, Debug)]
pub struct OutboundEncryptionKey {
    /// Raw key bytes (32 bytes for AES-256)
    pub key: Vec<u8>,
    /// Key index (0-255)
    pub key_index: u8,
    /// Timestamp (ms) when this key was created
    pub creation_ts: u64,
    /// List of participants this key has been shared with
    pub shared_with: Vec<ParticipantDeviceInfo>,
}

/// Signal sent to the application when new key material is available.
///
/// The application layer uses these raw bytes with key derivation/stretching
/// to produce the actual encryption keys needed for media encryption/decryption.
#[derive(Clone, Debug)]
pub struct KeyMaterialSignal {
    /// Raw key bytes
    pub key: Vec<u8>,
    /// Key index
    pub key_index: u8,
    /// RTC backend identity string for this participant
    /// Used by the media layer to identify the key source
    pub rtc_backend_identity: String,
    /// How long the consumer must wait before *using* this key to encrypt,
    /// in milliseconds (MSC4143 `delayBeforeUse`). `0` means usable at once.
    ///
    /// Non-zero only for our own rotated outbound key: peers need time to
    /// receive it before we start encrypting with it. Inbound keys and the very
    /// first outbound key are always `0` — an inbound key is only ever used to
    /// *decrypt*, where waiting would just drop frames.
    ///
    /// Honouring this is the consumer's job, and it must not do so by blocking:
    /// the core signals on its caller's task, which for the FFI is a
    /// synchronous host call. Schedule the activation and return.
    pub use_after_ms: u64,
}

/// Configuration for the encryption manager.
///
/// These parameters control key rotation behavior as specified in MSC4143.
#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    /// Time to wait (ms) before using a newly distributed key (MSC4143: delayBeforeUse).
    ///
    /// This ensures the key has time to be delivered to all participants before
    /// it is used for encryption. Default: 5000ms (5 seconds).
    pub delay_before_use_ms: u64,

    /// Grace period (ms) for key rotation (MSC4143: keyRotationGracePeriod).
    ///
    /// If a new participant joins within this period after the current key was
    /// created, the current key is reused instead of rotating. This prevents
    /// expensive key rotations when users quickly join in a row.
    ///
    /// Must be greater than `delay_before_use_ms` to have an effect.
    /// Default: 10000ms (10 seconds).
    pub key_rotation_grace_period_ms: u64,

    /// Whether to manage media keys (default: true).
    ///
    /// If false, the encryption manager will not distribute keys or signal
    /// key material to the application. This is useful for testing or for
    /// sessions that don't require encryption.
    pub manage_media_keys: bool,

    /// Whether to discard keys from devices that are not cross-signed
    /// (default: true).
    ///
    /// MSC4143 defers to [MSC4153] here: clients that exclude insecure devices
    /// elsewhere SHOULD also exclude them as key sources. Turn this off only
    /// where unverified devices are expected, such as throwaway test logins.
    ///
    /// [MSC4153]: https://github.com/matrix-org/matrix-spec-proposals/pull/4153
    pub require_cross_signed_sender: bool,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            delay_before_use_ms: 5000,            // MSC4143 default
            key_rotation_grace_period_ms: 10_000, // MSC4143 default
            manage_media_keys: true,
            require_cross_signed_sender: true,
        }
    }
}

/// Filter for detecting outdated keys.
///
/// This handles the case where keys might arrive out of order, e.g., after a
/// quick join/leave/join, there might be multiple keys at the same index but
/// with different timestamps. The filter keeps only the latest key at each
/// index for each participant.
///
/// From MSC4143: "It is possible that keys arrive in the wrong order. For example,
/// after a quick join/leave/join, there will be 2 keys of index 0 distributed, and
/// if they are received in the wrong order, the stream won't be decryptable."
///
/// # What this can actually detect
///
/// MSC4143 puts no creation timestamp on the wire, so the only timestamp
/// available is the moment *we* received the key. Receive order is therefore the
/// whole of our ordering information, and "the latest key" can only mean "the
/// last one received" — this filter cannot recognise a genuinely stale key that
/// merely arrived late, which is the case the MSC is describing. It is kept
/// because it is the right shape for the day a sender timestamp exists, and
/// because it still bounds one real case: two deliveries stamped identically.
///
/// Deduplicating *identical* re-deliveries is a separate job, done on the key
/// material itself where a re-send of the same key is recognisable without any
/// timestamp at all.
#[derive(Clone, Debug)]
pub struct OutdatedKeyFilter {
    /// Buffer tracking the latest timestamp per (member_id, key_index)
    /// Key: "member_id:index", Value: timestamp
    pub buffer: HashMap<String, u64>,
    /// Buffer TTL in milliseconds - entries older than this will be cleaned up
    pub buffer_ttl_ms: u64,
}

impl Default for OutdatedKeyFilter {
    fn default() -> Self {
        Self {
            buffer: HashMap::new(),
            buffer_ttl_ms: 5000, // 5 seconds
        }
    }
}

impl OutdatedKeyFilter {
    /// Creates a new OutdatedKeyFilter with the specified TTL.
    pub fn with_ttl(ttl_ms: u64) -> Self {
        Self {
            buffer: HashMap::new(),
            buffer_ttl_ms: ttl_ms,
        }
    }

    /// Checks if a candidate key is outdated.
    ///
    /// Returns `true` if the key should be dropped (outdated), `false` otherwise.
    ///
    /// # Arguments
    /// * `member_id` - The member ID of the participant
    /// * `key_index` - The key index
    /// * `candidate_ts` - The timestamp of the candidate key
    ///
    /// # Logic
    /// If we already have a key from this member at the same index with a
    /// timestamp strictly newer than the candidate's, the candidate is outdated
    /// and should be dropped.
    ///
    /// Equal timestamps let the candidate through, deliberately. Timestamps are
    /// stamped on receipt (see the type docs), so two keys at one index in the
    /// same millisecond are a rekey we saw twice in quick succession, not
    /// evidence of staleness — and the later one is the one the sender is now
    /// encrypting with. Treating equal as outdated silently discarded it and left
    /// the stream undecryptable, which is the failure this filter exists to
    /// prevent.
    pub fn is_outdated(&self, member_id: &str, key_index: u8, candidate_ts: u64) -> bool {
        let key = format!("{}:{}", member_id, key_index);
        if let Some(&existing_ts) = self.buffer.get(&key) {
            // Only a strictly newer key already in hand makes this one outdated.
            if existing_ts > candidate_ts {
                return true;
            }
        }
        false
    }

    /// Adds a key to the filter and returns whether it was outdated.
    ///
    /// If the key is not outdated, it's added to the buffer.
    /// Returns `true` if the key was outdated (and should be dropped).
    pub fn check_and_add(&mut self, member_id: String, key_index: u8, candidate_ts: u64) -> bool {
        let outdated = self.is_outdated(&member_id, key_index, candidate_ts);
        if !outdated {
            let key = format!("{}:{}", member_id, key_index);
            self.buffer.insert(key, candidate_ts);
        }
        outdated
    }

    /// Cleans up old entries from the buffer.
    ///
    /// Removes entries whose timestamp is older than `current_ts - buffer_ttl_ms`.
    pub fn cleanup(&mut self, current_ts: u64) {
        self.buffer
            .retain(|_, ts| current_ts.saturating_sub(*ts) < self.buffer_ttl_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_participant_device_info_map_key() {
        let info = ParticipantDeviceInfo {
            user_id: "@alice:example.org".to_string(),
            device_id: "device123".to_string(),
            member_id: "xyzABCDEF0123".to_string(),
        };

        assert_eq!(info.map_key(), "xyzABCDEF0123");
    }

    #[test]
    fn test_encryption_config_default() {
        let config = EncryptionConfig::default();

        assert_eq!(config.delay_before_use_ms, 5000);
        assert_eq!(config.key_rotation_grace_period_ms, 10_000);
        assert!(config.manage_media_keys);
    }

    #[test]
    fn test_outdated_key_filter_not_outdated() {
        let filter = OutdatedKeyFilter::default();

        let member_id = "@alice:example.org:device123".to_string();

        // First key at index 0 with timestamp 1000 - should not be outdated
        assert!(!filter.is_outdated(&member_id, 0, 1000));
    }

    #[test]
    fn test_outdated_key_filter_same_index_newer_timestamp() {
        let mut filter = OutdatedKeyFilter::default();

        let member_id = "@alice:example.org:device123".to_string();

        // Add key at index 0 with timestamp 1000
        filter
            .buffer
            .insert("@alice:example.org:device123:0".to_string(), 1000);

        // New key at same index with newer timestamp 2000 - should not be outdated
        assert!(!filter.is_outdated(&member_id, 0, 2000));

        // New key at same index with older timestamp 500 - should be outdated
        assert!(filter.is_outdated(&member_id, 0, 500));
    }

    #[test]
    fn test_outdated_key_filter_different_index() {
        let mut filter = OutdatedKeyFilter::default();

        let member_id = "@alice:example.org:device123".to_string();

        // Add key at index 0 with timestamp 1000
        filter
            .buffer
            .insert("@alice:example.org:device123:0".to_string(), 1000);

        // Key at index 1 should not be outdated regardless of timestamp
        assert!(!filter.is_outdated(&member_id, 1, 500));
        assert!(!filter.is_outdated(&member_id, 1, 2000));
    }

    #[test]
    fn test_outdated_key_filter_check_and_add() {
        let mut filter = OutdatedKeyFilter::default();

        let member_id = "@alice:example.org:device123".to_string();

        // First key at index 0 with ts=1000 - not outdated, should be added
        assert!(!filter.check_and_add(member_id.clone(), 0, 1000));
        assert!(filter.buffer.contains_key("@alice:example.org:device123:0"));

        // Second key at index 0 with ts=500 - outdated, should NOT be added
        assert!(filter.check_and_add(member_id.clone(), 0, 500));
        // Buffer should still have the first key
        assert_eq!(
            filter.buffer.get("@alice:example.org:device123:0"),
            Some(&1000)
        );

        // Third key at index 0 with ts=2000 - not outdated, should replace
        assert!(!filter.check_and_add(member_id.clone(), 0, 2000));
        assert_eq!(
            filter.buffer.get("@alice:example.org:device123:0"),
            Some(&2000)
        );
    }

    #[test]
    fn test_outdated_key_filter_cleanup() {
        let mut filter = OutdatedKeyFilter::with_ttl(1000); // 1 second TTL

        // Add old key
        filter
            .buffer
            .insert("@alice:example.org:device123:0".to_string(), 1000);

        // Cleanup at ts=3000 - should remove key older than 1000ms
        filter.cleanup(3000);
        assert!(filter.buffer.is_empty());

        // Add recent key
        filter
            .buffer
            .insert("@alice:example.org:device123:0".to_string(), 2500);

        // Cleanup at ts=3000 - should keep key (only 500ms old)
        filter.cleanup(3000);
        assert!(!filter.buffer.is_empty());
    }
}
