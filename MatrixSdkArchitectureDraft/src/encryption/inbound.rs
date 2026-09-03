//! The remote-key side of the module, plus the shared [`KeyMap`].
//!
//! [`InboundKeys`] does four things with everybody else's keys:
//!
//! - **Verifies** each incoming key against the room, the origin metadata,
//!   the cross-signing flag, and the member event's sender and device.
//! - **Buffers** keys whose member event has not arrived yet (up to
//!   [`EARLY_KEY_TTL_MS`] and [`EARLY_KEY_CAP`] entries) and re-verifies
//!   them on every session change.
//! - **Filters replays** with the [`OutdatedKeyFilter`], so a newer arrival
//!   for a `(member, index)` slot wins.
//! - **Stores** the accepted keys as the [`KeyMap`], one entry per index per
//!   member, and reports which key changed.
//!
//! One wrinkle: the `KeyMap` is also where the host reads *our own* key, so
//! [`InboundKeys::set_own_key`] writes our key into the same map when the
//! send machine says it is in use. That is the only non-remote thing here,
//! and it exists because the host wants one map for all participants — the
//! send machine owns the outbound key and its lifecycle, this file merely
//! mirrors the in-use one into the map. So the split is: `send_machine.rs`
//! is ours, `inbound.rs` is theirs plus the shared map.
//!
//! Pure: no I/O, no clock of its own (`now` is passed in), so every rule is
//! a plain unit test.

use super::{
    EncryptionConfig, KeyMap, KeyOrigin, KeyOutcome, KeyRejection, MediaKey, MediaKeyChange,
    ReceivedEncryptionKey,
};
use crate::types::{DeviceAttribution, Member};
use std::collections::HashMap;

/// How long a key whose membership has not shown up yet is kept.
pub const EARLY_KEY_TTL_MS: u64 = 300_000;
/// How many such keys are kept (oldest evicted first).
pub const EARLY_KEY_CAP: usize = 256;

/// Rejects a `(member, index)` arrival that is *older* than one already
/// accepted for that slot. MSC4143 carries no timestamp, so "older" means
/// "received earlier" — the only order we have; equal timestamps pass (two
/// keys in one millisecond are a rekey seen twice). Entries expire after
/// `ttl_ms` so the map stays bounded by what arrived recently.
#[derive(Clone, Debug)]
pub struct OutdatedKeyFilter {
    seen: HashMap<(String, u8), u64>,
    ttl_ms: u64,
}

impl Default for OutdatedKeyFilter {
    fn default() -> Self {
        Self::with_ttl(5_000)
    }
}

impl OutdatedKeyFilter {
    pub fn with_ttl(ttl_ms: u64) -> Self {
        Self {
            seen: HashMap::new(),
            ttl_ms,
        }
    }

    pub fn is_outdated(&self, member_id: &str, index: u8, candidate_ts: u64) -> bool {
        self.seen
            .get(&(member_id.to_owned(), index))
            .is_some_and(|&existing| existing > candidate_ts)
    }

    /// Records the arrival unless it is outdated; returns the verdict.
    pub fn check_and_add(&mut self, member_id: &str, index: u8, ts: u64) -> bool {
        self.cleanup(ts);
        if self.is_outdated(member_id, index, ts) {
            return true;
        }
        self.seen.insert((member_id.to_owned(), index), ts);
        false
    }

    pub fn cleanup(&mut self, now: u64) {
        let ttl = self.ttl_ms;
        self.seen.retain(|_, ts| now.saturating_sub(*ts) < ttl);
    }
}

#[derive(Clone, Debug)]
struct EarlyKey {
    key: ReceivedEncryptionKey,
    received_ts: u64,
}

pub struct InboundKeys {
    room_id: String,
    config: EncryptionConfig,
    manage_media_keys: bool,
    own_member_id: Option<String>,
    key_map: KeyMap,
    filter: OutdatedKeyFilter,
    early: Vec<EarlyKey>,
    /// The most recent rejection per member, cleared when a key from that
    /// member is accepted. The crate already computes the *why* a peer's
    /// media is undecryptable and used to throw it away
    /// (`ErrorSurfaceAnalysis.md` §4.1); latching it is what puts it in
    /// `Status`, where a UI attaching late still finds it.
    rejections: HashMap<String, (KeyRejection, u64)>,
}

impl InboundKeys {
    /// Lives for one participation (`own_member_id`); dropping it forgets
    /// every key.
    pub fn new(
        room_id: String,
        config: EncryptionConfig,
        own_member_id: String,
        manage_media_keys: bool,
    ) -> Self {
        Self {
            room_id,
            config,
            manage_media_keys,
            own_member_id: Some(own_member_id),
            key_map: KeyMap::new(),
            filter: OutdatedKeyFilter::default(),
            early: Vec::new(),
            rejections: HashMap::new(),
        }
    }

    pub fn key_map(&self) -> &KeyMap {
        &self.key_map
    }

    pub fn manages_media_keys(&self) -> bool {
        self.manage_media_keys
    }

    pub fn early_key_count(&self) -> usize {
        self.early.len()
    }

    /// Whether we hold at least one usable key from this member.
    pub fn have_key_from(&self, member_id: &str) -> bool {
        self.key_map
            .get(member_id)
            .is_some_and(|ring| !ring.is_empty())
    }

    /// The latched reason this member's last key was discarded.
    pub fn rejection(&self, member_id: &str) -> Option<&KeyRejection> {
        self.rejections.get(member_id).map(|(r, _)| r)
    }

    /// When that rejection happened — carried alongside the reason because
    /// `Impairment::MediaKeyRejected` reports *when* the crate gave up on
    /// this member's key, and nothing else records it.
    pub fn rejected_at(&self, member_id: &str) -> Option<u64> {
        self.rejections.get(member_id).map(|(_, ts)| *ts)
    }

    /// Latch or clear the rejection for `member_id` from one outcome.
    /// `Buffered` is neither: the verdict is still pending.
    fn record(&mut self, member_id: &str, outcome: &Result<KeyOutcome, KeyRejection>, now: u64) {
        match outcome {
            Ok(KeyOutcome::Stored(_) | KeyOutcome::Duplicate) => {
                self.rejections.remove(member_id);
            }
            Ok(KeyOutcome::Buffered) => {}
            Err(rejection) => {
                self.rejections
                    .insert(member_id.to_owned(), (rejection.clone(), now));
            }
        }
    }

    /// One inbound key, pre-verification, against the current members.
    pub fn receive(
        &mut self,
        key: ReceivedEncryptionKey,
        members: &[Member],
        now: u64,
    ) -> Result<KeyOutcome, KeyRejection> {
        let member_id = key.member_id.clone();
        let outcome = self.receive_inner(key, members, now);
        self.record(&member_id, &outcome, now);
        outcome
    }

    fn receive_inner(
        &mut self,
        key: ReceivedEncryptionKey,
        members: &[Member],
        now: u64,
    ) -> Result<KeyOutcome, KeyRejection> {
        if !self.manage_media_keys {
            return Err(KeyRejection::NotManagingKeys);
        }
        self.verify_origin(&key)?;
        if key.key.len() != 32 {
            log::info!(
                "media key for {} has {} bytes (MSC4143 says 32); accepting",
                key.member_id,
                key.key.len()
            );
        }
        match members.iter().find(|m| m.member_id == key.member_id) {
            Some(member) => self.accept(key, member, now),
            None => {
                self.expire_early(now);
                if self.early.len() >= EARLY_KEY_CAP {
                    self.early.remove(0);
                }
                log::info!(
                    "no membership yet for key from {} ({}), holding it",
                    key.member_id,
                    key.sender_user_id
                );
                self.early.push(EarlyKey {
                    key,
                    received_ts: now,
                });
                Ok(KeyOutcome::Buffered)
            }
        }
    }

    /// The session changed: retry every held key against the new members.
    /// Returns the keys that were accepted (for the change callback).
    pub fn on_members(&mut self, members: &[Member], now: u64) -> Vec<MediaKeyChange> {
        self.expire_early(now);
        let held = std::mem::take(&mut self.early);
        let mut changes = Vec::new();
        for early in held {
            match members.iter().find(|m| m.member_id == early.key.member_id) {
                None => self.early.push(early),
                Some(member) => {
                    let member_id = member.member_id.clone();
                    let outcome = self.accept(early.key, member, now);
                    self.record(&member_id, &outcome, now);
                    match outcome {
                        Ok(KeyOutcome::Stored(change)) => changes.push(change),
                        Ok(_) => {}
                        Err(rejection) => log::warn!(
                            "held key for {member_id} rejected once its membership arrived: {rejection}"
                        ),
                    }
                }
            }
        }
        changes
    }

    /// Our own key came into use.
    pub fn set_own_key(&mut self, key: MediaKey) -> Option<MediaKeyChange> {
        let own = self.own_member_id.clone()?;
        match self.store(&own, key) {
            KeyOutcome::Stored(change) => Some(change),
            _ => None,
        }
    }

    /// Every remote joined member with a device has at least one key.
    pub fn has_received_all_member_keys(&self, members: &[Member]) -> bool {
        members
            .iter()
            .filter(|m| Some(&m.member_id) != self.own_member_id.as_ref() && m.device_id.is_some())
            .all(|m| {
                self.key_map
                    .get(&m.member_id)
                    .is_some_and(|ring| !ring.is_empty())
            })
    }

    fn expire_early(&mut self, now: u64) {
        self.early
            .retain(|e| now.saturating_sub(e.received_ts) < EARLY_KEY_TTL_MS);
    }

    /// Membership-independent checks, in this order: room, encryption,
    /// cross-signing.
    fn verify_origin(&self, key: &ReceivedEncryptionKey) -> Result<(), KeyRejection> {
        if key.room_id != self.room_id {
            return Err(KeyRejection::WrongRoom);
        }
        match &key.origin {
            KeyOrigin::Cleartext => Err(KeyRejection::Cleartext),
            KeyOrigin::Unknown => Err(KeyRejection::UnknownOrigin),
            KeyOrigin::Encrypted {
                sender_cross_signed,
                ..
            } => {
                if self.config.require_cross_signed_sender && *sender_cross_signed != Some(true) {
                    Err(KeyRejection::NotCrossSigned)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// The to-device sender and device must match the member event.
    fn verify_against_member(
        key: &ReceivedEncryptionKey,
        member: &Member,
    ) -> Result<(), KeyRejection> {
        // Held keys arrive here without re-running `verify_origin`.
        let KeyOrigin::Encrypted {
            sender_device_id, ..
        } = &key.origin
        else {
            return Err(KeyRejection::Cleartext);
        };
        if key.sender_user_id != member.user_id {
            return Err(KeyRejection::SenderMismatch);
        }
        match member.device_attribution {
            DeviceAttribution::Unknown => Ok(()),
            DeviceAttribution::Verified | DeviceAttribution::Claimed => {
                let Some(expected) = member.device_id.as_deref() else {
                    return Err(KeyRejection::UnattributableMember);
                };
                if sender_device_id.as_deref() != Some(expected) {
                    return Err(KeyRejection::DeviceMismatch);
                }
                Ok(())
            }
        }
    }

    fn accept(
        &mut self,
        key: ReceivedEncryptionKey,
        member: &Member,
        now: u64,
    ) -> Result<KeyOutcome, KeyRejection> {
        // Rejected keys never touch the filter: an impostor cannot poison
        // the (member, index) slot for the genuine key.
        Self::verify_against_member(&key, member)?;
        if self.filter.check_and_add(&member.member_id, key.index, now) {
            return Err(KeyRejection::Outdated);
        }
        let media_key = MediaKey {
            key: key.key,
            index: key.index,
            creation_ts_ms: now,
        };
        Ok(self.store(&member.member_id, media_key))
    }

    /// One entry per `(member, index)`: identical bytes are a redelivery
    /// (ignored), different bytes a rekey (replaced).
    fn store(&mut self, member_id: &str, key: MediaKey) -> KeyOutcome {
        let ring = self.key_map.entry(member_id.to_owned()).or_default();
        if let Some(existing) = ring.iter_mut().find(|k| k.index == key.index) {
            if existing.key == key.key {
                return KeyOutcome::Duplicate;
            }
            *existing = key.clone();
        } else {
            ring.push(key.clone());
        }
        KeyOutcome::Stored(MediaKeyChange {
            member_id: member_id.to_owned(),
            key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOM: &str = "!room:example.org";

    fn member(user: &str, device: Option<&str>, attribution: DeviceAttribution) -> Member {
        Member {
            member_id: format!("m-{user}"),
            user_id: user.into(),
            device_id: device.map(Into::into),
            device_attribution: attribution,
            membership_ts: None,
            display_name: None,
            avatar_url: None,
            intent: None,
            application_type: None,
            transports: Default::default(),
        }
    }

    fn bob() -> Member {
        member("@bob:x", Some("BOB1"), DeviceAttribution::Verified)
    }

    fn key_from(user: &str, device: Option<&str>, index: u8, bytes: u8) -> ReceivedEncryptionKey {
        ReceivedEncryptionKey {
            room_id: ROOM.into(),
            member_id: format!("m-{user}"),
            sender_user_id: user.into(),
            origin: KeyOrigin::Encrypted {
                sender_device_id: device.map(Into::into),
                sender_cross_signed: Some(true),
            },
            key: vec![bytes; 32],
            index,
        }
    }

    fn inbound() -> InboundKeys {
        InboundKeys::new(
            ROOM.into(),
            EncryptionConfig::default(),
            "m-own".into(),
            true,
        )
    }

    fn stored(outcome: Result<KeyOutcome, KeyRejection>) -> MediaKeyChange {
        match outcome {
            Ok(KeyOutcome::Stored(c)) => c,
            other => panic!("expected Stored, got {other:?}"),
        }
    }

    #[test]
    fn a_verified_key_lands_in_the_key_map() {
        let mut i = inbound();
        let c = stored(i.receive(key_from("@bob:x", Some("BOB1"), 0, 1), &[bob()], 10));
        assert_eq!(c.member_id, "m-@bob:x");
        assert_eq!(
            i.key_map()["m-@bob:x"],
            vec![MediaKey {
                key: vec![1; 32],
                index: 0,
                creation_ts_ms: 10
            }]
        );
    }

    #[test]
    fn cleartext_key_is_rejected() {
        let mut i = inbound();
        let mut k = key_from("@bob:x", Some("BOB1"), 0, 1);
        k.origin = KeyOrigin::Cleartext;
        assert_eq!(
            i.receive(k.clone(), &[bob()], 1),
            Err(KeyRejection::Cleartext)
        );
        k.origin = KeyOrigin::Unknown;
        assert_eq!(i.receive(k, &[bob()], 1), Err(KeyRejection::UnknownOrigin));
    }

    #[test]
    fn key_for_another_room_is_rejected_before_anything_else() {
        let mut i = inbound();
        let mut k = key_from("@bob:x", Some("BOB1"), 0, 1);
        k.room_id = "!other:x".into();
        k.origin = KeyOrigin::Cleartext;
        assert_eq!(i.receive(k, &[bob()], 1), Err(KeyRejection::WrongRoom));
    }

    #[test]
    fn cross_signing_requirement_is_configurable() {
        let mut k = key_from("@bob:x", Some("BOB1"), 0, 1);
        k.origin = KeyOrigin::Encrypted {
            sender_device_id: Some("BOB1".into()),
            sender_cross_signed: Some(false),
        };
        let mut strict = inbound();
        assert_eq!(
            strict.receive(k.clone(), &[bob()], 1),
            Err(KeyRejection::NotCrossSigned)
        );
        // unknown counts as not cross-signed
        k.origin = KeyOrigin::Encrypted {
            sender_device_id: Some("BOB1".into()),
            sender_cross_signed: None,
        };
        assert_eq!(
            strict.receive(k.clone(), &[bob()], 1),
            Err(KeyRejection::NotCrossSigned)
        );

        let mut lenient = InboundKeys::new(
            ROOM.into(),
            EncryptionConfig {
                require_cross_signed_sender: false,
                ..Default::default()
            },
            "m-own".into(),
            true,
        );
        stored(lenient.receive(k, &[bob()], 1));
    }

    #[test]
    fn sender_and_device_must_match_the_member_event() {
        let mut i = inbound();
        let mut k = key_from("@bob:x", Some("BOB1"), 0, 1);
        k.sender_user_id = "@mallory:x".into();
        assert_eq!(i.receive(k, &[bob()], 1), Err(KeyRejection::SenderMismatch));
        assert_eq!(
            i.receive(key_from("@bob:x", Some("BOB2"), 0, 1), &[bob()], 1),
            Err(KeyRejection::DeviceMismatch)
        );
        assert_eq!(
            i.receive(key_from("@bob:x", None, 0, 1), &[bob()], 1),
            Err(KeyRejection::DeviceMismatch)
        );
    }

    #[test]
    fn a_claimed_device_narrows_but_never_widens() {
        let mut i = inbound();
        let claimed = member("@bob:x", Some("BOB1"), DeviceAttribution::Claimed);
        assert_eq!(
            i.receive(
                key_from("@bob:x", Some("BOB2"), 0, 1),
                std::slice::from_ref(&claimed),
                1
            ),
            Err(KeyRejection::DeviceMismatch)
        );
        stored(i.receive(key_from("@bob:x", Some("BOB1"), 0, 1), &[claimed], 1));
    }

    #[test]
    fn member_with_unknown_origin_skips_the_device_check_but_none_device_rejects() {
        let mut i = inbound();
        let unknown = member("@bob:x", None, DeviceAttribution::Unknown);
        stored(i.receive(key_from("@bob:x", Some("ANY"), 0, 1), &[unknown], 1));
        let unattributable = member("@bob:x", None, DeviceAttribution::Verified);
        assert_eq!(
            i.receive(key_from("@bob:x", Some("ANY"), 1, 1), &[unattributable], 1),
            Err(KeyRejection::UnattributableMember)
        );
    }

    #[test]
    fn key_arriving_before_the_membership_is_buffered_and_verified_when_it_lands() {
        let mut i = inbound();
        assert_eq!(
            i.receive(key_from("@bob:x", Some("BOB1"), 0, 1), &[], 1),
            Ok(KeyOutcome::Buffered)
        );
        // an impostor's early key for the same member from another device
        let mut impostor = key_from("@bob:x", Some("MAL"), 0, 9);
        impostor.sender_user_id = "@mallory:x".into();
        assert_eq!(i.receive(impostor, &[], 2), Ok(KeyOutcome::Buffered));
        assert_eq!(i.early_key_count(), 2);
        assert!(i.key_map().is_empty());

        let changes = i.on_members(&[bob()], 3);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key.key, vec![1; 32]);
        assert_eq!(i.early_key_count(), 0);
    }

    #[test]
    fn early_key_buffer_is_bounded_by_ttl_and_capacity() {
        let mut i = inbound();
        for n in 0..EARLY_KEY_CAP + 5 {
            let mut k = key_from("@bob:x", Some("BOB1"), 0, 1);
            k.member_id = format!("m-{n}");
            i.receive(k, &[], 100).unwrap();
        }
        assert_eq!(i.early_key_count(), EARLY_KEY_CAP);
        i.on_members(&[], 100 + EARLY_KEY_TTL_MS);
        assert_eq!(i.early_key_count(), 0);
    }

    #[test]
    fn a_rejected_key_does_not_occupy_the_member_index_slot() {
        let mut i = inbound();
        let mut impostor = key_from("@bob:x", Some("MAL"), 0, 9);
        impostor.sender_user_id = "@mallory:x".into();
        assert!(i.receive(impostor, &[bob()], 5).is_err());
        // genuine key arrives *earlier* by timestamp than the impostor: must pass
        stored(i.receive(key_from("@bob:x", Some("BOB1"), 0, 1), &[bob()], 4));
    }

    /// The *why* behind "I cannot hear Bob" has to survive the moment it is
    /// computed, or a UI attaching later finds only a silent tile.
    #[test]
    fn a_rejection_is_latched_per_member_and_cleared_by_an_accepted_key() {
        let mut i = inbound();
        assert_eq!(i.rejection("m-@bob:x"), None);
        assert!(!i.have_key_from("m-@bob:x"));

        let mut cleartext = key_from("@bob:x", Some("BOB1"), 0, 1);
        cleartext.origin = KeyOrigin::Cleartext;
        assert!(i.receive(cleartext, &[bob()], 5).is_err());
        assert_eq!(i.rejection("m-@bob:x"), Some(&KeyRejection::Cleartext));

        // The latch is the *most recent* verdict, not the first.
        let mut impostor = key_from("@bob:x", Some("MAL"), 0, 9);
        impostor.sender_user_id = "@mallory:x".into();
        assert!(i.receive(impostor, &[bob()], 6).is_err());
        assert_eq!(i.rejection("m-@bob:x"), Some(&KeyRejection::SenderMismatch));

        // ...and it clears the moment a genuine key lands.
        stored(i.receive(key_from("@bob:x", Some("BOB1"), 0, 1), &[bob()], 7));
        assert_eq!(i.rejection("m-@bob:x"), None);
        assert!(i.have_key_from("m-@bob:x"));
    }

    /// A buffered key is not a verdict: nothing is latched while we wait for
    /// the membership, and the verdict on arrival is what latches.
    #[test]
    fn a_buffered_key_latches_nothing_until_its_membership_arrives() {
        let mut i = inbound();
        let mut impostor = key_from("@bob:x", Some("MAL"), 0, 9);
        impostor.sender_user_id = "@mallory:x".into();
        assert_eq!(i.receive(impostor, &[], 5), Ok(KeyOutcome::Buffered));
        assert_eq!(i.rejection("m-@bob:x"), None, "no verdict yet");
        i.on_members(&[bob()], 6);
        assert_eq!(i.rejection("m-@bob:x"), Some(&KeyRejection::SenderMismatch));
    }

    #[test]
    fn redelivery_rekey_and_outdated_semantics() {
        let mut i = inbound();
        stored(i.receive(key_from("@bob:x", Some("BOB1"), 0, 1), &[bob()], 10));
        assert_eq!(
            i.receive(key_from("@bob:x", Some("BOB1"), 0, 1), &[bob()], 10),
            Ok(KeyOutcome::Duplicate)
        );
        // same index, different bytes, same instant: a rekey seen twice -> replaces
        let c = stored(i.receive(key_from("@bob:x", Some("BOB1"), 0, 2), &[bob()], 10));
        assert_eq!(c.key.key, vec![2; 32]);
        assert_eq!(i.key_map()["m-@bob:x"].len(), 1);
        // an *older* arrival for a filled slot is outdated
        assert_eq!(
            i.receive(key_from("@bob:x", Some("BOB1"), 0, 3), &[bob()], 9),
            Err(KeyRejection::Outdated)
        );
        // another index is another slot
        stored(i.receive(key_from("@bob:x", Some("BOB1"), 1, 3), &[bob()], 9));
        assert_eq!(i.key_map()["m-@bob:x"].len(), 2);
    }

    #[test]
    fn outdated_filter_forgets_entries_after_its_ttl() {
        let mut f = OutdatedKeyFilter::with_ttl(100);
        assert!(!f.check_and_add("m", 0, 1000));
        assert!(f.check_and_add("m", 0, 999));
        assert!(!f.check_and_add("m", 0, 1000)); // equal passes
        assert!(!f.check_and_add("m", 1, 1101)); // cleanup ran: entry at 1000 is gone
        assert!(!f.is_outdated("m", 0, 999));
    }

    #[test]
    fn sixteen_byte_legacy_keys_are_accepted() {
        let mut i = inbound();
        let mut k = key_from("@bob:x", Some("BOB1"), 0, 1);
        k.key = vec![1; 16];
        stored(i.receive(k, &[bob()], 1));
    }

    #[test]
    fn keys_while_not_managing_media_keys_are_dropped() {
        let mut i = InboundKeys::new(
            ROOM.into(),
            EncryptionConfig::default(),
            "m-own".into(),
            false,
        );
        assert_eq!(
            i.receive(key_from("@bob:x", Some("BOB1"), 0, 1), &[bob()], 1),
            Err(KeyRejection::NotManagingKeys)
        );
    }

    #[test]
    fn own_key_and_completeness() {
        let mut i = inbound();
        let own = Member {
            member_id: "m-own".into(),
            ..member("@own:x", Some("OWN"), DeviceAttribution::Verified)
        };
        let carol_no_device = member("@carol:x", None, DeviceAttribution::Unknown);
        assert!(!i.has_received_all_member_keys(&[own.clone(), bob()]));
        stored(i.receive(key_from("@bob:x", Some("BOB1"), 0, 1), &[bob()], 1));
        // carol has no device: nobody could ever send us a key from her device
        assert!(i.has_received_all_member_keys(&[own, bob(), carol_no_device]));
        let c = i
            .set_own_key(MediaKey {
                key: vec![5; 32],
                index: 0,
                creation_ts_ms: 1,
            })
            .unwrap();
        assert_eq!(c.member_id, "m-own");
        assert!(
            i.set_own_key(MediaKey {
                key: vec![5; 32],
                index: 0,
                creation_ts_ms: 1
            })
            .is_none()
        );
    }
}
