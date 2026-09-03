//! Per-member media key exchange over to-device messages (MSC4143).
//!
//! See `README.md` in this directory for the plan, the rotation approach
//! (matrix-js-sdk PR #5505), the challenges and the test list.
//!
//! - [`send_machine`]: our key — rotation and distribution policy (pure).
//! - [`inbound`]: everybody else's keys — verification and the [`KeyMap`].
//! - [`matrix_encryption_event`] / [`legacy_element_call`]: the to-device content.
//! - `pump`: the one task that owns time and I/O.
//! - [`Machine`]: the owner of all of the above, what `participation` holds.

pub mod inbound;
pub(crate) mod legacy_element_call;
pub mod matrix_encryption_event;
mod pump;
#[cfg(test)]
mod rotation_simulation;
pub mod send_machine;

use crate::driver::{ToDeviceDriver, ToDeviceMessage, ToDeviceRecipient};
use crate::executor;
use crate::session::{ElementCallCompat, SessionSnapshot};
use crate::types::Member;
use inbound::InboundKeys;
use send_machine::{Action, SendMachine};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, watch};

/// Key material + index, ready for `room.set_key_for_participant`.
#[derive(Clone, Debug, PartialEq)]
pub struct MediaKey {
    pub key: Vec<u8>,
    pub index: u8,
    pub creation_ts_ms: u64,
}

/// member id -> the keys we hold for that member, one per index, in arrival
/// order. More than one because frames carry the index and a peer's previous
/// key stays needed for in-flight frames after they rotate.
pub type KeyMap = HashMap<String, Vec<MediaKey>>;

/// One key changed in the map — what the host feeds into its key provider.
#[derive(Clone, Debug, PartialEq)]
pub struct MediaKeyChange {
    pub member_id: String,
    pub key: MediaKey,
}

/// A member as a key recipient: a member with a device to send to. Compared
/// by [`Self::same_join`] — member id *and* membership timestamp — so that a
/// leave-and-rejoin under a reused id (MSC3401 compat) is a new participation.
#[derive(Clone, Debug, PartialEq)]
pub struct Participation {
    member: Member,
    device_id: String,
}

impl Participation {
    /// `None` for members without a device: nobody can send them a key.
    pub fn from_member(member: &Member) -> Option<Self> {
        let device_id = member.device_id.clone()?;
        Some(Self {
            member: member.clone(),
            device_id,
        })
    }

    pub fn member(&self) -> &Member {
        &self.member
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    pub fn same_join(&self, other: &Participation) -> bool {
        self.member.member_id == other.member.member_id
            && self.member.membership_ts == other.member.membership_ts
    }

    pub fn recipient(&self) -> ToDeviceRecipient {
        ToDeviceRecipient {
            user_id: self.member.user_id.clone(),
            device_id: self.device_id.clone(),
        }
    }
}

/// How an inbound key message arrived, from Olm decryption metadata.
#[derive(Clone, Debug, PartialEq)]
pub enum KeyOrigin {
    Encrypted {
        sender_device_id: Option<String>,
        /// MSC4153; `None` = the host did not say (treated as not signed).
        sender_cross_signed: Option<bool>,
    },
    Cleartext,
    Unknown,
}

/// A decoded inbound key message, pre-verification.
#[derive(Clone, Debug, PartialEq)]
pub struct ReceivedEncryptionKey {
    pub room_id: String,
    /// The member event this key names.
    pub member_id: String,
    pub sender_user_id: String,
    pub origin: KeyOrigin,
    pub key: Vec<u8>,
    pub index: u8,
}

/// Result of a key that passed verification.
#[derive(Clone, Debug, PartialEq)]
pub enum KeyOutcome {
    /// Stored; signal this change.
    Stored(MediaKeyChange),
    /// Identical redelivery of a key we hold.
    Duplicate,
    /// Its member event has not arrived yet: held *with its origin* and
    /// verified when the membership shows up — deferred, never skipped.
    Buffered,
}

/// Why an inbound key was discarded. Rejected keys never occupy a
/// `(member, index)` slot, so a bogus key cannot suppress the genuine one.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum KeyRejection {
    #[error("key arrived in cleartext")]
    Cleartext,
    #[error("host reported no origin for the key message")]
    UnknownOrigin,
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
    #[error("media keys are not managed in this call")]
    NotManagingKeys,
}

#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    /// Reject keys from non-cross-signed devices (MSC4153). Default on.
    pub require_cross_signed_sender: bool,
    /// Local default; overridden by the slot's negotiated encryption.
    pub manage_media_keys: bool,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            require_cross_signed_sender: true,
            manage_media_keys: true,
        }
    }
}

/// The rotation policy's knobs (matrix-js-sdk PR #5505; no hard participant
/// limit — the contingent is the only brake — plus an optional lifetime cap).
#[derive(Clone, Debug)]
pub struct SendMachineConfig {
    /// To-device messages the *whole call* may spend per minute. Every
    /// client derives its own rotation spacing from it:
    /// `grace_ms(N) = 60_000 · N · (N − 1) / contingent`.
    pub shared_per_minute_to_device_contingent: u32,
    /// Wait this long after sending a rotated key before encrypting with it.
    pub use_key_delay_ms: u64,
    /// Rotate at the latest this long after minting, even in a quiet call
    /// (our addition, from the shipped core; the PR has none). Off by default.
    pub max_key_lifetime_ms: Option<u64>,
}

impl Default for SendMachineConfig {
    fn default() -> Self {
        Self {
            shared_per_minute_to_device_contingent: 3000,
            use_key_delay_ms: 1000,
            max_key_lifetime_ms: None,
        }
    }
}

/// The machine's one status. `Joining` until our first key batch was served
/// *and* every remote member with a device has sent us a key; `Connected`
/// from then on (it does not fall back when a new member joins — their key
/// shows up as `fully_settled == false` instead). A machine that manages no
/// keys is `Connected` from the start.
#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    Joining {
        has_distributed_initial_keys: bool,
        has_received_all_member_keys: bool,
    },
    Connected {
        /// Members who left but still hold the key our media is encrypted
        /// with — display as "possibly still listening".
        left_members_with_keys: Vec<Member>,
        /// No rotation owed, no key switch pending, and every current member
        /// has sent us a key.
        fully_settled: bool,
        /// Creation of the current outbound key: what a joiner handed the
        /// current key can decrypt back to. A UI threshold.
        last_rotation_ts: u64,
    },
}

/// Whether one member and we can hear each other, per tile.
///
/// Two independent booleans on purpose: they fail for different reasons (our
/// to-device send to them vs. theirs to us), they clear independently, and a
/// UI renders them in different places — "they cannot hear you" is a warning
/// about your own media, "you cannot hear them" is a warning about theirs.
#[derive(Clone, Debug, PartialEq)]
pub struct MediaKeyState {
    /// They hold our current key: they can decrypt us.
    pub holds_our_key: bool,
    /// We hold theirs: we can decrypt them.
    pub have_their_key: bool,
    /// Why their most recent key was discarded, while we still lack one.
    /// `None` once a key from them is accepted.
    pub rejection: Option<KeyRejection>,
}

pub type KeyMapCallback = Box<dyn Fn(&KeyMap, &MediaKeyChange) + Send + Sync>;
pub type KeyRejectedCallback = Box<dyn Fn(&str, &KeyRejection) + Send + Sync>;

pub(crate) fn fill_random(bytes: &mut [u8]) {
    getrandom::getrandom(bytes).expect("matrix-rtc: OS randomness unavailable");
}

/// Uniform in `[0, 2)` — the PR's `Math.random() * 2`.
fn jitter() -> f64 {
    let mut b = [0u8; 8];
    fill_random(&mut b);
    (u64::from_le_bytes(b) >> 11) as f64 / (1u64 << 53) as f64 * 2.0
}

pub(crate) struct MachineState {
    room_id: String,
    slot_id: String,
    compat: ElementCallCompat,
    members: Vec<Member>,
    inbound: InboundKeys,
    send: SendMachine,
    /// `Status::Connected` once reached, for good.
    connected: bool,
}

pub(crate) struct MachineInner {
    state: Mutex<MachineState>,
    on_key_map_change: KeyMapCallback,
    on_key_rejected: Mutex<Option<KeyRejectedCallback>>,
}

/// The full encryption machine: owns the [`SendMachine`], verifies and
/// stores inbound keys, emits the [`KeyMap`] changes, and runs the pump.
///
/// Lives for exactly **one participation**: construct it with our fresh
/// membership *before* the own-membership machine sends the join event (so
/// our key reaches peers before — or with — our member event), and drop it
/// to leave (every key is forgotten, the pump stops). There is no
/// `join`/`leave`; a rejoin is a new `Machine`.
pub struct Machine {
    inner: Arc<MachineInner>,
    notify: Arc<Notify>,
}

impl Machine {
    /// Starts working immediately against the current session snapshot.
    /// `manage_media_keys` is the negotiated decision (slot encryption, with
    /// `EncryptionConfig::manage_media_keys` as the fallback); the facade may
    /// equally construct no machine at all for an unencrypted call.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        driver: Arc<dyn ToDeviceDriver>,
        room_id: String,
        slot_id: String,
        compat: ElementCallCompat,
        session: watch::Receiver<SessionSnapshot>,
        own: &Member,
        manage_media_keys: bool,
        config: EncryptionConfig,
        send_config: SendMachineConfig,
        on_key_map_change: KeyMapCallback,
    ) -> Result<Self, MachineError> {
        let own = Participation::from_member(own).ok_or(MachineError::OwnDeviceUnknown)?;
        let to_device = driver.subscribe_to_device_events();
        let inner = Arc::new(MachineInner {
            state: Mutex::new(MachineState {
                room_id: room_id.clone(),
                slot_id,
                compat,
                members: Vec::new(),
                inbound: InboundKeys::new(
                    room_id,
                    config,
                    own.member().member_id.clone(),
                    manage_media_keys,
                ),
                send: SendMachine::new(send_config, own, manage_media_keys),
                connected: !manage_media_keys,
            }),
            on_key_map_change,
            on_key_rejected: Mutex::new(None),
        });
        let notify = Arc::new(Notify::new());
        executor::spawn(
            pump::Pump {
                inner: Arc::downgrade(&inner),
                notify: notify.clone(),
                driver,
                session,
                to_device,
            }
            .run(),
        );
        Ok(Self { inner, notify })
    }

    pub fn key_map(&self) -> KeyMap {
        self.inner.state.lock().unwrap().inbound.key_map().clone()
    }

    /// Everyone holding the key our media is currently encrypted with. The
    /// facade diffs this against the *fresh* session roster to list members
    /// that left but may still be listening.
    pub fn key_holders(&self) -> Vec<Member> {
        self.inner.state.lock().unwrap().send.key_holders()
    }

    /// Re-emit every key we hold (for hosts attaching a media session late).
    pub fn replay_key_map(&self) {
        let map = self.key_map();
        for (member_id, ring) in &map {
            for key in ring {
                (self.inner.on_key_map_change)(
                    &map,
                    &MediaKeyChange {
                        member_id: member_id.clone(),
                        key: key.clone(),
                    },
                );
            }
        }
    }

    /// Our own member id for this participation — what
    /// `ParticipationManager::own_member_id` hands back.
    pub fn own_member_id(&self) -> Option<String> {
        self.inner
            .state
            .lock()
            .unwrap()
            .send
            .own()
            .map(|o| o.member().member_id.clone())
    }

    /// Whether this participation manages media keys at all. `false` for an
    /// unencrypted call, where per-member key state is meaningless.
    pub fn manages_media_keys(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap()
            .inbound
            .manages_media_keys()
    }

    /// Who can hear whom, for one member. The facade puts this on the tile
    /// and derives the aggregate impairments from it, so there is one source
    /// of truth rather than an aggregate that can disagree with the list.
    pub fn key_state(&self, member_id: &str) -> MediaKeyState {
        let state = self.inner.state.lock().unwrap();
        let have_their_key = state.inbound.have_key_from(member_id);
        MediaKeyState {
            holds_our_key: state.send.holds_our_key(member_id),
            have_their_key,
            // A rejection only matters while it leaves us without a key from
            // them; once one is accepted the tile is fine.
            rejection: (!have_their_key)
                .then(|| state.inbound.rejection(member_id).cloned())
                .flatten(),
        }
    }

    /// When [`MediaKeyState::rejection`] was recorded, for the impairment
    /// that reports it.
    pub fn key_rejected_at(&self, member_id: &str) -> Option<u64> {
        self.inner
            .state
            .lock()
            .unwrap()
            .inbound
            .rejected_at(member_id)
    }

    pub fn set_key_rejected_callback(&self, callback: KeyRejectedCallback) {
        *self.inner.on_key_rejected.lock().unwrap() = Some(callback);
    }

    pub fn status(&self) -> Status {
        let mut state = self.inner.state.lock().unwrap();
        let has_distributed_initial_keys = state.send.has_distributed_initial_keys();
        let has_received_all_member_keys =
            state.inbound.has_received_all_member_keys(&state.members);
        if has_distributed_initial_keys && has_received_all_member_keys {
            state.connected = true;
        }
        if state.connected {
            Status::Connected {
                left_members_with_keys: state.send.left_members_with_keys(),
                fully_settled: state.send.is_settled() && has_received_all_member_keys,
                last_rotation_ts: state.send.last_rotation_ts(),
            }
        } else {
            Status::Joining {
                has_distributed_initial_keys,
                has_received_all_member_keys,
            }
        }
    }

    /// Diagnostics (no key material).
    pub fn debug_snapshot(&self) -> serde_json::Value {
        let status = format!("{:?}", self.status());
        let state = self.inner.state.lock().unwrap();
        serde_json::json!({
            "status": status,
            "current_key_index": state.send.current_key().map(|k| k.index),
            "next_wake_ts": state.send.next_wake_ts(),
            "left_members_with_keys": state.send.left_members_with_keys().iter().map(|m| &m.member_id).collect::<Vec<_>>(),
            "held_early_keys": state.inbound.early_key_count(),
            "keys_per_member": state.inbound.key_map().iter().map(|(m, ring)| (m.clone(), ring.iter().map(|k| k.index).collect::<Vec<_>>())).collect::<HashMap<_, _>>(),
        })
    }

    // ---- steps, called by the pump; lock held briefly, callbacks after ----

    fn emit(inner: &MachineInner, changes: Vec<MediaKeyChange>) {
        if changes.is_empty() {
            return;
        }
        let map = inner.state.lock().unwrap().inbound.key_map().clone();
        for change in &changes {
            (inner.on_key_map_change)(&map, change);
        }
    }

    pub(crate) fn on_session(
        inner: &MachineInner,
        snapshot: SessionSnapshot,
        now: u64,
    ) -> Vec<Action> {
        let (changes, actions) = {
            let mut state = inner.state.lock().unwrap();
            state.members = snapshot.members;
            let members = state.members.clone();
            let changes = state.inbound.on_members(&members, now);
            let actions = state.send.on_session(&members, now, jitter());
            (changes, actions)
        };
        Self::emit(inner, changes);
        actions
    }

    pub(crate) fn on_wake(inner: &MachineInner, now: u64) -> Vec<Action> {
        inner.state.lock().unwrap().send.on_wake(now)
    }

    pub(crate) fn on_delivered(
        inner: &MachineInner,
        key_index: u8,
        served: &[Participation],
        now: u64,
    ) {
        inner
            .state
            .lock()
            .unwrap()
            .send
            .on_delivered(key_index, served, now);
    }

    pub(crate) fn use_own_key(inner: &MachineInner, key: MediaKey) {
        let change = inner.state.lock().unwrap().inbound.set_own_key(key);
        Self::emit(inner, change.into_iter().collect());
    }

    pub(crate) fn on_to_device(inner: &MachineInner, msg: ToDeviceMessage, now: u64) {
        if !matrix_encryption_event::is_key_event_type(&msg.event_type) {
            return;
        }
        let received = match matrix_encryption_event::to_received(&msg) {
            Ok(r) => r,
            Err(e) => {
                log::warn!("unparseable media key message from {}: {e}", msg.sender);
                return;
            }
        };
        let member_id = received.member_id.clone();
        let outcome = {
            let mut state = inner.state.lock().unwrap();
            let members = state.members.clone();
            state.inbound.receive(received, &members, now)
        };
        match outcome {
            Ok(KeyOutcome::Stored(change)) => Self::emit(inner, vec![change]),
            Ok(KeyOutcome::Duplicate) => {}
            Ok(KeyOutcome::Buffered) => {}
            Err(rejection) => {
                log::warn!(
                    "media key for {member_id} from {} rejected: {rejection}",
                    msg.sender
                );
                if let Some(cb) = inner.on_key_rejected.lock().unwrap().as_ref() {
                    cb(&member_id, &rejection);
                }
            }
        }
    }
}

impl Drop for Machine {
    fn drop(&mut self) {
        // Leaving = dropping. Wake the pump so it notices the machine is
        // gone and exits (releasing the driver streams).
        self.notify.notify_one();
    }
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum MachineError {
    #[error("our own membership has no device id")]
    OwnDeviceUnknown,
}
