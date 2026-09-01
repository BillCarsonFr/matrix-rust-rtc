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

//! Bridge from `matrix-rtc-core` key signals towards a transport's frame
//! encryption.
//!
//! `matrix-rtc-core` produces per-participant media keys and hands them to the
//! application via [`EncryptionKeySignalHandler::on_new_key_material`]. The
//! eventual destination is the transport's frame-encryption key ring — LiveKit
//! native's `KeyProvider`, livekit-js's `ExternalE2EEKeyProvider` on the web —
//! keyed by the participant's transport identity and the key index. The ring is
//! behind [`FrameKeyRing`], which is all a transport implements; everything
//! else here is transport-neutral bookkeeping: recording, the ring-size guard,
//! the rejected-key rule, the local sender's index switch, and listener
//! fan-out.
//!
//! A handler created via [`MediaKeyHandler::with_ring`] imports signalled keys
//! into the ring; one created via [`MediaKeyHandler::new`] only records them.
//!
//! This handler also owns the MSC4143 `delayBeforeUse` wait. `matrix-rtc-core`
//! deliberately holds no timer — it only states the delay, as
//! [`KeyMaterialSignal::use_after_ms`] — so that a synchronous FFI host can
//! drive it from a plain thread. Enforcing it is therefore a transport-layer
//! obligation, and this is the one implementation of it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use matrix_rtc_core::{DiscardedKey, EncryptionKeySignalHandler, KeyMaterialSignal, MaybeSend};

use crate::rt;

/// A transport's frame-encryption key ring: where imported media keys land.
///
/// The one seam a transport implements to receive the core's keys; LiveKit
/// native backs it with `KeyProvider::set_key`, the web with livekit-js's
/// `ExternalE2EEKeyProvider.setKey`.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait FrameKeyRing: MaybeSend {
    /// Number of key slots the ring holds. Keys whose index falls at or past it
    /// are rejected by [`MediaKeyHandler`] *before* reaching [`set_key`]: rings
    /// differ in how they fail on an out-of-range index (the native frame
    /// cryptor aborts the process), and peers control the value.
    ///
    /// [`set_key`]: FrameKeyRing::set_key
    fn ring_size(&self) -> u16;

    /// Install `key` for `identity` at `index`. `false` when the ring refused
    /// it — the handler then treats the key as rejected, exactly as if it had
    /// been out of range.
    async fn set_key(&self, identity: &str, index: u8, key: Vec<u8>) -> bool;
}

/// A piece of media key material destined for the transport's frame cryptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantKey {
    /// Transport-level participant identity the key belongs to (for LiveKit,
    /// the MSC4195 pseudonymous identity).
    pub rtc_backend_identity: String,
    /// Key index, used as the ring's key index.
    pub key_index: u8,
    /// Raw key bytes (key-derivation input material), as produced by
    /// `matrix-rtc-core`.
    pub key: Vec<u8>,
}

impl From<KeyMaterialSignal> for ParticipantKey {
    fn from(signal: KeyMaterialSignal) -> Self {
        Self {
            rtc_backend_identity: signal.rtc_backend_identity,
            key_index: signal.key_index,
            key: signal.key,
        }
    }
}

// The listener aliases are `Send + Sync` off wasm32 (they run on the handler's
// spawned tasks there) and unconstrained on it, mirroring `MaybeSend` — a
// bound-carrying `Fn` alias cannot take a supertrait, so the pair is spelled
// per target instead.

/// Callback invoked after every signalled key has been recorded (and, with a
/// ring, imported). Must not block: it runs on the signalling path.
#[cfg(not(target_arch = "wasm32"))]
pub type KeyImportListener = Box<dyn Fn(&ParticipantKey) + Send + Sync>;
/// Callback invoked after every signalled key has been recorded (and, with a
/// ring, imported). Must not block: it runs on the signalling path.
#[cfg(target_arch = "wasm32")]
pub type KeyImportListener = Box<dyn Fn(&ParticipantKey)>;

/// Notified when the core refuses a key, so the reason can reach the host.
#[cfg(not(target_arch = "wasm32"))]
pub type KeyDiscardListener = Box<dyn Fn(DiscardedKey) + Send + Sync>;
/// Notified when the core refuses a key, so the reason can reach the host.
#[cfg(target_arch = "wasm32")]
pub type KeyDiscardListener = Box<dyn Fn(DiscardedKey)>;

/// Notified when a key's MSC4143 `delayBeforeUse` has elapsed and it has been
/// installed — the moment our own rotation actually takes over.
///
/// Exists so the core can be told the window is over. Membership changes that
/// arrive during a rotation's window are coalesced into one rotation at the end
/// of it, and the core owns no timer to reach that instant with; this handler
/// already schedules exactly it. Wire this to
/// `RtcSessionManager::flush_due_key_rotation`.
///
/// Runs on the scheduled task, so it must not block. Sending on a channel is the
/// intended shape — the core is often `!Send` and cannot be touched from here.
#[cfg(not(target_arch = "wasm32"))]
pub type SwitchCompleteListener = Box<dyn Fn() + Send + Sync>;
/// Notified when a key's MSC4143 `delayBeforeUse` has elapsed and it has been
/// installed — the moment our own rotation actually takes over. See the
/// non-wasm alias for the full contract.
#[cfg(target_arch = "wasm32")]
pub type SwitchCompleteListener = Box<dyn Fn()>;

/// Switches our own outgoing frames to a new key index.
///
/// Installing a key only fills the ring; the sender keeps stamping whatever
/// index its frame cryptor was created with (0). Rotating without this
/// advertises a new key to peers while continuing to encrypt with the old one —
/// so a peer joining after the rotation cannot decrypt us, and the forward
/// secrecy the rotation exists for is not delivered.
///
/// Installed once the room is connected, because only the connection can reach
/// the frame cryptors, and the handler is built before it.
#[cfg(not(target_arch = "wasm32"))]
pub type LocalKeyIndexHook = Box<dyn Fn(u8) + Send + Sync>;
/// Switches our own outgoing frames to a new key index. See the non-wasm alias
/// for the full contract.
#[cfg(target_arch = "wasm32")]
pub type LocalKeyIndexHook = Box<dyn Fn(u8)>;

/// Records media keys signalled by `matrix-rtc-core` and, when built with
/// [`MediaKeyHandler::with_ring`], imports them into the transport's
/// [`FrameKeyRing`].
///
/// Implements [`EncryptionKeySignalHandler`] so it can be registered directly
/// with the core encryption manager.
#[derive(Default)]
pub struct MediaKeyHandler {
    /// `Arc` so a delayed application (see [`KeyMaterialSignal::use_after_ms`])
    /// can own a handle that outlives the signalling call — and, if it comes to
    /// it, the handler.
    keys: Arc<Mutex<HashMap<String, ParticipantKey>>>,
    ring: Option<Arc<dyn FrameKeyRing>>,
    listener: Arc<Mutex<Option<KeyImportListener>>>,
    discard_listener: Arc<Mutex<Option<KeyDiscardListener>>>,
    /// Told when a delayed key has come into use (see [`SwitchCompleteListener`]).
    switch_listener: Arc<Mutex<Option<SwitchCompleteListener>>>,
    /// Our own transport identity and how to re-index our sender; `None` until
    /// the room is connected. See [`LocalKeyIndexHook`].
    local_sender: Arc<Mutex<Option<(String, LocalKeyIndexHook)>>>,
}

impl MediaKeyHandler {
    /// Create an empty, record-only handler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a handler that additionally forwards every signalled key into
    /// `ring`, keyed by the participant's transport identity and the signalled
    /// key index.
    pub fn with_ring(ring: Arc<dyn FrameKeyRing>) -> Self {
        Self {
            ring: Some(ring),
            ..Self::default()
        }
    }

    /// Register a callback observing every signalled key (e.g. to surface
    /// `KeyImported` on a call event stream). Replaces any previous listener.
    pub fn set_key_import_listener(&self, listener: KeyImportListener) {
        *self
            .listener
            .lock()
            .expect("key handler listener mutex poisoned") = Some(listener);
    }

    /// Register a callback observing every *refused* key. Replaces any previous
    /// listener.
    pub fn set_key_discard_listener(&self, listener: KeyDiscardListener) {
        *self
            .discard_listener
            .lock()
            .expect("key handler discard listener mutex poisoned") = Some(listener);
    }

    /// Register a callback for the end of a key's `delayBeforeUse` window (see
    /// [`SwitchCompleteListener`]). Replaces any previous listener.
    ///
    /// Without it the handler still honours the delay; what is lost is the core's
    /// chance to perform a rotation it coalesced into the window at the instant the
    /// window ends, leaving that to the session heartbeat.
    pub fn set_switch_complete_listener(&self, listener: SwitchCompleteListener) {
        *self
            .switch_listener
            .lock()
            .expect("key handler switch listener mutex poisoned") = Some(listener);
    }

    /// Tell the handler which identity is ours and how to switch our sender to a
    /// new key index, so an activated rotation actually changes what we encrypt
    /// with. Without it the handler records and imports keys as before, and our
    /// own rotations never take effect.
    pub fn set_local_sender(&self, own_identity: impl Into<String>, hook: LocalKeyIndexHook) {
        *self
            .local_sender
            .lock()
            .expect("key handler local sender mutex poisoned") = Some((own_identity.into(), hook));
    }

    /// The latest recorded key for a participant identity, if any.
    pub fn key_for(&self, rtc_backend_identity: &str) -> Option<ParticipantKey> {
        self.keys
            .lock()
            .expect("key handler mutex poisoned")
            .get(rtc_backend_identity)
            .cloned()
    }

    /// A snapshot of every recorded key.
    pub fn keys(&self) -> Vec<ParticipantKey> {
        self.keys
            .lock()
            .expect("key handler mutex poisoned")
            .values()
            .cloned()
            .collect()
    }
}

impl MediaKeyHandler {
    /// Imports `key` into the ring, records it, and notifies the listener.
    ///
    /// Takes its state as parameters rather than `&self` so a delayed
    /// application can call it from a spawned task holding only cloned handles.
    async fn apply(
        ring: Option<&Arc<dyn FrameKeyRing>>,
        keys: &Mutex<HashMap<String, ParticipantKey>>,
        listener: &Mutex<Option<KeyImportListener>>,
        local_sender: &Mutex<Option<(String, LocalKeyIndexHook)>>,
        key: ParticipantKey,
    ) {
        // Whether the key ring refused this index. Our sender must not be moved
        // onto an index it holds no key for — that would make our media
        // undecryptable for everyone, rather than for the one peer whose key
        // failed.
        let mut rejected = false;
        if let Some(ring) = ring {
            // An index at or past the ring size must not reach the ring at all:
            // how a ring fails on it is its own business (the native frame
            // cryptor aborts the process rather than returning false), and
            // peers control this value. Drop (but still record) such a key:
            // media from that peer stays undecryptable, but the process
            // survives.
            if u16::from(key.key_index) < ring.ring_size() {
                if !ring
                    .set_key(&key.rtc_backend_identity, key.key_index, key.key.clone())
                    .await
                {
                    rejected = true;
                    // TODO: surface set_key failures to the host.
                    log::warn!(
                        "frame key ring rejected key index {} for participant {}; \
                         its media will not decrypt",
                        key.key_index,
                        key.rtc_backend_identity,
                    );
                }
            } else {
                rejected = true;
                log::warn!(
                    "dropping key index {} for participant {}: exceeds the ring size ({}); \
                     its media will not decrypt",
                    key.key_index,
                    key.rtc_backend_identity,
                    ring.ring_size(),
                );
            }
        }
        // Our own key: also move the sender onto the new index. `set_key` alone
        // only fills the ring, so without this we would advertise the rotation
        // and keep stamping the previous index.
        if !rejected
            && let Some((own_identity, hook)) = local_sender
                .lock()
                .expect("key handler local sender mutex poisoned")
                .as_ref()
            && own_identity == &key.rtc_backend_identity
        {
            hook(key.key_index);
        }

        keys.lock()
            .expect("key handler mutex poisoned")
            .insert(key.rtc_backend_identity.clone(), key.clone());
        if let Some(listener) = listener
            .lock()
            .expect("key handler listener mutex poisoned")
            .as_ref()
        {
            listener(&key);
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl EncryptionKeySignalHandler for MediaKeyHandler {
    /// Applies a signalled key, honouring the MSC4143 `delayBeforeUse` the core
    /// attaches as [`KeyMaterialSignal::use_after_ms`].
    ///
    /// A non-zero delay is scheduled, never slept through: this runs on the
    /// caller's task, and for the FFI that caller is a *synchronous* host call,
    /// so blocking here would stall a host thread for the whole delay. The core
    /// used to own this wait and did exactly that.
    ///
    /// Everything — ring import, recording, listener — happens at activation
    /// time rather than signalling time, so what the handler exposes matches
    /// what the transport is actually encrypting with. During the delay
    /// `key_for` keeps reporting the previous key, which is the one still in
    /// use.
    ///
    /// Ordering holds without extra machinery: the delay is a constant from
    /// `EncryptionConfig::delay_before_use_ms`, so keys signalled in index order
    /// get deadlines in the same order.
    async fn on_new_key_material(&self, signal: KeyMaterialSignal) {
        let use_after_ms = signal.use_after_ms;
        let key = ParticipantKey::from(signal);

        // Scheduling needs somewhere to run (a tokio runtime; on wasm the JS
        // event loop, which is always there). Falling back to applying
        // immediately deviates from the spec — peers may not have the key yet,
        // so some frames go undecryptable — but that is recoverable, whereas
        // never applying it at all breaks the session outright. Loud, because
        // it means a consumer is driving the core without a runtime.
        if use_after_ms > 0 && !rt::can_spawn() {
            log::warn!(
                "no runtime to schedule delayBeforeUse on: applying key index {} \
                 immediately instead of in {use_after_ms}ms. Peers may not have it yet.",
                key.key_index,
            );
        } else if use_after_ms > 0 {
            log::trace!(
                "scheduling key index {} for participant {} in {use_after_ms}ms",
                key.key_index,
                key.rtc_backend_identity,
            );

            let ring = self.ring.clone();
            let keys = Arc::clone(&self.keys);
            let listener = Arc::clone(&self.listener);
            let local_sender = Arc::clone(&self.local_sender);
            let switch_listener = Arc::clone(&self.switch_listener);
            // Dropping the handle detaches the task; the delayed application
            // must survive the signalling call.
            rt::spawn(async move {
                rt::sleep(Duration::from_millis(use_after_ms)).await;
                Self::apply(ring.as_ref(), &keys, &listener, &local_sender, key).await;

                // The window the core asked us to wait out has just closed, and this
                // task is the only thing that knew when that would be. A rotation may
                // have been coalesced into it — every membership change since the
                // signal was folded into one rotation owed at exactly this instant —
                // so tell whoever can perform it.
                //
                // Only reached for a delayed key, which is only ever our own outbound
                // one: inbound keys are signalled usable immediately.
                if let Some(notify) = switch_listener
                    .lock()
                    .expect("key handler switch listener mutex poisoned")
                    .as_ref()
                {
                    notify();
                }
            });
            return;
        }

        Self::apply(
            self.ring.as_ref(),
            &self.keys,
            &self.listener,
            &self.local_sender,
            key,
        )
        .await;
    }

    /// Forwards a refusal straight through: there is nothing to install, and the
    /// reason exists nowhere else once the core has logged it.
    async fn on_key_discarded(&self, discarded: DiscardedKey) {
        if let Some(listener) = self
            .discard_listener
            .lock()
            .expect("key handler discard listener mutex poisoned")
            .as_ref()
        {
            listener(discarded);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rotating a key must change what we *encrypt* with, not just what the ring
    /// holds. `set_key` only fills the ring; the index a sender stamps lives on
    /// its frame cryptor. Without this hook we advertise a rotation to peers and
    /// carry on encrypting with the previous key — so a peer joining after the
    /// rotation decrypts nothing, and rotate-on-departure stops delivering
    /// forward secrecy.
    #[tokio::test]
    async fn our_own_key_moves_the_sender_to_its_index() {
        let handler = MediaKeyHandler::new();
        let switched: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

        let recorder = Arc::clone(&switched);
        handler.set_local_sender(
            "me",
            Box::new(move |index| recorder.lock().unwrap().push(index)),
        );

        for key_index in [0u8, 1] {
            handler
                .on_new_key_material(KeyMaterialSignal {
                    key: vec![key_index; 4],
                    key_index,
                    rtc_backend_identity: "me".to_owned(),
                    use_after_ms: 0,
                })
                .await;
        }

        assert_eq!(
            *switched.lock().unwrap(),
            vec![0, 1],
            "the sender should follow every key of ours, in order"
        );
    }

    /// A peer's key belongs in the ring so we can *decrypt* them; it says nothing
    /// about the index we should be encrypting with. Moving our sender onto it
    /// would make our own media undecryptable for everyone.
    #[tokio::test]
    async fn a_peer_key_leaves_our_sender_alone() {
        let handler = MediaKeyHandler::new();
        let switched: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

        let recorder = Arc::clone(&switched);
        handler.set_local_sender(
            "me",
            Box::new(move |index| recorder.lock().unwrap().push(index)),
        );

        handler
            .on_new_key_material(KeyMaterialSignal {
                key: vec![9; 4],
                key_index: 3,
                rtc_backend_identity: "someone-else".to_owned(),
                use_after_ms: 0,
            })
            .await;

        assert!(
            switched.lock().unwrap().is_empty(),
            "a peer's key must not change the index we encrypt with"
        );
        assert!(
            handler.key_for("someone-else").is_some(),
            "it should still have been imported for decryption"
        );
    }

    /// The switch waits for `delayBeforeUse` along with the import: that delay
    /// exists precisely so peers install the new key before we start using it.
    #[tokio::test(start_paused = true)]
    async fn the_sender_moves_only_once_the_delay_has_elapsed() {
        let handler = MediaKeyHandler::new();
        let switched: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

        let recorder = Arc::clone(&switched);
        handler.set_local_sender(
            "me",
            Box::new(move |index| recorder.lock().unwrap().push(index)),
        );

        handler
            .on_new_key_material(KeyMaterialSignal {
                key: vec![1; 4],
                key_index: 2,
                rtc_backend_identity: "me".to_owned(),
                use_after_ms: 5_000,
            })
            .await;

        assert!(
            switched.lock().unwrap().is_empty(),
            "encrypting with the new index before peers hold it is what the delay prevents"
        );

        tokio::time::sleep(Duration::from_millis(5_100)).await;
        assert_eq!(*switched.lock().unwrap(), vec![2]);
    }

    #[tokio::test]
    async fn records_signalled_key_material() {
        let handler = MediaKeyHandler::new();
        handler
            .on_new_key_material(KeyMaterialSignal {
                key: vec![1, 2, 3, 4],
                key_index: 7,
                rtc_backend_identity: "participant-abc".to_owned(),
                use_after_ms: 0,
            })
            .await;

        assert_eq!(
            handler.key_for("participant-abc"),
            Some(ParticipantKey {
                rtc_backend_identity: "participant-abc".to_owned(),
                key_index: 7,
                key: vec![1, 2, 3, 4],
            })
        );
        assert!(handler.key_for("unknown").is_none());
    }

    /// `use_after_ms` must be *scheduled*, not slept through: the signalling
    /// call has to return at once, because for the FFI its caller is a
    /// synchronous host call. The core used to own this wait and blocked there.
    #[tokio::test]
    async fn a_delayed_key_is_applied_only_after_the_delay() {
        let handler = MediaKeyHandler::new();
        handler
            .on_new_key_material(KeyMaterialSignal {
                key: vec![7u8; 32],
                key_index: 4,
                rtc_backend_identity: "p".to_owned(),
                use_after_ms: 60,
            })
            .await;

        assert!(
            handler.key_for("p").is_none(),
            "the signalling call must return before the delay elapses, not block through it"
        );

        tokio::time::sleep(Duration::from_millis(250)).await;

        assert_eq!(
            handler.key_for("p").map(|key| key.key_index),
            Some(4),
            "the key should have been applied once the delay elapsed"
        );
    }

    /// The end of a delay has to be reported, and only once it has arrived.
    ///
    /// This is the core's only way to reach that instant — it holds no timer — and
    /// it is when a rotation coalesced into the window falls due. Reporting early
    /// would have the core rotate while the previous one is still propagating,
    /// which is the burst behaviour the coalescing exists to avoid; not reporting
    /// at all leaves the owed rotation to the session heartbeat.
    #[tokio::test]
    async fn the_end_of_a_delay_is_reported_once_it_elapses() {
        let handler = MediaKeyHandler::new();
        let switches = Arc::new(Mutex::new(0usize));
        let counter = Arc::clone(&switches);
        handler.set_switch_complete_listener(Box::new(move || {
            *counter.lock().unwrap() += 1;
        }));

        handler
            .on_new_key_material(KeyMaterialSignal {
                key: vec![7u8; 32],
                key_index: 4,
                rtc_backend_identity: "p".to_owned(),
                use_after_ms: 60,
            })
            .await;
        assert_eq!(
            *switches.lock().unwrap(),
            0,
            "the window is still open, so nothing has switched yet"
        );

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            *switches.lock().unwrap(),
            1,
            "the delay elapsed and the key was installed, so the window is over"
        );
    }

    /// A key usable at once opens no window, so there is nothing to report the end
    /// of — and a spurious report would have the core collect a rotation that is
    /// not owed.
    #[tokio::test]
    async fn an_undelayed_key_reports_no_switch() {
        let handler = MediaKeyHandler::new();
        let switches = Arc::new(Mutex::new(0usize));
        let counter = Arc::clone(&switches);
        handler.set_switch_complete_listener(Box::new(move || {
            *counter.lock().unwrap() += 1;
        }));

        handler
            .on_new_key_material(KeyMaterialSignal {
                key: vec![7u8; 32],
                key_index: 4,
                rtc_backend_identity: "p".to_owned(),
                use_after_ms: 0,
            })
            .await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(*switches.lock().unwrap(), 0);
    }

    /// Rotations keep index order: the delay is a constant, so deadlines are
    /// ordered the same as the signalling calls that scheduled them.
    #[tokio::test]
    async fn delayed_keys_apply_in_index_order() {
        let handler = MediaKeyHandler::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        handler.set_key_import_listener(Box::new(move |key| {
            recorder.lock().unwrap().push(key.key_index);
        }));

        for index in [1u8, 2u8, 3u8] {
            handler
                .on_new_key_material(KeyMaterialSignal {
                    key: vec![index; 32],
                    key_index: index,
                    rtc_backend_identity: "p".to_owned(),
                    use_after_ms: 60,
                })
                .await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn latest_key_per_identity_wins() {
        let handler = MediaKeyHandler::new();
        for index in [1u8, 2u8] {
            handler
                .on_new_key_material(KeyMaterialSignal {
                    key: vec![index],
                    key_index: index,
                    rtc_backend_identity: "p".to_owned(),
                    use_after_ms: 0,
                })
                .await;
        }
        assert_eq!(handler.keys().len(), 1);
        assert_eq!(handler.key_for("p").unwrap().key_index, 2);
    }

    /// The ring-size guard belongs to the handler, not the ring: rings differ in
    /// how they fail on an out-of-range index (the native one aborts the
    /// process), so the index must be stopped before reaching one.
    #[tokio::test]
    async fn an_out_of_range_index_never_reaches_the_ring() {
        struct SmallRing {
            imported: Mutex<Vec<u8>>,
        }

        #[async_trait]
        impl FrameKeyRing for SmallRing {
            fn ring_size(&self) -> u16 {
                16
            }
            async fn set_key(&self, _identity: &str, index: u8, _key: Vec<u8>) -> bool {
                self.imported.lock().unwrap().push(index);
                true
            }
        }

        let ring = Arc::new(SmallRing {
            imported: Mutex::new(Vec::new()),
        });
        let handler = MediaKeyHandler::with_ring(ring.clone());
        let switched: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&switched);
        handler.set_local_sender(
            "me",
            Box::new(move |index| recorder.lock().unwrap().push(index)),
        );

        for key_index in [15u8, 16] {
            handler
                .on_new_key_material(KeyMaterialSignal {
                    key: vec![key_index; 32],
                    key_index,
                    rtc_backend_identity: "me".to_owned(),
                    use_after_ms: 0,
                })
                .await;
        }

        assert_eq!(
            *ring.imported.lock().unwrap(),
            vec![15],
            "index 16 exceeds a 16-slot ring and must be dropped before the ring sees it"
        );
        assert_eq!(
            *switched.lock().unwrap(),
            vec![15],
            "the sender must not move onto an index the ring holds no key for"
        );
        assert_eq!(
            handler.key_for("me").map(|key| key.key_index),
            Some(16),
            "a rejected key is still recorded"
        );
    }

    /// A ring returning `false` marks the key rejected: recorded, but the sender
    /// stays where it is.
    #[tokio::test]
    async fn a_ring_refusal_leaves_the_sender_alone() {
        struct RefusingRing;

        #[async_trait]
        impl FrameKeyRing for RefusingRing {
            fn ring_size(&self) -> u16 {
                255
            }
            async fn set_key(&self, _identity: &str, _index: u8, _key: Vec<u8>) -> bool {
                false
            }
        }

        let handler = MediaKeyHandler::with_ring(Arc::new(RefusingRing));
        let switched: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&switched);
        handler.set_local_sender(
            "me",
            Box::new(move |index| recorder.lock().unwrap().push(index)),
        );

        handler
            .on_new_key_material(KeyMaterialSignal {
                key: vec![1u8; 32],
                key_index: 1,
                rtc_backend_identity: "me".to_owned(),
                use_after_ms: 0,
            })
            .await;

        assert!(switched.lock().unwrap().is_empty());
        assert!(handler.key_for("me").is_some());
    }
}
