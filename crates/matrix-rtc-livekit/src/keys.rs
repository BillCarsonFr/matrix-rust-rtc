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

//! Bridge from `matrix-rtc-core` key signals towards LiveKit frame encryption.
//!
//! `matrix-rtc-core` produces per-participant media keys and hands them to the
//! application via [`EncryptionKeySignalHandler::on_new_key_material`]. The
//! eventual destination is LiveKit's frame-encryption `KeyProvider`, keyed by
//! the participant's pseudonymous identity (see [`crate::identity`]) and the
//! key index.
//!
//! A bridge created via [`MediaKeyBridge::with_provider`] imports signalled
//! keys into a [`KeyProvider`] configured for MSC4195 per-participant mode
//! ([`msc4195_key_provider`]); one created via [`MediaKeyBridge::new`] only
//! records them. End-to-end media decryption against Element Call is not yet
//! validated.
//!
//! This bridge also owns the MSC4143 `delayBeforeUse` wait. `matrix-rtc-core`
//! deliberately holds no timer — it only states the delay, as
//! [`KeyMaterialSignal::use_after_ms`] — so that a synchronous FFI host can
//! drive it from a plain thread. Enforcing it is therefore a transport-layer
//! obligation, and for now this is the only implementation of it; a future
//! transport-agnostic layer is the natural home.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use livekit::e2ee::key_provider::{KeyDerivationAlgorithm, KeyProvider, KeyProviderOptions};
use livekit::id::ParticipantIdentity;
use matrix_rtc_core::{DiscardedKey, EncryptionKeySignalHandler, KeyMaterialSignal};

/// Largest key ring the native frame cryptor allocates.
///
/// A `key_ring_size` above 255 is accepted by [`KeyProviderOptions`] but not
/// allocated, and `set_key`/`get_key` with an index at or past the allocated
/// size aborts the process (fatal assertion in `ParticipantKeyHandler`)
/// instead of returning an error. Observed with `livekit` 0.7.48 /
/// `libwebrtc` 0.3.38. The `m.rtc.encryption_keys` `index` field is a `u8`,
/// so index 255 cannot be represented and must be rejected before the FFI
/// boundary.
pub const NATIVE_KEY_RING_MAX: i32 = 255;

/// [`KeyProviderOptions`] for MSC4195 per-participant mode.
///
/// MSC4195 specifies that per-participant keys are "imported as the byte array
/// input to the LiveKit key derivation function (which uses HKDF)" and that the
/// `index` field of `m.rtc.encryption_keys` is used as the key index. The ring
/// is sized to [`NATIVE_KEY_RING_MAX`], the widest the native layer allows.
pub fn msc4195_key_provider_options() -> KeyProviderOptions {
    KeyProviderOptions {
        key_derivation_algorithm: KeyDerivationAlgorithm::HKDF,
        key_ring_size: NATIVE_KEY_RING_MAX,
        ..KeyProviderOptions::default()
    }
}

/// A per-participant (non-shared) [`KeyProvider`] configured for MSC4195.
///
/// Pass the same provider to [`MediaKeyBridge::with_provider`] and to
/// [`livekit::RoomOptions`] (the `encryption` field) so keys signalled by
/// `matrix-rtc-core` reach the frame cryptor of the connected room.
pub fn msc4195_key_provider() -> KeyProvider {
    KeyProvider::new(msc4195_key_provider_options())
}

/// A piece of media key material destined for the SFU frame cryptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantKey {
    /// Pseudonymous LiveKit participant identity the key belongs to.
    pub rtc_backend_identity: String,
    /// Key index, used as the LiveKit `KeyProvider` key index.
    pub key_index: u8,
    /// Raw key bytes (HKDF input material), as produced by `matrix-rtc-core`.
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

/// Callback invoked after every signalled key has been recorded (and, with a
/// provider, imported). Must not block: it runs on the signalling path.
pub type KeyImportListener = Box<dyn Fn(&ParticipantKey) + Send + Sync>;

/// Notified when the core refuses a key, so the reason can reach the host.
pub type KeyDiscardListener = Box<dyn Fn(DiscardedKey) + Send + Sync>;

/// Notified when a key's MSC4143 `delayBeforeUse` has elapsed and it has been
/// installed — the moment our own rotation actually takes over.
///
/// Exists so the core can be told the window is over. Membership changes that
/// arrive during a rotation's window are coalesced into one rotation at the end
/// of it, and the core owns no timer to reach that instant with; this bridge
/// already schedules exactly it. Wire this to
/// `RtcSessionManager::flush_due_key_rotation`.
///
/// Runs on the scheduled task, so it must not block. Sending on a channel is the
/// intended shape — the core is often `!Send` and cannot be touched from here.
pub type SwitchCompleteListener = Box<dyn Fn() + Send + Sync>;

/// Switches our own outgoing frames to a new key index.
///
/// Installing a key only fills the provider's *ring*; the sender keeps stamping
/// whatever index its frame cryptor was created with (0). Rotating without this
/// advertises a new key to peers while continuing to encrypt with the old one —
/// so a peer joining after the rotation cannot decrypt us, and the forward
/// secrecy the rotation exists for is not delivered.
///
/// Installed once the room is connected, because only the connection can reach
/// the frame cryptors, and the bridge is built before it.
pub type LocalKeyIndexHook = Box<dyn Fn(u8) + Send + Sync>;

/// Records media keys signalled by `matrix-rtc-core` and, when built with
/// [`MediaKeyBridge::with_provider`], imports them into a LiveKit
/// [`KeyProvider`].
///
/// Implements [`EncryptionKeySignalHandler`] so it can be registered directly
/// with the core encryption manager.
#[derive(Default)]
pub struct MediaKeyBridge {
    /// `Arc` so a delayed application (see [`KeyMaterialSignal::use_after_ms`])
    /// can own a handle that outlives the signalling call — and, if it comes to
    /// it, the bridge.
    keys: Arc<Mutex<HashMap<String, ParticipantKey>>>,
    provider: Option<KeyProvider>,
    listener: Arc<Mutex<Option<KeyImportListener>>>,
    discard_listener: Arc<Mutex<Option<KeyDiscardListener>>>,
    /// Told when a delayed key has come into use (see [`SwitchCompleteListener`]).
    switch_listener: Arc<Mutex<Option<SwitchCompleteListener>>>,
    /// Our own MSC4195 identity and how to re-index our sender; `None` until the
    /// room is connected. See [`LocalKeyIndexHook`].
    local_sender: Arc<Mutex<Option<(String, LocalKeyIndexHook)>>>,
}

impl MediaKeyBridge {
    /// Create an empty, record-only bridge.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a bridge that additionally forwards every signalled key into
    /// `provider` via [`KeyProvider::set_key`], keyed by the participant's
    /// pseudonymous identity and the signalled key index.
    ///
    /// [`KeyProvider`] is a shared handle: clone it and hand the clone to
    /// [`livekit::RoomOptions`] so the connected room decrypts with the same
    /// key ring.
    pub fn with_provider(provider: KeyProvider) -> Self {
        Self {
            provider: Some(provider),
            ..Self::default()
        }
    }

    /// Register a callback observing every signalled key (e.g. to surface
    /// `KeyImported` on a call event stream). Replaces any previous listener.
    pub fn set_key_import_listener(&self, listener: KeyImportListener) {
        *self
            .listener
            .lock()
            .expect("key bridge listener mutex poisoned") = Some(listener);
    }

    /// Register a callback observing every *refused* key. Replaces any previous
    /// listener.
    pub fn set_key_discard_listener(&self, listener: KeyDiscardListener) {
        *self
            .discard_listener
            .lock()
            .expect("key bridge discard listener mutex poisoned") = Some(listener);
    }

    /// Register a callback for the end of a key's `delayBeforeUse` window (see
    /// [`SwitchCompleteListener`]). Replaces any previous listener.
    ///
    /// Without it the bridge still honours the delay; what is lost is the core's
    /// chance to perform a rotation it coalesced into the window at the instant the
    /// window ends, leaving that to the session heartbeat.
    pub fn set_switch_complete_listener(&self, listener: SwitchCompleteListener) {
        *self
            .switch_listener
            .lock()
            .expect("key bridge switch listener mutex poisoned") = Some(listener);
    }

    /// Tell the bridge which identity is ours and how to switch our sender to a
    /// new key index, so an activated rotation actually changes what we encrypt
    /// with. Without it the bridge records and imports keys as before, and our
    /// own rotations never take effect.
    pub fn set_local_sender(&self, own_identity: impl Into<String>, hook: LocalKeyIndexHook) {
        *self
            .local_sender
            .lock()
            .expect("key bridge local sender mutex poisoned") = Some((own_identity.into(), hook));
    }

    /// The latest recorded key for a participant identity, if any.
    pub fn key_for(&self, rtc_backend_identity: &str) -> Option<ParticipantKey> {
        self.keys
            .lock()
            .expect("key bridge mutex poisoned")
            .get(rtc_backend_identity)
            .cloned()
    }

    /// A snapshot of every recorded key.
    pub fn keys(&self) -> Vec<ParticipantKey> {
        self.keys
            .lock()
            .expect("key bridge mutex poisoned")
            .values()
            .cloned()
            .collect()
    }
}

impl MediaKeyBridge {
    /// Imports `key` into the provider, records it, and notifies the listener.
    ///
    /// Takes its state as parameters rather than `&self` so a delayed
    /// application can call it from a spawned task holding only cloned handles.
    fn apply(
        provider: Option<&KeyProvider>,
        keys: &Mutex<HashMap<String, ParticipantKey>>,
        listener: &Mutex<Option<KeyImportListener>>,
        local_sender: &Mutex<Option<(String, LocalKeyIndexHook)>>,
        key: ParticipantKey,
    ) {
        let index = i32::from(key.key_index);
        // Whether the key ring refused this index. Our sender must not be moved
        // onto an index it holds no key for — that would make our media
        // undecryptable for everyone, rather than for the one peer whose key
        // failed.
        let mut rejected = false;
        if let Some(provider) = provider {
            // Indices at or past the ring size abort the process in the native
            // frame cryptor rather than returning false, and peers control this
            // value (see NATIVE_KEY_RING_MAX). Drop (but still record) such a
            // key: media from that peer stays undecryptable, but the process
            // survives.
            if index < NATIVE_KEY_RING_MAX {
                // `rtc_backend_identity` equals `identity::pseudonymous_identity(...)`
                // and maps directly onto the LiveKit participant identity.
                let identity = ParticipantIdentity::from(key.rtc_backend_identity.clone());
                if !provider.set_key(&identity, index, key.key.clone()) {
                    rejected = true;
                    // TODO: surface set_key failures to the host.
                    log::warn!(
                        "LiveKit KeyProvider rejected key index {index} for participant {}; \
                         its media will not decrypt",
                        key.rtc_backend_identity,
                    );
                }
            } else {
                rejected = true;
                log::warn!(
                    "dropping key index {index} for participant {}: exceeds native ring size \
                     (NATIVE_KEY_RING_MAX = {NATIVE_KEY_RING_MAX}); its media will not decrypt",
                    key.rtc_backend_identity,
                );
            }
        }
        // Our own key: also move the sender onto the new index. `set_key` alone
        // only fills the ring, so without this we would advertise the rotation
        // and keep stamping the previous index.
        if !rejected
            && let Some((own_identity, hook)) = local_sender
                .lock()
                .expect("key bridge local sender mutex poisoned")
                .as_ref()
            && own_identity == &key.rtc_backend_identity
        {
            hook(key.key_index);
        }

        keys.lock()
            .expect("key bridge mutex poisoned")
            .insert(key.rtc_backend_identity.clone(), key.clone());
        if let Some(listener) = listener
            .lock()
            .expect("key bridge listener mutex poisoned")
            .as_ref()
        {
            listener(&key);
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl EncryptionKeySignalHandler for MediaKeyBridge {
    /// Applies a signalled key, honouring the MSC4143 `delayBeforeUse` the core
    /// attaches as [`KeyMaterialSignal::use_after_ms`].
    ///
    /// A non-zero delay is scheduled, never slept through: this runs on the
    /// caller's task, and for the FFI that caller is a *synchronous* host call,
    /// so blocking here would stall a host thread for the whole delay. The core
    /// used to own this wait and did exactly that.
    ///
    /// Everything — provider import, recording, listener — happens at activation
    /// time rather than signalling time, so what the bridge exposes matches what
    /// LiveKit is actually encrypting with. During the delay `key_for` keeps
    /// reporting the previous key, which is the one still in use.
    ///
    /// Ordering holds without extra machinery: the delay is a constant from
    /// `EncryptionConfig::delay_before_use_ms`, so keys signalled in index order
    /// get deadlines in the same order.
    async fn on_new_key_material(&self, signal: KeyMaterialSignal) {
        let use_after_ms = signal.use_after_ms;
        let key = ParticipantKey::from(signal);

        if use_after_ms == 0 {
            Self::apply(
                self.provider.as_ref(),
                &self.keys,
                &self.listener,
                &self.local_sender,
                key,
            );
            return;
        }

        // `Handle::spawn` rather than `tokio::spawn` so the absence of a runtime
        // is a branch we handle instead of a panic. Falling back to applying
        // immediately deviates from the spec — peers may not have the key yet,
        // so some frames go undecryptable — but that is recoverable, whereas
        // never applying it at all breaks the session outright. Loud, because it
        // means a consumer is driving the core without a runtime.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            log::warn!(
                "no tokio runtime to schedule delayBeforeUse on: applying key index {} \
                 immediately instead of in {use_after_ms}ms. Peers may not have it yet.",
                key.key_index,
            );
            Self::apply(
                self.provider.as_ref(),
                &self.keys,
                &self.listener,
                &self.local_sender,
                key,
            );
            return;
        };

        log::trace!(
            "scheduling key index {} for participant {} in {use_after_ms}ms",
            key.key_index,
            key.rtc_backend_identity,
        );

        let provider = self.provider.clone();
        let keys = Arc::clone(&self.keys);
        let listener = Arc::clone(&self.listener);
        let local_sender = Arc::clone(&self.local_sender);
        let switch_listener = Arc::clone(&self.switch_listener);
        handle.spawn(async move {
            tokio::time::sleep(Duration::from_millis(use_after_ms)).await;
            Self::apply(provider.as_ref(), &keys, &listener, &local_sender, key);

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
                .expect("key bridge switch listener mutex poisoned")
                .as_ref()
            {
                notify();
            }
        });
    }

    /// Forwards a refusal straight through: there is nothing to install, and the
    /// reason exists nowhere else once the core has logged it.
    async fn on_key_discarded(&self, discarded: DiscardedKey) {
        if let Some(listener) = self
            .discard_listener
            .lock()
            .expect("key bridge discard listener mutex poisoned")
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
    /// holds. `KeyProvider::set_key` only fills the ring; the index a sender
    /// stamps lives on its frame cryptor. Without this hook we advertise a
    /// rotation to peers and carry on encrypting with the previous key — so a
    /// peer joining after the rotation decrypts nothing, and rotate-on-departure
    /// stops delivering forward secrecy.
    #[tokio::test]
    async fn our_own_key_moves_the_sender_to_its_index() {
        let bridge = MediaKeyBridge::new();
        let switched: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

        let recorder = Arc::clone(&switched);
        bridge.set_local_sender(
            "me",
            Box::new(move |index| recorder.lock().unwrap().push(index)),
        );

        for key_index in [0u8, 1] {
            bridge
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
        let bridge = MediaKeyBridge::new();
        let switched: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

        let recorder = Arc::clone(&switched);
        bridge.set_local_sender(
            "me",
            Box::new(move |index| recorder.lock().unwrap().push(index)),
        );

        bridge
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
            bridge.key_for("someone-else").is_some(),
            "it should still have been imported for decryption"
        );
    }

    /// The switch waits for `delayBeforeUse` along with the import: that delay
    /// exists precisely so peers install the new key before we start using it.
    #[tokio::test(start_paused = true)]
    async fn the_sender_moves_only_once_the_delay_has_elapsed() {
        let bridge = MediaKeyBridge::new();
        let switched: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

        let recorder = Arc::clone(&switched);
        bridge.set_local_sender(
            "me",
            Box::new(move |index| recorder.lock().unwrap().push(index)),
        );

        bridge
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
        let bridge = MediaKeyBridge::new();
        bridge
            .on_new_key_material(KeyMaterialSignal {
                key: vec![1, 2, 3, 4],
                key_index: 7,
                rtc_backend_identity: "participant-abc".to_owned(),
                use_after_ms: 0,
            })
            .await;

        assert_eq!(
            bridge.key_for("participant-abc"),
            Some(ParticipantKey {
                rtc_backend_identity: "participant-abc".to_owned(),
                key_index: 7,
                key: vec![1, 2, 3, 4],
            })
        );
        assert!(bridge.key_for("unknown").is_none());
    }

    /// `use_after_ms` must be *scheduled*, not slept through: the signalling
    /// call has to return at once, because for the FFI its caller is a
    /// synchronous host call. The core used to own this wait and blocked there.
    #[tokio::test]
    async fn a_delayed_key_is_applied_only_after_the_delay() {
        let bridge = MediaKeyBridge::new();
        bridge
            .on_new_key_material(KeyMaterialSignal {
                key: vec![7u8; 32],
                key_index: 4,
                rtc_backend_identity: "p".to_owned(),
                use_after_ms: 60,
            })
            .await;

        assert!(
            bridge.key_for("p").is_none(),
            "the signalling call must return before the delay elapses, not block through it"
        );

        tokio::time::sleep(Duration::from_millis(250)).await;

        assert_eq!(
            bridge.key_for("p").map(|key| key.key_index),
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
        let bridge = MediaKeyBridge::new();
        let switches = Arc::new(Mutex::new(0usize));
        let counter = Arc::clone(&switches);
        bridge.set_switch_complete_listener(Box::new(move || {
            *counter.lock().unwrap() += 1;
        }));

        bridge
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
        let bridge = MediaKeyBridge::new();
        let switches = Arc::new(Mutex::new(0usize));
        let counter = Arc::clone(&switches);
        bridge.set_switch_complete_listener(Box::new(move || {
            *counter.lock().unwrap() += 1;
        }));

        bridge
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
        let bridge = MediaKeyBridge::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        bridge.set_key_import_listener(Box::new(move |key| {
            recorder.lock().unwrap().push(key.key_index);
        }));

        for index in [1u8, 2u8, 3u8] {
            bridge
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
        let bridge = MediaKeyBridge::new();
        for index in [1u8, 2u8] {
            bridge
                .on_new_key_material(KeyMaterialSignal {
                    key: vec![index],
                    key_index: index,
                    rtc_backend_identity: "p".to_owned(),
                    use_after_ms: 0,
                })
                .await;
        }
        assert_eq!(bridge.keys().len(), 1);
        assert_eq!(bridge.key_for("p").unwrap().key_index, 2);
    }

    #[test]
    fn msc4195_provider_accepts_per_participant_hkdf_import() {
        let provider = msc4195_key_provider();
        let identity = ParticipantIdentity::from("participant-abc".to_owned());

        // The full addressable range of the native ring (see
        // NATIVE_KEY_RING_MAX for why index 255 is excluded).
        assert!(provider.set_key(&identity, 0, vec![1u8; 32]));
        assert!(provider.set_key(&identity, NATIVE_KEY_RING_MAX - 1, vec![2u8; 32]));
        assert!(provider.get_key(&identity, 0).is_some());
        assert!(
            provider
                .get_key(&identity, NATIVE_KEY_RING_MAX - 1)
                .is_some()
        );
    }

    #[tokio::test]
    async fn bridge_rejects_unrepresentable_key_index() {
        let provider = msc4195_key_provider();
        let bridge = MediaKeyBridge::with_provider(provider);

        // Index 255 cannot be stored in the native ring; forwarding it would
        // abort the process. The bridge must survive it and still record.
        bridge
            .on_new_key_material(KeyMaterialSignal {
                key: vec![4u8; 32],
                key_index: u8::MAX,
                rtc_backend_identity: "participant-p2".to_owned(),
                use_after_ms: 0,
            })
            .await;

        assert_eq!(bridge.key_for("participant-p2").unwrap().key_index, u8::MAX);
    }

    #[tokio::test]
    async fn bridge_forwards_key_material_to_provider() {
        let provider = msc4195_key_provider();
        let bridge = MediaKeyBridge::with_provider(provider.clone());

        bridge
            .on_new_key_material(KeyMaterialSignal {
                key: vec![9u8; 32],
                key_index: 3,
                rtc_backend_identity: "participant-p1".to_owned(),
                use_after_ms: 0,
            })
            .await;

        // Recorded, as before.
        assert!(bridge.key_for("participant-p1").is_some());
        // And imported into the shared provider handle.
        let identity = ParticipantIdentity::from("participant-p1".to_owned());
        assert!(provider.get_key(&identity, 3).is_some());
    }

    #[test]
    fn room_options_accept_msc4195_e2ee() {
        use livekit::RoomOptions;
        use livekit::e2ee::{E2eeOptions, EncryptionType};

        // RoomOptions is #[non_exhaustive]: mutate a default instance.
        let mut options = RoomOptions::default();
        options.encryption = Some(E2eeOptions {
            encryption_type: EncryptionType::Gcm,
            key_provider: msc4195_key_provider(),
        });
        assert!(options.encryption.is_some());
    }
}
