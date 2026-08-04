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
use matrix_rtc_core::{EncryptionKeySignalHandler, KeyMaterialSignal};

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
            keys: Arc::new(Mutex::new(HashMap::new())),
            provider: Some(provider),
            listener: Arc::new(Mutex::new(None)),
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
        key: ParticipantKey,
    ) {
        let index = i32::from(key.key_index);
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
                    // TODO: surface set_key failures to the host.
                    log::warn!(
                        "LiveKit KeyProvider rejected key index {index} for participant {}; \
                         its media will not decrypt",
                        key.rtc_backend_identity,
                    );
                }
            } else {
                log::warn!(
                    "dropping key index {index} for participant {}: exceeds native ring size \
                     (NATIVE_KEY_RING_MAX = {NATIVE_KEY_RING_MAX}); its media will not decrypt",
                    key.rtc_backend_identity,
                );
            }
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

#[async_trait(?Send)]
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
            Self::apply(self.provider.as_ref(), &self.keys, &self.listener, key);
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
            Self::apply(self.provider.as_ref(), &self.keys, &self.listener, key);
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
        handle.spawn(async move {
            tokio::time::sleep(Duration::from_millis(use_after_ms)).await;
            Self::apply(provider.as_ref(), &keys, &listener, key);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
