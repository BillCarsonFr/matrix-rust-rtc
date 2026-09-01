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

//! The native LiveKit backing for `matrix-rtc-media`'s key handler.
//!
//! The bookkeeping — recording, the ring-size guard, the rejected-key rule,
//! the local sender's index switch, the MSC4143 `delayBeforeUse` wait — lives
//! in [`matrix_rtc_media::keys`], shared with the web binding. This module
//! contributes only what is LiveKit-native: a [`KeyProvider`] configured for
//! MSC4195 per-participant mode, and its [`FrameKeyRing`] adapter.
//!
//! `MediaKeyBridge` remains this crate's name for the shared
//! [`MediaKeyHandler`]; build one over a provider with
//! [`msc4195_media_key_bridge`].

use async_trait::async_trait;
use livekit::e2ee::key_provider::{KeyDerivationAlgorithm, KeyProvider, KeyProviderOptions};
use livekit::id::ParticipantIdentity;
use matrix_rtc_media::keys::{FrameKeyRing, MediaKeyHandler};

pub use matrix_rtc_media::keys::{
    KeyDiscardListener, KeyImportListener, LocalKeyIndexHook, ParticipantKey,
    SwitchCompleteListener,
};

/// This crate's name for the shared key handler, kept from when the type lived
/// here.
pub type MediaKeyBridge = MediaKeyHandler;

/// Largest key ring the native frame cryptor allocates.
///
/// A `key_ring_size` above 255 is accepted by [`KeyProviderOptions`] but not
/// allocated, and `set_key`/`get_key` with an index at or past the allocated
/// size aborts the process (fatal assertion in `ParticipantKeyHandler`)
/// instead of returning an error. Observed with `livekit` 0.7.48 /
/// `libwebrtc` 0.3.38. The `m.rtc.encryption_keys` `index` field is a `u8`,
/// so index 255 cannot be represented and must be rejected before the FFI
/// boundary — which is exactly what [`FrameKeyRing::ring_size`] exists for.
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
/// Pass the same provider to [`msc4195_media_key_bridge`] and to
/// [`livekit::RoomOptions`] (the `encryption` field) so keys signalled by
/// `matrix-rtc-core` reach the frame cryptor of the connected room.
pub fn msc4195_key_provider() -> KeyProvider {
    KeyProvider::new(msc4195_key_provider_options())
}

/// [`FrameKeyRing`] over a LiveKit [`KeyProvider`].
struct LiveKitKeyRing(KeyProvider);

#[async_trait]
impl FrameKeyRing for LiveKitKeyRing {
    fn ring_size(&self) -> u16 {
        NATIVE_KEY_RING_MAX as u16
    }

    async fn set_key(&self, identity: &str, index: u8, key: Vec<u8>) -> bool {
        // `rtc_backend_identity` equals `identity::pseudonymous_identity(...)`
        // and maps directly onto the LiveKit participant identity.
        let identity = ParticipantIdentity::from(identity.to_owned());
        self.0.set_key(&identity, i32::from(index), key)
    }
}

/// A [`MediaKeyBridge`] that forwards every signalled key into `provider` via
/// [`KeyProvider::set_key`], keyed by the participant's pseudonymous identity
/// and the signalled key index.
///
/// [`KeyProvider`] is a shared handle: clone it and hand the clone to
/// [`livekit::RoomOptions`] so the connected room decrypts with the same key
/// ring.
pub fn msc4195_media_key_bridge(provider: KeyProvider) -> MediaKeyBridge {
    MediaKeyHandler::with_ring(std::sync::Arc::new(LiveKitKeyRing(provider)))
}

#[cfg(test)]
mod tests {
    use matrix_rtc_core::{EncryptionKeySignalHandler, KeyMaterialSignal};

    use super::*;

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
        let bridge = msc4195_media_key_bridge(provider);

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
        let bridge = msc4195_media_key_bridge(provider.clone());

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
