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
//! **E2EE is intentionally not wired in this phase.** MSC4195 itself notes that
//! the LiveKit Rust SDK currently lacks the per-participant HKDF key import this
//! path needs (see [livekit/rust-sdks#796]). Until that lands, [`MediaKeyBridge`]
//! records the signalled key material — so the seam is exercised and
//! observable — and marks precisely where the `KeyProvider` hand-off belongs.
//!
//! [livekit/rust-sdks#796]: https://github.com/livekit/rust-sdks/issues/796

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use matrix_rtc_core::{EncryptionKeySignalHandler, KeyMaterialSignal};

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

/// Records media keys signalled by `matrix-rtc-core`, standing in for the
/// future LiveKit `KeyProvider` hand-off.
///
/// Implements [`EncryptionKeySignalHandler`] so it can be registered directly
/// with the core encryption manager.
#[derive(Default)]
pub struct MediaKeyBridge {
    keys: Mutex<HashMap<String, ParticipantKey>>,
}

impl MediaKeyBridge {
    /// Create an empty bridge.
    pub fn new() -> Self {
        Self::default()
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

#[async_trait(?Send)]
impl EncryptionKeySignalHandler for MediaKeyBridge {
    async fn on_new_key_material(&self, signal: KeyMaterialSignal) {
        // TODO(e2ee): forward this key to the LiveKit frame-encryption
        // KeyProvider once the Rust SDK supports per-participant HKDF key import
        // (MSC4195 / livekit/rust-sdks#796). `signal.rtc_backend_identity`
        // already equals `identity::pseudonymous_identity(...)`, so it maps
        // directly onto the LiveKit participant.
        let key = ParticipantKey::from(signal);
        self.keys
            .lock()
            .expect("key bridge mutex poisoned")
            .insert(key.rtc_backend_identity.clone(), key);
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

    #[tokio::test]
    async fn latest_key_per_identity_wins() {
        let bridge = MediaKeyBridge::new();
        for index in [1u8, 2u8] {
            bridge
                .on_new_key_material(KeyMaterialSignal {
                    key: vec![index],
                    key_index: index,
                    rtc_backend_identity: "p".to_owned(),
                })
                .await;
        }
        assert_eq!(bridge.keys().len(), 1);
        assert_eq!(bridge.key_for("p").unwrap().key_index, 2);
    }
}
