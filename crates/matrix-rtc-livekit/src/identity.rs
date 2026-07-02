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

//! Hash derivations defined by [MSC4195].
//!
//! Both the `livekit_alias` and the pseudonymous LiveKit participant identity
//! are computed as `base64(SHA256(JSON.serialize([...])))`, where
//! `JSON.serialize` is [Matrix canonical JSON] and `base64` is the unpadded
//! standard alphabet.
//!
//! The `livekit_alias` and SFU URL normally reach the client baked into the JWT
//! returned by the authorisation service, so a connecting client does not need
//! to derive the alias itself. The pseudonymous identity, however, is needed to
//! map a received per-participant encryption key (from `matrix-rtc-core`) onto
//! the corresponding LiveKit participant once E2EE is wired up.
//!
//! [MSC4195]: https://github.com/matrix-org/matrix-spec-proposals/pull/4195
//! [Matrix canonical JSON]: https://spec.matrix.org/v1.18/appendices/#canonical-json

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use sha2::{Digest, Sha256};

/// Serialize an array of strings to Matrix canonical JSON.
///
/// MSC4195 only ever hashes arrays whose elements are JSON strings, so the
/// canonical form is exactly the compact, key-less array `serde_json` already
/// emits (no insignificant whitespace, standard string escaping). The function
/// is intentionally restricted to `&[&str]` to make that guarantee explicit.
fn canonical_json_string_array(elements: &[&str]) -> String {
    // `serde_json` serializes a slice of strings with no whitespace, which
    // matches canonical JSON for this restricted shape.
    serde_json::to_string(elements).expect("serializing a string array never fails")
}

/// `base64(SHA256(canonical_json(elements)))` using the unpadded standard
/// base64 alphabet, the primitive shared by all MSC4195 hash derivations.
fn hash_string_array(elements: &[&str]) -> String {
    let canonical = canonical_json_string_array(elements);
    let digest = Sha256::digest(canonical.as_bytes());
    STANDARD_NO_PAD.encode(digest)
}

/// Derive the `livekit_alias` for a MatrixRTC slot, the minimal form from
/// MSC4195: `base64(SHA256(JSON.serialize([room_id, slot_id])))`.
///
/// Production deployments may add server-held random bits for stronger
/// metadata protection; that variant is intentionally not implemented here as
/// the alias normally arrives via the authorisation service's JWT.
pub fn livekit_alias(room_id: &str, slot_id: &str) -> String {
    hash_string_array(&[room_id, slot_id])
}

/// Derive the pseudonymous LiveKit participant identity from MSC4195:
/// `base64(SHA256(JSON.serialize([user_id, claimed_device_id, member_id])))`.
///
/// This is the `sub` the authorisation service places in the JWT, and the value
/// used to associate an inbound `m.rtc.encryption_keys` key with a LiveKit
/// participant.
pub fn pseudonymous_identity(user_id: &str, claimed_device_id: &str, member_id: &str) -> String {
    hash_string_array(&[user_id, claimed_device_id, member_id])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_json_has_no_whitespace() {
        assert_eq!(
            canonical_json_string_array(&["!roomid:example.com", "slot1234"]),
            r#"["!roomid:example.com","slot1234"]"#
        );
    }

    // Verified test vector from the MSC4195 appendix (alias without random bits).
    #[test]
    fn livekit_alias_matches_msc4195_vector() {
        assert_eq!(
            livekit_alias("!roomid:example.com", "slot1234"),
            "O8437W3+jmzMVjoIP3tNwbm+XxHQk2iKpOA7aqw3qSc"
        );
    }

    #[test]
    fn pseudonymous_identity_is_stable_and_unpadded() {
        let id = pseudonymous_identity("@user:matrix.example.com", "DEVICEID", "xyzABCDEF10123");
        // SHA256 -> 32 bytes -> 43 unpadded base64 chars, no '=' padding.
        assert_eq!(id.len(), 43);
        assert!(!id.contains('='));
        // Deterministic.
        assert_eq!(
            id,
            pseudonymous_identity("@user:matrix.example.com", "DEVICEID", "xyzABCDEF10123")
        );
    }
}
