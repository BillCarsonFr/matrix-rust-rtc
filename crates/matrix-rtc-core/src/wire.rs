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

//! Stable-to-unstable event type mapping for bindings without ruma.
//!
//! The core speaks the stable MSC4143 identifiers (`m.rtc.member`,
//! `m.rtc.slot`) everywhere, because that is what the MSC will land as. Nothing
//! deployed answers to them yet: Element Call, the SFU and every other client
//! publish and match on the unstable `org.matrix.msc4143.*` ids, so a
//! membership put on the wire as `m.rtc.member` is invisible to peers.
//!
//! Bindings must therefore translate on the way out. The `matrix-sdk` host does
//! it through ruma's alias table (`matrix-rtc-bridge`'s `sdk::wire_event_type`);
//! the FFI and WASM bindings hand event types to a native SDK that passes the
//! string through verbatim, and neither has ruma to ask — they call
//! [`wire_event_type`]
//! instead. Keep the two in sync: when ruma flips to the stable ids after FCP,
//! this table becomes the identity mapping and can go away.
//!
//! Only the outbound direction needs it. Inbound, the core accepts both
//! spellings (see `RawStickyEvent::validate`), so peers on either id are seen.

/// Translate a core event type to the identifier that goes on the wire.
///
/// Types the table does not know pass through untouched, so a host can route
/// its own event types through the same command callbacks.
pub fn wire_event_type(event_type: &str) -> &str {
    match event_type {
        "m.rtc.member" => "org.matrix.msc4143.rtc.member",
        "m.rtc.slot" => "org.matrix.msc4143.rtc.slot",
        "m.rtc.encryption_key" => "org.matrix.msc4143.rtc.encryption_key",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KEY_MESSAGE_TYPE, SLOT_EVENT_TYPE};

    #[test]
    fn maps_the_types_the_core_sends() {
        assert_eq!(
            wire_event_type("m.rtc.member"),
            "org.matrix.msc4143.rtc.member"
        );
        assert_eq!(
            wire_event_type(SLOT_EVENT_TYPE),
            "org.matrix.msc4143.rtc.slot"
        );
        // Already unstable in the core; must survive a round through the table.
        assert_eq!(wire_event_type(KEY_MESSAGE_TYPE), KEY_MESSAGE_TYPE);
    }

    #[test]
    fn leaves_unknown_types_alone() {
        assert_eq!(wire_event_type("com.example.custom"), "com.example.custom");
    }

    #[test]
    fn is_idempotent() {
        for stable in ["m.rtc.member", SLOT_EVENT_TYPE, KEY_MESSAGE_TYPE] {
            let once = wire_event_type(stable);
            assert_eq!(wire_event_type(once), once);
        }
    }
}
