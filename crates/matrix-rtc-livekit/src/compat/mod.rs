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

//! Interoperability with MatrixRTC implementations that predate the 2026
//! MSC4143 rewrite.
//!
//! # This module is scaffolding, and is meant to be deleted
//!
//! Everything here exists for one reason: the only other MatrixRTC
//! implementation available to test against — Element Call on the JS SDK — still
//! speaks the pre-2026 wire format. Once it catches up, delete this directory,
//! the three call sites listed below, and
//! [`CallOptions::legacy_element_call`](crate::CallOptions::legacy_element_call).
//! Nothing else should ever grow a dependency on it.
//!
//! # Why it is shaped this way
//!
//! The rest of the stack speaks current MSC4143 and nothing else — no dialect
//! parameter reaches `matrix-rtc-core`, no legacy field appears in a domain
//! type. Compatibility is confined to two JSON funnels at the very edge, both
//! in [`element_call`]:
//!
//! - **Inbound** ([`element_call::normalize_member_content`],
//!   [`element_call::parse_key_message`]) is *permissive and always on*. Every
//!   rule is of the form "the modern field is absent and the legacy one is
//!   present", so a spec-shaped event passes through byte-identical and no flag
//!   is needed to protect it. Being always on is deliberate: a legacy path that
//!   only compiles under a flag is a legacy path that rots.
//! - **Outbound** ([`element_call::ElementCallDialect`]) is *opt-in*, because it
//!   is the only half that changes what other clients see. It is switched on per
//!   call via `CallOptions::legacy_element_call`.
//!
//! # Call sites
//!
//! 1. `matrix_bridge::snapshot` — normalises inbound `m.rtc.member` content.
//! 2. `matrix_bridge::SdkCommandSender` — applies the outbound dialect.
//! 3. `call::register_legacy_key_receiver` — ingests legacy to-device keys.

pub mod element_call;

pub use element_call::{
    ElementCallDialect, LEGACY_KEY_EVENT_TYPE, LegacyKeyMessage, MemberContent,
};
