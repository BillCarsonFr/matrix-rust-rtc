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
//! speaks a pre-2026 wire format. Once it catches up, delete this directory, the
//! call sites listed below, and `matrix_rtc_livekit::CallOptions::element_call_compat`.
//! Nothing else should ever grow a dependency on it.
//!
//! # Two generations, not one
//!
//! There are two, and they disagree about more than field names:
//!
//! - [`element_call`] — Element Call as of 2025. Already MSC4354 sticky-based;
//!   only the fields inside the member content differ.
//! - [`element_call_state`] — Element Call before MSC4354, when membership was
//!   `org.matrix.msc3401.call.member` **room state**. A different carrier, a
//!   different SFU participant identity, and a different token endpoint.
//!
//! [`ElementCallCompat`] selects between them and current MSC4143. It is an enum
//! rather than a pair of flags because the generations are mutually exclusive by
//! construction: a membership is a sticky event or a state event, never both.
//!
//! # Why it is shaped this way
//!
//! The rest of the stack speaks current MSC4143 and nothing else — no dialect
//! parameter reaches `matrix-rtc-core`, no legacy field appears in a domain
//! type. Compatibility is confined to JSON funnels at the very edge:
//!
//! - **Inbound for the sticky dialect**
//!   ([`element_call::normalize_member_content`],
//!   [`element_call::parse_key_message`]) is *permissive and always on*. Every
//!   rule is of the form "the modern field is absent and the legacy one is
//!   present", so a spec-shaped event passes through byte-identical and no flag
//!   is needed to protect it. Being always on is deliberate: a legacy path that
//!   only compiles under a flag is a legacy path that rots.
//! - **Everything about the state dialect** is *opt-in*, in both directions.
//!   Reading it is not normalisation — it is a second membership source, in a
//!   different part of the room, with a lifetime the content states rather than
//!   the homeserver enforcing. See [`element_call_state`] for why that cannot be
//!   always-on the way its sibling is.
//! - **Outbound** ([`OutboundDialect`]) is opt-in for both, because it is the
//!   half that changes what other clients see.
//!
//! # What this module cannot own
//!
//! Three things about the state dialect refuse to be JSON, and are named here so
//! the boundary stays deliberate rather than eroding: the choice of ruma request
//! type (a membership is `PUT .../state/...` rather than a sticky send), the
//! choice of token endpoint, and the participant-identity derivation. Each is one
//! `match` on [`ElementCallCompat`] or [`OutboundDialect`] at the call site.
//! [`MemberEventRoute`] exists precisely to keep the *decision* here while the
//! *performing* stays in the `sdk` module.
//!
//! The identity derivation is the one that also refuses to be *Matrix*: it
//! hashes per MSC4195, which is a LiveKit document. So it lives wholly in the
//! transport crate — `matrix_rtc_livekit::identity_mapper` matches on
//! [`ElementCallCompat`] there rather than this module reaching for a hash it has
//! no business knowing.
//!
//! # Call sites
//!
//! In this crate:
//!
//! 1. `sdk::snapshot` — normalises inbound `m.rtc.member` content.
//! 2. `sdk::SdkCommandSender` — routes and rewrites outbound events.
//! 3. `sdk::element_call_state_snapshot` — reads inbound state membership.
//! 4. `sdk::run_sticky_bridge` — the room-state wake source a state-carried
//!    membership needs.
//!
//! In `matrix-rtc-livekit`:
//!
//! 5. `call::register_legacy_key_receiver` — ingests legacy to-device keys.
//! 6. `call::Call::join` — mode selection and the member id.
//! 7. `identity_mapper` — the participant-identity derivation (see above).
//! 8. `transport_impl` + `token` — `/sfu/get`.
//!
//! In `matrix-rtc-ffi`, which reaches the same dialects from a host that owns its
//! own Matrix stack (see that crate's `compat` module):
//!
//! 9. `compat` — the FFI-shaped mode, the raw-JSON ingestion, and the two
//!    identifiers the mode changes.
//! 10. `commands::FfiCommandSender` — routes and rewrites outbound events, per
//!     room.
//! 11. `RtcSessionManagerHandle::{join, leave, set_current_membership,
//!     receive_legacy_encryption_key}` — mode selection, and both inbound halves.
//! 12. `media::session` — the identity derivation and token endpoint again.

use serde_json::Value;

pub mod element_call;
pub mod element_call_state;

pub use element_call::{
    ElementCallDialect, LEGACY_KEY_EVENT_TYPE, LegacyKeyMessage, MemberContent,
};
pub use element_call_state::{
    ElementCallStateDialect, STATE_MEMBER_EVENT_TYPE, StateMemberEvent, StateMembership,
};

/// Which MatrixRTC generation a call renders itself for, and reads.
///
/// Mutually exclusive by construction, because the two legacy generations
/// disagree about the *carrier* of a membership, not just its fields. Reading the
/// 2025 sticky dialect needs no flag and is always on regardless of this setting;
/// see the module docs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ElementCallCompat {
    /// Current MSC4143 + MSC4354 only. The default, and the only mode that
    /// interoperates with spec-current peers.
    #[default]
    Off,
    /// Element Call as of 2025: MSC4354 sticky events carrying the pre-2026 field
    /// names alongside the spec ones.
    ///
    /// Joins stay MSC4143-valid — the legacy fields ride alongside — so one event
    /// serves both dialects. Leaves and media keys cannot be additive: a leave
    /// becomes the legacy bare-sticky-key content, and keys go out as
    /// `io.element.call.encryption_keys` *instead of* the spec type, since a
    /// to-device message has only one type.
    StickyEvents,
    /// Element Call before MSC4354: membership as
    /// `org.matrix.msc3401.call.member` **room state**, plain
    /// `{user}:{device}` SFU identities, and the pre-MSC4195 `/sfu/get` token
    /// endpoint.
    ///
    /// Nothing about this mode is additive — a call joined this way is visible to
    /// that generation of Element Call and to nobody else. It exists for interop
    /// testing.
    StateEvents,
}

impl ElementCallCompat {
    /// Whether membership is carried as room state in this mode.
    pub fn reads_state_membership(self) -> bool {
        matches!(self, Self::StateEvents)
    }
}

/// Where a member event goes on the wire, and in what shape.
///
/// The dialect decides; `sdk` performs. That split is what keeps every
/// legacy byte inside this module while the ruma request types stay out of it.
#[derive(Clone, Debug)]
pub enum MemberEventRoute {
    /// An MSC4354 sticky timeline event — the only carrier the spec knows, and
    /// the one both [`ElementCallCompat::Off`] and
    /// [`ElementCallCompat::StickyEvents`] take.
    Sticky { event_type: String, content: Value },
    /// A room state event, which is how MatrixRTC membership worked before
    /// MSC4354. The core's sticky `duration_ms` is meaningless here: room state
    /// has no TTL, and the lifetime is stated inside the content instead.
    State {
        event_type: &'static str,
        state_key: String,
        content: Value,
    },
}

/// The outbound dialect a command sender speaks: exactly one generation at a
/// time.
#[derive(Clone, Debug)]
pub enum OutboundDialect {
    /// Current MSC4143 + MSC4354, unmodified.
    None,
    /// Element Call as of 2025.
    Sticky(ElementCallDialect),
    /// Element Call before MSC4354.
    State(ElementCallStateDialect),
}

impl OutboundDialect {
    /// Decide where an outbound event goes and what it says.
    ///
    /// Anything that is not a membership passes through as a sticky event
    /// untouched, in every mode — the legacy generations differ about
    /// memberships, not about everything.
    pub fn route_member_event(&self, event_type: String, content: Value) -> MemberEventRoute {
        if !ElementCallDialect::is_member_event(&event_type) {
            return MemberEventRoute::Sticky {
                event_type,
                content,
            };
        }

        match self {
            Self::None => MemberEventRoute::Sticky {
                event_type,
                content,
            },
            Self::Sticky(dialect) => {
                let mut content = content;
                dialect.rewrite_member_content(&mut content);
                MemberEventRoute::Sticky {
                    event_type,
                    content,
                }
            }
            Self::State(dialect) => MemberEventRoute::State {
                event_type: STATE_MEMBER_EVENT_TYPE,
                state_key: dialect.state_key(),
                content: dialect.member_content(&content),
            },
        }
    }

    /// Render an outbound MSC4075 notification's content for the target
    /// generation.
    ///
    /// Returns the content unchanged for anything that is not a notification and
    /// for [`OutboundDialect::None`]. The event type never moves — both
    /// generations already agree on it — so only the content is passed.
    ///
    /// Both legacy generations read the same flat shape, so both delegate to the
    /// one implementation: the pre-sticky one never sends notifications itself,
    /// but a client of that vintage still parses this shape and there is no
    /// other for it to read.
    pub fn rewrite_notification(&self, event_type: &str, content: Value) -> Value {
        let flattened = match self {
            Self::None => None,
            Self::Sticky(_) | Self::State(_) => {
                element_call::flatten_notification_content(event_type, &content)
            }
        };
        flattened.unwrap_or(content)
    }

    /// Translate an outbound media key into the legacy type and shape, or `None`
    /// to send it untouched.
    ///
    /// Both legacy generations use the same to-device type and the same content,
    /// so both arms delegate to the one implementation. The state dialect gets
    /// the right `member.id` for free: in that mode the core's member id already
    /// *is* the legacy `membershipID`.
    pub fn rewrite_key_message(
        &self,
        message_type: &str,
        content: &Value,
    ) -> Option<(String, Value)> {
        match self {
            Self::None => None,
            Self::Sticky(dialect) => dialect.rewrite_key_message(message_type, content),
            Self::State(dialect) => element_call::rewrite_key_message(
                dialect.own_device_id(),
                dialect.slot_id(),
                message_type,
                content,
            ),
        }
    }
}
