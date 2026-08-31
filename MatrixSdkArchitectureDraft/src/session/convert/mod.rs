//! Per-event-type converters to the internal candidate model — the *only*
//! place wire formats are known. One file per generation: deleting a legacy
//! generation is deleting its file plus one arm of the session's internal
//! event dispatch.

pub(crate) mod msc3401;
pub(crate) mod msc4143;

pub use msc3401::LEGACY_SLOT_ID;

use crate::types::{EventOrigin, Member, MemberTransports};

/// Which generation a candidate was converted from. The join-condition
/// projection is generation-agnostic except for the two scoped conditions
/// (see [`JoinExclusionReason`](super::JoinExclusionReason)).
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CandidateSource {
    Msc4143,
    Msc3401State,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CandidateMembership {
    Join,
    Leave,
}

/// The normalized candidate every converter produces — no synthetic wire
/// events, straight to the Member representation plus validity metadata.
#[derive(Clone, Debug)]
pub(crate) struct MemberCandidate {
    pub member: Member,
    pub source: CandidateSource,
    pub membership: CandidateMembership,
    /// Decryption metadata. For MSC3401 candidates the device is *claimed*
    /// (parsed from state-key/content), never verified — key verification
    /// must treat it accordingly.
    pub origin: EventOrigin,
    /// One expiry field, two wire sources: the sticky duration (MSC4354) or
    /// the MSC3401 `expires` timestamp.
    pub expires_at: Option<u64>,
    pub transports: MemberTransports,
}
