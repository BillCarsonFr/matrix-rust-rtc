//! Per-event-type converters to the internal candidate model — the *only*
//! place wire formats are known. One file per generation: deleting a legacy
//! generation is deleting its file plus one arm of the session's internal
//! event dispatch.

pub(crate) mod msc3401;
pub(crate) mod msc4143;

pub use msc3401::LEGACY_SLOT_ID;

use crate::types::{EventOrigin, LeaveReason, Member};
use serde_json::Value;

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
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MemberCandidate {
    pub member: Member,
    pub source: CandidateSource,
    pub membership: CandidateMembership,
    /// Decryption metadata. For MSC3401 candidates the device is *claimed*
    /// (parsed from state-key/content), never verified — key verification
    /// must treat it accordingly.
    pub origin: EventOrigin,
    /// One expiry field, two wire sources: the MSC4354 sticky `end_time`
    /// (computed at ingest from the duration and the receive time) or the
    /// MSC3401 `joined_at + expires`. `None` means the event carried no
    /// lifetime at all; the sticky map refuses such candidates.
    pub expires_at: Option<u64>,
    /// The slot the candidate belongs to ([`LEGACY_SLOT_ID`] for MSC3401).
    pub slot_id: String,
    /// The event's `origin_server_ts` — `start_ts` is computed from it.
    pub origin_server_ts: u64,
    /// MSC4143 `leave_reason`, on a leave.
    pub leave_reason: Option<LeaveReason>,
    /// MSC3401-only details the projection needs for focus resolution and
    /// diagnostics. `None` for MSC4143 candidates.
    pub legacy: Option<LegacyDetails>,
}

/// What the MSC3401 converter has to carry into the projection because that
/// generation resolves transports *across* members (see `msc3401.rs`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LegacyDetails {
    /// The dialect's own session name, `"{application}#{call_id or ROOM}"`.
    /// Diagnostics only — it tells several legacy call ids apart in
    /// `excluded_candidates` and the debug JSON; the session key is
    /// [`LEGACY_SLOT_ID`].
    pub call_id: String,
    /// The event's `state_key`: the `RoomState.legacy` key, the focus
    /// tie-break, and what appears in log lines.
    pub state_key: String,
    /// `min(created_ts, origin_server_ts)` — the `oldest_membership`
    /// ordering key and the expiry base.
    pub joined_at: u64,
    /// `foci_preferred[0]`, verbatim.
    pub own_focus: Option<Value>,
    /// `focus_active.focus_selection == "multi_sfu"`.
    pub prefers_own_focus: bool,
}

impl MemberCandidate {
    pub(crate) fn is_join(&self) -> bool {
        self.membership == CandidateMembership::Join
    }
}
