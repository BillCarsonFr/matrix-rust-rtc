//! Legacy `org.matrix.msc3401.call.member` room-state memberships ->
//! candidates. Only runs with `ElementCallCompat::StateEvents`. Delete-by
//! date: this whole file goes with that generation.

use super::MemberCandidate;
use crate::types::RawMatrixEvent;

/// MSC3401 has no slot concept; its candidates land in this well-known slot
/// id (the MSC3401 room call is the one with the empty call id).
pub const LEGACY_SLOT_ID: &str = "";

/// Convert one state membership. One event carries the memberships of every
/// device of a user, so one event yields many candidates; devices are
/// *claimed* by content, and the per-device `expires` timestamp becomes the
/// candidate expiry.
pub(crate) fn member_candidates(event: &RawMatrixEvent) -> Vec<MemberCandidate> {
    todo!()
}
