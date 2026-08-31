//! `m.rtc.member` sticky events (stable and unstable types) -> candidates.

use super::MemberCandidate;
use crate::types::RawMatrixEvent;

/// Convert one sticky member event. The permissive 2025-dialect read is
/// folded in and always on: absent modern fields are filled from the
/// dialect's, spec-shaped events pass through untouched. `None` for content
/// that is no membership at all.
pub(crate) fn member_candidate(event: &RawMatrixEvent) -> Option<MemberCandidate> {
    todo!()
}

/// The 2025-dialect field mapping — delete with that generation.
fn fill_from_2025_dialect(content: &mut serde_json::Value) {
    todo!()
}
