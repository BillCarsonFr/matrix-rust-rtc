//! Pure projection of Matrix events into session state.
//!
//! The session is the **single converter** from raw Matrix events to the
//! [`Member`] representation. Every event type is converted in place, in
//! [`convert`] (one file per generation):
//!
//! | event type | converted to |
//! |---|---|
//! | `m.rtc.member` (sticky; modern + 2025 dialect) | member candidate |
//! | `org.matrix.msc3401.call.member` (state; only with `StateEvents` compat) | member candidates, one per device |
//! | `m.rtc.slot` (state) | slot condition |
//! | `m.room.member` (state) | room-membership condition |
//! | `m.room.encryption` (state) | encryption condition |
//!
//! Unknown types are ignored; room-state conditions stay unenforced until
//! the first event of their type arrives.
//!
//! Two entry points share that logic:
//!
//! - [`Session`] — **live**: constructed with the [`RoomEventsDriver`]
//!   slice, it seeds itself (`read_state` / `read_events`) and consumes the
//!   driver's streams; consumers read it only through [`SessionSnapshot`]s
//!   (current value or `watch` subscription).
//! - [`compute_sessions_from_events`] — **static**: a pure function over
//!   already-synced events returning plain [`SessionSnapshot`]s. Nothing
//!   subscribes, nothing updates — you need to pass a driver (i.e. build a
//!   `Session`) for that.

pub(crate) mod convert;
pub use convert::LEGACY_SLOT_ID;

use crate::driver::RoomEventsDriver;
use crate::types::{Member, RawMatrixEvent, RtcTransport};
use std::sync::Arc;
use tokio::sync::watch;

/// Pre-2026 Element Call interop, selected per call.
///
/// Consumed in two places: the session's *read* side (`StateEvents` enables
/// the MSC3401 converter; the permissive 2025-dialect read is always on) and
/// `own_membership`'s *write* side (the dialect our own events are rendered
/// in — the opt-in half, since it changes what other clients see).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum ElementCallCompat {
    #[default]
    Off,
    /// 2025 format: already MSC4354-sticky, different member-content fields.
    StickyEvents,
    /// Pre-MSC4354: `org.matrix.msc3401.call.member` room state,
    /// `{user}:{device}` identities, `/sfu/get` tokens. Not additive — such
    /// a call is visible to that generation and nobody else.
    StateEvents,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SessionConfig {
    pub compat: ElementCallCompat,
}

/// Resolved `m.rtc.slot` state, per MSC4143: open requires `status: "open"`
/// plus an application whose type agrees with the state key; anything else
/// (missing application, unknown status, empty content) is closed.
#[derive(Clone, Debug, PartialEq)]
pub enum SlotState {
    Open(OpenSlot),
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OpenSlot {
    pub application_type: String,
    /// What the slot declared.
    pub encryption: Option<SlotEncryption>,
    /// What was negotiated against the room's encryption state (`None` when
    /// the declaration was dropped, e.g. mechanism declared in an
    /// unencrypted room).
    pub mechanism: Option<EncryptionMechanism>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SlotEncryption {
    pub mechanism_type: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EncryptionMechanism {
    /// `m.per_member` — the only implemented mechanism.
    PerMember,
    /// Closes the slot in an encrypted room (we cannot take part compliantly).
    Unsupported(String),
}

/// Why a candidate member event is not in the joined projection.
///
/// Two conditions are scoped by the candidate's generation:
/// `SlotClosed` never applies to MSC3401 candidates (that generation has no
/// slot concept), and `UnencryptedInEncryptedRoom` never applies to them
/// either (state events are unencrypted by nature; MSC4143 requires
/// encryption only of *sticky* member events).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum JoinExclusionReason {
    SlotClosed,
    UnencryptedInEncryptedRoom,
    SenderNotInRoom,
    Expired,
}

/// The one read surface of a session — a plain, cloneable value carrying
/// the joined projection plus its metadata and conveniences. Produced live
/// by [`Session`] and statically by [`compute_sessions_from_events`]; the
/// matrix-rust-sdk `room_info` computation populates its fields from these
/// on every room update.
#[derive(Clone, Debug, Default)]
pub struct SessionSnapshot {
    pub room_id: String,
    pub slot_id: String,
    /// The joined projection.
    pub members: Vec<Member>,
    /// Union of all members' published transports.
    pub transports: Vec<RtcTransport>,
    /// Candidates excluded from the projection, with reasons — the
    /// load-bearing diagnostics (a member silently vanishing is the hardest
    /// failure to debug from outside).
    pub excluded_candidates: Vec<(Member, JoinExclusionReason)>,
    /// `None` while no slot state was supplied (condition unenforced).
    pub slot_state: Option<SlotState>,
    /// The slot-prescribed encryption decision (overrides local config at
    /// join time); `None` while unknown.
    pub negotiated_encryption: Option<bool>,
    /// `origin_server_ts` of the earliest joined membership, while active.
    pub start_ts: Option<u64>,
    /// From the open slot (or the memberships' application).
    pub application_type: Option<String>,
}

impl SessionSnapshot {
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub fn is_active(&self) -> bool {
        !self.members.is_empty()
    }

}

/// A **live** single-`(room_id, slot_id)` RTC session: constructed with the
/// [`RoomEventsDriver`] slice, it seeds itself from `read_state` /
/// `read_events` and then consumes the driver's live streams. All reads go
/// through [`SessionSnapshot`].
pub struct Session {
    room_id: String,
    slot_id: String,
    config: SessionConfig,
    driver: Arc<dyn RoomEventsDriver>,
    snapshot_tx: watch::Sender<SessionSnapshot>,
}

impl Session {
    /// Subscribes to the driver's room-event and state-update streams and
    /// seeds the initial state. Conversion runs in an internal pump (a task
    /// natively; on wasm driven by the sink emissions — an `emit` is fully
    /// processed before it returns, which the web acceptance tests rely on).
    pub fn new(
        room_id: String,
        slot_id: String,
        driver: Arc<dyn RoomEventsDriver>,
        config: SessionConfig,
    ) -> Self {
        todo!()
    }

    /// The current value.
    pub fn snapshot(&self) -> SessionSnapshot {
        todo!()
    }

    /// Reactive stream: current value + every change.
    pub fn subscribe(&self) -> watch::Receiver<SessionSnapshot> {
        todo!()
    }

    /// Per-candidate verdicts and current state as JSON, for bug reports.
    pub fn debug_snapshot(&self) -> serde_json::Value {
        todo!()
    }
}

/// Static, cheap session computation for room-list / room-header info —
/// call it on every room change, no manager, no driver, nothing live.
/// Takes *all* of a room's relevant events in one slice (sticky and state;
/// the same dispatch a live [`Session`] runs) and groups by
/// `(room_id, slot_id)` — MSC3401 candidates land in [`LEGACY_SLOT_ID`].
/// Returns plain [`SessionSnapshot`]s: values, not subscriptions.
pub fn compute_sessions_from_events(
    events: &[RawMatrixEvent],
    config: &SessionConfig,
) -> Vec<SessionSnapshot> {
    todo!()
}

// room_info
// is computed from required state
// and in the future it will also have sticky events
