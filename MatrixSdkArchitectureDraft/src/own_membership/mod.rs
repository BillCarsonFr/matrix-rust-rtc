//! The join/leave state machine for *our* membership.
//!
//! Talks only to the [`OwnMembershipDriver`] slice of the driver. Behavior
//! (kept from the current implementation): fresh member id per join, delayed
//! leave armed *before* the join event, keep-alive via MSC4140 `restart`
//! plus sticky re-send before `duration_ms` elapses, optional delegation of
//! the delayed leave to the SFU.

use crate::driver::{DriverError, OwnMembershipDriver};
use crate::session::{ElementCallCompat, SessionSnapshot};
use crate::types::{LeaveReason, RtcTransport, TransportIntent};
use std::sync::Arc;
use tokio::sync::watch;

/// Join progress, step by step (drives `participation::Status::Joining`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct JoinStatus {
    pub has_fetched_transports: bool,
    pub has_fetched_initial_member_list: bool,
    pub has_created_transport_token: bool,
    pub has_sent_delayed_leave_event: bool,
    pub has_sent_member_join_event: bool,
    pub has_delegated_delayed_event: bool,
    pub has_started_heartbeat: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ConnectedStatus {
    /// When the delayed leave would fire if we stopped restarting it.
    pub delayed_event_kick_ts: Option<u64>,
    pub heartbeat_last_restart_ts: Option<u64>,
    pub delegation_setup_ts: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LeaveStatus {
    pub transport_disconnected: bool,
    pub leave_event_sent: bool,
}

#[derive(Clone, Debug)]
pub struct JoinParams {
    pub application_type: String,
    /// Sticky lifetime; the machine re-sends the membership before it elapses.
    pub sticky_duration_ms: u64,
    /// Delay of the dead man's switch leave event.
    pub keep_alive_timeout_ms: u64,
    /// Hand the delayed leave to the SFU (MSC4195) once connected.
    pub delegate_delayed_leave: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    #[error("already joined")]
    AlreadyJoined,
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
    #[error(transparent)]
    Driver(#[from] DriverError),
}

#[derive(Debug, thiserror::Error)]
pub enum LeaveError {
    #[error("not joined")]
    NotJoined,
    #[error(transparent)]
    Driver(#[from] DriverError),
}

/// Called once the published transport is final (after token creation), so
/// the connections manager can index it.
pub type TransportCreatedCallback = Box<dyn Fn(&RtcTransport) + Send + Sync>;

pub struct OwnMembershipManager {
    session: watch::Receiver<SessionSnapshot>,
    driver: Arc<dyn OwnMembershipDriver>,
    on_transport_created: TransportCreatedCallback,
    /// Write-side compat: which dialect *our own* events are rendered in
    /// (the opt-in half — it changes what other clients see). The read side
    /// lives in the session's converters.
    compat: ElementCallCompat,
}

impl OwnMembershipManager {
    /// The driver is passed only as the trait slice, so the manager cannot
    /// reach more Matrix surface than it needs.
    pub fn new(
        session: watch::Receiver<SessionSnapshot>,
        driver: Arc<dyn OwnMembershipDriver>,
        compat: ElementCallCompat,
        on_transport_created: TransportCreatedCallback,
    ) -> Self {
        todo!()
    }

    /// Run the join sequence (see [`JoinStatus`] for the steps).
    pub async fn join(
        &self,
        intent: TransportIntent,
        params: JoinParams,
    ) -> Result<(), JoinError> {
        todo!()
    }

    /// Send the leave sticky event (with `leave_reason`) and cancel/settle
    /// the delayed event.
    pub async fn leave(&self, reason: Option<LeaveReason>) -> Result<(), LeaveError> {
        todo!()
    }

    /// One keep-alive tick: MSC4140 `restart` + sticky re-send when due.
    pub async fn heartbeat(&self) {
        todo!()
    }

    /// The member id of the current join, if any.
    pub fn member_id(&self) -> Option<String> {
        todo!()
    }

    pub fn join_status(&self) -> JoinStatus {
        todo!()
    }

    pub fn connected_status(&self) -> ConnectedStatus {
        todo!()
    }

    pub fn leave_status(&self) -> LeaveStatus {
        todo!()
    }

    /// Slot administration (usually requires elevated power levels).
    pub async fn open_slot(
        &self,
        room_id: String,
        slot_id: String,
        application_type: String,
    ) -> Result<(), DriverError> {
        todo!()
    }

    pub async fn close_slot(&self, room_id: String, slot_id: String) -> Result<(), DriverError> {
        todo!()
    }
}

/// Render our member content in the 2025 sticky dialect
/// (`ElementCallCompat::StickyEvents` write side). Delete with that
/// generation.
fn sticky_member_content_2025(content: &serde_json::Value) -> serde_json::Value {
    todo!()
}

/// Render our membership as an MSC3401 state event
/// (`ElementCallCompat::StateEvents` write side). Delete with that
/// generation.
fn state_member_content_msc3401(content: &serde_json::Value) -> serde_json::Value {
    todo!()
}
