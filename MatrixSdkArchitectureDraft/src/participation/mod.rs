//! The facade a calling host uses: owns and wires the session, the
//! own-membership machine, the connections manager, and the encryption
//! machine. Hosts consume four outputs, each as a getter + change-callback
//! pair:
//!
//! - **memberships** — the UI-shaped view: one tile per [`SessionMembership`].
//! - **connections** — the transport-shaped view: which LK rooms to hold.
//! - **key map** — what to feed into frame encryption.
//! - **status** — join/connected/leave progress.
//!
//! Memberships and connections are deliberately redundant (both derive from
//! the same session state): render tiles from the former, then use the
//! latter to acquire each tile's media — `membership.connections` names the
//! LK room (`ws_url`), `membership.transport_identity` the participant in it.

use crate::connections::{ConnectionWithMembers, ConnectionsManager};
use crate::driver::MatrixDriver;
use crate::encryption::{self, KeyMap, KeyMapCallback};
use crate::own_membership::{
    self, JoinError, JoinParams, LeaveError, OwnMembershipManager,
};
use crate::session::{Session, SessionConfig};
use crate::types::{LeaveReason, Member, TransportIntent};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct JoinStatus {
    pub own_membership: own_membership::JoinStatus,
    /// `encryption::Status::Joining` while keys are still being exchanged;
    /// `None` for unencrypted calls (no machine).
    pub encryption: Option<encryption::Status>,
}

#[derive(Clone, Debug)]
pub struct ConnectedStatus {
    pub own_membership: own_membership::ConnectedStatus,
    /// `encryption::Status::Connected`; `None` for unencrypted calls.
    pub encryption: Option<encryption::Status>,
}

#[derive(Clone, Debug)]
pub enum Status {
    Disconnected,
    Joining(JoinStatus),
    Connected(ConnectedStatus),
    Leaving(own_membership::LeaveStatus),
}

/// Lifecycle of one membership tile.
#[derive(Clone, Debug, PartialEq)]
pub enum MembershipState {
    /// In the session's joined projection.
    Joined,
    /// No longer joined (leave/expired sticky event), but still holding a
    /// not-yet-rotated copy of our media key — kept in the list so the UI
    /// can render "leaving / may still be listening" until the rotation
    /// settles. Only occurs in key-managed (encrypted) calls; entries drop
    /// out once `encryption` reports them keyless.
    LeftWithKeys,
}

/// One entry of the membership list — everything a host needs to render a
/// tile and later attach its media.
#[derive(Clone, Debug)]
pub struct SessionMembership {
    pub member: Member,
    pub state: MembershipState,
    /// Connection keys (`ws_url`s) of the connections this member publishes
    /// on — the LK room(s) carrying their media. Empty for receive-only
    /// members and for `LeftWithKeys` entries.
    pub connections: Vec<String>,
    /// The participant identity inside those LK rooms (MSC4195 pseudonymous
    /// hash; plain `{user}:{device}` in legacy compat mode). `None` when it
    /// cannot be derived yet.
    pub transport_identity: Option<String>,
}

pub type MembershipsCallback = Box<dyn Fn(&[SessionMembership]) + Send + Sync>;
pub type ConnectionsCallback = Box<dyn Fn(&[ConnectionWithMembers]) + Send + Sync>;
pub type StatusCallback = Box<dyn Fn(&Status) + Send + Sync>;

pub struct ParticipationManager {
    session: Session,
    own_membership: OwnMembershipManager,
    connections: ConnectionsManager,
    /// One `encryption::Machine` per participation, `None` while not joined
    /// (and for unencrypted calls). Ordering in `join()` matters:
    ///
    /// 1. generate the fresh `member_id` (`types::generate_member_id`);
    /// 2. construct the encryption machine with that member **before** the
    ///    own-membership machine sends the join event — it starts distributing
    ///    our key at once, so peers hold it by the time our member event lands
    ///    (they buffer keys that arrive early), and it is already subscribed
    ///    to the to-device stream when their keys come back;
    /// 3. then `own_membership.join(..)`.
    ///
    /// `leave()` drops it (after or before the leave event, either is fine):
    /// dropping forgets every key and stops the pump. There is no
    /// `join`/`leave` on the machine itself; a rejoin is a new machine.
    encryption: Option<encryption::Machine>,
}

impl ParticipationManager {
    /// Hands the session its `RoomEventsDriver` slice (the session seeds
    /// and feeds itself) and routes the driver's to-device stream into the
    /// encryption machine — no other event routing happens here. `config`
    /// selects the per-call compat mode (session read side, own-membership
    /// write side).
    pub fn new(
        room_id: String,
        slot_id: String,
        user_id: String,
        device_id: String,
        driver: Arc<dyn MatrixDriver>,
        config: SessionConfig,
    ) -> Self {
        todo!()
    }

    pub async fn join(
        &self,
        intent: TransportIntent,
        params: JoinParams,
    ) -> Result<(), JoinError> {
        todo!()
    }

    pub async fn leave(&self, reason: Option<LeaveReason>) -> Result<(), LeaveError> {
        todo!()
    }

    /// The current membership list: the session's joined projection, plus
    /// left members that still hold our keys ([`MembershipState`]).
    pub fn memberships(&self) -> Vec<SessionMembership> {
        todo!()
    }

    pub fn on_memberships_change(&self, callback: MembershipsCallback) {
        todo!()
    }

    pub fn connections(&self) -> Vec<ConnectionWithMembers> {
        todo!()
    }

    pub fn on_connections_change(&self, callback: ConnectionsCallback) {
        todo!()
    }

    pub fn key_map(&self) -> KeyMap {
        todo!()
    }

    pub fn on_key_map_change(&self, callback: KeyMapCallback) {
        todo!()
    }

    pub fn status(&self) -> Status {
        todo!()
    }

    pub fn on_status_change(&self, callback: StatusCallback) {
        todo!()
    }

    /// Diagnostics: current state + per-candidate join verdicts as JSON.
    pub fn debug_snapshot(&self) -> serde_json::Value {
        todo!()
    }
}
