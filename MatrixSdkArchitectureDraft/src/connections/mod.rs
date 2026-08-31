//! Maps a [`Session`] to the SFU connections a host must hold
//! (multi-focus, MSC4195). `ws_url` is the connection index.

use crate::driver::{DriverError, TokenDriver};
use crate::session::{Session, SessionSnapshot};
use crate::types::{Member, TransportIntent};
use std::sync::Arc;
use tokio::sync::watch;

/// Everything a host needs to open one LiveKit room connection.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionData {
    pub jwt_token: String,
    pub ws_url: String,
}

#[derive(Clone, Debug)]
pub struct ConnectionWithMembers {
    pub connection: ConnectionData,
    pub members: Vec<Member>,
}

/// The members reachable through `connection` (grouped by the published
/// transports' connection key — for LiveKit, `livekit_service_url`).
pub fn members_for_connection_data(connection: &ConnectionData, session: &Session) -> Vec<Member> {
    todo!()
}

/// The MSC4195 pseudonymous participant identity inside an LK room — what
/// `SessionMembership::transport_identity` carries.
pub fn participant_identity(user_id: &str, device_id: &str, member_id: &str) -> String {
    todo!()
}

/// The pre-MSC4195 plain `{user}:{device}` identity
/// (`ElementCallCompat::StateEvents` generation). Delete with it.
pub fn legacy_participant_identity(user_id: &str, device_id: &str) -> String {
    todo!()
}

/// Tracks which connections the session requires and mints/reuses tokens
/// for our own transport (including refresh on expiry).
pub struct ConnectionsManager {
    session: watch::Receiver<SessionSnapshot>,
    driver: Arc<dyn TokenDriver>,
}

impl ConnectionsManager {
    pub fn new(session: watch::Receiver<SessionSnapshot>, driver: Arc<dyn TokenDriver>) -> Self {
        todo!()
    }

    pub fn subscribe_connections(&self) -> watch::Receiver<Vec<ConnectionData>> {
        todo!()
    }

    pub fn subscribe_connections_with_members(
        &self,
    ) -> watch::Receiver<Vec<ConnectionWithMembers>> {
        todo!()
    }

    pub fn connections(&self) -> Vec<ConnectionWithMembers> {
        todo!()
    }

    /// Resolve the `ConnectionData` for publishing the local member:
    /// discovers transports (`GET /rtc/transports`) when the intent does not
    /// name one, performs the MSC4195 `get_token` exchange — or returns an
    /// already-valid token. (In `StateEvents` compat mode the legacy
    /// `/sfu/get` endpoint replaces `get_token`.)
    pub async fn add_own_transport(
        &self,
        intent: TransportIntent,
    ) -> Result<ConnectionData, DriverError> {
        todo!()
    }
}
