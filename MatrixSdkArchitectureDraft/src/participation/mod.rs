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
//! LK room (`service_url`), `membership.transport_identity` the participant.
//!
//! Getters compute from fresh inputs (`Session::snapshot()` is
//! drain-on-read), so a read right after a driver `emit` is current on every
//! platform; the change callbacks fire from one pump task a tick later.

use crate::connections::{self, ConnectionWithMembers, ConnectionsManager};
use crate::driver::{DriverError, MatrixDriver, OwnMembershipDriver};
use crate::encryption::{
    self, EncryptionConfig, KeyMap, KeyMapCallback, MediaKeyChange, SendMachineConfig,
};
use crate::executor;
pub use crate::own_membership::OwnIdentity;
use crate::own_membership::{
    self, JoinError, JoinParams, LeaveError, OwnMembershipManager, new_member_id,
};
use crate::session::{ElementCallCompat, Session, SessionConfig};
use crate::types::{
    DeviceAttribution, LeaveReason, Member, MemberTransports, TransportIntent, wire_event_type,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::Notify;

/// How long `join` waits for the session to finish seeding before joining
/// anyway (a driver whose `read_*` never answer must not block the call).
const SEED_WAIT_MS: u64 = 5_000;

#[derive(Clone, Debug, Default)]
pub struct ParticipationConfig {
    /// Per-call compat mode (session read side, own-membership write side,
    /// legacy token endpoint and identity).
    pub compat: ElementCallCompat,
    pub encryption: EncryptionConfig,
    pub rotation: SendMachineConfig,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinStatus {
    pub own_membership: own_membership::JoinStatus,
    /// `encryption::Status::Joining` while keys are still being exchanged;
    /// `Connected` from the start for unencrypted calls.
    pub encryption: encryption::Status,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectedStatus {
    pub own_membership: own_membership::ConnectedStatus,
    pub encryption: encryption::Status,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    Disconnected,
    Joining(JoinStatus),
    Connected(ConnectedStatus),
    Leaving(own_membership::LeaveStatus),
}

/// Lifecycle of one membership tile.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MembershipState {
    /// In the session's joined projection.
    Joined,
    /// No longer joined (leave/expired sticky event), but still holding a
    /// not-yet-rotated copy of our media key — kept in the list so the UI
    /// can render "leaving / may still be listening" until the rotation
    /// settles. Only occurs in key-managed (encrypted) calls.
    LeftWithKeys,
}

/// One entry of the membership list — everything a host needs to render a
/// tile and later attach its media.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionMembership {
    pub member: Member,
    pub state: MembershipState,
    /// Connection keys (`livekit_service_url`s) this member publishes on —
    /// the LK room(s) carrying their media. Empty for receive-only members
    /// and for `LeftWithKeys` entries.
    pub connections: Vec<String>,
    /// The participant identity inside those LK rooms (MSC4195 pseudonymous
    /// hash; plain `{user}:{device}` in legacy compat mode). `None` when the
    /// member has no device.
    pub transport_identity: Option<String>,
}

pub type MembershipsCallback = Box<dyn Fn(&[SessionMembership]) + Send + Sync>;
pub type ConnectionsCallback = Box<dyn Fn(&[ConnectionWithMembers]) + Send + Sync>;
pub type StatusCallback = Box<dyn Fn(&Status) + Send + Sync>;

#[derive(Default)]
struct Callbacks {
    memberships: Option<MembershipsCallback>,
    connections: Option<ConnectionsCallback>,
    // TODO rename to encryption key map
    key_map: Option<KeyMapCallback>,
    status: Option<StatusCallback>,
}

/// The last values the callbacks saw (publish-on-change).
#[derive(Default)]
struct Published {
    memberships: Vec<SessionMembership>,
    connections: Vec<ConnectionWithMembers>,
    status: Option<Status>,
    own_status: Option<own_membership::Status>,
}

struct Inner {
    room_id: String,
    slot_id: String,
    own: OwnIdentity,
    config: ParticipationConfig,
    driver: Arc<dyn MatrixDriver>,
    session: Session,
    own_membership: OwnMembershipManager,
    connections: Arc<ConnectionsManager>,
    /// One `encryption::Machine` per participation, `None` while not joined.
    /// Constructed in `join()` *before* the own-membership machine sends the
    /// join event (peers hold our key by the time our member event lands, and
    /// it already listens for theirs); dropped in `leave()` — dropping
    /// forgets every key and stops its pump. A rejoin is a new machine.
    encryption: Mutex<Option<encryption::Machine>>,
    callbacks: Mutex<Callbacks>,
    published: Mutex<Published>,
    /// Poked by the key-map callback: memberships/status may have changed.
    key_map_changed: Arc<Notify>,
    /// Serialises `join`/`leave`.
    lifecycle: tokio::sync::Mutex<()>,
}

pub struct ParticipationManager {
    inner: Arc<Inner>,
    notify: Arc<Notify>,
}

impl ParticipationManager {
    /// Hands the session its `RoomEventsDriver` slice (the session seeds and
    /// feeds itself), the own-membership machine its `OwnMembershipDriver`
    /// slice, connections the `TokenDriver` slice, and — at join time — the
    /// encryption machine the `ToDeviceDriver` slice. No other routing.
    pub fn new(
        room_id: String,
        slot_id: String,
        own: OwnIdentity,
        driver: Arc<dyn MatrixDriver>,
        config: ParticipationConfig,
    ) -> Self {
        let compat = config.compat;
        let session = Session::new(
            room_id.clone(),
            slot_id.clone(),
            driver.clone(),
            SessionConfig { compat },
        );
        let connections = Arc::new(ConnectionsManager::new(
            room_id.clone(),
            slot_id.clone(),
            own.clone(),
            compat,
            session.subscribe(),
            driver.clone(),
        ));
        let resolver_connections = connections.clone();
        let own_membership = OwnMembershipManager::new(
            room_id.clone(),
            slot_id.clone(),
            own.clone(),
            session.subscribe(),
            driver.clone(),
            compat,
            Box::new(move |member_id, intent| {
                let connections = resolver_connections.clone();
                Box::pin(async move { connections.add_own_transport(member_id, intent).await })
            }),
        );
        let key_map_changed = Arc::new(Notify::new());
        let inner = Arc::new(Inner {
            room_id,
            slot_id,
            own,
            config,
            driver,
            session,
            own_membership,
            connections,
            encryption: Mutex::new(None),
            callbacks: Mutex::default(),
            published: Mutex::default(),
            key_map_changed: key_map_changed.clone(),
            lifecycle: tokio::sync::Mutex::new(()),
        });
        let notify = Arc::new(Notify::new());
        executor::spawn(run(Arc::downgrade(&inner), notify.clone(), key_map_changed));
        Self { inner, notify }
    }

    /// Join: wait for the session seed, mint the member id, build the
    /// encryption machine (negotiated encryption overrides the local
    /// default), then publish the membership.
    pub async fn join(&self, intent: TransportIntent, params: JoinParams) -> Result<(), JoinError> {
        let inner = &self.inner;
        let _lifecycle = inner.lifecycle.lock().await;
        if !matches!(
            inner.own_membership.status(),
            own_membership::Status::NotJoined
        ) {
            return Err(JoinError::AlreadyJoined);
        }
        inner.wait_seeded().await;
        let snapshot = inner.session.snapshot();
        let compat = inner.config.compat;
        if inner.slot_id != crate::session::LEGACY_SLOT_ID
            && snapshot.slot_state == Some(crate::session::SlotState::Closed)
        {
            return Err(JoinError::SlotClosed);
        }
        let member_id = new_member_id(compat, &inner.own);
        let manage_media_keys = snapshot
            .negotiated_encryption
            .unwrap_or(inner.config.encryption.manage_media_keys);
        log::info!(
            "[{}/{}] join: member {member_id}, media keys {}",
            inner.room_id,
            inner.slot_id,
            if manage_media_keys {
                "managed"
            } else {
                "not managed"
            }
        );
        let member = Member {
            member_id: member_id.clone(),
            user_id: inner.own.user_id.clone(),
            device_id: Some(inner.own.device_id.clone()),
            device_attribution: DeviceAttribution::Verified,
            membership_ts: Some(executor::now_ms()),
            display_name: None,
            avatar_url: None,
            intent: params.intent.clone(),
            application_type: Some(params.application_type.clone()),
            transports: MemberTransports::default(),
        };
        let weak = Arc::downgrade(inner);
        let machine = encryption::Machine::new(
            inner.driver.clone(),
            inner.room_id.clone(),
            inner.slot_id.clone(),
            compat,
            inner.session.subscribe(),
            &member,
            manage_media_keys,
            inner.config.encryption.clone(),
            inner.config.rotation.clone(),
            Box::new(move |map: &KeyMap, change: &MediaKeyChange| {
                let Some(inner) = weak.upgrade() else { return };
                if let Some(cb) = inner
                    .callbacks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .key_map
                    .as_ref()
                {
                    cb(map, change);
                }
                inner.key_map_changed.notify_one();
            }),
        )
        .map_err(|e| JoinError::InvalidParams(e.to_string()))?;
        *inner.lock_encryption() = Some(machine);
        if matches!(intent, TransportIntent::ReceiveOnly { .. }) {
            inner.connections.set_own_member(member_id.clone());
        }
        match inner.own_membership.join(member_id, intent, params).await {
            Ok(()) => Ok(()),
            Err(error) => {
                inner.end_participation();
                Err(error)
            }
        }
    }

    /// Leave: send the leave membership, settle the delayed event, forget
    /// every key and token.
    pub async fn leave(&self, reason: Option<LeaveReason>) -> Result<(), LeaveError> {
        let inner = &self.inner;
        let _lifecycle = inner.lifecycle.lock().await;
        let result = inner.own_membership.leave(reason).await;
        match &result {
            Ok(()) | Err(LeaveError::NotJoined) => inner.end_participation(),
            Err(LeaveError::Driver(_)) => {}
        }
        result
    }

    /// Slot administration (usually needs elevated power levels): open this
    /// manager's slot for `application_type`, declaring `m.per_member`
    /// encryption when `encrypted`.
    pub async fn open_slot(
        &self,
        application_type: &str,
        encrypted: bool,
    ) -> Result<(), DriverError> {
        if !self
            .inner
            .slot_id
            .starts_with(&format!("{application_type}#"))
        {
            return Err(DriverError::Other(format!(
                "slot id {:?} does not start with {application_type:?}#",
                self.inner.slot_id
            )));
        }
        let mut content = json!({ "status": "open", "application": { "type": application_type } });
        if encrypted {
            content["encryption"] = json!({ "type": "m.per_member" });
        }
        self.send_slot(content).await
    }

    pub async fn close_slot(&self) -> Result<(), DriverError> {
        self.send_slot(json!({ "status": "closed" })).await
    }

    async fn send_slot(&self, content: Value) -> Result<(), DriverError> {
        let driver: &dyn OwnMembershipDriver = self.inner.driver.as_ref();
        driver
            .send_state_event(
                self.inner.room_id.clone(),
                wire_event_type("m.rtc.slot").to_owned(),
                self.inner.slot_id.clone(),
                content,
            )
            .await
            .map(|_| ())
    }

    /// The session as the room sees it (slot state, negotiated encryption,
    /// joined members) — the same value `compute_sessions_from_events`
    /// returns for room info, kept live here. Read it to know whether the
    /// slot is open before joining, or to show call metadata.
    pub fn session(&self) -> crate::session::SessionSnapshot {
        self.inner.session.snapshot()
    }

    /// The current membership list: the session's joined projection, plus
    /// left members that still hold our keys ([`MembershipState`]).
    pub fn memberships(&self) -> Vec<SessionMembership> {
        self.inner.memberships()
    }

    pub fn on_memberships_change(&self, callback: MembershipsCallback) {
        self.inner
            .callbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .memberships = Some(callback);
    }

    pub fn connections(&self) -> Vec<ConnectionWithMembers> {
        self.inner.connections.connections()
    }

    pub fn on_connections_change(&self, callback: ConnectionsCallback) {
        self.inner
            .callbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .connections = Some(callback);
    }

    /// Empty while not joined.
    pub fn key_map(&self) -> KeyMap {
        self.inner
            .lock_encryption()
            .as_ref()
            .map(|m| m.key_map())
            .unwrap_or_default()
    }

    /// Fires with the full map and the single changed key.
    pub fn on_key_map_change(&self, callback: KeyMapCallback) {
        self.inner
            .callbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .key_map = Some(callback);
    }

    pub fn status(&self) -> Status {
        self.inner.status()
    }

    pub fn on_status_change(&self, callback: StatusCallback) {
        self.inner
            .callbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .status = Some(callback);
    }

    /// Diagnostics: every part's state as JSON (no key material).
    pub fn debug_snapshot(&self) -> Value {
        let inner = &self.inner;
        json!({
            "room_id": inner.room_id,
            "slot_id": inner.slot_id,
            "status": format!("{:?}", inner.status()),
            "session": inner.session.debug_snapshot(),
            "own_membership": inner.own_membership.debug_snapshot(),
            "encryption": inner.lock_encryption().as_ref().map(|m| m.debug_snapshot()),
            "connections": inner.connections.connections().iter().map(|c| json!({
                "service_url": c.connection.service_url,
                "ws_url": c.connection.ws_url,
                "members": c.members.iter().map(|m| &m.member_id).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    }
}

impl Drop for ParticipationManager {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

impl Inner {
    fn lock_encryption(&self) -> std::sync::MutexGuard<'_, Option<encryption::Machine>> {
        self.encryption.lock().unwrap_or_else(|e| e.into_inner())
    }

    async fn wait_seeded(&self) {
        let mut rx = self.session.subscribe();
        if rx.borrow().seeded {
            return;
        }
        let deadline = executor::sleep_ms(SEED_WAIT_MS);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() || rx.borrow().seeded {
                        return;
                    }
                }
                _ = &mut deadline => {
                    log::warn!("[{}] the session did not finish seeding within {SEED_WAIT_MS}ms; joining anyway", self.room_id);
                    return;
                }
            }
        }
    }

    /// Drop the encryption machine (forgetting every key) and our transport.
    fn end_participation(&self) {
        let machine = self.lock_encryption().take();
        drop(machine);
        self.connections.clear_own();
        self.key_map_changed.notify_one();
    }

    fn memberships(&self) -> Vec<SessionMembership> {
        let compat = self.config.compat;
        let snapshot = self.session.snapshot();
        let mut list: Vec<SessionMembership> = snapshot
            .members
            .iter()
            .map(|member| SessionMembership {
                connections: connections::member_service_urls(member),
                transport_identity: connections::transport_identity(compat, member),
                member: member.clone(),
                state: MembershipState::Joined,
            })
            .collect();
        // Holders of our current key, diffed against the *fresh* roster (the
        // encryption machine's own view of the session lags one pump step).
        let holders = self
            .lock_encryption()
            .as_ref()
            .map(|m| m.key_holders())
            .unwrap_or_default();
        for member in holders {
            if list.iter().any(|m| m.member.member_id == member.member_id) {
                continue;
            }
            list.push(SessionMembership {
                transport_identity: connections::transport_identity(compat, &member),
                member,
                state: MembershipState::LeftWithKeys,
                connections: Vec::new(),
            });
        }
        list
    }

    fn status(&self) -> Status {
        let own = self.own_membership.status();
        let encryption = self.lock_encryption().as_ref().map(|m| m.status());
        match (own, encryption) {
            (own_membership::Status::Joining(o), Some(encryption)) => Status::Joining(JoinStatus {
                own_membership: o,
                encryption,
            }),
            (own_membership::Status::Connected(o), Some(encryption)) => {
                Status::Connected(ConnectedStatus {
                    own_membership: o,
                    encryption,
                })
            }
            (own_membership::Status::Leaving(l), _) => Status::Leaving(l),
            // No machine: not joined (or the join is still being set up).
            _ => Status::Disconnected,
        }
    }

    /// Recompute the outputs and fire the callbacks whose value changed.
    /// Called by the pump; callbacks run without any lock held.
    fn refresh_outputs(&self) {
        self.reap_ended_participation();
        let memberships = self.memberships();
        let connections = self.connections.connections();
        let status = self.status();
        let (fire_memberships, fire_connections, fire_status) = {
            let mut published = self.published.lock().unwrap_or_else(|e| e.into_inner());
            let m = published.memberships != memberships;
            let c = published.connections != connections;
            let s = published.status.as_ref() != Some(&status);
            if m {
                published.memberships = memberships.clone();
            }
            if c {
                published.connections = connections.clone();
            }
            if s {
                published.status = Some(status.clone());
            }
            (m, c, s)
        };
        let callbacks = self.callbacks.lock().unwrap_or_else(|e| e.into_inner());
        if fire_memberships && let Some(cb) = &callbacks.memberships {
            cb(&memberships);
        }
        if fire_connections && let Some(cb) = &callbacks.connections {
            cb(&connections);
        }
        if fire_status && let Some(cb) = &callbacks.status {
            cb(&status);
        }
    }

    /// The own-membership machine can end a participation on its own (the
    /// slot closed): once it reports `NotJoined` after being connected, drop
    /// the encryption machine and tokens — unless `leave()` is doing it.
    fn reap_ended_participation(&self) {
        let own = self.own_membership.status();
        let previous = {
            let mut published = self.published.lock().unwrap_or_else(|e| e.into_inner());
            published.own_status.replace(own.clone())
        };
        let ended = matches!(own, own_membership::Status::NotJoined)
            && matches!(
                previous,
                Some(own_membership::Status::Connected(_) | own_membership::Status::Leaving(_))
            );
        if ended
            && let Ok(_guard) = self.lifecycle.try_lock()
            && self.lock_encryption().is_some()
        {
            log::info!(
                "[{}] participation ended by the own-membership machine; dropping keys and tokens",
                self.room_id
            );
            self.end_participation();
        }
    }
}

/// The facade's pump: wake on any input change, recompute, fire callbacks.
async fn run(inner: Weak<Inner>, notify: Arc<Notify>, key_map_changed: Arc<Notify>) {
    let Some(strong) = inner.upgrade() else {
        return;
    };
    let mut session = strong.session.subscribe();
    let mut connections = strong.connections.subscribe();
    let mut own_status = strong.own_membership.subscribe_status();
    drop(strong);
    loop {
        let Some(strong) = inner.upgrade() else {
            return;
        };
        strong.refresh_outputs();
        drop(strong);
        tokio::select! {
            r = session.changed() => if r.is_err() { return },
            r = connections.changed() => if r.is_err() { return },
            r = own_status.changed() => if r.is_err() { return },
            _ = key_map_changed.notified() => {}
            _ = notify.notified() => if inner.upgrade().is_none() { return },
        }
    }
}
