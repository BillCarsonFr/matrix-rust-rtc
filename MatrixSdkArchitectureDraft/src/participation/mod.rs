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
    self, EncryptionConfig, KeyMap, KeyMapCallback, MediaKeyChange, MediaKeyState,
    SendMachineConfig,
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
    /// Live problems already visible during the join (session state unread,
    /// a key that has not reached a peer). Same vocabulary as `Connected`.
    pub impairments: Vec<Impairment>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectedStatus {
    pub own_membership: own_membership::ConnectedStatus,
    pub encryption: encryption::Status,
    /// Everything currently wrong, most severe first. Derived from the
    /// structured state above plus the connection and session views.
    pub impairments: Vec<Impairment>,
}

/// The encryption side is deliberately absent: while leaving, keys are being
/// forgotten and any statement about them would be about to be false. The
/// impairments stay, so a leave hanging on a failing driver is visible.
#[derive(Clone, Debug, PartialEq)]
pub struct LeaveStatus {
    pub own_membership: own_membership::LeaveStatus,
    pub impairments: Vec<Impairment>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    /// Not in the call, with the reason we are not — a `leave()`, a slot
    /// closed under us, a failed join, or simply "never joined".
    Disconnected(DisconnectCause),
    Joining(JoinStatus),
    Connected(ConnectedStatus),
    Leaving(LeaveStatus),
}

impl Status {
    /// Everything currently wrong, most severe first — the same vec every
    /// non-disconnected variant carries, and empty when disconnected.
    ///
    /// A host that only wants *problem* transitions should diff this rather
    /// than the whole `Status`: `Connected` also changes on every keep-alive
    /// restart beat (the timestamps move), which is health, not news.
    pub fn impairments(&self) -> &[Impairment] {
        match self {
            Self::Disconnected(_) => &[],
            Self::Joining(s) => &s.impairments,
            Self::Connected(s) => &s.impairments,
            Self::Leaving(s) => &s.impairments,
        }
    }
}

/// How severe an [`Impairment`] is, and therefore where a host renders it.
/// Ordered most severe first, which is also the order `impairments` is
/// sorted in.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub enum Severity {
    /// We are, or are about to be, out of the call — or peers cannot use
    /// our media.
    Critical,
    /// Degraded but functioning; a crash or a timeout would now hurt.
    Degraded,
    /// Worth surfacing in diagnostics, not in the call UI.
    Notice,
}

/// A condition that is true *right now* and that the crate is still working
/// on. Every variant clears by itself when the underlying operation
/// succeeds — an impairment is never terminal. Anything terminal ends the
/// participation and appears as [`DisconnectCause`] instead.
///
/// This is deliberately redundant with the structured state in
/// [`own_membership::ConnectedStatus`] and the per-tile
/// [`encryption::MediaKeyState`], in the same way memberships and
/// connections are redundant: the structured values are authoritative and
/// carry the timestamps a UI renders countdowns from, while this flat,
/// severity-ordered list means a host that renders one warning banner cannot
/// *miss* a condition it did not know to look for. It is a pure projection —
/// it never carries a fact the structured fields do not.
///
/// Timestamps are unix-ms (`_ts` = a point in time, `_ms` = a duration).
#[derive(Clone, Debug, PartialEq)]
pub enum Impairment {
    // ---- our own membership ------------------------------------------
    /// The dead man's switch could not be restarted. It is still armed:
    /// unless a restart succeeds, the homeserver publishes our leave at
    /// `fires_at_ts` and we drop out of the call.
    KeepAliveRestartFailing {
        since_ts: u64,
        fires_at_ts: u64,
        last_error: String,
    },

    /// The delay's full period elapsed with no successful restart, so the
    /// homeserver has in all likelihood already published our leave — we
    /// have simply not seen it come back yet. A replacement is being armed
    /// and the membership re-published. This is the state between "we are
    /// probably out" and the roster confirming it.
    ///
    /// Named for what the crate *knows*: it deduces the delay fired, it
    /// never observed a removal.
    KeepAliveExpired { since_ts: u64 },

    /// No dead man's switch is armed: this homeserver refuses delayed
    /// events. If this client dies, our tile survives until
    /// `membership_expires_at_ts`. `permanent` distinguishes a homeserver
    /// that will never do it from a transient refusal we re-probe.
    KeepAliveUnavailable {
        permanent: bool,
        membership_expires_at_ts: u64,
    },

    /// Re-publishing our sticky membership is failing. It expires at
    /// `expires_at_ts` unless a refresh gets through.
    MembershipRefreshFailing {
        since_ts: u64,
        expires_at_ts: u64,
        last_error: String,
    },

    /// Our membership was in the roster and is gone: right now nobody sees
    /// us. `republished_at_ts` is set once the self-heal re-sent it and we
    /// are waiting for the echo.
    OwnMembershipMissing {
        since_ts: u64,
        republished_at_ts: Option<u64>,
    },

    /// Our membership is on the server but the session refuses to project
    /// it — nobody sees us, and the self-heal deliberately will not fix
    /// this. Clears if the room state that caused the exclusion changes.
    ///
    /// Separate from `OwnMembershipMissing` because the remedies differ:
    /// missing is re-sent, excluded cannot be.
    OwnMembershipExcluded {
        reason: crate::session::JoinExclusionReason,
    },

    // ---- media keys ---------------------------------------------------
    /// Our current media key has not reached these members; they cannot
    /// decrypt us. Redelivery is in progress with the same key.
    MediaKeyNotDelivered { member_ids: Vec<String> },

    /// These members have not sent us a usable key; we cannot decrypt them.
    MediaKeyNotReceived { member_ids: Vec<String> },

    /// A key from this member was discarded and it still has none we can
    /// use — the answer to "why can't I hear Bob?", which the crate computes
    /// and used to drop on the floor.
    MediaKeyRejected {
        member_id: String,
        sender_user_id: String,
        reason: encryption::KeyRejection,
        at_ts: u64,
    },

    // ---- transports ---------------------------------------------------
    /// No token could be minted for this connection, so the media of
    /// `member_ids` is unavailable. Retried at `retry_at_ts`.
    ConnectionUnavailable {
        service_url: String,
        member_ids: Vec<String>,
        last_error: String,
        retry_at_ts: u64,
    },

    /// The token we are still handing out for this connection has expired
    /// and could not be renewed — the host's LiveKit connection will fail.
    ConnectionTokenExpired {
        service_url: String,
        expired_at_ts: u64,
        last_error: String,
    },

    // ---- session ------------------------------------------------------
    /// Room state the session needs could not be read, so the conditions it
    /// gates are unenforced — most importantly, whether this call is
    /// encrypted is *unknown*, not "no". Clears if a live state update
    /// supplies the value.
    SessionStateUnread {
        reads: Vec<crate::session::SessionRead>,
    },

    /// `join()` went ahead before the session finished seeding: the
    /// slot-closed pre-check was skipped and the encryption decision fell
    /// back to local config instead of the slot's negotiation. Latched for
    /// the participation — it is a fact about how this join was made, and
    /// no later read can undo it.
    JoinedBeforeSeed { at_ts: u64 },
}

impl Impairment {
    pub fn severity(&self) -> Severity {
        match self {
            Self::KeepAliveExpired { .. }
            | Self::OwnMembershipMissing { .. }
            | Self::OwnMembershipExcluded { .. }
            | Self::KeepAliveRestartFailing { .. }
            | Self::MembershipRefreshFailing { .. }
            | Self::ConnectionTokenExpired { .. }
            | Self::ConnectionUnavailable { .. } => Severity::Critical,
            Self::MediaKeyNotDelivered { .. }
            | Self::MediaKeyNotReceived { .. }
            | Self::MediaKeyRejected { .. }
            | Self::KeepAliveUnavailable { .. }
            | Self::SessionStateUnread { .. } => Severity::Degraded,
            Self::JoinedBeforeSeed { .. } => Severity::Notice,
        }
    }

    /// Total order for `impairments`: severity, then a fixed variant rank,
    /// then the ids inside. Stable across recomputation with unchanged
    /// inputs, so publish-on-change never flaps on ordering alone.
    fn sort_key(&self) -> (Severity, u8, String) {
        let (rank, discriminator) = match self {
            Self::KeepAliveExpired { .. } => (0, String::new()),
            Self::OwnMembershipMissing { .. } => (1, String::new()),
            Self::OwnMembershipExcluded { .. } => (2, String::new()),
            Self::KeepAliveRestartFailing { .. } => (3, String::new()),
            Self::MembershipRefreshFailing { .. } => (4, String::new()),
            Self::ConnectionTokenExpired { service_url, .. } => (5, service_url.clone()),
            Self::ConnectionUnavailable { service_url, .. } => (6, service_url.clone()),
            Self::MediaKeyNotDelivered { .. } => (7, String::new()),
            Self::MediaKeyNotReceived { .. } => (8, String::new()),
            Self::MediaKeyRejected { member_id, .. } => (9, member_id.clone()),
            Self::KeepAliveUnavailable { .. } => (10, String::new()),
            Self::SessionStateUnread { .. } => (11, String::new()),
            Self::JoinedBeforeSeed { .. } => (12, String::new()),
        };
        (self.severity(), rank, discriminator)
    }
}

/// Why [`Status::Disconnected`] is the current state.
///
/// Terminal by construction: unlike an [`Impairment`], none of these clears
/// on its own — only a host decision (a new `join()`, a new manager) changes
/// anything.
#[derive(Clone, Debug, PartialEq)]
pub enum DisconnectCause {
    /// No join has been attempted on this manager.
    NeverJoined,
    /// The host called `leave()`.
    LeftByHost { reason: Option<LeaveReason> },
    /// The slot was closed under us; the own-membership machine left on its
    /// own with `LeaveReason::slot_closed()`.
    SlotClosed,
    /// `join()` failed; the participation never started. Carries how far it
    /// got, which the abort used to throw away — so a progress UI can say
    /// "we got a token but the membership event was rejected".
    JoinFailed {
        at_ts: u64,
        progress: own_membership::JoinStatus,
        error: JoinError,
    },
    /// A pump stopped. The manager is dead and will not recover; the host
    /// must build a new one.
    ManagerStopped { component: Component },
}

/// Slot administration failures.
///
/// The slot-id/application mismatch is a *local precondition* — nothing was
/// sent — and used to arrive as a `DriverError::Other`, conflating a caller
/// mistake with a homeserver failure.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum SlotError {
    #[error("{0}")]
    InvalidSlotId(String),
    #[error(transparent)]
    Driver(#[from] DriverError),
}

/// Which pump stopped, for [`DisconnectCause::ManagerStopped`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Component {
    Session,
    OwnMembership,
    Connections,
    Encryption,
    Participation,
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
    /// Whether this member and we can hear each other. `None` when this call
    /// does not manage media keys (nothing to say) or we have not joined.
    ///
    /// Per-tile truth: "who can hear whom" is answerable without going
    /// through the aggregate `encryption::Status`, which names no member.
    pub media_key: Option<MediaKeyState>,
}

pub type MembershipsCallback = Box<dyn Fn(&[SessionMembership]) + Send + Sync>;
pub type ConnectionsCallback = Box<dyn Fn(&[ConnectionWithMembers]) + Send + Sync>;
pub type StatusCallback = Box<dyn Fn(&Status) + Send + Sync>;
/// `(member_id, why)` — the member whose key was discarded, and the reason.
pub type KeyRejectedCallback = Box<dyn Fn(&str, &encryption::KeyRejection) + Send + Sync>;

#[derive(Default)]
struct Callbacks {
    memberships: Option<MembershipsCallback>,
    connections: Option<ConnectionsCallback>,
    // TODO rename to encryption key map
    key_map: Option<KeyMapCallback>,
    status: Option<StatusCallback>,
    key_rejected: Option<KeyRejectedCallback>,
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
    /// Why we are not in a call. Kept across the disconnect so
    /// `Status::Disconnected` can say *why* rather than being a bare unit.
    disconnect: Mutex<DisconnectCause>,
    /// Set when `join()` gave up waiting for the seed. Latched for the
    /// participation: it describes how this join was made, and no later read
    /// undoes the fact that the slot pre-check was skipped.
    joined_before_seed: Mutex<Option<u64>>,
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
            disconnect: Mutex::new(DisconnectCause::NeverJoined),
            joined_before_seed: Mutex::new(None),
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
        let seeded = inner.wait_seeded().await;
        if !seeded {
            *inner
                .joined_before_seed
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(executor::now_ms());
        }
        let snapshot = inner.session.snapshot();
        let compat = inner.config.compat;
        if inner.slot_id != crate::session::LEGACY_SLOT_ID
            && snapshot.slot_state == Some(crate::session::SlotState::Closed)
        {
            return Err(inner.join_failed(JoinError::SlotClosed));
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
        .map_err(|e| inner.join_failed(JoinError::EncryptionSetup(e)))?;
        // Wire the key-rejection callback the machine has always had and
        // nobody ever called: it is what turns "why can't I hear Bob?" from
        // a computed-then-discarded verdict into something a host can log.
        // The poke also wakes the pump, since a rejection changes no key
        // and therefore nothing else would.
        let weak = Arc::downgrade(inner);
        machine.set_key_rejected_callback(Box::new(
            move |member_id: &str, rejection: &encryption::KeyRejection| {
                let Some(inner) = weak.upgrade() else { return };
                if let Some(cb) = inner
                    .callbacks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .key_rejected
                    .as_ref()
                {
                    cb(member_id, rejection);
                }
                inner.key_map_changed.notify_one();
            },
        ));
        *inner.lock_encryption() = Some(machine);
        if matches!(intent, TransportIntent::ReceiveOnly { .. }) {
            inner.connections.set_own_member(member_id.clone());
        }
        match inner.own_membership.join(member_id, intent, params).await {
            Ok(()) => Ok(()),
            Err(error) => {
                inner.end_participation();
                Err(inner.join_failed(error))
            }
        }
    }

    /// Our member id for this participation, `None` while not joined.
    ///
    /// Every self-referential check needs it — am I in the roster, was I
    /// excluded, which LiveKit participant is me. Matching on
    /// `(user_id, device_id)` is not a substitute: one device may hold
    /// several RTC members, and a rejoin mints a fresh id.
    pub fn own_member_id(&self) -> Option<String> {
        self.inner
            .lock_encryption()
            .as_ref()
            .and_then(|m| m.own_member_id())
    }

    /// Our own entry in [`Self::memberships`], when the session projects it.
    /// `None` while not joined *or* while our echo has not come back — which
    /// is itself reported as
    /// [`own_membership::RosterPresence::AwaitingEcho`].
    pub fn own_membership(&self) -> Option<SessionMembership> {
        let id = self.own_member_id()?;
        self.memberships()
            .into_iter()
            .find(|m| m.member.member_id == id)
    }

    /// Every wanted connection a host cannot use right now, with the members
    /// whose media is affected. Empty is the healthy case; the same facts
    /// also appear as [`Impairment::ConnectionUnavailable`] /
    /// [`Impairment::ConnectionTokenExpired`].
    pub fn connection_problems(&self) -> Vec<connections::ConnectionProblem> {
        self.inner.connections.problems()
    }

    /// Leave: send the leave membership, settle the delayed event, forget
    /// every key and token.
    ///
    /// A driver failure leaves us `Connected` and keeps every key, so the
    /// host may retry. A *delayed-leave cancel* that fails still returns
    /// `Ok` — the delay is itself a leave — and is reported as
    /// [`own_membership::DelayedLeaveOutcome::MayStillFire`].
    pub async fn leave(&self, reason: Option<LeaveReason>) -> Result<(), LeaveError> {
        let inner = &self.inner;
        let _lifecycle = inner.lifecycle.lock().await;
        let result = inner.own_membership.leave(reason.clone()).await;
        match &result {
            Ok(()) | Err(LeaveError::NotJoined) => {
                inner.set_disconnect_cause(DisconnectCause::LeftByHost { reason });
                inner.end_participation();
            }
            // A failed leave returns the machine to `Connected`, so nothing
            // ended and the cause must not be touched.
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
    ) -> Result<(), SlotError> {
        if !self
            .inner
            .slot_id
            .starts_with(&format!("{application_type}#"))
        {
            return Err(SlotError::InvalidSlotId(format!(
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

    pub async fn close_slot(&self) -> Result<(), SlotError> {
        self.send_slot(json!({ "status": "closed" })).await
    }

    async fn send_slot(&self, content: Value) -> Result<(), SlotError> {
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
            .map_err(SlotError::Driver)
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

    /// Fires whenever an inbound media key is discarded, with the member it
    /// named and why.
    ///
    /// **Secondary** to the latched per-tile
    /// [`encryption::MediaKeyState::rejection`] and
    /// [`Impairment::MediaKeyRejected`]: a UI attaching late reads those,
    /// this is for logging and telemetry.
    pub fn on_key_rejected(&self, callback: KeyRejectedCallback) {
        self.inner
            .callbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .key_rejected = Some(callback);
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

    /// Waits for the session seed; `false` when it gave up after
    /// [`SEED_WAIT_MS`] and the join will proceed blind — the caller latches
    /// that as [`Impairment::JoinedBeforeSeed`], because it means the
    /// slot-closed pre-check was skipped and the encryption decision fell
    /// back to local config.
    async fn wait_seeded(&self) -> bool {
        let mut rx = self.session.subscribe();
        if rx.borrow().seeded {
            return true;
        }
        let deadline = executor::sleep_ms(SEED_WAIT_MS);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                    if rx.borrow().seeded {
                        return true;
                    }
                }
                _ = &mut deadline => {
                    log::warn!("[{}] the session did not finish seeding within {SEED_WAIT_MS}ms; joining anyway", self.room_id);
                    return false;
                }
            }
        }
    }

    /// Drop the encryption machine (forgetting every key) and our transport.
    fn end_participation(&self) {
        let machine = self.lock_encryption().take();
        drop(machine);
        self.connections.clear_own();
        *self
            .joined_before_seed
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        self.key_map_changed.notify_one();
    }

    /// Record a failed join (with how far it got) and hand the error back.
    fn join_failed(&self, error: JoinError) -> JoinError {
        self.set_disconnect_cause(DisconnectCause::JoinFailed {
            at_ts: executor::now_ms(),
            progress: self.own_membership.last_join_progress(),
            error: error.clone(),
        });
        error
    }

    fn set_disconnect_cause(&self, cause: DisconnectCause) {
        *self.disconnect.lock().unwrap_or_else(|e| e.into_inner()) = cause;
    }

    fn disconnect_cause(&self) -> DisconnectCause {
        self.disconnect
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn memberships(&self) -> Vec<SessionMembership> {
        let compat = self.config.compat;
        let snapshot = self.session.snapshot();
        let encryption = self.lock_encryption();
        // `None` for our own tile too: we trivially hold our own key, and
        // saying "you cannot hear yourself" would be noise.
        let key_state = |member_id: &str| {
            encryption
                .as_ref()
                .filter(|m| {
                    m.manages_media_keys() && m.own_member_id().as_deref() != Some(member_id)
                })
                .map(|m| m.key_state(member_id))
        };
        let mut list: Vec<SessionMembership> = snapshot
            .members
            .iter()
            .map(|member| SessionMembership {
                connections: connections::member_service_urls(member),
                transport_identity: connections::transport_identity(compat, member),
                media_key: key_state(&member.member_id),
                member: member.clone(),
                state: MembershipState::Joined,
            })
            .collect();
        // Holders of our current key, diffed against the *fresh* roster (the
        // encryption machine's own view of the session lags one pump step).
        let holders = encryption
            .as_ref()
            .map(|m| m.key_holders())
            .unwrap_or_default();
        for member in holders {
            if list.iter().any(|m| m.member.member_id == member.member_id) {
                continue;
            }
            list.push(SessionMembership {
                transport_identity: connections::transport_identity(compat, &member),
                media_key: key_state(&member.member_id),
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
                impairments: self.impairments(None),
                own_membership: o,
                encryption,
            }),
            (own_membership::Status::Connected(o), Some(encryption)) => {
                Status::Connected(ConnectedStatus {
                    impairments: self.impairments(Some(&o)),
                    own_membership: o,
                    encryption,
                })
            }
            (own_membership::Status::Leaving(l), _) => Status::Leaving(LeaveStatus {
                own_membership: l,
                impairments: self.impairments(None),
            }),
            // No machine: not joined (or the join is still being set up).
            _ => Status::Disconnected(self.disconnect_cause()),
        }
    }

    /// The one place `impairments` is derived, so the flat list and the
    /// structured state can never disagree.
    ///
    /// This method only *gathers* the inputs; every rule lives in the free
    /// functions below, which are pure and unit-tested — the same split the
    /// rest of the crate uses between a pump and its machine.
    ///
    /// `own` is the connected own-membership state when there is one; while
    /// joining or leaving its mechanisms have no meaningful state yet (or
    /// any more) and only the session-, key- and connection-side conditions
    /// apply.
    fn impairments(&self, own: Option<&own_membership::ConnectedStatus>) -> Vec<Impairment> {
        let mut out = Vec::new();
        if let Some(own) = own {
            own_membership_impairments(own, &mut out);
        }
        // `memberships()` takes the encryption lock itself, so it has to run
        // to completion before we take it here (`Mutex` is not reentrant).
        let memberships = self.memberships();
        {
            let machine = self.lock_encryption();
            media_key_impairments(
                &memberships,
                |id| machine.as_ref().and_then(|m| m.key_rejected_at(id)),
                &mut out,
            );
        }
        let connections = self.connections.connections();
        connection_impairments(
            &self.connections.problems(),
            |url| {
                connections
                    .iter()
                    .find(|c| c.connection.service_url == url)
                    .and_then(|c| c.connection.expires_at_ts)
            },
            &mut out,
        );
        session_impairments(
            self.session.snapshot().failed_reads,
            *self
                .joined_before_seed
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
            &mut out,
        );
        // Severity, then a fixed variant rank, then the ids inside: a total
        // order over values that do not change, so recomputing with the same
        // inputs yields the same vec and `PartialEq` does not flap.
        out.sort_by_key(Impairment::sort_key);
        out
    }

    /// Publish the status through the callback if it changed, without
    /// touching the other outputs. Used by the liveness guard, which runs
    /// after the pump's normal path is over.
    fn publish_status_now(&self) {
        let status = self.status();
        let fire = {
            let mut published = self.published.lock().unwrap_or_else(|e| e.into_inner());
            let changed = published.status.as_ref() != Some(&status);
            if changed {
                published.status = Some(status.clone());
            }
            changed
        };
        if fire
            && let Some(cb) = &self
                .callbacks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .status
        {
            cb(&status);
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
            // The machine only ends a participation on its own for one
            // reason: it built a `LeaveReason::slot_closed()` for the wire,
            // which the host never got to see until now.
            self.set_disconnect_cause(DisconnectCause::SlotClosed);
            self.end_participation();
        }
    }
}

/// The own-membership half of the projection: one impairment per failing
/// mechanism, none for a healthy one.
fn own_membership_impairments(own: &own_membership::ConnectedStatus, out: &mut Vec<Impairment>) {
    match &own.keep_alive {
        // Armed and Delegated are health: neither yields an impairment.
        // `Delegated` in particular must not — after delegation we stop
        // restarting on purpose, and reading that as a failing restart is
        // the footgun this whole design exists to remove.
        own_membership::KeepAlive::Armed { .. } | own_membership::KeepAlive::Delegated { .. } => {}
        own_membership::KeepAlive::RestartFailing {
            since_ts,
            fires_at_ts,
            last_error,
        } => out.push(Impairment::KeepAliveRestartFailing {
            since_ts: *since_ts,
            fires_at_ts: *fires_at_ts,
            last_error: last_error.clone(),
        }),
        own_membership::KeepAlive::Expired { since_ts } => out.push(Impairment::KeepAliveExpired {
            since_ts: *since_ts,
        }),
        own_membership::KeepAlive::Unavailable { permanent, .. } => {
            out.push(Impairment::KeepAliveUnavailable {
                permanent: *permanent,
                // Without a dead man's switch, the sticky expiry is the only
                // thing that will ever clear a ghost tile — so that is the
                // deadline this impairment reports.
                membership_expires_at_ts: own.membership.expires_at_ts,
            })
        }
    }
    if let Some(since_ts) = own.membership.refresh_failing_since_ts {
        out.push(Impairment::MembershipRefreshFailing {
            since_ts,
            expires_at_ts: own.membership.expires_at_ts,
            last_error: own
                .membership
                .last_refresh_error
                .clone()
                .unwrap_or_default(),
        });
    }
    match &own.roster {
        own_membership::RosterPresence::Present | own_membership::RosterPresence::AwaitingEcho => {}
        own_membership::RosterPresence::Missing {
            since_ts,
            republished_at_ts,
        } => out.push(Impairment::OwnMembershipMissing {
            since_ts: *since_ts,
            republished_at_ts: *republished_at_ts,
        }),
        own_membership::RosterPresence::Excluded { reason } => {
            out.push(Impairment::OwnMembershipExcluded { reason: *reason })
        }
    }
}

/// Aggregated from the same per-tile [`encryption::MediaKeyState`] the
/// membership list carries — one source of truth, so a banner and a tile can
/// never contradict each other.
fn media_key_impairments(
    memberships: &[SessionMembership],
    rejected_at: impl Fn(&str) -> Option<u64>,
    out: &mut Vec<Impairment>,
) {
    let (mut undelivered, mut unreceived) = (Vec::new(), Vec::new());
    for membership in memberships {
        // `LeftWithKeys` tiles are not expected to exchange keys, and a
        // member with no device cannot be sent one at all — neither is an
        // impairment of ours.
        if membership.state != MembershipState::Joined || membership.member.device_id.is_none() {
            continue;
        }
        let Some(key) = &membership.media_key else {
            continue;
        };
        let id = &membership.member.member_id;
        if !key.holds_our_key {
            undelivered.push(id.clone());
        }
        if !key.have_their_key {
            unreceived.push(id.clone());
        }
        if let Some(reason) = &key.rejection {
            out.push(Impairment::MediaKeyRejected {
                member_id: id.clone(),
                sender_user_id: membership.member.user_id.clone(),
                reason: reason.clone(),
                at_ts: rejected_at(id).unwrap_or_default(),
            });
        }
    }
    undelivered.sort();
    unreceived.sort();
    if !undelivered.is_empty() {
        out.push(Impairment::MediaKeyNotDelivered {
            member_ids: undelivered,
        });
    }
    if !unreceived.is_empty() {
        out.push(Impairment::MediaKeyNotReceived {
            member_ids: unreceived,
        });
    }
}

fn connection_impairments(
    problems: &[connections::ConnectionProblem],
    expires_at: impl Fn(&str) -> Option<u64>,
    out: &mut Vec<Impairment>,
) {
    for problem in problems {
        out.push(match problem.kind {
            connections::ConnectionProblemKind::NoToken => Impairment::ConnectionUnavailable {
                service_url: problem.service_url.clone(),
                member_ids: problem.member_ids.clone(),
                last_error: problem.last_error.clone(),
                retry_at_ts: problem.retry_at_ts,
            },
            connections::ConnectionProblemKind::TokenExpired => {
                Impairment::ConnectionTokenExpired {
                    expired_at_ts: expires_at(&problem.service_url).unwrap_or_default(),
                    service_url: problem.service_url.clone(),
                    last_error: problem.last_error.clone(),
                }
            }
        });
    }
}

fn session_impairments(
    failed_reads: Vec<crate::session::SessionRead>,
    joined_before_seed: Option<u64>,
    out: &mut Vec<Impairment>,
) {
    if !failed_reads.is_empty() {
        out.push(Impairment::SessionStateUnread {
            reads: failed_reads,
        });
    }
    if let Some(at_ts) = joined_before_seed {
        out.push(Impairment::JoinedBeforeSeed { at_ts });
    }
}

/// Publishes `ManagerStopped` when the facade's pump leaves — on a clean
/// exit and on unwind alike. Without it the manager just freezes: `status()`
/// keeps returning the last value and no callback ever fires again, so "the
/// pump died" and "nothing changed" look identical from outside
/// (`ErrorSurfaceAnalysis.md` §6).
///
/// The boundary this cannot cover: a panic inside the *callback dispatch*
/// itself. Nothing inside the manager can observe that, and pretending
/// otherwise would be worse than documenting it.
struct LivenessGuard {
    inner: Weak<Inner>,
}

impl Drop for LivenessGuard {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            // The manager itself is gone; nobody is left to tell.
            return;
        };
        log::error!(
            "[{}] the participation pump stopped; the manager will not recover",
            inner.room_id
        );
        inner.set_disconnect_cause(DisconnectCause::ManagerStopped {
            component: Component::Participation,
        });
        inner.publish_status_now();
    }
}

/// The facade's pump: wake on any input change, recompute, fire callbacks.
async fn run(inner: Weak<Inner>, notify: Arc<Notify>, key_map_changed: Arc<Notify>) {
    let Some(strong) = inner.upgrade() else {
        return;
    };
    let mut session = strong.session.subscribe();
    let mut connections = strong.connections.subscribe();
    // A mint failure changes no *connection* — the key is simply still
    // absent — so without this watch the pump would never wake for the
    // impairment that reports it.
    let mut problems = strong.connections.subscribe_problems();
    let mut own_status = strong.own_membership.subscribe_status();
    drop(strong);
    let _guard = LivenessGuard {
        inner: inner.clone(),
    };
    loop {
        let Some(strong) = inner.upgrade() else {
            return;
        };
        strong.refresh_outputs();
        drop(strong);
        tokio::select! {
            r = session.changed() => if r.is_err() { return },
            r = connections.changed() => if r.is_err() { return },
            r = problems.changed() => if r.is_err() { return },
            r = own_status.changed() => if r.is_err() { return },
            // Also poked by the key-rejection callback: a rejection changes
            // no key, so nothing else would wake for it.
            _ = key_map_changed.notified() => {}
            _ = notify.notified() => if inner.upgrade().is_none() { return },
        }
    }
}

#[cfg(test)]
mod tests {
    //! The impairment projection is pure, so every raise/clear pair is a
    //! plain unit test here. The end-to-end behaviour — that a failing
    //! keep-alive actually *fires* `on_status_change` — lives in
    //! `tests/participation.rs`, because that is a property of the pump.
    use super::*;
    use crate::connections::{ConnectionProblem, ConnectionProblemKind};
    use crate::encryption::{KeyRejection, MediaKeyState};
    use crate::session::{JoinExclusionReason, SessionRead};
    use crate::types::DeviceAttribution;

    const T0: u64 = 1_700_000_000_000;

    fn own(keep_alive: own_membership::KeepAlive) -> own_membership::ConnectedStatus {
        own_membership::ConnectedStatus {
            keep_alive,
            membership: own_membership::MembershipPublication {
                lifetime_ms: 240_000,
                last_published_ts: T0,
                expires_at_ts: T0 + 240_000,
                refresh_failing_since_ts: None,
                last_refresh_error: None,
            },
            roster: own_membership::RosterPresence::Present,
        }
    }

    fn healthy() -> own_membership::ConnectedStatus {
        own(own_membership::KeepAlive::Armed {
            delay_ms: 30_000,
            last_restart_ts: T0,
            fires_at_ts: T0 + 30_000,
        })
    }

    fn from_own(status: &own_membership::ConnectedStatus) -> Vec<Impairment> {
        let mut out = Vec::new();
        own_membership_impairments(status, &mut out);
        out
    }

    fn membership(member_id: &str, media_key: Option<MediaKeyState>) -> SessionMembership {
        SessionMembership {
            member: Member {
                member_id: member_id.into(),
                user_id: format!("@{member_id}:x"),
                device_id: Some("DEV".into()),
                device_attribution: DeviceAttribution::Verified,
                membership_ts: None,
                display_name: None,
                avatar_url: None,
                intent: None,
                application_type: None,
                transports: MemberTransports::default(),
            },
            state: MembershipState::Joined,
            connections: Vec::new(),
            transport_identity: None,
            media_key,
        }
    }

    fn from_keys(memberships: &[SessionMembership]) -> Vec<Impairment> {
        let mut out = Vec::new();
        media_key_impairments(memberships, |_| Some(T0), &mut out);
        out
    }

    // -- own membership --------------------------------------------------------

    /// A healthy connection raises nothing at all: the vec is a problem
    /// list, not a state dump.
    #[test]
    fn a_healthy_own_membership_raises_nothing() {
        assert!(from_own(&healthy()).is_empty());
    }

    #[test]
    fn a_failing_restart_raises_and_clears() {
        let failing = own(own_membership::KeepAlive::RestartFailing {
            since_ts: T0,
            fires_at_ts: T0 + 30_000,
            last_error: "http error: 500".into(),
        });
        assert_eq!(
            from_own(&failing),
            vec![Impairment::KeepAliveRestartFailing {
                since_ts: T0,
                fires_at_ts: T0 + 30_000,
                last_error: "http error: 500".into(),
            }]
        );
        // One successful restart puts the machine back to `Armed`.
        assert!(from_own(&healthy()).is_empty());
    }

    /// The footgun of `ErrorSurfaceAnalysis.md` §3.1: after delegation the
    /// client stops restarting on purpose, so a delegated keep-alive must
    /// never be read as a failing one however old its timestamps are.
    #[test]
    fn a_delegated_keep_alive_never_yields_restart_failing() {
        let delegated = own(own_membership::KeepAlive::Delegated {
            delegated_at_ts: T0,
            // Long in the past — for a delegated switch this is health.
            earliest_fire_ts: T0 + 3_600_000,
        });
        assert!(from_own(&delegated).is_empty());
    }

    #[test]
    fn an_expired_keep_alive_raises_and_clears_when_a_replacement_is_armed() {
        let expired = own(own_membership::KeepAlive::Expired { since_ts: T0 });
        assert_eq!(
            from_own(&expired),
            vec![Impairment::KeepAliveExpired { since_ts: T0 }]
        );
        assert!(from_own(&healthy()).is_empty());
    }

    #[test]
    fn an_unavailable_keep_alive_raises_with_the_expiry_it_leaves_us_on_and_clears() {
        let unavailable = own(own_membership::KeepAlive::Unavailable {
            permanent: true,
            next_probe_ts: None,
        });
        assert_eq!(
            from_own(&unavailable),
            vec![Impairment::KeepAliveUnavailable {
                permanent: true,
                membership_expires_at_ts: T0 + 240_000,
            }]
        );
        assert!(from_own(&healthy()).is_empty());
    }

    #[test]
    fn a_failing_membership_refresh_raises_and_clears() {
        let mut failing = healthy();
        failing.membership.refresh_failing_since_ts = Some(T0 + 1);
        failing.membership.last_refresh_error = Some("http error: 500".into());
        assert_eq!(
            from_own(&failing),
            vec![Impairment::MembershipRefreshFailing {
                since_ts: T0 + 1,
                expires_at_ts: T0 + 240_000,
                last_error: "http error: 500".into(),
            }]
        );
        assert!(from_own(&healthy()).is_empty());
    }

    #[test]
    fn a_vanished_own_membership_raises_and_clears_and_an_awaited_echo_does_not_raise() {
        let mut missing = healthy();
        missing.roster = own_membership::RosterPresence::Missing {
            since_ts: T0,
            republished_at_ts: Some(T0 + 5),
        };
        assert_eq!(
            from_own(&missing),
            vec![Impairment::OwnMembershipMissing {
                since_ts: T0,
                republished_at_ts: Some(T0 + 5),
            }]
        );
        let mut awaiting = healthy();
        awaiting.roster = own_membership::RosterPresence::AwaitingEcho;
        assert!(
            from_own(&awaiting).is_empty(),
            "an echo still in flight is not a vanished membership"
        );
        assert!(from_own(&healthy()).is_empty());
    }

    #[test]
    fn an_excluded_own_membership_raises_and_clears() {
        let mut excluded = healthy();
        excluded.roster = own_membership::RosterPresence::Excluded {
            reason: JoinExclusionReason::UnencryptedInEncryptedRoom,
        };
        assert_eq!(
            from_own(&excluded),
            vec![Impairment::OwnMembershipExcluded {
                reason: JoinExclusionReason::UnencryptedInEncryptedRoom,
            }]
        );
        assert!(from_own(&healthy()).is_empty());
    }

    // -- media keys ------------------------------------------------------------

    #[test]
    fn undelivered_and_unreceived_keys_raise_separately_and_clear() {
        let settled = MediaKeyState {
            holds_our_key: true,
            have_their_key: true,
            rejection: None,
        };
        assert!(from_keys(&[membership("b", Some(settled.clone()))]).is_empty());

        let deaf = MediaKeyState {
            holds_our_key: false,
            ..settled.clone()
        };
        assert_eq!(
            from_keys(&[membership("b", Some(deaf))]),
            vec![Impairment::MediaKeyNotDelivered {
                member_ids: vec!["b".into()]
            }]
        );

        let mute = MediaKeyState {
            have_their_key: false,
            ..settled.clone()
        };
        assert_eq!(
            from_keys(&[membership("b", Some(mute))]),
            vec![Impairment::MediaKeyNotReceived {
                member_ids: vec!["b".into()]
            }]
        );
        // They fail independently, which is why they are two booleans.
        assert!(from_keys(&[membership("b", Some(settled))]).is_empty());
    }

    #[test]
    fn a_rejected_key_raises_per_member_with_the_reason_and_clears() {
        let rejected = MediaKeyState {
            holds_our_key: true,
            have_their_key: false,
            rejection: Some(KeyRejection::NotCrossSigned),
        };
        let raised = from_keys(&[membership("b", Some(rejected))]);
        assert!(raised.contains(&Impairment::MediaKeyRejected {
            member_id: "b".into(),
            sender_user_id: "@b:x".into(),
            reason: KeyRejection::NotCrossSigned,
            at_ts: T0,
        }));
        let accepted = MediaKeyState {
            holds_our_key: true,
            have_their_key: true,
            rejection: None,
        };
        assert!(from_keys(&[membership("b", Some(accepted))]).is_empty());
    }

    /// An unencrypted call has nothing to say about keys, and a member with
    /// no device cannot be sent one — neither is our impairment.
    #[test]
    fn members_without_key_state_or_a_device_raise_nothing() {
        assert!(from_keys(&[membership("b", None)]).is_empty());
        let mut deviceless = membership(
            "b",
            Some(MediaKeyState {
                holds_our_key: false,
                have_their_key: false,
                rejection: None,
            }),
        );
        deviceless.member.device_id = None;
        assert!(from_keys(&[deviceless]).is_empty());
    }

    // -- connections and session ----------------------------------------------

    #[test]
    fn connection_problems_raise_per_kind_and_clear() {
        let problem = |kind| ConnectionProblem {
            service_url: "https://lk".into(),
            member_ids: vec!["b".into()],
            kind,
            last_error: "not authorized: no".into(),
            retry_at_ts: T0 + 5_000,
        };
        let mut out = Vec::new();
        connection_impairments(
            &[problem(ConnectionProblemKind::NoToken)],
            |_| None,
            &mut out,
        );
        assert_eq!(
            out,
            vec![Impairment::ConnectionUnavailable {
                service_url: "https://lk".into(),
                member_ids: vec!["b".into()],
                last_error: "not authorized: no".into(),
                retry_at_ts: T0 + 5_000,
            }]
        );

        let mut out = Vec::new();
        connection_impairments(
            &[problem(ConnectionProblemKind::TokenExpired)],
            |_| Some(T0),
            &mut out,
        );
        assert_eq!(
            out,
            vec![Impairment::ConnectionTokenExpired {
                service_url: "https://lk".into(),
                expired_at_ts: T0,
                last_error: "not authorized: no".into(),
            }]
        );

        let mut out = Vec::new();
        connection_impairments(&[], |_| None, &mut out);
        assert!(out.is_empty(), "a minted token clears it");
    }

    #[test]
    fn failed_seed_reads_and_a_blind_join_raise_and_clear() {
        let mut out = Vec::new();
        session_impairments(vec![SessionRead::Slot], Some(T0), &mut out);
        assert_eq!(
            out,
            vec![
                Impairment::SessionStateUnread {
                    reads: vec![SessionRead::Slot]
                },
                Impairment::JoinedBeforeSeed { at_ts: T0 },
            ]
        );
        // A live state update supplied the slot, and the next participation
        // was not blind.
        let mut out = Vec::new();
        session_impairments(Vec::new(), None, &mut out);
        assert!(out.is_empty());
    }

    // -- ordering --------------------------------------------------------------

    #[test]
    fn severity_orders_critical_before_degraded_before_notice() {
        assert!(Severity::Critical < Severity::Degraded);
        assert!(Severity::Degraded < Severity::Notice);
        assert_eq!(
            Impairment::KeepAliveExpired { since_ts: T0 }.severity(),
            Severity::Critical
        );
        assert_eq!(
            Impairment::MediaKeyNotDelivered { member_ids: vec![] }.severity(),
            Severity::Degraded
        );
        assert_eq!(
            Impairment::JoinedBeforeSeed { at_ts: T0 }.severity(),
            Severity::Notice
        );
    }

    /// The vec is compared by `PartialEq` to decide whether to publish, so
    /// an unstable order would fire `on_status_change` on every beat.
    #[test]
    fn the_order_is_total_and_stable_across_recomputation() {
        let build = || {
            let mut out = Vec::new();
            let mut status = own(own_membership::KeepAlive::Expired { since_ts: T0 });
            status.membership.refresh_failing_since_ts = Some(T0);
            own_membership_impairments(&status, &mut out);
            media_key_impairments(
                &[
                    membership(
                        "z",
                        Some(MediaKeyState {
                            holds_our_key: false,
                            have_their_key: false,
                            rejection: Some(KeyRejection::Cleartext),
                        }),
                    ),
                    membership(
                        "a",
                        Some(MediaKeyState {
                            holds_our_key: false,
                            have_their_key: false,
                            rejection: Some(KeyRejection::Outdated),
                        }),
                    ),
                ],
                |_| Some(T0),
                &mut out,
            );
            connection_impairments(
                &[
                    ConnectionProblem {
                        service_url: "https://b".into(),
                        member_ids: vec![],
                        kind: ConnectionProblemKind::NoToken,
                        last_error: String::new(),
                        retry_at_ts: 0,
                    },
                    ConnectionProblem {
                        service_url: "https://a".into(),
                        member_ids: vec![],
                        kind: ConnectionProblemKind::NoToken,
                        last_error: String::new(),
                        retry_at_ts: 0,
                    },
                ],
                |_| None,
                &mut out,
            );
            session_impairments(vec![SessionRead::Slot], Some(T0), &mut out);
            out.sort_by_key(Impairment::sort_key);
            out
        };
        let first = build();
        assert_eq!(first, build(), "unchanged inputs, unchanged vec");

        let severities: Vec<Severity> = first.iter().map(Impairment::severity).collect();
        assert!(
            severities.windows(2).all(|w| w[0] <= w[1]),
            "most severe first: {severities:?}"
        );
        // Ties inside one variant break on the ids, not on insertion order.
        let urls: Vec<&str> = first
            .iter()
            .filter_map(|i| match i {
                Impairment::ConnectionUnavailable { service_url, .. } => Some(service_url.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(urls, vec!["https://a", "https://b"]);
        let rejected: Vec<&str> = first
            .iter()
            .filter_map(|i| match i {
                Impairment::MediaKeyRejected { member_id, .. } => Some(member_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(rejected, vec!["a", "z"]);
    }
}
