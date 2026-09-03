//! The join/leave state machine for *our* membership.
//!
//! Talks only to the [`OwnMembershipDriver`] slice of the driver plus one
//! async resolver hook (the facade's `connections.add_own_transport`).
//! Behaviour: member id supplied by the facade (fresh per join; `{user}:{device}`
//! under `StateEvents`), delayed leave armed *before* the join event and
//! kept alive via MSC4140 `restart` (never cancel+reschedule), sticky re-send
//! at half its lifetime, optional delegation of the delayed leave to the SFU
//! (MSC4195), automatic `slot_closed` leave, rate-limited self-heal of a
//! vanished membership. See `OwnMembershipImplementationPlan.md`.
//!
//! Policy is the pure [`machine`] (no clock, no I/O); [`pump`] is the one
//! task owning time (`crate::executor`) and the driver calls.

mod compat_2025;
mod compat_msc3401;
pub(crate) mod machine;
mod pump;
pub(crate) mod wire;

use crate::driver::{DriverError, OwnMembershipDriver};
use crate::executor;
use crate::session::{ElementCallCompat, SessionSnapshot};
use crate::types::{LeaveReason, RtcTransport, TransportIntent, generate_member_id};
use machine::{Input, Machine};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::{Notify, oneshot, watch};

/// Default delay of the dead man's switch (30 s).
pub const DEFAULT_KEEP_ALIVE_TIMEOUT_MS: u64 = 30_000;
/// Default sticky lifetime of our membership (1 h).
pub const DEFAULT_STICKY_DURATION_MS: u64 = 60 * 60 * 1000;
/// Homeservers clamp sticky durations to an hour invisibly; we clamp here so
/// the refresh is derived from the lifetime the entry actually has.
pub const MAX_STICKY_DURATION_MS: u64 = 60 * 60 * 1000;
/// Membership lifetime on a homeserver that refuses delayed events (5 min —
/// MSC4354's floor; without a dead man's switch only expiry clears a ghost).
pub const DEFAULT_DEGRADED_LIFETIME_MS: u64 = 5 * 60 * 1000;

/// Who we are — the write side of compat needs it (`member.user_id` /
/// `member.device_id` in the 2025 dialect, the `{user}:{device}` id and state
/// key in MSC3401).
#[derive(Clone, Debug, PartialEq)]
pub struct OwnIdentity {
    pub user_id: String,
    pub device_id: String,
}

/// The member id for a new join: fresh and random (MSC4143), or
/// `{user}:{device}` under `StateEvents` — that generation's peers use it as
/// the LiveKit participant identity. The facade calls this *before*
/// constructing the encryption machine and passes the same id to `join`.
pub fn new_member_id(compat: ElementCallCompat, own: &OwnIdentity) -> String {
    match compat {
        ElementCallCompat::StateEvents => compat_msc3401::member_id(own),
        ElementCallCompat::Off | ElementCallCompat::StickyEvents => generate_member_id(),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinParams {
    pub application_type: String,
    /// `application["m.call.intent"]` (the MSC3401 dialect requires one and
    /// defaults to `"video"`).
    pub intent: Option<String>,
    /// Sticky lifetime; clamped to [`MAX_STICKY_DURATION_MS`]. The
    /// membership is re-sent at half of it.
    pub sticky_duration_ms: u64,
    /// Delay of the dead man's switch; raised to ≥ 1 h when delegating.
    pub keep_alive_timeout_ms: u64,
    /// Lifetime used instead when the delayed leave is refused before the
    /// first publish (default [`DEFAULT_DEGRADED_LIFETIME_MS`]).
    pub degraded_lifetime_ms: Option<u64>,
    /// Hand the delayed leave to the SFU (MSC4195) once joined.
    pub delegate_delayed_leave: bool,
}

impl JoinParams {
    pub fn new(application_type: impl Into<String>) -> Self {
        Self {
            application_type: application_type.into(),
            intent: None,
            sticky_duration_ms: DEFAULT_STICKY_DURATION_MS,
            keep_alive_timeout_ms: DEFAULT_KEEP_ALIVE_TIMEOUT_MS,
            degraded_lifetime_ms: None,
            delegate_delayed_leave: false,
        }
    }

    pub(crate) fn validate(&self, member_id: &str, intent: &TransportIntent) -> Result<(), String> {
        if self.application_type.is_empty() {
            return Err("application_type is required".into());
        }
        if self.sticky_duration_ms == 0 {
            return Err("sticky_duration_ms must be > 0".into());
        }
        if self.keep_alive_timeout_ms == 0 {
            return Err("keep_alive_timeout_ms must be > 0".into());
        }
        if member_id.is_empty() {
            return Err("member_id is required".into());
        }
        if let TransportIntent::Publish(t) = intent
            && t.transport_type.is_empty()
        {
            return Err("a published transport needs a type".into());
        }
        Ok(())
    }
}

/// Join progress, step by step (drives `participation::Status::Joining`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct JoinStatus {
    pub has_fetched_transports: bool,
    /// Mirrors `SessionSnapshot::seeded` as observed here.
    pub has_fetched_initial_member_list: bool,
    pub has_created_transport_token: bool,
    pub has_sent_delayed_leave_event: bool,
    pub has_sent_member_join_event: bool,
    pub has_delegated_delayed_event: bool,
    pub has_started_heartbeat: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ConnectedStatus {
    /// When the delayed leave would fire if we stopped restarting it
    /// (after delegation: the earliest the homeserver could fire it).
    pub delayed_event_kick_ts: Option<u64>,
    pub heartbeat_last_restart_ts: Option<u64>,
    pub delegation_setup_ts: Option<u64>,
    /// `false` = degraded: no dead man's switch, cleanup rides on expiry.
    pub delayed_leave_supported: bool,
    /// The lifetime every membership event of this join is published with.
    pub membership_lifetime_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LeaveStatus {
    pub leave_event_sent: bool,
    pub delayed_leave_settled: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    NotJoined,
    Joining(JoinStatus),
    Connected(ConnectedStatus),
    Leaving(LeaveStatus),
}

#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    #[error("already joined")]
    AlreadyJoined,
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
    #[error("the slot is closed")]
    SlotClosed,
    #[error("no transport to publish on: {0}")]
    TransportUnavailable(DriverError),
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

#[cfg(not(target_arch = "wasm32"))]
pub type ResolveTransportFuture =
    Pin<Box<dyn Future<Output = Result<RtcTransport, DriverError>> + Send>>;
#[cfg(target_arch = "wasm32")]
pub type ResolveTransportFuture = Pin<Box<dyn Future<Output = Result<RtcTransport, DriverError>>>>;

/// Resolves the transport we publish on: discovers one when the intent does
/// not name it, mints its token (MSC4195 needs our member id), records it as
/// our own — returns the final transport. The facade implements it over
/// `connections.add_own_transport`.
pub type TransportResolver =
    Box<dyn Fn(String, TransportIntent) -> ResolveTransportFuture + Send + Sync>;

pub(crate) struct Inner {
    machine: Mutex<Machine>,
    status_tx: watch::Sender<Status>,
}

impl Inner {
    /// Publish the machine's status if it changed (`watch` publish-on-change).
    pub(crate) fn publish_status(&self) {
        let status = self
            .machine
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .status();
        self.status_tx.send_if_modified(|current| {
            if *current == status {
                return false;
            }
            log::info!(
                "own membership: {:?} -> {:?}",
                variant(current),
                variant(&status)
            );
            *current = status;
            true
        });
    }
}

fn variant(status: &Status) -> &'static str {
    match status {
        Status::NotJoined => "NotJoined",
        Status::Joining(_) => "Joining",
        Status::Connected(_) => "Connected",
        Status::Leaving(_) => "Leaving",
    }
}

/// Our membership in one `(room, slot)`. Constructing it spawns the pump;
/// dropping it stops the pump (nothing is sent on drop — the delayed leave,
/// if armed, does the cleanup).
pub struct OwnMembershipManager {
    inner: Arc<Inner>,
    commands: UnboundedSender<Input>,
    notify: Arc<Notify>,
}

impl OwnMembershipManager {
    /// `driver` is only the trait slice this module needs.
    pub fn new(
        room_id: String,
        slot_id: String,
        own: OwnIdentity,
        session: watch::Receiver<SessionSnapshot>,
        driver: Arc<dyn OwnMembershipDriver>,
        compat: ElementCallCompat,
        resolve_transport: TransportResolver,
    ) -> Self {
        let machine = Machine::new(room_id.clone(), slot_id.clone(), own, compat);
        let (status_tx, _) = watch::channel(Status::NotJoined);
        let inner = Arc::new(Inner {
            machine: Mutex::new(machine),
            status_tx,
        });
        let (commands, command_rx) = unbounded_channel();
        let notify = Arc::new(Notify::new());
        executor::spawn(
            pump::Pump {
                inner: Arc::downgrade(&inner),
                notify: notify.clone(),
                driver,
                resolve_transport,
                session,
                commands: command_rx,
                room_id,
                slot_id,
            }
            .run(),
        );
        Self {
            inner,
            commands,
            notify,
        }
    }

    /// Run the join sequence (see [`JoinStatus`]). Resolves after the last
    /// outbound call of the join was made.
    pub async fn join(
        &self,
        member_id: String,
        intent: TransportIntent,
        params: JoinParams,
    ) -> Result<(), JoinError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(Input::Join {
                member_id,
                intent,
                params,
                reply,
            })
            .map_err(|_| stopped())?;
        rx.await.map_err(|_| stopped())?
    }

    /// Send the leave (with `leave_reason`, default code `leave`) and
    /// cancel/settle the delayed event.
    pub async fn leave(&self, reason: Option<LeaveReason>) -> Result<(), LeaveError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(Input::Leave { reason, reply })
            .map_err(|_| stopped())?;
        rx.await.map_err(|_| stopped())?
    }

    pub fn status(&self) -> Status {
        self.inner.status_tx.borrow().clone()
    }

    /// Current status + every change (heartbeat outcomes, delegation, the
    /// automatic `slot_closed` leave, self-heal — all without a host call).
    pub fn subscribe_status(&self) -> watch::Receiver<Status> {
        self.inner.status_tx.subscribe()
    }

    pub fn debug_snapshot(&self) -> serde_json::Value {
        self.inner
            .machine
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .debug_json()
    }
}

fn stopped() -> DriverError {
    DriverError::Other("own membership manager stopped".into())
}

impl Drop for OwnMembershipManager {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}
