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

/// What is true of our membership right now, per mechanism.
///
/// Three independent mechanisms keep us in a call — the dead man's switch,
/// the sticky publication, and the session's projection of our event — and
/// each fails on its own. They were five loose timestamps once, which could
/// not express a failing keep-alive at all and whose meaning silently
/// inverted after delegation (`ErrorSurfaceAnalysis.md` §3.1). Each is now a
/// value whose *state* changes when the mechanism does, so
/// `participation::ParticipationManager::on_status_change` fires on the
/// failure instead of the host having to poll and diff timestamps.
///
/// Timestamps are unix-ms; `_ts` is a point in time, `_ms` a duration.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectedStatus {
    pub keep_alive: KeepAlive,
    pub membership: MembershipPublication,
    pub roster: RosterPresence,
}

/// The dead man's switch (MSC4140) that clears our membership if this client
/// dies. Mutually exclusive states of one mechanism, so a host renders one
/// of five things and never has to reconcile flags that disagree.
#[derive(Clone, Debug, PartialEq)]
pub enum KeepAlive {
    /// Armed, and we restart it ourselves every `delay_ms / 3`.
    Armed {
        delay_ms: u64,
        last_restart_ts: u64,
        /// When the homeserver publishes our leave if no further restart
        /// lands — `last_restart_ts + delay_ms`.
        fires_at_ts: u64,
    },
    /// Handed to the SFU (MSC4195). We no longer restart it, so a frozen
    /// `last_restart_ts` is expected here rather than a fault — which is
    /// exactly why this is a variant and not a flag beside the timestamps.
    Delegated {
        delegated_at_ts: u64,
        /// The earliest the homeserver could fire it. The SFU keeps it
        /// alive while we are connected, so this passing is not a problem.
        earliest_fire_ts: u64,
    },
    /// Armed, but restarts are failing — unless one succeeds, the homeserver
    /// publishes our leave at `fires_at_ts` and we drop out of the call.
    RestartFailing {
        since_ts: u64,
        fires_at_ts: u64,
        last_error: String,
    },
    /// Its full delay elapsed with no successful restart, so it has in all
    /// likelihood already fired: we are probably out of the call and have
    /// simply not seen the leave come back yet. A replacement is being armed.
    Expired { since_ts: u64 },
    /// None armed. `permanent` = this homeserver refuses delayed events for
    /// good (404/403); otherwise we re-probe at `next_probe_ts` (`Some(0)`
    /// meaning "at the next pump beat"). Without a switch, a crashed client
    /// leaves a ghost tile until [`MembershipPublication::expires_at_ts`].
    Unavailable {
        permanent: bool,
        next_probe_ts: Option<u64>,
    },
}

/// Our sticky membership event on the server (MSC4354). It expires unless we
/// re-publish it, so `expires_at_ts` is the deadline a failing refresh runs
/// against — the host no longer has to derive it from a lifetime.
#[derive(Clone, Debug, PartialEq)]
pub struct MembershipPublication {
    /// The lifetime every membership event of this join is published with.
    pub lifetime_ms: u64,
    pub last_published_ts: u64,
    /// `last_published_ts + lifetime_ms` — when the server drops us if no
    /// refresh lands.
    pub expires_at_ts: u64,
    /// Set while refreshes are failing (`None` when healthy).
    pub refresh_failing_since_ts: Option<u64>,
    pub last_refresh_error: Option<String>,
}

/// Whether the session projects our own membership — i.e. whether anybody
/// can see us.
#[derive(Clone, Debug, PartialEq)]
pub enum RosterPresence {
    /// Our join event was sent, its echo has not come back yet. Not a fault.
    AwaitingEcho,
    Present,
    /// It was in the roster and is gone. `republished_at_ts` is set once the
    /// self-heal re-sent it and we are waiting for that echo.
    Missing {
        since_ts: u64,
        republished_at_ts: Option<u64>,
    },
    /// The event is on the server but the session refuses to project it, so
    /// nobody sees us. The self-heal deliberately does not re-send here: only
    /// the room state that caused the exclusion changing can clear it.
    Excluded {
        reason: crate::session::JoinExclusionReason,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LeaveStatus {
    pub leave_event_sent: bool,
    /// What became of the armed dead man's switch; `None` until the leave
    /// reaches that step (or when none was armed).
    pub delayed_leave: Option<DelayedLeaveOutcome>,
}

/// What happened to the dead man's switch when we left.
///
/// Cancelling it can fail, and `leave()` still returns `Ok` — correctly, the
/// delay *is* a leave, so we are out either way. But the two outcomes are
/// not the same on the wire, and the host used to be told nothing.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DelayedLeaveOutcome {
    /// Cancelled cleanly; nothing further will be published for us.
    Cancelled,
    /// The cancel failed — most likely because it had already fired. A
    /// stray delayed leave event of ours may still land in the room.
    MayStillFire,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    NotJoined,
    Joining(JoinStatus),
    Connected(ConnectedStatus),
    Leaving(LeaveStatus),
}

/// `Clone` + `PartialEq` because a failed join is *carried* by
/// `participation::DisconnectCause::JoinFailed`, which lives inside a
/// publish-on-change `Status`.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum JoinError {
    #[error("already joined")]
    AlreadyJoined,
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
    #[error("the slot is closed")]
    SlotClosed,
    /// The homeserver advertises no usable RTC transport — a configuration
    /// problem; retrying will not help.
    #[error("the homeserver advertises no usable RTC transport: {0}")]
    NoTransport(DriverError),
    /// A transport exists but its token could not be minted — auth or
    /// network; retrying may help. Split from [`Self::NoTransport`] because
    /// the two are different user-facing stories with different remedies.
    #[error("the transport refused to mint a token: {0}")]
    TokenRefused(DriverError),
    /// The encryption machine could not be built. Was an
    /// `InvalidParams("our own membership has no device id")` — a crate
    /// precondition dressed as a caller mistake.
    #[error("the encryption machine could not be built: {0}")]
    EncryptionSetup(crate::encryption::MachineError),
    #[error(transparent)]
    Driver(#[from] DriverError),
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum LeaveError {
    #[error("not joined")]
    NotJoined,
    #[error(transparent)]
    Driver(#[from] DriverError),
}

#[cfg(not(target_arch = "wasm32"))]
pub type ResolveTransportFuture =
    Pin<Box<dyn Future<Output = Result<RtcTransport, ResolveTransportError>> + Send>>;
#[cfg(target_arch = "wasm32")]
pub type ResolveTransportFuture =
    Pin<Box<dyn Future<Output = Result<RtcTransport, ResolveTransportError>>>>;

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

/// How a `Publish` intent failed to resolve, so the facade can tell "your
/// homeserver advertises no SFU" from "the SFU refused your token". Both
/// used to collapse into one `DriverError` at the [`TransportResolver`]
/// boundary.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ResolveTransportError {
    #[error("{0}")]
    NoTransport(DriverError),
    #[error("{0}")]
    TokenRefused(DriverError),
}

impl From<ResolveTransportError> for JoinError {
    fn from(error: ResolveTransportError) -> Self {
        match error {
            ResolveTransportError::NoTransport(e) => JoinError::NoTransport(e),
            ResolveTransportError::TokenRefused(e) => JoinError::TokenRefused(e),
        }
    }
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

    /// How far the last *failed* join got. The abort resets the machine to
    /// `NotJoined`, so without this the step-by-step flags that say which
    /// step failed would be gone by the time `join()` returns.
    pub fn last_join_progress(&self) -> JoinStatus {
        self.inner
            .machine
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_join_progress()
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

/// The pump is gone (the manager was dropped, or its task ended): typed so
/// the facade can report it as `DisconnectCause::ManagerStopped` rather than
/// string-matching an `Other`.
fn stopped() -> DriverError {
    DriverError::Stopped
}

impl Drop for OwnMembershipManager {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}
