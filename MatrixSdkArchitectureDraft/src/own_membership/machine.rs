//! The pure own-membership policy: states, inputs, actions. `now` is a
//! parameter, nothing here awaits or touches the driver — the pump executes
//! the returned actions and feeds their outcomes back in. Every timing rule
//! (restart cadence, half-life refresh, "must have fired", re-probe,
//! self-heal, delegation fallback) is a unit test below.

use super::ResolveTransportError;
use super::wire::{self, Route, WireContext};
use super::{
    ConnectedStatus, DEFAULT_DEGRADED_LIFETIME_MS, DelayedLeaveOutcome, JoinError, JoinParams,
    JoinStatus, KeepAlive, LeaveError, LeaveStatus, MAX_STICKY_DURATION_MS, MembershipPublication,
    OwnIdentity, RosterPresence, Status,
};
use crate::driver::{DriverError, SendEventResponse};
use crate::session::{
    ElementCallCompat, JoinExclusionReason, LEGACY_SLOT_ID, SessionSnapshot, SlotState,
};
use crate::types::{
    DeviceAttribution, LeaveReason, Member, MemberTransports, RtcTransport, TransportIntent,
};
use serde_json::{Value, json};
use tokio::sync::oneshot;

/// Restart the delayed leave this many times per delay period.
const RESTARTS_PER_TIMEOUT: u64 = 3;
/// Re-probe an *unexplained* delayed-event refusal this often.
pub(crate) const DELAYED_LEAVE_PROBE_INTERVAL_MS: u64 = 5 * 60 * 1000;
/// MSC4195: a delegated delayed event should have a delay of at least 1 h.
pub(crate) const DELEGATION_MIN_DELAY_MS: u64 = 60 * 60 * 1000;
/// Re-publish our membership when it vanished from the roster (§3.7).
/// One line to turn off if it misbehaves in the field.
const SELF_HEAL: bool = true;

pub(crate) enum Input {
    Join {
        member_id: String,
        intent: TransportIntent,
        params: JoinParams,
        reply: oneshot::Sender<Result<(), JoinError>>,
    },
    Leave {
        reason: Option<LeaveReason>,
        reply: oneshot::Sender<Result<(), LeaveError>>,
    },
    Session(SessionSnapshot),
    Wake,
    Outcome(Outcome),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum SendKind {
    Join,
    Refresh,
    Heal,
    Leave,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Action {
    ResolveTransport {
        member_id: String,
        intent: TransportIntent,
    },
    ArmDelayedLeave {
        route: Route,
        delay_ms: u64,
    },
    SendMembership {
        route: Route,
        kind: SendKind,
    },
    RestartDelayedLeave {
        delay_id: String,
    },
    CancelDelayedLeave {
        delay_id: String,
    },
    Delegate {
        delay_id: String,
        member: Value,
    },
}

#[derive(Debug)]
pub(crate) enum Outcome {
    TransportResolved(Result<RtcTransport, ResolveTransportError>),
    DelayedArmed(Result<String, DriverError>),
    MembershipSent {
        kind: SendKind,
        result: Result<SendEventResponse, DriverError>,
    },
    Restarted(Result<(), DriverError>),
    Cancelled(Result<(), DriverError>),
    Delegated(Result<(), DriverError>),
}

/// What this homeserver has told us about MSC4140 — learned by trying.
#[derive(Clone, Copy, Debug, PartialEq)]
enum DelayedLeaveSupport {
    Unknown,
    Supported,
    Unsupported { last_probe_ms: u64, permanent: bool },
}

/// `Unsupported | Unauthorized` = "this homeserver will never do it".
fn classify_refusal(error: &DriverError, now: u64) -> DelayedLeaveSupport {
    DelayedLeaveSupport::Unsupported {
        last_probe_ms: now,
        permanent: matches!(
            error,
            DriverError::Unsupported(_) | DriverError::Unauthorized(_)
        ),
    }
}

/// Everything decided when the join starts, frozen for the participation.
#[derive(Clone, Debug)]
struct JoinPlan {
    member: Member,
    params: JoinParams,
    join_content: Value,
    /// Chosen before the first publish, never moved (MSC4354 "last to expire
    /// wins" ignores a shorter refresh).
    published_lifetime_ms: u64,
    keep_alive_timeout_ms: u64,
    /// MSC3401 `created_ts`, pinned at the join.
    created_ts: u64,
    join_event_id: Option<String>,
}

/// The armed dead man's switch.
#[derive(Clone, Debug)]
struct DelayedLeave {
    delay_id: String,
    timeout_ms: u64,
    last_restart_ms: u64,
    next_restart_at: u64,
}

impl DelayedLeave {
    fn armed(delay_id: String, timeout_ms: u64, now: u64) -> Self {
        Self {
            delay_id,
            timeout_ms,
            last_restart_ms: now,
            next_restart_at: now + timeout_ms / RESTARTS_PER_TIMEOUT,
        }
    }

    /// A delay cannot still be pending once its full period elapsed since
    /// the last successful restart — the only leak-free moment to re-arm.
    fn must_have_fired(&self, now: u64) -> bool {
        now.saturating_sub(self.last_restart_ms) > self.timeout_ms
    }

    /// The first instant [`Self::must_have_fired`] is true for a restart at
    /// `last_restart_ms` — what the wake schedule and `fires_at_ts` use.
    fn expires_at(&self, last_restart_ms: u64) -> u64 {
        last_restart_ms + self.timeout_ms + 1
    }
}

/// The armed delay is gone because its period elapsed: record it so
/// `status()` can project [`super::KeepAlive::Expired`], and drop the id (a
/// fired delay cannot be restarted). A replacement is armed by the caller.
fn note_expired(c: &mut Connected, now: u64) {
    c.delayed = None;
    c.keep_alive_expired_since = Some(now);
    // Superseded: "we are probably out" is the stronger statement.
    c.keep_alive_failing_since = None;
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum JoinStage {
    Resolving,
    Arming,
    Sending,
    Delegating,
}

struct Joining {
    flags: JoinStatus,
    member_id: String,
    intent: TransportIntent,
    params: JoinParams,
    plan: Option<JoinPlan>,
    delayed: Option<DelayedLeave>,
    stage: JoinStage,
    reply: Option<oneshot::Sender<Result<(), JoinError>>>,
}

/// While connected, everything that can go *wrong* and later come right
/// again is state, not a log line: a failing restart, a failing refresh, a
/// membership that vanished from the roster. `status()` is a pure projection
/// over these fields, so the moment one of them changes the facade's
/// `on_status_change` fires — which is the whole point (a failing keep-alive
/// used to mutate nothing observable, see `ErrorSurfaceAnalysis.md` §3.1).
struct Connected {
    plan: JoinPlan,
    delayed: Option<DelayedLeave>,
    sticky_sent_at: u64,
    refresh_retry_at: Option<u64>,
    delegated_at: Option<u64>,
    /// Our id was in the roster at least once — before that, an absence is
    /// the echo still in flight, not a vanished membership.
    seen_in_roster: bool,
    healed_at: Option<u64>,
    /// Start of the current *run* of failing restarts — set on the first
    /// failure, cleared by the first success, so a UI can say "failing for
    /// 12 s" instead of "failed once, some time".
    keep_alive_failing_since: Option<u64>,
    last_restart_error: Option<String>,
    /// The delay's full period elapsed with no successful restart, so the
    /// homeserver has in all likelihood published our leave already and a
    /// replacement is being armed. Cleared when one is.
    keep_alive_expired_since: Option<u64>,
    /// Start of the current run of failing sticky refreshes; follows
    /// `refresh_retry_at`.
    refresh_failing_since: Option<u64>,
    last_refresh_error: Option<String>,
    /// Our membership was in the roster and is not any more: right now
    /// nobody sees us.
    roster_missing_since: Option<u64>,
    /// When the self-heal re-sent the membership; we are awaiting its echo.
    roster_republished_at: Option<u64>,
    /// The membership is on the server but the session refuses to project
    /// it. The self-heal deliberately does not fix this (see
    /// [`Machine::heal_if_vanished`]) — only the room state that caused the
    /// exclusion changing can.
    roster_excluded: Option<JoinExclusionReason>,
}

struct Leaving {
    flags: LeaveStatus,
    connected: Connected,
    reply: Option<oneshot::Sender<Result<(), LeaveError>>>,
}

enum State {
    NotJoined,
    Joining(Joining),
    Connected(Connected),
    Leaving(Leaving),
}

pub(crate) struct Machine {
    room_id: String,
    slot_id: String,
    own: OwnIdentity,
    compat: ElementCallCompat,
    state: State,
    /// Persists across joins of this manager.
    support: DelayedLeaveSupport,
    snapshot: Option<SessionSnapshot>,
    /// The step-by-step flags of the last aborted join, kept because
    /// `abort_join` resets the state and the facade reports them as
    /// `DisconnectCause::JoinFailed { progress, .. }`.
    last_join_progress: JoinStatus,
}

impl Machine {
    pub(crate) fn new(
        room_id: String,
        slot_id: String,
        own: OwnIdentity,
        compat: ElementCallCompat,
    ) -> Self {
        Self {
            room_id,
            slot_id,
            own,
            compat,
            state: State::NotJoined,
            support: DelayedLeaveSupport::Unknown,
            snapshot: None,
            last_join_progress: JoinStatus::default(),
        }
    }

    pub(crate) fn step(&mut self, input: Input, now: u64) -> Vec<Action> {
        match input {
            Input::Join {
                member_id,
                intent,
                params,
                reply,
            } => self.on_join(member_id, intent, params, reply, now),
            Input::Leave { reason, reply } => self.on_leave(reason, reply, now),
            Input::Session(snapshot) => self.on_session(snapshot, now),
            Input::Wake => self.on_wake(now),
            Input::Outcome(outcome) => self.on_outcome(outcome, now),
        }
    }

    /// Earliest of: delayed-leave restart, sticky refresh, delayed-event
    /// re-probe. `None` unless connected.
    pub(crate) fn next_wake_ts(&self) -> Option<u64> {
        let State::Connected(c) = &self.state else {
            return None;
        };
        let mut candidates = vec![self.refresh_due_at(c)];
        if let Some(d) = &c.delayed
            && c.delegated_at.is_none()
        {
            candidates.push(d.next_restart_at);
            // The instant the delay *must* have fired. Without it the
            // transition to `KeepAlive::Expired` would only be noticed at
            // the next restart beat, i.e. up to a third of the delay late.
            candidates.push(d.expires_at(d.last_restart_ms));
        }
        if c.delayed.is_none()
            && let Some(probe) = self.probe_due_at()
        {
            candidates.push(probe);
        }
        candidates.into_iter().min()
    }

    pub(crate) fn status(&self) -> Status {
        match &self.state {
            State::NotJoined => Status::NotJoined,
            State::Joining(j) => Status::Joining(j.flags),
            State::Connected(c) => Status::Connected(self.connected_status(c)),
            State::Leaving(l) => Status::Leaving(l.flags),
        }
    }

    pub(crate) fn debug_json(&self) -> Value {
        let (state, plan, delayed) = match &self.state {
            State::NotJoined => ("NotJoined", None, None),
            State::Joining(j) => ("Joining", j.plan.as_ref(), j.delayed.as_ref()),
            State::Connected(c) => ("Connected", Some(&c.plan), c.delayed.as_ref()),
            State::Leaving(l) => (
                "Leaving",
                Some(&l.connected.plan),
                l.connected.delayed.as_ref(),
            ),
        };
        json!({
            "state": state,
            "status": format!("{:?}", self.status()),
            "member_id": plan.map(|p| p.member.member_id.clone()),
            "join_event_id": plan.and_then(|p| p.join_event_id.clone()),
            "published_lifetime_ms": plan.map(|p| p.published_lifetime_ms),
            "keep_alive_timeout_ms": plan.map(|p| p.keep_alive_timeout_ms),
            "delay_id": delayed.map(|d| d.delay_id.clone()),
            "last_restart_ms": delayed.map(|d| d.last_restart_ms),
            "delayed_leave_support": format!("{:?}", self.support),
            "next_wake_ts": self.next_wake_ts(),
            "compat": format!("{:?}", self.compat),
        })
    }

    /// A **pure projection** of [`Connected`] — no clock, no inference. In
    /// particular the `Armed -> Expired` transition is performed by
    /// [`Self::on_wake`] (which does have a `now`) and merely *read* here;
    /// deducing it from a clock inside `status()` would make the value
    /// depend on when it was asked for rather than on what happened.
    fn connected_status(&self, c: &Connected) -> ConnectedStatus {
        ConnectedStatus {
            keep_alive: self.keep_alive_status(c),
            membership: MembershipPublication {
                lifetime_ms: c.plan.published_lifetime_ms,
                last_published_ts: c.sticky_sent_at,
                expires_at_ts: c.sticky_sent_at + c.plan.published_lifetime_ms,
                refresh_failing_since_ts: c.refresh_failing_since,
                last_refresh_error: c.last_refresh_error.clone(),
            },
            roster: match (c.roster_excluded, c.roster_missing_since, c.seen_in_roster) {
                (Some(reason), _, _) => RosterPresence::Excluded { reason },
                (None, Some(since_ts), _) => RosterPresence::Missing {
                    since_ts,
                    republished_at_ts: c.roster_republished_at,
                },
                (None, None, true) => RosterPresence::Present,
                (None, None, false) => RosterPresence::AwaitingEcho,
            },
        }
    }

    fn keep_alive_status(&self, c: &Connected) -> KeepAlive {
        match (&c.delayed, c.delegated_at) {
            // Delegated first: after delegation we stop restarting on
            // purpose, so none of the failure states can apply.
            (Some(d), Some(delegated_at_ts)) => KeepAlive::Delegated {
                delegated_at_ts,
                earliest_fire_ts: delegated_at_ts + d.timeout_ms,
            },
            (Some(d), None) => match (&c.keep_alive_failing_since, &c.last_restart_error) {
                (Some(since_ts), last_error) => KeepAlive::RestartFailing {
                    since_ts: *since_ts,
                    fires_at_ts: d.last_restart_ms + d.timeout_ms,
                    last_error: last_error.clone().unwrap_or_default(),
                },
                (None, _) => KeepAlive::Armed {
                    delay_ms: d.timeout_ms,
                    last_restart_ts: d.last_restart_ms,
                    fires_at_ts: d.last_restart_ms + d.timeout_ms,
                },
            },
            (None, _) => match c.keep_alive_expired_since {
                Some(since_ts) => KeepAlive::Expired { since_ts },
                // The tri-state the homeserver taught us, which used to
                // reach nothing but `debug_snapshot` (analysis §3.8).
                None => {
                    let next_probe_ts = self.probe_due_at();
                    KeepAlive::Unavailable {
                        permanent: next_probe_ts.is_none(),
                        next_probe_ts,
                    }
                }
            },
        }
    }

    // ---- helpers ---------------------------------------------------------

    fn is_legacy_slot(&self) -> bool {
        self.slot_id == LEGACY_SLOT_ID
    }

    fn slot_closed(&self) -> bool {
        !self.is_legacy_slot()
            && self
                .snapshot
                .as_ref()
                .is_some_and(|s| s.slot_state == Some(SlotState::Closed))
    }

    fn wire_ctx<'a>(&'a self, plan: &'a JoinPlan) -> WireContext<'a> {
        WireContext {
            compat: self.compat,
            own: &self.own,
            room_id: &self.room_id,
            application_type: &plan.params.application_type,
            created_ts: plan.created_ts,
        }
    }

    fn membership_route(&self, plan: &JoinPlan, spec: &Value, now: u64) -> Route {
        wire::route(&self.wire_ctx(plan), spec, plan.published_lifetime_ms, now)
    }

    fn arm_action(&self, plan: &JoinPlan, now: u64) -> Action {
        let spec = wire::leave_content(
            &self.slot_id,
            &plan.member.member_id,
            &LeaveReason::delayed_leave(),
        );
        Action::ArmDelayedLeave {
            route: self.membership_route(plan, &spec, now),
            delay_ms: plan.keep_alive_timeout_ms,
        }
    }

    fn refresh_due_at(&self, c: &Connected) -> u64 {
        c.refresh_retry_at
            .unwrap_or(c.sticky_sent_at + c.plan.published_lifetime_ms / 2)
    }

    /// When arming a delayed leave is worth trying again while none is armed.
    fn probe_due_at(&self) -> Option<u64> {
        match self.support {
            DelayedLeaveSupport::Unknown | DelayedLeaveSupport::Supported => Some(0),
            DelayedLeaveSupport::Unsupported {
                permanent: true, ..
            } => None,
            DelayedLeaveSupport::Unsupported {
                last_probe_ms,
                permanent: false,
            } => Some(last_probe_ms + DELAYED_LEAVE_PROBE_INTERVAL_MS),
        }
    }

    // ---- join --------------------------------------------------------------

    fn on_join(
        &mut self,
        member_id: String,
        intent: TransportIntent,
        params: JoinParams,
        reply: oneshot::Sender<Result<(), JoinError>>,
        now: u64,
    ) -> Vec<Action> {
        if !matches!(self.state, State::NotJoined) {
            let _ = reply.send(Err(JoinError::AlreadyJoined));
            return Vec::new();
        }
        if let Err(message) = params.validate(&member_id, &intent) {
            let _ = reply.send(Err(JoinError::InvalidParams(message)));
            return Vec::new();
        }
        if self.slot_closed() {
            let _ = reply.send(Err(JoinError::SlotClosed));
            return Vec::new();
        }
        let mut params = params;
        if params.sticky_duration_ms > MAX_STICKY_DURATION_MS {
            log::warn!(
                "[{}] sticky_duration_ms {} clamped to {MAX_STICKY_DURATION_MS} (homeservers honour no more)",
                self.room_id,
                params.sticky_duration_ms
            );
            params.sticky_duration_ms = MAX_STICKY_DURATION_MS;
        }
        let flags = JoinStatus {
            has_fetched_initial_member_list: self.snapshot.as_ref().is_some_and(|s| s.seeded),
            ..JoinStatus::default()
        };
        log::info!("[{}/{}] joining as {member_id}", self.room_id, self.slot_id);
        let mut joining = Joining {
            flags,
            member_id,
            intent: intent.clone(),
            params,
            plan: None,
            delayed: None,
            stage: JoinStage::Resolving,
            reply: Some(reply),
        };
        let actions = match intent {
            TransportIntent::Publish(_) => {
                joining.flags.has_fetched_transports = true;
                vec![Action::ResolveTransport {
                    member_id: joining.member_id.clone(),
                    intent,
                }]
            }
            TransportIntent::ReceiveOnly { .. } => {
                joining.flags.has_fetched_transports = true;
                joining.flags.has_created_transport_token = true;
                self.plan_and_arm(&mut joining, now)
            }
        };
        self.state = State::Joining(joining);
        actions
    }

    /// Step 2 + 3 of the join: freeze the plan, arm the delayed leave (or,
    /// on a homeserver known to refuse, go straight to the degraded publish).
    fn plan_and_arm(&self, joining: &mut Joining, now: u64) -> Vec<Action> {
        let params = &joining.params;
        let transports = match &joining.intent {
            TransportIntent::Publish(t) => MemberTransports {
                published: vec![t.clone()],
                can_subscribe: vec![t.transport_type.clone()],
            },
            TransportIntent::ReceiveOnly { can_subscribe } => MemberTransports {
                published: Vec::new(),
                can_subscribe: can_subscribe.clone(),
            },
        };
        let member = Member {
            member_id: joining.member_id.clone(),
            user_id: self.own.user_id.clone(),
            device_id: Some(self.own.device_id.clone()),
            device_attribution: DeviceAttribution::Verified,
            membership_ts: Some(now),
            display_name: None,
            avatar_url: None,
            intent: params.intent.clone(),
            application_type: Some(params.application_type.clone()),
            transports,
        };
        let join_content = wire::join_content(
            &self.slot_id,
            &member.member_id,
            &params.application_type,
            params.intent.as_deref(),
            &joining.intent,
        );
        let keep_alive_timeout_ms = if params.delegate_delayed_leave {
            params.keep_alive_timeout_ms.max(DELEGATION_MIN_DELAY_MS)
        } else {
            params.keep_alive_timeout_ms
        };
        let plan = JoinPlan {
            member,
            params: params.clone(),
            join_content,
            published_lifetime_ms: params.sticky_duration_ms,
            keep_alive_timeout_ms,
            created_ts: now,
            join_event_id: None,
        };
        if matches!(
            self.support,
            DelayedLeaveSupport::Unsupported {
                permanent: true,
                ..
            }
        ) {
            joining.stage = JoinStage::Sending;
            self.degrade(
                joining.plan.insert(plan),
                "the homeserver refused delayed events before",
            );
            vec![self.send_join_action(joining, now)]
        } else {
            joining.stage = JoinStage::Arming;
            let action = self.arm_action(&plan, now);
            joining.plan = Some(plan);
            vec![action]
        }
    }

    fn degrade(&self, plan: &mut JoinPlan, why: &str) {
        let degraded = plan
            .params
            .degraded_lifetime_ms
            .unwrap_or(DEFAULT_DEGRADED_LIFETIME_MS);
        log::warn!(
            "[{}] no dead man's switch ({why}); publishing with the degraded lifetime {degraded}ms instead of {}ms",
            self.room_id,
            plan.published_lifetime_ms
        );
        plan.published_lifetime_ms = degraded;
    }

    fn send_join_action(&self, joining: &Joining, now: u64) -> Action {
        let plan = joining
            .plan
            .as_ref()
            .expect("plan exists once arming started");
        Action::SendMembership {
            route: self.membership_route(plan, &plan.join_content, now),
            kind: SendKind::Join,
        }
    }

    fn finish_join(&mut self, joining: Joining, now: u64) -> Vec<Action> {
        let mut joining = joining;
        let plan = joining
            .plan
            .take()
            .expect("plan exists at the end of a join");
        if let Some(reply) = joining.reply.take() {
            let _ = reply.send(Ok(()));
        }
        log::info!(
            "[{}/{}] joined as {}",
            self.room_id,
            self.slot_id,
            plan.member.member_id
        );
        self.state = State::Connected(Connected {
            plan,
            delayed: joining.delayed,
            sticky_sent_at: now,
            refresh_retry_at: None,
            delegated_at: if joining.flags.has_delegated_delayed_event {
                Some(now)
            } else {
                None
            },
            seen_in_roster: false,
            healed_at: None,
            keep_alive_failing_since: None,
            last_restart_error: None,
            keep_alive_expired_since: None,
            refresh_failing_since: None,
            last_refresh_error: None,
            roster_missing_since: None,
            roster_republished_at: None,
            roster_excluded: None,
        });
        // The slot may have closed while we were joining.
        if self.slot_closed() {
            self.begin_leave(LeaveReason::slot_closed(), None, now)
        } else {
            Vec::new()
        }
    }

    pub(crate) fn last_join_progress(&self) -> JoinStatus {
        self.last_join_progress
    }

    fn abort_join(&mut self, joining: Joining, error: JoinError) -> Vec<Action> {
        let mut joining = joining;
        log::warn!("[{}/{}] join failed: {error}", self.room_id, self.slot_id);
        self.last_join_progress = joining.flags;
        if let Some(reply) = joining.reply.take() {
            let _ = reply.send(Err(error));
        }
        self.state = State::NotJoined;
        Vec::new()
    }

    // ---- leave -------------------------------------------------------------

    fn on_leave(
        &mut self,
        reason: Option<LeaveReason>,
        reply: oneshot::Sender<Result<(), LeaveError>>,
        now: u64,
    ) -> Vec<Action> {
        if !matches!(self.state, State::Connected(_)) {
            let _ = reply.send(Err(LeaveError::NotJoined));
            return Vec::new();
        }
        self.begin_leave(reason.unwrap_or_else(LeaveReason::leave), Some(reply), now)
    }

    fn begin_leave(
        &mut self,
        reason: LeaveReason,
        reply: Option<oneshot::Sender<Result<(), LeaveError>>>,
        now: u64,
    ) -> Vec<Action> {
        let State::Connected(connected) = std::mem::replace(&mut self.state, State::NotJoined)
        else {
            unreachable!("begin_leave is only called while connected")
        };
        log::info!(
            "[{}/{}] leaving ({})",
            self.room_id,
            self.slot_id,
            reason.code
        );
        let spec = wire::leave_content(&self.slot_id, &connected.plan.member.member_id, &reason);
        let route = self.membership_route(&connected.plan, &spec, now);
        self.state = State::Leaving(Leaving {
            flags: LeaveStatus::default(),
            connected,
            reply,
        });
        vec![Action::SendMembership {
            route,
            kind: SendKind::Leave,
        }]
    }

    fn finish_leave(&mut self, leaving: Leaving) -> Vec<Action> {
        let mut leaving = leaving;
        if let Some(reply) = leaving.reply.take() {
            let _ = reply.send(Ok(()));
        }
        log::info!("[{}/{}] left", self.room_id, self.slot_id);
        self.state = State::NotJoined;
        Vec::new()
    }

    // ---- session -----------------------------------------------------------

    fn on_session(&mut self, snapshot: SessionSnapshot, now: u64) -> Vec<Action> {
        self.snapshot = Some(snapshot);
        let seeded = self.snapshot.as_ref().is_some_and(|s| s.seeded);
        match std::mem::replace(&mut self.state, State::NotJoined) {
            State::Joining(mut j) => {
                j.flags.has_fetched_initial_member_list = seeded;
                self.state = State::Joining(j);
                Vec::new()
            }
            State::Connected(c) if self.slot_closed() => {
                log::info!("[{}/{}] slot closed under us", self.room_id, self.slot_id);
                self.state = State::Connected(c);
                self.begin_leave(LeaveReason::slot_closed(), None, now)
            }
            State::Connected(mut c) => {
                let actions = self.heal_if_vanished(&mut c, seeded, now);
                self.state = State::Connected(c);
                actions
            }
            other => {
                self.state = other;
                Vec::new()
            }
        }
    }

    /// Self-heal (§3.7): our id in neither `members` nor
    /// `excluded_candidates` after it was seen once → re-publish, at most once
    /// per keep-alive; re-arm the delay if it must have fired.
    fn heal_if_vanished(&self, c: &mut Connected, seeded: bool, now: u64) -> Vec<Action> {
        let snapshot = self.snapshot.as_ref().expect("stored by on_session");
        let id = &c.plan.member.member_id;
        if snapshot.members.iter().any(|m| &m.member_id == id) {
            c.seen_in_roster = true;
            // Present: every roster complaint clears at once, including the
            // echo we were waiting for after a heal.
            c.roster_missing_since = None;
            c.roster_republished_at = None;
            c.roster_excluded = None;
            return Vec::new();
        }
        if let Some((_, reason)) = snapshot
            .excluded_candidates
            .iter()
            .find(|(m, _)| &m.member_id == id)
        {
            log::warn!(
                "[{}] our membership {id} is excluded from the session: {reason:?}",
                self.room_id
            );
            // Excluded is *not* missing: the event is on the server and
            // re-sending it would change nothing, so it gets its own state
            // (and its own impairment) with a different remedy.
            c.roster_excluded = Some(*reason);
            c.roster_missing_since = None;
            c.roster_republished_at = None;
            return Vec::new();
        }
        c.roster_excluded = None;
        if !c.seen_in_roster || !seeded {
            // Never seen yet: the echo is still in flight, not a vanishing.
            return Vec::new();
        }
        // Vanished. Recorded whether or not the self-heal is allowed to act,
        // because "right now nobody sees us" is true either way.
        c.roster_missing_since.get_or_insert(now);
        if !SELF_HEAL {
            return Vec::new();
        }
        if c.healed_at
            .is_some_and(|t| now.saturating_sub(t) < c.plan.keep_alive_timeout_ms)
        {
            return Vec::new();
        }
        log::warn!(
            "[{}] our membership {id} vanished from the roster; re-publishing",
            self.room_id
        );
        c.healed_at = Some(now);
        c.roster_republished_at = Some(now);
        let mut actions = vec![Action::SendMembership {
            route: self.membership_route(&c.plan, &c.plan.join_content, now),
            kind: SendKind::Heal,
        }];
        if c.delegated_at.is_none() && c.delayed.as_ref().is_some_and(|d| d.must_have_fired(now)) {
            note_expired(c, now);
            actions.push(self.arm_action(&c.plan, now));
        }
        actions
    }

    // ---- heartbeat ---------------------------------------------------------

    fn on_wake(&mut self, now: u64) -> Vec<Action> {
        // The one place the `KeepAlive::Expired` transition happens.
        // `status()` has no clock and must stay a pure projection, so the
        // pump's wake — scheduled for exactly this instant by
        // `next_wake_ts` — is what turns "armed" into "expired".
        // `delegated_at` guards the footgun of analysis §3.1: after
        // delegation we deliberately stop restarting, so a frozen
        // `last_restart_ms` is health, not expiry.
        if let State::Connected(c) = &mut self.state
            && c.delegated_at.is_none()
            && c.delayed.as_ref().is_some_and(|d| d.must_have_fired(now))
        {
            log::warn!(
                "[{}] the delayed leave outlived its {}ms delay with no successful restart; it fired — arming a replacement",
                self.room_id,
                c.delayed.as_ref().expect("checked").timeout_ms
            );
            note_expired(c, now);
            // Falls through: with no delay armed and the homeserver known to
            // accept them, the body below arms the replacement.
        }
        let State::Connected(c) = &self.state else {
            return Vec::new();
        };
        let mut actions = Vec::new();
        if self.refresh_due_at(c) <= now {
            log::debug!("[{}] refreshing the sticky membership", self.room_id);
            actions.push(Action::SendMembership {
                route: self.membership_route(&c.plan, &c.plan.join_content, now),
                kind: SendKind::Refresh,
            });
        }
        match &c.delayed {
            Some(d) if c.delegated_at.is_none() && d.next_restart_at <= now => {
                log::trace!(
                    "[{}] heartbeat: restarting delayed leave {}",
                    self.room_id,
                    d.delay_id
                );
                actions.push(Action::RestartDelayedLeave {
                    delay_id: d.delay_id.clone(),
                });
            }
            None if self.probe_due_at().is_some_and(|t| t <= now) => {
                log::debug!("[{}] arming a delayed leave (none armed)", self.room_id);
                actions.push(self.arm_action(&c.plan, now));
            }
            _ => {}
        }
        actions
    }

    // ---- outcomes ----------------------------------------------------------

    fn on_outcome(&mut self, outcome: Outcome, now: u64) -> Vec<Action> {
        match std::mem::replace(&mut self.state, State::NotJoined) {
            State::Joining(joining) => self.on_join_outcome(joining, outcome, now),
            State::Connected(connected) => {
                self.state = State::Connected(connected);
                self.on_connected_outcome(outcome, now)
            }
            State::Leaving(leaving) => self.on_leave_outcome(leaving, outcome, now),
            State::NotJoined => {
                log::debug!(
                    "[{}] outcome {outcome:?} arrived while not joined; ignored",
                    self.room_id
                );
                Vec::new()
            }
        }
    }

    fn on_join_outcome(&mut self, mut joining: Joining, outcome: Outcome, now: u64) -> Vec<Action> {
        match (joining.stage, outcome) {
            (JoinStage::Resolving, Outcome::TransportResolved(Ok(transport))) => {
                joining.flags.has_created_transport_token = true;
                joining.intent = TransportIntent::Publish(transport);
                let actions = self.plan_and_arm(&mut joining, now);
                self.state = State::Joining(joining);
                actions
            }
            (JoinStage::Resolving, Outcome::TransportResolved(Err(e))) => {
                self.abort_join(joining, e.into())
            }
            (JoinStage::Arming, Outcome::DelayedArmed(result)) => {
                let plan = joining.plan.as_mut().expect("plan exists while arming");
                match result {
                    Ok(delay_id) => {
                        self.support = DelayedLeaveSupport::Supported;
                        joining.delayed = Some(DelayedLeave::armed(
                            delay_id,
                            plan.keep_alive_timeout_ms,
                            now,
                        ));
                        joining.flags.has_sent_delayed_leave_event = true;
                    }
                    Err(e) => {
                        self.support = classify_refusal(&e, now);
                        self.degrade(
                            plan,
                            &format!("the delayed leave could not be scheduled: {e}"),
                        );
                    }
                }
                joining.stage = JoinStage::Sending;
                let action = self.send_join_action(&joining, now);
                self.state = State::Joining(joining);
                vec![action]
            }
            (
                JoinStage::Sending,
                Outcome::MembershipSent {
                    kind: SendKind::Join,
                    result,
                },
            ) => match result {
                Ok(response) => {
                    joining.flags.has_sent_member_join_event = true;
                    joining.flags.has_started_heartbeat = true;
                    let plan = joining.plan.as_mut().expect("plan exists while sending");
                    plan.join_event_id = response.event_id;
                    let delegate = plan.params.delegate_delayed_leave
                        && joining.delayed.is_some()
                        && self.compat != ElementCallCompat::StateEvents;
                    if delegate {
                        joining.stage = JoinStage::Delegating;
                        let action = Action::Delegate {
                            delay_id: joining.delayed.as_ref().expect("checked").delay_id.clone(),
                            member: plan.join_content["member"].clone(),
                        };
                        self.state = State::Joining(joining);
                        vec![action]
                    } else {
                        self.finish_join(joining, now)
                    }
                }
                Err(e) => {
                    if joining.delayed.is_some() {
                        log::warn!(
                            "[{}] the armed delayed leave is left to fire; it clears nothing we published",
                            self.room_id
                        );
                    }
                    self.abort_join(joining, JoinError::Driver(e))
                }
            },
            (JoinStage::Delegating, Outcome::Delegated(result)) => {
                match result {
                    Ok(()) => {
                        joining.flags.has_delegated_delayed_event = true;
                        log::info!("[{}] delayed leave delegated to the SFU", self.room_id);
                    }
                    Err(e) => log::warn!(
                        "[{}] delegation failed ({e}); restarting the {}ms delayed leave ourselves",
                        self.room_id,
                        joining.plan.as_ref().map_or(0, |p| p.keep_alive_timeout_ms)
                    ),
                }
                self.finish_join(joining, now)
            }
            (stage, outcome) => {
                log::error!(
                    "[{}] unexpected outcome {outcome:?} in join stage {stage:?}",
                    self.room_id
                );
                self.state = State::Joining(joining);
                Vec::new()
            }
        }
    }

    fn on_connected_outcome(&mut self, outcome: Outcome, now: u64) -> Vec<Action> {
        let State::Connected(c) = &mut self.state else {
            unreachable!()
        };
        let room_id = self.room_id.clone();
        match outcome {
            Outcome::Restarted(Ok(())) => {
                if let Some(d) = &mut c.delayed {
                    d.last_restart_ms = now;
                    d.next_restart_at = now + d.timeout_ms / RESTARTS_PER_TIMEOUT;
                }
                // The dead man's switch is healthy again: clearing these is
                // half of what makes the failure observable at all (the
                // other half is setting them below).
                c.keep_alive_failing_since = None;
                c.last_restart_error = None;
                Vec::new()
            }
            Outcome::Restarted(Err(e)) => {
                let Some(d) = &mut c.delayed else {
                    return Vec::new();
                };
                d.next_restart_at = now + d.timeout_ms / RESTARTS_PER_TIMEOUT;
                let fired = d.must_have_fired(now);
                // Recorded *before* anything else: this is the mutation that
                // turns a failing restart from a log line into a published
                // status change (`ErrorSurfaceAnalysis.md` §3.1). Set once
                // per run of failures, so the timestamp means "failing
                // since", not "failed last at".
                c.keep_alive_failing_since.get_or_insert(now);
                c.last_restart_error = Some(e.to_string());
                if fired {
                    log::warn!(
                        "[{room_id}] delayed leave was not restarted for longer than its delay; it fired — arming a replacement"
                    );
                    note_expired(c, now);
                    let plan = c.plan.clone();
                    return vec![self.arm_action(&plan, now)];
                }
                log::warn!(
                    "[{room_id}] failed to restart delayed leave {} ({e}); retrying on the next beat",
                    c.delayed.as_ref().expect("still armed").delay_id
                );
                Vec::new()
            }
            Outcome::DelayedArmed(Ok(delay_id)) => {
                if matches!(self.support, DelayedLeaveSupport::Unsupported { .. }) {
                    log::info!(
                        "[{room_id}] the homeserver accepts delayed events after all; dead man's switch armed"
                    );
                }
                self.support = DelayedLeaveSupport::Supported;
                c.delayed = Some(DelayedLeave::armed(
                    delay_id,
                    c.plan.keep_alive_timeout_ms,
                    now,
                ));
                // A replacement is armed: neither "expired" nor "failing"
                // is true any more.
                c.keep_alive_expired_since = None;
                c.keep_alive_failing_since = None;
                c.last_restart_error = None;
                Vec::new()
            }
            Outcome::DelayedArmed(Err(e)) => {
                log::warn!("[{room_id}] failed to arm a delayed leave: {e}");
                self.support = classify_refusal(&e, now);
                c.delayed = None;
                // No switch armed at all now: `Unavailable`, which carries
                // the refusal's permanence, replaces `Expired`.
                c.keep_alive_expired_since = None;
                Vec::new()
            }
            Outcome::MembershipSent {
                kind: SendKind::Refresh | SendKind::Heal,
                result: Ok(_),
            } => {
                c.sticky_sent_at = now;
                c.refresh_retry_at = None;
                c.refresh_failing_since = None;
                c.last_refresh_error = None;
                Vec::new()
            }
            Outcome::MembershipSent {
                kind: SendKind::Refresh | SendKind::Heal,
                result: Err(e),
            } => {
                log::warn!("[{room_id}] failed to refresh the membership ({e}); retrying soon");
                c.refresh_retry_at = Some(now + (c.plan.published_lifetime_ms / 10).max(1_000));
                // Follows `refresh_retry_at` exactly: while a retry is
                // pending the publication is failing, and the host can read
                // how long it has been and against what expiry.
                c.refresh_failing_since.get_or_insert(now);
                c.last_refresh_error = Some(e.to_string());
                Vec::new()
            }
            other => {
                log::debug!(
                    "[{room_id}] outcome {other:?} arrived after its join/leave ended; ignored"
                );
                Vec::new()
            }
        }
    }

    fn on_leave_outcome(
        &mut self,
        mut leaving: Leaving,
        outcome: Outcome,
        _now: u64,
    ) -> Vec<Action> {
        match outcome {
            Outcome::MembershipSent {
                kind: SendKind::Leave,
                result: Ok(_),
            } => {
                leaving.flags.leave_event_sent = true;
                match &leaving.connected.delayed {
                    Some(d) => {
                        let action = Action::CancelDelayedLeave {
                            delay_id: d.delay_id.clone(),
                        };
                        self.state = State::Leaving(leaving);
                        vec![action]
                    }
                    None => self.finish_leave(leaving),
                }
            }
            Outcome::MembershipSent {
                kind: SendKind::Leave,
                result: Err(e),
            } => {
                log::warn!(
                    "[{}] the leave event could not be sent ({e}); still joined",
                    self.room_id
                );
                if let Some(reply) = leaving.reply.take() {
                    let _ = reply.send(Err(LeaveError::Driver(e)));
                }
                self.state = State::Connected(leaving.connected);
                Vec::new()
            }
            Outcome::Cancelled(result) => {
                // Either way the leave is done — but say which, so a host
                // knows whether a stray delayed event may still land.
                leaving.flags.delayed_leave = Some(match result {
                    Ok(()) => DelayedLeaveOutcome::Cancelled,
                    Err(e) => {
                        log::debug!(
                            "[{}] delayed leave could not be cancelled ({e}); it most likely fired already",
                            self.room_id
                        );
                        DelayedLeaveOutcome::MayStillFire
                    }
                });
                leaving.connected.delayed = None;
                self.finish_leave(leaving)
            }
            other => {
                log::debug!("[{}] outcome {other:?} ignored while leaving", self.room_id);
                self.state = State::Leaving(leaving);
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::SendEventResponse;
    use serde_json::json;

    const ROOM: &str = "!room:x";
    const SLOT: &str = "m.call#ROOM";
    const T0: u64 = 1_700_000_000_000;

    fn own() -> OwnIdentity {
        OwnIdentity {
            user_id: "@me:x".into(),
            device_id: "DEV".into(),
        }
    }

    fn lk() -> RtcTransport {
        RtcTransport {
            transport_type: "livekit".into(),
            properties: json!({ "livekit_service_url": "https://lk" }),
        }
    }

    fn params() -> JoinParams {
        JoinParams {
            sticky_duration_ms: 240_000,
            keep_alive_timeout_ms: 15_000,
            ..JoinParams::new("m.call")
        }
    }

    fn seeded(members: Vec<&str>, slot: Option<SlotState>) -> SessionSnapshot {
        SessionSnapshot {
            room_id: ROOM.into(),
            slot_id: SLOT.into(),
            members: members
                .into_iter()
                .map(|id| Member {
                    member_id: id.into(),
                    user_id: "@me:x".into(),
                    device_id: Some("DEV".into()),
                    device_attribution: DeviceAttribution::Verified,
                    membership_ts: None,
                    display_name: None,
                    avatar_url: None,
                    intent: None,
                    application_type: None,
                    transports: Default::default(),
                })
                .collect(),
            slot_state: slot,
            seeded: true,
            ..Default::default()
        }
    }

    fn ok_sent() -> Result<SendEventResponse, DriverError> {
        Ok(SendEventResponse {
            event_id: Some("$join".into()),
            delay_id: None,
        })
    }

    struct H {
        m: Machine,
    }

    impl H {
        fn new(compat: ElementCallCompat) -> Self {
            let slot = if compat == ElementCallCompat::StateEvents {
                LEGACY_SLOT_ID
            } else {
                SLOT
            };
            Self {
                m: Machine::new(ROOM.into(), slot.into(), own(), compat),
            }
        }

        fn join(
            &mut self,
            intent: TransportIntent,
            params: JoinParams,
            now: u64,
        ) -> (Vec<Action>, oneshot::Receiver<Result<(), JoinError>>) {
            let (reply, rx) = oneshot::channel();
            (
                self.m.step(
                    Input::Join {
                        member_id: "m-1".into(),
                        intent,
                        params,
                        reply,
                    },
                    now,
                ),
                rx,
            )
        }

        fn leave(&mut self, now: u64) -> (Vec<Action>, oneshot::Receiver<Result<(), LeaveError>>) {
            let (reply, rx) = oneshot::channel();
            (
                self.m.step(
                    Input::Leave {
                        reason: None,
                        reply,
                    },
                    now,
                ),
                rx,
            )
        }

        fn outcome(&mut self, o: Outcome, now: u64) -> Vec<Action> {
            self.m.step(Input::Outcome(o), now)
        }

        /// A receive-only join with an accepting homeserver, connected at `now`.
        fn connected(&mut self, now: u64) {
            self.m.step(Input::Session(seeded(vec![], None)), now);
            let (a, mut rx) = self.join(
                TransportIntent::ReceiveOnly {
                    can_subscribe: vec!["livekit".into()],
                },
                params(),
                now,
            );
            assert!(matches!(a[0], Action::ArmDelayedLeave { .. }));
            let a = self.outcome(Outcome::DelayedArmed(Ok("delay-1".into())), now);
            assert!(matches!(
                a[0],
                Action::SendMembership {
                    kind: SendKind::Join,
                    ..
                }
            ));
            let a = self.outcome(
                Outcome::MembershipSent {
                    kind: SendKind::Join,
                    result: ok_sent(),
                },
                now,
            );
            assert!(a.is_empty());
            assert!(matches!(rx.try_recv(), Ok(Ok(()))));
            assert!(matches!(self.m.status(), Status::Connected(_)));
        }
    }

    fn sticky_duration(a: &Action) -> u64 {
        match a {
            Action::SendMembership {
                route: Route::Sticky { duration_ms, .. },
                ..
            }
            | Action::ArmDelayedLeave {
                route: Route::Sticky { duration_ms, .. },
                ..
            } => *duration_ms,
            other => panic!("not a sticky route: {other:?}"),
        }
    }

    fn content(a: &Action) -> &Value {
        match a {
            Action::SendMembership {
                route: Route::Sticky { content, .. },
                ..
            }
            | Action::ArmDelayedLeave {
                route: Route::Sticky { content, .. },
                ..
            }
            | Action::SendMembership {
                route: Route::State { content, .. },
                ..
            }
            | Action::ArmDelayedLeave {
                route: Route::State { content, .. },
                ..
            } => content,
            other => panic!("no content: {other:?}"),
        }
    }

    // -- refusal classification -------------------------------------------------

    /// `RateLimited` is explicitly transient: a homeserver that throttles us
    /// once must still be re-probed, or one busy minute would cost us the
    /// dead man's switch for the rest of the participation.
    #[test]
    fn only_unsupported_and_unauthorized_are_permanent_refusals() {
        let permanent = |e: &DriverError| {
            matches!(
                classify_refusal(e, T0),
                DelayedLeaveSupport::Unsupported {
                    permanent: true,
                    ..
                }
            )
        };
        assert!(permanent(&DriverError::Unsupported("404".into())));
        assert!(permanent(&DriverError::Unauthorized("403".into())));
        assert!(!permanent(&DriverError::Http("503".into())));
        assert!(!permanent(&DriverError::RateLimited {
            retry_after_ms: Some(2_000)
        }));
        assert!(!permanent(&DriverError::Stopped));
        assert!(!permanent(&DriverError::Other("?".into())));
    }

    // -- join ------------------------------------------------------------------

    #[test]
    fn join_refuses_a_closed_slot_but_not_the_legacy_slot_or_an_unsupplied_one() {
        let mut h = H::new(ElementCallCompat::Off);
        h.m.step(Input::Session(seeded(vec![], Some(SlotState::Closed))), T0);
        let (a, mut rx) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        assert!(a.is_empty());
        assert!(matches!(rx.try_recv(), Ok(Err(JoinError::SlotClosed))));
        assert_eq!(h.m.status(), Status::NotJoined);

        let mut h = H::new(ElementCallCompat::Off);
        let (a, _) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        assert!(
            matches!(a[0], Action::ArmDelayedLeave { .. }),
            "unsupplied slot state is no refusal"
        );

        let mut h = H::new(ElementCallCompat::StateEvents);
        h.m.step(
            Input::Session(SessionSnapshot {
                slot_state: Some(SlotState::Closed),
                ..seeded(vec![], None)
            }),
            T0,
        );
        let (a, _) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        assert!(matches!(a[0], Action::ArmDelayedLeave { .. }));
    }

    #[test]
    fn join_validates_params_and_refuses_a_second_join() {
        let mut h = H::new(ElementCallCompat::Off);
        let (_, mut rx) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams::new(""),
            T0,
        );
        assert!(matches!(
            rx.try_recv(),
            Ok(Err(JoinError::InvalidParams(_)))
        ));
        h.connected(T0);
        let (a, mut rx) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        assert!(a.is_empty());
        assert!(matches!(rx.try_recv(), Ok(Err(JoinError::AlreadyJoined))));
    }

    #[test]
    fn the_member_list_flag_mirrors_the_seeded_snapshot() {
        let mut h = H::new(ElementCallCompat::Off);
        h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        assert!(matches!(
            h.m.status(),
            Status::Joining(JoinStatus {
                has_fetched_initial_member_list: false,
                ..
            })
        ));
        h.m.step(Input::Session(seeded(vec![], None)), T0);
        assert!(matches!(
            h.m.status(),
            Status::Joining(JoinStatus {
                has_fetched_initial_member_list: true,
                ..
            })
        ));
    }

    #[test]
    fn publish_intent_resolves_the_transport_and_flips_both_transport_flags() {
        let mut h = H::new(ElementCallCompat::Off);
        let bare = RtcTransport {
            transport_type: "livekit".into(),
            properties: json!({}),
        };
        let (a, _) = h.join(TransportIntent::Publish(bare.clone()), params(), T0);
        assert_eq!(
            a,
            vec![Action::ResolveTransport {
                member_id: "m-1".into(),
                intent: TransportIntent::Publish(bare)
            }]
        );
        assert!(matches!(
            h.m.status(),
            Status::Joining(JoinStatus {
                has_fetched_transports: true,
                has_created_transport_token: false,
                ..
            })
        ));
        let a = h.outcome(Outcome::TransportResolved(Ok(lk())), T0);
        assert!(matches!(a[0], Action::ArmDelayedLeave { .. }));
        assert!(matches!(
            h.m.status(),
            Status::Joining(JoinStatus {
                has_created_transport_token: true,
                ..
            })
        ));
        h.outcome(Outcome::DelayedArmed(Ok("d".into())), T0);
        let a = h.outcome(Outcome::DelayedArmed(Ok("d".into())), T0);
        // (unexpected duplicate outcome is ignored) — now the join content
        // carries the *resolved* transport
        assert!(a.is_empty());
    }

    #[test]
    fn the_join_content_carries_the_resolved_transport() {
        let mut h = H::new(ElementCallCompat::Off);
        h.join(
            TransportIntent::Publish(RtcTransport {
                transport_type: "livekit".into(),
                properties: json!({}),
            }),
            params(),
            T0,
        );
        h.outcome(Outcome::TransportResolved(Ok(lk())), T0);
        let a = h.outcome(Outcome::DelayedArmed(Ok("d".into())), T0);
        assert_eq!(
            content(&a[0])["transports"]["published"][0]["livekit_service_url"],
            "https://lk"
        );
        assert_eq!(
            content(&a[0])["transports"]["can_subscribe"],
            json!(["livekit"])
        );
    }

    #[test]
    fn a_failed_resolver_returns_to_not_joined_keeping_which_step_failed() {
        // Discovery and token minting are different stories with different
        // remedies, and the abort keeps how far the join got.
        let mut h = H::new(ElementCallCompat::Off);
        let (_, mut rx) = h.join(TransportIntent::Publish(lk()), params(), T0);
        let a = h.outcome(
            Outcome::TransportResolved(Err(ResolveTransportError::TokenRefused(
                DriverError::Http("503".into()),
            ))),
            T0,
        );
        assert!(a.is_empty());
        assert!(matches!(rx.try_recv(), Ok(Err(JoinError::TokenRefused(_)))));
        assert_eq!(h.m.status(), Status::NotJoined);
        assert_eq!(h.m.next_wake_ts(), None);
        assert!(
            h.m.last_join_progress().has_fetched_transports,
            "the progress flags survive the abort"
        );
        assert!(!h.m.last_join_progress().has_created_transport_token);

        let mut h = H::new(ElementCallCompat::Off);
        let (_, mut rx) = h.join(TransportIntent::Publish(lk()), params(), T0);
        h.outcome(
            Outcome::TransportResolved(Err(ResolveTransportError::NoTransport(
                DriverError::Unsupported("none advertised".into()),
            ))),
            T0,
        );
        assert!(matches!(rx.try_recv(), Ok(Err(JoinError::NoTransport(_)))));
    }

    #[test]
    fn join_arms_the_delayed_leave_before_the_membership_with_one_lifetime() {
        let mut h = H::new(ElementCallCompat::Off);
        let (a, _) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec!["livekit".into()],
            },
            params(),
            T0,
        );
        match &a[0] {
            Action::ArmDelayedLeave {
                route:
                    Route::Sticky {
                        content,
                        duration_ms,
                        event_type,
                    },
                delay_ms,
            } => {
                assert_eq!(*delay_ms, 15_000);
                assert_eq!(*duration_ms, 240_000);
                assert_eq!(event_type, "org.matrix.msc4143.rtc.member");
                assert_eq!(content["leave_reason"]["code"], "delayed_leave");
                assert_eq!(content["member"]["id"], "m-1");
            }
            other => panic!("{other:?}"),
        }
        let a = h.outcome(Outcome::DelayedArmed(Ok("delay-1".into())), T0);
        assert!(matches!(
            &a[0],
            Action::SendMembership {
                kind: SendKind::Join,
                ..
            }
        ));
        assert_eq!(sticky_duration(&a[0]), 240_000);
        assert_eq!(content(&a[0])["member"]["membership"], "join");
        assert!(matches!(
            h.m.status(),
            Status::Joining(JoinStatus {
                has_sent_delayed_leave_event: true,
                has_sent_member_join_event: false,
                ..
            })
        ));
    }

    #[test]
    fn a_homeserver_without_delayed_events_can_still_be_joined_with_a_short_first_membership() {
        let mut h = H::new(ElementCallCompat::Off);
        let (_, mut rx) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        let a = h.outcome(
            Outcome::DelayedArmed(Err(DriverError::Unsupported("M_UNRECOGNIZED".into()))),
            T0,
        );
        assert_eq!(
            sticky_duration(&a[0]),
            DEFAULT_DEGRADED_LIFETIME_MS,
            "the first membership is already short"
        );
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: ok_sent(),
            },
            T0,
        );
        assert!(matches!(rx.try_recv(), Ok(Ok(()))));
        match h.m.status() {
            Status::Connected(c) => {
                assert_eq!(
                    c.keep_alive,
                    KeepAlive::Unavailable {
                        permanent: true,
                        next_probe_ts: None
                    }
                );
                assert_eq!(c.membership.lifetime_ms, DEFAULT_DEGRADED_LIFETIME_MS);
                assert_eq!(
                    c.membership.expires_at_ts,
                    T0 + DEFAULT_DEGRADED_LIFETIME_MS
                );
            }
            other => panic!("{other:?}"),
        }
        // Degraded heartbeat: only the refresh, on the short lifetime; a
        // stated refusal is never probed again.
        assert_eq!(
            h.m.next_wake_ts(),
            Some(T0 + DEFAULT_DEGRADED_LIFETIME_MS / 2)
        );
        let a = h.m.step(Input::Wake, T0 + DEFAULT_DEGRADED_LIFETIME_MS / 2);
        assert_eq!(a.len(), 1);
        assert!(matches!(
            a[0],
            Action::SendMembership {
                kind: SendKind::Refresh,
                ..
            }
        ));
        assert_eq!(
            sticky_duration(&a[0]),
            DEFAULT_DEGRADED_LIFETIME_MS,
            "every membership of a join states one lifetime"
        );
    }

    #[test]
    fn an_unexplained_refusal_is_retried_and_can_recover_without_moving_the_lifetime() {
        let mut h = H::new(ElementCallCompat::Off);
        h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        h.outcome(
            Outcome::DelayedArmed(Err(DriverError::Http("timeout".into()))),
            T0,
        );
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: ok_sent(),
            },
            T0,
        );
        let probe_at = T0 + DELAYED_LEAVE_PROBE_INTERVAL_MS;
        assert_eq!(
            h.m.next_wake_ts(),
            Some(DEFAULT_DEGRADED_LIFETIME_MS / 2 + T0).min(Some(probe_at))
        );
        let a = h.m.step(Input::Wake, probe_at);
        assert!(
            a.iter()
                .any(|a| matches!(a, Action::ArmDelayedLeave { .. })),
            "re-probed after 5 min"
        );
        h.outcome(Outcome::DelayedArmed(Ok("late".into())), probe_at);
        match h.m.status() {
            Status::Connected(c) => {
                assert!(matches!(c.keep_alive, KeepAlive::Armed { .. }));
                assert_eq!(
                    c.membership.lifetime_ms, DEFAULT_DEGRADED_LIFETIME_MS,
                    "frozen at join"
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_failed_join_returns_to_not_joined_and_arms_no_heartbeat() {
        let mut h = H::new(ElementCallCompat::Off);
        let (_, mut rx) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        h.outcome(Outcome::DelayedArmed(Ok("delay-1".into())), T0);
        let a = h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: Err(DriverError::Http("500".into())),
            },
            T0,
        );
        assert!(
            a.is_empty(),
            "the armed delay is left to fire, no cancel request"
        );
        assert!(matches!(rx.try_recv(), Ok(Err(JoinError::Driver(_)))));
        assert_eq!(h.m.status(), Status::NotJoined);
        assert_eq!(h.m.next_wake_ts(), None);
    }

    #[test]
    fn sticky_duration_is_clamped_to_one_hour() {
        let mut h = H::new(ElementCallCompat::Off);
        let (a, _) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                sticky_duration_ms: 5 * 60 * 60 * 1000,
                ..params()
            },
            T0,
        );
        assert_eq!(sticky_duration(&a[0]), MAX_STICKY_DURATION_MS);
    }

    #[test]
    fn delegation_raises_the_delay_to_one_hour_and_a_success_stops_client_restarts() {
        let mut h = H::new(ElementCallCompat::Off);
        let (a, mut rx) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                delegate_delayed_leave: true,
                ..params()
            },
            T0,
        );
        assert!(matches!(
            a[0],
            Action::ArmDelayedLeave {
                delay_ms: DELEGATION_MIN_DELAY_MS,
                ..
            }
        ));
        h.outcome(Outcome::DelayedArmed(Ok("d".into())), T0);
        let a = h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: ok_sent(),
            },
            T0,
        );
        assert_eq!(
            a,
            vec![Action::Delegate {
                delay_id: "d".into(),
                member: json!({ "id": "m-1", "membership": "join" })
            }]
        );
        assert!(
            rx.try_recv().is_err(),
            "the reply waits for the delegation request"
        );
        h.outcome(Outcome::Delegated(Ok(())), T0 + 1);
        assert!(matches!(rx.try_recv(), Ok(Ok(()))));
        match h.m.status() {
            Status::Connected(c) => {
                assert_eq!(
                    c.keep_alive,
                    KeepAlive::Delegated {
                        delegated_at_ts: T0 + 1,
                        earliest_fire_ts: T0 + 1 + DELEGATION_MIN_DELAY_MS,
                    }
                );
            }
            other => panic!("{other:?}"),
        }
        // Only the sticky refresh remains on the clock.
        assert_eq!(h.m.next_wake_ts(), Some(T0 + 1 + 120_000));
        let a = h.m.step(Input::Wake, T0 + DELEGATION_MIN_DELAY_MS);
        assert!(
            a.iter()
                .all(|a| !matches!(a, Action::RestartDelayedLeave { .. }))
        );
    }

    #[test]
    fn a_failed_delegation_falls_back_to_client_restarts_of_the_same_delay() {
        let mut h = H::new(ElementCallCompat::Off);
        let (_, mut rx) = h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                delegate_delayed_leave: true,
                ..params()
            },
            T0,
        );
        h.outcome(Outcome::DelayedArmed(Ok("d".into())), T0);
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: ok_sent(),
            },
            T0,
        );
        let a = h.outcome(
            Outcome::Delegated(Err(DriverError::Unsupported("nope".into()))),
            T0,
        );
        assert!(a.is_empty(), "no cancel, no replacement");
        assert!(matches!(rx.try_recv(), Ok(Ok(()))));
        assert!(
            matches!(h.m.status(), Status::Connected(c) if matches!(c.keep_alive, KeepAlive::Armed { .. })),
            "delegation fell back: we keep restarting it ourselves"
        );
        assert_eq!(
            h.m.next_wake_ts(),
            Some(T0 + 120_000).min(Some(T0 + DELEGATION_MIN_DELAY_MS / 3))
        );
        let a = h.m.step(Input::Wake, T0 + DELEGATION_MIN_DELAY_MS / 3);
        assert!(
            a.iter()
                .any(|a| matches!(a, Action::RestartDelayedLeave { delay_id } if delay_id == "d"))
        );
    }

    #[test]
    fn delegation_is_skipped_under_state_events_and_when_nothing_is_armed() {
        let mut h = H::new(ElementCallCompat::StateEvents);
        h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                delegate_delayed_leave: true,
                ..params()
            },
            T0,
        );
        h.outcome(Outcome::DelayedArmed(Ok("d".into())), T0);
        let a = h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: ok_sent(),
            },
            T0,
        );
        assert!(a.is_empty());
        assert!(matches!(h.m.status(), Status::Connected(_)));

        let mut h = H::new(ElementCallCompat::Off);
        h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                delegate_delayed_leave: true,
                ..params()
            },
            T0,
        );
        h.outcome(
            Outcome::DelayedArmed(Err(DriverError::Http("x".into()))),
            T0,
        );
        let a = h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: ok_sent(),
            },
            T0,
        );
        assert!(a.is_empty());
    }

    // -- heartbeat ---------------------------------------------------------------

    #[test]
    fn next_wake_is_the_earliest_of_restart_and_refresh_and_the_heartbeat_restarts_in_place() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        assert_eq!(
            h.m.next_wake_ts(),
            Some(T0 + 5_000),
            "restart every timeout/3"
        );
        let a = h.m.step(Input::Wake, T0 + 5_000);
        assert_eq!(
            a,
            vec![Action::RestartDelayedLeave {
                delay_id: "delay-1".into()
            }]
        );
        h.outcome(Outcome::Restarted(Ok(())), T0 + 5_000);
        assert!(matches!(
            h.m.status(),
            Status::Connected(ConnectedStatus {
                keep_alive: KeepAlive::Armed {
                    delay_ms: 15_000,
                    last_restart_ts,
                    fires_at_ts
                },
                ..
            }) if last_restart_ts == T0 + 5_000 && fires_at_ts == T0 + 20_000
        ));
        assert_eq!(h.m.next_wake_ts(), Some(T0 + 10_000));
    }

    #[test]
    fn a_failed_restart_does_not_immediately_arm_a_replacement_but_a_fired_delay_is_replaced() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        h.m.step(Input::Wake, T0 + 5_000);
        let a = h.outcome(
            Outcome::Restarted(Err(DriverError::Http("500".into()))),
            T0 + 5_000,
        );
        assert!(a.is_empty(), "may still be armed: retry next beat");
        assert_eq!(h.m.next_wake_ts(), Some(T0 + 10_000));
        h.m.step(Input::Wake, T0 + 10_000);
        h.outcome(
            Outcome::Restarted(Err(DriverError::Http("500".into()))),
            T0 + 10_000,
        );
        assert_eq!(h.m.next_wake_ts(), Some(T0 + 15_000), "the third beat");
        h.m.step(Input::Wake, T0 + 15_000);
        h.outcome(
            Outcome::Restarted(Err(DriverError::Http("500".into()))),
            T0 + 15_000,
        );
        // The expiry instant is itself a wake deadline, so the replacement
        // is armed the moment the delay must have fired rather than at the
        // next restart beat (which would be a third of the delay later).
        assert_eq!(h.m.next_wake_ts(), Some(T0 + 15_001), "the expiry instant");
        let a = h.m.step(Input::Wake, T0 + 15_001);
        assert!(
            matches!(
                a[0],
                Action::ArmDelayedLeave {
                    delay_ms: 15_000,
                    ..
                }
            ),
            "past the full delay: it fired"
        );
        h.outcome(Outcome::DelayedArmed(Ok("delay-2".into())), T0 + 15_001);
        let a = h.m.step(Input::Wake, T0 + 20_001);
        assert_eq!(
            a,
            vec![Action::RestartDelayedLeave {
                delay_id: "delay-2".into()
            }]
        );
    }

    /// A restart call that hangs past the delay and *then* fails: the
    /// outcome path must re-arm too, without waiting for the next wake.
    #[test]
    fn a_restart_failure_arriving_after_the_delay_elapsed_arms_a_replacement() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        h.m.step(Input::Wake, T0 + 5_000);
        let a = h.outcome(
            Outcome::Restarted(Err(DriverError::Http("timeout".into()))),
            T0 + 15_001,
        );
        assert!(matches!(a[0], Action::ArmDelayedLeave { .. }));
    }

    #[test]
    fn heartbeat_refreshes_the_sticky_membership_once_half_expired_and_retries_soon_on_failure() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        let a = h.m.step(Input::Wake, T0 + 119_999);
        assert!(
            a.iter()
                .all(|a| !matches!(a, Action::SendMembership { .. })),
            "fresh entry left alone"
        );
        let a = h.m.step(Input::Wake, T0 + 120_000);
        let refresh = a
            .iter()
            .find(|a| {
                matches!(
                    a,
                    Action::SendMembership {
                        kind: SendKind::Refresh,
                        ..
                    }
                )
            })
            .expect("refresh due");
        assert_eq!(sticky_duration(refresh), 240_000);
        assert_eq!(content(refresh)["member"]["membership"], "join");
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Refresh,
                result: Err(DriverError::Http("500".into())),
            },
            T0 + 120_000,
        );
        assert!(
            h.m.next_wake_ts().unwrap() <= T0 + 120_000 + 24_000,
            "retry at lifetime/10, not the next half-life"
        );
        h.m.step(Input::Wake, T0 + 144_000);
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Refresh,
                result: ok_sent(),
            },
            T0 + 144_000,
        );
        let a = h.m.step(Input::Wake, T0 + 145_000);
        assert!(
            a.iter()
                .all(|a| !matches!(a, Action::SendMembership { .. }))
        );
    }

    #[test]
    fn no_heartbeat_actions_while_leaving_or_not_joined() {
        let mut h = H::new(ElementCallCompat::Off);
        assert!(h.m.step(Input::Wake, T0).is_empty());
        h.connected(T0);
        h.leave(T0 + 1);
        assert_eq!(h.m.next_wake_ts(), None);
        assert!(h.m.step(Input::Wake, T0 + 100_000_000).is_empty());
    }

    // -- session reactions -------------------------------------------------------

    #[test]
    fn a_closed_slot_leaves_with_code_slot_closed_and_settles_the_delay() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        let a = h.m.step(
            Input::Session(seeded(vec!["m-1"], Some(SlotState::Closed))),
            T0 + 1,
        );
        assert!(matches!(
            a[0],
            Action::SendMembership {
                kind: SendKind::Leave,
                ..
            }
        ));
        assert_eq!(content(&a[0])["leave_reason"]["code"], "slot_closed");
        assert!(matches!(h.m.status(), Status::Leaving(_)));
        let a = h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Leave,
                result: ok_sent(),
            },
            T0 + 1,
        );
        assert_eq!(
            a,
            vec![Action::CancelDelayedLeave {
                delay_id: "delay-1".into()
            }]
        );
        h.outcome(Outcome::Cancelled(Ok(())), T0 + 1);
        assert_eq!(h.m.status(), Status::NotJoined);
    }

    #[test]
    fn a_closed_slot_is_ignored_for_the_legacy_slot() {
        let mut h = H::new(ElementCallCompat::StateEvents);
        h.connected(T0);
        let a = h.m.step(
            Input::Session(SessionSnapshot {
                slot_state: Some(SlotState::Closed),
                ..seeded(vec!["m-1"], None)
            }),
            T0 + 1,
        );
        assert!(a.is_empty());
        assert!(matches!(h.m.status(), Status::Connected(_)));
    }

    #[test]
    fn a_vanished_own_membership_is_resent_once_per_keep_alive_and_only_after_it_was_seen() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        // The echo has not arrived: an absent id is not a vanished membership.
        assert!(
            h.m.step(Input::Session(seeded(vec![], None)), T0 + 1)
                .is_empty()
        );
        assert!(
            h.m.step(Input::Session(seeded(vec!["m-1"], None)), T0 + 2)
                .is_empty()
        );
        let a = h.m.step(Input::Session(seeded(vec![], None)), T0 + 3);
        assert_eq!(a.len(), 1);
        assert!(matches!(
            a[0],
            Action::SendMembership {
                kind: SendKind::Heal,
                ..
            }
        ));
        assert_eq!(content(&a[0])["member"]["membership"], "join");
        // Rate limited.
        assert!(
            h.m.step(Input::Session(seeded(vec![], None)), T0 + 4)
                .is_empty()
        );
        // With the delay certainly fired, the heal re-arms it too.
        let a =
            h.m.step(Input::Session(seeded(vec![], None)), T0 + 3 + 15_001);
        assert_eq!(a.len(), 2);
        assert!(matches!(a[1], Action::ArmDelayedLeave { .. }));
    }

    #[test]
    fn an_excluded_own_membership_is_not_healed() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        h.m.step(Input::Session(seeded(vec!["m-1"], None)), T0 + 1);
        let mut s = seeded(vec![], None);
        s.excluded_candidates = vec![(
            seeded(vec!["m-1"], None).members.remove(0),
            crate::session::JoinExclusionReason::SenderNotInRoom,
        )];
        assert!(h.m.step(Input::Session(s), T0 + 2).is_empty());
    }

    // -- leave ---------------------------------------------------------------------

    #[test]
    fn leave_sends_the_leave_then_cancels_the_delay_with_the_plain_leave_code() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        let (a, mut rx) = h.leave(T0 + 1);
        assert!(matches!(
            a[0],
            Action::SendMembership {
                kind: SendKind::Leave,
                ..
            }
        ));
        assert_eq!(content(&a[0])["leave_reason"], json!({ "code": "leave" }));
        assert_eq!(sticky_duration(&a[0]), 240_000);
        let a = h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Leave,
                result: ok_sent(),
            },
            T0 + 1,
        );
        assert!(matches!(
            h.m.status(),
            Status::Leaving(LeaveStatus {
                leave_event_sent: true,
                delayed_leave: None
            })
        ));
        assert_eq!(
            a,
            vec![Action::CancelDelayedLeave {
                delay_id: "delay-1".into()
            }]
        );
        assert!(rx.try_recv().is_err());
        // A 404 (already fired) still completes the leave.
        h.outcome(
            Outcome::Cancelled(Err(DriverError::Http("404".into()))),
            T0 + 1,
        );
        assert!(matches!(rx.try_recv(), Ok(Ok(()))));
        assert_eq!(h.m.status(), Status::NotJoined);
    }

    #[test]
    fn leave_still_fails_when_the_leave_event_cannot_be_sent_and_returns_to_connected() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        let (_, mut rx) = h.leave(T0 + 1);
        let a = h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Leave,
                result: Err(DriverError::Http("500".into())),
            },
            T0 + 1,
        );
        assert!(a.is_empty());
        assert!(matches!(rx.try_recv(), Ok(Err(LeaveError::Driver(_)))));
        assert!(matches!(h.m.status(), Status::Connected(_)));
        assert_eq!(
            h.m.next_wake_ts(),
            Some(T0 + 5_000),
            "heartbeat still running"
        );
    }

    #[test]
    fn a_degraded_leave_cancels_nothing_and_leave_while_not_joined_is_an_error() {
        let mut h = H::new(ElementCallCompat::Off);
        let (_, mut rx) = h.leave(T0);
        assert!(matches!(rx.try_recv(), Ok(Err(LeaveError::NotJoined))));
        h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        h.outcome(
            Outcome::DelayedArmed(Err(DriverError::Unsupported("x".into()))),
            T0,
        );
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: ok_sent(),
            },
            T0,
        );
        let (_, mut rx) = h.leave(T0 + 1);
        let a = h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Leave,
                result: ok_sent(),
            },
            T0 + 1,
        );
        assert!(a.is_empty());
        assert!(matches!(rx.try_recv(), Ok(Ok(()))));
        assert_eq!(h.m.status(), Status::NotJoined);
    }

    // -- compat -------------------------------------------------------------------

    #[test]
    fn state_events_join_goes_out_as_room_state_and_the_leave_as_empty_content() {
        let mut h = H::new(ElementCallCompat::StateEvents);
        let (a, _) = h.join(TransportIntent::Publish(lk()), params(), T0);
        assert!(matches!(a[0], Action::ResolveTransport { .. }));
        let a = h.outcome(Outcome::TransportResolved(Ok(lk())), T0);
        match &a[0] {
            Action::ArmDelayedLeave {
                route:
                    Route::State {
                        event_type,
                        state_key,
                        content,
                    },
                delay_ms: 15_000,
            } => {
                assert_eq!(event_type, "org.matrix.msc3401.call.member");
                assert_eq!(state_key, "_@me:x_DEV_m.call");
                assert_eq!(*content, json!({}));
            }
            other => panic!("{other:?}"),
        }
        let a = h.outcome(Outcome::DelayedArmed(Ok("d".into())), T0);
        match &a[0] {
            Action::SendMembership {
                route: Route::State { content, .. },
                kind: SendKind::Join,
            } => {
                assert_eq!(content["created_ts"], T0);
                assert_eq!(content["expires"], 240_000);
                assert_eq!(content["foci_preferred"][0]["livekit_alias"], ROOM);
            }
            other => panic!("{other:?}"),
        }
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: ok_sent(),
            },
            T0,
        );
        // The refresh moves `expires`, not `created_ts`.
        let a = h.m.step(Input::Wake, T0 + 120_000);
        let refresh = a
            .iter()
            .find(|a| {
                matches!(
                    a,
                    Action::SendMembership {
                        kind: SendKind::Refresh,
                        ..
                    }
                )
            })
            .unwrap();
        assert_eq!(content(refresh)["created_ts"], T0);
        assert_eq!(content(refresh)["expires"], 360_000);
    }

    #[test]
    fn sticky_events_join_is_additive_and_the_leave_a_bare_key() {
        let mut h = H::new(ElementCallCompat::StickyEvents);
        h.join(
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            params(),
            T0,
        );
        let a = h.outcome(Outcome::DelayedArmed(Ok("d".into())), T0);
        let c = content(&a[0]);
        assert_eq!(c["member"]["user_id"], "@me:x");
        assert_eq!(c["member"]["device_id"], "DEV");
        assert_eq!(c["versions"], json!([]));
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Join,
                result: ok_sent(),
            },
            T0,
        );
        let (a, _) = h.leave(T0 + 1);
        assert_eq!(*content(&a[0]), json!({ "msc4354_sticky_key": "m-1" }));
    }

    /// A cancel that fails still leaves us out of the call (the delay is
    /// itself a leave), but the host must be able to tell that a stray
    /// delayed event may still land.
    #[test]
    fn a_failed_cancel_still_leaves_but_says_the_delay_may_still_fire() {
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        let (_, mut rx) = h.leave(T0 + 1);
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Leave,
                result: ok_sent(),
            },
            T0 + 1,
        );
        h.outcome(
            Outcome::Cancelled(Err(DriverError::Http("404".into()))),
            T0 + 2,
        );
        assert!(matches!(rx.try_recv(), Ok(Ok(()))), "still a clean leave");
        assert_eq!(h.m.status(), Status::NotJoined);

        // The clean path says the other thing.
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        h.leave(T0 + 1);
        h.outcome(
            Outcome::MembershipSent {
                kind: SendKind::Leave,
                result: ok_sent(),
            },
            T0 + 1,
        );
        assert_eq!(
            h.m.status(),
            Status::Leaving(LeaveStatus {
                leave_event_sent: true,
                delayed_leave: None,
            })
        );
    }

    #[test]
    fn status_publishes_only_on_change() {
        // The `watch` publish-on-change relies on `PartialEq`; the status
        // must be stable across inputs that change nothing.
        let mut h = H::new(ElementCallCompat::Off);
        h.connected(T0);
        // Our echo arriving is a real change (`AwaitingEcho -> Present`)...
        assert!(matches!(
            h.m.status(),
            Status::Connected(ConnectedStatus {
                roster: RosterPresence::AwaitingEcho,
                ..
            })
        ));
        h.m.step(Input::Session(seeded(vec!["m-1"], None)), T0 + 1);
        let before = h.m.status();
        assert!(matches!(
            before,
            Status::Connected(ConnectedStatus {
                roster: RosterPresence::Present,
                ..
            })
        ));
        // ...but seeing the same roster again is not.
        h.m.step(Input::Session(seeded(vec!["m-1"], None)), T0 + 2);
        assert_eq!(h.m.status(), before);
        assert!(h.m.debug_json()["delay_id"].is_string());
    }
}
