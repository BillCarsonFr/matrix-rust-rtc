# `own_membership` module — implementation plan

Scope: turn `src/own_membership/mod.rs` from `todo!()` into the working
join/leave machine for *our* membership described in
`../../MatrixSdkArchitecture.md`. Read together with
`../encryption/README.md` (the module whose runtime approach this plan
copies: a pure state machine stepped by one pump task that owns time and
I/O through `crate::executor`) and `../session/SessionImplementationPlan.md`
(the `SessionSnapshot` this module consumes).

Contents

1. [Responsibilities](#1-responsibilities)
2. [What exists today and what to port](#2-what-exists-today-and-what-to-port)
3. [Decisions](#3-decisions)
4. [Design](#4-design)
5. [Behaviour, step by step](#5-behaviour-step-by-step)
6. [Compat write side — field mappings](#6-compat-write-side--field-mappings)
7. [Challenges](#7-challenges)
8. [Phases](#8-phases)
9. [Tests](#9-tests)
10. [Contract notes for the other modules](#10-contract-notes-for-the-other-modules)
11. [Implementation status](#11-implementation-status)

---

## 1. Responsibilities

This module owns **everything about our own `m.rtc.member` membership and
nothing about anybody else's**. It manages one membership at a time — it is
not the participation lifecycle (that is `participation`'s job, which owns
this manager and calls it at the right moment). Concretely, it is
responsible for:

1. **Publishing the membership.** Turning `join(member_id, intent, params)`
   into the ordered set of Matrix writes that makes us a compliant member:
   resolve the transport we publish on, arm the dead man's switch **before**
   announcing ourselves, send the join membership, optionally delegate the
   dead man's switch to the SFU (MSC4195), start the keep-alive. Every step
   is a flag in `JoinStatus` so a UI can show a precise spinner.

2. **Keeping the membership alive while joined.** Our membership expires on
   two independent clocks and the module tends both, on its own timers:
   - the **delayed leave** (MSC4140, `keep_alive_timeout_ms`) is restarted
     with the `restart` action, never cancel+reschedule — and only until it
     has been delegated to the SFU;
   - the **sticky-map entry** (MSC4354, `sticky_duration_ms`) is re-sent
     verbatim once it is halfway to expiry.
   Failures degrade instead of failing the call: a homeserver without
   delayed events gets a short-lived membership (5 min) instead of a dead
   man's switch; a failed restart is retried, and a replacement is armed only
   once the old delay *must* have fired (leak-free re-arm).

3. **Withdrawing the membership.** `leave(reason)` sends the leave
   membership with MSC4143's `leave_reason {code, reason}` (default `leave`),
   then cancels or settles the delayed event, and stops the keep-alive. A
   slot that closes under us leaves automatically with code `slot_closed`,
   and a membership that vanished from the roster (a fired delayed leave, a
   lapsed sticky entry) is re-published.

4. **Reporting our own state.** One `Status`: `NotJoined | Joining(JoinStatus)
   | Connected(ConnectedStatus) | Leaving(LeaveStatus)`, readable
   synchronously and observable as a `watch`. `participation::Status`
   composes it with the encryption status.

5. **The write side of Element Call compat.** With
   `ElementCallCompat::StickyEvents` or `::StateEvents` selected, *our* events
   are rendered in that dialect (this is the opt-in half — it changes what
   other clients see). Reading those dialects lives in `session::convert`;
   rendering them lives here, one deletable file per generation. Because the
   dialect decides what a valid member id *is*, the pure
   `new_member_id(compat, own)` function lives here too, for the facade to
   call.

6. **Telling `connections` which transport we publish on.** Once the
   transport is resolved it is handed over through the resolver hook, so the
   connections manager can compute the full connection set from the session
   plus our own transport.

It is **not** responsible for:

- the participation lifecycle: waiting for the session to seed, deciding
  the encryption mode, generating the member id at the right moment,
  constructing the encryption machine before the join event — all
  `participation` (it owns this manager and the session; see §5.0);
- reading anyone's membership (the session does; this module only *watches*
  the `SessionSnapshot` for whether the slot is still open and whether our
  own member id is still in the roster);
- tokens, transport discovery or the LiveKit connection (`connections`,
  reached only through the resolver hook — this module never holds a
  `TokenDriver`);
- slot administration (`open_slot` / `close_slot` move to `participation`;
  they have nothing to do with our membership);
- media keys (`encryption`);
- MSC4075 notifications (no home in the architecture yet — see §7 item 13);
- updating a *live* membership's content (no `update_membership` API is in
  the architecture; a change of transport is leave + join);
- cleaning up *other* stale memberships of our own device (opt-in
  facade-level feature per the session plan §1.2; not here).

I/O boundary: **only** `crate::driver::OwnMembershipDriver` (sticky, state,
delayed send/restart/cancel, delegate) plus one async resolver hook supplied
by the facade. Time and detached work: **only** `crate::executor`.

## 2. What exists today and what to port

| current code (`../../../crates`) | what to take | where it lands |
|---|---|---|
| `matrix-rtc-core/src/own_membership.rs` | the whole dead-man's-switch model: arm before join, `restart` never cancel, `KeepAliveInfo.last_restart_ms`, `rearm_if_certainly_fired`, `DelayedLeaveSupport {Unknown, Supported, Unsupported{permanent,last_probe_ms}}` + 5-min re-probe, `published_lifetime_ms` fixed at join, `refresh_sticky_if_due` at half lifetime with verbatim content, leave tolerating a failed cancel, `NotJoined` after a failed join — and its ~30 tests | `machine.rs` (as a pure state machine) + `pump.rs`; tests in §9 |
| `matrix-rtc-core/src/join.rs` | `DEFAULT_KEEP_ALIVE_TIMEOUT_MS` (30 s), `DEFAULT_STICKY_DURATION_MS` (1 h), `MAX_STICKY_DURATION_MS` (1 h, clamp with a warning), `DEFAULT_DEGRADED_LIFETIME_MS` (5 min, with the MSC4354 "not below 5 min" rationale), `TransportIntent` semantics, `validate()` | `mod.rs` constants + `JoinParams::validate` |
| `matrix-rtc-core/src/event.rs` `RawStickyEventContent::{for_join, for_leave}` | the spec-shaped join/leave content: `slot_id`, `member {id, membership}`, `application {type}`, `msc4354_sticky_key = member.id`, `transports` omitted when empty, `leave_reason` | `wire.rs` (`json!` builders, no serde structs needed) |
| `matrix-rtc-core/src/session.rs` `LeaveCode` | the three codes `leave`, `delayed_leave`, `slot_closed` as constructors on `types::LeaveReason` | `types.rs` (`LeaveReason::{leave, delayed_leave, slot_closed}`) |
| `matrix-rtc-bridge/src/compat/element_call.rs` `ElementCallDialect::rewrite_member_content` + tests | 2025 sticky dialect write side (additive join, wholesale leave) | `compat_2025.rs` |
| `matrix-rtc-bridge/src/compat/element_call_state.rs` `ElementCallStateDialect::{state_key, membership_id, member_content, foci_preferred}` + tests | MSC3401 state dialect write side (state key, pinned `created_ts`, moving `expires`, `foci_preferred` + `livekit_alias`, default intent) | `compat_msc3401.rs` |
| `matrix-rtc-bridge/src/compat/mod.rs` `MemberEventRoute` / `OutboundDialect::route_member_event` | the *route* concept: a membership goes out as a sticky event or as a state event, decided by the dialect, performed by the sender | `wire.rs::Route` |
| `matrix-rtc-bridge/src/sdk.rs` `delayed_command_error` | which driver errors mean "this homeserver will never do delayed events": `DriverError::Unsupported` (404 `M_UNRECOGNIZED`) and `DriverError::Unauthorized` (403 `M_FORBIDDEN`, matrix.org's "Sending delayed events has been disallowed") | `machine.rs::classify_delayed_refusal` |
| `matrix-rtc-livekit/src/call.rs` `spawn_heartbeat` (15 s), `leave` ordering (stop heartbeat first) | the fact that the heartbeat is a *task* and that leave must stop it before cancelling | `pump.rs` (the pump owns the heartbeat; a `Leaving` state suppresses restarts) |
| `../encryption/{mod,pump}.rs` | the runtime pattern: `Arc<Inner>` + `Weak` in the pump, `Notify` on `Drop`, `Mutex` never held across an `await`, callbacks after the lock, `tokio::select!` over `watch::changed()` / `sleep_ms(next_wake)` / command channel, `executor::spawn` from the synchronous constructor | `pump.rs` verbatim in shape |

Not ported here: `matrix-rtc-core/src/slot.rs` `RawSlotEventContent::{for_open,
for_close}` — slot administration is `participation`'s.

What is genuinely new here (no equivalent today):

- **The module owns its timers.** Today the host calls `heartbeat()` every
  15 s and the core compares timestamps. Here the pump computes
  `next_wake_ts()` from the state and sleeps through `executor::sleep_ms`, on
  native and wasm alike. Nothing in the host has to tick.
- **Delegated delayed leave** (MSC4195 `/rtc/livekit/delegate_delayed_leave`).
  Nothing in `crates/` implements it; the draft driver already carries
  `delegate_livekit_delayed_leave(DelegatedDelayedLeaveRequest)`.
- **Session-driven reactions**: automatic `slot_closed` leave, and a
  rate-limited self-heal when our own membership vanished from the roster
  (§3.7).
- **The transport is resolved through a hook**, not by a host that already
  knows it (§3.3).

## 3. Decisions

Settled with the module owner; recorded here because they change signatures
other modules already compile against (`participation`, `uniffi_api`,
`driver`).

### 3.1 Our own identity is a constructor parameter, supplied by `participation`
The write side of compat needs `user_id`/`device_id` (`member.user_id` /
`member.device_id` in the 2025 dialect; `{user}:{device}` member id and the
state key in MSC3401). `OwnMembershipManager::new` takes `own: OwnIdentity {
user_id, device_id }` plus `room_id` and `slot_id` (the manager is bound to
one slot and must not have to read them out of the first snapshot).

**Where they come from:** `participation::ParticipationManager::new` gains
the identity as a parameter and passes it down to this manager (and uses it
itself for the `Member` it hands the encryption machine). Neither the driver
traits nor this module ask the host for it.

### 3.2 Member id generation is a pure function here; *when* to call it is the facade's
`types::generate_member_id()` gives the fresh random id MSC4143 wants. Under
`StateEvents` the id **must** be `{user}:{device}` because that generation's
peers use it as the LiveKit participant identity (session plan §6.2). The
compat mode's write side is this module's knowledge, so it exports

```rust
pub fn new_member_id(compat: ElementCallCompat, own: &OwnIdentity) -> String;
```

and `join()` **takes** the member id rather than minting one. The facade
calls `new_member_id` at the point in its lifecycle where the id is needed
(to construct the encryption machine *before* this module sends the join
event — `encryption/README.md`, "Lifecycle"), then passes the same id to
`join`. A `join` with an id that differs from the encryption machine's would
be a facade bug; this module cannot detect it and does not try.

### 3.3 The transport hook is async and returns the resolved transport
The skeleton's `TransportCreatedCallback = Fn(&RtcTransport)` is a
notification; the join sequence needs an *answer* (which URL to publish, and
that its token exists) before it can render the join content.

```rust
pub type ResolveTransportFuture = Pin<Box<dyn Future<Output = Result<RtcTransport, DriverError>> + MaybeSend>>;
pub type TransportResolver = Box<dyn Fn(TransportIntent) -> ResolveTransportFuture + Send + Sync>;
```

(`MaybeSend` = `Send` natively, nothing on wasm — the same split as the
driver futures.) The facade implements it over
`connections.add_own_transport(intent)`: connections discovers the transport
when the intent does not name one, mints the token, **records the transport
as our own**, and returns it. From then on the connections manager computes
the full connection set from two inputs — the session's members' published
transports *and* our own transport — so the LiveKit room we publish on is in
the host's `connections()` list even before our member event has round-tripped
through the session.

Flags: `has_fetched_transports` flips when the intent names a transport or
the resolver returned one; `has_created_transport_token` flips when the
resolver returns `Ok`. For `ReceiveOnly` the resolver is **not** called and
both flags flip at once (tokens for the connections we *subscribe* to are
`connections`' reactive job, keyed by the session's `ws_url`s, and are not a
join precondition). This keeps "talks only to `OwnMembershipDriver`"
literally true.

### 3.4 The delayed leave must be a *sticky* delayed event
`sdk.rs` notes that today's delayed leave is a plain (non-sticky) delayed
event, so when it fires it clears nothing from the sticky map — the dead
man's switch is "ceremonial" and crash cleanup rides on the sticky TTL. In
this crate it is worse: the session's MSC4354 map **refuses** a member event
without sticky metadata (session plan §1.1), so a non-sticky delayed leave
would not even take us out of our own roster. MSC4354 hints the two "may be
combined … to provide heartbeat semantics (e.g. required for MatrixRTC)".

Extend the driver:
`send_delayed_event(room_id, event_type, content, delay_ms, sticky_duration_ms: Option<u64>)`.
This module passes `Some(published_lifetime_ms)` — the *same* duration as
the join, so "last to expire wins" lets the leave (stamped at fire time)
out-expire the join. An adapter that cannot express both (ruma today) may
ignore the field; the module's behaviour is correct either way, only the
ghost window differs. FFI mirror: one extra `Option<u64>` on
`MatrixDriverCallback::send_delayed_event` and `mockDriver.ts`.

The `Option` exists only for the transition. Keep it confined to the
driver signature and one line in `pump.rs` so that, once every delayed leave
in the ecosystem is sticky, it can be made mandatory (or folded into
`delay_ms`'s neighbour) in one commit with no logic change here.

### 3.5 `StateEvents` needs a delayed **state** event
In the MSC3401 dialect the delayed leave is a delayed *state* event with
`{}` content (that is the only thing that empties a state membership), and
`send_delayed_event` has no `state_key`. Add
`send_delayed_state_event(room_id, event_type, state_key, content, delay_ms)`
to `OwnMembershipDriver`, documented as "compat only — delete with
`StateEvents`". Restart/cancel are id-based and need no dialect.

### 3.6 `Status` gains `NotJoined`; the typo goes; `LeaveStatus` fields
`Status::Leaveing` → `Status::Leaving`; add `Status::NotJoined` (before the
first join and after a completed leave — the facade maps it to
`Disconnected`). `Status` and its payloads derive `Clone, Debug, PartialEq`
so a `watch` can publish only on change. Add
`subscribe_status() -> watch::Receiver<Status>`: heartbeat outcomes,
delegation, the automatic `slot_closed` leave and the self-heal all change
the status without a host call, and the facade's `on_status_change` needs
to hear about it.

`LeaveStatus.transport_disconnected` cannot be known here (whether the host
closed its LiveKit room is a host fact). `LeaveStatus { leave_event_sent,
delayed_leave_settled }` — the two things this module actually does during
a leave. The facade composes a transport flag above if it wants one.

### 3.7 Reactions to the session while joined
The manager holds the session `watch` for two reasons:

- **Slot closed while joined** (MSC4143 candidates only; never for
  `LEGACY_SLOT_ID`): send the leave with `leave_reason.code = "slot_closed"`,
  settle the delayed event, go `NotJoined`. The session already excludes
  every member of a closed slot; the leave tidies the sticky map and the
  delayed event so nothing fires later.
- **Self-heal** (rate-limited): while `Connected`, if our `member_id` is in
  neither `members` nor `excluded_candidates`, our sticky entry is gone (the
  delayed leave fired during a heartbeat gap, or the refresh kept failing).
  Re-send the join content verbatim with the same duration (which
  out-expires the fired leave) and, if the delay "must have fired" (§5.2),
  re-arm it. At most once per `keep_alive_timeout_ms`. If our id **is** in
  `excluded_candidates` do nothing: `SenderNotInRoom` (we left the room) and
  `UnencryptedInEncryptedRoom` (the host sent our event in cleartext) are
  not healed by resending — they are logged at `warn`.

`join()` additionally refuses to publish into a slot the current snapshot
reports as `Closed` (`JoinError::SlotClosed`); an unsupplied slot state is
not a refusal. Waiting for the session to *seed* before joining is the
facade's job (§5.0); `has_fetched_initial_member_list` mirrors the snapshot's
`seeded` flag as this module observes it.

### 3.8 Delegation and the ≥ 1 h delay
MSC4195: a delayed event meant to be delegated SHOULD use a delay of at
least 1 h, and servers MAY reject shorter ones with `M_INVALID_PARAM`. When
`delegate_delayed_leave` is set, arm the delayed leave with
`max(keep_alive_timeout_ms, 3_600_000)`. After a successful delegation the
client **stops restarting** it (the homeserver does, keyed on the
participant's SFU connection) and keeps only the sticky refresh;
`ConnectedStatus.delegation_setup_ts` is set. If delegation fails
(`Unsupported`, `Unauthorized`, or any error), keep the *same* 1 h delay and
fall back to restarting it ourselves — leak-free, no cancel, at the cost of
crash cleanup taking up to an hour (which equals the sticky lifetime anyway).
Alternative considered and rejected: arm a short replacement and cancel the
1 h one — a failed cancel leaks a leave that fires mid-call.

Delegation is attempted **after** the join event (the homeserver waits for
the participant to connect; the token exists from the resolver step, the
host connects when `connections` publishes the `ConnectionData`). Not
attempted under `StateEvents` (that generation has no MSC4195 endpoints) or
when the delayed leave could not be armed.

### 3.9 Keep-alive cadence and the two lifetimes
- Restart interval = `keep_alive_timeout_ms / 3` (two retries fit before the
  delay fires; the web tests use 15 s → 5 s, Element Call uses 5 s with an
  8 s delay). Not configurable in v1; the interval is derived, not chosen.
- `sticky_duration_ms` clamped to `MAX_STICKY_DURATION_MS` (1 h) with a
  warning; refresh at half the *published* lifetime.
- `published_lifetime_ms` is `sticky_duration_ms`, or
  `DEFAULT_DEGRADED_LIFETIME_MS` (5 min) when the delayed leave was refused
  *before the first publish*, and never moves afterwards (MSC4354 ignores a
  shorter duration on the same key). `JoinParams` gains
  `degraded_lifetime_ms: Option<u64>` (default 5 min) — the FFI record
  mirrors it as optional.
- Refusal classification: `DriverError::Unsupported | Unauthorized` →
  permanent (never probed again); anything else → re-probe on the heartbeat
  every 5 min (`DELAYED_LEAVE_PROBE_INTERVAL_MS`).

### 3.10 `JoinParams` additions and validation
`intent: Option<String>` (→ `application["m.call.intent"]`; the MSC3401
dialect *requires* it and defaults to `"video"`), `degraded_lifetime_ms:
Option<u64>` (3.9). `validate()`: `application_type` non-empty,
`sticky_duration_ms > 0`, `keep_alive_timeout_ms > 0`, `member_id`
non-empty; a `Publish` transport must have a non-empty `transport_type`.
Failing → `JoinError::InvalidParams`.

### 3.11 The leave that fails to send
Today a failed leave send leaves the core stuck in `Leaving`. Here: return
`LeaveError::Driver(e)` and go **back to `Connected`** with the heartbeat
running, so the host can retry `leave()`; dropping the manager instead is
allowed and documented (the delayed leave, if armed, does the cleanup — that
is what it is for). `leave()` while `NotJoined`/`Joining` →
`LeaveError::NotJoined` (a `Joining` in progress cannot be interrupted in
v1; the facade serialises `join`/`leave`).

---

## 4. Design

### 4.1 Layout

```
src/own_membership/
  OwnMembershipImplementationPlan.md   this file
  mod.rs            public API: Status/JoinStatus/ConnectedStatus/LeaveStatus, JoinParams,
                    JoinError/LeaveError, OwnIdentity, new_member_id, TransportResolver,
                    OwnMembershipManager (constructor spawns the pump; methods enqueue commands)
  machine.rs        the pure state machine: states, inputs (commands, driver outcomes, session
                    facts, wake), outputs (Action), next_wake_ts(), status() — no await, no clock
  wire.rs           spec-shaped member content (join/leave/delayed leave); `Route` (sticky vs state);
                    dispatch to the two compat renderers; the only place that knows event-type strings
  compat_2025.rs    ElementCallCompat::StickyEvents write side (delete with that generation)
  compat_msc3401.rs ElementCallCompat::StateEvents write side (delete with that generation)
  pump.rs           the one task: command channel + session watch + deadline timer + driver calls
  test_support.rs   #[cfg(test)]: recording/faulting OwnMembershipDriver mock, a fake clock,
                    snapshot builders
```

### 4.2 Principle: the policy is a pure state machine, the pump owns time and I/O

Same split as `encryption`. `machine.rs` never awaits, never reads a clock,
never touches the driver:

```rust
pub(crate) enum State {
    NotJoined,
    Joining   { flags: JoinStatus, join: JoinPlan, delayed: Option<DelayedLeave>, .. },
    Connected { join: JoinPlan, delayed: Option<DelayedLeave>, sticky: SentSticky,
                delegated_at: Option<u64>, support: DelayedLeaveSupport, healed_at: Option<u64> },
    Leaving   { flags: LeaveStatus, join: JoinPlan, delayed: Option<DelayedLeave>, .. },
}

pub(crate) enum Input {
    Join  { member_id, intent, params, reply },   // from the host, via the manager
    Leave { reason: Option<LeaveReason>, reply },
    Session(SessionSnapshot),                     // every watch change (publish-on-change upstream)
    Wake,                                         // next_wake_ts() reached
    Outcome(Outcome),                             // a driver call / the resolver finished
}

pub(crate) enum Action {
    ResolveTransport(TransportIntent),
    ArmDelayedLeave { route: Route, delay_ms: u64 },     // Route: sticky (with duration) or state
    SendMembership  { route: Route },                     // join, refresh, leave, slot_closed leave
    RestartDelayedLeave { delay_id: String },
    CancelDelayedLeave  { delay_id: String },
    Delegate { delay_id: String, member: Value },
    Reply(ReplyTo, Result<..>),                           // resolve a pending host future
    PublishStatus(Status),                                // watch::Sender::send_if_modified
}

pub(crate) enum Outcome {
    TransportResolved(Result<RtcTransport, DriverError>),
    DelayedArmed(Result<String /* delay_id */, DriverError>),
    MembershipSent { kind: SendKind, result: Result<SendEventResponse, DriverError> },
    Restarted(Result<(), DriverError>),
    Cancelled(Result<(), DriverError>),
    Delegated(Result<(), DriverError>),
}

impl Machine {
    pub fn step(&mut self, input: Input, now: u64) -> Vec<Action>;
    pub fn next_wake_ts(&self) -> Option<u64>;   // min(next restart, next sticky refresh, next probe)
    pub fn status(&self) -> Status;
}
```

`JoinPlan` is everything decided when the join starts and frozen for the
participation: `member: Member`, spec-shaped `join_content: Value`,
`published_lifetime_ms`, `keep_alive_timeout_ms` (already raised to 1 h when
delegating), `delegate: bool`, `join_event_id: Option<String>`. `DelayedLeave
{ delay_id, timeout_ms, last_restart_ms }` is the core's `KeepAliveInfo`.
`SentSticky { sent_at_ms }` is the core's, minus the content (it is in the
plan).

**Rendering happens at send time, from the spec content.** `wire::route(compat,
own, room_id, slot_id, spec_content, lifetime_ms, now) -> Route` produces
either `Route::Sticky { event_type, content, duration_ms }` or
`Route::State { event_type, state_key, content }`. This is what makes the
MSC3401 refresh move `expires` while keeping `created_ts` pinned (§6.3),
and it keeps the compat knowledge out of `machine.rs` entirely: the
machine's actions carry `Route`s, which the pump executes with the matching
driver method.

Why the split: every timing rule (restart cadence, half-life refresh,
"must have fired", 5-min re-probe, self-heal rate limit, delegation
fallback) becomes a unit test with a fake clock and a scripted list of
outcomes; the same machine drives native and wasm because only `pump.rs`
touches the platform.

### 4.3 The pump

```
loop {
    let wake_at = inner.lock().machine.next_wake_ts();
    select! {
        cmd = commands.recv()          => actions = machine.step(Input::from(cmd), now()),
        changed = session.changed()    => actions = machine.step(Input::Session(session.borrow_and_update().clone()), now()),
        _ = sleep_until(wake_at)       => actions = machine.step(Input::Wake, now()),
        _ = notify.notified()          => if inner.upgrade().is_none() { return },   // Manager dropped
    }
    for action in actions {
        match action {
            Reply(..) | PublishStatus(..) => /* immediate, no await */,
            io action => { let outcome = driver_call(action).await; actions.extend(machine.step(Input::Outcome(outcome), now())) }
        }
    }
}
```

Rules carried over from `encryption/pump.rs`:

- One `Mutex<Machine>` inside `Arc<Inner>`; the pump holds a `Weak`. The lock
  is held only for `step()`; **never across an await**. Status is published
  after the lock is released (`watch::Sender::send_if_modified`).
- Driver calls are awaited **inside** the loop, one at a time, in action
  order. That serialises all I/O of one membership: a heartbeat restart and
  a `leave` cancel cannot interleave; the core needed the host to guarantee
  this (`drop(heartbeat)` before `leave`). Commands and session changes
  that arrive during a call are picked up on the next iteration (`watch`
  keeps only the latest value; the command channel is unbounded).
- `Drop for OwnMembershipManager` notifies the pump, which exits and
  releases the driver `Arc` and the session receiver. Nothing is sent on
  drop (see 3.11).
- `executor::spawn` from the synchronous constructor (no ambient runtime);
  `executor::sleep_ms` for the deadline; `executor::now_ms` for every
  timestamp. No `tokio::time`, no `wasm-bindgen` here.
- The `tokio::select!` macro needs tokio's `macros` feature, which the
  Cargo.toml already enables and which is in tokio's wasm-allowed set.

Host-facing async methods (`join`, `leave`) push a command with a
`tokio::sync::oneshot` reply and await it, so they are `Send` futures
natively and `?Send` on wasm through the same `cfg_attr` pattern the driver
uses. `status()` locks and reads.

### 4.4 Public surface (after §3)

```rust
pub struct OwnIdentity { pub user_id: String, pub device_id: String }

/// Fresh random id (MSC4143), or `{user}:{device}` under `StateEvents`.
pub fn new_member_id(compat: ElementCallCompat, own: &OwnIdentity) -> String;

pub struct JoinParams {
    pub application_type: String,
    pub intent: Option<String>,               // application["m.call.intent"]
    pub sticky_duration_ms: u64,              // clamped to 1 h
    pub keep_alive_timeout_ms: u64,           // raised to ≥ 1 h when delegating
    pub degraded_lifetime_ms: Option<u64>,    // default 5 min
    pub delegate_delayed_leave: bool,
}

pub enum Status { NotJoined, Joining(JoinStatus), Connected(ConnectedStatus), Leaving(LeaveStatus) }
pub struct JoinStatus { has_fetched_transports, has_fetched_initial_member_list, has_created_transport_token,
                        has_sent_delayed_leave_event, has_sent_member_join_event, has_delegated_delayed_event,
                        has_started_heartbeat }
pub struct ConnectedStatus { delayed_event_kick_ts: Option<u64>, heartbeat_last_restart_ts: Option<u64>,
                             delegation_setup_ts: Option<u64>,
                             delayed_leave_supported: bool, membership_lifetime_ms: u64 }   // two additions from the core
pub struct LeaveStatus { leave_event_sent: bool, delayed_leave_settled: bool }

pub enum JoinError { AlreadyJoined, InvalidParams(String), SlotClosed, TransportUnavailable(DriverError), Driver(DriverError) }
pub enum LeaveError { NotJoined, Driver(DriverError) }

impl OwnMembershipManager {
    pub fn new(room_id, slot_id, own: OwnIdentity, session: watch::Receiver<SessionSnapshot>,
               driver: Arc<dyn OwnMembershipDriver>, compat: ElementCallCompat,
               resolve_transport: TransportResolver) -> Self;
    pub async fn join(&self, member_id: String, intent: TransportIntent, params: JoinParams) -> Result<(), JoinError>;
    pub async fn leave(&self, reason: Option<LeaveReason>) -> Result<(), LeaveError>;
    pub fn status(&self) -> Status;
    pub fn subscribe_status(&self) -> watch::Receiver<Status>;
    pub fn own_member(&self) -> Option<Member>;        // Some while Joining/Connected/Leaving: the member as published
    pub fn debug_snapshot(&self) -> serde_json::Value;   // state name, flags, delay id, timestamps, support, next_wake
}
```

`ConnectedStatus.delayed_event_kick_ts` = `last_restart_ms + timeout_ms`
while a delay is armed and not delegated; `None` when degraded; after
delegation it is the homeserver's business and stays at the last value we
know (`delegation_setup_ts + timeout_ms`), documented as "at the earliest".

## 5. Behaviour, step by step

### 5.0 What the facade does around `join` (for context, not this module)

`participation::join(intent, params)`:

1. waits for the session to be seeded (`SessionSnapshot.seeded`) and reads
   `negotiated_encryption` / `slot_state`;
2. `member_id = own_membership::new_member_id(compat, &own)`;
3. constructs the `encryption::Machine` with a `Member { member_id, user_id,
   device_id, .. }` (encrypted calls only);
4. `own_membership.join(member_id, intent, params).await` — everything below;
5. on error drops the encryption machine again.

`participation::leave` = `own_membership.leave(reason)` then drop the
encryption machine. Slot administration (`open_slot`/`close_slot`) is a
facade method on the same driver.

### 5.1 `join(member_id, intent, params)` — state `Joining` → `Connected`

| step | does | on failure | flag |
|---|---|---|---|
| 0 | `NotJoined`? else `AlreadyJoined`. `params.validate()`. Snapshot `slot_state == Some(Closed)` (MSC4143 slot only) → refuse. | `InvalidParams`, `SlotClosed` | `has_fetched_initial_member_list` = `snapshot.seeded` |
| 1 | `Publish(t)`: `has_fetched_transports` at once, then resolver → the transport whose token exists (may differ from `t`, e.g. discovered URL filled in). `ReceiveOnly`: resolver not called. | `TransportUnavailable(e)` → `NotJoined` | `has_fetched_transports`, `has_created_transport_token` |
| 2 | build `Member { member_id, user_id, device_id, device_attribution: Verified, membership_ts: Some(now), intent, application_type, transports }`; build spec join content (`wire::join_content`); freeze `JoinPlan` (`published_lifetime_ms = clamp(sticky)`, `keep_alive = delegate ? max(keep_alive, 1 h) : keep_alive`). | — | — |
| 3 | **arm the delayed leave first**: `wire::delayed_leave_content` (spec leave, `leave_reason {code: "delayed_leave", reason: "Dead man's switch: client failed to heartbeat"}`) routed per compat, `delay_ms = plan.keep_alive`, sticky duration = `published_lifetime_ms` (§3.4) | refused → classify (§3.9), **do not fail**; this is before the first publish, so `published_lifetime_ms = degraded_lifetime_ms`; log `warn` | `has_sent_delayed_leave_event` only on success |
| 4 | send the join membership: `Route` from the spec content with `published_lifetime_ms`; keep the `event_id` on the plan | `Driver(e)` → `NotJoined` (a delayed leave that was armed is left to fire: it cleans up nothing we published, and cancelling it is a second request that can fail; log) | `has_sent_member_join_event` |
| 5 | `SentSticky { sent_at_ms: now }`; heartbeat scheduling begins (`next_wake_ts()` becomes `Some`) | — | `has_started_heartbeat` |
| 6 | if `plan.delegate && delayed.is_some() && compat != StateEvents`: `Delegate { delay_id, member: plan.join_content["member"] }` | any error → log `warn`, fall back to client restarts (§3.8); flag stays false | `has_delegated_delayed_event` |
| 7 | → `Connected { delegated_at, support, .. }`; reply `Ok(())` | — | — |

The reply is sent after step 6 (one request; it fails fast on servers
without the endpoint), so when the host's `join` future resolves every
outbound call of the join has been made.

### 5.2 `Connected` — the heartbeat, on `Wake`

`next_wake_ts()` = min over the following that apply:

| clock | due at | action | outcome handling |
|---|---|---|---|
| delayed-leave restart (only when armed **and not delegated**) | `last_restart_ms + timeout_ms / 3` | `RestartDelayedLeave` | `Ok` → `last_restart_ms = now`. `Err` → keep the id, retry next beat; **if** `now − last_restart_ms > timeout_ms` the delay must have fired → drop it and `ArmDelayedLeave` again (`rearm_if_certainly_fired`); a refused re-arm → classify support, log |
| re-probe (only when `support == Unsupported { permanent: false }` and nothing armed) | `last_probe_ms + 5 min` | `ArmDelayedLeave` | `Ok` → `Supported`, log "accepts delayed events after all"; lifetime does **not** change (frozen) |
| sticky refresh | `sent_at_ms + published_lifetime_ms / 2` | `SendMembership` (join content re-rendered — same spec content, `now` moves MSC3401's `expires`) | `Ok` → `sent_at_ms = now`. `Err` → retry next beat (next wake = now + `published_lifetime_ms / 10`, bounded) |

Permanent refusal → never probed again. After delegation the first row is
off for the rest of the participation.

### 5.3 `Connected` — on `Session(snapshot)`

- `slot_state` became `Some(Closed)` (MSC4143 slot only) → run 5.4 with
  `LeaveReason::slot_closed()`; the reply goes nowhere (no host future);
  status ends `NotJoined`.
- our `member_id` absent from both `members` and `excluded_candidates`, and
  `healed_at.map_or(true, |t| now − t >= keep_alive)` → `SendMembership`
  (join) and, if the delay must have fired, `ArmDelayedLeave`; `healed_at =
  now`; log `warn`. Present in `excluded_candidates` → log only.
- `has_fetched_initial_member_list` follows `snapshot.seeded` while still
  `Joining`.
- anything else → no action (the roster is the session's business).

### 5.4 `leave(reason)` — state `Leaving` → `NotJoined`

| step | does | on failure | flag |
|---|---|---|---|
| 0 | `Connected`? else `NotJoined` | `LeaveError::NotJoined` | — |
| 1 | → `Leaving` (suppresses every heartbeat action); render leave: spec `{ slot_id, member {id, membership: "leave"}, msc4354_sticky_key, leave_reason }` with `reason.unwrap_or(LeaveReason::leave())`, routed per compat, duration `published_lifetime_ms` | `Driver(e)` → back to `Connected` (§3.11) | `leave_event_sent` |
| 2 | if a delay id is held: `CancelDelayedLeave` | **not propagated** (404 = it already fired = we are gone either way); log `debug` | `delayed_leave_settled` (set on `Ok` *and* `Err`) |
| 3 | → `NotJoined`; reply `Ok(())`. A delegated delay is cancelled the same way (the homeserver's delegation is invalidated by the cancel). | — | — |

Under `StateEvents` step 1 sends the state event with `{}` content and
step 2 cancels the delayed *state* event.

## 6. Compat write side — field mappings

Extracted from `crates/matrix-rtc-bridge/src/compat/{element_call,
element_call_state}.rs`. Only the write side is listed; the read side is in
the session plan §6. Both renderers take the **spec-shaped** content this
module built plus `own`, `room_id`, `slot_id`, `lifetime_ms`, `now`.

### 6.1 Spec (`Off`) — `wire.rs`

Join:
```json
{ "slot_id": "<slot>", "msc4354_sticky_key": "<member_id>",
  "member": { "id": "<member_id>", "membership": "join" },
  "application": { "type": "m.call", "m.call.intent": "<intent, if given>" },
  "transports": { "published": [ { "type": "livekit", "livekit_service_url": "…" } ],
                  "can_subscribe": [ "livekit" ] } }
```
`transports` omitted when both lists are empty. `Publish(t)` → `published =
[t]`, `can_subscribe = [t.transport_type]` (publishing a type declares we can
receive it — kept from the core). `ReceiveOnly { can_subscribe }` →
`published = []`.

Leave / delayed leave: `{ "slot_id", "msc4354_sticky_key", "member": { "id",
"membership": "leave" }, "leave_reason": { "code", "reason"? } }`. Event type
on the wire: `wire_event_type("m.rtc.member")` =
`org.matrix.msc4143.rtc.member`. Route: **sticky**, `duration_ms =
lifetime_ms` for every membership event of the join (join, refresh, leave,
delayed leave) — one duration per sticky key, as MSC4354 asks.

### 6.2 2025 sticky dialect (`StickyEvents`) — `compat_2025.rs`

Same event type and route as spec. Rewrites, applied to the spec content:

| case | rule |
|---|---|
| join / refresh | **additive**: keep every spec field; add `rtc_transports` = `transports.published` verbatim (only if non-empty and absent); add `versions: []`; add `member.user_id = own.user_id`, `member.device_id = own.device_id` (only if absent) |
| leave, delayed leave | **wholesale**: `{ "msc4354_sticky_key": "<member_id>" }` — Element Call has no `membership` field, signals departure by a bare sticky key, and rejects a leave that has no `rtc_transports`; padding one would read as "joined, publishing nothing" (a worse ghost). A spec-current peer cannot parse a bare key either and treats it as departed — same outcome |

Media keys in this dialect are `encryption`'s job.

### 6.3 MSC3401 state dialect (`StateEvents`) — `compat_msc3401.rs`

Route: **state** — `send_state_event` / `send_delayed_state_event`, event
type `org.matrix.msc3401.call.member`, state key
`_{user_id}_{device_id}_{application}{call_id}` (leading underscore: Synapse
rejects user-id-shaped state keys from anyone but that user). `(application,
call_id)`: `application = params.application_type`, `call_id = ""` — the
draft creates a `StateEvents` call under `LEGACY_SLOT_ID` (`""`), which is
the room call; the bridge's `legacy_session("m.call#ROOM")` → `("m.call", "")`
is the same value. Member id: `{user_id}:{device_id}` (§3.2).

| output | rule |
|---|---|
| leave, delayed leave | `{}` — the whole protocol for it |
| `application` | `params.application_type` (a **string**) |
| `call_id` | `""` |
| `scope` | `"m.room"` |
| `device_id`, `membershipID` | `own.device_id`, `{user}:{device}` |
| `created_ts` | pinned at the **first** send of this join (peers select the oldest member's focus by it; re-stamping would make us perpetually newest) — stored on the `JoinPlan` |
| `expires` | `now − created_ts + lifetime_ms` → peers read `created_ts + expires = now + lifetime`; this is what makes a refresh move the deadline |
| `focus_active` | `{ "type": "livekit", "focus_selection": "multi_sfu" }` |
| `foci_preferred` | `transports.published` filtered to `type == "livekit"`, each with `livekit_alias = room_id` added (must equal the `room` the legacy `/sfu/get` is called with — `connections` owns that side; note in §10) |
| `m.call.intent` | `params.intent` or `"video"` (required by that generation's validator) |

The 30-second re-send the core did in this mode is replaced by the same
half-life refresh as sticky (it recomputes `expires`).

## 7. Challenges

Each item: the problem, the decision, status.

1. **No tokio runtime on wasm, timers needed.** Restart cadence, sticky
   refresh, re-probe and self-heal are all deadlines. *Decision*: all time
   through `crate::executor::{spawn, sleep_ms, now_ms}`; the machine exposes
   `next_wake_ts()` and the pump sleeps once per iteration (identical to
   `encryption/pump.rs`). *Status*: executor done and probed on wasm; pump
   to write.

2. **Detached task from a synchronous constructor and from FFI.**
   `OwnMembershipManager::new` is sync; `executor::spawn` handles both
   targets. *Status*: pattern proven by `encryption`.

3. **`Send`/`?Send`.** Driver futures and the resolver future are `Send`
   natively and `?Send` on wasm. Commands carry a `oneshot::Sender`; the
   host-facing `async fn`s only await the oneshot, so they stay `Send`-clean
   on native regardless of what the pump awaits. *Decision*: `MaybeSend`
   alias in `mod.rs` for the resolver future; the dual `cfg_attr` on nothing
   else (the manager has no trait). *Status*: to do.

4. **The delayed leave clears nothing unless it is sticky** (§3.4) —
   and this crate's own session would ignore a non-sticky leave. *Decision*:
   driver gains `sticky_duration_ms: Option<u64>` on `send_delayed_event`;
   module passes the published lifetime; the `Option` is transitional and
   confined to one line. Upstream (ruma/matrix-rust-sdk, MSC4354 wording on
   `origin_server_ts` = fire time) still has to catch up; documented on the
   trait. *Status*: driver + FFI + mock change pending.

5. **MSC4354 "last to expire wins" and the frozen lifetime.** A shorter
   refresh loses to the longer entry already in the map, so the degraded
   lifetime must be chosen before the first publish — only possible because
   the delayed leave is armed one step earlier. *Decision*: kept from the
   core exactly (`published_lifetime_ms` frozen at join; a late refusal or a
   late recovery does not move it). *Status*: rule ported into the machine.

6. **Leaking a delayed event marks us departed mid-call.** Restart never
   cancel+reschedule; re-arm only when `now − last_restart > timeout` (the
   old one must have fired); delegation fallback keeps the same delay
   instead of replacing it (§3.8). *Status*: rules ported.

7. **Delegation needs a ≥ 1 h delay** and a participant that connects to
   the SFU. *Decision*: §3.8. Open question flagged for the host docs: a
   host that disconnects from the SFU on purpose (network switch, focus
   change) while delegated will be left by the homeserver; nothing this
   module can prevent — the host should `leave()` first or not delegate.
   *Status*: designed.

8. **Our identity is nobody's input yet** (§3.1). *Decision*: constructor
   parameter here, supplied by `ParticipationManager::new`. *Status*:
   signature change here and in `participation`.

9. **Ordering with the encryption machine.** The encryption machine must
   exist before our join event goes out and needs our member id.
   *Decision*: the member id is generated by the facade via this module's
   pure `new_member_id` and passed into `join`; lifecycle stays in
   `participation` (§3.2, §5.0). *Status*: designed.

10. **`has_fetched_initial_member_list` is a facade gate.** Waiting for the
    seed is `participation`'s step; the flag here mirrors
    `SessionSnapshot.seeded` (session contract note) so the status stays
    truthful even if a facade calls `join` early. *Status*: pending on
    `session` for the field.

11. **`StateEvents` delayed leave is a state event** (§3.5). *Decision*:
    `send_delayed_state_event` on the driver, compat-only. *Status*: driver
    + FFI + mock change pending.

12. **Concurrency of `join`/`leave` with the heartbeat.** *Decision*: every
    driver call of one membership goes through the pump's single loop, so
    nothing interleaves; the machine additionally refuses `leave` during
    `Joining` (`NotJoined` error) rather than trying to cancel a half-done
    join. *Status*: designed.

13. **MSC4075 notifications have no home.** The core sends a ring/notify
    event at join when the session was empty. The architecture does not
    mention it; it is not this module's job. *Decision*: out of scope; the
    facade may add it later using the join `event_id` (kept on the
    `JoinPlan` and exposed through `debug_snapshot` for that reason).
    *Status*: recorded.

14. **Host callbacks may re-enter.** A status listener may call `status()`
    or `leave()` from inside the callback. *Decision*: publish through a
    `watch` (no callback held by this module); the facade's callback runs
    on its side after our lock is released. *Status*: designed.

15. **Rate limits on join.** Joining sends a delayed leave *and* a
    membership (2 requests, 3 with delegation); the driver owns retries
    (the bridge's `rtc_request_config`: 15 s timeout, 5 attempts). This
    module does not retry the join itself; it does retry heartbeat actions
    on the next beat. *Status*: documented on `OwnMembershipDriver`.

16. **Wasm test synchrony.** After `await manager.join(..)` resolves,
    every outbound call of the join has been made (the reply is sent after
    the last action), so the web tests can assert the outbound log without
    a tick; heartbeat-driven traffic needs real time (short timeouts in
    tests). *Status*: designed; matches the existing acceptance test
    "join arms the delayed leave before sending the membership".

## 8. Phases

Each phase ends with `cargo test` green natively; phase 5 adds the wasm run.

### Phase 1 — pure content (no state)
1. `types.rs`: `LeaveReason::{leave, delayed_leave, slot_closed}` constructors
   and the three code constants.
2. `wire.rs`: `join_content`, `leave_content`, `delayed_leave_content`,
   `Route`, `route()` dispatching on `ElementCallCompat` (compat arms call
   into phase 4 files; until then they return the spec route).
3. `mod.rs`: constants, `OwnIdentity`, `new_member_id`, `JoinParams::validate`,
   the `Status` family with derives, error enums with the new variants.

### Phase 2 — the pure machine
1. `machine.rs`: states, `Input`/`Action`/`Outcome`, `step`, `next_wake_ts`,
   `status`, `debug_json`. No driver, no clock: `now` is a parameter.
2. `test_support.rs`: `FakeClock`, snapshot builders (`seeded`, slot open /
   closed, roster with / without our id).
3. Every rule in §5 as a unit test (§9.2) — including the ported core tests,
   rewritten as scripted `Outcome` sequences.

### Phase 3 — the pump and the manager
1. `pump.rs` per §4.3; `OwnMembershipManager::new` spawns it; `join`/`leave`
   as command + oneshot; `status`/`subscribe_status`/`own_member`; `Drop` →
   notify.
2. Driver-facing changes: `send_delayed_event(.., sticky_duration_ms)`,
   `send_delayed_state_event` (§3.4, §3.5) on `driver::OwnMembershipDriver`
   and `uniffi_api` + `mockDriver.ts`.
3. Pump tests with a recording mock driver and real short timers (§9.3).

### Phase 4 — compat write side
1. `compat_2025.rs`: `rewrite_join`, `leave_content` (bare key) + ported
   tests.
2. `compat_msc3401.rs`: `state_key`, `member_id`, `member_content(spec,
   own, room_id, application, created_ts, lifetime_ms, now)`, `{}` leave +
   ported tests. `created_ts` pinned on the `JoinPlan` at the first send.
3. `wire::route` arms; `new_member_id` compat branch; `Delegate` skipped
   under `StateEvents`.

### Phase 5 — facade wiring and acceptance
1. `participation`: identity through `ParticipationManager::new`; `join` per
   §5.0 (seed wait → `new_member_id` → encryption machine →
   `own_membership.join`); `leave`; the `TransportResolver` over
   `connections.add_own_transport`; `Status` composition (`NotJoined` →
   `Disconnected`); `open_slot`/`close_slot` on the facade.
2. `uniffi_api`: `FfiJoinParams` gains `intent`, `degradedLifetimeMs`;
   status mapping; `FfiMatrixDriver` for the two driver additions.
3. `web-test-app/test/participation.test.ts`: make the existing join test
   pass and add §9.4.

### Phase 6 — hardening
- Self-heal (§3.7) and its rate limit, behind a `const` so it can be turned
  off in one line if it misbehaves in the field.
- Log tiers: state transitions `info`, heartbeat beats `trace`, every
  degradation `warn` once (not per beat).
- No `unwrap` on driver data; malformed `SendEventResponse` (no `event_id`)
  is tolerated.

## 9. Tests

Conventions: `machine.rs` tests are deterministic (fake `now`, scripted
outcomes, assert on the returned actions); `pump.rs` tests use the recording
mock driver and *short* real timers through `executor` (as
`encryption/pump.rs::tests` does); web tests go through the real bindings.
Names are the intended test names; the ported ones keep the core's name
where it still describes the behaviour.

### 9.1 `wire.rs` / compat renderers / `new_member_id`
- `join_content_has_slot_member_application_sticky_key_and_transports`
- `publishing_a_transport_declares_it_subscribable` (core `test_machine_join_with_transport`)
- `receive_only_publishes_nothing_and_keeps_can_subscribe`
- `empty_transports_are_omitted`
- `intent_lands_in_application_m_call_intent`
- `leave_content_carries_only_slot_member_key_and_reason` (core `test_leave_uses_unstable_sticky_key_and_round_trips`)
- `delayed_leave_content_uses_the_delayed_leave_code` (core `test_machine_join_schedules_delayed_leave`)
- `member_event_type_goes_out_in_the_unstable_spelling`
- `spec_route_is_sticky_with_the_published_lifetime_for_every_event_kind`
- `member_id_is_fresh_per_call` / `member_id_is_user_colon_device_under_state_events`
- 2025: `legacy_join_is_additive_and_keeps_every_spec_field`, `legacy_join_mirrors_published_into_rtc_transports`, `legacy_join_adds_versions_user_id_and_device_id_only_when_absent`, `legacy_leave_is_a_bare_sticky_key`, `legacy_delayed_leave_is_a_bare_sticky_key`
- MSC3401: `state_key_is_underscore_user_device_application_call_id`, `a_join_becomes_state_content_with_pinned_created_ts`, `a_refresh_moves_expires_but_not_created_ts`, `foci_preferred_carries_livekit_alias_equal_to_the_room_id`, `intent_defaults_to_video`, `a_leave_is_empty_content`, `the_route_is_a_state_event`

### 9.2 `machine.rs` — the policy
Join:
- `join_refuses_a_closed_slot`, `join_ignores_slot_state_for_the_legacy_slot`, `join_proceeds_when_slot_state_is_unsupplied`
- `the_member_list_flag_mirrors_the_seeded_snapshot`
- `publish_intent_resolves_the_transport_and_flips_both_transport_flags`
- `receive_only_skips_the_resolver`
- `a_failed_resolver_returns_to_not_joined_with_transport_unavailable`
- `a_second_join_while_joined_is_already_joined`
- `join_arms_the_delayed_leave_before_the_membership` (core)
- `a_homeserver_without_delayed_events_can_still_be_joined` (core)
- `the_first_membership_of_a_degraded_join_is_already_short` (core)
- `every_membership_of_a_join_states_one_lifetime` (core)
- `a_failed_join_returns_to_not_joined` (core) — and arms no heartbeat
- `sticky_duration_is_clamped_to_one_hour_with_a_warning`
- `delegation_raises_the_delay_to_one_hour`
- `a_successful_delegation_stops_client_restarts_and_records_the_timestamp`
- `a_failed_delegation_falls_back_to_client_restarts_of_the_same_delay`
- `delegation_is_skipped_under_state_events_and_when_nothing_is_armed`
- `own_member_reports_the_published_member_with_resolved_transports`

Heartbeat:
- `next_wake_is_the_earliest_of_restart_refresh_and_probe`
- `heartbeat_restarts_the_delayed_leave_in_place` (core `test_machine_heartbeat_restarts_delayed_leave`)
- `a_failed_restart_does_not_immediately_arm_a_replacement` (core)
- `a_delay_that_must_have_fired_is_replaced` (core)
- `heartbeat_refreshes_the_sticky_membership_once_half_expired` (core)
- `heartbeat_leaves_a_fresh_sticky_membership_alone` (core)
- `a_degraded_membership_is_refreshed_on_the_short_lifetime` (core)
- `a_degraded_heartbeat_restarts_nothing` (core)
- `a_stated_refusal_is_never_asked_again` (core)
- `an_unexplained_refusal_is_retried_and_can_recover` (core) — lifetime stays
- `a_failed_refresh_is_retried_soon_not_at_the_next_half_life`
- `no_heartbeat_actions_while_leaving_or_not_joined`

Session reactions:
- `a_closed_slot_leaves_with_code_slot_closed_and_settles_the_delay`
- `a_closed_slot_is_ignored_for_the_legacy_slot`
- `a_vanished_own_membership_is_resent_once_per_keep_alive`
- `an_excluded_own_membership_is_not_healed`
- `a_vanished_membership_with_a_fired_delay_is_rearmed`

Leave:
- `leave_sends_the_leave_then_cancels_the_delay` (core `test_machine_leave_sends_leave_event`)
- `leave_defaults_to_the_plain_leave_code`
- `leave_succeeds_when_the_delayed_event_already_fired` (core)
- `leave_still_fails_when_the_leave_event_cannot_be_sent` (core) — and returns to `Connected`
- `a_degraded_leave_cancels_nothing` (core)
- `heartbeat_stops_refreshing_the_sticky_after_leaving` (core)
- `leave_while_not_joined_is_an_error`
- `status_publishes_only_on_change`

### 9.3 `pump.rs` — wiring with real (short) timers
- `join_runs_the_sequence_in_order_and_replies_after_the_last_action`
  (resolver → delayed → sticky → delegate, in the recording mock's log)
- `the_heartbeat_restarts_the_delay_on_the_executor_clock` (15 ms timeout →
  ≥ 2 restarts within 50 ms)
- `a_leave_during_a_heartbeat_does_not_interleave_with_it` (mock blocks the
  restart; leave lands after it)
- `a_session_slot_close_leaves_without_a_host_call`
- `dropping_the_manager_stops_the_pump_and_releases_the_driver`
  (`Arc::strong_count`, `receiver_count`, like encryption's test)
- `status_watch_wakes_on_heartbeat_outcomes`

### 9.4 `web-test-app/test/participation.test.ts` — through the wasm bindings
- keep: `join arms the delayed leave before sending the membership` (make it
  pass; also assert `stickyDurationMs` on the delayed event and
  `keepAliveTimeoutMs` as `delayMs`).
- `join in StickyEvents compat adds rtc_transports, versions and member.user_id/device_id`
- `join in StateEvents compat sends a state event with an underscore state key and a user:device member id`
- `with keepAliveTimeoutMs 60 the mock sees restartDelayed within 200 ms` (proves timers on wasm)
- `leave sends membership: leave then cancelDelayed and the status returns to Disconnected`
- `delegateDelayedLeave is called after the membership when requested, with a ≥ 1 h delay`
- `a slot close state update makes the manager leave with code slot_closed`
- `the publishing transport appears in connections() right after join resolves` (the
  own-transport input to `connections`, §3.3)

## 10. Contract notes for the other modules

Things this plan relies on or changes elsewhere:

- **`driver::OwnMembershipDriver`**: `send_delayed_event` gains
  `sticky_duration_ms: Option<u64>` (§3.4, transitional); new
  `send_delayed_state_event` (§3.5, compat-only). `DriverError::Unsupported
  | Unauthorized` from the three delayed-event methods is read as "never"
  (§3.9) — adapters must map 404 `M_UNRECOGNIZED` / 403 `M_FORBIDDEN` to
  them. `DelegatedDelayedLeaveRequest.member` stays the full member content
  (MSC4533 style); the adapter sends `member.id` as MSC4195's `member_id`.
  The doc comment on `send_state_event` should mention both users: the
  facade's slot administration and this module's MSC3401 dialect.
- **`session`**: `SessionSnapshot.seeded: bool`, published `true` exactly
  once seeding finished (even after read failures). `excluded_candidates`
  must include our own candidate with its reason (self-heal decision).
  Publish-on-change is assumed (every `changed()` is a real change).
- **`participation`**: `ParticipationManager::new` takes `OwnIdentity` and
  passes it to this manager; owns the join lifecycle of §5.0 (seed wait,
  `new_member_id`, encryption machine, then `join`); implements the
  `TransportResolver` over `connections.add_own_transport`; maps
  `Status::NotJoined` to `Disconnected`; serialises `join`/`leave` calls;
  hosts `open_slot` / `close_slot` (port `RawSlotEventContent::{for_open,
  for_close}` from the core there, with the `slot_id` starts with
  `"{application_type}#"` check).
- **`connections`**: `add_own_transport(intent)` must return the transport
  it minted the token for and keep it as the *own transport* input, so the
  connection set is computed from session members **plus** our own transport
  (§3.3). Under `StateEvents` the legacy `/sfu/get` `room` field must equal
  the `livekit_alias` we write (`room_id`), or the two clients land in
  different LiveKit rooms (§6.3).
- **`types`**: `LeaveReason` constructors; `Member.membership_ts` is set to
  the join instant on our own member (needed by `encryption` under
  `StateEvents`, harmless otherwise).
- **`uniffi_api`**: `FfiJoinParams { intent: Option<String>,
  degraded_lifetime_ms: Option<u64> }`; `FfiStatus` unchanged (four
  values); the two driver additions on `MatrixDriverCallback`;
  `FfiParticipationManager::new` gains the identity; `mockDriver.ts`
  records `stickyDurationMs` on delayed events and a new
  `delayedStateEvent` call kind.
- **`MatrixSdkArchitecture.md`**: update the `own_membership` bullets —
  `join(member_id, intent, params)`, the `TransportResolver`,
  `Status::NotJoined`, `LeaveStatus` fields, the sticky delayed leave — and
  move the slot-administration bullet to `participation`.

## 11. Implementation status

| piece | status |
|---|---|
| `types.rs` `LeaveReason` constructors | todo |
| `wire.rs` (spec member content, `Route`) | todo |
| `mod.rs` (`OwnIdentity`, `new_member_id`, params, status, errors) | todo |
| `machine.rs` (pure policy: join/heartbeat/leave/session reactions) | todo |
| `test_support.rs` (mock driver, fake clock, snapshot builders) | todo |
| `pump.rs` + `OwnMembershipManager` (commands, watch, timers, drop) | todo |
| driver additions (`sticky_duration_ms`, `send_delayed_state_event`) + FFI + mock | todo |
| `compat_2025.rs` | todo |
| `compat_msc3401.rs` | todo |
| `participation` wiring (identity, §5.0 lifecycle, resolver, slot admin) | todo (facade is `todo!()`) |
| web `participation.test.ts` additions | todo |
| architecture doc update | todo |

Run everything (once implemented):

```sh
cargo test --lib own_membership
cargo check --features runtime-probe --target wasm32-unknown-unknown
cd web-test-app && npm run ubrn:web && npx vitest run test/participation.test.ts
```
