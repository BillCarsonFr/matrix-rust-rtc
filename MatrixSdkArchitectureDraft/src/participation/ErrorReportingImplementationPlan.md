# Error reporting — implementation plan

Companion to [`ErrorSurfaceAnalysis.md`](./ErrorSurfaceAnalysis.md), which
enumerates what a frontend on the public `ParticipationManager` API can and
cannot learn. This plan closes those gaps.

---

## 0. The organising split

Every failure in the analysis is one of two things, and the two get
*different* API shapes:

**Recoverable — a live condition the crate is still working on.**
A retry loop is running; the condition clears by itself when the underlying
operation succeeds. It has no single moment of failure, so it can never be a
return value. **These belong in `Status`, and only in `Status`.** Examples:
the keep-alive restart failing, our sticky membership not refreshing, our
media key not reaching one participant, a token that cannot be minted, our
membership missing from the roster after a self-heal.

**Terminal — a one-shot fact.**
The participation is over, or never started, and only a host decision (a new
`join()`, a new manager) changes anything. These belong in the *errors of the
calls that produced them* and in `DisconnectCause`. Examples: `join()` was
refused, the slot closed under us, a pump died.

The dividing question is: **"if the host does nothing, can this clear on its
own?"** Yes → `Status`. No → an error / `DisconnectCause`.

One consequence drives the whole design. `Status` is published on change by
`PartialEq`; today a failing keep-alive restart mutates no observable field,
so **the most important failure in the crate fires no callback at all**
(analysis §3.1). Modelling the recoverable set as *state* rather than as
logged events is what turns polling into being told.

### Redundancy is deliberate

`ConnectedStatus` will carry both:

* **structured, mutually-exclusive per-mechanism state** — `KeepAlive`,
  `MembershipPublication`, `RosterPresence` — which is authoritative and
  carries the timestamps a UI renders countdowns from; and
* **a flat, severity-ordered `impairments: Vec<Impairment>`** derived from it
  — so a host that renders one warning banner cannot *miss* a condition it
  did not know to look for.

This is the same deliberate redundancy the crate already applies to
memberships vs connections (`participation/mod.rs` module doc). `impairments`
is a pure projection: it never carries a fact the structured fields do not.

---

# Part 1 — Better status reporting

## 1.1 `Status`

```rust
pub enum Status {
    Disconnected(DisconnectCause),
    Joining(JoinStatus),
    Connected(ConnectedStatus),
    Leaving(LeaveStatus),
}

pub struct JoinStatus {
    pub own_membership: own_membership::JoinStatus,
    pub encryption: encryption::Status,
    /// Live problems already visible during the join (session state unread,
    /// a key that has not reached a peer). Same vocabulary as `Connected`.
    pub impairments: Vec<Impairment>,
}

pub struct ConnectedStatus {
    pub own_membership: own_membership::ConnectedStatus,
    pub encryption: encryption::Status,
    /// Everything currently wrong, most severe first. Derived from the
    /// structured state above plus the connection and session views.
    pub impairments: Vec<Impairment>,
}

pub struct LeaveStatus {
    pub own_membership: own_membership::LeaveStatus,
    pub impairments: Vec<Impairment>,
}
```

`Status::Leaving` currently drops the encryption side entirely; keep it
absent (keys are being forgotten) but do carry impairments so a leave that
hangs on a failing driver is visible.

## 1.2 `Impairment` — the recoverable vocabulary

```rust
/// A condition that is true *right now* and that the crate is still working
/// on. Every variant clears by itself when the underlying operation
/// succeeds — an impairment is never terminal. Anything terminal ends the
/// participation and appears as [`DisconnectCause`] instead.
///
/// Timestamps are unix-ms, matching the rest of the crate (`_ts` = a point
/// in time, `_ms` = a duration).
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
    /// this (`machine.rs::heal_if_vanished`). Clears if the room state that
    /// caused the exclusion changes.
    OwnMembershipExcluded { reason: JoinExclusionReason },

    // ---- media keys ---------------------------------------------------

    /// Our current media key has not reached these members; they cannot
    /// decrypt us. Redelivery is in progress with the same key.
    MediaKeyNotDelivered { member_ids: Vec<String> },

    /// These members have not sent us a usable key; we cannot decrypt them.
    MediaKeyNotReceived { member_ids: Vec<String> },

    /// A key from this member was discarded and it still has none we can
    /// use. Carries the *why* the crate already computes today and throws
    /// away (analysis §4.1).
    MediaKeyRejected {
        member_id: String,
        sender_user_id: String,
        reason: KeyRejection,
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
    SessionStateUnread { reads: Vec<SessionRead> },

    /// `join()` went ahead before the session finished seeding
    /// (`SEED_WAIT_MS`): the slot-closed pre-check was skipped and the
    /// encryption decision fell back to local config instead of the slot's
    /// negotiation. Latched for the participation.
    JoinedBeforeSeed { at_ts: u64 },
}

/// Which seed read failed, for `SessionStateUnread`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SessionRead { Slot, RoomEncryption, RoomMembers, MemberEvents }

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

impl Impairment {
    pub fn severity(&self) -> Severity { .. }
}
```

Severity assignment (used to sort `impairments`, most severe first):

| variant | severity |
| --- | --- |
| `KeepAliveExpired`, `OwnMembershipMissing`, `OwnMembershipExcluded` | `Critical` |
| `KeepAliveRestartFailing`, `MembershipRefreshFailing`, `ConnectionTokenExpired`, `ConnectionUnavailable` | `Critical` |
| `MediaKeyNotDelivered`, `MediaKeyNotReceived`, `MediaKeyRejected` | `Degraded` |
| `KeepAliveUnavailable`, `SessionStateUnread` | `Degraded` |
| `JoinedBeforeSeed` | `Notice` |

**Naming rules applied.** No variant claims more certainty than the crate
has: `KeepAliveExpired`, not `RemovedFromCall` — we deduce the delay fired,
we did not observe it. No variant is named after the mechanism that failed
where the *consequence* is what a host renders: `MediaKeyNotDelivered`, not
`ToDeviceSendFailed`. `Missing` (was there, gone) and `Excluded` (there,
refused) stay separate because their remedies differ.

## 1.3 `own_membership::ConnectedStatus` — structured replacement

Replaces the five loose fields, which today cannot express the failing case
at all and whose meaning silently inverts after delegation (analysis §3.1).

```rust
pub struct ConnectedStatus {
    pub keep_alive: KeepAlive,
    pub membership: MembershipPublication,
    pub roster: RosterPresence,
}

/// The dead man's switch that clears our membership if this client dies.
/// Mutually exclusive states of one mechanism.
#[derive(Clone, Debug, PartialEq)]
pub enum KeepAlive {
    /// Armed, and we restart it ourselves.
    Armed { delay_ms: u64, last_restart_ts: u64, fires_at_ts: u64 },
    /// Handed to the SFU (MSC4195); we no longer restart it, so
    /// `last_restart_ts` freezing is expected, not a fault.
    Delegated { delegated_at_ts: u64, earliest_fire_ts: u64 },
    /// Armed, but restarts are failing — see `Impairment::KeepAliveRestartFailing`.
    RestartFailing { since_ts: u64, fires_at_ts: u64, last_error: String },
    /// Its delay elapsed without a successful restart; a replacement is
    /// being armed.
    Expired { since_ts: u64 },
    /// None armed. `permanent` = the homeserver refuses delayed events for
    /// good; otherwise we re-probe at `next_probe_ts`.
    Unavailable { permanent: bool, next_probe_ts: Option<u64> },
}

pub struct MembershipPublication {
    pub lifetime_ms: u64,
    pub last_published_ts: u64,
    /// `last_published_ts + lifetime_ms` — when the server drops us if no
    /// refresh lands. The host no longer has to derive this.
    pub expires_at_ts: u64,
    pub refresh_failing_since_ts: Option<u64>,
    pub last_refresh_error: Option<String>,
}

pub enum RosterPresence {
    /// Sent, echo not back yet.
    AwaitingEcho,
    Present,
    Missing { since_ts: u64, republished_at_ts: Option<u64> },
    Excluded { reason: JoinExclusionReason },
}
```

`JoinStatus`'s flags stay as they are — they are already good.

### Machine changes required (`own_membership/machine.rs`)

1. `Connected` gains `keep_alive_failing_since: Option<u64>`,
   `last_restart_error: Option<String>`, `last_refresh_error: Option<String>`,
   `roster_missing_since: Option<u64>`, `roster_excluded: Option<JoinExclusionReason>`.
2. `Outcome::Restarted(Err(e))` (line ~858) records `keep_alive_failing_since`
   (set once, on the first failure) and `last_restart_error`;
   `Restarted(Ok)` clears both. **This is the change that makes the failure
   push-visible.**
3. `Outcome::MembershipSent { Refresh|Heal, Err }` (line ~914) records
   `last_refresh_error`; `refresh_failing_since` follows the existing
   `refresh_retry_at`.
4. `heal_if_vanished` (line ~657) records `roster_missing_since` /
   `roster_excluded` / clears them on `Present`, and stamps
   `republished_at_ts` when it emits the `Heal` send.
5. **`next_wake_ts` must include `last_restart_ms + timeout_ms`** so
   `KeepAlive::Expired` is published the moment it becomes true rather than
   at the next restart beat. `Machine::status()` stays a pure projection —
   the expiry transition is performed by `on_wake`, never inferred inside
   `status()` (which has no clock).
6. `KeepAlive::Unavailable { permanent }` reads the existing
   `DelayedLeaveSupport` tri-state, which today only reaches
   `debug_snapshot` (analysis §3.8), and `next_probe_ts` from `probe_due_at()`.
7. Expose `RESTARTS_PER_TIMEOUT`-derived cadence implicitly: hosts get
   `fires_at_ts` and `delay_ms` directly and never need the private const.

## 1.4 Per-member key state

Per-tile truth, so "who can hear whom" is answerable without the aggregate.

```rust
pub struct SessionMembership {
    // .. existing ..
    /// `None` when this call does not manage media keys.
    pub media_key: Option<MediaKeyState>,
}

pub struct MediaKeyState {
    /// They hold our current key — they can decrypt us.
    pub holds_our_key: bool,
    /// We hold theirs — we can decrypt them.
    pub have_their_key: bool,
    /// Why their most recent key was discarded, while we still lack one.
    pub rejection: Option<KeyRejection>,
}
```

Two independent booleans, deliberately not folded into one enum — they fail
independently and a UI renders them in different places.

### Encryption changes (`encryption/mod.rs`, `inbound.rs`, `send_machine.rs`)

* Promote `Machine::key_holders()` to a facade-visible per-member query, or
  add `Machine::key_state(member_id) -> MediaKeyState`; `send_machine`
  already tracks `shared_with`, so `holds_our_key` is a lookup.
* `inbound` latches the last `KeyRejection` per `member_id`, cleared when a
  key from that member is accepted. Rejections already flow through
  `on_key_rejected`; latching them is what puts them in `Status`.
* `Machine::status()` gains nothing; the aggregate impairments are computed
  by the facade from per-member state, so there is one source of truth.

## 1.5 Connection health

```rust
pub struct ConnectionData {
    pub service_url: String,
    pub ws_url: String,
    pub jwt_token: String,
    /// From the JWT's `exp`; `None` when it carries none. A host can check
    /// its own token instead of discovering staleness through a failed
    /// LiveKit connect (analysis §5.2).
    pub expires_at_ts: Option<u64>,
}

pub struct ConnectionProblem {
    pub service_url: String,
    /// Members whose media is unavailable because of it.
    pub member_ids: Vec<String>,
    pub kind: ConnectionProblemKind,
    pub last_error: String,
    pub retry_at_ts: u64,
}

pub enum ConnectionProblemKind {
    /// Wanted, never minted — the connection is absent from `connections()`.
    NoToken,
    /// Present in `connections()` but its JWT is past `exp`.
    TokenExpired,
}
```

`ConnectionsManager` already holds `retry_after` and `Token::expires_at_ms`;
this only exposes them. Add `ConnectionsManager::problems()` and
`ParticipationManager::connection_problems()`, and keep the *stale* token in
`connections()` (dropping it would break a host that is still connected) —
now flagged rather than silent.

## 1.6 Session seed honesty

`SessionSnapshot` gains:

```rust
/// Reads that failed while seeding. `seeded` says the seed *finished*; this
/// says whether it learned anything. An empty vec is the healthy case.
pub failed_reads: Vec<SessionRead>,
```

`slot_state: None` / `negotiated_encryption: None` keep meaning "unknown",
but a host can now tell "unknown because absent" from "unknown because the
read failed" — which is the difference between rendering *unencrypted* and
rendering *unknown* (analysis §1.2).

`live.rs` collects the failures it currently only `log::warn!`s
(lines ~393, 409, 435, 446, 457) and clears an entry when a live state update
later supplies that value.

## 1.7 The facade: assembling impairments

`Inner::status()` becomes the one place that derives `impairments`:

| source | impairments |
| --- | --- |
| `own_membership::ConnectedStatus.keep_alive` | `KeepAliveRestartFailing` / `KeepAliveExpired` / `KeepAliveUnavailable` |
| `.membership` | `MembershipRefreshFailing` |
| `.roster` | `OwnMembershipMissing` / `OwnMembershipExcluded` |
| per-member `MediaKeyState` | `MediaKeyNotDelivered` / `MediaKeyNotReceived` / `MediaKeyRejected` |
| `connections.problems()` | `ConnectionUnavailable` / `ConnectionTokenExpired` |
| `session().failed_reads`, join-time flag | `SessionStateUnread` / `JoinedBeforeSeed` |

Sort by `severity()`, then by variant order, then by the ids inside, so the
vec is stable and `PartialEq` publish-on-change does not flap.

The facade pump must now also wake on connection-token failures and on
key-rejection latches; `key_map_changed` already covers the latter, and
`connections.subscribe()` the former — but note it publishes on *connections*
change, and a mint failure changes no connection. Add a `problems` watch or
fold problems into the existing watch value.

### Callback churn

`ConnectedStatus` already carries restart timestamps, so `on_status_change`
already fires roughly every restart beat; this plan does not regress that.
Add `Status::impairments()` plus a documented note that hosts wanting only
problem transitions should diff `impairments`, not the whole `Status`.

---

# Part 2 — Other errors

Terminal facts. Not impairments; they do not clear on their own.

## 2.1 `DisconnectCause` — why `Status::Disconnected` happened

Today `Disconnected` is a unit: an admin closing the slot, our own `leave()`,
and a join that never completed are indistinguishable (analysis §3.6).

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum DisconnectCause {
    /// No join has been attempted on this manager.
    NeverJoined,
    /// The host called `leave()`.
    LeftByHost { reason: Option<LeaveReason> },
    /// The slot was closed under us; the own-membership machine left on its
    /// own. The crate already builds `LeaveReason::slot_closed()` for the
    /// wire and currently discards it.
    SlotClosed,
    /// `join()` failed; the participation never started. Carries how far it
    /// got, which `abort_join` currently throws away (analysis §2).
    JoinFailed {
        at_ts: u64,
        progress: own_membership::JoinStatus,
        error: JoinError,
    },
    /// A pump stopped. The manager is dead and will not recover; the host
    /// must build a new one.
    ManagerStopped { component: Component },
}

pub enum Component { Session, OwnMembership, Connections, Encryption, Participation }
```

`JoinError` must derive `Clone` for this (`DriverError` and
`encryption::MachineError` already do).

## 2.2 `JoinError` taxonomy

```rust
pub enum JoinError {
    AlreadyJoined,
    InvalidParams(String),
    SlotClosed,
    /// The homeserver advertises no usable RTC transport — a configuration
    /// problem; retrying will not help.
    NoTransport(DriverError),
    /// A transport exists but its token could not be minted — auth or
    /// network; retrying may help.
    TokenRefused(DriverError),
    /// The encryption machine could not be built. Was
    /// `InvalidParams("our own membership has no device id")`, a crate
    /// precondition dressed as a caller mistake (analysis §2).
    EncryptionSetup(encryption::MachineError),
    /// The membership event could not be sent.
    Driver(DriverError),
}
```

`TransportUnavailable` splits into `NoTransport` / `TokenRefused`. The
resolver (`connections::add_own_transport`) must report which step failed —
today both collapse into one `DriverError` at the `TransportResolver`
boundary, so the resolver's future needs a two-variant error or a marker.

## 2.3 `LeaveError` and leave truthfulness

* A failed delayed-leave cancel currently sets `delayed_leave_settled = true`
  regardless and returns `Ok` (`machine.rs:958`). Keep returning `Ok` — the
  delay *is* a leave — but report it: `LeaveStatus.delayed_leave_settled`
  becomes `DelayedLeaveOutcome { Cancelled, MayStillFire }` so a host knows a
  stray delayed event may land.
* `DriverError::Other("own membership manager stopped")` becomes
  `DriverError::Stopped`, and surfaces as
  `DisconnectCause::ManagerStopped { component: OwnMembership }`.

## 2.4 Manager liveness

`participation::run` exits when every input watch closes; today the manager
then freezes silently (analysis §6). Give the pump a guard that, on exit,
publishes `Status::Disconnected(DisconnectCause::ManagerStopped { .. })`
through the status callback — the guard runs on a clean exit and on unwind.
A panic in the *callback dispatch* itself remains undetectable from inside;
document that boundary rather than pretending to cover it.

## 2.5 `DriverError`

```rust
pub enum DriverError {
    Http(String),
    Unauthorized(String),
    Unsupported(String),
    /// `M_LIMIT_EXCEEDED`. Today an opaque `Http(String)`, so no host can
    /// back off correctly (analysis §8).
    RateLimited { retry_after_ms: Option<u64> },
    Stopped,
    Other(String),
}
```

`classify_refusal` keeps treating only `Unsupported | Unauthorized` as
permanent; `RateLimited` is explicitly transient.

## 2.6 Identity: `own_member_id`

Not an error, but every self-referential check in Part 1 needs it, and the
host needs it to recognise its own LiveKit participant (analysis §3.4).

```rust
impl ParticipationManager {
    pub fn own_member_id(&self) -> Option<String>;
    pub fn own_membership(&self) -> Option<SessionMembership>;
}
```

Matching on `(user_id, device_id)` is not a substitute: one device may hold
several RTC members, and a rejoin mints a fresh id.

## 2.7 `on_key_rejected`

`encryption::Machine::set_key_rejected_callback` exists and the facade never
calls it. Wire it through `Callbacks` and add
`ParticipationManager::on_key_rejected(cb)`. **Secondary** to §1.4: the
latched per-member `rejection` is what a late-attaching UI reads; the
callback is for logging and telemetry.

## 2.8 `open_slot` validation

The slot-id/application mismatch returns `DriverError::Other`, conflating a
local precondition with a homeserver failure. Give `open_slot`/`close_slot`
their own `SlotError { InvalidSlotId(String), Driver(DriverError) }`.

## 2.9 Mirror everything across the FFI

None of the above reaches an actual frontend until this lands (analysis §7).

* `FfiStatus` becomes a real enum with payloads: `FfiJoinStatus`,
  `FfiConnectedStatus`, `FfiLeaveStatus`, `FfiDisconnectCause`, plus
  `FfiKeepAlive`, `FfiMembershipPublication`, `FfiRosterPresence`,
  `FfiImpairment`, `FfiSeverity`. Delete the "the sub-statuses are in
  `debug_snapshot`" comment — `debug_snapshot` goes back to being
  diagnostics, not a UI contract.
* `FfiSessionSnapshot` gains `seeded`, `failed_reads` and
  `excluded_candidates`.
* `FfiMembership` gains `media_key`.
* `FfiConnectionData` gains `expires_at_ts`; add `connection_problems()`.
* `RtcError` gains `RateLimited`, `NoTransport`, `TokenRefused`,
  `EncryptionSetup`, and stops collapsing `Http`/`Other` into `Driver`.
* `FfiParticipationManager` gains `own_member_id()`, `own_membership()`,
  `set_key_rejected_listener()`.

---

# Order of work

Each step compiles and is testable on its own.

1. **`DriverError`** (§2.5) — additive, touches every driver impl once.
2. **Machine state for recoverable conditions** (§1.3) — the `Connected`
   fields, the outcome handlers, the `next_wake_ts` expiry wake. Pure-machine
   unit tests: a failing restart yields `RestartFailing`, and crossing
   `last_restart_ms + timeout_ms` yields `Expired` *without* a further
   outcome arriving.
3. **`own_membership::ConnectedStatus`** rewritten to the structured shape;
   port existing tests.
4. **Session seed honesty** (§1.6).
5. **Connection health** (§1.5) + the problems watch.
6. **Per-member key state** (§1.4) + latched rejections.
7. **`Impairment`, `Severity`, and facade assembly** (§1.2, §1.7) — the
   payoff step; everything before it is plumbing.
8. **`DisconnectCause`, `JoinError` split, leave truthfulness, liveness
   guard, `own_member_id`, `on_key_rejected`, `SlotError`** (§2.1–§2.8).
9. **FFI mirroring** (§2.9) and the web test app.
10. **Docs**: fold the new surface into `MatrixSdkArchitecture.md` §
    `participation`, and add an "observing failures" section to the README
    with the three recipes the analysis spells out (own membership health,
    who-can-hear-whom, unreachable connections).

## Test obligations

* Every `Impairment` variant has a test that *raises* it and a test that
  *clears* it — an impairment that cannot clear is a modelling bug.
* A test asserting `on_status_change` **fires** when a keep-alive restart
  starts failing. This is the regression the whole plan exists to prevent.
* A test asserting `KeepAlive::Delegated` never produces
  `KeepAliveRestartFailing` — the frozen-timestamp footgun of analysis §3.1.
* `impairments` ordering is stable across recomputation with unchanged
  inputs (no callback flapping).
