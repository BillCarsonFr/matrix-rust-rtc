# The `ParticipationManager` error surface, from a frontend's seat

A host that only sees `participation::ParticipationManager` has exactly this
toolbox:

| kind | members |
| --- | --- |
| fallible calls | `join -> JoinError`, `leave -> LeaveError`, `open_slot`/`close_slot -> DriverError` |
| polled reads | `session()`, `memberships()`, `connections()`, `key_map()`, `status()` |
| push | `on_status_change`, `on_memberships_change`, `on_connections_change`, `on_key_map_change` |
| escape hatch | `debug_snapshot() -> Value` |

Two structural facts govern everything below.

1. **`join`/`leave` are the only calls that can fail.** Every other failure
   happens inside a pump, is `log::warn!`ed, and must be *inferred* from a
   read. There is no error stream and no `Status::Failed`.
2. **All four callbacks are publish-on-change over `PartialEq`.** A failure
   that does not mutate a field of `Status` (or of the membership/connection
   lists) fires *nothing*. Such failures are only visible to a host that
   polls `status()` on its own timer — and the pump's own liveness is not
   observable either, so "nothing changed" and "the pump died" look the same.

Below: every failure the crate can produce today, how a host extracts it, and
where that breaks. ✅ = clean, ⚠️ = derivable but awkward/undocumented,
❌ = not extractable.

---

## 1. Before joining

### 1.1 The slot is closed ✅
`session().slot_state == Some(SlotState::Closed)` before the call, and
`join()` returns `JoinError::SlotClosed`. Both paths exist; the lobby can grey
out the join button and the race (closed between render and tap) is still
caught by the call.

### 1.2 The seed reads failed ❌
`Session` marks `seeded = true` *after the reads regardless of their outcome*
(`session/mod.rs:157`, `live.rs:393-457` — each failure is a `log::warn!`
only). So:

* `slot_state: None` means either "no `m.rtc.slot` event exists" **or** "we
  could not read it". A host cannot tell "the slot condition is unenforced
  because the server did not answer" from "this room has no slot".
* `negotiated_encryption: None` has the same ambiguity, and this one decides
  whether the call is E2EE. A host that renders a padlock from
  `session().encrypted` will show *unencrypted* for a room whose slot read
  timed out.

**Gap:** `SessionSnapshot` needs the seed outcome, e.g.
`seed_failures: Vec<(&'static str /* read */, DriverError)>` or at minimum a
`seed_complete: bool` distinct from `seeded`.

### 1.3 `join()` raced the seed ❌
`Inner::wait_seeded` gives up after `SEED_WAIT_MS` (5 s) and joins anyway with
only a `log::warn!` (`participation/mod.rs:400`). Consequences the host never
learns: the `SlotClosed` pre-check was skipped, and `manage_media_keys` fell
back to the *local* config instead of the slot's negotiation — i.e. we may
have joined an encrypted call unencrypted, or vice versa. Nothing in `Status`
or `SessionSnapshot` records that the join was made blind.

---

## 2. `JoinError`

| variant | cause | extractable |
| --- | --- | --- |
| `AlreadyJoined` | second `join()` | ✅ |
| `SlotClosed` | pre-check against a seeded snapshot | ✅ |
| `InvalidParams(String)` | `JoinParams::validate` **and** `encryption::MachineError::OwnDeviceUnknown` | ⚠️ |
| `TransportUnavailable(DriverError)` | `get_rtc_transports` failed **or** `get_livekit_token` failed | ⚠️ |
| `Driver(DriverError)` | the join membership event could not be sent | ✅ |

**Gap (`InvalidParams`):** `participation/mod.rs` maps *any*
`encryption::Machine::new` failure through `JoinError::InvalidParams(e.to_string())`.
A host sees `"invalid parameters: our own membership has no device id"` — a
crate-internal precondition dressed as a caller mistake, distinguishable only
by string matching. It deserves its own variant.

**Gap (`TransportUnavailable`):** two different user-facing stories collapse
into one variant — "your homeserver advertises no SFU" (a configuration
problem, retrying is pointless) and "the SFU refused to mint your token" (an
auth/network problem, retrying may work). Both arrive as
`TransportUnavailable(DriverError::Http(..))`.

**Gap (how far did the join get?):** `abort_join` resets the machine to
`NotJoined`, so the `JoinStatus` flags that say *which* step failed
(`has_created_transport_token`, `has_sent_delayed_leave_event`, …) are gone by
the time `join()` returns. A progress UI cannot say "we got a token but the
membership event was rejected". Either carry the flags in the error or keep a
`last_join_failure()`.

### 2.1 A join that "succeeds" degraded ✅ (Rust) / ❌ (FFI)
If the delayed leave cannot be armed, the join **does not fail**: the machine
degrades to `degraded_lifetime_ms` (default 5 min) and publishes anyway. The
host learns this only afterwards, from `ConnectedStatus`:

```rust
let Status::Connected(s) = pm.status() else { return };
if !s.own_membership.delayed_leave_supported {
    // no dead man's switch: if this client dies, a ghost tile survives
    // for up to s.own_membership.membership_lifetime_ms.
}
```

That is a genuine, well-shaped read. It just never crosses the FFI (§7).

---

## 3. While connected — the invisible half

Nothing here can return an error. Everything must be polled out of
`ConnectedStatus`, which today is:

```rust
delayed_event_kick_ts, heartbeat_last_restart_ts, delegation_setup_ts,
delayed_leave_supported, membership_lifetime_ms
```

### 3.1 The delayed-leave restart is failing ❌ — *the gap you named*

`Outcome::Restarted(Err(e))` only bumps `next_restart_at` and logs
(`machine.rs:872-886`). `last_restart_ms` — the source of both
`heartbeat_last_restart_ts` and `delayed_event_kick_ts` — is updated **only on
success**. So a failing restart changes no observable field, which means:

* **`on_status_change` never fires.** The single most important "you are about
  to be kicked" condition is push-invisible by construction.
* The host must poll and diff timestamps itself. The best recipe available
  today:

```rust
let Status::Connected(s) = pm.status() else { return };
let os = s.own_membership;
if os.delegation_setup_ts.is_none()          // see below — mandatory guard
    && let (Some(last), Some(kick)) = (os.heartbeat_last_restart_ts, os.delayed_event_kick_ts)
{
    let timeout = kick - last;               // keep_alive_timeout_ms is not exposed
    let cadence = timeout / 3;               // RESTARTS_PER_TIMEOUT is a private const
    if now_ms() > last + 2 * cadence { /* probably failing */ }
}
```

Three separate things are wrong with that:

1. `keep_alive_timeout_ms` is not in `ConnectedStatus`; it has to be recovered
   as `kick_ts - last_restart_ts`.
2. `RESTARTS_PER_TIMEOUT = 3` is private, so the healthy cadence is a guess.
   If it ever changes, every host's heuristic silently mis-tunes.
3. **After delegation the same fields freeze on purpose.** With
   `delegate_delayed_leave`, the client stops restarting entirely, so
   `heartbeat_last_restart_ts` stays pinned at the join and
   `delayed_event_kick_ts == delegated_at + timeout` — a *fixed* timestamp
   that goes into the past almost immediately. A healthy delegated client is
   byte-identical to a dying non-delegated one apart from
   `delegation_setup_ts`. A host that renders "kicked in {kick_ts - now}"
   will show a negative countdown for every correctly delegated call.

And the state you actually want to distinguish — *"restarts are failing but
the delay has not fired yet"* vs *"the delay must have fired; we were removed
server-side and a replacement is being armed"* (`must_have_fired`,
`machine.rs:145`) — is not representable at all.

### 3.2 The sticky membership refresh is failing ❌ — worse than 3.1
`MembershipSent { kind: Refresh, result: Err }` sets `refresh_retry_at` and
retries at `lifetime/10` (`machine.rs:917-920`). Neither `refresh_retry_at`
nor `sticky_sent_at` is exposed. `ConnectedStatus` gives
`membership_lifetime_ms` but **not when the membership was last published**,
so a host cannot compute its expiry, let alone notice it is not being
refreshed. Unlike 3.1 there is not even a frozen timestamp to watch.

The only signal is *lagging and indirect*: after the lifetime elapses the
server drops our sticky entry, we vanish from `memberships()`, and — if
`SELF_HEAL` and we were seen in the roster — the machine re-publishes at most
once per keep-alive. The user sees themselves flicker out of their own tile
list with no explanation.

### 3.3 We were kicked and self-healed ❌
`heal_if_vanished` re-publishes silently (`machine.rs:657`). No status field,
no counter, no callback. To detect it a host must watch its own entry in
`memberships()` — which brings us to:

### 3.4 There is no way to identify *ourselves* ❌ — a load-bearing gap
`ParticipationManager` exposes no `own_member_id()`. Every self-referential
check above (am I in the roster? was I excluded? does this tile hold my key?)
requires the member id the facade minted in `join()` and never handed back.

A host can *try* to match on `(user_id, device_id)` from
`SessionMembership::member`, but that is ambiguous by design: one device may
hold several RTC members, and a rejoin mints a fresh member id while
`(user_id, device_id)` stays the same. The host also needs the member id (via
`transport_identity`) to recognise its *own* LiveKit participant.

**Minimum fix:** `pub fn own_member_id(&self) -> Option<String>`, ideally
`own_membership() -> Option<SessionMembership>`.

### 3.5 Our membership is published but excluded from the projection ⚠️
`UnencryptedInEncryptedRoom`, `SenderNotInRoom`, `SlotClosed`, `Expired`: the
event landed, but the session refuses to project it, and `heal_if_vanished`
deliberately does **not** heal an excluded member (`machine.rs:666-673`). The
machine stays `Connected` while we are invisible to every peer — the worst
possible failure to debug from a UI.

It *is* extractable: `session().excluded_candidates` carries
`(Member, JoinExclusionReason)`. But it requires §3.4 to find our own entry,
`Status` actively says `Connected`, and nothing prompts a host to look. This
should be promoted into `Status` (`Connected { own_excluded: Option<JoinExclusionReason> }`)
rather than left as a snapshot field a host has to know about.

### 3.6 The slot was closed under us / any disconnect ❌
When the slot closes, the own-membership machine leaves on its own, the facade
reaps the participation, and `status()` becomes `Status::Disconnected`. That
variant is **a unit** — it carries no cause. `Disconnected` after an admin
closed the slot, after our own `leave()`, and after a join that never
completed are indistinguishable. The machine even builds a
`LeaveReason::slot_closed()` for the wire, and the host never sees it.

A host can guess from `session().slot_state == Some(Closed)`, but that read is
subject to §1.2 and races the reap.

**Fix:** `Status::Disconnected(DisconnectCause)` with at least
`NeverJoined | LeftByHost | SlotClosed | JoinFailed`.

### 3.7 Delegation was requested but fell back ✅
`Outcome::Delegated(Err)` is not fatal; the client keeps restarting the delay
itself. Derivable: the host knows it passed `delegate_delayed_leave: true`, so
`ConnectedStatus::delegation_setup_ts.is_none()` while connected *is* the
fallback signal. Worth documenting, because it changes app behaviour (a
delegated call may background; a fallen-back one must not).

### 3.8 "Unsupported" flickers ⚠️
`delayed_leave_supported` is literally `c.delayed.is_some()`. It reads `false`
both for "this homeserver refuses delayed events, permanently" and for "we
transiently failed to arm one just now and will re-probe in 5 min". The real
tri-state, `DelayedLeaveSupport::{Unknown, Supported, Unsupported{permanent}}`,
lives only in `debug_snapshot`. A "your homeserver can't clean up after a
crash" banner built on this field will flap.

---

## 4. Encryption

### 4.1 A peer's media key was rejected ❌ — the biggest missing callback
`KeyRejection` has nine well-shaped variants (`NotCrossSigned`,
`SenderMismatch`, `DeviceMismatch`, `Cleartext`, `UnknownOrigin`,
`UnattributableMember`, `WrongRoom`, `Outdated`, `NotManagingKeys`) and
`encryption::Machine` already has `set_key_rejected_callback`.

**`ParticipationManager` never calls it and exposes no `on_key_rejected`.**

So the answer to "why can't I hear Bob?" — *his device isn't cross-signed*,
*his key arrived in cleartext*, *the sender doesn't match his member event* —
is computed, typed, and then dropped on the floor. The only symptom a host can
observe is negative and permanent: Bob is in `memberships()`, absent from
`key_map()`, and `Status::Connected { encryption: .., fully_settled: false }`
never settles. Wiring the existing callback through the facade is a
~10-line change and is the single highest-value fix in this document.

### 4.2 A peer never sent us a key ✅
Derivable cleanly, and worth documenting as the supported recipe:

```rust
let have: HashSet<_> = pm.key_map().keys().collect();
let silent: Vec<_> = pm.memberships().iter()
    .filter(|m| m.state == MembershipState::Joined
             && m.member.device_id.is_some()
             && !have.contains(&m.member.member_id))
    .collect();   // "connecting…" on those tiles
```

Corroborated by `encryption::Status::{Joining{has_received_all_member_keys},
Connected{fully_settled}}`.

### 4.3 *We* could not deliver our key to a peer ❌
The inverse of 4.2 is not observable. `send_to_device` failures — whole-batch
(`Err`) or per-recipient (`ToDeviceDelivery::error`) — are retried with the
same key, and only served recipients enter `shared_with`. The crate knows
exactly who does not hold our key (`send.key_holders()`), but
`key_holders()` is not on the facade; it is used only internally to compute
`MembershipState::LeftWithKeys`.

`key_map()` is the **inbound** map: it says who *we* can hear, never who can
hear *us*. Aggregate `fully_settled: false` is the only hint, and it does not
name a member.

**Fix:** put it on the tile — `SessionMembership { holds_our_key: bool, .. }`.

### 4.4 Members who left but still hold our key ✅
`MembershipState::LeftWithKeys` plus
`encryption::Status::Connected { left_members_with_keys, last_rotation_ts }`.
This one is modelled well.

### 4.5 Unattributable / unverified peers ✅
`SessionMembership::member.device_attribution` gives
`Verified | Claimed | Unknown` for the shield UI.

---

## 5. Connections and tokens

### 5.1 A token could not be minted ⚠️
`State::connections()` `filter_map`s away every wanted key that has no token,
so a failed mint means the connection is **silently absent** from
`connections()`, retried every `MINT_RETRY_MS`. No error, no retry timestamp,
no reason (`retry_after` is private and not even in the facade's
`debug_snapshot`).

It is derivable only by diffing the two redundant views:

```rust
let held: HashSet<_> = pm.connections().iter()
    .map(|c| c.connection.service_url.clone()).collect();
let unreachable: HashSet<_> = pm.memberships().iter()
    .flat_map(|m| m.connections.iter().cloned())
    .filter(|u| !held.contains(u)).collect();
// members on those URLs will have no media, indefinitely
```

Nothing documents this, and the diff yields no cause — "the SFU is down",
"your OpenID token was refused", and "we haven't tried yet" are one state.

### 5.2 A token expired and could not be re-minted ❌
On a failed refresh the **old token is kept** (`Ok` inserts, `Err` only sets
`retry_after`). `connections()` therefore keeps serving a `ConnectionData`
whose `jwt_token` is expired, with nothing marking it stale — and
`ConnectionData` carries no `expires_at`, so the host cannot check either. The
LiveKit connection just fails from the host's side with no explanation from
this crate. This is a silent-wrong-answer, strictly worse than 5.1's silent
omission.

**Fix:** `ConnectionData { expires_at_ms: Option<u64>, .. }`, plus a
`connection_problems() -> Vec<ConnectionProblem { service_url, last_error, retry_at }>`.

---

## 6. `LeaveError` and shutdown

* `NotJoined` ✅, `Driver(e)` ✅ — a failed leave returns the machine to
  `Connected` and the facade correctly does **not** drop keys, so the host may
  retry.
* ⚠️ The delayed-leave cancel failing during leave is swallowed:
  `delayed_leave_settled` is set to `true` regardless and `leave()` returns
  `Ok` (`machine.rs:958-966`). Benign — the delay is itself a leave — but the
  host cannot know a stray delayed event may still land.
* ⚠️ A dead pump surfaces as
  `DriverError::Other("own membership manager stopped")` — stringly typed, and
  indistinguishable from any other `Other`.
* ❌ If the facade's own pump (`participation::run`) exits — every input watch
  closed — the manager freezes: `status()` keeps returning the last value and
  no callback ever fires again. There is no terminal/`Failed` state and no
  liveness signal.

---

## 7. Everything above, across the FFI

For a uniffi/wasm frontend the surface is far narrower than the Rust one:

* **`FfiStatus` is four opaque variants** — `Disconnected | Joining |
  Connected | Leaving` — with the doc comment *"the sub-statuses are in
  `debug_snapshot`"*. So **every** field this document relies on
  (`delayed_leave_supported`, `delayed_event_kick_ts`,
  `heartbeat_last_restart_ts`, `delegation_setup_ts`, `membership_lifetime_ms`,
  the whole `encryption::Status`) is reachable only by parsing an unversioned,
  explicitly-diagnostic JSON string. That is not a UI contract.
* **`FfiSessionSnapshot` drops `excluded_candidates` and `seeded`**, so §1.2
  and §3.5 are unreachable at any cost.
* `RtcError` collapses `DriverError::Http` and `Other` into `Driver(String)`.
* There is no key-rejection listener (§4.1) and no `own_member_id` (§3.4).

Anything promoted into a typed status below has to be mirrored here or the fix
does not reach the actual frontend.

---

## 8. Ranked gaps and the minimal API that closes them

1. **`on_key_rejected` on the facade** (§4.1). The machinery exists; wire
   `set_key_rejected_callback` through `Callbacks` and add
   `ParticipationManager::on_key_rejected`. Cheapest, highest value.
2. **`own_member_id()` / `own_membership()`** (§3.4). Unblocks §3.3, §3.5 and
   own-participant matching in LiveKit.
3. **`Status::Disconnected(DisconnectCause)`** (§3.6):
   `NeverJoined | LeftByHost | SlotClosed | JoinFailed`.
4. **Health in `ConnectedStatus`** (§3.1, §3.2, §3.8) — replace the raw
   timestamps with intent-revealing state:
   ```rust
   pub struct ConnectedStatus {
       pub delayed_leave: DelayedLeaveHealth,   // Armed { kick_ts } | Delegated { earliest_kick_ts }
                                                // | RestartFailing { since_ms, last_error }
                                                // | MayHaveFired | Unsupported { permanent: bool }
       pub keep_alive_timeout_ms: u64,          // stop making hosts derive it
       pub membership_last_published_ts: u64,   // §3.2
       pub membership_expires_at_ts: u64,
       pub membership_refresh_failing_since: Option<u64>,
       pub own_excluded: Option<JoinExclusionReason>,  // §3.5
       pub membership_lifetime_ms: u64,
   }
   ```
   Because these fields *change* when a restart fails, the failure finally
   becomes push-visible through `on_status_change` — the fix is not just more
   data, it is the difference between polling and being told.
5. **Per-tile key state** (§4.3): `SessionMembership { holds_our_key: bool }`.
6. **Connection health** (§5.1, §5.2): `ConnectionData::expires_at_ms` plus a
   `connection_problems()` list with cause and `retry_at`.
7. **Seed honesty** (§1.2, §1.3): distinguish "read failed" from "absent" in
   `SessionSnapshot`, and record that a join proceeded unseeded.
8. **Error taxonomy**: an `EncryptionSetup` variant on `JoinError` instead of
   `InvalidParams(String)`; split `TransportUnavailable` into discovery vs
   token; add `DriverError::RateLimited { retry_after_ms }` (today every
   `M_LIMIT_EXCEEDED` is an opaque `Http(String)`, so no host can back off
   correctly).
9. **Mirror all of it across the FFI** (§7) — a typed `FfiStatus`,
   `excluded_candidates` and `seeded` on `FfiSessionSnapshot`.
