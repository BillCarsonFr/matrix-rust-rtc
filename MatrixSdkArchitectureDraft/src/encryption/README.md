# `encryption` — per-member media keys over to-device messages (MSC4143)

Plan and status for the encryption module of the draft crate. Read together
with `../../MatrixSdkArchitecture.md` (module boundaries) and
`../../../KEY_ROTATION.md` (the rotation policy currently shipped in
`matrix-rtc-core`, which this module deliberately departs from — see
[Differences to the shipped core policy](#differences-to-the-shipped-core-policy)).

Contents

1. [Scope](#scope)
2. [The rotation approach: matrix-js-sdk PR #5505](#the-rotation-approach-matrix-js-sdk-pr-5505)
3. [Design of this module](#design-of-this-module)
4. [Differences to the shipped core policy](#differences-to-the-shipped-core-policy)
5. [Challenges](#challenges) — every known problem, the decision taken, and its status
6. [Runtime verification: `tokio::sync::watch` and timers under uniffi/wasm](#runtime-verification-tokiosyncwatch-and-timers-under-uniffiwasm)
7. [Tests](#tests) — the list of tests this module needs
8. [Decisions taken](#decisions-taken)
9. [Implementation status](#implementation-status)

## Scope

The module owns exactly two things:

- **Outbound**: our own media key — when to mint a new one, whom to send it
  to, when to start encrypting with it (`send_machine.rs`, driven by
  `pump.rs`).
- **Inbound**: everybody else's media keys — verify, store, and hand them to
  the host as the `KeyMap` (`inbound.rs`).

It does *not* do frame encryption (host's LiveKit SDK), does not send or
receive anything except through the `ToDeviceDriver` slice, and does not
decide *whether* keys are managed at all — the session's
`negotiated_encryption` does (see `session`).

Outputs: `key_map()` + change callback, and one `status()`:

```rust
enum Status {
    Joining   { has_distributed_initial_keys, has_received_all_member_keys },
    Connected { left_members_with_keys: Vec<Member>, fully_settled, last_rotation_ts },
}
```

`Joining` until our first key batch was served and every remote member with
a device has sent us a key; `Connected` from then on, for good (a later
joiner whose key is still missing shows as `fully_settled == false`). A
machine that manages no keys is `Connected` from the start.

Lifecycle: a `Machine` lives for exactly **one participation** — constructed
with our fresh membership, working from the moment it exists, done when it
is dropped (see [Lifecycle](#lifecycle-one-machine-per-participation)).

## The rotation approach: matrix-js-sdk PR #5505

["Encryption-Key rotation slow down"](https://github.com/matrix-org/matrix-js-sdk/pull/5505)
(branch `toger5/rotation-slow-down`, open) changes
`RTCEncryptionManager.rolloutOutboundKey`. Summary of the problem and the
mechanism, in the PR's own terms:

### The problem: quadratic, synchronised traffic

Every participant encrypts with its own key and distributes it to every other
participant as one Olm-encrypted to-device message. With `N` participants:

- one rotation by one client costs `N - 1` messages,
- one membership change (join/leave) makes *every* client rotate, so it costs
  `N * (N - 1)` messages for the call as a whole,
- and every client sees the change at the same moment, so all `N` rotations
  hit the homeserver in the same second (lockstep bursts).

### The mechanism: a shared per-minute contingent, a grace period, and jitter

One configuration value replaces the old fixed `keyRotationGracePeriodMs`:

```ts
/** To-device messages the whole call may spend per minute. Default 3000. */
sharedPerMinuteToDeviceContingent?: number;
```

From it, every client derives how long it must wait between two of its own
rotations, so that the *call* stays inside the contingent:

```
gracePeriodMs(N) = 60_000 * N * (N - 1) / contingent
```

| contingent | 50 users | 100 users | 200 users |
| --- | --- | --- | --- |
| 2000 | 1.2 min | 4.9 min | 19.9 min |
| 3000 | 0.8 min | 3.3 min | 13.2 min |
| 5000 | 0.5 min | 1.9 min | 7.9 min |

`N` counts the current memberships *including our own*. The grace period is
recomputed from the live participant count every time it is used, so it grows
and shrinks with the call.

The PR discusses two ways to apply it and implements the second:

1. **Time windows** — each client starts a (jittered) window; joins/leaves
   inside a window are recorded and answered by one rotation at the window's
   end; the next window's length is recomputed from the participant count.
2. **Block + jitter** (implemented, smaller diff to the existing code) — a
   rotation is *blocked* until `rotationBlockedUntilTs`. If the key rotated
   recently, the next rotation waits for the block to end. If it did not, the
   block is started *now* with a random length so that the burst every client
   would otherwise produce at the same instant is spread out.

The state is two timestamps, `rotationBlockedUntilTs` and
`scheduledForBlockTs`, plus one timer. Rules, in the order the code checks
them (`jitter = Math.random() * 2`, uniform in `[0, 2)`):

| situation | rotate? | send to | block | schedule a wake-up |
| --- | --- | --- | --- | --- |
| no membership change and not a scheduled wake-up | — | nobody | — | — |
| `N >= keyRotationParticipantLimit` (hard limit, optional) | no | joiners only, current key | — | — |
| first key ever | no (it *is* the key) | everyone | `now + grace * jitter` | no |
| scheduled wake-up fires | **yes**, everyone gets the new key | everyone | `now + grace` (no jitter) | no |
| change while **not blocked** (`blockedUntil <= now`) | not now | joiners only, current key | `now + grace * jitter` | at `blockedUntil` |
| change while **blocked** (`now < blockedUntil`) | not now | joiners only, current key | unchanged | at `blockedUntil`, once |

"Membership change" means: someone we already shared the current key with is
gone (`anyLeft`), or someone is present whom we have not served yet
(`anyJoined`). A participant whose `membershipTs` changed counts as left *and*
joined. A scheduled wake-up **always rotates** — it only ever exists because a
change happened during the block — and does **not** schedule the next one; that
happens only if another change lands during the fresh block.

Two further rules survive from before the PR:

- **`useKeyDelay`** (1 s in `join()`): after sending a rotated key, wait before
  encrypting with it, so recipients have it installed. The first key is used
  immediately.
- **`keyRotationParticipantLimit`** (default: none): at or above this many
  participants rotation is suppressed entirely; the current key is still handed
  to joiners. Surfaced to the UI as `isKeyRotationSuppressed`. **Not ported**:
  the contingent is the only brake here (see [Decisions](#decisions-taken)).

### What the PR's tests pin down (we port these — see [Tests](#tests))

- A leave in a settled call rotates after `grace * jitter`, to the remaining
  members only, and the new key is used locally only after `useKeyDelay`.
- Changes during a block do not move the deadline and do not earn extra
  delays; the rotation lands exactly when the block started by the *previous
  key* ends (blocks are anchored to the key, not to the last change).
- A **simulated call** (10 / 100 / 300 participants, 25 simulated clients,
  seeded PRNG, one flapping participant joining/leaving four times per grace
  period for six periods, then quiet): the extrapolated to-device rate stays
  under the contingent, every client's rotation gap ≥ the grace period, the
  mean gap ≤ 1.5 × grace, rotations do not happen in lockstep (>50% land on
  distinct instants), everything goes quiet after the last change plus two
  grace periods, and larger calls rotate proportionally less often but never
  stop.

## Design of this module

### Layout

```
src/encryption/
  README.md          this file
  mod.rs             public types, Machine (owns everything below), status
  matrix_encryption_event.rs m.rtc.encryption_key content: build + parse, base64
  legacy_element_call.rs   io.element.call.encryption_keys dialect (compat, deletable)
  inbound.rs         verification rules, early-key buffer, OutdatedKeyFilter, KeyMap store
  send_machine.rs    the rotation/distribution policy — pure, clock + jitter injected
  rotation_simulation.rs   #[cfg(test)]: the PR's simulated call against send_machine
  pump.rs            the one task: session watch + deadline timer + driver sends
```

### Principle: the policy is a pure state machine, the pump owns time

The PR's manager mixes policy and `sleep()`s. Here the policy is a value type
that never awaits, never reads a clock, and never draws a random number:

```rust
impl SendMachine {
    /// One per participation: our membership and the negotiated decision.
    fn new(config: SendMachineConfig, own: Participation, manage_media_keys: bool) -> Self;
    /// The session changed. `now` and `jitter` (uniform [0,2)) are supplied.
    fn on_session(&mut self, members: &[Member], now: u64, jitter: f64) -> Vec<Action>;
    /// The deadline from `next_wake_ts()` arrived.
    fn on_wake(&mut self, now: u64) -> Vec<Action>;
    /// A batch finished; only recipients that actually got the key are recorded.
    fn on_delivered(&mut self, key_index: u8, served: &[Participation], now: u64);
    fn next_wake_ts(&self) -> Option<u64>;
}
enum Action {
    Send { key: MediaKey, to: Vec<Participation> },
    /// Start encrypting with this key (the `useKeyDelay` has elapsed).
    UseOwnKey(MediaKey),
}
```

`pump.rs` is the only place with `await`s and timers:

```
loop {
    select! {
        _ = session.changed()            => actions = machine.on_session(&snapshot, now(), jitter()),
        _ = sleep_until(next_wake_ts)    => actions = machine.on_wake(now()),
        msg = to_device.recv()           => inbound.receive(msg) -> key map change,
    }
    for Send { key, to } in actions { served = driver.send_to_device(..).await; machine.on_delivered(..) }
}
```

Sends are awaited *inside* the loop, so session changes that land during a
batch coalesce into the next iteration (`watch` keeps only the latest value) —
the same "one rollout at a time, re-run once afterwards" semantics as the PR's
`currentKeyDistributionPromise` / `needToEnsureKeyAgain`, without the flags.

Why: every timing rule becomes a unit test with a fake clock and a pinned
jitter (the PR does the same with `vi.useFakeTimers()` and
`Math.random` mocks), the simulation test runs thousands of rollouts without
real time passing, and the policy is identical on native and wasm because
only `pump.rs` touches the platform (`crate::executor`).

One rule on top of the PR's: **a rotation never lands on a key that is not
in use yet.** If the deadline for an owed rotation arrives while the previous
rotated key is still inside its `use_key_delay`, the rotation is deferred to
the switch and performed right after it (same `on_wake` step). This replaces
any floor on the grace period: small calls keep the PR's numbers (40 ms at
`N = 2`) but can never mint over a key that is still propagating
(`a_rotation_owed_while_a_key_switch_is_pending_waits_for_the_switch`).

### Lifecycle: one machine per participation

`Machine::new(driver, room, slot, compat, session, own: &Member,
manage_media_keys, config, send_config, on_key_map_change)` starts working
immediately against the current session snapshot; dropping it is leaving
(the pump exits, the driver streams are released, every key is forgotten).
There is no `join`/`leave`; a rejoin is a new machine. The facade holds
`Option<Machine>` and, in its `join()`:

1. generates the fresh `member_id`,
2. constructs the encryption machine with it — **before** the own-membership
   machine sends the join event, so our key is on its way when our member
   event lands (peers hold keys that arrive early for 5 min) and we are
   already subscribed to the to-device stream when theirs come back —
3. then runs the own-membership join.

This was evaluated against keeping `join()`/`leave()` on the machine; the
reasons to prefer the RAII form, and the reasons considered for going the
other way:

For one machine per participation:

- Our `member_id` is fresh per join, so a machine's identity *is* a
  participation; a `join()` that resets everything is a constructor spelled
  differently, and a `leave()` that clears everything is `Drop`.
- "Unencrypted call" becomes "no machine" (`Option<Machine>`), which is the
  no-op manager the js-sdk comments wish they had, instead of a
  `manage_media_keys = false` machine that still consumes streams.
- No half-states: nothing can be observed between "constructed" and
  "joined" or after "left" — `key_map()` after leaving is impossible, not
  merely empty; no `NotManagingKeys`/`not joined` branches in the pump.
- Ordering is enforced by construction: the facade cannot forget to start
  key distribution before the join event, because the machine exists
  before the own-membership step runs.
- Streams are re-subscribed per participation, so the foreign driver's
  fan-out never carries a stale subscriber from a previous join.

Reasons that would argue for keeping `join`/`leave` (considered, judged
not decisive here):

- **Keys across a rejoin.** A leave-and-rejoin throws away every remote key.
  Peers rotate on our leave anyway (we are a leaver to them) and send fresh
  keys to our new membership, so nothing useful is lost; the cost is a
  short window equal to their rotation deferral, identical in both designs.
- **`negotiated_encryption` may not be known at construction** if the
  session is still seeding. With `join()` the decision could be taken
  lazily on the first snapshot. But the facade must already wait for the
  slot state to run the join conditions, so it knows the decision when it
  constructs the machine; passing `manage_media_keys` explicitly keeps the
  decision in one place.
- **Re-subscription cost**: one `subscribe_to_device_events()` and one
  `watch` receiver per join — negligible, and the previous ones are dropped.
- **Toggling encryption mid-call** (slot flips while joined) would need a
  running machine to change mode. Neither design handles it today; if it
  ever must, "drop and construct" is the same operation as a rejoin.
- **FFI ergonomics**: the machine is never an FFI object; the facade owns it,
  so the foreign side sees no difference.
- **Status while not joined**: the facade's `Option<Machine>` is `None`,
  mapping to `Disconnected`. Slightly more code in
  the facade, none in the machine.

### Participation identity

Every diff (`joined`, `left`, `shared_with`) compares
`Participation { member_id, user_id, device_id, membership_ts }` by
`same_join()` = `member_id` **and** `membership_ts`. Under MSC4143 `member_id`
is fresh per join, so the timestamp is redundant; in the MSC3401 compat
dialect the id is `{user}:{device}` and a leave-and-rejoin comes back under
the *same* id as a fresh session holding no keys — compared by id alone it
would never be served (KEY_ROTATION.md, "What counts as the same
participation"; the PR keys on `membershipTs` for the same reason). This
needs `Member.membership_ts` (see [Challenges](#challenges), item 9).

### Inbound: verification, buffering, storage

Kept from `matrix-rtc-core` (see `inbound.rs`), applied in this order:

1. `room_id` in the message must be ours → else `WrongRoom`.
2. The to-device message must have arrived Olm-encrypted → else `Cleartext`.
3. If `require_cross_signed_sender` (default on, MSC4153) the sending device
   must be cross-signed → else `NotCrossSigned`.
4. Decode the key (standard base64, padding optional). Any length is
   accepted; ≠ 32 bytes is logged (js-sdk sends 16).
5. Find the member event for `member_id`. **Not found → buffer** the key
   *with its origin* (bounded, see Challenges 11) and retry on every session
   change; found → continue.
6. The to-device `sender` must equal the member event's sender →
   `SenderMismatch`. The sending device must equal the member's device:
   verified device (from the sticky event's decryption metadata) or claimed
   device (MSC3401 state key — a claim *narrows* what we accept, never
   widens) → `DeviceMismatch`; member with no attributable device →
   `UnattributableMember`; member origin unknown (host reported nothing) →
   check skipped.
7. `OutdatedKeyFilter`: a `(member, index)` slot that was filled by a *newer*
   arrival rejects this one (`Outdated`). Equal timestamps pass (two keys in
   the same millisecond are a rekey seen twice). Rejected keys in steps 1–6
   never touch the filter, so an impostor cannot poison a slot.
8. Store: one entry per `(member, index)`; identical bytes → silently
   ignored (redelivery); different bytes at a known index → replaced
   ("rekey"). Emit the key map change.

Rejections are reported through a callback with the reason (for diagnostics
and the `debug_snapshot`), never as errors — a discarded key is not a
failure of the machine.

### Wire format

Outbound (`m.rtc.encryption_key`, sent as `org.matrix.msc4143.rtc.encryption_key`
via `wire_event_type`; both spellings accepted inbound):

```json
{
  "room_id": "!room:example.org",
  "member_id": "<our member.id>",
  "media_key": { "index": 3, "key": "<base64, 32 bytes>" },
  "format": 0
}
```

With `ElementCallCompat::StateEvents` the message is rendered as
`io.element.call.encryption_keys` (`keys: {index, key}`, `member.id`,
`device_id`, `call_id`, `session`, `sent_ts`) and parsed back from either the
object or the historical array form — all of it in `legacy_element_call.rs`
so deleting the dialect is deleting one file plus one match arm.

### Status and `LeftWithKeys`

- `Joining → Connected` latches when `has_distributed_initial_keys` (first
  batch served, or nobody to serve) and `has_received_all_member_keys` are
  both true.
- `left_members_with_keys` = participations that hold the key our media is
  *currently encrypted with* but are no longer in the session. A leaver stays
  listed through the owed rotation and the `use_key_delay` after it, and drops
  out when we switch to the new key (which they never received). This feeds
  the facade's `MembershipState::LeftWithKeys`.
- `fully_settled` = no wake-up scheduled, no `pending_use`, and every current
  remote member with a device has sent us a key.
- `last_rotation_ts` = `creation_ts` of the current outbound key — a joiner
  handed the current key can decrypt back to this instant.
- `has_distributed_initial_keys` = first batch served;
  `has_received_all_member_keys` = every joined remote member (with a device)
  has ≥ 1 key in the map.

## Differences to the shipped core policy

`matrix-rtc-core` implements the expiry model in `KEY_ROTATION.md`: fixed
`key_rotation_grace_period_ms` (10 s), `delay_before_use_ms` (5 s),
`max_key_lifetime_ms` (1 h 30), rotation = expiry arriving, joiners into a
fresh key leave nothing owed. This module implements the PR instead, because
the PR is what Element Call will ship and the two sides of a call must agree
on cost, not on mechanism — but the differences matter and are worth
stating:

| | core (`KEY_ROTATION.md`) | this module (PR #5505) |
| --- | --- | --- |
| grace period | constant 10 s | `60 s · N(N−1) / contingent`, grows with the call |
| a join into a settled key | rotates immediately (joiner gets the outgoing key too) | joiner gets the current key; rotation deferred by `grace · jitter` |
| a join into a fresh key | nothing owed (only leavers dirty a key) | rotation owed at the end of the block |
| lockstep between clients | not addressed | jitter `[0, 2) · grace` |
| lifetime cap | 1 h 30 | none in the PR — kept here as an *optional* `max_key_lifetime_ms`, off by default |
| timers | none; host polls `rotation_due_at_ms` / `flush_due_rotation` | owned by `pump.rs` via `crate::executor`; the *policy* still exposes `next_wake_ts()` so a host-driven mode stays possible |
| delayed use | `use_after_ms` travels as data to the consumer | the machine emits the own key only when it may be used (`UseOwnKey` after `use_key_delay_ms`); an owed rotation waits for that switch |
| hard participant limit | none | none (the PR's `keyRotationParticipantLimit` is not ported) |
| lifecycle | `join()`/`leave()` on a long-lived manager | one `Machine` per participation, dropped to leave |

The deterministic-core / self-driving-pump split keeps the core's best
property (every number is a unit test) while giving hosts the PR's simpler
contract (no deadline polling).

## Challenges

Each item: what the problem is, what this plan does about it, current status.

1. **No tokio runtime on wasm, so no `tokio::time`.** The PR's algorithm is
   timer-driven (`sleep(blockTime)`, `sleep(useKeyDelay)`), and
   `tokio::time::sleep` panics on wasm32-unknown-unknown. *Decision*: all
   time goes through `crate::executor::{spawn, sleep_ms, now_ms}` — tokio
   current-thread runtime on its own thread natively,
   `wasm_bindgen_futures::spawn_local` + `gloo_timers` + `web_time` on wasm.
   Nothing else in the crate may touch tokio `rt`/`time` or wasm-bindgen.
   *Status*: implemented and verified end to end (see
   [Runtime verification](#runtime-verification-tokiosyncwatch-and-timers-under-uniffiwasm)).

2. **Detached tasks.** The pump must outlive the FFI call that created the
   manager (a *synchronous* constructor, and sink `emit`s — no ambient
   runtime context natively either, so `tokio::spawn` would panic there).
   *Decision*: `executor::spawn` uses a crate-owned runtime handle natively
   (the matrix-rust-sdk FFI pattern) and `spawn_local` on wasm. *Status*:
   implemented, tested natively (`executor::tests`) and on wasm (probe).

3. **`Send` vs `?Send`.** Driver futures are `Send` natively (uniffi's tokio
   runtime) and `?Send` on wasm (they wrap JS promises); `executor::spawn`
   has the matching bound per target. The pump is written once and must
   compile under both — no `Send` bounds of our own on trait objects held
   across `await`s beyond what `async_trait` already implies. *Status*:
   pattern in place (dual `cfg_attr` on the driver traits); pump not yet
   written.

4. **tokio feature unification in the ubrn shim breaks wasm.** The
   ubrn-generated wasm crate is `edition = "2018"` (feature resolver v1),
   which unifies *target-specific* features into every build. A native-only
   `tokio/rt-multi-thread` therefore reached the wasm build and failed with
   "Only features sync,macros,io-util,rt,time are supported on wasm".
   *Decision*: one identical tokio feature set on all targets —
   `["sync", "rt", "time"]`, all wasm-allowed — and a *current-thread*
   runtime natively. *Status*: fixed, reproduced before/after with
   `npm run ubrn:web`.

5. **`emit` is no longer synchronous end to end.** The session module's
   doc claims "an `emit` is fully processed before it returns". With pumps
   on `spawn_local` that is false: the watch send inside `emit` only *wakes*
   the pump; it runs on the microtask queue after `emit` returns. *Decision*:
   accept it (it is also what makes the wasm build free of re-entrancy
   hazards) and have the web tests `await` a tick after injecting events
   (`runtimeProbe.test.ts` already does). The session doc needs the same
   correction. *Status*: documented; session doc to update.

6. **Randomness on wasm.** Key material (32 bytes, CSPRNG) and the jitter
   draw need an entropy source; `getrandom 0.2` refuses to build for
   wasm32-unknown-unknown without its `js` feature, `getrandom 0.3` needs an
   extra `--cfg` rustflag that the ubrn build does not set. *Decision*:
   `getrandom = { version = "0.2", features = ["js"] }` (what the core does),
   jitter derived from 8 random bytes. The policy takes `jitter` as a
   parameter so tests pin it. *Status*: dependency to add with `send_machine.rs`.

7. **Grace period collapses for small calls.** `60 s · N(N−1) / 3000` is
   40 ms at `N = 2` and 120 ms at `N = 3`: small calls rotate practically on
   every change, and the block is shorter than `use_key_delay` (1 s), so the
   PR can mint a key while the previous one is still propagating (its doc
   comment notes the grace "must be higher than `useKeyDelay` to have an
   effect"). *Decision*: no floor on the grace period (the PR's numbers are
   kept exactly); instead an owed rotation **blocks until the pending key
   has been switched to**, then runs. *Status*: implemented in
   `SendMachine::on_wake`, tested.

8. **Key length and base64.** js-sdk mints 16-byte keys, the core 32; the
   core encodes *padded* standard base64 while its comment says unpadded;
   js-sdk's `decodeBase64` accepts both. Keys are HKDF input on the LiveKit
   side, so length is not an interop problem as long as nobody rejects the
   other's. *Decision*: mint 32 bytes, encode padded standard base64, decode
   leniently (padding optional), accept any inbound length (log ≠ 32).

9. **`Member` lacks what the module needs.** (a) `membership_ts` for
   `same_join` (item above); (b) whether `device_id` is *verified* (sticky
   event decryption metadata) or *claimed* (MSC3401 state key) — the device
   check treats the two differently. *Decision*: add
   `Member.membership_ts: Option<u64>` and a `DeviceAttribution { Verified,
   Claimed, None }`-style field (or reuse `EventOrigin` on the member).
   *Status*: type change pending; touches `session::convert` and `FfiMember`.

10. **Recipient addressing.** A key goes to *the device that published the
    membership*, never `*`. Members without a device id cannot be served —
    they are skipped with a log line and never enter `shared_with` (so they
    also never cause a rotation when they leave). Our own stale membership
    (same user *and* device, older `member_id`) is never a recipient; other
    devices of our own user are.

11. **Keys arriving before the membership.** The core buffers them
    unbounded and retries forever. *Decision*: buffer with a TTL (5 min —
    our key is sent *before* our member event by design, so peers' early
    keys are the normal case, not an edge case) and a cap (256 entries,
    oldest evicted), drained on every session change, re-verified against
    the membership when it lands (an impostor's early key dies there).

12. **No timestamp on the wire.** MSC4143 carries none, so "newer" can only
    mean "received later": the `OutdatedKeyFilter` compares receipt times and
    a genuinely stale-but-late key is undetectable; identical redeliveries
    are deduplicated on bytes instead. Documented limitation, inherited.

13. **`KeyMap` must hold more than one key per member.** Frames carry the
    key index and a peer's old key stays needed for in-flight frames after
    they rotate; LiveKit key providers keep a ring per participant. The
    architecture's `HashMap<member_id, MediaKey>` cannot express that.
    *Decision*: `KeyMap = HashMap<String, Vec<MediaKey>>` (one entry per
    index, arrival order) and the callback carries the single changed key
    so hosts call `set_key_for_participant(identity, key, index)` per event.
    Index space is `u8` (wraps 255 → 0, as js-sdk `% 256`); the host's key
    ring must be sized 256 or map indices. Architecture doc to update.

14. **Only served recipients count.** `send_to_device` reports per-recipient
    results; a failed or unanswered recipient is *not* recorded in
    `shared_with`, so the next rollout retries them with the same key and an
    unreachable member never triggers rotations. A whole-batch `Err` records
    nobody.

15. **Lifecycle.** Our `member_id` exists only from the join (fresh per
    join). *Decision*: one `Machine` per participation, constructed with the
    member before the own-membership join event, dropped to leave; no
    `join`/`leave` methods. Evaluated in
    [Lifecycle](#lifecycle-one-machine-per-participation). Inbound keys
    while not managing keys are logged and dropped (PR behaviour).

16. **Concurrency of the state.** The pump owns the `SendMachine`; host
    getters (`key_map()`, statuses) read a `Mutex`-guarded snapshot that the
    pump updates after each step. No lock is held across an `await`; native
    host threads and the runtime thread therefore never deadlock on a
    callback (ubrn's threading note).

17. **Two things noticed in the PR while porting it** (written up as an
    issue against Element Call, `ElementCallIssueEncryption.md`). Both have
    one root cause:
    `createNewOutboundSession()` replaces `outboundSession` *before* the key
    is sent or used, and the local switch only happens on the happy path.
    (a) A scheduled wake-up while alone creates key `K+1`, finds nobody to
    send to and returns: media stays on `K`, the next joiner is handed `K+1`
    and cannot decrypt us until a later rotation. (b) If `sendKey` throws,
    the `catch` only logs: `K+1` is never used, the next membership update
    hands it to everyone as "current" while frames carry `K`, and the leaver
    who triggered the rotation is never locked out. This module switches at
    once when there is nobody to send to (`rotating_alone_switches_at_once`)
    and arms the switch only after at least one recipient has the key,
    resending a key nobody received on the next change
    (`a_rotated_key_nobody_received_is_resent_not_used`).

18. **Origin metadata we refuse to guess about.** A to-device key whose
    origin the host did not report (`EventOrigin::Unknown`) is rejected
    (`UnknownOrigin`), unlike *member events* with unknown origin, whose
    device checks are skipped: the host always knows whether it decrypted a
    to-device message. Likewise `sender_cross_signed: None` counts as not
    cross-signed when MSC4153 checking is on.

19. **Legacy dialect.** Under `StateEvents` compat the key message type,
    content shape, 16-byte keys and `sent_ts` differ, and the sender's
    `member_id` is `{user}:{device}`. Confined to `legacy_element_call.rs`
    with one dispatch arm each in `matrix_encryption_event.rs` build/parse.

## Runtime verification: `tokio::sync::watch` and timers under uniffi/wasm

Question: is `tokio::sync::watch::Receiver` (the session's subscription
type) usable from Rust compiled through uniffi-bindgen-react-native's wasm
target, where there is no tokio runtime?

**Answer: yes, verified with a real build and real tests.** What holds:

- `tokio::sync` is runtime-independent (it only needs `Waker`s) and is in
  tokio's wasm-allowed feature set (`sync, macros, io-util, rt, time`).
- uniffi on wasm polls an exported `async fn` from JS: `rust_future_poll`
  stores a waker whose `wake()` calls a JS continuation, which resolves a
  promise, which re-polls on the next microtask
  (`typescript/src/async-rust-call.ts` in ubrn). So a
  `watch::Sender::send` from a *synchronous* FFI call wakes and completes
  the awaiting future.
- Futures **not** returned to JS (our pumps) have no poller unless spawned
  with `wasm_bindgen_futures::spawn_local`; tokio timers panic there. Hence
  `crate::executor`.

Proof: `src/uniffi_api/runtime_probe.rs` (feature `runtime-probe`, enabled
only by `web-test-app/ubrn.config.yaml`) exports a `RuntimeProbe` object,
and `web-test-app/test/runtimeProbe.test.ts` asserts against the wasm
build:

| probe | verifies | result |
| --- | --- | --- |
| `nextChange()` pending, then sync `set(7)` | `watch::changed()` wakes from a sync send inside an exported future | pass |
| `spawnForwarder(listener)`, then `set(1); set(2); set(3)` | detached task (`spawn_local`) outlives the call, awaits `changed()` repeatedly, calls back into JS | pass |
| `await sleep(60)` | timer inside an exported future | pass |
| `setAfter(50, 42)` + `await nextChange()` | timer inside a detached task wakes a waiting future | pass |
| `nowMs()` ≈ `Date.now()` | clock on wasm (`web_time`) | pass |

```sh
cd web-test-app && npm run ubrn:web && npx vitest run test/runtimeProbe.test.ts   # 5 passed
cargo test --lib executor                                                          # native counterpart
```

Two things the probe *taught* us and that are now fixed: challenge 4
(feature unification) and challenge 5 (`emit` completes before the pump
runs — the tests `await` a `setTimeout(0)` tick).

## Tests

Deterministic Rust unit tests use a fake clock, a pinned jitter and a
recording `ToDeviceSendDriver` mock; the simulation drives many
`SendMachine`s from one fake clock. Web acceptance tests go through the
real bindings. Names are the intended test names.

### `send_machine.rs` — rotation policy (ported from PR #5505)

- `first_key_is_sent_to_everyone_and_used_immediately`
- `first_key_starts_a_jittered_block_and_schedules_nothing`
- `a_leave_in_a_settled_call_rotates_after_grace_times_jitter_to_the_remaining_members_only`
- `nothing_is_sent_while_waiting_for_the_jitter_when_nobody_joined`
- `a_rotated_key_is_used_only_after_use_key_delay`
- `joiners_during_a_block_get_the_current_key_immediately`
- `changes_during_a_block_neither_move_the_deadline_nor_earn_a_second_delay`
- `a_scheduled_wake_rotates_and_starts_an_unjittered_block_without_scheduling`
- `a_change_in_the_fresh_block_after_a_rotation_schedules_exactly_one_wake`
- `several_changes_while_blocked_produce_one_rotation`
- `no_change_and_no_wake_produces_no_action`
- `grace_period_grows_with_participant_count` (table from the PR: 2000/3000/5000 × 50/100/200)
- `grace_period_has_no_floor`
- `a_rotation_owed_while_a_key_switch_is_pending_waits_for_the_switch`
- `a_changed_membership_ts_counts_as_left_and_joined` (rejoin under the same id)
- `a_member_without_a_device_is_skipped_not_broadcast`
- `our_own_stale_membership_is_never_a_recipient`
- `another_device_of_our_own_user_is_a_recipient`
- `only_served_recipients_enter_shared_with`
- `a_failed_recipient_is_retried_with_the_same_key_on_the_next_change`
- `an_unreachable_member_leaving_causes_no_rotation`
- `key_index_wraps_from_255_to_0`
- `left_members_with_keys_lists_leavers_until_the_switch`
- `optional_max_key_lifetime_forces_a_rotation_in_a_quiet_call`

### `rotation_simulation.rs` — the PR's simulated call (test-only module)

- `a_call_stays_inside_the_contingent_and_goes_quiet_after_the_last_change`
  — 10 / 100 / 300 participants, 25 simulated clients, seeded PRNG (seed
  printed), flapping participant; asserts: extrapolated messages/min ≤
  contingent; min rotation gap ≥ grace(N); mean gap ≤ 1.5 × grace(N+1); >50%
  of rotations on distinct instants; no sends after last change + 2 ×
  grace(N+1); interval(100) > 10 × interval(10); interval(300) > 5 ×
  interval(100); rotations(300) > 0.
- `documented_costs` — prints the per-step send counts for the README
  tables (the core's `KEY_ROTATION.md` guard, re-targeted).

### `inbound.rs` — verification and storage

- `cleartext_key_is_rejected`
- `key_for_another_room_is_rejected_before_anything_else`
- `key_from_non_cross_signed_device_is_rejected_when_required`
- `key_from_non_cross_signed_device_is_accepted_when_not_required`
- `key_whose_sender_differs_from_the_member_event_is_rejected`
- `key_from_a_different_device_than_the_member_event_is_rejected`
- `a_claimed_device_narrows_but_never_widens`
- `member_with_unknown_origin_skips_the_device_check`
- `member_without_attributable_device_rejects_the_key`
- `key_arriving_before_the_membership_is_buffered_and_verified_when_it_lands`
- `buffered_impostor_key_dies_when_the_real_membership_lands`
- `early_key_buffer_is_bounded_by_ttl_and_capacity`
- `a_rejected_key_does_not_occupy_the_member_index_slot`
- `a_newer_key_at_the_same_index_replaces_the_old_one`
- `an_identical_redelivery_is_not_re_emitted`
- `outdated_filter_rejects_older_arrivals_and_passes_equal_timestamps`
- `outdated_filter_forgets_entries_after_its_ttl`
- `sixteen_byte_legacy_keys_are_accepted_with_a_log_line`
- `unpadded_and_padded_base64_both_decode`
- `keys_while_not_managing_media_keys_are_dropped`
- `has_received_all_member_keys_tracks_remote_members_with_devices`

### `matrix_encryption_event.rs` / `legacy_element_call.rs`

- `outbound_content_declares_format_0_and_no_version`
- `stable_and_unstable_event_types_parse_inbound`
- `outbound_type_is_the_unstable_spelling`
- `legacy_content_carries_member_device_call_id_session_and_sent_ts`
- `legacy_keys_object_and_array_forms_both_parse_highest_index_wins`
- `legacy_member_id_falls_back_to_user_colon_device`

### `pump.rs` — wiring with real (short) timers

- `session_changes_wakes_and_inbound_keys_flow_through_the_pump` (done: first
  key to the driver, own key in the map, inbound key verified, a leaver ->
  timer-driven rotation, dropping the machine stops the pump and releases the
  driver and the session subscription)
- `a_session_change_during_a_send_is_processed_after_it` (coalescing) — todo

### `web-test-app/test` — acceptance through the wasm bindings

- `runtimeProbe.test.ts` — the five runtime probes (done).
- `encryption.test.ts` (to write):
  - `joining an encrypted call sends our key to every member's device`
  - `a remote key that passes verification appears in the key map`
  - `a cleartext remote key is dropped`
  - `a key for an unknown member is held until its membership arrives`
  - `a leaver stays LeftWithKeys until we switch to the rotated key`
  - `the rotated own key appears in the key map after the use delay`
  - `unencrypted slot: no keys are sent and inbound keys are ignored`

## Decisions taken

Reviewed 2026-09-02; all resolved.

1. **No grace floor; block until the pending key is in use** (challenge 7).
   The contingent formula stays exactly the PR's. An owed rotation whose
   deadline falls inside the previous key's `use_key_delay` waits for that
   switch and runs right after it.
2. **Optional lifetime cap** (`max_key_lifetime_ms`), **off by default**.
3. **`KeyMap` = `member_id → Vec<MediaKey>`** with a per-key change callback.
4. **Early-key buffer**: TTL **5 min**, 256 entries.
5. **`Member` gains `membership_ts` and `device_attribution`**; `FfiMember`
   mirrors them; `session::convert` fills them in.
6. **One `Machine` per participation, no `join`/`leave`**: constructed before
   the own-membership join event, dropped to leave (evaluation in
   [Lifecycle](#lifecycle-one-machine-per-participation)). The ordering is
   documented on `ParticipationManager.encryption`.
7. **No hard participant limit.** The PR's `keyRotationParticipantLimit` is
   not ported; the contingent-derived grace period is the only brake on
   rotation traffic. A 300-participant call rotates every ~30 min instead of
   never.

## Implementation status

| piece | status |
| --- | --- |
| `crate::executor` (spawn / sleep / clock, native + wasm) | done, tested on both |
| runtime probe + wasm test suite | done, 5/5 passing |
| tokio feature set safe for the ubrn shim | done |
| `matrix_encryption_event.rs` (build/parse, base64, type dispatch) | done, 5 tests |
| `legacy_element_call.rs` | done, 3 tests |
| `inbound.rs` (verification, early-key buffer, filter, store) | done, 15 tests |
| `send_machine.rs` (PR #5505 policy + wait-for-switch rule, pure) | done, 17 tests |
| `pump.rs` + `Machine` (one per participation; session watch, timers, driver sends, callbacks, drop = leave) | done, 1 end-to-end test with a mock driver and real timers |
| `rotation_simulation.rs` (the PR's simulated call, `#[cfg(test)]`) | done, passes for 10/100/300 participants |
| `Member.membership_ts` / `device_attribution`, `FfiMember` mirror | done (type change; `session::convert` must fill them in) |
| `KeyMap` shape change | done in code (`member_id -> Vec<MediaKey>`); architecture doc updated |
| `participation` wiring: construct `Machine` in `join()` before the own-membership join, drop in `leave()` | todo (facade is still `todo!()`; ordering documented on the field) |
| FFI: `FfiMediaKey` list is already per-key; per-key change listener | todo |
| web `encryption.test.ts` | todo — needs the facade |

Run everything:

```sh
cargo test                                   # unit + pump + simulation
cargo test --lib rotation_simulation -- --nocapture    # prints the per-size traffic
cargo check --features runtime-probe --target wasm32-unknown-unknown
cd web-test-app && npm run ubrn:web && npx vitest run test/runtimeProbe.test.ts
```

Simulation output with the default contingent (one seed; every seed so far
lands in the same band, the PR reports the same order of magnitude):

| participants | grace | rotations / client | to-device msgs/min (of 3000) | distinct rotation instants |
| --- | --- | --- | --- | --- |
| 10 | 1.8 s | 5.7 | 2318 | 57 of 57 |
| 100 | 3.3 min | 6.0 | 2318 | 149 of 149 |
| 300 | 29.9 min | 6.1 | 2373 | 153 of 153 |
