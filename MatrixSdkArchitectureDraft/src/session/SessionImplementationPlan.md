# `session` module — implementation plan

Scope: turn `src/session/**` from `todo!()` into the working "single converter"
described in `MatrixSdkArchitecture.md`. Everything else (`own_membership`,
`connections`, `encryption`, `participation`) only consumes
`watch::Receiver<SessionSnapshot>`, so this module can be finished and tested
on its own first.

## Implementation status (2026-09-02)

Phases 1–4 and 6 are implemented and tested natively (`cargo test`: 147
tests in `session`/`types`, plus 4 FFI tests with `--features uniffi`); the
crate also type-checks for `wasm32-unknown-unknown --features uniffi`. Of
Phase 5, the FFI glue is done (`RoomEventSink`/`StateUpdateSink`/
`ToDeviceSink::emit`, `FfiMatrixDriver::new` with Rust-side fan-out,
`read_events`/`read_state`, `compute_sessions_from_events`). **Not done:**
5.3 and §4.9 — the wasm acceptance tests go through `FfiParticipationManager`,
whose `participation::Manager` is still `todo!()`; that is outside this plan.

Where the code deviates from the text below, the code is right and the reason
is one of these:

- `MemberCandidate` carries `slot_id`, `origin_server_ts`, `leave_reason` and
  an `Option<LegacyDetails>` (call id, state key, `joined_at`, focus data);
  transports live only in `member.transports` (no duplicate field). MSC3401
  transports are assigned at projection time (`msc3401::assign_transports`).
- `dispatch::classify(event, config, now)` computes the MSC4354 `end_time`
  (`sticky::end_time`) and puts it in the candidate; converters take no clock.
  "Already expired on arrival" is judged in `RoomState::ingest` for both
  generations (`expires_at <= now`, deadline inclusive).
- Removals are kept as tombstones until their `end_time` so a stale join
  arriving after a removal cannot resurrect the entry — required for the
  order-independence test in §4.4. Both member type spellings key the same
  map entry.
- `Member.member_id` is `member.id` (falling back to the sticky key when a
  leave omits it); the sticky key is the *map* identity, and a mismatch is
  logged. `RtcTransport.properties` is the transport object minus `type`.
- Room-member condition: the live path enforces it only after the seed
  supplied the set (a single live `m.room.member` is not a roster), and an
  **empty** `read_state(m.room.member)` counts as unsupplied — a readable room
  always contains the reader, so an empty answer is a host with nothing to
  offer. The static path infers the set from the slice.
- `start_ts` uses the MSC3401 `joined_at` for legacy members (a re-sent state
  event moves `origin_server_ts`, not the join). `members` and
  `excluded_candidates` are sorted by `(user_id, member_id)`.
- The transition after an expiry publishes once more to clear the one-shot
  `Expired` entry (1.6): that is a real change of `excluded_candidates`.
- Time is injected through a `Clock` trait (`Session::with_clock`); the
  system clock is `executor`.
- Fixtures: `mockDriver.ts` now puts `msc4354_sticky_key` into member content
  and uses `SLOT_ID = "m.call#ROOM"`; both were required by MSC4143/MSC4354
  and the previous values made every member event a non-membership and every
  slot closed.

What we can port from `../crates` (the current implementation):

| current code | what to take | where it lands |
|---|---|---|
| `matrix-rtc-core/src/slot.rs` | `RawSlotEvent::resolve` + its 16 tests, almost verbatim | `session/slot.rs` |
| `matrix-rtc-core/src/event.rs` | MSC4143 content rules (`join` needs `member.id` + `application.type`; unknown membership = left; sticky_key both spellings) + tests | `session/convert/msc4143.rs` |
| `matrix-rtc-core/src/session.rs` | `join_condition`, `refresh` (publish only on change), `negotiated_encryption`, `debug_snapshot`, and the *batch does not publish per event* rule | `session/state.rs` |
| `matrix-rtc-core/src/manager.rs` | room-state bookkeeping: "unsupplied vs. supplied-but-absent" slot knowledge, per-room scoping, seeding a session created after state was known | `session/state.rs` |
| `matrix-rtc-core/src/wire.rs` | only the `wire_event_type` tests (the table is already implemented in `types.rs`) | `types.rs` |
| `matrix-rtc-bridge/src/compat/element_call.rs` | the 2025-dialect normaliser (`normalize_member_content`, `claimed_device_id`) + tests | `session/convert/msc4143.rs` (`fill_from_2025_dialect`) |
| `matrix-rtc-bridge/src/compat/element_call_state.rs` | MSC3401 state-event → memberships + tests | `session/convert/msc3401.rs` |
| matrix-rust-sdk `matrix-sdk-base/src/sticky/{map,extract}.rs` | the MSC4354 map rules (key, end-time, tie-break, removal) — *not* the code, it is ruma-typed | `session/sticky.rs` |

What is genuinely new here (no equivalent today):

- the session consumes **raw events** and therefore has to run its own MSC4354
  sticky map (expiry, conflict resolution, removals). Today matrix-rust-sdk's
  `live_sticky_events()` did that and the core only saw a resolved map.
- one dispatch funnel for *every* event type (member sticky, MSC3401 state,
  slot, `m.room.member`, `m.room.encryption`) instead of three host entry
  points (`apply_room_sticky_state`, `on_room_slots_received`,
  `set_room_members` …).
- the static `compute_sessions_from_events` path (many rooms, many slots, no
  driver, no timers).
- self-seeding via `read_state` / `read_events` and self-feeding from the
  driver streams.

---

## 1. Decisions to settle before writing code

Each has a recommendation; they are listed because they change signatures
other modules already compile against.

### 1.1 The session owns the sticky map
`RoomEventsDriver` hands over raw `m.rtc.member` events, not a resolved map.
So the session must implement MSC4354 itself, following the rules
matrix-rust-sdk uses (they are convergent, which is what makes the static
path order-independent):

- key = `(sender, event_type, sticky_key)`; `sticky_key` read from
  `content.msc4354_sticky_key` **or** `content.sticky_key`.
- `end_time = min(origin_server_ts, received_ts) + min(duration_ms, 3_600_000)`
  with `duration_ms` from top-level `msc4354_sticky.duration_ms` (accept
  `sticky` too); if `unsigned.msc4354_sticky_duration_ttl_ms` is present,
  prefer `received_ts + ttl`.
- an incoming event replaces the current entry only if
  `(end_time, event_id)` is strictly greater; otherwise it is ignored.
- an event whose content carries nothing but the sticky key is a **removal**,
  subject to the same tie-break (a stale removal must not wipe a fresh join).
- already-expired on arrival → ignored.
- `received_ts` = `executor::now_ms()` at ingest; the static path uses the
  call time.
- a member event with **no** sticky metadata anywhere (no duration, no ttl)
  is not a sticky event: the converter still yields a candidate
  (`expires_at = None`) but the map refuses it (`Unchanged`, one debug line).
  Admitting it would create an entry that never expires and cannot be
  ordered against real ones.

Consequence: `MemberCandidate.expires_at` is the map's `end_time`, and the
live session needs one timer for the earliest expiry (see 1.3).

### 1.2 No own-participation rule in the session
Today's core excludes `SupersededOwnParticipation` (a candidate from our own
device under a different `member_id`). **This is not ported.** MSC4143 allows
one device to hold several members of one session on purpose (e.g. a
multiplayer game where the same client is a player and, as a second member,
the moderator). A device-based dedup rule in the projection would make that
impossible, so the session treats every candidate from our own device as an
ordinary member and knows nothing about who "we" are.

A stale membership after a crash or a quick rejoin is therefore visible in
the roster until it expires or its leave lands. Cleaning it up is an
application decision and lives above the session: `participation::Manager`
can expose an opt-in method (e.g. "leave all non-active own memberships of
this session") that sends leave events for our device's other member ids.
That is `own_membership`/`participation` scope and is out of this plan.

### 1.3 Wasm: synchronous reads after `emit`
`web-test-app/test/participation.test.ts` emits an event and reads
`manager.memberships()` synchronously. On wasm a pump task only runs after the
current JS task yields (`runtimeProbe.test.ts` shows this: `await tick()`).
Two ways to make the tests honest:

- **Recommended: drain-on-read + pump.** The session's core is a synchronous
  `ingest(&mut RoomState, event)`. The pump task (native and wasm) drains the
  channels and publishes. Additionally `Session::snapshot()` (and thus every
  FFI getter) first drains whatever is pending in the receivers, under the
  same mutex, so a read right after an `emit` is fresh. Listener callbacks
  still arrive one microtask later; tests that assert on listeners
  `await tick()`. Because receiver and state share one lock, an event is
  processed exactly once regardless of who drains it.
- Alternative: change the acceptance tests to `await tick()` everywhere and
  keep a pure pump. Simpler code, weaker guarantee for hosts.

Whichever is chosen, three places currently describe three different
behaviours and must end up saying the same thing: the mod-level doc on
`Session::new` ("an `emit` is fully processed before it returns"),
`MatrixSdkArchitecture.md`'s FFI section ("`emit` only *wakes* the pump …
web tests `await` a tick") and this plan.

Two consequences of the recommended option that the skeleton does not show:

- **Lock model.** `UnboundedReceiver::recv()` borrows the receiver across an
  await, so the pump cannot hold the state mutex while waiting and the
  receivers cannot be shared with `snapshot()` through `tokio::select!`.
  Instead the pump is one `poll_fn`: lock, `poll_recv` both receivers and
  poll the expiry sleep, ingest everything that is ready, publish, unlock,
  return `Pending` when nothing was ready (the wakers are registered under
  the lock). `snapshot()` locks and loops `try_recv`. Both paths go through
  the same lock and the same `ingest`, so an event is processed exactly once.
  This settles Phase 3.3: no `tokio::select!`, no `macros` feature.
- **Getters above the session.** The web test reads
  `manager.memberships()`, not `Session::snapshot()`. Drain-on-read only
  helps if `participation::Manager` (and every FFI getter that reflects
  session state) builds its answer from `session.snapshot()` rather than
  from the last value it saw on its `watch` receiver. That is a
  `participation` obligation and is noted in §5.

### 1.4 The `RawMatrixEvent.event` contract
Fix it in one place (`types.rs` doc + `mockDriver.ts`): the *full* event
object as the host SDK holds it — `type`, `sender`, `event_id`, `room_id`,
`origin_server_ts`, `content`, optional `state_key`, optional top-level
`msc4354_sticky`, optional `unsigned`. For encrypted events this is the
*decrypted* event (the host resolves `m.room.encrypted` before handing it
over); `origin` carries the decryption metadata. `read_state` returns the same
shape. Nothing else in the crate parses *room event* JSON (`encryption` parses
to-device content, `uniffi_api` only deserialises strings into `Value`), so
these accessors live in `session/dispatch.rs` (`event_type()`, `sender()`, `state_key()`,
`origin_server_ts()`, `content()`, `sticky_duration_ms()`, `event_id()`).

### 1.5 `types::Member` — what exists, what is missing
Fields `Member` already has and that this module must fill correctly:

- `device_attribution: DeviceAttribution` (`Verified | Claimed | Unknown`).
  This is how "claimed, never verified" is expressed; `encryption::inbound`
  matches on it. `EventOrigin` stays as it is — three variants, also used for
  to-device key origin — so **no `EventOrigin::Claimed`**. The converters set
  `device_attribution`: `Verified` when the origin is
  `Encrypted { sender_device_id: Some }`, `Claimed` when the device comes
  from MSC3401 content or the 2025 dialect's `member.device_id`, `Unknown`
  otherwise (precedence in 6.1). `MemberCandidate.origin` keeps the raw
  `EventOrigin` for the encryption condition.
- `membership_ts: Option<u64>`. `encryption::send_machine` uses a change in
  it to tell a leave-and-rejoin from an unchanged membership. The MSC3401
  converter sets it to `joined_at` (6.2); MSC4143 leaves it `None` (fresh
  `member_id` per join). `start_ts` is computed from the candidate's
  `origin_server_ts`, which stays on `MemberCandidate` (internal).

Fields the downstream modules need and `Member` cannot carry yet:

- `connections::members_for_connection_data` groups members by their
  published transports' `livekit_service_url` → `Member` needs
  `transports: MemberTransports` (the snapshot's `transports` union alone
  does not say who publishes where).
- `application_type` on `Member` (MSC4143 requires it on a join) — add it, so
  a slot-less MSC3401 session can still report an application type.

### 1.6 Where `Expired` shows up
Expiry is a map concern, not a projection condition. An entry that is already
expired on arrival is ignored (1.1; the MSC3401 map applies the same rule),
and a live entry that reaches its `expires_at` is removed by the expiry
timer. To keep the diagnostics promised by `excluded_candidates`, that
removal reports the dropped candidates and the snapshot published by the very
transition that drops them lists them as `(member, Expired)`. Nothing is
retained beyond that, and the static path — one `now`, nothing arrives later
— never reports `Expired`.

### 1.7 `compute_sessions_from_events` result set
Return one snapshot per `(room_id, slot_id)` that has **either** a slot state
event **or** at least one member candidate. Rooms with an `m.rtc.slot` that is
closed and empty are still returned (`slot_state = Some(Closed)`, no members)
so room_info can distinguish "closed" from "never had a call".

---

## 2. Target file layout

```
src/session/
  mod.rs          public API only: SessionConfig, SessionSnapshot, Session,
                  compute_sessions_from_events, re-exports
  dispatch.rs     RawMatrixEvent JSON accessors + `classify(event, config) -> Ingest`
                  (the ONLY place that knows stable+unstable event-type strings)
  sticky.rs       MSC4354 map: StickyKey, StickyEntry, upsert/remove/expire,
                  next_expiry()
  slot.rs         RawSlot parse + resolve(room_encryption) -> SlotState
                  (port of core slot.rs)
  state.rs        RoomState: ingest(Ingest) -> Changed, project(slot_id) ->
                  SessionSnapshot, join_condition(), negotiated_encryption(),
                  debug_json()   ← pure, no I/O, no clock except a `now` arg
  live.rs         Session internals: Arc<Mutex<Live>>, seeding, pump task,
                  expiry timer, drain-on-read, drop guard
  convert/
    mod.rs        MemberCandidate, CandidateSource, CandidateMembership (as is)
    msc4143.rs    spec-shaped + 2025-dialect fill (delete-by-date block)
    msc3401.rs    state-event generation (delete-by-date file)
  test_support.rs (cfg(test)) JSON builders mirroring mockDriver.ts, a
                  FakeRoomEventsDriver, a controllable clock
```

`Ingest` (internal enum produced by `dispatch::classify`):

```
Member(MemberCandidate)              // m.rtc.member / org.matrix.msc4143.rtc.member
MemberRemoval(StickyKey)             // MSC4354 removal: content = sticky key only (1.1)
LegacyMember(MemberCandidate)        // org.matrix.msc3401.call.member (StateEvents only), one per event
LegacyMemberRemoval { state_key }    // same type, empty content
Slot { slot_id, RawSlot }            // m.rtc.slot / org.matrix.msc4143.rtc.slot
RoomMember { user_id, joined: bool } // m.room.member
RoomEncryption                       // m.room.encryption with an algorithm; empty content is Ignored
Ignored(&'static str)                // logged at trace
```

`RoomState` (one per room; the live `Session` holds one and projects a single
slot, the static path holds a `HashMap<room_id, RoomState>`):

```
sticky: sticky::Map<MemberCandidate>          // MSC4143 candidates, keyed per 1.1
legacy: HashMap<state_key, MemberCandidate>     // MSC3401 candidates (StateEvents); state key = per user+device+call
slots: HashMap<slot_id, RawSlot>, slot_state_supplied: bool
room_encryption: Option<bool>
room_members: Option<HashSet<user_id>>       // None = unenforced
```

Projection (`project(&self, slot_id, now) -> SessionSnapshot`) is the port of
`join_condition` + `refresh`, plus generation scoping:

| condition | MSC4143 candidate | MSC3401 candidate |
|---|---|---|
| slot closed (only when slot state supplied) | `SlotClosed` | not applied |
| cleartext in encrypted room (`origin.was_encrypted()==Some(false)`) | `UnencryptedInEncryptedRoom` | not applied |
| sender not in `room_members` (only when supplied) | `SenderNotInRoom` | `SenderNotInRoom` |
| dropped by the expiry timer in this transition (1.6) | `Expired` | `Expired` |

(No own-device rule — see 1.2.)

`slot_state`: `None` until slot state was supplied (seed or first slot
event/state batch), then `Some(resolve(slot event, room_encryption))` or
`Some(Closed)` when supplied-but-absent. `negotiated_encryption`: `None`
while `slot_state` is `None`, otherwise `Some(open && mechanism supported)`.
`start_ts`: min `origin_server_ts` over joined members. `application_type`:
open slot's type, else the first joined member's.

---

## 3. Phases

Each phase ends with `cargo test` green natively; phase 5 adds the wasm run.
Order is chosen so every phase has a test harness without the later ones.

### Phase 1 — pure parsing (no state)
1. `dispatch.rs` accessors + `classify` for the spec shapes only (member,
   slot, room member, room encryption; MSC3401 arm returns `Ignored` for now).
2. `convert/msc4143.rs`: spec-shaped conversion (port `event.rs` rules).
   Produces `MemberCandidate { member, source: Msc4143, membership,
   origin, expires_at: None (filled by sticky layer), transports,
   origin_server_ts }`.
3. `slot.rs`: port `resolve`.
4. `types.rs`: `generate_member_id` (needs `rand`/`getrandom` with the `js`
   feature for wasm — add deps), the `wire_event_type` tests, the `Member`
   additions from 1.5 (`transports`, `application_type`).

### Phase 2 — pure state + static path
1. `sticky.rs` with the 1.1 rules and `next_expiry()`.
2. `state.rs`: `ingest` for every `Ingest` variant, `project`,
   `debug_json`.
3. `compute_sessions_from_events`: classify all → group by room → ingest →
   project every `(room, slot)` per 1.7. This is the room_info entry point and
   is fully testable now.

### Phase 3 — live `Session`
1. `live.rs`: `Session::new` builds `Arc<Mutex<Live { state, room_rx,
   state_rx, snapshot_tx }>>`, subscribes to both streams **before** seeding
   (so nothing is missed between read and subscribe), then `executor::spawn`s
   the pump.
2. Seeding order inside the pump: `read_state(m.room.encryption, Key(""))`,
   `read_state(m.rtc.slot, Any)` (+ unstable type), `read_state(m.room.member,
   Any)`, `read_events(m.rtc.member, None, limit)` (+ unstable type), and with
   `StateEvents`: `read_state(org.matrix.msc3401.call.member, Any)`. Each read
   failure is logged and leaves that condition **unenforced** (matches
   today's opt-in semantics). One publish at the end of seeding, not per read.
3. Pump loop: the `poll_fn` of 1.3 — lock, `poll_recv` both receivers and
   poll the pending `sleep_ms` for the next expiry, ingest whatever is
   ready, publish once, return `Pending`. A state batch is ingested whole
   and published once. No `tokio::select!`; note the pattern in
   `executor.rs`.
4. Drain-on-read (1.3) in `snapshot()`; `subscribe()` returns
   `snapshot_tx.subscribe()`; `Drop` closes this session's receivers and
   ends the pump. Whether the host-facing sink `emit` then returns `false`
   depends on the driver's fan-out: it does once the *last* subscriber is
   gone (one manager per driver in the acceptance tests, so 4.9 can assert
   it directly).
5. Publish only when the projected snapshot differs (`PartialEq` on
   `SessionSnapshot`; derive it).

### Phase 4 — compat read side
1. `msc4143.rs::fill_from_2025_dialect` (always on, only fills absent modern
   fields); bare sticky-key leave is the MSC4354 removal of 1.1 and needs no
   dialect code; claimed device → `Member.device_id` +
   `DeviceAttribution::Claimed` when the event's origin names none.
2. `dispatch` MSC3401 arm gated on `config.compat == StateEvents`:
   `msc3401.rs::member_candidate(event, now)` → `Ingest::LegacyMember`
   (one per-device state event is at most one membership; the
   `memberships[]` array generation is out of scope, see the callout in
   §6.2), empty content → `Ingest::LegacyMemberRemoval { state_key }`.
   `RoomState.legacy` is keyed by `state_key`. Field mappings: §6.2.
3. Legacy focus resolution is cross-member, so it runs in `project()` over
   the slot's surviving legacy candidates (rules in §6.2). Keep it in
   `msc3401.rs` as a pure `resolve_focus(candidates) -> Vec<..>` so the file
   still deletes cleanly.
4. Legacy expiry uses the same timer as sticky expiry (`next_expiry` over
   both maps). No 30 s poll is needed any more: the session knows every
   `expires_at`.
5. The legacy slot (`LEGACY_SLOT_ID`) never gets a `slot_state`: it stays
   `None` even when the room has `m.rtc.slot` state (today's bridge passes no
   slot snapshot in `StateEvents` mode for the same reason).

(Exact field mappings: see §6, filled from the current bridge code.)

### Phase 5 — FFI glue + acceptance
1. `uniffi_api`: `RoomEventSink::emit` / `StateUpdateSink::emit` parse JSON →
   `RawMatrixEvent` and `try_send`, returning `false` on a closed channel;
   `compute_sessions_from_events` FFI maps to `FfiSessionSnapshot`
   (`slot_open`, `encrypted`, `member_count`, `is_active`).
2. `FfiMatrixDriver::read_events/read_state` parse the JSON string vectors.
3. Make the existing two session-relevant acceptance tests pass and add the
   ones in §4.9.

### Phase 6 — hardening
- Malformed input never panics (`serde_json::Value` probing, no `unwrap`).
- Bound the log volume: per-candidate verdicts at `debug`, changes at `info`,
  unchanged at `trace` (same tiers as the current core).
- Cost of the static path: one pass, no clones per event; benchmark 500 rooms
  × 5 events in a `#[test]` with a generous bound to catch quadratic slips.

---

## 4. Test plan

Conventions: unit tests inline per file; JSON fixtures come from
`test_support.rs` builders that mirror `web-test-app/src/mockDriver.ts`
field-for-field (one source of truth per language, kept in sync by a test
that parses a mockDriver-shaped event). Time is injected (`now` argument on
pure functions; a `Clock` trait or `now_fn` on `Live` for the pump) so no
test sleeps.

### 4.1 `dispatch.rs`
- classifies `m.rtc.member` **and** `org.matrix.msc4143.rtc.member` as Member;
  same for the two slot types; `m.room.member`, `m.room.encryption`.
- `org.matrix.msc3401.call.member` → `Ignored` with `Off`/`StickyEvents`,
  `LegacyMember` / `LegacyMemberRemoval` (empty content) with `StateEvents`.
- unknown type → `Ignored`; missing `type`/`sender`/`content` → `Ignored`
  (never an error that aborts a batch).
- `m.room.member` with `membership: join` → joined=true; `leave`/`ban`/
  `invite`/missing → false.
- `m.room.encryption` with any algorithm → encrypted; empty content →
  encrypted=false is **not** inferred (empty content is ignored; a room cannot
  be un-encrypted).
- sticky metadata: `msc4354_sticky.duration_ms`, `sticky.duration_ms`,
  `unsigned.msc4354_sticky_duration_ttl_ms`; a member event without any
  sticky metadata still converts (candidate with `expires_at = None`) — the
  map refuses it (1.1, tested in 4.4).

### 4.2 `convert/msc4143.rs` (port of core `event.rs` tests + new)
- spec-shaped join parses (member id, application, transports, can_subscribe,
  origin device).
- `membership: leave` wins over join-shaped content.
- unknown membership value → treated as leave, does not fail parsing.
- join without `application.type` → not a join.
- `sticky_key` accepted under `msc4354_sticky_key` and `sticky_key`;
  `sticky_key != member.id` → sticky key wins for identity, warning logged.
- livekit transport parsed; unknown transport preserved as-is
  (`transport_type` + properties).
- receive-only member: empty `published`, `can_subscribe` kept.
- content not an object / `member` not an object → `None`.
- leave carries `leave_reason {code, reason}`.
- `origin` passes through untouched (Encrypted/Cleartext/Unknown).

### 4.3 `slot.rs` (port all 16 core tests)
open with application+encryption; absent encryption; closed status; open
without application → closed; unknown status → closed; empty content →
closed; application type must align with state key; prefix must end at `#`;
no encryption in encrypted room → closed; per_member in encrypted room →
open; unsupported mechanism in encrypted room → closed; declared encryption
dropped in unencrypted room; plain slot in unencrypted room; unknown room
encryption → face value; unstable `org.matrix.msc4143.per_member`
recognised; (skip the two `for_open/for_close` builder tests — they belong to
`own_membership`).

### 4.4 `sticky.rs`
- key identity: same sender+type+key replaces; different sender same key is a
  different entry.
- tie-break: later `end_time` wins; equal `end_time` → higher `event_id` wins;
  lower/equal → ignored (returns `Unchanged`).
- removal: supersedes only when it wins the tie-break; stale removal keeps the
  live join.
- expired on arrival → ignored; `expires_at = None` (no sticky metadata) →
  ignored.
- `end_time` math: `min(origin_server_ts, received_ts)`; duration clamped to
  1h; `unsigned` ttl preferred when present.
- `expire(now)` removes exactly the due entries and reports them;
  `next_expiry()` is the min.
- order independence: for a fixed set of events, every permutation yields
  the same map (seeded shuffles, ≥20 permutations).

### 4.5 `state.rs` — projection (ports of core `session.rs`/`manager.rs` tests)
Slot condition
- members joined while slot state unsupplied.
- members joined against an open slot; left when the slot is closed;
  closing and reopening re-evaluates (candidates survive).
- slot state supplied for the room but absent for this slot → `Closed`.
- slot condition never applies to MSC3401 candidates.
- slot state is scoped to its room (static path: two rooms, one closed).

Encryption condition
- cleartext member events are fine in an unencrypted room; excluded in an
  encrypted one; `Unknown` origin is never judged.
- unencrypted slot closes in an encrypted room; resolution reacts to room
  encryption arriving **before or after** the slot event.
- `negotiated_encryption`: `None` unsupplied; `Some(true)` per_member+open;
  `Some(false)` no encryption object; `Some(false)` when closed.
- `UnencryptedInEncryptedRoom` never applies to MSC3401 candidates.

Room membership condition
- unenforced until supplied; a sender who left the room is excluded; rejoin
  restores; `m.room.member` for an unrelated user changes nothing.

Multiple members per device (1.2)
- two candidates from the same sender and device with different member ids
  are **both** joined; a leave for one leaves the other untouched.

Change detection
- re-applying identical events publishes nothing (`Changed::No`); a batch
  publishes once (six joins → one snapshot, not six intermediate ones).
- a sticky removal removes the candidate; a leave-shaped replacement removes
  it; an expired entry surfaces once as `Expired` (1.6).

Snapshot metadata
- `member_count`/`is_active`; `start_ts` = earliest joined
  `origin_server_ts`, `None` when empty; `application_type` from open slot,
  else from members, else `None`; `transports` = union without duplicates;
  `excluded_candidates` carries every excluded candidate with its reason.
- `debug_json` contains slot knowledge, room encryption, room-members count,
  negotiated encryption, joined keys, and per-candidate `condition`.

### 4.6 `compute_sessions_from_events`
- groups by room and slot: two rooms × two slots → four snapshots; MSC3401
  members land in `LEGACY_SLOT_ID` only with `StateEvents`.
- a room with a closed slot and no members is still returned (1.7); a room
  with neither → nothing.
- unknown / malformed events never poison the rest of the batch.
- order independence (shuffle test) and idempotence (calling twice on the
  same slice gives equal output).
- with all origins `Unknown`, the encrypted-room rule is unenforced (member
  count may overshoot — the documented trade).
- a mockDriver-shaped join event (copy of the TS builder output as a fixture
  string) yields one joined member with the LK transport.

### 4.7 live `Session` (native, `FakeRoomEventsDriver` with scripted
`read_*` results, `UnboundedSender` handles for both streams, injected clock)
- subscribes to both streams **before** the first read and the initial
  snapshot reflects everything the reads returned (one publish).
- a read that errors leaves that condition unenforced and does not fail
  construction.
- a live member event updates `snapshot()` and wakes `subscribe()`.
- a state batch (slot closed + encryption) is applied atomically: exactly one
  `changed()` wake-up.
- events for another slot in the same room do not change this session's
  snapshot but their room conditions do apply.
- expiry: with a 1s duration the candidate disappears when the clock passes
  `end_time`; an earlier-expiring event re-arms the timer; a refresh (same
  key, later `end_time`) extends it.
- drain-on-read: send on the room stream, call `snapshot()` **without**
  yielding, the member is there; the pump later finds nothing to do and
  publishes nothing extra.
- dropping the `Session` closes the stream receivers (a subsequent
  `tx.send` errors) and ends the pump.
- unchanged inputs never publish (count `changed()` wake-ups).

### 4.8 compat converters (ported from the bridge tests)
2025 dialect (`msc4143.rs`, port from `element_call.rs` + `ingest.rs`):
- `legacy_join_gains_a_membership_and_typed_transports` — `membership: join`
  inferred, `rtc_transports` lifted into `transports.published` with
  `can_subscribe` deduped from the types.
- `spec_shaped_join_is_untouched` / `spec_leave_is_untouched` — byte-identical
  pass-through; an explicit leave is never promoted.
- `bare_sticky_key_leave_is_reported_for_dropping` — under both key
  spellings → `Ingest::MemberRemoval`.
- `content_missing_everything_is_left_to_fail_normally` — no slot id, no
  sticky key → ordinary "not a membership" (`None`).
- `membership_is_not_inferred_without_an_application`.
- `claimed_device_is_read_only_when_stated`; plus the attribution ranking
  from `ingest.rs`: `prefers_the_decrypted_device_over_the_claimed_one`
  (`Verified`), `falls_back_to_the_claimed_device_when_nothing_decrypted`,
  cleartext + claimed → `Claimed`, unknown + claimed → `Claimed`, nothing
  anywhere → `device_id: None`, `Unknown`.
- `rtc_transports: []` inserts no `transports`; elements without a string
  `type` are skipped from `can_subscribe`.

MSC3401 (`msc3401.rs`, port from `element_call_state.rs` + `ingest.rs`):
- `a_join_becomes_msc4143_member_content` — exact `Member` + candidate fields
  (slot id `LEGACY_SLOT_ID`, `legacy_call_id` `m.call#ROOM`, member id,
  application type `m.call`, intent, `device_id` with
  `DeviceAttribution::Claimed`, `membership_ts == joined_at`, `expires_at`).
- `the_empty_call_id_becomes_the_room_sentinel` — `""` → `#ROOM`,
  `standup` → `#standup`.
- `an_empty_content_is_dropped_as_a_leave` — removes that state key's
  candidate.
- `a_memberships_array_yields_no_candidate` — guards the out-of-scope
  decision: content with a `memberships[]` array produces nothing, even when
  the array entries are well-formed.
- `a_membership_with_no_device_id_is_dropped` (absent and `""`).
- `a_membership_with_no_application_is_dropped` (absent, and object-shaped).
- `the_expiry_boundary_is_exclusive_of_the_deadline`;
  `a_first_join_expires_from_origin_server_ts`;
  `a_membership_with_no_expires_gets_four_hours`;
  `a_created_ts_after_the_event_is_clamped_to_the_event`.
- `the_membership_id_falls_back_to_user_and_device` — `{user}:{device}`
  when `membershipID` is absent.
- focus resolution over survivors:
  `oldest_membership_uses_the_oldest_members_focus`,
  `oldest_membership_is_resolved_per_slot`,
  `multi_sfu_keeps_each_members_own_focus`,
  `a_member_with_no_focus_of_its_own_borrows_one`,
  `a_membership_with_no_focus_anywhere_still_appears` (empty transports),
  `an_expired_member_does_not_get_to_be_the_oldest`,
  `the_translation_is_deterministic_under_reordering` (state-key tie-break),
  `the_focus_object_is_passed_through_verbatim` (`livekit_alias` kept).
- generation scoping (new): a legacy candidate in an encrypted room
  (`origin: Unknown`, attribution `Claimed`) is **not** excluded; a closed `m.rtc.slot` in the room
  does **not** close the legacy slot.
- `dispatch`: the MSC3401 type is `Ignored` unless `StateEvents`.
- mixed rooms: with `StateEvents`, a sticky MSC4143 member and a legacy
  state member coexist in the static output under different slot ids (the
  sticky event's `slot_id` vs `LEGACY_SLOT_ID`) — replaces today's "sticky
  wins on a shared key" merge, which is no longer needed because generations
  no longer share a map.

### 4.9 wasm acceptance (`web-test-app/test/participation.test.ts`, run through the real bindings)
Keep the existing two ("remote member join shows up", "starts disconnected")
and add session-focused ones:
- a slot `closed` state update empties the memberships; reopening restores.
- an `m.rtc.member` with a 200 ms `duration_ms` disappears after ~300 ms
  (real `setTimeout`; proves `executor::sleep_ms` drives expiry on wasm).
- unstable type `org.matrix.msc4143.rtc.member` is accepted.
- `computeSessionsFromEvents([...])` on the same fixtures returns one
  snapshot with `memberCount === 1`, `slotOpen === true`.
- a listener fires after `await tick()` with a list equal to the getter.
- the emit after `manager` is dropped returns `false`.

---

## 5. Contract notes for the consuming modules

Things the other modules will rely on and that this plan fixes:

- `SessionSnapshot` derives `PartialEq` and is published only on change —
  `own_membership`/`encryption` may treat every `changed()` as a real
  roster/slot change (the current core's encryption manager depends on
  exactly this to avoid useless rotations).
- `Member.transports` (1.5) is what `connections` groups on.
- `Member.device_attribution == Claimed` (1.5) is what `encryption` treats
  as "addressable but not authenticated"; `EventOrigin` gains no variant.
- `Member.membership_ts` is what `encryption` uses to tell a rejoin under
  the same member id from an unchanged membership; only the MSC3401
  converter sets it.
- `participation::Manager::memberships()` and the FFI getters must read
  through `Session::snapshot()` (drain-on-read, 1.3) — a cached `watch`
  value is stale right after an `emit`.
- `negotiated_encryption` is read by `participation` at join time to override
  `EncryptionConfig.manage_media_keys`; it is not renegotiated mid-call
  (same as today).
- `LEGACY_SLOT_ID == ""` is the slot id a `StateEvents` call is created with.

## 6. Compat field mappings (read side)

Extracted from `crates/matrix-rtc-bridge/src/compat/{element_call,
element_call_state, ingest}.rs`. Only the read side is listed; the write side
moves to `own_membership`.

### 6.1 2025 Element Call sticky dialect (`StickyEvents`, read is always on)

Same event type as spec (`m.rtc.member` / `org.matrix.msc4143.rtc.member`),
same `slot_id`, same `application` object (incl. `m.call.intent`). Differences
and the in-place fill (`fill_from_2025_dialect`), which only ever *adds* a
modern field that is absent:

| dialect field | spec field | rule |
|---|---|---|
| `member: { user_id, device_id, id }` (no `membership`) | `member: { id, membership }` | insert `membership: "join"` iff `application.type` is a non-empty string, `member` is an object, `member.membership` is absent and `member.id` is a non-empty string. Never infer `leave`. |
| leave = content is **only** `{ msc4354_sticky_key }` (no `slot_id`) | full content with `member.membership: "leave"` | nothing dialect-specific: this *is* the MSC4354 removal of 1.1 (`Ingest::MemberRemoval`). With neither `slot_id` nor a sticky key it is simply not a membership |
| `rtc_transports: [ { type, livekit_service_url, … } ]` | `transports: { published, can_subscribe }` | iff `transports` absent and the array non-empty: `published` = array verbatim, `can_subscribe` = deduped `type` strings (elements without a string `type` skipped) |
| `member.device_id` (self-asserted) | device from decryption metadata | `claimed_device_id` = non-empty `member.device_id`; used **only** when the origin names no device: `Encrypted{None}` / `Cleartext` / `Unknown` + claim → `device_id = claim`, `DeviceAttribution::Claimed`; `Encrypted{Some}` always wins → `Verified`; neither → `None`, `Unknown` |
| `versions: []`, `m.relation` | — | ignored |
| `msc4354_sticky_key` **or** `sticky_key` | `msc4354_sticky_key` | both accepted (unstable first) |

Member id: `member.id` verbatim (equals the sticky key in practice). No
expiry logic of its own — the sticky map's `duration_ms` rules it.

### 6.2 MSC3401 state dialect (`StateEvents` only)

Event type `org.matrix.msc3401.call.member`, state key
`_{user}_{device}_{application}{call_id}` (not parsed; it is the
`RoomState.legacy` key, appears in logs and breaks focus ties). **One state
event = at most one membership.**

> **Out of scope, do not implement:** the generation before this one put a
> `memberships[]` array (all devices of a user) into a single state event.
> It is not supported by the current crates either. The only handling is a
> guard: content that carries `memberships` is ignored, so it can neither be
> mis-parsed as a per-device membership nor sneak back in as a feature.

Content fields read: `application` (**string**, e.g. `"m.call"`; an object is
refused), `call_id` (`""` = room call), `device_id` (required non-empty),
`membershipID`, `expires` (duration ms), `created_ts` (absolute ms),
`m.call.intent`, `foci_preferred[]` (`{type, livekit_service_url,
livekit_alias}`), `focus_active: { type, focus_selection }`. Not read:
`scope`, `expires_ts`.

Drop rules (each logged, except the leave): empty content `{}` = leave →
`LegacyMemberRemoval`; has `memberships` → drop; `application` not a
non-empty string → drop; `device_id` missing/empty → drop; already expired
at ingest → ignored (1.6; a live candidate that later expires surfaces once
as `Expired`).

Derivations:

| output | rule |
|---|---|
| `joined_at` | `min(created_ts, origin_server_ts)` if `created_ts` present, else `origin_server_ts` |
| `expires_at` | `joined_at + (expires or 14_400_000 /* 4 h */)`; expired when `expires_at <= now` (deadline itself counts as expired) |
| `slot_id` of the candidate | `LEGACY_SLOT_ID` (`""`) — the session key. The dialect's own `"{application}#{call_id or ROOM}"` is kept only as `legacy_call_id` on the candidate (diagnostics: tells several legacy call ids apart in `excluded_candidates`/debug) |
| `Member.application_type` | `application` verbatim (`m.call`) |
| `member_id` | `membershipID` if non-empty, else `"{sender}:{device_id}"` (this string is also the LiveKit participant identity in this generation — see `connections::legacy_participant_identity`) |
| `device_id` / `device_attribution` | `content.device_id` / `DeviceAttribution::Claimed`; `MemberCandidate.origin` = the event's `EventOrigin` unchanged (state events arrive `Unknown`/`Cleartext`) |
| `membership_ts` | `joined_at` (rejoin detection in `encryption`, 1.5) |
| `intent` | `m.call.intent` |
| `own_focus` | `foci_preferred[0]` verbatim |
| `prefers_own_focus` | `focus_active.focus_selection == "multi_sfu"` |

Focus resolution (`resolve_focus`, run over the **surviving** candidates of
one legacy slot at projection time): if `prefers_own_focus` and `own_focus`
is set → own focus; else the focus of the oldest survivor of the same slot
(ordered by `joined_at`, ties by `state_key`), falling back to the member's
own focus; if nothing anywhere → member appears with empty transports.
Result → `Member.transports.published = [ RtcTransport { transport_type:
focus.type, properties: focus verbatim } ]`, `can_subscribe = [focus.type]`.

Note for `own_membership`: in `StateEvents` mode *our own* member id must be
`{user}:{device}` (not a random id), because that generation's peers use it
as the LiveKit participant identity.

### 6.3 What today's bridge does that this plan drops on purpose
- The `memberships[]`-array generation stays unsupported (callout in 6.2).
- Fabricating `RawStickyEvent`s with a rewritten `m.rtc.member` type — the
  converters produce `MemberCandidate`s directly.
- The 30 s state-membership poll — `expires_at` is known, so the expiry
  timer covers it.
- "Sticky wins over the state member with the same key" — the generations
  live under different slot ids now.
- Aborting a whole tick on one malformed event (`manager.rs`
  `try_convert_membership_event` propagates the error) — malformed events are
  skipped individually (test 4.6).
