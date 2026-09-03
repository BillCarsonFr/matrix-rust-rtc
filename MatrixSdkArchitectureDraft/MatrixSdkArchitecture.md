# Matrix RTC SDK Architecture (Draft)

A plan for `matrix-rtc` crate that has clear responsibility: Do everything that needs to be done to
participate in an RTC session. It should NOT do any call specific work but be the foundation for any
rtc app. See [MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143).
Input and output interfaces:
 - the input is compleatly covered by a matrix-rtc-driver interface.
 - The output is a simple api exposing:
  - connections - metadata (SFU URL + JWT) needed to directly connect to a media stream. Currently Livekit room
  - member - metadata for each rtc member. What participant Ids to subscribe to in the livekit rooms
  - KeyMap` (member → media key) - all keys that are currently in use. For encryption on publish and decryption on retrival)

1. **The media plane leaves the SDK.**  No libwebrtc,
   no transport engine, wasm-compatible everywhere.
2. **One Matrix seam: `MatrixDriver`.** The trait mirrors the widget
   `MatrixDriver` in matrix-rust-sdk so that implementation drops in
   unchanged; the same trait is exported through UniFFI as a foreign trait,
   so a matrix-js-sdk-backed driver can be written later.

```text
                                host application
┌──────────────────────────────────────────────────────────────────────────────┐
│               MatrixDriver — implemented by the host, one per room           │
│                                                                              │
│   send    sticky / state / delayed events (MSC4354 · MSC4140                 │
│           restart/cancel) · delegate delayed leave to the SFU (MSC4195)      │
│   send    to-device messages (per-recipient delivery results)                │
│   read    timeline events · room state                                       │
│   sfu_endpoints OpenID · GET /rtc/transports · LiveKit get_token             │
│   emit    live streams: room events · state updates · to-device              │
└──────────────┬───────────────────────────────────────────▲───────────────────┘
               │ streams in                                │ commands out
               ▼                                           │ (one trait slice per part)
┌─ ParticipationManager::new(room_id, slot_id, own_identity, driver, config) ──┐
│                                                                              │
│  ┌─ Session ────────────────────────────────────────────────────┐            │
│  │ feeds itself: RoomEventsDriver slice — seeds via reads,      │            │
│  │ then consumes the live streams. The single converter:        │            │
│  │ every event type → Member candidates; joined projection      │            │
│  │ (slot · encryption · room conditions). All reads are         │            │
│  │ SessionSnapshots (same values as the static path ⇓)          │            │
│  └─────────┬────────────────────────┬──────────────────────┬────┘            │
│            │                        │                      │                 │
│            │ subscribe()            │ subscribe()          │ subscribe()     │
│            ▼                        ▼                      ▼                 │
│   ┌─ OwnMembership ────┐  ┌─ Connections ──────┐  ┌─ Encryption ──────┐      │ Connection = Established/ResolvedTransport, TransportConnection
│   │ join / leave state │  │ ConnectionData per │  │ SendMachine:      │      │
│   │ machine · delayed  │  │ service_url (multi-│  │ rotation +        │      │
│   │ leave · heartbeat  │  │ focus) · mint /    │  │ distribution;     │      │
│   │                    │  │ refresh tokens     │  │ KeyMap: verify +  │      │
│   │ ⇅ OwnMembership-   │  │                    │  │ store inbound keys│      │
│   │   Driver           │  │ ⇅ TokenDriver      │  │                   │      │
│   │                    │─▶│                    │  │ ⇅ ToDeviceDriver  │      │
│   └────────────────────┘  └────────────────────┘  │ ◀ to-device stream│      │
│     resolve_transport(member_id, intent) → add_own_transport                 │
│                                                                              │
├─ public surface ─────────────────────────────────────────────────────────────┤
│   join(intent, params) · leave(reason)                                       │
│                                                                              │
│   memberships() + on_memberships_change    one tile per entry: Session's     │
│                                            joined set ∪ Encryption's         │
│                                            left-with-keys members            │
│   connections() + on_connections_change    the LK rooms to hold              │
│   key_map()     + on_key_map_change        feed into frame encryption        │
│   status()      + on_status_change         Joining / Connected / Leaving     │
└──────────────────────────────────────────────────────────────────────────────┘
               ▼ callbacks → host renders tiles, holds LK rooms, sets keys
```

## Modules

### `session`

Pure projection of Matrix events into session state. No I/O. The session is
the **single converter** from raw Matrix events to the Member
representation — sticky *and* state, modern (member via sticky events) *and* legacy (member via state events). Nothing is
pre-translated into synthetic sticky events; This implies, that this module is the only one doing compoatibity.
Compatibilty should be encapsulated into seperate files, so that removing the compat later is low risk.
Hosts (via `MatrixDriver`) feeds everything into one funnel and the session dispatches on event type:

| event type | converted to |
|---|---|
| `m.rtc.member` (sticky; modern + 2025 dialect, permissive read always on) | member candidate |
| `org.matrix.msc3401.call.member` (state; only with `StateEvents` compat) | member candidates, one per device |
| `m.rtc.slot` (state) | slot condition |
| `m.room.member` / `m.room.encryption` (state) | room conditions |

- **All reads go through `SessionSnapshot`** — a plain, cloneable value
  carrying the joined projection plus its metadata (`slot_state`,
  `negotiated_encryption`, `start_ts`, `excluded_candidates`, `seeded`,
  `failed_reads`) and the conveniences functions (`member_count()`,
  `is_active()`).
- `failed_reads: Vec<SessionRead>` is the seed's honesty: `seeded` says the
  seed *finished*, this says whether it learned anything. `slot_state: None`
  with an empty `failed_reads` means "this room has no slot"; with `Slot` in
  it, "we could not find out". The same distinction decides whether
  `negotiated_encryption: None` should render as *unencrypted* or as
  *unknown*. Entries clear when a live state update later supplies the
  value, so it is a live condition rather than a permanent verdict.
- `compute_sessions_from_events(events, config) -> Vec<SessionSnapshot>` —
  static, cheap, and returns *values, not subscriptions*: nothing updates.
  A observable session will be created by passing a driver (i.e. build a `Session` see below).
  `compute_sessions_from_events` is the room-info path: matrix-rust-sdk's `room_info` computation
  calls it on every room update and populates its fields from the snapshot. One slice of all
  events, can even be many rooms at once (probably not helpful in reality);
  A list of `SessionSnapshot` is returned to allow mutliple slots per room.
- `Session::new(room_id, slot_id, driver: RoomEventsDriver, config)` — the
  **live** entry point: the session seeds itself (`read_state` /
  `read_events`) and consumes the driver's streams; consumers use
  `snapshot()` and `subscribe()`. There is no manual `update` — feeding a
  session is the driver's job.
- Converters live in `session::convert`, **one file per generation**, each
  producing the normalized internal candidate (member + source generation +
  origin + one `expires_at` from either the sticky duration or MSC3401's
  `expires`). Deleting a legacy generation = deleting its file plus one
  dispatch arm. We keep an enum `matrix_rtc_generation` for the sake of
  using different methods to compute the connection and member ID.
- **Join-condition projection** (behavior kept from today): candidates are
  re-projected whenever inputs move; exclusions carry a reason. Two
  conditions are **scoped by generation**: `SlotClosed` and
  `UnencryptedInEncryptedRoom` never apply to MSC3401 candidates (no slot
  concept; state events are unencrypted by nature — MSC4143 requires
  encryption only of sticky member events). Room-state conditions stay
  unenforced until the host supplies that state.
- **Encryption negotiation**: the slot prescribes whether RTC data is
  encrypted (mechanism required in encrypted rooms, forbidden in unencrypted
  ones, taken at face value when unknown); the result overrides any local
  config at join time.
- Accepts both stable (`m.rtc.*`) and unstable (`org.matrix.msc4143.*`) types
  inbound; `wire_event_type` translates outbound.
- Device attribution on MSC3401 candidates is *claimed* (from state-key /
  content), never verified — key verification treats it accordingly (that
  generation has its own to-device key type anyway).

### `connections`

Maps a `Session` to the SFU connections a host must hold (multi-focus,
MSC4195). The connection key is the transport's `livekit_service_url`
(`ConnectionData.service_url`); `ws_url` is the SFU websocket URL the token
response named.

- `ConnectionData { service_url, ws_url, jwt_token }`, `ConnectionWithMembers`.
- `ConnectionsManager`: `subscribe()` / `connections()` (one list, members per
  connection), `add_own_transport(member_id, intent)` — discovers the transport
  (`GET /rtc/transports` via the driver) when the intent names none, performs
  the MSC4195 `get_token` exchange (the legacy `/sfu/get` in `StateEvents`
  compat) and records it as our own — and `clear_own()` on leave. Tokens for
  the connections the session's members publish on are minted lazily by its
  pump and re-minted a minute before the JWT's `exp`. Nothing is published
  before we have a member id (the token names it).
- The participant-identity derivations as pure fns: MSC4195 pseudonymous
  hash, and the legacy `{user}:{device}` form.

### `own_membership`

The join/leave state machine for *our* membership. Talks only to the
`OwnMembershipDriver` slice of the driver plus one async resolver hook (the
facade's `connections.add_own_transport`). A pure machine (`machine.rs`, `now`
is an input) stepped by one pump task that owns time and I/O — the same shape
as `encryption`. Plan and status: `src/own_membership/OwnMembershipImplementationPlan.md`.

- `Status = NotJoined | Joining(JoinStatus) | Connected(ConnectedStatus) | Leaving(LeaveStatus)`,
  readable and observable (`subscribe_status()`); the facade maps `NotJoined`
  to `Disconnected`. `JoinStatus { has_fetched_transports,
  has_fetched_initial_member_list, has_created_transport_token,
  has_sent_delayed_leave_event, has_sent_member_join_event,
  has_delegated_delayed_event, has_started_heartbeat }`, `ConnectedStatus {
  keep_alive, membership, roster }`, `LeaveStatus { leave_event_sent,
  delayed_leave: Option<DelayedLeaveOutcome> }`.
- `ConnectedStatus` is one value per *mechanism*, because the three fail
  independently and each has its own remedy:
  - `KeepAlive = Armed { delay_ms, last_restart_ts, fires_at_ts } |
    Delegated { delegated_at_ts, earliest_fire_ts } | RestartFailing {
    since_ts, fires_at_ts, last_error } | Expired { since_ts } |
    Unavailable { permanent, next_probe_ts }`. `Delegated` is its own variant
    precisely because after delegation we stop restarting on purpose — a
    frozen `last_restart_ts` is health there, and a flag beside raw
    timestamps could not say so. `Expired` means "the delay's full period
    elapsed with no successful restart, so it has in all likelihood fired" —
    named for what the crate deduces, not for a removal it never observed.
    The `Armed -> Expired` transition is performed by `on_wake`
    (`next_wake_ts` includes the expiry instant); `status()` stays a pure
    projection with no clock of its own.
  - `MembershipPublication { lifetime_ms, last_published_ts, expires_at_ts,
    refresh_failing_since_ts, last_refresh_error }` — the sticky entry the
    server drops if no refresh lands, with the deadline precomputed.
  - `RosterPresence = AwaitingEcho | Present | Missing { since_ts,
    republished_at_ts } | Excluded { reason }`. `Missing` (was there, gone,
    self-heal re-sends) and `Excluded` (on the server, refused by the
    projection, self-heal deliberately does not) stay separate because their
    remedies differ.
  Because those values *change* when a mechanism starts failing, a failing
  keep-alive restart is finally push-visible through the facade's
  `on_status_change`. It used to mutate nothing observable
  (`last_restart_ms` moved only on success), which made the single most
  important "you are about to be kicked out" condition fire no callback at
  all — see `src/participation/ErrorSurfaceAnalysis.md` §3.1.
- A leave whose delayed-leave cancel fails still returns `Ok` — the delay is
  itself a leave — but says so: `DelayedLeaveOutcome::MayStillFire` warns
  that a stray delayed event of ours may still land.
- `OwnMembershipManager::new(room_id, slot_id, own, session, driver, compat, resolve_transport)`,
  `join(member_id, intent, params)`, `leave(reason)`. The member id is minted
  by the facade (`new_member_id(compat, &own)`: fresh per join, or
  `{user}:{device}` under `StateEvents`) so the encryption machine can be built
  with it *before* the join event goes out.
- Behaviour kept from today: delayed leave armed *before* the join event — as
  a delayed **sticky** event, so it clears our sticky-map entry when it fires;
  keep-alive = MSC4140 `restart` every `keep_alive_timeout_ms / 3` (never
  cancel+reschedule; a replacement is armed only once the old delay must have
  fired) plus a sticky re-send at half the published lifetime; a homeserver
  that refuses delayed events gets a 5-minute membership instead (lifetime
  frozen at join, MSC4354 ignores a shorter refresh); optional delegation of
  the delayed leave to the SFU (MSC4195, delay raised to ≥ 1 h, client restarts
  stop on success); leave sends `leave_reason {code, reason}` and cancels the
  delayed event.
- Reacts to the session: a slot closing under us leaves with `slot_closed`; a
  membership that vanished from the roster after it was seen is re-published
  (rate-limited).
- `TransportIntent::Publish(transport)` or `ReceiveOnly { can_subscribe }` —
  receive-only members (recorders, observers) are first-class; only `Publish`
  calls the resolver.
- Owns the **write side of compat**: with `StickyEvents`/`StateEvents`
  selected, *our own* events are rendered in that dialect (one deletable file
  per generation; MSC3401 goes out as room state, incl. a delayed *state*
  event for the leave). The read side lives in the session's converters.

### `encryption`

Per-member media key exchange over to-device messages (MSC4143). Plan,
challenges, test list and status: `src/encryption/README.md`. In short:

- `KeyMap = HashMap<member_id, Vec<MediaKey>>` — one entry per key *index*
  per member (frames carry the index; a peer's previous key stays needed for
  in-flight frames after they rotate). `MediaKey` carries key bytes + index
  so hosts can feed any LiveKit key provider; the change callback carries the
  single changed key.
- Rotation policy = matrix-js-sdk PR #5505 ("rotation slow down"): a shared
  per-minute to-device contingent (default 3000) gives every client a grace
  period `60 s · N(N−1) / contingent`; a change while unblocked starts a
  *jittered* block and defers the rotation to its end, changes while blocked
  coalesce into that same deadline, joiners get the current key at once, the
  rotated key is used `use_key_delay` after it was sent, and an owed rotation
  waits for that switch rather than minting over a propagating key. No hard
  participant limit: the contingent is the only brake. Implemented as a **pure state
  machine** (`send_machine.rs`: `now` and jitter are inputs, sends/key
  switches are outputs, the next deadline is a query) driven by one task
  (`pump.rs`) that owns the session `watch`, the to-device stream, the timer
  and the driver sends.
- **Inbound verification kept from today**: cleartext or origin-less keys
  discarded; the to-device sender+device must match the member event (a
  claimed MSC3401 device narrows, never widens; keys arriving early are held
  *with their origin* and verified when the membership lands); non-cross-signed
  senders rejected unless configured otherwise (MSC4153); an outdated-key
  filter stops replays from occupying a `(member, index)` slot.
- `Machine::new(driver: ToDeviceDriver, room, slot, compat, session,
  own_member, manage_media_keys, config, send_config, on_key_map_change)` —
  **one machine per participation**: it works from construction and dropping
  it is leaving. The facade constructs it in `join()` *before* the
  own-membership machine sends the join event and drops it in `leave()`.
- One `status()`: `Status::Joining { has_distributed_initial_keys,
  has_received_all_member_keys }` until both hold, then `Status::Connected {
  left_members_with_keys, fully_settled, last_rotation_ts }` for good (UI can
  show "members possibly still listening" and the join leak window).

### `executor`

The platform seam for the two things the crate must do on its own clock:
run a detached task and wait for a deadline (`spawn`, `sleep_ms`, `now_ms`).
Native: a crate-owned current-thread tokio runtime on its own thread (tasks
start from synchronous constructors and sink `emit`s, where no runtime
context exists). wasm32: `wasm_bindgen_futures::spawn_local`, `setTimeout`
via gloo-timers, `Date.now()` via web-time. Nothing else in the crate may
touch tokio `rt`/`time` or wasm-bindgen; `tokio::sync` is fine everywhere.
The tokio feature set is identical on every target on purpose (the ubrn
shim's v1 feature resolver unifies target-specific features into the wasm
build). Verified through the real wasm bindings by
`src/uniffi_api/runtime_probe.rs` + `web-test-app/test/runtimeProbe.test.ts`.

### `participation`

The facade a calling host uses. Owns and wires the four parts above.

- `ParticipationManager::new(room_id, slot_id, own: OwnIdentity, driver, config: ParticipationConfig { compat, encryption, rotation })`;
  hands the session its `RoomEventsDriver` slice (the session seeds and feeds
  itself), own-membership its `OwnMembershipDriver` slice, connections the
  `TokenDriver` slice, and — per participation — the encryption machine the
  `ToDeviceDriver` slice.
- `join(intent, params)`: waits for the session seed, checks the slot against
  the fresh snapshot, mints the member id, builds the encryption machine with
  the negotiated encryption (`negotiated_encryption` overrides the local
  default; an unencrypted call gets a machine that manages no keys), then
  `own_membership.join`. `leave(reason)` sends the leave, then drops the
  machine (forgetting every key) and the tokens. A participation the
  own-membership machine ends on its own (`slot_closed`) is reaped the same way.
- `Status = Disconnected(DisconnectCause) | Joining(JoinStatus) |
  Connected(ConnectedStatus) | Leaving(LeaveStatus)` composing the
  own-membership and encryption statuses. Every non-disconnected variant also
  carries `impairments: Vec<Impairment>`, reachable uniformly through
  `Status::impairments()`.
- **The organising split.** A failure is one of two things, and the two get
  different API shapes. *Recoverable* — a retry loop is live and the
  condition clears by itself — is modelled as **state inside `Status`**, and
  only there; it has no single moment of failure, so it can never be a return
  value. *Terminal* — the participation is over or never started, and only a
  host decision changes anything — belongs in the errors of the calls that
  produced it and in `DisconnectCause`. The dividing question is "if the host
  does nothing, can this clear on its own?".
- `Impairment` is the recoverable vocabulary: `KeepAliveRestartFailing`,
  `KeepAliveExpired`, `KeepAliveUnavailable`, `MembershipRefreshFailing`,
  `OwnMembershipMissing`, `OwnMembershipExcluded`, `MediaKeyNotDelivered`,
  `MediaKeyNotReceived`, `MediaKeyRejected`, `ConnectionUnavailable`,
  `ConnectionTokenExpired`, `SessionStateUnread`, `JoinedBeforeSeed`. Every
  variant clears by itself when the underlying operation succeeds — an
  impairment is never terminal. Each has a `severity()`
  (`Critical | Degraded | Notice`) and the vec is sorted most severe first,
  then by a fixed variant rank, then by the ids inside, so recomputing with
  unchanged inputs yields an identical vec and publish-on-change does not
  flap.
- The list is **deliberately redundant** with the structured state above, the
  same way memberships and connections are: the structured values are
  authoritative and carry the timestamps a UI renders countdowns from, while
  the flat list means a host that renders one warning banner cannot *miss* a
  condition it did not know to look for. It is a pure projection derived in
  one place (`Inner::impairments`, over pure free functions) and never
  carries a fact the structured fields do not.
- `DisconnectCause = NeverJoined | LeftByHost { reason } | SlotClosed |
  JoinFailed { at_ts, progress, error } | ManagerStopped { component }`.
  `Disconnected` used to be a unit: an admin closing the slot, our own
  `leave()` and a join that never completed were indistinguishable.
  `JoinFailed` carries the step-by-step `JoinStatus` the abort used to throw
  away, so a progress UI can say "we got a token but the membership event was
  rejected". `ManagerStopped` is published by a drop guard on the facade
  pump — a clean exit and an unwind both reach it. A panic inside the
  *callback dispatch* itself remains undetectable from inside; that boundary
  is documented rather than pretended away.
- `JoinError` splits `TransportUnavailable` into `NoTransport` (the
  homeserver advertises no usable SFU — a configuration problem, retrying is
  pointless) and `TokenRefused` (auth or network — retrying may work), and
  gains `EncryptionSetup`, which was an `InvalidParams(String)`: a crate
  precondition dressed as a caller mistake. `open_slot`/`close_slot` return
  `SlotError { InvalidSlotId, Driver }` so a local precondition is not
  conflated with a homeserver failure.
- `own_member_id()` / `own_membership()` hand back the member id the facade
  mints. Every self-referential check needs it — am I in the roster, was I
  excluded, which LiveKit participant is me — and `(user_id, device_id)` is
  not a substitute: one device may hold several RTC members, and a rejoin
  mints a fresh id.
- `connection_problems()` names the connections a host cannot use:
  `NoToken` (wanted, never minted — absent from `connections()` entirely) or
  `TokenExpired` (still served, because dropping it would break a host that
  is still connected, but past its `exp`). `ConnectionData` carries
  `expires_at_ts` so a host can check its own token instead of discovering
  staleness through a LiveKit connect that fails for no stated reason.
- `on_key_rejected(cb)` fires when an inbound media key is discarded, with
  the member and the typed `KeyRejection`. It is **secondary** to the latched
  per-tile `SessionMembership::media_key.rejection`, which is what a UI
  attaching late reads; the callback is for logging and telemetry.
- Getter + change-callback pairs for the four host-facing outputs:
  **memberships** (`SessionMembership[]`), **connections**
  (`ConnectionWithMembers[]`), **`KeyMap`** (callback carries the single
  changed key), **`Status`** — plus `session()`, the live `SessionSnapshot`
  (is the slot open, is the call encrypted; a host reads it before `join`,
  since a slot it just opened is open only once sync echoes the state). Getters compute from fresh inputs
  (`Session::snapshot()` is drain-on-read); one pump task fires the callbacks
  on change.
- Memberships vs. connections is a deliberate redundancy: memberships is the
  UI-shaped view (one tile per entry), connections the transport-shaped view
  (which LK rooms to hold). A `SessionMembership` carries what the tile→media
  lookup needs — the `service_url`s the member publishes on and the transport
  participant identity (MSC4195 hash; `{user}:{device}` in legacy compat).
- `SessionMembership::media_key: Option<MediaKeyState { holds_our_key,
  have_their_key, rejection }>` puts "who can hear whom" on the tile. `None`
  when the call manages no media keys, for our own tile, and while not
  joined. Two independent booleans on purpose: they fail for different
  reasons (our to-device send to them vs. theirs to us), clear independently,
  and a UI renders them in different places. `key_map()` is the *inbound*
  map — it says who we can hear, never who can hear us.
- A membership's state is `Joined` or `LeftWithKeys`: a member that left the
  session (leave/expired sticky) but still holds a not-yet-rotated copy of
  our media key stays in the list, so the UI can render "leaving — may still
  be listening" until the rotation settles. Computed as the encryption
  machine's key holders minus the fresh session roster.
- Slot administration: `open_slot(application_type, encrypted)` /
  `close_slot()` send `m.rtc.slot` state through the driver.
- Our own membership reaches the roster through the homeserver echo, like
  anybody else's — there is no synthetic local entry. Mock drivers (Rust tests,
  TS mock) therefore echo accepted sticky/state events.

### `driver`

The only Matrix I/O boundary, shaped after matrix-rust-sdk's widget
`MatrixDriver` so it drops in. Split into capability traits so each manager
receives no more than it needs; `MatrixDriver` is the sum.

- `OwnMembershipDriver`: send sticky / state / delayed events (a delayed event
  may carry `sticky_duration_ms` — the delayed leave is sticky), delayed state
  events (MSC3401 compat only), restart/cancel delayed (MSC4140), delegate
  LiveKit delayed leave. `DriverError::Unsupported | Unauthorized` from the
  delayed-event methods mean "this homeserver never will" (404 / 403).
- `DriverError = Http | Unauthorized | Unsupported | RateLimited {
  retry_after_ms } | Stopped | Other`. The taxonomy exists so a caller can
  decide what to do next without string matching: `Unsupported` /
  `Unauthorized` are permanent for this homeserver, `RateLimited`
  (`M_LIMIT_EXCEEDED`, previously an opaque `Http(String)` no host could back
  off from) is explicitly transient, and `Stopped` means the manager behind
  the call is gone and no retry helps — it surfaces as
  `DisconnectCause::ManagerStopped`.
- `ToDeviceSendDriver`: send one to-device message to a set of devices with
  per-recipient results. `ToDeviceDriver` adds the inbound stream (decrypted,
  with origin metadata and the MSC4153 cross-signing verdict).
- `RoomEventsDriver`: `read_events`/`read_state` + live room-event and
  state-update streams. Every inbound event carries an `EventOrigin`
  (encrypted+device / cleartext / unknown) from decryption metadata — the
  session and encryption rules depend on it.
- `TokenDriver`: `GET /rtc/transports`, LiveKit `get_token` (the adapter
  performs the OpenID hop itself; the request names our MSC4195 member claims
  and, in `StateEvents` compat, the legacy `/sfu/get`).

### Compat (no module of its own)

Pre-2026 Element Call interop is done **in place** rather than as an edge
translation to synthetic sticky events. `ElementCallCompat` (Off /
StickyEvents / StateEvents, selected per call) is consumed where each half
lives: reading in `session::convert` (one deletable file per generation),
writing in `own_membership`, the legacy token endpoint and identity in
`connections`. The trade: the session knows legacy formats (the delete-by
date now touches session code) — bought back by the one-file-per-generation
isolation; in exchange there are no fabricated events, one conversion point,
and one intake API.

### Bindings

One UniFFI surface (feature `uniffi`) for **every** platform: Swift and
Kotlin via uniffi-bindgen, React Native and web/wasm via
uniffi-bindgen-react-native. The generated API — and therefore the
documented API — is identical everywhere, which is the point: one driver
contract, one set of docs and examples, bugs fixed once. Nothing
high-frequency crosses this boundary (the media plane is host-owned), so
generated bindings cost nothing where it matters.

- Records with JSON-string payloads, listener callback interfaces for the
  change streams, `MatrixDriverCallback` as an async foreign trait — the
  same seam a matrix-rust-sdk adapter (mobile) and a matrix-js-sdk driver
  (web) implement.
- `FfiStatus` mirrors `Status` in full — `Disconnected { cause }`,
  `Joining { own_membership, encryption, impairments }`,
  `Connected { keep_alive, membership, roster, encryption, impairments }`,
  `Leaving { leave_event_sent, delayed_leave, impairments }` — with
  `FfiKeepAlive`, `FfiMembershipPublication`, `FfiRosterPresence`,
  `FfiImpairment`, `FfiSeverity`, `FfiDisconnectCause` and `FfiJoinError`
  alongside. It used to be four opaque variants with the sub-statuses left
  in `debug_snapshot`; `debug_snapshot` is diagnostics, an unversioned JSON
  dump for bug reports, and was never a UI contract. `FfiSessionSnapshot`
  carries `seeded`, `failed_reads` and `excluded_candidates`;
  `FfiMembership` carries `media_key`; `FfiConnectionData` carries
  `expires_at_ts`; `connection_problems()`, `own_member_id()`,
  `own_membership()` and `set_key_rejected_listener()` are exported.
  `RtcError` gained `RateLimited`, `NoTransport`, `TokenRefused`,
  `EncryptionSetup` and `Stopped`, and stops collapsing `Http` into
  `Driver`.
- The FFI mirrors the Rust shape `Manager::new(MatrixDriver::new(room))`
  literally: the host wraps its `MatrixDriverCallback` in an exported
  `FfiMatrixDriver` object — the one place the foreign trait becomes a
  `driver::MatrixDriver` — then hands it (with the room, slot, user and
  device id) to any number of managers (one room can hold several slots).
- Inbound events stay a driver job across the FFI: the foreign driver's
  `subscribe_*` methods receive Rust-exported **sink objects** — called
  synchronously, exactly once, at `FfiMatrixDriver` construction — and
  `emit` into them from the host SDK's event handlers (the to-device sink
  also takes the MSC4153 cross-signing verdict); `emit -> false` is the
  drop-guard signal to unhook. Hosts implement single-sink semantics;
  fan-out to managers happens Rust-side, so a foreign driver is consumed
  exactly like a native one — no push inlets on the manager.
- On `wasm32` the crate builds against uniffi's
  `wasm-unstable-single-threaded` feature (no tokio runtime; futures are
  `?Send`, hence the dual `async_trait` cfg pattern on the driver traits).
  Background work runs through `executor` (`spawn_local` on wasm), so a sink
  `emit` only *wakes* the consuming pump, which runs on the microtask queue
  after `emit` returns. Getters are nevertheless fresh: the session is
  **drain-on-read** — `Session::snapshot()` (and every FFI getter built on
  it) first ingests whatever the sinks have queued, under the same lock the
  pump uses, so an event is processed exactly once whoever gets to it. Only
  listener callbacks arrive a microtask later; web tests `await` a tick
  before asserting on a listener, never before a getter.
- Logging: everything through the `log` facade; bindings install the sink
  (console / logcat / host callback) with `RUST_LOG`-style filters. Key
  material, JWTs and OpenID tokens are never logged.

## Usage

For in-app status indication (room list / room header, no call):

```rust
// inside whatever computes the room info — session computation is cheap
// enough to just run on every room change
on_room_change() {
    // one slice of everything — sticky and state; the session dispatches
    // returns plain SessionSnapshots — values, not subscriptions
    let snapshots = compute_sessions_from_events(all_rtc_events, config);
    update_room_info(snapshots.map(|s| { s.member_count(), s.start_ts, s.application_type, .. }));
}
```

For a call. The host owns two pieces of state and reconciles both from the
callbacks — this should be doable with any LK SDK on any platform:

```rust
// host state
let mut lk_rooms: Map<service_url, lk::Room>; // index = ConnectionData.service_url
let mut tiles:    Map<member_id, Tile>;       // index = Member.member_id

on_join_pressed(matrix_room) {
    let manager = ParticipationManager::new(room_id, slot_id, own_identity, MatrixDriver::new(matrix_room), config);
    manager.join(intent, params).await;

    // 1. Tiles: one per membership. Media may not be attachable yet — the
    //    LK room might still be connecting; attach_media is retried below.
    manager.on_memberships_change(|memberships| {
        tiles.remove_all_not_in(memberships);
        for m in memberships {
            let tile = tiles.get_or_create(m.member.member_id);
            tile.set_info(m.member.display_name, m.member.avatar_url);
            match m.state {
                Joined       => tile.show_active(),
                // left the session but may still hold our current key
                LeftWithKeys => tile.show_leaving(),
            }
            attach_media(tile, m); // no-op for LeftWithKeys / receive-only
        }
    });

    // 2. LK rooms: diff against what we hold, connect/disconnect the rest.
    manager.on_connections_change(|connections| {
        for gone in lk_rooms.keys_not_in(connections) {
            lk_rooms.remove(gone).disconnect();
        }
        for conn in connections.filter(|c| !lk_rooms.has(c.connection.service_url)) {
            let room = lk::Room::new(conn.connection.ws_url, conn.connection.jwt_token);
            room.connect().then(|| {
                // room is up now: attach media for tiles that were waiting on it
                for m in manager.memberships() { attach_media(tiles[m.member.member_id], m); }
            });
            lk_rooms.insert(conn.connection.service_url, room);
        }
    });

    // 3. Keys: route each changed key to the LK room(s) of that member.
    manager.on_key_map_change(|key_map, change| {
        // one changed key per callback: (member_id, key bytes, index)
        if let Some(m) = manager.memberships().find(|m| m.member.member_id == change.member_id) {
            for service_url in m.connections {
                lk_rooms[service_url].set_key_for_participant(m.transport_identity, change.key.key, change.key.index);
            }
        }
    });

    // 4. Status: drive the call UI (spinner during Joining, banner on Leaving).
    manager.on_status_change(|status| call_ui.set_status(status));
}

// tile -> media: membership names the LK room and the participant in it
attach_media(tile, membership) {
    for service_url in membership.connections {
        if let Some(room) = lk_rooms.get(service_url) && room.is_connected() {
            tile.attach(room.participant(membership.transport_identity).tracks());
        }
    }
}

on_leave_pressed() {
    manager.leave(reason);
    for room in lk_rooms.drain() { room.disconnect(); }
    tiles.clear();
}
```

## Coverage vs. the current implementation

| Today (matrix-rust-rtc) | Here |
|---|---|
| `matrix-rtc-core` session/manager/slot/join-conditions | `session` |
| `matrix-rtc-core` `OwnMembershipMachine`, join/leave/heartbeat | `own_membership` |
| `matrix-rtc-core` `EncryptionManager`, key checks, rotation | `encryption` |
| `RtcCommandSender` + FFI `CommandSenderCallback` + `SdkCommandSender` | `driver` (one trait family, all bindings) |
| `matrix-rtc-livekit` token/`identity` (MSC4195) | `connections` (token via driver; identity mapping stays a pure fn) |
| `matrix-rtc-livekit` session/keys/transport + `matrix-rtc-media` engine | **host's LiveKit SDK** — out of SDK scope by design |
| `matrix-rtc-bridge` `compat` | in place: `session::convert` (read), `own_membership` (write), `connections` (token/identity) |
| `matrix-rtc-bridge` `run_sticky_bridge` | driver streams → `participation` wiring |
| `matrix-rtc-ffi` / `matrix-rtc-wasm` | one `uniffi` surface (uniffi-bindgen + uniffi-bindgen-react-native) |

The one capability that intentionally has no home here is the in-Rust media
engine (frame streams, constraints, multi-focus connection pool, recording
bots). Hosts that need it — Rust bots — use livekit-rust directly against
`ConnectionData`/`KeyMap`, which is the same contract every other platform
gets.

The implementation of this plan lives in `MatrixSdkArchitectureDraft/`
(`README.md` there for build/test; per-module plans and status next to each
module).
