# Architecture

This document explains the initial architecture of the Matrix RTC Rust workspace.

## Why this structure

The goal is to keep protocol logic in one Rust core crate and make all platform adaptation explicit at the edges.

- `matrix-rtc-core` owns RTC domain behavior.
- `matrix-rtc-bridge` owns how that behavior reaches a Matrix homeserver.
- `matrix-rtc-wasm` owns JavaScript-facing conversion and wasm export details.
- `matrix-rtc-ffi` owns native binding-facing conversion and UniFFI boundary types.
- `matrix-rtc-livekit-proto` owns the pure MSC4195 control plane (identity
  derivations, token shapes, dialect choices) shared by the native transport
  and the web binding.

Three axes, kept separate on purpose: the core answers *what the protocol says*,
`matrix-rtc-bridge` *how it reaches a homeserver*, and `matrix-rtc-media` +
a transport crate *how bytes flow*. Only the top-level facade
(`matrix_rtc_livekit::call::Call`) knows all three.

Arrows point at what a crate depends on:

```
 matrix-rtc-wasm ─────────────────┐       matrix-rtc-ffi
   │     │                        │            │      ╎
   │     │                        │            │      ╎ feature "media"
   │     ▼                        │            ▼      ▼
   │   matrix-rtc-livekit-proto   │         matrix-rtc-livekit ──┐
   │     │        │               │            │                 │
   │     │        ▼               ▼            ▼                 ▼
   │     │      matrix-rtc-bridge ◀────────────┤   ┌─▶ matrix-rtc-media
   │     │        │                            │   │      │
   ▼     ▼        ▼                            ▼   │      ▼
┌────────────────────────────────────────────────────────────────────┐
│                          matrix-rtc-core                           │
└────────────────────────────────────────────────────────────────────┘
   (matrix-rtc-wasm and matrix-rtc-livekit both take the ─▶ edge to
    matrix-rtc-media; the ffi takes it only under "media")
```

Two things that shape reveals. **`matrix-rtc-bridge` and `matrix-rtc-media` are
siblings, not layers** — the control plane and the media plane both sit on the
core, neither knows the other exists, and `matrix-rtc-livekit` is the first crate
that needs both. And **the media plane splits by what owns the bytes**:
`matrix-rtc-media` (the roster/pool engine) and `matrix-rtc-livekit-proto` (the
pure MSC4195 control plane) compile for wasm32 and are shared by both bindings,
while `matrix-rtc-livekit` (libwebrtc, reqwest) stays native-only — browsers keep
using livekit-js for the media itself, driven through a JS delegate.
`matrix-rtc-ffi`'s default build stays slim: the transport and media crates enter
only under its `media` feature, which is what keeps that mobile artifact free of
`libwebrtc`. For legibility the diagram omits one edge: under `media`,
`matrix-rtc-ffi` also depends on `matrix-rtc-media` directly, not just through
the transport.

This keeps the core reusable and testable while avoiding platform-specific dependencies in core.

## Who drives the call

The DAG above never mentions a Matrix SDK. That is not because there is only one
place it could go — it is because there are **two**, and neither is on a default
dependency path. `matrix-sdk` enters the workspace only through the `matrix-sdk`
feature of `matrix-rtc-bridge` and `matrix-rtc-livekit`, which is off by default.

`RtcCommandSender` (defined in `matrix-rtc-core`) is the seam that makes both
topologies work: `FfiCommandSender` (a host callback) and
`matrix_rtc_bridge::SdkCommandSender` (a real `matrix_sdk::Client`) are two
implementations of one trait, and the core cannot tell them apart.

### Host-driven — production mobile and web

The Matrix client lives **outside** this workspace, *above* the bindings. It is a
consumer, not a dependency:

```
┌────────────────────────────────────────────────────────┐
│ host app and its own Matrix client                     │
│ matrix-rust-sdk (mobile) / matrix-js-sdk (web)         │
└────────────────────────────────────────────────────────┘
        │ outbound commands      ▲ inbound events
        │ CommandSenderCallback  │ stickies + keys
        ▼                        │
┌────────────────────────────────────────────────────────┐
│ matrix-rtc-ffi   /   matrix-rtc-wasm                   │
│ no matrix-sdk anywhere in the graph                    │
└────────────────────────────────────────────────────────┘
                              │
                              ▼
   matrix-rtc-core  (+ media / livekit under "media")
```

The bindings carry no Matrix SDK at all — not even transitively, and not even
with the FFI's `media` feature on. `cargo tree -p matrix-rtc-ffi --features media`
contains zero `matrix-sdk` entries, because the FFI depends on
`matrix-rtc-livekit` *without* its `matrix-sdk` feature, deliberately.

### Rust-driven — tests, examples, recording bots

Here the Rust process owns a `matrix_sdk::Client` and the SDK is a **dependency
below**. This is the topology of the e2e call test, `join_and_record`,
`load_test`, and the `connect` example:

```
┌────────────────────────────────────────────────────────┐
│ tests/, examples/, recording bot                       │
│ owns a matrix_sdk::Client directly                     │
└────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────┐
│ matrix_rtc_livekit::call::Call                         │
│ matrix_rtc_bridge::SdkCommandSender ──▶ matrix_sdk     │
│ both behind feature "matrix-sdk", off by default       │
└────────────────────────────────────────────────────────┘
                              │
                              ▼
   matrix-rtc-core
```

### The consequence worth knowing

**`call::Call` exists only in the Rust-driven topology.** It is gated on
`matrix-sdk`, the FFI does not enable that feature, so the FFI cannot use
`Call::join` and hand-rolls the equivalent wiring in `src/media/session.rs` plus
`RtcSessionManagerHandle`. The workspace therefore has two implementations of
"join a slot and attach media" — a facade that compiles for only half its
consumers.

That is what makes extracting `Call::join`'s transport-agnostic half into
`matrix-rtc-bridge` worthwhile rather than cosmetic: that half needs an
`RtcCommandSender`, not a `Client`, so it would serve both topologies and let the
two implementations converge.

## High-level data flow

This is the host-driven topology above, in detail.

1. A Matrix client receives a sticky event (`MSC4354`) for MatrixRTC membership (`MSC4143`).
2. Platform binding converts the incoming shape into a core input event.
3. `RtcSessionManager` ingests a room-scoped initial sticky snapshot or incremental sticky update.
4. The manager groups events by `(room_id, slot_id)` and forwards each batch once to a single-session `RtcSession`.

At this stage there is no persistence, network transport, or encryption key distribution logic yet.

## Crate boundaries

## `crates/matrix-rtc-core`

- Public API:
  - `RtcSession::new` (single session)
  - `RtcSession::initial_events` (single session)
  - `RtcSession::handle_update` (single session)
  - `RtcSessionManager::on_sticky_events_snapshot_received` (multi session)
  - `RtcSessionManager::on_sticky_events_update_received` (multi session)
  - `RtcSessionManager::initial_sticky_for_room` (room-scoped)
  - `RtcSessionManager::sticky_update_for_room` (room-scoped)
- Input boundary:
  - `RawStickyEvent`, `RawStickyEventUpdate`, and `StickyEventsUpdate` represent SDK-provided sticky snapshot/diff data.
- Conversion:
  - Converts only RTC membership event types (`m.rtc.member` and `org.matrix.msc4143.rtc.member`) into `CallMembershipEvent`.
- Session state:
  - In-memory membership is owned directly by `RtcSession`.
  - `RtcSessionManager` owns multiple `RtcSession` instances keyed by `(room_id, slot_id)`.
  - `RtcSession` exposes reactive membership snapshot subscriptions for a single session.
  - TODO: add a manager-level lifecycle subscription API for session added/removed events.

## `crates/matrix-rtc-bridge`

- The Matrix side of the stack, and deliberately transport-free — nothing in it
  knows what a LiveKit SFU is, so a second transport reuses it unchanged.
- `sdk` (behind the **`matrix-sdk` feature**): `SdkCommandSender` implements the
  core's `RtcCommandSender`, turning outbound commands (join/leave sticky events,
  the dead man's switch delayed events, Olm-encrypted `m.rtc.encryption_key`
  to-device messages) into Client-Server requests; `run_membership_bridge` feeds
  the room's live membership back into an `RtcSessionManager`. Which carrier it
  reads is a build-time fact: MSC4354 sticky events need the
  **`experimental-sticky` feature** and the SDK fork that implements them (see
  `.cargo/experimental-sticky.toml`); the default build, against upstream
  matrix-rust-sdk, reads only `org.matrix.msc3401.call.member` room state, so
  it serves the pre-sticky `ElementCallCompat::StateEvents` mode and nothing
  else. `STICKY_EVENTS_SUPPORTED` exposes which build this is.
- `OpenIdTokenSource` (always available): the host's route to a Matrix OpenID
  token, which a transport exchanges for its own credentials. The trait is
  unconditional so a transport can name it; the `matrix_sdk::Client` impl sits
  behind the feature.
- `compat` (always available): interop with MatrixRTC implementations that predate
  the 2026 MSC4143 rewrite — today only Element Call on the JS SDK, the sole other
  implementation available to test against. Pure JSON translation with no Matrix
  SDK and no async runtime, which is the reason the `matrix-sdk` feature is
  optional at all: its ~50 unit tests build in seconds against no git
  dependencies. Scaffolding with a delete-by date, selected per call by
  `matrix_rtc_livekit::CallOptions::element_call_compat`, covering two
  generations:
  - **`StickyEvents`**, the 2025 format: already MSC4354 sticky-based, differing
    only in the fields inside the member content. Confined to JSON funnels at the
    edge so no dialect parameter or legacy field reaches the core. *Reading* it is
    permissive and always on (it only fills in modern fields that are absent, so
    spec-shaped events pass through untouched); *writing* it is opt-in, being the
    half that changes what other clients see.
  - **`StateEvents`**, the format before MSC4354: membership as
    `org.matrix.msc3401.call.member` **room state**, a plain `{user}:{device}` SFU
    participant identity, and the pre-MSC4195 `/sfu/get` token endpoint. Opt-in in
    both directions, and not additive — such a call is visible to that generation
    and to nobody else. The core still sees only MSC4143: the state events are
    translated into synthetic sticky memberships in `sdk`, and the slot condition
    is left unenforced because that generation has no slot concept.
  - Three things refuse to be JSON and so live outside `compat` as one `match`
    each: the ruma request type (`sdk`), and — in `matrix-rtc-livekit`, because
    both are MSC4195 rather than Matrix concerns — the token endpoint and the
    identity derivation.

## `crates/matrix-rtc-media`

- The transport-agnostic media model: `Participant` roster keyed by
  `member_id`, `CallEvent` (the unified membership + media event stream),
  owned frame types (`AudioFrame` PCM, `VideoFrame` I420), per-stream
  `MediaConstraints` (visibility, rendered size, quality cap → subscribe-side
  simulcast control, debounced and re-applied by the engine whenever a
  stream (re)appears), and the publish surface (`PublishOptions` →
  `LocalTrackHandle`; the application pushes captured frames in, the
  transport owns encoding/simulcast, publications go to the own focus, and
  `CallEngine::unpublish` retracts one — a mute keeps the publication up,
  which is right for a camera but not for a stopped screen share).
- Defines the `MediaTransport`/`TransportConnection`/`RemoteTrackHandle`
  traits a transport backend implements (LiveKit today; P2P/WebTransport
  designed for) and the `CallEngine` that reconciles core membership
  snapshots with transport `ConnectionEvent`s: reverse identity mapping
  (pseudonymous identity → membership), buffering of media that arrives
  before its membership, roster/event emission.
- The engine owns the **multi-focus connection pool** (MSC4195 multi-SFU):
  members are grouped by their published transports' connection key
  (LiveKit: the `livekit_service_url`); the engine connects to every peer
  focus via `MediaTransport::connect` (exponential backoff on failure),
  closes connections whose last member left after an idle grace, and
  reconnects a dead peer-focus connection after tearing down its streams.
  Only the *own* focus — established synchronously by the caller so join can
  fail fast, then handed over via `adopt_own_connection` — ends the call
  when it dies.
- Depends only on `matrix-rtc-core` + tokio/futures — no LiveKit, no
  libwebrtc, fully unit-testable (`FakeTransport`). Compiles for wasm32:
  the transport traits are `Send + Sync` off wasm (via `MaybeSend`) and
  unconstrained on it, and tasks/timers go through the `rt` seam (tokio
  natively; `spawn_local` + setTimeout-backed sleeps in the browser, where
  the engine's actor runs on the JS microtask queue).
- Also owns the transport-agnostic media-key handler (`keys`):
  `FrameKeyRing` is the seam a transport's key ring implements (LiveKit
  native's `KeyProvider`, livekit-js's `ExternalE2EEKeyProvider`), and
  `MediaKeyHandler` owns the recording, ring-size guard, rejected-key rule,
  local sender's index switch, and the MSC4143 `delayBeforeUse` wait.
- Design/feasibility notes and the phased plan:
  `agent-workspace/media-abstraction/PLAN.md`.

## `crates/matrix-rtc-livekit-proto`

- The pure half of the MSC4195 LiveKit control plane, shared by the native
  transport and the web binding: `identity` (the hash derivations),
  `token` (`/get_token` and legacy `/sfu/get` request builders and the
  response decoder — no IO), `TokenEndpoint`, and `identity_mapper` (the
  per-generation identity derivation, deliberately a Rust closure because
  `RtcIdentityMapper` is `Send + Sync`).
- Compiles for wasm32; `matrix-rtc-livekit` re-exports it under its
  original paths and adds the IO (reqwest, the LiveKit client).

## `crates/matrix-rtc-wasm`

- Exposes `WasmRtcSessionManager` to JavaScript.
- Accepts `JsValue` payloads for snapshots and updates and deserializes via `serde-wasm-bindgen`.
- Maps JSON fields to core sticky event DTOs.
- Host-driven hooks the page must call: `heartbeat` on an interval while
  joined (`HEARTBEAT_INTERVAL_MS`), `receiveEncryptionKey` per decrypted
  key to-device message, `flushDueKeyRotation` when told a switch
  completed; plus `openSlot`/`closeSlot`.
- `media/`: `connectMedia` attaches media to a joined slot — the shared
  `CallEngine` (roster + multi-focus pool) over `JsMediaTransport`, a JS
  delegate driving livekit-js. Rust owns the protocol (token requests via
  `-proto`, identities, pool policy, key bookkeeping via the shared
  `MediaKeyHandler` backed by the delegate's `setKey`); JS owns the IO
  (OpenID token, `fetch`, `Room.connect`) and all media. Room events come
  back through the typed `WasmConnectionEventSink`; roster entries carry
  `rtc_identity` for joining to livekit-js participants. Roster/event/
  switch-complete delivery is push (delegate callbacks from spawned
  pumps) — deliberately not async session methods, which would park a
  wasm-bindgen object borrow across an await.

## `web`

- Browser-first JavaScript packaging around `crates/matrix-rtc-wasm`.
- Uses `wasm-pack` to generate browser and Node.js runtime bundles into ignored `pkg/` subdirectories.
- Keeps generated JavaScript/WASM artifacts out of git while providing a small JS test surface.
- `src/matrix-rtc-call.mjs` (export `./call`): the `MatrixRtcCall` wrapper —
  implements the media delegate over `livekit-client` (optional peer
  dependency, injected), drives the heartbeat, and joins roster entries to
  live livekit-js participants by `rtc_identity`.

## `crates/matrix-rtc-ffi`

- Exposes UniFFI objects and records for Swift/Kotlin consumers.
- Keeps FFI DTOs local to the crate and converts them into core DTOs.
- Preserves session subscription semantics through a polling subscription object.
- Signalling extras for media hosts (always available): `transports` flow
  through both directions (`transports_json` passthrough on inbound sticky
  events; typed `FfiRtcTransport` on membership records) and decrypted
  `m.rtc.encryption_key` to-device messages are fed in via
  `RtcSessionManagerHandle::receive_encryption_key`.
- Behind the **`media` cargo feature** (default off — pulls the LiveKit
  client and libwebrtc, ~8–15 MB per ABI): `src/media/` exposes the
  transport-agnostic media model to mobile. The host joins the slot through
  the manager as usual, then `connect_media_session` attaches media (E2EE
  key bridge into the core, the `CallEngine` with its multi-focus pool, the
  own-focus SFU connection). `MediaSession` surfaces `next_event()` (async
  pull → Kotlin `Flow` / Swift `AsyncStream`), the participant roster,
  `set_constraints`, frame streams (audio frames by value; video frames as
  objects with safe copies *and* zero-copy plane pointers), and local
  publications the host pushes captured PCM/I420 into. OpenID tokens come
  from a host-implemented `OpenIdTokenProvider` (async foreign trait);
  outbound keys ride the existing `CommandSenderCallback`. All media work
  runs on a dedicated multithreaded tokio runtime — the manager's `?Send`
  futures never touch it. Android gets a `JNI_OnLoad` that initialises
  libwebrtc.

## `crates/matrix-rtc-livekit`

- Implements the MSC4195 LiveKit transport: the "LiveKit SDK" layer that turns
  `matrix-rtc-core`'s membership/key outputs into a live SFU media session.
- Owns the authorisation-service `/get_token` exchange (`token`; and the
  pre-MSC4195 `/sfu/get` one, for legacy interop) and the MSC4195
  hash derivations (`identity`), drives a LiveKit `Room` (`session`), and bridges
  core media keys into LiveKit per-participant frame encryption (`keys`,
  `MediaKeyBridge` → `KeyProvider`, HKDF mode, GCM frames).
- Obtains the Matrix OpenID token via `matrix-rtc-bridge`'s `OpenIdTokenSource`
  trait, so the crate is not hard-wired to a particular Matrix SDK. `MemberClaims`
  stays here: those are the `/get_token` request body's claims, which no
  homeserver ever sees.
- Implements `matrix-rtc-media`'s transport traits in `transport_impl`
  (`LiveKitMediaTransport`): connection key = `livekit_service_url`, remote
  identity = MSC4195 pseudonymous identity, `RoomEvent` → `ConnectionEvent`
  translation, and `NativeAudioStream` → owned PCM frame streams behind
  `RemoteTrackHandle`.
- Behind `matrix-sdk` it ships `call` — a `Call::join`/`Call::leave` facade that
  composes `matrix-rtc-bridge`'s signalling with this transport: membership, key
  exchange, transport discovery, the E2EE SFU connection, and a `CallEngine` in
  one handle (the crate README's quick start; also what the e2e test drives).
  `Call::subscribe_call_events`/`Call::participants` are the transport-agnostic
  surface; the raw `Call::events`/`Call::session` accessors remain during the
  transition. `Call::join` is currently the only place the Matrix and media
  halves are interleaved — extracting its transport-agnostic half into the bridge
  is a known follow-up.
- Selects the pre-2026 compatibility mode per call via
  `CallOptions::element_call_compat`, and owns the two parts of it that refuse to
  be JSON and so cannot live in `matrix-rtc-bridge`'s `compat`: the token endpoint
  (`TokenEndpoint`, `token`/`lib`) and the participant-identity derivation
  (`identity_mapper`, which hashes per MSC4195 — a LiveKit document). Also
  `call::register_legacy_key_receiver`, for that generation's to-device key type.
- Native-only by nature (the LiveKit client pulls in `libwebrtc`); never targets wasm.

## Spec alignment

- `MSC4143` (MatrixRTC): membership events represented by `m.rtc.member`.
- `MSC4354` (Sticky events): membership updates are received as sticky events.
- `MSC4075` (Notifications & ringing): `m.rtc.notification`, send side only.

The core uses the stable ids (`m.rtc.member`, `m.rtc.slot`) internally, but the
deployed ecosystem still matches on the unstable `org.matrix.msc4143.*` ones, so
bindings translate on the way out: the `matrix-sdk` host via ruma's alias table
(`matrix-rtc-bridge`'s `sdk::wire_event_type`), the FFI and wasm bindings — which hand the
type to an SDK that puts the string on the wire verbatim — via
`matrix_rtc_core::wire_event_type`. Inbound, both spellings are accepted.

Current implementation only establishes event intake and membership state wiring; protocol completeness is intentionally deferred.

### MSC4143 catch-up status

The vendored reference in `skills/msc/references/msc4143.md` tracks the rewritten
proposal. The `m.rtc.member` wire format now matches it:

- `member.membership` (`join` / `leave`) is the explicit join signal; the old
  inference from content shape is gone.
- `leave_reason {code, reason}` replaces `disconnect_reason {class, reason,
  description}`. Note the inversion: `code` is the machine-readable half and
  `reason` the human-readable one.
- `transports {published, can_subscribe}` replaces the flat `rtc_transports`
  array.
- `member.claimed_user_id`, `member.claimed_device_id`, `versions`,
  `m.relates_to` and `created_ts` are gone. The sending device now comes from the
  event's decryption metadata and rides on `RawStickyEvent::origin`, which the
  Matrix bridge fills from `EncryptionInfo` — for sticky events that is the fork
  SDK's "keep encryption info for sticky events" change, one of the reasons the
  sticky carrier needs `experimental-sticky`.
- `member.id` is generated fresh per join (`generate_member_id`), as the spec
  requires; it is no longer derived from the user and device IDs.

Inbound `m.rtc.encryption_key` messages are checked before use. A key is only
stored and signalled once it has been matched against the sender's member event:

- The host reports how the message arrived via `KeyOrigin`, built from Olm
  decryption metadata. Cleartext messages are discarded, since nothing in the
  payload can be trusted to identify a sender.
- The to-device sender and its device must equal the sender and sending device
  of the `m.rtc.member` event the message names, or the key is discarded. A key
  naming another room is discarded too.
- If the member event names no device to check against — cleartext, or encrypted
  but not attributable to one — the match cannot be performed, so the key is
  discarded rather than accepted on the user match alone. An encrypted member
  event should always resolve to a device (Olm messages carry the sender's device
  keys), so that half is a backstop rather than an expected path. The exception
  is `EventOrigin::Unknown`, where the host reported nothing at all and the rule
  is skipped like every other unreported fact.
- Keys from devices that are not cross-signed are discarded unless
  `EncryptionConfig::require_cross_signed_sender` is turned off (MSC4153).
- A key that arrives before its member event is buffered *with its origin* and
  checked when the membership shows up — verification is deferred, never
  skipped. Rejected keys never reach the outdated-key filter, so a bogus key
  cannot take the `(member, index)` slot and suppress the genuine one.

The outgoing key message declares `format: 0` as the spec requires.

### Notifications and ringing (MSC4075)

Membership says who is *in* a session, never who should be *summoned* to one, so
a mobile client had nothing to raise an incoming call from. `notification.rs`
builds the `m.rtc.notification` that fills the gap; `RtcSession::join` sends it
when the host set `JoinSessionParams::notify`. Three decisions are worth knowing:

- **The relation is what forced a breaking host change.** MSC4075 requires an
  `m.reference` to the sender's own `m.rtc.member` event, and nothing in this
  workspace had ever seen an event id — `send_sticky_event` returned `()` and
  `RawStickyEvent` carried no id either (it does now, for reactions; see
  below). Both `send_sticky_event` and
  `send_state_event` now return `String`, filled from the send response at every
  implementation site, and `OwnMembershipMachine::join` hands the membership's id
  back to its caller. Not `Option<String>`: every Matrix send responds with an
  event id, so an implementation that cannot produce one is broken, and failing
  loudly beats a call that joins fine and quietly never rings. `send_state_event`
  is included because the pre-MSC4354 Element Call dialect routes the membership
  through it. Recovering the id from our own membership echoing back through sync
  (what matrix-js-sdk does) was rejected for putting a full sync round trip in
  front of the ring.
- **Only the starter notifies.** The MSC leaves the question open, but every
  joiner sending one rings the room once per participant, so the send is
  suppressed unless the roster holds somebody *else*. "Else" is load-bearing:
  the host feeds the room's whole sticky map, so our own membership is in it as
  soon as the homeserver echoes it back, and a session outlives `leave()`
  keeping the previous call's membership as a candidate. Counting either
  concludes somebody else started the call and rings nobody. The check therefore
  excludes memberships from our own user whose sending device is ours *or
  unreported* — deliberately wider than the roster's own
  `SupersededOwnParticipation` rule, which needs a known device and so leaves
  such a candidate in. Being wrong that way costs one extra ring in an
  unencrypted room; being wrong the other way is a call that silently never
  rings.
- **The content states the call fields twice.** The MSC nests them under
  `application`; Element Call and ruma's `RtcNotificationEventContent` read them
  at the top level, and ruma *requires* them there, so a purely nested event
  fails to deserialize in the very SDK the mobile client uses. Both are written.
  `element_call_compat` additionally strips `application` and `m.text` for the
  byte-exact legacy shape.

Receiving is not implemented: the MSC's ring conditions, lifetime expiry against
`origin_server_ts`, `m.call.ring.ack` acknowledgements and the sender-side
"still ringing" indication are all absent, and on mobile the first signal is a
push notification that never passes through this workspace anyway.

### Reactions and the raised hand (Element Call, unspecced)

Element Call's reactions are ordinary room events that *relate to the reacting
member's own membership event*: an emoji reaction is an `io.element.call.reaction`
with an `m.reference` and `emoji` / `name` fields, a raised hand is an
`m.reaction` annotation with key `🖐️`, lowered by redacting it. Nothing in the
content is trusted beyond that relation — the receiver checks that the reaction's
sender is the membership's sender, which the homeserver authenticated. The
protocol lives in `reactions.rs` and is exercised through `RtcSession`; the
decisions worth knowing:

- **Only the protocol is in the SDK; sound and display are the host's.** The
  media crate has no playout path, and capture and render are platform-side by
  design, so a received reaction carries a sound *hint* (`ReactionSound`,
  resolved from Element Call's catalogue by `name`; unknown names map to the
  generic sound) and the host plays its bundled asset. Element Call's "play
  reaction sounds" toggle is therefore a host setting; what the SDK owns is the
  gating — an enable flag, the three-second per-member active window Element Call
  applies on receipt, and a send cooldown that refuses what peers would drop.
- **The membership event id moves, and the hand follows it.** A sticky refresh
  re-sends the membership and the new event replaces the old in the sticky map;
  matrix-js-sdk's `CallMembership.eventId` follows it, and Element Call drops a
  raised hand whose membership event has moved on, re-querying the new event's
  relations. So `OwnMembershipMachine` now tracks the latest event id, and the
  heartbeat re-annotates our hand onto the new event (redacting the old
  annotation) whenever it has moved — every 30 minutes at the default lifetime.
  Peers may see the hand drop for one round trip in between; that is the
  protocol's, not ours. As a *receiver* we are more lenient: a hand stays up for
  as long as the member is in the call, and a reaction is validated against every
  membership event id seen for that member, not only the latest.
- **Event ids had to reach the core.** `RawStickyEvent` and `JoinedMembership`
  carry the membership event id (optional in the DTO, so a host that cannot
  supply one still compiles — but its members cannot then be reacted for). The
  roster republishes when only ids moved; nothing downstream churns on it, since
  the media engine and key distribution diff by `member_id` and `membership_ts`.
- **Hands raised before we joined come from `/relations`.** The timeline we see
  live starts at our join; the annotation lives in the relations of the member's
  membership event. The session lists membership events whose relations it has
  not seen (`pending_relation_lookups`), the host answers each with
  `rel_type=m.annotation`, `event_type=m.reaction` (`on_relations_received`), and
  only hands are taken from the answer — an hour-old applause is not replayed.
  The matrix-sdk bridge and the `matrix-js-sdk` host module do this on every
  tick; an FFI host does it itself, one request per new membership event id.
- **Inbound needed a new intake.** The core only ever saw sticky, slot and
  to-device traffic. `RawTimelineEvent` (`on_room_timeline_events`) and
  `on_event_redacted` are routed by *room* — a reaction names no slot — to every
  session of the room, each keeping what relates to its own members. The matrix-sdk
  bridge feeds them from a room event handler (`register_timeline_receiver` →
  `run_timeline_bridge`, the same `Send`-handler-to-`spawn_local`-pump shape as
  media keys); the web host from `RoomEvent.Timeline`, `MatrixEventEvent.Decrypted`
  and `RoomEvent.Redaction`.
- **Outbound needed two commands.** `RtcCommandSender::send_room_event` (a plain
  message-like send, encrypted by the client SDK in an encrypted room) and
  `redact_event`, mirrored on the uniffi callback and the JS host object.

The media layer merges the result onto the roster: `Participant.hand_raised_at_ms`
plus `CallEvent::HandRaised` / `HandLowered` / `Reaction`, so a UI can order
tiles by who asked first without touching the core.

### Slots and the join conditions

`m.rtc.slot` is modelled in `slot.rs` and resolved to `SlotState::Open`/`Closed`
per MSC4143: open requires `status = "open"` plus an application whose `type`
agrees with the state key, and anything else — a closed status, a missing
application, empty content, a status from a future revision — is closed.

A session keeps its member events as *candidates* and projects the joined set
from them, so the conditions are re-evaluated whenever their inputs move rather
than only at ingestion. Closing a slot therefore leaves everyone in it, and
reopening restores whoever is still sticky. The projection also drives key
distribution, so a member who drops out of the joined set stops receiving keys.

The two room-state conditions are only enforced once a host supplies the state:

- `RtcSessionManager::on_room_slots_received` takes a room's complete slot state.
  Calling it is what switches the room from "unknown" (condition unevaluable, so
  unenforced) to enforcing; a slot absent from that call is closed, not unknown.
- `RtcSessionManager::on_room_members_received` supplies the room's joined users.

This is deliberate: enforcing an unevaluable condition would silently empty every
session for hosts that do not yet feed room state. The Matrix bridge feeds both.

`open_slot` / `close_slot` send the state event through the command sender's new
`send_state_event`.

### Encryption negotiation

Whether RTC data is encrypted is prescribed by the slot, not chosen locally.
`RawSlotEvent::resolve` takes the room's encryption state alongside the event,
because MSC4143 ties the two together in both directions:

- **Encrypted room.** A slot MUST carry an `encryption` object, so one without it
  resolves closed. A mechanism this client cannot implement also closes the slot,
  since encryption is required there and taking part without it would break the
  same requirement. `m.per_member` (and its unstable id) is the only one
  implemented.
- **Unencrypted room.** RTC encryption MUST NOT be used, so a declared mechanism
  is dropped rather than honoured. The slot stays open; `OpenSlot::mechanism` is
  `None` while `OpenSlot::encryption` still reports what was declared, so callers
  can see the mismatch.
- **Unknown.** Neither rule applies and the declared mechanism is taken at face
  value, matching how the other room-state conditions stay unenforced until a
  host opts in via `on_room_encryption_received`.

`RtcSession::negotiated_encryption` turns that into the key-management decision
at join time, overriding `EncryptionConfig::manage_media_keys`; the local flag
only applies where there is no slot state to negotiate from. A slot whose
mechanism changes mid-session is not renegotiated — the dangerous direction, the
slot closing, is already covered because that leaves every member.

Separately, a member event that arrived in the clear does not count as joined in
an encrypted room. That and the sending device are one value,
`RawStickyEvent::origin` (`EventOrigin`), because both come from the same
decryption metadata — a cleartext event cannot carry a sending device, and the
type makes that unrepresentable. `EventOrigin::Unknown` is distinct from
`Cleartext`: it means the host did not report, so the rule is skipped rather
than failed. The flat `sender_device_id` / `was_encrypted` pair survives only in
the wasm and FFI wire records, which converge into the enum at the boundary.

### Transports and who chooses them

Transport discovery is deliberately **not** in the core. Which transport to
publish on is an application decision: the app calls
`GET /_matrix/client/v1/rtc/transports` itself and passes the result into
`join`. The core has no HTTP of its own, and adding a fetch would have meant
every host implementing one whose result it then handed back to itself.
The `e2e_call` integration test (`crates/matrix-rtc-livekit/tests/e2e_call/`)
shows the pattern, using ruma's `api::client::rtc::transports` endpoint and
falling back to a configured URL where the homeserver has not implemented it.

What the core does model is the *intent*, via `TransportIntent`:

- `Publish(transport)` — publish on this transport, and advertise its type as
  `can_subscribe`.
- `ReceiveOnly { can_subscribe }` — publish nothing. MSC4143 puts no REQUIRED
  marker on `transports`, so a member that only receives — a recorder, an
  observer — is a valid participant rather than a broken one. Stating
  `can_subscribe` still matters, since that is what tells other members which
  transport to publish on so this one can hear them.

Still outstanding:

1. **Prompt reaction to slot changes** — the Matrix bridge re-reads room state
   on sticky-event ticks, so a slot closing in an otherwise idle room is noticed
   late. A room-state subscription would fix it.
2. **Mid-session renegotiation** — a slot that changes its encryption mechanism
   while a session is live keeps the mechanism negotiated at join.
3. **Slot state comes from a server fetch, not the store** — sliding sync only
   delivers state types listed in `required_state`, and the SDK's room-list
   defaults do not include the MSC4143 slot type, so the local store reports
   every room as slotless (which the core reads as "slot closed, everyone
   left"). The bridge therefore fetches `GET /rooms/{id}/state` on each tick
   and skips the update when the fetch fails. The real fix is adding the slot
   type to the fork SDK's sliding sync `required_state`, then reverting
   `slot_snapshot` to the state store.
4. **A unified `CallEvent` stream on the `Call` facade** — landed as
   `matrix-rtc-media::CallEvent` via `Call::subscribe_call_events` (peer
   joined/left, stream started/stopped, key imported, connection health,
   ended-with-reason). Remaining: migrate the e2e test and examples off the
   raw `Call::events`/`Call::session` accessors and delete them, and surface
   slot-close (item 1) as `CallEvent::Ended`.

## Logging

Every crate emits through the [`log`] facade — as do `livekit` and `libwebrtc`, so one
`log::Log` implementation captures the SFU and WebRTC stacks too. Nothing is visible
until a binding installs that implementation; hosts must do this first or the SDK is
silent:

| Binding | Entry point | Destination |
|---|---|---|
| `matrix-rtc-ffi` | `setup_logging(RtcLogConfig, Option<Arc<dyn RtcLogSink>>)` | logcat on Android (tag `matrix-rtc`), stderr elsewhere, and/or a host `RtcLogSink` |
| `matrix-rtc-wasm` | `initLogging(level, filter)` | the JS console |

Both take the same `RUST_LOG` filter syntax, e.g.
`"matrix_rtc_core::session=trace,livekit=info,webrtc_sys=warn"`. Hosts can push their own
lines into the stream with `log_event` / `logEvent` so app and SDK logs interleave in one
timeline, and dump current state with `debug_snapshot()` / `debugSnapshot()`.

**Conventions.**

- **Targets are module paths** (the `log` default — no explicit `target:`). The filterable
  roots are `matrix_rtc_core`, `matrix_rtc_media`, `matrix_rtc_livekit`, `matrix_rtc_ffi`,
  plus third-party `livekit` and `webrtc_sys`.
- **Session-scoped lines are prefixed `[{room_id}/{slot_id}]`.** `RtcSession` carries a
  pre-formatted `log_tag` for this, set by `RtcSessionManager` from its `SessionKey` — a
  session does not otherwise know which slot it belongs to.
- **Levels.** `error` = broken invariant. `warn` = recoverable or protocol-deviant.
  `info` = lifecycle milestones only (a whole call should produce a few dozen). `debug` =
  one line per decision: sticky event ingested, membership diff, slot resolved, key
  received, command sent. `trace` = hot paths and payloads — keep-alive ticks, per-frame
  work, event content JSON.
- **Never logged:** key material, LiveKit JWTs, OpenID tokens. `token.rs` logs a JWT's
  length, never the token. Event content JSON is `trace`-only because to-device messages
  carry keys.

**The membership-projection logs are the load-bearing ones.** A member silently vanishing
from the roster is the hardest failure to diagnose from the outside, so
`RtcSession::join_condition` returns a `JoinCondition` reason rather than a `bool`, and
`refresh` logs both the joined/left diff and every excluded candidate with its reason
(`SlotClosed`, `UnencryptedInEncryptedRoom`, `SenderNotInRoom`). `debug_snapshot()`
reports the same per-candidate verdicts as JSON.

## Non-goals in this first skeleton

- No dependency on `ruma` in core.
- No persistence/storage layer.
- No to-device processing.
- No transport integration (`MSC4195`) yet.
- No production-ready ABI/error model yet.

## Next increments

1. Add a richer membership schema validation layer aligned with MSC field requirements.
2. Introduce explicit machine outputs (commands/events) to communicate with host clients.
3. Add persistence abstraction for sessions and sticky membership maps.
4. Add transport discovery and focus modeling (`MSC4195`).
5. Model `transports.published` / `can_subscribe` in sticky membership DTOs and membership projections (`MSC4143` / `MSC4195`).
