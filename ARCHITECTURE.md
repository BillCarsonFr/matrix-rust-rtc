# Architecture

This document explains the initial architecture of the Matrix RTC Rust workspace.

## Why this structure

The goal is to keep protocol logic in one Rust core crate and make all platform adaptation explicit at the edges.

- `matrix-rtc-core` owns RTC domain behavior.
- `matrix-rtc-wasm` owns JavaScript-facing conversion and wasm export details.
- `matrix-rtc-ffi` owns native binding-facing conversion and UniFFI boundary types.

This keeps the core reusable and testable while avoiding platform-specific dependencies in core.

## High-level data flow

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

## `crates/matrix-rtc-media`

- The transport-agnostic media model: `Participant` roster keyed by
  `member_id`, `CallEvent` (the unified membership + media event stream),
  owned frame types (`AudioFrame` PCM, `VideoFrame` I420), and per-stream
  `MediaConstraints` (visibility, rendered size, quality cap → subscribe-side
  simulcast control).
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
  libwebrtc, fully unit-testable (`FakeTransport`). Everything is `Send`:
  the only core input is the membership `watch` channel, so the core's
  `?Send` command futures never constrain the media side.
- Design/feasibility notes and the phased plan:
  `agent-workspace/media-abstraction/PLAN.md`.

## `crates/matrix-rtc-wasm`

- Exposes `WasmRtcSessionManager` to JavaScript.
- Accepts `JsValue` payloads for snapshots and updates and deserializes via `serde-wasm-bindgen`.
- Maps JSON fields to core sticky event DTOs.

## `web`

- Browser-first JavaScript packaging around `crates/matrix-rtc-wasm`.
- Uses `wasm-pack` to generate browser and Node.js runtime bundles into ignored `pkg/` subdirectories.
- Keeps generated JavaScript/WASM artifacts out of git while providing a small JS test surface.

## `crates/matrix-rtc-ffi`

- Exposes UniFFI objects and records for Swift/Kotlin consumers.
- Keeps FFI DTOs local to the crate and converts them into core DTOs.
- Preserves session subscription semantics through a polling subscription object.

## `crates/matrix-rtc-livekit`

- Implements the MSC4195 LiveKit transport: the "LiveKit SDK" layer that turns
  `matrix-rtc-core`'s membership/key outputs into a live SFU media session.
- Owns the authorisation-service `/get_token` exchange (`token`) and the MSC4195
  hash derivations (`identity`), drives a LiveKit `Room` (`session`), and bridges
  core media keys into LiveKit per-participant frame encryption (`keys`,
  `MediaKeyBridge` → `KeyProvider`, HKDF mode, GCM frames).
- Obtains the Matrix OpenID token via the `OpenIdTokenSource` trait; a default
  `matrix_sdk::Client` impl sits behind the optional `matrix-sdk` feature, so the
  crate is not hard-wired to a particular Matrix SDK.
- Implements `matrix-rtc-media`'s transport traits in `transport_impl`
  (`LiveKitMediaTransport`): connection key = `livekit_service_url`, remote
  identity = MSC4195 pseudonymous identity, `RoomEvent` → `ConnectionEvent`
  translation, and `NativeAudioStream` → owned PCM frame streams behind
  `RemoteTrackHandle`.
- Behind `matrix-sdk` it also ships the integration layers: `matrix_bridge`
  (SDK-backed `RtcCommandSender` + the sticky/room-state bridge into the core)
  and `call` — a `Call::join`/`Call::leave` facade wrapping membership
  signalling, key exchange, transport discovery, the E2EE SFU connection, and
  a `CallEngine` in one handle (the crate README's quick start; also what the
  e2e test drives). `Call::subscribe_call_events`/`Call::participants` are the
  transport-agnostic surface; the raw `Call::events`/`Call::session` accessors
  remain during the transition.
- Native-only by nature (the LiveKit client pulls in `libwebrtc`); never targets wasm.

## Spec alignment

- `MSC4143` (MatrixRTC): membership events represented by `m.rtc.member`.
- `MSC4354` (Sticky events): membership updates are received as sticky events.

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
  LiveKit bridge fills from `EncryptionInfo` — available since the SDK's "keep
  encryption info for sticky events" commit, which is why the pin moved.
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
session for hosts that do not yet feed room state. The LiveKit bridge feeds both.

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

1. **Prompt reaction to slot changes** — the LiveKit bridge re-reads room state
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
