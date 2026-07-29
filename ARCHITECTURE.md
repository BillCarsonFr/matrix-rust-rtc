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
  hash derivations (`identity`), drives a LiveKit `Room` (`session`, subscribe-only
  for now), and bridges core media keys toward LiveKit frame encryption (`keys`).
- Obtains the Matrix OpenID token via the `OpenIdTokenSource` trait; a default
  `matrix_sdk::Client` impl sits behind the optional `matrix-sdk` feature, so the
  crate is not hard-wired to a particular Matrix SDK.
- Native-only by nature (the LiveKit client pulls in `libwebrtc`); never targets wasm.
- E2EE frame encryption is deferred: the LiveKit Rust SDK lacks the per-participant
  HKDF key import MSC4195 specifies (livekit/rust-sdks#796), so media is currently
  unencrypted and the key bridge only records signalled material.

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
  event's decryption metadata and rides on `RawStickyEvent::sender_device_id`,
  which the LiveKit bridge fills from `EncryptionInfo::sender_device` — available
  since the SDK's "keep encryption info for sticky events" commit, which is why
  the pin moved.
- `member.id` is generated fresh per join (`generate_member_id`), as the spec
  requires; it is no longer derived from the user and device IDs.

Inbound `m.rtc.encryption_key` messages are checked before use. A key is only
stored and signalled once it has been matched against the sender's member event:

- The host reports how the message arrived via `KeyOrigin`, built from Olm
  decryption metadata. Cleartext messages are discarded, since nothing in the
  payload can be trusted to identify a sender.
- The to-device sender and its device must equal the sender and
  `sender_device_id` of the `m.rtc.member` event the message names, or the key is
  discarded. A key naming another room is discarded too.
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

Still outstanding, in the order they are planned:

1. **Encryption negotiation** — whether to encrypt, and with which mechanism,
   should come from the slot's `encryption.type` plus room encryption rather than
   a local config flag. `SlotState::Open` already carries the parsed
   `encryption` object for this.
2. **Transport discovery** — `GET /_matrix/client/v1/rtc/transports` is not
   implemented; LiveKit URLs come from configuration.
3. **Prompt reaction to slot changes** — the LiveKit bridge re-reads room state
   on sticky-event ticks, so a slot closing in an otherwise idle room is noticed
   late. A room-state subscription would fix it.

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
