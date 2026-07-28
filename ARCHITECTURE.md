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

Still outstanding, in the order they are planned:

1. **Key targeting hardening** — `receive_key` still ignores the sender/device it
   is handed, so the MUSTs around matching them to the member event, and around
   discarding cleartext keys, are unimplemented.
2. **Slots** — `m.rtc.slot` is not modelled at all. That blocks the MSC4143 join
   conditions (an open slot must exist, the sender must be joined to the room),
   eviction on slot close, and the `status` / `encryption` fields the slot event
   gained.
3. **Encryption negotiation** — the mechanism should come from the slot's
   `encryption.type` plus room encryption rather than a local config flag. The
   to-device key message also still sends `version: "0"` where the spec now says
   `format: 0`, and the MSC4153 cross-signing check is missing.
4. **Transport discovery** — `GET /_matrix/client/v1/rtc/transports` is not
   implemented; LiveKit URLs come from configuration.

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
