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

## `crates/matrix-rtc-ffi`

- Exposes UniFFI objects and records for Swift/Kotlin consumers.
- Keeps FFI DTOs local to the crate and converts them into core DTOs.
- Preserves session subscription semantics through a polling subscription object.

## Spec alignment

- `MSC4143` (MatrixRTC): membership events represented by `m.rtc.member`.
- `MSC4354` (Sticky events): membership updates are received as sticky events.

Current implementation only establishes event intake and membership state wiring; protocol completeness is intentionally deferred.

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
5. Model `rtc_transports` in sticky membership DTOs and membership projections (`MSC4143` / `MSC4195`).




