# Architecture merge plan: `MatrixSdkArchitectureDraft` becomes the RTC layer

Status: proposal, 2026-09-15. Written against `toger5/architecture` at `a095ba7`
(draft) and the workspace crates as of the same commit.

This document answers three questions and then lays out the transition:

1. Is `matrix-rtc-ffi` call-specific or MatrixRTC-generic?
2. Is it valid for `MatrixSdkArchitectureDraft` (crate `matrix-rtc`) to become the
   one RTC layer, replacing `matrix-rtc-core`, `matrix-rtc-bridge` and
   `matrix-rtc-wasm`?
3. Should `matrix-rtc-livekit`, `matrix-rtc-ffi` and `matrix-rtc-media` be renamed
   to `matrix-call-*`?

Hard constraint throughout: **the public API of `matrix-rtc-ffi` stays the same**
(every UniFFI object, record, enum, error, callback interface and free function,
the namespace `matrix_rtc_ffi`, and the artifact names `libmatrix_rtc_ffi.*`).

---

## 1. Findings

### 1.1 What exists today

| Crate | Lines | Role | Fate in this plan |
|---|---|---|---|
| `matrix-rtc-core` | 12.5k | Sessions, own-membership dead-man's-switch, per-member key exchange, slots, MSC4075 send | **Replaced** by `matrix-rtc` (draft) |
| `matrix-rtc-bridge` | 4.4k | `compat/` (pre-2026 Element Call dialects, pure JSON) + `sdk.rs` (`SdkCommandSender`, `run_sticky_bridge` over the matrix-sdk fork) | `compat` **replaced** (draft does it in place); `sdk.rs` **re-homed** as a `MatrixDriver` impl |
| `matrix-rtc-wasm` + `web/` | 4.0k + JS | wasm-bindgen JS API + `MatrixRtcCall`/`MatrixHost` JS layers | **Replaced** by the draft's uniffi-bindgen-react-native (ubrn) bindings, which Element Call already vendors |
| `matrix-rtc-livekit-proto` | 0.5k | MSC4195 identity hash + `/get_token` shapes, wasm-clean | **Dissolved**: identity is in draft `connections`; token shapes fold into the livekit crate |
| `matrix-rtc-media` | 5.2k | `CallEngine` (roster, multi-focus pool, constraints, frames), `MediaKeyHandler` | **Kept**, re-based on `matrix-rtc` outputs, renamed `matrix-call-media` |
| `matrix-rtc-livekit` | 3.0k + examples/e2e | LiveKit media transport, token IO, key bridge, `Call` facade (Rust-driven topology) | **Kept**, re-based, renamed `matrix-call-livekit` |
| `matrix-rtc-ffi` | 6.4k | UniFFI surface for Swift/Kotlin: slim signalling API + `media` feature | **Kept as-is on the outside**, reimplemented as a facade over `matrix-rtc`; renamed `matrix-call-ffi` at the end with library/namespace names preserved |
| `MatrixSdkArchitectureDraft` (`matrix-rtc`) | 11.5k + 1.5k tests | Participation manager over a host-implemented `MatrixDriver`; one uniffi surface for Swift/Kotlin/RN/web | **Becomes `crates/matrix-rtc`**, the first layer |

Draft implementation status: no `todo!()` left in code (the plan documents that
still say so are stale), 260 Rust tests plus 23 black-box facade tests plus 37
vitest suites through the real wasm bindings, one opt-in test against the docker
backend. Not yet exercised: real media (the backend test is unencrypted and has
no SFU media), and the Element Call interop suite.

### 1.2 Answer 1: is `matrix-rtc-ffi` call-specific?

**Not as a whole. It is two surfaces in one crate, split exactly along the
`media` cargo feature.**

Generic MatrixRTC (default build):

- `RtcSessionManagerHandle` and everything it takes and returns
  (`StickyEvent`, `SlotEvent`, `JoinedMembership`, `FfiJoinSessionParams`,
  `FfiEncryptionConfig`, `FfiReceivedEncryptionKey`, `MembershipSnapshotSubscription`).
  `application` and `slot_id` are free strings
  (`crates/matrix-rtc-ffi/src/commands.rs:172-175`); `open_slot` takes the
  application type from the caller (`src/lib.rs:484`). No `"m.call"` literal exists
  in non-test FFI code.
- `CommandSenderCallback` is generic Matrix I/O: sticky, state, delayed, to-device.
- `FfiElementCallCompat` and the raw compat funnels (`src/compat.rs`) are Element
  Call specific by name but generic in mechanism.
- Logging.

Call/LiveKit-specific:

- The whole `media` module: `MediaSession`, `connect_media_session`,
  `OpenIdTokenProvider`, frames, `FfiStreamKind` (microphone/camera/screenshare),
  `FfiCallEvent`, `FfiReceiveStats`, libwebrtc JNI bootstrap.
- Three leaks into the slim surface: `FfiRtcTransport::LiveKit { livekit_service_url }`
  and `FfiTransportConfig.livekit_service_url` (`src/lib.rs:217-223`,
  `src/commands.rs:87-93`, `into_core` hard-codes `"livekit"` at `:250`);
  `FfiNotifyConfig` with `Ring` and an MSC4196 `intent` that the core writes as the
  literal key `m.call.intent` for every application type
  (`crates/matrix-rtc-core/src/notification.rs:210,219`); and
  `FfiElementCallCompat::StateEvents`, which implies MSC3401 and the `/sfu/get`
  endpoint.

Conclusion: the slim surface is a generic RTC signalling API with LiveKit-shaped
transport DTOs; the `media` feature is the call. The draft's own `uniffi_api`
(namespace `matrix_rtc`) is the generic successor of the slim surface, with a
different shape (driver instead of push methods). After hosts migrate to it, what
remains in `matrix-rtc-ffi` is the compat shell plus the media surface, which *is*
call-specific. That is the point at which renaming the crate is honest.

### 1.3 Answer 2: is "the draft becomes the first layer" valid?

**Yes.** The draft already covers what `core` + `bridge/compat` + `wasm` do, with
a cleaner seam (one `MatrixDriver` trait family instead of `RtcCommandSender` +
push-in methods + per-binding JS/FFI conversion), and it adds what the old stack
lacked (connections/token management, structured `Status` with impairments,
`LeftWithKeys`, per-tile key state). Element Call is already integrating against
its web bindings (`web-test-app/ubrn.element-call.config.yaml`).

Parity gaps that must be closed before the old crates can go (all are bounded):

| # | Gap | Where it is today | What the draft needs |
|---|---|---|---|
| D1 | Own-transport token minting is mandatory on `join` (`connections/mod.rs:512-520`); a `TokenRefused` fails the join | Old core never minted; the media layer minted its own | A policy so that `DriverError::Unsupported` from `get_livekit_token` for our *own* transport records the transport without a token (surfaces as `ConnectionProblemKind::NoToken` + impairment) instead of failing the join. Needed for the FFI facade in the slim build |
| D2 | MSC4075 `m.rtc.notification` send has no home (`own_membership/OwnMembershipImplementationPlan.md:726`) | `core/src/notification.rs`, sent in `RtcSession::join` when `notify` is set and nobody else is in the roster | Port `notification.rs` (content builder, "only the starter notifies" rule, `element_call_compat` rewrite) into `own_membership`, keyed off the join event id the `JoinPlan` already keeps. Add `JoinParams::notify` |
| D3 | Sticky events without `msc4354_sticky.duration_ms` or `unsigned…ttl_ms` are not admitted to the map (`session/sticky.rs:98`) | Old `StickyEvent` record has no duration and relies on full-replace semantics | Either the FFI facade synthesises a duration and emits removals on diff (see §3.3), or the draft gains a `replace_sticky_state(room, events)` inlet on `RoomEventSink`. Recommend the facade approach first, inlet only if the diffing proves fragile |
| D4 | No `matrix-sdk`-backed driver | `bridge/sdk.rs` (`SdkCommandSender`, `run_sticky_bridge`) | A `MatrixDriver` impl over `matrix_sdk::Room` (new crate, §2.2). Reuses `bridge/sdk.rs` request code and the fork pin |
| D5 | `open_slot(application_type, encrypted: bool)` cannot express `FfiSlotEncryption::Other { encryption_type }` | Old FFI accepts it | Either extend `open_slot` to take the mechanism string, or the facade returns `InvalidInput` for `Other`. Recommend extending (one string) |
| D6 | `FfiEncryptionConfig.key_rotation_grace_period_ms` has no counterpart; the draft derives grace from the shared contingent (`SendMachineConfig`) | Old core used a fixed grace period | Accept the field, log that it is ignored, document in CHANGELOG. Do not port the old policy |
| D7 | `send_delayed_event` carries `sticky_duration_ms` so the delayed leave clears the sticky entry | Old `CommandSenderCallback::send_delayed_event` has no such parameter | Facade drops it (today's behaviour, already a documented limitation in `CHANGELOG.md`). New-API hosts get the fix for free |
| D8 | Two `KeyRejection` vocabularies (`encryption::KeyRejection` 9 variants vs core's) | `FfiKeyRejection` in `media/types.rs` mirrors core's | Map draft variants onto the existing FFI enum; add variants only if lossless mapping is impossible (that would be an additive API change, acceptable) |

None of these changes the draft's architecture. D1 and D2 are the only ones with
behavioural weight.

### 1.4 Answer 3: rename to `matrix-call-*`?

**Yes for `media`, `livekit` and (at the end) `ffi`; no for anything that ends up
in `matrix-rtc`.** The naming rule this plan adopts:

- `matrix-rtc-*` = participating in an MSC4143 session for *any* application type.
  After the merge that is exactly one Rust crate, `matrix-rtc`, plus its
  generated bindings, plus the matrix-sdk driver crate.
- `matrix-call-*` = the `m.call` application: media engine, LiveKit media transport,
  the native media FFI, recording/load bots.

Per crate:

- `matrix-rtc-media` → **`matrix-call-media`**. Its vocabulary is `CallEngine`,
  `CallEvent`, microphone/camera/screenshare, audio/video frames. Nothing in it
  serves a non-call application.
- `matrix-rtc-livekit` → **`matrix-call-livekit`**. Today it also holds MSC4195
  control-plane logic (token exchange, identity hash, delegation) which is
  RTC-generic. After the merge that logic lives in `matrix-rtc::connections` and
  the `TokenDriver`, so what remains is the LiveKit *media* transport, the frame
  key bridge and the `Call` facade. The rename becomes accurate only after §4
  phase 3, so do it then.
- `matrix-rtc-ffi` → **`matrix-call-ffi`**, cargo package name only, in the last
  phase. Keep `[lib] name = "matrix_rtc_ffi"` and pin the namespace with
  `uniffi::setup_scaffolding!("matrix_rtc_ffi")` so `libmatrix_rtc_ffi.so`,
  `uniffi.matrix_rtc_ffi.*` (Kotlin) and the Swift module name do not move.
  Renaming those is an API break for every host (`System.loadLibrary`, imports,
  the hand-written `MatrixRtc.kt` which names generated vtables such as
  `uniffiCallbackInterfaceCommandSenderCallback`).
- `matrix-rtc-livekit-proto` → dissolved (see §2.2).
- `matrix-rtc-wasm` / `web` → replaced by the draft's ubrn output; the npm package
  is the RTC layer, so it keeps an rtc name (suggest `@element-hq/matrix-rtc`, to
  be confirmed with what Element Call's `sync-matrix-rtc-sdk.sh` expects).

---

## 2. Target architecture

### 2.1 Layers

```
 host app: Element X (Kotlin/Swift) · Element Call (web) · Rust bots
        │ implements MatrixDriver              ▲ memberships · connections · key map · status
        ▼                                      │
┌──────────────────────────────────────────────┴───────────────────────────────┐
│  matrix-rtc                (crates/matrix-rtc, was MatrixSdkArchitectureDraft) │
│  session · own_membership · connections · encryption · participation          │
│  driver traits · executor · uniffi_api (namespace matrix_rtc)                  │
└───────────────┬──────────────────────────────┬───────────────────────────────┘
                │                              │
   ┌────────────▼───────────┐     ┌────────────▼──────────────────────────────┐
   │ matrix-rtc-sdk         │     │ matrix-call-media                          │
   │ MatrixDriver over      │     │ CallEngine over SessionMembership +        │
   │ matrix_sdk::Room       │     │ ConnectionWithMembers + KeyMap             │
   │ (fork pin lives here)  │     └────────────▲──────────────────────────────┘
   └────────────▲───────────┘                  │
                │                 ┌────────────┴──────────────────────────────┐
                │                 │ matrix-call-livekit                        │
                └─────────────────┤ LiveKit media transport · frame key ring ·│
                   (feature       │ Call facade (Rust-driven topology)        │
                    matrix-sdk)   └────────────▲──────────────────────────────┘
                                               │ feature "media"
                                  ┌────────────┴──────────────────────────────┐
                                  │ matrix-call-ffi  (lib matrix_rtc_ffi)      │
                                  │ RtcSessionManagerHandle facade (unchanged │
                                  │ API) · MediaSession · exports BOTH uniffi │
                                  │ namespaces: matrix_rtc_ffi + matrix_rtc   │
                                  └───────────────────────────────────────────┘
```

Dependency direction is strictly downward. `matrix-rtc` depends on nothing in the
workspace, and must stay that way: its wasm build goes through the ubrn shim
crate, whose edition-2018 feature resolver unifies target-specific features, so
any native-only feature reachable from `matrix-rtc` breaks the web build (see
`MatrixSdkArchitectureDraft/Cargo.toml:31-39` and the memory note on this).

### 2.2 Crate decisions

**`crates/matrix-rtc`** — the draft moved into the workspace. Changes: drop its
empty `[workspace]` table, join `[workspace.members]`, take `serde`/`thiserror`/
`log`/`async-trait` from `[workspace.dependencies]`. Keep its own tokio feature
list (`sync, rt, time, macros`) and its own `uniffi` pin; the workspace pin moves
to match (§4 phase 0).

**`crates/matrix-rtc-sdk`** (new, small) — `SdkMatrixDriver: MatrixDriver` over
`matrix_sdk::Room` and `Client`. Built from `bridge/sdk.rs`: the delayed/state/
sticky/to-device request code, `delayed_command_error` (maps `M_UNRECOGNIZED`/
`M_FORBIDDEN` to `DriverError::Unsupported`), the three inbound streams from
`subscribe_to_sticky_events` / `subscribe_to_updates` / to-device handlers,
`read_state` via `GET /rooms/{id}/state` (the sliding-sync `required_state`
caveat in `ARCHITECTURE.md` still applies), `get_rtc_transports` via ruma's
`rtc/transports` endpoint with the configured fallback, and `get_livekit_token`
via reqwest + `client.get_openid_token()` using the request/response shapes from
`livekit-proto/token.rs`. This is the crate that owns the matrix-sdk fork pin and
the `[patch.crates-io]` block. It is RTC-generic and the natural candidate to
upstream into matrix-rust-sdk later, which is why it is not folded into the
livekit crate. Not part of any binding artifact.

**`crates/matrix-call-media`** (was `matrix-rtc-media`) — same engine, different
inputs. Replace every `use matrix_rtc_core::…` (`JoinedMembership`, `RtcTransport`,
`KeyMaterialSignal`, `EncryptionKeySignalHandler`, `DiscardedKey`, `KeyRejection`,
`MaybeSend`) with `matrix_rtc` types:

- Roster input: `Vec<SessionMembership>` (via `on_memberships_change`) instead of
  `watch::Receiver<Vec<JoinedMembership>>`. `SessionMembership` already carries
  `connections` (service URLs) and `transport_identity`, so
  `MediaTransport::remote_identity()` and the `RtcIdentityMapper` seam are deleted:
  the identity derivation has exactly one owner, `matrix_rtc::connections`.
- Connection input: `ConnectionWithMembers` (service URL, ws URL, JWT, expiry)
  instead of the engine minting tokens through `MediaTransport::connect`. The pool
  still owns backoff, idle grace and reconnect, but `connect` receives the token.
- Key input: `KeyMap` + `MediaKeyChange` (via `on_key_map_change`) feeding
  `FrameKeyRing::set_key(identity, index, key)`. The `delayBeforeUse` wait and the
  local sender index switch move out of `MediaKeyHandler`: the draft's send
  machine already delays `UseOwnKey` and mirrors our own in-use key into the
  `KeyMap`, so the media layer just applies what it is given. Delete the
  `SwitchCompleteListener` → `flush_due_key_rotation` loop and `LocalKeyIndexHook`.
- `KeyDiscarded`/`FrameEncryptionState` events keep working from `on_key_rejected`
  and the per-tile `media_key` state.

**`crates/matrix-call-livekit`** (was `matrix-rtc-livekit`) — `transport_impl.rs`,
`keys.rs`, `session.rs` adapt to the media crate's new traits. `token.rs` absorbs
`livekit-proto/token.rs` (request builder + response parser + reqwest IO) and is
used by `matrix-rtc-sdk`'s `get_livekit_token`; `identity.rs` is deleted in favour
of `matrix_rtc::connections::participant_identity` (the MSC4195 appendix test
vector moves with it). `call.rs`'s `Call::join` becomes: build `SdkMatrixDriver`,
`ParticipationManager::new`, `join(intent, params)`, then wire the three callbacks
into a `CallEngine` + `LiveKitMediaTransport`. This removes the second copy of the
"join a slot and attach media" wiring that `ARCHITECTURE.md` flags as a known
defect. The four examples and the `e2e_call` test move with it.

**`crates/matrix-call-ffi`** (was `matrix-rtc-ffi`) — see §3.

**`web/`** — replaced by `MatrixSdkArchitectureDraft/web-test-app` moved to `web/`:
the ubrn bindings, the `jsSdkDriver.ts`, the acceptance suites, the demo. The
livekit-client integration that `web/src/matrix-rtc-call.mjs` provided becomes a
small TS layer implementing the reconciliation loop in
`MatrixSdkArchitecture.md` "Usage" (LK rooms keyed by `service_url`, keys by
`transport_identity`). It is call-specific and lives beside the demo, not in the
bindings package. The Playwright web peer (`interop/tests/web-peer-*.spec.ts`)
drives this layer.

**Deleted**: `crates/matrix-rtc-core`, `crates/matrix-rtc-bridge`,
`crates/matrix-rtc-wasm`, `crates/matrix-rtc-livekit-proto`, `web/src/*.mjs`,
`web/scripts/build-bindings.sh`.

---

## 3. Preserving the `matrix-rtc-ffi` public API

### 3.1 Strategy

`matrix-rtc-ffi` keeps every exported item and its signature. Internally,
`RtcSessionManagerHandle` becomes a facade over `matrix_rtc`:

- One `CompatDriver` per `RtcSessionManagerHandle`, implementing
  `matrix_rtc::driver::MatrixDriver`. Outbound methods forward to the host's
  `CommandSenderCallback` (same seven methods, same JSON-string payloads; the
  `wire_event_type` translation stays on the facade side). Inbound streams are fed
  by the handle's push methods. `read_state`/`read_events` answer from the
  facade's per-room cache of what the host last pushed.
- Per `(room_id, slot_id)`: a live `matrix_rtc::Session` for observation before any
  join (`member_count`, `subscribe_membership_snapshots` work for sessions we
  never join, and the draft's `ParticipationManager::new` needs an `OwnIdentity`
  which the old API only supplies at `join`), and a `ParticipationManager` created
  at `join` with the identity from `FfiJoinSessionParams` and the per-join
  `ParticipationConfig` (`compat`, `encryption`, `rotation`).
- The `media` module keeps `connect_media_session(manager, config, token_provider)`
  and reads the manager's `ParticipationManager` for that `(room, slot)`: its
  `memberships()`/`connections()`/`key_map()` outputs feed the `CallEngine`.

The FFI binary also carries the draft's `uniffi_api` (feature `uniffi` of
`matrix-rtc`, on by default in the FFI). uniffi library-mode bindgen, which the
build scripts already use (`--library …/libmatrix_rtc_ffi.so`), emits one Kotlin
package / Swift module per namespace, so one AAR/xcframework exposes both
`uniffi.matrix_rtc_ffi` (unchanged) and `uniffi.matrix_rtc` (new). Hosts migrate
one surface at a time. The facade never passes `matrix_rtc` uniffi types across
its own surface, so there are no cross-namespace external types to declare.

### 3.2 Method-by-method mapping

| `RtcSessionManagerHandle` (unchanged) | Facade behaviour on `matrix_rtc` |
|---|---|
| `set_command_sender(cb)` | Store in `CompatDriver`; all `OwnMembershipDriver`/`ToDeviceSendDriver` calls forward to it |
| `set_current_sticky_state(room, Vec<StickyEvent>)` | Reverse-build one `m.rtc.member` JSON event per record (fields → `member.id`, `msc4354_sticky_key`, `slot_id`, `application.type`, `membership`, `leave_reason`, `transports` from `transports_json`), attach `EventOrigin` from `sender_device_id`/`was_encrypted`, synthesise `msc4354_sticky.duration_ms` (D3), emit into the room sink; diff against the previous full set and emit a sticky **removal** (content with only the sticky key) for every key that vanished. Cache as the answer to `read_events` |
| `set_current_membership(room, raw, legacy_state)` | Raw member events emit as-is into the room sink; legacy `org.matrix.msc3401.call.member` state emits into the state sink. Same diff/removal rule for the sticky half |
| `on_room_slots_received(room, slots)` | Emit synthesised `m.rtc.slot` state events into the state sink; cache for `read_state`. Before the first call, `read_state(m.rtc.slot)` returns `DriverError::Other` so the draft records `failed_reads` and leaves the slot condition unenforced, matching today's "unknown until fed" |
| `on_room_members_received(room, user_ids)` | Emit synthesised `m.room.member` join state; cache |
| `on_room_encryption_received(room, encrypted)` | Emit synthesised `m.room.encryption` state (or its absence); cache |
| `open_slot(room, slot, app, encryption)` / `close_slot` | Needs a manager or a standalone slot sender. Use a small `matrix_rtc` helper that sends `m.rtc.slot` through a `MatrixDriver` without a manager (or construct a throwaway manager). D5 for `Other` |
| `receive_encryption_key(FfiReceivedEncryptionKey)` | Rebuild the `m.rtc.encryption_key` content (`room_id`, `member.id`, `keys[{index, key}]`), emit into the to-device sink with `origin` from `was_encrypted`/`sender_device_id` and `sender_cross_signed` |
| `receive_legacy_encryption_key(...)` | Emit `io.element.call.encryption_keys` with the given content JSON |
| `join(params) -> member_id` | Create the manager (identity from params, config from `element_call_compat`, `encryption_config`, D6 for the lossy field), `join(intent, JoinParams)`. `transport: None` → `ReceiveOnly { can_subscribe }`; `Some` → `Publish`. `notify` → D2. Return `own_member_id()`. Map `JoinError` → `MatrixRtcFfiError::InvalidInput(msg)` as today (the error type has two variants; keep it) |
| `leave(room, slot, params)` | `manager.leave(reason)`; drop the manager, keep the observing `Session` |
| `heartbeat(room, slot) -> bool` | The draft pumps its own keep-alive; return `true` iff `status()` is `Connected`/`Joining`, no other effect. Document the semantic change (the built-in driver already ran regardless of this call) |
| `own_member_id`, `member_count`, `session_count`, `debug_snapshot` | Direct |
| `subscribe_membership_snapshots(room, slot)` | A `watch` fed from `on_memberships_change`, filtered to `MembershipState::Joined`, mapped to the old `JoinedMembership` record (`sticky_key = member_id`, `sender = user_id`, `sender_device_id = device_id`, `transports`/`can_subscribe` from `MemberTransports`) |
| `connect_media_session(...)` and `MediaSession::*` | `CallEngine` over the manager's three outputs; `OpenIdTokenProvider` is installed into the `CompatDriver` so `get_livekit_token` works from then on (before that it returns `Unsupported`, D1). `local_identity()` = `own_transport_identity()` |

### 3.3 Semantic deltas to record in `CHANGELOG.md`

All API-preserving, all behavioural:

- Sticky expiry is now computed by the session from a synthesised one-hour
  duration plus the host's full-replace diff, not by the host alone. A host that
  stops calling `set_current_sticky_state` sees members expire after an hour where
  previously they stayed forever.
- `heartbeat()` is a no-op that reports joined-ness.
- `key_rotation_grace_period_ms` is accepted and ignored (D6).
- `join()` no longer fails when the homeserver refuses delayed events *and* it
  still degrades the membership lifetime — unchanged. It also no longer waits on
  anything token-related in the slim build (D1).
- Keep-alive failures, roster exclusion and key rejections that were previously
  invisible are now observable, but only through the new namespace
  (`FfiParticipationManager::set_status_listener`). The old surface exposes no new
  fields.

---

## 4. Transition phases

Each phase ends with a green gate. The gates reuse existing infrastructure:
`cargo test` per crate, `make test-e2e` (docker backend, `e2e_call`),
`make test-interop` (Playwright against real Element Call; the only proof the
wire formats are read the same way EC reads them), `./scripts/build-android-aar.sh`
and `./scripts/build-ios-xcframework.sh` (slim and `MEDIA=1`), `cd web && npm test`.

### Phase 0 — land the draft in the workspace (no behaviour change)

1. Commit the ~20 uncommitted draft files on `toger5/architecture`.
2. `git mv MatrixSdkArchitectureDraft crates/matrix-rtc`; delete its
   `[workspace]` table and `Cargo.lock`; add it to `[workspace.members]`.
3. Move `web-test-app` to `web-new/` (temporary name; becomes `web/` in phase 4).
   Add `exclude = ["web-new/rust_modules"]` to the root `Cargo.toml` so the ubrn
   shim crate is not treated as a workspace member. Point `ubrn.config.yaml`
   `rust.directory` at `../crates/matrix-rtc`.
4. Upgrade the workspace `uniffi` from `0.29` to `=0.31.2` (`matrix-rtc-ffi`,
   `uniffi-bindgen`), matching ubrn `0.31.0-5`. Regenerate Kotlin/Swift, diff the
   generated names that `mobile/android/matrixrtc/src/main/kotlin/org/matrix/rtc/MatrixRtc.kt`
   pins (`uniffiCallbackInterface*`), fix if they moved. Run both mobile build
   scripts.
5. CI: add `cargo test -p matrix-rtc --features uniffi`, the wasm32 check, and the
   `web-new` vitest job. Keep every existing job.

Gate: everything that was green is still green; the draft's own commands from its
README pass from the workspace root.

### Phase 1 — parity work inside `matrix-rtc` (D1–D6)

1. D1: own-transport token failure with `Unsupported` publishes the transport
   without a token; add a black-box test in `tests/participation.rs`.
2. D2: port `core/src/notification.rs` into `own_membership` with its six unit tests
   and the "only the starter notifies" test from `core/src/session.rs`.
3. D3: decide facade-diff vs `replace_sticky_state` inlet after prototyping the
   facade in phase 3; default is facade-diff.
4. D5: `open_slot` takes the mechanism as a string; D6: nothing to do in the draft.
5. New crate `matrix-rtc-sdk` (D4): `SdkMatrixDriver` built from `bridge/sdk.rs`.
   Its first test is the existing opt-in backend scenario from
   `web-new/test/backend.test.ts`, replayed from Rust against `make backend-up`.
6. Refresh the stale status tables in the draft's `*ImplementationPlan.md` and
   `encryption/README.md` so they stop claiming `todo!()`.

Gate: `cargo test -p matrix-rtc --features uniffi`, wasm32 check, `web-new` suites,
and the new `matrix-rtc-sdk` backend test.

### Phase 2 — the call layer moves onto `matrix-rtc`

1. `matrix-rtc-media` → depends on `matrix-rtc`, not `core`; trait changes from
   §2.2. Port the 51 engine/keys tests to the new inputs (the `FakeTransport`
   pattern survives; the key handler tests shrink because the delay logic is gone).
2. `matrix-rtc-livekit` → `transport_impl`, `keys`, `session` on the new media
   traits; absorb `livekit-proto/token.rs`; delete `identity.rs`; rewrite
   `call.rs` over `ParticipationManager` + `SdkMatrixDriver`; port the four
   examples and `tests/e2e_call`.
3. Delete `matrix-rtc-livekit-proto`.

Gate: `make test-e2e` (all four `e2e_call` scenarios, including two-foci),
`make test-interop` for the two Rust-peer specs (`ec-2024 state events`,
`ec-2025 sticky events`). This is the first time the draft's compat dialects meet
a real Element Call; expect fixes in `session::convert` and `own_membership::compat_*`.

### Phase 3 — `matrix-rtc-ffi` becomes a facade (API unchanged)

1. Implement `CompatDriver` and the per-`(room, slot)` `Session`/`ParticipationManager`
   map inside `RtcSessionManagerHandle` per §3.2. Enable `matrix-rtc/uniffi` so the
   new namespace ships alongside.
2. Port the 34 FFI tests; add facade tests for the sticky diff/removal rule, the
   "unknown until fed" slot behaviour, the `JoinedMembership` mapping, and the
   `receive_encryption_key` round trip.
3. `media/session.rs` over the manager's outputs; `make test-ffi-media`.
4. Mobile: both build scripts, slim and `MEDIA=1`; confirm generated Kotlin/Swift
   for namespace `matrix_rtc_ffi` is identical to phase 0 apart from doc comments
   (diff the generated files, that is the API-stability proof). Confirm
   `MatrixRtc.kt`'s vtable pinning still finds every callback interface, and add
   the new `MatrixDriverCallback` and listener vtables to it.
5. Drop `matrix-rtc-core` and `matrix-rtc-bridge` from the FFI's dependencies.

Gate: mobile builds, FFI tests, generated-binding diff, `make test-e2e`.

### Phase 4 — web

1. `web-new/` → `web/`. Add the livekit-client reconciliation layer for the demo
   and the Playwright web peer; port `interop/helpers/web-peer.ts` and the two
   `web-peer-*` specs to the new demo page.
2. Publish shape: one npm package from the ubrn output, name to be agreed with
   Element Call's vendoring script.
3. Delete `crates/matrix-rtc-wasm`, the old `web/src`, `web/test`, `web/demo`.

Gate: `cd web && npm test`, `make test-interop` including the web-peer specs.

### Phase 5 — delete, rename, document

1. Delete `crates/matrix-rtc-core` and `crates/matrix-rtc-bridge` (nothing depends
   on them after phase 3). Carry over anything still unique: `core/tests/key_rotation.rs`
   scenarios as draft `send_machine` tests if not already covered by
   `rotation_simulation.rs`; the `bridge/compat` unit fixtures that the draft's
   converters lack.
2. Rename packages: `matrix-rtc-media` → `matrix-call-media`, `matrix-rtc-livekit`
   → `matrix-call-livekit`, `matrix-rtc-ffi` → `matrix-call-ffi` with
   `[lib] name = "matrix_rtc_ffi"` and `uniffi::setup_scaffolding!("matrix_rtc_ffi")`.
   Update `scripts/build-*.sh` (`-p` flags only), `Makefile`, CI, `log` target
   roots in `mobile/README.md` (`matrix_rtc_core` → `matrix_rtc`, etc.).
3. Rewrite `ARCHITECTURE.md` (the draft's `MatrixSdkArchitecture.md` becomes its
   core, with the call layer appended), `README.md`, `AGENTS.md` layout section,
   `CHANGELOG.md` (§3.3 deltas plus a "new namespace `matrix_rtc`" entry).

Gate: full `make quality-check`, both mobile builds, `make test-e2e`, `make test-interop`.

---

## 5. Risks and open questions

- **uniffi 0.29 → 0.31.2** (phase 0). Generated Kotlin/Swift may change shape
  (async plumbing, callback vtable names). This is the only step that can
  accidentally alter the preserved API, which is why the phase-3 gate diffs the
  generated bindings. Mitigation: do the upgrade alone, in its own PR.
- **Two uniffi namespaces in one library.** Supported by library-mode bindgen, but
  the Android `pinCallbackThreads()` helper must learn the new vtables, and the
  slim/media Kotlin source-set split in `build.gradle` needs the new interfaces
  added to both.
- **Executor duplication.** `matrix-rtc` runs its own current-thread tokio runtime
  on a `matrix-rtc` thread; the FFI has its multi-thread runtime for uniffi async
  exports and media. Both are fine to coexist, but the facade must not block one
  on the other (no `block_on` across them). The draft's callbacks fire on its
  executor thread; `MediaSession` must hop to its own runtime before touching
  libwebrtc.
- **Sticky full-replace vs event-stream** (D3). The facade diff re-derives removals
  the draft would otherwise learn from the sticky map. If the host feeds partial
  sets (contrary to the documented contract) members will flicker. If prototyping
  shows this is fragile, add the `replace_sticky_state` inlet to `RoomEventSink`
  and use it from both the facade and the js-sdk driver.
- **Compat dialects meet real Element Call for the first time** in phase 2. The
  draft's converters were written from the old crate's fixtures and the spec, not
  against EC. Budget time for wire-format fixes there.
- **Media not exercised by the draft today.** Everything about `ConnectionData`
  (ws URL vs service URL, token expiry) and `transport_identity` is validated only
  in phase 2's e2e run. Keep `Call` in `matrix-call-livekit` as the Rust-side
  proving ground before the FFI facade depends on the same outputs.
- **Two integrators, two APIs, one binary.** The mobile integrator keeps
  `matrix_rtc_ffi`; Element Call already uses `matrix_rtc` (web). Decide when the
  old namespace is deprecated; this plan does not remove it.
- **Should `matrix-rtc-sdk` be a separate crate?** Alternative is a `matrix-sdk`
  feature on `matrix-call-livekit` as today. Separate is recommended because the
  driver is RTC-generic, keeps the git pin out of `matrix-rtc`, and is the piece
  most likely to move upstream.
- **Naming of the web package** and whether the livekit-client TS layer should ship
  at all, or stay demo-only. Element Call has its own LiveKit integration.

---

## 6. Verification matrix

| Check | Phase 0 | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|---|
| `cargo test` all workspace crates | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `cargo check -p matrix-rtc --features runtime-probe --target wasm32-unknown-unknown` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `cargo clippy --all-targets --all-features -- -D warnings` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| draft web suites (vitest via ubrn) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `matrix-rtc-sdk` backend test | | ✓ | ✓ | ✓ | ✓ | ✓ |
| `make test-e2e` (`e2e_call`, 4 scenarios) | ✓ (old) | ✓ (old) | ✓ (new) | ✓ | ✓ | ✓ |
| `make test-interop` Rust-peer specs | ✓ (old) | ✓ (old) | ✓ (new) | ✓ | ✓ | ✓ |
| `make test-interop` web-peer specs | ✓ (old) | ✓ (old) | ✓ (old) | ✓ (old) | ✓ (new) | ✓ |
| `build-android-aar.sh` slim + `MEDIA=1`, `build-ios-xcframework.sh` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Generated Kotlin/Swift for `matrix_rtc_ffi` unchanged vs phase 0 | | | | ✓ | ✓ | ✓ |
| `make test-ffi-media` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
