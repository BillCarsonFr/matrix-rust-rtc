# Finishing the crate — `own_membership`, `connections`, `participation`, bindings, web app

Scope: everything in `MatrixSdkArchitecture.md` that is still `todo!()` after
`session` and `encryption` landed, plus the test harnesses around it. Read
together with `../own_membership/OwnMembershipImplementationPlan.md` (its
design is followed as written; deviations are listed in §2), 
`../session/SessionImplementationPlan.md` §5 (the contract the facade owes the
session) and `../encryption/README.md` "Lifecycle" (when the machine is built).

Contents

1. [Order of work](#1-order-of-work)
2. [Interface decisions (small public API)](#2-interface-decisions-small-public-api)
3. [`own_membership`](#3-own_membership)
4. [`connections`](#4-connections)
5. [`participation`](#5-participation)
6. [`uniffi_api`](#6-uniffi_api)
7. [Native integration suite](#7-native-integration-suite)
8. [Web test app](#8-web-test-app)
9. [matrix-js-sdk driver + demo backend](#9-matrix-js-sdk-driver--demo-backend)
10. [Docs](#10-docs)
11. [Status](#11-status)

---

## 1. Order of work

Each step ends with `cargo test` green; the web steps add the wasm run.

| step | what | verify |
|---|---|---|
| A | driver/type changes (§2) · `own_membership` = wire + compat renderers + pure machine + pump | `cargo test --lib own_membership` |
| B | `connections`: identity fns, connection set from session ∪ own transport, lazy tokens, refresh | `cargo test --lib connections` |
| C | `participation` facade · `uniffi_api` fills · `tests/participation.rs` (mock `MatrixDriver`, ported manager-level tests) | `cargo test`, `cargo test --features uniffi`, wasm `cargo check` |
| D | README + architecture doc brought in line | read-through |
| E | web-test-app: rebuild bindings, make `participation.test.ts` pass, add session/own-membership/encryption suites, simulated remote peers (keys), richer tiles | `npm run ubrn:web && npm test`, `npm run dev` |
| F | `jsSdkDriver.ts` (matrix-js-sdk behind `MatrixDriverCallback`), app mode switch, opt-in integration test against `demo/backend` | `MATRIX_RTC_BACKEND=1 npx vitest run test/backend.test.ts` |

## 2. Interface decisions (small public API)

Every cross-module seam was re-checked for the smallest surface that still
carries the architecture. Changes to what the skeleton compiled with:

- **`driver::TokenDriver` loses `get_open_id`.** The OpenID exchange is part
  of *how* an adapter obtains a LiveKit token (matrix-js-sdk:
  `getOpenIdToken()` then `POST /get_token`); the crate never needs the OpenID
  token itself. `LivekitTokenRequest { url, room_id, slot_id, member, legacy_sfu_get }`
  — the last flag is the `StateEvents` compat's `/sfu/get` (body `{room,
  openid_token, device_id}`, `room` = the `livekit_alias` `own_membership`
  writes = `room_id`), delete with that generation.
- **`OwnMembershipDriver::send_delayed_event(.., sticky_duration_ms: Option<u64>)`**
  and **`send_delayed_state_event`** (compat only) — own-membership plan
  §3.4/§3.5.
- **`own_membership`** public surface is the plan's §4.4 minus `own_member()`
  (the facade does not need it, see next bullet) and minus slot
  administration (moves to the facade). `OwnIdentity`, `new_member_id`,
  `JoinParams`, `Status` family, `OwnMembershipManager { new, join, leave,
  status, subscribe_status, debug_snapshot }`, `TransportResolver`.
- **Our own membership reaches the roster through the homeserver echo, not a
  facade shortcut.** A real homeserver hands our sticky event back through
  sync; the session then lists us like anybody else and the encryption machine
  sees us in `members` exactly as in production. The mock drivers (Rust and
  TS) therefore *echo* every accepted sticky/state event into the room-event
  sink — they model a homeserver, not a black hole. The facade's memberships
  stay "session joined set ∪ encryption's left-with-keys", as the
  architecture says, with no synthetic local entry.
- **`connections`**: one subscription (`subscribe() ->
  watch::Receiver<Vec<ConnectionWithMembers>>`), one getter, one
  `add_own_transport(intent) -> Result<RtcTransport, DriverError>` (returns the
  transport whose token now exists; the own-membership resolver is exactly
  this call). `members_for_connection_data` becomes a private helper; the two
  pure identity functions stay public (the facade fills
  `SessionMembership.transport_identity`).
- **`participation::ParticipationManager::new(room_id, slot_id, own:
  OwnIdentity, driver, config: ParticipationConfig)`** where
  `ParticipationConfig { compat, encryption: EncryptionConfig, rotation:
  SendMachineConfig }` (one config in, instead of three parameters). Public:
  `join`, `leave`, `open_slot`, `close_slot`, the four getter/callback pairs,
  `status`, `debug_snapshot`. Callbacks are set once each (`Option`, replace).
- **`participation::Status`** stays the four-variant enum; `Joining` and
  `Connected` compose the own-membership and encryption statuses,
  `Leaving` carries `own_membership::LeaveStatus`.
- **FFI**: `FfiParticipationManager::new(room_id, slot_id, user_id,
  device_id, driver, compat)`; `FfiJoinParams` gains `intent`,
  `degraded_lifetime_ms`; `MatrixDriverCallback` mirrors the driver changes
  (`get_open_id` removed, `send_delayed_event` gains `sticky_duration_ms`,
  `send_delayed_state_event` added, `get_livekit_token` request gains
  `legacy_sfu_get`). `FfiStatus` stays four values (the sub-statuses are in
  `debug_snapshot`).

## 3. `own_membership`

Implemented as the module plan says (pure `machine.rs`, `wire.rs`,
`compat_2025.rs`, `compat_msc3401.rs`, `pump.rs`); ports from
`crates/matrix-rtc-core/src/{own_membership,join}.rs` and
`crates/matrix-rtc-bridge/src/compat/{element_call,element_call_state}.rs`.
Tests per plan §9.1–9.3 (names kept).

Deviations from that plan's text, all towards less surface:

- `own_member()` and `open_slot`/`close_slot` are not on the manager.
- `LeaveStatus { leave_event_sent, delayed_leave_settled }` (plan §3.6).
- The `TransportResolver` future is `Send` natively, `?Send` on wasm through
  one `MaybeSend` alias in `mod.rs`.

## 4. `connections`

```
ConnectionsManager::new(room_id, slot_id, own: OwnIdentity, compat, session: watch::Receiver<SessionSnapshot>, driver: Arc<dyn TokenDriver>)
  .subscribe() -> watch::Receiver<Vec<ConnectionWithMembers>>
  .connections() -> Vec<ConnectionWithMembers>
  .add_own_transport(intent) -> Result<RtcTransport, DriverError>
participant_identity(user_id, device_id, member_id) -> String      // MSC4195: sha256 over the canonical triple, base64url unpadded (port of livekit-proto identity.rs)
legacy_participant_identity(user_id, device_id) -> String          // "{user}:{device}"
```

- The wanted set = `ws_url`s of every joined member's published LiveKit
  transports ∪ our own transport (kept after `add_own_transport`, cleared on
  `clear_own_transport()` — the facade calls that on leave). A connection is
  published once its token exists.
- Tokens: one per `ws_url`, minted through `TokenDriver::get_livekit_token`
  by a pump task (session watch + own-transport notify + refresh timer). The
  JWT's `exp` claim is parsed (base64url, no verification) and the token is
  re-minted one minute before it; a token without `exp` is kept.
- `add_own_transport(Publish(t))`: `t` names a `livekit_service_url` → mint
  for it; otherwise `get_rtc_transports()` picks the first `livekit` entry.
  `ReceiveOnly` never reaches here (the own-membership plan §3.3).
- Members per connection: members whose `transports.published` contains a
  LiveKit transport with that URL. Receive-only members are on no connection.

## 5. `participation`

`join(intent, params)` (own-membership plan §5.0):

1. wait for `SessionSnapshot.seeded` (with a bound; a driver that never
   answers `read_*` still lets us join — the session logs it);
2. `member_id = new_member_id(compat, &own)`;
3. `manage_media_keys = snapshot.negotiated_encryption.unwrap_or(config.encryption.manage_media_keys)`;
   build `Member { member_id, user_id, device_id, Verified, membership_ts: now, .. }`
   and construct `encryption::Machine` (always — an unencrypted call gets a
   machine with `manage_media_keys = false`, which is `Connected` at once and
   sends nothing; one code path);
4. `own_membership.join(member_id, intent, params)`; on error drop the machine.

`leave(reason)`: `own_membership.leave(reason)`, then drop the machine and
`connections.clear_own_transport()`. `join`/`leave` are serialised by an
async mutex.

Outputs, all computed from fresh inputs (`session.snapshot()` for drain-on-
read, session plan §5), never from a cached watch value:

- `memberships()`: session's joined members → `Joined` (connections = their
  LiveKit `ws_url`s, identity per compat), then encryption's
  `left_members_with_keys` not in that set → `LeftWithKeys` (no connections).
- `connections()`: `connections.connections()`.
- `key_map()`: the machine's map, empty when not joined.
- `status()`: `NotJoined → Disconnected`; `Joining(o) → Joining { own, encryption: machine.status() }`; likewise `Connected`; `Leaving`.

One pump task per manager fires the change callbacks: it selects over the
session watch, the connections watch, the own-membership status watch and a
`Notify` the key-map callback pokes; on each wake it recomputes the four
values and calls a callback only when its value changed (`PartialEq` on the
derived types). The key-map callback is wired straight through
(`encryption::KeyMapCallback` fires with the single changed key).

Slot administration: `open_slot(application_type, encryption)` /
`close_slot()` render MSC4143 `m.rtc.slot` content (port of the core's
`RawSlotEventContent::{for_open, for_close}`, `slot_id` must start with
`"{application_type}#"`) and send it through the `OwnMembershipDriver`
state-event method.

## 6. `uniffi_api`

- `FfiMatrixDriver`: the remaining `todo!()`s translate JSON ↔ `Value` and
  records ↔ driver types; `RtcError → DriverError` keeps
  `Rejected → Unauthorized` (delayed-event refusal classification depends on
  it) and gains `Unsupported`.
- `FfiParticipationManager`: thin wrapper; listeners become the Rust
  callbacks; `FfiStatus` from `participation::Status`; `FfiMembership` gains
  `transports` (the published `ws_url`s are already `connections`) — the tile
  metadata the web app renders comes from `FfiMember` + `FfiMembership`, so
  `FfiMember` gains `application_type`.
- `KeyMapListener::on_key_map_change(key_map, change)` — the full map plus
  the single changed key, as the Rust callback.

## 7. Native integration suite

`tests/participation.rs` — black-box tests of `ParticipationManager` through a
`MockMatrixDriver` that models a homeserver: records outbound calls, echoes
accepted sticky/state events into the room-event stream (origin
`Encrypted { our device }`), answers `read_*` from a scripted room state,
mints `jwt-for-{url}` tokens, and hosts *scripted remote peers* that reply to
our media key with theirs (so the key exchange can be asserted end to end).
Ported from `crates/matrix-rtc-core/src/lib.rs` (the manager-level tests
that are not already session tests):

- `a_receive_only_member_joins_and_publishes_nothing`
- `a_publishing_member_advertises_its_transport`
- `slot_encryption_turns_key_distribution_on` / `absent_slot_encryption_turns_key_distribution_off`
- `a_rejoin_in_the_same_process_distributes_a_key_to_the_incumbent`
- `a_member_whose_sticky_entry_expired_is_dropped`
- `a_left_session_still_publishes_the_peer_roster`
- `our_own_membership_does_not_count_as_somebody_else` (as: our echo does not trigger a key send to ourselves)

plus the facade's own: join order (delayed leave → sticky → token before
both), connections contain our `ws_url` right after `join`, memberships list
us after the echo, `LeftWithKeys` until rotation, status transitions,
leave sends leave + cancel, drop stops every pump (driver `Arc` count).

## 8. Web test app

- `npm run ubrn:web`, then make `test/participation.test.ts` pass (listener
  assertions gain `await tick()`).
- New suites: `session.test.ts` (session plan §4.9), `ownMembership.test.ts`
  (own-membership plan §9.4), `encryption.test.ts` (encryption README list).
- `mockDriver.ts`: echo (above), `sendDelayedStateEvent`, `stickyDurationMs`
  on delayed calls, `legacySfuGet` on token calls, and **simulated remote
  peers**: `driver.addRemotePeer({userId, deviceId, memberId})` injects the
  join and, when the SDK sends that device our key, replies with the peer's
  own key over the to-device sink (index 0, rotating on demand). The app's
  "simulate keys" toggle uses it.
- Tiles show every field the SDK exposes: member id, user, device +
  attribution, application/intent, state, published `ws_url`s, transport
  identity, key indexes held, and "you" for our own member id.

## 9. matrix-js-sdk driver + demo backend

- `src/jsSdkDriver.ts`: `MatrixDriverCallback` over a `MatrixClient`
  (adapted from `web/src/matrix-js-sdk-host.mjs`): `_unstable_sendStickyEvent`,
  `_unstable_sendDelayedEvent` (+ sticky variant), `_unstable_updateDelayedEvent`
  restart/cancel, `sendStateEvent`, `encryptAndSendToDevices` when crypto is
  on else `sendToDevice`, `getOpenIdToken` + `fetch(.../get_token)`,
  `GET /_matrix/client/v1/rtc/transports` with `.well-known` fallback,
  `read_events`/`read_state` from the room's timeline/state, and the three
  sinks fed from `RoomEvent.Timeline`, `RoomState.Events`, `ClientEvent.ToDeviceEvent`
  with `EventOrigin` from `event.isEncrypted()` / `getClaimedDeviceId()`.
- App: a "backend" panel (homeserver URL, user/password or register, room id)
  that swaps the mock for the js-sdk driver.
- `test/backend.test.ts` (opt-in, `MATRIX_RTC_BACKEND=1`, needs
  `make backend-up` in the repo root): two js-sdk clients in one unencrypted
  room, two managers; A joins publishing on `ws://localhost:7880` → B's
  memberships list A with that connection and A's `connections()` carries a
  real JWT from lk-jwt-service; B joins; A leaves → gone on B. Encrypted
  media keys need Olm and are left to the browser demo (rust-crypto with
  IndexedDB) — stated limit.

## 10. Docs

- `README.md` (crate): what it is, how to build/test natively and for wasm,
  the module map, pointer to the web app. Short.
- `MatrixSdkArchitecture.md`: the `own_membership` bullets (member id passed
  into `join`, resolver, `NotJoined`, sticky delayed leave), slot admin under
  `participation`, `TokenDriver` without `get_open_id`, the echo note for
  mocks.
- `web-test-app/README.md`: drop the "fails on purpose" section; document the
  suites, the simulated peers and the backend mode.

## 11. Status

| step | status |
|---|---|
| A own_membership (`wire`, `compat_2025`, `compat_msc3401`, `machine`, `pump`) | done — 54 tests |
| B connections (identity, connection set, lazy tokens, refresh) | done — 6 tests |
| C participation + FFI + `tests/participation.rs` | done — 17 black-box tests; `cargo test --features uniffi`, wasm `cargo check`, clippy `-D warnings` clean |
| D docs (README, architecture, web README) | done |
| E web-test-app (bindings rebuilt, 4 suites, simulated peers, tiles) | done — 31 tests through the wasm bindings |
| F js-sdk driver + backend test | done — `test/backend.test.ts` passes against `demo/backend` (Synapse msc4354+msc4140, lk-jwt-service token, two users, membership both ways, leave); run notes in `web-test-app/README.md` |

Deviations from the sections above, decided while implementing:

- `ConnectionData` is `{ service_url, ws_url, jwt_token }`: the connection
  *key* stays the transport's `livekit_service_url`, the SFU websocket URL
  comes from the token response (MSC4195 returns both).
- `LivekitTokenResponse` carries that optional `url`.
- `LeftWithKeys` is computed as the encryption machine's *key holders* minus
  the fresh session roster (`Machine::key_holders()`), not from its
  `Status::Connected.left_members_with_keys` — the machine's own roster view
  lags one pump step behind a drain-on-read getter.
- `join` checks the slot against the fresh snapshot itself (in addition to
  the own-membership machine's check on its watched copy).
- `ToDeviceSink::emit` gained `sender_cross_signed: Option<bool>` (MSC4153);
  without it every remote key was rejected as unsigned across the FFI.
- The pre-existing `TransportCreatedCallback` became the `TransportResolver`
  taking `(member_id, intent)` — the token names our member id.
- `ParticipationManager::session()` (FFI `session()` → `FfiSessionSnapshot`)
  was added: against a real homeserver a slot you just opened is open only
  once sync echoes the state, so a host needs to observe it before `join`.
- `connections` re-mints a key no more often than every 5 s whatever the
  JWT's `exp` says — a service handing out already-near-expiry tokens made
  the pump spin on the shared executor thread.
- The matrix-js-sdk driver feeds sticky events from js-sdk's sticky store
  (`RoomStickyEventsEvent.Update`), not the timeline: our own sends only
  appear there as local echoes without the sticky metadata.
