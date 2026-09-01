# matrix-rtc-wasm

The browser binding for MatrixRTC: `matrix-rtc-core`'s signalling state
machine plus the shared media engine (`matrix-rtc-media`), compiled to
wasm32 and exposed to JavaScript with wasm-bindgen.

The division of labour is deliberate and strict:

- **Rust owns the protocol.** Membership state (MSC4143/MSC4354), the dead
  man's switch (MSC4140), media-key lifecycle and rotation, the participant
  roster, the multi-focus connection pool, MSC4195 identities and token
  request shapes, and the Element Call compatibility dialects.
- **JS owns the IO and the media.** Your Matrix client (matrix-js-sdk) sends
  the events Rust asks it to and feeds sync back in; livekit-js owns the SFU
  connection, tracks, rendering, and frame encryption — Rust hands it keys and
  reads back its room events. No media bytes ever cross into wasm.

This crate compiles **only** for `wasm32-unknown-unknown` (its futures wrap
JS promises and are `!Send`); on other targets it is an empty crate.

## Building

```sh
# From the repo root; outputs into web/pkg/{browser,node}.
./web/scripts/build-bindings.sh
```

The `web/` npm package wraps the output (`matrix-rtc-wasm` from
`web/package.json`, exports `.` for the bindings and `./call` for the
JS wrapper). See `web/README.md` for the packaging details.

## Get started

The complete, working reference is **`web/demo/`** — a call page over
matrix-js-sdk + livekit-client that is also the interop test peer. What
follows is the shape of it.

### The quick way: the three shipped layers

The package ships every layer of the integration, each dependency an
optional, injected peer: `matrix-rtc-wasm/call` (`MatrixRtcCall`, the
livekit-client half) and `matrix-rtc-wasm/matrix-js-sdk-host`
(`createMatrixSession` + `MatrixHost`, the Matrix half). You write the app
glue.

```js
import init, * as bindings from 'matrix-rtc-wasm';
import { ManagerOpQueue, MatrixRtcCall } from 'matrix-rtc-wasm/call';
import { MatrixHost, createMatrixSession } from 'matrix-rtc-wasm/matrix-js-sdk-host';
import * as sdk from 'matrix-js-sdk';
import * as livekit from 'livekit-client';
import E2EEWorker from 'livekit-client/e2ee-worker?worker';

await init();
bindings.initLogging('info', '');

// A crypto-ready, syncing client (cross-signing bootstrapped — MSC4153).
const { client, userId, deviceId } =
  await createMatrixSession({ sdk, homeserverUrl, user, password });

const managerOps = new ManagerOpQueue();       // see "Rules of the road"
const manager = new bindings.WasmRtcSessionManager();
const host = new MatrixHost({ sdk, client, managerOps });
manager.setup_command_sender(host.commandSender());
await host.attachRoom(manager, roomId);        // sync feeding, keys in

// 1. Publish our membership (starts the keep-alive machinery).
const memberId = await managerOps.enqueue(() =>
  manager.join({
    user_id: userId,
    device_id: deviceId,
    room_id: roomId,
    slot_id: 'm.call#ROOM',
    application: 'm.call',
    transport: { type: 'livekit', livekit_service_url: focusUrl },
  }),
);

// 2. Attach media: roster + LiveKit connection lifecycle.
const call = new MatrixRtcCall({
  manager,
  bindings,
  livekit,
  managerOps,
  getOpenIdToken: () => matrixClient.getOpenIdToken(),
  roomOptions: { e2ee: { worker: new E2EEWorker() } },
});
call.onParticipants = (roster) => render(roster); // entries carry rtc_identity
call.onEvent = (event) => console.log(event);     // key_imported, stream_started, ...
await call.connect({ roomId, slotId: 'm.call#ROOM', userId, deviceId,
                     livekitServiceUrl: focusUrl });

// 3. Media is livekit-js as usual, via the rooms the wrapper opened.
const room = call.rooms.get(focusUrl);
await room.localParticipant.enableCameraAndMicrophone();
```

Every roster entry is
`{ member_id, user_id, device_id, is_local, reachable, streams, rtc_identity }`;
join `rtc_identity` to `room.getParticipantByIdentity()` for the live
livekit-js participant.

### The host contract

Only needed when you bring your own Matrix stack instead of
`matrix-rtc-wasm/matrix-js-sdk-host` (which implements all of this section).

`setup_command_sender` takes an object the manager dispatches outbound
commands on. Contents arrive as plain JS objects; every method returns a
Promise. With matrix-js-sdk (v42+):

| Method | matrix-js-sdk | Resolves to |
| --- | --- | --- |
| `sendStickyEvent(roomId, type, content, durationMs)` | `_unstable_sendStickyEvent(roomId, durationMs, null, type, content)` — pass `durationMs` through verbatim | `{event_id}` |
| `sendStateEvent(roomId, type, stateKey, content)` | `sendStateEvent(roomId, type, content, stateKey)` | `{event_id}` |
| `sendDelayedEvent(roomId, type, content, delayMs)` | `_unstable_sendDelayedEvent(roomId, {delay}, null, type, content)` | the bare `delay_id` string |
| `restartDelayedEvent(roomId, delayId)` | `_unstable_updateDelayedEvent(delayId, Restart)` — a true MSC4140 restart, **never** cancel+resend | anything |
| `cancelDelayedEvent(roomId, delayId)` | `_unstable_updateDelayedEvent(delayId, Cancel)` | anything |
| `sendToDeviceMessage(recipients, type, content)` | `encryptAndSendToDevice(type, recipients, content)` — Olm-encrypted, per specific device | `[{userId, deviceId, error?}]`, or nothing = all delivered |

Sync feeding is per room, complete snapshots, in this order (it matters:
encryption decides how slots resolve, slots decide whether members count):

```js
await manager.on_room_encryption_received(roomId, isEncrypted);
await manager.on_room_slots_received(roomId, slotStateEvents);   // full set, [] included
await manager.on_room_members_received(roomId, joinedUserIds);
await manager.setCurrentMembership(roomId, memberEvents, legacyStateEvents);
```

`setCurrentMembership` is the raw funnel — each entry is
`{ sender, sender_device_id?, was_encrypted?, type, content }` with `content`
verbatim; it normalises pre-2026 Element Call shapes and is a no-op on
spec-current ones, so feed everything through it. In an encrypted room,
decrypt each sticky event first and attribute `sender_device_id` from the
decryption metadata (`web/demo/src/matrix-host.mjs` shows the js-sdk recipe,
including matching the megolm sender key against the device list).

Inbound media keys: for each decrypted key to-device message, call
`manager.receiveEncryptionKey({...})` (spec type) or
`manager.receiveLegacyEncryptionKey({...})` (`io.element.call.encryption_keys`)
with the Olm sender metadata and cross-signing status — MSC4153 discards keys
from devices that are not cross-signed, so bootstrap cross-signing before
joining anything encrypted.

The page owns every clock: call `manager.heartbeat(roomId, slotId)` on an
interval (`HEARTBEAT_INTERVAL_MS()`, 10 s) while joined — without it the dead
man's switch fires and peers see you depart mid-call.

### Element Call compatibility

Pass `element_call_compat` on `join` — `"off"` (default, spec-current),
`"sticky_events"` (Element Call "Matrix 2.0", 2025), or `"state_events"`
(what deployed Element Call speaks) — and the binding handles the rest:
outbound membership/key rewrites, identities, the token endpoint. The two
extra host duties per mode are in the module docs of `src/compat.rs`; in
`state_events` the host also implements
`sendDelayedStateEvent(roomId, type, stateKey, content, delayMs)` and feeds
`org.matrix.msc3401.call.member` room state as `setCurrentMembership`'s third
argument.

**Both sides of a call must use the same mode.** A mismatch is a silence, not
an error: identities derive differently, so the peer sits in the roster with
no streams and its keys bind to nothing.

## Rules of the road

- **One in-flight manager call at a time.** The wasm object throws
  `"recursive use of an object"` on concurrent calls, and several methods
  await your Matrix client mid-call. Route every manager call — sync feeds,
  join/leave, heartbeat — through one shared `ManagerOpQueue`.
- **Plain objects, not ES `Map`s**, for everything you hand the binding
  (the binding already guarantees the reverse direction).
- The `member.id` is generated per join and returned by `join(...)` — never
  supply or reuse one; read it back with `ownMemberId` when needed.
- Frame E2EE on the web means a **per-participant** key provider and
  `room.setE2EEEnabled(true)`; `MatrixRtcCall` does both (livekit-js's
  `ExternalE2EEKeyProvider` is shared-key — wrong for MSC4195).
- The generated `.d.ts` carries **real TypeScript types** for every value
  crossing the boundary (`MatrixClientHost`, `JoinParamsIn`, `RtcParticipant`,
  the `RtcCallEvent` union, ...). They are hand-written in `src/ts_types.rs`
  and must be updated alongside the serde structs they describe;
  `web/test/typecheck.test.mjs` type-checks them through a consumer.

## Testing

- `wasm-pack test --node crates/matrix-rtc-wasm` — the binding's own tests.
- `cd web && npm test` — the wrapper and roster against fakes.
- `make test-interop` — the real thing: this binding in a browser sharing
  encrypted calls with the native stack and with Element Call (both
  dialects), against a real homeserver and SFU. See `interop/README.md`.
