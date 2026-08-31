# web

Browser-first JavaScript bindings for `matrix-rtc-wasm`.

This package is a thin packaging layer around the Rust wasm crate in `crates/matrix-rtc-wasm`. It uses `wasm-pack` to generate runtime bindings into `web/pkg/`, which stays uncommitted.

## Package shape

- `pkg/browser/`: browser-first wasm-pack output built with `--target web`
- `pkg/node/`: Node.js fallback output built with `--target nodejs`
- `src/`: the `MatrixRtcCall` wrapper (export `./call`), joining the Rust
  roster with livekit-js
- `test/`: JavaScript smoke tests for package exports and generated bindings

## The call model (`./call`)

`MatrixRtcCall` composes a joined `WasmRtcSessionManager` slot with
`livekit-client` (an optional peer dependency, injected — so it is also
mockable):

```js
import { MatrixRtcCall } from 'matrix-rtc-wasm/call';
import * as livekit from 'livekit-client';

const call = new MatrixRtcCall({
  manager,                 // WasmRtcSessionManager: command sender set up,
                           // sticky state/room state being fed, slot joined
  bindings,                // the wasm module
  livekit,
  getOpenIdToken: () => matrixClient.getOpenIdToken(),
  roomOptions: { e2ee: { worker: e2eeWorker } }, // omit to run without frame E2EE
});
call.onParticipants = (roster) => render(roster);
await call.connect({ roomId, slotId, userId, deviceId, livekitServiceUrl });
```

Every roster entry is the Rust participant (`member_id`, `user_id`,
`is_local`, `reachable`, `streams`, `rtc_identity`) plus
`livekitParticipant` — the live livekit-js participant, joined by
`rtc_identity`. Rust owns the roster/pool/identity/key logic (the same
engine the mobile bindings run); this wrapper owns `fetch`, the livekit-js
rooms, the RoomEvent translation, and the heartbeat interval.

Media stays entirely in livekit-js: publish, subscribe, and render through
`livekitParticipant` / the `Room` as usual.

## Build

```bash
cd web
npm run build
```

That runs:

1. `wasm-pack build ../crates/matrix-rtc-wasm --target web`
2. `wasm-pack build ../crates/matrix-rtc-wasm --target nodejs`

## Test

```bash
cd web
npm test
```

The tests are written to skip the runtime smoke check if `pkg/` has not been generated yet.

## Notes

- Generated files are intentionally not committed.
- The package export map favors browser usage by default.
- If you want a published npm name later, a scoped name like `@matrix-org/matrix-rtc-wasm` would be a natural fit.

