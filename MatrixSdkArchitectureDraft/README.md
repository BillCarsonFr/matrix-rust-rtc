# matrix-rtc (architecture draft)

An implementation of the plan in [`MatrixSdkArchitecture.md`](MatrixSdkArchitecture.md):
one crate that does everything needed to *participate* in a MatrixRTC session
([MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143)) and
nothing media-specific. The host supplies one `MatrixDriver` (all Matrix I/O)
and consumes four outputs: memberships, connections, key map, status.

```text
MatrixDriver (host)  ──▶  ParticipationManager  ──▶  memberships · connections · key map · status
```

| module | job |
|---|---|
| `session` | raw Matrix events → session state (MSC4354 sticky map, slot/room conditions, compat read side) |
| `own_membership` | our join/leave: delayed leave first, sticky refresh, MSC4140 restarts, MSC4195 delegation, compat write side |
| `connections` | which SFU connections to hold, with tokens (MSC4195) |
| `encryption` | per-member media keys over to-device (rotation policy, inbound verification) |
| `participation` | the facade wiring the four above |
| `driver` | the Matrix I/O traits the host implements |
| `executor` | spawn / sleep / clock on native (tokio) and wasm |
| `uniffi_api` | one binding surface for Swift, Kotlin, React Native and web |

Plans and status live next to each module (`*ImplementationPlan.md`,
`encryption/README.md`).

## Build and test

```sh
cargo test                                                        # unit + tests/participation.rs (black-box, mock driver)
cargo test --features uniffi                                      # + FFI tests
cargo clippy --all-targets --features uniffi -- -D warnings
cargo check --features runtime-probe --target wasm32-unknown-unknown
```

`tests/participation.rs` drives `ParticipationManager` only through its public
API against a mock `MatrixDriver` that behaves like a homeserver (echoes our
events, scripted peers answer our media key).

## Web

[`web-test-app/`](web-test-app/README.md) holds the uniffi-generated npm
bindings (via uniffi-bindgen-react-native's wasm target), the acceptance
suites through the real bindings, a demo page, a `MatrixDriver` mock and a
matrix-js-sdk driver for a real homeserver (`../demo/backend`).

```sh
cd web-test-app
npm install
npm run ubrn:web   # build the crate for wasm32 + generate TS bindings
npm test           # acceptance suites (vitest)
npm run dev        # demo page
```

## Using it (Rust)

```rust
let manager = ParticipationManager::new(
    room_id, slot_id,
    OwnIdentity { user_id, device_id },
    driver,                               // Arc<dyn MatrixDriver>
    ParticipationConfig::default(),
);
manager.on_memberships_change(Box::new(|m| render_tiles(m)));
manager.on_connections_change(Box::new(|c| hold_lk_rooms(c)));
manager.on_key_map_change(Box::new(|_map, change| set_key(change)));
// a slot you just opened is open only once sync echoed it: check `manager.session().slot_state`
manager.join(TransportIntent::Publish(livekit_transport), JoinParams::new("m.call")).await?;
// ...
manager.leave(None).await?;
```

Room-list / header info without a manager: `compute_sessions_from_events(&events, &config)`.
