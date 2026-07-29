# `e2e_call` — end-to-end MatrixRTC/LiveKit test (two Rust clients)

An integration test that drives the **whole Rust MatrixRTC stack** end to end —
no browser, no microphone. The flow mirrors setting up a real call from scratch:
`alice` **creates an encrypted room and invites** `bob`, `bob` **joins**, and only
once the two-member room membership has settled do both peers join the same
MatrixRTC slot, discover each other through MSC4354 sticky events, exchange
per-participant media keys over encrypted to-device messages, connect to the
LiveKit SFU **with per-participant frame E2EE enabled**, and prove real media:
`alice` publishes a 440 Hz tone, `bob` decrypts what the SFU forwards and verifies
the frequency.

It is `#[ignore]`-gated because it needs the `demo/backend` docker stack; CI
runs it on every PR (`.github/workflows/e2e-call.yml`).

Source: [`e2e_call/main.rs`](./e2e_call/main.rs) (flow) and
[`e2e_call/provision.rs`](./e2e_call/provision.rs) (throwaway-user
registration).

## What it proves

| Layer | What's exercised |
| --- | --- |
| **Room setup** | `alice` creates an **encrypted** room (`m.room.encryption`) with `shared` history visibility and invites `bob`; `bob` joins; both wait for the 2-member membership before any RTC signalling. |
| **Signalling** | Each client publishes its own `m.rtc.member` membership as a sticky event (+ a dead-man's-switch delayed leave) and discovers the peer via `subscribe_to_sticky_events` → `RtcSessionManager`. Both peers join the room before RTC signalling. Success = each side sees 2 members. |
| **Transport** | MSC4195 OpenID→JWT token exchange and SFU connect for both clients. |
| **Encryption** | The core `EncryptionManager` generates a per-participant media key and distributes it to the peer as an Olm-encrypted `m.rtc.encryption_key` to-device message (`SdkCommandSender::send_to_device_message`). Each side imports received keys into its LiveKit `KeyProvider` (`MediaKeyBridge`), addressed by the MSC4195 pseudonymous identity. Success = `bob` imported `alice`'s key. |
| **Media** | 440 Hz tone published by `alice`, **GCM frame-encrypted** at the SFU, decrypted and recorded by `bob`, verified with a Goertzel filter (`media::detect_tone > 0.5`). A WAV is written to `<temp_dir>/e2e_received.wav` (uploaded as a CI artifact on failure). The tone only decodes because the keys were exchanged and mapped correctly. |

## How it's wired

```
matrix_sdk::Client ──login──▶ SyncService (sliding sync; sticky ext auto-on under unstable-msc4354)
        │                              │
        │                              ▼
        │                     room.subscribe_to_sticky_events()
        │                              │  run_sticky_bridge (src/matrix_bridge.rs)
        ▼                              ▼
 SdkCommandSender ──▶ RtcSessionManager (join → own membership sticky + delayed leave; heartbeat)
 (src/matrix_bridge.rs)   │  └─ EncryptionManager: generates + distributes per-participant keys
        │                 │        via encrypt_and_send_raw_to_device (to-device, Olm-encrypted)
        │                 └─ MediaKeyBridge (src/keys.rs): received keys ──▶ LiveKit KeyProvider
        ▼                          ▲ (keyed by MSC4195 pseudonymous identity)
 matrix_rtc_livekit::connect_e2ee(..., key_provider) ──▶ LiveKitSession (GCM frame E2EE)
                                            └─ publish_tone / record_track (src/media.rs)

 Received m.rtc.encryption_key to-device events ──▶ AnyToDeviceEvent handler
   ──(mpsc, Send)──▶ spawn_local key pump ──▶ RtcSessionManager::receive_encryption_key
```

The manager/bridge/heartbeat futures are `!Send` (the core `RtcCommandSender` is
`?Send`), so the test runs on a single-thread `tokio::task::LocalSet`, with a
300 s overall timeout so a wedged stack fails fast.

## Prerequisites

The `demo/backend` stack (Synapse + LiveKit SFU + `lk-jwt` auth service).
**Element Web is not needed** — the two Rust clients are each other's peer, and
users, room, and slot are all created at runtime.

```sh
make backend-up
```

## Dependency caveat (important)

The test needs the experimental sticky-events SDK. `matrix-rtc-livekit/Cargo.toml`
pins it to **`BillCarsonFr/matrix-rust-sdk` rev `1815b08c`** — see the long
comment there for why the rev cannot move freely (it pairs a ruma fork carrying
the MSC4143 event types with the "keep encryption info for sticky events"
commit this crate's bridge relies on).

The root `Cargo.toml` also carries a `[patch.crates-io]` block copied from the fork
(cargo doesn't propagate a git dep's own patches to the consumer).

## Build & run

First build is long (it compiles `matrix-sdk` + native `libwebrtc`).

```sh
cargo test -p matrix-rtc-livekit --features matrix-sdk,testing --test e2e_call -- --ignored --nocapture
```

That's the whole invocation against a local `make backend-up` stack — every
setting has a matching default, and two throwaway users (`alice-<run>` /
`bob-<run>`) are registered on the fly, so a persistent `data/` dir never causes
collisions. Overrides for pointing at another deployment:

| Env var | Default | Meaning |
| --- | --- | --- |
| `HOMESERVER_URL` | `http://localhost:8008` | Synapse CS-API base URL |
| `LIVEKIT_SERVICE_URL` | `http://localhost:6080` | `lk-jwt` `/get_token` base URL (fallback when the homeserver doesn't implement the MSC4143 transports endpoint) |
| `SLOT_ID` | `m.call#ROOM` | MatrixRTC slot |
| `ALICE` / `ALICE_PW`, `BOB` / `BOB_PW` | *(auto-provisioned)* | Use pre-existing users instead of registering throwaways (for stacks with closed registration) |
| `INSECURE_TLS` | *(unset)* | set (any value) to accept self-signed certs on a remote TLS stack |
| `RUST_LOG` | `info` | tracing filter |

### Expected output (success)

```
[provision] registered alice-…
[provision] registered bob-…
[alice-…] logged in as @alice-…:synapse (device …)
[bob-…]   logged in as @bob-…:synapse   (device …)
[alice] created encrypted room !… , invited @bob-…:synapse
[bob]   joined room !…
[alice] room has 2 joined members
[bob]   room has 2 joined members
[alice] opened slot m.call#ROOM
[alice-…] joined RTC session (membership …)
[bob-…]   joined RTC session (membership …)
[alice-…] connected to the SFU (per-participant frame E2EE enabled)
[bob-…]   connected to the SFU (per-participant frame E2EE enabled)
[alice] sees 2 members (sticky round-trip OK)
[bob]   sees 2 members (sticky round-trip OK)
[alice] publishing 440 Hz tone
[bob]   track subscribed from …
[bob]   wrote /tmp/e2e_received.wav (… samples)
[bob]   440 Hz energy ratio: 0.9xx
[bob]   imported alice's per-participant media key: true
=== RESULT ===
…
END-TO-END TEST PASSED (with per-participant frame E2EE)
```

## Notes / troubleshooting

- **Benign log noise.** `matrix_sdk::latest_events…: Failed to deserialize the event`
  (`missing field slot_id`, `CallMemberEventContent` variant mismatch, `missing field
  m.call.id`) and `event_cache…: missing target event id from the redaction event`
  come from the SDK's room-preview builder failing to type legacy/RTC member events.
  They don't touch the sticky path. Quiet them with
  `RUST_LOG=matrix_sdk::latest_events=off,matrix_sdk::event_cache=off`.
- **macOS.** `build.rs` passes `-ObjC` to the linker for examples *and tests* so
  libwebrtc's Objective-C categories aren't dead-stripped (otherwise it aborts at
  runtime with `+[NSString stringForAbslStringView:]: unrecognized selector`). If
  that ever produces duplicate-symbol link errors, switch to `-force_load <libwebrtc.a>`.
- **`room … did not sync within 30s`** — the freshly created room (or `bob`'s
  invite) hasn't come down sliding sync yet; usually a transient re-run fixes it.
- **Stale `Cargo.lock`** resolving crates.io `matrix-sdk 0.18` → run
  `cargo update -p matrix-sdk`.

## Current limitations / follow-ups

- Media is **subscribe-only on the receiver** (only `alice` publishes; `bob`
  records). Frame E2EE is now enabled in both directions, but only `alice→bob`
  media is exercised.
- Key distribution uses `CollectStrategy::AllDevices` (sends to every device
  regardless of verification state) because the throwaway logins have no
  cross-signing set up. A production integration should prefer
  `IdentityBasedStrategy` (MSC4153).
- `receive_encryption_key` fans a received key out to every session in the room
  (the MSC4143 key content carries no `slot_id`); exact for one slot per room.
- The dead-man's-switch delayed leave is a *plain* (non-sticky) delayed event, so
  crash cleanup currently relies on the join sticky's TTL rather than the disconnect
  superseding it — making the delayed leave sticky is a TODO.
