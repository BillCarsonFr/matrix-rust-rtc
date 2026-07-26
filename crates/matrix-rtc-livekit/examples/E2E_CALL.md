# `e2e_call` — end-to-end MatrixRTC/LiveKit test (two Rust clients)

A hand-run example that drives the **whole Rust MatrixRTC stack** end to end — no
browser, no microphone. The flow mirrors setting up a real call from scratch:
`alice` **creates an encrypted room and invites** `bob`, `bob` **joins**, and only
once the two-member room membership has settled do both peers join the same
MatrixRTC slot, discover each other through MSC4354 sticky events, connect to the
LiveKit SFU, and prove real media: `alice` publishes a 440 Hz tone, `bob` records
what the SFU forwards and verifies the frequency.

Source: [`e2e_call.rs`](./e2e_call.rs).

## What it proves

| Layer | What's exercised |
| --- | --- |
| **Room setup** | `alice` creates an **encrypted** room (`m.room.encryption`) with `shared` history visibility and invites `bob`; `bob` joins; both wait for the 2-member membership before any RTC signalling. |
| **Signalling** | Each client publishes its own `m.rtc.member` membership as a sticky event (+ a dead-man's-switch delayed leave) and discovers the peer via `subscribe_to_sticky_events` → `RtcSessionManager`. Both peers join the room before RTC signalling. Success = each side sees 2 members. |
| **Transport** | MSC4195 OpenID→JWT token exchange and SFU connect for both clients. |
| **Media** | 440 Hz tone published by `alice`, recorded by `bob`, verified with a Goertzel filter (`media::detect_tone > 0.5`). A WAV is written to `/tmp/e2e_received.wav`. Media-frame E2EE is out of scope (see `src/keys.rs`); media is cleartext. |

## How it's wired

```
matrix_sdk::Client ──login──▶ SyncService (sliding sync; sticky ext auto-on under unstable-msc4354)
        │                              │
        │                              ▼
        │                     room.subscribe_to_sticky_events()
        │                              │  run_sticky_bridge (src/matrix_bridge.rs)
        ▼                              ▼
 SdkCommandSender ──▶ RtcSessionManager (join → own membership sticky + delayed leave; heartbeat)
 (src/matrix_bridge.rs)
        │
        ▼
 matrix_rtc_livekit::connect(...) ──▶ LiveKitSession  ──publish_tone / record_track (src/media.rs)
```

The manager/bridge/heartbeat futures are `!Send` (the core `RtcCommandSender` is
`?Send`), so the example runs on a single-thread `tokio::task::LocalSet`.

## Prerequisites

The `demo/backend` stack (Synapse + LiveKit SFU + `lk-jwt` auth service). **Element
Web is not needed** — the two Rust clients are each other's peer.

```sh
cd demo/backend
./setup.sh up
./setup.sh register alice secret
./setup.sh register bob secret
# no room to create — alice creates the encrypted room and invites bob at runtime
```

## Dependency caveat (important)

The example needs the experimental sticky-events SDK. `matrix-rtc-livekit/Cargo.toml`
pins it to **`BillCarsonFr/matrix-rust-sdk` rev `625c1a3`**, *not* the
`valere/experimental_sticky` branch HEAD: HEAD's "update ruma" commit repoints `ruma`
to upstream, which lacks the MSC4143 `events::rtc::member` types that
`matrix-sdk-base`'s `unstable-msc4354` code needs, so HEAD does not compile. Bump the
rev once the fork's `ruma` pin is fixed.

The root `Cargo.toml` also carries a `[patch.crates-io]` block copied from the fork
(cargo doesn't propagate a git dep's own patches to the consumer).

## Build & run

First build is long (it compiles `matrix-sdk` + native `libwebrtc`).

```sh
cargo build -p matrix-rtc-livekit --features matrix-sdk,testing --example e2e_call

HOMESERVER_URL=https://synapse.m.localhost \
ALICE=alice ALICE_PW=secret BOB=bob BOB_PW=secret \
SLOT_ID='m.call#ROOM' \
LIVEKIT_SERVICE_URL=https://matrix-rtc.m.localhost/livekit/jwt INSECURE_TLS=1 \
cargo run -p matrix-rtc-livekit --features matrix-sdk,testing --example e2e_call
```

| Env var | Meaning |
| --- | --- |
| `HOMESERVER_URL` | Synapse CS-API base URL |
| `ALICE` / `ALICE_PW`, `BOB` / `BOB_PW` | the two users' localparts + passwords |
| `SLOT_ID` | MatrixRTC slot, default `m.call#ROOM` |
| `LIVEKIT_SERVICE_URL` | `lk-jwt` `/get_token` base URL |
| `INSECURE_TLS` | set (any value) to accept the dev CA |

### Expected output (success)

```
[alice] logged in as @alice:… (device …)
[bob]   logged in as @bob:…   (device …)
[alice] created encrypted room !… , invited @bob:…
[bob]   joined room !…
[alice] room has 2 joined members
[bob]   room has 2 joined members
[alice] joined RTC session (membership …)
[bob]   joined RTC session (membership …)
[alice] connected to the SFU
[bob]   connected to the SFU
[alice] sees 2 members (sticky round-trip OK)
[bob]   sees 2 members (sticky round-trip OK)
[alice] publishing 440 Hz tone
[bob]   track subscribed from …
[bob]   wrote /tmp/e2e_received.wav (… samples)
[bob]   440 Hz energy ratio: 0.9xx
=== RESULT ===
… 
END-TO-END TEST PASSED
```

## Notes / troubleshooting

- **Benign log noise.** `matrix_sdk::latest_events…: Failed to deserialize the event`
  (`missing field slot_id`, `CallMemberEventContent` variant mismatch, `missing field
  m.call.id`) and `event_cache…: missing target event id from the redaction event`
  come from the SDK's room-preview builder failing to type legacy/RTC member events.
  They don't touch the sticky path. Quiet them with
  `RUST_LOG=matrix_sdk::latest_events=off,matrix_sdk::event_cache=off`.
- **macOS.** `build.rs` passes `-ObjC` to the linker for examples so libwebrtc's
  Objective-C categories aren't dead-stripped (otherwise it aborts at runtime with
  `+[NSString stringForAbslStringView:]: unrecognized selector`). If that ever
  produces duplicate-symbol link errors, switch to `-force_load <libwebrtc.a>`.
- **`room … did not sync within 30s`** — the freshly created room (or `bob`'s
  invite) hasn't come down sliding sync yet; usually a transient re-run fixes it.
- **Stale `Cargo.lock`** resolving crates.io `matrix-sdk 0.18` → run
  `cargo update -p matrix-sdk`.

## Current limitations / follow-ups

- Media is **subscribe-only on the receiver** and E2EE is stubbed (see `src/keys.rs`).
- The dead-man's-switch delayed leave is a *plain* (non-sticky) delayed event, so
  crash cleanup currently relies on the join sticky's TTL rather than the disconnect
  superseding it — making the delayed leave sticky is a TODO.
- Next steps: a pinned `docker-compose` backend, then promote this flow to a gated
  `#[ignore]` integration test (reusing `matrix_bridge` + `media::detect_tone`) with
  two in-process clients and programmatic assertions.
```
