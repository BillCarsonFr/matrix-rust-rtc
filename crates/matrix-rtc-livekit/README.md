# matrix-rtc-livekit

The [MSC4195](https://github.com/matrix-org/matrix-spec-proposals/pull/4195)
LiveKit transport for MatrixRTC — the "LiveKit SDK" layer that turns the
membership and key outputs of `matrix-rtc-core` into a live SFU media session
with per-participant frame E2EE.

This crate is **native-only** (the LiveKit client pulls in `libwebrtc`); it
never targets wasm. Building it requires a C++ toolchain.

## Quick start: join a call and record

With the `matrix-sdk` feature, [`Call::join`](src/call.rs) wraps the whole
stack — MSC4143 membership signalling (via MSC4354 sticky events), media-key
exchange over Olm-encrypted to-device messages, the MSC4195 token exchange,
and an E2EE-enabled SFU connection — in one handle:

```rust,no_run
use std::time::Duration;
use livekit::{RoomEvent, track::RemoteTrack};
use matrix_rtc_livekit::{Call, CallOptions, media};
use matrix_sdk_ui::sync_service::SyncService;

async fn record_a_call() -> Result<(), Box<dyn std::error::Error>> {
    // A logged-in, syncing client that has joined the call's room.
    let client = matrix_sdk::Client::builder()
        .homeserver_url("http://localhost:8008")
        .build()
        .await?;
    client.matrix_auth().login_username("bob", "secret").send().await?;
    let sync = SyncService::builder(client.clone()).build().await?;
    sync.start().await;

    let room = client.get_room("!room:example.org".try_into()?).unwrap();

    // Join: membership + key exchange + E2EE SFU connection.
    let mut call = Call::join(&room, CallOptions::default()).await?;

    // React to the LiveKit event stream: record the first audio track.
    while let Some(event) = call.events().recv().await {
        if let RoomEvent::TrackSubscribed { track: RemoteTrack::Audio(audio), .. } = event {
            let pcm = media::record_track(&audio, Duration::from_secs(5)).await;
            media::write_wav("call.wav", &pcm, media::SAMPLE_RATE)?;
            break;
        }
    }

    call.leave().await?;
    Ok(())
}
```

Two things the snippet glosses over:

- **Runtime.** The core's command sender is `?Send`, so `Call::join` must run
  inside a `tokio::task::LocalSet`:

  ```rust,no_run
  fn main() -> Result<(), Box<dyn std::error::Error>> {
      // Two rustls crypto backends end up in the dependency tree
      // (livekit → ring, matrix-sdk → aws-lc-rs); pick one explicitly.
      rustls::crypto::aws_lc_rs::default_provider().install_default().unwrap();

      let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
      runtime.block_on(tokio::task::LocalSet::new().run_until(record_a_call()))
  }
  ```

- **An open slot.** MSC4143 counts nobody as joined against a closed slot; the
  room creator opens one with `matrix_rtc_livekit::open_slot(...)` (see the
  example below).

The runnable version is [`examples/join_and_record.rs`](examples/join_and_record.rs) —
it adds slot opening and an optional test-tone publisher, so **two instances of
it can call each other** against the local [`demo/backend`](../../demo/backend/README.md)
stack:

```sh
make backend-up   # from the repo root

# terminal 1: publish a tone
MX_USER=alice MX_PASSWORD=secret ROOM_ID='!room:synapse' OPEN_SLOT=1 PUBLISH_TONE=1 \
cargo run -p matrix-rtc-livekit --example join_and_record --features matrix-sdk,testing

# terminal 2: record it
MX_USER=bob MX_PASSWORD=secret ROOM_ID='!room:synapse' \
cargo run -p matrix-rtc-livekit --example join_and_record --features matrix-sdk,testing
```

(Register the users and create the room as described in the
[`demo/backend` README](../../demo/backend/README.md). Note the example
disables the MSC4153 cross-signing requirement because dev-stack users have no
cross-signing — production clients must keep the core's default.)

## Module map

| Module | What it does |
| --- | --- |
| **`call`** *(feature `matrix-sdk`)* | The high-level facade: `Call::join`/`Call::leave`, `open_slot`, LiveKit transport discovery (MSC4143 `GET /rtc/transports` with fallback). Start here. |
| **`matrix_bridge`** *(feature `matrix-sdk`)* | The layer under `call`: `SdkCommandSender` turns core commands (sticky membership events, delayed leaves, slot state, encrypted to-device keys) into Client-Server API calls; `run_sticky_bridge` feeds live sticky events and room state back into the core. |
| **`token`** | MSC4195 token exchange: Matrix OpenID token → LiveKit SFU JWT via the authorisation service's `POST /get_token`. The OpenID token comes through the `OpenIdTokenSource` trait, so this layer is not hard-wired to a particular Matrix SDK. |
| **`identity`** | The MSC4195 hash derivations (`livekit_alias`, pseudonymous participant identity) used to map keys onto LiveKit participants. |
| **`session`** | Connects to the SFU and exposes the LiveKit room + event stream. |
| **`keys`** | Bridges `matrix-rtc-core` media keys into the LiveKit `KeyProvider` (`MediaKeyBridge`), keyed by pseudonymous identity — this is what makes frame E2EE per-participant. |
| **`media`** | `record_track` / `write_wav` (shipped: the recording-bot path), plus test-gated tone generation and frequency detection. |

## End-to-end encryption

Frame E2EE **is wired**: the core generates a per-participant media key,
distributes it as an Olm-encrypted `m.rtc.encryption_key` to-device message,
and `MediaKeyBridge` imports received keys into the LiveKit `KeyProvider`
(MSC4195 per-participant HKDF mode, GCM frames). Use `connect_e2ee` — or
`Call::join`, which does — rather than plain `connect`. The end-to-end test
asserts a tone survives an encrypt→SFU→decrypt round trip.

## Features

| Feature | Effect |
| --- | --- |
| `matrix-sdk` *(off by default)* | The `call` facade, the `matrix_bridge`, an `OpenIdTokenSource` impl for `matrix_sdk::Client`, and the examples. Pulls in the experimental sticky-events fork of matrix-sdk (see the pin comment in `Cargo.toml`). |
| `testing` | Test-only parts of `media` (tone generator, Goertzel detector), used by the examples and the e2e test. |

## Examples & tests

- [`examples/join_and_record.rs`](examples/join_and_record.rs) — the quick
  start, runnable (see above).
- [`examples/connect.rs`](examples/connect.rs) — the low-level path only:
  token exchange + subscribe-only SFU connect, no membership signalling, no
  E2EE. Useful for poking at an authorisation service.
- [`tests/e2e_call`](tests/E2E_CALL.md) — the full two-client end-to-end test
  (runs in CI on every PR), built on `Call`.
