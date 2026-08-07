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

- **When the loop ends.** The event stream ending *is* the "call is over"
  signal: the channel closes on `leave()` and after any unrecoverable
  disconnect (a `RoomEvent::Disconnected` with the reason arrives first;
  transient drops auto-resume and don't end the stream). What the stream does
  **not** have is a deadline — if no peer ever publishes, the loop above waits
  forever, so a real bot should wrap the wait in `tokio::time::timeout`.

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
[`demo/backend` README](../../demo/backend/README.md). The MSC4153
cross-signing requirement stays at the core's default: each user's first run
bootstraps cross-signing and prints a **recovery key**, and since every run
logs in a fresh device, subsequent runs need that key passed as
`RECOVERY_KEY=...` so the new device gets cross-signed.)

## Load testing

[`examples/load_test.rs`](examples/load_test.rs) drives a deployment with N
virtual participants: it logs in N devices of **one** account, walks them
through the same join sequence a real client uses, and has each publish a video
file as its camera track. Run it while watching the same call in Element Web to
inspect SFU/homeserver/client behaviour.

The easiest way in is
[`scripts/load-test-sample.sh`](../../scripts/load-test-sample.sh): copy it,
fill in the block at the top once, then run your copy. `scripts/load-test.sh`
is git-ignored, so the password and recovery key in it stay out of the
repository. Extra flags pass straight through and win over the block, so
one-off tweaks need no edit:

```sh
cp scripts/load-test-sample.sh scripts/load-test.sh
$EDITOR scripts/load-test.sh
./scripts/load-test.sh                               # your defaults
./scripts/load-test.sh --devices 10 --no-simulcast   # one-off override
```

Or invoke it directly:

```sh
cargo run --release -p matrix-rtc-livekit --example load_test \
    --features matrix-sdk -- \
    --user loadtest --password secret --recovery-key 'EsT ...' \
    --room '!room:synapse' --video clip.mp4 --devices 5
```

Every argument also reads from an environment variable (`MX_USER`,
`MX_PASSWORD`, `RECOVERY_KEY`, `ROOM_ID`, `HOMESERVER_URL`) — prefer that for
the secrets. `--help` lists the rest; the ones that matter most are
`--resolution` / `--fps` / `--clip-seconds`, `--audio`, `--subscribe`, and the
`--ramp-ms` / `--login-delay-ms` pacing.

Notes:

- **`ffmpeg` must be on `PATH`**, unless the input is already `.y4m` or raw
  `.yuv`. It is invoked once at startup to decode `--clip-seconds` of the file
  into memory; every device then loops that same buffer from a different
  offset. One shared decode keeps the CPU going into encoding, which is what
  the run is meant to exercise.
- **The recovery key is required.** Each device is a fresh login, and neither
  the SDK's identity-based to-device strategy nor the core's MSC4153 policy
  will exchange media keys with a device that is not cross-signed.
- **`--store <folder>` reuses devices between runs.** Device *i* keeps its
  session and sqlite crypto store in `<folder>/device-i`; a run restores what is
  there and only logs in for the devices that are missing. Two payoffs: repeat
  runs pay no login cost at all (so the `--login-delay-ms` rate limiting above
  stops mattering), and a restored device keeps its Megolm sessions, so it can
  decrypt member events sent *before* the run started — a fresh device cannot,
  which is why a peer already in the call can stay invisible to it until they
  re-join. Implies `--keep-devices`, since logging out would revoke the stored
  token; `--purge-devices` clears the folder along with the devices.
- **The room must be encrypted.** A member event's sending device is only known
  from its decryption metadata, so in a cleartext room memberships cannot be
  mapped to media at all. The tool warns and continues.
- **Devices are removed on exit** (a plain logout per device). A run killed
  with `kill -9` leaves them behind; `--purge-devices` deletes every device
  whose display name carries `--device-prefix` and exits.
- **`--sticky-duration-ms` defaults to 2 minutes here, against the library's
  one hour.** A run that is killed rather than left cleanly leaves its
  memberships standing until this elapses: the dead man's switch does *not*
  clear them, because its delayed leave is a plain event that never replaces
  the sticky entry. An hour of ghost participants makes the next attempt
  useless. The cost is one extra membership send per device every half of it,
  and it must stay well above twice the 15 s heartbeat or memberships lapse
  between beats.
- **Scale is bounded by your machine, not the SFU** — see
  [How many devices?](#how-many-devices) below.
- Devices join publish-only by default (`CallOptions::auto_subscribe = false`).
  `--subscribe` makes them decode every peer as a real client would, at N×N
  cost.
- **Keep simulcast on when a real client is watching.** `--no-simulcast`
  publishes a single encoding with no rid, so the SFU advertises no layers to
  choose from. A subscriber using adaptive stream (Element Call, Element Web,
  any livekit-client app) asks for a specific layer, that request resolves to
  nothing, and no video is forwarded — a grey tile with *no* decryption errors,
  since no frames arrive at all. Audio still works, which makes it look like an
  E2EE fault when it isn't. Reserve `--no-simulcast` for scale runs where
  nobody renders the participants.
- N devices of one account is equivalent to N accounts here: membership is keyed
  on a random per-join `member_id`, the MSC4195 identity hashes
  `(user, device, member_id)`, and the core distributes media keys to other
  devices of our own user like any other peer.
- **Sharing the call with Element Call** (the JS SDK, which still speaks a
  pre-2026 MatrixRTC format) needs `--element-call-compat sticky` /
  `LEGACY_ELEMENT_CALL=sticky`, plus `--open-slot`: that client publishes no
  `m.rtc.slot`, and a slot nobody opened reads as closed, which projects every
  member out of the call. Media keys then go out under the legacy to-device type
  *instead of* the spec one, so such a run exchanges keys with Element Call or
  with spec-current peers, never both. Reading that format needs no flag.
  For a build older still — membership as `org.matrix.msc3401.call.member` room
  state — use `state` instead of `sticky`, and leave `--open-slot` off: that
  generation has no slot concept. See [`compat`](src/compat/).

Homeservers with rate limiting may reject a burst of logins or messages;
`--login-delay-ms` and `--ramp-ms` space them out. **Past 3 devices this is not
optional against a stock Synapse**: `rc_login` defaults to `per_second: 0.17,
burst_count: 3`, so the first three logins go through and the rest need ~6 s
each. Exceeding it returns 429 `Too many login attempts`. The tool waits that
out — five attempts per device with doubling backoff, honouring the server's
`retry_after` when it sends one — so a run survives a limiter it outruns, but
pacing it properly is faster than being throttled into it. Use
`--login-delay-ms 7000` or more for double-digit device counts; only a
homeserver you control (`demo/backend` sets every `rc_*` to 1000/1000)
tolerates the tighter default. Errors that are not rate limiting — a wrong
password, an unreachable homeserver — still fail on the first attempt.

### How many devices?

**Short answer: start at 5, double until the stats line sags, and stay one
step below that.** The numbers below are derived from the code, not measured —
treat them as a starting point, and the stats line as the truth.

The binding constraint is almost always **video encoding on the machine
running the tool**. Every device is a separate peer connection with its own
encoder set (the LiveKit Rust SDK has no publish-pre-encoded path), and how
many encoders that is depends on resolution and simulcast —
`compute_video_encodings` in livekit `src/room/options.rs`:

| `--resolution` | simulcast on | simulcast off |
| --- | --- | --- |
| 320x240 (long edge < 480) | 1 | 1 |
| 640x360 *(default)* | 2 | 1 |
| 1280x720 | 3 | 1 |

So 10 devices at 640x360 is 20 concurrent VP8 encoders with simulcast on, or
10 with `--no-simulcast` (which the sample script sets). As a rough budget,
one encoder per available core is a sane place to begin — a modern laptop
running the sample script's defaults (640x360, 15 fps, no simulcast) should
manage somewhere in the low tens; with simulcast on, halve it; at 720p with
simulcast, halve it again. `--subscribe` adds N×N decoding on top and cuts
whatever you land on by a lot.

**The measurement that settles it** is the health line printed every 10 s:

```
  [0] 150 frames/10s, 900 total, 0 errors, 4 members
```

While `frames/10s` stays near `--fps × 10`, every device is really publishing
at the rate you asked for. Once it drops, the generator is saturated: the
extra devices are not producing the load you think they are, and any
server-side number you read from that run is understating things. That is the
point to stop adding devices and instead run a second process, or a second
machine.

Two ceilings that are *not* about your CPU, and that you may actually want to
hit deliberately:

- **Media-key distribution is quadratic.** Keys go out as individually
  Olm-encrypted to-device messages, one per recipient device — bringing up N
  devices costs O(N²) messages. Teardown is worse: the core rotates the key
  whenever anyone leaves (`matrix-rtc-core/src/encryption/mod.rs`), so each
  departure makes every survivor re-key and re-send. With a few dozen devices
  the leave storm at shutdown is heavier than the join ramp was. If the
  homeserver struggles at the *end* of a run rather than during it, this is
  why.
- **One sliding-sync connection per device**, all for the same account, plus N
  logins during setup. `--login-delay-ms` and `--ramp-ms` pace the setup; the
  steady-state syncs are unpaced.

## Module map

| Module | What it does |
| --- | --- |
| **`call`** *(feature `matrix-sdk`)* | The high-level facade: `Call::join`/`Call::leave`, `open_slot`, LiveKit transport discovery (MSC4143 `GET /rtc/transports` with fallback). Start here. |
| **`matrix_bridge`** *(feature `matrix-sdk`)* | The layer under `call`: `SdkCommandSender` turns core commands (sticky membership events, delayed leaves, slot state, encrypted to-device keys) into Client-Server API calls; `run_sticky_bridge` feeds live sticky events and room state back into the core. |
| **`token`** | MSC4195 token exchange: Matrix OpenID token → LiveKit SFU JWT via the authorisation service's `POST /get_token`. The OpenID token comes through the `OpenIdTokenSource` trait, so this layer is not hard-wired to a particular Matrix SDK. |
| **`compat`** | Interop with MatrixRTC implementations that predate the 2026 MSC4143 rewrite (today: Element Call on the JS SDK), in two generations selected by `CallOptions::element_call_compat`. `StickyEvents` is the 2025 format — JSON funnels at the crate's edge, so no legacy field reaches the core; reading it is always on, writing it is opt-in. `StateEvents` is the format before MSC4354, with membership as `org.matrix.msc3401.call.member` room state, plain `{user}:{device}` SFU identities and the `/sfu/get` token endpoint; opt-in in both directions, and visible to nobody but that generation. Scaffolding, to be deleted once Element Call catches up. |
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
- [`examples/load_test.rs`](examples/load_test.rs) — the load generator: N
  devices of one account publishing a video file (see above).
- [`examples/connect.rs`](examples/connect.rs) — the low-level path only:
  token exchange + subscribe-only SFU connect, no membership signalling, no
  E2EE. Useful for poking at an authorisation service.
- [`tests/e2e_call`](tests/E2E_CALL.md) — the full two-client end-to-end test
  (runs in CI on every PR), built on `Call`.
