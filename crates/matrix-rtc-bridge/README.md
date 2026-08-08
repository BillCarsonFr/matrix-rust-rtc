# matrix-rtc-bridge

The Matrix side of the MatrixRTC stack: how `matrix-rtc-core`'s protocol
behaviour reaches a homeserver, and how it interoperates with clients that speak
an older wire format.

This crate is deliberately **transport-free** — nothing in it knows what a
LiveKit SFU is. That is the point: a second transport (P2P/WebTransport, which
`matrix-rtc-media` already designs for) reuses it unchanged.

```
matrix-rtc-core        what the protocol says
      ▲
matrix-rtc-bridge      how it reaches a homeserver     ← this crate
      ▲
matrix-rtc-livekit     how bytes flow (MSC4195 SFU)
```

One direction only. The bridge never depends on a transport; a transport depends
on the bridge.

## Module map

| Module | What it does |
| --- | --- |
| **`compat`** | Interop with MatrixRTC implementations that predate the 2026 MSC4143 rewrite (today: Element Call on the JS SDK), in two generations. `StickyEvents` is the 2025 format — MSC4354 stickies with pre-2026 field names; reading it is always on, writing it is opt-in. `StateEvents` is the format before MSC4354, with membership as `org.matrix.msc3401.call.member` **room state**; opt-in in both directions, and visible to nobody but that generation. Pure JSON in, pure JSON out — no Matrix SDK, no async runtime. Scaffolding, to be deleted once Element Call catches up. |
| **`sdk`** *(feature `matrix-sdk`)* | `SdkCommandSender` implements the core's `RtcCommandSender`, turning outbound commands (join/leave stickies, dead man's switch delayed events, Olm-encrypted `m.rtc.encryption_key` to-device messages) into Client-Server requests. `run_sticky_bridge` feeds the SDK's live sticky events — and, in the pre-sticky compat mode, room state — back into an `RtcSessionManager`. Owns the ruma pin the whole signalling path depends on. |
| **`OpenIdTokenSource`** *(crate root)* | The host's route to a Matrix OpenID token, which a transport exchanges for its own credentials. The trait is always available so a transport can name it; the `matrix_sdk::Client` impl is behind the feature. |

## Features

| Feature | Effect |
| --- | --- |
| *(default)* | `compat` and the `OpenIdTokenSource` trait. Depends only on `matrix-rtc-core`, serde/serde_json, thiserror, async-trait and log — no Matrix SDK, no async runtime, no git dependencies. |
| `matrix-sdk` *(off by default)* | `sdk`, plus the `OpenIdTokenSource` impl for `matrix_sdk::Client`. Pulls in the experimental sticky-events fork of matrix-sdk (see the pin comment in `Cargo.toml`). |

## Testing

`compat` is the largest and most-tested part of this crate and needs nothing but
`serde_json`, which is why the SDK is optional at all:

```sh
cargo test -p matrix-rtc-bridge                    # 48 tests, no git deps, no libwebrtc
cargo test -p matrix-rtc-bridge --features matrix-sdk   # 50 (adds sdk.rs)
```

That first line is the reason this crate exists. All of these tests used to live
inside `matrix-rtc-livekit`, where running them meant building `libwebrtc` and so
needing a C++ toolchain — a native media dependency gating tests that only ever
compare JSON.

## What this crate deliberately does not own

The boundary is load-bearing, so the exclusions are explicit:

- **`MemberClaims`** stays in `matrix-rtc-livekit`. Those are the `member` claims
  of the MSC4195 `/get_token` request body, which no homeserver ever sees.
- **The participant-identity derivation** (`matrix_rtc_livekit::identity_mapper`)
  and **the token endpoint** (`TokenEndpoint`). `compat` decides *which
  generation*; what that means for an identity or an endpoint is MSC4195 — a
  LiveKit document — so it lives with the transport. Each is one `match` on
  `ElementCallCompat`.
- **Media, frame encryption, and SFU connections.** `matrix-rtc-media` defines
  the transport-agnostic media model; a transport crate implements it.

`compat` keeps its own notes on why compatibility is confined to JSON funnels at
the edge — read the module docs before touching it.

## Known follow-up

`matrix_rtc_livekit::call::Call::join` still interleaves the Matrix and media
halves. Its transport-agnostic part (manager + command sender + sticky bridge +
to-device key handlers + key pump + heartbeat + join/leave) is what a second
transport would actually want to reuse, and `matrix-rtc-ffi`'s `media/session.rs`
plus `RtcSessionManagerHandle` is a parallel implementation of the same wiring.
Extracting it here would let the two converge. The injection seam already exists:
LiveKit's `MediaKeyBridge` implements the core's `EncryptionKeySignalHandler`, so
the handler can be passed in rather than constructed in place.

Also unmoved: `discover_livekit_transport`, which is really a
`GET /rtc/transports` query (MSC4143) that happens to filter for LiveKit.
