# matrix-rtc-livekit

The [MSC4195](https://github.com/matrix-org/matrix-spec-proposals/pull/4195)
LiveKit transport for MatrixRTC — the "LiveKit SDK" layer that turns the
membership and key outputs of `matrix-rtc-core` into a live SFU media session.

This crate is **native-only** (the LiveKit client pulls in `libwebrtc`); it never
targets wasm.

## What it does

- **`token`** — exchanges a Matrix OpenID token for a LiveKit SFU JWT via the
  authorisation service's `POST /get_token` endpoint. The OpenID token itself is
  obtained through the `OpenIdTokenSource` trait, so the crate is not hard-wired to
  a particular Matrix SDK.
- **`identity`** — the MSC4195 hash derivations (`livekit_alias`, pseudonymous
  participant identity) used to map keys onto LiveKit participants.
- **`session`** — connects to the SFU and exposes the LiveKit room event stream.
  Subscribe-only for now (the recording/transcription-bot shape).
- **`keys`** — bridges `matrix-rtc-core` media keys toward LiveKit frame encryption.

## Features

- `matrix-sdk` *(off by default)* — provides a default `OpenIdTokenSource`
  implementation for `matrix_sdk::Client`, and enables the `connect` example.

> This crate depends on `livekit`, which pulls in `libwebrtc` (native C++), so
> building it requires a C++ toolchain. CI jobs without one must not build it.

## End-to-end encryption status

E2EE frame encryption is **not yet wired**. `matrix-rtc-core` produces
per-participant keys (an HKDF input), but the LiveKit Rust SDK currently lacks the
per-participant HKDF key import MSC4195 specifies
([livekit/rust-sdks#796](https://github.com/livekit/rust-sdks/issues/796)).
Until that lands, media is unencrypted and `MediaKeyBridge` only records the
signalled key material, marking exactly where the LiveKit `KeyProvider` hand-off
will go.

## Example

`examples/connect.rs` logs into a homeserver, performs the token exchange,
connects to the SFU, and logs subscribed remote tracks. See
[`demo/backend`](../../demo/backend/README.md) for a local stack to run it against:

```sh
cargo run -p matrix-rtc-livekit --example connect --features matrix-sdk
```
