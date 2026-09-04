# matrix-rust-rtc

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_3.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

> **Note:** This project is developed with AI assistance.

<p align="center">
  <img src="img/matrix-rust-rtc-icon.svg" height="300">
</p>

A Rust implementation of a Matrix RTC (Real-Time Communication) client SDK:
MSC4143 call-membership signalling, per-participant media-key exchange, and a
**transport-agnostic media layer** — one Rust codebase behind Kotlin/Swift
bindings (UniFFI) and a signalling-only WebAssembly build for the web.

## The media layer

The centrepiece of the project. To the application, a call is a set of
**participants with observable frame streams** (microphone, camera,
screenshare) plus per-stream **constraints** (visibility, rendered size,
low-bandwidth mode). Everything underneath is hidden in Rust:

- **No LiveKit types on the API surface.** LiveKit is one implementation of
  the `MediaTransport` trait (`crates/matrix-rtc-media`); future transports
  (P2P, WebTransport) slot into the same model.
- **MSC4195 multi-SFU built in**: each member publishes to their own focus
  and the engine maintains one connection per distinct focus in the call,
  with backoff, roster union, and identity mapping — so Kotlin/Swift never
  reimplement it.
- **Constraints drive simulcast subscribe-side**: tell the engine how a tile
  is rendered and it picks the right layer, pauses off-screen streams, and
  re-applies settings across reconnects.
- **Frame E2EE throughout**: media keys are exchanged over Olm-encrypted
  to-device messages and applied per participant across all connections.

```rust,no_run
use futures_util::StreamExt;
use matrix_rtc_livekit::{Call, CallOptions};
use matrix_rtc_media::{
    CallEvent, Dimensions, MediaConstraints, MediaStreamKind, PublishOptions, VideoDetail,
    VideoSourceConfig,
};

async fn video_call(room: &matrix_sdk::Room) -> Result<(), Box<dyn std::error::Error>> {
    // Membership signalling + key exchange + connections to every focus.
    let call = Call::join(room, CallOptions::default()).await?;

    // Publish the camera: push platform-captured I420 frames into the handle.
    let camera = call
        .publish(PublishOptions::camera(VideoSourceConfig { width: 1280, height: 720 }))
        .await?;

    let mut events = call.subscribe_call_events();
    while let Ok(event) = events.recv().await {
        match event {
            CallEvent::StreamStarted { member_id, kind: MediaStreamKind::Camera } => {
                // Transport-neutral frames: I420 video, PCM audio.
                let track = call.remote_track(&member_id, MediaStreamKind::Camera).unwrap();
                let mut frames = track.video_frames().unwrap();
                tokio::spawn(async move {
                    while let Some(frame) = frames.next().await {
                        // render frame.width x frame.height I420 planes
                    }
                });

                // Say how the tile is rendered; the engine selects the
                // matching simulcast layer server-side (and pauses the
                // stream entirely while `visible: false`).
                call.set_constraints(&member_id, MediaStreamKind::Camera, MediaConstraints {
                    detail: VideoDetail::Dimensions(Dimensions { width: 320, height: 180 }),
                    ..Default::default()
                });
            }
            CallEvent::Ended { .. } => break,
            _ => {}
        }
    }

    drop(camera);
    call.leave().await?;
    Ok(())
}
```

The same model crosses the FFI boundary — on Android/Kotlin (media-enabled
build, see below):

```kotlin
val session = connectMediaSession(manager, config, tokenProvider)
val stream = session.videoStream(memberId, FfiStreamKind.CAMERA)!!
while (true) {
    val frame = stream.next() ?: break
    // safe copy: frame.data(plane) — or zero-copy while holding the frame:
    // frame.planePtr(plane) / frame.stride(plane) / frame.planeLen(plane)
}
```

Audio frames cross by value; video frames are handles with both safe-copy and
zero-copy plane access, latest-frame-wins so slow consumers drop frames
instead of lagging. See [ARCHITECTURE.md](ARCHITECTURE.md) for the full
design, the module docs in `crates/matrix-rtc-ffi/src/media/mod.rs` for the
host-app integration flow, and
[crates/matrix-rtc-livekit/README.md](crates/matrix-rtc-livekit/README.md)
for a runnable two-client example against the local backend.

## Workspace crates

- `crates/matrix-rtc-media`: the transport-agnostic media model — participants,
  frame streams, constraints resolver, and the `CallEngine` connection pool
  (MSC4195 multi-SFU). Depends on core + tokio only; **no LiveKit**, fully
  unit-tested against a fake transport.
- `crates/matrix-rtc-livekit`: MSC4195 LiveKit transport — SFU token exchange,
  per-participant frame E2EE, the `MediaTransport` implementation, and the
  high-level `Call::join` facade. Native-only (pulls in `libwebrtc`).
- `crates/matrix-rtc-core`: single-session machine plus room-scoped session
  manager and MSC4143/MSC4354 event conversion boundary.
- `crates/matrix-rtc-bridge`: the Matrix side — SDK-backed command sender and
  membership bridge into the core (behind the `matrix-sdk` feature; MSC4354 sticky events need `experimental-sticky` and the fork SDK, the default build speaks the pre-sticky room-state dialect only), the
  `OpenIdTokenSource` trait, and `compat` for pre-2026 Element Call wire formats.
  **No LiveKit**; `compat` needs no Matrix SDK either, so its tests run in
  seconds against no git dependencies.
- `crates/matrix-rtc-ffi`: UniFFI-based Kotlin/Swift bindings — the
  room-scoped manager API always, plus the media layer behind the `media`
  cargo feature (default off, keeps the slim artifact libwebrtc-free).
- `crates/matrix-rtc-wasm`: wasm bindings for the web (signalling only —
  browsers keep using livekit-js for media).
- `web`: browser-first JavaScript package and wasm-pack build/test scaffold.
- `mobile/android`, `mobile/ios`: Gradle library module / Swift Package and
  packaging scripts (AAR, XCFramework).
- `demo/backend`: self-contained MatrixRTC backend (Synapse +
  lk-jwt-service + **two** LiveKit SFUs for multi-focus testing, docker
  compose) used by the e2e call test on CI and for local development.

## Quick Mobile Builds

To build the Android AAR and iOS XCFramework with one command each:

```bash
# Prerequisites
cargo install uniffi_bindgen
cargo install cargo-ndk

# Add required Rust targets
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android

# Slim (signalling-only) artifacts
./scripts/build-android-aar.sh
./scripts/build-ios-xcframework.sh

# Media-enabled artifacts (frame streams + publishing; statically links libwebrtc)
make build-android-media
make build-ios-media
```

See [mobile/PACKAGING.md](mobile/PACKAGING.md) for detailed documentation —
including what changes with the media variant (binary sizes, `libwebrtc.jar`
on Android, the required `-ObjC` linker flag on iOS), integration guides, and
CI/CD setup. [mobile/README.md](mobile/README.md) covers what a host app has to
do (native loading, logging, keep-alive, diagnosing dead media), and
[CHANGELOG.md](CHANGELOG.md) tracks what changed for integrators — read its
Breaking section before bumping the SDK.

## Quick Web Builds

```bash
cd web
npm run build
npm test
```

The `web/` package uses `wasm-pack` to generate browser-first bindings under `web/pkg/`.

## Manual Binding Generation

If you prefer to generate bindings manually without building the full AAR/XCFramework:

```bash
cargo build -p matrix-rtc-ffi --release

# Generate Swift bindings
uniffi-bindgen generate \
  --library target/release/libmatrix_rtc_ffi.dylib \
  --language swift \
  --out-dir ./bindings/swift

# Generate Kotlin bindings
uniffi-bindgen generate \
  --library target/release/libmatrix_rtc_ffi.so \
  --language kotlin \
  --out-dir ./bindings/kotlin
```

## Basic commands

```bash
cargo check
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

End-to-end call test against the local backend stack — two clients exchange
E2EE-encrypted tone audio and pattern video, in both single-focus and
two-foci (multi-SFU) scenarios, including a constraints pause/resume pass
(see [demo/backend/README.md](demo/backend/README.md); also run by CI on
every PR):

```bash
make backend-up
make test-e2e
```

The media FFI smoke tests (no backend needed, compiles libwebrtc):

```bash
make test-ffi-media
```

## Pre-commit checklist

Before committing any change, run:

```bash
cargo check
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Then run binding tasks when relevant:

- If changes touch `crates/matrix-rtc-wasm/**` or `web/**`:

```bash
cd web && npm run build
cd web && npm test
```

- If changes touch `crates/matrix-rtc-ffi/**`, `mobile/**`, or `scripts/build-*.sh`:

```bash
./scripts/build-android-aar.sh
./scripts/build-ios-xcframework.sh
```

(`./scripts/build-ios-xcframework.sh` is macOS-only; add `MEDIA=1` when the
change touches the media feature.)

If a required platform/toolchain is not available locally, document the skip reason in the PR description and ensure the corresponding CI job passes before merge.

Finally, record anything a host integrator would notice in
[CHANGELOG.md](./CHANGELOG.md) — new or changed API, behaviour changes, and
especially breaking changes to the command-sender callbacks, which surface as
compile errors in the host app.

## License

Licensed under the [AGPL-3.0](LICENSE).
