# Mobile Build Setup

This directory contains build scripts and configuration for packaging the Matrix RTC FFI library for mobile platforms.

## Quick Start

### Install Build Tools

```bash
# Install Rust toolchain additions and build tools
cargo install uniffi_bindgen cargo-ndk

# Add iOS targets
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios

# Add Android targets
rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android
```

### Build Android AAR

```bash
./scripts/build-android-aar.sh
```

Output: `mobile/android/matrixrtc/build/outputs/aar/matrixrtc-release.aar`

### Build iOS XCFramework

```bash
./scripts/build-ios-xcframework.sh
```

Output: `mobile/ios/build/MatrixRtcFFI.xcframework`

## Load the native library first (Android)

```kotlin
import org.matrix.rtc.MatrixRtc

MatrixRtc.initialize()
```

This must run before any other SDK call. The generated bindings reach the native
library through JNA, which `dlopen`s it — and `dlopen` does not run `JNI_OnLoad`,
only `System.loadLibrary` does. In the media build `JNI_OnLoad` is what gives
libwebrtc its `JavaVM` and class loader, so without this call the first attempt to
open a session aborts the process from inside libwebrtc rather than throwing.

It is idempotent, and the `RtcLogging` helpers below call it themselves, so setting
logging up as your first SDK call covers it too.

## Turn on logging first

The SDK is silent until the host installs a logger. Do this before creating an
`RtcSessionManagerHandle`, or you will see nothing at all — not even errors.

**Android**

```kotlin
import org.matrix.rtc.RtcLogging
import uniffi.matrix_rtc_ffi.RtcLogLevel

RtcLogging.initLogcat(RtcLogLevel.DEBUG)
// noisier, for one subsystem:
RtcLogging.initLogcat(RtcLogLevel.INFO, "matrix_rtc_core=debug,matrix_rtc_livekit=trace")
```

```bash
adb logcat -s matrix-rtc
```

To route into Timber, a rageshake file, or any host pipeline, use
`RtcLogging.init(...) { record -> ... }` instead. The callback runs on a dedicated Rust
thread and may block; records are dropped rather than queued without bound if it falls
behind (`droppedLogRecordCount()` reports how many).

**iOS**

```swift
try setupLogging(config: RtcLogConfig(level: .debug, filter: "", writeToSystem: true),
                 sink: nil)
```

Output goes to stderr, which appears in the Xcode console.

**Filter syntax** is `RUST_LOG`'s. Targets are Rust module paths matched by prefix; the
roots are `matrix_rtc_core`, `matrix_rtc_media`, `matrix_rtc_livekit`, `matrix_rtc_ffi`,
plus third-party `livekit` and `webrtc_sys`.

**Useful extras**

- `RtcLogging.log(level, message)` (or `logEvent(...)`) puts your own lines in the same
  timeline as the SDK's, which is usually how you tell an SDK bug from an integration one.
- `manager.debugSnapshot()` returns JSON of every session, its room state, and each
  candidate member with the reason it is or is not joined — attach it to bug reports.
- Key material, LiveKit JWTs and OpenID tokens are never logged at any level.

See the "Logging" section of [../ARCHITECTURE.md](../ARCHITECTURE.md) for the level
conventions the SDK follows.

## Diagnosing "the call connects but I see/hear nothing"

Frames are produced at a fixed cadence whether or not RTP is arriving — an audio
stream with no incoming packets still yields 10 ms buffers of jitter-buffer
concealment (silence), and a video stream holds its last picture. So a silent call
and a silent-because-nothing-is-arriving call look identical at the frame level.
Two APIs separate them, without reading logcat.

**`MediaSession.receiveStats(memberId, kind)`** returns cumulative RTP counters, or
`null` if that stream isn't subscribed or no RTCP report has landed yet. Every field
is a running total, so sample twice and compare:

| Between two samples | Diagnosis |
| --- | --- |
| `packetsReceived` flat | Nothing is arriving: network, subscription, or SFU |
| `packetsReceived` up, `framesDecoded` flat | Arriving but not decoding (video) |
| `concealedSamples` up in step with `totalSamplesReceived` | The "audio" is entirely fabricated |
| both up, `packetsLost`/`jitter` rising | Arriving and decoding, but lossy |

**`FfiCallEvent.FrameEncryptionState`** on the event stream names the cause when it is
a key problem. `MissingKey` means frames carry a key index we hold nothing for (their
key never reached us, or arrived under a different identity); `DecryptionFailed` means
we have a key for that index and it doesn't work. It is reported per participant, not
per stream — the frame cryptor is keyed by participant identity.

```kotlin
when (val event = session.nextEvent()) {
    is FfiCallEvent.FrameEncryptionState ->
        if (event.state != FfiFrameEncryptionState.OK) {
            val stats = session.receiveStats(event.memberId, FfiStreamKind.MICROPHONE)
            // packetsReceived > 0 here means the media path is fine and the key is not
            Timber.w("no media from ${event.memberId}: ${event.state}, stats=$stats")
        }
    else -> { /* ... */ }
}
```

## Full Documentation

See [PACKAGING.md](./PACKAGING.md) for complete documentation including:
- Detailed build workflows
- Integration instructions for iOS and Android apps
- CI/CD examples
- Troubleshooting guides

