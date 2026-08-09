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

## Staying in the call (keep-alive)

Two independent clocks expire your membership, and the SDK tends both for you.
`join()` starts a keep-alive driver and `leave()` stops it — **there is nothing
to call.**

| Clock | Default | Kept alive by |
| --- | --- | --- |
| Delayed leave (dead man's switch) | 30 s | Restarting its timer every 10 s |
| Sticky-map entry for your membership | 1 h | Re-sending the membership once it is halfway to expiry |

Both are configurable per join via `keepAliveTimeoutMs` and `stickyDurationMs`.
Shortening `stickyDurationMs` buys nothing but extra traffic — the refresh
interval is derived from it. Values above one hour are clamped, because servers
clamp them too and the refresh has to stay ahead of the real expiry.

Two `CommandSenderCallback` methods are load-bearing here, and both are easy to
implement in a way that looks right and silently breaks the call:

- **`sendStickyEvent(..., durationMs)`** — pass `durationMs` through verbatim;
  with matrix-rust-sdk that is `.with_sticky_duration_ms(durationMs)`.
  Substituting a value of your own breaks the refresh: shorter and the
  membership disappears mid-call, longer and a ghost membership outlives a
  crash.
- **`restartDelayedEvent(roomId, eventId)`** — implement as
  `update_delayed_event` with `UpdateAction.Restart` (MSC4140's "heartbeat
  ping"). Do **not** implement it as cancel-then-reschedule: that leaves the
  call unprotected in between, burns the server's `max_scheduled` quota, and a
  failed cancel leaks a delay that later fires — and because the sticky map
  resolves conflicts by *last to expire*, that leave out-expires your live
  membership and shows you as having left a call you are still in.

To drive the keep-alive from your own scheduler instead (a foreground service, a
workmanager job), call `manager.heartbeat(roomId, slotId)` on your own cadence;
it returns `false` when there is no joined session. The built-in driver runs
regardless, so only reach for this if you need a different cadence.

A client that dies without leaving stays visible to peers for up to
`stickyDurationMs`, not `keepAliveTimeoutMs` — see Known limitations in
[../CHANGELOG.md](../CHANGELOG.md).

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

## Testing against Element Call

Element Call is the only other MatrixRTC implementation there is to test against,
and it still speaks a pre-2026 wire format — two of them, in fact, which disagree
about where a membership lives rather than merely what it says. Pass
`elementCallCompat` on the join to speak one of them:

| Mode | Element Call generation | Membership lives in |
| --- | --- | --- |
| `null` / `OFF` | none — current MSC4143 + MSC4354 | sticky events |
| `STICKY_EVENTS` | 2025 | sticky events, legacy fields alongside the spec ones |
| `STATE_EVENTS` | before MSC4354 | `org.matrix.msc3401.call.member` room state |

It is one decision, not a wire-format flag: it also fixes the `member.id` you
join with, how an inbound media key is bound to a membership, your SFU
participant identity, and which token endpoint mints your JWT. Choose it once, on
the join — the media session reads it back from there. Getting a mode wrong
produces no error: the call connects, the roster may even fill in, and nothing
decrypts.

Reading the 2025 sticky dialect needs no mode and is always on. What your host
must do differently:

```kotlin
manager.join(FfiJoinSessionParams(
    // …
    elementCallCompat = FfiElementCallCompat.STATE_EVENTS,
))
```

- **Feed membership as raw content.** Use `setCurrentMembership(roomId,
  memberEvents, legacyStateEvents)` instead of `setCurrentStickyState`: a legacy
  content states its transports somewhere else and its membership nowhere at all,
  and the typed `StickyEvent` has no room for either. Pass both lists in one
  call — it replaces the room's whole membership, so feeding them separately
  makes the roster flicker between the two halves of the call.
  `legacyStateEvents` is the room's `org.matrix.msc3401.call.member` state, and
  is empty except in `STATE_EVENTS`. Include each event's `originServerTs`: it is
  the deadline base for a membership that states no `created_ts`, and `0` reads
  as long expired.
- **Feed legacy media keys.** `io.element.call.encryption_keys` to-device
  messages go to `receiveLegacyEncryptionKey(...)`. Forget this and the roster
  fills in, everything looks joined, and every remote tile stays black.
- **In `STATE_EVENTS` only:** implement `sendDelayedStateEvent` — the dead man's
  switch has to be a state event too.

Slots need nothing from you. Both generations of Element Call publish no
`m.rtc.slot`, so the truthful "no slots" you feed would resolve the session
closed and drop every member, you included. In `STATE_EVENTS` the SDK absorbs
that: the join forgets any slot state already supplied for the room (it usually
arrives with sync, before the user joins) and later updates for that room are
ignored, leaving the condition unenforced — that generation predates the concept
entirely, so "unknowable" is the honest answer rather than "closed". Keep calling
`onRoomSlotsReceived` unconditionally.

In `STICKY_EVENTS` the condition stays enforced, because those rooms are
otherwise spec-shaped and a slot in one is meaningful. If you feed slot state
there, someone has to open the slot — otherwise the room reads as all-closed and
the call is empty. Element Call will not, so open it yourself:

```kotlin
manager.openSlot(roomId, "m.call#ROOM", "m.call", FfiSlotEncryption.PerMember)
```

`encryption` must be `PerMember` in an encrypted room and `null` elsewhere — the
mismatch resolves the slot closed for everyone. It needs the power level for
`m.rtc.slot` state (by default the room creator), and `closeSlot(roomId, slotId)`
ends the call for every member, which is not the same as leaving it.

Two traps that cost whole debugging sessions on the native path, and are not
specific to it:

- Element Call must join **after** your device has logged in and been seen by the
  homeserver. A fresh device cannot decrypt a member event sent before it
  existed, so the roster stays empty and every key is buffered.
- Use an **encrypted** room. The core discards a media key that arrived in the
  clear, as MSC4143 requires.

## Full Documentation

See [PACKAGING.md](./PACKAGING.md) for complete documentation including:
- Detailed build workflows
- Integration instructions for iOS and Android apps
- CI/CD examples
- Troubleshooting guides

