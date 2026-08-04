# Changelog

Notable changes to matrix-rust-rtc. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project has not
had a tagged release yet, so everything so far lives under Unreleased.

Entries begin with the Android integration work — earlier history is in the git
log only.

## Unreleased

### Breaking

Hosts implementing the FFI/WASM command sender must update. All three are
compile errors, not silent behaviour changes.

- **`CommandSenderCallback.sendStickyEvent` takes a new `durationMs`
  argument.** Pass it through verbatim — with matrix-rust-sdk that is
  `.with_sticky_duration_ms(durationMs)`. Substituting a value of your own
  breaks the membership refresh: shorter and the membership disappears mid-call,
  longer and a ghost membership outlives a crash.
- **`CommandSenderCallback.restartDelayedEvent(roomId, eventId)` is new.**
  Implement it as `update_delayed_event` with `UpdateAction.Restart` (MSC4140's
  "heartbeat ping"). Do *not* implement it as cancel-then-reschedule; see
  Fixed below for why.
- **`FfiCallEvent` gained a `FrameEncryptionState` variant.** uniffi generates
  the enum as a sealed class, so exhaustive `when` blocks without an `else` need
  a new arm.
- `FfiJoinSessionParams` gained `stickyDurationMs: Long?` — pass `null` for the
  default.
- WASM hosts: the JS client must implement `restartDelayedEvent(roomId,
  eventId)` and accept the new `durationMs` argument on `sendStickyEvent`.
- Android media builds must call `MatrixRtc.initialize()` before touching any
  FFI object. Required only for the media AAR (the slim signalling-only build
  loads fine through JNA); harmless to call either way.

### Added

- `MediaSession.receiveStats(memberId, kind)` reports cumulative receive-side
  RTP counters — packets, bytes, loss, jitter, `framesDecoded`, and the audio
  concealment counters. The receive path emits frames at a fixed cadence whether
  or not RTP arrives, so this is the only way a host can distinguish "nothing is
  arriving" from "arriving but not decoding".
- `FfiCallEvent.FrameEncryptionState` surfaces the frame cryptor's verdict
  (`Ok` / `MissingKey` / `DecryptionFailed` / `EncryptionFailed` /
  `InternalError`). Reported per participant, not per stream.
- The keep-alive now runs by itself: `join()` starts a driver and `leave()`
  stops it. `RtcSessionManagerHandle.heartbeat(roomId, slotId)` is exported for
  hosts that would rather drive it from their own scheduler.
- The membership's sticky-map entry is re-sent once it is halfway to expiring,
  configurable per join via `stickyDurationMs` (default 1 h, the maximum
  MSC4354 allows).
- `MatrixRtc.initialize()` on Android owns native library loading, so
  libwebrtc's `JNI_OnLoad` runs.
- `Call::receive_stats` for parity on the native Rust facade.
- `scripts/build-android-aar.sh` now fails the build if a produced `.so`
  exports no `Java_livekit_org_webrtc*` symbols.

### Fixed

- **Android aborted on session start.** `libmatrix_rtc_ffi.so` exported none of
  libwebrtc's `Java_*` JNI symbols, so its Java classes had no native
  implementation and constructing `PeerConnectionFactory` killed the process.
  The link arguments have to be emitted from the crate that owns the cdylib —
  `cargo:rustc-link-arg` from an rlib dependency is silently dropped.
- **TLS handshake failure on the LiveKit signal socket.** `rustls-native-certs`
  finds no certificates on Android; the connection now also carries the bundled
  webpki root store.
- **Panic and permanently wedged manager.** The MSC4143 `delayBeforeUse` wait
  armed a tokio timer with no reactor in context, and the resulting panic
  poisoned the manager's mutex — after which every call failed, including
  `leave()`. The core no longer arms timers at all (the delay travels to the
  consumer as `KeyMaterialSignal::use_after_ms`), and poisoned locks are
  recovered rather than propagated.
- **Media keys received before the media session attached were dropped.**
  Key signals with no handler installed were discarded, and nothing re-signalled
  them until a rotation — which needs a membership change. They are now replayed
  on attach, under identities derived through the installed identity mapper.
- **The keep-alive never ran on the FFI path.** `heartbeat()` existed and was
  driven by the native Rust facade, but nothing called it through the FFI and it
  was not exported, so a host could not even opt in.
- **The heartbeat no longer cancels and reschedules the delayed leave.** It uses
  MSC4140's `restart`: one request instead of two, and never a moment with
  nothing armed. The old approach could leak a delay whose leave then fires —
  and because MSC4354 resolves sticky conflicts by *last to expire*, that leave
  out-expires the live membership and shows the user as having left a call they
  are still in.
- **`leave()` failed when the delayed leave had already fired.** The 404 from
  cancelling an expired delay aborted the leave after the leave event had
  already been sent, leaving the state machine stuck in `Leaving`. Cancellation
  failure is now logged, not propagated — an already-fired delay is the outcome
  a leave wanted anyway.
- **`MediaSession.disconnect()` threw on a normal hangup.** LiveKit answers
  `AlreadyClosed` both for a second disconnect and for a room the SFU already
  tore down; closing an already-closed connection now succeeds.

### Known limitations

- The delayed leave is a plain, non-sticky delayed event, so it clears nothing
  from the sticky map when it fires. Crash cleanup therefore relies on the
  membership's own sticky TTL, and a client that dies without leaving lingers as
  a ghost for up to `stickyDurationMs` rather than `keepAliveTimeoutMs`. This is
  a ruma limitation rather than a protocol one — both `delay` and
  `sticky_duration_ms` are query parameters on the same send endpoint, but no
  ruma request type carries both. See the note in
  `crates/matrix-rtc-livekit/src/matrix_bridge.rs`.
- Frame encryption state is reported per participant, not per stream: the frame
  cryptor is keyed by participant identity, so a failure does not say which of
  their tracks it came from.
- Android honours only the bundled root certificate store for the SFU
  connection, so a deployment fronted by an enterprise or internal CA will still
  fail the TLS handshake. Fixing that needs `rustls-platform-verifier` support
  in `livekit-api`.
