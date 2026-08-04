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
- **The SDK now owns the `member.id`.** `FfiJoinSessionParams.membershipId` and
  `MediaSessionConfig.memberId` are gone; `join` returns the id it generated, and
  `connect_media_session` reads it from the join. Drop both fields and use the
  return value (or `ownMemberId(roomId, slotId)`, which cannot go stale).
  A host-chosen id was silently destructive when reused across joins: the
  MSC4195 participant identity is derived from it, so a rejoin kept the identity
  peers already held a key for while our key index restarted at 0 — every peer
  then decrypted our media with the previous call's key and never recovered.
  MSC4143 requires a fresh id per join, and nothing validated that.
- **`FfiCallEvent` gained a `FrameEncryptionState` variant.** uniffi generates
  the enum as a sealed class, so exhaustive `when` blocks without an `else` need
  a new arm.
- `FfiJoinSessionParams` gained `stickyDurationMs: Long?` — pass `null` for the
  default.
- WASM hosts: the JS client must implement `restartDelayedEvent(roomId,
  eventId)` and accept the new `durationMs` argument on `sendStickyEvent`.
  `join` no longer accepts `membership_id` and now resolves to the generated
  `member.id`; `ownMemberId(...)` reads it back.
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
- `ownMemberId` on the manager and session handles (FFI and WASM), and
  `RtcSession::own_member_id` / `RtcSessionManager::own_member_id` in the core.
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
  on attach, under identities derived through the installed identity mapper, and
  after the key-import listener is wired — so replayed keys surface as
  `KeyImported` events instead of being applied invisibly.
- **The keep-alive never ran on the FFI path.** `heartbeat()` existed and was
  driven by the native Rust facade, but nothing called it through the FFI and it
  was not exported, so a host could not even opt in.
- **The heartbeat no longer cancels and reschedules the delayed leave.** It uses
  MSC4140's `restart`: one request instead of two, and never a moment with
  nothing armed. The old approach could leak a delay whose leave then fires —
  and because MSC4354 resolves sticky conflicts by *last to expire*, that leave
  out-expires the live membership and shows the user as having left a call they
  are still in.
- **We could try to send our own media key to our own device.** The recipient
  filter excluded us by `member.id`, which is fresh per join — so a stale
  membership of our own device (crash without leaving, rejoin inside its sticky
  lifetime) read as a peer. Olm has no session with the sending device, so that
  send failed; and since our own user+device under a new `member.id` also forces
  a key rotation, the ghost forced rotations and broke them for as long as it
  stayed visible. The filter now matches on user + device. Other devices of our
  own user remain ordinary recipients.
- **Media keys are no longer broadcast to every device of a user.** A membership
  that named no sending device fell back to `"*"`, which hands the key to devices
  that are not in the call — and, for our own user, to this device. Such a
  membership is now logged and skipped.
- **One unreachable recipient no longer abandons a key rotation.** Distribution
  stopped at the first failing send, so later recipients got nothing *and* the
  new key was never stored or signalled: we kept encrypting with the old key and
  the rotation vanished silently. Failures are now logged per recipient and the
  rollout continues. (Recipients are still recorded as shared with regardless of
  outcome, so a failed send is not retried — see the `TODO` in
  `encryption/mod.rs`.)
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
