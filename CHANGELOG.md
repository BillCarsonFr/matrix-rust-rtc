# Changelog

Notable changes to matrix-rust-rtc. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project has not
had a tagged release yet, so everything so far lives under Unreleased.

Entries begin with the Android integration work — earlier history is in the git
log only.

## Unreleased

### Breaking

Hosts implementing the FFI/WASM command sender must update. All of these are
compile errors, not silent behaviour changes.

**v0.2.0 sweep** — deliberately batched into one release while there is still a
single integrator, rather than dripped out over several:

- **`sendToDeviceMessage` now takes a recipient *list* and returns a result per
  recipient**, replacing the per-device call whose only failure channel was an
  exception. That exception landed inside the core's loop over members, so one
  undeliverable recipient silenced every member after it; and because the core
  had no way to learn who was actually served, it recorded everyone as holding
  the key and never retried. Now: return `FfiToDeviceDelivery { userId,
  deviceId, error }` per recipient — a recipient you report as delivered is
  never re-sent to, one you mark failed or omit is retried on the next rollout.
  Reserve `Err` for "the batch could not be attempted at all". This also removes
  a loop and several FFI crossings, and maps onto matrix-rust-sdk's own
  recipient-list `sendToDeviceMessage`.
- **`restartDelayedEvent` / `cancelDelayedEvent` take `delayId`, not
  `eventId`.** The value was always the MSC4140 delay id; the parameter name
  said otherwise, and transposing the two fails silently, surfacing minutes
  later as the dead man's switch retiring a live membership. Renamed everywhere,
  including `sendDelayedEvent`'s documented return.
- **One way in for membership: `setCurrentStickyState(roomId, events)`.** It
  carries the room's *complete* current sticky state and **replaces** what the
  core held — a member absent from it is gone, and an empty list clears the
  room. `initialStickyForRoom`, `onStickyEventsSnapshotReceived`,
  `stickyUpdateForRoom` and `onStickyEventsUpdateReceived` are all gone, along
  with the `StickyEventUpdate` record.

  A delta bought nothing: the core discarded the `previous` half of an update,
  flattened every removal to a plain leave, and an explicit leave arrives inside
  the current state anyway as a leave-shaped sticky replacing the join under the
  same key. Meanwhile `matrix-sdk-ffi` hands hosts a full snapshot on every
  change (it collapses the SDK's delta before it crosses the boundary), so the
  current state is the shape hosts already have — and the core now does the
  diffing once instead of each host doing it. Feeding a shrunken set is how an
  expired membership departs.
- **`RtcSessionHandle` is gone.** Its `leave()` could only ever return an error,
  and a session created through it was invisible to `RtcSessionManagerHandle`,
  so nothing else could act on it. Its one unique capability — observing the
  roster — is now on the manager. Drive sessions through the manager handle.
- **`FfiAudioFrame.data` is a `ByteArray` of little-endian `int16`**, not a
  `List<Short>`. uniffi renders a sample list boxed — roughly 48 000 objects a
  second at 48 kHz mono, the single biggest cost this API imposed on a host.
  Read it with
  `ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer()`;
  `AudioTrack`/`AudioRecord` take the byte form directly.
- **`FfiCallEvent.ActiveSpeakers` carries `speakers: List<FfiSpeakingMember>`**
  (`memberId` + `level`) rather than bare member ids. The level arrives in the
  same transport event, and dropping it forced hosts to meter the PCM
  themselves to answer "how loud".
- **`EncryptionManager::get_encryption_keys()` is gone.** It derived participant
  identities itself, ignoring the installed identity mapper, so anything
  importing from it addressed keys under identities the transport never sees —
  indistinguishable from the keys never arriving. Use
  `replayKeysToHandler`/`replay_encryption_keys`, which derive identities the
  same way the live signal path does.

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

- **Publishing raises `StreamStarted` against our own `member_id`, and
  `MediaSession.setLocalMuted(kind, muted)` mutes it.** Our own publications
  never arrive as transport events — nothing subscribes us to ourselves — so a
  host's own roster entry lacked the microphone it was actively capturing, and
  alone in a call nothing later prompted a re-read to correct it. Hosts were
  shadowing their own mute state to render themselves truthfully.

  Muting goes to the transport as well as the roster, so peers are told: a muted
  sender and one that has merely stopped pushing frames look the same to a peer
  otherwise, and the second is what a wedged client looks like.
  `StreamMuted`/`StreamUnmuted` are emitted against our `member_id` exactly as
  for anyone else, so a host can render itself from the same source.
- **`subscribeMembershipSnapshots(roomId, slotId)` on
  `RtcSessionManagerHandle`.** The roster previously existed only on
  `RtcSessionHandle`, while media requires the manager — so a host driving the
  manager could read `memberCount` but never learn *who* was in the call, and
  polling a count once a second was the only way to notice a change. Returns
  `null` when no session exists for that slot yet. This is also what a host
  watches to be told a call has started.
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
- **`FfiCallEvent.FrameEncryptionState` now carries a `diagnostic`.** The frame
  cryptor reports *that* it cannot decrypt, never why; this adds whether any key
  was installed for that participant (`NoKeyInstalled` / `KeysInstalled(indices)`),
  which splits a `MissingKey` into the two cases needing different
  investigations — nothing ever arrived, versus frames carrying an index we have
  not been given yet.
- **`FfiCallEvent.KeyDiscarded` reports a key that arrived and was *refused*,
  with the reason** (`FfiKeyRejection`: cleartext, not cross-signed, room /
  sender / device mismatch, unverifiable device) and the device that sent it.
  Previously these reasons were warn-logged inside the core and never crossed the
  FFI, so a refused key was indistinguishable from one that never arrived — a
  trust problem looking exactly like a delivery problem. `NotCrossSigned` in
  particular is a "verify this device" prompt, which is why the reason is typed
  rather than a message.
  Corresponding core API: `EncryptionKeySignalHandler::on_key_discarded`
  (defaulted, so existing handlers need no change) and `DiscardedKey`.
- Core-side log lines that an Android integration had to add by hand to make
  failures distinguishable: recipient *and* index on every key send (flagging a
  send addressed to ourselves), a per-rollout delivery summary naming members the
  key could not be delivered to, the provenance of each received key
  (`from user/device cross_signed=…`) before it is judged, and frame-encryption
  changes as a transition (`Ok -> MissingKey`) rather than a bare state.

### Fixed

- **Key rotation never took effect: we advertised a new key and kept encrypting
  with the old one.** `KeyProvider::set_key` only fills the key *ring*; the index
  a sender stamps lives on its frame cryptor, which is created at index 0 and
  moves only via `set_key_index` — something nothing called. Two consequences:
  any peer joining after a rotation held only the new index and could decrypt
  nothing from us, and **rotate-on-departure did not deliver the forward secrecy
  it exists for** — we reported the rotation and carried on using the key the
  departed member holds.

  The key bridge now moves our sender onto each of our own keys as it activates
  (at activation, so MSC4143's `delayBeforeUse` still gates it), and the
  connection re-asserts the current index after every publish, because a track
  published after a rotation gets a fresh cryptor at index 0. A key the ring
  refused never moves the sender — that would make our media undecryptable for
  everyone rather than for one peer.

  No test had ever rotated a key: in both existing e2e scenarios the second peer
  joins inside the 10 s grace period, so the rollout reuses the current key. The
  new `e2e_call_rejoin_in_the_same_process` scenario is what exposed it.
- **The second call in a process distributed no media key at all.** A session is
  keyed by `(room_id, slot_id)` and outlives `leave()` on purpose, so a rejoin
  started with the previous call's roster already in place. Key distribution was
  only ever driven by a roster *change*, and there is none when the incumbent's
  membership is unchanged across our leave and rejoin — so the first call in a
  process distributed its key and every later one silently distributed nothing,
  leaving peers at `MISSING_KEY` for the whole call. `join()` now drives the first
  distribution itself, unconditionally.
- **A previous participation of our own device counted as a peer.** MSC4143 mints
  a fresh `member.id` per join, so the superseded one lingered in the roster as a
  phantom member: the media layer opened a receive stream for it and waited for a
  key that could never arrive, and it forced a key rotation whose only recipient
  was a device that cannot receive its own to-device messages. It is now excluded
  from the joined set (matched on user *and* device, so other devices of our own
  user stay ordinary peers), and `leave()` republishes the roster immediately
  rather than waiting for the host's next sticky delta.
- **The same key index was imported repeatedly.** The eager first-key signal was
  gated on `shared_with` being empty, which is also the state a rollout with no
  recipients leaves behind — so in a solo call every membership update re-signalled
  the same index, and each reached the transport as a fresh key import. Gated on
  the index last signalled instead.
- **A peer's rekey at an index we already held was discarded.** `OutdatedKeyFilter`
  stamps receive time (MSC4143 puts no creation timestamp on the wire) and treated
  an equal timestamp as stale, so a rekey arriving in the same millisecond was
  dropped and that peer's media never decrypted. Equal timestamps now pass, one
  entry is kept per index holding the newest material, and identical
  re-deliveries are recognised on the key material itself.
- **A rotation nobody could decrypt still waited out its `delayBeforeUse`.** The
  delay exists to keep members who hold the *outgoing* key decrypting while its
  replacement propagates. After the call had sat empty the key we held had gone to
  nobody, so waiting protected no one and left the arriving member — holding
  nothing — undecryptable for the full delay instead of just for its own key's
  delivery. It is now skipped when no member present holds the outgoing key.
- **The coalesced follow-up key distribution was always dropped.**
  `ensure_key_distribution` recursed *before* clearing its in-progress flag, so
  the follow-up took the "already in progress" branch and returned without rolling
  out anything. Replaced with a loop.
- **Keys imported before a membership arrived could be lost.** The engine held one
  pending index per identity, so a second import before the sticky membership
  landed overwrote the first; identity-keyed buffers also survived a departure and
  could resurface against a later member. Both fixed.
- **A peer using the stable `sticky_key` spelling vanished from the call.** The
  core accepted only `msc4354_sticky_key`, while matrix-rust-sdk reads both, so
  such a member event failed to deserialize and was dropped whole — silently.
  Both spellings are now accepted, and an unparseable member event is logged
  instead of ignored.
- **Keys held at `Call::join` time surfaced no `KeyImported`.** The native facade
  never replayed them, and installed the identity mapper *after* the signal
  handler, so a key signalled in between was imported under a fallback identity
  the SFU never uses. Both corrected.
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
  membership is now logged and skipped, and the matrix-rust-sdk bridge no longer
  implements a `"*"` fan-out at all: `sendToDeviceMessage` always names exactly
  one device, and a host implementation must not widen it.
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
