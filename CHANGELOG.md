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

- **`CallOptions::legacy_element_call: bool` is now
  `CallOptions::element_call_compat: ElementCallCompat`** (`Off` / `StickyEvents`
  / `StateEvents`). There are two pre-2026 Element Call generations to render for
  now, and they disagree about the *carrier* of a membership rather than just its
  fields, so they are mutually exclusive by construction — an enum makes the
  impossible fourth state unrepresentable. `LiveKitTransportConfig` likewise
  gains a `token_endpoint: TokenEndpoint` field. The load generator's
  `--legacy-element-call` flag becomes `--element-call-compat <off|sticky|state>`,
  and `join_and_record`'s `LEGACY_ELEMENT_CALL` now takes `sticky` or `state`
  (any other value still means `sticky`).

- **`sendStickyEvent` and `sendStateEvent` now return the event id.**
  Kotlin/Swift: `String`; JS: resolve to matrix-js-sdk's `{event_id}` (a bare
  string and `{eventId}` are accepted too), and resolving to anything else is
  now a failed send rather than a silent success.

  The id is what MSC4075 needs: a call notification must carry an `m.reference`
  relation to the membership event that justifies it. It is not optional because
  every Matrix send responds with one — `PUT /rooms/{id}/send/{type}/{txn}` and
  `PUT /rooms/{id}/state/{type}/{key}` both do — so an implementation that
  cannot produce it is broken, and failing loudly beats a call that joins fine
  and quietly never rings. `sendStateEvent` is included because in `StateEvents`
  compat mode the *membership* goes out through it; nothing reads the id of a
  slot event.

  The alternative, recovering the id from our own membership echoing back
  through sync (what matrix-js-sdk does), puts a full sync round trip in front
  of the ring.
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
- **`CommandSenderCallback` is async, and so is every manager-handle method
  that touches the core.** In Kotlin they are `suspend fun`s; in Swift, `async`.
  A synchronous callback forced hosts to bridge to their async Matrix client
  themselves — `runBlocking` on a dedicated dispatcher on Android — while every
  corresponding matrix-rust-sdk call is already async.

  `setCommandSender` now takes the interface directly rather than a boxed one
  (uniffi `with_foreign`), and `CommandSenderError` gained a `Display` impl,
  which uniffi requires of a foreign-trait error type.

  This was blocked until now by a subtlety worth recording: uniffi's async
  exports must be `Send`, while `matrix-rtc-core`'s command traits were
  `?Send` on every target to accommodate WASM's JS-backed futures. They are now
  `Send` everywhere except `wasm32`, via a `cfg_attr` pair that **every
  implementation of those traits must carry**. A consequence is that
  `matrix-rtc-wasm` no longer compiles for a host target — its crate body is
  gated to `wasm32` and a `Clippy (wasm32)` CI job type-checks it instead.

  Internally this removed the blocking bridge entirely: no `block_on`, and the
  keep-alive is a spawned task rather than a thread per session (it needed a
  thread only because its future held a `std::sync::MutexGuard` and so was not
  `Send`).
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
- **`CommandSenderCallback.sendDelayedStateEvent(roomId, eventType, stateKey,
  contentJson, delayMs)` is new.** It is only ever called in
  `ElementCallCompat.STATE_EVENTS`, where the membership is room state and so its
  dead man's switch has to be too — the message-like `sendDelayedEvent` has no
  state key to send it under. Hosts not doing Element Call interop can implement
  it as a thrown `SendError`; the SDK will not reach it. See Added below.
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

- **MSC4075 call notifications, so a call can ring.** A membership says who is
  *in* a session; it never said who should be *summoned* to one, which is why a
  mobile client had nothing to raise an incoming call from. Passing `notify` on a
  join now sends an `m.rtc.notification` (wire:
  `org.matrix.msc4075.rtc.notification`) as a sticky event, with an `m.reference`
  relation to our own `m.rtc.member` event.

  - `NotifyConfig { notification_type: Ring | Notification, intent, lifetime_ms,
    mentions }` on `JoinSessionParams::notify`, `CallOptions::notify`,
    `FfiJoinSessionParams.notify` and the WASM join params' `notify`. Unset —
    the default — joins quietly.
  - **Only the member who *starts* the session notifies.** MSC4075 leaves the
    question open, but every joiner sending one would ring the room once per
    participant, so the core suppresses it when anybody *else* is already in the
    session — our own memberships are excluded from that count, both the current
    one (the host feeds the whole sticky map, which contains our echo) and a
    previous one still held as a candidate across a `leave()` in the same
    process. This matches matrix-js-sdk's `oldMemberships.length === 0`, and the
    captured Element Call exchange in
    `skills/e2e-testing/references/Alice-Bob-Call-Events.md`, where Bob starts
    and Alice, joining second, sends nothing. The application still decides
    whether to ask at all — Element Call passes the notification type only when
    the app *starts* a call, never when it joins one.
  - **The content carries the call fields twice, on purpose.** MSC4075 as
    written nests them under `application`; nothing deployed reads them there.
    Element Call puts `notification_type`, `sender_ts`, `lifetime` and
    `m.call.intent` at the top level, and so does ruma's
    `RtcNotificationEventContent` — which is what matrix-rust-sdk hands a mobile
    client, and which *requires* all three at the top level, so a purely nested
    event fails to deserialize there and rings nobody. Both shapes are written;
    a reader of either ignores the other as unknown fields. `element_call_compat`
    additionally strips `application` and `m.text`, for the byte-exact legacy
    shape.
  - Not implemented, and a separate change: the receiving rules (push rules,
    `m.mentions` match, an open slot, not already joined), lifetime expiry
    against `origin_server_ts`, optimistic ringing, `m.call.ring.ack`
    acknowledgements, and the sender-side "still ringing" indication. On mobile
    the first signal is a push notification, which arrives outside this
    workspace entirely.
  - `skills/msc/references/msc4075.md` vendors the proposal.

- **An automated Element Call interop test.** Both compat dialects were
  validated against real Element Call by hand, once; nothing re-checked them, so
  a regression in a dialect, an identity derivation or a key binding stayed
  invisible until somebody repeated a manual call. Now a Rust client and a real
  Element Call join the same call in CI, and each side asserts it can see,
  key-exchange with and decrypt media from the other:

  - `demo/backend/docker-compose.interop.yml` — a TLS overlay on the existing
    backend stack (nginx fronting `*.m.localhost` with a CA minted at up time,
    plus Element Web). Element Call is a widget in an iframe and needs a secure
    context, which `http://localhost` cannot give it. Element Web **ships
    Element Call embedded**, so there is no Element Call service to run — the
    version under test is the one users get. The base compose file is untouched
    and `make test-e2e` is unaffected. See `demo/backend/INTEROP.md`.
  - `crates/matrix-rtc-livekit/examples/interop_peer.rs` — the Rust half, driven
    over stdin, reporting JSON lines on stdout.
  - `interop/` — the Playwright suite that owns the browser and drives that
    peer, parametrised over Element Call's `Compatibility: state events`
    (`ElementCallCompat::StateEvents`) and `Matrix 2.0` (`StickyEvents`).
    `ElementCallCompat::Off` has no Element Call counterpart yet and stays
    `e2e_call`'s job.
  - `.github/workflows/interop-element-call.yml` — nightly against Element Web
    `develop` (the drift signal), on demand, and on PRs labelled `interop`
    against a pinned image.

- **`openSlot(roomId, slotId, applicationType, encryption)` and
  `closeSlot(roomId, slotId)` on the manager handle**, wrapping the core calls the
  native path has always had. A room has no slot until somebody opens one, and a
  host that reports slot state projects every member out of a room that has none —
  so in a room where no other client opens slots, this is what makes a call
  possible at all. That includes every room whose other participant is Element
  Call: no generation of it publishes `m.rtc.slot`. `encryption` is the new
  `FfiSlotEncryption` (`PerMember`, or `Other { encryptionType }`), required in an
  encrypted room and forbidden elsewhere. A `slotId` that does not start with
  `{applicationType}#` is refused here rather than by the homeserver, which would
  accept a slot every client then treats as closed.

- **Element Call interop over the FFI**, so an Android or iOS host can be tested
  against Element Call rather than only against another Rust client. Both
  generations, selected per join with
  `FfiJoinSessionParams.elementCallCompat: FfiElementCallCompat?`
  (`OFF` / `STICKY_EVENTS` / `STATE_EVENTS`, `null` meaning `OFF`).

  Chosen once and remembered for the room, because it is one decision rather
  than a wire-format flag: it also fixes the `member.id` the session joins with,
  how an inbound media key is bound to a membership, the SFU participant
  identity, and which authorisation-service endpoint mints the token. The media
  session reads it back from the join rather than taking it again — those two
  disagreeing is not an error but a silence, a fully connected call in which
  nothing decrypts.

  The outbound half needs no host change: memberships, the delayed leave and
  media keys are rewritten (or re-routed to room state) inside the binding. The
  inbound half does, because reading a legacy membership means parsing a content
  that states its transports somewhere else, its membership nowhere at all, and —
  a generation earlier — is not a sticky event:

  - `setCurrentMembership(roomId, memberEvents, legacyStateEvents)` takes raw
    event content and does the parsing in Rust. Both sources in one call, because
    it replaces the room's whole membership: fed separately, each call would wipe
    the other's members and the roster would flicker between the two halves of
    the call. Spec-current hosts keep using `setCurrentStickyState`.
  - `receiveLegacyEncryptionKey(sender, contentJson, wasEncrypted,
    senderDeviceId, senderIsCrossSigned)` ingests
    `io.element.call.encryption_keys`. Easily forgotten, and expensive to forget:
    the roster fills in, everything looks joined, and every remote tile stays
    black.
  - In `STATE_EVENTS` only: implement `sendDelayedStateEvent` (see Breaking).

  Slots need no host handling. Element Call publishes no `m.rtc.slot` in either
  generation, so the truthful "no slots" a host feeds would resolve the session
  closed and project out every member, itself included. In `STATE_EVENTS` the
  mode absorbs it: the join calls the new
  `RtcSessionManager::forget_room_slots(room_id)` — the way back from
  `on_room_slots_received`, which was otherwise irreversible, since an empty slot
  list means "no open slots" rather than "I have nothing to say" — and later slot
  updates for that room are ignored. The condition returns to unenforced, which
  is the honest answer for a generation that predates the concept. `STICKY_EVENTS`
  keeps it enforced, because those rooms are otherwise spec-shaped and a slot in
  one is meaningful; a host feeding slot state there needs the slot opened, as
  the native tools do with `--open-slot`.

  The translation itself is `matrix-rtc-bridge`'s `compat`, unchanged and shared
  with the native path; `matrix-rtc-ffi` gained a dependency on that crate
  (without its `matrix-sdk` feature — pure JSON, no git dependencies, so the slim
  mobile artifact is unaffected).

- **Interop with *pre-sticky* Element Call — membership as room state
  (`matrix-rtc-livekit`'s `compat::element_call_state`).** One generation older
  than the dialect below: before MSC4354 existed, Element Call carried MatrixRTC
  membership as `org.matrix.msc3401.call.member` **room state events**, using the
  state key for per-device keying and room state for the delivery guarantee
  stickiness now provides.

  Four things change at once, so unlike its sibling this cannot be an additive
  rewrite — a call joined this way is visible to that generation and to nobody
  else:

  - **Carrier.** The membership is a state event keyed
    `_{user}_{device}_{application}{call_id}`, sent with
    `PUT /rooms/{id}/state/...`, and the dead man's switch becomes a *delayed
    state* event. That last one is an improvement the spec path still lacks: a
    delayed state event with `{}` content genuinely empties our membership,
    whereas a delayed sticky leave clears nothing from the sticky map, so crash
    cleanup there still rides on the sticky TTL.
  - **SFU identity.** The plain `{user}:{device}` string the legacy
    authorisation service mints, not the MSC4195 hash of
    `[user, device, member_id]`. All four derivation sites — our own identity,
    the core's `RtcIdentityMapper`, `remote_identity`, and the local frame
    cryptor's compare — now share one `Arc`, because a divergence there is not an
    error but a silence: peers sit in the roster with no media and their keys land
    under an identity the SFU never assigned.
  - **Token.** `POST /sfu/get` with `{room, openid_token, device_id}`, not
    MSC4195's `/get_token`. Deliberately *not* a 404-driven fallback: the endpoint
    is bundled with the identity derivation and the membership carrier, both
    decided before any HTTP happens, so a 404 cannot retroactively change what we
    already published — and succeeding with the wrong derivation yields a fully
    connected session in which nothing decrypts and nobody appears.
  - **Content.** `application` as a string plus `call_id`/`scope` instead of a
    `slot_id`, `membershipID` instead of `member.id`, an in-content
    `created_ts + expires` lifetime, and `foci_preferred` +
    `focus_active.focus_selection` instead of `transports`.

  The core still speaks only MSC4143. Inbound, state events are read from the SDK
  store (`m.call.member` *is* in sliding sync's `required_state`, unlike the
  MSC4143 slot type) and translated into synthetic sticky memberships with
  `EventOrigin::Claimed`, with two things resolved in the translation because
  nothing downstream can: expiry, since the core has no membership deadline and no
  timer, and `focus_selection: "oldest_membership"`, which makes a peer's SFU a
  property of the room's whole membership rather than of that peer's event.

  Two consequences worth knowing. The bridge grows a room-state wake source and a
  30 s poll in this mode, because a state-carried call produces **no sticky
  traffic at all** and a bridge waiting only on sticky events would seed once and
  sleep through the whole call. And the slot condition is left *unenforced*
  (`SlotKnowledge::Unsupplied`) rather than reported empty: that generation has no
  slot concept, so saying "no slots" would resolve every session closed and drop
  every member, us included.

  Opt-in in both directions, unlike the sticky dialect's always-on reader — this
  one reads a different event type in a different part of the room, so left on
  everywhere any room that ever hosted an old Element Call would show a call that
  ended months ago.

- **Interop with pre-2026 Element Call (`matrix-rtc-livekit`'s `compat`
  module).** Element Call on the JS SDK is the only other MatrixRTC
  implementation available to test against, and it still speaks the wire format
  from before the 2026 MSC4143 rewrite: `member: {user_id, device_id, id}` with
  no `membership`, a flat `rtc_transports` array, a leave whose content is a
  bare sticky key, and media keys as `io.element.call.encryption_keys`.

  All of it is confined to two JSON funnels at the crate's edge — the core never
  sees a dialect parameter or a legacy field. **Reading** the old format needs no
  flag and is always on: every rule fires only where the modern field is absent
  and its legacy counterpart is present, so a spec-shaped event is passed through
  untouched. **Writing** it is opt-in per call via
  `CallOptions::element_call_compat = StickyEvents`, since it is the half that changes what
  peers see. A join stays MSC4143-valid, the legacy fields riding alongside; a
  leave and a media key cannot be both at once, so a leave becomes the legacy
  bare-sticky-key content (Element Call has no `membership` field, and a spec
  leave padded to satisfy its validator would read to it as still *joined*) and
  keys go out under the legacy type *instead of* the spec one, a to-device
  message having only one type. Delete the module and the flag once Element Call
  catches up.

- **`EventOrigin::Claimed`** — a sending device a member event asserts rather
  than one decryption proved. Element Call runs as a widget and the widget API
  gives it no decryption metadata, so a self-asserted device is the only one it
  can state or read; without somewhere to put it, no media key can travel in
  either direction with such a peer. Ranked below `Encrypted` throughout:
  `was_encrypted()` stays `None`, so it never satisfies the "member events MUST
  be encrypted in an encrypted room" rule, and an inbound key must still arrive
  Olm-encrypted from the very device named. Produced only by the compatibility
  path, and only where no authenticated device was available.

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
