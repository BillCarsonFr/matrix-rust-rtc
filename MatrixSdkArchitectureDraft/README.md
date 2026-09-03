# matrix-rtc (architecture draft)

An implementation of the plan in [`MatrixSdkArchitecture.md`](MatrixSdkArchitecture.md):
one crate that does everything needed to *participate* in a MatrixRTC session
([MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143)) and
nothing media-specific. The host supplies one `MatrixDriver` (all Matrix I/O)
and consumes four outputs: memberships, connections, key map, status.

```text
MatrixDriver (host)  ──▶  ParticipationManager  ──▶  memberships · connections · key map · status
```

| module | job |
|---|---|
| `session` | raw Matrix events → session state (MSC4354 sticky map, slot/room conditions, compat read side) |
| `own_membership` | our join/leave: delayed leave first, sticky refresh, MSC4140 restarts, MSC4195 delegation, compat write side |
| `connections` | which SFU connections to hold, with tokens (MSC4195) |
| `encryption` | per-member media keys over to-device (rotation policy, inbound verification) |
| `participation` | the facade wiring the four above |
| `driver` | the Matrix I/O traits the host implements |
| `executor` | spawn / sleep / clock on native (tokio) and wasm |
| `uniffi_api` | one binding surface for Swift, Kotlin, React Native and web |

Plans and status live next to each module (`*ImplementationPlan.md`,
`encryption/README.md`).

## Build and test

```sh
cargo test                                                        # unit + tests/participation.rs (black-box, mock driver)
cargo test --features uniffi                                      # + FFI tests
cargo clippy --all-targets --features uniffi -- -D warnings
cargo check --features runtime-probe --target wasm32-unknown-unknown
```

`tests/participation.rs` drives `ParticipationManager` only through its public
API against a mock `MatrixDriver` that behaves like a homeserver (echoes our
events, scripted peers answer our media key).

## Web

[`web-test-app/`](web-test-app/README.md) holds the uniffi-generated npm
bindings (via uniffi-bindgen-react-native's wasm target), the acceptance
suites through the real bindings, a demo page, a `MatrixDriver` mock and a
matrix-js-sdk driver for a real homeserver (`../demo/backend`).

```sh
cd web-test-app
npm install
npm run ubrn:web   # build the crate for wasm32 + generate TS bindings
npm test           # acceptance suites (vitest)
npm run dev        # demo page
```

## Using it (Rust)

```rust
let manager = ParticipationManager::new(
    room_id, slot_id,
    OwnIdentity { user_id, device_id },
    driver,                               // Arc<dyn MatrixDriver>
    ParticipationConfig::default(),
);
manager.on_memberships_change(Box::new(|m| render_tiles(m)));
manager.on_connections_change(Box::new(|c| hold_lk_rooms(c)));
manager.on_key_map_change(Box::new(|_map, change| set_key(change)));
// a slot you just opened is open only once sync echoed it: check `manager.session().slot_state`
manager.join(TransportIntent::Publish(livekit_transport), JoinParams::new("m.call")).await?;
// ...
manager.leave(None).await?;
```

Room-list / header info without a manager: `compute_sessions_from_events(&events, &config)`.

## Observing failures

Two API shapes, split by one question: **if the host does nothing, can this
clear on its own?**

* **Yes → it is state in `Status`.** A retry loop is live, so there is no
  single moment of failure to return. `Status::Joining`, `Connected` and
  `Leaving` each carry `impairments: Vec<Impairment>` — everything currently
  wrong, most severe first — alongside the structured per-mechanism state.
  Every `Impairment` clears by itself when the underlying operation succeeds.
* **No → it is an error or a `DisconnectCause`.** The participation is over
  or never started: `join()`/`leave()`/`open_slot()` return typed errors, and
  `Status::Disconnected(DisconnectCause)` says why we are not in the call
  (`NeverJoined`, `LeftByHost`, `SlotClosed`, `JoinFailed { progress, error }`,
  `ManagerStopped`).

Because the recoverable conditions are *state*, `on_status_change` fires when
one starts — a failing keep-alive is no longer something you have to poll for.
Hosts that want only problem transitions should diff `status.impairments()`
rather than the whole `Status`, which also changes on every healthy keep-alive
beat.

```rust
manager.on_status_change(Box::new(|status| {
    for problem in status.impairments() {
        match problem.severity() {
            Severity::Critical => banner(problem),   // we are, or are about to be, out
            Severity::Degraded => badge(problem),    // still working, but a crash would hurt
            Severity::Notice => log::info!("{problem:?}"),
        }
    }
}));
```

### Is my own membership healthy?

The three mechanisms that keep you in a call fail independently, so each is
its own value. `impairments` already flattens them; read the structured state
when you need the timestamps to render a countdown from.

```rust
let Status::Connected(s) = manager.status() else { return };
match &s.own_membership.keep_alive {
    // Healthy. `Delegated` is health too: after MSC4195 delegation the client
    // stops restarting on purpose, so do not read its frozen timestamps as a
    // stall — that is exactly why it is a variant and not a flag.
    KeepAlive::Armed { fires_at_ts, .. } => countdown_hidden(*fires_at_ts),
    KeepAlive::Delegated { .. } => {}
    // The dead man's switch is still armed but we cannot kick it: unless a
    // restart lands, the homeserver publishes our leave at `fires_at_ts`.
    KeepAlive::RestartFailing { fires_at_ts, last_error, .. } =>
        warn_user(*fires_at_ts, last_error),
    // Its full delay elapsed with no successful restart, so it has in all
    // likelihood already fired: we are probably out and have not seen the
    // leave come back yet. A replacement is being armed.
    KeepAlive::Expired { .. } => warn_probably_removed(),
    // No switch at all: this homeserver refuses delayed events. If this
    // client dies, your tile survives until the membership expires.
    KeepAlive::Unavailable { permanent, .. } =>
        warn_no_cleanup_on_crash(*permanent, s.own_membership.membership.expires_at_ts),
}
// Nobody can see you while this is not `Present`. `Missing` is re-sent by the
// self-heal; `Excluded` deliberately is not — only the room state that caused
// it changing can clear it, so surface the reason.
match &s.own_membership.roster {
    RosterPresence::Present | RosterPresence::AwaitingEcho => {}
    RosterPresence::Missing { .. } => warn_invisible(),
    RosterPresence::Excluded { reason } => warn_excluded(reason),
}
```

### Who can hear whom?

`key_map()` is the **inbound** map: it says who *you* can hear, never who can
hear *you*. The per-tile `media_key` answers both, and carries the reason a
peer's key was discarded — which is the answer to "why can't I hear Bob?".

```rust
for tile in manager.memberships() {
    let Some(key) = &tile.media_key else { continue };   // unencrypted call
    if !key.holds_our_key { badge(&tile, "cannot hear you yet"); }
    if !key.have_their_key {
        // `rejection` is `Some` when their key arrived and was discarded:
        // NotCrossSigned, Cleartext, SenderMismatch, ...
        match &key.rejection {
            Some(why) => badge(&tile, &format!("cannot be decrypted: {why}")),
            None => badge(&tile, "connecting…"),
        }
    }
}
// `own_member_id()` is how you recognise your own LiveKit participant, and
// your own entry anywhere else. `(user_id, device_id)` is not a substitute:
// one device may hold several RTC members, and a rejoin mints a fresh id.
let me = manager.own_member_id();
```

`on_key_rejected(cb)` fires on each discarded key, for logging and telemetry.
It is secondary to the latched per-tile `rejection`, which is what a UI
attaching late still finds.

### Which connections are unreachable?

A connection with no token is silently *absent* from `connections()`, and one
whose token expired is present but unusable (it is kept on purpose — dropping
it would break a host that is still connected). Both are named here.

```rust
for problem in manager.connection_problems() {
    match problem.kind {
        // Never minted: the media of `member_ids` is unavailable until the
        // retry at `retry_at_ts` succeeds.
        ConnectionProblemKind::NoToken =>
            no_media_for(&problem.member_ids, &problem.last_error, problem.retry_at_ts),
        // Still handed out, but past its `exp`: your LiveKit connect will
        // fail. `ConnectionData::expires_at_ts` carries the same deadline.
        ConnectionProblemKind::TokenExpired =>
            reconnect_will_fail(&problem.service_url, &problem.last_error),
    }
}
```

### One caveat about "unknown"

`session().slot_state == None` and `negotiated_encryption == None` mean
*unknown*, and `failed_reads` says which kind: empty means "there is nothing
there", `SessionRead::Slot` in it means "we could not find out". A padlock
rendered from `negotiated_encryption` alone shows *unencrypted* for a room
whose slot read timed out. Entries clear when a live state update supplies the
value. If `join()` went ahead before the seed finished, `JoinedBeforeSeed` is
latched for the participation: the slot pre-check was skipped and the
encryption decision fell back to local config.
