# Matrix RTC SDK Architecture (Draft)

A plan for a single crate — `matrix-rtc` — that replaces the current
multi-crate workspace while behaving the same. Two ideas drive it:

1. **The media plane leaves the SDK.** The crate produces exactly two things a
   host needs to run a call with *any* LiveKit SDK (livekit-js, Swift, Kotlin,
   Rust): the list of `ConnectionData` (SFU URL + JWT) to connect to, and the
   `KeyMap` (member → media key) to feed into frame encryption. No libwebrtc,
   no transport engine, wasm-compatible everywhere.
2. **One Matrix seam: `MatrixDriver`.** The trait mirrors the widget
   `MatrixDriver` in matrix-rust-sdk so that implementation drops in
   unchanged; the same trait is exported through UniFFI as a foreign trait,
   so a matrix-js-sdk-backed driver can be written later.

```
                 host app (any platform)
        │ MatrixDriver impl        ▲ ConnectionData[] + KeyMap
        ▼                          │  → host's own LiveKit SDK
┌──────────────────────────────────────────────────┐
│ matrix-rtc                                       │
│  participation ── owns ──┬─ session              │
│   (facade)               ├─ connections          │
│                          ├─ own_membership       │
│                          └─ encryption           │
│  driver (MatrixDriver traits)        bindings    │
└──────────────────────────────────────────────────┘
```

## Modules

### `session`

Pure projection of Matrix events into session state. No I/O. The session is
the **single converter** from raw Matrix events to the Member
representation — sticky *and* state, modern *and* legacy. Nothing is
pre-translated into synthetic sticky events; hosts feed everything into one
funnel and the session dispatches on event type:

| event type | converted to |
|---|---|
| `m.rtc.member` (sticky; modern + 2025 dialect, permissive read always on) | member candidate |
| `org.matrix.msc3401.call.member` (state; only with `StateEvents` compat) | member candidates, one per device |
| `m.rtc.slot` (state) | slot condition |
| `m.room.member` / `m.room.encryption` (state) | room conditions |

- **All reads go through `SessionSnapshot`** — a plain, cloneable value
  carrying the joined projection plus its metadata (`slot_state`,
  `negotiated_encryption`, `start_ts`, `excluded_candidates`) and the
  conveniences (`member_count()`, `is_active()`).
- `compute_sessions_from_events(events, config) -> Vec<SessionSnapshot>` —
  static, cheap, and returns *values, not subscriptions*: nothing updates
  unless you pass a driver (i.e. build a `Session`). This is the room-info
  path: matrix-rust-sdk's `room_info` computation calls it on every room
  update and populates its fields from the snapshot. One slice of all
  events, many rooms at once; grouped by `(room_id, slot_id)` — MSC3401
  candidates land in the well-known `LEGACY_SLOT_ID`.
- `Session::new(room_id, slot_id, driver: RoomEventsDriver, config)` — the
  **live** entry point: the session seeds itself (`read_state` /
  `read_events`) and consumes the driver's streams; consumers use
  `snapshot()` and `subscribe()`. There is no manual `update` — feeding a
  session is the driver's job.
- Converters live in `session::convert`, **one file per generation**, each
  producing the normalized internal candidate (member + source generation +
  origin + one `expires_at` from either the sticky duration or MSC3401's
  `expires`). Deleting a legacy generation = deleting its file plus one
  dispatch arm.
- **Join-condition projection** (behavior kept from today): candidates are
  re-projected whenever inputs move; exclusions carry a reason. Two
  conditions are **scoped by generation**: `SlotClosed` and
  `UnencryptedInEncryptedRoom` never apply to MSC3401 candidates (no slot
  concept; state events are unencrypted by nature — MSC4143 requires
  encryption only of sticky member events). Room-state conditions stay
  unenforced until the host supplies that state.
- **Encryption negotiation**: the slot prescribes whether RTC data is
  encrypted (mechanism required in encrypted rooms, forbidden in unencrypted
  ones, taken at face value when unknown); the result overrides any local
  config at join time.
- Accepts both stable (`m.rtc.*`) and unstable (`org.matrix.msc4143.*`) types
  inbound; `wire_event_type` translates outbound.
- Device attribution on MSC3401 candidates is *claimed* (from state-key /
  content), never verified — key verification treats it accordingly (that
  generation has its own to-device key type anyway).

### `connections`

Maps a `Session` to the SFU connections a host must hold (multi-focus,
MSC4195). `ws_url` is the connection index.

- `ConnectionData { jwt_token, ws_url }`, `ConnectionWithMembers`.
- `members_for_connection_data(conn, session) -> Vec<Member>`.
- `ConnectionsManager`: `subscribe_connections()`,
  `subscribe_connections_with_members()`, and `add_own_transport(intent)` —
  which performs transport discovery (`GET /rtc/transports` via the driver)
  and the MSC4195 `/rtc/livekit/get_token` exchange (the legacy `/sfu/get`
  in `StateEvents` compat), or returns an existing token. Token refresh on
  expiry lives here too.
- The participant-identity derivations as pure fns: MSC4195 pseudonymous
  hash, and the legacy `{user}:{device}` form.

### `own_membership`

The join/leave state machine for *our* membership. Talks only to the
`OwnMembershipDriver` slice of the driver.

- `JoinStatus { has_fetched_transports, has_fetched_initial_member_list,
  has_created_transport_token, has_sent_delayed_leave_event,
  has_sent_member_join_event, has_delegated_delayed_event,
  has_started_heartbeat }`
- `ConnectedStatus { delayed_event_kick_ts, heartbeat_last_restart_ts,
  delegation_setup_ts }`, `LeaveStatus { transport_disconnected,
  leave_event_sent }`.
- `Manager::new(session, driver, compat, on_transport_created)`,
  `join(intent)`, `leave(reason)`.
- Behavior kept from today: fresh `member_id` per join; delayed leave armed
  *before* the join event; keep-alive = MSC4140 `restart` (never
  cancel+reschedule) plus sticky re-send before `duration_ms` elapses;
  optional delegation of the delayed leave to the SFU (MSC4195); leave sends
  `leave_reason {code, reason}` and cancels/settles the delayed event.
- `TransportIntent::Publish(transport)` or `ReceiveOnly { can_subscribe }` —
  receive-only members (recorders, observers) are first-class.
- Owns the **write side of compat**: with `StickyEvents`/`StateEvents`
  selected, *our own* events are rendered in that dialect (the opt-in half —
  it changes what other clients see). The read side lives in the session's
  converters.
- Slot administration (`open_slot`/`close_slot` state events) rides the same
  driver slice.

### `encryption`

Per-member media key exchange over to-device messages (MSC4143). Split into a
send machine and an owning machine that also holds received keys.

- `KeyMap = HashMap<member_id, MediaKey>`; `MediaKey` carries key bytes +
  index so hosts can feed any LiveKit key provider.
- `SendMachine::new(driver: ToDeviceSendDriver, session, config,
  on_key_for_own_member_change)` — chunks the call into intervals; session
  changes inside an interval trigger key rotation (rate-limited; joiners get
  the current key within a configurable "leak window", leavers force a
  rotation). Distribution is per-device with per-recipient delivery results:
  only recipients that actually got the key are recorded as served.
- `Machine::new(driver: ToDeviceDriver, session, own_member,
  on_key_map_change)` — owns the `SendMachine` and the `KeyMap`.
- **Inbound verification kept from today**: cleartext keys discarded; the
  to-device sender+device must match the named member event (keys arriving
  early are buffered *with their origin* and verified when the membership
  lands); non-cross-signed senders rejected unless configured otherwise
  (MSC4153); an outdated-key filter stops replays from occupying a
  `(member, index)` slot.
- `JoinStatus { has_distributed_initial_keys, has_received_all_member_keys }`;
  `ConnectedStatus { left_members_with_keys, fully_settled, last_rotation_ts }`
  (UI can show "members possibly still listening" and the join leak window).

### `participation`

The facade a calling host uses. Owns and wires the four parts above.

- `Manager::new(driver: MatrixDriver, config)`; hands the session its
  `RoomEventsDriver` slice (the session seeds and feeds itself) and routes
  only the to-device stream into the encryption machine.
- `join()` / `leave()`; `Status = Disconnected | Joining(JoinStatus) |
  Connected(ConnectedStatus) | Leaving(LeaveStatus)` composing the
  own-membership and encryption statuses.
- Getter + change-callback pairs for the four host-facing outputs:
  **memberships** (`SessionMembership[]`), **connections**
  (`ConnectionWithMembers[]`), **`KeyMap`**, **`Status`**.
- Memberships vs. connections is a deliberate redundancy: memberships is the
  UI-shaped view (one tile per entry), connections the transport-shaped view
  (which LK rooms to hold). A `SessionMembership` carries what the tile→media
  lookup needs — the `ws_url`s the member publishes on and the transport
  participant identity (MSC4195 hash; `{user}:{device}` in legacy compat).
- A membership's state is `Joined` or `LeftWithKeys`: a member that left the
  session (leave/expired sticky) but still holds a not-yet-rotated copy of
  our media key stays in the list, so the UI can render "leaving — may still
  be listening" until the rotation settles (joining session state with
  `encryption::ConnectedStatus::left_members_with_keys` is exactly
  facade-level work).

### `driver`

The only Matrix I/O boundary, shaped after matrix-rust-sdk's widget
`MatrixDriver` so it drops in. Split into capability traits so each manager
receives no more than it needs; `MatrixDriver` is the sum.

- `OwnMembershipDriver`: send sticky / state / delayed events, restart/cancel
  delayed (MSC4140), delegate LiveKit delayed leave.
- `ToDeviceSendDriver`: send one to-device message to a set of devices with
  per-recipient results. `ToDeviceDriver` adds the inbound stream (decrypted,
  with origin metadata).
- `RoomEventsDriver`: `read_events`/`read_state` + live room-event and
  state-update streams. Every inbound event carries an `EventOrigin`
  (encrypted+device / cleartext / unknown) from decryption metadata — the
  session and encryption rules depend on it.
- `TokenDriver`: OpenID token, `GET /rtc/transports`, LiveKit `get_token`.

### Compat (no module of its own)

Pre-2026 Element Call interop is done **in place** rather than as an edge
translation to synthetic sticky events. `ElementCallCompat` (Off /
StickyEvents / StateEvents, selected per call) is consumed where each half
lives: reading in `session::convert` (one deletable file per generation),
writing in `own_membership`, the legacy token endpoint and identity in
`connections`. The trade: the session knows legacy formats (the delete-by
date now touches session code) — bought back by the one-file-per-generation
isolation; in exchange there are no fabricated events, one conversion point,
and one intake API.

### Bindings

One UniFFI surface (feature `uniffi`) for **every** platform: Swift and
Kotlin via uniffi-bindgen, React Native and web/wasm via
uniffi-bindgen-react-native. The generated API — and therefore the
documented API — is identical everywhere, which is the point: one driver
contract, one set of docs and examples, bugs fixed once. Nothing
high-frequency crosses this boundary (the media plane is host-owned), so
generated bindings cost nothing where it matters.

- Records with JSON-string payloads, listener callback interfaces for the
  change streams, `MatrixDriverCallback` as an async foreign trait — the
  same seam a matrix-rust-sdk adapter (mobile) and a matrix-js-sdk driver
  (web) implement.
- The FFI mirrors the Rust shape `Manager::new(MatrixDriver::new(room))`
  literally: the host wraps its `MatrixDriverCallback` in an exported
  `FfiMatrixDriver` object — the one place the foreign trait becomes a
  `driver::MatrixDriver` — then hands it to any number of managers (one room
  can hold several slots).
- Inbound events stay a driver job across the FFI: the foreign driver's
  `subscribe_*` methods receive Rust-exported **sink objects** — called
  synchronously, exactly once, at `FfiMatrixDriver` construction — and
  `emit` into them from the host SDK's event handlers; `emit -> false` is
  the drop-guard signal to unhook. Hosts implement single-sink semantics;
  fan-out to managers happens Rust-side, so a foreign driver is consumed
  exactly like a native one — no push inlets on the manager.
- On `wasm32` the crate builds against uniffi's
  `wasm-unstable-single-threaded` feature (no tokio runtime; futures are
  `?Send`, hence the dual `async_trait` cfg pattern on the driver traits).
- Logging: everything through the `log` facade; bindings install the sink
  (console / logcat / host callback) with `RUST_LOG`-style filters. Key
  material, JWTs and OpenID tokens are never logged.

## Usage

For in-app status indication (room list / room header, no call):

```rust
// inside whatever computes the room info — session computation is cheap
// enough to just run on every room change
on_room_change() {
    // one slice of everything — sticky and state; the session dispatches
    // returns plain SessionSnapshots — values, not subscriptions
    let snapshots = compute_sessions_from_events(all_rtc_events, config);
    update_room_info(snapshots.map(|s| { s.member_count(), s.start_ts, s.application_type, .. }));
}
```

For a call. The host owns two pieces of state and reconciles both from the
callbacks — this should be doable with any LK SDK on any platform:

```rust
// host state
let mut lk_rooms: Map<ws_url, lk::Room>;      // index = ConnectionData.ws_url
let mut tiles:    Map<member_id, Tile>;       // index = Member.member_id

on_join_pressed(matrix_room) {
    let manager = participation::Manager::new(MatrixDriver::new(matrix_room), config);
    manager.join(intent, params);

    // 1. Tiles: one per membership. Media may not be attachable yet — the
    //    LK room might still be connecting; attach_media is retried below.
    manager.on_memberships_change(|memberships| {
        tiles.remove_all_not_in(memberships);
        for m in memberships {
            let tile = tiles.get_or_create(m.member.member_id);
            tile.set_info(m.member.display_name, m.member.avatar_url);
            match m.state {
                Joined       => tile.show_active(),
                // left the session but may still hold our current key
                LeftWithKeys => tile.show_leaving(),
            }
            attach_media(tile, m); // no-op for LeftWithKeys / receive-only
        }
    });

    // 2. LK rooms: diff against what we hold, connect/disconnect the rest.
    manager.on_connections_change(|connections| {
        for gone in lk_rooms.keys_not_in(connections) {
            lk_rooms.remove(gone).disconnect();
        }
        for conn in connections.filter(|c| !lk_rooms.has(c.connection.ws_url)) {
            let room = lk::Room::new(conn.connection.ws_url, conn.connection.jwt_token);
            room.connect().then(|| {
                // room is up now: attach media for tiles that were waiting on it
                for m in manager.memberships() { attach_media(tiles[m.member.member_id], m); }
            });
            lk_rooms.insert(conn.connection.ws_url, room);
        }
    });

    // 3. Keys: route each changed key to the LK room(s) of that member.
    manager.on_key_map_change(|key_map, changes| {
        for m in manager.memberships().filter(|m| changes.has(m.member.member_id)) {
            let key = key_map[m.member.member_id];
            for ws_url in m.connections {
                lk_rooms[ws_url].set_key_for_participant(m.transport_identity, key.key, key.index);
            }
        }
    });

    // 4. Status: drive the call UI (spinner during Joining, banner on Leaving).
    manager.on_status_change(|status| call_ui.set_status(status));
}

// tile -> media: membership names the LK room and the participant in it
attach_media(tile, membership) {
    for ws_url in membership.connections {
        if let Some(room) = lk_rooms.get(ws_url) && room.is_connected() {
            tile.attach(room.participant(membership.transport_identity).tracks());
        }
    }
}

on_leave_pressed() {
    manager.leave(reason);
    for room in lk_rooms.drain() { room.disconnect(); }
    tiles.clear();
}
```

## Coverage vs. the current implementation

| Today (matrix-rust-rtc) | Here |
|---|---|
| `matrix-rtc-core` session/manager/slot/join-conditions | `session` |
| `matrix-rtc-core` `OwnMembershipMachine`, join/leave/heartbeat | `own_membership` |
| `matrix-rtc-core` `EncryptionManager`, key checks, rotation | `encryption` |
| `RtcCommandSender` + FFI `CommandSenderCallback` + `SdkCommandSender` | `driver` (one trait family, all bindings) |
| `matrix-rtc-livekit` token/`identity` (MSC4195) | `connections` (token via driver; identity mapping stays a pure fn) |
| `matrix-rtc-livekit` session/keys/transport + `matrix-rtc-media` engine | **host's LiveKit SDK** — out of SDK scope by design |
| `matrix-rtc-bridge` `compat` | in place: `session::convert` (read), `own_membership` (write), `connections` (token/identity) |
| `matrix-rtc-bridge` `run_sticky_bridge` | driver streams → `participation` wiring |
| `matrix-rtc-ffi` / `matrix-rtc-wasm` | one `uniffi` surface (uniffi-bindgen + uniffi-bindgen-react-native) |

The one capability that intentionally has no home here is the in-Rust media
engine (frame streams, constraints, multi-focus connection pool, recording
bots). Hosts that need it — Rust bots — use livekit-rust directly against
`ConnectionData`/`KeyMap`, which is the same contract every other platform
gets.

A compilable skeleton of this plan (all methods `todo!()`) lives in
`MatrixSdkArchitectureDraft/`.
