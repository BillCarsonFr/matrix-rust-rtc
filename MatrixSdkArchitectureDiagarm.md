# Matrix RTC SDK — stack diagram

The current shape of the `matrix-rtc` draft, top to bottom: the host
implements one `MatrixDriver` per room (capabilities listed in its box),
constructs one `ParticipationManager` per slot, and consumes everything
through the manager's public getter + callback pairs.


```
MatrixDriverRetryWrapper(MatrixDriver)
```

```text
                                host application
┌──────────────────────────────────────────────────────────────────────────────┐
│               MatrixDriver — implemented by the host, one per room           │
│                                                                              │
│   send    sticky / state / delayed events (MSC4354 · MSC4140                 │
│           restart/cancel) · delegate delayed leave to the SFU (MSC4195)      │
│   send    to-device messages (per-recipient delivery results)                │
│   read    timeline events · room state                                       │
│   sfu_endpoints OpenID · GET /rtc/transports · LiveKit get_token             │
│   emit    live streams: room events · state updates · to-device              │
└──────────────┬───────────────────────────────────────────▲───────────────────┘
               │ streams in                                │ commands out
               ▼                                           │ (one trait slice per part)
┌─ ParticipationManager::new(room_id, slot_id, driver, config) ────────────────┐
│                                                                              │
│  ┌─ Session ────────────────────────────────────────────────────┐            │
│  │ feeds itself: RoomEventsDriver slice — seeds via reads,      │            │
│  │ then consumes the live streams. The single converter:        │            │
│  │ every event type → Member candidates; joined projection      │            │
│  │ (slot · encryption · room conditions). All reads are         │            │
│  │ SessionSnapshots (same values as the static path ⇓)          │            │
│  └─────────┬────────────────────────┬──────────────────────┬────┘            │
│            │                        │                      │                 │
│            │ subscribe()            │ subscribe()          │ subscribe()     │
│            ▼                        ▼                      ▼                 │
│   ┌─ OwnMembership ────┐  ┌─ Connections ──────┐  ┌─ Encryption ──────┐      │ Connection = Established/ResolvedTransport, TransportConnection
│   │ join / leave state │  │ ConnectionData per │  │ SendMachine:      │      │
│   │ machine · delayed  │  │ ws_url (multi-     │  │ rotation +        │      │
│   │ leave · heartbeat  │  │ focus) · mint /    │  │ distribution;     │      │
│   │                    │  │ reuse tokens       │  │ KeyMap: verify +  │      │
│   │ ⇅ OwnMembership-   │  │                    │  │ store inbound keys│      │
│   │   Driver           │  │ ⇅ TokenDriver      │  │                   │      │
│   │                    │─▶│                    │  │ ⇅ ToDeviceDriver  │      │
│   └────────────────────┘  └────────────────────┘  │ ◀ to-device stream│      │
│     on_transport_created → add_own_transport      └───────────────────┘      │
│                                                                              │
├─ public surface ─────────────────────────────────────────────────────────────┤
│   join(intent, params) · leave(reason)                                       │
│                                                                              │
│   memberships() + on_memberships_change    one tile per entry: Session's     │
│                                            joined set ∪ Encryption's         │
│                                            left-with-keys members            │
│   connections() + on_connections_change    the LK rooms to hold              │
│   key_map()     + on_key_map_change        feed into frame encryption        │
│   status()      + on_status_change         Joining / Connected / Leaving     │
└──────────────────────────────────────────────────────────────────────────────┘
               ▼ callbacks → host renders tiles, holds LK rooms, sets keys
```

Add emoji reactions to the rust-core crate.
rtc crate should be for ANY rtc application not just m.call

- **Session sits at the top and is the only part touching the driver's
  event streams** — it holds the `RoomEventsDriver` slice itself: it seeds
  its state via `read_state`/`read_events` and consumes the live streams,
  converting every event type in place (sticky members, legacy state
  members, slots, room conditions). There is no manual `update`.
- **All session reads are `SessionSnapshot` values** — the same type the
  static path returns; `Session` adds only liveness (`snapshot()` +
  `subscribe()`).
- **The other three parts never see raw events**: each holds a session
  subscription (`subscribe()` → watch) and reacts to the joined projection.
- **Each part gets only its driver slice** (`⇅` = outbound commands on that
  slice): OwnMembership sends the sticky/delayed/state events, Connections
  exchanges tokens, Encryption sends keys — and additionally receives the
  driver's to-device stream (routed by the manager).
- **The one lateral edge**: OwnMembership's `on_transport_created` hands the
  finalized transport to Connections' `add_own_transport`.
- **The outputs recombine two parts once**: `memberships` merges the
  Session's joined set with Encryption's left-with-keys members
  (`LeftWithKeys` tiles); the other outputs map 1:1.

## The second entry point — session computation without a call

The session logic has a second consumer that never constructs a manager:
Element X-style room info. Hundreds of rooms, recomputed on every room
update — a pure function over already-synced events returning plain
`SessionSnapshot` values. The reactivity lives in the caller
(matrix-rust-sdk's `room_info` recomputes per update and populates its
fields from the snapshot's metadata functions):

```text
                                host application
     room list · room header · pre-join lobby · notification process
        (iOS NSE) · matrix-rust-sdk room_info — no call, no driver
                                   │
                                   │ all synced RTC events (sticky + state),
                                   │ one batch per room update, many rooms at once
                                   ▼
            ┌──────────────────────────────────────────────┐
            │ compute_sessions_from_events(events, config) │
            │                                              │
            │ the same converter + joined projection a     │
            │ live Session runs — pure, no I/O. Origins    │
            │ are unknown here, so origin-dependent        │
            │ exclusions stay unenforced (counts may       │
            │ overshoot slightly).                         │
            └──────────────────────┬───────────────────────┘
                                   │ Vec<SessionSnapshot> — plain values, nothing
                                   │ subscribes (a live Session needs a driver)
                                   ▼
    member_count() · is_active() · start_ts · application_type ·
    slot_state · negotiated_encryption
  → populate room_info on each room update → "ongoing call" pill ·
    header facepile · lobby verdicts · stale-ring suppression
```

The lobby → in-call transition is the seam between the two entry points:
the lobby renders from static snapshots, and pressing join constructs the
`ParticipationManager` above — whose live Session then becomes the
authoritative view (with origins, so the member count can drop slightly
at join).

Over the FFI the picture is identical — `FfiMatrixDriver` wraps the
host-implemented `MatrixDriverCallback` (sinks in, commands out) and is
consumed as a `dyn MatrixDriver` like any native driver;
`compute_sessions_from_events` is exported as a plain function returning
`FfiSessionSnapshot` records (the conveniences precomputed as fields).
