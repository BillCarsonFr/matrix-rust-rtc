# Key rotation

How `matrix-rtc-core` decides when to mint a new media key, why it does not simply
mint one per membership change, and what that trade costs in each direction.

Implemented in `crates/matrix-rtc-core/src/encryption/mod.rs`
(`rollout_outbound_key`); the scenarios that pin the numbers below live in
`crates/matrix-rtc-core/tests/key_rotation.rs`.

## What a rotation costs

Every participant encrypts with its own key and distributes it to every other
participant as an Olm-encrypted to-device message (MSC4143). So one member
rotating in a call of `n` costs `n - 1` to-device messages, and a membership
change that makes *everybody* rotate costs `n * (n - 1)`.

That is the floor for a call that forms and never changes: 8 people joining an
empty call, each handing one key to the other seven, is 56 messages. It is also
the unit of waste — a rotation that was not needed costs the same as one that was.

Two things make it worse than the formula suggests:

- **Bursts.** People arrive and leave in bulk, a second or two apart — a meeting
  starting, a meeting ending. Rotating per change turns one social event into `k`
  rotations, each broadcast to everyone still present.
- **Rotations are not instant.** A new key is not used the moment it is minted:
  peers need `delay_before_use_ms` to install it first (MSC4143 `delayBeforeUse`),
  and until then the previous key is still what frames are stamped with. A
  rotation made while another is still propagating abandons a key that was
  distributed to every member and never encrypted a single frame.

Measured, from the scenario suite:

Counting keys across the whole call, not per member — a rotation is one key minted by
one participant, so eight people each minting one is eight keys and no rotations:

| Scenario (default config) | Keys minted | To-device sends | Floor |
| --- | --- | --- | --- |
| 8 join an empty call, 1s apart | 8 — one each, no rotation | 56 | 56 |
| 8 join, 30s apart (each into a settled call) | 36 — 8 plus 28 rotations | 195 | 56 |
| then, in a settled 5-member call: | | | |
| 3 leave in one roster update | +2 | +2 | — |
| 3 leave 500ms apart, with no grace period | +9 | +20 | — |
| 3 leave 500ms apart, with the grace period | +6 | +14 | — |
| 3-member call, 4 hours, no membership change | +6 | +12 | — |

The two departure rows are the same social event costing 30% less traffic and three
fewer keys — from any one member's point of view, two rotations instead of three. The
two arrival rows are the same eight people, differing only in timing. The last row is
the lifetime cap alone: two rotations per member over four hours, buying a bound on
how much any single key is worth.

### Where each of those numbers comes from

Step by step, as the scenario suite reports it. `alice#2 x3` reads as "alice sent key
index 2 to three peers" — one rotation of hers, broadcast. A step with no sends is a
change the policy absorbed. Regenerate with:

```sh
cargo test -p matrix-rtc-core --test key_rotation documented_costs -- --nocapture
```

**8 join an empty call, 1s apart.** Nobody rotates: each arrival is handed the key
every member already has, and hands out its own. The `xN` on the newcomer is the
newcomer serving everyone at once; the `x1`s are each incumbent serving the newcomer.

```
bob joins:    2 send(s) — alice#0 x1, bob#0 x1
carol joins:  4 send(s) — alice#0 x1, bob#0 x1, carol#0 x2
dave joins:   6 send(s) — alice#0 x1, bob#0 x1, carol#0 x1, dave#0 x3
erin joins:   8 send(s) — ... erin#0 x4
frank joins: 10 send(s) — ... frank#0 x5
grace joins: 12 send(s) — ... grace#0 x6
heidi joins: 14 send(s) — ... heidi#0 x7
                 total 56, and every member still on key index 0
```

**8 join, 30s apart.** The same arrivals, each meeting a settled key. Every incumbent
rotates *and* hands the newcomer the key it is still encrypting with — which is the
second entry per member, always `x1`, so `alice#7 x7, alice#6 x1` is one rotation
broadcast to seven peers plus one hand-off to heidi.

```
bob joins:    2 send(s) — alice#1 x1, bob#0 x1
carol joins:  8 send(s) — alice#2 x2, alice#1 x1, bob#1 x2, bob#0 x1, carol#0 x2
dave joins:  15 send(s) — alice#3 x3, alice#2 x1, bob#2 x3, bob#1 x1, carol#1 x3, carol#0 x1, dave#0 x3
erin joins:  24 send(s) — alice#4 x4, alice#3 x1, ... erin#0 x4
frank joins: 35 send(s) — alice#5 x5, alice#4 x1, ... frank#0 x5
grace joins: 48 send(s) — alice#6 x6, alice#5 x1, ... grace#0 x6
heidi joins: 63 send(s) — alice#7 x7, alice#6 x1, ... heidi#0 x7
                 total 195, and alice alone has burned indexes 0 through 7
```

**5-member call, 3 leave in one roster update.** One roster move, so one rotation
each from the two members left, to each other:

```
bob, carol and dave leave together: 2 send(s) — alice#1 x1, erin#1 x1
```

**5-member call, 3 leave 500ms apart, no grace.** Each departure rotates the moment
it lands, and each rotation is broadcast to everyone still there — so the cost is
front-loaded and paid three times. (The indexes start at 5 because with no grace the
five joins each rotated too.)

```
bob leaves:   12 send(s) — alice#5 x3, carol#4 x3, dave#3 x3, erin#2 x3
carol leaves:  6 send(s) — alice#6 x2, dave#4 x2, erin#3 x2
dave leaves:   2 send(s) — alice#7 x1, erin#4 x1
                  total 20, 3 keys from alice
```

**5-member call, 3 leave 500ms apart, with grace.** Bob's departure rotates because
the call had settled. Carol's and dave's land while that key is fresh and cost
*nothing at all* — they are answered by the single rotation that expires it:

```
bob leaves:                          12 send(s) — alice#1 x3, carol#1 x3, dave#1 x3, erin#1 x3
carol leaves:                         0 send(s)
dave leaves:                          0 send(s)
the key expires, answering all three: 2 send(s) — alice#2 x1, erin#2 x1
                                         total 14, 2 keys from alice
```

**3-member call, 4 hours, no membership change.** Nothing but the lifetime cap. The
rotations land on the hour here because the scenario only checks in hourly, standing
in for a consumer's tick; a precise driver would fire them at 1h30 and 3h.

```
hour 1: 0 send(s)
hour 2: 6 send(s) — alice#1 x2, bob#1 x2, carol#1 x2
hour 3: 0 send(s)
hour 4: 6 send(s) — alice#2 x2, bob#2 x2, carol#2 x2
           total 12, 2 keys each
```

## What *not* rotating costs

The risk is not symmetric between somebody arriving and somebody leaving, and the
difference is what the policy is built on.

- **A joiner given the current key gains the past.** Everything that key has
  already encrypted becomes readable to them — history they were not part of.
- **A leaver whose key is not retired gains the future.** Everything that key goes
  on encrypting after they are gone stays readable to them, with no end.

The second is worse in kind: a joiner's exposure is bounded by how old the key
already is, while a leaver's grows for as long as the call continues. A leaver
must therefore always end up with a retired key; the only question is how soon.

Both are "virtual" in one important sense: reading either requires a recording of
the SFU streams, which a participant does not normally have. Exploiting it means
colluding with the SFU. That is what makes a few seconds of either acceptable, and
it is why the answer is a delay rather than a refusal.

## The model: every key has an expiry

A key is minted with an expiry, and **rotating is nothing more than that expiry
arriving**. Membership changes do not decide to rotate; they bring the expiry
forward.

```
mint                                                                      expiry
 |<-- delay_before_use -->|                                                  |
 |<------- key_rotation_grace_period ------->|                               |
 |       propagating      |      fresh       |        stable ...             |
 |                                                          max_key_lifetime |
```

- **Propagating** — we are still encrypting with the *previous* key; this one is
  distributed but not yet in use.
- **Fresh** — young enough that handing it to a newcomer leaks only a little past,
  and young enough that replacing it would waste most of its life.
- **Stable** — settled; every member has had it for a while and it has encrypted
  real history.
- **Expiry** — at the latest `max_key_lifetime_ms` after minting, so that no single
  key ever protects more than that much of a call.

### The rules

Three adjustments to one instant:

| Event | Effect on the expiry |
| --- | --- |
| Minted | `creation + max_key_lifetime_ms` |
| A member who holds it leaves, or their participation changes | pulled in to `min(expiry, creation + grace)` |
| A member arrives and the key is no longer fresh | pulled in to `now` |
| A member arrives and the key is still fresh | none — just send them the key |

...and a rotation happens exactly when `now >= expiry`.

The departure rule needs no test for whether the key is fresh, which is what makes
this a simplification rather than a relabelling. `min(expiry, creation + grace)`
lands in the *future* while the key is fresh — deferring, and coalescing every later
change into the same instant — and in the *past* once it has settled, which rotates
immediately. It is also idempotent, so a second leaver in the same window recomputes
the same deadline.

The asymmetry between the last two rows is the past/future difference above. A
departure always dirties the key. An arrival handed a fresh key dirties nothing, so
it must not touch the expiry at all — which is why an arrival can never leave a
rotation owed.

Three further consequences fall out rather than being designed in:

1. **A burst costs two rotations, whatever its size.** The first change rotates
   (the key was stable), everything for the next `key_rotation_grace_period_ms`
   coalesces, and one rotation closes the window. Ten people leaving over five
   seconds cost the same as two.
2. **At most one rotation per grace period**, in a call whose roster never stops
   moving. The rate limit is not a separate mechanism; it is what anchoring the
   deadline to the *key's* age instead of the *last change* gives you. Anchoring
   to the last change would let a call with a leaver every few seconds defer for
   ever.
3. **A joiner never leaves a rotation owed.** They either meet a fresh key (and
   are simply sent it) or a stable one (which rotates at once). Only a departure
   makes a key dirty, which is what the asymmetry above says it should be.

### Why the deadline is not uniform per leaver

Freshness is measured from the key's mint, so a member who leaves early in the
window waits nearly the whole grace period to be locked out, while one who leaves
just before the end waits almost no time. That unevenness is deliberate: it is
what bounds the deferral. The alternative — every leaver waits a fixed delay from
*their own* departure — has no bound, because each new leaver pushes the deadline
back.

## Parameters

Three, each answering a question the others cannot. Note what is *not* here: no
parameter for how long a deferred rotation waits, because that is derived —
`key creation + key_rotation_grace_period_ms`.

| Parameter | Default | What it is | What moves it |
| --- | --- | --- | --- |
| `delay_before_use_ms` | 5s | How long peers need to install a key before we encrypt with it. A *network* fact. | Sync/to-device latency. Lower it if delivery is fast. |
| `key_rotation_grace_period_ms` | 10s | How long a key counts as fresh: re-shared to joiners, and a shield against re-rotation. | How much past/future history you will trade for how much key traffic. |
| `max_key_lifetime_ms` | 1h30 | How much of a call one key may ever protect. | How much a single recovered key should be worth to an attacker. |

Keeping the first two separate matters in one specific way: the rotation cadence of
a busy call is the grace period, not the propagation delay. Lowering
`delay_before_use_ms` because delivery got faster must not silently make a churning
call rotate twice as often.

Both floors are enforced rather than merely documented. A grace period below
`delay_before_use_ms` is incoherent — a key that has not come into use is certainly
fresh — and a lifetime below the grace period is meaningless, so each is raised to
the one below it.

## Exposure, stated plainly

| Who | What they can decrypt that they should not | Bounded by |
| --- | --- | --- |
| A member who leaves | Media sent after they left | `key_rotation_grace_period_ms` + `delay_before_use_ms` |
| A member who joins while the key is fresh | Media sent before they arrived | `key_rotation_grace_period_ms` |
| A member who joins a settled call | The key in use as they arrived, so its history | `max_key_lifetime_ms` — see below |
| Whoever recovers one key from a device | The media that key encrypted | `max_key_lifetime_ms` |

The last row is what the lifetime cap is for, and it is worth being exact about the
threat: it bounds a key extracted *at a point in time* — a memory dump, a leaked
log, a key pulled off a device that has since been secured — against ciphertext a
colluding SFU recorded. It does nothing against a device that stays compromised,
which is sent every later key as well.

The third row is the one worth arguing about. When a joiner meets a stable key we
rotate, and the rotation takes `delay_before_use_ms` to come into use, during which
frames are still stamped with the *outgoing* key. The joiner is handed that outgoing
key as well, or they would decrypt nothing at all until the window closed — a media
blackout on every join into an established call. The cost is that the outgoing key
may have been in use for a while, and its whole history comes with it — up to
`max_key_lifetime_ms`, which is the other reason to keep that cap sane.

The alternative is to withhold it and accept the blackout. That is a UX-versus-history
judgement, not a technical one, and it is currently decided in favour of UX.

## Who performs a deferred rotation

`matrix-rtc-core` owns no timer — deliberately, so a synchronous FFI host can
drive it from a plain thread — so it cannot wake itself up when a rotation falls
due. It exposes the deadline instead:

- `RtcSessionManager::key_rotation_due_at_ms(room_id, slot_id)` — when one is owed,
  if any.
- `RtcSessionManager::flush_due_key_rotation(room_id, slot_id)` — performs it, if
  due. A no-op otherwise, so it is safe on any tick.

`matrix-rtc-livekit` is the reference driver, and it needs *two* wake-ups, because
neither alone is enough:

- `MediaKeyBridge` reports every key coming into use (the same scheduled task that
  installs it). That is when a rotation *becomes* owed — a member left while the
  key was fresh — but not when it is due: freshness outlasts `delayBeforeUse`, so
  there is usually nothing to do yet.
- A timer set from the reported deadline. This is the wake-up that performs it.

`RtcSession::heartbeat` flushes as well, so a consumer that wires neither is late
rather than broken.

This is load-bearing. If nothing ever collected an owed rotation, a member who
left while a key was fresh would keep a working key for as long as the roster
stayed still — strictly worse than rotating per departure.

## Related

- MSC4143 for `delayBeforeUse`, `keyRotationGracePeriod`, and to-device key
  distribution.
- `ARCHITECTURE.md` — "Encryption negotiation" for whether keys are managed at all.
- `crates/matrix-rtc-core/tests/key_rotation.rs` — every number in this file.
