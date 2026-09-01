# Element Call interop test

A Rust MatrixRTC client and a **real Element Call**, in the same call, asserted
from both sides.

[`crates/matrix-rtc-livekit/tests/e2e_call`](../crates/matrix-rtc-livekit/tests/E2E_CALL.md)
already proves our stack talks to itself in every dialect, including the
pre-sticky one. What it cannot prove is that Element Call *agrees* with our
reading of the wire format — for that you need Element Call, and Element Call
needs a browser.

## Shape

Playwright drives both halves. It owns the browser (Element Web, and the
Element Call widget Element Web bundles) and spawns the Rust client as a child
process, talking to it over a line protocol:

```
peer stdout: {"event":"ready","room_id":"!abc:synapse.m.localhost", ...}
playwright:  bob accepts the invite, sets Element Call's RTC dialect
playwright → peer stdin: "join"
peer stdout: {"event":"joined","identity":"...","membership_id":"..."}
playwright:  bob joins the call
peer stdout: {"event":"members","count":2}              ← EC's membership parsed
peer stdout: {"event":"track_subscribed","identity":"…"} ← the SFU's identity for EC
peer stdout: {"event":"key_imported","identity":"…"}     ← EC's media key installed
peer stdout: {"event":"audio_rms","value":0.31,…}        ← EC's frames decrypt
playwright:  expect 2 tiles, "Rust Peer" visible, no "Waiting for media..."
```

The Rust peer is `examples/interop_peer.rs`; its protocol is documented at the
top of that file.

## Scenarios

| Test name             | Element Call ("Developer" tab) | Our `ElementCallCompat` | Generation |
| --------------------- | ------------------------------ | ----------------------- | ---------- |
| `ec-2024 state events`| `Compatibility: state events`  | `StateEvents`           | pre-MSC4354: membership as `org.matrix.msc3401.call.member` room state, plain `{user}:{device}` identities, `/sfu/get` |
| `ec-2025 sticky events`| `Matrix 2.0`                  | `StickyEvents`          | MSC4354 sticky membership carrying the pre-2026 field names, MSC4195 hashed identities, `/get_token` |

The tests are **not** named after Element Call's UI labels, deliberately.
`Matrix 2.0` is Element Call's name for its 2025 generation, not for the
spec-current one: the actually-current 2026 format is
`ElementCallCompat::Off`, which Element Call does not speak at all, so it has no
counterpart here and `e2e_call` remains its only test. The UI strings live in
one place, `RTC_MODE_LABEL` in `helpers/element-web.ts`.

### The web peer

`web-peer-rust.spec.ts` runs a third kind of peer: the **web stack** —
`web/demo` in test mode (wasm bindings + matrix-js-sdk + livekit-js in a
browser page, served by the `webServer` in `playwright.config.ts` and driven
through `helpers/web-peer.ts`, the page-shaped sibling of `rust-peer.ts`).
It shares a call with the Rust peer in the spec-current dialect
(`ElementCallCompat::Off`), asserting both directions: membership, key
import under the SFU identity, the video pattern read back from a `<video>`,
and audio energy. This is the web stack's only test against a real
homeserver, authorisation service, and SFU — everything else it has runs
against fakes.

## Running it

```sh
make interop-up          # the TLS stack (see demo/backend/INTEROP.md)
make test-interop        # builds the Rust peer, then runs this suite
```

or, once the stack is up and the peer is built:

```sh
cargo build -p matrix-rtc-livekit --features matrix-sdk,testing --example interop_peer
cd interop && npm ci && npx playwright install --with-deps chromium
npx playwright test                 # add --headed to watch it
npx playwright show-report
```

First run needs the two host-side prerequisites in
[`demo/backend/INTEROP.md`](../demo/backend/INTEROP.md): `/etc/hosts` entries
for `*.m.localhost`, and the stack's dev CA trusted (the Rust client validates
it on both the homeserver and the `wss://` SFU leg).

| Variable | Default |
| -------- | ------- |
| `ELEMENT_WEB_URL` | `https://app.m.localhost` |
| `HOMESERVER_URL` | `https://synapse.m.localhost` |
| `INTEROP_PEER_BIN` | `../target/debug/examples/interop_peer` |
| `NODE_EXTRA_CA_CERTS` | the stack's `data/tls/local-ca.crt`, if present |

## When it breaks

- **On a locator** — almost certainly Element Web or the bundled Element Call
  moved. Every UI string lives in `helpers/element-web.ts`; nothing else in
  this suite touches the DOM.
- **On `members` never reaching 2** — the membership dialect. Our side parsed
  nothing from Element Call, or Element Call rejected ours. The attached
  `interop-peer.log` has the Matrix side.
- **On `key_imported`** — the identity derivation or the key binding. Both are
  silent failures by nature: tracks buffer forever and keys install under an
  identity nobody holds.
- **On `audio_rms`** — signalling and keys worked but frames did not decrypt.
  The recording is attached to the report as `element-call-audio.wav`.
- **On "Waiting for media..."** — the mirror image: *our* media did not reach
  Element Call. Historically this has meant simulcast (a single plain encoding
  is invisible to an adaptive-stream subscriber), which is why the peer
  publishes a 640x360 pattern rather than something smaller.

## Provenance

`helpers/element-web.ts` is adapted from
[`playwright/widget/test-helpers.ts`](https://github.com/element-hq/element-call/blob/livekit/playwright/widget/test-helpers.ts)
in element-hq/element-call (AGPL-3.0). Registration deliberately does **not**
follow their Synapse admin HMAC helper — our dev homeserver has open
registration on, which is what `e2e_call`'s `provision.rs` already uses.
