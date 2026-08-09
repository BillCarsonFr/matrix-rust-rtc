# Element Call interop stack

The base [`docker-compose.yml`](./docker-compose.yml) is plain HTTP on
`localhost`, which is all our Rust clients need. Element Call is a widget in an
iframe, and WebRTC plus the widget API need a **secure context** — so testing
against it needs TLS end to end.

[`docker-compose.interop.yml`](./docker-compose.interop.yml) is an overlay that
adds exactly that, and nothing else:

- **nginx** terminating TLS for `synapse.m.localhost`, `matrix-rtc.m.localhost`
  and `app.m.localhost`, using a CA minted at stack-up time into
  `./data/tls/` (git-ignored — no private key lives in this repo).
- **Element Web** (`ghcr.io/element-hq/element-web:develop`), which **ships
  Element Call embedded**: it depends on `@element-hq/element-call-embedded` and
  serves the widget from its own origin at `/widgets/element-call/index.html`.
  There is no `element-call` service, and there is no `element_call.url` key in
  Element Web's config schema — `Developer.elementCallUrl` (a device-level
  setting) is the only way to point it somewhere else.
- Overrides so Synapse answers to a browser-reachable `server_name`
  (`synapse.m.localhost`) and lk-jwt hands clients a `wss://` SFU URL.

The base file is untouched, so `make backend-up` / `make test-e2e` keep working
exactly as before.

## The two stacks are mutually exclusive

Both compose files share the project name `matrix-rtc-backend`, so `interop-up`
**converts** the running stack rather than starting a second one beside it.
That is deliberate — they publish the same ports (8008, 6080, 7880, …), so they
could never run at the same time anyway, and sharing the name means switching
between them recreates containers instead of failing on port collisions.

Two consequences:

- The homeserver has its own volume per mode (`synapse-data` vs
  `synapse-interop-data`), because the two run under different `server_name`s
  and an account is bound to the one it was created under.
- `make interop-down` / `make backend-down` both run `docker compose down -v`
  against that shared project, so **either one removes both volumes**.
  Everything in them is throwaway dev state that regenerates on the next boot
  (test users are provisioned per run), but it does mean a `backend-down` also
  resets the interop homeserver.

```sh
make interop-up      # base + overlay, then the readiness probes
make interop-logs
make interop-down
make test-interop    # builds the Rust peer, then runs the Playwright test
```

Why `*.m.localhost`: browsers resolve any `*.localhost` name to loopback on
their own, and the names match Element Call's own dev backend, so their
configuration is a useful reference. Everything else on the host does **not**
resolve them — hence the `/etc/hosts` step below.

## Host prerequisites (once per machine)

**1. Resolve the names.** Add to `/etc/hosts`:

```
127.0.0.1 synapse.m.localhost matrix-rtc.m.localhost app.m.localhost
```

**2. Trust the dev CA — which the test does for you.** Nothing needs a
machine-wide install:

| Who | How | Set up by |
| --- | --- | --------- |
| Rust client | `SSL_CERT_FILE` | `interop/helpers/rust-peer.ts`, per spawned process |
| Node | `NODE_EXTRA_CA_CERTS` | `interop/playwright.config.ts` |
| Browser | nothing — `ignoreHTTPSErrors` | `interop/playwright.config.ts` |

The Rust client validates the CA on *two* legs — the homeserver (`reqwest`,
via matrix-sdk) and the SFU signalling socket (`livekit`, built here with
`rustls-tls-native-roots`). Both go through
[`rustls-native-certs`](https://docs.rs/rustls-native-certs), which loads
**only** from `SSL_CERT_FILE` when that is set, in place of the platform store,
**on every platform including macOS**. Scoping it to the peer process is safe
precisely because the peer talks to nothing but this stack.

So for the test itself there is no step 2. Running the peer (or the examples)
by hand outside the harness is the case that needs it:

```sh
export SSL_CERT_FILE="$PWD/demo/backend/data/tls/local-ca.crt"
```

The *browser* never needs the CA under Playwright. Poking the stack by hand in
your own browser is the one case that does — either click through the warning,
or install the CA:

```sh
# macOS
sudo security add-trusted-cert -d -r trustRoot \
  -k /Library/Keychains/System.keychain demo/backend/data/tls/local-ca.crt

# Debian/Ubuntu
sudo cp demo/backend/data/tls/local-ca.crt /usr/local/share/ca-certificates/matrix-rtc-dev.crt
sudo update-ca-certificates
```

### Using mkcert instead

`init-certs` only mints when `data/tls/m.localhost.key` is absent, so mkcert is
a drop-in — run it **before** `make interop-up` and the stack uses your pair,
your system and browser trust stores are already set up by `mkcert -install`,
and both steps above become no-ops:

```sh
mkcert -install
mkdir -p demo/backend/data/tls
mkcert -cert-file demo/backend/data/tls/m.localhost.crt \
       -key-file  demo/backend/data/tls/m.localhost.key \
       "*.m.localhost" m.localhost localhost
```

No `local-ca.crt` is produced in this case (mkcert keeps its CA in
`$(mkcert -CAROOT)`), so skip step 2 entirely.

We do not use mkcert *by default* because it would make the compose stack
depend on a host-side binary it cannot install itself — there is no official
mkcert image — while replacing a single `update-ca-certificates` line on CI.
Element Call solves the same problem by committing a CA and its private key to
their repo; we generate instead.

## What the stack looks like

| Purpose                          | URL                                            |
| -------------------------------- | ---------------------------------------------- |
| Element Web                      | `https://app.m.localhost`                      |
| Element Call widget (bundled)    | `https://app.m.localhost/widgets/element-call/` |
| Synapse (client-server API)      | `https://synapse.m.localhost`                  |
| MatrixRTC authorisation service  | `https://matrix-rtc.m.localhost/livekit/jwt`   |
| LiveKit SFU (signalling)         | `wss://matrix-rtc.m.localhost/livekit/sfu`     |

### The focus is advertised twice, and the two must agree

The two clients discover the authorisation service by different routes:

| Client | Route | Configured in |
| ------ | ----- | ------------- |
| Element Call | `/.well-known/matrix/client` → `org.matrix.msc4143.rtc_foci` | `nginx/interop.conf` |
| Rust client | MSC4143 `/rtc_transports` (needs `msc4143_enabled`) | `matrix_rtc.transports` in `homeserver.interop.yaml` |

Both must name `https://matrix-rtc.m.localhost/livekit/jwt`. If they disagree,
the two halves of a call select different foci and nothing about the failure
says so. (`.well-known` is served by nginx rather than Synapse precisely
because that is where the `rtc_foci` entry has to be injected.)

Synapse's federation API is reachable at `synapse.m.localhost:8448` *inside* the
compose network only. lk-jwt validates client OpenID tokens against
`/_matrix/federation/v1/openid/userinfo`, and gomatrixserverlib speaks HTTPS
only — that is the whole reason a TLS proxy is required even for a stack nobody
federates with.

## Element Web / Element Call versions

The Element Call under test is whichever version Element Web bundles. That is
deliberate: it is what users actually get, and it removes any Element
Web/Element Call version skew from the test.

CI pins the image by digest for label-triggered PR runs and leaves it floating
for the nightly drift run, via the `ELEMENT_WEB_IMAGE` variable:

```sh
ELEMENT_WEB_IMAGE=ghcr.io/element-hq/element-web:develop@sha256:… make interop-up
```

To refresh the pin:

```sh
docker pull ghcr.io/element-hq/element-web:develop
docker inspect --format='{{index .RepoDigests 0}}' ghcr.io/element-hq/element-web:develop
```

and update the default in `.github/workflows/interop-element-call.yml`.

## Troubleshooting

- **`wait-ready.sh --interop` fails on the first HTTPS probe** — almost always
  the `/etc/hosts` entries; the script says so explicitly.
- **`certificate is not standards compliant: -67901` (macOS)** — the leaf's
  validity period exceeds Apple's 398-day cap for TLS server certificates.
  `init-certs` mints 397 days, so this means `./data/tls` holds an older pair:
  `make interop-down && rm -rf demo/backend/data/tls && make interop-up`.
  Note that certificates minted by hand (or by an older Element Call
  `dev_tls_setup`, which uses 800 days) hit this too.
- **`InvalidCertificate(UnknownIssuer)` from the Rust peer** — the dev CA did
  not reach it. The peer applies `SSL_CERT_FILE` in *two* places because there
  are two reqwest crates in that binary; see `dev_ca_pem` in
  `examples/interop_peer.rs`. `INSECURE_TLS=1` bypasses the whole question and
  is the fastest way to confirm a failure really is TLS.
- **The Rust peer fails TLS but `curl -k` works** — the CA is not in the system
  trust store, or `livekit-api` resolved its webpki bundle instead of the
  native roots. Try `SSL_CERT_FILE` pointing at a bundle containing the dev CA;
  as a last resort `INSECURE_TLS=1` covers the homeserver leg only.
- **`element-call (bundled)` probe 404s** — the Element Web image is older than
  the embedded-widget change, or the tag moved. Check
  `https://app.m.localhost/widgets/element-call/index.html` by hand.
- **lk-jwt returns 500 on `/get_token`** — the federation hop. `make
  interop-logs` and look at `auth-service`; check nginx is answering on 8448
  inside the network.
- **Media never flows but signalling works** — ICE. The SFU advertises
  `127.0.0.1` (see `livekit/livekit.yaml`); browser and Rust client both reach
  it via the published UDP range `50100-50200`, TCP `7881` as fallback.
