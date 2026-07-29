# Local MatrixRTC backend (Synapse + lk-jwt-service + LiveKit)

A self-contained docker-compose stack providing everything the Rust MatrixRTC
clients need — used by the `e2e_call` integration test on CI and for local
development:

- **Synapse** (`ghcr.io/element-hq/synapse:latest`) — homeserver with MSC4354
  (sticky events) and MSC4140 (delayed events) enabled, rate limits disabled,
  open registration for throwaway test users.
- **lk-jwt-service** (`ghcr.io/element-hq/lk-jwt-service:0.4.4`) — the
  MatrixRTC authorisation service (MSC4195 `/get_token`).
- **LiveKit SFU** (`livekit/livekit-server:v1.10.1`).

No nginx, no TLS on the client side, no federation pair — this is the minimal
subset of [Element Call's dev backend](https://github.com/element-hq/element-call)
that the Rust e2e flow needs. All state lives in `./data/` (git-ignored); wipe
it for a factory reset.

## Usage

```sh
make backend-up      # docker compose up -d --wait + readiness probe
make backend-logs    # follow logs
make backend-down    # tear down (incl. volumes)
```

or directly:

```sh
docker compose -f demo/backend/docker-compose.yml up -d --wait
./demo/backend/wait-ready.sh
```

Endpoints once up (all on `localhost` — do **not** expose these beyond it;
registration is open and the secrets are well-known dev values):

| Purpose                     | URL                     |
| --------------------------- | ----------------------- |
| Synapse (client-server API) | `http://localhost:8008` |
| RTC auth service (lk-jwt)   | `http://localhost:6080` |
| LiveKit SFU (WebSocket)     | `ws://localhost:7880`   |

## Running the e2e call test against it

The integration test provisions its own throwaway users and defaults to the
endpoints above, so this is all it takes:

```sh
cargo test -p matrix-rtc-livekit --features matrix-sdk,testing --test e2e_call -- --ignored --nocapture
```

See [`crates/matrix-rtc-livekit/tests/E2E_CALL.md`](../../crates/matrix-rtc-livekit/tests/E2E_CALL.md).

## Why auth-service shares the LiveKit container's network namespace

lk-jwt uses `LIVEKIT_URL` both as the SFU URL it returns to clients in
`/get_token` responses **and** as its own RoomService endpoint for creating the
room on the SFU, so a single URL has to work from the host and from inside the
container. `network_mode: "service:livekit"` makes `ws://localhost:7880` do
both: in-container it loops back to the SFU directly, on the host it goes
through the published port. That is also why the `6080` port mapping lives on
the `livekit` service. (Element Call's dev stack solves the same problem with
an nginx hostname that resolves on both sides — that arrives here with the
future TLS overlay.)

## Why Synapse has a TLS listener (and why you never see it)

lk-jwt-service validates client OpenID tokens against Synapse's federation API
(`/_matrix/federation/v1/openid/userinfo`), and its federation client speaks
HTTPS only — `LIVEKIT_INSECURE_SKIP_VERIFY_TLS` skips certificate
*verification*, not TLS. So the one-shot `init-certs` service mints a
self-signed cert (into `./data/tls/`, generated not committed) and Synapse
serves federation over TLS on 8448. `server_name` is `synapse` — the compose
service name — so gomatrixserverlib's discovery fallback (`synapse:8448`)
resolves inside the compose network. The port is not published to the host;
everything client-facing stays plain HTTP.

## Registering users and a room by hand

For the interactive examples (`join_and_record`, `connect` — or curl poking),
either use open registration:

```sh
curl -s -X POST http://localhost:8008/_matrix/client/v3/register \
  -H 'Content-Type: application/json' \
  -d '{"username": "alice", "password": "secret", "auth": {"type": "m.login.dummy"}}'
```

or the Synapse admin CLI (backed by `registration_shared_secret`, which also
keeps `/_synapse/admin/v1/register` available if open registration is ever
turned off):

```sh
docker compose -f demo/backend/docker-compose.yml exec synapse \
  register_new_matrix_user -u alice -p secret -a -c /cfg/homeserver.yaml http://localhost:8008
```

The `join_and_record` example additionally needs a room both users have
joined. With `alice` and `bob` registered as above:

```sh
TOKEN() { curl -s -X POST http://localhost:8008/_matrix/client/v3/login \
  -d "{\"type\": \"m.login.password\", \"user\": \"$1\", \"password\": \"secret\"}" | jq -r .access_token; }
ALICE=$(TOKEN alice); BOB=$(TOKEN bob)

# alice creates the room and invites bob (add "initial_state" for an encrypted room)
ROOM=$(curl -s -X POST "http://localhost:8008/_matrix/client/v3/createRoom?access_token=$ALICE" \
  -d '{"invite": ["@bob:synapse"]}' | jq -r .room_id)

# bob accepts
curl -s -X POST "http://localhost:8008/_matrix/client/v3/join/$ROOM?access_token=$BOB" -d '{}'
echo "ROOM_ID=$ROOM"
```

## Future: Element Web / Element Call + TLS

Browser clients need TLS end to end. The plan is a compose **overlay**
(`docker-compose.tls.yml`, applied with `-f docker-compose.yml -f
docker-compose.tls.yml`) adding nginx + element-web + element-call, with
`init-certs` grown into a local CA minting per-host certs. The base file stays
HTTP-only; client-facing URLs are already env-indirected (`LIVEKIT_WS_URL` in a
`.env` file overrides what lk-jwt hands to clients), and the e2e test reads
`HOMESERVER_URL` / `LIVEKIT_SERVICE_URL` / `INSECURE_TLS`, so the same test
binary will run against the TLS overlay unchanged.

## Troubleshooting

- **`up --wait` hangs on synapse** — `docker compose logs synapse`; a config
  parse error (e.g. an experimental flag renamed by a new `synapse:latest`)
  shows up there. Pin the image by digest until fixed (see the comment in
  `docker-compose.yml`).
- **lk-jwt returns 500 on `/get_token`** — usually the federation hop:
  `docker compose logs auth-service` should show the OpenID lookup failing.
  Check that `./data/tls/` contains `synapse.crt`/`synapse.key` and that
  Synapse came up after `init-certs` completed.
- **Media never flows but signalling works** — ICE. The SFU advertises
  `127.0.0.1` (see `livekit/livekit.yaml`), which host-run clients reach via
  the published UDP range `50100-50200` with TCP `7881` as fallback.
