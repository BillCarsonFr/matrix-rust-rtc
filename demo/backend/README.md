# Local MatrixRTC backend for the `matrix-rtc-livekit` demo

This brings up a complete, ElementWeb-interoperable MatrixRTC stack so the
`connect` example (and future demos) can join a real call:

- **Synapse** homeserver
- **LiveKit** SFU
- **lk-jwt-service** (the MatrixRTC authorisation service, MSC4195 `/get_token`)
- **nginx** reverse proxy terminating TLS with the dev `*.m.localhost` CA
- **Element Web** (embeds Element Call) — the other participant that publishes media

Rather than re-derive Synapse / LiveKit / nginx / TLS config by hand, this reuses
[Element Call's dev backend](https://github.com/element-hq/element-call) verbatim
— it is the reference deployment and guarantees interop. `setup.sh` clones it and
starts the minimal (non-federated) subset of services.

## Prerequisites

- Docker + Docker Compose
- `git`

## 1. Start the stack

```sh
./setup.sh up
```

This clones `element-hq/element-call` (the `livekit` branch) into
`./element-call/` if absent, then runs:

```sh
docker compose -f dev-backend-docker-compose.yml up synapse auth-service livekit nginx element-web
```

Services / hostnames once up:

| Purpose            | URL                                                  |
| ------------------ | ---------------------------------------------------- |
| Synapse (CS API)   | `https://synapse.m.localhost`                        |
| RTC auth service   | `https://matrix-rtc.m.localhost/livekit/jwt`         |
| LiveKit SFU        | `wss://matrix-rtc.m.localhost/livekit/sfu`           |
| Element Web        | `http://localhost:8081`                              |

The homeserver advertises the SFU via `.well-known/matrix/client`
(`org.matrix.msc4143.rtc_foci`), served by nginx.

## 2. Trust the dev TLS CA

The stack uses a self-signed CA for `*.m.localhost`. Either trust the CA, or
accept the browser exception for each host. The CA ships in the cloned repo:

```
./element-call/backend/dev_tls_m.localhost.crt
```

- **Browser (Element Web):** visit `https://synapse.m.localhost/.well-known/matrix/client`
  and `https://matrix-rtc.m.localhost/livekit/jwt/healthz` and accept the exceptions,
  or import the CA into your browser's trust store.
- **The Rust example:** pass `INSECURE_TLS=1` (it sets
  `danger_accept_invalid_certs` on the HTTP client and disables SDK TLS
  verification) — dev only.

## 3. Create a user

```sh
./setup.sh register alice secret
```

(Wraps `register_new_matrix_user` inside the Synapse container.)

## 4. Join a call from Element Web

Open `http://localhost:8081`, log in as your user, create/open a room, and start
a call. Element Web publishes audio/video into the LiveKit room.

## 5. Run the connect example against it

```sh
HOMESERVER_URL=https://synapse.m.localhost \
MX_USER=alice MX_PASSWORD=secret \
ROOM_ID='!yourroom:synapse.m.localhost' \
SLOT_ID='m.call#ROOM' \
LIVEKIT_SERVICE_URL=https://matrix-rtc.m.localhost/livekit/jwt \
INSECURE_TLS=1 \
cargo run -p matrix-rtc-livekit --example connect --features matrix-sdk
```

The example logs in, fetches an SFU token, connects, and prints a line for each
remote track it subscribes to from the Element Web participant.

## Tear down

```sh
./setup.sh down
```

> [!NOTE]
> The Element Call `livekit` branch is a moving target. If a service fails to
> start after an update, `rm -rf element-call` and re-run `./setup.sh up` to
> re-clone, or pin the clone to a known-good commit in `setup.sh`.
