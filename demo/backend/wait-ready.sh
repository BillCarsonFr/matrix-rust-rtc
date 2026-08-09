#!/usr/bin/env bash
# Copyright 2026 Valere Fedronic
#
# This file is part of matrix-rust-rtc.
#
# matrix-rust-rtc is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version. See <https://www.gnu.org/licenses/>.
#
# Host-side readiness probe for the MatrixRTC backend stack. `docker compose up
# --wait` already gates on the synapse and livekit healthchecks; this exists
# mainly for lk-jwt-service, whose FROM-scratch image cannot carry a compose
# healthcheck (no shell, and 0.4.4 predates the healthcheck binary).
#
# With --interop it additionally probes the TLS overlay
# (docker-compose.interop.yml): the proxied homeserver, the MatrixRTC origin,
# Element Web, and the Element Call widget Element Web bundles.

set -euo pipefail

INTEROP=0
if [ "${1:-}" = "--interop" ]; then
  INTEROP=1
fi

probe() {
  local name="$1" url="$2"
  shift 2
  for _ in $(seq 1 60); do
    if curl -fsS -o /dev/null "$@" "$url"; then
      echo "[wait-ready] $name is up ($url)"
      return 0
    fi
    sleep 2
  done
  echo "[wait-ready] ERROR: $name never became ready at $url" >&2
  return 1
}

probe "synapse" "http://localhost:8008/health"
probe "lk-jwt-service" "http://localhost:6080/healthz"
probe "livekit" "http://localhost:7880/"
probe "lk-jwt-service-2" "http://localhost:6081/healthz"
probe "livekit-2" "http://localhost:7890/"

if [ "$INTEROP" = "1" ]; then
  # -k: the stack's CA is minted at up time, and whether the *host* trusts it
  # is a separate concern from whether the services are answering. The Rust
  # client does need that trust — see INTEROP.md.
  if ! curl -fsS -o /dev/null -k --max-time 5 https://synapse.m.localhost/_matrix/client/versions; then
    if ! grep -qE '^\s*[^#].*\bsynapse\.m\.localhost\b' /etc/hosts 2>/dev/null; then
      echo "[wait-ready] ERROR: synapse.m.localhost does not resolve. Add to /etc/hosts:" >&2
      echo "  127.0.0.1 synapse.m.localhost matrix-rtc.m.localhost app.m.localhost" >&2
      exit 1
    fi
  fi

  probe "synapse (via nginx)" "https://synapse.m.localhost/_matrix/client/versions" -k
  probe "rtc focus well-known" "https://synapse.m.localhost/.well-known/matrix/client" -k
  probe "lk-jwt-service (via nginx)" "https://matrix-rtc.m.localhost/livekit/jwt/healthz" -k
  probe "element-web" "https://app.m.localhost/" -k
  # The interop test drives the Element Call *bundled into Element Web*. If
  # that path 404s, every selector downstream fails in a way that reads like a
  # test bug — catch it here instead.
  probe "element-call (bundled)" "https://app.m.localhost/widgets/element-call/index.html" -k

  echo "[wait-ready] interop stack is ready"
  exit 0
fi

echo "[wait-ready] backend stack is ready"
