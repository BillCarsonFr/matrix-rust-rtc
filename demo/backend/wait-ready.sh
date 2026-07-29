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

set -euo pipefail

probe() {
  local name="$1" url="$2"
  for _ in $(seq 1 60); do
    if curl -fsS -o /dev/null "$url"; then
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

echo "[wait-ready] backend stack is ready"
