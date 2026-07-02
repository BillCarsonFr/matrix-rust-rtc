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
# Drives Element Call's dev backend (Synapse + LiveKit + lk-jwt-service + nginx
# + Element Web) for the matrix-rtc-livekit demo. See README.md.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EC_DIR="$SCRIPT_DIR/element-call"
EC_REPO="https://github.com/element-hq/element-call.git"
EC_BRANCH="livekit"
COMPOSE_FILE="dev-backend-docker-compose.yml"
# Minimal non-federated subset (no *-1 services).
SERVICES="synapse auth-service livekit nginx element-web"

ensure_clone() {
  if [ ! -d "$EC_DIR/.git" ]; then
    echo "Cloning element-call ($EC_BRANCH) into $EC_DIR ..."
    git clone --depth 1 --branch "$EC_BRANCH" "$EC_REPO" "$EC_DIR"
  fi
}

compose() {
  ( cd "$EC_DIR" && docker compose -f "$COMPOSE_FILE" "$@" )
}

cmd="${1:-up}"
case "$cmd" in
  up)
    ensure_clone
    # shellcheck disable=SC2086
    compose up $SERVICES
    ;;
  down)
    ensure_clone
    compose down
    ;;
  register)
    user="${2:?usage: setup.sh register <user> <password>}"
    password="${3:?usage: setup.sh register <user> <password>}"
    # EC's dev homeserver config lives at /data/homeserver.yaml in the container.
    compose exec synapse register_new_matrix_user \
      -u "$user" -p "$password" -a \
      -c /data/homeserver.yaml http://localhost:8008
    ;;
  *)
    echo "usage: setup.sh [up|down|register <user> <password>]" >&2
    exit 1
    ;;
esac
