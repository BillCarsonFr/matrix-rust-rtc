#!/bin/bash
# Copyright 2026 Valere Fedronic
#
# This file is part of matrix-rust-rtc.
#
# matrix-rust-rtc is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# matrix-rust-rtc is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEB_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$WEB_DIR")"
WASM_CRATE="$ROOT_DIR/crates/matrix-rtc-wasm"
BROWSER_OUT="$WEB_DIR/pkg/browser"
NODE_OUT="$WEB_DIR/pkg/node"

if ! command -v wasm-pack >/dev/null 2>&1; then
  echo "wasm-pack is required. Install it with: cargo install wasm-pack"
  exit 1
fi

rm -rf "$BROWSER_OUT" "$NODE_OUT"
mkdir -p "$BROWSER_OUT" "$NODE_OUT"

echo "Building browser bindings..."
wasm-pack build "$WASM_CRATE" --release --target web --out-dir "$BROWSER_OUT" --out-name matrix_rtc_wasm

echo "Building node bindings..."
wasm-pack build "$WASM_CRATE" --release --target nodejs --out-dir "$NODE_OUT" --out-name matrix_rtc_wasm

echo "Done. Generated bindings in:"
echo "  $BROWSER_OUT"
echo "  $NODE_OUT"

