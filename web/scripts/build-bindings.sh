#!/bin/bash
# Copyright 2026 Valere Fedronic
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

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

