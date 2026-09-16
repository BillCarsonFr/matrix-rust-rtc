#!/usr/bin/env bash
# Installs the packed tarball into a scratch project and exercises the
# published surface the way a consumer would: the `exports` map, the `node`
# condition and the default wasm lookup in Node, and a Vite production build
# that must emit the .wasm as an asset. Needs network (pulls @ubjs/core and
# vite from the public registry).
set -euo pipefail
cd "$(dirname "$0")/.."

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tgz="$(npm pack --pack-destination "$tmp" --silent | tail -1)"
echo "packed $tgz"

cd "$tmp"
npm init -y >/dev/null
npm install --silent --no-audit --no-fund "./$tgz"

echo "--- node: default wasm lookup through the node condition"
node --input-type=module -e '
  import { initAsync, isInitialized, computeSessionsFromEvents, FfiElementCallCompat } from "@element-hq/matrix-rtc";
  import { installConsoleLogSink } from "@element-hq/matrix-rtc/log-sink";
  import { MockMatrixDriver } from "@element-hq/matrix-rtc/testing";
  if (isInitialized()) throw new Error("initialised before initAsync");
  await initAsync();
  await initAsync(); // idempotent
  installConsoleLogSink();
  const sessions = computeSessionsFromEvents([], FfiElementCallCompat.Off);
  new MockMatrixDriver();
  console.log("ok: initialised, computeSessionsFromEvents ->", sessions.length, "sessions");
'

echo "--- node: explicit source (bytes) works too"
node --input-type=module -e '
  import { readFile } from "node:fs/promises";
  import { createRequire } from "node:module";
  import { initAsync } from "@element-hq/matrix-rtc";
  const wasm = createRequire(import.meta.url).resolve("@element-hq/matrix-rtc/wasm");
  await initAsync(await readFile(wasm));
  console.log("ok: initialised from bytes at", wasm);
'

echo "--- vite: production build emits the wasm as an asset"
npm install --silent --no-audit --no-fund -D vite
mkdir -p app
cat > app/index.html <<'HTML'
<!doctype html><html><body><script type="module" src="./main.js"></script></body></html>
HTML
cat > app/main.js <<'JS'
import { initAsync, computeSessionsFromEvents, FfiElementCallCompat } from "@element-hq/matrix-rtc";
await initAsync();
document.body.textContent = String(computeSessionsFromEvents([], FfiElementCallCompat.Off).length);
JS
npx vite build --logLevel warn app --outDir ../out --emptyOutDir >/dev/null
ls out/assets/*.wasm >/dev/null
echo "ok: vite emitted $(ls out/assets/*.wasm)"
