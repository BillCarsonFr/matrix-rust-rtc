# Publishing the `matrix-rtc` crate as an npm package

Status: proposal, 2026-09-16. Companion to `architecture-merge-plan.md`
(phase 4 there assumes this package exists).

**Implementation (2026-09-16):** sections 2, 3, 4 and 5 are implemented in
`MatrixSdkArchitectureDraft/web-test-app` (see its README) and
`.github/workflows/npm-web-bindings.yml`, with the registry choice changed to
**GitHub Packages** (`publishConfig.registry = https://npm.pkg.github.com`,
published with the workflow `GITHUB_TOKEN`; consumers need a `read:packages`
token). Deviations from the text below: the size profile is a custom cargo
profile `web` (ubrn cannot retype `release.opt-level`), `dist/` keeps the
`generated/` subdirectory, and the runtime-probe crate split (§7 step 6) is
not done. Paths are relative to
`MatrixSdkArchitectureDraft/` unless stated; after the merge they move to
`crates/matrix-rtc` and `web/`.

Goal: Element Call (and any other web or Node host) depends on a versioned npm
package instead of running `scripts/sync-matrix-rtc-sdk.sh` to vendor generated
TypeScript. One build, one artifact, one version number that names the exact
Rust crate it was compiled from.

---

## 1. What exists today

`web-test-app/` already produces everything the package needs, but as a private
test harness:

| Piece | Today | Notes |
|---|---|---|
| Bindings generator | `uniffi-bindgen-react-native` 0.31.0-5, `npm run ubrn:web` = `ubrn build web --config ubrn.config.yaml` | Builds the generated shim crate `rust_modules/wasm` (edition 2018, `crate-type cdylib`) for `wasm32-unknown-unknown`, then runs wasm-bindgen in-process (target `web`, `--omit-default-module-path`, out-name `index`) |
| Generated TS | `src/generated/matrix_rtc.ts` (400 KB, 127 exports, 276 doc blocks carried over from Rust) and `matrix_rtc-ffi.ts` (12 KB) | Both are `// @ts-nocheck`; runtime imports are `@ubjs/core` and `./wasm-bindgen/index.js` |
| Wasm | `src/generated/wasm-bindgen/index.js` (123 KB) + `index_bg.wasm` (1.64 MB, release, not `wasm-opt`'d) | No `index.d.ts` is emitted: ubrn drives `wasm-bindgen-cli-support` with TypeScript output off and only maps `--no-typescript`, not the inverse |
| Entry point | ubrn-generated `src/index.web.ts` (imports the `.wasm` directly; Vite cannot bundle that) and the hand-written `src/wasmLoader.ts` (`?url` import, Vite-only) | Two entry points, both bundler-specific. Node tests use a third (`test/wasmInit.ts`, `readFileSync`) |
| Host-side code | `src/logSink.ts` (console `LogSink`), `src/jsSdkDriver.ts` (463 lines, `MatrixDriverCallback` over matrix-js-sdk ≥ 42), `src/mockDriver.ts` (508 lines, homeserver mock with simulated peers), `src/app.ts` (demo) | `jsSdkDriver` and `logSink` are useful to every consumer; `mockDriver` to consumers' tests; `app.ts` is demo only |
| Feature sets | `ubrn.config.yaml` builds `uniffi` + `runtime-probe`; `ubrn.element-call.config.yaml` builds `uniffi` only into `src/generated-element-call` | Two different binding surfaces from one crate. The probe (`src/uniffi_api/runtime_probe.rs`) is test-only |
| Version pins | `uniffi = "=0.31.2"` ↔ ubrn `0.31.0-5` ↔ `@ubjs/core ^0.31.0-5` ↔ `wasm-bindgen = "=0.2.100"` (`Cargo.patch.toml`) ↔ `wasm-bindgen-futures = "=0.4.50"` | Move together or the build breaks; there is no version check today beyond a comment |
| Package metadata | `name: matrix-rtc-web`, `private: true`, `browser: src/index.web.ts`, `matrix-js-sdk` as a hard dependency | Nothing publishable yet |
| Consumer | Element Call vendors `src/generated-element-call/` via its own sync script | Script not in this repo; its expectations (package name, import path) need confirming before choosing the name |

---

## 2. Package design

### 2.1 Name, version, license

- **Name**: `@element-hq/matrix-rtc` (scoped, public). Alternative `matrix-rtc-sdk`
  if the scope is not wanted. Confirm against what Element Call's sync script
  imports so the switch is a one-line change there.
- **Version**: the crate's `Cargo.toml` version, verbatim. The package is a
  build of the crate, so a second version number would only invite drift. A
  `scripts/sync-version.mjs` copies `version` from `Cargo.toml` into
  `package.json` and fails the build if they differ. Pre-1.0: breaking changes
  bump the minor. Pre-releases as `0.x.y-alpha.N`, published under the `next`
  dist-tag, `latest` only for tagged releases.
- **License**: `AGPL-3.0-only`, same as the workspace; ship `LICENSE`. Runtime
  dependency `@ubjs/core` is MPL-2.0, which is compatible.

### 2.2 Layout and exports

```
web/                              (was web-test-app)
  package.json                    public, files: ["dist", "README.md", "LICENSE"]
  ubrn.config.yaml                features: [uniffi]     → the published build
  ubrn.dev.config.yaml            features: [uniffi, runtime-probe] → tests only
  Cargo.patch.toml                wasm-bindgen pin + release profile (§3.2)
  src/
    index.ts                      public entry (§2.3)
    index.node.ts                 Node entry: same API, reads the .wasm from disk
    log-sink.ts                   ConsoleLogSink, installConsoleLogSink
    drivers/matrix-js-sdk.ts      JsSdkMatrixDriver, createJsSdkBackend
    testing/mock-driver.ts        MockMatrixDriver + simulated peers
    generated/                    gitignored, produced by ubrn
      matrix_rtc.ts, matrix_rtc-ffi.ts, wasm-bindgen/{index.js, index_bg.wasm}
    generated/wasm-bindgen/index.d.ts   checked in: hand-written stub (§3.3)
  dist/                           tsc output + copied wasm-bindgen files
  demo/                           app.ts, index.html, vite config (not published)
  test/                           vitest suites, run against dist/
```

`package.json` `exports`:

| Specifier | Resolves to | Purpose |
|---|---|---|
| `.` | `dist/index.js` (`node` condition → `dist/index.node.js`), types `dist/index.d.ts` | All generated bindings re-exported, plus `initAsync`, `isInitialized` |
| `./wasm` | `dist/wasm-bindgen/index_bg.wasm` | Lets a host get the asset URL its own way (`new URL('@element-hq/matrix-rtc/wasm', import.meta.url)`, Vite `?url`) |
| `./log-sink` | `dist/log-sink.js` | Optional console sink |
| `./driver/matrix-js-sdk` | `dist/drivers/matrix-js-sdk.js` | `MatrixDriverCallback` over matrix-js-sdk; `matrix-js-sdk >= 42` becomes an **optional peer dependency** |
| `./testing` | `dist/testing/mock-driver.js` | Homeserver mock for consumers' unit tests |
| `./package.json` | itself | Tooling |

`dependencies`: `@ubjs/core` pinned **exactly** to the ubrn version that generated
the bindings (the generated code calls its internals; a caret range here is a
runtime break waiting to happen). `peerDependencies`: `matrix-js-sdk` (optional).
`sideEffects`: leave unset. The generated module has top-level state (`uniffiCaller`,
converters) that must not be tree-shaken away.

### 2.3 Initialisation API

Neither ubrn's generated `index.web.ts` (direct `.wasm` import) nor `wasmLoader.ts`
(`?url`) is portable. The public entry owns loading and stays bundler-neutral:

```ts
export type WasmSource = URL | string | Request | Response | BufferSource | WebAssembly.Module;

/** Load and initialise the bindings. Idempotent; concurrent callers share one promise. */
export async function initAsync(source?: WasmSource): Promise<void>;
export function isInitialized(): boolean;
export * from "./generated/matrix_rtc";
```

- Default `source` is `new URL("./wasm-bindgen/index_bg.wasm", import.meta.url)`,
  which Vite, webpack 5, Rollup and Node ESM all rewrite or resolve correctly.
  This is exactly the default that wasm-bindgen's `web` target would emit if
  ubrn did not pass `--omit-default-module-path`; we re-add it in our own file
  so the generated code stays untouched.
- `index.node.ts` (selected by the `node` export condition) resolves a `file:`
  URL through `fs.readFile` because Node's `fetch` does not serve `file:`. The
  existing `test/wasmInit.ts` becomes this file.
- After `initAsync` from wasm-bindgen resolves, call the generated
  `bindings.initialize()` once (checksums and callback vtables), then mark
  initialised. Every exported object throws a clear error if used before
  `initAsync`; today that is an opaque "wasm is undefined".
- No log sink is installed by default. Hosts call `setLogSink` from the bindings
  or `installConsoleLogSink()` from `./log-sink`.

### 2.4 Crate-side changes

- Add `console_error_panic_hook` on `wasm32` and call `set_once()` from the
  bindings' initialisation path (ubrn's troubleshooting guide; a panic today is
  `RuntimeError: unreachable` with no message).
- Move `runtime_probe` out of the published crate into a test-only crate
  (`crates/matrix-rtc-runtime-probe`, depending on `matrix-rtc`) with its own
  ubrn config. Until then the dev config keeps the feature and the published
  config omits it (§4 explains how tests still cover the published build).
- Pin the toolchain with a `rust-toolchain.toml` at the repo root (stable
  channel, `wasm32-unknown-unknown` target) so CI and developers build identical
  wasm.

---

## 3. Build pipeline

`npm run build` runs, in order:

### 3.1 `ubrn build web --release --config ubrn.config.yaml`

Produces `src/generated/`. `--release` matters: the debug shim is 42 MB, the
release one 2 MB before wasm-bindgen.

### 3.2 Size: release profile and `wasm-opt`

`Cargo.patch.toml` is merged into the generated shim manifest, so it is the
place to tune the profile without touching the crate:

```toml
[dependencies]
wasm-bindgen = "=0.2.100"

[profile.release]
opt-level = "z"
lto = true
codegen-units = 1
panic = "abort"
strip = true
```

Then `wasm-opt -Oz --enable-bulk-memory --enable-reference-types` (binaryen, via
the `binaryen` npm package so it needs no system install) on
`index_bg.wasm`. Measure before committing to `opt-level = "z"`: it trades speed
for size and the crate does real work per event. Record the size in the CI
summary; today's baseline is 1.64 MB uncompressed.

### 3.3 TypeScript output

`tsc -p tsconfig.build.json` emits ESM `.js` + `.d.ts` for `src/**` into `dist/`,
`target: ES2022` (the generated code uses `FinalizationRegistry` and `bigint`),
`moduleResolution: bundler`, `declaration: true`, `skipLibCheck: true`.

Two things stand in the way and both are fixable locally:

- `src/generated/wasm-bindgen/index.js` has no types. Until ubrn exposes
  wasm-bindgen's TypeScript output (a one-line `bindgen.typescript(true)`; worth
  an upstream PR), check in a hand-written `index.d.ts` covering what
  `matrix_rtc.ts` uses: `export default function initAsync(opts?: { module_or_path?: WasmSource }): Promise<InitOutput>`,
  `export class RustCallStatus`, and the `uniffi_*` export namespace typed as
  `Record<string, Function>`. `matrix_rtc.ts` is `@ts-nocheck`, so nothing else
  needs to type-check against it.
- The generated files reference `process.env.NODE_ENV` (5 sites) to pick debug
  checks. Fine in bundlers and Node; a plain `<script type="module">` consumer
  gets `typeof process !== "object"` → debug on. Document, do not patch.

### 3.4 Copy and verify

Copy `src/generated/wasm-bindgen/{index.js,index_bg.wasm}` into
`dist/wasm-bindgen/`. Then `npm pack --dry-run` and assert the tarball contains
exactly `dist/**`, `README.md`, `LICENSE`, `package.json` and nothing from
`src/generated/` or `rust_modules/`.

### 3.5 Version and pin checks (fail the build, do not warn)

- `package.json` version equals `Cargo.toml` version.
- `dependencies["@ubjs/core"]` equals `devDependencies["uniffi-bindgen-react-native"]`.
- The `uniffi` version in `Cargo.toml` matches the uniffi version ubrn was built
  for (read from ubrn's `Cargo.toml` in `node_modules`).
- `Cargo.patch.toml`'s `wasm-bindgen` pin matches the version compiled into ubrn
  (`wasm-bindgen-cli-support` in its `Cargo.lock`).

---

## 4. Testing what gets published

The vitest suites (`participation`, `session`, `ownMembership`, `encryption`,
37 tests) import `../src/generated/...` today. Repoint them at `dist/` through a
vitest alias so they exercise the compiled package, not the TS sources. The
`runtimeProbe` suite needs the probe feature, so it runs against the dev build
(`ubrn.dev.config.yaml`, output to `src/generated-dev/`) until the probe moves to
its own crate.

Add three package-level checks:

1. **Tarball install test**: `npm pack`, install the tarball into a scratch
   directory with a minimal Vite app and a minimal Node script, run
   `initAsync()` with no arguments in both, join a mock session. This is the
   only test of the `exports` map, the `node` condition and the default wasm URL.
2. **Type test**: a `tsc --noEmit` over a consumer file that imports every
   subpath, with `skipLibCheck: false`, so the shipped `.d.ts` files are
   themselves valid.
3. **Backend test** (`test/backend.test.ts`, opt-in) unchanged, against the
   docker stack, driven through `./driver/matrix-js-sdk`.

---

## 5. Publishing workflow

`.github/workflows/publish-npm.yml`:

- **On pull request and push to main**: build, test, `npm pack --dry-run`, and
  post the wasm size and the public-API diff (diff of `dist/matrix_rtc.d.ts`
  against the last published version, fetched with `npm pack @element-hq/matrix-rtc@latest`)
  as a job summary. A binding-surface change becomes visible in review instead
  of after release.
- **On push to main**: publish `0.x.y-alpha.<run-number>` under dist-tag `next`,
  so Element Call can track main without vendoring.
- **On tag `matrix-rtc-v*`**: publish the tagged version under `latest` with
  `npm publish --provenance --access public`. Prefer npm trusted publishing
  (OIDC from GitHub Actions) over a long-lived `NPM_TOKEN`; if a token is
  unavoidable, scope it to this package and store it as an environment secret
  with required reviewers.
- Runner setup: `dtolnay/rust-toolchain` reading `rust-toolchain.toml` with the
  wasm32 target, Node 22 LTS, `npm ci`, Rust and cargo caches keyed on
  `Cargo.lock` + `Cargo.patch.toml`. ubrn compiles wasm-bindgen in-process, so no
  `wasm-bindgen-cli` install is needed; binaryen comes from npm.

Release checklist (a `RELEASING.md` next to the package):

1. Bump `version` in `crates/matrix-rtc/Cargo.toml`; run `npm run sync-version`.
2. Update `CHANGELOG.md`: the generated TypeScript surface *is* the API, so list
   added/removed/renamed uniffi items, not Rust internals.
3. Tag `matrix-rtc-v<version>`, push; CI publishes.
4. Bump the dependency in Element Call, delete its sync script.

---

## 6. Consumer guidance (goes into the package README)

- Vite: no config needed; `initAsync()` default works. To control caching or CDN
  placement: `import wasmUrl from '@element-hq/matrix-rtc/wasm?url'; await initAsync(wasmUrl)`.
- webpack 5: `experiments.asyncWebAssembly` is **not** required (the wasm is
  fetched, not imported); ensure `.wasm` is emitted as an asset
  (`type: 'asset/resource'`) so `new URL(..., import.meta.url)` resolves.
- Node ≥ 20: `import { initAsync } from '@element-hq/matrix-rtc'` picks the node
  entry; `initAsync()` reads the file. Useful for vitest and for bots.
- Serve `.wasm` as `application/wasm` so browsers use streaming compilation.
- No COOP/COEP headers are needed: the build is single-threaded
  (`wasm-unstable-single-threaded`), no `SharedArrayBuffer`.
- The reconciliation loop (LiveKit rooms keyed by `service_url`, keys by
  `transport_identity`) from `MatrixSdkArchitecture.md` "Usage", in TypeScript,
  as the README's main example.

---

## 7. Sequencing

1. **Package skeleton** (no publish): rename the directory, `private: false`,
   `exports` map, `index.ts`/`index.node.ts`, move `logSink`/`jsSdkDriver`/
   `mockDriver` to their subpaths, `matrix-js-sdk` to optional peer, build
   scripts, `tsconfig.build.json`, wasm-bindgen `index.d.ts` stub, pin checks.
   Gate: `npm run build && npm test` against `dist/`, tarball install test green.
2. **Crate hygiene**: `console_error_panic_hook`, `rust-toolchain.toml`,
   `Cargo.patch.toml` release profile, `wasm-opt`, size baseline recorded.
3. **CI**: PR job with size and API diff; `next` publishing from main
   (needs the npm scope and trusted-publisher configuration first).
4. **Element Call**: switch from the sync script to `@element-hq/matrix-rtc@next`;
   keep the `ubrn.element-call.config.yaml` until that lands, then delete it.
5. **First tagged release** once the merge plan's phase 4 makes this `web/` the
   only web binding in the repo. Tag `matrix-rtc-v0.1.0`.
6. **Later**: probe crate split; upstream the ubrn TypeScript-output flag and
   drop the hand-written `index.d.ts`.

---

## 8. Open questions and risks

- **Package name and scope** need confirming with whoever owns the npm org and
  with Element Call's current import path.
- **Two surfaces, one crate**: as long as `runtime-probe` is a feature of the
  published crate, the tested build and the published build differ by that
  feature. The probe crate split removes the difference; until then the tarball
  test on the published build is the guard.
- **ubrn is pre-1.0** (`0.31.0-5`) and its generated code depends on `@ubjs/core`
  internals. Every ubrn bump is a coordinated bump of five pins (§1) and a
  regenerate; treat it as a release in its own right and run the full suite.
- **Wasm size** (1.64 MB today) is the main user-visible cost. The profile and
  `wasm-opt` changes are expected to bring it under 1 MB uncompressed, but
  `opt-level = "z"` needs a quick throughput check on the key-rotation
  simulation before it is adopted.
- **AGPL for a library on npm** is unusual and will be asked about. It matches
  the workspace license; if a more permissive license is wanted for the
  bindings package, that decision is upstream of this plan.
- **API stability**: everything `export`ed from `matrix_rtc.ts` is public. The
  Ffi-prefixed names (`FfiParticipationManager`, `FfiStatus`, ...) will be what
  consumers type. Decide before 0.1.0 whether to keep the prefix (matches
  Kotlin/Swift, one set of docs) or strip it in the crate's uniffi layer for
  the JS audience; renaming after publishing is a breaking change.
