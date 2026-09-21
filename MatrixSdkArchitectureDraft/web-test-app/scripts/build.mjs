// The package build, in order:
//   1. pins            versions that must move together (scripts/check-pins.mjs)
//   2. ubrn            compile the crate for wasm32 (release) + generate the TS bindings
//   3. types stub      wasm-bindgen ships no .d.ts; copy ours next to index.js
//   4. wasm-opt        binaryen -Oz on the binary
//   5. tsc             ESM + .d.ts into dist/
//   6. copy            the wasm-bindgen glue and binary into dist/
//   7. pack check      the tarball contains dist/, README, LICENSE and nothing else
//
// `--dev` builds the test-only variant (ubrn.dev.config.yaml, runtime-probe
// feature) into src/generated-dev and stops after step 3: nothing of it is
// published, and test/runtimeProbe.test.ts loads it from there directly.
import { execFileSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { readFileSync } from "node:fs";

const pkgDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const dev = process.argv.includes("--dev");
const require = createRequire(import.meta.url);

const run = (cmd, args, opts = {}) => {
  console.log(`\n$ ${cmd} ${args.join(" ")}`);
  execFileSync(cmd, args, { cwd: pkgDir, stdio: "inherit", ...opts });
};
const bin = (name) => resolve(pkgDir, "node_modules", ".bin", name);
const kb = (n) => `${(n / 1024).toFixed(0)} KB`;
const report = (label, file) => {
  const bytes = readFileSync(file);
  console.log(`${label}: ${kb(bytes.length)} (${kb(gzipSync(bytes).length)} gzipped)`);
};

const generated = resolve(pkgDir, dev ? "src/generated-dev" : "src/generated");
const wasmDir = resolve(generated, "wasm-bindgen");
const wasm = resolve(wasmDir, "index_bg.wasm");

// 1
run("node", ["scripts/check-pins.mjs"]);

// 2
run(bin("ubrn"), ["build", "web", "--profile", "web", "--config", dev ? "ubrn.dev.config.yaml" : "ubrn.config.yaml"]);
if (!existsSync(wasm)) throw new Error(`ubrn produced no ${wasm}`);
report("wasm after ubrn", wasm);

// 3
cpSync(resolve(pkgDir, "src/wasm-bindgen-types/index.d.ts"), resolve(wasmDir, "index.d.ts"));

if (dev) {
  console.log("\ndev build done:", generated);
  process.exit(0);
}

// 4 — binaryen from npm, so no system install. The feature flags match what
// rustc enables for wasm32-unknown-unknown by default.
run(bin("wasm-opt"), [
  "-Oz",
  "--enable-bulk-memory",
  "--enable-reference-types",
  "--enable-nontrapping-float-to-int",
  "--enable-sign-ext",
  "--enable-mutable-globals",
  "--enable-multivalue",
  "-o", wasm,
  wasm,
]);
report("wasm after wasm-opt", wasm);

// 5 — from a clean dist/: a stale file from an earlier build (or a demo
// `vite build` that once wrote here) must not end up in the tarball.
rmSync(resolve(pkgDir, "dist"), { recursive: true, force: true });
// Two passes. The node entry needs @types/node; compiled together, tsc's
// declaration emit would inline Node's `ErrorConstructor` augmentation
// (`prepareStackTrace(..., NodeJS.CallSite[])`) into every generated error
// class, and the shipped .d.ts would only compile for consumers that have
// @types/node. So: the node entry first, then everything else without Node
// types, overwriting the shared files.
run(bin("tsc"), ["-p", "tsconfig.build.node.json"]);
run(bin("tsc"), ["-p", "tsconfig.build.json"]);

// 6
const distWasmDir = resolve(pkgDir, "dist/generated/wasm-bindgen");
mkdirSync(distWasmDir, { recursive: true });
for (const f of ["index.js", "index.d.ts", "index_bg.wasm"]) cpSync(resolve(wasmDir, f), resolve(distWasmDir, f));

// 7 — everything the exports map points at exists, and the tarball is tight.
const pkg = JSON.parse(readFileSync(resolve(pkgDir, "package.json"), "utf8"));
const targets = [];
for (const entry of Object.values(pkg.exports)) {
  if (typeof entry === "string") targets.push(entry);
  else targets.push(...Object.values(entry));
}
for (const t of targets) {
  if (!existsSync(resolve(pkgDir, t))) throw new Error(`exports map points at a missing file: ${t}`);
}
const packed = JSON.parse(execFileSync("npm", ["pack", "--dry-run", "--json"], { cwd: pkgDir, encoding: "utf8" }))[0];
const allowed =
  /^(README\.md|LICENSE|package\.json|dist\/(index|index\.node|init|log-sink)\.(js|d\.ts)|dist\/(drivers|testing)\/[\w-]+\.(js|d\.ts)|dist\/generated\/(matrix_rtc|matrix_rtc-ffi)\.(js|d\.ts)|dist\/generated\/wasm-bindgen\/(index\.js|index\.d\.ts|index_bg\.wasm))$/;
const stray = packed.files.map((f) => f.path).filter((p) => !allowed.test(p));
if (stray.length) throw new Error(`tarball would contain unexpected files:\n  ${stray.join("\n  ")}`);
console.log(`\n${packed.filename}: ${packed.entryCount} files, ${kb(packed.unpackedSize)} unpacked`);
report("dist wasm", resolve(distWasmDir, "index_bg.wasm"));
