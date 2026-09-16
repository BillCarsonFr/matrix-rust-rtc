// Fails the build when any of the coupled versions drift. The generated
// bindings call into `@ubjs/core` internals and the wasm-bindgen schema must
// match the CLI that produced the glue, so these are hard errors, not
// warnings.
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const pkgDir = resolve(here, "..");
const crateDir = resolve(pkgDir, "..");
const require = createRequire(import.meta.url);

const read = (p) => readFileSync(p, "utf8");
const pkg = JSON.parse(read(resolve(pkgDir, "package.json")));
const crateToml = read(resolve(crateDir, "Cargo.toml"));
const patchToml = read(resolve(pkgDir, "Cargo.patch.toml"));

const ubrnDir = dirname(require.resolve("uniffi-bindgen-react-native/package.json"));
const ubrnPkg = JSON.parse(read(resolve(ubrnDir, "package.json")));
const ubrnLock = read(resolve(ubrnDir, "Cargo.lock"));

const must = (re, text, what) => {
  const m = text.match(re);
  if (!m) throw new Error(`could not find ${what}`);
  return m[1];
};
const lockVersion = (name) =>
  must(new RegExp(`name = "${name}"\\nversion = "([^"]+)"`), ubrnLock, `${name} in ubrn's Cargo.lock`);

const crateVersion = must(/^version = "([^"]+)"/m, crateToml, "crate version");
const crateUniffi = must(/uniffi = \{ version = "=([^"]+)"/, crateToml, "uniffi pin in Cargo.toml");
const crateWasmBindgenFutures = must(/wasm-bindgen-futures = "=([^"]+)"/, crateToml, "wasm-bindgen-futures pin");
const patchWasmBindgen = must(/wasm-bindgen = "=([^"]+)"/, patchToml, "wasm-bindgen pin in Cargo.patch.toml");

const checks = [
  ["package.json version", pkg.version, "Cargo.toml version", crateVersion],
  ["dependencies[@ubjs/core]", pkg.dependencies?.["@ubjs/core"], "devDependencies[uniffi-bindgen-react-native]", pkg.devDependencies?.["uniffi-bindgen-react-native"]],
  ["devDependencies[uniffi-bindgen-react-native]", pkg.devDependencies?.["uniffi-bindgen-react-native"], "installed ubrn", ubrnPkg.version],
  ["Cargo.toml uniffi", crateUniffi, "ubrn's uniffi", lockVersion("uniffi")],
  ["Cargo.patch.toml wasm-bindgen", patchWasmBindgen, "ubrn's wasm-bindgen-cli-support", lockVersion("wasm-bindgen-cli-support")],
];

let failed = false;
for (const [aName, a, bName, b] of checks) {
  const ok = a !== undefined && a === b;
  console.log(`${ok ? "ok  " : "FAIL"} ${aName} = ${a}  ${ok ? "==" : "!="}  ${bName} = ${b}`);
  if (!ok) failed = true;
}
// wasm-bindgen-futures 0.4.x pairs with wasm-bindgen 0.2.(x+50): 0.4.50 <-> 0.2.100.
const expectedFutures = `0.4.${Number(patchWasmBindgen.split(".")[2]) - 50}`;
const futuresOk = crateWasmBindgenFutures === expectedFutures;
console.log(`${futuresOk ? "ok  " : "FAIL"} Cargo.toml wasm-bindgen-futures = ${crateWasmBindgenFutures}  ${futuresOk ? "pairs with" : "does not pair with"}  wasm-bindgen ${patchWasmBindgen}`);
if (!futuresOk) failed = true;

if (failed) {
  console.error("\nversion pins drifted — bump them together (see README, 'Gotchas')");
  process.exit(1);
}
