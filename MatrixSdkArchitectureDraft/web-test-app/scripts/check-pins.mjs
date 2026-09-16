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
// The npm tarball ships ubrn's workspace Cargo.toml but no Cargo.lock (one
// appears only after the CLI has compiled itself once), so the versions
// ubrn was built against come from its `[workspace.dependencies]`.
const ubrnToml = read(resolve(ubrnDir, "Cargo.toml"));

const must = (re, text, what) => {
  const m = text.match(re);
  if (!m) throw new Error(`could not find ${what}`);
  return m[1];
};
const ubrnDep = (name) =>
  must(new RegExp(`^${name} = "=([^"]+)"`, "m"), ubrnToml, `${name} pin in ubrn's Cargo.toml`);
// ubrn pins `uniffi = "=0.31"`, i.e. any 0.31.x: compare on as many components
// as the shorter side states.
const samePrefix = (a, b) => {
  const n = Math.min(a.split(".").length, b.split(".").length);
  return a.split(".").slice(0, n).join(".") === b.split(".").slice(0, n).join(".");
};

const crateVersion = must(/^version = "([^"]+)"/m, crateToml, "crate version");
const crateUniffi = must(/uniffi = \{ version = "=([^"]+)"/, crateToml, "uniffi pin in Cargo.toml");
const crateWasmBindgenFutures = must(/wasm-bindgen-futures = "=([^"]+)"/, crateToml, "wasm-bindgen-futures pin");
const patchWasmBindgen = must(/wasm-bindgen = "=([^"]+)"/, patchToml, "wasm-bindgen pin in Cargo.patch.toml");

const checks = [
  ["package.json version", pkg.version, "Cargo.toml version", crateVersion],
  ["dependencies[@ubjs/core]", pkg.dependencies?.["@ubjs/core"], "devDependencies[uniffi-bindgen-react-native]", pkg.devDependencies?.["uniffi-bindgen-react-native"]],
  ["devDependencies[uniffi-bindgen-react-native]", pkg.devDependencies?.["uniffi-bindgen-react-native"], "installed ubrn", ubrnPkg.version],
  ["Cargo.toml uniffi", crateUniffi, "ubrn's uniffi", ubrnDep("uniffi"), samePrefix],
  ["Cargo.patch.toml wasm-bindgen", patchWasmBindgen, "ubrn's wasm-bindgen-cli-support", ubrnDep("wasm-bindgen-cli-support")],
];

let failed = false;
for (const [aName, a, bName, b, eq = (x, y) => x === y] of checks) {
  const ok = a !== undefined && b !== undefined && eq(a, b);
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
