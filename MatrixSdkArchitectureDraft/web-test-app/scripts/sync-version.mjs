// Copies the crate version from ../Cargo.toml into package.json. The package
// is a build of the crate, so it has no version of its own.
import { readFileSync, writeFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const pkgDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const crateToml = readFileSync(resolve(pkgDir, "..", "Cargo.toml"), "utf8");
const version = crateToml.match(/^version = "([^"]+)"/m)?.[1];
if (!version) throw new Error("no version in ../Cargo.toml");

const pkgPath = resolve(pkgDir, "package.json");
const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
if (pkg.version === version) {
  console.log(`package.json already at ${version}`);
} else {
  console.log(`package.json ${pkg.version} -> ${version}`);
  pkg.version = version;
  writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n");
}
