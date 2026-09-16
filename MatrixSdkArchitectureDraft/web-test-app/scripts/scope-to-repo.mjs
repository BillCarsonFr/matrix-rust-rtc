// Rewrites the package name's scope and `repository.url` to the GitHub
// repository given as `owner/name`, in place. GitHub Packages accepts a
// GITHUB_TOKEN publish only into the workflow repository's own owner scope,
// so CI runs this right before `npm publish`; the source tree keeps the
// canonical `@element-hq` name.
import { readFileSync, writeFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const repo = process.argv[2];
if (!/^[\w.-]+\/[\w.-]+$/.test(repo ?? "")) {
  console.error("usage: scope-to-repo.mjs <owner>/<repository>");
  process.exit(2);
}
const [owner] = repo.split("/");
const pkgPath = resolve(dirname(fileURLToPath(import.meta.url)), "..", "package.json");
const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
const unscoped = pkg.name.replace(/^@[^/]+\//, "");
const name = `@${owner.toLowerCase()}/${unscoped}`;
console.log(`${pkg.name} -> ${name}  (repository: https://github.com/${repo}.git)`);
pkg.name = name;
pkg.repository = { ...pkg.repository, type: "git", url: `https://github.com/${repo}.git` };
writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n");
