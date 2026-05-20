import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';

const packageJsonPath = new URL('../package.json', import.meta.url);
const packageJson = JSON.parse(await readFile(packageJsonPath, 'utf8'));

test('package exports are browser-first', () => {
  assert.equal(packageJson.name, 'matrix-rtc-wasm');
  assert.equal(packageJson.private, true);
  assert.equal(packageJson.exports['.'].browser, './pkg/browser/matrix_rtc_wasm.js');
  assert.equal(packageJson.exports['.'].node, './pkg/node/matrix_rtc_wasm.js');
  assert.equal(packageJson.main, './pkg/node/matrix_rtc_wasm.js');
  assert.equal(packageJson.module, './pkg/browser/matrix_rtc_wasm.js');
});

test('build script is wired to wasm-pack', () => {
  assert.match(packageJson.scripts.build, /build-bindings\.sh$/);
  assert.match(packageJson.scripts['build:browser'], /wasm-pack build/);
  assert.match(packageJson.scripts['build:node'], /wasm-pack build/);
});

