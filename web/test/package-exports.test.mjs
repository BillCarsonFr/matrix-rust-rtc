import { describe, it, expect } from 'vitest';
import { readFile } from 'node:fs/promises';

const packageJsonPath = new URL('../package.json', import.meta.url);
const packageJson = JSON.parse(await readFile(packageJsonPath, 'utf8'));

describe('package exports', () => {
  it('are browser-first', () => {
    expect(packageJson.name).toBe('matrix-rtc-wasm');
    expect(packageJson.private).toBe(true);
    expect(packageJson.exports['.'].browser).toBe('./pkg/browser/matrix_rtc_wasm.js');
    expect(packageJson.exports['.'].node).toBe('./pkg/node/matrix_rtc_wasm.js');
    expect(packageJson.main).toBe('./pkg/node/matrix_rtc_wasm.js');
    expect(packageJson.module).toBe('./pkg/browser/matrix_rtc_wasm.js');
  });

  it('ships the three layers of the deliverable', () => {
    // The protocol (.), the livekit-client half, the matrix-js-sdk half —
    // each integration layer with its dependency as an optional peer.
    expect(packageJson.exports['./call']).toBe('./src/matrix-rtc-call.mjs');
    expect(packageJson.exports['./matrix-js-sdk-host']).toBe('./src/matrix-js-sdk-host.mjs');
    expect(packageJson.peerDependenciesMeta['livekit-client'].optional).toBe(true);
    expect(packageJson.peerDependenciesMeta['matrix-js-sdk'].optional).toBe(true);
  });

  it('build script is wired to wasm-pack', () => {
    expect(packageJson.scripts.build).toMatch(/build-bindings\.sh$/);
    expect(packageJson.scripts['build:browser']).toMatch(/wasm-pack build/);
    expect(packageJson.scripts['build:node']).toMatch(/wasm-pack build/);
  });
});
