import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { test } from 'node:test';

const browserBindingUrl = new URL('../pkg/browser/matrix_rtc_wasm.js', import.meta.url);

test('generated browser binding surface is available after build', async (t) => {
  if (!existsSync(browserBindingUrl)) {
    t.skip('bindings have not been generated yet');
    return;
  }

  const mod = await import(browserBindingUrl.href);

  assert.ok(mod);
  assert.equal(typeof mod.WasmRtcSession, 'function');
  assert.equal(typeof mod.WasmRtcSessionManager, 'function');
  assert.equal(typeof mod.WasmMembershipSnapshotSubscription, 'function');
});

