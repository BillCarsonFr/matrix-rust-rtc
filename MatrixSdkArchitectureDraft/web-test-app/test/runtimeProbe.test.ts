// Runtime-assumption probes for the wasm build (feature `runtime-probe`).
// These pin down what the encryption module (and every other pump in the
// crate) relies on when there is no tokio runtime:
//   1. a Rust `async fn` awaiting `tokio::sync::watch::Receiver::changed()`
//      resolves when a *synchronous* FFI call sends on the channel;
//   2. a detached task (`executor::spawn` -> `spawn_local`) keeps running
//      after the FFI call that started it returned, and can call back into JS;
//   3. timers (`executor::sleep_ms` -> `setTimeout`) fire, inside an exported
//      future and inside a detached task.
import { beforeAll, describe, expect, it } from "vitest";
import { RuntimeProbe, type ProbeListener } from "../src/generated/matrix_rtc";
import { initWasm } from "./wasmInit";

beforeAll(async () => {
  await initWasm();
});

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

class Recorder implements ProbeListener {
  values: number[] = [];
  closed = false;
  onValue(value: number): void {
    this.values.push(value);
  }
  onClosed(): void {
    this.closed = true;
  }
}

describe("runtime probe (wasm)", () => {
  it("clock works (SystemTime via web-time)", () => {
    const probe = new RuntimeProbe();
    const now = Number(probe.nowMs());
    expect(Math.abs(now - Date.now())).toBeLessThan(1000);
  });

  it("watch::changed() in an exported async fn resolves on a sync send", async () => {
    const probe = new RuntimeProbe();
    let resolved: number | undefined;
    const pending = probe.nextChange().then((v) => (resolved = v));
    await tick();
    expect(resolved).toBeUndefined();

    probe.set(7); // synchronous FFI call -> watch::Sender::send_replace
    expect(await pending).toBe(7);
    expect(probe.current()).toBe(7);
  });

  it("a detached task survives the call that spawned it and calls back into JS", async () => {
    const probe = new RuntimeProbe();
    const rec = new Recorder();
    probe.spawnForwarder(rec);
    await tick();
    expect(rec.values).toEqual([]);

    probe.set(1);
    probe.set(2);
    await tick();
    // watch coalesces: the task sees the latest value, possibly skipping 1.
    expect(rec.values.at(-1)).toBe(2);
    expect(rec.values.length).toBeGreaterThanOrEqual(1);

    probe.set(3);
    await tick();
    expect(rec.values.at(-1)).toBe(3);
  });

  it("a timer fires inside an exported future", async () => {
    const probe = new RuntimeProbe();
    const start = Date.now();
    const elapsed = Number(await probe.sleep(60n));
    expect(Date.now() - start).toBeGreaterThanOrEqual(55);
    expect(elapsed).toBeGreaterThanOrEqual(55);
  });

  it("a timer fires inside a detached task and wakes a waiting future", async () => {
    const probe = new RuntimeProbe();
    const start = Date.now();
    probe.setAfter(50n, 42);
    const got = await probe.nextChange();
    expect(got).toBe(42);
    expect(Date.now() - start).toBeGreaterThanOrEqual(45);
  });
});
