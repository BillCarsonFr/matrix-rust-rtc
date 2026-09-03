// Media key exchange through the wasm bindings (encryption README test
// list): distribution to members, verification of inbound keys, buffering of
// early keys, and the unencrypted case.
import { beforeAll, describe, expect, it } from "vitest";
import { FfiEventOrigin, type FfiMediaKey } from "../src/generated/matrix_rtc";
import { encryptionKeyContent, waitFor } from "../src/mockDriver";
import { encryptedRoomState, joinParams, newManager, receiveOnly } from "./helpers";
import { initWasm } from "./wasmInit";

beforeAll(async () => {
  await initWasm();
});

const peerA = { userId: "@a:example.org", deviceId: "ADEV", memberId: "m-a", key: new Uint8Array(32).fill(1) };
const peerB = { userId: "@b:example.org", deviceId: "BDEV", memberId: "m-b", key: new Uint8Array(32).fill(2) };

describe("encryption", () => {
  it("joining an encrypted call sends our key to every member's device", async () => {
    const { driver, manager } = newManager({ roomState: encryptedRoomState() });
    driver.peerJoins(driver.addPeer(peerA));
    driver.peerJoins(driver.addPeer(peerB));
    await manager.join(receiveOnly(), joinParams);
    await waitFor("one batch", () => driver.calls("toDevice").length >= 1);
    const batch = driver.calls("toDevice")[0];
    expect(batch.eventType).toBe("org.matrix.msc4143.rtc.encryption_key");
    expect(batch.recipients).toEqual(
      expect.arrayContaining([
        { userId: peerA.userId, deviceId: peerA.deviceId },
        { userId: peerB.userId, deviceId: peerB.deviceId },
      ]),
    );
    expect(batch.content.media_key.index).toBe(0);
    expect(batch.content.room_id).toBe(driver.calls("stickyEvent")[0].roomId);
    // the peers answered: three key rings
    await waitFor("all keys", () => new Set(manager.keyMap().map((k) => k.memberId)).size === 3);
  });

  it("a remote key that passes verification appears in the key map and the listener", async () => {
    const { driver, manager } = newManager({ roomState: encryptedRoomState() });
    const changes: FfiMediaKey[] = [];
    manager.setKeyMapListener({ onKeyMapChange: (_map, change) => changes.push(change) });
    await manager.join(receiveOnly(), joinParams);
    driver.peerJoins(peerA); // not a simulated peer: no automatic reply
    driver.peerSendsKey(peerA, 0);
    // Inbound keys are verified by the encryption pump (not drain-on-read
    // like the session): the map fills a tick later.
    await waitFor("key stored", () => manager.keyMap().some((k) => k.memberId === peerA.memberId));
    const key = manager.keyMap().find((k) => k.memberId === peerA.memberId)!;
    expect(Array.from(new Uint8Array(key.key))).toEqual(Array.from(peerA.key));
    expect(key.index).toBe(0);
    await waitFor("listener", () => changes.some((c) => c.memberId === peerA.memberId));
  });

  it("a cleartext remote key is dropped", async () => {
    const { driver, manager } = newManager({ roomState: encryptedRoomState() });
    await manager.join(receiveOnly(), joinParams);
    driver.peerJoins(peerA);
    driver.emitToDevice(
      "m.rtc.encryption_key",
      peerA.userId,
      encryptionKeyContent({ memberId: peerA.memberId, index: 0 }),
      new FfiEventOrigin.Cleartext(),
    );
    expect(manager.keyMap().some((k) => k.memberId === peerA.memberId)).toBe(false);
    // …and so is one from the wrong device
    driver.emitToDevice(
      "m.rtc.encryption_key",
      peerA.userId,
      encryptionKeyContent({ memberId: peerA.memberId, index: 0 }),
      new FfiEventOrigin.Encrypted({ senderDeviceId: "OTHERDEV" }),
    );
    expect(manager.keyMap().some((k) => k.memberId === peerA.memberId)).toBe(false);
  });

  it("a key for an unknown member is held until its membership arrives", async () => {
    const { driver, manager } = newManager({ roomState: encryptedRoomState() });
    await manager.join(receiveOnly(), joinParams);
    driver.peerSendsKey(peerA, 0);
    await new Promise((r) => setTimeout(r, 50));
    expect(manager.keyMap().some((k) => k.memberId === peerA.memberId)).toBe(false);
    driver.peerJoins(peerA);
    await waitFor("held key verified on join", () => manager.keyMap().some((k) => k.memberId === peerA.memberId));
  });

  it("unencrypted slot: no keys are sent and inbound keys are ignored", async () => {
    const { driver, manager } = newManager();
    driver.peerJoins(driver.addPeer(peerA));
    await manager.join(receiveOnly(), joinParams);
    await new Promise((r) => setTimeout(r, 100));
    expect(driver.calls("toDevice")).toEqual([]);
    driver.peerSendsKey(peerA, 0);
    expect(manager.keyMap()).toEqual([]);
  });

  it("leaving forgets every key", async () => {
    const { driver, manager } = newManager({ roomState: encryptedRoomState() });
    driver.peerJoins(driver.addPeer(peerA));
    await manager.join(receiveOnly(), joinParams);
    await waitFor("keys", () => manager.keyMap().length === 2);
    await manager.leave(undefined, undefined);
    expect(manager.keyMap()).toEqual([]);
  });
});
