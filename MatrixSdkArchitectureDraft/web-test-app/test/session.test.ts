// Session behaviour through the wasm bindings (session plan §4.9): slot
// conditions, sticky expiry on the JS clock, event-type spellings, the static
// `computeSessionsFromEvents` path, listeners, and the drop guard.
import { beforeAll, describe, expect, it } from "vitest";
import {
  FfiElementCallCompat,
  FfiEventOrigin,
  computeSessionsFromEvents,
  type FfiMembership,
} from "../src/generated/matrix_rtc";
import {
  memberJoinEvent,
  slotClosedEvent,
  slotOpenEvent,
  tick,
  waitFor,
} from "../src/mockDriver";
import { newManager } from "./helpers";
import { initWasm } from "./wasmInit";

beforeAll(async () => {
  await initWasm();
});

const encrypted = (deviceId: string) => new FfiEventOrigin.Encrypted({ senderDeviceId: deviceId });
const remote = { userId: "@remote:example.org", memberId: "m-1" };

describe("session", () => {
  it("a slot close state update empties the memberships; reopening restores", async () => {
    const { driver, manager } = newManager();
    driver.emitRoomEvent(memberJoinEvent(remote), encrypted("RDEV"));
    expect(manager.memberships()).toHaveLength(1);
    // the seed (readState) runs in the pump: the slot state lands a tick later
    await waitFor("seeded", () => manager.session().slotOpen === true);
    driver.emitStateUpdate([slotClosedEvent()]);
    expect(manager.memberships()).toHaveLength(0);
    expect(manager.session().slotOpen).toBe(false);
    expect(manager.session().memberCount).toBe(0);
    driver.emitStateUpdate([slotOpenEvent()]);
    expect(manager.memberships()).toHaveLength(1);
  });

  it("an m.rtc.member with a 200 ms duration disappears after ~300 ms", async () => {
    const { driver, manager } = newManager();
    driver.emitRoomEvent(memberJoinEvent({ ...remote, durationMs: 200 }), encrypted("RDEV"));
    expect(manager.memberships()).toHaveLength(1);
    await waitFor("expiry", () => manager.memberships().length === 0, 1500);
  });

  it("the unstable type org.matrix.msc4143.rtc.member is accepted", () => {
    const { driver, manager } = newManager();
    driver.emitRoomEvent(memberJoinEvent({ ...remote, unstableType: true }), encrypted("RDEV"));
    expect(manager.memberships().map((m) => m.member.memberId)).toEqual(["m-1"]);
  });

  it("computeSessionsFromEvents returns one snapshot for the same fixtures", () => {
    const snapshots = computeSessionsFromEvents([memberJoinEvent(remote), slotOpenEvent()], FfiElementCallCompat.Off);
    expect(snapshots).toHaveLength(1);
    expect(snapshots[0].memberCount).toBe(1);
    expect(snapshots[0].isActive).toBe(true);
    expect(snapshots[0].slotOpen).toBe(true);
    expect(snapshots[0].applicationType).toBe("m.call");
    expect(snapshots[0].members[0].memberId).toBe("m-1");
  });

  it("a listener fires after a tick with a list equal to the getter", async () => {
    const { driver, manager } = newManager();
    const seen: FfiMembership[][] = [];
    manager.setMembershipsListener({ onMembershipsChange: (m) => seen.push(m) });
    driver.emitRoomEvent(memberJoinEvent(remote), encrypted("RDEV"));
    await tick();
    expect(seen.at(-1)).toEqual(manager.memberships());
  });

  it("the emit after the manager is destroyed returns false", () => {
    const { driver, manager } = newManager();
    expect(driver.emitRoomEvent(memberJoinEvent(remote), encrypted("RDEV"))).toBe(true);
    manager.uniffiDestroy();
    expect(driver.emitRoomEvent(memberJoinEvent(remote), encrypted("RDEV"))).toBe(false);
  });
});
