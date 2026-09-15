// Session behaviour through the wasm bindings (session plan §4.9): slot
// conditions, sticky expiry on the JS clock, event-type spellings, the static
// `computeSessionsFromEvents` path, listeners, and the drop guard.
import { beforeAll, describe, expect, it } from "vitest";
import {
  FfiElementCallCompat,
  FfiEventOrigin,
  FfiStatus,
  FfiTransportIntent,
  computeSessionsFromEvents,
  type FfiMembership,
} from "../src/generated/matrix_rtc";
import {
  memberJoinEvent,
  roomMemberEvent,
  slotClosedEvent,
  slotOpenEvent,
  tick,
  waitFor,
} from "../src/mockDriver";
import { joinParams, newManager } from "./helpers";
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

  it("a room without a slot has no call until a client opens one", async () => {
    // No slot = no call: members are excluded and join is refused. The
    // client that starts the call opens the slot (a state event the
    // homeserver echoes), after which both work.
    {
      const { driver, manager } = newManager({ roomState: [] });
      await waitFor("seeded", () => manager.session().seeded);
      expect(manager.session().slotOpen).toBe(false);
      driver.emitRoomEvent(memberJoinEvent(remote), encrypted("RDEV"));
      expect(manager.memberships()).toHaveLength(0);
      expect(manager.session().excludedCandidates[0]?.member.eventId).toMatch(/^\$ev-/);
      await expect(
        manager.join(new FfiTransportIntent.ReceiveOnly({ canSubscribe: ["livekit"] }), joinParams),
      ).rejects.toThrow();

      await manager.openSlot("m.call", false);
      const slot = driver.calls("stateEvent")[0];
      expect(slot.eventType).toBe("org.matrix.msc4143.rtc.slot");
      expect(slot.stateKey).toBe("m.call#ROOM");
      expect(slot.content).toEqual({ status: "open", application: { type: "m.call" } });
      expect(manager.session().slotOpen).toBe(true);
      expect(manager.memberships()).toHaveLength(1);
      await manager.join(new FfiTransportIntent.ReceiveOnly({ canSubscribe: ["livekit"] }), joinParams);
      expect(FfiStatus.Connected.instanceOf(manager.status())).toBe(true);
      await manager.leave(undefined, undefined);
    }
  });

  it("members carry the display name and avatar from m.room.member, kept current", async () => {
    const { driver, manager } = newManager({
      roomState: [
        JSON.parse(slotOpenEvent()),
        JSON.parse(roomMemberEvent({ userId: remote.userId, displayName: "Alice", avatarUrl: "mxc://example.org/a" })),
      ],
    });
    await waitFor("seeded", () => manager.session().seeded);
    driver.emitRoomEvent(memberJoinEvent(remote), encrypted("RDEV"));
    const [member] = manager.memberships();
    expect(member.member.displayName).toBe("Alice");
    expect(member.member.avatarUrl).toBe("mxc://example.org/a");
    // a rename lands as a state update and the getter is fresh
    driver.emitStateUpdate([roomMemberEvent({ userId: remote.userId, displayName: "Alicia" })]);
    expect(manager.memberships()[0].member.displayName).toBe("Alicia");
    expect(manager.memberships()[0].member.avatarUrl).toBeUndefined();
    // ...and the listener sees it a tick later
    const changes: FfiMembership[][] = [];
    manager.setMembershipsListener({ onMembershipsChange: (m) => changes.push(m) });
    driver.emitStateUpdate([roomMemberEvent({ userId: remote.userId, displayName: "Alice again" })]);
    await tick();
    expect(changes.at(-1)?.[0].member.displayName).toBe("Alice again");
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
