// Acceptance tests for the ParticipationManager, driven entirely through the
// TS MatrixDriver mock: inbound events are injected, outbound traffic and
// the manager's outputs are asserted.
//
// NOTE: the Rust crate is a skeleton — every method is todo!(), so today
// each test fails with a wasm panic ("unreachable"). They define the
// expected behavior and become the harness for implementing the todo!()s:
// make them pass one by one.
import { beforeAll, describe, expect, it } from "vitest";
import {
  FfiElementCallCompat,
  FfiEventOrigin,
  FfiMatrixDriver,
  FfiMembershipState,
  FfiParticipationManager,
  FfiStatus,
  FfiTransportIntent,
  type FfiMembership,
} from "../src/generated/matrix_rtc";
import {
  LK_SERVICE_URL,
  MockMatrixDriver,
  ROOM_ID,
  SLOT_ID,
  encryptionKeyContent,
  memberJoinEvent,
  memberLeaveEvent,
  slotOpenEvent,
} from "../src/mockDriver";
import { initWasm } from "./wasmInit";

beforeAll(async () => {
  await initWasm();
});

const encrypted = (deviceId: string) =>
  new FfiEventOrigin.Encrypted({ senderDeviceId: deviceId });

function newManager() {
  const driver = new MockMatrixDriver();
  // the subscribe_* handshake happens here, exactly once per driver
  const matrixDriver = new FfiMatrixDriver(driver);
  const manager = new FfiParticipationManager(
    ROOM_ID,
    SLOT_ID,
    matrixDriver,
    FfiElementCallCompat.Off,
  );
  return { driver, matrixDriver, manager };
}

const joinParams = {
  applicationType: "m.call",
  stickyDurationMs: 240_000n,
  keepAliveTimeoutMs: 15_000n,
  delegateDelayedLeave: false,
};

const receiveOnly = () => new FfiTransportIntent.ReceiveOnly({ canSubscribe: ["livekit"] });

describe("ParticipationManager", () => {
  it("starts disconnected with no memberships", () => {
    const { manager } = newManager();
    expect(manager.status()).toBe(FfiStatus.Disconnected);
    expect(manager.memberships()).toEqual([]);
    expect(manager.connections()).toEqual([]);
    expect(manager.keyMap()).toEqual([]);
  });

  it("a remote member join shows up as a Joined membership", async () => {
    const { driver, manager } = newManager();
    const changes: FfiMembership[][] = [];
    manager.setMembershipsListener({
      onMembershipsChange: (m) => changes.push(m),
    });

    driver.emitRoomEvent(slotOpenEvent(), new FfiEventOrigin.Cleartext());
    driver.emitRoomEvent(
      memberJoinEvent({ userId: "@remote:example.org", deviceId: "RDEV", memberId: "m-1" }),
      encrypted("RDEV"),
    );

    const memberships = manager.memberships();
    expect(memberships).toHaveLength(1);
    expect(memberships[0].member.memberId).toBe("m-1");
    expect(memberships[0].member.userId).toBe("@remote:example.org");
    expect(memberships[0].state).toBe(FfiMembershipState.Joined);
    // the tile knows which LK room carries this member's media
    expect(memberships[0].connections).toContain(LK_SERVICE_URL);
    // and the listener fired with the same list
    expect(changes.at(-1)).toEqual(memberships);
  });

  it("join arms the delayed leave before sending the membership", async () => {
    const { driver, manager } = newManager();
    driver.emitRoomEvent(slotOpenEvent(), new FfiEventOrigin.Cleartext());
    await manager.join(receiveOnly(), joinParams);

    // outbound order: dead man's switch first, then the sticky join
    const kinds = driver.outbound.map((c) => c.kind);
    const delayedAt = kinds.indexOf("delayedEvent");
    const stickyAt = kinds.indexOf("stickyEvent");
    expect(delayedAt).toBeGreaterThanOrEqual(0);
    expect(stickyAt).toBeGreaterThan(delayedAt);

    const sticky = driver.calls("stickyEvent")[0] as Extract<
      import("../src/mockDriver").OutboundCall,
      { kind: "stickyEvent" }
    >;
    expect(sticky.roomId).toBe(ROOM_ID);
    // wire type: unstable spelling goes out, per wire_event_type
    expect(["m.rtc.member", "org.matrix.msc4143.rtc.member"]).toContain(sticky.eventType);
    // sticky duration passes through verbatim
    expect(sticky.durationMs).toBe(240_000n);

    expect(manager.status()).not.toBe(FfiStatus.Disconnected);
    // our own membership is in the list
    expect(manager.memberships().some((m) => m.state === FfiMembershipState.Joined)).toBe(true);
  });

  it("a member that left while holding our key stays as LeftWithKeys", async () => {
    const { driver, manager } = newManager();
    const remote = { userId: "@remote:example.org", deviceId: "RDEV", memberId: "m-1" };

    driver.emitRoomEvent(slotOpenEvent(), new FfiEventOrigin.Cleartext());
    await manager.join(receiveOnly(), joinParams);
    driver.emitRoomEvent(memberJoinEvent(remote), encrypted("RDEV"));

    // the remote sent us their key…
    driver.emitToDevice(
      "m.rtc.encryption_key",
      remote.userId,
      encryptionKeyContent({ memberId: remote.memberId, index: 0 }),
      encrypted("RDEV"),
    );
    expect(manager.keyMap().map((k) => k.memberId)).toContain(remote.memberId);

    // …and we distributed ours to them, so when they leave they still hold it
    driver.emitRoomEvent(memberLeaveEvent(remote), encrypted("RDEV"));

    const gone = manager.memberships().find((m) => m.member.memberId === remote.memberId);
    expect(gone).toBeDefined();
    expect(gone!.state).toBe(FfiMembershipState.LeftWithKeys);
    // once our key rotates away from them, the membership disappears —
    // covered by a follow-up test when rotation timing is implementable.
  });
});
