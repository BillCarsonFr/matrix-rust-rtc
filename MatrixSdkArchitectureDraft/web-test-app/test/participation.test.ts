// Acceptance tests for the ParticipationManager, driven entirely through the
// TS MatrixDriver mock: inbound events are injected, outbound traffic and
// the manager's outputs are asserted. Getters are fresh right after an emit
// (drain-on-read); listener callbacks arrive a tick later.
import { beforeAll, describe, expect, it } from "vitest";
import {
  FfiEventOrigin,
  FfiDisconnectCause,
  FfiKeepAlive,
  FfiMembershipState,
  FfiStatus,
  type FfiMembership,
} from "../src/generated/matrix_rtc";
import { LK_SERVICE_URL, OWN_USER_ID, ROOM_ID, memberJoinEvent, tick, waitFor } from "../src/mockDriver";
import { encryptedRoomState, joinParams, newManager, publishLk, receiveOnly } from "./helpers";
import { initWasm } from "./wasmInit";

beforeAll(async () => {
  await initWasm();
});

const encrypted = (deviceId: string) =>
  new FfiEventOrigin.Encrypted({ senderDeviceId: deviceId });

describe("ParticipationManager", () => {
  it("starts disconnected with no memberships", () => {
    const { manager } = newManager();
    expect(FfiStatus.Disconnected.instanceOf(manager.status())).toBe(true);
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

    driver.emitRoomEvent(
      memberJoinEvent({ userId: "@remote:example.org", memberId: "m-1" }),
      encrypted("RDEV"),
    );

    const memberships = manager.memberships();
    expect(memberships).toHaveLength(1);
    expect(memberships[0].member.memberId).toBe("m-1");
    expect(memberships[0].member.userId).toBe("@remote:example.org");
    expect(memberships[0].member.deviceId).toBe("RDEV");
    expect(memberships[0].state).toBe(FfiMembershipState.Joined);
    // the tile knows which LK room carries this member's media
    expect(memberships[0].connections).toContain(LK_SERVICE_URL);
    expect(memberships[0].transportIdentity).toBeTypeOf("string");
    expect(memberships[0].member.publishedTransports[0].transportType).toBe("livekit");
    // and the listener fires a tick later with the same list
    await tick();
    expect(changes.at(-1)).toEqual(memberships);
  });

  it("join arms the delayed leave before sending the membership", async () => {
    const { driver, manager } = newManager();
    await manager.join(receiveOnly(), joinParams);

    // outbound order: dead man's switch first, then the sticky join
    const kinds = driver.outbound.map((c) => c.kind);
    const delayedAt = kinds.indexOf("delayedEvent");
    const stickyAt = kinds.indexOf("stickyEvent");
    expect(delayedAt).toBeGreaterThanOrEqual(0);
    expect(stickyAt).toBeGreaterThan(delayedAt);

    const delayed = driver.calls("delayedEvent")[0];
    expect(delayed.delayMs).toBe(15_000n);
    expect(delayed.stickyDurationMs).toBe(240_000n);
    expect(delayed.content.leave_reason.code).toBe("delayed_leave");

    const sticky = driver.calls("stickyEvent")[0];
    expect(sticky.roomId).toBe(ROOM_ID);
    // wire type: unstable spelling goes out, per wire_event_type
    expect(sticky.eventType).toBe("org.matrix.msc4143.rtc.member");
    // sticky duration passes through verbatim
    expect(sticky.durationMs).toBe(240_000n);
    expect(sticky.content.member.membership).toBe("join");

    expect(FfiStatus.Connected.instanceOf(manager.status())).toBe(true);
    // our own membership is in the list (the mock homeserver echoed it)
    const me = manager.memberships().find((m) => m.member.userId === OWN_USER_ID);
    expect(me?.state).toBe(FfiMembershipState.Joined);
    expect(me?.member.memberId).toBe(sticky.content.member.id);
  });

  it("a member that left while holding our key stays as LeftWithKeys until rotation", async () => {
    const { driver, manager } = newManager({ roomState: encryptedRoomState() });
    const remote = driver.addPeer({ userId: "@remote:example.org", deviceId: "RDEV", memberId: "m-1" });

    await manager.join(receiveOnly(), joinParams);
    driver.peerJoins(remote);

    // we distribute our key to them and they answer with theirs
    await waitFor("key exchange", () => manager.keyMap().some((k) => k.memberId === remote.memberId));
    expect(driver.calls("toDevice")[0].recipients).toEqual([{ userId: remote.userId, deviceId: remote.deviceId }]);

    // …so when they leave they still hold it
    driver.peerLeaves(remote);
    const gone = manager.memberships().find((m) => m.member.memberId === remote.memberId);
    expect(gone).toBeDefined();
    expect(gone!.state).toBe(FfiMembershipState.LeftWithKeys);
    expect(gone!.connections).toEqual([]);

    // once our key rotates away from them, the membership disappears and
    // our key ring carries the new index (default 1 s use delay)
    await waitFor(
      "rotation settles",
      () => !manager.memberships().some((m) => m.member.memberId === remote.memberId),
      5000,
    );
    const ownKeys = manager.keyMap().filter((k) => k.memberId !== remote.memberId);
    expect(ownKeys.map((k) => k.index)).toEqual([0, 1]);
  });

  it("the status listener follows join and leave", async () => {
    const { manager } = newManager();
    const statuses: FfiStatus[] = [];
    manager.setStatusListener({ onStatusChange: (s) => statuses.push(s) });
    await manager.join(receiveOnly(), joinParams);
    await waitFor("connected seen", () => FfiStatus.Connected.instanceOf(statuses.at(-1)));
    await manager.leave("m.user_hangup", undefined);
    await waitFor("disconnected seen", () => FfiStatus.Disconnected.instanceOf(statuses.at(-1)));
    expect(FfiStatus.Disconnected.instanceOf(manager.status())).toBe(true);
  });

  // The status used to be four opaque variants with everything else behind
  // debugSnapshot's unversioned JSON. Everything a UI needs is now typed.
  it("the status carries the typed keep-alive, publication, roster and impairments", async () => {
    const { manager } = newManager();
    const before = manager.status();
    if (!FfiStatus.Disconnected.instanceOf(before)) throw new Error("expected Disconnected");
    expect(FfiDisconnectCause.NeverJoined.instanceOf(before.inner.cause)).toBe(true);
    expect(manager.ownMemberId()).toBeUndefined();

    await manager.join(publishLk(), joinParams);
    const status = manager.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    const armed = status.inner.keepAlive;
    if (!FfiKeepAlive.Armed.instanceOf(armed)) throw new Error("expected Armed");
    // The deadline a UI renders a countdown from, without deriving it.
    expect(armed.inner.firesAtTs).toBe(armed.inner.lastRestartTs + armed.inner.delayMs);
    expect(status.inner.membership.expiresAtTs).toBe(
      status.inner.membership.lastPublishedTs + status.inner.membership.lifetimeMs,
    );
    expect(status.inner.membership.refreshFailingSinceTs).toBeUndefined();
    expect(status.inner.impairments).toEqual([]);
    expect(manager.connectionProblems()).toEqual([]);

    // Identity: the member id the facade mints, and our own tile.
    const ownId = manager.ownMemberId();
    expect(ownId).toBeTruthy();
    await waitFor("our echo", () => manager.ownMembership() !== undefined);
    expect(manager.ownMembership()!.member.memberId).toBe(ownId);

    // Seed honesty reaches the FFI too.
    expect(manager.session().seeded).toBe(true);
    expect(manager.session().failedReads).toEqual([]);
    expect(manager.session().excludedCandidates).toEqual([]);
  });

  it("debugSnapshot is JSON with every part", async () => {
    const { manager } = newManager();
    await manager.join(publishLk(), joinParams);
    const snapshot = JSON.parse(manager.debugSnapshot());
    expect(snapshot.room_id).toBe(ROOM_ID);
    expect(snapshot.own_membership.state).toBe("Connected");
    expect(snapshot.encryption).toBeTruthy();
    expect(snapshot.connections).toHaveLength(1);
  });
});
