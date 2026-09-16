// Own-membership behaviour through the wasm bindings (own-membership plan
// §9.4): compat dialects, timers on the JS clock, leave, delegation, the
// automatic slot_closed leave, and the own transport in connections().
import { beforeAll, describe, expect, it } from "vitest";
import { FfiElementCallCompat, FfiStatus } from "../src/generated/matrix_rtc";
import { LK_SERVICE_URL, OWN_DEVICE_ID, OWN_USER_ID, slotClosedEvent, waitFor } from "../src/mockDriver";
import { joinParams, newManager, publishLk, receiveOnly } from "./helpers";
import { initWasm } from "./wasmInit";

beforeAll(async () => {
  await initWasm();
});

describe("own membership", () => {
  it("join in StateEvents compat sends a state event with an underscore state key and a user:device member id", async () => {
    // The legacy generation has no slot: the manager is created for "".
    const { driver, manager } = newManager({ compat: FfiElementCallCompat.StateEvents, slotId: "", roomState: [] });
    await manager.join(publishLk(), joinParams);
    const delayed = driver.calls("delayedStateEvent")[0];
    expect(delayed.stateKey).toBe(`_${OWN_USER_ID}_${OWN_DEVICE_ID}_m.call`);
    expect(delayed.content).toEqual({});
    const state = driver.calls("stateEvent")[0];
    expect(state.eventType).toBe("org.matrix.msc3401.call.member");
    expect(state.stateKey).toBe(`_${OWN_USER_ID}_${OWN_DEVICE_ID}_m.call`);
    expect(state.content.membershipID).toBe(`${OWN_USER_ID}:${OWN_DEVICE_ID}`);
    expect(state.content.foci_preferred[0].livekit_alias).toBe(driver.calls("getLivekitToken")[0].roomId);
    expect(driver.calls("getLivekitToken")[0].legacySfuGet).toBe(true);
    // the echoed state event lists us with the legacy identity
    const me = manager.memberships().find((m) => m.member.userId === OWN_USER_ID);
    expect(me?.member.memberId).toBe(`${OWN_USER_ID}:${OWN_DEVICE_ID}`);
    expect(me?.transportIdentity).toBe(`${OWN_USER_ID}:${OWN_DEVICE_ID}`);
  });

  it("with keepAliveTimeoutMs 60 the mock sees restartDelayed within 200 ms", async () => {
    const { driver, manager } = newManager();
    await manager.join(receiveOnly(), { ...joinParams, keepAliveTimeoutMs: 60n });
    await waitFor("restart", () => driver.calls("restartDelayed").length >= 1, 500);
    expect(driver.calls("restartDelayed")[0].delayId).toBe(driver.calls("delayedEvent")[0].delayId);
    await manager.leave(undefined, undefined);
  });

  it("leave sends membership: leave then cancelDelayed and the status returns to Disconnected", async () => {
    const { driver, manager } = newManager();
    await manager.join(receiveOnly(), joinParams);
    await manager.leave("m.user_hangup", "done");
    const kinds = driver.outbound.map((c) => c.kind);
    const leaveAt = driver.outbound.findIndex((c) => c.kind === "stickyEvent" && c.content.member?.membership === "leave");
    expect(leaveAt).toBeGreaterThan(0);
    expect(kinds[leaveAt + 1]).toBe("cancelDelayed");
    const leave = driver.outbound[leaveAt] as Extract<(typeof driver.outbound)[number], { kind: "stickyEvent" }>;
    expect(leave.content.leave_reason).toEqual({ code: "m.user_hangup", reason: "done" });
    expect(FfiStatus.Disconnected.instanceOf(manager.status())).toBe(true);
    // our echoed leave took us out of the roster
    expect(manager.memberships().some((m) => m.member.userId === OWN_USER_ID)).toBe(false);
  });

  it("updateApplication re-publishes the membership with the new intent", async () => {
    const { driver, manager } = newManager();
    await expect(manager.updateApplication("video")).rejects.toThrow();
    await manager.join(receiveOnly(), { ...joinParams, intent: "audio" });
    expect(driver.calls("stickyEvent")[0].content.application["m.call.intent"]).toBe("audio");
    await manager.updateApplication("video");
    await waitFor("re-publish", () => driver.calls("stickyEvent").length >= 2);
    const republished = driver.calls("stickyEvent")[1].content;
    expect(republished.application["m.call.intent"]).toBe("video");
    expect(republished.member.id).toBe(manager.ownMemberId());
    // the echo puts the new intent on our own tile
    expect(manager.ownMembership()?.member.intent).toBe("video");
    await manager.leave(undefined, undefined);
  });

  it("delegation arms a long leave after the join, tries the homeserver, then swaps out the short leave", async () => {
    const { driver, manager } = newManager();
    await manager.join(publishLk(), { ...joinParams, delegateDelayedLeave: true });
    const kinds = driver.outbound.map((c) => c.kind).filter((k) => k !== "getLivekitToken");
    // short leave · join · long leave · homeserver delegation · cancel of the short one
    expect(kinds).toEqual(["delayedEvent", "stickyEvent", "delayedEvent", "delegateViaHomeserver", "cancelDelayed"]);
    const [short, long] = driver.calls("delayedEvent");
    expect(short.delayMs).toBe(15_000n);
    expect(long.delayMs).toBe(3_600_000n);
    expect(driver.calls("delegateViaHomeserver")[0].delayId).toBe(long.delayId);
    expect(driver.calls("cancelDelayed")[0].delayId).toBe(short.delayId);
    const status = manager.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(status.inner.keepAlive.tag).toBe("Delegated");
    await manager.leave(undefined, undefined);
  });

  it("a receive-only member has no transport to delegate to and keeps its own leave", async () => {
    const { driver, manager } = newManager();
    await manager.join(receiveOnly(), { ...joinParams, delegateDelayedLeave: true });
    expect(driver.outbound.map((c) => c.kind)).toEqual(["delayedEvent", "stickyEvent"]);
    const status = manager.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(status.inner.keepAlive.tag).toBe("Armed");
    await manager.leave(undefined, undefined);
  });

  it("delegation falls back to the authorisation service of the transport we publish on", async () => {
    const { driver, manager } = newManager();
    driver.refuseHomeserverDelegation = true;
    await manager.join(publishLk(), { ...joinParams, delegateDelayedLeave: true });
    const viaTransport = driver.calls("delegateViaTransport")[0];
    expect(viaTransport.livekitServiceUrl).toBe(LK_SERVICE_URL);
    expect(viaTransport.delayTimeoutMs).toBe(3_600_000n);
    expect(viaTransport.legacySfuGet).toBe(false);
    expect(viaTransport.delayId).toBe(driver.calls("delayedEvent")[1].delayId);
    // the short leave is the one cancelled
    expect(driver.calls("cancelDelayed")[0].delayId).toBe(driver.calls("delayedEvent")[0].delayId);
    const status = manager.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(status.inner.keepAlive.tag).toBe("Delegated");
    await manager.leave(undefined, undefined);
  });

  it("when no route takes the delegation the short leave stays and the long one is cancelled", async () => {
    const { driver, manager } = newManager();
    driver.refuseHomeserverDelegation = true;
    driver.refuseTransportDelegation = true;
    await manager.join(publishLk(), { ...joinParams, delegateDelayedLeave: true, keepAliveTimeoutMs: 60n });
    expect(driver.calls("cancelDelayed")[0].delayId).toBe(driver.calls("delayedEvent")[1].delayId);
    const status = manager.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(status.inner.keepAlive.tag).toBe("Armed");
    // ...and we keep restarting the short one ourselves
    await waitFor("restart", () => driver.calls("restartDelayed").some((c) => c.delayId === driver.calls("delayedEvent")[0].delayId), 500);
    await manager.leave(undefined, undefined);
  });

  it("a slot close state update makes the manager leave with code slot_closed", async () => {
    const { driver, manager } = newManager();
    await manager.join(receiveOnly(), joinParams);
    driver.emitStateUpdate([slotClosedEvent()]);
    await waitFor("left", () => FfiStatus.Disconnected.instanceOf(manager.status()));
    const leave = driver.calls("stickyEvent").find((c) => c.content.member?.membership === "leave");
    expect(leave?.content.leave_reason.code).toBe("slot_closed");
    expect(driver.calls("cancelDelayed")).toHaveLength(1);
  });

  it("a homeserver without delayed events degrades the membership lifetime", async () => {
    const { driver, manager } = newManager();
    driver.refuseDelayedEvents = true;
    await manager.join(receiveOnly(), joinParams);
    expect(driver.calls("stickyEvent")[0].durationMs).toBe(300_000n);
    expect(FfiStatus.Connected.instanceOf(manager.status())).toBe(true);
  });

  it("the publishing transport appears in connections() right after join resolves", async () => {
    const { driver, manager } = newManager();
    await manager.join(publishLk(), joinParams);
    const connections = manager.connections();
    expect(connections).toHaveLength(1);
    expect(connections[0].connection.serviceUrl).toBe(LK_SERVICE_URL);
    expect(connections[0].connection.wsUrl).toBe("wss://lk.example.org");
    expect(connections[0].connection.jwtToken).toBe(`jwt-for-${LK_SERVICE_URL}`);
    const token = driver.calls("getLivekitToken")[0];
    expect(token.member.claimed_user_id).toBe(OWN_USER_ID);
    expect(token.member.claimed_device_id).toBe(OWN_DEVICE_ID);
    // the token exists before anything is published
    const kinds = driver.outbound.map((c) => c.kind);
    expect(kinds.indexOf("getLivekitToken")).toBeLessThan(kinds.indexOf("delayedEvent"));
    await manager.leave(undefined, undefined);
    expect(manager.connections()).toEqual([]);
  });
});
