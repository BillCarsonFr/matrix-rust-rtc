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
  it("join in StickyEvents compat adds rtc_transports, versions and member.user_id/device_id", async () => {
    const { driver, manager } = newManager({ compat: FfiElementCallCompat.StickyEvents });
    await manager.join(publishLk(), joinParams);
    const content = driver.calls("stickyEvent")[0].content;
    expect(content.rtc_transports).toEqual([{ type: "livekit", livekit_service_url: LK_SERVICE_URL }]);
    expect(content.versions).toEqual([]);
    expect(content.member.user_id).toBe(OWN_USER_ID);
    expect(content.member.device_id).toBe(OWN_DEVICE_ID);
    // the spec fields stay
    expect(content.member.membership).toBe("join");
    expect(content.transports.published).toHaveLength(1);
    await manager.leave(undefined, undefined);
    // a legacy leave is a bare sticky key
    expect(driver.calls("stickyEvent")[1].content).toEqual({ msc4354_sticky_key: content.member.id });
  });

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
    expect(manager.status()).toBe(FfiStatus.Disconnected);
    // our echoed leave took us out of the roster
    expect(manager.memberships().some((m) => m.member.userId === OWN_USER_ID)).toBe(false);
  });

  it("delegateDelayedLeave is called after the membership when requested, with a ≥ 1 h delay", async () => {
    const { driver, manager } = newManager();
    await manager.join(receiveOnly(), { ...joinParams, delegateDelayedLeave: true });
    const kinds = driver.outbound.map((c) => c.kind);
    expect(kinds.indexOf("delegateDelayedLeave")).toBeGreaterThan(kinds.indexOf("stickyEvent"));
    expect(driver.calls("delayedEvent")[0].delayMs).toBe(3_600_000n);
    expect(driver.calls("delegateDelayedLeave")[0].delayId).toBe(driver.calls("delayedEvent")[0].delayId);
  });

  it("a slot close state update makes the manager leave with code slot_closed", async () => {
    const { driver, manager } = newManager();
    await manager.join(receiveOnly(), joinParams);
    driver.emitStateUpdate([slotClosedEvent()]);
    await waitFor("left", () => manager.status() === FfiStatus.Disconnected);
    const leave = driver.calls("stickyEvent").find((c) => c.content.member?.membership === "leave");
    expect(leave?.content.leave_reason.code).toBe("slot_closed");
    expect(driver.calls("cancelDelayed")).toHaveLength(1);
  });

  it("a homeserver without delayed events degrades the membership lifetime", async () => {
    const { driver, manager } = newManager();
    driver.refuseDelayedEvents = true;
    await manager.join(receiveOnly(), joinParams);
    expect(driver.calls("stickyEvent")[0].durationMs).toBe(300_000n);
    expect(manager.status()).toBe(FfiStatus.Connected);
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
