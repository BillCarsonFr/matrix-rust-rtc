// Full integration against the demo backend (../../demo/backend: Synapse +
// lk-jwt-service + LiveKit) through the matrix-js-sdk driver: two real users
// in one room, two managers, membership + connections + token exchange over
// the real homeserver and authorisation service.
//
// Opt-in: `MATRIX_RTC_BACKEND=1 npx vitest run test/backend.test.ts` after
// `make backend-up` in the repository root. The room is unencrypted (no Olm
// in this node process), so media keys do not flow here — see the browser
// demo with "crypto on" for that.
import { beforeAll, describe, expect, it } from "vitest";
import {
  FfiElementCallCompat,
  FfiMatrixDriver,
  FfiMembershipState,
  FfiParticipationManager,
  FfiStatus,
  FfiTransportIntent,
} from "../src/generated/matrix_rtc";
import { createJsSdkBackend, type JsSdkBackend } from "../src/jsSdkDriver";
import { waitFor } from "../src/mockDriver";
import { initWasm } from "./wasmInit";

const HOMESERVER_URL = process.env.HOMESERVER_URL ?? "http://localhost:8008";
const LK_SERVICE_URL = process.env.LK_SERVICE_URL ?? "http://localhost:6080";
const SLOT_ID = "m.call#ROOM";
const enabled = process.env.MATRIX_RTC_BACKEND === "1";

const joinParams = {
  applicationType: "m.call",
  intent: undefined,
  stickyDurationMs: 240_000n,
  keepAliveTimeoutMs: 15_000n,
  degradedLifetimeMs: undefined,
  delegateDelayedLeave: false,
};

const publish = () =>
  new FfiTransportIntent.Publish({
    transport: { transportType: "livekit", propertiesJson: JSON.stringify({ livekit_service_url: LK_SERVICE_URL }) },
  });

function managerFor(b: JsSdkBackend) {
  const driver = new FfiMatrixDriver(b.driver);
  return new FfiParticipationManager(b.roomId, b.slotId, b.userId, b.deviceId, driver, FfiElementCallCompat.Off);
}

describe.skipIf(!enabled)("demo backend (matrix-js-sdk driver)", () => {
  let alice: JsSdkBackend;
  let bob: JsSdkBackend;
  const log = (who: string) => (line: string) => console.log(`[${who}] ${line}`);

  beforeAll(async () => {
    await initWasm();
    alice = await createJsSdkBackend({ homeserverUrl: HOMESERVER_URL, slotId: SLOT_ID, lkServiceUrl: LK_SERVICE_URL, displayName: "Alice", log: log("alice") });
    bob = await createJsSdkBackend({ homeserverUrl: HOMESERVER_URL, slotId: SLOT_ID, roomId: alice.roomId, lkServiceUrl: LK_SERVICE_URL, displayName: "Bob", log: log("bob") });
  }, 60_000);

  it("two users see each other's membership, hold real tokens, and leave cleanly", async () => {
    const a = managerFor(alice);
    const b = managerFor(bob);
    try {
      // Nobody opened the slot yet: the real room's state says "no slot".
      expect(a.session().slotOpen).not.toBe(true);
      await a.openSlot("m.call", false);
      // The slot opens once sync echoes the state event.
      await waitFor("slot open on alice", () => a.session().slotOpen === true, 20_000);
      await waitFor("slot open on bob", () => b.session().slotOpen === true, 20_000);

      await a.join(publish(), joinParams);
      expect(FfiStatus.Connected.instanceOf(a.status())).toBe(true);

      // Alice's own connection carries a JWT minted by lk-jwt-service.
      const connections = a.connections();
      expect(connections).toHaveLength(1);
      expect(connections[0].connection.serviceUrl).toBe(LK_SERVICE_URL);
      expect(connections[0].connection.jwtToken.split(".")).toHaveLength(3);
      expect(connections[0].connection.wsUrl).toMatch(/^wss?:\/\//);

      // Bob's session sees Alice through sync.
      await waitFor("bob sees alice", () => b.memberships().some((m) => m.member.userId === alice.userId), 20_000);
      const seen = b.memberships().find((m) => m.member.userId === alice.userId)!;
      expect(seen.state).toBe(FfiMembershipState.Joined);
      expect(seen.connections).toEqual([LK_SERVICE_URL]);
      expect(seen.member.deviceId).toBeUndefined(); // unencrypted room: no decryption metadata
      expect(seen.transportIdentity).toBeUndefined();
      // Alice sees her own echo.
      await waitFor("alice sees herself", () => a.memberships().some((m) => m.member.userId === alice.userId), 20_000);

      await b.join(publish(), joinParams);
      await waitFor("alice sees bob", () => a.memberships().some((m) => m.member.userId === bob.userId), 20_000);
      // Both publish on the same focus: one connection with two members.
      await waitFor("two members on the connection", () => a.connections()[0]?.members.length === 2, 20_000);

      await a.leave("m.user_hangup", undefined);
      expect(FfiStatus.Disconnected.instanceOf(a.status())).toBe(true);
      expect(a.connections()).toEqual([]);
      await waitFor("bob sees alice gone", () => !b.memberships().some((m) => m.member.userId === alice.userId), 20_000);
      await b.leave(undefined, undefined);
    } finally {
      a.uniffiDestroy();
      b.uniffiDestroy();
      await alice.stop();
      await bob.stop();
    }
  }, 120_000);
});
