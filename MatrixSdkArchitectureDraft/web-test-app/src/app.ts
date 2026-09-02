// Demo: how a web host drives the matrix-rtc SDK.
//
// The flow mirrors ../MatrixSdkArchitecture.md "Usage": tiles come from the
// memberships callback, LK rooms from the connections callback, keys from
// the key-map callback. The MatrixDriver is the TS mock (a matrix-js-sdk
// implementation would replace it 1:1); a real app would also hold LiveKit
// Room objects — here they are just rendered as text.
import { initWasm } from "./wasmLoader";
import {
  FfiElementCallCompat,
  FfiEventOrigin,
  FfiMatrixDriver,
  FfiMembershipState,
  FfiParticipationManager,
  FfiTransportIntent,
  type FfiConnectionWithMembers,
  type FfiMediaKey,
  type FfiMembership,
  FfiStatus,
} from "./generated/matrix_rtc";
import {
  MockMatrixDriver,
  ROOM_ID,
  SLOT_ID,
  encryptionKeyContent,
  memberJoinEvent,
  memberLeaveEvent,
  slotOpenEvent,
} from "./mockDriver";

const $ = (id: string) => document.getElementById(id)!;
const logError = (e: unknown) => {
  $("errors").textContent = `${String(e)}\n` + $("errors").textContent;
};
const guarded = (f: () => void | Promise<void>) => async () => {
  try {
    await f();
  } catch (e) {
    logError(e);
  }
};

function renderTiles(memberships: FfiMembership[]) {
  const tiles = $("tiles");
  tiles.replaceChildren(
    ...memberships.map((m) => {
      const tile = document.createElement("div");
      tile.className = "tile" + (m.state === FfiMembershipState.LeftWithKeys ? " leaving" : "");
      const initial = (m.member.displayName ?? m.member.userId).replace(/^@/, "")[0] ?? "?";
      tile.innerHTML = `
        <div class="avatar">${initial.toUpperCase()}</div>
        <div>${m.member.displayName ?? m.member.userId}</div>
        <div class="state">${
          m.state === FfiMembershipState.LeftWithKeys ? "leaving — may still hold keys" : "in call"
        }</div>
        <div class="state">${
          // a real app would attach media here:
          //   lkRooms.get(m.connections[0])?.participant(m.transportIdentity)
          m.connections.length > 0 ? `media via ${m.connections[0]}` : "no published media"
        }</div>`;
      return tile;
    }),
  );
}

async function main() {
  await initWasm();
  $("status").textContent = "wasm loaded — Disconnected";

  const driver = new MockMatrixDriver();
  driver.onOutbound = (call) => {
    $("outbound").textContent = `${JSON.stringify(call, (_k, v) => (typeof v === "bigint" ? v.toString() : v))}\n` + $("outbound").textContent;
  };

  // one driver per room (subscribe handshake happens here, exactly once);
  // managers — one per slot — share it
  const matrixDriver = new FfiMatrixDriver(driver);
  const manager = new FfiParticipationManager(ROOM_ID, SLOT_ID, matrixDriver, FfiElementCallCompat.Off);

  manager.setMembershipsListener({ onMembershipsChange: renderTiles });
  manager.setConnectionsListener({
    onConnectionsChange: (connections: FfiConnectionWithMembers[]) => {
      // a real app would diff lkRooms here: connect new ws_urls, drop gone ones
      $("connections").textContent = JSON.stringify(connections, null, 2);
    },
  });
  manager.setKeyMapListener({
    onKeyMapChange: (keyMap: FfiMediaKey[]) => {
      // a real app: lkRooms[..].setKeyForParticipant(identity, key, index)
      $("keymap").textContent = JSON.stringify(
        keyMap.map((k) => ({ memberId: k.memberId, index: k.index, bytes: k.key.length })),
        null,
        2,
      );
    },
  });
  manager.setStatusListener({
    onStatusChange: (status: FfiStatus) => {
      $("status").textContent = FfiStatus[status];
    },
  });

  const encrypted = new FfiEventOrigin.Encrypted({ senderDeviceId: "REMOTEDEV" });

  $("join").onclick = guarded(() =>
    manager.join(new FfiTransportIntent.ReceiveOnly({ canSubscribe: ["livekit"] }), {
      applicationType: "m.call",
      stickyDurationMs: 240_000n,
      keepAliveTimeoutMs: 15_000n,
      delegateDelayedLeave: false,
    }),
  );
  $("leave").onclick = guarded(() => manager.leave("m.user_hangup", undefined));
  $("inject-slot").onclick = guarded(() => {
    driver.emitRoomEvent(slotOpenEvent(), new FfiEventOrigin.Cleartext());
  });

  let remoteCounter = 0;
  let lastRemote: { userId: string; deviceId: string; memberId: string } | undefined;
  $("inject-join").onclick = guarded(() => {
    remoteCounter += 1;
    lastRemote = {
      userId: `@remote${remoteCounter}:example.org`,
      deviceId: "REMOTEDEV",
      memberId: `member-${remoteCounter}`,
    };
    driver.emitRoomEvent(memberJoinEvent(lastRemote), encrypted);
  });
  $("inject-leave").onclick = guarded(() => {
    if (!lastRemote) return;
    driver.emitRoomEvent(memberLeaveEvent(lastRemote), encrypted);
  });
  $("inject-key").onclick = guarded(() => {
    if (!lastRemote) return;
    driver.emitToDevice(
      "m.rtc.encryption_key",
      lastRemote.userId,
      encryptionKeyContent({ memberId: lastRemote.memberId, index: 0 }),
      encrypted,
    );
  });
}

main().catch(logError);
