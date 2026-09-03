// Demo: how a web host drives the matrix-rtc SDK.
//
// The flow mirrors ../MatrixSdkArchitecture.md "Usage": tiles come from the
// memberships callback, LK rooms from the connections callback, keys from
// the key-map callback. Two drivers are available: the TS mock (a fake
// homeserver with simulated peers) and a matrix-js-sdk driver against a real
// homeserver (demo/backend). A real app would also hold LiveKit Room objects
// — here they are rendered as text.
import { initWasm } from "./wasmLoader";
import {
  FfiElementCallCompat,
  FfiMatrixDriver,
  FfiMembershipState,
  FfiParticipationManager,
  FfiStatus,
  FfiTransportIntent,
  type FfiConnectionWithMembers,
  type FfiMediaKey,
  type FfiMembership,
  type MatrixDriverCallback,
} from "./generated/matrix_rtc";
import {
  LK_SERVICE_URL,
  MockMatrixDriver,
  OWN_DEVICE_ID,
  OWN_USER_ID,
  ROOM_ID,
  SLOT_ID,
  roomEncryptionEvent,
  slotClosedEvent,
  slotEvent,
  type RemotePeer,
} from "./mockDriver";
import { createJsSdkBackend, type JsSdkBackend } from "./jsSdkDriver";

const $ = <T extends HTMLElement = HTMLElement>(id: string) => document.getElementById(id) as T;
const logError = (e: unknown) => {
  $("errors").textContent = `${new Date().toISOString().slice(11, 19)} ${String(e)}\n` + $("errors").textContent;
  console.error(e);
};
const guarded = (f: () => void | Promise<void>) => async () => {
  try {
    await f();
  } catch (e) {
    logError(e);
  }
};
const jsonish = (v: unknown) => JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? x.toString() : x), 2);

/** What the UI needs from whichever driver is active. */
interface Backend {
  driver: MatrixDriverCallback;
  roomId: string;
  slotId: string;
  userId: string;
  deviceId: string;
  lkServiceUrl: string;
  describe: string;
  /** Mock-only controls (null for a real homeserver). */
  mock: MockMatrixDriver | null;
  stop?: () => Promise<void>;
}

let backend: Backend | null = null;
let manager: FfiParticipationManager | null = null;
let keyMap: FfiMediaKey[] = [];
let memberships: FfiMembership[] = [];
let ownMemberId: string | undefined;

function renderTiles() {
  const tiles = $("tiles");
  const keysFor = (memberId: string) =>
    keyMap.filter((k) => k.memberId === memberId).map((k) => `#${k.index}`).join(" ") || "none";
  tiles.replaceChildren(
    ...memberships.map((m) => {
      const tile = document.createElement("div");
      const isMe = m.member.userId === backend?.userId && m.member.deviceId === backend?.deviceId;
      tile.className = "tile" + (m.state === FfiMembershipState.LeftWithKeys ? " leaving" : "") + (isMe ? " me" : "");
      const name = m.member.displayName ?? m.member.userId;
      const initial = name.replace(/^@/, "")[0] ?? "?";
      const rows: [string, string][] = [
        ["state", m.state === FfiMembershipState.LeftWithKeys ? "left — may still hold our key" : "in call"],
        ["member id", m.member.memberId],
        ["user", m.member.userId],
        ["device", `${m.member.deviceId ?? "?"} (${["verified", "claimed", "unknown"][m.member.deviceAttribution] ?? m.member.deviceAttribution})`],
        ["application", `${m.member.applicationType ?? "?"}${m.member.intent ? ` · ${m.member.intent}` : ""}`],
        ["publishes on", m.connections.join(", ") || "nothing (receive-only)"],
        ["can subscribe", m.member.canSubscribe.join(", ") || "—"],
        ["LK identity", m.transportIdentity ?? "—"],
        ["keys held", keysFor(m.member.memberId)],
      ];
      if (m.member.membershipTs) rows.push(["joined", new Date(Number(m.member.membershipTs)).toLocaleTimeString()]);
      tile.innerHTML = `
        <div class="avatar">${initial.toUpperCase()}</div>
        <div class="name">${name}${isMe ? " <span class='you'>(you)</span>" : ""}</div>
        <dl>${rows.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("")}</dl>`;
      return tile;
    }),
  );
}

function renderConnections(connections: FfiConnectionWithMembers[]) {
  // a real app would diff lkRooms here: connect new serviceUrls, drop gone ones
  $("connections").textContent = connections.length
    ? connections
        .map(
          (c) =>
            `${c.connection.serviceUrl}\n  ws: ${c.connection.wsUrl}\n  jwt: ${c.connection.jwtToken.slice(0, 24)}…\n  members: ${c.members.map((m) => m.memberId).join(", ") || "none"}`,
        )
        .join("\n")
    : "(none — join to mint tokens)";
}

function renderKeyMap() {
  $("keymap").textContent = keyMap.length
    ? keyMap.map((k) => `${k.memberId === ownMemberId ? "(own) " : ""}${k.memberId} #${k.index}: ${k.key.byteLength} bytes`).join("\n")
    : "(empty)";
}

function setStatus(text: string) {
  $("status").textContent = text;
}

async function teardownManager() {
  if (manager) {
    try {
      if (manager.status() !== FfiStatus.Disconnected) await manager.leave("m.user_hangup", undefined);
    } catch (e) {
      logError(e);
    }
    manager.uniffiDestroy();
    manager = null;
  }
  if (backend?.stop) await backend.stop();
  backend = null;
  memberships = [];
  keyMap = [];
  renderTiles();
  renderConnections([]);
  renderKeyMap();
  $("outbound").textContent = "";
}

function createManager(b: Backend) {
  backend = b;
  $("backend-info").textContent = b.describe;
  // one driver per room (subscribe handshake happens here, exactly once);
  // managers — one per slot — share it
  const matrixDriver = new FfiMatrixDriver(b.driver);
  manager = new FfiParticipationManager(b.roomId, b.slotId, b.userId, b.deviceId, matrixDriver, FfiElementCallCompat.Off);
  manager.setMembershipsListener({
    onMembershipsChange: (m) => {
      memberships = m;
      renderTiles();
    },
  });
  manager.setConnectionsListener({ onConnectionsChange: renderConnections });
  manager.setKeyMapListener({
    onKeyMapChange: (map, change) => {
      // a real app: lkRooms[..].setKeyForParticipant(identity, change.key, change.index)
      keyMap = map;
      renderKeyMap();
      renderTiles();
      console.debug("key changed", change.memberId, change.index);
    },
  });
  manager.setStatusListener({
    onStatusChange: (status) => {
      const session = manager!.session();
      setStatus(`${FfiStatus[status]} · slot ${session.slotOpen === undefined ? "unknown" : session.slotOpen ? "open" : "closed"} · ${session.encrypted ? "encrypted" : "unencrypted"}`);
      $("debug").textContent = jsonish(JSON.parse(manager!.debugSnapshot()));
    },
  });
  setStatus("Disconnected");
  $("debug").textContent = jsonish(JSON.parse(manager.debugSnapshot()));
}

function mockBackend(): Backend {
  const driver = new MockMatrixDriver();
  const encrypted = $<HTMLInputElement>("mock-encrypted").checked;
  driver.roomState = encrypted
    ? [JSON.parse(roomEncryptionEvent()), JSON.parse(slotEvent({ status: "open", encrypted: true }))]
    : [JSON.parse(slotEvent({ status: "open" }))];
  driver.onOutbound = (call) => {
    $("outbound").textContent = `${JSON.stringify(call, (_k, v) => (typeof v === "bigint" ? v.toString() : v))}\n` + $("outbound").textContent;
  };
  return {
    driver,
    roomId: ROOM_ID,
    slotId: SLOT_ID,
    userId: OWN_USER_ID,
    deviceId: OWN_DEVICE_ID,
    lkServiceUrl: LK_SERVICE_URL,
    describe: `mock homeserver · ${encrypted ? "encrypted" : "unencrypted"} room · slot ${SLOT_ID} open`,
    mock: driver,
  };
}

async function main() {
  await initWasm();
  setStatus("wasm loaded — pick a backend");

  const joinIntent = () =>
    $<HTMLSelectElement>("intent").value === "publish"
      ? new FfiTransportIntent.Publish({
          transport: {
            transportType: "livekit",
            propertiesJson: JSON.stringify({ livekit_service_url: backend!.lkServiceUrl }),
          },
        })
      : new FfiTransportIntent.ReceiveOnly({ canSubscribe: ["livekit"] });

  $("mock-start").onclick = guarded(async () => {
    await teardownManager();
    createManager(mockBackend());
    $("mock-controls").hidden = false;
  });

  $("backend-start").onclick = guarded(async () => {
    await teardownManager();
    $("mock-controls").hidden = true;
    setStatus("logging in…");
    const b: JsSdkBackend = await createJsSdkBackend({
      homeserverUrl: $<HTMLInputElement>("hs-url").value,
      user: $<HTMLInputElement>("hs-user").value,
      password: $<HTMLInputElement>("hs-password").value,
      roomId: $<HTMLInputElement>("hs-room").value || undefined,
      slotId: SLOT_ID,
      log: (line) => {
        $("outbound").textContent = `${line}\n` + $("outbound").textContent;
      },
    });
    $<HTMLInputElement>("hs-room").value = b.roomId;
    createManager({ ...b, mock: null });
  });

  $("join").onclick = guarded(async () => {
    if (!manager) throw new Error("pick a backend first");
    await manager.join(joinIntent(), {
      applicationType: "m.call",
      intent: undefined,
      stickyDurationMs: 240_000n,
      keepAliveTimeoutMs: BigInt($<HTMLInputElement>("keep-alive").value || "15000"),
      degradedLifetimeMs: undefined,
      delegateDelayedLeave: $<HTMLInputElement>("delegate").checked,
    });
    ownMemberId = JSON.parse(manager.debugSnapshot()).own_membership.member_id;
    renderKeyMap();
  });
  $("leave").onclick = guarded(async () => {
    await manager?.leave("m.user_hangup", undefined);
  });
  $("open-slot").onclick = guarded(async () => {
    await manager?.openSlot("m.call", $<HTMLInputElement>("mock-encrypted").checked);
  });
  $("close-slot").onclick = guarded(async () => {
    await manager?.closeSlot();
  });

  // --- mock-only: simulated peers -----------------------------------------
  let peerCounter = 0;
  const peers: RemotePeer[] = [];
  const mock = () => {
    if (!backend?.mock) throw new Error("mock backend only");
    return backend.mock;
  };
  $("peer-join").onclick = guarded(() => {
    peerCounter += 1;
    const peer: RemotePeer = {
      userId: `@peer${peerCounter}:example.org`,
      deviceId: `PEERDEV${peerCounter}`,
      memberId: `m-peer-${peerCounter}`,
      key: new Uint8Array(32).fill(peerCounter),
    };
    peers.push(peer);
    if ($<HTMLInputElement>("simulate-keys").checked) mock().addPeer(peer);
    mock().peerJoins(peer);
  });
  $("peer-leave").onclick = guarded(() => {
    const peer = peers.pop();
    if (peer) mock().peerLeaves(peer);
  });
  $("peer-key").onclick = guarded(() => {
    const peer = peers.at(-1);
    if (peer) mock().peerSendsKey(peer, Number($<HTMLInputElement>("peer-key-index").value || "0"));
  });
  $("mock-slot-close").onclick = guarded(() => {
    mock().emitStateUpdate([slotClosedEvent()]);
  });
  $("mock-slot-open").onclick = guarded(() => {
    mock().emitStateUpdate([slotEvent({ status: "open", encrypted: $<HTMLInputElement>("mock-encrypted").checked })]);
  });
  $("mock-refuse-delayed").onchange = () => {
    if (backend?.mock) backend.mock.refuseDelayedEvents = $<HTMLInputElement>("mock-refuse-delayed").checked;
  };

  setInterval(() => {
    if (manager) $("debug").textContent = jsonish(JSON.parse(manager.debugSnapshot()));
  }, 2000);
}

main().catch(logError);
