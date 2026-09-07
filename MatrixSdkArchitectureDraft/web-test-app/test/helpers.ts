// Shared fixtures for the wasm acceptance suites.
import {
  FfiElementCallCompat,
  FfiMatrixDriver,
  FfiParticipationManager,
  FfiTransportIntent,
} from "../src/generated/matrix_rtc";
import {
  LK_SERVICE_URL,
  MockMatrixDriver,
  OWN_DEVICE_ID,
  OWN_USER_ID,
  ROOM_ID,
  SLOT_ID,
  roomEncryptionEvent,
  slotEvent,
  slotOpenEvent,
} from "../src/mockDriver";

export function newManager(opts: { roomState?: any[]; compat?: FfiElementCallCompat; slotId?: string } = {}) {
  const driver = new MockMatrixDriver();
  driver.roomState = opts.roomState ?? [JSON.parse(slotOpenEvent())];
  // the subscribe_* handshake happens here, exactly once per driver
  const matrixDriver = new FfiMatrixDriver(driver);
  const manager = new FfiParticipationManager(
    ROOM_ID,
    opts.slotId ?? SLOT_ID,
    OWN_USER_ID,
    OWN_DEVICE_ID,
    matrixDriver,
    opts.compat ?? FfiElementCallCompat.Off,
  );
  return { driver, matrixDriver, manager };
}

export const joinParams = {
  applicationType: "m.call",
  intent: undefined,
  stickyDurationMs: 240_000n,
  keepAliveTimeoutMs: 15_000n,
  degradedLifetimeMs: undefined,
  delegateDelayedLeave: false,
};

export const receiveOnly = () => new FfiTransportIntent.ReceiveOnly({ canSubscribe: ["livekit"] });
export const publishLk = () =>
  new FfiTransportIntent.Publish({
    transport: { transportType: "livekit", propertiesJson: JSON.stringify({ livekit_service_url: LK_SERVICE_URL }) },
  });

/** Encrypted room with an encrypted (`m.per_member`) slot open. */
export const encryptedRoomState = () => [
  JSON.parse(roomEncryptionEvent()),
  JSON.parse(slotEvent({ status: "open", encrypted: true })),
];
