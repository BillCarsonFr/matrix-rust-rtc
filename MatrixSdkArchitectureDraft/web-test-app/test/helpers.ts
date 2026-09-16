// Shared fixtures for the wasm acceptance suites.
import {
  FfiElementCallCompat,
  FfiMatrixDriver,
  FfiParticipationManager,
  FfiTransportIntent,
} from "@element-hq/matrix-rtc";
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
} from "@element-hq/matrix-rtc/testing";

export function newManager(
  opts: {
    roomState?: any[];
    compat?: FfiElementCallCompat;
    slotId?: string;
    manageMediaKeys?: boolean;
    requireCrossSignedSender?: boolean;
  } = {},
) {
  const driver = new MockMatrixDriver();
  driver.roomState = opts.roomState ?? [JSON.parse(slotOpenEvent())];
  // the subscribe_* handshake happens here, exactly once per driver
  const matrixDriver = new FfiMatrixDriver(driver);
  const manager = new FfiParticipationManager(ROOM_ID, opts.slotId ?? SLOT_ID, OWN_USER_ID, OWN_DEVICE_ID, matrixDriver, {
    compat: opts.compat ?? FfiElementCallCompat.Off,
    manageMediaKeys: opts.manageMediaKeys ?? true,
    requireCrossSignedSender: opts.requireCrossSignedSender ?? true,
    useKeyDelayMs: 1000n,
    // grace(2) = 120 ms with the jitter pinned: a join and a leave a few
    // milliseconds apart share one rotation block (see the Rust fixture).
    sharedPerMinuteToDeviceContingent: 1000,
    rotationJitter: 1.0,
  });
  return { driver, matrixDriver, manager };
}

export const joinParams = {
  applicationType: "m.call",
  intent: undefined,
  stickyDurationMs: 240_000n,
  keepAliveTimeoutMs: 15_000n,
  degradedLifetimeMs: undefined,
  delegateDelayedLeave: false,
  delegatedDelayMs: 3_600_000n,
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
