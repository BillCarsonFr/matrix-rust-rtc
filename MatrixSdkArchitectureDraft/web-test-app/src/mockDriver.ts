// A MatrixDriver implemented in TypeScript — the same seam a
// matrix-js-sdk-backed driver implements (see jsSdkDriver.ts). This mock
// models a homeserver: it records every outbound call for assertions, returns
// canned responses, **echoes** accepted sticky/state events back through the
// room-event sink (as sync would, so our own membership reaches the roster
// like anybody else's), and hosts simulated remote peers that answer our
// media key with theirs.
import type {
  FfiEventOrigin,
  FfiLivekitToken,
  FfiLivekitTokenRequest,
  FfiRtcTransport,
  FfiSendEventResponse,
  FfiToDeviceDelivery,
  FfiToDeviceRecipient,
  MatrixDriverCallback,
  RoomEventSinkInterface,
  StateUpdateSinkInterface,
  ToDeviceSinkInterface,
} from "./generated/matrix_rtc";
import { FfiEventOrigin as Origin, RtcError } from "./generated/matrix_rtc";

export const LK_SERVICE_URL = "https://lk.example.org";
export const ROOM_ID = "!room:example.org";
// MSC4143: a slot id is `{application_type}#{id}`; a bare "m.call" resolves closed.
export const SLOT_ID = "m.call#ROOM";
export const OWN_USER_ID = "@me:example.org";
export const OWN_DEVICE_ID = "MYDEV";

export type OutboundCall =
  | { kind: "stickyEvent"; roomId: string; eventType: string; content: any; durationMs: bigint }
  | { kind: "stateEvent"; roomId: string; eventType: string; stateKey: string; content: any }
  | {
      kind: "delayedEvent";
      roomId: string;
      eventType: string;
      content: any;
      delayMs: bigint;
      stickyDurationMs: bigint | undefined;
      delayId: string;
    }
  | {
      kind: "delayedStateEvent";
      roomId: string;
      eventType: string;
      stateKey: string;
      content: any;
      delayMs: bigint;
      delayId: string;
    }
  | { kind: "restartDelayed"; roomId: string; delayId: string }
  | { kind: "cancelDelayed"; roomId: string; delayId: string }
  | { kind: "delegateDelayedLeave"; roomId: string; slotId: string; delayId: string }
  | { kind: "toDevice"; recipients: FfiToDeviceRecipient[]; eventType: string; content: any }
  | { kind: "getRtcTransports" }
  | { kind: "getLivekitToken"; url: string; roomId: string; slotId: string; member: any; legacySfuGet: boolean };

/** A simulated remote participant. */
export interface RemotePeer {
  userId: string;
  deviceId: string;
  memberId: string;
  /** 32 key bytes; defaults to a constant pattern. */
  key?: Uint8Array;
}

export class MockMatrixDriver implements MatrixDriverCallback {
  readonly outbound: OutboundCall[] = [];
  /** Optional observer so the demo app can render the outbound log live. */
  onOutbound?: (call: OutboundCall) => void;
  /** Refuse delayed events like a homeserver without MSC4140 (404). */
  refuseDelayedEvents = false;
  /** Room state answered by `readState` (the session seed). */
  roomState: any[] = [];
  /** Simulated peers answer our media key with theirs (index 0). */
  readonly peers: RemotePeer[] = [];
  private nextDelayId = 0;
  private nextEventId = 0;

  constructor(private readonly ownUserId = OWN_USER_ID, private readonly ownDeviceId = OWN_DEVICE_ID) {}

  private record(call: OutboundCall) {
    this.outbound.push(call);
    this.onOutbound?.(call);
  }

  calls<K extends OutboundCall["kind"]>(kind: K): Extract<OutboundCall, { kind: K }>[] {
    return this.outbound.filter((c) => c.kind === kind) as Extract<OutboundCall, { kind: K }>[];
  }

  private eventId(): string {
    return `$echo-${this.nextEventId++}`;
  }

  async sendStickyEvent(
    roomId: string,
    eventType: string,
    contentJson: string,
    durationMs: bigint,
  ): Promise<FfiSendEventResponse> {
    const content = JSON.parse(contentJson);
    this.record({ kind: "stickyEvent", roomId, eventType, content, durationMs });
    const eventId = this.eventId();
    // The homeserver echoes our event through sync.
    this.echo(
      {
        type: eventType,
        sender: this.ownUserId,
        event_id: eventId,
        room_id: roomId,
        origin_server_ts: Date.now(),
        msc4354_sticky: { duration_ms: Number(durationMs) },
        content,
      },
      new Origin.Encrypted({ senderDeviceId: this.ownDeviceId }),
    );
    return { eventId, delayId: undefined };
  }

  async sendStateEvent(
    roomId: string,
    eventType: string,
    stateKey: string,
    contentJson: string,
  ): Promise<FfiSendEventResponse> {
    const content = JSON.parse(contentJson);
    this.record({ kind: "stateEvent", roomId, eventType, stateKey, content });
    const eventId = this.eventId();
    this.echo(
      {
        type: eventType,
        sender: this.ownUserId,
        event_id: eventId,
        room_id: roomId,
        state_key: stateKey,
        origin_server_ts: Date.now(),
        content,
      },
      new Origin.Cleartext(),
    );
    return { eventId, delayId: undefined };
  }

  async sendDelayedEvent(
    roomId: string,
    eventType: string,
    contentJson: string,
    delayMs: bigint,
    stickyDurationMs: bigint | undefined,
  ): Promise<string> {
    const delayId = `delay-${this.nextDelayId++}`;
    this.record({
      kind: "delayedEvent",
      roomId,
      eventType,
      content: JSON.parse(contentJson),
      delayMs,
      stickyDurationMs,
      delayId,
    });
    if (this.refuseDelayedEvents) {
      // 404 M_UNRECOGNIZED: "this homeserver will never do delayed events".
      throw new RtcError.Unsupported("M_UNRECOGNIZED: delayed events are not supported");
    }
    return delayId;
  }

  async sendDelayedStateEvent(
    roomId: string,
    eventType: string,
    stateKey: string,
    contentJson: string,
    delayMs: bigint,
  ): Promise<string> {
    const delayId = `delay-${this.nextDelayId++}`;
    this.record({
      kind: "delayedStateEvent",
      roomId,
      eventType,
      stateKey,
      content: JSON.parse(contentJson),
      delayMs,
      delayId,
    });
    return delayId;
  }

  async restartDelayedEvent(roomId: string, delayId: string): Promise<void> {
    this.record({ kind: "restartDelayed", roomId, delayId });
  }

  async cancelDelayedEvent(roomId: string, delayId: string): Promise<void> {
    this.record({ kind: "cancelDelayed", roomId, delayId });
  }

  async delegateLivekitDelayedLeave(
    roomId: string,
    slotId: string,
    _memberJson: string,
    delayId: string,
  ): Promise<void> {
    this.record({ kind: "delegateDelayedLeave", roomId, slotId, delayId });
  }

  async sendToDevice(
    recipients: FfiToDeviceRecipient[],
    eventType: string,
    contentJson: string,
  ): Promise<FfiToDeviceDelivery[]> {
    this.record({ kind: "toDevice", recipients, eventType, content: JSON.parse(contentJson) });
    // Simulated peers answer with their own key.
    for (const recipient of recipients) {
      const peer = this.peers.find(
        (p) => p.userId === recipient.userId && p.deviceId === recipient.deviceId,
      );
      if (peer) queueMicrotask(() => this.peerSendsKey(peer, 0));
    }
    // every recipient reachable
    return recipients.map((recipient) => ({ recipient, error: undefined }));
  }

  async getRtcTransports(): Promise<FfiRtcTransport[]> {
    this.record({ kind: "getRtcTransports" });
    return [
      {
        transportType: "livekit",
        propertiesJson: JSON.stringify({ livekit_service_url: LK_SERVICE_URL }),
      },
    ];
  }

  async getLivekitToken(request: FfiLivekitTokenRequest): Promise<FfiLivekitToken> {
    this.record({
      kind: "getLivekitToken",
      url: request.url,
      roomId: request.roomId,
      slotId: request.slotId,
      member: JSON.parse(request.memberJson),
      legacySfuGet: request.legacySfuGet,
    });
    return { jwt: "jwt-for-" + request.url, url: request.url.replace("https", "wss") };
  }

  async readEvents(): Promise<string[]> {
    return [];
  }

  async readState(eventType: string, stateKey: string | undefined): Promise<string[]> {
    return this.roomState
      .filter((e) => e.type === eventType && (stateKey === undefined || e.state_key === stateKey))
      .map((e) => JSON.stringify(e));
  }

  // --- inbound: the SDK subscribes exactly once, during FfiMatrixDriver
  // construction, and hands us sinks; a real driver hooks matrix-js-sdk
  // listeners onto them. The mock stores them so the app/tests can emit
  // fabricated events. Single-sink semantics: fan-out to multiple managers
  // happens on the Rust side.

  private roomEventSink?: RoomEventSinkInterface;
  private toDeviceSink?: ToDeviceSinkInterface;
  private stateUpdateSink?: StateUpdateSinkInterface;

  subscribeRoomEvents(sink: RoomEventSinkInterface): void {
    this.roomEventSink = sink;
  }

  subscribeToDeviceEvents(sink: ToDeviceSinkInterface): void {
    this.toDeviceSink = sink;
  }

  subscribeStateUpdates(sink: StateUpdateSinkInterface): void {
    this.stateUpdateSink = sink;
  }

  private echo(event: any, origin: FfiEventOrigin) {
    this.roomEventSink?.emit(JSON.stringify(event), origin);
  }

  /** Emit any room event — sticky or state; the session dispatches on type. */
  emitRoomEvent(eventJson: string, origin: FfiEventOrigin): boolean {
    if (!this.roomEventSink) throw new Error("SDK has not subscribed to room events");
    return this.roomEventSink.emit(eventJson, origin);
  }

  /** `senderCrossSigned` is the MSC4153 verdict; peers are cross-signed by default. */
  emitToDevice(
    eventType: string,
    sender: string,
    contentJson: string,
    origin: FfiEventOrigin,
    senderCrossSigned: boolean | undefined = true,
  ): boolean {
    if (!this.toDeviceSink) throw new Error("SDK has not subscribed to to-device events");
    return this.toDeviceSink.emit(eventType, sender, contentJson, origin, senderCrossSigned);
  }

  emitStateUpdate(eventsJson: string[]): boolean {
    if (!this.stateUpdateSink) throw new Error("SDK has not subscribed to state updates");
    return this.stateUpdateSink.emit(eventsJson);
  }

  // --- simulated peers -------------------------------------------------------

  addPeer(peer: RemotePeer): RemotePeer {
    this.peers.push(peer);
    return peer;
  }

  /** The peer publishes a join (on `LK_SERVICE_URL` unless given). */
  peerJoins(peer: RemotePeer, opts: { lkServiceUrl?: string; durationMs?: number } = {}): boolean {
    return this.emitRoomEvent(
      memberJoinEvent({ userId: peer.userId, memberId: peer.memberId, ...opts }),
      new Origin.Encrypted({ senderDeviceId: peer.deviceId }),
    );
  }

  peerLeaves(peer: RemotePeer): boolean {
    return this.emitRoomEvent(
      memberLeaveEvent({ userId: peer.userId, memberId: peer.memberId }),
      new Origin.Encrypted({ senderDeviceId: peer.deviceId }),
    );
  }

  peerSendsKey(peer: RemotePeer, index: number): boolean {
    return this.emitToDevice(
      "m.rtc.encryption_key",
      peer.userId,
      encryptionKeyContent({ memberId: peer.memberId, index, key: peer.key }),
      new Origin.Encrypted({ senderDeviceId: peer.deviceId }),
    );
  }
}

// ---------------------------------------------------------------------------
// Inbound event fabrication — the MSC4143/MSC4354 wire shapes the session's
// dispatch reads (see src/session/dispatch.rs for the accessor contract and
// src/session/test_support.rs for the Rust twin of these builders, kept in
// sync by a fixture test). Adjust here (one place), not in every test.
// ---------------------------------------------------------------------------

let eventCounter = 0;

export function memberJoinEvent(opts: {
  userId: string;
  memberId: string;
  lkServiceUrl?: string;
  durationMs?: number;
  /** `org.matrix.msc4143.rtc.member` instead of the stable type. */
  unstableType?: boolean;
}): string {
  return JSON.stringify({
    type: opts.unstableType ? "org.matrix.msc4143.rtc.member" : "m.rtc.member",
    sender: opts.userId,
    event_id: `$ev-${eventCounter++}`,
    room_id: ROOM_ID,
    origin_server_ts: Date.now(),
    msc4354_sticky: { duration_ms: opts.durationMs ?? 240_000 },
    content: {
      slot_id: SLOT_ID,
      // MSC4354: the sticky key lives in the content and equals member.id.
      msc4354_sticky_key: opts.memberId,
      member: { id: opts.memberId, membership: "join" },
      application: { type: "m.call" },
      transports: {
        published: [
          { type: "livekit", livekit_service_url: opts.lkServiceUrl ?? LK_SERVICE_URL },
        ],
        can_subscribe: ["livekit"],
      },
    },
  });
}

export function memberLeaveEvent(opts: { userId: string; memberId: string }): string {
  return JSON.stringify({
    type: "m.rtc.member",
    sender: opts.userId,
    event_id: `$ev-${eventCounter++}`,
    room_id: ROOM_ID,
    origin_server_ts: Date.now(),
    msc4354_sticky: { duration_ms: 240_000 },
    content: {
      slot_id: SLOT_ID,
      msc4354_sticky_key: opts.memberId,
      member: { id: opts.memberId, membership: "leave" },
      leave_reason: { code: "leave" },
    },
  });
}

export function slotEvent(opts: { status: "open" | "closed"; encrypted?: boolean } = { status: "open" }): string {
  const content: any = { status: opts.status, application: { type: "m.call" } };
  if (opts.encrypted) content.encryption = { type: "m.per_member" };
  return JSON.stringify({
    type: "m.rtc.slot",
    sender: "@admin:example.org",
    event_id: `$ev-${eventCounter++}`,
    room_id: ROOM_ID,
    state_key: SLOT_ID,
    origin_server_ts: Date.now(),
    content,
  });
}

export const slotOpenEvent = () => slotEvent({ status: "open" });
export const slotClosedEvent = () => slotEvent({ status: "closed" });

export function roomEncryptionEvent(): string {
  return JSON.stringify({
    type: "m.room.encryption",
    sender: "@admin:example.org",
    event_id: `$ev-${eventCounter++}`,
    room_id: ROOM_ID,
    state_key: "",
    origin_server_ts: Date.now(),
    content: { algorithm: "m.megolm.v1.aes-sha2" },
  });
}

const DEFAULT_KEY = new Uint8Array(32).fill(7);

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/=+$/, "");
}

/** MSC4143 `m.rtc.encryption_key` content. */
export function encryptionKeyContent(opts: { memberId: string; index: number; key?: Uint8Array }): string {
  return JSON.stringify({
    room_id: ROOM_ID,
    member_id: opts.memberId,
    media_key: { index: opts.index, key: base64(opts.key ?? DEFAULT_KEY) },
    format: 0,
  });
}

/** One microtask tick: listener callbacks arrive after the emitting task yields. */
export const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

export async function waitFor(what: string, cond: () => boolean, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for: ${what}`);
    await new Promise((r) => setTimeout(r, 10));
  }
}
