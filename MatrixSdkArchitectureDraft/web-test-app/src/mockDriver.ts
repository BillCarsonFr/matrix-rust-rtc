// A MatrixDriver implemented in TypeScript — the same seam a
// matrix-js-sdk-backed driver will implement. This mock records every
// outbound call for assertions and returns canned homeserver responses.
import type {
  FfiEventOrigin,
  FfiLivekitTokenRequest,
  FfiOpenIdToken,
  FfiRtcTransport,
  FfiSendEventResponse,
  FfiToDeviceDelivery,
  FfiToDeviceRecipient,
  MatrixDriverCallback,
  RoomEventSinkInterface,
  StateUpdateSinkInterface,
  ToDeviceSinkInterface,
} from "./generated/matrix_rtc";

export const LK_SERVICE_URL = "https://lk.example.org";

export type OutboundCall =
  | { kind: "stickyEvent"; roomId: string; eventType: string; content: unknown; durationMs: bigint }
  | { kind: "stateEvent"; roomId: string; eventType: string; stateKey: string; content: unknown }
  | { kind: "delayedEvent"; roomId: string; eventType: string; content: unknown; delayMs: bigint; delayId: string }
  | { kind: "restartDelayed"; roomId: string; delayId: string }
  | { kind: "cancelDelayed"; roomId: string; delayId: string }
  | { kind: "delegateDelayedLeave"; roomId: string; slotId: string; delayId: string }
  | { kind: "toDevice"; recipients: FfiToDeviceRecipient[]; eventType: string; content: unknown }
  | { kind: "getOpenId" }
  | { kind: "getRtcTransports" }
  | { kind: "getLivekitToken"; url: string; roomId: string; slotId: string };

export class MockMatrixDriver implements MatrixDriverCallback {
  readonly outbound: OutboundCall[] = [];
  private nextDelayId = 0;
  /** Optional observer so the demo app can render the outbound log live. */
  onOutbound?: (call: OutboundCall) => void;

  private record(call: OutboundCall) {
    this.outbound.push(call);
    this.onOutbound?.(call);
  }

  calls(kind: OutboundCall["kind"]): OutboundCall[] {
    return this.outbound.filter((c) => c.kind === kind);
  }

  async sendStickyEvent(
    roomId: string,
    eventType: string,
    contentJson: string,
    durationMs: bigint,
  ): Promise<FfiSendEventResponse> {
    this.record({ kind: "stickyEvent", roomId, eventType, content: JSON.parse(contentJson), durationMs });
    return { eventId: `$sticky-${this.outbound.length}`, delayId: undefined };
  }

  async sendStateEvent(
    roomId: string,
    eventType: string,
    stateKey: string,
    contentJson: string,
  ): Promise<FfiSendEventResponse> {
    this.record({ kind: "stateEvent", roomId, eventType, stateKey, content: JSON.parse(contentJson) });
    return { eventId: `$state-${this.outbound.length}`, delayId: undefined };
  }

  async sendDelayedEvent(
    roomId: string,
    eventType: string,
    contentJson: string,
    delayMs: bigint,
  ): Promise<string> {
    const delayId = `delay-${this.nextDelayId++}`;
    this.record({ kind: "delayedEvent", roomId, eventType, content: JSON.parse(contentJson), delayMs, delayId });
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
    // every recipient reachable
    return recipients.map((recipient) => ({ recipient, error: undefined }));
  }

  async getOpenId(): Promise<FfiOpenIdToken> {
    this.record({ kind: "getOpenId" });
    return {
      accessToken: "openid-token",
      tokenType: "Bearer",
      matrixServerName: "example.org",
      expiresInMs: 3600_000n,
    };
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

  async getLivekitToken(request: FfiLivekitTokenRequest): Promise<string> {
    this.record({
      kind: "getLivekitToken",
      url: request.url,
      roomId: request.roomId,
      slotId: request.slotId,
    });
    return "jwt-for-" + request.url;
  }

  async readEvents(): Promise<string[]> {
    return [];
  }

  async readState(): Promise<string[]> {
    return [];
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

  /** Emit any room event — sticky or state; the session dispatches on type. */
  emitRoomEvent(eventJson: string, origin: FfiEventOrigin): boolean {
    if (!this.roomEventSink) throw new Error("SDK has not subscribed to room events");
    return this.roomEventSink.emit(eventJson, origin);
  }

  emitToDevice(
    eventType: string,
    sender: string,
    contentJson: string,
    origin: FfiEventOrigin,
  ): boolean {
    if (!this.toDeviceSink) throw new Error("SDK has not subscribed to to-device events");
    return this.toDeviceSink.emit(eventType, sender, contentJson, origin);
  }

  emitStateUpdate(eventsJson: string[]): boolean {
    if (!this.stateUpdateSink) throw new Error("SDK has not subscribed to state updates");
    return this.stateUpdateSink.emit(eventsJson);
  }
}

// ---------------------------------------------------------------------------
// Inbound event fabrication — plausible MSC4143/MSC4354 wire shapes. If the
// todo!() implementations settle on different field names, adjust here (one
// place), not in every test.
// ---------------------------------------------------------------------------

export const ROOM_ID = "!room:example.org";
export const SLOT_ID = "m.call";

let eventCounter = 0;

export function memberJoinEvent(opts: {
  userId: string;
  deviceId: string;
  memberId: string;
  lkServiceUrl?: string;
}): string {
  return JSON.stringify({
    type: "m.rtc.member",
    sender: opts.userId,
    event_id: `$ev-${eventCounter++}`,
    room_id: ROOM_ID,
    origin_server_ts: Date.now(),
    msc4354_sticky: { duration_ms: 240_000 },
    content: {
      slot_id: SLOT_ID,
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
      member: { id: opts.memberId, membership: "leave" },
      leave_reason: { code: "m.user_hangup" },
    },
  });
}

export function slotOpenEvent(): string {
  return JSON.stringify({
    type: "m.rtc.slot",
    sender: "@admin:example.org",
    event_id: `$ev-${eventCounter++}`,
    room_id: ROOM_ID,
    state_key: SLOT_ID,
    origin_server_ts: Date.now(),
    content: { status: "open", application: { type: "m.call" } },
  });
}

export function encryptionKeyContent(opts: { memberId: string; index: number }): string {
  return JSON.stringify({
    room_id: ROOM_ID,
    member_id: opts.memberId,
    keys: [{ index: opts.index, key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" }],
    format: 0,
  });
}
