// A MatrixDriver backed by matrix-js-sdk — the real-homeserver twin of
// mockDriver.ts. Outbound: MSC4354 sticky events, MSC4140 delayed events
// (restart, never cancel+resend), room state, to-device messages (Olm when
// crypto is on), MSC4195 token exchange with the OpenID hop, transport
// discovery. Inbound: the three sinks fed from js-sdk's timeline, state and
// to-device streams with `EventOrigin` from the decryption metadata.
//
// Adapted from web/src/matrix-js-sdk-host.mjs (the previous wasm crate's
// host). Requires matrix-js-sdk v42+ (`_unstable_` sticky/delayed APIs).
import * as sdk from "matrix-js-sdk";
import type { MatrixClient, MatrixEvent, Room } from "matrix-js-sdk";
import { RtcError, FfiEventOrigin as Origin } from "./generated/matrix_rtc";
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

type Log = (line: string) => void;

/** Map js-sdk / HTTP failures onto the FFI error the SDK reasons about. */
function toRtcError(error: unknown): Error {
  if (error instanceof RtcError.InvalidInput || error instanceof RtcError.Driver) return error as Error;
  if (error instanceof sdk.UnsupportedDelayedEventsEndpointError || error instanceof sdk.UnsupportedStickyEventsEndpointError) {
    return new RtcError.Unsupported(String(error));
  }
  const status = (error as { httpStatus?: number }).httpStatus;
  const code = (error as { errcode?: string }).errcode;
  if (status === 404 || code === "M_UNRECOGNIZED") return new RtcError.Unsupported(String(error));
  if (status === 403 || code === "M_FORBIDDEN") return new RtcError.Rejected(String(error));
  return new RtcError.Driver(String(error));
}

async function guard<T>(f: () => Promise<T>): Promise<T> {
  try {
    return await f();
  } catch (error) {
    throw toRtcError(error);
  }
}

export class JsSdkMatrixDriver implements MatrixDriverCallback {
  private readonly room: Room;
  private readonly detachers: (() => void)[] = [];
  /** curve25519 sender key -> device id, per megolm-attributed sender. */
  private readonly senderDeviceCache = new Map<string, string>();

  constructor(
    private readonly client: MatrixClient,
    private readonly roomId: string,
    private readonly log: Log = () => {},
  ) {
    const room = client.getRoom(roomId);
    if (!room) throw new Error(`not joined to ${roomId}`);
    this.room = room;
  }

  detach() {
    for (const detach of this.detachers.splice(0)) detach();
  }

  // --- outbound -------------------------------------------------------------

  async sendStickyEvent(roomId: string, eventType: string, contentJson: string, durationMs: bigint): Promise<FfiSendEventResponse> {
    return guard(async () => {
      const res = await this.client._unstable_sendStickyEvent(roomId, Number(durationMs), null, eventType as any, JSON.parse(contentJson));
      this.log(`sticky ${eventType} → ${res.event_id}`);
      return { eventId: res.event_id, delayId: undefined };
    });
  }

  async sendStateEvent(roomId: string, eventType: string, stateKey: string, contentJson: string): Promise<FfiSendEventResponse> {
    return guard(async () => {
      const res = await this.client.sendStateEvent(roomId, eventType as any, JSON.parse(contentJson), stateKey);
      this.log(`state ${eventType}[${stateKey}] → ${res.event_id}`);
      return { eventId: res.event_id, delayId: undefined };
    });
  }

  async sendDelayedEvent(roomId: string, eventType: string, contentJson: string, delayMs: bigint, stickyDurationMs: bigint | undefined): Promise<string> {
    return guard(async () => {
      const content = JSON.parse(contentJson);
      const res =
        stickyDurationMs !== undefined
          ? await this.client._unstable_sendStickyDelayedEvent(roomId, Number(stickyDurationMs), { delay: Number(delayMs) }, null, eventType as any, content)
          : await this.client._unstable_sendDelayedEvent(roomId, { delay: Number(delayMs) }, null, eventType as any, content);
      this.log(`delayed ${eventType} (${delayMs} ms) → ${res.delay_id}`);
      return res.delay_id;
    });
  }

  async sendDelayedStateEvent(roomId: string, eventType: string, stateKey: string, contentJson: string, delayMs: bigint): Promise<string> {
    return guard(async () => {
      const res = await this.client._unstable_sendDelayedStateEvent(roomId, { delay: Number(delayMs) }, eventType as any, JSON.parse(contentJson), stateKey);
      return res.delay_id;
    });
  }

  async restartDelayedEvent(_roomId: string, delayId: string): Promise<void> {
    await guard(() => this.client._unstable_updateDelayedEvent(delayId, sdk.UpdateDelayedEventAction.Restart));
  }

  async cancelDelayedEvent(_roomId: string, delayId: string): Promise<void> {
    await guard(() => this.client._unstable_updateDelayedEvent(delayId, sdk.UpdateDelayedEventAction.Cancel));
  }

  async delegateLivekitDelayedLeave(roomId: string, slotId: string, memberJson: string, delayId: string): Promise<void> {
    // MSC4195: the homeserver hands the delayed leave to the SFU once the
    // participant is connected. Best effort: the SDK restarts it itself when
    // this fails.
    await guard(() =>
      this.client.http.authedRequest(
        sdk.Method.Post,
        "/rtc/livekit/delegate_delayed_leave",
        undefined,
        { room_id: roomId, slot_id: slotId, member_id: JSON.parse(memberJson).id, delay_id: delayId },
        { prefix: "/_matrix/client/unstable/org.matrix.msc4195" },
      ),
    );
  }

  async sendToDevice(recipients: FfiToDeviceRecipient[], eventType: string, contentJson: string): Promise<FfiToDeviceDelivery[]> {
    return guard(async () => {
      const content = JSON.parse(contentJson);
      if (this.client.getCrypto()) {
        // Olm-encrypted, per specific device — never `*`.
        await this.client.encryptAndSendToDevice(eventType, recipients, content);
      } else {
        this.log(`to-device ${eventType} sent in cleartext (no crypto): peers will reject it`);
        const map: Map<string, Map<string, Record<string, unknown>>> = new Map();
        for (const r of recipients) {
          if (!map.has(r.userId)) map.set(r.userId, new Map());
          map.get(r.userId)!.set(r.deviceId, content);
        }
        await this.client.sendToDevice(eventType, map as any);
      }
      return recipients.map((recipient) => ({ recipient, error: undefined }));
    });
  }

  async getRtcTransports(): Promise<FfiRtcTransport[]> {
    return guard(async () => {
      let foci: any[];
      try {
        const res = (await this.client.http.authedRequest(sdk.Method.Get, "/rtc/transports", undefined, undefined, { prefix: "/_matrix/client/v1" })) as any;
        foci = res.rtc_transports ?? res.transports ?? [];
      } catch (error) {
        this.log(`GET /rtc/transports failed (${error}); falling back to .well-known`);
        const response = await fetch(`${this.client.baseUrl.replace(/\/$/, "")}/.well-known/matrix/client`);
        if (!response.ok) throw new Error(`well-known fetch failed: ${response.status}`);
        foci = ((await response.json()) as any)["org.matrix.msc4143.rtc_foci"] ?? [];
      }
      return foci.map(({ type, ...properties }: any) => ({ transportType: String(type), propertiesJson: JSON.stringify(properties) }));
    });
  }

  async getLivekitToken(request: FfiLivekitTokenRequest): Promise<FfiLivekitToken> {
    return guard(async () => {
      const openid_token = await this.client.getOpenIdToken();
      const base = request.url.replace(/\/$/, "");
      const member = JSON.parse(request.memberJson);
      const [endpoint, body] = request.legacySfuGet
        ? [`${base}/sfu/get`, { room: request.roomId, openid_token, device_id: member.claimed_device_id }]
        : [`${base}/get_token`, { room_id: request.roomId, slot_id: request.slotId, openid_token, member }];
      const response = await fetch(endpoint, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
      if (!response.ok) {
        const text = await response.text();
        if (response.status === 403) throw new RtcError.Rejected(`${endpoint}: ${response.status} ${text}`);
        throw new RtcError.Driver(`${endpoint}: ${response.status} ${text}`);
      }
      const token = (await response.json()) as { jwt: string; url?: string };
      this.log(`token for ${request.url} → sfu ${token.url ?? request.url}`);
      return { jwt: token.jwt, url: token.url };
    });
  }

  // --- reads (the session seed) ---------------------------------------------

  async readEvents(eventType: string, _stateKey: string | undefined, limit: number): Promise<string[]> {
    const out: string[] = [];
    for (const event of this.room._unstable_getStickyEvents() as MatrixEvent[]) {
      await this.client.decryptEventIfNeeded(event);
      if (event.getType() !== eventType) continue;
      out.push(JSON.stringify(this.rawEvent(event)));
      if (out.length >= limit) break;
    }
    return out;
  }

  async readState(eventType: string, stateKey: string | undefined): Promise<string[]> {
    const events =
      stateKey === undefined
        ? this.room.currentState.getStateEvents(eventType)
        : [this.room.currentState.getStateEvents(eventType, stateKey)].filter((e): e is MatrixEvent => Boolean(e));
    return events.map((event) => JSON.stringify(this.rawEvent(event)));
  }

  /** The full (decrypted) event object the session's dispatch reads. */
  private rawEvent(event: MatrixEvent): Record<string, unknown> {
    const raw = event.event as Record<string, unknown>;
    return {
      ...raw,
      type: event.getType(),
      content: event.getContent(),
      sender: event.getSender(),
      event_id: event.getId(),
      room_id: event.getRoomId(),
      origin_server_ts: event.getTs(),
      state_key: event.getStateKey(),
    };
  }

  private async originOf(event: MatrixEvent): Promise<FfiEventOrigin> {
    if (!event.isEncrypted()) return new Origin.Cleartext();
    return new Origin.Encrypted({ senderDeviceId: await this.senderDeviceOf(event) });
  }

  /** The device that megolm-encrypted `event` (sender key → device list). */
  private async senderDeviceOf(event: MatrixEvent): Promise<string | undefined> {
    const senderKey = event.getSenderKey();
    const sender = event.getSender();
    const crypto = this.client.getCrypto();
    if (!senderKey || !sender || !crypto) return undefined;
    const cached = this.senderDeviceCache.get(senderKey);
    if (cached) return cached;
    const devices = await crypto.getUserDeviceInfo([sender], true);
    for (const device of devices.get(sender)?.values() ?? []) {
      if (device.getIdentityKey() === senderKey) {
        this.senderDeviceCache.set(senderKey, device.deviceId);
        return device.deviceId;
      }
    }
    return undefined;
  }

  // --- inbound sinks ----------------------------------------------------------

  subscribeRoomEvents(sink: RoomEventSinkInterface): void {
    const emit = async (event: MatrixEvent) => {
      await this.client.decryptEventIfNeeded(event);
      if (event.isDecryptionFailure()) {
        this.log(`event ${event.getId()} failed to decrypt; skipped`);
        return;
      }
      if (!sink.emit(JSON.stringify(this.rawEvent(event)), await this.originOf(event))) detach();
    };
    // Sticky events come from js-sdk's sticky store: sync delivers them in the
    // room's `msc4354_sticky` section (our own included — the timeline only
    // shows our own sends as local echoes, without the sticky metadata).
    const onSticky = (added: MatrixEvent[], updated: { current: MatrixEvent }[]) => {
      for (const event of [...added, ...updated.map((u) => u.current)]) void emit(event);
    };
    // Everything else (state events in the timeline) comes from the timeline;
    // local echoes and sticky events are skipped there.
    const onTimeline = (event: MatrixEvent, room: Room | undefined, toStartOfTimeline: boolean | undefined) => {
      if (room?.roomId !== this.roomId || toStartOfTimeline) return;
      if (event.status !== null || (event.event as { msc4354_sticky?: unknown }).msc4354_sticky) return;
      void emit(event);
    };
    const detach = () => {
      this.room.off(sdk.RoomStickyEventsEvent.Update, onSticky);
      this.client.off(sdk.RoomEvent.Timeline, onTimeline);
    };
    this.room.on(sdk.RoomStickyEventsEvent.Update, onSticky);
    this.client.on(sdk.RoomEvent.Timeline, onTimeline);
    this.detachers.push(detach);
  }

  subscribeStateUpdates(sink: StateUpdateSinkInterface): void {
    // Client-level listener: the room-level re-emit is unreliable under
    // MSC4222 `state_after` churn (see the previous host's notes).
    const onState = (event: MatrixEvent) => {
      if (event.getRoomId() !== this.roomId) return;
      if (!sink.emit([JSON.stringify(this.rawEvent(event))])) detach();
    };
    const detach = () => this.client.off(sdk.RoomStateEvent.Events, onState);
    this.client.on(sdk.RoomStateEvent.Events, onState);
    this.detachers.push(detach);
  }

  subscribeToDeviceEvents(sink: ToDeviceSinkInterface): void {
    const onToDevice = async ({ message, encryptionInfo }: sdk.ReceivedToDeviceMessage) => {
      const origin: FfiEventOrigin = encryptionInfo
        ? new Origin.Encrypted({ senderDeviceId: encryptionInfo.senderDevice })
        : new Origin.Cleartext();
      // MSC4153: is the sending device cross-signed by its owner?
      let crossSigned: boolean | undefined;
      const crypto = this.client.getCrypto();
      if (crypto && encryptionInfo?.sender && encryptionInfo.senderDevice) {
        const status = await crypto.getDeviceVerificationStatus(encryptionInfo.sender, encryptionInfo.senderDevice);
        crossSigned = status?.signedByOwner ?? false;
      }
      if (!sink.emit(message.type, message.sender, JSON.stringify(message.content ?? {}), origin, crossSigned)) detach();
    };
    const detach = () => this.client.off(sdk.ClientEvent.ReceivedToDeviceMessage, onToDevice);
    this.client.on(sdk.ClientEvent.ReceivedToDeviceMessage, onToDevice);
    this.detachers.push(detach);
  }
}

// ---------------------------------------------------------------------------
// Session bootstrap for the demo app and the backend test
// ---------------------------------------------------------------------------

export interface JsSdkBackend {
  driver: JsSdkMatrixDriver;
  client: MatrixClient;
  roomId: string;
  slotId: string;
  userId: string;
  deviceId: string;
  /** The MatrixRTC authorisation service (`livekit_service_url`). */
  lkServiceUrl: string;
  describe: string;
  stop: () => Promise<void>;
}

export interface JsSdkBackendOptions {
  homeserverUrl: string;
  /** Localpart or full user id; blank = register a throwaway user. */
  user?: string;
  password?: string;
  /** Join this room; blank = create a public room. */
  roomId?: string;
  slotId: string;
  /** Default: the demo backend's lk-jwt-service. */
  lkServiceUrl?: string;
  displayName?: string;
  /** Initialise rust-crypto (Olm to-device, cross-signing). Off by default. */
  crypto?: boolean;
  log?: Log;
}

/** Log in (or register), sync, make sure we are in a room — return a driver for it. */
export async function createJsSdkBackend(opts: JsSdkBackendOptions): Promise<JsSdkBackend> {
  const log = opts.log ?? (() => {});
  const homeserverUrl = opts.homeserverUrl.replace(/\/$/, "");
  let userId: string;
  let deviceId: string;
  let accessToken: string;
  const password = opts.password || `test-${Date.now().toString(16)}`;

  if (!opts.user) {
    const localpart = `rtc-${Date.now().toString(16)}${Math.floor(Math.random() * 0xffff).toString(16)}`;
    log(`registering ${localpart}`);
    const response = await fetch(`${homeserverUrl}/_matrix/client/v3/register`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username: localpart, password, auth: { type: "m.login.dummy" } }),
    });
    if (!response.ok) throw new Error(`registration failed: ${response.status} ${await response.text()}`);
    const body = (await response.json()) as { user_id: string; device_id: string; access_token: string };
    ({ user_id: userId, device_id: deviceId, access_token: accessToken } = body);
  } else {
    const bootstrap = sdk.createClient({ baseUrl: homeserverUrl });
    const localpart = opts.user.startsWith("@") ? opts.user.slice(1).split(":")[0] : opts.user;
    const login = await bootstrap.loginRequest({
      type: "m.login.password",
      identifier: { type: "m.id.user", user: localpart },
      password,
    });
    ({ user_id: userId, device_id: deviceId, access_token: accessToken } = login);
  }

  const client = sdk.createClient({ baseUrl: homeserverUrl, accessToken, userId, deviceId });
  if (opts.crypto) {
    // In-memory on purpose: every login here is a fresh device.
    await client.initRustCrypto({ useIndexedDB: false });
    log("bootstrapping cross-signing (MSC4153: peers drop keys from unsigned devices)");
    await client.getCrypto()!.bootstrapCrossSigning({
      setupNewCrossSigning: true,
      authUploadDeviceSigningKeys: async (makeRequest) => {
        await makeRequest({ type: "m.login.password", identifier: { type: "m.id.user", user: userId }, password });
      },
    });
  }
  if (opts.displayName) await client.setDisplayName(opts.displayName);

  client.startClient();
  await new Promise<void>((resolve, reject) => {
    client.once(sdk.ClientEvent.Sync, (state) => (state === "PREPARED" ? resolve() : reject(new Error(`sync failed: ${state}`))));
  });

  let roomId = opts.roomId;
  if (roomId) {
    if (!client.getRoom(roomId)) {
      await client.joinRoom(roomId);
      await waitForRoom(client, roomId);
    }
  } else {
    const created = await client.createRoom({ preset: sdk.Preset.PublicChat, name: `matrix-rtc demo ${new Date().toISOString()}` });
    roomId = created.room_id;
    await waitForRoom(client, roomId);
  }
  log(`ready as ${userId} (${deviceId}) in ${roomId}`);

  const driver = new JsSdkMatrixDriver(client, roomId, log);
  return {
    driver,
    client,
    roomId,
    slotId: opts.slotId,
    userId,
    deviceId,
    lkServiceUrl: opts.lkServiceUrl ?? "http://localhost:6080",
    describe: `${homeserverUrl} · ${userId} · room ${roomId}${opts.crypto ? " · crypto on" : " · no crypto"}`,
    stop: async () => {
      driver.detach();
      client.stopClient();
    },
  };
}

async function waitForRoom(client: MatrixClient, roomId: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (!client.getRoom(roomId)) {
    if (Date.now() > deadline) throw new Error(`room ${roomId} did not appear in sync`);
    await new Promise((r) => setTimeout(r, 50));
  }
}
