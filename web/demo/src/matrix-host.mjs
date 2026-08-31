// Copyright 2026 Valere Fedronic
//
// This file is part of matrix-rust-rtc.
//
// matrix-rust-rtc is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// matrix-rust-rtc is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

/**
 * The Matrix side of the web peer: matrix-js-sdk behind the wasm manager's
 * host contract.
 *
 * Two halves:
 * - the command-sender object the manager dispatches on (`commandSender()`):
 *   sticky sends (MSC4354), delayed events (MSC4140 — restart is the restart
 *   action, never cancel+resend), state events, and Olm-encrypted per-device
 *   to-device messages;
 * - sync feeding (`attachRoom()`): complete per-room snapshots pushed in the
 *   same order the native bridge uses — encryption, slots, members, then the
 *   sticky membership set (replace, not merge) — plus inbound
 *   `m.rtc.encryption_key` to-device messages with their Olm decryption
 *   metadata and MSC4153 cross-signing status.
 *
 * Every manager call goes through the shared ManagerOpQueue: the wasm object
 * allows one in-flight call at a time.
 */

import {
  ClientEvent,
  RoomStateEvent,
  RoomStickyEventsEvent,
  UpdateDelayedEventAction,
  createClient,
} from 'matrix-js-sdk';

const MEMBER_EVENT_TYPES = ['m.rtc.member', 'org.matrix.msc4143.rtc.member'];
const SLOT_EVENT_TYPES = ['m.rtc.slot', 'org.matrix.msc4143.rtc.slot'];
const KEY_MESSAGE_TYPES = ['m.rtc.encryption_key', 'org.matrix.msc4143.rtc.encryption_key'];

/**
 * Register a throwaway user (the dev homeserver has open registration) and
 * return a logged-in, crypto-ready client. `login` with existing credentials
 * skips registration.
 */
export async function createMatrixSession({
  homeserverUrl,
  user,
  password,
  displayName = 'Web Peer',
  log = () => {},
}) {
  let localpart = user;
  let pass = password;
  if (!localpart) {
    localpart = `web-${Date.now().toString(16)}${Math.floor(Math.random() * 0xffff).toString(16)}`;
    pass = `test-${localpart}`;
    log(`registering ${localpart}`);
    const response = await fetch(`${homeserverUrl}/_matrix/client/v3/register`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        username: localpart,
        password: pass,
        auth: { type: 'm.login.dummy' },
        inhibit_login: true,
      }),
    });
    if (!response.ok) {
      throw new Error(`registration failed: ${response.status} ${await response.text()}`);
    }
  }

  const bootstrap = createClient({ baseUrl: homeserverUrl });
  const login = await bootstrap.loginRequest({
    type: 'm.login.password',
    identifier: { type: 'm.id.user', user: localpart },
    password: pass,
  });

  const client = createClient({
    baseUrl: homeserverUrl,
    accessToken: login.access_token,
    userId: login.user_id,
    deviceId: login.device_id,
  });

  await client.initRustCrypto();
  // MSC4153: peers discard our media keys unless this device is cross-signed,
  // so bootstrap before any key can be exchanged — the same order the native
  // interop peer enforces. Uploading the signing keys needs UIA.
  log('bootstrapping cross-signing');
  await client.getCrypto().bootstrapCrossSigning({
    setupNewCrossSigning: true,
    authUploadDeviceSigningKeys: async (makeRequest) => {
      await makeRequest({
        type: 'm.login.password',
        identifier: { type: 'm.id.user', user: login.user_id },
        password: pass,
      });
    },
  });
  await client.setDisplayName(displayName);

  client.startClient();
  await new Promise((resolve, reject) => {
    client.once(ClientEvent.Sync, (state) =>
      state === 'PREPARED' ? resolve() : reject(new Error(`sync failed: ${state}`)),
    );
  });
  log(`ready as ${login.user_id} (${login.device_id})`);
  return { client, userId: login.user_id, deviceId: login.device_id, password: pass };
}

export class MatrixHost {
  /**
   * @param {object} options
   * @param {object} options.client - a crypto-ready, syncing MatrixClient.
   * @param {object} options.managerOps - the shared ManagerOpQueue.
   * @param {(line: string) => void} [options.log]
   */
  constructor({ client, managerOps, log = () => {} }) {
    this.client = client;
    this.managerOps = managerOps;
    this.log = log;
    /** curve25519 sender key -> device id, per megolm-attributed sender. */
    this.senderDeviceCache = new Map();
    this.detachers = [];
  }

  /** The object `setup_command_sender` takes. */
  commandSender() {
    const client = this.client;
    return {
      sendStickyEvent: (roomId, eventType, content, durationMs) =>
        // The manager chose the duration; pass it through verbatim.
        client._unstable_sendStickyEvent(roomId, durationMs, null, eventType, content),
      sendStateEvent: (roomId, eventType, stateKey, content) =>
        client.sendStateEvent(roomId, eventType, content, stateKey),
      sendDelayedEvent: async (roomId, eventType, content, delayMs) => {
        const response = await client._unstable_sendDelayedEvent(
          roomId,
          { delay: delayMs },
          null,
          eventType,
          content,
        );
        // The binding expects the bare MSC4140 delay id.
        return response.delay_id;
      },
      restartDelayedEvent: (_roomId, delayId) =>
        client._unstable_updateDelayedEvent(delayId, UpdateDelayedEventAction.Restart),
      cancelDelayedEvent: (_roomId, delayId) =>
        client._unstable_updateDelayedEvent(delayId, UpdateDelayedEventAction.Cancel),
      sendToDeviceMessage: async (recipients, messageType, content) => {
        // Olm-encrypted, per specific device — never `*`. Resolving with
        // nothing reports every recipient as served; a throw reports the
        // batch unattempted, and the core re-sends on the next rollout.
        await client.encryptAndSendToDevice(messageType, recipients, content);
      },
    };
  }

  /**
   * Start feeding one room into the manager and route its inbound key
   * messages. Resolves once the initial snapshot is fed.
   */
  async attachRoom(manager, roomId) {
    const room = this.client.getRoom(roomId);
    if (!room) throw new Error(`not joined to ${roomId}`);
    this.room = room;
    this.manager = manager;

    const refeed = () => this.scheduleFeed();
    room.on(RoomStickyEventsEvent.Update, refeed);
    room.on(RoomStateEvent.Events, refeed);
    room.on(RoomStateEvent.Members, refeed);
    this.detachers.push(() => {
      room.off(RoomStickyEventsEvent.Update, refeed);
      room.off(RoomStateEvent.Events, refeed);
      room.off(RoomStateEvent.Members, refeed);
    });

    const onToDevice = (payload) => this.onToDeviceMessage(payload);
    this.client.on(ClientEvent.ReceivedToDeviceMessage, onToDevice);
    this.detachers.push(() => this.client.off(ClientEvent.ReceivedToDeviceMessage, onToDevice));

    await this.feed();
  }

  detach() {
    for (const detach of this.detachers.splice(0)) detach();
  }

  /** Coalesce bursts of room updates into one feed at a time. */
  scheduleFeed() {
    if (this.feedPending) return;
    this.feedPending = true;
    queueMicrotask(() => {
      this.feedPending = false;
      this.feed().catch((error) => this.log(`feed failed: ${error}`));
    });
  }

  /**
   * Push the room's complete current state, in the native bridge's order:
   * encryption decides how slots resolve, slots decide whether members count,
   * and the sticky set replaces the membership wholesale.
   */
  async feed() {
    const { room, manager } = this;
    const roomId = room.roomId;

    const encrypted = Boolean(room.currentState.getStateEvents('m.room.encryption', ''));
    const slots = SLOT_EVENT_TYPES.flatMap((type) =>
      room.currentState.getStateEvents(type).map((ev) => ({
        slot_id: ev.getStateKey(),
        content: ev.getContent(),
      })),
    );
    const members = room.getJoinedMembers().map((member) => member.userId);
    const sticky = await this.stickySnapshot();

    await this.managerOps.enqueue(async () => {
      await manager.on_room_encryption_received(roomId, encrypted);
      await manager.on_room_slots_received(roomId, slots);
      await manager.on_room_members_received(roomId, members);
      await manager.set_current_sticky_state(roomId, sticky);
    });
  }

  /**
   * The room's active sticky membership events, decrypted, in the shape
   * `set_current_sticky_state` takes. The sticky store does not decrypt and
   * keys encrypted events under `m.room.encrypted`, so iterate everything,
   * decrypt, then filter by the decrypted type.
   */
  async stickySnapshot() {
    const events = [];
    for (const event of this.room._unstable_getStickyEvents()) {
      await this.client.decryptEventIfNeeded(event);
      if (event.isDecryptionFailure()) {
        this.log(`sticky event ${event.getId()} failed to decrypt; skipping`);
        continue;
      }
      const type = event.getType();
      if (!MEMBER_EVENT_TYPES.includes(type)) continue;

      const wasEncrypted = event.isEncrypted();
      events.push({
        room_id: this.room.roomId,
        sender: event.getSender(),
        sender_device_id: wasEncrypted ? await this.senderDeviceOf(event) : undefined,
        was_encrypted: wasEncrypted,
        type,
        content: event.getContent(),
      });
    }
    return events;
  }

  /**
   * The device that megolm-encrypted `event`: js-sdk exposes the sender's
   * curve25519 key, and the device list proves which device owns it.
   */
  async senderDeviceOf(event) {
    const senderKey = event.getSenderKey();
    const sender = event.getSender();
    if (!senderKey || !sender) return undefined;
    const cached = this.senderDeviceCache.get(senderKey);
    if (cached) return cached;

    const devices = await this.client.getCrypto().getUserDeviceInfo([sender], true);
    for (const device of devices.get(sender)?.values() ?? []) {
      if (device.getIdentityKey() === senderKey) {
        this.senderDeviceCache.set(senderKey, device.deviceId);
        return device.deviceId;
      }
    }
    this.log(`no device of ${sender} owns sender key ${senderKey}`);
    return undefined;
  }

  /** Route a decrypted `m.rtc.encryption_key` into the manager. */
  async onToDeviceMessage({ message, encryptionInfo }) {
    if (!KEY_MESSAGE_TYPES.includes(message.type)) return;
    const content = message.content ?? {};

    // MSC4153: the key is only accepted from a cross-signed device.
    let senderIsCrossSigned = false;
    if (encryptionInfo?.sender && encryptionInfo.senderDevice) {
      const status = await this.client
        .getCrypto()
        .getDeviceVerificationStatus(encryptionInfo.sender, encryptionInfo.senderDevice);
      senderIsCrossSigned = status?.signedByOwner ?? false;
    }

    this.managerOps
      .enqueue(() =>
        this.manager.receiveEncryptionKey({
          room_id: content.room_id,
          member_id: content.member_id,
          key_b64: content.media_key?.key,
          key_index: content.media_key?.index,
          was_encrypted: encryptionInfo !== null,
          sender_user_id: encryptionInfo?.sender,
          sender_device_id: encryptionInfo?.senderDevice,
          sender_is_cross_signed: senderIsCrossSigned,
        }),
      )
      .catch((error) => this.log(`encryption key rejected: ${error}`));
  }

  /** The `livekit_service_url` the homeserver advertises, from well-known. */
  async discoverFocus() {
    const response = await fetch(
      `${this.client.baseUrl.replace(/\/$/, '')}/.well-known/matrix/client`,
    );
    if (!response.ok) throw new Error(`well-known fetch failed: ${response.status}`);
    const wellKnown = await response.json();
    const foci = wellKnown['org.matrix.msc4143.rtc_foci'] ?? [];
    const livekit = foci.find((focus) => focus.type === 'livekit');
    if (!livekit?.livekit_service_url) {
      throw new Error('well-known advertises no livekit focus');
    }
    return livekit.livekit_service_url;
  }
}
