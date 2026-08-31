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
 * The call model for web apps: the wasm session manager's roster and
 * connection lifecycle, joined with livekit-js participants.
 *
 * This is the thin JS half of the web media session. Rust owns the protocol
 * (membership reconciliation, the multi-focus pool, MSC4195 identities, token
 * request shapes, key bookkeeping); this wrapper owns what only JS can:
 * `fetch`, the livekit-js `Room`, and the RoomEvent-to-sink translation. Every
 * roster entry is a plain object from Rust plus `livekitParticipant` — the
 * live livekit-js participant, when the room knows the entry's `rtc_identity`.
 *
 * `livekit-client` is not imported here: the app passes its module (or a
 * compatible mock) to the constructor, keeping it a peer dependency and this
 * file testable without it.
 */

/** livekit-js `Track.Source` values that map onto roster stream kinds. */
const KNOWN_KINDS = new Set([
  'microphone',
  'camera',
  'screen_share',
  'screen_share_audio',
]);

export class MatrixRtcCall {
  /**
   * @param {object} options
   * @param {object} options.manager - a `WasmRtcSessionManager` whose command
   *   sender is set up and which is being fed sticky events / room state.
   * @param {object} options.bindings - the wasm module (for
   *   `HEARTBEAT_INTERVAL_MS`).
   * @param {object} options.livekit - the `livekit-client` module (peer
   *   dependency): `Room`, `RoomEvent`, `ExternalE2EEKeyProvider` are used.
   * @param {() => Promise<object>} options.getOpenIdToken - resolves with a
   *   Matrix OpenID token (`matrix-js-sdk`: `client.getOpenIdToken()`).
   * @param {(url: string, body: object) => Promise<{status: number, body: string}>}
   *   [options.fetchJson] - the token POST; defaults to global `fetch`.
   * @param {object} [options.roomOptions] - extra livekit-js `Room` options,
   *   merged under the E2EE ones (pass `e2ee.worker` here to enable frame
   *   encryption).
   */
  constructor({ manager, bindings, livekit, getOpenIdToken, fetchJson, roomOptions }) {
    this.manager = manager;
    this.bindings = bindings;
    this.livekit = livekit;
    this.getOpenIdToken = getOpenIdToken;
    this.fetchJson = fetchJson ?? defaultFetchJson;
    this.roomOptions = roomOptions ?? {};
    /** connectionKey -> Room */
    this.rooms = new Map();
    this.keyProvider = null;
    this.session = null;
    this.heartbeatTimer = null;
    /**
     * Serializes every manager mutation: the wasm object allows one in-flight
     * call at a time, and both the heartbeat and the rotation flush await the
     * app's Matrix client mid-call.
     */
    this.managerQueue = Promise.resolve();
    /** @type {(participants: object[]) => void} */
    this.onParticipants = () => {};
    /** @type {(event: object) => void} */
    this.onEvent = () => {};
  }

  /**
   * Attach media to a slot this manager has already joined. Resolves once the
   * own-focus room is connected; roster changes then arrive via
   * `onParticipants` and call events via `onEvent`.
   *
   * @param {object} config - `{ roomId, slotId, userId, deviceId,
   *   livekitServiceUrl, keyRingSize?, elementCallCompat? }`
   */
  async connect(config) {
    if (this.session) throw new Error('already connected');
    this.config = config;
    this.keyProvider = new this.livekit.ExternalE2EEKeyProvider();

    this.session = await this.manager.connectMedia(
      {
        room_id: config.roomId,
        slot_id: config.slotId,
        user_id: config.userId,
        device_id: config.deviceId,
        livekit_service_url: config.livekitServiceUrl,
        key_ring_size: config.keyRingSize,
        element_call_compat: config.elementCallCompat,
      },
      this.delegate(),
    );

    // The page owns the keep-alive clock.
    const interval = this.bindings.HEARTBEAT_INTERVAL_MS();
    this.heartbeatTimer = setInterval(() => {
      this.enqueueManagerOp(() => this.manager.heartbeat(config.roomId, config.slotId));
    }, interval);

    return this.participants();
  }

  /** Chain a manager call so at most one is in flight (see `managerQueue`). */
  enqueueManagerOp(op) {
    this.managerQueue = this.managerQueue.then(op, () => {}).catch((error) => {
      console.warn('matrix-rtc: manager call failed:', error);
    });
    return this.managerQueue;
  }

  /** The current roster, each entry with its `livekitParticipant` when live. */
  participants() {
    return this.withLivekitParticipants(this.session.participants());
  }

  /** Close every room, stop the timers, and shut the session down. */
  async disconnect() {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.session) {
      // Closes peer-focus rooms via the engine and the own focus through the
      // delegate's handle.
      await this.session.disconnect();
      this.session = null;
    }
    this.rooms.clear();
  }

  /** The delegate the wasm transport drives (see the transport module docs). */
  delegate() {
    const call = this;
    return {
      getOpenIdToken: () => call.getOpenIdToken(),
      fetchJson: (url, body) => call.fetchJson(url, body),
      connect: (request, sink) => call.connectRoom(request, sink),
      setKey: (identity, index, key) =>
        // livekit-js's provider: material, participant identity, key index.
        Promise.resolve(call.keyProvider.setKey(key, identity, index)).then(
          (accepted) => accepted ?? true,
        ),
      // livekit-js moves the local sender with `setKey` for the local
      // participant; nothing further to do. Kept as a seam for versions (or
      // providers) where the sender index is a separate call.
      setLocalKeyIndex: () => {},
      // The push half: roster changes, call events, and the moment a key's
      // delayBeforeUse window closes (when a coalesced rotation falls due).
      onParticipants: (roster) =>
        call.onParticipants(call.withLivekitParticipants(roster)),
      onEvent: (event) => call.onEvent(event),
      onSwitchComplete: () =>
        call.enqueueManagerOp(() =>
          call.manager.flushDueKeyRotation(call.config.roomId, call.config.slotId),
        ),
    };
  }

  /**
   * One livekit-js room per focus: connect it, register the RoomEvent
   * translation onto the sink, and hand back the close handle.
   */
  async connectRoom(request, sink) {
    const room = new this.livekit.Room({
      ...this.roomOptions,
      e2ee: this.roomOptions.e2ee
        ? { keyProvider: this.keyProvider, ...this.roomOptions.e2ee }
        : undefined,
    });
    this.registerSink(room, sink, request.connectionKey);
    await room.connect(request.sfuUrl, request.jwt);
    this.rooms.set(request.connectionKey, room);
    return {
      close: async () => {
        this.rooms.delete(request.connectionKey);
        await room.disconnect();
      },
    };
  }

  /** The RoomEvent → sink translation table. */
  registerSink(room, sink, connectionKey) {
    const { RoomEvent } = this.livekit;
    const kindOf = (publication) =>
      KNOWN_KINDS.has(publication?.source) ? publication.source : null;

    room.on(RoomEvent.ParticipantConnected, (participant) =>
      sink.remoteJoined(participant.identity),
    );
    room.on(RoomEvent.ParticipantDisconnected, (participant) =>
      sink.remoteLeft(participant.identity),
    );
    room.on(RoomEvent.TrackSubscribed, (_track, publication, participant) => {
      const kind = kindOf(publication);
      if (kind) sink.trackAdded(participant.identity, kind);
    });
    room.on(RoomEvent.TrackUnsubscribed, (_track, publication, participant) => {
      const kind = kindOf(publication);
      if (kind) sink.trackRemoved(participant.identity, kind);
    });
    // Local publications too, so our own roster entry carries our streams.
    room.on(RoomEvent.LocalTrackPublished, (publication) => {
      const kind = kindOf(publication);
      if (kind) sink.trackAdded(room.localParticipant.identity, kind);
    });
    room.on(RoomEvent.LocalTrackUnpublished, (publication) => {
      const kind = kindOf(publication);
      if (kind) sink.trackRemoved(room.localParticipant.identity, kind);
    });
    room.on(RoomEvent.TrackMuted, (publication, participant) => {
      const kind = kindOf(publication);
      if (kind) sink.trackMuted(participant.identity, kind, true);
    });
    room.on(RoomEvent.TrackUnmuted, (publication, participant) => {
      const kind = kindOf(publication);
      if (kind) sink.trackMuted(participant.identity, kind, false);
    });
    room.on(RoomEvent.ActiveSpeakersChanged, (speakers) =>
      sink.activeSpeakers(
        speakers.map((speaker) => ({
          identity: speaker.identity,
          level: speaker.audioLevel ?? 0,
        })),
      ),
    );
    room.on(RoomEvent.Reconnecting, () => sink.reconnecting());
    room.on(RoomEvent.Reconnected, () => sink.reconnected());
    room.on(RoomEvent.Disconnected, (reason) => {
      this.rooms.delete(connectionKey);
      sink.closed(String(reason ?? 'disconnected'));
      sink.free?.();
    });
  }

  /** Join roster entries to live livekit-js participants by rtc_identity. */
  withLivekitParticipants(roster) {
    return roster.map((entry) => ({
      ...entry,
      livekitParticipant: entry.rtc_identity
        ? this.findParticipant(entry.rtc_identity)
        : undefined,
    }));
  }

  findParticipant(identity) {
    for (const room of this.rooms.values()) {
      if (room.localParticipant?.identity === identity) {
        return room.localParticipant;
      }
      const participant = room.getParticipantByIdentity?.(identity);
      if (participant) return participant;
    }
    return undefined;
  }
}

async function defaultFetchJson(url, body) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: await response.text() };
}
