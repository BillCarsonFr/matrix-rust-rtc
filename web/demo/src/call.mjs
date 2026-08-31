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
 * The call: one wasm manager + one MatrixRtcCall over a joined slot, with
 * tiles for humans and events for the test driver.
 *
 * Everything protocol-shaped lives below this file (the wasm engine, the
 * MatrixHost); this is the app glue — join sequencing, publishing, tile
 * rendering, and the observable events the Playwright driver asserts on.
 */

import * as livekit from 'livekit-client';
import E2EEWorker from 'livekit-client/e2ee-worker?worker';
import init, * as bindings from 'matrix-rtc-wasm';
import { ManagerOpQueue, MatrixRtcCall } from 'matrix-rtc-wasm/call';

import { MatrixHost, createMatrixSession } from './matrix-host.mjs';
import {
  AUDIO_RMS_FLOOR,
  patternVideoTrack,
  rmsMeter,
  sampleVideoHalves,
  toneAudioTrack,
} from './tracks.mjs';

const DEFAULT_SLOT_ID = 'm.call#ROOM';

export class WebPeerApp {
  /**
   * @param {object} options
   * @param {(event: object) => void} options.emit - observable events (the
   *   test protocol; the page log in human mode).
   * @param {(line: string) => void} options.log
   * @param {HTMLElement} [options.tilesElement]
   */
  constructor({ emit, log, tilesElement }) {
    this.emit = emit;
    this.log = log;
    this.tilesElement = tilesElement;
    this.managerOps = new ManagerOpQueue();
    this.tiles = new Map();
    this.meters = [];
    this.ready = init().then(() => {
      bindings.initLogging('info', '');
    });
  }

  /** Log in (or register a throwaway user) and start syncing. */
  async login({ homeserver, user, password, displayName = 'Web Peer' }) {
    await this.ready;
    const session = await createMatrixSession({
      homeserverUrl: homeserver,
      user,
      password,
      displayName,
      log: this.log,
    });
    this.client = session.client;
    this.userId = session.userId;
    this.deviceId = session.deviceId;

    this.manager = new bindings.WasmRtcSessionManager();
    this.host = new MatrixHost({
      client: this.client,
      managerOps: this.managerOps,
      log: this.log,
    });
    this.manager.setup_command_sender(this.host.commandSender());

    this.emit({ event: 'ready', user_id: this.userId, device_id: this.deviceId });
    return { userId: this.userId, deviceId: this.deviceId };
  }

  /** Join a Matrix room we were invited to. */
  async joinRoom(roomId) {
    await this.client.joinRoom(roomId);
    // The room object needs a sync round-trip to be fully populated.
    await this.waitForRoom(roomId);
    this.emit({ event: 'room_joined', room_id: roomId });
  }

  /**
   * Create an encrypted room for the call and invite a peer (Phase B — the
   * shape mirrors the native interop peer's `create_encrypted_room`).
   */
  async createRoom({ name, invite }) {
    const response = await this.client.createRoom({
      name,
      invite: invite ? [invite] : [],
      initial_state: [
        {
          type: 'm.room.encryption',
          state_key: '',
          content: { algorithm: 'm.megolm.v1.aes-sha2' },
        },
      ],
      // The pre-sticky dialect writes membership as room state; PL 0 lets
      // every member do that (state_default is 50).
      power_level_content_override: {
        events: { 'org.matrix.msc3401.call.member': 0 },
      },
    });
    await this.waitForRoom(response.room_id);
    this.emit({ event: 'room_created', room_id: response.room_id, room_name: name });
    return response.room_id;
  }

  async waitForRoom(roomId, timeoutMs = 30_000) {
    const deadline = Date.now() + timeoutMs;
    while (this.client.getRoom(roomId)?.getMyMembership() !== 'join') {
      if (Date.now() > deadline) throw new Error(`room ${roomId} never appeared in sync`);
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }

  /**
   * Join the RTC slot and attach media: feed the room, publish the
   * membership, connect the SFU through the engine, publish tracks.
   */
  async join({ roomId, slotId = DEFAULT_SLOT_ID, compat, publish = {} }) {
    const focusUrl = await this.host.discoverFocus();
    this.roomId = roomId;
    this.slotId = slotId;
    this.log(`joining ${roomId} / ${slotId} on focus ${focusUrl}`);

    // Feed before joining: the manager must know the room's encryption state
    // (it decides how the membership counts) before it publishes ours.
    await this.host.attachRoom(this.manager, roomId);

    const memberId = await this.managerOps.enqueue(() =>
      this.manager.join({
        user_id: this.userId,
        device_id: this.deviceId,
        room_id: roomId,
        slot_id: slotId,
        application: 'm.call',
        transport: { type: 'livekit', livekit_service_url: focusUrl },
        element_call_compat: compat === 'off' ? undefined : compat,
      }),
    );
    this.memberId = memberId;

    this.call = new MatrixRtcCall({
      manager: this.manager,
      bindings,
      livekit,
      managerOps: this.managerOps,
      getOpenIdToken: () => this.client.getOpenIdToken(),
      roomOptions: { e2ee: { worker: new E2EEWorker() } },
    });
    this.call.onParticipants = (roster) => this.onRoster(roster);
    this.call.onEvent = (event) => this.onCallEvent(event);
    this.call.onRoomConnected = (room, key) => this.onRoomConnected(room, key);

    await this.call.connect({
      roomId,
      slotId,
      userId: this.userId,
      deviceId: this.deviceId,
      livekitServiceUrl: focusUrl,
      elementCallCompat: compat === 'off' ? undefined : compat,
    });

    await this.publishMedia(focusUrl, publish);
    this.emit({ event: 'joined', member_id: memberId, identity: this.call.session.ownRtcIdentity() });
  }

  /** Publish into the own-focus room: synthetic tracks, or the real devices. */
  async publishMedia(focusUrl, { pattern, tone, devices } = {}) {
    const room = this.call.rooms.get(focusUrl);
    if (!room) throw new Error('own-focus room missing after connect');
    if (devices) {
      await room.localParticipant.enableCameraAndMicrophone();
      return;
    }
    if (pattern) {
      await room.localParticipant.publishTrack(patternVideoTrack(), {
        source: livekit.Track.Source.Camera,
      });
    }
    if (tone) {
      await room.localParticipant.publishTrack(toneAudioTrack(), {
        source: livekit.Track.Source.Microphone,
      });
    }
  }

  /** Leave the slot and tear the media session down. */
  async leave() {
    for (const meter of this.meters.splice(0)) meter.stop();
    if (this.call) {
      await this.call.disconnect();
      this.call = null;
    }
    if (this.roomId) {
      await this.managerOps.enqueue(() => this.manager.leave(this.roomId, this.slotId, {}));
      this.host.detach();
    }
    this.emit({ event: 'left' });
  }

  // --- observation ---------------------------------------------------------

  onRoster(roster) {
    this.emit({ event: 'members', count: roster.length });
    this.renderTiles(roster);
  }

  onCallEvent(event) {
    if (event.type === 'key_imported') {
      const entry = this.call
        .participants()
        .find((participant) => participant.member_id === event.member_id);
      this.emit({
        event: 'key_imported',
        member_id: event.member_id,
        identity: entry?.rtc_identity,
        key_index: event.key_index,
      });
      return;
    }
    this.emit({ event: 'call_event', ...event });
  }

  /** Per-room media observation: attach, then meter what arrives. */
  onRoomConnected(room) {
    room.on(livekit.RoomEvent.TrackSubscribed, (track, publication, participant) => {
      this.emit({
        event: 'track_subscribed',
        identity: participant.identity,
        kind: track.kind,
      });
      const element = this.attachTrack(track, participant.identity);
      if (track.kind === 'audio') this.startAudioMeter(track, participant.identity);
      if (track.kind === 'video' && element) this.startVideoMeter(element, participant.identity);
    });
  }

  startAudioMeter(track, identity) {
    const meter = rmsMeter(track.mediaStreamTrack);
    const timer = setInterval(() => {
      const value = meter.read();
      if (value > 0) {
        this.emit({ event: 'audio_rms', identity, value, floor: AUDIO_RMS_FLOOR });
      }
    }, 1000);
    this.meters.push({ stop: () => (clearInterval(timer), meter.stop()) });
  }

  startVideoMeter(video, identity) {
    const timer = setInterval(() => {
      const halves = sampleVideoHalves(video);
      if (halves) {
        this.emit({ event: 'video_pattern', identity, ...halves });
      }
    }, 1000);
    this.meters.push({ stop: () => clearInterval(timer) });
  }

  // --- tiles (the human half; harmless under test) --------------------------

  renderTiles(roster) {
    if (!this.tilesElement) return;
    const seen = new Set();
    for (const participant of roster) {
      seen.add(participant.member_id);
      let tile = this.tiles.get(participant.member_id);
      if (!tile) {
        tile = document.createElement('div');
        tile.className = 'tile';
        tile.innerHTML =
          '<video autoplay playsinline muted></video><div class="who"></div><div class="meta"></div>';
        this.tilesElement.append(tile);
        this.tiles.set(participant.member_id, tile);
      }
      tile.querySelector('.who').textContent =
        `${participant.user_id}${participant.is_local ? ' (you)' : ''}`;
      tile.querySelector('.meta').textContent =
        `${participant.member_id} · ${participant.streams
          .map((stream) => `${stream.kind}${stream.muted ? ' (muted)' : ''}`)
          .join(', ') || 'no streams'}`;
      tile.dataset.identity = participant.rtc_identity ?? '';
    }
    for (const [memberId, tile] of this.tiles) {
      if (!seen.has(memberId)) {
        tile.remove();
        this.tiles.delete(memberId);
      }
    }
  }

  /** Attach a subscribed track to its member's tile (by identity). */
  attachTrack(track, identity) {
    if (!this.tilesElement) return null;
    for (const tile of this.tiles.values()) {
      if (tile.dataset.identity === identity) {
        const video = tile.querySelector('video');
        if (track.kind === 'video') {
          track.attach(video);
          return video;
        }
        // Audio: attach to a detached element so it plays (muted <video> won't).
        const audio = track.attach();
        tile.append(audio);
        return audio;
      }
    }
    // No tile yet (membership still propagating): attach off-DOM so meters work.
    const element = track.attach();
    element.muted = track.kind === 'video';
    return element;
  }
}
