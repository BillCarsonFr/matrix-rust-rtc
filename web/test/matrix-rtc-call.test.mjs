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

// MatrixRtcCall over a mocked livekit-client module and the real wasm
// bindings: the RoomEvent translation table, the roster join by
// rtc_identity, and the delegate wiring (token fetch, key provider, close).

import { describe, it, expect } from 'vitest';
import { existsSync } from 'node:fs';
import { MatrixRtcCall } from '../src/matrix-rtc-call.mjs';

const nodeBindingUrl = new URL('../pkg/node/matrix_rtc_wasm.js', import.meta.url);

const ROOM_ID = '!call:example.org';
const SLOT_ID = 'm.call#CALL';
const USER_ID = '@me:example.org';
const DEVICE_ID = 'MYDEVICE';
const OWN_FOCUS = 'https://rtc.example.org/livekit/jwt';

// --- a livekit-client stand-in -------------------------------------------

const RoomEvent = {
  ParticipantConnected: 'participantConnected',
  ParticipantDisconnected: 'participantDisconnected',
  TrackSubscribed: 'trackSubscribed',
  TrackUnsubscribed: 'trackUnsubscribed',
  LocalTrackPublished: 'localTrackPublished',
  LocalTrackUnpublished: 'localTrackUnpublished',
  TrackMuted: 'trackMuted',
  TrackUnmuted: 'trackUnmuted',
  ActiveSpeakersChanged: 'activeSpeakersChanged',
  Reconnecting: 'reconnecting',
  Reconnected: 'reconnected',
  Disconnected: 'disconnected',
};

class FakeParticipant {
  constructor(identity) {
    this.identity = identity;
  }
}

function fakeLivekit() {
  const instances = [];
  class FakeRoom {
    constructor(options) {
      this.options = options;
      this.handlers = new Map();
      this.remotes = new Map();
      this.localParticipant = new FakeParticipant('local-unset');
      this.disconnected = false;
      instances.push(this);
    }
    on(event, handler) {
      this.handlers.set(event, handler);
      return this;
    }
    async connect(url, token) {
      this.url = url;
      this.token = token;
    }
    async disconnect() {
      this.disconnected = true;
    }
    getParticipantByIdentity(identity) {
      return this.remotes.get(identity);
    }
    emit(event, ...args) {
      this.handlers.get(event)?.(...args);
    }
  }
  class BaseKeyProvider {
    constructor(options) {
      this.options = options;
      this.set = [];
    }
    onSetEncryptionKey(cryptoKey, identity, index) {
      this.set.push({ cryptoKey, identity, index });
    }
  }
  const createKeyMaterialFromBuffer = async (buffer) => ({ fakeKeyFor: buffer.byteLength });
  return {
    module: { Room: FakeRoom, RoomEvent, BaseKeyProvider, createKeyMaterialFromBuffer },
    instances,
  };
}

// --- fixtures shared with media-roster.test.mjs ---------------------------

function mockMatrixClient() {
  let counter = 0;
  return {
    sendStickyEvent: () => Promise.resolve({ event_id: `$sticky-${counter++}` }),
    sendDelayedEvent: () => Promise.resolve(`delay-${counter++}`),
    restartDelayedEvent: () => Promise.resolve(),
    cancelDelayedEvent: () => Promise.resolve(),
    sendStateEvent: () => Promise.resolve({ event_id: `$state-${counter++}` }),
  };
}

function memberEvent({ sender, deviceId, memberId }) {
  return {
    room_id: ROOM_ID,
    sender,
    sender_device_id: deviceId,
    was_encrypted: true,
    type: 'm.rtc.member',
    content: {
      slot_id: SLOT_ID,
      // The wire spelling (MSC4354 unstable id), as real events carry it.
      msc4354_sticky_key: memberId,
      application: { type: 'm.call' },
      member: { id: memberId, membership: 'join' },
      transports: {
        published: [{ type: 'livekit', livekit_service_url: OWN_FOCUS }],
        can_subscribe: ['livekit'],
      },
    },
  };
}

async function waitFor(probe, description, attempts = 100) {
  for (let i = 0; i < attempts; i++) {
    const value = probe();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${description}`);
}

describe('MatrixRtcCall over a mocked livekit-client', () => {
  it('joins roster entries to livekit participants and translates room events', async () => {
    if (!existsSync(nodeBindingUrl)) {
      console.warn('pkg/node not built; run `npm run build` first');
      return;
    }
    const bindings = await import(nodeBindingUrl);
    const manager = new bindings.WasmRtcSessionManager();
    manager.setup_command_sender(mockMatrixClient());

    const memberId = await manager.join({
      user_id: USER_ID,
      device_id: DEVICE_ID,
      room_id: ROOM_ID,
      slot_id: SLOT_ID,
      application: 'm.call',
      transport: { type: 'livekit', livekit_service_url: OWN_FOCUS },
    });
    await manager.set_current_sticky_state(ROOM_ID, [
      memberEvent({ sender: USER_ID, deviceId: DEVICE_ID, memberId }),
      memberEvent({
        sender: '@peer:example.org',
        deviceId: 'PEERDEVICE',
        memberId: 'peer-member-1',
      }),
    ]);

    const { module: livekit, instances } = fakeLivekit();
    const rosterUpdates = [];
    const call = new MatrixRtcCall({
      manager,
      bindings,
      livekit,
      getOpenIdToken: () =>
        Promise.resolve({
          access_token: 'opaque',
          token_type: 'Bearer',
          matrix_server_name: 'example.org',
          expires_in: 3600,
        }),
      fetchJson: () =>
        Promise.resolve({
          status: 200,
          body: JSON.stringify({ jwt: 'the-jwt', url: 'wss://sfu.example.org' }),
        }),
    });
    call.onParticipants = (roster) => rosterUpdates.push(roster);

    await call.connect({
      roomId: ROOM_ID,
      slotId: SLOT_ID,
      userId: USER_ID,
      deviceId: DEVICE_ID,
      livekitServiceUrl: OWN_FOCUS,
    });

    // One focus in play, so one Room, connected with what the token fetch
    // returned.
    expect(instances).toHaveLength(1);
    const room = instances[0];
    expect(room.url).toBe('wss://sfu.example.org');
    expect(room.token).toBe('the-jwt');

    // Roster entries join to livekit participants by rtc_identity once the
    // room knows them.
    const roster = await waitFor(
      () => (call.participants().length === 2 ? call.participants() : null),
      'a two-entry roster',
    );
    const peer = roster.find((p) => p.member_id === 'peer-member-1');
    expect(peer.livekitParticipant).toBeUndefined();

    const peerParticipant = new FakeParticipant(peer.rtc_identity);
    room.remotes.set(peer.rtc_identity, peerParticipant);
    room.emit(RoomEvent.ParticipantConnected, peerParticipant);
    expect(
      call.participants().find((p) => p.member_id === 'peer-member-1')
        .livekitParticipant,
    ).toBe(peerParticipant);

    // A subscribed track flows through the translation table into the roster
    // and out through onParticipants.
    room.emit(
      RoomEvent.TrackSubscribed,
      {},
      { source: 'camera' },
      peerParticipant,
    );
    await waitFor(() => {
      const latest = rosterUpdates.at(-1);
      const entry = latest?.find((p) => p.member_id === 'peer-member-1');
      return entry?.streams?.length === 1;
    }, 'the camera stream to reach onParticipants');
    const latest = rosterUpdates.at(-1).find((p) => p.member_id === 'peer-member-1');
    expect(latest.streams[0]).toEqual({ kind: 'camera', muted: false });
    expect(latest.livekitParticipant).toBe(peerParticipant);

    await call.disconnect();
    expect(room.disconnected).toBe(true);
  }, 20000);
});
