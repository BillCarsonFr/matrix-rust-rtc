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

// The media roster end-to-end, without livekit: a fake transport delegate
// stands in for livekit-js, so what is under test is everything Rust owns —
// join, membership reconciliation, the multi-focus connection pool, the
// MSC4195 identities on roster entries, and stream state fed through the
// connection-event sink.

import { describe, it, expect } from 'vitest';
import { existsSync } from 'node:fs';

const nodeBindingUrl = new URL('../pkg/node/matrix_rtc_wasm.js', import.meta.url);

const ROOM_ID = '!call:example.org';
const SLOT_ID = 'm.call#CALL';
const USER_ID = '@me:example.org';
const DEVICE_ID = 'MYDEVICE';
const OWN_FOCUS = 'https://rtc.example.org/livekit/jwt';
const PEER_FOCUS = 'https://rtc.othersite.org/livekit/jwt';

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

function memberEvent({ sender, deviceId, memberId, focus }) {
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
        published: [{ type: 'livekit', livekit_service_url: focus }],
        can_subscribe: ['livekit'],
      },
    },
  };
}

// Stands in for the livekit-js half: resolves token fetches with a canned SFU
// token, records connects and hands their sinks back to the test, and counts
// closes and local key-index switches.
function fakeDelegate() {
  const log = {
    tokenRequests: [],
    connects: [],
    sinks: [],
    keysSet: [],
    localKeyIndexes: [],
    events: [],
    rosters: [],
    closed: 0,
  };
  const delegate = {
    getOpenIdToken: () =>
      Promise.resolve({
        access_token: 'opaque',
        token_type: 'Bearer',
        matrix_server_name: 'example.org',
        expires_in: 3600,
      }),
    fetchJson: (url, body) => {
      log.tokenRequests.push({ url, body });
      return Promise.resolve({
        status: 200,
        body: JSON.stringify({ jwt: 'the-jwt', url: 'wss://sfu.example.org' }),
      });
    },
    connect: (request, sink) => {
      log.connects.push(request);
      log.sinks.push(sink);
      return Promise.resolve({
        close: () => {
          log.closed += 1;
          return Promise.resolve();
        },
      });
    },
    setKey: (identity, index, key) => {
      log.keysSet.push({ identity, index, length: key.length });
      return Promise.resolve(true);
    },
    setLocalKeyIndex: (index) => {
      log.localKeyIndexes.push(index);
    },
    onEvent: (event) => {
      log.events.push(event);
    },
    onParticipants: (roster) => {
      log.rosters.push(roster);
    },
  };
  return { delegate, log };
}

// The engine's actor runs on the microtask queue; poll until it has caught up.
async function waitFor(probe, description, attempts = 100) {
  for (let i = 0; i < attempts; i++) {
    const value = probe();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${description}`);
}

describe('web media roster over a fake transport delegate', () => {
  it('reconciles memberships, foci, and sink events into the roster', async () => {
    if (!existsSync(nodeBindingUrl)) {
      console.warn('pkg/node not built; run `npm run build:node` first');
      return;
    }
    const bindings = await import(nodeBindingUrl);
    const manager = new bindings.WasmRtcSessionManager();
    manager.setup_command_sender(mockMatrixClient());

    // Join, then feed the sticky state back as a server would echo it:
    // ourselves, plus one peer on our focus and one on a second focus.
    const memberId = await manager.join({
      user_id: USER_ID,
      device_id: DEVICE_ID,
      room_id: ROOM_ID,
      slot_id: SLOT_ID,
      application: 'm.call',
      transport: { type: 'livekit', livekit_service_url: OWN_FOCUS },
    });
    await manager.set_current_sticky_state(ROOM_ID, [
      memberEvent({ sender: USER_ID, deviceId: DEVICE_ID, memberId, focus: OWN_FOCUS }),
      memberEvent({
        sender: '@peer:example.org',
        deviceId: 'PEERDEVICE',
        memberId: 'peer-member-1',
        focus: OWN_FOCUS,
      }),
      memberEvent({
        sender: '@far:othersite.org',
        deviceId: 'FARDEVICE',
        memberId: 'far-member-1',
        focus: PEER_FOCUS,
      }),
    ]);

    const { delegate, log } = fakeDelegate();
    const session = await manager.connectMedia(
      {
        room_id: ROOM_ID,
        slot_id: SLOT_ID,
        user_id: USER_ID,
        device_id: DEVICE_ID,
        livekit_service_url: OWN_FOCUS,
      },
      delegate,
    );

    // The own focus was connected through the token exchange Rust built and
    // the delegate fetched. (No ordering with the pooled connect below: the
    // engine starts reconciling the moment it exists.)
    const ownConnect = log.connects.find((c) => c.connectionKey === OWN_FOCUS);
    expect(ownConnect).toEqual({
      connectionKey: OWN_FOCUS,
      sfuUrl: 'wss://sfu.example.org',
      jwt: 'the-jwt',
    });
    const ownTokenRequest = log.tokenRequests.find(
      (r) => r.url === `${OWN_FOCUS}/get_token`,
    );
    expect(ownTokenRequest.body.member.id).toBe(memberId);

    // The second focus is the engine's to open: one pooled connection for the
    // far member, keyed by its livekit_service_url.
    await waitFor(
      () => log.connects.some((c) => c.connectionKey === PEER_FOCUS),
      'the peer-focus connection',
    );
    expect(log.connects.length).toBe(2);

    // Roster: signalling truth, each entry carrying its MSC4195 identity.
    const roster = await waitFor(() => {
      const participants = session.participants();
      return participants.length === 3 ? participants : null;
    }, 'a three-entry roster');
    const me = roster.find((p) => p.member_id === memberId);
    const peer = roster.find((p) => p.member_id === 'peer-member-1');
    expect(me.is_local).toBe(true);
    expect(me.rtc_identity).toBe(session.ownRtcIdentity());
    expect(peer.is_local).toBe(false);
    expect(peer.reachable).toBe(true);
    // base64(SHA256(...)), unpadded: 43 chars.
    expect(peer.rtc_identity).toHaveLength(43);

    // Media arriving through the sink attaches to the signalled entry.
    const sink = log.sinks[0];
    sink.trackAdded(peer.rtc_identity, 'camera');
    let updated = await waitFor(() => {
      const participants = session.participants();
      const entry = participants.find((p) => p.member_id === 'peer-member-1');
      return entry.streams.length === 1 ? entry : null;
    }, 'the camera stream on the peer');
    expect(updated.streams[0]).toEqual({ kind: 'camera', muted: false });

    sink.trackMuted(peer.rtc_identity, 'camera', true);
    updated = await waitFor(() => {
      const participants = session.participants();
      const entry = participants.find((p) => p.member_id === 'peer-member-1');
      return entry.streams[0].muted ? entry : null;
    }, 'the muted camera stream');

    // The event callback reports the same transitions, and the roster
    // callback carried the same truth.
    const streamEvents = await waitFor(() => {
      const seen = log.events.filter(
        (event) => event.type === 'stream_started' || event.type === 'stream_muted',
      );
      return seen.length >= 2 ? seen : null;
    }, 'the stream events to reach onEvent');
    expect(streamEvents[0]).toEqual({
      type: 'stream_started',
      member_id: 'peer-member-1',
      kind: 'camera',
    });
    expect(streamEvents[1].type).toBe('stream_muted');
    expect(log.rosters.length).toBeGreaterThan(0);

    // A transport-level leave is diagnostics, never roster truth.
    sink.remoteLeft(peer.rtc_identity);
    expect(session.participants().length).toBe(3);

    // Disconnect closes the own focus through the delegate; pooled peer-focus
    // connections close inside the engine.
    await session.disconnect();
    expect(log.closed).toBeGreaterThanOrEqual(1);
  }, 20000);
});
