import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { test } from 'node:test';

const nodeBindingUrl = new URL('../pkg/node/matrix_rtc_wasm.js', import.meta.url);

const ROOM_ID = '!RhkzuEOlOxpckXJkhY:synapse.m.localhost';
const SLOT_ID = 'm.call#ROOM';

function bobJoinEvent() {
  return {
    event_id: '$_ErcrEWx3Hj77_wScF-U4e9aS6cVi37RvFUeq12BiaI',
    room_id: ROOM_ID,
    type: 'org.matrix.msc4143.rtc.member',
    sender: '@bob:synapse.othersite.m.localhost',
    content: {
      application: {
        type: 'm.call',
        'm.call.intent': 'video',
      },
      slot_id: SLOT_ID,
      rtc_transports: [
        {
          type: 'livekit',
          livekit_service_url: 'https://matrix-rtc.othersite.m.localhost/livekit/jwt',
        },
      ],
      member: {
        device_id: 'WDQHAPEYDK',
        user_id: '@bob:synapse.othersite.m.localhost',
        id: 'bcab799f-abae-4d38-bf1b-77238346349a',
      },
      versions: [],
      msc4354_sticky_key: 'bcab799f-abae-4d38-bf1b-77238346349a',
      sticky_key: 'bcab799f-abae-4d38-bf1b-77238346349a',
    },
  };
}

function aliceJoinEvent() {
  return {
    event_id: '$imqekRtWGLcITMI6YuMF0xgpT4S8LMr78eseonO2_Nw',
    room_id: ROOM_ID,
    type: 'org.matrix.msc4143.rtc.member',
    sender: '@alice:synapse.m.localhost',
    content: {
      application: {
        type: 'm.call',
        'm.call.intent': 'video',
      },
      slot_id: SLOT_ID,
      rtc_transports: [
        {
          type: 'livekit',
          livekit_service_url: 'https://matrix-rtc.m.localhost/livekit/jwt',
        },
      ],
      member: {
        device_id: 'VJHNJJCVOA',
        user_id: '@alice:synapse.m.localhost',
        id: 'd50437bd-424a-498d-912f-b0f1d2ba7f18',
      },
      versions: [],
      msc4354_sticky_key: 'd50437bd-424a-498d-912f-b0f1d2ba7f18',
      sticky_key: 'd50437bd-424a-498d-912f-b0f1d2ba7f18',
    },
  };
}

function aliceLeaveEvent() {
  return {
    event_id: '$4cN54YJqNgWjtv1g0U1kx_bRhu_kfGV5_6qIAzxUXMA',
    room_id: ROOM_ID,
    type: 'org.matrix.msc4143.rtc.member',
    sender: '@alice:synapse.m.localhost',
    content: {
      slot_id: SLOT_ID,
      msc4354_sticky_key: 'd50437bd-424a-498d-912f-b0f1d2ba7f18',
      sticky_key: 'd50437bd-424a-498d-912f-b0f1d2ba7f18',
    },
  };
}

test('manager ingests realistic sticky DTOs and updates member count', async (t) => {
  if (!existsSync(nodeBindingUrl)) {
    t.skip('bindings have not been generated yet (run npm run build in web/)');
    return;
  }

  const { WasmRtcSessionManager } = await import(nodeBindingUrl.href);
  const manager = new WasmRtcSessionManager();

  manager.on_sticky_events_snapshot_received(ROOM_ID, [bobJoinEvent()]);
  assert.equal(manager.session_count(), 1);
  assert.equal(manager.member_count(ROOM_ID, SLOT_ID), 1);

  manager.on_sticky_events_update_received(ROOM_ID, {
    added: [aliceJoinEvent()],
    updated: [],
    removed: [],
  });
  assert.equal(manager.session_count(), 1);
  assert.equal(manager.member_count(ROOM_ID, SLOT_ID), 2);

  manager.on_sticky_events_update_received(ROOM_ID, {
    added: [],
    updated: [],
    removed: [aliceLeaveEvent()],
  });
  assert.equal(manager.member_count(ROOM_ID, SLOT_ID), 1);
});

