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

test('session membership snapshots include livekit transport data', async (t) => {
  if (!existsSync(nodeBindingUrl)) {
    t.skip('bindings have not been generated yet (run npm run build in web/)');
    return;
  }

  const { WasmRtcSession } = await import(nodeBindingUrl.href);
  const session = new WasmRtcSession();
  
  const subscription = session.subscribe_membership_snapshots();
  
  // Apply the initial event with transport data
  session.on_sticky_events_snapshot_received([bobJoinEvent()]);
  
  // Get the first snapshot
  let snapshot = subscription.next_snapshot();
  assert.notEqual(snapshot, null);
  
  // The snapshot is already an array of objects, not a JSON string
  const members = snapshot;
  assert.equal(members.length, 1);
  assert.equal(members[0].sender, '@bob:synapse.othersite.m.localhost');
  
  // Verify transports are present and contain livekit
  // The wasm serialization produces an array with the variant name as key
  assert.deepEqual(members[0].transports, [
    { LiveKit: { livekit_service_url: 'https://matrix-rtc.othersite.m.localhost/livekit/jwt' } }
  ]);
});

test('session handles unknown transport types as unsupported', async (t) => {
  if (!existsSync(nodeBindingUrl)) {
    t.skip('bindings have not been generated yet (run npm run build in web/)');
    return;
  }

  const { WasmRtcSession } = await import(nodeBindingUrl.href);
  const session = new WasmRtcSession();
  
  const subscription = session.subscribe_membership_snapshots();
  
  // Create an event with an unknown transport type
  const unknownTransportEvent = {
    room_id: ROOM_ID,
    sender: '@charlie:synapse.m.localhost',
    type: 'org.matrix.msc4143.rtc.member',
    content: {
      application: { type: 'm.call' },
      slot_id: SLOT_ID,
      rtc_transports: [
        {
          type: 'custom_transport',
          custom_url: 'https://custom.example.com',
        },
      ],
      member: {
        device_id: 'DEVICE123',
        user_id: '@charlie:synapse.m.localhost',
        id: 'charlie-device-id',
      },
      sticky_key: 'charlie-device-id',
    },
  };
  
  session.on_sticky_events_snapshot_received([unknownTransportEvent]);
  
  // Get the snapshot
  let snapshot = subscription.next_snapshot();
  assert.notEqual(snapshot, null);
  
  // The snapshot is already an array of objects
  const members = snapshot;
  assert.equal(members.length, 1);
  
  // Verify the unknown transport is preserved as Unsupported
  // Note: BTreeMap is serialized as a Map in JavaScript
  const transport = members[0].transports[0];
  assert.equal(transport.Unsupported.transport_type, 'custom_transport');
  assert.equal(transport.Unsupported.extra_fields.get('custom_url'), 'https://custom.example.com');
});

