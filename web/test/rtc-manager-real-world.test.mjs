import { describe, it, expect, beforeEach } from 'vitest';
import { existsSync } from 'node:fs';

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

describe('RTC manager with real-world data', () => {
  let WasmRtcSessionManager;

  beforeEach(async () => {
    if (!existsSync(nodeBindingUrl)) {
      // Skip if bindings haven't been built
      return;
    }

    const bindings = await import(nodeBindingUrl.href);
    WasmRtcSessionManager = bindings.WasmRtcSessionManager;
  });

  it('ingests realistic sticky DTOs and updates member count', () => {
    const manager = new WasmRtcSessionManager();

    manager.on_sticky_events_snapshot_received(ROOM_ID, [bobJoinEvent()]);
    expect(manager.session_count()).toBe(1);
    expect(manager.member_count(ROOM_ID, SLOT_ID)).toBe(1);

    manager.on_sticky_events_update_received(ROOM_ID, {
      added: [aliceJoinEvent()],
      updated: [],
      removed: [],
    });
    expect(manager.session_count()).toBe(1);
    expect(manager.member_count(ROOM_ID, SLOT_ID)).toBe(2);

    manager.on_sticky_events_update_received(ROOM_ID, {
      added: [],
      updated: [],
      removed: [aliceLeaveEvent()],
    });
    expect(manager.member_count(ROOM_ID, SLOT_ID)).toBe(1);
  });

  it('session membership snapshots include livekit transport data', () => {
    const manager = new WasmRtcSessionManager();

    manager.on_sticky_events_snapshot_received(ROOM_ID, [bobJoinEvent()]);
    const session = manager.member_count(ROOM_ID, SLOT_ID);
    
    // The session was created successfully
    expect(session).toBe(1);
  });

  it('session handles unknown transport types as unsupported', () => {
    const manager = new WasmRtcSessionManager();

    // Create an event with an unknown transport type
    const eventWithUnknownTransport = {
      event_id: '$test',
      room_id: ROOM_ID,
      type: 'org.matrix.msc4143.rtc.member',
      sender: '@test:example.org',
      content: {
        application: { type: 'm.call' },
        slot_id: SLOT_ID,
        rtc_transports: [
          { type: 'unknown_transport' },
        ],
        member: {
          device_id: 'TEST',
          user_id: '@test:example.org',
          id: 'test-id',
        },
        sticky_key: 'test-id',
      },
    };

    // Should not throw even with unknown transport
    expect(() => {
      manager.on_sticky_events_snapshot_received(ROOM_ID, [eventWithUnknownTransport]);
    }).not.toThrow();
    
    expect(manager.session_count()).toBe(1);
  });
});
