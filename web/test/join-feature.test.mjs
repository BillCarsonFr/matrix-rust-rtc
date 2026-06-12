import { describe, it, expect, vi, beforeEach } from 'vitest';
import { existsSync } from 'node:fs';

const nodeBindingUrl = new URL('../pkg/node/matrix_rtc_wasm.js', import.meta.url);

const ROOM_ID = '!test:example.org';
const SLOT_ID = 'm.call#TEST';
const USER_ID = '@alice:example.org';
const DEVICE_ID = 'device123';

// Helper function to get values from wasm-bindgen serialized content (Map, string, or object)
function getContentValue(content, key) {
  if (content instanceof Map) {
    return content.get(key);
  } else if (typeof content === 'string') {
    const obj = JSON.parse(content);
    return obj[key];
  } else {
    return content[key];
  }
}

// Create a mock Matrix client
function createMockMatrixClient() {
  const stickyEventsSent = [];
  const delayedEventsSent = [];
  const cancelledEvents = [];

  const client = {
    sendStickyEvent: vi.fn((roomId, eventType, content, callback) => {
      stickyEventsSent.push({ roomId, eventType, content });
      setTimeout(() => callback(), 0);
    }),
    sendDelayedEvent: vi.fn((roomId, eventType, content, delayMs, callback) => {
      const eventId = `delayed-event-${delayedEventsSent.length}`;
      delayedEventsSent.push({ roomId, eventType, content, delayMs, eventId });
      setTimeout(() => callback(null, eventId), 0);
    }),
    cancelDelayedEvent: vi.fn((roomId, eventId, callback) => {
      cancelledEvents.push({ roomId, eventId });
      setTimeout(() => callback(), 0);
    }),
    // Expose internal state for assertions
    _getStickyEvents: () => stickyEventsSent,
    _getDelayedEvents: () => delayedEventsSent,
    _getCancelledEvents: () => cancelledEvents,
    _clear: () => {
      stickyEventsSent.length = 0;
      delayedEventsSent.length = 0;
      cancelledEvents.length = 0;
    }
  };

  return client;
}

describe('WASM bindings with mock client', () => {
  let bindings;
  let mockClient;

  beforeEach(async () => {
    if (!existsSync(nodeBindingUrl)) {
      // Skip all tests if bindings haven't been built
      return;
    }

    // Import the bindings
    bindings = await import(nodeBindingUrl.href);

    // Create a fresh mock client for each test
    mockClient = createMockMatrixClient();
  });

  describe('JsCommandSender', () => {
    it('can be created with a client object', () => {
      const sender = new bindings.JsCommandSender(mockClient);
      expect(sender).toBeDefined();
    });
  });

  describe('WasmRtcSessionManager', () => {
    it('can setup command sender with mock client', () => {
      const manager = new bindings.WasmRtcSessionManager();
      
      expect(manager.has_command_sender()).toBe(false);
      manager.setup_command_sender(mockClient);
      expect(manager.has_command_sender()).toBe(true);
    });

    it('join() with valid params schedules events', async () => {
      const manager = new bindings.WasmRtcSessionManager();
      manager.setup_command_sender(mockClient);

      // Clear any previous events
      mockClient._clear();

      const joinParams = {
        user_id: USER_ID,
        device_id: DEVICE_ID,
        room_id: ROOM_ID,
        slot_id: SLOT_ID,
        application: 'm.call',
        transport: {
          type: 'livekit',
          livekit_service_url: 'https://example.com/livekit/jwt',
        },
      };

      // Join the session
      manager.join(joinParams);

      // Wait for the mock callbacks to fire
      await new Promise(resolve => setTimeout(resolve, 50));

      // Verify that events were sent
      const stickyEvents = mockClient._getStickyEvents();
      expect(stickyEvents.length).toBe(1);
      expect(stickyEvents[0].roomId).toBe(ROOM_ID);
      expect(stickyEvents[0].eventType).toBe('m.rtc.member');

      // Check the content
      const content = stickyEvents[0].content;
      expect(getContentValue(content, 'slot_id')).toBe(SLOT_ID);
      expect(getContentValue(content, 'sticky_key')).toBe(`${USER_ID}-${DEVICE_ID}`);

      // Verify delayed event was scheduled for keep-alive
      const delayedEvents = mockClient._getDelayedEvents();
      expect(delayedEvents.length).toBe(1);
      expect(delayedEvents[0].roomId).toBe(ROOM_ID);
      expect(delayedEvents[0].eventType).toBe('m.rtc.member');

      // Check delayed event content
      const delayedContent = delayedEvents[0].content;
      expect(getContentValue(delayedContent, 'disconnect_reason')).toBe('keep_alive_timeout');
    });

    it('leave() with disconnect reason works', async () => {
      const manager = new bindings.WasmRtcSessionManager();
      manager.setup_command_sender(mockClient);

      // Clear any previous events
      mockClient._clear();

      // First join the session
      const joinParams = {
        user_id: USER_ID,
        device_id: DEVICE_ID,
        room_id: ROOM_ID,
        slot_id: SLOT_ID,
        application: 'm.call',
        transport: {
          type: 'livekit',
          livekit_service_url: 'https://example.com/livekit/jwt',
        },
      };

      manager.join(joinParams);

      // Clear the events from join
      mockClient._clear();

      // Now leave
      const leaveParams = {
        disconnect_reason: 'user_left',
      };

      manager.leave(ROOM_ID, SLOT_ID, leaveParams);

      // Wait for the mock callbacks to fire
      await new Promise(resolve => setTimeout(resolve, 50));

      // Leave should send a sticky event with disconnect_reason
      const stickyEvents = mockClient._getStickyEvents();
      expect(stickyEvents.length).toBe(1);
      expect(stickyEvents[0].eventType).toBe('m.rtc.member');

      // Check the content
      const content = stickyEvents[0].content;
      expect(getContentValue(content, 'disconnect_reason')).toBe('user_left');

      // Should also cancel the delayed event
      const cancelledEvents = mockClient._getCancelledEvents();
      expect(cancelledEvents.length).toBe(1);
      expect(cancelledEvents[0].roomId).toBe(ROOM_ID);
      expect(cancelledEvents[0].eventId).toBe(`delayed-${ROOM_ID}-${USER_ID}-${DEVICE_ID}`);
    });
  });

  describe('WasmRtcSession', () => {
    it('can setup command sender with mock client', () => {
      const session = new bindings.WasmRtcSession();
      
      expect(session.has_command_sender()).toBe(false);
      session.setup_command_sender(mockClient);
      expect(session.has_command_sender()).toBe(true);
    });

    it('join() with valid params works', async () => {
      const session = new bindings.WasmRtcSession();
      session.setup_command_sender(mockClient);

      // Clear any previous events
      mockClient._clear();

      const joinParams = {
        user_id: USER_ID,
        device_id: DEVICE_ID,
        room_id: ROOM_ID,
        slot_id: SLOT_ID,
        application: 'm.call',
        transport: {
          type: 'livekit',
          livekit_service_url: 'https://example.com/livekit/jwt',
        },
      };

      session.join(joinParams);

      // Wait for the mock callbacks to fire
      await new Promise(resolve => setTimeout(resolve, 10));

      const stickyEvents = mockClient._getStickyEvents();
      expect(stickyEvents.length).toBe(1);

      // Check the content
      const content = stickyEvents[0].content;
      expect(getContentValue(content, 'slot_id')).toBe(SLOT_ID);
    });
  });

  describe('Error handling', () => {
    it('join with missing required params throws', () => {
      const manager = new bindings.WasmRtcSessionManager();
      manager.setup_command_sender(mockClient);

      // Missing user_id
      const invalidParams = {
        device_id: DEVICE_ID,
        room_id: ROOM_ID,
        slot_id: SLOT_ID,
        application: 'm.call',
        transport: {
          type: 'livekit',
          livekit_service_url: 'https://example.com/livekit/jwt',
        },
      };

      expect(() => manager.join(invalidParams)).toThrow(/invalid join params/);
    });
  });
});
