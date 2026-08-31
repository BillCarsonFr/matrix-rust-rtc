/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

/**
 * Compile-only consumer of the generated TypeScript declarations
 * (`pkg/browser/matrix_rtc_wasm.d.ts`). Never executed: the vitest suite runs
 * `tsc --noEmit` over it, so a declaration that stops parsing, an export that
 * disappears, or a signature that regresses to `any`-shaped nonsense fails the
 * build. The declarations are hand-written in the crate's `ts_types.rs`;
 * field-level drift against the serde structs still needs a human eye there.
 */

import type {
  JoinParamsIn,
  MatrixClientHost,
  MediaDelegate,
  MembershipSnapshot,
  RtcCallEvent,
  RtcParticipant,
  WasmMediaSession,
  WasmRtcSessionManager,
} from '../pkg/browser/matrix_rtc_wasm';

export async function smoke(
  manager: WasmRtcSessionManager,
  host: MatrixClientHost,
  delegate: MediaDelegate,
): Promise<void> {
  manager.setup_command_sender(host);

  const params: JoinParamsIn = {
    user_id: '@a:hs',
    device_id: 'DEV',
    room_id: '!r:hs',
    slot_id: 'm.call#ROOM',
    application: 'm.call',
    transport: { type: 'livekit', livekit_service_url: 'https://sfu' },
    element_call_compat: 'sticky_events',
  };
  const memberId: string = await manager.join(params);

  const session: WasmMediaSession = await manager.connectMedia(
    {
      room_id: '!r:hs',
      slot_id: 'm.call#ROOM',
      user_id: '@a:hs',
      device_id: 'DEV',
      livekit_service_url: 'https://sfu',
    },
    delegate,
  );

  const roster: RtcParticipant[] = session.participants();
  for (const participant of roster) {
    // Option fields serialize as null, not absence.
    const identity: string | null = participant.rtc_identity;
    void identity;
  }

  // The event union discriminates on `type`.
  const handle = (event: RtcCallEvent): string => {
    switch (event.type) {
      case 'key_imported':
        return `${event.member_id}@${event.key_index}`;
      case 'active_speakers':
        return event.speakers.map((speaker) => speaker.member_id).join(',');
      case 'ended':
        return event.reason;
      default:
        return event.type;
    }
  };
  void handle;
  void memberId;

  await manager.receiveEncryptionKey({
    room_id: '!r:hs',
    member_id: memberId,
    key_b64: 'AAAA',
    key_index: 0,
    was_encrypted: true,
    sender_user_id: '@a:hs',
    sender_device_id: 'DEV',
    sender_is_cross_signed: true,
  });

  await manager.setCurrentMembership(
    '!r:hs',
    [{ sender: '@a:hs', type: 'm.rtc.member', content: {} }],
    [{ sender: '@a:hs', state_key: '_a_DEV', origin_server_ts: 1, content: {} }],
  );

  const snapshots: MembershipSnapshot[] | null = null;
  void snapshots;
}
