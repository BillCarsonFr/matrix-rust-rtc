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
 * The test-driver surface: the web analogue of the rust interop peer's stdio
 * protocol (`interop_peer.rs`), for Playwright's `WebPeer` helper.
 *
 * In: `window.webPeer.command(jsonString)` — `{cmd: "login"|"create_room"|
 * "join_room"|"join"|"leave", ...payload}`. Each command resolves after its
 * work completes; failures surface both as a rejected promise and an `error`
 * event.
 *
 * Out: every observable event goes to `window.__onWebPeerEvent(jsonString)`
 * (a Playwright `exposeFunction`), and to the on-page log for humans.
 */

export function installPeerApi(app, log) {
  const emitToDriver = (event) => {
    const line = JSON.stringify(event);
    log(line);
    window.__onWebPeerEvent?.(line);
  };
  app.emit = emitToDriver;

  window.webPeer = {
    async command(jsonString) {
      const command = JSON.parse(jsonString);
      try {
        switch (command.cmd) {
          case 'login':
            return await app.login(command);
          case 'create_room':
            return await app.createRoom(command);
          case 'join_room':
            return await app.joinRoom(command.roomId);
          case 'join':
            return await app.join(command);
          case 'leave':
            return await app.leave();
          default:
            throw new Error(`unknown command: ${command.cmd}`);
        }
      } catch (error) {
        emitToDriver({ event: 'error', command: command.cmd, message: String(error) });
        throw error;
      }
    },
  };

  emitToDriver({ event: 'page_ready' });
}
