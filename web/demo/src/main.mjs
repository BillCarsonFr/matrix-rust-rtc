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

import { WebPeerApp } from './call.mjs';
import { installPeerApi } from './peer-api.mjs';

const params = new URLSearchParams(location.search);
const logElement = document.getElementById('log');

function log(line) {
  const stamped = `${new Date().toISOString().slice(11, 23)} ${line}`;
  console.log(`web-peer: ${line}`);
  logElement.append(stamped, document.createElement('br'));
  logElement.scrollTop = logElement.scrollHeight;
}

const app = new WebPeerApp({
  emit: (event) => log(JSON.stringify(event)),
  log,
  tilesElement: document.getElementById('tiles'),
});

if (params.get('test') === '1') {
  // Headless mode: Playwright drives everything through window.webPeer.
  document.getElementById('controls').hidden = true;
  installPeerApi(app, log);
} else {
  const field = (id) => document.getElementById(id);
  if (params.get('homeserver')) field('homeserver').value = params.get('homeserver');

  document.getElementById('joinBtn').addEventListener('click', async () => {
    field('joinBtn').disabled = true;
    try {
      // One session per page: logging in again with a blank User field would
      // register a NEW throwaway user — who is not invited to any room the
      // previous one created. Reload the page to switch users.
      if (!app.client) {
        await app.login({
          homeserver: field('homeserver').value,
          user: field('user').value || undefined,
          password: field('password').value || undefined,
        });
        field('user').value = app.userId;
      } else {
        log(`reusing session as ${app.userId} (reload the page to switch users)`);
      }
      let roomId = field('room').value.trim();
      if (roomId) {
        await app.joinRoom(roomId);
      } else {
        roomId = await app.createRoom({
          name: `Web call ${new Date().toISOString()}`,
          invite: field('invite').value.trim() || undefined,
        });
        field('room').value = roomId;
      }
      await app.join({
        roomId,
        compat: field('mode').value,
        publish: { devices: true },
      });
      field('leaveBtn').disabled = false;
    } catch (error) {
      log(`join failed: ${error}`);
      field('joinBtn').disabled = false;
    }
  });

  document.getElementById('leaveBtn').addEventListener('click', async () => {
    field('leaveBtn').disabled = true;
    try {
      await app.leave();
    } catch (error) {
      log(`leave failed: ${error}`);
    }
    field('joinBtn').disabled = false;
  });
}
