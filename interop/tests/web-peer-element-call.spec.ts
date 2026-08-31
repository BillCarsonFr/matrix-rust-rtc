/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

/**
 * The web stack and a real Element Call in the same call — EC's 2025 sticky
 * generation ("Matrix 2.0" in its Developer tab), our `sticky_events` mode.
 *
 * The web sibling of `element-call.spec.ts`'s second scenario, proving the
 * wasm compat seam end-to-end from a browser host: our membership goes out
 * with the legacy mirror fields (or EC shows no tile), EC's membership-less
 * member content normalises into our roster, and media keys cross as
 * `io.element.call.encryption_keys` in both directions (or nothing decrypts —
 * asserted by the video pattern EC reads back and the audio energy we meter).
 */

import { expect, test } from "@playwright/test";

import {
  acceptRoomInvite,
  callFrame,
  joinCall,
  loginToElementWeb,
  setRtcModeBeforeJoining,
} from "../helpers/element-web";
import { registerUser } from "../helpers/register";
import { expectPeerVideoPattern } from "../helpers/video";
import { WebPeer } from "../helpers/web-peer";

const HOMESERVER_URL = process.env.HOMESERVER_URL ?? "https://synapse.m.localhost";

test(`Web client and Element Call share a call — ec-2025 sticky events`, async ({
  browser,
}, testInfo) => {
  // Two logins with crypto bootstrap, a room, a real call and media.
  test.slow();

  const bob = await registerUser("ec");
  const page = await loginToElementWeb(browser, bob);

  // The web peer creates the room (and opens the slot): it puts our device in
  // the room before Element Call ever joins — a device that arrives later
  // cannot decrypt a membership Element Call has already sent.
  const web = await WebPeer.open(browser);
  await web.send({ cmd: "login", homeserver: HOMESERVER_URL });
  await web.waitFor("ready");

  try {
    const roomName = `Web interop 2_0 ${Date.now().toString(16)}`;
    await web.send({ cmd: "create_room", name: roomName, invite: bob.userId });
    const created = await web.waitFor("room_created");
    const roomId = created.room_id as string;

    await acceptRoomInvite(page, roomName);

    // Before joining: the dialect decides the membership carrier, the SFU
    // identity and the token endpoint together, so it cannot change mid-call.
    await setRtcModeBeforeJoining(page, "2_0");

    // The web peer joins the slot first, publishing the interop pattern+tone
    // in the sticky compat dialect.
    await web.send({
      cmd: "join",
      roomId,
      compat: "sticky_events",
      publish: { pattern: true, tone: true },
    });
    await web.waitFor("joined", { timeout: 120_000 });

    await joinCall(page);

    // ---- Element Call sees the web client ----------------------------------
    const frame = callFrame(page);
    await expect(frame.getByTestId("videoTile")).toHaveCount(2, { timeout: 90_000 });
    await expect(frame.getByText("Web Peer")).toBeVisible({ timeout: 30_000 });
    // Our frames decrypt and decode on EC's side: the published luma split
    // survives the whole legacy key path.
    await expectPeerVideoPattern(frame);

    // ---- the web client sees Element Call ----------------------------------
    await web.waitFor("members", {
      timeout: 90_000,
      predicate: (event) => (event.count as number) >= 2,
    });
    const subscribed = await web.waitFor("track_subscribed", {
      timeout: 90_000,
      predicate: (event) => event.kind === "audio",
    });
    // EC's legacy key installed under the identity the SFU assigned it.
    await web.waitFor("key_imported", {
      timeout: 90_000,
      predicate: (event) => event.identity === subscribed.identity,
    });
    // EC publishes Chrome's pulsed fake microphone; a peak reading above the
    // floor proves its audio decrypts with real energy in it.
    await web.waitFor("audio_rms", {
      timeout: 120_000,
      predicate: (event) =>
        event.identity === subscribed.identity &&
        (event.value as number) > (event.floor as number),
    });

    await web.send({ cmd: "leave" });
    await web.waitFor("left", { timeout: 60_000 });
  } finally {
    await web.dispose().catch(() => {});
    await testInfo.attach("web-peer.log", { body: web.log });
  }
});
