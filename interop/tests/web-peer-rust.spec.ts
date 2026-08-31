/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

/**
 * The web stack and the Rust stack in the same call, spec-current dialect
 * (`ElementCallCompat::Off`) — no Element Call involved.
 *
 * This is the web bindings' first contact with everything real: a real
 * homeserver (MSC4354 sticky sends, MSC4140 delayed leave, Olm-encrypted key
 * to-device with cross-signing), a real authorisation service, a real SFU —
 * and, above all, the first meeting of the two frame-E2EE implementations we
 * ship: livekit-js's `ExternalE2EEKeyProvider` fed by the wasm engine on one
 * side, the Rust `KeyProvider` fed by the native bridge on the other. Both
 * directions are asserted; media that decodes proves keys, identities, and
 * token exchange all agreed.
 */

import { expect, test } from "@playwright/test";

import { RustPeer } from "../helpers/rust-peer";
import { WebPeer } from "../helpers/web-peer";

const HOMESERVER_URL = process.env.HOMESERVER_URL ?? "https://synapse.m.localhost";
const DISPLAY_NAME = "Rust Peer";

test(`Web client and Rust client share a call — spec-current dialect`, async ({
  browser,
}, testInfo) => {
  // Registration with crypto bootstrap, a room, a real call and media.
  test.slow();

  // The web peer first: the Rust peer creates the room and needs someone to
  // invite.
  const web = await WebPeer.open(browser);
  await web.send({ cmd: "login", homeserver: HOMESERVER_URL });
  const webUser = (await web.waitFor("ready")).user_id as string;

  const peer = RustPeer.spawn({
    // Spec-current MSC4143/MSC4354 on both sides; no Element Call dialect.
    ELEMENT_CALL_COMPAT: "off",
    INVITE_USER: webUser,
    DISPLAY_NAME,
    ROOM_NAME: `Web interop ${Date.now().toString(16)}`,
    OUT_WAV: testInfo.outputPath("web-peer-audio.wav"),
    RUST_LOG:
      process.env.RUST_LOG ??
      "info,matrix_rtc_core=debug,matrix_rtc_livekit=debug,matrix_rtc_bridge=debug,livekit=warn,webrtc_sys=warn",
  });

  try {
    const ready = await peer.waitFor("ready");
    const roomId = ready.room_id as string;

    // Join the room (the peer waits for 2 joined members before joining the
    // slot), then let the peer publish its membership and slot first.
    await web.send({ cmd: "join_room", roomId });
    await web.waitFor("room_joined");

    peer.send("join");
    const peerJoined = await peer.waitFor("joined", { timeout: 120_000 });
    const peerIdentity = peerJoined.identity as string;

    // The web peer joins the slot and publishes the interop pattern + tone.
    await web.send({
      cmd: "join",
      roomId,
      compat: "off",
      publish: { pattern: true, tone: true },
    });
    await web.waitFor("joined", { timeout: 120_000 });

    // ---- the web client sees the Rust client ------------------------------
    await web.waitFor("members", {
      timeout: 90_000,
      predicate: (event) => (event.count as number) >= 2,
    });
    await web.waitFor("track_subscribed", {
      timeout: 90_000,
      predicate: (event) => event.kind === "audio" && event.identity === peerIdentity,
    });
    // The peer's media key installed under the identity the SFU assigned it.
    await web.waitFor("key_imported", {
      timeout: 90_000,
      predicate: (event) => event.identity === peerIdentity,
    });
    // Its video decrypts and decodes: the published pattern's luma split
    // survives all the way through the frame cryptor to a <video> readback.
    // (Same MIN_SPLIT = 40 as helpers/video.ts; the published contrast is
    // 235 vs 16.)
    const pattern = await web.waitFor("video_pattern", {
      timeout: 90_000,
      predicate: (event) =>
        event.identity === peerIdentity &&
        (event.left as number) - (event.right as number) >= 40,
    });
    expect((pattern.left as number) - (pattern.right as number)).toBeGreaterThanOrEqual(40);

    // ---- the Rust client sees the web client ------------------------------
    await peer.waitFor("members", {
      timeout: 90_000,
      predicate: (event) => (event.count as number) >= 2,
    });
    const subscribed = await peer.waitFor("track_subscribed", {
      timeout: 90_000,
      predicate: (event) => event.kind === "audio",
    });
    await peer.waitFor("key_imported", {
      timeout: 90_000,
      predicate: (event) => event.identity === subscribed.identity,
    });
    // The web tone decrypts with real energy in it — a continuous sine, so
    // well above the pulsed-fake-device floor.
    const audio = await peer.waitFor("audio_rms", { timeout: 120_000 });
    expect(
      audio.value as number,
      `web audio should carry energy (see ${testInfo.outputPath("web-peer-audio.wav")})`,
    ).toBeGreaterThan(audio.floor as number);

    // ---- teardown is part of the protocol ----------------------------------
    await web.send({ cmd: "leave" });
    await web.waitFor("left", { timeout: 60_000 });
    peer.send("leave");
    await peer.waitFor("left", { timeout: 60_000 });
  } finally {
    await web.dispose().catch(() => {});
    await peer.dispose().catch(() => {});
    await testInfo.attach("interop-peer.log", { body: peer.stderr });
    await testInfo.attach("web-peer.log", { body: web.log });
  }
});
