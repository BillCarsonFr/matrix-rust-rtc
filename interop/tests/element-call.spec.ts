/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

/**
 * A Rust MatrixRTC client and a real Element Call in the same call.
 *
 * Both directions are asserted, because each proves a different half of the
 * compat dialect:
 *
 * - **Rust sees Element Call**: its membership parses into our roster, its
 *   media key installs under the identity the *SFU* assigned it, and its
 *   frames decrypt (audio with real energy in it).
 * - **Element Call sees Rust**: our membership renders as a tile with our
 *   display name, and Element Call is not stuck on "Waiting for media...",
 *   which is what it shows when nothing decodes.
 *
 * `crates/matrix-rtc-livekit/tests/e2e_call` already proves our stack talks to
 * itself in every mode. What it cannot prove is that Element Call agrees with
 * our reading of the wire format — that needs a browser, and this is it.
 */

import { expect, test } from "@playwright/test";

import {
  acceptRoomInvite,
  callFrame,
  joinCall,
  loginToElementWeb,
  setRtcModeBeforeJoining,
  type RtcMode,
} from "../helpers/element-web";
import { registerUser } from "../helpers/register";
import { RustPeer } from "../helpers/rust-peer";
import { expectPeerVideoPattern } from "../helpers/video";

interface Scenario {
  /** Element Call's Developer-tab dialect. */
  ec: RtcMode;
  /** The matching `ElementCallCompat` for the Rust peer. */
  rust: "state" | "sticky";
  title: string;
}

const SCENARIOS: Scenario[] = [
  // First, because this is the dialect real Element Call deployments speak
  // today, and the one with the most documented traps (plain `{user}:{device}`
  // identities, `/sfu/get`, membership as room state, no slot).
  { ec: "compat", rust: "state", title: "ec-2024 state events" },
  // Element Call labels this "Matrix 2.0", but it is its *2025* sticky
  // generation — MSC4354 membership carrying the pre-2026 field names. The
  // actually-spec-current format is our `ElementCallCompat::Off`, which Element
  // Call does not speak at all. Naming the test after the UI string would
  // propagate that confusion; the string itself lives in `RTC_MODE_LABEL`.
  { ec: "2_0", rust: "sticky", title: "ec-2025 sticky events" },
];

const DISPLAY_NAME = "Rust Peer";

for (const scenario of SCENARIOS) {
  test(`Rust client and Element Call share a call — ${scenario.title}`, async ({
    browser,
  }, testInfo) => {
    // Two logins, a room, a real call and media. Nothing here is quick.
    test.slow();

    const bob = await registerUser("ec");
    const page = await loginToElementWeb(browser, bob);

    // The Rust peer creates the room, not Element Web: that is where the
    // `org.matrix.msc3401.call.member` power level comes from (the state
    // dialect needs it at PL 0), and it puts our device in the room before
    // Element Call ever joins — a device that arrives later cannot decrypt a
    // membership Element Call has already sent.
    const peer = RustPeer.spawn({
      ELEMENT_CALL_COMPAT: scenario.rust,
      INVITE_USER: bob.userId,
      DISPLAY_NAME,
      ROOM_NAME: `Interop ${scenario.ec} ${Date.now().toString(16)}`,
      OUT_WAV: testInfo.outputPath("element-call-audio.wav"),
      RUST_LOG:
        process.env.RUST_LOG ??
        "info,matrix_rtc_core=debug,matrix_rtc_livekit=debug,matrix_rtc_bridge=debug,livekit=warn,webrtc_sys=warn",
    });

    try {
      const ready = await peer.waitFor("ready");
      const roomName = ready.room_name as string;

      await acceptRoomInvite(page, roomName);

      // Before joining: the dialect decides the membership carrier, the SFU
      // identity and the token endpoint together, so it cannot change mid-call.
      await setRtcModeBeforeJoining(page, scenario.ec);

      peer.send("join");
      await peer.waitFor("joined", { timeout: 120_000 });

      await joinCall(page);

      // ---- Element Call sees the Rust client ----------------------------
      const frame = callFrame(page);
      // Signalling: a tile and a name. Note these are satisfied by membership
      // alone — Element Call renders a tile for a participant whose media
      // never decodes — so they are necessary, not sufficient.
      await expect(frame.getByTestId("videoTile")).toHaveCount(2, { timeout: 90_000 });
      await expect(frame.getByText(DISPLAY_NAME)).toBeVisible({ timeout: 30_000 });
      // Media: the peer's frames actually decrypted and decoded inside Element
      // Call. This is the *only* assertion on this side that distinguishes a
      // working call from a connected-but-silent one.
      await expectPeerVideoPattern(frame);

      // ---- The Rust client sees Element Call -----------------------------
      // Membership: Element Call's join parsed into our roster.
      await peer.waitFor("members", {
        timeout: 90_000,
        predicate: (event) => (event.count as number) >= 2,
      });
      // Identity: whatever the SFU actually assigned Element Call, not one we
      // derived — a derivation mismatch is silent (tracks buffer forever).
      const subscribed = await peer.waitFor("track_subscribed", {
        timeout: 90_000,
        predicate: (event) => event.kind === "audio",
      });
      // Key exchange, both dialects' to-device format included.
      await peer.waitFor("key_imported", {
        timeout: 90_000,
        predicate: (event) => event.identity === subscribed.identity,
      });
      // Frames actually decrypt. Element Call publishes Chrome's fake capture
      // device (a pulsed tone, not a sine), so this is energy, not frequency.
      const audio = await peer.waitFor("audio_rms", { timeout: 120_000 });
      expect(
        audio.value as number,
        `Element Call's audio decoded to near-silence (rms ${audio.value}); ` +
          `the recording is attached as element-call-audio.wav`,
      ).toBeGreaterThan(audio.floor as number);

      // ---- Teardown -------------------------------------------------------
      peer.send("leave");
      await peer.waitFor("left", { timeout: 60_000 });
    } finally {
      // The peer's log is the only view of the Matrix side of a failure.
      await peer.dispose();
      if (peer.stderr) {
        await testInfo.attach("interop-peer.log", {
          body: peer.stderr,
          contentType: "text/plain",
        });
      }
    }
  });
}
