/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

import { defineConfig, devices } from "@playwright/test";

import { devCaPath } from "./helpers/dev-ca";

/**
 * The interop stack mints its CA at up time, and nothing here expects it to be
 * installed machine-wide:
 *
 * - **Node** gets it here, for the registration calls this suite makes.
 * - **The Rust peer** gets it as `SSL_CERT_FILE` when it is spawned
 *   (`helpers/rust-peer.ts`).
 * - **The browser** does not need it at all: `ignoreHTTPSErrors` below covers
 *   it, and an ignored certificate error still leaves the origin `https://`,
 *   so Element Call keeps the secure context WebRTC and the widget API need.
 */
const devCa = devCaPath();
if (!process.env.NODE_EXTRA_CA_CERTS && devCa) {
  process.env.NODE_EXTRA_CA_CERTS = devCa;
}

export default defineConfig({
  testDir: "./tests",
  // One call at a time: both scenarios share a single SFU and homeserver, and
  // interleaved logs from two live calls are unreadable when one fails.
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  // A real call has real flakes (ICE, sync latency). One retry, and the trace
  // from the first attempt is kept either way.
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : [["list"]],
  // A whole scenario: two logins, a room, a call, media, teardown. The Rust
  // peer caps itself at 240s, so this has to sit above that to let the peer's
  // own diagnosis win over a bare Playwright timeout.
  timeout: 6 * 60 * 1000,
  expect: { timeout: 20_000 },
  use: {
    baseURL: process.env.ELEMENT_WEB_URL ?? "https://app.m.localhost",
    ignoreHTTPSErrors: true,
    trace: "retain-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        permissions: ["microphone", "camera"],
        launchOptions: {
          args: [
            // CI runners have no capture hardware; these give Chrome a
            // synthetic camera and microphone and auto-grant the prompt.
            "--use-fake-ui-for-media-stream",
            "--use-fake-device-for-media-stream",
            "--mute-audio",
          ],
        },
      },
    },
  ],
});
