/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

/**
 * Reading the Rust peer's video back out of Element Call.
 *
 * This is the only assertion that proves media flows *towards* Element Call.
 * Everything else on the browser side (a tile, a display name) is satisfied by
 * signalling alone: Element Call renders a tile for any participant in the
 * call, decoding or not.
 *
 * The peer publishes a frame whose left half is bright (Y=235) and right half
 * dark (Y=16) — the same pattern `e2e_call` verifies in Rust. Finding that
 * split in a `<video>` inside the widget means our frames arrived, were
 * decrypted with the key Element Call imported from us, and decoded.
 */

import { expect, type FrameLocator } from "@playwright/test";

/** Mean luma of each half of one `<video>`, as rendered right now. */
export interface HalvesLuma {
  left: number;
  right: number;
}

/**
 * Sample every playing `<video>` in the widget and return each one's
 * left/right mean luma.
 *
 * Runs in the iframe's own context. A `MediaStream` is not CORS-tainted, so
 * the canvas readback is allowed.
 */
export async function sampleVideoHalves(frame: FrameLocator): Promise<HalvesLuma[]> {
  return frame.locator("video").evaluateAll((elements) =>
    (elements as HTMLVideoElement[]).flatMap((video) => {
      // HAVE_CURRENT_DATA: anything less has no frame to read.
      if (video.readyState < 2 || !video.videoWidth || !video.videoHeight) return [];

      const canvas = document.createElement("canvas");
      canvas.width = video.videoWidth;
      canvas.height = video.videoHeight;
      const context = canvas.getContext("2d");
      if (!context) return [];
      context.drawImage(video, 0, 0, canvas.width, canvas.height);

      const { data } = context.getImageData(0, 0, canvas.width, canvas.height);
      const half = Math.floor(canvas.width / 2);
      let left = 0;
      let right = 0;
      let leftCount = 0;
      let rightCount = 0;
      // Every 4th pixel in each direction: 16x fewer samples, same verdict.
      for (let y = 0; y < canvas.height; y += 4) {
        for (let x = 0; x < canvas.width; x += 4) {
          const index = (y * canvas.width + x) * 4;
          const luma =
            0.299 * data[index] + 0.587 * data[index + 1] + 0.114 * data[index + 2];
          if (x < half) {
            left += luma;
            leftCount += 1;
          } else {
            right += luma;
            rightCount += 1;
          }
        }
      }
      if (!leftCount || !rightCount) return [];
      return [{ left: left / leftCount, right: right / rightCount }];
    }),
  );
}

/**
 * Wait until one of the widget's videos shows the peer's pattern.
 *
 * The threshold is deliberately far below the published contrast (235 vs 16):
 * the frame survives VP8, scaling and whatever simulcast layer Element Call
 * settles on, but nothing else on screen has a bright-left/dark-right split —
 * Chrome's fake capture device, which is what Element Call publishes, is
 * evenly lit across its width.
 */
export async function expectPeerVideoPattern(
  frame: FrameLocator,
  timeout = 60_000,
): Promise<void> {
  const MIN_SPLIT = 40;
  await expect
    .poll(
      async () => {
        const samples = await sampleVideoHalves(frame);
        // The best split across every playing video. A failure prints the last
        // value, which separates the cases: 0 means nothing decoded at all,
        // and a small number means something decoded but not our pattern.
        return samples.reduce((best, { left, right }) => Math.max(best, left - right), 0);
      },
      {
        timeout,
        message:
          "no <video> in Element Call showed the peer's bright-left/dark-right pattern " +
          "(value is the best left-minus-right mean luma seen; our frames publish ~219)",
      },
    )
    .toBeGreaterThan(MIN_SPLIT);
}
