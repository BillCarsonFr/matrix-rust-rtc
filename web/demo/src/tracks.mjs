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
 * Synthetic media for the test peer, and meters for what arrives.
 *
 * The published pattern and tone deliberately match the native interop peer's
 * (`interop_peer.rs`): a 640×360 frame, left half bright (Y=235), right half
 * dark (Y=16), and a 440 Hz sine. The luma split is what
 * `interop/helpers/video.ts` asserts on the Element Call side, and a
 * continuous sine keeps the peer's RMS assertion far from its floor — unlike
 * Chrome's pulsed fake microphone.
 */

/** Same floor as the rust peer's `AUDIO_RMS_FLOOR`, on the i16 sample scale. */
export const AUDIO_RMS_FLOOR = 200;

const PATTERN_WIDTH = 640;
const PATTERN_HEIGHT = 360;

/** The interop test pattern as a camera-like video track. */
export function patternVideoTrack() {
  const canvas = document.createElement('canvas');
  canvas.width = PATTERN_WIDTH;
  canvas.height = PATTERN_HEIGHT;
  const context = canvas.getContext('2d');
  const draw = () => {
    // rgb(v,v,v) has Rec.601 luma v, matching the peer's I420 Y planes.
    context.fillStyle = 'rgb(235,235,235)';
    context.fillRect(0, 0, PATTERN_WIDTH / 2, PATTERN_HEIGHT);
    context.fillStyle = 'rgb(16,16,16)';
    context.fillRect(PATTERN_WIDTH / 2, 0, PATTERN_WIDTH / 2, PATTERN_HEIGHT);
  };
  draw();
  // captureStream only produces frames when the canvas paints; keep painting.
  const timer = setInterval(draw, 1000 / 15);
  const track = canvas.captureStream(15).getVideoTracks()[0];
  track.addEventListener('ended', () => clearInterval(timer));
  return track;
}

/** A continuous 440 Hz tone as a microphone-like audio track. */
export function toneAudioTrack(frequency = 440) {
  const audioContext = new AudioContext();
  const oscillator = audioContext.createOscillator();
  oscillator.frequency.value = frequency;
  const gain = audioContext.createGain();
  gain.gain.value = 0.5;
  const destination = audioContext.createMediaStreamDestination();
  oscillator.connect(gain).connect(destination);
  oscillator.start();
  const track = destination.stream.getAudioTracks()[0];
  track.addEventListener('ended', () => audioContext.close());
  return track;
}

/**
 * Mean Rec.601 luma of the left and right halves of a playing `<video>`, or
 * `null` while it has no decodable frame — the technique of
 * `interop/helpers/video.ts`, without the iframe.
 */
export function sampleVideoHalves(video) {
  if (video.readyState < 2 || !video.videoWidth || !video.videoHeight) return null;
  const canvas = document.createElement('canvas');
  canvas.width = video.videoWidth;
  canvas.height = video.videoHeight;
  const context = canvas.getContext('2d', { willReadFrequently: true });
  context.drawImage(video, 0, 0);
  const { data, width, height } = context.getImageData(0, 0, canvas.width, canvas.height);

  let leftSum = 0;
  let leftCount = 0;
  let rightSum = 0;
  let rightCount = 0;
  for (let y = 0; y < height; y += 4) {
    for (let x = 0; x < width; x += 4) {
      const offset = (y * width + x) * 4;
      const luma =
        0.299 * data[offset] + 0.587 * data[offset + 1] + 0.114 * data[offset + 2];
      if (x < width / 2) {
        leftSum += luma;
        leftCount += 1;
      } else {
        rightSum += luma;
        rightCount += 1;
      }
    }
  }
  if (!leftCount || !rightCount) return null;
  return { left: leftSum / leftCount, right: rightSum / rightCount };
}

/**
 * An RMS meter over a remote audio track, on the same i16 scale as the rust
 * peer's `audio_rms` (so the same floor applies). Returns `{ read, stop }`.
 */
export function rmsMeter(mediaStreamTrack) {
  const audioContext = new AudioContext();
  const source = audioContext.createMediaStreamSource(new MediaStream([mediaStreamTrack]));
  const analyser = audioContext.createAnalyser();
  analyser.fftSize = 2048;
  source.connect(analyser);
  const samples = new Float32Array(analyser.fftSize);

  return {
    read() {
      analyser.getFloatTimeDomainData(samples);
      let sum = 0;
      for (const sample of samples) sum += sample * sample;
      return Math.sqrt(sum / samples.length) * 32768;
    },
    stop() {
      audioContext.close();
    },
  };
}
