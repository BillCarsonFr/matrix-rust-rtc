#!/bin/bash
# Copyright 2026 Valere Fedronic
#
# This file is part of matrix-rust-rtc.
#
# matrix-rust-rtc is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# matrix-rust-rtc is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.
#
# Template for running the MatrixRTC load generator
# (crates/matrix-rtc-livekit/examples/load_test.rs).
#
# Copy it, fill in the block below, and run your copy:
#
#     cp scripts/load-test-sample.sh scripts/load-test.sh
#     $EDITOR scripts/load-test.sh
#     ./scripts/load-test.sh
#
# `scripts/load-test.sh` is git-ignored, so the password and recovery key you
# put in it stay out of the repository. Keep the credentials out of THIS file.
#
# Extra flags are passed straight through and win over the block below, so
# one-off tweaks need no edit:
#
#     ./scripts/load-test.sh --devices 10 --no-simulcast
#
# Prerequisites: `ffmpeg` on PATH (unless VIDEO is .y4m/.yuv), and an account
# that has already joined ROOM_ID with a call open in it.

set -euo pipefail

# --- edit me ---------------------------------------------------------------

HOMESERVER="http://localhost:8008"
MX_USER="loadtest"
MX_PASSWORD="secret"
# Printed by the first `join_and_record` run against a fresh account.
RECOVERY_KEY=""
ROOM_ID='!room:synapse'

# Video published by every device. Scaled to RESOLUTION by ffmpeg; .y4m/.yuv
# are used as-is and must already match it.
VIDEO="clip.mp4"

# Virtual participants. Each is a real, separate device login.
#
# How many is reasonable depends on how much video encoding this machine can
# do: one encoder per device with SIMULCAST=0 below, two at 640x360 with it on,
# three at 720p. Start low, double, and watch the "frames/10s" health line the
# tool prints every 10 seconds — once it falls below FPS x 10 the generator is
# saturated and further devices add no real load. See "How many devices?" in
# crates/matrix-rtc-livekit/README.md.
DEVICES=3

# Encode cost per device is driven by these three. At 640x360 livekit
# publishes 2 simulcast layers and at 720p it publishes 3, so simulcast is off
# here by default: one encoding per device roughly halves the encode work and
# lets more devices fit on one machine. Set SIMULCAST=1 when the layer
# selection itself is what you want to exercise — a real client publishes with
# it on, and the SFU only has layers to choose between when it is.
RESOLUTION="640x360"
FPS=15
SIMULCAST=0

# Seconds of the file to decode up front and then loop.
CLIP_SECONDS=10

# 0 = run until Ctrl-C.
DURATION=0

# Publish a per-device tone as well as video. Noisy with several devices.
AUDIO=0

# Also receive and decode every peer's video, like a real client. Costs N x N;
# leave off unless you are specifically testing the receive path.
SUBSCRIBE=0

# LiveKit authorisation service, used only when the homeserver advertises no
# transport of its own.
LIVEKIT_URL="http://localhost:6080"

SLOT_ID="m.call#ROOM"

# Publish the m.rtc.slot state event first. Needs the power level for it, and
# is unnecessary when a real client already opened the call.
OPEN_SLOT=0

# Accept self-signed certificates (the demo/backend stack).
INSECURE_TLS=0

# Pacing, to stay under homeserver rate limits.
RAMP_MS=500
LOGIN_DELAY_MS=250

# Prefix of the device display names created here, and the one --purge-devices
# matches on.
DEVICE_PREFIX="rtc-loadtest"

# --- end edit --------------------------------------------------------------

cd "$(dirname "$0")/.."

if [[ -z "$RECOVERY_KEY" ]]; then
    echo "error: RECOVERY_KEY is empty." >&2
    echo "Every device is a fresh login, and a device that is not cross-signed" >&2
    echo "exchanges no media keys in either direction — participants would show" >&2
    echo "up with no video. Set it in the block at the top of this file." >&2
    exit 1
fi

args=(
    --homeserver "$HOMESERVER"
    --user "$MX_USER"
    --password "$MX_PASSWORD"
    --recovery-key "$RECOVERY_KEY"
    --room "$ROOM_ID"
    --video "$VIDEO"
    --devices "$DEVICES"
    --slot-id "$SLOT_ID"
    --livekit-url "$LIVEKIT_URL"
    --resolution "$RESOLUTION"
    --fps "$FPS"
    --clip-seconds "$CLIP_SECONDS"
    --duration "$DURATION"
    --ramp-ms "$RAMP_MS"
    --login-delay-ms "$LOGIN_DELAY_MS"
    --device-prefix "$DEVICE_PREFIX"
)
[[ "$SIMULCAST" == "0" ]] && args+=(--no-simulcast)
[[ "$AUDIO" == "1" ]] && args+=(--audio)
[[ "$SUBSCRIBE" == "1" ]] && args+=(--subscribe)
[[ "$OPEN_SLOT" == "1" ]] && args+=(--open-slot)
[[ "$INSECURE_TLS" == "1" ]] && args+=(--insecure-tls)

# --release matters: a debug build cannot keep the encoders fed and you end up
# measuring the generator instead of the deployment.
exec cargo run --release -p matrix-rtc-livekit --example load_test \
    --features matrix-sdk -- "${args[@]}" "$@"
