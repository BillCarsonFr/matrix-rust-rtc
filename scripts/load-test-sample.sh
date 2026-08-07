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
#
# SET SIMULCAST=1 WHENEVER A REAL CLIENT IS WATCHING. Element Call (and any
# livekit-client subscriber) uses adaptive stream: it picks a layer from the
# rendered tile size and asks the SFU for it. A publication with no layers to
# choose from resolves to nothing and the SFU forwards no video at all — the
# watcher sees a grey tile, with no decryption errors anywhere, because no
# frames ever arrive. Audio is unaffected, which makes it look like an E2EE
# problem when it is not. Leave it off only for pure scale runs where nobody is
# rendering the participants.
RESOLUTION="640x360"
FPS=15
SIMULCAST=1

# Seconds of the file to decode up front and then loop.
CLIP_SECONDS=10

# 0 = run until stopped by typing :q
#
# Ctrl-C does NOT work: signals never arrive in this process (SIGINT and
# SIGTERM are both ignored even with handlers installed, and only SIGKILL
# lands) — libwebrtc masks them process-wide. Type :q to stop, or set a
# DURATION and let the run end itself.
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
#
# Set this when the call was opened by Element Call (see LEGACY_ELEMENT_CALL):
# that client publishes no m.rtc.slot at all, and a slot nobody opened reads as
# closed, which projects every member — including these devices — out of the
# call.
OPEN_SLOT=0

# Share the call with Element Call on the JS SDK, which still speaks the
# MatrixRTC wire format from before the 2026 MSC4143 rewrite. Our membership
# then also carries the fields it needs (notably the device id it addresses
# media keys to, which it cannot obtain any other way — it runs as a widget and
# gets no decryption metadata).
#
# Media keys then go out under the legacy to-device type INSTEAD of the spec
# one, since a to-device message has only one type. So a run with this on
# exchanges keys with Element Call or with spec-current peers, never both.
# Reading the old format needs no flag and is always on.
LEGACY_ELEMENT_CALL=0

# Accept self-signed certificates (the demo/backend stack).
INSECURE_TLS=0

# How long the homeserver keeps each membership in the sticky map.
#
# Short on purpose (the library default is an hour). A run that is killed rather
# than left cleanly leaves its memberships standing until this elapses — the
# dead man's switch does NOT clear them, because its delayed leave is a plain
# event that never replaces the sticky entry. An hour of ghosts poisons the room
# for the next attempt; two minutes does not.
#
# Costs one extra membership send per device every half of this. Keep it well
# above twice the 15s heartbeat, or memberships lapse between beats.
STICKY_DURATION_MS=120000

# Pacing, to stay under homeserver rate limits.
RAMP_MS=500

# Gap between logins. Every device is a fresh login, and synapse's `rc_login`
# defaults to `per_second: 0.17, burst_count: 3` — three logins immediately,
# then one per ~6 seconds. Exceed that and the homeserver answers 429 "Too many
# login attempts"; the tool waits it out (5 attempts per device with doubling
# backoff), so a run survives, but being throttled is slower than pacing right.
#
# So DEVICES up to 3 works at any pacing, and beyond that you want >= 6000 here:
# 10 devices is then roughly a 50-second ramp. Raise it further if a deployment
# has tightened the limiter. Only a local synapse you control (see demo/backend,
# which sets every rc_* to 1000/1000) can take the old 250ms.
LOGIN_DELAY_MS=7000

# Prefix of the device display names created here, and the one --purge-devices
# matches on.
DEVICE_PREFIX="rtc-loadtest"

# Persist each device's session and crypto store here, and reuse them next run.
# Empty = off (every run logs in fresh devices).
#
# Two reasons to set it:
#   * Logins happen once. A fresh run of DEVICES=10 otherwise spends ~50s being
#     rate-limited through LOGIN_DELAY_MS above; with a store it spends none.
#   * A restored device keeps its megolm sessions, so it can decrypt member
#     events sent BEFORE the run started. A fresh device cannot, which is why a
#     peer already in the call can be invisible until they re-join.
#
# Devices are then kept on exit (logging out would revoke the stored token).
# `--purge-devices` deletes the devices and this folder together.
#
# The folder is created if missing; it need not pre-exist. A folder that already
# has contents and was not created by this tool is refused rather than adopted,
# since a failed restore deletes <store>/device-N inside it.
#
# It holds access tokens and the devices' crypto stores unencrypted, so keep it
# out of the repository and off shared disks — somewhere like
# "$HOME/.matrix-rtc-loadtest" rather than a path under this checkout.
STORE=""

# --- end edit --------------------------------------------------------------

cd "$(dirname "$0")/.."

# Log filter, used only when RUST_LOG is not already set — so
# `RUST_LOG=debug ./scripts/load-test.sh` still overrides it wholesale.
#
# Ours at debug, and four SDK subsystems silenced because they are pure noise
# for this tool rather than because they are unimportant:
#
#   event_cache        "missing target event id from the redaction event"
#   latest_events      the SDK trying to parse our MSC4143 member events as
#                      legacy m.call.* ones: one error per membership we send
#   identities::manager "Our own device might have been deleted" — expected when
#                      a run mints and deletes devices every time
#   gossiping          "Received a forwarded room key that we didn't request" —
#                      our own devices sharing keys with each other
#
# Drop a line to see one of them again, or use RUST_LOG=debug for everything.
: "${RUST_LOG:=warn,matrix_rtc_core=debug,matrix_rtc_media=debug,matrix_rtc_livekit=debug,matrix_sdk::event_cache=off,matrix_sdk::latest_events=off,matrix_sdk_crypto::identities::manager=off,matrix_sdk_crypto::gossiping=off}"
export RUST_LOG

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
    --sticky-duration-ms "$STICKY_DURATION_MS"
)
[[ "$SIMULCAST" == "0" ]] && args+=(--no-simulcast)
[[ "$AUDIO" == "1" ]] && args+=(--audio)
[[ "$SUBSCRIBE" == "1" ]] && args+=(--subscribe)
[[ "$OPEN_SLOT" == "1" ]] && args+=(--open-slot)
[[ "$LEGACY_ELEMENT_CALL" == "1" ]] && args+=(--legacy-element-call)
[[ "$INSECURE_TLS" == "1" ]] && args+=(--insecure-tls)
[[ -n "$STORE" ]] && args+=(--store "$STORE")

# --release matters: a debug build cannot keep the encoders fed and you end up
# measuring the generator instead of the deployment.
#
# Built and then exec'd directly rather than run through `cargo run`, so that
# Ctrl-C reaches this process and nothing else. Under `cargo run`, SIGINT goes
# to the whole foreground process group: cargo dies at once and the generator is
# orphaned out of that group, so every later Ctrl-C lands nowhere while it keeps
# printing to the same terminal. `exec` also means no shell is left wrapping it,
# so the PID you see is the one to signal.
cargo build --release -p matrix-rtc-livekit --example load_test --features matrix-sdk
exec ./target/release/examples/load_test "${args[@]}" "$@"
