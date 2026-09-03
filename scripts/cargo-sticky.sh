#!/usr/bin/env bash
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

# Run cargo against the MSC4354-capable matrix-rust-sdk fork.
#
#   scripts/cargo-sticky.sh <cargo subcommand and args>
#
# The workspace depends on upstream matrix-rust-sdk; this applies the
# `.cargo/experimental-sticky.toml` overlay that redirects it to the fork. The
# caller still passes the features (`experimental-sticky` plus the SDK's own
# `unstable-msc4354` on matrix-sdk and matrix-sdk-ui — see the Makefile's
# STICKY_FEATURES), because a cargo feature cannot forward to an SDK feature
# upstream lacks.
#
# Two side effects are contained here so the two SDK trees never fight:
# - the build goes to target/sticky (override with CARGO_TARGET_DIR);
# - the redirected SDK re-resolves the lockfile, so Cargo.lock is swapped for
#   Cargo.sticky.lock for the duration and the committed (upstream) lockfile is
#   put back afterwards, whatever the exit status. Cargo.sticky.lock is kept, and
#   committed, so sticky builds resolve the same way every time.
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p target
cp Cargo.lock target/.Cargo.lock.upstream
if [ -f Cargo.sticky.lock ]; then
    cp Cargo.sticky.lock Cargo.lock
fi

restore() {
    # The lockfile cargo just used is the sticky one; keep it for next time.
    cp Cargo.lock Cargo.sticky.lock
    mv -f target/.Cargo.lock.upstream Cargo.lock
}
trap restore EXIT

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target/sticky}" \
    cargo --config .cargo/experimental-sticky.toml "$@"
