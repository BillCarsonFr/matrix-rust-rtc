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

set -e

# Point the root Package.swift at a released MatrixRtcFFI.xcframework.zip.
#
#   scripts/update-package-swift.sh <version> <sha256>
#
# <version> is the bare semver (0.2.0); the manifest derives the tag (v0.2.0)
# and the release asset URL from it. <sha256> is the zip's checksum, written by
# scripts/build-ios-xcframework.sh --zip.

if [ $# -ne 2 ]; then
    echo "usage: $0 <version> <sha256>" >&2
    exit 1
fi
VERSION="$1"
CHECKSUM="$2"

case "$CHECKSUM" in
    [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]*) ;;
    *) echo "❌ '$CHECKSUM' does not look like a SHA-256 hex digest" >&2; exit 1 ;;
esac
if [ "${#CHECKSUM}" -ne 64 ]; then
    echo "❌ '$CHECKSUM' does not look like a SHA-256 hex digest" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$(dirname "$SCRIPT_DIR")/Package.swift"

# sed -i differs between BSD and GNU; write to a temp file instead.
TMP="$(mktemp)"
sed -e "s|^let version = \".*\"|let version = \"$VERSION\"|" \
    -e "s|^let checksum = \".*\"|let checksum = \"$CHECKSUM\"|" \
    "$MANIFEST" > "$TMP"
mv "$TMP" "$MANIFEST"

grep -q "^let version = \"$VERSION\"" "$MANIFEST" || { echo "❌ version line not updated in $MANIFEST" >&2; exit 1; }
grep -q "^let checksum = \"$CHECKSUM\"" "$MANIFEST" || { echo "❌ checksum line not updated in $MANIFEST" >&2; exit 1; }

echo "✅ $MANIFEST now points at v$VERSION ($CHECKSUM)"
