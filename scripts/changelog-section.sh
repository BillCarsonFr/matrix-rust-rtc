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

# Print the CHANGELOG.md section for one version — the body of the GitHub
# release. Fails if there is none, which is how the release workflow enforces
# that the "Unreleased" heading was renamed before releasing.
#
#   scripts/changelog-section.sh <version>
#
# The heading may be any of `## 0.2.0`, `## v0.2.0`, `## [0.2.0]`, `## [v0.2.0]`,
# optionally followed by ` - <date>`.

if [ $# -ne 1 ]; then
    echo "usage: $0 <version>" >&2
    exit 1
fi
VERSION="$1"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHANGELOG="$(dirname "$SCRIPT_DIR")/CHANGELOG.md"

SECTION="$(awk -v version="$VERSION" '
    BEGIN { pattern = "^## \\[?v?" version "\\]?( |$)"; gsub(/\./, "\\.", pattern) }
    /^## / { if (found) exit; if ($0 ~ pattern) { found = 1; next } }
    found { print }
    END { exit !found }
' "$CHANGELOG")" || {
    echo "❌ No '## v$VERSION' section in $CHANGELOG. Rename the Unreleased heading first." >&2
    exit 1
}

printf '%s\n' "$SECTION"
