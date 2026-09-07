// swift-tools-version:5.9
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

import PackageDescription

// Swift package for the released MatrixRtcFFI.xcframework (media variant:
// device + Apple Silicon simulator). The two constants below are rewritten by
// scripts/update-package-swift.sh from the release workflow, which then tags the
// commit; the zip is the asset attached to that tag's GitHub release, and
// Sources/MatrixRtc holds the Swift generated for that same build.
//
// Add it in Xcode with this repository's URL at a `v*` tag, then add `-ObjC` to
// the app target's "Other Linker Flags" (libwebrtc's Objective-C categories are
// otherwise dead-stripped from the static archive). See mobile/PACKAGING.md.
//
// For a local build, copy mobile/ios/Debug-Package.swift over this file.
let version = "0.0.0"
let checksum = "0000000000000000000000000000000000000000000000000000000000000000"
let url = "https://github.com/BillCarsonFr/matrix-rust-rtc/releases/download/v\(version)/MatrixRtcFFI.xcframework.zip"

let package = Package(
    name: "MatrixRtc",
    platforms: [
        .iOS(.v16),
    ],
    products: [
        // Static (the default): the archive is linked into the app target, so
        // the app's own `-ObjC` flag applies to it. A dynamic product would
        // link the archive into a framework without that flag.
        .library(name: "MatrixRtc", targets: ["MatrixRtc"]),
    ],
    targets: [
        .binaryTarget(name: "MatrixRtcFFI", url: url, checksum: checksum),
        .target(name: "MatrixRtc", dependencies: ["MatrixRtcFFI"]),
    ]
)
