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

// Local-development twin of the root Package.swift: points at the xcframework
// built by `scripts/build-ios-xcframework.sh --swift-out Sources/MatrixRtc`
// instead of a released zip. Copy it over the root manifest (and do not commit
// the result) to add the repository as a local package in Xcode.
let package = Package(
    name: "MatrixRtc",
    platforms: [
        .iOS(.v16),
    ],
    products: [
        .library(name: "MatrixRtc", targets: ["MatrixRtc"]),
    ],
    targets: [
        .binaryTarget(name: "MatrixRtcFFI", path: "mobile/ios/build/MatrixRtcFFI.xcframework"),
        .target(name: "MatrixRtc", dependencies: ["MatrixRtcFFI"]),
    ]
)
