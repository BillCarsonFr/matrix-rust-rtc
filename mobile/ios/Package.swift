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

let package = Package(
    name: "MatrixRtcFFI",
    platforms: [
        .iOS(.v12)
    ],
    products: [
        .library(
            name: "MatrixRtcFFI",
            targets: ["MatrixRtcFFI", "MatrixRtcFFIRust"]
        ),
    ],
    targets: [
        .target(
            name: "MatrixRtcFFI",
            dependencies: ["MatrixRtcFFIRust"],
            path: "Sources/Swift"
        ),
        .binaryTarget(
            name: "MatrixRtcFFIRust",
            path: "build/MatrixRtcFFI.xcframework"
        ),
    ]
)

