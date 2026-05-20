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

