# iOS Integration Tests

This directory will contain Swift tests for the XCFramework and bindings.

## Test Structure

Once the XCFramework and Swift bindings are built, create tests in your Xcode project:

```swift
import XCTest
@testable import MatrixRtcFFI

final class MatrixRtcFFITests: XCTestCase {
    func testSessionCreation() throws {
        let session = RtcSessionHandle()
        // Test session operations
    }

    func testMembershipSnapshot() throws {
        let session = RtcSessionHandle()
        let subscription = try session.subscribeMembershipSnapshots()
        let initial = try subscription.nextSnapshot()
        XCTAssertNotNil(initial)
    }
}
```

## Running Tests

For a SwiftPM package:

```bash
cd mobile/ios
swift test
```

For Xcode project:

```bash
xcodebuild test -scheme MatrixRtcFFI
```

