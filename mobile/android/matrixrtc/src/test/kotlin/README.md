# Android Integration Tests

This directory will contain integration tests for Kotlin bindings once they're generated.

## Test Structure

When the build script generates Kotlin bindings to `src/main/java`, add corresponding tests here:

```kotlin
import org.junit.Test
import org.matrix.rtc.*

class MatrixRtcFfiTest {
    @Test
    fun testSessionCreation() {
        val session = RtcSessionHandle()
        // Test session operations
    }

    @Test
    fun testMembershipSnapshot() {
        val session = RtcSessionHandle()
        val subscription = session.subscribeMembershipSnapshots()
        // Test subscription behavior
    }
}
```

## Running Tests

```bash
cd mobile/android
./gradlew test
```

