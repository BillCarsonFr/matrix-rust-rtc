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

package org.matrix.rtc

/**
 * Loads the SDK's native library. **Call [initialize] once before any other
 * SDK call** — including the generated `uniffi.matrix_rtc_ffi.*` API.
 *
 * ```kotlin
 * MatrixRtc.initialize()
 * RtcLogging.initLogcat()
 * ```
 *
 * ### Why this is not automatic
 *
 * The generated bindings reach the native library through JNA
 * (`Native.load`), which resolves it with `dlopen`. `dlopen` does **not**
 * invoke `JNI_OnLoad` — only ART's `System.loadLibrary` does. In the media
 * build `JNI_OnLoad` is what hands libwebrtc the `JavaVM` and class loader it
 * needs before a `PeerConnectionFactory` can exist, so on a JNA-only load the
 * first attempt to open a session aborts the process (SIGABRT from inside
 * libwebrtc's JNI helpers, not a catchable exception).
 *
 * Doing this from a `ContentProvider` or `androidx.startup` initializer would
 * make it invisible to callers, but it would also map the library — 20 MB-plus
 * in the media variant — into every process at launch, whether or not the app
 * ever places a call. Loading is therefore the host's call, and this method is
 * the whole of it: no [android.content.Context], no configuration.
 *
 * Idempotent and thread-safe; extra calls are no-ops.
 */
object MatrixRtc {

    private val lock = Any()

    @Volatile
    private var loaded = false

    /**
     * Loads `libmatrix_rtc_ffi.so` and, in the media build, runs its
     * `JNI_OnLoad` so libwebrtc gets its JVM hooks.
     *
     * @throws UnsatisfiedLinkError if the native library is missing from the
     *   APK for this device's ABI — the AAR ships `arm64-v8a`, `armeabi-v7a`
     *   and `x86_64`. Surfacing this here is the point of the explicit call:
     *   the same failure discovered later, inside libwebrtc, is an
     *   unrecoverable process abort.
     */
    @JvmStatic
    fun initialize() {
        if (loaded) return
        synchronized(lock) {
            if (loaded) return
            System.loadLibrary("matrix_rtc_ffi")
            loaded = true
        }
    }
}
