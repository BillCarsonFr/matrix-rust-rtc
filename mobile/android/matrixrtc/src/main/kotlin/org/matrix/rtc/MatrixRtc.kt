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

import com.sun.jna.Callback
import com.sun.jna.CallbackThreadInitializer
import com.sun.jna.Native
import com.sun.jna.Structure
import uniffi.matrix_rtc_ffi.uniffiCallbackInterfaceCommandSenderCallback
import uniffi.matrix_rtc_ffi.uniffiCallbackInterfaceRtcLogSink
import uniffi.matrix_rtc_ffi.uniffiForeignFutureFreeImpl
import uniffi.matrix_rtc_ffi.uniffiRustFutureContinuationCallbackImpl

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
     * `JNI_OnLoad` so libwebrtc gets its JVM hooks. Also settles how JNA
     * treats the threads Rust calls back on — see [pinCallbackThreads], which
     * has to happen before the first callback reaches Rust.
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
            pinCallbackThreads()
            System.loadLibrary("matrix_rtc_ffi")
            loaded = true
        }
    }

    /**
     * Keeps every native thread that calls back into Kotlin attached to the
     * JVM for the life of the thread, rather than for the life of one call.
     *
     * Without this, JNA attaches a thread on its first callback and calls
     * `DetachCurrentThread` as soon as that callback returns. libwebrtc caches a
     * `JNIEnv*` in a pthread key (`g_jni_ptr`) for every thread *it* attaches
     * and detaches only from that key's destructor, so a JNA detach on a shared
     * thread leaves the cached pointer behind with no attachment under it. The
     * next `AttachCurrentThreadIfNeeded` on that thread fails
     * `RTC_CHECK(!pthread_getspecific(g_jni_ptr))` — "TLS has a JNIEnv* but not
     * attached?" — and aborts the process.
     *
     * The two libraries do share threads: uniffi wakes a Rust future by
     * invoking the continuation callback inline on whichever thread completed
     * the work, which for a video frame is libwebrtc's own decode thread. That
     * makes the collision one JNA callback per delivered frame.
     *
     * `detach = false` is what fixes it. The name is a diagnostic bonus: a JNA
     * attach passes none, so the JVM assigns `Thread-<n>` from a process-wide
     * counter and pushes it down to the thread's `comm`, which is why a
     * tombstone from one of these threads names it `Thread-10694` and why
     * `ps -T` appears to show thousands of short-lived threads.
     *
     * Costs one JVM thread peer per native thread that ever runs a callback —
     * libwebrtc's pool plus our Tokio workers, created once.
     *
     * Must run before anything hands these callbacks to Rust, i.e. before
     * `UniffiLib.INSTANCE` is first touched.
     */
    private fun pinCallbackThreads() {
        val initializer = CallbackThreadInitializer(
            /* daemon = */ true,
            /* detach = */ false,
            "matrixrtc-jna-cb",
        )

        Native.setCallbackThreadInitializer(uniffiRustFutureContinuationCallbackImpl, initializer)
        Native.setCallbackThreadInitializer(uniffiForeignFutureFreeImpl, initializer)

        // Trait-interface methods are reached through generated vtable structs;
        // walking their fields keeps a regenerated binding with new methods
        // covered without another edit here.
        for (vtable in arrayOf(
            uniffiCallbackInterfaceRtcLogSink.vtable,
            uniffiCallbackInterfaceCommandSenderCallback.vtable,
        )) {
            pinVTableCallbacks(vtable, initializer)
        }

        // Media-only callback interfaces; see the per-variant source dirs.
        pinMediaCallbackThreads(initializer)
    }
}

/**
 * Pin every JNA callback reachable through one generated vtable struct.
 *
 * Top-level rather than a member so the per-variant `pinMediaCallbackThreads`
 * can reuse it.
 */
internal fun pinVTableCallbacks(vtable: Structure, initializer: CallbackThreadInitializer) {
    for (field in vtable.javaClass.fields) {
        (field.get(vtable) as? Callback)?.let {
            Native.setCallbackThreadInitializer(it, initializer)
        }
    }
}
