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

import uniffi.matrix_rtc_ffi.MatrixRtcFfiException
import uniffi.matrix_rtc_ffi.RtcLogConfig
import uniffi.matrix_rtc_ffi.RtcLogLevel
import uniffi.matrix_rtc_ffi.RtcLogRecord
import uniffi.matrix_rtc_ffi.RtcLogSink
import uniffi.matrix_rtc_ffi.logEvent
import uniffi.matrix_rtc_ffi.setupLogging

/**
 * Turns on logging in the Rust SDK.
 *
 * Nothing the SDK logs is visible until one of these is called: the Rust side
 * uses the `log` facade, which discards everything until an implementation is
 * installed. Call this once, before creating an `RtcSessionManagerHandle`.
 *
 * This is a thin convenience wrapper over the generated
 * `uniffi.matrix_rtc_ffi.setupLogging`; use that directly if you need the full
 * [RtcLogConfig].
 *
 * Every entry point here calls [MatrixRtc.initialize] first, so setting logging
 * up as the app's first SDK call also loads the native library. Hosts that skip
 * logging must call [MatrixRtc.initialize] themselves.
 */
object RtcLogging {

    /**
     * Sends SDK logs to logcat under the tag `matrix-rtc`.
     *
     * ```kotlin
     * RtcLogging.initLogcat(RtcLogLevel.DEBUG, "matrix_rtc_core::session=trace")
     * ```
     *
     * Then: `adb logcat -s matrix-rtc`
     *
     * @param level level for targets no [filter] directive matches.
     * @param filter `RUST_LOG`-style per-target overrides, e.g.
     *   `"matrix_rtc_core=trace,livekit=info,webrtc_sys=warn"`. Targets are Rust
     *   module paths and match by prefix; the roots are `matrix_rtc_core`,
     *   `matrix_rtc_media`, `matrix_rtc_livekit`, `matrix_rtc_ffi`, plus
     *   third-party `livekit` and `webrtc_sys`.
     * @throws MatrixRtcFfiException if [filter] is not a valid `RUST_LOG` spec.
     */
    @JvmStatic
    @JvmOverloads
    @Throws(MatrixRtcFfiException::class)
    fun initLogcat(level: RtcLogLevel = RtcLogLevel.DEBUG, filter: String = "") {
        MatrixRtc.initialize()
        setupLogging(
            RtcLogConfig(level = level, filter = filter, writeToSystem = true),
            sink = null,
        )
    }

    /**
     * Sends SDK logs to [sink] — for routing into Timber, a rageshake log file,
     * or any host-side pipeline.
     *
     * [sink] is called on a dedicated Rust thread, never on the thread that
     * emitted the record, so it may block. Records it logs itself are dropped
     * rather than fed back, so logging from inside it is safe.
     *
     * @param alsoToLogcat also write to logcat, in addition to [sink].
     * @throws MatrixRtcFfiException if [filter] is not a valid `RUST_LOG` spec.
     */
    @JvmStatic
    @JvmOverloads
    @Throws(MatrixRtcFfiException::class)
    fun init(
        level: RtcLogLevel = RtcLogLevel.DEBUG,
        filter: String = "",
        alsoToLogcat: Boolean = false,
        sink: (RtcLogRecord) -> Unit,
    ) {
        MatrixRtc.initialize()
        setupLogging(
            RtcLogConfig(level = level, filter = filter, writeToSystem = alsoToLogcat),
            sink = object : RtcLogSink {
                override fun log(record: RtcLogRecord) = sink(record)
            },
        )
    }

    /**
     * Writes a host log line into the same stream as the SDK's own, so app and
     * SDK lines interleave in one timeline.
     */
    @JvmStatic
    @JvmOverloads
    fun log(level: RtcLogLevel, message: String, target: String = "app") {
        MatrixRtc.initialize()
        logEvent(level, target, message)
    }
}
