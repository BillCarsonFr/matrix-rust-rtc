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

import com.sun.jna.CallbackThreadInitializer
import uniffi.matrix_rtc_ffi.uniffiCallbackInterfaceOpenIdTokenProvider

/**
 * Media build: pin the callback interfaces that only the `media` feature's
 * bindings generate.
 *
 * `OpenIdTokenProvider` is declared in `crates/matrix-rtc-ffi/src/media/`, so
 * uniffi emits no `uniffiCallbackInterfaceOpenIdTokenProvider` for the slim
 * artifact and naming it from shared code fails that build. The slim twin of
 * this file is a no-op; see [MatrixRtc.initialize] for why any of it happens.
 */
internal fun pinMediaCallbackThreads(initializer: CallbackThreadInitializer) {
    pinVTableCallbacks(uniffiCallbackInterfaceOpenIdTokenProvider.vtable, initializer)
}
