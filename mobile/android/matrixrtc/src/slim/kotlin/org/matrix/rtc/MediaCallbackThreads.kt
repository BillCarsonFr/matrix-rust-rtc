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

/**
 * Signalling-only build: there are no media callback interfaces to pin.
 *
 * The media twin of this file pins `OpenIdTokenProvider`, which uniffi only
 * generates when the crate is built with the `media` feature. Splitting the two
 * by source dir keeps a reference to a type this variant lacks a compile error
 * instead of a reflective lookup that silently finds nothing — and what this
 * pinning prevents is a process abort, so failing quietly is the wrong failure.
 */
@Suppress("UNUSED_PARAMETER")
internal fun pinMediaCallbackThreads(initializer: CallbackThreadInitializer) = Unit
