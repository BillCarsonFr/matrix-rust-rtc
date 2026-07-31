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

//! Android initialisation for libwebrtc.
//!
//! libwebrtc requires the JVM before any peer connection can be created;
//! this `JNI_OnLoad` runs automatically when the host loads the library
//! (same pattern as livekit's own FFI). The AAR must also bundle
//! `libwebrtc.jar` — see `mobile/PACKAGING.md`.
//!
//! Only needed for media; audio/video *device* selection stays with the
//! platform (`AudioManager` etc.) — this SDK only moves raw frames.

use std::os::raw::c_void;

use jni::JavaVM;
use jni::sys::{JNI_VERSION_1_6, jint};

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn JNI_OnLoad(vm: JavaVM, _reserved: *mut c_void) -> jint {
    log::info!("JNI_OnLoad: initialising libwebrtc for Android");
    livekit::webrtc::android::initialize_android(&vm);
    JNI_VERSION_1_6
}
