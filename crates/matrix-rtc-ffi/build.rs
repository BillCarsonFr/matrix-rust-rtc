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

//! Android-only linker fix for the static `libwebrtc` that `livekit` pulls in.
//!
//! libwebrtc's Java classes (`livekit.org.webrtc.*`, shipped in the bundled
//! `libwebrtc.jar`) are backed by `Java_livekit_org_webrtc_*` native methods
//! inside `libwebrtc.a`. Nothing in Rust references them, so the linker
//! dead-strips those archive members and the resulting `.so` exports zero
//! `Java_*` symbols. At runtime the very first Java→C++ call then fails with
//! "No implementation found for ..." — and because it happens inside
//! `PeerConnectionFactory` construction (`DefaultVideoEncoderFactory`, which
//! `RtcRuntime()` builds unconditionally), *no* session can be created, not
//! even audio-only. Worse, libwebrtc's JNI helpers call `NewStringUTF` without
//! an `ExceptionCheck`, so ART turns the pending exception into a SIGABRT
//! rather than an error we could surface.
//!
//! `webrtc_sys_build::configure_jni_symbols()` already fixes exactly this: it
//! reads the `Java_livekit_org_webrtc*` symbols out of `libwebrtc.a` and emits
//! a `-Wl,--undefined=` for each (so the members are pulled in) plus a
//! `--version-script` keeping them exported. `webrtc-sys`'s own build script
//! calls it — but it emits `cargo:rustc-link-arg`, and those apply only to the
//! *emitting* package's linked targets. `webrtc-sys` is an rlib, so nothing is
//! linked there and the flags are silently dropped on the way to our cdylib.
//! Only the crate that owns the cdylib can inject them, hence this file.
//!
//! This is the Android half of the same class of bug that
//! `matrix-rtc-livekit/build.rs` fixes with `-ObjC` for Apple targets. That one
//! lives in the livekit crate because it targets examples/tests; this one has
//! to live here because the artifact it fixes is *our* `cdylib`.

fn main() {
    // Nothing below depends on the crate sources.
    println!("cargo:rerun-if-changed=build.rs");

    #[cfg(feature = "media")]
    export_android_jni_symbols();
}

/// Re-emit libwebrtc's JNI link args against the cdylib being built.
///
/// Cheap: by the time this runs, `webrtc-sys`'s build script has already
/// downloaded libwebrtc (the `scratch` dir is keyed to the shared target
/// directory, not the calling package, so `download_webrtc()` sees an existing
/// directory and returns immediately) and the symbol scan is one `llvm-readelf`
/// over the archive.
///
/// The generated version script lists only `global:` symbols with no
/// `local: *;`, so everything else — the uniffi and cxxbridge exports — keeps
/// default visibility.
#[cfg(feature = "media")]
fn export_android_jni_symbols() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("android") {
        return;
    }

    // Hard failure, not a warning: a media AAR without these symbols builds
    // and installs fine, then aborts the host process the first time it tries
    // to open a session. `configure_jni_symbols` needs `ANDROID_NDK_HOME` to
    // point at a versioned NDK directory *verbatim* (it does no discovery of
    // its own); `scripts/build-android-aar.sh` normalises that before calling
    // cargo.
    webrtc_sys_build::configure_jni_symbols().expect(
        "failed to export libwebrtc's Java_* JNI symbols; without them the media \
         AAR aborts (SIGABRT) when libwebrtc builds its PeerConnectionFactory. \
         Check that ANDROID_NDK_HOME points at a versioned NDK directory \
         containing toolchains/llvm/prebuilt/<host>/bin/llvm-readelf",
    );
}
