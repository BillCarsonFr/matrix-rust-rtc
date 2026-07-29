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

//! Apple-only linker fix for the static `libwebrtc` that `livekit` pulls in.
//!
//! WebRTC ships helpers such as `+[NSString stringForAbslStringView:]` as
//! Objective-C *categories*. Category methods live in object files that the
//! linker dead-strips from a static archive unless the whole member is loaded,
//! so at runtime libwebrtc hits "unrecognized selector" and aborts while
//! building its `PeerConnectionFactory`. `-ObjC` forces the linker to load every
//! archive member that defines an Objective-C class or category.
//!
//! `webrtc-sys` can't do this itself: a dependency's build script cannot inject
//! link args into a downstream binary — only the crate that owns the
//! binary/example/test targets can. Hence this lives here.

fn main() {
    // Examples and tests only: this crate ships no bin target (emitting
    // `rustc-link-arg-bins` without one is a hard error). Both kinds of
    // runnable artifact create libwebrtc's PeerConnectionFactory — the
    // `connect` example and the `e2e_call` integration test.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" || target_os == "ios" {
        println!("cargo:rustc-link-arg-examples=-ObjC");
        println!("cargo:rustc-link-arg-tests=-ObjC");
    }
}
