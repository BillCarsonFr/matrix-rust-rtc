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

//! The runtime the media layer runs on.
//!
//! The exported handle methods are `async` and driven by uniffi's own Tokio
//! integration (`async_runtime = "tokio"`), so they need nothing from here.
//! The media layer does: `connect_media_session` hops onto this runtime so every
//! task it spawns afterwards — the engine actor, the connection pool, IO —
//! inherits the same context regardless of which thread the FFI call arrived on.
//!
//! It must be multi-threaded. The core arms a `tokio::time::sleep` on the
//! key-rotation path (the MSC4143 `delayBeforeUse` wait), and a current-thread
//! runtime with nothing driving it would leave that timer to never fire.

/// The process-wide runtime backing the media layer.
#[cfg(feature = "media")]
pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("matrix-rtc")
            .build()
            .expect("failed to build the matrix-rtc tokio runtime")
    })
}
