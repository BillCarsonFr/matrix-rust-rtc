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

//! The runtime the synchronous FFI entry points drive their futures on.
//!
//! The exported handle methods are synchronous, so each one bridges to the
//! core's async API by blocking. That bridge **must** supply a Tokio runtime
//! context: the core arms a `tokio::time::sleep` on the key-rotation path
//! (`matrix_rtc_core::encryption`, the MSC4195 `delayBeforeUse` wait), and
//! `tokio::time` panics with "there is no reactor running, must be called from
//! the context of a Tokio 1.x runtime" if none is installed on the calling
//! thread.
//!
//! `futures::executor::block_on` — what these entry points used to call — does
//! not install one. On a host thread (a JVM thread on Android, say) that panic
//! is guaranteed the first time a peer joins late enough to trigger a rotation,
//! which is why the first participant always worked and the second never did.
//!
//! Owning the runtime here rather than taking a `Handle` from the caller is
//! deliberate: `Handle::current()` would have to be captured at construction
//! time, and construction is itself a synchronous FFI call from a host thread
//! with no runtime to capture.

use std::future::Future;

/// The process-wide runtime backing every FFI entry point.
///
/// Multi-threaded on purpose. The blocking bridge below polls its future on the
/// *calling* thread, so the time and IO drivers have to be advanced by someone
/// else — a current-thread runtime would install a context whose timers nothing
/// is driving, and the `delayBeforeUse` sleep would simply never fire.
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

/// Runs `future` to completion from a synchronous FFI entry point.
///
/// Note the futures reaching this are deliberately **not** `Send` (the core
/// invokes host callbacks whose futures need not be), which is why this blocks
/// rather than spawning: `block_on` polls on the calling thread and imposes no
/// `Send` bound, while `spawn` would require both.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        // Already inside a runtime — reached through a callback that the media
        // runtime is driving, for instance. `Runtime::block_on` would panic
        // ("Cannot start a runtime from within a runtime"), and we do not need
        // it to: the context the core wants is already installed on this
        // thread, and the surrounding multi-threaded runtime keeps driving its
        // timers while this one thread blocks.
        Ok(_) => futures::executor::block_on(future),
        Err(_) => runtime().block_on(future),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    /// The regression test for the Android crash: a synchronous entry point is
    /// called from a host thread with no runtime of its own, and the future it
    /// runs arms a timer. Under `futures::executor::block_on` this panicked with
    /// "there is no reactor running".
    #[test]
    fn block_on_supplies_a_timer_with_no_ambient_runtime() {
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "this test is only meaningful on a thread with no runtime installed"
        );

        super::block_on(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
        });
    }

    /// The `Handle::try_current` branch: reached through a callback the media
    /// runtime is driving. `Runtime::block_on` would panic here ("Cannot start a
    /// runtime from within a runtime"), and the timer must still fire.
    #[test]
    fn block_on_nests_inside_a_runtime() {
        super::runtime().block_on(async {
            super::block_on(async {
                tokio::time::sleep(Duration::from_millis(1)).await;
            });
        });
    }
}
