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

//! The crate's task and timer primitives, per target.
//!
//! Off `wasm32` this is tokio: `spawn` requires `Send` futures and `sleep`
//! rides the runtime's timer driver. On `wasm32` there is no runtime — tasks
//! go to the JS microtask queue via `wasm_bindgen_futures::spawn_local` (no
//! `Send`, one thread) and sleeps are `setTimeout`-backed (`gloo-timers`).
//! `tokio::time` is not an option there: it panics at runtime on
//! `wasm32-unknown-unknown` (`Instant::now` is unimplemented), which is why
//! every engine task and timer goes through this seam instead.

use std::future::Future;
use std::time::Duration;

/// A spawned task, aborted on request. Dropping the handle detaches the task.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct TaskHandle(tokio::task::JoinHandle<()>);

#[cfg(not(target_arch = "wasm32"))]
impl TaskHandle {
    pub(crate) fn abort(&self) {
        self.0.abort();
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn spawn<F>(future: F) -> TaskHandle
where
    F: Future<Output = ()> + Send + 'static,
{
    TaskHandle(tokio::spawn(future))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// A spawned task, aborted on request. Dropping the handle detaches the task.
#[cfg(target_arch = "wasm32")]
pub(crate) struct TaskHandle(futures_util::future::AbortHandle);

#[cfg(target_arch = "wasm32")]
impl TaskHandle {
    pub(crate) fn abort(&self) {
        self.0.abort();
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn<F>(future: F) -> TaskHandle
where
    F: Future<Output = ()> + 'static,
{
    let (future, handle) = futures_util::future::abortable(future);
    wasm_bindgen_futures::spawn_local(async move {
        // Aborted is the expected outcome for a task cancelled via the handle.
        let _ = future.await;
    });
    TaskHandle(handle)
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn sleep(duration: Duration) {
    // setTimeout takes u32 milliseconds; the engine's longest delay is the
    // 30 s backoff cap, nowhere near the ~49-day u32 limit.
    let millis = u32::try_from(duration.as_millis()).unwrap_or(u32::MAX);
    gloo_timers::future::TimeoutFuture::new(millis).await;
}
