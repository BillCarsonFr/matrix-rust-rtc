//! The platform seam for the two things this crate must do on its own clock
//! and that no host can see: run a **detached task** and **wait for a
//! deadline**. Everything else is either a pure state transition or a driver
//! call.
//!
//! - native: a crate-owned current-thread tokio runtime parked on its own OS
//!   thread (`HANDLE`). It is deliberately *not* the runtime uniffi runs
//!   exported `async fn`s on — tasks are spawned from synchronous
//!   constructors and sink `emit`s, where no runtime context exists — the
//!   same pattern as matrix-rust-sdk's FFI. `sleep_ms` creates its timer
//!   under `HANDLE.enter()`, so it is driven by this runtime's timer wheel
//!   wherever the future is eventually polled.
//! - wasm32: `wasm_bindgen_futures::spawn_local` (the future runs on the JS
//!   microtask queue), `setTimeout` via gloo-timers, `Date.now()` via
//!   web-time. There is one thread, so nothing here is `Send`.
//!
//! Use these three functions and nothing else from tokio's `rt`/`time` or
//! from wasm-bindgen in the rest of the crate; `tokio::sync` stays available
//! everywhere (it is runtime-independent).

use std::future::Future;

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use super::*;
    use std::sync::LazyLock;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::runtime::{Builder, Handle};

    /// A current-thread runtime parked on its own OS thread. Current-thread
    /// (feature `rt`) rather than multi-thread on purpose: `rt-multi-thread`
    /// is not in tokio's wasm-allowed feature set, and the ubrn shim's v1
    /// feature resolver would unify it into the wasm build (see Cargo.toml).
    static HANDLE: LazyLock<Handle> = LazyLock::new(|| {
        let runtime = Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("matrix-rtc: failed to build the tokio runtime");
        let handle = runtime.handle().clone();
        std::thread::Builder::new()
            .name("matrix-rtc".into())
            .spawn(move || runtime.block_on(std::future::pending::<()>()))
            .expect("matrix-rtc: failed to spawn the runtime thread");
        handle
    });

    /// Run `future` to completion in the background.
    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        HANDLE.spawn(future);
    }

    /// Resolve after `ms` milliseconds.
    pub async fn sleep_ms(ms: u64) {
        let sleep = {
            let _guard = HANDLE.enter();
            tokio::time::sleep(Duration::from_millis(ms))
        };
        sleep.await
    }

    /// Wall clock, milliseconds since the Unix epoch.
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use super::*;
    use web_time::{SystemTime, UNIX_EPOCH};

    /// Run `future` to completion in the background (JS microtask queue).
    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        wasm_bindgen_futures::spawn_local(future);
    }

    /// Resolve after `ms` milliseconds (`setTimeout`).
    pub async fn sleep_ms(ms: u64) {
        gloo_timers::future::TimeoutFuture::new(ms.min(u32::MAX as u64) as u32).await;
    }

    /// Wall clock, milliseconds since the Unix epoch (`Date.now()`).
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

pub use imp::{now_ms, sleep_ms, spawn};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use tokio::sync::watch;

    #[test]
    fn spawn_needs_no_ambient_runtime_and_sleep_is_driven_by_ours() {
        // No #[tokio::test] on purpose: this is the situation of a uniffi
        // constructor or a sink `emit` — plain thread, no runtime context.
        let (tx, mut rx) = watch::channel(0u32);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        spawn(async move {
            sleep_ms(20).await;
            tx.send_replace(1);
        });
        spawn(async move {
            rx.changed().await.unwrap();
            done_tx.send(*rx.borrow_and_update()).unwrap();
        });
        let got = done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("spawned tasks never ran");
        assert_eq!(got, 1);
    }
}
