//! Test-only FFI surface (feature `runtime-probe`) that pins the runtime
//! assumptions the crate rests on, through the *real* bindings on every
//! platform — the wasm one in particular, where there is no tokio runtime:
//!
//! 1. an exported `async fn` awaiting `tokio::sync::watch::Receiver::changed`
//!    resolves when a synchronous FFI call (`set`) sends on the channel;
//! 2. a detached task ([`crate::executor::spawn`]) keeps running after the
//!    FFI call that started it returned, and can call back into the host;
//! 3. a timer ([`crate::executor::sleep_ms`]) fires, both inside an exported
//!    `async fn` and inside a detached task.
//!
//! `web-test-app/test/runtimeProbe.test.ts` runs these against the wasm
//! build; `executor::tests` covers native.

use std::sync::Arc;
use tokio::sync::watch;

use crate::executor::{now_ms, sleep_ms, spawn};

#[uniffi::export(with_foreign)]
pub trait ProbeListener: Send + Sync {
    fn on_value(&self, value: u32);
    fn on_closed(&self);
}

#[derive(uniffi::Object)]
pub struct RuntimeProbe {
    tx: watch::Sender<u32>,
}

#[cfg_attr(not(target_arch = "wasm32"), uniffi::export(async_runtime = "tokio"))]
#[cfg_attr(target_arch = "wasm32", uniffi::export)]
impl RuntimeProbe {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        let (tx, _rx) = watch::channel(0);
        Arc::new(Self { tx })
    }

    /// Synchronous send on the watch channel.
    pub fn set(&self, value: u32) {
        self.tx.send_replace(value);
    }

    pub fn current(&self) -> u32 {
        *self.tx.borrow()
    }

    /// Resolves with the next value sent after this call.
    pub async fn next_change(&self) -> u32 {
        let mut rx = self.tx.subscribe();
        // Err only when the sender is gone, which cannot happen while `self`
        // is alive.
        let _ = rx.changed().await;
        *rx.borrow_and_update()
    }

    /// Detached task: forwards every change to the listener until the probe
    /// is dropped, then calls `on_closed`.
    pub fn spawn_forwarder(&self, listener: Arc<dyn ProbeListener>) {
        let mut rx = self.tx.subscribe();
        spawn(async move {
            while rx.changed().await.is_ok() {
                let value = *rx.borrow_and_update();
                listener.on_value(value);
            }
            listener.on_closed();
        });
    }

    /// Detached task with a timer: sends `value` after `delay_ms`.
    pub fn set_after(&self, delay_ms: u64, value: u32) {
        let tx = self.tx.clone();
        spawn(async move {
            sleep_ms(delay_ms).await;
            tx.send_replace(value);
        });
    }

    /// Timer inside an exported future; returns the measured elapsed ms.
    pub async fn sleep(&self, ms: u64) -> u64 {
        let start = now_ms();
        sleep_ms(ms).await;
        now_ms().saturating_sub(start)
    }

    pub fn now_ms(&self) -> u64 {
        now_ms()
    }
}
