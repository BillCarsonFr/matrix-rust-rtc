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

//! Logging for the wasm bindings.
//!
//! The counterpart of `matrix-rtc-ffi`'s `logging` module: the core emits
//! through the [`log`] facade, which does nothing until an implementation is
//! installed, so [`init_logging`] is what makes the SDK observable in a browser.
//! Records go to the JS console, and the filter spec is the same `RUST_LOG`
//! syntax the native bindings accept.

use std::sync::{OnceLock, RwLock};

use log::{LevelFilter, Log, Metadata, Record};
use wasm_bindgen::prelude::*;

/// Installs the logger, or reconfigures it if already installed.
///
/// `level` is one of `error`, `warn`, `info`, `debug`, `trace`. `filter` holds
/// `RUST_LOG`-style per-target overrides against module-path targets, e.g.
/// `"matrix_rtc_core::session=trace,matrix_rtc_wasm=debug"`; pass an empty
/// string for none.
///
/// Also installs a panic hook that reports Rust panics to the console instead
/// of leaving an opaque `unreachable executed` trap.
///
/// ```js
/// import { initLogging } from "matrix-rtc-wasm";
/// initLogging("debug", "matrix_rtc_core::session=trace");
/// ```
#[wasm_bindgen(js_name = initLogging)]
pub fn init_logging(level: &str, filter: &str) -> Result<(), JsError> {
    let level = parse_level(level)?;

    let mut builder = env_filter::Builder::new();
    builder.filter_level(level);

    let spec = filter.trim();
    if !spec.is_empty() {
        builder
            .try_parse(spec)
            .map_err(|err| JsError::new(&format!("invalid log filter {spec:?}: {err}")))?;
    }
    let filter = builder.build();
    let max_level = filter.filter();

    let logger = WasmLogger::global();
    match logger.filter.write() {
        Ok(mut current) => *current = filter,
        Err(_) => return Err(JsError::new("the logger lock is poisoned")),
    }

    log::set_max_level(max_level);
    // Err means a logger is already installed; if it is ours the reconfigure
    // above already took effect.
    let _ = log::set_logger(logger);

    console_error_panic_hook::set_once();

    log::info!("logging configured: level={level} filter={spec:?}");

    Ok(())
}

/// Emits a record from JS into the same stream as the SDK's own, so host and
/// SDK lines interleave in one console timeline.
#[wasm_bindgen(js_name = logEvent)]
pub fn log_event(level: &str, target: &str, message: &str) -> Result<(), JsError> {
    let level = parse_level(level)?
        .to_level()
        .ok_or_else(|| JsError::new("level 'off' cannot be used to emit a record"))?;

    log::log!(target: target, level, "{message}");

    Ok(())
}

fn parse_level(level: &str) -> Result<LevelFilter, JsError> {
    level.parse::<LevelFilter>().map_err(|_| {
        JsError::new(&format!(
            "unknown log level {level:?}; expected error, warn, info, debug or trace",
        ))
    })
}

struct WasmLogger {
    filter: RwLock<env_filter::Filter>,
}

static LOGGER: OnceLock<WasmLogger> = OnceLock::new();

impl WasmLogger {
    fn global() -> &'static WasmLogger {
        LOGGER.get_or_init(|| WasmLogger {
            // Replaced by `init_logging` before this is reachable from `log`.
            filter: RwLock::new(
                env_filter::Builder::new()
                    .filter_level(LevelFilter::Off)
                    .build(),
            ),
        })
    }
}

impl Log for WasmLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.filter
            .read()
            .is_ok_and(|filter| filter.enabled(metadata))
    }

    fn log(&self, record: &Record<'_>) {
        let Ok(filter) = self.filter.read() else {
            return;
        };
        if !filter.matches(record) {
            return;
        }
        drop(filter);

        console_log::log(record);
    }

    fn flush(&self) {}
}
