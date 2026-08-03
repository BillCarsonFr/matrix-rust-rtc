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

//! Logging for the native bindings.
//!
//! Every crate in this workspace emits through the [`log`] facade, and so do
//! `livekit` and `libwebrtc` — but a facade with no implementation installed is
//! a no-op. Nothing else in the process installs one for us: a host app that
//! embeds `matrix-sdk-ffi` links it as a *separate* `cdylib`, with its own copy
//! of the `log` and `tracing` globals, so its subscriber cannot see our
//! records. [`setup_logging`] is therefore the entry point that makes the SDK
//! observable at all, and hosts should call it before anything else.
//!
//! Records fan out to two independent sinks:
//!
//! * the platform log — logcat on Android, stderr elsewhere — so an integrator
//!   gets useful output from `adb logcat -s matrix-rtc` with zero Kotlin code;
//! * an optional [`RtcLogSink`] implemented by the host, for routing into its
//!   own file/rageshake pipeline.
//!
//! Filtering uses `RUST_LOG` syntax (see [`RtcLogConfig::filter`]) against
//! module-path targets, so `matrix_rtc_core::session=trace` works the way
//! anyone who has used `RUST_LOG` expects.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use log::{Level, LevelFilter, Log, Metadata, Record};

use crate::MatrixRtcFfiError;

/// How many records may be queued for the host [`RtcLogSink`] before new ones
/// are dropped. Bounded, and dropping rather than blocking, so a slow host sink
/// cannot stall an audio or video thread.
const SINK_QUEUE_CAPACITY: usize = 4096;

/// Log every this-many dropped records, so overflow is visible without the
/// warning itself becoming the flood.
const DROP_REPORT_INTERVAL: u64 = 1024;

/// Severity of a log record. Mirrors [`log::Level`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, uniffi::Enum)]
pub enum RtcLogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<RtcLogLevel> for LevelFilter {
    fn from(level: RtcLogLevel) -> Self {
        match level {
            RtcLogLevel::Error => LevelFilter::Error,
            RtcLogLevel::Warn => LevelFilter::Warn,
            RtcLogLevel::Info => LevelFilter::Info,
            RtcLogLevel::Debug => LevelFilter::Debug,
            RtcLogLevel::Trace => LevelFilter::Trace,
        }
    }
}

impl From<RtcLogLevel> for Level {
    fn from(level: RtcLogLevel) -> Self {
        match level {
            RtcLogLevel::Error => Level::Error,
            RtcLogLevel::Warn => Level::Warn,
            RtcLogLevel::Info => Level::Info,
            RtcLogLevel::Debug => Level::Debug,
            RtcLogLevel::Trace => Level::Trace,
        }
    }
}

impl From<Level> for RtcLogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => RtcLogLevel::Error,
            Level::Warn => RtcLogLevel::Warn,
            Level::Info => RtcLogLevel::Info,
            Level::Debug => RtcLogLevel::Debug,
            Level::Trace => RtcLogLevel::Trace,
        }
    }
}

/// What [`setup_logging`] should install.
#[derive(Clone, Debug, uniffi::Record)]
pub struct RtcLogConfig {
    /// Level applied to every target that no `filter` directive matches.
    pub level: RtcLogLevel,

    /// `RUST_LOG`-style per-target overrides, e.g.
    /// `"matrix_rtc_core::session=trace,livekit=info,webrtc_sys=warn"`.
    ///
    /// Targets are module paths, so a directive matches by prefix: the
    /// filterable roots are `matrix_rtc_core`, `matrix_rtc_media`,
    /// `matrix_rtc_livekit`, `matrix_rtc_ffi`, plus third-party `livekit` and
    /// `webrtc_sys`. Empty means "no overrides".
    #[uniffi(default = "")]
    pub filter: String,

    /// Write to the platform log (logcat on Android, stderr elsewhere).
    ///
    /// Turn off when the host installs an [`RtcLogSink`] and does not want the
    /// records duplicated.
    #[uniffi(default = true)]
    pub write_to_system: bool,
}

/// One log record, as handed to a host [`RtcLogSink`].
#[derive(Clone, Debug, uniffi::Record)]
pub struct RtcLogRecord {
    pub level: RtcLogLevel,
    /// Module path of the emitting code, e.g. `matrix_rtc_core::session`.
    pub target: String,
    pub message: String,
    /// Milliseconds since the Unix epoch, captured when the record was emitted
    /// rather than when the host receives it — delivery is asynchronous.
    pub timestamp_ms: u64,
    pub thread: String,
    pub file: Option<String>,
    pub line: Option<u32>,
}

/// A host-supplied destination for log records.
///
/// Implementations are called from a single dedicated thread, never from the
/// thread that emitted the record, so they may block without stalling the SDK.
/// Records logged from inside `log` are discarded rather than re-queued, so an
/// implementation that logs is safe (but will not see its own lines here).
#[uniffi::export(with_foreign)]
pub trait RtcLogSink: Send + Sync {
    fn log(&self, record: RtcLogRecord);
}

/// Installs the logger, or reconfigures it if already installed.
///
/// Safe to call repeatedly: `log::set_logger` can only be called once per
/// process, so subsequent calls swap the filter, the platform-log setting and
/// the sink in place. Also installs a panic hook that logs the payload and a
/// backtrace before delegating to the previously installed hook — without it a
/// panic on an SDK background thread is an unexplained `SIGABRT`.
///
/// Returns [`MatrixRtcFfiError::InvalidInput`] if `config.filter` is not a
/// valid `RUST_LOG` spec; the previous configuration is left untouched in that
/// case.
#[uniffi::export]
pub fn setup_logging(
    config: RtcLogConfig,
    sink: Option<Arc<dyn RtcLogSink>>,
) -> Result<(), MatrixRtcFfiError> {
    let filter = build_filter(&config)?;
    let max_level = filter.filter();

    let logger = RtcLogger::global();
    {
        let mut state = logger
            .state
            .write()
            .map_err(|_| MatrixRtcFfiError::InternalLockPoisoned)?;
        state.filter = filter;
        state.write_to_system = config.write_to_system;
        state.sink = sink;
    }

    log::set_max_level(max_level);
    // Err means a logger is already installed. If it is ours the reconfigure
    // above already took effect; if it is the host's, the host wins and we do
    // not fight it.
    let _ = log::set_logger(logger);

    install_panic_hook();

    log::info!(
        "logging configured: level={:?} filter={:?} system={} sink={}",
        config.level,
        config.filter,
        config.write_to_system,
        logger.has_sink(),
    );

    Ok(())
}

/// Emits a record from the host into the same stream as the SDK's own, so the
/// host's lines and ours interleave in one timeline.
#[uniffi::export]
pub fn log_event(level: RtcLogLevel, target: String, message: String) {
    log::log!(target: &target, level.into(), "{message}");
}

/// Number of records dropped because the host sink could not keep up.
#[uniffi::export]
pub fn dropped_log_record_count() -> u64 {
    RtcLogger::global().dropped.load(Ordering::Relaxed)
}

// --- implementation --------------------------------------------------------

fn build_filter(config: &RtcLogConfig) -> Result<env_filter::Filter, MatrixRtcFfiError> {
    let mut builder = env_filter::Builder::new();
    builder.filter_level(config.level.into());

    let spec = config.filter.trim();
    if !spec.is_empty() {
        builder.try_parse(spec).map_err(|err| {
            MatrixRtcFfiError::InvalidInput(format!("invalid log filter {spec:?}: {err}"))
        })?;
    }

    Ok(builder.build())
}

thread_local! {
    /// Set on the sink-delivery thread. A host sink that logs would otherwise
    /// feed its own records back into the queue it is draining.
    static ON_SINK_THREAD: Cell<bool> = const { Cell::new(false) };
}

struct LoggerState {
    filter: env_filter::Filter,
    write_to_system: bool,
    sink: Option<Arc<dyn RtcLogSink>>,
}

struct RtcLogger {
    state: RwLock<LoggerState>,
    sink_tx: SyncSender<RtcLogRecord>,
    dropped: AtomicU64,
    #[cfg(target_os = "android")]
    logcat: android_logger::AndroidLogger,
}

static LOGGER: OnceLock<RtcLogger> = OnceLock::new();

impl RtcLogger {
    fn global() -> &'static RtcLogger {
        LOGGER.get_or_init(|| {
            let (sink_tx, sink_rx) = sync_channel(SINK_QUEUE_CAPACITY);
            spawn_sink_thread(sink_rx);

            RtcLogger {
                state: RwLock::new(LoggerState {
                    // Replaced by `setup_logging` before this is reachable from
                    // `log`; `Off` keeps it silent if it somehow is not.
                    filter: env_filter::Builder::new()
                        .filter_level(LevelFilter::Off)
                        .build(),
                    write_to_system: false,
                    sink: None,
                }),
                sink_tx,
                dropped: AtomicU64::new(0),
                #[cfg(target_os = "android")]
                logcat: android_logger::AndroidLogger::new(
                    android_logger::Config::default()
                        // Our own filter has already run by the time we hand a
                        // record over; a second one would only mask it.
                        .with_max_level(LevelFilter::Trace)
                        .with_tag("matrix-rtc"),
                ),
            }
        })
    }

    fn has_sink(&self) -> bool {
        self.state.read().is_ok_and(|state| state.sink.is_some())
    }

    fn write_to_system(&self, record: &Record<'_>) {
        #[cfg(target_os = "android")]
        {
            self.logcat.log(record);
        }

        #[cfg(not(target_os = "android"))]
        {
            use std::fmt::Write as _;
            use std::io::Write as _;

            let mut line = String::with_capacity(128);
            let _ = write!(
                line,
                "{} {:<5} {}: {}",
                format_wall_clock(now_ms()),
                record.level(),
                record.target(),
                record.args(),
            );
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "{line}");
        }
    }

    fn queue_for_sink(&self, record: &Record<'_>) {
        // Delivering a record emitted *by* the sink would either recurse or, in
        // the common case of a full queue, count as a drop and provoke another
        // record. Neither is worth the one line.
        if ON_SINK_THREAD.with(Cell::get) {
            return;
        }

        match self.sink_tx.try_send(to_ffi_record(record)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped.is_multiple_of(DROP_REPORT_INTERVAL) {
                    // Straight to the platform log: the queue is what is
                    // broken, so routing this through it would be futile.
                    self.write_to_system(
                        &Record::builder()
                            .level(Level::Warn)
                            .target(module_path!())
                            .args(format_args!(
                                "log sink is not keeping up: {dropped} records dropped"
                            ))
                            .build(),
                    );
                }
            }
            // The drain thread is gone; nothing to be done, and the platform
            // log still works.
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

impl Log for RtcLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.state
            .read()
            .is_ok_and(|state| state.filter.enabled(metadata))
    }

    fn log(&self, record: &Record<'_>) {
        let Ok(state) = self.state.read() else {
            return;
        };

        if !state.filter.matches(record) {
            return;
        }

        let write_to_system = state.write_to_system;
        let has_sink = state.sink.is_some();
        drop(state);

        if write_to_system {
            self.write_to_system(record);
        }

        if has_sink {
            self.queue_for_sink(record);
        }
    }

    fn flush(&self) {}
}

fn spawn_sink_thread(records: Receiver<RtcLogRecord>) {
    let spawned = std::thread::Builder::new()
        .name("matrix-rtc-log".to_owned())
        .spawn(move || {
            ON_SINK_THREAD.with(|on_sink_thread| on_sink_thread.set(true));

            // Ends when the sender is dropped, which only happens if the
            // process is tearing down: the logger itself is `'static`.
            for record in records {
                // Re-read the sink per record so `setup_logging` can swap it
                // without restarting the thread.
                let sink = LOGGER
                    .get()
                    .and_then(|logger| logger.state.read().ok()?.sink.clone());

                if let Some(sink) = sink {
                    sink.log(record);
                }
            }
        });

    if spawned.is_err() {
        // Without the drain thread the queue fills once and then every record
        // counts as dropped; the platform log is unaffected.
        eprintln!("matrix-rtc: could not spawn the log sink thread; host sink disabled");
    }
}

fn to_ffi_record(record: &Record<'_>) -> RtcLogRecord {
    RtcLogRecord {
        level: record.level().into(),
        target: record.target().to_owned(),
        message: record.args().to_string(),
        timestamp_ms: now_ms(),
        thread: std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_owned(),
        file: record.file().map(str::to_owned),
        line: record.line(),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since_epoch| since_epoch.as_millis() as u64)
        .unwrap_or_default()
}

/// `HH:MM:SS.mmm` in UTC, for the stderr sink.
///
/// Hand-rolled because the alternative is a date/time dependency for one line
/// of formatting; logcat stamps its own records, so this is the only caller.
#[cfg(not(target_os = "android"))]
fn format_wall_clock(timestamp_ms: u64) -> String {
    let seconds_today = (timestamp_ms / 1000) % 86_400;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        seconds_today / 3600,
        (seconds_today % 3600) / 60,
        seconds_today % 60,
        timestamp_ms % 1000,
    )
}

static PANIC_HOOK: std::sync::Once = std::sync::Once::new();

fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            log::error!(
                target: "matrix_rtc_ffi::panic",
                "panic on thread {:?}: {info}\n{}",
                std::thread::current().name().unwrap_or("unnamed"),
                std::backtrace::Backtrace::force_capture(),
            );
            previous(info);
        }));
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CollectingSink {
        records: Mutex<Vec<RtcLogRecord>>,
    }

    impl CollectingSink {
        fn messages(&self) -> Vec<String> {
            self.records
                .lock()
                .unwrap()
                .iter()
                .map(|record| record.message.clone())
                .collect()
        }
    }

    impl RtcLogSink for CollectingSink {
        fn log(&self, record: RtcLogRecord) {
            self.records.lock().unwrap().push(record);
        }
    }

    fn config(level: RtcLogLevel, filter: &str) -> RtcLogConfig {
        RtcLogConfig {
            level,
            filter: filter.to_owned(),
            // Tests must not spray the test runner's stderr.
            write_to_system: false,
        }
    }

    /// Waits for the drain thread to hand `expected` to the sink, and returns
    /// everything the sink has by then.
    ///
    /// Waits for that specific message rather than a record count: the logger is
    /// process-wide, so the sink also receives whatever the other tests in this
    /// binary emit from `matrix_rtc_core` targets.
    fn wait_for(sink: &CollectingSink, expected: &str) -> Vec<String> {
        for _ in 0..200 {
            let messages = sink.messages();
            if messages.iter().any(|message| message == expected) {
                return messages;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        sink.messages()
    }

    #[test]
    #[cfg(not(target_os = "android"))]
    fn wall_clock_formatting_splits_the_epoch_into_time_of_day() {
        // 1970-01-01T00:00:00.000Z, then +1ms, and the last millisecond of a day.
        assert_eq!(format_wall_clock(0), "00:00:00.000");
        assert_eq!(format_wall_clock(1), "00:00:00.001");
        assert_eq!(format_wall_clock(86_399_999), "23:59:59.999");
        // Rolls over rather than running past 24h.
        assert_eq!(format_wall_clock(86_400_000), "00:00:00.000");
        // 2026-08-03T12:34:56.789Z.
        assert_eq!(format_wall_clock(1_785_760_496_789), "12:34:56.789");
    }

    #[test]
    fn an_invalid_filter_is_rejected_without_disturbing_the_current_config() {
        let error = build_filter(&config(RtcLogLevel::Info, "core=nonsense")).unwrap_err();

        assert!(
            matches!(error, MatrixRtcFfiError::InvalidInput(ref message) if message.contains("core=nonsense")),
            "unexpected error: {error:?}",
        );
    }

    #[test]
    fn per_target_directives_override_the_baseline_level() {
        let filter = build_filter(&config(
            RtcLogLevel::Warn,
            "matrix_rtc_core=trace,livekit=error",
        ))
        .unwrap();

        let enabled = |target: &str, level: Level| {
            filter.enabled(&Metadata::builder().target(target).level(level).build())
        };

        // Prefix match reaches submodules.
        assert!(enabled("matrix_rtc_core::session", Level::Trace));
        assert!(!enabled("livekit::room", Level::Warn));
        // Unmatched targets fall back to the baseline.
        assert!(enabled("matrix_rtc_media::engine", Level::Warn));
        assert!(!enabled("matrix_rtc_media::engine", Level::Info));
        // `set_max_level` must not clip the most permissive directive.
        assert_eq!(filter.filter(), LevelFilter::Trace);
    }

    /// One test drives the whole global-logger lifecycle: `log::set_logger` is
    /// process-wide, so splitting this up would make the cases race.
    #[test]
    fn the_installed_logger_filters_and_delivers_to_the_host_sink() {
        let sink = Arc::new(CollectingSink::default());

        setup_logging(
            config(RtcLogLevel::Info, "matrix_rtc_core=debug"),
            Some(sink.clone()),
        )
        .unwrap();

        // Emitted from a `matrix_rtc_core` target: passes the directive.
        log::log!(target: "matrix_rtc_core::session", Level::Debug, "kept: core debug");
        // Below the baseline for an unmatched target: filtered out.
        log::log!(target: "some_other_crate", Level::Debug, "dropped: other debug");
        // The host's own entry point lands in the same stream.
        log_event(
            RtcLogLevel::Info,
            "host".to_owned(),
            "kept: from the host".to_owned(),
        );

        // The host record is emitted last, so seeing it means the two before it
        // have been drained too.
        let messages = wait_for(&sink, "kept: from the host");
        assert!(
            messages.iter().any(|m| m == "kept: core debug"),
            "missing core record in {messages:?}",
        );
        assert!(
            messages.iter().any(|m| m == "kept: from the host"),
            "missing host record in {messages:?}",
        );
        assert!(
            !messages.iter().any(|m| m.starts_with("dropped:")),
            "filtered record leaked into {messages:?}",
        );

        // Reconfiguring is allowed and takes effect: same process, second call.
        let second = Arc::new(CollectingSink::default());
        setup_logging(config(RtcLogLevel::Error, ""), Some(second.clone())).unwrap();

        log::log!(target: "matrix_rtc_core::session", Level::Debug, "dropped: below new level");
        log::log!(target: "matrix_rtc_core::session", Level::Error, "kept: error");

        let messages = wait_for(&second, "kept: error");
        assert!(
            messages.iter().any(|m| m == "kept: error"),
            "missing error record in {messages:?}",
        );
        assert!(
            !messages.iter().any(|m| m.starts_with("dropped:")),
            "stale filter still in effect: {messages:?}",
        );

        // Detaching the sink must not fail or panic.
        setup_logging(config(RtcLogLevel::Error, ""), None).unwrap();
    }
}
