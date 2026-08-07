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

//! Load generator: log in N devices of one account, join them all to the same
//! MatrixRTC call, and have each publish a video file as its camera track.
//!
//! Meant to be run while a human watches the same call in Element Web and
//! inspects SFU/homeserver/client performance. The tool measures nothing about
//! the deployment itself — it only reports its own health (frames actually
//! captured per device), so a machine that ran out of CPU is not mistaken for a
//! server that fell over.
//!
//! N devices of one user is equivalent to N users here: membership is keyed on
//! a random per-join `member_id`, the SFU identity hashes `(user, device,
//! member_id)`, and the core sends media keys to other devices of our own user
//! like any other peer. Each virtual participant is a **real, separate login** —
//! reusing one device id would make the participations shadow each other.
//!
//! ```sh
//! cargo run --release -p matrix-rtc-livekit --example load_test \
//!     --features matrix-sdk -- \
//!     --user loadtest --room '!room:synapse' --video clip.mp4 --devices 5
//! ```
//!
//! Needs `ffmpeg` on `PATH` unless the input is already `.y4m` or raw `.yuv`.
//!
//! **Cross-signing is mandatory.** A run that logs in fresh devices needs them
//! cross-signed, because MSC4153 (enforced by the core, and by the SDK's
//! identity-based to-device strategy) only lets cross-signed devices exchange
//! media keys. Pass the account's recovery key; the first run of
//! `join_and_record` against a fresh account prints one.
//!
//! Devices are deleted on exit. If the process is killed hard, `--purge-devices`
//! removes whatever it left behind.
//!
//! `--store <folder>` changes both of those: devices and their crypto stores
//! persist between runs, so logins (and the rate limiting that comes with them)
//! happen once, and a restored device can still decrypt member events sent
//! before the run began. Devices are then kept on exit, and `--purge-devices`
//! clears the folder along with them.

use std::error::Error;
use std::fmt::Write as _;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use matrix_rtc_core::SlotEncryption;
use matrix_rtc_livekit::compat::ElementCallCompat;
use matrix_rtc_livekit::{Call, CallOptions, open_slot};
use matrix_rtc_media::{
    AudioFrame, AudioSourceConfig, I420Buffer, LocalTrackHandle, PublishOptions, VideoFrame,
    VideoRotation, VideoSourceConfig,
};
use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::encryption::EncryptionSettings;
use matrix_sdk::ruma::api::client::uiaa;
use matrix_sdk::ruma::api::error::{ErrorKind, RetryAfter};
use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
use matrix_sdk::ruma::{OwnedDeviceId, RoomId};
use matrix_sdk::{Client, Room};
use matrix_sdk_ui::sync_service::SyncService;
use tokio::signal::unix::{SignalKind, signal};

/// Clap-facing mirror of [`ElementCallCompat`], which lives in a crate that
/// should not grow a `clap` dependency for a delete-by-date module.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
enum ElementCallCompatArg {
    /// Current MSC4143 + MSC4354 only.
    Off,
    /// Element Call as of 2025: sticky events with the pre-2026 field names.
    Sticky,
    /// Element Call before MSC4354: membership as room state.
    State,
}

impl From<ElementCallCompatArg> for ElementCallCompat {
    fn from(arg: ElementCallCompatArg) -> Self {
        match arg {
            ElementCallCompatArg::Off => Self::Off,
            ElementCallCompatArg::Sticky => Self::StickyEvents,
            ElementCallCompatArg::State => Self::StateEvents,
        }
    }
}

/// Duration of one captured audio frame; 10 ms is the WebRTC convention.
const AUDIO_FRAME_MS: u32 = 10;
/// How often the run loop prints a health line.
const STATS_INTERVAL: Duration = Duration::from_secs(10);
/// How long to wait for a room to come down sync after login.
const ROOM_SYNC_TIMEOUT: Duration = Duration::from_secs(30);
/// How many times to attempt one device's login while being rate-limited.
///
/// Five attempts with doubling backoff outlast a synapse `rc_login` bucket
/// several times over; past that the pacing is wrong and saying so beats
/// retrying for minutes.
const LOGIN_ATTEMPTS: u32 = 5;
/// Ceiling for the login backoff, so a misconfigured run fails in a readable
/// time instead of doubling into the horizon.
const MAX_LOGIN_BACKOFF: Duration = Duration::from_secs(60);
/// What, typed at stdin, ends a run. Vim's `:q` spelling because that is the
/// reflex a scrolling terminal tool invites; the bare forms are there because
/// nobody should have to guess which one this accepts.
const QUIT_COMMANDS: [&str; 4] = [":q", ":quit", "q", "quit"];
/// Settle time after the warm-up messages, before any device joins the call.
const WARMUP_SETTLE: Duration = Duration::from_secs(2);

#[derive(Parser, Debug)]
#[command(
    name = "load_test",
    about = "Join a MatrixRTC call with N devices, each publishing a video file"
)]
struct Args {
    #[arg(long, env = "HOMESERVER_URL", default_value = "http://localhost:8008")]
    homeserver: String,

    /// Account localpart (or full user id) every device logs into.
    #[arg(long, env = "MX_USER")]
    user: String,

    #[arg(long, env = "MX_PASSWORD")]
    password: String,

    /// Recovery key of the account, so each fresh device gets cross-signed.
    /// Without it the devices cannot exchange media keys.
    #[arg(long, env = "RECOVERY_KEY")]
    recovery_key: String,

    /// Room hosting the call. The account must already have joined it.
    #[arg(long, env = "ROOM_ID")]
    room: String,

    /// Video file to publish. Anything ffmpeg reads, or `.y4m` / raw `.yuv`.
    #[arg(long)]
    video: PathBuf,

    /// How many devices (virtual participants) to run.
    #[arg(short = 'n', long, default_value_t = 1)]
    devices: usize,

    #[arg(long, default_value = "m.call#ROOM")]
    slot_id: String,

    #[arg(long, default_value = "m.call")]
    application: String,

    /// LiveKit authorisation service to use when the homeserver advertises no
    /// transport.
    #[arg(long, default_value = "http://localhost:6080")]
    livekit_url: String,

    /// Capture resolution, `WIDTHxHEIGHT`. Both must be even (I420); keep the
    /// long edge at 480 or more for real simulcast.
    #[arg(long, default_value = "640x360", value_parser = parse_resolution)]
    resolution: (u32, u32),

    #[arg(long, default_value_t = 15)]
    fps: u32,

    /// Seconds of the video to decode up front and then loop.
    #[arg(long, default_value_t = 10)]
    clip_seconds: u32,

    /// Refuse to decode a clip larger than this.
    #[arg(long, default_value_t = 512)]
    max_memory_mb: u64,

    /// Publish a single quality layer instead of simulcast.
    #[arg(long)]
    no_simulcast: bool,

    /// Also publish a per-device tone. Off by default: N tones at once are
    /// unpleasant to listen to while inspecting the call.
    #[arg(long)]
    audio: bool,

    /// Base tone frequency; device `i` publishes `audio_hz + i * 20`.
    #[arg(long, default_value_t = 440.0)]
    audio_hz: f64,

    /// Subscribe to peers' media. Off by default — decoding every other
    /// participant is what limits how many devices fit on one machine.
    #[arg(long)]
    subscribe: bool,

    /// Gap between successive call joins.
    #[arg(long, default_value_t = 500)]
    ramp_ms: u64,

    /// Gap between successive logins, to stay under homeserver rate limits.
    #[arg(long, default_value_t = 250)]
    login_delay_ms: u64,

    /// Seconds to keep publishing; 0 runs until Ctrl-C.
    #[arg(long, default_value_t = 0)]
    duration: u64,

    /// How long the homeserver keeps each membership in the sticky map.
    ///
    /// Short here on purpose, against the library default of an hour. A run
    /// that is killed rather than left cleanly (Ctrl-C mid-ramp, `kill -9`, a
    /// panic) leaves its memberships standing until this elapses — the dead
    /// man's switch does not clear them, because its delayed leave is a plain
    /// event that never replaces the sticky entry. An hour of ghosts poisons
    /// the room for the next attempt; two minutes does not.
    ///
    /// The cost is signalling: the heartbeat re-sends each membership once it
    /// is halfway to expiring, so this many milliseconds means one extra send
    /// per device every half of it. Keep it well above twice the heartbeat
    /// interval (15s), or memberships lapse between beats.
    #[arg(long, default_value_t = 120_000)]
    sticky_duration_ms: u64,

    /// Publish the `m.rtc.slot` state event first. Needs the power level for
    /// it, and is unnecessary when a real client already opened the call.
    #[arg(long)]
    open_slot: bool,

    /// Render this run for an older Element Call generation, for a call shared
    /// with Element Call on the JS SDK.
    ///
    /// `sticky` is the pre-2026 dialect: media keys then go out under the legacy
    /// to-device type *only*, so spec-current peers will not decrypt this run.
    /// `state` is the generation before MSC4354, where membership is
    /// `org.matrix.msc3401.call.member` room state — nothing about such a run is
    /// visible to a spec-current peer. Note `--open-slot` is pointless with
    /// `state`: that generation has no slot concept.
    #[arg(long, value_enum, default_value_t = ElementCallCompatArg::Off)]
    element_call_compat: ElementCallCompatArg,

    #[arg(long)]
    insecure_tls: bool,

    /// Prefix of the device display names this tool creates and purges.
    #[arg(long, default_value = "rtc-loadtest")]
    device_prefix: String,

    /// Persist each device's session and crypto store under this folder, and
    /// reuse them on the next run.
    ///
    /// Device `i` lives in `<store>/device-i`. A run picks up whatever is
    /// already there and only logs in for the devices that are missing, so
    /// re-running with the same `--devices` costs no logins at all — which
    /// matters because synapse rate-limits them hard (see `--login-delay-ms`).
    ///
    /// The bigger win is the crypto store: a restored device keeps its Megolm
    /// sessions, so it can decrypt member events sent before this run started.
    /// A fresh device cannot, which is why a peer who joined earlier is
    /// invisible to it until they re-join.
    ///
    /// Implies `--keep-devices`: logging out would invalidate the very session
    /// being persisted. Use `--purge-devices` to clear both the devices and the
    /// folder.
    #[arg(long)]
    store: Option<PathBuf>,

    /// Leave the devices logged in on exit.
    #[arg(long)]
    keep_devices: bool,

    /// Delete every device whose display name carries the prefix, then exit.
    /// The cleanup path for a run that was killed before it could tidy up.
    #[arg(long)]
    purge_devices: bool,
}

fn parse_resolution(value: &str) -> Result<(u32, u32), String> {
    let (width, height) = value
        .split_once(['x', 'X'])
        .ok_or_else(|| format!("expected WIDTHxHEIGHT, got {value}"))?;
    let width: u32 = width.parse().map_err(|_| format!("bad width in {value}"))?;
    let height: u32 = height
        .parse()
        .map_err(|_| format!("bad height in {value}"))?;
    if width == 0 || height == 0 || !width.is_multiple_of(2) || !height.is_multiple_of(2) {
        return Err(format!("{value}: I420 needs non-zero, even dimensions"));
    }
    Ok((width, height))
}

fn main() -> Result<(), Box<dyn Error>> {
    // Both rustls crypto backends are in the tree (`ring` via livekit/reqwest,
    // `aws-lc-rs` via matrix-sdk), so no process-level provider is picked
    // automatically; choose one before any TLS happens.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls aws-lc-rs crypto provider");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let args = Args::parse();
    // `Call::join` drives `!Send` futures, so it must run inside a `LocalSet`.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(tokio::task::LocalSet::new().run_until(run(args)))
}

async fn run(args: Args) -> Result<(), Box<dyn Error>> {
    if args.purge_devices {
        return purge_devices(&args).await;
    }
    if args.devices == 0 {
        return Err("--devices must be at least 1".into());
    }
    if let Some(root) = &args.store {
        prepare_store(root)?;
    }

    // Decode once and share: N decoders would compete with the N encoders for
    // CPU and distort exactly what this tool is measuring.
    let clip = Arc::new(decode_clip(&args)?);
    println!(
        "decoded {} frames of {}x{} ({} MiB) from {}",
        clip.len(),
        args.resolution.0,
        args.resolution.1,
        clip.bytes() / (1024 * 1024),
        args.video.display()
    );

    // Ctrl-C is handled out here rather than inside the run loop so that an
    // interrupt during login or joining still reaches the cleanup below. A
    // call that was mid-`join` when the interrupt landed is not recorded yet,
    // so its membership expires through the dead man's switch instead of a
    // leave event; the device itself is still removed.
    let mut fleet = Fleet::new(&args);
    let interrupted = spawn_interrupt_watch();
    let stopped = stop_on_input(spawn_stdin_watch());
    let result = tokio::select! {
        result = fleet.run(&args, clip) => result,
        _ = interrupted => Ok(()),
        _ = stopped => {
            println!("stopping; leaving the call");
            Ok(())
        }
    };
    fleet.shutdown(&args).await;
    result
}

/// Stop the run on a line from stdin.
///
/// **This is the only reliable way to end a run**, because signals do not
/// arrive in this process: SIGINT and SIGTERM are both ignored even with
/// handlers installed, and only SIGKILL — which cannot be blocked — lands. That
/// is not something this tool or the SDK sets up; the realistic culprit is
/// libwebrtc, which spawns a large thread pool and is the one dependency here
/// capable of masking signals process-wide. Until that is fixed upstream,
/// Ctrl-C is not available and typing `:q` takes its place.
///
/// A plain OS thread doing a blocking read, rather than `tokio::io::stdin`: it
/// needs no extra tokio features, and the thread simply parks in `read_line`
/// for the life of the run.
fn spawn_stdin_watch() -> tokio::sync::oneshot::Receiver<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        if read_quit_command() {
            let _ = tx.send(());
        }
        // Otherwise stdin ended without asking us to stop (closed, or not a
        // terminal: `< /dev/null`, a CI runner). Dropping `tx` unblocks the
        // waiter, which `stop_on_input` reads as "no input to wait on" rather
        // than as a stop.
    });
    rx
}

/// Read stdin until it asks us to quit; `false` if it ends without doing so.
///
/// A deliberate command rather than any keypress: this output scrolls
/// continuously for the length of a run, and a stray Enter should not end one.
/// Unrecognised lines are called out rather than ignored, so a typo does not
/// look like the tool hanging.
fn read_quit_command() -> bool {
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        line.clear();
        match stdin.read_line(&mut line) {
            Ok(0) | Err(_) => return false,
            Ok(_) => {}
        }
        let command = line.trim();
        if command.is_empty() {
            continue;
        }
        if QUIT_COMMANDS
            .iter()
            .any(|quit| command.eq_ignore_ascii_case(quit))
        {
            return true;
        }
        eprintln!("unrecognised input {command:?}; type :q to stop the run");
    }
}

/// Wait for [`spawn_stdin_watch`] to report a line, and never resolve if stdin
/// cannot give us one.
///
/// A dropped sender means there is no usable stdin, which must not be mistaken
/// for a stop request — that would end every run started without a terminal the
/// instant it began.
async fn stop_on_input(rx: tokio::sync::oneshot::Receiver<()>) {
    if rx.await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Watch for interrupts, and make the second one final.
///
/// The returned receiver fires on the first SIGINT or SIGTERM, which the run
/// loop races to start a graceful shutdown. A second signal exits outright.
///
/// That escape hatch is not a nicety. A graceful shutdown is one leave and one
/// logout per device, in sequence, and the logout goes through the SDK's
/// default request config, which retries a 429 without limit — so on a
/// homeserver this tool has just been hammering, "shutting down" can outlast
/// anyone's patience with no way to abort but `kill -9` from another terminal.
///
/// Spawned with `tokio::spawn`, so it lives on the multithreaded runtime rather
/// than the `LocalSet` every device's signalling shares. A saturated local
/// thread therefore cannot delay it.
///
/// SIGTERM is handled alongside SIGINT so a plain `kill` behaves the same as
/// Ctrl-C; nothing else in the process claims it.
fn spawn_interrupt_watch() -> tokio::sync::oneshot::Receiver<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut interrupt =
            signal(SignalKind::interrupt()).expect("SIGINT handler must be installable");
        let mut terminate =
            signal(SignalKind::terminate()).expect("SIGTERM handler must be installable");

        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
        println!("\ninterrupted; leaving the call (interrupt again to exit immediately)");
        let _ = tx.send(());

        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
        eprintln!(
            "second interrupt; exiting without a clean leave. Memberships expire in \
             --sticky-duration-ms; devices may need --purge-devices."
        );
        // 130 is the conventional "terminated by SIGINT" status.
        std::process::exit(130);
    });
    rx
}

/// One decoded, looping clip shared by every device.
struct Clip {
    frames: Vec<VideoFrame>,
}

impl Clip {
    fn len(&self) -> usize {
        self.frames.len()
    }

    fn bytes(&self) -> u64 {
        self.frames
            .first()
            .map(|frame| frame_bytes(&frame.buffer) * self.frames.len() as u64)
            .unwrap_or(0)
    }
}

fn frame_bytes(buffer: &I420Buffer) -> u64 {
    (buffer.data_y.len() + buffer.data_u.len() + buffer.data_v.len()) as u64
}

/// A device that has logged in; `call` is set once it has joined.
struct Device {
    index: usize,
    client: Client,
    device_id: OwnedDeviceId,
    sync: Option<SyncService>,
    call: Option<Call>,
    captured: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    pumps: Vec<AbortOnDrop>,
}

/// Aborts the wrapped capture task on drop, so a device going away never
/// leaves a pump pushing frames into a dead publication.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Fleet {
    devices: Vec<Device>,
    run_id: String,
}

impl Fleet {
    fn new(args: &Args) -> Self {
        // A suffix unique per run, so device names from concurrent or previous
        // runs never collide.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before the epoch")
            .as_nanos();
        Self {
            devices: Vec::with_capacity(args.devices),
            run_id: format!("{nanos:x}"),
        }
    }

    /// Log in, warm up encryption, join, publish, then hold until the deadline
    /// or Ctrl-C. Everything created along the way is recorded in `self`, so a
    /// failure at any point still gets cleaned up by [`Fleet::shutdown`].
    async fn run(&mut self, args: &Args, clip: Arc<Clip>) -> Result<(), Box<dyn Error>> {
        let room_id = RoomId::parse(&args.room)?;
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(args.insecure_tls)
            .build()?;

        // 1. Log in every device and start its sync service.
        let mut rooms = Vec::with_capacity(args.devices);
        for index in 0..args.devices {
            if index > 0 {
                tokio::time::sleep(Duration::from_millis(args.login_delay_ms)).await;
            }
            let device = self.login(args, index).await?;
            self.devices.push(device);
            let device = self.devices.last_mut().expect("just pushed");
            let sync = SyncService::builder(device.client.clone()).build().await?;
            sync.start().await;
            device.sync = Some(sync);
            rooms.push(wait_for_room(&device.client, &room_id).await?);
            println!("[{index}] logged in as device {}", device.device_id);
        }

        if !rooms[0].latest_encryption_state().await?.is_encrypted() {
            eprintln!(
                "WARNING: {} is not encrypted. Membership events carry no sending device, so \
                 media cannot be attributed to a member and no media key is distributed — the \
                 call will show participants with no video.",
                args.room
            );
        }

        // 2. One message per device before any RTC traffic. In an encrypted
        //    room this establishes the Olm sessions and shares the megolm key
        //    with every other device (ours and the observer's), so the key
        //    exchange is not racing the joins later on.
        for (index, room) in rooms.iter().enumerate() {
            room.send(RoomMessageEventContent::text_plain(format!(
                "{} {} device {index} ready",
                args.device_prefix, self.run_id
            )))
            .await?;
        }
        println!("sent {} warm-up messages", args.devices);
        tokio::time::sleep(WARMUP_SETTLE).await;

        if args.open_slot {
            open_slot(
                &self.devices[0].client,
                room_id.as_str(),
                &args.slot_id,
                &args.application,
                Some(SlotEncryption {
                    encryption_type: "m.per_member".to_owned(),
                    extra: Default::default(),
                }),
            )
            .await?;
            println!("opened slot {}", args.slot_id);
        }

        // 3. Join and publish, one device at a time.
        for (index, room) in rooms.iter().enumerate() {
            if index > 0 {
                tokio::time::sleep(Duration::from_millis(args.ramp_ms)).await;
            }
            let call = Call::join(
                room,
                CallOptions {
                    slot_id: args.slot_id.clone(),
                    application: args.application.clone(),
                    livekit_service_url_fallback: Some(args.livekit_url.clone()),
                    http: Some(http.clone()),
                    auto_subscribe: args.subscribe,
                    element_call_compat: args.element_call_compat.into(),
                    sticky_duration_ms: Some(args.sticky_duration_ms),
                    ..CallOptions::default()
                },
            )
            .await?;

            let device = &mut self.devices[index];
            let video = spawn_video_pump(
                &call,
                args,
                clip.clone(),
                index,
                device.captured.clone(),
                device.errors.clone(),
            )
            .await?;
            device.pumps.push(video);
            if args.audio {
                let audio = spawn_audio_pump(&call, args, index, device.errors.clone()).await?;
                device.pumps.push(audio);
            }
            println!(
                "[{index}] joined as {} and publishing",
                call.local_identity()
            );
            device.call = Some(call);
        }

        // 4. Hold. The stats line is this tool's own health signal: a device
        //    whose frame count stops tracking `--fps` is starved locally, not
        //    evidence of anything server-side.
        let deadline =
            (args.duration > 0).then(|| Instant::now() + Duration::from_secs(args.duration));
        println!(
            "{} devices publishing; {}",
            args.devices,
            match deadline {
                Some(_) => format!("stopping after {}s (type :q to stop early)", args.duration),
                None => "type :q to stop".to_owned(),
            }
        );

        let mut ticker = tokio::time::interval(STATS_INTERVAL);
        ticker.tick().await; // fires immediately; skip it
        let mut previous = vec![0u64; self.devices.len()];
        loop {
            tokio::select! {
                _ = ticker.tick() => self.report(&mut previous).await,
                _ = sleep_until(deadline) => {
                    println!("duration elapsed; leaving the call");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn login(&self, args: &Args, index: usize) -> Result<Device, Box<dyn Error>> {
        let store = args
            .store
            .as_ref()
            .map(|root| device_store_dir(root, index));

        // A store we can restore from skips the login *and* the cross-signing
        // dance below, which is most of a device's startup cost.
        if let Some(dir) = store.as_deref()
            && session_file(dir).exists()
        {
            match restore_device(args, dir).await {
                Ok(client) => {
                    println!("[{index}] restored device {}", device_id_of(&client)?);
                    return self.device_from(client, index).await;
                }
                // Never fatal: a store whose device was deleted server-side
                // (--purge-devices, a logout elsewhere, a wiped account) would
                // otherwise wedge every later run with no way out but rm -rf.
                Err(error) => eprintln!(
                    "[{index}] stored session unusable ({error}); logging in fresh instead"
                ),
            }
        }

        if let Some(dir) = store.as_deref() {
            // Whatever is in there was just rejected, and a stale sqlite store
            // must not be carried into a new login.
            let _ = std::fs::remove_dir_all(dir);
            std::fs::create_dir_all(dir)?;
        }

        let client = build_client(args, store.as_deref()).await?;
        let display_name = format!("{}-{}-{index}", args.device_prefix, self.run_id);
        login_with_retry(&client, args, &display_name).await?;
        client
            .encryption()
            .wait_for_e2ee_initialization_tasks()
            .await;

        // Cross-sign this fresh device from the account's recovery key. Both
        // the SDK's identity-based to-device strategy and the core's MSC4153
        // policy drop keys from devices that are not cross-signed, in both
        // directions — so this is fatal, not a warning.
        client
            .encryption()
            .recovery()
            .recover(args.recovery_key.trim())
            .await
            .map_err(|error| {
                format!(
                    "[{index}] could not cross-sign the new device from the recovery key \
                     ({error}); without it no media key can be exchanged"
                )
            })?;

        if let Some(dir) = store.as_deref() {
            persist_session(&client, dir)?;
        }

        self.device_from(client, index).await
    }

    /// Assemble the bookkeeping around a client that is logged in, however it
    /// got there.
    async fn device_from(&self, client: Client, index: usize) -> Result<Device, Box<dyn Error>> {
        quiet_room_key_gossip(&client).await;
        let device_id = device_id_of(&client)?;
        Ok(Device {
            index,
            client,
            device_id,
            sync: None,
            call: None,
            captured: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            pumps: Vec::new(),
        })
    }

    async fn report(&self, previous: &mut [u64]) {
        let mut line = String::new();
        for device in &self.devices {
            let captured = device.captured.load(Ordering::Relaxed);
            let delta = captured.saturating_sub(previous[device.index]);
            previous[device.index] = captured;
            let members = match &device.call {
                Some(call) => call.member_count().await,
                None => 0,
            };
            let _ = writeln!(
                line,
                "  [{}] {delta} frames/{}s, {captured} total, {} errors, {members} members",
                device.index,
                STATS_INTERVAL.as_secs(),
                device.errors.load(Ordering::Relaxed),
            );
        }
        print!("{line}");
    }

    /// Best effort, in order, never bailing out early: a failure to leave must
    /// not leave the devices behind.
    async fn shutdown(&mut self, args: &Args) {
        let mut left = 0;
        let mut removed = 0;
        let mut captured = 0;
        let mut errors = 0;

        for device in &mut self.devices {
            device.pumps.clear();
            captured += device.captured.load(Ordering::Relaxed);
            errors += device.errors.load(Ordering::Relaxed);

            if let Some(call) = device.call.take() {
                match call.leave().await {
                    Ok(()) => left += 1,
                    Err(error) => eprintln!("[{}] leave failed: {error}", device.index),
                }
            }
            drop(device.sync.take());

            // `--store` implies keeping the device: logging out would revoke
            // the access token we just persisted, so the next run would restore
            // a session the homeserver has already forgotten.
            if args.keep_devices || args.store.is_some() {
                continue;
            }
            // `logout` deletes this device server-side without the interactive
            // auth `delete_devices` demands, which is why it is the normal
            // cleanup path.
            match device.client.matrix_auth().logout().await {
                Ok(_) => removed += 1,
                Err(error) => eprintln!(
                    "[{}] could not log out device {} ({error}); remove it with --purge-devices",
                    device.index, device.device_id
                ),
            }
        }

        println!(
            "summary: {left}/{} calls left cleanly, {captured} frames captured, {errors} capture \
             errors, {removed} devices removed",
            self.devices.len()
        );
    }
}

/// Resolve when `deadline` passes, or never if there is none.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => std::future::pending().await,
    }
}

/// Marker file written at the root of a `--store` folder when this tool creates
/// it.
///
/// It guards the `remove_dir_all` on the fresh-login path: a failed restore
/// deletes `<store>/device-i`, and pointing `--store` at a home directory
/// should not hand this tool licence to do that. A folder we did not create,
/// and that already holds something, is refused rather than adopted.
const STORE_MARKER: &str = ".matrix-rtc-load-test";

/// Create the `--store` folder if needed, or confirm we are allowed to use an
/// existing one.
///
/// Creating it is deliberate — asking the operator to `mkdir` first buys
/// nothing — but adopting an arbitrary populated directory is not.
fn prepare_store(root: &Path) -> Result<(), Box<dyn Error>> {
    if root.join(STORE_MARKER).exists() {
        return Ok(());
    }
    if root.exists() && root.read_dir()?.next().is_some() {
        return Err(format!(
            "{} is not empty and was not created by this tool. Point --store at a new or \
             empty folder; this one would have subdirectories created and deleted inside it.",
            root.display(),
        )
        .into());
    }
    std::fs::create_dir_all(root)?;
    std::fs::write(
        root.join(STORE_MARKER),
        "matrix-rtc load_test device store; delete this folder to forget every device\n",
    )?;
    Ok(())
}

/// Stop this client requesting and forwarding Megolm room keys.
///
/// The SDK does both automatically (`automatic-room-key-forwarding`, on by
/// default): a decryption failure fires an `m.room_key_request`, and other
/// verified devices answer with `m.forwarded_room_key`. That is *correct* for a
/// real client — it is how a new device catches up on member events sent before
/// it existed — but this tool exists to measure to-device traffic, and ten
/// fresh devices failing to decrypt each other's history produce a burst of
/// exactly the traffic under measurement, at exactly the wrong moment.
///
/// So it is switched off here, per client, rather than compiled out of the
/// library: `matrix-rtc-livekit` keeps the correct default for everyone else.
///
/// `olm_machine_for_testing` is the only public route to the `OlmMachine`
/// (`Encryption::olm_machine` is `pub(crate)`, `Client::base_client` likewise),
/// which is why this example needs matrix-sdk's `testing` feature.
///
/// What it costs: a device can no longer ask for Megolm sessions it missed, so
/// it sees peers whose member events predate it only when they next re-send —
/// half of `--sticky-duration-ms`. `--store` avoids that entirely, since a
/// restored device already holds its sessions.
async fn quiet_room_key_gossip(client: &Client) {
    let machine = client.olm_machine_for_testing().await;
    let Some(machine) = machine.as_ref() else {
        eprintln!("no olm machine on a logged-in client; room-key gossip stays on");
        return;
    };
    machine.set_room_key_forwarding_enabled(false);
    machine.set_room_key_requests_enabled(false);
}

/// Where device `index` keeps its sqlite store and session file under
/// `--store`.
fn device_store_dir(root: &Path, index: usize) -> PathBuf {
    root.join(format!("device-{index}"))
}

/// The file holding a device's `MatrixSession` (access token, device id).
///
/// Kept beside the sqlite store rather than inside it: the SDK owns that
/// database, and the session is ours to write.
fn session_file(dir: &Path) -> PathBuf {
    dir.join("session.json")
}

/// Build a client, backed by `store` when `--store` is in play and by memory
/// otherwise.
async fn build_client(args: &Args, store: Option<&Path>) -> Result<Client, Box<dyn Error>> {
    let mut builder = Client::builder()
        .homeserver_url(&args.homeserver)
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: true,
            ..EncryptionSettings::default()
        });
    if let Some(dir) = store {
        builder = builder.sqlite_store(dir, None);
    }
    if args.insecure_tls {
        builder = builder.disable_ssl_verification();
    }
    Ok(builder.build().await?)
}

/// Bring a device back from `--store`.
///
/// The `whoami` at the end is the point of the whole function: a session file
/// is just bytes on disk and says nothing about whether the homeserver still
/// honours the token. One cheap round trip tells us, and its failure is what
/// the caller turns into a fresh login.
async fn restore_device(args: &Args, dir: &Path) -> Result<Client, Box<dyn Error>> {
    let session: MatrixSession =
        serde_json::from_str(&std::fs::read_to_string(session_file(dir))?)?;
    let client = build_client(args, Some(dir)).await?;
    client.restore_session(session).await?;
    client
        .encryption()
        .wait_for_e2ee_initialization_tasks()
        .await;
    client.whoami().await?;
    Ok(client)
}

/// Write this device's session out so the next run can restore it.
fn persist_session(client: &Client, dir: &Path) -> Result<(), Box<dyn Error>> {
    let session = client
        .matrix_auth()
        .session()
        .ok_or("no session to persist after a successful login")?;
    std::fs::write(session_file(dir), serde_json::to_vec_pretty(&session)?)?;
    Ok(())
}

/// This client's device id, once it has one.
fn device_id_of(client: &Client) -> Result<OwnedDeviceId, Box<dyn Error>> {
    Ok(client
        .device_id()
        .ok_or("client has no device id")?
        .to_owned())
}

/// Log in, waiting the homeserver out if it rate-limits us.
///
/// Every device here is a fresh login, and synapse's `rc_login` defaults to
/// `per_second: 0.17, burst_count: 3` — three logins immediately, then one per
/// roughly six seconds. Without this, the first 429 propagates out of
/// `login()` and aborts the entire run, discarding the devices that had already
/// logged in and joined. `--login-delay-ms` is still what paces a healthy run;
/// this is the safety net for a deployment whose limiter is stricter than the
/// default, or a `--devices` count that outruns the pacing.
///
/// Only rate limiting is retried. A wrong password or an unreachable
/// homeserver fails on the first attempt, as it should.
async fn login_with_retry(
    client: &Client,
    args: &Args,
    display_name: &str,
) -> Result<(), Box<dyn Error>> {
    // A limiter that just rejected us will reject an immediate retry too, so
    // never start below a second however tight the configured pacing is.
    let mut backoff = Duration::from_millis(args.login_delay_ms).max(Duration::from_secs(1));

    for attempt in 1..=LOGIN_ATTEMPTS {
        let result = client
            .matrix_auth()
            .login_username(&args.user, &args.password)
            .initial_device_display_name(display_name)
            .send()
            .await;

        let error = match result {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };

        let Some(retry_after) = rate_limit_retry_after(&error) else {
            return Err(error.into());
        };
        if attempt == LOGIN_ATTEMPTS {
            return Err(format!(
                "still rate-limited after {LOGIN_ATTEMPTS} login attempts ({error}). \
                 Raise --login-delay-ms, or lower --devices."
            )
            .into());
        }

        // The server's own figure when it gives one; our doubling otherwise.
        let delay = retry_after.unwrap_or(backoff);
        eprintln!(
            "login rate-limited (attempt {attempt}/{LOGIN_ATTEMPTS}); waiting {:.1}s",
            delay.as_secs_f64(),
        );
        tokio::time::sleep(delay).await;
        backoff = (backoff * 2).min(MAX_LOGIN_BACKOFF);
    }

    // The loop returns on every path; `1..=LOGIN_ATTEMPTS` is never empty.
    unreachable!("the login loop always returns")
}

/// Whether the homeserver rate-limited this request, and for how long it asked
/// us to wait.
///
/// `None` means it was some other failure. `Some(None)` means we were rate
/// limited but the server named no delay — which is what synapse does for
/// `M_LIMIT_EXCEEDED` on login, so the caller has to pick its own.
fn rate_limit_retry_after(error: &matrix_sdk::Error) -> Option<Option<Duration>> {
    let ErrorKind::LimitExceeded(limit) = error.client_api_error_kind()? else {
        return None;
    };
    Some(match limit.retry_after.as_ref() {
        Some(RetryAfter::Delay(delay)) => Some(*delay),
        // An absolute deadline. Our own backoff beats reconciling clocks with
        // the homeserver for something this coarse.
        Some(RetryAfter::DateTime(_)) | None => None,
    })
}

async fn wait_for_room(client: &Client, room_id: &RoomId) -> Result<Room, Box<dyn Error>> {
    let deadline = Instant::now() + ROOM_SYNC_TIMEOUT;
    loop {
        if let Some(room) = client.get_room(room_id) {
            return Ok(room);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{room_id} did not come down sync within {}s — has the account joined it?",
                ROOM_SYNC_TIMEOUT.as_secs()
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Publish the clip as a camera track and pump frames into it at `--fps`.
///
/// Each device starts at a different offset in the clip: the encoders then do
/// not run in lockstep, and the tiles are visibly distinct in the observing
/// client.
async fn spawn_video_pump(
    call: &Call,
    args: &Args,
    clip: Arc<Clip>,
    index: usize,
    captured: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) -> Result<AbortOnDrop, Box<dyn Error>> {
    let (width, height) = args.resolution;
    let mut options = PublishOptions::camera(VideoSourceConfig { width, height });
    options.simulcast = !args.no_simulcast;
    let track = call.publish(options).await?;

    let offset = index * clip.len() / args.devices;
    let period = Duration::from_micros(1_000_000 / u64::from(args.fps.max(1)));
    let task = tokio::spawn(async move {
        let started = Instant::now();
        let mut ticker = tokio::time::interval(period);
        let mut n = 0usize;
        loop {
            ticker.tick().await;
            let mut frame = clip.frames[(offset + n) % clip.len()].clone();
            frame.timestamp_us = started.elapsed().as_micros() as i64;
            match track.capture_video(frame) {
                Ok(()) => {
                    captured.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    // The publication is gone (left, or the connection died);
                    // one line, then stop pushing into it.
                    eprintln!("[{index}] video capture stopped: {error}");
                    errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            n += 1;
        }
    });
    Ok(AbortOnDrop(task))
}

/// Publish a per-device sine tone, so the audio path carries load too.
async fn spawn_audio_pump(
    call: &Call,
    args: &Args,
    index: usize,
    errors: Arc<AtomicU64>,
) -> Result<AbortOnDrop, Box<dyn Error>> {
    let config = AudioSourceConfig::default();
    let track: Arc<dyn LocalTrackHandle> = call.publish(PublishOptions::microphone()).await?;
    let freq_hz = args.audio_hz + index as f64 * 20.0;
    let sample_rate = config.sample_rate;
    let samples_per_frame = (sample_rate / 1000 * AUDIO_FRAME_MS) as usize;
    let amplitude = 0.3 * f64::from(i16::MAX);

    let task = tokio::spawn(async move {
        let mut n: u64 = 0;
        loop {
            let mut data = Vec::with_capacity(samples_per_frame);
            for _ in 0..samples_per_frame {
                let t = n as f64 / f64::from(sample_rate);
                data.push((amplitude * (std::f64::consts::TAU * freq_hz * t).sin()) as i16);
                n += 1;
            }
            // Audio capture is paced by the transport, so this loop needs no
            // ticker of its own.
            let frame = AudioFrame {
                data,
                sample_rate,
                num_channels: config.num_channels,
                samples_per_channel: samples_per_frame as u32,
            };
            if let Err(error) = track.capture_audio(frame).await {
                eprintln!("[{index}] audio capture stopped: {error}");
                errors.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    });
    Ok(AbortOnDrop(task))
}

/// Log in once and delete every device left over from an earlier run.
async fn purge_devices(args: &Args) -> Result<(), Box<dyn Error>> {
    let mut builder = Client::builder().homeserver_url(&args.homeserver);
    if args.insecure_tls {
        builder = builder.disable_ssl_verification();
    }
    let client = builder.build().await?;
    client
        .matrix_auth()
        .login_username(&args.user, &args.password)
        .initial_device_display_name("matrix-rtc-livekit load_test purge")
        .send()
        .await?;

    let this_device = client.device_id().map(ToOwned::to_owned);
    let stale: Vec<OwnedDeviceId> = client
        .devices()
        .await?
        .devices
        .into_iter()
        .filter(|device| {
            Some(&device.device_id) != this_device.as_ref()
                && device
                    .display_name
                    .as_deref()
                    .is_some_and(|name| name.starts_with(&args.device_prefix))
        })
        .map(|device| device.device_id)
        .collect();

    if stale.is_empty() {
        println!("no devices named {}* to purge", args.device_prefix);
    } else {
        println!("purging {} device(s): {stale:?}", stale.len());
        // Deleting devices is interactive-auth guarded: the first call always
        // fails with the flow, the second carries the password.
        if let Err(error) = client.delete_devices(&stale, None).await {
            let info = error
                .as_uiaa_response()
                .ok_or_else(|| format!("device deletion failed: {error}"))?;
            let mut password = uiaa::Password::new(
                uiaa::UserIdentifier::Matrix(uiaa::MatrixUserIdentifier::new(args.user.clone())),
                args.password.clone(),
            );
            password.session = info.session.clone();
            client
                .delete_devices(&stale, Some(uiaa::AuthData::Password(password)))
                .await?;
        }
        println!("purged {} device(s)", stale.len());
    }

    // Every session under --store now names a device that no longer exists, so
    // clear the folder too — otherwise the next run restores tokens the
    // homeserver has forgotten and falls back to a fresh login per device
    // anyway, having paid for the attempt.
    if let Some(root) = &args.store
        && root.exists()
    {
        std::fs::remove_dir_all(root)?;
        println!("cleared the device store at {}", root.display());
    }

    // Do not leave the device this purge just created behind.
    client.matrix_auth().logout().await?;
    Ok(())
}

// --- video decoding ---------------------------------------------------------

/// Decode `--clip-seconds` of the input into I420 frames at `--resolution`.
fn decode_clip(args: &Args) -> Result<Clip, Box<dyn Error>> {
    let (width, height) = args.resolution;
    let extension = args
        .video
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let raw = match extension.as_str() {
        "y4m" => read_y4m(&args.video, width, height)?,
        "yuv" => std::fs::read(&args.video)?,
        _ => run_ffmpeg(args)?,
    };

    let frame_size = i420_size(width, height);
    // ffmpeg was already told `-t`; the raw formats are trimmed here instead,
    // taking `--fps` as their playback rate.
    let count = (raw.len() / frame_size).min((args.clip_seconds * args.fps.max(1)) as usize);
    if count == 0 {
        return Err(format!(
            "{} produced no {width}x{height} frames — wrong --resolution, or an empty input?",
            args.video.display()
        )
        .into());
    }
    let budget = args.max_memory_mb * 1024 * 1024;
    let wanted = (count * frame_size) as u64;
    if wanted > budget {
        return Err(format!(
            "{count} frames of {width}x{height} need {} MiB, over the --max-memory-mb limit of {} \
             MiB; lower --clip-seconds, --fps or --resolution",
            wanted / (1024 * 1024),
            args.max_memory_mb
        )
        .into());
    }

    let frames = raw
        .chunks_exact(frame_size)
        .take(count)
        .map(|chunk| split_i420(chunk, width, height))
        .collect();
    Ok(Clip { frames })
}

/// Bytes of one packed I420 frame.
fn i420_size(width: u32, height: u32) -> usize {
    let chroma = (width.div_ceil(2) * height.div_ceil(2)) as usize;
    (width * height) as usize + 2 * chroma
}

/// Split one packed I420 frame into the owned planes the media layer takes.
fn split_i420(chunk: &[u8], width: u32, height: u32) -> VideoFrame {
    let luma = (width * height) as usize;
    let chroma_width = width.div_ceil(2);
    let chroma = (chroma_width * height.div_ceil(2)) as usize;
    VideoFrame {
        buffer: I420Buffer {
            width,
            height,
            data_y: chunk[..luma].to_vec(),
            stride_y: width,
            data_u: chunk[luma..luma + chroma].to_vec(),
            stride_u: chroma_width,
            data_v: chunk[luma + chroma..luma + 2 * chroma].to_vec(),
            stride_v: chroma_width,
        },
        rotation: VideoRotation::Deg0,
        timestamp_us: 0,
    }
}

/// Decode, scale and resample the input to packed I420 in one ffmpeg pass.
fn run_ffmpeg(args: &Args) -> Result<Vec<u8>, Box<dyn Error>> {
    let (width, height) = args.resolution;
    let mut child = Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(&args.video)
        .args([
            "-t",
            &args.clip_seconds.to_string(),
            "-vf",
            &format!("scale={width}:{height},fps={}", args.fps),
            "-pix_fmt",
            "yuv420p",
            "-f",
            "rawvideo",
            "-",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| {
            format!(
                "could not run ffmpeg ({error}); install it, or pass a .y4m / .yuv file instead"
            )
        })?;

    let mut raw = Vec::new();
    child
        .stdout
        .take()
        .expect("stdout was piped")
        .read_to_end(&mut raw)?;
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("ffmpeg failed with {status}").into());
    }
    Ok(raw)
}

/// Read a y4m file into packed I420, checking it matches `--resolution`.
///
/// Only the frame headers are skipped; y4m is already planar 4:2:0, so the
/// payload needs no conversion. Anything else (scaling, other pixel formats)
/// is ffmpeg's job.
fn read_y4m(path: &Path, width: u32, height: u32) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = std::fs::read(path)?;
    let header_end = bytes
        .iter()
        .position(|&byte| byte == b'\n')
        .ok_or("not a y4m file: no header line")?;
    let header = std::str::from_utf8(&bytes[..header_end])?;
    if !header.starts_with("YUV4MPEG2") {
        return Err("not a y4m file: missing the YUV4MPEG2 signature".into());
    }
    let tagged = |prefix: char| -> Option<u32> {
        header
            .split_ascii_whitespace()
            .find_map(|tag| tag.strip_prefix(prefix))
            .and_then(|value| value.parse().ok())
    };
    let (file_width, file_height) = (tagged('W'), tagged('H'));
    if file_width != Some(width) || file_height != Some(height) {
        return Err(format!(
            "{} is {:?}x{:?}, but --resolution is {width}x{height}; y4m input is not rescaled",
            path.display(),
            file_width,
            file_height
        )
        .into());
    }

    let frame_size = i420_size(width, height);
    let mut raw = Vec::with_capacity(bytes.len());
    let mut cursor = header_end + 1;
    // Every frame is a `FRAME[ tags]\n` line followed by the raw planes.
    while cursor < bytes.len() {
        let Some(offset) = bytes[cursor..].iter().position(|&byte| byte == b'\n') else {
            break;
        };
        cursor += offset + 1;
        if cursor + frame_size > bytes.len() {
            break;
        }
        raw.extend_from_slice(&bytes[cursor..cursor + frame_size]);
        cursor += frame_size;
    }
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 4x2 y4m with `frames` frames, each plane filled with a per-frame
    /// constant so a split can be checked plane by plane.
    fn y4m(frames: u8) -> Vec<u8> {
        let mut out = b"YUV4MPEG2 W4 H2 F25:1 Ip A1:1 C420\n".to_vec();
        for n in 1..=frames {
            out.extend_from_slice(b"FRAME\n");
            out.extend(std::iter::repeat_n(n, 8)); // Y: 4x2
            out.extend(std::iter::repeat_n(n + 100, 2)); // U: 2x1
            out.extend(std::iter::repeat_n(n + 200, 2)); // V: 2x1
        }
        out
    }

    #[test]
    fn i420_size_counts_both_chroma_planes() {
        assert_eq!(i420_size(4, 2), 8 + 2 + 2);
        // Odd dimensions round the chroma planes up.
        assert_eq!(i420_size(3, 3), 9 + 2 * 4);
    }

    #[test]
    fn splits_planes_and_strides() {
        let frame = split_i420(&[1, 1, 1, 1, 1, 1, 1, 1, 101, 101, 201, 201], 4, 2);
        assert_eq!((frame.buffer.width, frame.buffer.height), (4, 2));
        assert_eq!(frame.buffer.stride_y, 4);
        assert_eq!(frame.buffer.stride_u, 2);
        assert_eq!(frame.buffer.stride_v, 2);
        assert_eq!(frame.buffer.data_y, vec![1; 8]);
        assert_eq!(frame.buffer.data_u, vec![101; 2]);
        assert_eq!(frame.buffer.data_v, vec![201; 2]);
    }

    #[test]
    fn reads_every_y4m_frame_and_drops_the_frame_markers() {
        let path = std::env::temp_dir().join("load_test_y4m_frames.y4m");
        std::fs::write(&path, y4m(3)).unwrap();

        let raw = read_y4m(&path, 4, 2).unwrap();
        let frame_size = i420_size(4, 2);
        assert_eq!(raw.len(), 3 * frame_size);
        let second = split_i420(&raw[frame_size..2 * frame_size], 4, 2);
        assert_eq!(second.buffer.data_y, vec![2; 8]);
        assert_eq!(second.buffer.data_u, vec![102; 2]);
        assert_eq!(second.buffer.data_v, vec![202; 2]);
    }

    #[test]
    fn y4m_resolution_must_match_the_requested_one() {
        let path = std::env::temp_dir().join("load_test_y4m_mismatch.y4m");
        std::fs::write(&path, y4m(1)).unwrap();
        assert!(read_y4m(&path, 640, 360).is_err());
    }

    #[test]
    fn resolution_parsing_rejects_odd_and_zero_dimensions() {
        assert_eq!(parse_resolution("640x360").unwrap(), (640, 360));
        assert!(parse_resolution("641x360").is_err());
        assert!(parse_resolution("640x0").is_err());
        assert!(parse_resolution("640").is_err());
    }
}
