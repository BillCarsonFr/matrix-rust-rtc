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

//! One half of the Element Call interop test: a Rust MatrixRTC client driven
//! over stdin, reporting what it observes as JSON lines on stdout.
//!
//! The other half is a real Element Call, running as a widget inside Element
//! Web, driven by Playwright — which also spawns this process. See
//! `interop/README.md` and `demo/backend/INTEROP.md`.
//!
//! Why a separate process rather than a scenario in `tests/e2e_call`: the
//! browser side has to be sequenced against the Matrix side (Element Call must
//! pick its dialect *before* joining, and must join *after* our device is in
//! the room), and Playwright is the thing that can see the browser. So
//! Playwright drives, and this is the half it drives.
//!
//! ## Protocol
//!
//! Commands, one per line on **stdin**:
//!
//! | Command | Effect |
//! | ------- | ------ |
//! | `join`  | open the slot if the dialect has one, join the call, publish a tone and a video pattern |
//! | `leave` | leave the call (the Matrix leave *and* the SFU disconnect) |
//! | `quit`  | leave if still joined, then exit 0 |
//!
//! Events, one JSON object per line on **stdout** (`{"event": "...", ...}`):
//!
//! | Event | Meaning |
//! | ----- | ------- |
//! | `ready` | logged in, syncing, room created and the peer invited |
//! | `joined` | our membership is published and the SFU connection is up |
//! | `members` | the call's member count changed — `>= 2` means the peer's membership parsed |
//! | `track_subscribed` | the SFU forwarded a remote track, with the identity it assigned the peer |
//! | `key_imported` | the peer's media key is installed under that identity |
//! | `audio_rms` | RMS of the peer's decrypted audio — proves frames decrypt, not just arrive |
//! | `left`, `error` | terminal outcomes |
//!
//! Everything human-readable goes to **stderr**, so stdout stays a clean
//! protocol stream.
//!
//! ## Environment
//!
//! | Variable | Default |
//! | -------- | ------- |
//! | `HOMESERVER_URL` | `https://synapse.m.localhost` |
//! | `ELEMENT_CALL_COMPAT` | `state` (also: `sticky`, `off`) |
//! | `INVITE_USER` | *required* — the Matrix ID Element Call will log in as |
//! | `DISPLAY_NAME` | `Rust Peer` — what the browser asserts it can see |
//! | `ROOM_NAME` | `Interop Call` |
//! | `SLOT_ID` | `m.call#ROOM` |
//! | `LIVEKIT_SERVICE_URL` | unset — only a fallback when the homeserver advertises no focus |
//! | `RECORD_SECS` | `3` |
//! | `OUT_WAV` | unset — when set, the recorded peer audio is written there |
//! | `MX_USER` / `MX_PASSWORD` | unset — register a throwaway user instead |
//! | `INSECURE_TLS` | unset — accept the dev CA without trusting it system-wide |

use std::env;
use std::error::Error;
use std::time::Duration;

use livekit::{RoomEvent, track::RemoteTrack};
use matrix_sdk::encryption::EncryptionSettings;
use matrix_sdk::ruma::api::client::room::create_room::v3::Request as CreateRoomRequest;
use matrix_sdk::ruma::events::InitialStateEvent;
use matrix_sdk::ruma::events::room::history_visibility::{
    HistoryVisibility, RoomHistoryVisibilityEventContent,
};
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{OwnedRoomId, RoomId, UserId};
use matrix_sdk::{Client, RoomMemberships};
use matrix_sdk_ui::sync_service::SyncService;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

use matrix_rtc_core::SlotEncryption;
use matrix_rtc_livekit::compat::ElementCallCompat;
use matrix_rtc_livekit::{Call, CallOptions, media, open_slot};
use matrix_rtc_media::{I420Buffer, PublishOptions, VideoFrame, VideoRotation, VideoSourceConfig};

/// Hard cap on a whole run, so a wedged stack fails the Playwright test with
/// this process's own diagnosis rather than a bare timeout.
const OVERALL_DEADLINE: Duration = Duration::from_secs(240);

/// 640x360: below 480px the LiveKit SDK emits a single-encoding simulcast
/// (rid "q" only), which an adaptive-stream subscriber like Element Call
/// resolves to no layer at all — a grey tile with zero decryption errors.
const PATTERN_WIDTH: u32 = 640;
const PATTERN_HEIGHT: u32 = 360;

/// Anything above this is unmistakably not silence. Element Call publishes
/// Chrome's fake capture device, which is a *pulsed* tone rather than a clean
/// sine, so this measures energy rather than a frequency — unlike
/// `e2e_call`, which controls both ends and can assert 440 Hz exactly.
const AUDIO_RMS_FLOOR: f64 = 200.0;

/// One protocol line. Flushed eagerly: the reader on the other end is
/// sequencing a browser against us, so a buffered event is a deadlock.
fn emit(event: serde_json::Value) {
    use std::io::Write;
    println!("{event}");
    let _ = std::io::stdout().flush();
}

/// An error plus its whole `source()` chain.
///
/// `Display` on the outer error alone is nearly useless for the failures this
/// process hits — "error sending request for url (…)" with the actual cause
/// (an unknown certificate issuer, a refused connection) one level down.
fn error_chain(error: &(dyn Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    message
}

/// The PEM the harness points us at with `SSL_CERT_FILE`, if any.
///
/// It has to be applied twice, because there are two reqwest crates in this
/// binary. `SSL_CERT_FILE` is read by `rustls-native-certs`, which only the
/// 0.12 instance (livekit's, and ours) is built to consult; matrix-sdk pulls a
/// separate 0.13 instance that nothing enables native roots on, so the same
/// certificate has to be handed to *it* explicitly via
/// `ClientBuilder::add_root_certificates`. A machine-wide trust install would
/// not reach it either.
fn dev_ca_pem() -> Option<Vec<u8>> {
    let path = env::var_os("SSL_CERT_FILE")?;
    match std::fs::read(&path) {
        Ok(pem) => Some(pem),
        Err(error) => {
            eprintln!("[peer] could not read SSL_CERT_FILE {path:?}: {error}");
            None
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    // Both rustls crypto backends are in the tree (`ring` via livekit/reqwest,
    // `aws-lc-rs` via matrix-sdk), so rustls cannot pick a process-level
    // provider by itself and panics on the first handshake.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        // stdout is the protocol stream; logs must not land in it.
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // `Call::join` drives `!Send` futures, so everything runs on a `LocalSet`.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(
        tokio::task::LocalSet::new()
            .run_until(async { tokio::time::timeout(OVERALL_DEADLINE, run()).await }),
    );

    match outcome {
        Err(_) => {
            emit(serde_json::json!({
                "event": "error",
                "message": format!("interop peer did not finish within {OVERALL_DEADLINE:?}"),
            }));
            Err("deadline exceeded".into())
        }
        Ok(Err(error)) => {
            emit(serde_json::json!({ "event": "error", "message": error_chain(error.as_ref()) }));
            Err(error)
        }
        Ok(Ok(())) => Ok(()),
    }
}

struct Config {
    homeserver: String,
    slot_id: String,
    livekit_service_url: Option<String>,
    display_name: String,
    room_name: String,
    invite_user: String,
    compat: ElementCallCompat,
    record_secs: u64,
    out_wav: Option<String>,
    insecure_tls: bool,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Config {
            homeserver: env::var("HOMESERVER_URL")
                .unwrap_or_else(|_| "https://synapse.m.localhost".to_owned()),
            slot_id: env::var("SLOT_ID").unwrap_or_else(|_| "m.call#ROOM".to_owned()),
            // Deliberately no default. The interop stack advertises its focus
            // over MSC4143 `/rtc_transports` (`matrix_rtc.transports` in
            // homeserver.interop.yaml), which is what we want the test to
            // exercise — a hardcoded fallback here would silently paper over a
            // broken advertisement.
            livekit_service_url: env::var("LIVEKIT_SERVICE_URL").ok(),
            display_name: env::var("DISPLAY_NAME").unwrap_or_else(|_| "Rust Peer".to_owned()),
            room_name: env::var("ROOM_NAME").unwrap_or_else(|_| "Interop Call".to_owned()),
            invite_user: env::var("INVITE_USER")
                .map_err(|_| "INVITE_USER is required (the Matrix ID Element Call logs in as)")?,
            // Defaults to the dialect real Element Call deployments speak today.
            compat: match env::var("ELEMENT_CALL_COMPAT").ok().as_deref() {
                Some("off") => ElementCallCompat::Off,
                Some("sticky") => ElementCallCompat::StickyEvents,
                None | Some("state") => ElementCallCompat::StateEvents,
                Some(other) => return Err(format!("unknown ELEMENT_CALL_COMPAT {other:?}").into()),
            },
            record_secs: env::var("RECORD_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
            out_wav: env::var("OUT_WAV").ok(),
            insecure_tls: env::var("INSECURE_TLS").is_ok(),
        })
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let cfg = Config::from_env()?;
    let invitee = UserId::parse(&cfg.invite_user)?;

    // This client is used for registration and handed to `Call::join` for the
    // MSC4195 token exchange. Its roots come from `SSL_CERT_FILE` via
    // rustls-native-certs, but add the certificate explicitly too rather than
    // depend on which root-store features the graph happens to unify on.
    let mut http = reqwest::Client::builder().danger_accept_invalid_certs(cfg.insecure_tls);
    if !cfg.insecure_tls
        && let Some(pem) = dev_ca_pem()
    {
        http = http.add_root_certificate(
            reqwest::Certificate::from_pem(&pem)
                .map_err(|error| format!("SSL_CERT_FILE is not a PEM certificate: {error}"))?,
        );
    }
    let http = http.build()?;

    // 1. Log in (registering a throwaway user unless credentials were given).
    let (user, password) = match (env::var("MX_USER"), env::var("MX_PASSWORD")) {
        (Ok(user), Ok(password)) => (user, password),
        _ => register_user(&http, &cfg.homeserver).await?,
    };
    // The sync service is bound for the whole run: dropping it stops the
    // sticky/key traffic the call depends on.
    let (client, _sync) = login_and_sync(&cfg, &user, &password).await?;
    // Element Call renders this next to the tile, and the browser half of the
    // test asserts on it — it is how we prove *our* membership reached EC.
    client
        .account()
        .set_display_name(Some(&cfg.display_name))
        .await?;

    // 2. Create the room. Deliberately us and not Element Web: this is where
    //    the `org.matrix.msc3401.call.member` power level comes from, and it
    //    puts our device in the room before Element Call ever joins — a device
    //    that shows up later cannot decrypt a membership EC already sent.
    let room_id = create_encrypted_room(&client, &cfg.room_name, &invitee).await?;
    let room = wait_for_room(&client, &room_id).await?;
    eprintln!("[peer] created {room_id}, invited {invitee}");

    emit(serde_json::json!({
        "event": "ready",
        "room_id": room_id.as_str(),
        "room_name": cfg.room_name,
        "user_id": client.user_id().map(|u| u.to_string()),
        "display_name": cfg.display_name,
    }));

    // 3. Wait for commands. Element Call has to accept the invite and pick its
    //    dialect before we publish anything.
    let mut commands = stdin_lines();
    let mut joined: Option<Joined> = None;
    // While joined, `watch_call` is what reads stdin, so the command it stops
    // on is dispatched on the next turn of this loop rather than lost.
    let mut pending: Option<String> = None;

    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => match commands.recv().await {
                Some(command) => command,
                // stdin closed: Playwright is gone, hang up.
                None => break,
            },
        };

        match command.trim() {
            "join" if joined.is_some() => eprintln!("[peer] already joined, ignoring"),
            "join" => {
                // Element Call must be a room member before we publish
                // anything, or its membership can never pair with ours.
                wait_for_joined_members(&room, 2).await?;
                joined = Some(join_call(&cfg, &client, &room, &room_id, &http).await?);
            }
            "leave" => match joined.take() {
                Some(joined) => {
                    joined.leave().await?;
                    emit(serde_json::json!({ "event": "left" }));
                }
                None => eprintln!("[peer] not joined, ignoring leave"),
            },
            "quit" => break,
            other => eprintln!("[peer] unknown command {other:?}"),
        }

        if let Some(active) = joined.as_mut() {
            pending = watch_call(&cfg, active, &mut commands).await?;
            if pending.is_none() {
                break;
            }
        }
    }

    if let Some(joined) = joined {
        joined.leave().await?;
        emit(serde_json::json!({ "event": "left" }));
    }
    Ok(())
}

/// A live call plus the publishers that have to outlive the `join` call —
/// dropping either handle stops the media Element Call is rendering.
struct Joined {
    call: Call,
    _tone: media::ToneHandle,
    video: tokio::task::JoinHandle<()>,
}

impl Joined {
    async fn leave(self) -> Result<(), Box<dyn Error>> {
        self.video.abort();
        self.call.leave().await?;
        Ok(())
    }
}

/// Feed stdin lines into a channel, so the call loop can select on them.
fn stdin_lines() -> mpsc::UnboundedReceiver<String> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::task::spawn_local(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

/// Register a fresh throwaway user, mirroring `tests/e2e_call/provision.rs`.
async fn register_user(
    http: &reqwest::Client,
    homeserver: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let suffix = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    let user = format!("rust-{suffix}");
    let password = format!("test-{suffix}");

    let response = http
        .post(format!(
            "{}/_matrix/client/v3/register",
            homeserver.trim_end_matches('/')
        ))
        .json(&serde_json::json!({
            "username": user,
            "password": password,
            "auth": {"type": "m.login.dummy"},
            // We log in ourselves, for a fresh device plus a sync service.
            "inhibit_login": true,
        }))
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("registration of {user} failed: {status} {body}").into());
    }
    eprintln!("[peer] registered {user}");
    Ok((user, password))
}

/// Log in and start syncing. The account is one device old, so it self-signs
/// and MSC4153's cross-signed-sender requirement holds at its strictest — the
/// same reasoning as `e2e_call`, and the reason there is no recovery-key dance
/// here.
async fn login_and_sync(
    cfg: &Config,
    user: &str,
    password: &str,
) -> Result<(Client, SyncService), Box<dyn Error>> {
    let mut builder = Client::builder()
        .homeserver_url(&cfg.homeserver)
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: true,
            ..EncryptionSettings::default()
        });
    if cfg.insecure_tls {
        builder = builder.disable_ssl_verification();
    } else if let Some(pem) = dev_ca_pem() {
        // matrix-sdk's own reqwest instance; see `dev_ca_pem`.
        builder = builder.add_root_certificates(
            matrix_sdk::reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|error| format!("SSL_CERT_FILE is not a PEM bundle: {error}"))?,
        );
    }
    let client = builder.build().await?;
    client
        .matrix_auth()
        .login_username(user, password)
        .initial_device_display_name("matrix-rtc interop peer")
        .send()
        .await?;
    // Bootstrapping runs in the background; block until the device really is
    // cross-signed so key exchange cannot race it.
    client
        .encryption()
        .wait_for_e2ee_initialization_tasks()
        .await;
    eprintln!(
        "[peer] logged in as {:?} (device {:?})",
        client.user_id(),
        client.device_id()
    );

    let sync = SyncService::builder(client.clone()).build().await?;
    sync.start().await;

    Ok((client, sync))
}

/// A fresh **encrypted** room with `shared` history visibility, `invitee`
/// invited, and the call-member state event opened up to PL 0.
async fn create_encrypted_room(
    client: &Client,
    name: &str,
    invitee: &UserId,
) -> Result<OwnedRoomId, Box<dyn Error>> {
    let mut request = CreateRoomRequest::new();
    request.name = Some(name.to_owned());
    request.invite = vec![invitee.to_owned()];
    // `org.matrix.msc3401.call.member` is a *state* event, so `state_default`
    // (50) gates it and the invitee, at PL 0, could never publish a membership
    // in the pre-sticky dialect. Real Element Call rooms ship exactly this
    // override, which is why nobody hits it there. Harmless for the sticky
    // dialects, which never send the type at all.
    request.power_level_content_override = Some(
        Raw::new(&serde_json::json!({
            "events": { matrix_rtc_livekit::compat::STATE_MEMBER_EVENT_TYPE: 0 },
        }))?
        .cast_unchecked(),
    );
    request.initial_state = vec![
        InitialStateEvent::with_empty_state_key(RoomHistoryVisibilityEventContent::new(
            HistoryVisibility::Shared,
        ))
        .to_raw_any(),
    ];

    let room = client.create_room(request).await?;
    room.enable_encryption().await?;
    Ok(room.room_id().to_owned())
}

async fn wait_for_room(
    client: &Client,
    room_id: &RoomId,
) -> Result<matrix_sdk::Room, Box<dyn Error>> {
    for _ in 0..60 {
        if let Some(room) = client.get_room(room_id) {
            return Ok(room);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("room {room_id} did not sync within 30s").into())
}

async fn wait_for_joined_members(
    room: &matrix_sdk::Room,
    target: usize,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..120 {
        let count = room
            .members(RoomMemberships::JOIN)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        if count >= target {
            eprintln!("[peer] room has {count} joined members");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("room did not reach {target} joined members within 60s").into())
}

/// Open the slot if the dialect has one, join, and start publishing.
async fn join_call(
    cfg: &Config,
    client: &Client,
    room: &matrix_sdk::Room,
    room_id: &RoomId,
    http: &reqwest::Client,
) -> Result<Joined, Box<dyn Error>> {
    // The pre-sticky generation has no slot concept at all. Publishing one
    // there is a claim about the room that Element Call never makes, and the
    // bridge leaves the slot condition unenforced in that mode anyway.
    if cfg.compat != ElementCallCompat::StateEvents {
        open_slot(
            client,
            room_id.as_str(),
            &cfg.slot_id,
            "m.call",
            Some(SlotEncryption {
                encryption_type: "m.per_member".to_owned(),
                extra: Default::default(),
            }),
        )
        .await?;
        eprintln!("[peer] opened slot {}", cfg.slot_id);
    }

    let call = Call::join(
        room,
        CallOptions {
            slot_id: cfg.slot_id.clone(),
            livekit_service_url_fallback: cfg.livekit_service_url.clone(),
            http: Some(http.clone()),
            element_call_compat: cfg.compat,
            ..CallOptions::default()
        },
    )
    .await?;

    // Publish both: Element Call sits on "Waiting for media..." until it has
    // something it can decode, and video is the half that has historically
    // broken silently (see the simulcast note on PATTERN_WIDTH).
    let tone = media::publish_tone(call.session(), 440.0).await?;
    let video = publish_pattern_video(&call).await?;

    emit(serde_json::json!({
        "event": "joined",
        "identity": call.local_identity(),
        "membership_id": call.membership_id(),
    }));
    Ok(Joined {
        call,
        _tone: tone,
        video,
    })
}

/// Publish the pattern as a camera track at ~15 fps.
async fn publish_pattern_video(call: &Call) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error>> {
    let track = call
        .publish(PublishOptions::camera(VideoSourceConfig {
            width: PATTERN_WIDTH,
            height: PATTERN_HEIGHT,
        }))
        .await?;
    Ok(tokio::task::spawn_local(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(66));
        let mut captured = 0u64;
        loop {
            ticker.tick().await;
            if let Err(error) = track.capture_video(pattern_frame()) {
                eprintln!("[peer] video capture stopped after {captured} frames: {error}");
                break;
            }
            captured += 1;
        }
    }))
}

/// An I420 frame whose left half is bright (Y=235) and right half dark (Y=16)
/// — a split that survives VP8 compression comfortably.
fn pattern_frame() -> VideoFrame {
    let width = PATTERN_WIDTH as usize;
    let height = PATTERN_HEIGHT as usize;
    let chroma_width = PATTERN_WIDTH.div_ceil(2) as usize;
    let chroma_height = PATTERN_HEIGHT.div_ceil(2) as usize;

    let mut data_y = vec![16u8; width * height];
    for row in data_y.chunks_exact_mut(width) {
        row[..width / 2].fill(235);
    }
    VideoFrame {
        buffer: I420Buffer {
            width: PATTERN_WIDTH,
            height: PATTERN_HEIGHT,
            data_y,
            stride_y: PATTERN_WIDTH,
            data_u: vec![128u8; chroma_width * chroma_height],
            stride_u: chroma_width as u32,
            data_v: vec![128u8; chroma_width * chroma_height],
            stride_v: chroma_width as u32,
        },
        rotation: VideoRotation::Deg0,
        timestamp_us: 0,
    }
}

/// Watch a joined call, reporting what it observes, until the next stdin
/// command arrives — which is returned for the caller's loop to dispatch, so a
/// command read here is never swallowed. `None` means stdin closed.
async fn watch_call(
    cfg: &Config,
    joined: &mut Joined,
    commands: &mut mpsc::UnboundedReceiver<String>,
) -> Result<Option<String>, Box<dyn Error>> {
    /// How long a single wait for an SFU event lasts before the loop goes
    /// back round to re-poll the counters.
    const POLL: Duration = Duration::from_millis(500);

    let call = &mut joined.call;
    let mut last_members = 0usize;
    // The identity the *SFU* assigned the peer, not one we derived. Getting
    // that derivation wrong is the failure mode with no error message: tracks
    // buffer forever and keys install under an identity nobody has.
    let mut peer_identity: Option<String> = None;
    let mut key_reported = false;
    let mut audio_reported = false;

    loop {
        // Polled here rather than in a `select!` arm: `call.events()` borrows
        // the call mutably for the whole of a select, which cannot coexist
        // with these shared borrows. Serialising them costs nothing at this
        // cadence.
        let members = call.member_count().await;
        if members != last_members {
            last_members = members;
            emit(serde_json::json!({ "event": "members", "count": members }));
        }
        if let Some(identity) = peer_identity.as_deref()
            && !key_reported
            && call.imported_key_for(identity)
        {
            key_reported = true;
            emit(serde_json::json!({ "event": "key_imported", "identity": identity }));
        }

        // `UnboundedReceiver::recv` is cancel-safe, so timing it out here
        // cannot drop an event.
        let event = tokio::select! {
            biased;
            command = commands.recv() => return Ok(command),
            event = tokio::time::timeout(POLL, call.events().recv()) => event,
        };
        let event = match event {
            Err(_elapsed) => continue,
            Ok(None) => return Err("SFU event stream ended (call over)".into()),
            Ok(Some(event)) => event,
        };

        match event {
            RoomEvent::TrackSubscribed {
                track: RemoteTrack::Audio(audio),
                participant,
                ..
            } => {
                let identity = participant.identity().to_string();
                emit(serde_json::json!({
                    "event": "track_subscribed",
                    "kind": "audio",
                    "identity": identity,
                }));
                peer_identity = Some(identity);

                if !audio_reported {
                    audio_reported = true;
                    let pcm =
                        media::record_track(&audio, Duration::from_secs(cfg.record_secs)).await;
                    if let Some(path) = &cfg.out_wav
                        && let Err(error) = media::write_wav(path, &pcm, media::SAMPLE_RATE)
                    {
                        eprintln!("[peer] could not write {path}: {error}");
                    }
                    emit(serde_json::json!({
                        "event": "audio_rms",
                        "value": rms(&pcm),
                        "floor": AUDIO_RMS_FLOOR,
                        "samples": pcm.len(),
                        // Diagnostic only: Element Call's source is Chrome's
                        // pulsed fake device, not a sine, so the assertion is
                        // on energy rather than on this.
                        "tone_440": media::detect_tone(&pcm, media::SAMPLE_RATE, 440.0),
                    }));
                }
            }
            RoomEvent::TrackSubscribed { participant, .. } => {
                emit(serde_json::json!({
                    "event": "track_subscribed",
                    "kind": "video",
                    "identity": participant.identity().to_string(),
                }));
            }
            RoomEvent::Disconnected { reason } => {
                return Err(format!("disconnected from the SFU: {reason:?}").into());
            }
            _ => {}
        }
    }
}

fn rms(pcm: &[i16]) -> f64 {
    if pcm.is_empty() {
        return 0.0;
    }
    let sum: f64 = pcm.iter().map(|&s| f64::from(s) * f64::from(s)).sum();
    (sum / pcm.len() as f64).sqrt()
}
