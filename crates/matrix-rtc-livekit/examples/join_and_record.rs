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

//! Join a MatrixRTC call with [`Call::join`] and record the first remote audio
//! track to a WAV file — the crate's quick start, runnable.
//!
//! Against the `demo/backend` stack (`make backend-up`), register two users
//! (see `demo/backend/README.md`), create a room with both joined, then run
//! two instances of this example — one publishing a test tone, one recording:
//!
//! ```sh
//! # terminal 1: opens the slot and publishes a 440 Hz tone
//! MX_USER=alice MX_PASSWORD=secret ROOM_ID='!room:synapse' \
//! OPEN_SLOT=1 PUBLISH_TONE=1 \
//! cargo run -p matrix-rtc-livekit --example join_and_record --features matrix-sdk,testing
//!
//! # terminal 2: records 5 s of whatever the SFU forwards
//! MX_USER=bob MX_PASSWORD=secret ROOM_ID='!room:synapse' \
//! cargo run -p matrix-rtc-livekit --example join_and_record --features matrix-sdk,testing
//! ```
//!
//! Env vars: `HOMESERVER_URL` (default `http://localhost:8008`), `MX_USER`,
//! `MX_PASSWORD`, `ROOM_ID` (required), `SLOT_ID` (default `m.call#ROOM`),
//! `LIVEKIT_SERVICE_URL` (default `http://localhost:6080`, used when the
//! homeserver doesn't advertise a transport), `RECOVERY_KEY` (see below),
//! `OPEN_SLOT`, `PUBLISH_TONE`, `RECORD_SECS` (default 5), `OUT_WAV`
//! (default `<temp_dir>/call_recording.wav`), `INSECURE_TLS`.
//!
//! **Cross-signing:** every run logs a fresh device into the account, and
//! MSC4153 (enforced by the core) requires media-key senders to be
//! cross-signed. The first run per user sets up cross-signing + recovery and
//! prints a recovery key; pass it as `RECOVERY_KEY` on subsequent runs so the
//! new device joins the existing identity.

use std::env;
use std::error::Error;
use std::time::Duration;

use livekit::{RoomEvent, track::RemoteTrack};
use matrix_rtc_core::SlotEncryption;
use matrix_rtc_livekit::{Call, CallOptions, media, open_slot};
use matrix_sdk::encryption::EncryptionSettings;
use matrix_sdk::ruma::RoomId;
use matrix_sdk_ui::sync_service::SyncService;

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    env::var(name).map_err(|_| format!("missing required env var {name}").into())
}

fn main() -> Result<(), Box<dyn Error>> {
    // The dependency tree enables both rustls crypto backends (`ring` via
    // livekit/reqwest, `aws-lc-rs` via matrix-sdk), so rustls can't auto-select
    // a process-level provider; install one before any TLS happens.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls aws-lc-rs crypto provider");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // `Call::join` drives `!Send` futures internally, so it must run inside a
    // `LocalSet` — this runtime skeleton is part of the quick start.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(tokio::task::LocalSet::new().run_until(run()))
}

async fn run() -> Result<(), Box<dyn Error>> {
    let homeserver =
        env::var("HOMESERVER_URL").unwrap_or_else(|_| "http://localhost:8008".to_owned());
    let user = required("MX_USER")?;
    let password = required("MX_PASSWORD")?;
    let room_id = RoomId::parse(required("ROOM_ID")?)?;
    let slot_id = env::var("SLOT_ID").unwrap_or_else(|_| "m.call#ROOM".to_owned());
    let livekit_service_url =
        env::var("LIVEKIT_SERVICE_URL").unwrap_or_else(|_| "http://localhost:6080".to_owned());
    let insecure_tls = env::var("INSECURE_TLS").is_ok();
    // Set for a call shared with Element Call on the JS SDK: our membership then
    // also carries the pre-2026 fields it needs, and our media key goes out
    // under the to-device type it listens for (and only that one).
    let legacy_element_call = env::var("LEGACY_ELEMENT_CALL").is_ok();
    let record_secs: u64 = env::var("RECORD_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    // 1. Log in and start syncing. Under `unstable-msc4354`, sliding sync
    //    auto-enables the sticky-events extension that carries `m.rtc.member`.
    //    Cross-signing is bootstrapped automatically on a first-ever login.
    let mut builder = matrix_sdk::Client::builder()
        .homeserver_url(&homeserver)
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: true,
            ..EncryptionSettings::default()
        });
    if insecure_tls {
        builder = builder.disable_ssl_verification();
    }
    let client = builder.build().await?;
    client
        .matrix_auth()
        .login_username(&user, &password)
        .initial_device_display_name("matrix-rtc-livekit join_and_record example")
        .send()
        .await?;
    println!("logged in as {user}");
    client
        .encryption()
        .wait_for_e2ee_initialization_tasks()
        .await;

    // 2. Cross-sign this device. MSC4153 requires media-key senders to be
    //    cross-signed and the core enforces it by default, but each run of
    //    this example logs a *fresh* device into the account — only the
    //    account's recovery key can bring it into the existing identity.
    //    The first run per user sets recovery up and prints that key.
    let recovery = client.encryption().recovery();
    match env::var("RECOVERY_KEY") {
        Ok(key) => {
            recovery.recover(key.trim()).await?;
            println!("recovered cross-signing; this device is now cross-signed");
        }
        Err(_) => match recovery.enable().await {
            Ok(key) => {
                println!("recovery enabled for {user}; pass RECOVERY_KEY='{key}' to future runs")
            }
            Err(error) => {
                return Err(format!(
                    "could not enable recovery ({error}); if this account was used before, \
                     rerun with RECOVERY_KEY=<key printed on its first run> so this new \
                     device can be cross-signed, or register a fresh user"
                )
                .into());
            }
        },
    }

    let sync = SyncService::builder(client.clone()).build().await?;
    sync.start().await;

    // 3. Wait for the room to come down sync.
    let mut room = None;
    for _ in 0..60 {
        room = client.get_room(&room_id);
        if room.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let room = room.ok_or("room did not sync within 30s — is the user a member?")?;

    // 4. Optionally open the slot (needs the room power level for `m.rtc.slot`
    //    state; typically the room creator does this once per call).
    if env::var("OPEN_SLOT").is_ok() {
        open_slot(
            &client,
            room_id.as_str(),
            &slot_id,
            "m.call",
            Some(SlotEncryption {
                encryption_type: "m.per_member".to_owned(),
                extra: Default::default(),
            }),
        )
        .await?;
        println!("opened slot {slot_id}");
    }

    // 5. Join the call: membership signalling, key exchange, and an
    //    E2EE-enabled SFU connection in one step.
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(insecure_tls)
        .build()?;
    let mut call = Call::join(
        &room,
        CallOptions {
            slot_id,
            livekit_service_url_fallback: Some(livekit_service_url),
            http: Some(http),
            legacy_element_call,
            ..CallOptions::default()
        },
    )
    .await?;
    println!(
        "joined the call as {} (frame E2EE enabled); waiting for a remote audio track...",
        call.local_identity()
    );

    // Optional: publish a 440 Hz test tone so another instance of this example
    // has something to record (`media::publish_tone` is test-gated — hence the
    // `testing` feature). A real publisher would publish a microphone track via
    // `call.session().room().local_participant().publish_track(...)`.
    let _tone = if env::var("PUBLISH_TONE").is_ok() {
        println!("publishing a 440 Hz tone; Ctrl-C to hang up");
        Some(media::publish_tone(call.session(), 440.0).await?)
    } else {
        None
    };

    // 6. React to room events: record the first subscribed audio track, or —
    //    when publishing — keep the call up until Ctrl-C.
    let out_wav = env::var("OUT_WAV").unwrap_or_else(|_| {
        env::temp_dir()
            .join("call_recording.wav")
            .to_string_lossy()
            .into_owned()
    });
    loop {
        let event = tokio::select! {
            event = call.events().recv() => match event {
                Some(event) => event,
                // Stream end == call over (leave or unrecoverable disconnect;
                // a Disconnected event with the reason precedes it).
                None => break,
            },
            _ = tokio::signal::ctrl_c() => break,
        };
        match event {
            RoomEvent::TrackSubscribed {
                track: RemoteTrack::Audio(audio),
                participant,
                ..
            } => {
                println!(
                    "recording {record_secs}s from {:?}...",
                    participant.identity()
                );
                let pcm = media::record_track(&audio, Duration::from_secs(record_secs)).await;
                media::write_wav(&out_wav, &pcm, media::SAMPLE_RATE)?;
                println!("wrote {out_wav} ({} samples)", pcm.len());
                break;
            }
            RoomEvent::ParticipantConnected(participant) => {
                println!("participant connected: {:?}", participant.identity());
            }
            RoomEvent::Disconnected { reason } => {
                println!("disconnected from SFU: {reason:?}");
                break;
            }
            _ => {}
        }
    }

    // 7. Hang up: sends the leave event and closes the SFU connection.
    call.leave().await?;
    println!("left the call");
    Ok(())
}
