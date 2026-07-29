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

//! Two-client end-to-end MatrixRTC call over LiveKit, driven entirely by the
//! Rust stack (no browser, no microphone).
//!
//! The flow mirrors a real call being set up from scratch:
//!
//! 1. Two throwaway users are provisioned on the homeserver (unless
//!    `ALICE`/`BOB` credentials are supplied), log in and start a sync service.
//! 2. `alice` **creates an encrypted room**, sets history visibility to
//!    `shared`, and **invites** `bob`.
//! 3. `bob` **joins** the room; both sides wait until the two-member room
//!    membership has settled.
//! 4. `alice` **opens the slot** with an `m.rtc.slot` state event; MSC4143
//!    only counts members as joined against an open slot.
//! 5. Only then do both peers join the call through the [`Call`] facade, which
//!    publishes each side's `m.rtc.member` membership as an MSC4354 sticky
//!    event (plus a dead man's switch delayed leave), exchanges media keys,
//!    and connects to the SFU with frame E2EE.
//! 6. `alice` publishes a 440 Hz tone and `bob` records what the SFU forwards
//!    and verifies the frequency.
//!
//! Runs against the `demo/backend` stack (see its README) with no further
//! configuration — every endpoint defaults to the stack's localhost ports and
//! users are created on the fly:
//!
//! ```sh
//! make backend-up
//! cargo test -p matrix-rtc-livekit --features matrix-sdk,testing \
//!     --test e2e_call -- --ignored --nocapture
//! ```

mod provision;

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
use matrix_sdk::ruma::{OwnedRoomId, RoomId, UserId};
use matrix_sdk::{Client, RoomMemberships};
use matrix_sdk_ui::sync_service::SyncService;

use matrix_rtc_core::SlotEncryption;
use matrix_rtc_livekit::{Call, CallOptions, media, open_slot};

use provision::Credentials;

/// Hard cap on the whole flow so a wedged stack fails the test instead of
/// eating the CI job timeout.
const OVERALL_DEADLINE: Duration = Duration::from_secs(300);

/// A logged-in, synced client (before it has joined any RTC session).
struct SyncedClient {
    client: Client,
    sync: SyncService,
}

/// A participant on a live call. The sync service is kept alive alongside the
/// call (dropping it stops the sticky/key traffic the call depends on).
struct Participant {
    call: Call,
    _sync: SyncService,
}

struct Config {
    homeserver: String,
    slot_id: String,
    livekit_service_url: String,
    insecure_tls: bool,
}

impl Config {
    /// Defaults target the `demo/backend` compose stack; every value can be
    /// overridden to point the same test at a remote (TLS) deployment.
    fn from_env() -> Self {
        Config {
            homeserver: env::var("HOMESERVER_URL")
                .unwrap_or_else(|_| "http://localhost:8008".to_owned()),
            slot_id: env::var("SLOT_ID").unwrap_or_else(|_| "m.call#ROOM".to_owned()),
            livekit_service_url: env::var("LIVEKIT_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:6080".to_owned()),
            insecure_tls: env::var("INSECURE_TLS").is_ok(),
        }
    }
}

/// Use `ALICE`/`BOB` (+`_PW`) when supplied (long-lived stacks with closed
/// registration); otherwise register fresh throwaway users for this run.
async fn credentials(cfg: &Config) -> Result<(Credentials, Credentials), Box<dyn Error>> {
    if let (Ok(alice), Ok(bob)) = (env::var("ALICE"), env::var("BOB")) {
        return Ok((
            Credentials {
                user: alice,
                password: env::var("ALICE_PW").map_err(|_| "ALICE is set but ALICE_PW is not")?,
            },
            Credentials {
                user: bob,
                password: env::var("BOB_PW").map_err(|_| "BOB is set but BOB_PW is not")?,
            },
        ));
    }

    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(cfg.insecure_tls)
        .build()?;
    let suffix = provision::run_suffix();
    let alice = provision::register_user(&http, &cfg.homeserver, "alice", &suffix).await?;
    let bob = provision::register_user(&http, &cfg.homeserver, "bob", &suffix).await?;
    Ok((alice, bob))
}

/// Log in and start the sync service. Under `unstable-msc4354`, sliding sync
/// auto-enables the sticky-events extension, so `m.rtc.member` stickies flow
/// into the base room's sticky store (see `matrix_bridge`).
///
/// Cross-signing is bootstrapped at login: each user is freshly registered
/// with a single device, so that device self-signs and the MSC4153
/// cross-signed-sender requirement holds at its (default) strictest.
async fn login_and_sync(cfg: &Config, who: &Credentials) -> Result<SyncedClient, Box<dyn Error>> {
    let mut builder = Client::builder()
        .homeserver_url(&cfg.homeserver)
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: true,
            ..EncryptionSettings::default()
        });
    if cfg.insecure_tls {
        builder = builder.disable_ssl_verification();
    }
    let client = builder.build().await?;
    client
        .matrix_auth()
        .login_username(&who.user, &who.password)
        .initial_device_display_name("matrix-rtc-livekit e2e_call")
        .send()
        .await?;
    // The bootstrap runs as a background task; block until the device is
    // actually cross-signed so key exchange can't race it.
    client
        .encryption()
        .wait_for_e2ee_initialization_tasks()
        .await;
    let user_id = client
        .user_id()
        .ok_or("no user id after login")?
        .to_string();
    let device_id = client
        .device_id()
        .ok_or("no device id after login")?
        .to_string();
    println!("[{}] logged in as {user_id} (device {device_id})", who.user);

    let sync = SyncService::builder(client.clone()).build().await?;
    sync.start().await;

    Ok(SyncedClient { client, sync })
}

/// `alice` creates a fresh **encrypted** room with `shared` history visibility
/// and invites `invitee`. Returns the new room id.
async fn create_encrypted_room(
    client: &Client,
    invitee: &UserId,
) -> Result<OwnedRoomId, Box<dyn Error>> {
    let mut request = CreateRoomRequest::new();
    request.invite = vec![invitee.to_owned()];
    request.initial_state = vec![
        InitialStateEvent::with_empty_state_key(RoomHistoryVisibilityEventContent::new(
            HistoryVisibility::Shared,
        ))
        .to_raw_any(),
    ];

    let room = client.create_room(request).await?;
    // Turn on megolm encryption for the room (sends `m.room.encryption`).
    room.enable_encryption().await?;
    Ok(room.room_id().to_owned())
}

/// Join the call through the [`Call`] facade — the very wiring the facade
/// exists to absorb, so this is deliberately thin.
async fn join_call(
    cfg: &Config,
    synced: SyncedClient,
    room: matrix_sdk::Room,
    user: &str,
) -> Result<Participant, Box<dyn Error>> {
    let SyncedClient { client: _, sync } = synced;
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(cfg.insecure_tls)
        .build()?;
    let call = Call::join(
        &room,
        CallOptions {
            slot_id: cfg.slot_id.clone(),
            livekit_service_url_fallback: Some(cfg.livekit_service_url.clone()),
            http: Some(http),
            ..CallOptions::default()
        },
    )
    .await?;
    println!(
        "[{user}] joined RTC session (membership {}) and connected to the SFU \
         (per-participant frame E2EE enabled)",
        call.membership_id()
    );

    Ok(Participant { call, _sync: sync })
}

/// Poll until the room is known to the client, or time out.
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

/// Poll until `room` reports at least `target` joined members, or time out.
async fn wait_for_joined_members(
    room: &matrix_sdk::Room,
    target: usize,
    label: &str,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..60 {
        let count = room
            .members(RoomMemberships::JOIN)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        if count >= target {
            println!("[{label}] room has {count} joined members");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("[{label}] room did not reach {target} joined members within 30s").into())
}

/// Poll a call's member count until it reaches `target`, or time out.
async fn wait_for_members(call: &Call, target: usize, label: &str) -> bool {
    let mut last_count = 0;
    for _ in 0..60 {
        last_count = call.member_count().await;
        if last_count >= target {
            println!("[{label}] sees {last_count} members (sticky round-trip OK)");
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // The count separates failure modes: 0 = the slot/room-state projection
    // dropped everyone (even ourselves); 1 = own membership round-tripped but
    // the peer's sticky never arrived.
    println!("[{label}] WARNING: observed {last_count} of {target} members within 30s");
    false
}

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up)"]
fn e2e_call_two_clients_audio() {
    // The dependency tree enables both rustls crypto backends (`ring` via
    // livekit/reqwest, `aws-lc-rs` via matrix-sdk), so rustls can't auto-select a
    // process-level `CryptoProvider` and panics on first TLS handshake. Install
    // one explicitly before any TLS happens.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls aws-lc-rs crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env();

    // The futures behind `Call::join` are `!Send` (the core command sender is
    // `?Send`), so the whole flow runs on a single-thread `LocalSet`.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    let outcome = runtime.block_on(
        tokio::task::LocalSet::new()
            .run_until(async { tokio::time::timeout(OVERALL_DEADLINE, run(cfg)).await }),
    );

    match outcome {
        Err(_) => panic!("e2e call did not finish within {OVERALL_DEADLINE:?}"),
        Ok(Err(error)) => panic!("e2e call failed: {error}"),
        Ok(Ok(())) => {}
    }
}

async fn run(cfg: Config) -> Result<(), Box<dyn Error>> {
    let (alice_creds, bob_creds) = credentials(&cfg).await?;

    // 1. Both clients log in and sync.
    let alice = login_and_sync(&cfg, &alice_creds).await?;
    let bob = login_and_sync(&cfg, &bob_creds).await?;
    let bob_id = bob.client.user_id().ok_or("bob has no user id")?.to_owned();

    // 2. Alice creates the encrypted room and invites bob.
    let room_id = create_encrypted_room(&alice.client, &bob_id).await?;
    println!("[alice] created encrypted room {room_id}, invited {bob_id}");

    // 3. Both sync the new room; bob accepts the invite, and we wait for the
    //    two-member membership to settle on both sides before any RTC signalling.
    let alice_room = wait_for_room(&alice.client, &room_id).await?;
    let bob_room = wait_for_room(&bob.client, &room_id).await?;
    bob_room.join().await?;
    println!("[bob] joined room {room_id}");
    wait_for_joined_members(&alice_room, 2, "alice").await?;
    wait_for_joined_members(&bob_room, 2, "bob").await?;

    // 4. Alice (the room creator, so the only one with the power level for it)
    //    opens the slot. Without an open `m.rtc.slot` in room state, MSC4143
    //    says no member of it counts as joined.
    open_slot(
        &alice.client,
        room_id.as_str(),
        &cfg.slot_id,
        "m.call",
        Some(SlotEncryption {
            encryption_type: "m.per_member".to_owned(),
            extra: Default::default(),
        }),
    )
    .await?;
    println!("[alice] opened slot {}", cfg.slot_id);

    // 5. Now both join the call (membership stickies + key exchange + SFU
    //    connect, all through the facade).
    let alice = join_call(&cfg, alice, alice_room, &alice_creds.user).await?;
    let mut bob = join_call(&cfg, bob, bob_room, &bob_creds.user).await?;

    // Signalling proof: each side discovers the other's membership
    // via stickies (own + peer == 2).
    let alice_sees = wait_for_members(&alice.call, 2, "alice").await;
    let bob_sees = wait_for_members(&bob.call, 2, "bob").await;

    // 6. Media proof: alice publishes a 440 Hz tone; bob records what the SFU
    //    forwards and checks the frequency.
    println!("[alice] publishing 440 Hz tone");
    let _tone = media::publish_tone(alice.call.session(), 440.0).await?;

    let tone_ok = record_and_verify_tone(&mut bob.call).await?;

    // Encryption proof: bob must have imported alice's media key under alice's
    // MSC4195 pseudonymous identity (the JWT `sub`) — direct evidence the key
    // crossed the wire and the identity mapping is correct. With GCM frame E2EE
    // enabled, bob could not have decoded the tone above without it.
    let alice_key_seen_by_bob = bob.call.imported_key_for(alice.call.local_identity());
    println!("[bob] imported alice's per-participant media key: {alice_key_seen_by_bob}");

    // Tear down both peers cleanly and symmetrically: `Call::leave` stops the
    // heartbeat, sends the leave event (cancelling the delayed leave), and
    // closes the SFU connection. Failures are logged rather than aborting
    // teardown of the other peer.
    for (participant, label) in [(alice, "alice"), (bob, "bob")] {
        if let Err(error) = participant.call.leave().await {
            eprintln!("[{label}] leave failed: {error}");
        }
    }

    println!("\n=== RESULT ===");
    println!("sticky membership discovered by alice: {alice_sees}");
    println!("sticky membership discovered by bob:   {bob_sees}");
    println!("alice's media key received by bob:      {alice_key_seen_by_bob}");
    println!("440 Hz tone received + verified by bob: {tone_ok}");

    if alice_sees && bob_sees && alice_key_seen_by_bob && tone_ok {
        println!("END-TO-END TEST PASSED (with per-participant frame E2EE)");
        Ok(())
    } else {
        Err("end-to-end test failed (see WARNING lines above)".into())
    }
}

/// Consume bob's SFU events until a remote audio track is subscribed, record a
/// couple of seconds, write a WAV, and verify the 440 Hz tone.
async fn record_and_verify_tone(call: &mut Call) -> Result<bool, Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        let event = match tokio::time::timeout_at(deadline, call.events().recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                println!("[bob] SFU event stream ended before any track arrived");
                return Ok(false);
            }
            Err(_) => {
                println!("[bob] WARNING: no remote track within 30s");
                return Ok(false);
            }
        };

        match event {
            RoomEvent::TrackSubscribed {
                track, participant, ..
            } => {
                println!("[bob] track subscribed from {:?}", participant.identity());
                if let RemoteTrack::Audio(audio) = track {
                    println!("[bob] recording ~2s of audio...");
                    let pcm = media::record_track(&audio, Duration::from_secs(2)).await;
                    // CI uploads this on failure, so keep the path predictable
                    // (temp_dir is /tmp on the linux runners).
                    let wav_path = std::env::temp_dir().join("e2e_received.wav");
                    let wav_path = wav_path.to_string_lossy();
                    if let Err(error) = media::write_wav(&wav_path, &pcm, media::SAMPLE_RATE) {
                        eprintln!("[bob] failed to write WAV: {error}");
                    } else {
                        println!("[bob] wrote {wav_path} ({} samples)", pcm.len());
                    }
                    let energy = media::detect_tone(&pcm, media::SAMPLE_RATE, 440.0);
                    println!("[bob] 440 Hz energy ratio: {energy:.3}");
                    return Ok(energy > 0.5);
                }
            }
            RoomEvent::Disconnected { reason } => {
                println!("[bob] disconnected from SFU: {reason:?}");
                return Ok(false);
            }
            _ => {}
        }
    }
}
