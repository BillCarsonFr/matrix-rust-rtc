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
//! 1. Both `alice` and `bob` log in and start a sync service.
//! 2. `alice` **creates an encrypted room**, sets history visibility to
//!    `shared`, and **invites** `bob`.
//! 3. `bob` **joins** the room; both sides wait until the two-member room
//!    membership has settled.
//! 4. Only then do both peers join the same MatrixRTC slot: each publishes its
//!    own `m.rtc.member` membership as an MSC4354 sticky event (plus a dead
//!    man's switch delayed leave) and discovers the other via the sticky-event
//!    subscription.
//! 5. Both connect to the LiveKit SFU; `alice` publishes a 440 Hz tone and
//!    `bob` records what the SFU forwards and verifies the frequency.
//!
//! Run against the `demo/backend` stack (see its README). Element Web is NOT
//! needed — the two Rust clients are each other's peer, and the room is created
//! on the fly, so no pre-existing `ROOM_ID` is required:
//!
//! ```sh
//! HOMESERVER_URL=https://synapse.m.localhost \
//! ALICE=alice ALICE_PW=secret BOB=bob BOB_PW=secret \
//! SLOT_ID='m.call#ROOM' \
//! LIVEKIT_SERVICE_URL=https://matrix-rtc.m.localhost/livekit/jwt INSECURE_TLS=1 \
//! cargo run -p matrix-rtc-livekit --features matrix-sdk,testing --example e2e_call
//! ```

use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use livekit::{RoomEvent, track::RemoteTrack};
use matrix_sdk::ruma::api::client::room::create_room::v3::Request as CreateRoomRequest;
use matrix_sdk::ruma::events::InitialStateEvent;
use matrix_sdk::ruma::events::room::history_visibility::{
    HistoryVisibility, RoomHistoryVisibilityEventContent,
};
use matrix_sdk::ruma::{OwnedRoomId, RoomId, UserId};
use matrix_sdk::{Client, RoomMemberships};
use matrix_sdk_ui::sync_service::SyncService;
use tokio::sync::Mutex;

use matrix_rtc_core::{JoinSessionParams, LiveKitTransport, RtcSessionManager, RtcTransport};
use matrix_rtc_livekit::{
    LiveKitConnection, LiveKitTransportConfig, MemberClaims, SdkCommandSender, connect, media,
    run_sticky_bridge,
};

type Manager = Arc<Mutex<RtcSessionManager<SdkCommandSender>>>;

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    env::var(name).map_err(|_| format!("missing required env var {name}").into())
}

/// A logged-in, synced client (before it has joined any RTC session).
struct SyncedClient {
    client: Client,
    sync: SyncService,
}

/// A participant that has joined the RTC session and has a live SFU connection.
struct Participant {
    manager: Manager,
    connection: LiveKitConnection,
    // Kept alive for the duration of the call (dropping stops sync / the bridge).
    _sync: SyncService,
    _bridge: tokio::task::JoinHandle<()>,
    // Aborted at teardown so a heartbeat tick can't re-arm a delayed leave after
    // `leave` has cancelled it.
    heartbeat: tokio::task::JoinHandle<()>,
}

struct Config {
    homeserver: String,
    slot_id: String,
    livekit_service_url: String,
    insecure_tls: bool,
}

/// Log in and start the sync service. Under `unstable-msc4354`, sliding sync
/// auto-enables the sticky-events extension, so `m.rtc.member` stickies flow
/// into the base room's sticky store (see `matrix_bridge`).
async fn login_and_sync(
    cfg: &Config,
    user: &str,
    password: &str,
) -> Result<SyncedClient, Box<dyn Error>> {
    let mut builder = Client::builder().homeserver_url(&cfg.homeserver);
    if cfg.insecure_tls {
        builder = builder.disable_ssl_verification();
    }
    let client = builder.build().await?;
    client
        .matrix_auth()
        .login_username(user, password)
        .initial_device_display_name("matrix-rtc-livekit e2e_call")
        .send()
        .await?;
    let user_id = client
        .user_id()
        .ok_or("no user id after login")?
        .to_string();
    let device_id = client
        .device_id()
        .ok_or("no device id after login")?
        .to_string();
    println!("[{user}] logged in as {user_id} (device {device_id})");

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

/// Join the RTC session for an already-synced client that is a joined member of
/// `room`: publishes our own membership sticky + arms the dead man's switch
/// delayed leave, then exchanges a token and connects to the SFU.
async fn join_rtc(
    cfg: &Config,
    room_id: &str,
    synced: SyncedClient,
    room: matrix_sdk::Room,
    user: &str,
) -> Result<Participant, Box<dyn Error>> {
    let SyncedClient { client, sync } = synced;
    let user_id = client.user_id().ok_or("no user id")?.to_string();
    let device_id = client.device_id().ok_or("no device id")?.to_string();

    // Wire the manager + sticky bridge.
    let manager: Manager = Arc::new(Mutex::new(RtcSessionManager::with_command_sender(
        Arc::new(SdkCommandSender::new(client.clone())),
    )));
    // The core's `RtcCommandSender` is `?Send`, so futures that drive the manager
    // are `!Send` and must run on the current thread via `spawn_local` (the caller
    // wraps the flow in a `LocalSet`).
    let bridge = tokio::task::spawn_local(run_sticky_bridge(room.clone(), manager.clone()));

    // Join the RTC session: publishes our own membership sticky + arms the dead
    // man's switch delayed leave.
    let membership_id = format!("{user_id}-{device_id}");
    let mut params = JoinSessionParams::new(
        user_id.clone(),
        device_id.clone(),
        room_id.to_owned(),
        cfg.slot_id.clone(),
        "m.call".to_owned(),
        RtcTransport::LiveKit(LiveKitTransport {
            livekit_service_url: cfg.livekit_service_url.clone(),
        }),
    );
    params.membership_id = Some(membership_id.clone());
    manager.lock().await.join(params).await?;
    println!("[{user}] joined RTC session (membership {membership_id})");

    // Heartbeat loop so the delayed leave keeps getting pushed back.
    let heartbeat = {
        let manager = manager.clone();
        let room_id = room_id.to_owned();
        let slot_id = cfg.slot_id.clone();
        tokio::task::spawn_local(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(15));
            loop {
                ticker.tick().await;
                manager.lock().await.heartbeat(&room_id, &slot_id).await;
            }
        })
    };

    // Token exchange + SFU connect (the client is the OpenID token source).
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(cfg.insecure_tls)
        .build()?;
    let lk_config = LiveKitTransportConfig {
        livekit_service_url: cfg.livekit_service_url.clone(),
        room_id: room_id.to_owned(),
        slot_id: cfg.slot_id.clone(),
        member: MemberClaims {
            id: membership_id,
            claimed_user_id: user_id.clone(),
            claimed_device_id: device_id,
        },
    };
    let connection = connect(&http, &lk_config, &client).await?;
    println!("[{user}] connected to the SFU");

    Ok(Participant {
        manager,
        connection,
        _sync: sync,
        _bridge: bridge,
        heartbeat,
    })
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

/// Poll a manager's member count until it reaches `target`, or time out.
async fn wait_for_members(
    manager: &Manager,
    room_id: &str,
    slot_id: &str,
    target: usize,
    label: &str,
) -> bool {
    for _ in 0..60 {
        let count = manager
            .lock()
            .await
            .member_count(room_id, slot_id)
            .unwrap_or(0);
        if count >= target {
            println!("[{label}] sees {count} members (sticky round-trip OK)");
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("[{label}] WARNING: did not observe {target} members within 30s");
    false
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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

    let cfg = Config {
        homeserver: required("HOMESERVER_URL")?,
        slot_id: env::var("SLOT_ID").unwrap_or_else(|_| "m.call#ROOM".to_owned()),
        livekit_service_url: required("LIVEKIT_SERVICE_URL")?,
        insecure_tls: env::var("INSECURE_TLS").is_ok(),
    };
    let alice_user = required("ALICE")?;
    let alice_pw = required("ALICE_PW")?;
    let bob_user = required("BOB")?;
    let bob_pw = required("BOB_PW")?;

    // The manager/bridge/heartbeat futures are `!Send` (the core command sender
    // is `?Send`), so the whole flow runs on a single-thread `LocalSet`.
    tokio::task::LocalSet::new()
        .run_until(run(cfg, alice_user, alice_pw, bob_user, bob_pw))
        .await
}

async fn run(
    cfg: Config,
    alice_user: String,
    alice_pw: String,
    bob_user: String,
    bob_pw: String,
) -> Result<(), Box<dyn Error>> {
    // 1. Both clients log in and sync.
    let alice = login_and_sync(&cfg, &alice_user, &alice_pw).await?;
    let bob = login_and_sync(&cfg, &bob_user, &bob_pw).await?;
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

    // 4. Now both join the RTC session (membership stickies + SFU connect).
    let room_id_str = room_id.to_string();
    let alice = join_rtc(&cfg, &room_id_str, alice, alice_room, &alice_user).await?;
    let mut bob = join_rtc(&cfg, &room_id_str, bob, bob_room, &bob_user).await?;

    // Signalling proof: each side discovers the other's membership
    // via stickies (own + peer == 2).
    let alice_sees = wait_for_members(&alice.manager, &room_id_str, &cfg.slot_id, 2, "alice").await;
    let bob_sees = wait_for_members(&bob.manager, &room_id_str, &cfg.slot_id, 2, "bob").await;

    // 5. Media proof: alice publishes a 440 Hz tone; bob records what the SFU
    //    forwards and checks the frequency.
    println!("[alice] publishing 440 Hz tone");
    let _tone = media::publish_tone(&alice.connection.session, 440.0).await?;

    let tone_ok = record_and_verify_tone(&mut bob).await?;

    // Tear down both peers cleanly and symmetrically: each stops its heartbeat,
    // sends its leave sticky (cancelling its delayed leave), and closes its SFU
    // connection.
    leave_and_close(alice, &room_id_str, &cfg.slot_id, "alice").await;
    leave_and_close(bob, &room_id_str, &cfg.slot_id, "bob").await;

    println!("\n=== RESULT ===");
    println!("sticky membership discovered by alice: {alice_sees}");
    println!("sticky membership discovered by bob:   {bob_sees}");
    println!("440 Hz tone received + verified by bob: {tone_ok}");

    if alice_sees && bob_sees && tone_ok {
        println!("END-TO-END TEST PASSED");
        Ok(())
    } else {
        Err("end-to-end test failed (see WARNING lines above)".into())
    }
}

/// Leave the RTC session cleanly and close the SFU connection.
///
/// Stops the heartbeat first so it can't re-arm a delayed leave after `leave`
/// cancels the current one, then sends the leave sticky and closes the SFU
/// connection. Consumes the participant (`close` takes the session by value);
/// failures are logged rather than aborting teardown of the other peer.
async fn leave_and_close(participant: Participant, room_id: &str, slot_id: &str, label: &str) {
    participant.heartbeat.abort();
    if let Err(error) = participant
        .manager
        .lock()
        .await
        .leave(room_id.to_owned(), slot_id.to_owned(), Default::default())
        .await
    {
        eprintln!("[{label}] leave failed: {error}");
    }
    if let Err(error) = participant.connection.session.close().await {
        eprintln!("[{label}] close failed: {error}");
    }
}

/// Consume bob's SFU events until a remote audio track is subscribed, record a
/// couple of seconds, write a WAV, and verify the 440 Hz tone.
async fn record_and_verify_tone(bob: &mut Participant) -> Result<bool, Box<dyn Error>> {
    let events = &mut bob.connection.events;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        let event = match tokio::time::timeout_at(deadline, events.recv()).await {
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
                    if let Err(error) =
                        media::write_wav("/tmp/e2e_received.wav", &pcm, media::SAMPLE_RATE)
                    {
                        eprintln!("[bob] failed to write WAV: {error}");
                    } else {
                        println!("[bob] wrote /tmp/e2e_received.wav ({} samples)", pcm.len());
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
