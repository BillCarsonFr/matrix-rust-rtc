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
//! 5. Only then do both peers join the same MatrixRTC slot: each publishes its
//!    own `m.rtc.member` membership as an MSC4354 sticky event (plus a dead
//!    man's switch delayed leave) and discovers the other via the sticky-event
//!    subscription.
//! 6. Both connect to the LiveKit SFU; `alice` publishes a 440 Hz tone and
//!    `bob` records what the SFU forwards and verifies the frequency.
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
use std::sync::Arc;
use std::time::Duration;

use livekit::{RoomEvent, track::RemoteTrack};
use matrix_sdk::deserialized_responses::{EncryptionInfo, VerificationLevel, VerificationState};
use matrix_sdk::ruma::api::client::room::create_room::v3::Request as CreateRoomRequest;
use matrix_sdk::ruma::api::client::rtc::transports::v1 as rtc_transports;
use matrix_sdk::ruma::events::room::history_visibility::{
    HistoryVisibility, RoomHistoryVisibilityEventContent,
};
use matrix_sdk::ruma::events::rtc::transport::RtcTransport as RumaRtcTransport;
use matrix_sdk::ruma::events::{AnyToDeviceEvent, InitialStateEvent};
use matrix_sdk::ruma::{OwnedRoomId, RoomId, UserId};
use matrix_sdk::{Client, RoomMemberships};
use matrix_sdk_ui::sync_service::SyncService;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use matrix_rtc_core::{
    EncryptionConfig, JoinSessionParams, KeyOrigin, LiveKitTransport, ReceivedEncryptionKey,
    RtcIdentityMapper, RtcSessionManager, RtcTransport, SlotEncryption, generate_member_id,
};
use matrix_rtc_livekit::{
    LiveKitConnection, LiveKitTransportConfig, MediaKeyBridge, MemberClaims, SdkCommandSender,
    connect_e2ee, identity::pseudonymous_identity, media, msc4195_key_provider, run_sticky_bridge,
};

use provision::Credentials;

/// Hard cap on the whole flow so a wedged stack fails the test instead of
/// eating the CI job timeout.
const OVERALL_DEADLINE: Duration = Duration::from_secs(300);

type Manager = Arc<Mutex<RtcSessionManager<SdkCommandSender>>>;

/// A media encryption key extracted from a peer's decrypted
/// `org.matrix.msc4143.rtc.encryption_key` to-device message, carried from the
/// (`Send`) event handler to the (`!Send`) key pump over an mpsc channel.
struct ReceivedKey {
    origin: KeyOrigin,
    room_id: String,
    member_id: String,
    key_index: u8,
    key_b64: String,
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
    // Frame-encryption key bridge: receives every media key signalled by the
    // core (own + peers) and imports it into the room's LiveKit KeyProvider.
    // Kept so the test can assert which peer keys were received and mapped.
    bridge: Arc<MediaKeyBridge>,
    // Our own MSC4195 pseudonymous LiveKit identity (the JWT `sub`); the peer
    // imports our key under this identity to decrypt our frames.
    own_identity: String,
    // Kept alive for the duration of the call (dropping stops sync / the bridge).
    _sync: SyncService,
    _bridge: tokio::task::JoinHandle<()>,
    // Drains received peer keys into the (!Send) manager on the LocalSet.
    key_pump: tokio::task::JoinHandle<()>,
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
async fn login_and_sync(cfg: &Config, who: &Credentials) -> Result<SyncedClient, Box<dyn Error>> {
    let mut builder = Client::builder().homeserver_url(&cfg.homeserver);
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

/// Ask the homeserver which RTC transports it offers, and take the LiveKit one.
///
/// Discovery is the application's job, not the SDK's: `matrix-rtc-core` takes
/// whichever transport it is handed. MSC4143 returns them in descending order of
/// preference, so the first LiveKit entry wins.
///
/// Falls back to `LIVEKIT_SERVICE_URL` when the homeserver does not implement
/// the endpoint yet, which keeps this runnable against older backends.
async fn discover_livekit_transport(
    client: &Client,
    fallback_url: &str,
) -> Result<LiveKitTransport, Box<dyn Error>> {
    match client.send(rtc_transports::Request::new()).await {
        Ok(response) => {
            for transport in response.rtc_transports {
                if let RumaRtcTransport::LiveKit(livekit) = transport {
                    println!(
                        "[discovery] homeserver offers livekit at {}",
                        livekit.service_url
                    );
                    return Ok(LiveKitTransport {
                        livekit_service_url: livekit.service_url,
                    });
                }
            }
            println!(
                "[discovery] homeserver advertises no livekit transport; using the configured URL"
            );
        }
        Err(error) => {
            println!(
                "[discovery] transports endpoint unavailable ({error}); using the configured URL"
            );
        }
    }

    Ok(LiveKitTransport {
        livekit_service_url: fallback_url.to_owned(),
    })
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
    let sticky_bridge = tokio::task::spawn_local(run_sticky_bridge(room.clone(), manager.clone()));

    // Frame encryption: a single shared KeyProvider handle feeds both the
    // LiveKit room (which encrypts our frames and decrypts peers') and the
    // MediaKeyBridge (which imports every key the core signals). MSC4195
    // per-participant HKDF mode.
    let provider = msc4195_key_provider();
    let bridge = Arc::new(MediaKeyBridge::with_provider(provider.clone()));

    // Receive path: peers distribute their media keys as Olm-encrypted
    // `m.rtc.encryption_key` to-device messages. The SDK decrypts and dispatches
    // them to a handler that must stay `Send`; forward the (Send) key bytes over
    // a channel to a `spawn_local` pump that drives the `!Send` manager.
    let (key_tx, mut key_rx) = unbounded_channel::<ReceivedKey>();
    register_key_receiver(&client, key_tx);

    // Join the RTC session: publishes our own membership sticky + arms the dead
    // man's switch delayed leave, then — still holding the manager lock so no
    // sticky update can interleave — wire the encryption manager to our bridge
    // and to the MSC4195 pseudonymous-identity derivation.
    // MSC4143 requires a fresh `member.id` on every join, so this must not be
    // derived from the (stable) user and device IDs.
    let membership_id = generate_member_id();
    let own_identity = pseudonymous_identity(&user_id, &device_id, &membership_id);
    let livekit = discover_livekit_transport(&client, &cfg.livekit_service_url).await?;
    let mut params = JoinSessionParams::new(
        user_id.clone(),
        device_id.clone(),
        room_id.to_owned(),
        cfg.slot_id.clone(),
        "m.call".to_owned(),
        RtcTransport::LiveKit(livekit.clone()),
    );
    params.membership_id = Some(membership_id.clone());
    // The two clients here are throwaway logins with no cross-signing set up, so
    // the MSC4153 requirement would discard every key they send each other. A
    // real client should leave this at its default (`true`).
    params.encryption_config = Some(EncryptionConfig {
        require_cross_signed_sender: false,
        ..EncryptionConfig::default()
    });
    {
        let mut mgr = manager.lock().await;
        mgr.join(params).await?;
        let identity_mapper: RtcIdentityMapper =
            Arc::new(|user_id: &str, device_id: &str, member_id: &str| {
                pseudonymous_identity(user_id, device_id, member_id)
            });
        if !mgr.set_encryption_signal_handler(room_id, &cfg.slot_id, bridge.clone()) {
            return Err("failed to register encryption signal handler".into());
        }
        mgr.set_encryption_identity_mapper(room_id, &cfg.slot_id, identity_mapper);
    }

    // Spawn the pump that drains received peer keys into the manager.
    let key_pump = {
        let manager = manager.clone();
        tokio::task::spawn_local(async move {
            while let Some(received) = key_rx.recv().await {
                if let Err(error) = manager
                    .lock()
                    .await
                    .receive_encryption_key(ReceivedEncryptionKey {
                        origin: received.origin,
                        room_id: received.room_id,
                        member_id: received.member_id,
                        key_b64: received.key_b64,
                        key_index: received.key_index,
                    })
                    .await
                {
                    eprintln!("failed to ingest received media key: {error}");
                }
            }
        })
    };
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
        livekit_service_url: livekit.livekit_service_url.clone(),
        room_id: room_id.to_owned(),
        slot_id: cfg.slot_id.clone(),
        member: MemberClaims {
            id: membership_id,
            claimed_user_id: user_id.clone(),
            claimed_device_id: device_id,
        },
    };
    let connection = connect_e2ee(&http, &lk_config, &client, provider).await?;
    println!("[{user}] connected to the SFU (per-participant frame E2EE enabled)");

    Ok(Participant {
        manager,
        connection,
        bridge,
        own_identity,
        _sync: sync,
        _bridge: sticky_bridge,
        key_pump,
        heartbeat,
    })
}

/// Register a to-device handler that forwards decrypted
/// `m.rtc.encryption_key` events to the key pump.
///
/// The handler is `Send` (it only moves owned key data into a channel), which
/// `add_event_handler` requires; the `!Send` work happens in the pump.
fn register_key_receiver(client: &Client, key_tx: UnboundedSender<ReceivedKey>) {
    client.add_event_handler(
        move |event: AnyToDeviceEvent, encryption_info: Option<EncryptionInfo>| {
            let key_tx = key_tx.clone();
            async move {
                if let AnyToDeviceEvent::RtcEncryptionKey(event) = event {
                    let _ = key_tx.send(ReceivedKey {
                        origin: key_origin(encryption_info.as_ref()),
                        room_id: event.content.room_id.to_string(),
                        member_id: event.content.member_id,
                        key_index: event.content.media_key.index,
                        key_b64: event.content.media_key.key,
                    });
                }
            }
        },
    );
}

/// Translate the SDK's decryption metadata into the core's [`KeyOrigin`].
///
/// `None` means the to-device message arrived unencrypted, which MSC4143 says
/// to discard — the core makes that call, this just reports it faithfully.
fn key_origin(info: Option<&EncryptionInfo>) -> KeyOrigin {
    let Some(info) = info else {
        return KeyOrigin::Cleartext;
    };

    // MSC4153 asks whether the sending device is cross-signed, not whether we
    // trust its owner: an unverified *identity* still signs its own devices.
    // States that leave the device unattributable count as not cross-signed.
    let sender_is_cross_signed = !matches!(
        info.verification_state,
        VerificationState::Unverified(
            VerificationLevel::UnsignedDevice
                | VerificationLevel::None(_)
                | VerificationLevel::MismatchedSender
        )
    );

    KeyOrigin::Encrypted {
        sender_user_id: info.sender.to_string(),
        sender_device_id: info.sender_device.as_ref().map(|d| d.to_string()),
        sender_is_cross_signed,
    }
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

    // The manager/bridge/heartbeat futures are `!Send` (the core command sender
    // is `?Send`), so the whole flow runs on a single-thread `LocalSet`.
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
    RtcSessionManager::with_command_sender(Arc::new(SdkCommandSender::new(alice.client.clone())))
        .open_slot(
            room_id.to_string(),
            cfg.slot_id.clone(),
            "m.call".to_owned(),
            Some(SlotEncryption {
                encryption_type: "m.per_member".to_owned(),
                extra: Default::default(),
            }),
        )
        .await?;
    println!("[alice] opened slot {}", cfg.slot_id);

    // 5. Now both join the RTC session (membership stickies + SFU connect).
    let room_id_str = room_id.to_string();
    let alice = join_rtc(&cfg, &room_id_str, alice, alice_room, &alice_creds.user).await?;
    let mut bob = join_rtc(&cfg, &room_id_str, bob, bob_room, &bob_creds.user).await?;

    // Signalling proof: each side discovers the other's membership
    // via stickies (own + peer == 2).
    let alice_sees = wait_for_members(&alice.manager, &room_id_str, &cfg.slot_id, 2, "alice").await;
    let bob_sees = wait_for_members(&bob.manager, &room_id_str, &cfg.slot_id, 2, "bob").await;

    // 6. Media proof: alice publishes a 440 Hz tone; bob records what the SFU
    //    forwards and checks the frequency.
    println!("[alice] publishing 440 Hz tone");
    let _tone = media::publish_tone(&alice.connection.session, 440.0).await?;

    let tone_ok = record_and_verify_tone(&mut bob).await?;

    // Encryption proof: bob must have imported alice's media key under alice's
    // MSC4195 pseudonymous identity (the JWT `sub`) — direct evidence the key
    // crossed the wire and the identity mapping is correct. With GCM frame E2EE
    // enabled, bob could not have decoded the tone above without it.
    let alice_key_seen_by_bob = bob.bridge.key_for(&alice.own_identity).is_some();
    println!("[bob] imported alice's per-participant media key: {alice_key_seen_by_bob}");

    // Tear down both peers cleanly and symmetrically: each stops its heartbeat,
    // sends its leave sticky (cancelling its delayed leave), and closes its SFU
    // connection.
    leave_and_close(alice, &room_id_str, &cfg.slot_id, "alice").await;
    leave_and_close(bob, &room_id_str, &cfg.slot_id, "bob").await;

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

/// Leave the RTC session cleanly and close the SFU connection.
///
/// Stops the heartbeat first so it can't re-arm a delayed leave after `leave`
/// cancels the current one, then sends the leave sticky and closes the SFU
/// connection. Consumes the participant (`close` takes the session by value);
/// failures are logged rather than aborting teardown of the other peer.
async fn leave_and_close(participant: Participant, room_id: &str, slot_id: &str, label: &str) {
    participant.heartbeat.abort();
    participant.key_pump.abort();
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
