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
//! Three scenarios share this flow: `e2e_call_two_clients_audio` (both on one
//! SFU, verified over the raw LiveKit event stream), `e2e_call_two_clients_two_foci`
//! (each participant on their own SFU — MSC4195 multi-SFU — with tones in both
//! directions verified through the transport-agnostic media API), and
//! `e2e_call_rejoin_in_the_same_process` (one peer hangs up and calls again
//! while the other stays, so the incumbent has to re-key the new participation).
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
use std::path::{Path, PathBuf};
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

use matrix_rtc_core::SlotEncryption;
use matrix_rtc_livekit::compat::ElementCallCompat;
use matrix_rtc_livekit::{Call, CallOptions, media, open_slot};
use matrix_rtc_media::{
    I420Buffer, MediaConstraints, MediaStreamKind, Participant as MediaParticipant, PublishOptions,
    VideoFrame, VideoRotation, VideoSourceConfig,
};

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
    /// Second focus (authorisation service of SFU 2), used by the two-foci
    /// scenario: bob publishes here while alice publishes on the first one.
    livekit_service_url_2: String,
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
            livekit_service_url_2: env::var("LIVEKIT_SERVICE_URL_2")
                .unwrap_or_else(|_| "http://localhost:6081".to_owned()),
            insecure_tls: env::var("INSECURE_TLS").is_ok(),
        }
    }
}

/// Which media topology a run exercises.
#[derive(Clone, Copy, PartialEq)]
enum RunMode {
    /// Both participants publish on the same focus (one SFU, one connection
    /// each). Media is verified through the raw LiveKit event stream.
    SingleFocus,
    /// Each participant publishes on their own focus (MSC4195 multi-SFU):
    /// every client keeps its own-focus connection for publishing and the
    /// engine opens a second connection to the peer's focus to subscribe.
    /// Media is verified through the transport-agnostic media API
    /// (`participants` / `remote_track` / frame streams), in both directions.
    TwoFoci,
    /// Single focus, but bob hangs up and calls again while alice stays: the
    /// tone must be audible on bob's *second* call.
    ///
    /// What this exercises is the **incumbent** side of a redial. Alice never
    /// leaves, so her `RtcSessionManager` is the long-lived one and has to carry
    /// the whole transition: retire the key bob's first participation held, then
    /// hand his *new* participation — a fresh `member_id`, holding nothing, on a
    /// brand-new frame cryptor — a key it can decrypt the very next frame with.
    /// The unit tests cover the key bookkeeping; only a real SFU shows whether
    /// media actually resumes.
    ///
    /// It deliberately does **not** claim to cover the joiner half.
    /// [`Call::join`] builds a fresh `RtcSessionManager` per call, so bob starts
    /// from pristine state here and cannot reproduce a bug that needs a manager
    /// carried across calls. That path belongs to hosts holding one
    /// `RtcSessionManagerHandle` for the whole Matrix session, and is covered
    /// in-process by `a_rejoin_in_the_same_process_distributes_a_key_to_the_incumbent`
    /// (matrix-rtc-core) and `a_rejoin_distributes_keys_without_new_sticky_events`
    /// (matrix-rtc-ffi).
    RejoinSameProcess,
    /// Single focus, but both participants speak the **pre-sticky Element Call**
    /// dialect: membership as `org.matrix.msc3401.call.member` room state, plain
    /// `{user}:{device}` SFU identities, and the `/sfu/get` token endpoint.
    ///
    /// The highest-value check of that path available without an actual Element
    /// Call, because in this mode both halves of the protocol are ours: it
    /// exercises the state-event write, the delayed *state* event and its
    /// cancellation, the `{}` leave, the unhashed identity end to end through
    /// frame decryption, and the legacy key exchange.
    ///
    /// Note no slot is opened: that generation has no slot concept, so the bridge
    /// leaves the condition unenforced rather than reporting an empty (all-closed)
    /// room. See `matrix_rtc_livekit::compat`.
    LegacyStateEvents,
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
/// into the base room's sticky store (see `matrix_rtc_bridge::sdk`).
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
    // `org.matrix.msc3401.call.member` is a *state* event, so `state_default`
    // (50) gates it and only the room creator could publish a membership — the
    // invitee, at PL 0, would fail their join with `M_FORBIDDEN`. Real Element
    // Call rooms ship exactly this override, which is why nobody hits it there.
    // Harmless for the sticky modes, which never send the type at all.
    //
    // Raw JSON rather than the typed content: this replaces the homeserver's
    // whole default `events` map, which is fine for a throwaway test room (alice
    // is the PL 100 creator and nobody demotes anyone), and it keeps the test off
    // a ruma struct whose shape is not the point of this test.
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
    livekit_service_url: &str,
    compat: ElementCallCompat,
) -> Result<Participant, Box<dyn Error>> {
    let SyncedClient { client: _, sync } = synced;
    let call = open_call(cfg, &room, user, livekit_service_url, compat).await?;
    Ok(Participant { call, _sync: sync })
}

/// The `Call::join` half of [`join_call`], reusable on its own so a participant
/// can redial on a client that is already logged in and syncing.
async fn open_call(
    cfg: &Config,
    room: &matrix_sdk::Room,
    user: &str,
    livekit_service_url: &str,
    compat: ElementCallCompat,
) -> Result<Call, Box<dyn Error>> {
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(cfg.insecure_tls)
        .build()?;
    let call = Call::join(
        room,
        CallOptions {
            slot_id: cfg.slot_id.clone(),
            livekit_service_url_fallback: Some(livekit_service_url.to_owned()),
            http: Some(http),
            element_call_compat: compat,
            ..CallOptions::default()
        },
    )
    .await?;
    println!(
        "[{user}] joined RTC session (membership {}) and connected to the SFU \
         (per-participant frame E2EE enabled)",
        call.membership_id()
    );

    Ok(call)
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

/// Poll until a peer's media key has been imported, or time out.
///
/// Distinct from [`wait_for_members`]: that one proves the *signalling* round
/// trip, this one proves the *key* round trip, which is a separate hop
/// (to-device, via the homeserver) and completes later. Anything that reads
/// media before this holds is measuring key latency rather than media.
async fn wait_for_key(call: &Call, peer_identity: &str, label: &str) -> bool {
    for _ in 0..60 {
        if call.imported_key_for(peer_identity) {
            println!("[{label}] imported the peer's media key");
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("[{label}] WARNING: no media key for {peer_identity} within 30s");
    false
}

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up)"]
fn e2e_call_two_clients_audio() {
    harness(RunMode::SingleFocus);
}

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up)"]
fn e2e_call_two_clients_two_foci() {
    harness(RunMode::TwoFoci);
}

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up)"]
fn e2e_call_rejoin_in_the_same_process() {
    harness(RunMode::RejoinSameProcess);
}

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up)"]
fn e2e_call_two_clients_pre_sticky_element_call() {
    harness(RunMode::LegacyStateEvents);
}

/// Process-wide one-time setup, safe to call from every test in this binary.
fn init_test_process() {
    // The dependency tree enables both rustls crypto backends (`ring` via
    // livekit/reqwest, `aws-lc-rs` via matrix-sdk), so rustls can't auto-select a
    // process-level `CryptoProvider` and panics on first TLS handshake. Install
    // one explicitly before any TLS happens (idempotent across tests).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
}

fn harness(mode: RunMode) {
    init_test_process();
    let cfg = Config::from_env();

    // The futures behind `Call::join` are `!Send` (the core command sender is
    // `?Send`), so the whole flow runs on a single-thread `LocalSet`.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    let outcome = runtime.block_on(
        tokio::task::LocalSet::new()
            .run_until(async { tokio::time::timeout(OVERALL_DEADLINE, run(cfg, mode)).await }),
    );

    match outcome {
        Err(_) => panic!("e2e call did not finish within {OVERALL_DEADLINE:?}"),
        Ok(Err(error)) => panic!("e2e call failed: {error}"),
        Ok(Ok(())) => {}
    }
}

async fn run(cfg: Config, mode: RunMode) -> Result<(), Box<dyn Error>> {
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

    let compat = match mode {
        RunMode::LegacyStateEvents => ElementCallCompat::StateEvents,
        _ => ElementCallCompat::Off,
    };

    // 4. Alice (the room creator, so the only one with the power level for it)
    //    opens the slot. Without an open `m.rtc.slot` in room state, MSC4143
    //    says no member of it counts as joined.
    //
    //    Skipped in the pre-sticky dialect, which has no slot concept at all:
    //    opening one there would be a claim about a room that generation of
    //    Element Call never makes, and the bridge leaves the slot condition
    //    unenforced in that mode anyway.
    if compat != ElementCallCompat::StateEvents {
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
    }

    // 5. Now both join the call (membership stickies + key exchange + SFU
    //    connect, all through the facade). In two-foci mode each participant
    //    publishes on their own SFU; the engines then cross-connect to the
    //    peer's focus for subscribing (MSC4195 multi-SFU).
    let bob_url = match mode {
        RunMode::SingleFocus | RunMode::RejoinSameProcess | RunMode::LegacyStateEvents => {
            cfg.livekit_service_url.clone()
        }
        RunMode::TwoFoci => cfg.livekit_service_url_2.clone(),
    };
    let alice_url = cfg.livekit_service_url.clone();
    // Kept so bob can redial on the same room handle after hanging up.
    let bob_room_again = bob_room.clone();
    let alice = join_call(
        &cfg,
        alice,
        alice_room,
        &alice_creds.user,
        &alice_url,
        compat,
    )
    .await?;
    let mut bob = join_call(&cfg, bob, bob_room, &bob_creds.user, &bob_url, compat).await?;

    // Signalling proof: each side discovers the other's membership
    // via stickies (own + peer == 2).
    let alice_sees = wait_for_members(&alice.call, 2, "alice").await;
    let bob_sees = wait_for_members(&bob.call, 2, "bob").await;

    // 6. Media proof. Single focus: alice publishes a 440 Hz tone and bob
    //    verifies it through the raw LiveKit event stream (the historical
    //    path). Two foci: tones flow in BOTH directions and are verified
    //    through the transport-agnostic media API — bob receiving alice's
    //    tone proves bob's engine connected to alice's focus, and vice versa.
    println!("[alice] publishing 440 Hz tone");
    let _alice_tone = media::publish_tone(alice.call.session(), 440.0).await?;

    // Bound to the outer scope so the capture tasks are not aborted mid-call:
    // publishers stay alive through teardown, exactly like the single-focus
    // scenario's tone.
    let mut _bob_tone = None;
    let mut _alice_video = None;
    let (tone_ok, reverse_tone_ok, video_ok, constraints_ok) = match mode {
        RunMode::SingleFocus | RunMode::RejoinSameProcess | RunMode::LegacyStateEvents => (
            record_and_verify_tone(&mut bob.call, "first-call").await?,
            true,
            true,
            true,
        ),
        RunMode::TwoFoci => {
            println!("[bob] publishing 660 Hz tone");
            _bob_tone = Some(media::publish_tone(bob.call.session(), 660.0).await?);
            let bob_hears = record_peer_tone(&bob.call, "bob", 440.0).await?;
            let alice_hears = record_peer_tone(&alice.call, "alice", 660.0).await?;

            // Video: alice publishes a synthetic half-bright/half-dark
            // pattern through the transport-agnostic publish path; bob
            // verifies the luma split across the two SFUs, then exercises
            // the constraints path by pausing and resuming the stream.
            println!("[alice] publishing pattern video");
            _alice_video = Some(publish_pattern_video(&alice.call).await?);
            let video_ok = verify_video_pattern(&bob.call, "bob").await?;
            let constraints_ok = if video_ok {
                verify_constraints_toggle(&bob.call, "bob").await?
            } else {
                false
            };

            (bob_hears, alice_hears, video_ok, constraints_ok)
        }
    };

    // Encryption proof: bob must have imported alice's media key under alice's
    // MSC4195 pseudonymous identity (the JWT `sub`) — direct evidence the key
    // crossed the wire and the identity mapping is correct. With GCM frame E2EE
    // enabled, bob could not have decoded the tone above without it.
    let alice_key_seen_by_bob = bob.call.imported_key_for(alice.call.local_identity());
    println!("[bob] imported alice's per-participant media key: {alice_key_seen_by_bob}");

    // Hang up and call again, with alice staying put and still publishing. Her
    // core is the long-lived one across this transition: it retires the key bob's
    // first participation held and must hand his new one a key it can decrypt the
    // next frame with, on a frame cryptor that starts empty.
    let (rejoin_tone_ok, rejoin_key_ok) = if mode == RunMode::RejoinSameProcess {
        let Participant {
            call: first_call,
            _sync,
        } = bob;
        println!("[bob] leaving, then redialling in the same process");
        first_call.leave().await?;

        let mut second = Participant {
            call: open_call(&cfg, &bob_room_again, &bob_creds.user, &bob_url, compat).await?,
            _sync,
        };
        if !wait_for_members(&second.call, 2, "bob-redial").await {
            println!("[bob] WARNING: the second call never saw alice's membership");
        }

        // Wait for alice's key before recording, which the first call never had
        // to do. There, both peers exchanged keys while the call settled and
        // alice only published afterwards. Here she is *already* publishing, so
        // the redial subscribes to her track within milliseconds of connecting —
        // while her key for this new membership is still making its way through
        // a to-device round trip. Recording on subscription samples that gap and
        // sees silence, which says nothing about whether the redial works.
        //
        // The wait is the test being fair, not the test being lenient: a peer
        // that never gets a key fails it just the same, on the deadline.
        let key_ok = wait_for_key(&second.call, alice.call.local_identity(), "bob-redial").await;
        let tone_ok = record_and_verify_tone(&mut second.call, "redial").await?;
        bob = second;
        (tone_ok, key_ok)
    } else {
        (true, true)
    };

    // Tear down both peers cleanly and symmetrically: `Call::leave` stops the
    // heartbeat, sends the leave event (cancelling the delayed leave), shuts
    // the media engine down (closing peer-focus connections), and closes the
    // own SFU connection. A per-leave timeout keeps a wedged teardown from
    // eating the overall deadline; failures are logged rather than aborting
    // teardown of the other peer, but they fail the test. `Call::leave` logs
    // each step at debug level (RUST_LOG=matrix_rtc_livekit=debug) so a
    // timeout here pinpoints which await wedged.
    let mut teardown_ok = true;
    for (participant, label) in [(alice, "alice"), (bob, "bob")] {
        match tokio::time::timeout(Duration::from_secs(30), participant.call.leave()).await {
            Ok(Ok(())) => println!("[{label}] left cleanly"),
            Ok(Err(error)) => {
                teardown_ok = false;
                eprintln!("[{label}] leave failed: {error}");
            }
            Err(_) => {
                teardown_ok = false;
                eprintln!("[{label}] WARNING: leave timed out after 30s (teardown wedged)");
            }
        }
    }

    println!("\n=== RESULT ===");
    println!("sticky membership discovered by alice: {alice_sees}");
    println!("sticky membership discovered by bob:   {bob_sees}");
    println!("alice's media key received by bob:      {alice_key_seen_by_bob}");
    println!("tone alice->bob received + verified:    {tone_ok}");
    if mode == RunMode::TwoFoci {
        println!("tone bob->alice received + verified:    {reverse_tone_ok}");
        println!("video pattern alice->bob verified:      {video_ok}");
        println!("constraints pause/resume verified:      {constraints_ok}");
    }
    if mode == RunMode::RejoinSameProcess {
        println!("alice's key received on bob's redial:   {rejoin_key_ok}");
        println!("tone alice->bob on bob's redial:        {rejoin_tone_ok}");
    }
    println!("clean teardown (leave both sides):      {teardown_ok}");

    if alice_sees
        && bob_sees
        && alice_key_seen_by_bob
        && tone_ok
        && reverse_tone_ok
        && video_ok
        && constraints_ok
        && rejoin_tone_ok
        && rejoin_key_ok
        && teardown_ok
    {
        println!("END-TO-END TEST PASSED (with per-participant frame E2EE)");
        Ok(())
    } else {
        Err("end-to-end test failed (see WARNING lines above)".into())
    }
}

/// Poll until the roster shows a remote participant, or `deadline`.
async fn wait_for_peer(call: &Call, deadline: tokio::time::Instant) -> Option<MediaParticipant> {
    loop {
        if let Some(peer) = call.participants().into_iter().find(|p| !p.is_local) {
            return Some(peer);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Poll until the peer's stream of `kind` is subscribed, or `deadline`.
async fn wait_for_remote_track(
    call: &Call,
    member_id: &str,
    kind: MediaStreamKind,
    deadline: tokio::time::Instant,
) -> Option<std::sync::Arc<dyn matrix_rtc_media::RemoteTrackHandle>> {
    loop {
        if let Some(track) = call.remote_track(member_id, kind) {
            return Some(track);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Abort-on-drop guard for the synthetic video capture task.
struct VideoPublisher {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for VideoPublisher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// 640x360 so livekit publishes a regular multi-layer simulcast track; below
// 480px the SDK emits a single-encoding simulcast (rid "q" only), a shape
// worth keeping out of a correctness test.
const PATTERN_WIDTH: u32 = 640;
const PATTERN_HEIGHT: u32 = 360;

/// An I420 frame whose left half is bright (Y=235) and right half dark
/// (Y=16) — a split that survives VP8 compression comfortably.
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

/// Publish the pattern as a camera track at ~15 fps through the
/// transport-agnostic publish path.
async fn publish_pattern_video(call: &Call) -> Result<VideoPublisher, Box<dyn Error>> {
    let track = call
        .publish(PublishOptions::camera(VideoSourceConfig {
            width: PATTERN_WIDTH,
            height: PATTERN_HEIGHT,
        }))
        .await?;
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(66));
        let mut captured = 0u64;
        loop {
            ticker.tick().await;
            if let Err(error) = track.capture_video(pattern_frame()) {
                eprintln!("[alice] video capture failed after {captured} frames: {error}");
                break;
            }
            captured += 1;
            // ~every 5s at 15fps, so a silent capture-side death is visible.
            if captured.is_multiple_of(75) {
                println!("[alice] captured {captured} video frames");
            }
        }
    });
    Ok(VideoPublisher { task })
}

/// Mean luma of the left and right halves of a frame, honouring the stride.
fn halves_mean_luma(buffer: &I420Buffer) -> (f64, f64) {
    let width = buffer.width as usize;
    let stride = buffer.stride_y as usize;
    let half = width / 2;
    let (mut left, mut right, mut count) = (0u64, 0u64, 0u64);
    for row in 0..buffer.height as usize {
        let line = &buffer.data_y[row * stride..][..width];
        left += line[..half].iter().map(|&y| u64::from(y)).sum::<u64>();
        right += line[half..].iter().map(|&y| u64::from(y)).sum::<u64>();
        count += half as u64;
    }
    (left as f64 / count as f64, right as f64 / count as f64)
}

/// Receive the peer's camera stream through the media API and verify the
/// half-bright/half-dark pattern.
async fn verify_video_pattern(call: &Call, label: &str) -> Result<bool, Box<dyn Error>> {
    use futures_util::StreamExt;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let Some(peer) = wait_for_peer(call, deadline).await else {
        println!("[{label}] WARNING: no remote participant on the roster");
        return Ok(false);
    };
    let Some(track) =
        wait_for_remote_track(call, &peer.member_id, MediaStreamKind::Camera, deadline).await
    else {
        println!(
            "[{label}] WARNING: no camera stream from {} within 60s",
            peer.member_id
        );
        return Ok(false);
    };
    println!("[{label}] receiving camera stream of {}", peer.member_id);

    let mut frames = track
        .video_frames()
        .ok_or("camera track has no video frame stream")?;
    let mut seen = 0usize;
    loop {
        let frame = match tokio::time::timeout_at(deadline, frames.next()).await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                println!("[{label}] WARNING: video frame stream ended");
                return Ok(false);
            }
            Err(_) => {
                println!("[{label}] WARNING: no video frame within the deadline");
                return Ok(false);
            }
        };
        seen += 1;
        // Skip the first few frames: early keyframes can still be settling.
        if seen < 5 {
            continue;
        }
        let (left, right) = halves_mean_luma(&frame.buffer);
        println!(
            "[{label}] video frame {}x{}: left/right luma {left:.0}/{right:.0}",
            frame.buffer.width, frame.buffer.height,
        );
        return Ok(left > 140.0 && right < 90.0);
    }
}

/// Exercise both constraint demand states on the peer's camera stream:
///
/// 1. `visible = false` → **pause** (subscription kept): frames stop, then
///    resume instantly on `visible = true` — the scroll case.
/// 2. `enabled = false` → **off**: the stream is released as fully as the
///    transport supports (LiveKit currently pauses too — its client-side
///    resubscribe is unreliable at 0.7.48), then `enabled = true` brings
///    frames back — the closed-tile case. The re-fetch loop below stays
///    valid for transports whose `Off` really unsubscribes (new track).
async fn verify_constraints_toggle(call: &Call, label: &str) -> Result<bool, Box<dyn Error>> {
    use futures_util::StreamExt;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let Some(peer) = wait_for_peer(call, deadline).await else {
        return Ok(false);
    };
    let Some(track) =
        wait_for_remote_track(call, &peer.member_id, MediaStreamKind::Camera, deadline).await
    else {
        return Ok(false);
    };
    let mut frames = track
        .video_frames()
        .ok_or("camera track has no video frame stream")?;

    // --- Pause / resume (visible) -----------------------------------------
    call.set_constraints(
        &peer.member_id,
        MediaStreamKind::Camera,
        MediaConstraints {
            visible: false,
            ..Default::default()
        },
    );

    // Frames may still be in flight; paused = a 3s window with none.
    let paused = loop {
        match tokio::time::timeout(Duration::from_secs(3), frames.next()).await {
            Ok(Some(_)) => {
                if tokio::time::Instant::now() >= deadline {
                    break false;
                }
            }
            Ok(None) => break false,
            Err(_) => break true,
        }
    };
    println!("[{label}] video paused while visible=false: {paused}");

    call.set_constraints(
        &peer.member_id,
        MediaStreamKind::Camera,
        MediaConstraints::default(),
    );
    let resumed = matches!(
        tokio::time::timeout(Duration::from_secs(15), frames.next()).await,
        Ok(Some(_))
    );
    println!("[{label}] video resumed after visible=true: {resumed}");

    // --- Unsubscribe / resubscribe (enabled) -------------------------------
    call.set_constraints(
        &peer.member_id,
        MediaStreamKind::Camera,
        MediaConstraints {
            enabled: false,
            ..Default::default()
        },
    );

    // The unsubscribe drops the track: the frame stream ends (or at least
    // goes silent while teardown propagates).
    let stopped = loop {
        match tokio::time::timeout(Duration::from_secs(5), frames.next()).await {
            Ok(Some(_)) => {
                if tokio::time::Instant::now() >= deadline {
                    break false;
                }
            }
            Ok(None) => break true, // stream ended: track dropped
            Err(_) => break true,   // silent long enough
        }
    };
    println!("[{label}] video stopped after enabled=false: {stopped}");

    // Re-enabling renegotiates the subscription; a NEW track appears (the
    // engine re-announces the stream), so re-fetch the handle.
    call.set_constraints(
        &peer.member_id,
        MediaStreamKind::Camera,
        MediaConstraints::default(),
    );
    // Wait out the old handle: the tracks map entry is replaced on
    // resubscription. Poll for frames on a fresh stream.
    let restart_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut restarted = false;
    while tokio::time::Instant::now() < restart_deadline {
        if let Some(track) = call.remote_track(&peer.member_id, MediaStreamKind::Camera)
            && let Some(mut fresh_frames) = track.video_frames()
            && matches!(
                tokio::time::timeout(Duration::from_secs(5), fresh_frames.next()).await,
                Ok(Some(_))
            )
        {
            restarted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("[{label}] video restarted after enabled=true: {restarted}");

    Ok(paused && resumed && stopped && restarted)
}

/// Receive a peer's tone through the transport-agnostic media API: find the
/// remote participant on the roster, wait for their microphone stream (in
/// two-foci mode this only appears once the engine has connected to *their*
/// focus), pull ~2s of PCM off the frame stream, and verify the frequency.
async fn record_peer_tone(call: &Call, label: &str, freq: f64) -> Result<bool, Box<dyn Error>> {
    use futures_util::StreamExt;
    use matrix_rtc_media::MediaStreamKind;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    let peer = loop {
        if let Some(peer) = call.participants().into_iter().find(|p| !p.is_local) {
            break peer;
        }
        if tokio::time::Instant::now() >= deadline {
            println!("[{label}] WARNING: no remote participant on the roster within 60s");
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    let track = loop {
        if let Some(track) = call.remote_track(&peer.member_id, MediaStreamKind::Microphone) {
            break track;
        }
        if tokio::time::Instant::now() >= deadline {
            println!(
                "[{label}] WARNING: no microphone stream from {} within 60s",
                peer.member_id
            );
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    println!(
        "[{label}] receiving microphone stream of {} ({})",
        peer.member_id, peer.user_id
    );

    let mut frames = track
        .audio_frames()
        .ok_or("microphone track has no audio frame stream")?;
    let target_samples = media::SAMPLE_RATE as usize * 2;
    let mut pcm: Vec<i16> = Vec::with_capacity(target_samples);
    while pcm.len() < target_samples {
        match tokio::time::timeout_at(deadline, frames.next()).await {
            Ok(Some(frame)) => pcm.extend_from_slice(&frame.data),
            Ok(None) => {
                println!("[{label}] frame stream ended after {} samples", pcm.len());
                break;
            }
            Err(_) => {
                println!(
                    "[{label}] WARNING: frame stream stalled at {} samples",
                    pcm.len()
                );
                break;
            }
        }
    }

    let energy = media::detect_tone(&pcm, media::SAMPLE_RATE, freq);
    println!(
        "[{label}] {freq} Hz energy ratio: {energy:.3} over {} samples",
        pcm.len()
    );
    Ok(energy > 0.5)
}

/// Consume bob's SFU events until a remote audio track is subscribed, record a
/// couple of seconds, write a WAV, and verify the 440 Hz tone.
/// Where this test writes recordings: `target/e2e/`, inside the repository.
///
/// Not the system temp directory — on macOS that is a per-user
/// `/var/folders/…` path nobody can guess from the outside, so the evidence a
/// failed run leaves behind is effectively hidden. `target/` is already
/// git-ignored and `cargo clean` disposes of it.
///
/// Derived from `CARGO_TARGET_TMPDIR` (`<target-dir>/tmp`) because that is the
/// only pointer cargo gives an integration test to the target directory, which
/// `CARGO_TARGET_DIR` may have relocated.
fn artifact_dir() -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .parent()
        .unwrap_or_else(|| Path::new("target"))
        .join("e2e");
    if let Err(error) = std::fs::create_dir_all(&dir) {
        eprintln!("could not create {}: {error}", dir.display());
    }
    dir
}

/// `label` names the recording, so a scenario that records more than once (the
/// redial) keeps both files instead of overwriting the first — the two are
/// different evidence when only the second one fails.
async fn record_and_verify_tone(call: &mut Call, label: &str) -> Result<bool, Box<dyn Error>> {
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
                    // CI uploads target/e2e/ on failure, so keep the name stable.
                    let wav_path = artifact_dir().join(format!("received-{label}.wav"));
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
