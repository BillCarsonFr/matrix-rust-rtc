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

//! Minimal end-to-end connect/subscribe example.
//!
//! Logs into a Matrix homeserver, performs the MSC4195 token exchange, connects
//! to the LiveKit SFU for a MatrixRTC slot, and logs every room event (notably
//! subscribed remote tracks). To see media, join the same call from another
//! client (e.g. Element Web) so it publishes tracks the SFU forwards here.
//!
//! Run against the `demo/backend` stack (see its README), e.g.:
//!
//! ```sh
//! HOMESERVER_URL=https://synapse.m.localhost \
//! MX_USER=alice MX_PASSWORD=secret \
//! ROOM_ID='!yourroom:synapse.m.localhost' \
//! SLOT_ID='m.call#ROOM' \
//! LIVEKIT_SERVICE_URL=https://matrix-rtc.m.localhost/livekit/jwt \
//! INSECURE_TLS=1 \
//! cargo run -p matrix-rtc-livekit --example connect --features matrix-sdk
//! ```

use std::env;
use std::error::Error;

use livekit::RoomEvent;
use matrix_rtc_livekit::{LiveKitTransportConfig, MemberClaims, TokenEndpoint, connect};

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    env::var(name).map_err(|_| format!("missing required env var {name}").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let homeserver = required("HOMESERVER_URL")?;
    let user = required("MX_USER")?;
    let password = required("MX_PASSWORD")?;
    let room_id = required("ROOM_ID")?;
    let slot_id = env::var("SLOT_ID").unwrap_or_else(|_| "m.call#ROOM".to_owned());
    let livekit_service_url = required("LIVEKIT_SERVICE_URL")?;
    let insecure_tls = env::var("INSECURE_TLS").is_ok();

    // 1. Log into the homeserver.
    let mut builder = matrix_sdk::Client::builder().homeserver_url(&homeserver);
    if insecure_tls {
        builder = builder.disable_ssl_verification();
    }
    let client = builder.build().await?;
    client
        .matrix_auth()
        .login_username(&user, &password)
        .initial_device_display_name("matrix-rtc-livekit connect example")
        .send()
        .await?;
    let device_id = client
        .device_id()
        .ok_or("no device id after login")?
        .to_string();
    println!("logged in as {user} (device {device_id})");

    // 2. Build the transport config. `member.id` is the opaque membership id;
    //    a real client uses its `m.rtc.member` membership id.
    let member_id = env::var("MEMBER_ID").unwrap_or_else(|_| format!("{user}-{device_id}"));
    let config = LiveKitTransportConfig {
        livekit_service_url,
        room_id,
        slot_id,
        member: MemberClaims {
            id: member_id,
            claimed_user_id: user.clone(),
            claimed_device_id: device_id,
        },
        token_endpoint: TokenEndpoint::default(),
    };

    // 3. HTTP client for the authorisation service exchange.
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(insecure_tls)
        .build()?;

    // 4. Token exchange + SFU connect (subscribe-only). The `matrix_sdk::Client`
    //    is the `OpenIdTokenSource` (via the `matrix-sdk` feature).
    println!(
        "connecting to the SFU for room {} slot {}...",
        config.room_id, config.slot_id
    );
    let connection = connect(&http, &config, &client).await?;
    println!("connected to the SFU; waiting for tracks (join the call from another client)...");

    // 5. Log room events, highlighting subscribed remote tracks.
    let mut events = connection.events;
    while let Some(event) = events.recv().await {
        match event {
            RoomEvent::ParticipantConnected(participant) => {
                println!("👤 participant connected: {:?}", participant.identity());
            }
            RoomEvent::TrackSubscribed { participant, .. } => {
                println!("🎧 subscribed to a track from {:?}", participant.identity());
            }
            RoomEvent::Disconnected { reason } => {
                println!("disconnected from SFU: {reason:?}");
                break;
            }
            other => println!("· event: {other:?}"),
        }
    }

    connection.session.close().await?;
    Ok(())
}
