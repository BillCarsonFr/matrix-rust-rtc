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

//! MSC4195 LiveKit transport for MatrixRTC.
//!
//! This crate is the "LiveKit SDK" layer that turns the membership and key
//! outputs of [`matrix-rtc-core`] into a live SFU media session. It is
//! deliberately a **native-only** crate (the LiveKit client pulls in
//! `libwebrtc`) and is responsible for:
//!
//! - exchanging a Matrix OpenID token for a LiveKit JWT via the authorisation
//!   service ([`token`]);
//! - deriving the MSC4195 `livekit_alias` and pseudonymous participant identity
//!   ([`identity`]);
//! - connecting to the SFU with per-participant GCM frame E2EE ([`session`],
//!   [`connect_e2ee`]);
//! - bridging `matrix-rtc-core`'s media keys into LiveKit frame encryption
//!   ([`keys`]).
//!
//! With the `matrix-sdk` feature, [`matrix_bridge`] connects the core to a
//! `matrix_sdk::Client`, and [`call::Call`] wraps the whole stack in a
//! join/leave facade — start there.
//!
//! [`matrix-rtc-core`]: matrix_rtc_core

pub mod identity;
pub mod keys;
// Audio helpers. Recording a subscribed track and writing WAVs is shipped API
// (the recording-bot use case); the synthetic-tone generator and frequency
// detector inside are test utilities gated on `cfg(test)`/the `testing` feature.
pub mod media;
pub mod session;
pub mod token;
pub mod transport_impl;

#[cfg(feature = "matrix-sdk")]
pub mod call;
#[cfg(feature = "matrix-sdk")]
pub mod matrix_bridge;

pub use keys::{
    KeyImportListener, MediaKeyBridge, NATIVE_KEY_RING_MAX, ParticipantKey, msc4195_key_provider,
    msc4195_key_provider_options,
};
pub use session::{LiveKitConnection, LiveKitSession};
pub use token::{MemberClaims, OpenIdToken, OpenIdTokenSource, SfuToken};
pub use transport_impl::{LiveKitMediaTransport, LiveKitTransportConnection};

/// Android initialisation, re-exported so consumers (e.g. the FFI crate)
/// don't need a direct `livekit`/`libwebrtc` dependency for it.
#[cfg(target_os = "android")]
pub mod android {
    /// Initialise libwebrtc's JVM hooks. Must run before any peer
    /// connection is created — typically from `JNI_OnLoad`.
    pub fn initialize_android(vm: &jni::JavaVM) {
        livekit::webrtc::android::initialize_android(vm);
    }
}

#[cfg(feature = "matrix-sdk")]
pub use call::{Call, CallError, CallOptions, discover_livekit_transport, open_slot};
#[cfg(feature = "matrix-sdk")]
pub use matrix_bridge::{SdkCommandSender, run_sticky_bridge};

/// Configuration identifying the MatrixRTC slot to connect to.
#[derive(Clone, Debug)]
pub struct LiveKitTransportConfig {
    /// `livekit_service_url` advertised by the transport (the authorisation
    /// service base URL, e.g. `https://matrix-rtc.example.com/livekit/jwt`).
    pub livekit_service_url: String,
    /// Matrix room ID hosting the `m.rtc.member` event.
    pub room_id: String,
    /// MatrixRTC slot ID.
    pub slot_id: String,
    /// `member` claims identifying this membership to the authorisation service.
    pub member: MemberClaims,
}

/// Connect to the LiveKit SFU for a MatrixRTC slot.
///
/// Performs the full MSC4195 flow: obtain a Matrix OpenID token from the host
/// (`token_source`), exchange it for an SFU JWT at the authorisation service,
/// and connect to the returned SFU URL. The connection is subscribe-only.
pub async fn connect(
    http: &reqwest::Client,
    config: &LiveKitTransportConfig,
    token_source: &dyn OpenIdTokenSource,
) -> Result<LiveKitConnection, Error> {
    let openid_token = token_source.open_id_token().await?;
    let sfu_token = token::get_token(
        http,
        &config.livekit_service_url,
        &config.room_id,
        &config.slot_id,
        &config.member,
        &openid_token,
    )
    .await?;
    LiveKitSession::connect(&sfu_token).await
}

/// Like [`connect`], but enables MSC4195 per-participant GCM frame E2EE on the
/// LiveKit room using the supplied `key_provider`.
///
/// `key_provider` MUST be the same handle also given to
/// [`MediaKeyBridge::with_provider`] (a [`livekit::e2ee::key_provider::KeyProvider`]
/// is a cheap shared handle — clone it), so keys signalled by `matrix-rtc-core`
/// and imported through the bridge reach the frame cryptor of this room.
pub async fn connect_e2ee(
    http: &reqwest::Client,
    config: &LiveKitTransportConfig,
    token_source: &dyn OpenIdTokenSource,
    key_provider: livekit::e2ee::key_provider::KeyProvider,
) -> Result<LiveKitConnection, Error> {
    use livekit::RoomOptions;
    use livekit::e2ee::{E2eeOptions, EncryptionType};

    let openid_token = token_source.open_id_token().await?;
    let sfu_token = token::get_token(
        http,
        &config.livekit_service_url,
        &config.room_id,
        &config.slot_id,
        &config.member,
        &openid_token,
    )
    .await?;

    // RoomOptions is #[non_exhaustive]; mutate a default instance.
    let mut options = RoomOptions::default();
    options.encryption = Some(E2eeOptions {
        encryption_type: EncryptionType::Gcm,
        key_provider,
    });
    // Publisher-side layer control: the SFU tells us which simulcast layers
    // are actually subscribed and unneeded ones stop being encoded.
    options.dynacast = true;
    LiveKitSession::connect_with_options(&sfu_token, options).await
}

/// Errors produced by the LiveKit transport.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A transport-level HTTP error while talking to the authorisation service.
    #[error("HTTP error talking to the LiveKit authorisation service: {0}")]
    Http(#[from] reqwest::Error),

    /// The authorisation service rejected the request (non-2xx response).
    #[error("LiveKit authorisation service returned {status}: {body}")]
    Service { status: u16, body: String },

    /// Obtaining the Matrix OpenID token from the host failed.
    #[error("failed to obtain Matrix OpenID token: {0}")]
    OpenIdToken(String),

    /// Connecting to or operating the LiveKit SFU room failed.
    #[error("LiveKit room error: {0}")]
    Room(#[from] livekit::RoomError),
}
