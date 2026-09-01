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
//! Everything Matrix-side — feeding the core from a `matrix_sdk::Client`, and
//! translating the pre-2026 Element Call wire dialects — belongs to
//! [`matrix_rtc_bridge`] instead, which knows nothing about LiveKit. With the
//! `matrix-sdk` feature, [`call::Call`] composes that bridge with this
//! transport into a join/leave facade — start there.
//!
//! [`matrix-rtc-core`]: matrix_rtc_core

// The pure control plane — hash derivations, token shapes, dialect choices —
// lives in `matrix-rtc-livekit-proto`, which the web binding shares; this crate
// re-exports it under the paths it always had.
pub use matrix_rtc_livekit_proto::identity;
pub use matrix_rtc_livekit_proto::{LiveKitTransportConfig, TokenEndpoint, identity_mapper};

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

// Interop with MatrixRTC implementations that predate the 2026 MSC4143 rewrite.
// Scaffolding with a delete-by date; nothing else should depend on it. Lives in
// `matrix-rtc-bridge` (it is pure Matrix wire translation, with no LiveKit in
// it), and is re-exported because `CallOptions::element_call_compat` names
// `ElementCallCompat` in this crate's own public API.
pub use matrix_rtc_bridge::compat;

pub use keys::{
    KeyDiscardListener, KeyImportListener, LocalKeyIndexHook, MediaKeyBridge, NATIVE_KEY_RING_MAX,
    ParticipantKey, SwitchCompleteListener, msc4195_key_provider, msc4195_key_provider_options,
    msc4195_media_key_bridge,
};
pub use session::{LiveKitConnection, LiveKitSession};
pub use token::{MemberClaims, SfuToken};
// The OpenID token is a Client-Server API concern, so it belongs to the bridge.
// Re-exported because `LiveKitTransportConfig` and `connect`/`connect_e2ee` name
// these in this crate's own signatures — a host implementing the token source
// should not need a second dependency to do it.
pub use matrix_rtc_bridge::{OpenIdToken, OpenIdTokenError, OpenIdTokenSource};
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
// The SDK-backed bridge itself lives in `matrix-rtc-bridge`; re-exported so a
// host driving a call keeps one dependency.
#[cfg(feature = "matrix-sdk")]
pub use matrix_rtc_bridge::{SdkCommandSender, run_sticky_bridge};

/// Obtain a fresh OpenID token and exchange it for an SFU JWT.
///
/// The one place the token dialect is chosen, so the two `connect` entry points
/// cannot drift apart.
async fn acquire_token(
    http: &reqwest::Client,
    config: &LiveKitTransportConfig,
    token_source: &dyn OpenIdTokenSource,
) -> Result<SfuToken, Error> {
    let openid_token = token_source.open_id_token().await?;
    match config.token_endpoint {
        TokenEndpoint::Msc4195 => {
            token::get_token(
                http,
                &config.livekit_service_url,
                &config.room_id,
                &config.slot_id,
                &config.member,
                &openid_token,
            )
            .await
        }
        // `room` is the room id, because that is the `livekit_alias` this
        // generation announces on a focus; the two must agree or the clients land
        // in different LiveKit rooms. See
        // `compat::element_call_state::ElementCallStateDialect::member_content`.
        TokenEndpoint::LegacyElementCall => {
            token::get_legacy_token(
                http,
                &config.livekit_service_url,
                &config.room_id,
                &config.member.claimed_device_id,
                &openid_token,
            )
            .await
        }
    }
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
    let sfu_token = acquire_token(http, config, token_source).await?;
    LiveKitSession::connect(&sfu_token).await
}

/// Like [`connect`], but enables MSC4195 per-participant GCM frame E2EE on the
/// LiveKit room using the supplied `key_provider`.
///
/// `key_provider` MUST be the same handle also given to
/// [`MediaKeyBridge::with_provider`] (a [`livekit::e2ee::key_provider::KeyProvider`]
/// is a cheap shared handle — clone it), so keys signalled by `matrix-rtc-core`
/// and imported through the bridge reach the frame cryptor of this room.
///
/// `auto_subscribe` false makes the connection publish-only: peers' tracks are
/// never subscribed, so no remote media (and no decoder) exists for them. Only
/// a load generator wants this — a real client leaves it `true`.
pub async fn connect_e2ee(
    http: &reqwest::Client,
    config: &LiveKitTransportConfig,
    token_source: &dyn OpenIdTokenSource,
    key_provider: livekit::e2ee::key_provider::KeyProvider,
    auto_subscribe: bool,
) -> Result<LiveKitConnection, Error> {
    use livekit::RoomOptions;
    use livekit::e2ee::{E2eeOptions, EncryptionType};

    let sfu_token = acquire_token(http, config, token_source).await?;

    // RoomOptions is #[non_exhaustive]; mutate a default instance.
    let mut options = RoomOptions::default();
    options.encryption = Some(E2eeOptions {
        encryption_type: EncryptionType::Gcm,
        key_provider,
    });
    // Publisher-side layer control: the SFU tells us which simulcast layers
    // are actually subscribed and unneeded ones stop being encoded.
    options.dynacast = true;
    options.auto_subscribe = auto_subscribe;
    LiveKitSession::connect_with_options(&sfu_token, options).await
}

/// Errors produced by the LiveKit transport.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A transport-level HTTP error while talking to the authorisation service.
    #[error("HTTP error talking to the LiveKit authorisation service: {0}")]
    Http(#[from] reqwest::Error),

    /// The authorisation service rejected the request or answered with a body
    /// that does not decode.
    #[error(transparent)]
    Service(#[from] matrix_rtc_livekit_proto::TokenServiceError),

    /// Obtaining the Matrix OpenID token from the host failed.
    #[error(transparent)]
    OpenIdToken(#[from] OpenIdTokenError),

    /// Connecting to or operating the LiveKit SFU room failed.
    #[error("LiveKit room error: {0}")]
    Room(#[from] livekit::RoomError),
}
