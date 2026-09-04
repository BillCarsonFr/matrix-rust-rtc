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

//! Matrix-side bridging for MatrixRTC.
//!
//! [`matrix-rtc-core`] owns *what the protocol says*; a transport crate (e.g.
//! `matrix-rtc-livekit`) owns *how media flows*. This crate owns the third
//! thing: *how the protocol reaches a Matrix homeserver*. It is deliberately
//! transport-free — nothing here knows what a LiveKit SFU is — so a second
//! transport can reuse it unchanged.
//!
//! Three pieces, in increasing order of how much they depend on:
//!
//! - [`compat`] — translation between the current MSC4143 wire format and the
//!   pre-2026 dialects Element Call still speaks. Pure JSON in, pure JSON out;
//!   no Matrix SDK, no async runtime. Available unconditionally.
//! - [`OpenIdTokenSource`] — the host's route to a Matrix OpenID token, which a
//!   transport exchanges for its own credentials. The trait is always available
//!   so a transport can name it; the `matrix_sdk::Client` implementation sits
//!   behind the `matrix-sdk` feature.
//! - [`sdk`] — the SDK-backed bridge proper: [`SdkCommandSender`] carries the
//!   core's outbound commands (memberships, delayed leaves, to-device keys) to a
//!   `matrix_sdk::Client`, and [`run_membership_bridge`] feeds inbound
//!   membership — sticky events and/or room state — back into the core. Behind
//!   the `matrix-sdk` feature.
//!
//! # Why `matrix-sdk` is off by default
//!
//! [`compat`] is the largest and most-tested part of this crate and needs
//! nothing but `serde_json`. Keeping the SDK optional means its tests build in
//! seconds against no git dependencies — which is the whole reason this crate
//! was split out of the LiveKit transport, where every one of them was trapped
//! behind a `libwebrtc` build.
//!
//! # Why sticky events are a second feature
//!
//! `matrix-sdk` alone builds against upstream matrix-rust-sdk, which has no
//! MSC4354 support, so that build carries membership as room state only
//! ([`ElementCallCompat::StateEvents`]). `experimental-sticky` adds the
//! spec-current sticky carrier and needs the fork SDK that implements it; see
//! [`STICKY_EVENTS_SUPPORTED`] and the crate manifest.
//!
//! [`matrix-rtc-core`]: matrix_rtc_core

use async_trait::async_trait;
use matrix_rtc_core::MaybeSend;
use serde::{Deserialize, Serialize};

pub mod compat;

#[cfg(feature = "matrix-sdk")]
pub mod sdk;

#[cfg(feature = "matrix-sdk")]
pub use sdk::{
    STICKY_EVENTS_SUPPORTED, SdkCommandSender, TimelineIngest, register_timeline_receiver,
    run_membership_bridge, run_timeline_bridge, timeline_ingest_from_raw,
};

pub use compat::{
    ElementCallCompat, ElementCallDialect, ElementCallStateDialect, LEGACY_KEY_EVENT_TYPE,
    LegacyKeyMessage, MemberContent, MemberEventRoute, OutboundDialect, STATE_MEMBER_EVENT_TYPE,
    StateMemberEvent, StateMembership,
};

/// A Matrix OpenID token, as returned by the Client-Server API
/// `POST /_matrix/client/v3/user/{userId}/openid/request_token` endpoint.
///
/// `Serialize` because a transport's authorisation service is expected to
/// receive the whole object verbatim and validate it against the homeserver
/// itself; this crate never inspects the fields.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenIdToken {
    pub access_token: String,
    pub token_type: String,
    pub matrix_server_name: String,
    pub expires_in: u64,
}

/// Obtaining an OpenID token from the host failed.
///
/// Deliberately opaque: the failure is whatever the host's Matrix client said,
/// and no caller can act on the distinction. A transport wraps this in its own
/// error type rather than the reverse, which is what lets the trait live here
/// instead of in the transport crate.
#[derive(Debug, thiserror::Error)]
#[error("failed to obtain Matrix OpenID token: {0}")]
pub struct OpenIdTokenError(pub String);

/// Source of a Matrix OpenID token.
///
/// The host application implements this (typically backed by its Matrix client)
/// so a transport can acquire a fresh OpenID token when it needs to fetch its
/// own credentials — for LiveKit, the MSC4195 SFU JWT. A default
/// `matrix_sdk::Client` implementation is provided behind the `matrix-sdk`
/// feature.
///
/// [`MaybeSend`] rather than `Send + Sync`: on the web the source is the host's
/// matrix-js-sdk client, reached through a `JsValue` delegate.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait OpenIdTokenSource: MaybeSend {
    /// Request a fresh OpenID token for the current account.
    async fn open_id_token(&self) -> Result<OpenIdToken, OpenIdTokenError>;
}

/// Default [`OpenIdTokenSource`] backed by a `matrix_sdk::Client`.
///
/// Requests a fresh OpenID token for the logged-in account via the
/// Client-Server API and maps it into [`OpenIdToken`].
#[cfg(feature = "matrix-sdk")]
#[async_trait]
impl OpenIdTokenSource for matrix_sdk::Client {
    async fn open_id_token(&self) -> Result<OpenIdToken, OpenIdTokenError> {
        let response = self
            .account()
            .request_openid_token()
            .await
            .map_err(|error| OpenIdTokenError(error.to_string()))?;
        Ok(OpenIdToken {
            access_token: response.access_token,
            token_type: response.token_type.to_string(),
            matrix_server_name: response.matrix_server_name.to_string(),
            expires_in: response.expires_in.as_secs(),
        })
    }
}
