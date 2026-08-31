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

//! LiveKit SFU authorisation service token exchange ([MSC4195]) — the IO half.
//!
//! The endpoint URLs, request bodies, and response decoding live in
//! [`matrix_rtc_livekit_proto::token`], where the web binding shares them; this
//! module only POSTs what that one builds. Obtaining the OpenID token itself is
//! a Client-Server API concern and belongs to [`matrix_rtc_bridge`]: the host
//! supplies one through
//! [`OpenIdTokenSource`](matrix_rtc_bridge::OpenIdTokenSource), so nothing here
//! is wired to a particular Matrix SDK.
//!
//! [MSC4195]: https://github.com/matrix-org/matrix-spec-proposals/pull/4195

use matrix_rtc_bridge::OpenIdToken;
pub use matrix_rtc_livekit_proto::token::{MemberClaims, SfuToken};
use matrix_rtc_livekit_proto::token::{get_token_request, legacy_token_request, parse_sfu_token};

use crate::Error;

/// Exchange an OpenID token for a LiveKit SFU JWT via the authorisation
/// service's `/get_token` endpoint.
///
/// `livekit_service_url` is the `livekit_service_url` advertised by the
/// transport (e.g. `https://matrix-rtc.example.com/livekit/jwt`); the
/// `/get_token` path is appended to it.
pub async fn get_token(
    http: &reqwest::Client,
    livekit_service_url: &str,
    room_id: &str,
    slot_id: &str,
    member: &MemberClaims,
    openid_token: &OpenIdToken,
) -> Result<SfuToken, Error> {
    let (endpoint, body) =
        get_token_request(livekit_service_url, room_id, slot_id, member, openid_token);
    post_for_token(http, &endpoint, &body, &format!("{room_id}/{slot_id}")).await
}

/// Exchange an OpenID token for a LiveKit SFU JWT via the **pre-MSC4195**
/// `/sfu/get` endpoint.
///
/// For interoperating with Element Call builds older than MSC4354. There is
/// deliberately no fallback from `/get_token` to this on a 404 — see
/// [`matrix_rtc_livekit_proto::token::legacy_token_request`]. Temporary; see
/// [`crate::compat`].
pub async fn get_legacy_token(
    http: &reqwest::Client,
    livekit_service_url: &str,
    room: &str,
    device_id: &str,
    openid_token: &OpenIdToken,
) -> Result<SfuToken, Error> {
    let (endpoint, body) = legacy_token_request(livekit_service_url, room, device_id, openid_token);
    post_for_token(http, &endpoint, &body, room).await
}

/// POST a built request to a token endpoint and decode the `{jwt, url}`
/// response. `log_tag` is what prefixes the log lines; the JWT itself is never
/// logged.
async fn post_for_token(
    http: &reqwest::Client,
    endpoint: &str,
    body: &serde_json::Value,
    log_tag: &str,
) -> Result<SfuToken, Error> {
    log::debug!("[{log_tag}] requesting an SFU token from {endpoint}");

    let response = http
        .post(endpoint)
        .json(body)
        .send()
        .await
        .inspect_err(|error| log::warn!("SFU token request to {endpoint} failed: {error}"))?;

    let status = response.status().as_u16();
    let body = response.text().await?;
    let token = parse_sfu_token(status, &body).inspect_err(|error| {
        log::warn!("SFU token request to {endpoint} failed: {error}");
    })?;
    // Never the JWT itself.
    log::debug!(
        "[{log_tag}] SFU token granted for {} (jwt {} chars)",
        token.url,
        token.jwt.len(),
    );
    Ok(token)
}
