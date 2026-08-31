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

//! LiveKit SFU authorisation service token exchange shapes ([MSC4195]).
//!
//! A MatrixRTC client obtains a LiveKit JWT by `POST`ing a Matrix OpenID token
//! (plus the room/slot/member identifiers) to the authorisation service's
//! `/get_token` endpoint; the service validates the OpenID token against the
//! homeserver and returns a `{ jwt, url }` pair used to connect to the SFU.
//!
//! This module owns the exchange's *shapes* — endpoint URL, request body,
//! response decoding — and nothing sends them: the native transport POSTs the
//! built request with reqwest, the web binding hands it to the browser's
//! `fetch`. Both feed the raw response back through [`parse_sfu_token`], so
//! the wire format has exactly one owner. Obtaining the OpenID token itself is
//! a Client-Server API concern and belongs to [`matrix_rtc_bridge`]: the host
//! supplies one through
//! [`OpenIdTokenSource`](matrix_rtc_bridge::OpenIdTokenSource).
//!
//! [MSC4195]: https://github.com/matrix-org/matrix-spec-proposals/pull/4195

use serde::{Deserialize, Serialize};

use matrix_rtc_bridge::OpenIdToken;

/// Contents of the `member` field of the `m.rtc.member` event, identifying the
/// joining membership to the authorisation service.
///
/// Stays here rather than in the bridge: these are the `member` claims of the
/// MSC4195 `/get_token` request body, which no homeserver ever sees.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberClaims {
    pub id: String,
    pub claimed_user_id: String,
    pub claimed_device_id: String,
}

/// JSON body of a `POST /get_token` request.
#[derive(Serialize)]
struct GetTokenRequest<'a> {
    room_id: &'a str,
    slot_id: &'a str,
    openid_token: &'a OpenIdToken,
    member: &'a MemberClaims,
}

/// Successful `POST /get_token` response: the JWT and the SFU URL to use.
#[derive(Clone, Debug, Deserialize)]
pub struct SfuToken {
    /// JWT to authenticate with the LiveKit SFU.
    pub jwt: String,
    /// WebSocket URL of the LiveKit SFU to connect to for this slot.
    pub url: String,
}

/// The authorisation service's answer could not be turned into an [`SfuToken`].
///
/// Transport-level failures (DNS, TLS, aborted fetch) are not here — they
/// belong to whoever performed the IO and never reach [`parse_sfu_token`].
#[derive(Debug, thiserror::Error)]
pub enum TokenServiceError {
    /// The service rejected the request (non-2xx response).
    #[error("LiveKit authorisation service returned {status}: {body}")]
    Rejected { status: u16, body: String },
    /// A 2xx response whose body is not the `{jwt, url}` shape.
    #[error("LiveKit authorisation service response did not decode: {0}")]
    Decode(#[from] serde_json::Error),
}

/// Build the MSC4195 `POST /get_token` request: the endpoint URL and the JSON
/// body to send to it.
///
/// `livekit_service_url` is the `livekit_service_url` advertised by the
/// transport (e.g. `https://matrix-rtc.example.com/livekit/jwt`); the
/// `/get_token` path is appended to it.
pub fn get_token_request(
    livekit_service_url: &str,
    room_id: &str,
    slot_id: &str,
    member: &MemberClaims,
    openid_token: &OpenIdToken,
) -> (String, serde_json::Value) {
    let endpoint = format!("{}/get_token", livekit_service_url.trim_end_matches('/'));
    let body = serde_json::to_value(GetTokenRequest {
        room_id,
        slot_id,
        openid_token,
        member,
    })
    .expect("token request bodies are plain structs of strings");
    (endpoint, body)
}

/// JSON body of a pre-MSC4195 `POST /sfu/get` request.
#[derive(Serialize)]
struct LegacyGetTokenRequest<'a> {
    /// The LiveKit room name, which the legacy service derives from this string
    /// alone.
    ///
    /// It MUST equal the `livekit_alias` we announce in `foci_preferred` —
    /// Element Call uses the Matrix room id for both — or the two clients end up
    /// in different LiveKit rooms and never see each other, with a perfectly
    /// healthy connection on both sides.
    room: &'a str,
    openid_token: &'a OpenIdToken,
    /// The participant identity this service mints is
    /// `{openid subject}:{device_id}`, which is why that generation's identities
    /// are unhashed and why we cannot choose the shape.
    device_id: &'a str,
}

/// Build the **pre-MSC4195** `POST /sfu/get` request: the endpoint URL and the
/// JSON body to send to it.
///
/// For interoperating with Element Call builds older than MSC4354. Note there is
/// deliberately **no** fallback from `/get_token` to this on a 404: the endpoint
/// is not an independent choice, it comes bundled with the identity derivation
/// and the membership carrier, both decided before any HTTP happens — so a 404
/// cannot retroactively change what we already published. Worse, succeeding here
/// with the wrong identity derivation produces a fully connected SFU session in
/// which nothing decrypts and nobody appears, and peer foci retry connects
/// indefinitely, so a wrong guess would never surface. A 404 stays loud.
///
/// Temporary; see the bridge's `compat`.
pub fn legacy_token_request(
    livekit_service_url: &str,
    room: &str,
    device_id: &str,
    openid_token: &OpenIdToken,
) -> (String, serde_json::Value) {
    let endpoint = format!("{}/sfu/get", livekit_service_url.trim_end_matches('/'));
    let body = serde_json::to_value(LegacyGetTokenRequest {
        room,
        openid_token,
        device_id,
    })
    .expect("token request bodies are plain structs of strings");
    (endpoint, body)
}

/// Decode a token endpoint's raw response into an [`SfuToken`].
///
/// The two dialects differ only in the path and the request body — the response
/// is identical — so one decoder serves both.
pub fn parse_sfu_token(status: u16, body: &str) -> Result<SfuToken, TokenServiceError> {
    if !(200..300).contains(&status) {
        return Err(TokenServiceError::Rejected {
            status,
            body: body.to_owned(),
        });
    }
    Ok(serde_json::from_str(body)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_token() -> OpenIdToken {
        OpenIdToken {
            access_token: "FPkexLLvKbAHKclQhpvgfWxx".to_owned(),
            token_type: "Bearer".to_owned(),
            matrix_server_name: "matrix.example.com".to_owned(),
            expires_in: 3600,
        }
    }

    fn sample_member() -> MemberClaims {
        MemberClaims {
            id: "xyzABCDEF10123".to_owned(),
            claimed_user_id: "@user:matrix.example.com".to_owned(),
            claimed_device_id: "DEVICEID".to_owned(),
        }
    }

    // Mirrors the request body shape from the MSC4195 /get_token example.
    #[test]
    fn get_token_request_serializes_per_msc4195() {
        let token = sample_token();
        let member = sample_member();
        let (endpoint, body) = get_token_request(
            "https://matrix-rtc.example.com/livekit/jwt/",
            "!tDLCaLXijNtYcJZEey:example.com",
            "the_id",
            &member,
            &token,
        );

        assert_eq!(
            endpoint,
            "https://matrix-rtc.example.com/livekit/jwt/get_token"
        );
        let expected = serde_json::json!({
            "room_id": "!tDLCaLXijNtYcJZEey:example.com",
            "slot_id": "the_id",
            "openid_token": {
                "access_token": "FPkexLLvKbAHKclQhpvgfWxx",
                "expires_in": 3600,
                "matrix_server_name": "matrix.example.com",
                "token_type": "Bearer"
            },
            "member": {
                "id": "xyzABCDEF10123",
                "claimed_device_id": "DEVICEID",
                "claimed_user_id": "@user:matrix.example.com"
            }
        });
        assert_eq!(body, expected);
    }

    // Mirrors the successful response example from MSC4195.
    #[test]
    fn sfu_token_deserializes_per_msc4195() {
        let token = parse_sfu_token(
            200,
            r#"{"jwt": "thejwt", "url": "wss://matrix-rtc.example.com/livekit/sfu"}"#,
        )
        .unwrap();
        assert_eq!(token.jwt, "thejwt");
        assert_eq!(token.url, "wss://matrix-rtc.example.com/livekit/sfu");
    }

    /// The pre-MSC4195 body: a flat `room` instead of `room_id` + `slot_id`, a
    /// bare `device_id` instead of the `member` object, and no member id at all —
    /// which is why that generation's SFU identity carries no session component.
    #[test]
    fn legacy_get_token_request_serializes_for_sfu_get() {
        let token = sample_token();
        let (endpoint, body) = legacy_token_request(
            "https://matrix-rtc.example.com/livekit/jwt",
            "!tDLCaLXijNtYcJZEey:example.com",
            "DEVICEID",
            &token,
        );

        assert_eq!(
            endpoint,
            "https://matrix-rtc.example.com/livekit/jwt/sfu/get"
        );
        let expected = serde_json::json!({
            "room": "!tDLCaLXijNtYcJZEey:example.com",
            "openid_token": {
                "access_token": "FPkexLLvKbAHKclQhpvgfWxx",
                "expires_in": 3600,
                "matrix_server_name": "matrix.example.com",
                "token_type": "Bearer"
            },
            "device_id": "DEVICEID"
        });
        assert_eq!(body, expected);
        // Nothing of the MSC4195 body leaks in: the legacy service rejects a
        // body it cannot parse rather than ignoring the extra keys.
        assert!(body.get("room_id").is_none());
        assert!(body.get("slot_id").is_none());
        assert!(body.get("member").is_none());
    }

    /// The response is the one part the two dialects agree on, so `SfuToken`
    /// serves both and there is no legacy response type.
    #[test]
    fn the_legacy_response_is_the_same_shape() {
        let token =
            parse_sfu_token(200, r#"{"jwt": "thejwt", "url": "wss://sfu.example.com"}"#).unwrap();
        assert_eq!(token.jwt, "thejwt");
    }

    #[test]
    fn a_rejection_keeps_the_status_and_body() {
        let error = parse_sfu_token(403, "no dice").unwrap_err();
        match error {
            TokenServiceError::Rejected { status, body } => {
                assert_eq!(status, 403);
                assert_eq!(body, "no dice");
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn a_malformed_success_body_is_a_decode_error() {
        let error = parse_sfu_token(200, r#"{"jwt": 42}"#).unwrap_err();
        assert!(matches!(error, TokenServiceError::Decode(_)));
    }
}
