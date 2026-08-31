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

//! The browser LiveKit transport: `matrix-rtc-media`'s transport traits over a
//! JS delegate driving livekit-js.
//!
//! The division of labour mirrors [`JsCommandSender`](crate::JsCommandSender):
//! Rust owns the protocol — token request building and response decoding
//! (`matrix-rtc-livekit-proto`), identity derivation, connection keying, and
//! the engine's pool/backoff policy — while JS owns the IO: the OpenID token
//! (matrix-js-sdk has it natively), the token `fetch` (so app-level CORS,
//! proxy, and abort policy stay where the app configures them), and
//! `Room.connect` itself. Media never crosses into Rust: livekit-js keeps the
//! tracks, and the roster learns of them through the
//! [`WasmConnectionEventSink`] the delegate feeds.
//!
//! The delegate object must implement:
//!
//! - `getOpenIdToken() -> Promise<{access_token, token_type,
//!   matrix_server_name, expires_in}>`
//! - `fetchJson(url, body) -> Promise<{status, body}>` — POST `body` (a plain
//!   object) as JSON to `url`, resolving with the HTTP status and the raw
//!   response text
//! - `connect({connectionKey, sfuUrl, jwt}, sink) -> Promise<handle>` —
//!   connect a livekit-js `Room` to `sfuUrl` with `jwt`, register the
//!   RoomEvent translation onto `sink`, and resolve with a handle exposing
//!   `close() -> Promise`
//! - `setLocalKeyIndex(index)` — move the local sender onto a rotated key
//!   index (see `MediaKeyHandler::set_local_sender`)
//! - `setKey(identity, index, key: Uint8Array) -> Promise<boolean|void>` —
//!   install a media key in livekit-js's key provider (the
//!   [`FrameKeyRing`](matrix_rtc_media::keys::FrameKeyRing) seam)

use std::sync::Arc;

use async_trait::async_trait;
use js_sys::{Array, Function, Promise, Reflect, Uint8Array};
use matrix_rtc_bridge::OpenIdToken;
use matrix_rtc_core::{JoinedMembership, RtcIdentityMapper, RtcTransport};
use matrix_rtc_livekit_proto::token::{get_token_request, legacy_token_request, parse_sfu_token};
use matrix_rtc_livekit_proto::{SfuToken, TokenEndpoint};
use matrix_rtc_media::keys::FrameKeyRing;
use matrix_rtc_media::{
    ConnectionContext, ConnectionEvent, FrameEncryptionState, MediaStreamKind, MediaTransport,
    RemoteTrackHandle, SpeakingParticipant, TransportConnection, TransportError,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

/// Renders a thrown JS value into a message.
fn js_error_string(error: JsValue) -> String {
    if let Ok(error) = error.clone().dyn_into::<js_sys::Error>() {
        String::from(error.message())
    } else {
        error.as_string().unwrap_or_else(|| format!("{error:?}"))
    }
}

/// Calls `name` on `target` and awaits the returned Promise (a non-Promise
/// result resolves immediately). Errors carry the method name: the delegate is
/// app-provided, so "which method" is the actionable half of any failure.
async fn call_delegate(
    target: &JsValue,
    name: &str,
    args: &[&JsValue],
) -> Result<JsValue, TransportError> {
    let method = Reflect::get(target, &JsValue::from_str(name))
        .ok()
        .filter(|method| !method.is_undefined())
        .ok_or_else(|| TransportError::Unsupported(format!("delegate missing method: {name}")))?;
    let method: Function = method
        .dyn_into()
        .map_err(|_| TransportError::Unsupported(format!("delegate.{name} is not a function")))?;

    let js_args = Array::new();
    for arg in args {
        js_args.push(arg);
    }
    let result = Reflect::apply(&method, target, &js_args)
        .map_err(|error| TransportError::Connect(format!("{name}: {}", js_error_string(error))))?;

    let promise = if result.is_instance_of::<Promise>() {
        result.unchecked_into::<Promise>()
    } else {
        Promise::resolve(&result)
    };
    JsFuture::from(promise)
        .await
        .map_err(|error| TransportError::Connect(format!("{name}: {}", js_error_string(error))))
}

/// The stream-kind vocabulary shared with JS, matching livekit-js's
/// `Track.Source` strings.
pub(crate) fn stream_kind_str(kind: MediaStreamKind) -> &'static str {
    match kind {
        MediaStreamKind::Microphone => "microphone",
        MediaStreamKind::Camera => "camera",
        MediaStreamKind::ScreenShare => "screen_share",
        MediaStreamKind::ScreenShareAudio => "screen_share_audio",
        MediaStreamKind::Data => "data",
    }
}

fn parse_stream_kind(kind: &str) -> Option<MediaStreamKind> {
    Some(match kind {
        "microphone" => MediaStreamKind::Microphone,
        "camera" => MediaStreamKind::Camera,
        "screen_share" => MediaStreamKind::ScreenShare,
        "screen_share_audio" => MediaStreamKind::ScreenShareAudio,
        "data" => MediaStreamKind::Data,
        _ => return None,
    })
}

/// A remote track as the roster sees it: existence and kind, nothing more.
/// The actual `MediaStreamTrack` never crosses into Rust — livekit-js renders
/// it — so every frame accessor keeps its `None` default.
struct JsRemoteTrack {
    kind: MediaStreamKind,
}

#[async_trait(?Send)]
impl RemoteTrackHandle for JsRemoteTrack {
    fn kind(&self) -> MediaStreamKind {
        self.kind
    }
}

/// Feeds livekit-js room events into the engine, in the transport-neutral
/// vocabulary. One sink per connection; the delegate's `connect` receives it
/// and registers the RoomEvent translation onto it.
///
/// Call [`WasmConnectionEventSink::closed`] when the room disconnects for
/// good, then release the sink (`free()`): the engine treats a sink that goes
/// away as the connection ending.
#[wasm_bindgen]
pub struct WasmConnectionEventSink {
    tx: mpsc::UnboundedSender<ConnectionEvent>,
}

impl WasmConnectionEventSink {
    fn send(&self, event: ConnectionEvent) {
        // A closed channel means the engine is gone (shutdown/drop); events
        // after that are meaningless, not an error.
        let _ = self.tx.send(event);
    }
}

#[wasm_bindgen]
impl WasmConnectionEventSink {
    /// livekit-js `ParticipantConnected`.
    #[wasm_bindgen(js_name = remoteJoined)]
    pub fn remote_joined(&self, identity: String) {
        self.send(ConnectionEvent::RemoteJoined { identity });
    }

    /// livekit-js `ParticipantDisconnected`. Roster departures come from
    /// signalling, not from this — the engine uses it for diagnostics.
    #[wasm_bindgen(js_name = remoteLeft)]
    pub fn remote_left(&self, identity: String) {
        self.send(ConnectionEvent::RemoteLeft { identity });
    }

    /// livekit-js `TrackSubscribed` — and `LocalTrackPublished`, under our own
    /// identity, so our roster entry carries our streams too.
    ///
    /// `kind` is `microphone | camera | screen_share | screen_share_audio |
    /// data` (livekit-js `Track.Source`, snake_case). Unknown kinds are
    /// dropped with a log line.
    #[wasm_bindgen(js_name = trackAdded)]
    pub fn track_added(&self, identity: String, kind: String) {
        let Some(kind) = parse_stream_kind(&kind) else {
            log::warn!("sink: dropping track of unknown kind {kind:?} from {identity}");
            return;
        };
        self.send(ConnectionEvent::TrackAdded {
            identity,
            kind,
            track: Arc::new(JsRemoteTrack { kind }),
        });
    }

    /// livekit-js `TrackUnsubscribed` / `LocalTrackUnpublished`.
    #[wasm_bindgen(js_name = trackRemoved)]
    pub fn track_removed(&self, identity: String, kind: String) {
        let Some(kind) = parse_stream_kind(&kind) else {
            return;
        };
        self.send(ConnectionEvent::TrackRemoved { identity, kind });
    }

    /// livekit-js `TrackMuted` / `TrackUnmuted`.
    #[wasm_bindgen(js_name = trackMuted)]
    pub fn track_muted(&self, identity: String, kind: String, muted: bool) {
        let Some(kind) = parse_stream_kind(&kind) else {
            return;
        };
        self.send(if muted {
            ConnectionEvent::TrackMuted { identity, kind }
        } else {
            ConnectionEvent::TrackUnmuted { identity, kind }
        });
    }

    /// livekit-js `ActiveSpeakersChanged`: an array of
    /// `{identity, level}` (level `0.0`–`1.0`; pass `0` when unknown).
    #[wasm_bindgen(js_name = activeSpeakers)]
    pub fn active_speakers(&self, speakers: JsValue) {
        #[derive(Deserialize)]
        struct Speaker {
            identity: String,
            #[serde(default)]
            level: f32,
        }
        let speakers: Vec<Speaker> = match serde_wasm_bindgen::from_value(speakers) {
            Ok(speakers) => speakers,
            Err(error) => {
                log::warn!("sink: unreadable active-speakers payload: {error}");
                return;
            }
        };
        self.send(ConnectionEvent::ActiveSpeakers {
            speakers: speakers
                .into_iter()
                .map(|speaker| SpeakingParticipant {
                    identity: speaker.identity,
                    level: speaker.level,
                })
                .collect(),
        });
    }

    /// The frame cryptor's verdict on a participant's media changed. `state`
    /// is `ok | missing_key | decryption_failed | encryption_failed |
    /// internal_error`.
    #[wasm_bindgen(js_name = encryptionState)]
    pub fn encryption_state(&self, identity: String, state: String) {
        let state = match state.as_str() {
            "ok" => FrameEncryptionState::Ok,
            "missing_key" => FrameEncryptionState::MissingKey,
            "decryption_failed" => FrameEncryptionState::DecryptionFailed,
            "encryption_failed" => FrameEncryptionState::EncryptionFailed,
            "internal_error" => FrameEncryptionState::InternalError,
            other => {
                log::warn!("sink: unknown encryption state {other:?} from {identity}");
                return;
            }
        };
        self.send(ConnectionEvent::EncryptionStateChanged { identity, state });
    }

    /// livekit-js `Reconnecting`.
    pub fn reconnecting(&self) {
        self.send(ConnectionEvent::Reconnecting);
    }

    /// livekit-js `Reconnected`.
    pub fn reconnected(&self) {
        self.send(ConnectionEvent::Reconnected);
    }

    /// livekit-js `Disconnected`: the room is gone and will not resume.
    pub fn closed(&self, message: String) {
        self.send(ConnectionEvent::Closed { message });
    }
}

/// What the delegate's `connect` receives.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectRequest<'a> {
    /// The engine's grouping key for this connection — the
    /// `livekit_service_url` of the focus.
    connection_key: &'a str,
    /// WebSocket URL of the SFU, from the token response.
    sfu_url: &'a str,
    /// The LiveKit JWT to connect with.
    jwt: &'a str,
}

/// `matrix-rtc-media`'s transport over the JS delegate.
pub(crate) struct JsMediaTransport {
    delegate: JsValue,
    identity_mapper: RtcIdentityMapper,
    token_endpoint: TokenEndpoint,
}

impl JsMediaTransport {
    pub(crate) fn new(
        delegate: JsValue,
        identity_mapper: RtcIdentityMapper,
        token_endpoint: TokenEndpoint,
    ) -> Self {
        Self {
            delegate,
            identity_mapper,
            token_endpoint,
        }
    }

    /// Obtain a fresh OpenID token from the delegate and exchange it for an
    /// SFU JWT: Rust builds the request and decodes the response
    /// (`matrix-rtc-livekit-proto`), the delegate performs the fetch.
    async fn acquire_token(
        &self,
        livekit_service_url: &str,
        ctx: &ConnectionContext,
    ) -> Result<SfuToken, TransportError> {
        let openid = call_delegate(&self.delegate, "getOpenIdToken", &[]).await?;
        let openid: OpenIdToken = serde_wasm_bindgen::from_value(openid).map_err(|error| {
            TransportError::Connect(format!("getOpenIdToken resolved to a non-token: {error}"))
        })?;

        let (url, body) = match self.token_endpoint {
            TokenEndpoint::Msc4195 => get_token_request(
                livekit_service_url,
                &ctx.room_id,
                &ctx.slot_id,
                &matrix_rtc_livekit_proto::MemberClaims {
                    id: ctx.member.member_id.clone(),
                    claimed_user_id: ctx.member.user_id.clone(),
                    claimed_device_id: ctx.member.device_id.clone(),
                },
                &openid,
            ),
            // `room` is the room id, because that is the `livekit_alias` this
            // generation announces on a focus; the two must agree or the
            // clients land in different LiveKit rooms.
            TokenEndpoint::LegacyElementCall => legacy_token_request(
                livekit_service_url,
                &ctx.room_id,
                &ctx.member.device_id,
                &openid,
            ),
        };

        // The JSON-compatible serializer, so the body reaches the delegate as
        // a plain object — the default turns serde maps into ES `Map`s, which
        // `JSON.stringify` renders as `{}`.
        let body = body
            .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
            .map_err(|error| {
                TransportError::Connect(format!("token request did not serialize: {error}"))
            })?;
        let response = call_delegate(
            &self.delegate,
            "fetchJson",
            &[&JsValue::from_str(&url), &body],
        )
        .await?;

        #[derive(Deserialize)]
        struct FetchResponse {
            status: u16,
            body: String,
        }
        let response: FetchResponse =
            serde_wasm_bindgen::from_value(response).map_err(|error| {
                TransportError::Connect(format!("fetchJson resolved to a non-response: {error}"))
            })?;

        parse_sfu_token(response.status, &response.body)
            .map_err(|error| TransportError::Connect(error.to_string()))
    }

    /// The concrete half of [`MediaTransport::connect`], returning the
    /// clonable connection so the caller can keep a handle to the own-focus
    /// one (mirrors the native `connect_livekit`).
    pub(crate) async fn connect_js(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> Result<
        (
            JsTransportConnection,
            mpsc::UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    > {
        let sfu_token = self.acquire_token(connection_key, ctx).await?;

        let (tx, rx) = mpsc::unbounded_channel();
        let sink = WasmConnectionEventSink { tx };
        let request = serde_wasm_bindgen::to_value(&ConnectRequest {
            connection_key,
            sfu_url: &sfu_token.url,
            jwt: &sfu_token.jwt,
        })
        .map_err(|error| {
            TransportError::Connect(format!("connect request did not serialize: {error}"))
        })?;

        let handle =
            call_delegate(&self.delegate, "connect", &[&request, &JsValue::from(sink)]).await?;

        log::info!(
            "transport: connected to focus {connection_key} at {}",
            sfu_token.url
        );
        Ok((
            JsTransportConnection {
                connection_key: connection_key.to_owned(),
                handle,
            },
            rx,
        ))
    }
}

#[async_trait(?Send)]
impl MediaTransport for JsMediaTransport {
    fn transport_type(&self) -> &'static str {
        "livekit"
    }

    fn connection_key(&self, transport: &RtcTransport) -> Option<String> {
        match transport {
            RtcTransport::LiveKit(livekit) => Some(livekit.livekit_service_url.clone()),
            RtcTransport::Unsupported(_) => None,
        }
    }

    fn remote_identity(&self, member: &JoinedMembership) -> Option<String> {
        // Same rule as the native transport: without an attributable sending
        // device there is no identity to expect on the SFU. Through the mapper
        // rather than `pseudonymous_identity` directly, so a legacy call
        // derives peer identities exactly as it derives its own.
        let device_id = member.origin.sender_device_id()?;
        Some((self.identity_mapper)(
            &member.sender,
            device_id,
            &member.member_id,
        ))
    }

    async fn connect(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> Result<
        (
            Box<dyn TransportConnection>,
            mpsc::UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    > {
        let (connection, events) = self.connect_js(connection_key, ctx).await?;
        Ok((Box::new(connection), events))
    }
}

/// A live connection: the handle the delegate's `connect` resolved with.
/// Clones share the same underlying JS object.
#[derive(Clone)]
pub(crate) struct JsTransportConnection {
    connection_key: String,
    handle: JsValue,
}

#[async_trait(?Send)]
impl TransportConnection for JsTransportConnection {
    fn connection_key(&self) -> &str {
        &self.connection_key
    }

    // `publish` and `apply_constraints` keep their defaults: on the web the
    // app publishes and constrains through livekit-js directly.

    async fn close(&self) -> Result<(), TransportError> {
        call_delegate(&self.handle, "close", &[]).await.map(|_| ())
    }
}

/// livekit-js's key ring, through the delegate's `setKey`.
pub(crate) struct JsFrameKeyRing {
    delegate: JsValue,
    ring_size: u16,
}

impl JsFrameKeyRing {
    pub(crate) fn new(delegate: JsValue, ring_size: u16) -> Self {
        Self {
            delegate,
            ring_size,
        }
    }
}

#[async_trait(?Send)]
impl FrameKeyRing for JsFrameKeyRing {
    fn ring_size(&self) -> u16 {
        self.ring_size
    }

    async fn set_key(&self, identity: &str, index: u8, key: Vec<u8>) -> bool {
        let bytes = Uint8Array::from(key.as_slice());
        let result = call_delegate(
            &self.delegate,
            "setKey",
            &[
                &JsValue::from_str(identity),
                &JsValue::from(index),
                &bytes.into(),
            ],
        )
        .await;
        match result {
            // A delegate with nothing to report resolves with undefined;
            // an explicit `false` is a refusal.
            Ok(value) => value.as_bool().unwrap_or(true),
            Err(error) => {
                log::warn!("setKey({identity}, {index}) failed: {error}");
                false
            }
        }
    }
}
