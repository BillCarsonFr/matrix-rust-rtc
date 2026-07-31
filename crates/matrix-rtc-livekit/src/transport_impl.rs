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

//! [`MediaTransport`] implementation for the MSC4195 LiveKit transport.
//!
//! This is where LiveKit stops being visible: everything above this module
//! (the [`matrix_rtc_media::CallEngine`], the `Call` facade's unified event
//! stream, and eventually the FFI) speaks the transport-neutral vocabulary of
//! `matrix-rtc-media`, and this module translates it to SFU reality —
//! the token exchange, the E2EE room connection, `RoomEvent`s, and
//! `NativeAudioStream` frames.
//!
//! The connection grouping key is the `livekit_service_url`: MSC4195 says
//! members announcing the same focus share one LiveKit room (same
//! `livekit_alias`), while every other focus needs its own subscribe-side
//! connection.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use livekit::prelude::{
    Participant, RemoteTrack, RoomEvent, TrackKind, TrackPublication, TrackSource,
};
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use matrix_rtc_core::{JoinedMembership, RtcTransport};
use matrix_rtc_media::{
    AudioFrame, ConnectionContext, ConnectionEvent, MediaStreamKind, MediaTransport,
    RemoteTrackHandle, TransportConnection, TransportError,
};

use crate::identity::pseudonymous_identity;
use crate::session::LiveKitSession;
use crate::token::{MemberClaims, OpenIdTokenSource};
use crate::{LiveKitTransportConfig, connect_e2ee};

/// Sample rate remote audio is resampled to before crossing the transport
/// boundary, matching what the recording helpers in [`crate::media`] use.
const AUDIO_SAMPLE_RATE: i32 = 48_000;
const AUDIO_CHANNELS: i32 = 1;

/// The MSC4195 LiveKit backend of [`MediaTransport`].
///
/// One instance serves a whole call: [`MediaTransport::connect`] can be
/// invoked once per focus, and every connection shares the same E2EE
/// `KeyProvider` handle (keys are indexed by pseudonymous identity, which is
/// globally unique per membership, so one ring serves all foci).
pub struct LiveKitMediaTransport {
    http: reqwest::Client,
    token_source: Arc<dyn OpenIdTokenSource>,
    key_provider: livekit::e2ee::key_provider::KeyProvider,
}

impl LiveKitMediaTransport {
    /// `key_provider` MUST be the same handle given to the
    /// [`crate::MediaKeyBridge`] importing the core's media keys.
    pub fn new(
        http: reqwest::Client,
        token_source: Arc<dyn OpenIdTokenSource>,
        key_provider: livekit::e2ee::key_provider::KeyProvider,
    ) -> Self {
        Self {
            http,
            token_source,
            key_provider,
        }
    }

    /// Typed variant of [`MediaTransport::connect`], for callers that need
    /// access to the underlying [`LiveKitSession`] (the `Call` facade's
    /// deprecated raw accessors).
    pub async fn connect_livekit(
        &self,
        livekit_service_url: &str,
        ctx: &ConnectionContext,
    ) -> Result<
        (
            LiveKitTransportConnection,
            UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    > {
        let config = LiveKitTransportConfig {
            livekit_service_url: livekit_service_url.to_owned(),
            room_id: ctx.room_id.clone(),
            slot_id: ctx.slot_id.clone(),
            member: MemberClaims {
                id: ctx.member.member_id.clone(),
                claimed_user_id: ctx.member.user_id.clone(),
                claimed_device_id: ctx.member.device_id.clone(),
            },
        };
        let connection = connect_e2ee(
            &self.http,
            &config,
            self.token_source.as_ref(),
            self.key_provider.clone(),
        )
        .await
        .map_err(|error| TransportError::Connect(error.to_string()))?;

        // The receiver returned by the room connect carries events from the
        // very beginning (nothing can be missed); hand it to the translator.
        let (events_tx, events_rx) = unbounded_channel();
        tokio::spawn(translate_room_events(connection.events, events_tx));

        Ok((
            LiveKitTransportConnection {
                connection_key: livekit_service_url.to_owned(),
                session: Arc::new(connection.session),
            },
            events_rx,
        ))
    }
}

#[async_trait]
impl MediaTransport for LiveKitMediaTransport {
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
        // MSC4195: identity = hash(user, device, member_id); the device is
        // the one that encrypted the member event (MSC4143 dropped
        // `claimed_device_id`). Without an attributable device there is no
        // identity to expect on the SFU.
        let device_id = member.origin.sender_device_id()?;
        Some(pseudonymous_identity(
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
            UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    > {
        let (connection, events) = self.connect_livekit(connection_key, ctx).await?;
        Ok((Box::new(connection), events))
    }
}

/// One live SFU connection.
///
/// A cheap clonable handle: the caller can keep one clone (for raw session
/// access and the final close) while another is owned by the engine's pool.
#[derive(Clone)]
pub struct LiveKitTransportConnection {
    connection_key: String,
    session: Arc<LiveKitSession>,
}

impl LiveKitTransportConnection {
    /// The underlying session, for the `Call` facade's transition-period raw
    /// accessors.
    pub fn session(&self) -> &LiveKitSession {
        &self.session
    }
}

#[async_trait]
impl TransportConnection for LiveKitTransportConnection {
    fn connection_key(&self) -> &str {
        &self.connection_key
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.session
            .room()
            .close()
            .await
            .map_err(|error| TransportError::Closed(error.to_string()))
    }
}

/// Translate the LiveKit room event stream into the transport-neutral
/// [`ConnectionEvent`] vocabulary. Runs until the room closes its channel;
/// the outbound channel closing (engine gone) stops it early.
async fn translate_room_events(
    mut events: UnboundedReceiver<RoomEvent>,
    tx: UnboundedSender<ConnectionEvent>,
) {
    while let Some(event) = events.recv().await {
        let translated = match event {
            RoomEvent::ParticipantConnected(participant) => Some(ConnectionEvent::RemoteJoined {
                identity: participant.identity().to_string(),
            }),
            RoomEvent::ParticipantDisconnected(participant) => Some(ConnectionEvent::RemoteLeft {
                identity: participant.identity().to_string(),
            }),
            RoomEvent::TrackSubscribed {
                track,
                publication,
                participant,
            } => {
                let kind = stream_kind(publication.source(), publication.kind());
                Some(ConnectionEvent::TrackAdded {
                    identity: participant.identity().to_string(),
                    kind,
                    track: Arc::new(LiveKitRemoteTrack { kind, track }),
                })
            }
            RoomEvent::TrackUnsubscribed {
                publication,
                participant,
                ..
            } => Some(ConnectionEvent::TrackRemoved {
                identity: participant.identity().to_string(),
                kind: stream_kind(publication.source(), publication.kind()),
            }),
            RoomEvent::TrackMuted {
                participant,
                publication,
            } => remote_mute_event(&participant, &publication, true),
            RoomEvent::TrackUnmuted {
                participant,
                publication,
            } => remote_mute_event(&participant, &publication, false),
            RoomEvent::ActiveSpeakersChanged { speakers } => {
                Some(ConnectionEvent::ActiveSpeakers {
                    identities: speakers
                        .iter()
                        .map(|speaker| speaker.identity().to_string())
                        .collect(),
                })
            }
            RoomEvent::Reconnecting => Some(ConnectionEvent::Reconnecting),
            RoomEvent::Reconnected => Some(ConnectionEvent::Reconnected),
            RoomEvent::Disconnected { reason } => Some(ConnectionEvent::Closed {
                message: format!("{reason:?}"),
            }),
            // Everything else (data, transcriptions, metadata, local echoes
            // of our own publications, ...) stays LiveKit-internal for now.
            _ => None,
        };

        if let Some(event) = translated
            && tx.send(event).is_err()
        {
            return;
        }
    }
}

/// Mute translation, shared by the muted/unmuted arms. Mute events fire for
/// local tracks too; those map onto our own membership via the identity, so
/// they are forwarded like any other.
fn remote_mute_event(
    participant: &Participant,
    publication: &TrackPublication,
    muted: bool,
) -> Option<ConnectionEvent> {
    let identity = participant.identity().to_string();
    let kind = stream_kind(publication.source(), publication.kind());
    Some(if muted {
        ConnectionEvent::TrackMuted { identity, kind }
    } else {
        ConnectionEvent::TrackUnmuted { identity, kind }
    })
}

/// Map LiveKit's track source/kind pair onto the transport-neutral stream
/// kind. `Unknown` sources fall back on the media kind.
fn stream_kind(source: TrackSource, kind: TrackKind) -> MediaStreamKind {
    match source {
        TrackSource::Microphone => MediaStreamKind::Microphone,
        TrackSource::Camera => MediaStreamKind::Camera,
        TrackSource::Screenshare => MediaStreamKind::ScreenShare,
        TrackSource::ScreenshareAudio => MediaStreamKind::ScreenShareAudio,
        TrackSource::Unknown => match kind {
            TrackKind::Audio => MediaStreamKind::Microphone,
            TrackKind::Video => MediaStreamKind::Camera,
        },
    }
}

/// A subscribed LiveKit track behind the transport-neutral handle.
struct LiveKitRemoteTrack {
    kind: MediaStreamKind,
    track: RemoteTrack,
}

impl RemoteTrackHandle for LiveKitRemoteTrack {
    fn kind(&self) -> MediaStreamKind {
        self.kind
    }

    fn audio_frames(&self) -> Option<futures_util::stream::BoxStream<'static, AudioFrame>> {
        let RemoteTrack::Audio(track) = &self.track else {
            return None;
        };
        let stream = NativeAudioStream::new(track.rtc_track(), AUDIO_SAMPLE_RATE, AUDIO_CHANNELS);
        Some(
            stream
                .map(|frame| AudioFrame {
                    data: frame.data.into_owned(),
                    sample_rate: frame.sample_rate,
                    num_channels: frame.num_channels,
                    samples_per_channel: frame.samples_per_channel,
                })
                .boxed(),
        )
    }

    // `video_frames` stays at the default `None` until the video phase.
}
