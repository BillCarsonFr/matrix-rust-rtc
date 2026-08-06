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

use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::StreamExt;
use livekit::id::ParticipantIdentity;
use livekit::options::TrackPublishOptions;
use livekit::prelude::{
    LocalAudioTrack, LocalTrack, LocalVideoTrack, Participant, RemoteTrack, RoomEvent,
    RtcAudioSource, TrackDimension, TrackKind, TrackPublication, TrackSource,
};
use livekit::track::VideoQuality as LkVideoQuality;
use livekit::webrtc::audio_source::AudioSourceOptions;
use livekit::webrtc::audio_source::native::NativeAudioSource;
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use livekit::webrtc::native::frame_cryptor::EncryptionState as LkEncryptionState;
use livekit::webrtc::prelude::AudioFrame as LkAudioFrame;
use livekit::webrtc::stats::RtcStats;
use livekit::webrtc::video_frame::{
    I420Buffer as LkI420Buffer, VideoBuffer, VideoFrame as LkVideoFrame,
    VideoRotation as LkVideoRotation,
};
use livekit::webrtc::video_source::native::NativeVideoSource;
use livekit::webrtc::video_source::{RtcVideoSource, VideoResolution};
use livekit::webrtc::video_stream::native::NativeVideoStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use matrix_rtc_core::{JoinedMembership, RtcTransport};
use matrix_rtc_media::{
    AudioFrame, ConnectionContext, ConnectionEvent, FrameEncryptionState, I420Buffer,
    LocalTrackHandle, MediaStreamKind, MediaTransport, PublishOptions, QualityLimit, ReceiveStats,
    RemoteTrackHandle, ResolvedConstraints, SpeakingParticipant, StreamDemand, TransportConnection,
    TransportError, VideoDetail, VideoFrame, VideoRotation,
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
                own_identity: crate::identity::pseudonymous_identity(
                    &ctx.member.user_id,
                    &ctx.member.device_id,
                    &ctx.member.member_id,
                ),
                local_key_index: Arc::new(Mutex::new(None)),
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
    /// Our own MSC4195 participant identity on this connection.
    own_identity: String,
    /// The key index our own frames must carry, once a rotation has activated.
    ///
    /// Held because a frame cryptor is created per published track and starts at
    /// index 0: a track published after a rotation would otherwise be stamped
    /// with an index no peer holds a key for.
    local_key_index: Arc<Mutex<Option<u8>>>,
}

impl LiveKitTransportConnection {
    /// The underlying session, for the `Call` facade's transition-period raw
    /// accessors.
    pub fn session(&self) -> &LiveKitSession {
        &self.session
    }

    /// Switch our own outgoing frames to `key_index`, and remember it for tracks
    /// published later.
    ///
    /// The key provider's `set_key` only fills the key *ring*; the index a
    /// sender stamps lives on its frame cryptor and changes only here. Called
    /// when one of our own rotated keys activates — see
    /// [`crate::LocalKeyIndexHook`].
    pub fn set_local_key_index(&self, key_index: u8) {
        *self
            .local_key_index
            .lock()
            .expect("local key index mutex poisoned") = Some(key_index);
        self.apply_local_key_index(key_index);
    }

    /// Re-assert the current index on our senders, for a track published after a
    /// rotation: its cryptor is new, and new cryptors start at index 0.
    fn reassert_local_key_index(&self) {
        let current = *self
            .local_key_index
            .lock()
            .expect("local key index mutex poisoned");
        if let Some(key_index) = current {
            self.apply_local_key_index(key_index);
        }
    }

    fn apply_local_key_index(&self, key_index: u8) {
        let mut switched = 0usize;
        for ((identity, _track_sid), cryptor) in self.session.room().e2ee_manager().frame_cryptors()
        {
            if identity.as_str() == self.own_identity {
                cryptor.set_key_index(i32::from(key_index));
                switched += 1;
            }
        }
        if switched == 0 {
            // Nothing published yet; `reassert_local_key_index` picks it up when
            // a track is.
            log::debug!(
                "no local frame cryptor to move to key index {key_index} yet (nothing published)"
            );
        } else {
            log::debug!("our own frames now carry key index {key_index} ({switched} cryptor(s))");
        }
    }
}

#[async_trait]
impl TransportConnection for LiveKitTransportConnection {
    fn connection_key(&self) -> &str {
        &self.connection_key
    }

    async fn publish(
        &self,
        options: PublishOptions,
    ) -> Result<Arc<dyn LocalTrackHandle>, TransportError> {
        let kind = options.kind;
        let source_kind = livekit_track_source(kind);
        match kind {
            MediaStreamKind::Microphone | MediaStreamKind::ScreenShareAudio => {
                let config = options.audio.unwrap_or_default();
                let source = NativeAudioSource::new(
                    AudioSourceOptions::default(),
                    config.sample_rate,
                    config.num_channels,
                    1000,
                );
                let track = LocalAudioTrack::create_audio_track(
                    "audio",
                    RtcAudioSource::Native(source.clone()),
                );
                self.session
                    .room()
                    .local_participant()
                    .publish_track(
                        LocalTrack::Audio(track),
                        TrackPublishOptions {
                            source: source_kind,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| TransportError::Connect(error.to_string()))?;
                // A frame cryptor is created per published track, at index 0.
                // If we have already rotated, this one must be moved onto the
                // current index or its frames carry a key nobody holds.
                self.reassert_local_key_index();
                Ok(Arc::new(LiveKitLocalTrack {
                    kind,
                    source: LocalSource::Audio(source),
                }))
            }
            MediaStreamKind::Camera | MediaStreamKind::ScreenShare => {
                let config = options.video.ok_or_else(|| {
                    TransportError::Unsupported(
                        "publishing video requires a VideoSourceConfig".into(),
                    )
                })?;
                let source = NativeVideoSource::new(
                    VideoResolution {
                        width: config.width,
                        height: config.height,
                    },
                    matches!(kind, MediaStreamKind::ScreenShare),
                );
                let track = LocalVideoTrack::create_video_track(
                    "video",
                    RtcVideoSource::Native(source.clone()),
                );
                // Below 480px livekit computes a single simulcast encoding
                // (rid "q" only) — a degenerate shape the SFU delivered no
                // frames for in testing. One layer wants a plain encoding.
                let simulcast = options.simulcast && u32::max(config.width, config.height) >= 480;
                self.session
                    .room()
                    .local_participant()
                    .publish_track(
                        LocalTrack::Video(track),
                        TrackPublishOptions {
                            source: source_kind,
                            simulcast,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| TransportError::Connect(error.to_string()))?;
                self.reassert_local_key_index();
                Ok(Arc::new(LiveKitLocalTrack {
                    kind,
                    source: LocalSource::Video(source),
                }))
            }
            MediaStreamKind::Data => Err(TransportError::Unsupported(
                "data publishing is not implemented yet".into(),
            )),
        }
    }

    async fn apply_constraints(
        &self,
        identity: &str,
        kind: MediaStreamKind,
        resolved: ResolvedConstraints,
    ) -> Result<(), TransportError> {
        // The participant or publication being gone is not an error: the
        // engine re-applies constraints when the stream (re)appears.
        let participants = self.session.room().remote_participants();
        let Some(participant) = participants.get(&ParticipantIdentity::from(identity)) else {
            return Ok(());
        };
        let Some(publication) = participant
            .track_publications()
            .into_values()
            .find(|publication| stream_kind(publication.source(), publication.kind()) == kind)
        else {
            return Ok(());
        };

        // `Off` SHOULD be a full unsubscribe (`set_subscribed(false)`), but
        // livekit 0.7.48's client-side *re*subscribe is unreliable: the SFU
        // resumes RTP on the previous receiver without a new OnTrack, so no
        // `TrackSubscribed` fires, `publication.track()` stays `None`, and
        // every subsequent settings call no-ops — the stream is stranded
        // (observed against livekit-server v1.10.1). Until that works
        // upstream, `Off` maps to the pause path too: identical zero
        // bandwidth (dynacast even stops the publisher's encoder), only an
        // idle decoder object is retained.
        //
        // `Paused`/`Off` keep the subscription: `set_enabled(false)` sets
        // the `disabled` flag server-side — no data, instant resume.
        publication.set_enabled(matches!(resolved.demand, StreamDemand::Active));

        // CAUTION: every SDK setter sends a full-replacement
        // `UpdateTrackSettings`. `set_video_quality` in particular sends
        // *only* the (protocol-deprecated) quality field — zeroed dimensions
        // and `disabled: false` — so it must never follow a pause or carry a
        // size hint alongside. `VideoDetail` being an exclusive enum plus the
        // pause guard below keeps every combination consistent.
        if matches!(kind, MediaStreamKind::Camera | MediaStreamKind::ScreenShare) {
            match resolved.detail {
                VideoDetail::Auto => {}
                VideoDetail::Dimensions(dimensions) => {
                    // Sends {disabled, width, height}: consistent with the
                    // pause state set above.
                    publication.update_video_dimensions(TrackDimension(
                        dimensions.width,
                        dimensions.height,
                    ));
                }
                VideoDetail::Quality(limit) => {
                    if matches!(resolved.demand, StreamDemand::Active) {
                        // No-op (with a livekit warning) on non-simulcast
                        // tracks.
                        publication.set_video_quality(match limit {
                            QualityLimit::Low => LkVideoQuality::Low,
                            QualityLimit::Medium => LkVideoQuality::Medium,
                            QualityLimit::High => LkVideoQuality::High,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Idempotent: closing a connection that is already closed succeeds.
    ///
    /// LiveKit answers `RoomError::AlreadyClosed` both for a second `close()`
    /// and for a room the SFU already tore down — the latter being the normal
    /// case when the server disconnected us before the user hung up. Either way
    /// the postcondition callers want ("this connection is not open") already
    /// holds, so reporting a failure only makes a clean hangup look broken.
    async fn close(&self) -> Result<(), TransportError> {
        match self.session.room().close().await {
            Ok(()) => Ok(()),
            Err(livekit::RoomError::AlreadyClosed) => {
                log::debug!(
                    "connection {} was already closed; treating as success",
                    self.connection_key,
                );
                Ok(())
            }
            Err(error) => Err(TransportError::Closed(error.to_string())),
        }
    }
}

/// The native source behind a local publication.
enum LocalSource {
    Audio(NativeAudioSource),
    Video(NativeVideoSource),
}

/// A local LiveKit publication accepting raw frames from the application.
struct LiveKitLocalTrack {
    kind: MediaStreamKind,
    source: LocalSource,
}

#[async_trait]
impl LocalTrackHandle for LiveKitLocalTrack {
    fn kind(&self) -> MediaStreamKind {
        self.kind
    }

    async fn capture_audio(&self, frame: AudioFrame) -> Result<(), TransportError> {
        let LocalSource::Audio(source) = &self.source else {
            return Err(TransportError::Unsupported(
                "this publication does not accept audio frames".into(),
            ));
        };
        let frame = LkAudioFrame {
            data: Cow::Owned(frame.data),
            sample_rate: frame.sample_rate,
            num_channels: frame.num_channels,
            samples_per_channel: frame.samples_per_channel,
        };
        source
            .capture_frame(&frame)
            .await
            .map_err(|error| TransportError::Closed(error.to_string()))
    }

    fn capture_video(&self, frame: VideoFrame) -> Result<(), TransportError> {
        let LocalSource::Video(source) = &self.source else {
            return Err(TransportError::Unsupported(
                "this publication does not accept video frames".into(),
            ));
        };
        let buffer = to_libwebrtc_i420(&frame.buffer)?;
        source.capture_frame(&LkVideoFrame {
            rotation: to_livekit_rotation(frame.rotation),
            timestamp_us: frame.timestamp_us,
            frame_metadata: None,
            buffer,
        });
        Ok(())
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
                    // `audio_level` comes with the same event; without it a host
                    // has to meter the PCM itself to answer "how loud".
                    speakers: speakers
                        .iter()
                        .map(|speaker| SpeakingParticipant {
                            identity: speaker.identity().to_string(),
                            level: speaker.audio_level(),
                        })
                        .collect(),
                })
            }
            RoomEvent::Reconnecting => Some(ConnectionEvent::Reconnecting),
            RoomEvent::Reconnected => Some(ConnectionEvent::Reconnected),
            RoomEvent::Disconnected { reason } => Some(ConnectionEvent::Closed {
                message: format!("{reason:?}"),
            }),
            RoomEvent::E2eeStateChanged { participant, state } => {
                // Media that is subscribed but never decodes is invisible
                // without this (MissingKey / DecryptionFailed = the key
                // exchange or identity mapping went wrong for that stream).
                // Forwarded rather than merely logged: a host cannot build
                // diagnostics on our log output.
                encryption_state(state).map(|state| ConnectionEvent::EncryptionStateChanged {
                    identity: participant.identity().to_string(),
                    state,
                })
            }
            // Everything else (data, transcriptions, metadata, local echoes
            // of our own publications, ...) stays LiveKit-internal for now.
            _ => None,
        };

        let Some(event) = translated else { continue };

        match &event {
            // High frequency; would drown everything else at `debug`.
            ConnectionEvent::ActiveSpeakers { .. } => log::trace!("sfu event: {event:?}"),
            _ => log::debug!("sfu event: {event:?}"),
        }

        if tx.send(event).is_err() {
            log::debug!("sfu event stream closed by the engine; stopping the translator");
            return;
        }
    }

    log::debug!("sfu closed its event stream; stopping the translator");
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

/// The LiveKit track source a publication of `kind` is announced under
/// (the reverse of [`stream_kind`]).
fn livekit_track_source(kind: MediaStreamKind) -> TrackSource {
    match kind {
        MediaStreamKind::Microphone => TrackSource::Microphone,
        MediaStreamKind::Camera => TrackSource::Camera,
        MediaStreamKind::ScreenShare => TrackSource::Screenshare,
        MediaStreamKind::ScreenShareAudio => TrackSource::ScreenshareAudio,
        MediaStreamKind::Data => TrackSource::Unknown,
    }
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

/// Map the frame cryptor's state onto the transport-neutral vocabulary.
///
/// `None` for the two states that carry no diagnostic value: `New` is the
/// cryptor's initial state (fired before anything has been attempted), and
/// `KeyRatcheted` cannot occur with the MSC4195 per-participant key provider,
/// which never ratchets.
fn encryption_state(state: LkEncryptionState) -> Option<FrameEncryptionState> {
    match state {
        LkEncryptionState::Ok => Some(FrameEncryptionState::Ok),
        LkEncryptionState::MissingKey => Some(FrameEncryptionState::MissingKey),
        LkEncryptionState::DecryptionFailed => Some(FrameEncryptionState::DecryptionFailed),
        LkEncryptionState::EncryptionFailed => Some(FrameEncryptionState::EncryptionFailed),
        LkEncryptionState::InternalError => Some(FrameEncryptionState::InternalError),
        LkEncryptionState::New => None,
        LkEncryptionState::KeyRatcheted => {
            log::warn!("frame cryptor reported a key ratchet, which MSC4195 never asks for");
            None
        }
    }
}

/// A subscribed LiveKit track behind the transport-neutral handle.
struct LiveKitRemoteTrack {
    kind: MediaStreamKind,
    track: RemoteTrack,
}

#[async_trait]
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

    fn video_frames(&self) -> Option<futures_util::stream::BoxStream<'static, VideoFrame>> {
        let RemoteTrack::Video(track) = &self.track else {
            return None;
        };
        // The native stream keeps only the latest frame (queue of 1), so a
        // slow consumer drops frames instead of buffering.
        let stream = NativeVideoStream::new(track.rtc_track());
        Some(stream.map(to_media_video_frame).boxed())
    }

    async fn receive_stats(&self) -> Option<ReceiveStats> {
        let stats = match self.track.get_stats().await {
            Ok(stats) => stats,
            Err(error) => {
                log::debug!("could not read receive stats: {error}");
                return None;
            }
        };
        // One RTCP report per SSRC; the RTX and FEC streams get their own
        // `InboundRtp` entries, so sum rather than taking the first.
        let inbound: Vec<_> = stats
            .iter()
            .filter_map(|entry| match entry {
                RtcStats::InboundRtp(inbound) => Some(inbound),
                _ => None,
            })
            .collect();
        if inbound.is_empty() {
            // Before the first RTCP report there is nothing to report; a
            // caller polling early gets `None` rather than a misleading zero.
            return None;
        }
        Some(ReceiveStats {
            packets_received: inbound.iter().map(|s| s.received.packets_received).sum(),
            packets_lost: inbound.iter().map(|s| s.received.packets_lost).sum(),
            bytes_received: inbound.iter().map(|s| s.inbound.bytes_received).sum(),
            // Jitter is a per-stream measurement, not a total: report the
            // worst of them.
            jitter: inbound
                .iter()
                .map(|s| s.received.jitter)
                .fold(0.0_f64, f64::max),
            frames_decoded: inbound
                .iter()
                .map(|s| u64::from(s.inbound.frames_decoded))
                .sum(),
            frames_dropped: inbound
                .iter()
                .map(|s| u64::from(s.inbound.frames_dropped))
                .sum(),
            total_samples_received: inbound
                .iter()
                .map(|s| s.inbound.total_samples_received)
                .sum(),
            concealed_samples: inbound.iter().map(|s| s.inbound.concealed_samples).sum(),
            silent_concealed_samples: inbound
                .iter()
                .map(|s| s.inbound.silent_concealed_samples)
                .sum(),
            concealment_events: inbound.iter().map(|s| s.inbound.concealment_events).sum(),
        })
    }
}

/// Convert a decoded LiveKit frame into the owned, transport-neutral I420
/// frame (decoder buffers may be NV12/native; normalize once here).
fn to_media_video_frame(frame: livekit::webrtc::video_frame::BoxVideoFrame) -> VideoFrame {
    let i420 = frame.buffer.to_i420();
    let (stride_y, stride_u, stride_v) = i420.strides();
    let (data_y, data_u, data_v) = i420.data();
    VideoFrame {
        buffer: I420Buffer {
            width: i420.width(),
            height: i420.height(),
            data_y: data_y.to_vec(),
            stride_y,
            data_u: data_u.to_vec(),
            stride_u,
            data_v: data_v.to_vec(),
            stride_v,
        },
        rotation: match frame.rotation {
            LkVideoRotation::VideoRotation0 => VideoRotation::Deg0,
            LkVideoRotation::VideoRotation90 => VideoRotation::Deg90,
            LkVideoRotation::VideoRotation180 => VideoRotation::Deg180,
            LkVideoRotation::VideoRotation270 => VideoRotation::Deg270,
        },
        timestamp_us: frame.timestamp_us,
    }
}

fn to_livekit_rotation(rotation: VideoRotation) -> LkVideoRotation {
    match rotation {
        VideoRotation::Deg0 => LkVideoRotation::VideoRotation0,
        VideoRotation::Deg90 => LkVideoRotation::VideoRotation90,
        VideoRotation::Deg180 => LkVideoRotation::VideoRotation180,
        VideoRotation::Deg270 => LkVideoRotation::VideoRotation270,
    }
}

/// Copy an application-provided I420 buffer into a libwebrtc one, honouring
/// both sides' strides. Errors (rather than panicking) on inconsistent plane
/// sizes — the buffer comes from the application.
fn to_libwebrtc_i420(buffer: &I420Buffer) -> Result<LkI420Buffer, TransportError> {
    let mut out = LkI420Buffer::new(buffer.width, buffer.height);
    let (dst_stride_y, dst_stride_u, dst_stride_v) = out.strides();
    let (dst_y, dst_u, dst_v) = out.data_mut();

    let chroma_width = buffer.width.div_ceil(2) as usize;
    let chroma_height = buffer.height.div_ceil(2) as usize;
    copy_plane(
        dst_y,
        dst_stride_y,
        &buffer.data_y,
        buffer.stride_y,
        buffer.width as usize,
        buffer.height as usize,
    )?;
    copy_plane(
        dst_u,
        dst_stride_u,
        &buffer.data_u,
        buffer.stride_u,
        chroma_width,
        chroma_height,
    )?;
    copy_plane(
        dst_v,
        dst_stride_v,
        &buffer.data_v,
        buffer.stride_v,
        chroma_width,
        chroma_height,
    )?;
    Ok(out)
}

fn copy_plane(
    dst: &mut [u8],
    dst_stride: u32,
    src: &[u8],
    src_stride: u32,
    width: usize,
    rows: usize,
) -> Result<(), TransportError> {
    let dst_stride = dst_stride as usize;
    let src_stride = src_stride as usize;
    if width > src_stride
        || rows.saturating_sub(1) * src_stride + width > src.len()
        || rows.saturating_sub(1) * dst_stride + width > dst.len()
    {
        return Err(TransportError::Unsupported(
            "video frame plane does not match its declared dimensions".into(),
        ));
    }
    for row in 0..rows {
        dst[row * dst_stride..][..width].copy_from_slice(&src[row * src_stride..][..width]);
    }
    Ok(())
}
