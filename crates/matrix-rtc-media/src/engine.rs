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

//! The call engine: one flat participant roster over N transport connections.
//!
//! The engine consumes two inputs — the core's membership snapshots (the
//! signalling truth about who is in the call) and [`ConnectionEvent`]s from
//! transport connections — and reconciles them into the [`Participant`]
//! roster and the unified [`CallEvent`] stream.
//!
//! Identity mapping is the load-bearing piece: transports know participants
//! by their own identities (LiveKit: the MSC4195 pseudonymous identity), and
//! the engine reverse-maps those to `member_id`s using
//! [`MediaTransport::remote_identity`]. Transport participants that map to no
//! membership are never surfaced as participants (and their media could not
//! be decrypted anyway — keys are distributed per membership). Media that
//! arrives *before* its membership (SFU connects are often faster than sticky
//! event propagation) is buffered and flushed when the membership lands.
//!
//! This phase manages a roster over connections attached by the caller
//! ([`CallEngine::attach_connection`]); connection lifecycle — grouping
//! members by focus, connecting/closing as memberships come and go — moves
//! into the engine with multi-focus support.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use matrix_rtc_core::JoinedMembership;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;

use crate::event::{CallEvent, EndedReason};
use crate::participant::{MediaStreamKind, Participant, StreamState};
use crate::transport::{ConnectionEvent, MediaTransport, RemoteTrackHandle};

/// Capacity of the broadcast [`CallEvent`] channel. Subscribers that fall
/// further behind than this observe a `Lagged` error and miss events; they
/// should resynchronise from [`CallEngine::participants`].
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Subscribed remote tracks, keyed by member and stream kind. Shared between
/// the actor (writes) and [`CallEngine::remote_track`] (reads).
type TrackMap = Arc<Mutex<HashMap<(String, MediaStreamKind), Arc<dyn RemoteTrackHandle>>>>;

/// Tracks whose transport identity has no membership yet, buffered per
/// identity until the membership lands.
type PendingTracks = HashMap<String, Vec<(MediaStreamKind, Arc<dyn RemoteTrackHandle>)>>;

/// Static configuration of a [`CallEngine`].
pub struct EngineConfig {
    /// Transport backends, in descending order of preference.
    pub transports: Vec<Arc<dyn MediaTransport>>,
    /// `member.id` of our own membership, used to flag the local participant.
    pub own_member_id: String,
}

/// Everything the engine can be told from outside its actor task.
enum ActorMessage {
    /// Start consuming a connection's event stream.
    AttachConnection {
        connection_key: String,
        events: mpsc::UnboundedReceiver<ConnectionEvent>,
    },
    /// An event from an attached connection.
    Connection {
        connection_key: String,
        event: ConnectionEvent,
    },
    /// An attached connection's event stream ended.
    ConnectionEnded { connection_key: String },
    /// A media decryption key was imported for a transport identity.
    KeyImported { identity: String, key_index: u8 },
}

/// A cheap, cloneable handle for feeding the engine from other components
/// (e.g. a key bridge reporting imports), without owning the engine.
#[derive(Clone)]
pub struct EngineHandle {
    messages: mpsc::UnboundedSender<ActorMessage>,
}

impl EngineHandle {
    /// See [`CallEngine::notify_key_imported`]. No-op once the engine is gone.
    pub fn notify_key_imported(&self, identity: impl Into<String>, key_index: u8) {
        let _ = self.messages.send(ActorMessage::KeyImported {
            identity: identity.into(),
            key_index,
        });
    }
}

/// One MatrixRTC call as a flat set of participants with media streams.
///
/// Created per joined slot; dropping the engine stops its background task.
pub struct CallEngine {
    messages: mpsc::UnboundedSender<ActorMessage>,
    events_tx: broadcast::Sender<CallEvent>,
    participants_rx: watch::Receiver<Vec<Participant>>,
    tracks: TrackMap,
    task: JoinHandle<()>,
}

impl Drop for CallEngine {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CallEngine {
    /// Start the engine over the core's membership snapshot channel
    /// (`RtcSession::subscribe_membership_snapshots`). The current snapshot is
    /// applied immediately.
    ///
    /// Must be called within a tokio runtime (spawns the actor task, which is
    /// `Send` — no `LocalSet` needed).
    pub fn new(
        config: EngineConfig,
        memberships: watch::Receiver<Vec<JoinedMembership>>,
    ) -> CallEngine {
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let (participants_tx, participants_rx) = watch::channel(Vec::new());
        let tracks: TrackMap = Arc::new(Mutex::new(HashMap::new()));

        let actor = Actor {
            transports: config.transports,
            own_member_id: config.own_member_id,
            events_tx: events_tx.clone(),
            participants_tx,
            tracks: tracks.clone(),
            messages_tx: messages_tx.clone(),
            roster: Vec::new(),
            identity_map: HashMap::new(),
            pending_tracks: HashMap::new(),
            pending_keys: HashMap::new(),
            degraded: false,
            ended: false,
        };
        let task = tokio::spawn(actor.run(memberships, messages_rx));

        CallEngine {
            messages: messages_tx,
            events_tx,
            participants_rx,
            tracks,
            task,
        }
    }

    /// A cloneable handle for feeding the engine from other components.
    pub fn handle(&self) -> EngineHandle {
        EngineHandle {
            messages: self.messages.clone(),
        }
    }

    /// Subscribe to the unified call event stream.
    pub fn subscribe_events(&self) -> broadcast::Receiver<CallEvent> {
        self.events_tx.subscribe()
    }

    /// The current participant roster.
    pub fn participants(&self) -> Vec<Participant> {
        self.participants_rx.borrow().clone()
    }

    /// Watch the participant roster; the receiver always holds the latest
    /// snapshot.
    pub fn subscribe_participants(&self) -> watch::Receiver<Vec<Participant>> {
        self.participants_rx.clone()
    }

    /// Attach an established transport connection: the engine consumes its
    /// event stream until it ends.
    ///
    /// Transitional API — once the engine owns connection lifecycle
    /// (multi-focus), connections are opened via [`MediaTransport::connect`]
    /// internally instead.
    pub fn attach_connection(
        &self,
        connection_key: impl Into<String>,
        events: mpsc::UnboundedReceiver<ConnectionEvent>,
    ) {
        let _ = self.messages.send(ActorMessage::AttachConnection {
            connection_key: connection_key.into(),
            events,
        });
    }

    /// Report that a media decryption key for `identity` was imported, so the
    /// engine can surface [`CallEvent::KeyImported`] against the right member.
    pub fn notify_key_imported(&self, identity: impl Into<String>, key_index: u8) {
        let _ = self.messages.send(ActorMessage::KeyImported {
            identity: identity.into(),
            key_index,
        });
    }

    /// The handle for a participant's subscribed stream, to open frame
    /// streams from. `None` while no such stream is up (see
    /// [`CallEvent::StreamStarted`] / [`CallEvent::StreamStopped`]).
    pub fn remote_track(
        &self,
        member_id: &str,
        kind: MediaStreamKind,
    ) -> Option<Arc<dyn RemoteTrackHandle>> {
        self.tracks
            .lock()
            .expect("track map mutex poisoned")
            .get(&(member_id.to_owned(), kind))
            .cloned()
    }
}

/// A roster entry plus the identity bookkeeping that doesn't belong on the
/// public [`Participant`].
struct Actor {
    transports: Vec<Arc<dyn MediaTransport>>,
    own_member_id: String,
    events_tx: broadcast::Sender<CallEvent>,
    participants_tx: watch::Sender<Vec<Participant>>,
    tracks: TrackMap,
    /// Handed to connection forwarder tasks so their events funnel into the
    /// same mailbox.
    messages_tx: mpsc::UnboundedSender<ActorMessage>,
    roster: Vec<Participant>,
    /// Transport identity → `member_id`.
    identity_map: HashMap<String, String>,
    /// Media that arrived before its membership, flushed when it lands.
    pending_tracks: PendingTracks,
    /// Imported keys awaiting their membership (latest index per identity).
    pending_keys: HashMap<String, u8>,
    degraded: bool,
    ended: bool,
}

impl Actor {
    async fn run(
        mut self,
        mut memberships: watch::Receiver<Vec<JoinedMembership>>,
        mut messages: mpsc::UnboundedReceiver<ActorMessage>,
    ) {
        // The watch channel always has a value; start from it.
        let initial = memberships.borrow_and_update().clone();
        self.apply_snapshot(initial);

        let mut memberships_open = true;
        loop {
            tokio::select! {
                changed = memberships.changed(), if memberships_open => match changed {
                    Ok(()) => {
                        let snapshot = memberships.borrow_and_update().clone();
                        self.apply_snapshot(snapshot);
                    }
                    // The core session is gone; media may outlive it briefly,
                    // so keep serving connection events.
                    Err(_) => memberships_open = false,
                },
                message = messages.recv() => match message {
                    Some(message) => self.handle_message(message),
                    // All senders gone: the engine handle was dropped.
                    None => break,
                },
            }
        }
    }

    fn handle_message(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::AttachConnection {
                connection_key,
                mut events,
            } => {
                let forward = self.messages_tx.clone();
                tokio::spawn(async move {
                    while let Some(event) = events.recv().await {
                        if forward
                            .send(ActorMessage::Connection {
                                connection_key: connection_key.clone(),
                                event,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                    let _ = forward.send(ActorMessage::ConnectionEnded { connection_key });
                });
            }
            ActorMessage::Connection {
                connection_key,
                event,
            } => {
                self.handle_connection_event(&connection_key, event);
            }
            ActorMessage::ConnectionEnded { connection_key } => {
                // A connection stream ending without a `Closed` event still
                // means this call's media is over (single-connection phase;
                // with a pool this becomes a per-connection removal).
                log::debug!("connection {connection_key} event stream ended");
                self.end(EndedReason::ConnectionClosed {
                    message: "connection event stream ended".to_owned(),
                });
            }
            ActorMessage::KeyImported {
                identity,
                key_index,
            } => {
                match self.identity_map.get(&identity) {
                    Some(member_id) => {
                        let member_id = member_id.clone();
                        self.emit(CallEvent::KeyImported {
                            member_id,
                            key_index,
                        });
                    }
                    None => {
                        // To-device keys regularly beat sticky memberships.
                        self.pending_keys.insert(identity, key_index);
                    }
                }
            }
        }
    }

    fn handle_connection_event(&mut self, connection_key: &str, event: ConnectionEvent) {
        match event {
            ConnectionEvent::RemoteJoined { identity } => {
                if !self.identity_map.contains_key(&identity) {
                    // Either their membership is still propagating (buffered
                    // media will flush when it lands) or the participant does
                    // not belong to this call. Diagnostics only.
                    log::debug!(
                        "remote participant {identity} on {connection_key} has no known membership"
                    );
                    self.emit(CallEvent::UnknownParticipant { identity });
                }
            }
            ConnectionEvent::RemoteLeft { .. } => {
                // Roster truth is the membership; a transport-level leave on
                // its own changes nothing (tracks get their own events).
            }
            ConnectionEvent::TrackAdded {
                identity,
                kind,
                track,
            } => match self.identity_map.get(&identity).cloned() {
                Some(member_id) => self.add_stream(&member_id, kind, track),
                None => {
                    log::debug!(
                        "buffering {kind:?} track of unknown identity {identity} on {connection_key}"
                    );
                    self.pending_tracks
                        .entry(identity)
                        .or_default()
                        .push((kind, track));
                }
            },
            ConnectionEvent::TrackRemoved { identity, kind } => {
                match self.identity_map.get(&identity).cloned() {
                    Some(member_id) => self.remove_stream(&member_id, kind),
                    None => {
                        if let Some(pending) = self.pending_tracks.get_mut(&identity) {
                            pending.retain(|(pending_kind, _)| *pending_kind != kind);
                        }
                    }
                }
            }
            ConnectionEvent::TrackMuted { identity, kind } => {
                self.set_muted(&identity, kind, true);
            }
            ConnectionEvent::TrackUnmuted { identity, kind } => {
                self.set_muted(&identity, kind, false);
            }
            ConnectionEvent::ActiveSpeakers { identities } => {
                let member_ids: Vec<String> = identities
                    .iter()
                    .filter_map(|identity| self.identity_map.get(identity).cloned())
                    .collect();
                self.emit(CallEvent::ActiveSpeakers { member_ids });
            }
            ConnectionEvent::Reconnecting => self.set_degraded(true),
            ConnectionEvent::Reconnected => self.set_degraded(false),
            ConnectionEvent::Closed { message } => {
                self.end(EndedReason::ConnectionClosed { message });
            }
        }
    }

    fn apply_snapshot(&mut self, snapshot: Vec<JoinedMembership>) {
        // Departures first, so a member_id rejoining in the same snapshot
        // (new membership, same key space) starts clean.
        let current_ids: Vec<String> = snapshot
            .iter()
            .map(|member| member.member_id.clone())
            .collect();
        let departed: Vec<Participant> = self
            .roster
            .iter()
            .filter(|participant| !current_ids.contains(&participant.member_id))
            .cloned()
            .collect();
        for participant in departed {
            self.remove_member(&participant.member_id);
        }

        for member in snapshot {
            if self.roster.iter().any(|p| p.member_id == member.member_id) {
                continue;
            }
            self.add_member(&member);
        }

        self.publish_roster();
    }

    fn add_member(&mut self, member: &JoinedMembership) {
        let reachable = member.transports.iter().any(|transport| {
            self.transports
                .iter()
                .any(|backend| backend.connection_key(transport).is_some())
        });
        let identity = self
            .transports
            .iter()
            .find_map(|backend| backend.remote_identity(member));

        self.roster.push(Participant {
            member_id: member.member_id.clone(),
            user_id: member.sender.clone(),
            device_id: member.origin.sender_device_id().map(str::to_owned),
            is_local: member.member_id == self.own_member_id,
            reachable,
            streams: Vec::new(),
        });
        self.emit(CallEvent::ParticipantJoined {
            member_id: member.member_id.clone(),
            user_id: member.sender.clone(),
        });

        if let Some(identity) = identity {
            self.identity_map
                .insert(identity.clone(), member.member_id.clone());

            // Media and keys that arrived before this membership.
            if let Some(pending) = self.pending_tracks.remove(&identity) {
                for (kind, track) in pending {
                    self.add_stream(&member.member_id.clone(), kind, track);
                }
            }
            if let Some(key_index) = self.pending_keys.remove(&identity) {
                self.emit(CallEvent::KeyImported {
                    member_id: member.member_id.clone(),
                    key_index,
                });
            }
        }
    }

    fn remove_member(&mut self, member_id: &str) {
        self.roster
            .retain(|participant| participant.member_id != member_id);
        self.identity_map.retain(|_, mapped| mapped != member_id);
        self.tracks
            .lock()
            .expect("track map mutex poisoned")
            .retain(|(track_member, _), _| track_member != member_id);
        self.emit(CallEvent::ParticipantLeft {
            member_id: member_id.to_owned(),
        });
    }

    fn add_stream(
        &mut self,
        member_id: &str,
        kind: MediaStreamKind,
        track: Arc<dyn RemoteTrackHandle>,
    ) {
        self.tracks
            .lock()
            .expect("track map mutex poisoned")
            .insert((member_id.to_owned(), kind), track);

        let Some(participant) = self.roster.iter_mut().find(|p| p.member_id == member_id) else {
            return;
        };
        if participant.streams.iter().any(|stream| stream.kind == kind) {
            // Same stream re-announced (e.g. events replayed on attach); the
            // handle above is refreshed, but it is not a new stream.
            return;
        }
        participant.streams.push(StreamState { kind, muted: false });
        self.emit(CallEvent::StreamStarted {
            member_id: member_id.to_owned(),
            kind,
        });
        self.publish_roster();
    }

    fn remove_stream(&mut self, member_id: &str, kind: MediaStreamKind) {
        self.tracks
            .lock()
            .expect("track map mutex poisoned")
            .remove(&(member_id.to_owned(), kind));

        let Some(participant) = self.roster.iter_mut().find(|p| p.member_id == member_id) else {
            return;
        };
        let had_stream = participant.streams.iter().any(|stream| stream.kind == kind);
        participant.streams.retain(|stream| stream.kind != kind);
        if had_stream {
            self.emit(CallEvent::StreamStopped {
                member_id: member_id.to_owned(),
                kind,
            });
            self.publish_roster();
        }
    }

    fn set_muted(&mut self, identity: &str, kind: MediaStreamKind, muted: bool) {
        let Some(member_id) = self.identity_map.get(identity).cloned() else {
            return;
        };
        let Some(stream) = self
            .roster
            .iter_mut()
            .find(|p| p.member_id == member_id)
            .and_then(|p| p.streams.iter_mut().find(|stream| stream.kind == kind))
        else {
            return;
        };
        if stream.muted == muted {
            return;
        }
        stream.muted = muted;
        let event = if muted {
            CallEvent::StreamMuted { member_id, kind }
        } else {
            CallEvent::StreamUnmuted { member_id, kind }
        };
        self.emit(event);
        self.publish_roster();
    }

    fn set_degraded(&mut self, degraded: bool) {
        if self.degraded == degraded {
            return;
        }
        self.degraded = degraded;
        self.emit(CallEvent::MediaConnectionState { degraded });
    }

    fn end(&mut self, reason: EndedReason) {
        if self.ended {
            return;
        }
        self.ended = true;
        self.emit(CallEvent::Ended { reason });
    }

    fn emit(&self, event: CallEvent) {
        // Err means no subscriber right now; the roster watch still updates.
        let _ = self.events_tx.send(event);
    }

    fn publish_roster(&self) {
        let _ = self.participants_tx.send(self.roster.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_core::stream::BoxStream;
    use futures_util::StreamExt;
    use matrix_rtc_core::{EventOrigin, LiveKitTransport, RtcTransport};
    use tokio::sync::mpsc::UnboundedSender;

    use super::*;
    use crate::frame::AudioFrame;
    use crate::transport::{
        ConnectionContext, MediaTransport, TransportConnection, TransportError,
    };

    struct FakeTransport;

    #[async_trait::async_trait]
    impl MediaTransport for FakeTransport {
        fn transport_type(&self) -> &'static str {
            "fake"
        }

        fn connection_key(&self, transport: &RtcTransport) -> Option<String> {
            match transport {
                RtcTransport::LiveKit(livekit) => Some(livekit.livekit_service_url.clone()),
                RtcTransport::Unsupported(_) => None,
            }
        }

        fn remote_identity(&self, member: &JoinedMembership) -> Option<String> {
            Some(format!("id-{}", member.member_id))
        }

        async fn connect(
            &self,
            _connection_key: &str,
            _ctx: &ConnectionContext,
        ) -> Result<
            (
                Box<dyn TransportConnection>,
                mpsc::UnboundedReceiver<ConnectionEvent>,
            ),
            TransportError,
        > {
            Err(TransportError::Unsupported(
                "connections are attached in this phase".into(),
            ))
        }
    }

    struct FakeTrack {
        kind: MediaStreamKind,
    }

    impl RemoteTrackHandle for FakeTrack {
        fn kind(&self) -> MediaStreamKind {
            self.kind
        }

        fn audio_frames(&self) -> Option<BoxStream<'static, AudioFrame>> {
            let frame = AudioFrame {
                data: vec![1, -1],
                sample_rate: 48_000,
                num_channels: 1,
                samples_per_channel: 2,
            };
            Some(futures_util::stream::iter([frame]).boxed())
        }
    }

    fn member(member_id: &str, user_id: &str) -> JoinedMembership {
        JoinedMembership {
            room_id: "!room:example.org".to_owned(),
            slot_id: "m.call#ROOM".to_owned(),
            sender: user_id.to_owned(),
            origin: EventOrigin::encrypted(Some("DEVICE".to_owned())),
            sticky_key: member_id.to_owned(),
            member_id: member_id.to_owned(),
            application: Some("m.call".to_owned()),
            transports: vec![RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://sfu.example.org".to_owned(),
            })],
            can_subscribe: vec!["livekit".to_owned()],
        }
    }

    struct Fixture {
        engine: CallEngine,
        events: broadcast::Receiver<CallEvent>,
        memberships: watch::Sender<Vec<JoinedMembership>>,
    }

    fn fixture() -> Fixture {
        let (memberships, memberships_rx) = watch::channel(Vec::new());
        let engine = CallEngine::new(
            EngineConfig {
                transports: vec![Arc::new(FakeTransport)],
                own_member_id: "own".to_owned(),
            },
            memberships_rx,
        );
        let events = engine.subscribe_events();
        Fixture {
            engine,
            events,
            memberships,
        }
    }

    async fn next_event(events: &mut broadcast::Receiver<CallEvent>) -> CallEvent {
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("timed out waiting for a call event")
            .expect("event channel closed")
    }

    fn attach(engine: &CallEngine) -> UnboundedSender<ConnectionEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        engine.attach_connection("https://sfu.example.org", rx);
        tx
    }

    #[tokio::test]
    async fn snapshot_builds_the_roster() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![
                member("own", "@alice:example.org"),
                member("bob", "@bob:example.org"),
            ])
            .unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::ParticipantJoined {
                member_id: "own".to_owned(),
                user_id: "@alice:example.org".to_owned(),
            }
        );
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::ParticipantJoined {
                member_id: "bob".to_owned(),
                user_id: "@bob:example.org".to_owned(),
            }
        );

        let participants = fx.engine.participants();
        assert_eq!(participants.len(), 2);
        let own = participants.iter().find(|p| p.member_id == "own").unwrap();
        assert!(own.is_local);
        let bob = participants.iter().find(|p| p.member_id == "bob").unwrap();
        assert!(!bob.is_local);
        assert!(bob.reachable);
        assert_eq!(bob.device_id.as_deref(), Some("DEVICE"));
    }

    #[tokio::test]
    async fn member_without_supported_transport_is_unreachable() {
        let mut fx = fixture();
        let mut eve = member("eve", "@eve:example.org");
        eve.transports = vec![RtcTransport::Unsupported(
            matrix_rtc_core::UnsupportedTransport {
                transport_type: "p2p".to_owned(),
                extra_fields: Default::default(),
            },
        )];
        fx.memberships.send(vec![eve]).unwrap();

        let _ = next_event(&mut fx.events).await;
        let participants = fx.engine.participants();
        assert!(!participants[0].reachable);
    }

    #[tokio::test]
    async fn track_maps_to_member_and_yields_frames() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined

        let connection = attach(&fx.engine);
        connection
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
                track: Arc::new(FakeTrack {
                    kind: MediaStreamKind::Microphone,
                }),
            })
            .unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamStarted {
                member_id: "bob".to_owned(),
                kind: MediaStreamKind::Microphone,
            }
        );

        let track = fx
            .engine
            .remote_track("bob", MediaStreamKind::Microphone)
            .expect("track handle should be available");
        let frames: Vec<AudioFrame> = track.audio_frames().unwrap().collect().await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].sample_rate, 48_000);

        // Mute, unmute, then removal.
        connection
            .send(ConnectionEvent::TrackMuted {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
            })
            .unwrap();
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamMuted {
                member_id: "bob".to_owned(),
                kind: MediaStreamKind::Microphone,
            }
        );
        connection
            .send(ConnectionEvent::TrackRemoved {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
            })
            .unwrap();
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamStopped {
                member_id: "bob".to_owned(),
                kind: MediaStreamKind::Microphone,
            }
        );
        assert!(
            fx.engine
                .remote_track("bob", MediaStreamKind::Microphone)
                .is_none()
        );
    }

    #[tokio::test]
    async fn early_media_waits_for_its_membership() {
        let mut fx = fixture();
        let connection = attach(&fx.engine);

        // Track and key arrive before the sticky membership.
        connection
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Camera,
                track: Arc::new(FakeTrack {
                    kind: MediaStreamKind::Camera,
                }),
            })
            .unwrap();
        fx.engine.notify_key_imported("id-bob", 3);

        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::ParticipantJoined {
                member_id: "bob".to_owned(),
                user_id: "@bob:example.org".to_owned(),
            }
        );
        // The buffered track and key both flush on the membership; their
        // relative order depends on when the connection forwarder ran.
        let flushed = [
            next_event(&mut fx.events).await,
            next_event(&mut fx.events).await,
        ];
        assert!(flushed.contains(&CallEvent::StreamStarted {
            member_id: "bob".to_owned(),
            kind: MediaStreamKind::Camera,
        }));
        assert!(flushed.contains(&CallEvent::KeyImported {
            member_id: "bob".to_owned(),
            key_index: 3,
        }));
    }

    #[tokio::test]
    async fn departure_cleans_up_roster_and_tracks() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await;

        let connection = attach(&fx.engine);
        connection
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
                track: Arc::new(FakeTrack {
                    kind: MediaStreamKind::Microphone,
                }),
            })
            .unwrap();
        let _ = next_event(&mut fx.events).await; // StreamStarted

        fx.memberships.send(Vec::new()).unwrap();
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::ParticipantLeft {
                member_id: "bob".to_owned(),
            }
        );
        assert!(fx.engine.participants().is_empty());
        assert!(
            fx.engine
                .remote_track("bob", MediaStreamKind::Microphone)
                .is_none()
        );
    }

    #[tokio::test]
    async fn closed_connection_ends_the_call() {
        let mut fx = fixture();
        let connection = attach(&fx.engine);
        connection
            .send(ConnectionEvent::Closed {
                message: "server shutting down".to_owned(),
            })
            .unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::Ended {
                reason: EndedReason::ConnectionClosed {
                    message: "server shutting down".to_owned(),
                },
            }
        );
    }

    #[tokio::test]
    async fn reconnect_cycle_reports_degraded_media() {
        let mut fx = fixture();
        let connection = attach(&fx.engine);
        connection.send(ConnectionEvent::Reconnecting).unwrap();
        connection.send(ConnectionEvent::Reconnecting).unwrap(); // deduplicated
        connection.send(ConnectionEvent::Reconnected).unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::MediaConnectionState { degraded: true }
        );
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::MediaConnectionState { degraded: false }
        );
    }
}
