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
//! # Connection pool (MSC4195 multi-SFU)
//!
//! Every member publishes media on the focus they announced in their
//! membership's `transports`; subscribing to them means connecting to *their*
//! focus. The engine groups members by [`MediaTransport::connection_key`] and
//! keeps exactly one connection per key:
//!
//! - a key appearing in the snapshot opens a connection via
//!   [`MediaTransport::connect`], retried with exponential backoff while
//!   members still need it;
//! - a key whose last member left is closed after a short idle grace (so
//!   membership flaps don't churn connections);
//! - a peer-focus connection dying tears down its members' streams and
//!   reconnects — only the *own* focus (the one we publish on, adopted via
//!   [`CallEngine::adopt_own_connection`]) ending ends the call.
//!
//! # Identity mapping
//!
//! Transports know participants by their own identities (LiveKit: the MSC4195
//! pseudonymous identity), and the engine reverse-maps those to `member_id`s
//! using [`MediaTransport::remote_identity`]. Transport participants that map
//! to no membership are never surfaced as participants (and their media could
//! not be decrypted anyway — keys are distributed per membership). Media that
//! arrives *before* its membership (SFU connects are often faster than sticky
//! event propagation) is buffered and flushed when the membership lands.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use matrix_rtc_core::{DiscardedKey, JoinedMembership, RaisedHand, ReceivedReaction};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::constraints::MediaConstraints;
use crate::event::{
    CallEvent, EndedReason, FrameEncryptionDiagnostic, FrameEncryptionState, SpeakingMember,
};
use crate::local::{LocalTrackHandle, PublishOptions};
use crate::participant::{MediaStreamKind, Participant, StreamState};
use crate::rt;
use crate::stats::ReceiveStats;
use crate::transport::{
    ConnectionContext, ConnectionEvent, MediaTransport, RemoteTrackHandle, TransportConnection,
    TransportError,
};

/// Capacity of the broadcast [`CallEvent`] channel. Subscribers that fall
/// further behind than this observe a `Lagged` error and miss events; they
/// should resynchronise from [`CallEngine::participants`].
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// How long a connection with no remaining members is kept before closing,
/// so a membership flap (sticky expiry glitch, quick rejoin) doesn't tear a
/// connection down just to rebuild it.
const IDLE_GRACE: Duration = Duration::from_secs(10);

/// First-retry delay after a failed connect; doubles per attempt.
const BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Retry delay ceiling.
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Constraint changes are coalesced for this long before being applied —
/// scroll-driven visibility churn is the hot path, and only the final state
/// matters to the transport.
const CONSTRAINTS_DEBOUNCE: Duration = Duration::from_millis(150);

fn backoff_delay(attempt: u32) -> Duration {
    BACKOFF_BASE
        .saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1)))
        .min(BACKOFF_MAX)
}

/// Subscribed remote tracks, keyed by member and stream kind. Shared between
/// the actor (writes) and [`CallEngine::remote_track`] (reads).
type TrackMap = Arc<Mutex<HashMap<(String, MediaStreamKind), Arc<dyn RemoteTrackHandle>>>>;

/// Tracks whose transport identity has no membership yet, buffered per
/// identity until the membership lands.
type PendingTracks = HashMap<String, Vec<(MediaStreamKind, Arc<dyn RemoteTrackHandle>)>>;

/// What [`MediaTransport::connect`] resolves to, as carried in the mailbox.
type ConnectOutcome = Result<
    (
        Box<dyn TransportConnection>,
        mpsc::UnboundedReceiver<ConnectionEvent>,
    ),
    TransportError,
>;

/// Static configuration of a [`CallEngine`].
pub struct EngineConfig {
    /// Transport backends, in descending order of preference.
    pub transports: Vec<Arc<dyn MediaTransport>>,
    /// `member.id` of our own membership, used to flag the local participant.
    pub own_member_id: String,
    /// Call-scoped context passed to [`MediaTransport::connect`].
    pub ctx: ConnectionContext,
    /// Connection key of the focus we publish on. The engine never opens this
    /// one itself — the caller establishes it (so join can fail fast) and
    /// hands it over via [`CallEngine::adopt_own_connection`].
    pub own_connection_key: Option<String>,
    /// The core's raised-hand snapshots
    /// (`RtcSession::subscribe_raised_hands`), merged onto the roster as
    /// [`Participant::hand_raised_at_ms`] and reported as
    /// [`CallEvent::HandRaised`] / [`CallEvent::HandLowered`]. `None` leaves
    /// every hand down.
    pub raised_hands: Option<watch::Receiver<Vec<RaisedHand>>>,
    /// The core's emoji reactions (`RtcSession::subscribe_reactions`),
    /// forwarded as [`CallEvent::Reaction`] for members on the roster. `None`
    /// reports no reactions.
    pub reactions: Option<broadcast::Receiver<ReceivedReaction>>,
}

/// Everything the engine can be told from outside its actor task.
enum ActorMessage {
    /// Take ownership of the caller-established own-focus connection.
    AdoptConnection {
        connection: Box<dyn TransportConnection>,
        events: mpsc::UnboundedReceiver<ConnectionEvent>,
    },
    /// An event from a pooled connection. `generation` guards against events
    /// of a replaced connection being applied to its successor.
    Connection {
        connection_key: String,
        generation: u64,
        event: ConnectionEvent,
    },
    /// A pooled connection's event stream ended.
    ConnectionEnded {
        connection_key: String,
        generation: u64,
    },
    /// A [`MediaTransport::connect`] attempt resolved.
    ConnectFinished {
        connection_key: String,
        attempt: u32,
        result: ConnectOutcome,
    },
    /// A backoff timer elapsed; try connecting again.
    RetryConnect {
        connection_key: String,
        attempt: u32,
    },
    /// An idle grace timer elapsed; close the connection if still unneeded.
    CloseIfIdle {
        connection_key: String,
        idle_generation: u64,
    },
    /// A media decryption key was imported for a transport identity.
    KeyImported {
        identity: String,
        key_index: u8,
    },
    KeyDiscarded(DiscardedKey),
    /// A local publication went up; the roster must show it and peers were
    /// already told by the transport.
    LocalPublished {
        kind: MediaStreamKind,
        track: Arc<dyn LocalTrackHandle>,
    },
    /// A local retraction resolved at the transport; the roster must drop it.
    /// `track` identifies which publication was retracted, so a same-kind
    /// publish that landed while the retraction was in flight is not the one
    /// removed.
    LocalUnpublished {
        kind: MediaStreamKind,
        track: Arc<dyn LocalTrackHandle>,
    },
    SetLocalMuted {
        kind: MediaStreamKind,
        muted: bool,
        respond: oneshot::Sender<Result<(), TransportError>>,
    },
    /// Retract one of our own publications.
    Unpublish {
        kind: MediaStreamKind,
        respond: oneshot::Sender<Result<(), TransportError>>,
    },
    /// Publish a local track on the own-focus connection.
    Publish {
        options: PublishOptions,
        respond: oneshot::Sender<Result<Arc<dyn LocalTrackHandle>, TransportError>>,
    },
    /// Store new constraints for one stream and arm the debounce timer.
    SetConstraints {
        member_id: String,
        kind: MediaStreamKind,
        constraints: MediaConstraints,
    },
    /// A constraints debounce timer elapsed; apply if still current.
    ApplyConstraints {
        member_id: String,
        kind: MediaStreamKind,
        generation: u64,
    },
    /// Close every pooled connection and stop.
    Shutdown {
        ack: oneshot::Sender<()>,
    },
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

    /// See [`CallEngine::notify_key_discarded`]. No-op once the engine is gone.
    pub fn notify_key_discarded(&self, report: DiscardedKey) {
        let _ = self.messages.send(ActorMessage::KeyDiscarded(report));
    }
}

/// One MatrixRTC call as a flat set of participants with media streams.
///
/// Created per joined slot; dropping the engine stops its background task
/// (pooled connections then disconnect as they are dropped — call
/// [`CallEngine::shutdown`] first for an orderly close).
pub struct CallEngine {
    messages: mpsc::UnboundedSender<ActorMessage>,
    events_tx: broadcast::Sender<CallEvent>,
    participants_rx: watch::Receiver<Vec<Participant>>,
    tracks: TrackMap,
    task: rt::TaskHandle,
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
    /// Must be called within a tokio runtime off `wasm32` (spawns the actor
    /// task, which is `Send` there — no `LocalSet` needed). On `wasm32` the
    /// actor runs on the JS microtask queue via `spawn_local`.
    pub fn new(
        config: EngineConfig,
        memberships: watch::Receiver<Vec<JoinedMembership>>,
    ) -> CallEngine {
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let (participants_tx, participants_rx) = watch::channel(Vec::new());
        // On wasm32 the map is `!Send` (track handles hold JS values), but it
        // is still shared — engine and actor — so `Arc` stays, uncontended.
        #[cfg_attr(target_arch = "wasm32", expect(clippy::arc_with_non_send_sync))]
        let tracks: TrackMap = Arc::new(Mutex::new(HashMap::new()));

        let actor = Actor {
            transports: config.transports,
            own_member_id: config.own_member_id,
            ctx: config.ctx,
            own_connection_key: config.own_connection_key,
            events_tx: events_tx.clone(),
            participants_tx,
            tracks: tracks.clone(),
            messages_tx: messages_tx.clone(),
            roster: Vec::new(),
            members_snapshot: Vec::new(),
            identity_map: HashMap::new(),
            member_identities: HashMap::new(),
            constraints: HashMap::new(),
            pending_tracks: HashMap::new(),
            local_tracks: HashMap::new(),
            encryption_states: HashMap::new(),
            installed_keys: HashMap::new(),
            pending_keys: HashMap::new(),
            raised_hands: HashMap::new(),
            pool: HashMap::new(),
            connection_generation: 0,
            degraded_keys: HashSet::new(),
            ended: false,
        };
        let task = rt::spawn(actor.run(
            memberships,
            messages_rx,
            config.raised_hands,
            config.reactions,
        ));

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

    /// Hand the caller-established own-focus connection to the engine, which
    /// consumes its events and owns its lifetime from here (except closing —
    /// the caller keeps that, so a clean leave can report the close result).
    pub fn adopt_own_connection(
        &self,
        connection: Box<dyn TransportConnection>,
        events: mpsc::UnboundedReceiver<ConnectionEvent>,
    ) {
        let _ = self
            .messages
            .send(ActorMessage::AdoptConnection { connection, events });
    }

    /// Report that a media decryption key for `identity` was imported, so the
    /// engine can surface [`CallEvent::KeyImported`] against the right member.
    pub fn notify_key_imported(&self, identity: impl Into<String>, key_index: u8) {
        let _ = self.messages.send(ActorMessage::KeyImported {
            identity: identity.into(),
            key_index,
        });
    }

    /// Report that a media key was refused, so the engine can surface
    /// [`CallEvent::KeyDiscarded`] with the reason.
    ///
    /// Keyed by `member_id` rather than a transport identity: the core refuses a
    /// key before any identity derivation applies to it, and a refused key may
    /// well name a member the transport has never seen.
    pub fn notify_key_discarded(&self, report: DiscardedKey) {
        let _ = self.messages.send(ActorMessage::KeyDiscarded(report));
    }

    /// Publish a local track on the own-focus connection (see
    /// [`PublishOptions`]); push captured frames into the returned handle.
    /// Fails while that connection is not up.
    pub async fn publish(
        &self,
        options: PublishOptions,
    ) -> Result<Arc<dyn LocalTrackHandle>, TransportError> {
        let (respond, response) = oneshot::channel();
        self.messages
            .send(ActorMessage::Publish { options, respond })
            .map_err(|_| TransportError::Closed("the engine is gone".into()))?;
        response
            .await
            .map_err(|_| TransportError::Closed("the engine is gone".into()))?
    }

    /// Mute or unmute one of our own publications.
    ///
    /// Peers are told, so their UI can show it — which is the difference
    /// between a muted sender and one that has simply stopped sending. The
    /// roster and the event stream are updated too, so a host can render its own
    /// state from the same source it renders everyone else's rather than
    /// shadowing it separately.
    ///
    /// Errors if nothing of that kind is published.
    pub async fn set_local_muted(
        &self,
        kind: MediaStreamKind,
        muted: bool,
    ) -> Result<(), TransportError> {
        let (respond, response) = oneshot::channel();
        self.messages
            .send(ActorMessage::SetLocalMuted {
                kind,
                muted,
                respond,
            })
            .map_err(|_| TransportError::Closed("the engine is gone".into()))?;
        response
            .await
            .map_err(|_| TransportError::Closed("the engine is gone".into()))?
    }

    /// Retract one of our own publications at the transport.
    ///
    /// Where a mute keeps the publication up so peers can tell a deliberate
    /// off from a wedged sender, unpublish removes it: peers drop the stream
    /// (their transport reports it removed) instead of rendering an empty
    /// tile — what a stopped screen share needs, since a screen has no "off"
    /// state worth showing. On success the own roster entry drops the stream
    /// and [`CallEvent::StreamStopped`] is emitted, and the handle obtained
    /// from [`CallEngine::publish`] is dead: `capture_*` calls on it error.
    ///
    /// On failure the publication stays on the roster and stays usable
    /// (mute, capture, retry this call): the retraction can fail while the
    /// room is live, and dropping local state then would leave a still-live
    /// publication nothing can retract. Errors if nothing of that kind is
    /// published.
    pub async fn unpublish(&self, kind: MediaStreamKind) -> Result<(), TransportError> {
        let (respond, response) = oneshot::channel();
        self.messages
            .send(ActorMessage::Unpublish { kind, respond })
            .map_err(|_| TransportError::Closed("the engine is gone".into()))?;
        response
            .await
            .map_err(|_| TransportError::Closed("the engine is gone".into()))?
    }

    /// Set the subscription constraints for one stream of one participant.
    ///
    /// Applied after a short debounce (rapid changes coalesce, e.g. while
    /// scrolling a participant grid) and re-applied automatically whenever
    /// the stream (re)appears. Constraints are keyed by membership: they die
    /// when the member leaves.
    pub fn set_constraints(
        &self,
        member_id: impl Into<String>,
        kind: MediaStreamKind,
        constraints: MediaConstraints,
    ) {
        let _ = self.messages.send(ActorMessage::SetConstraints {
            member_id: member_id.into(),
            kind,
            constraints,
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

    /// Receive-side RTP counters for a participant's stream.
    ///
    /// `None` while no such stream is subscribed, or when the transport
    /// reports no counters. Because the receive path emits frames whether or
    /// not RTP arrives, this is how a caller distinguishes "nothing is
    /// arriving" from "arriving but not decoding" — sample twice and compare,
    /// the fields are cumulative totals. See [`ReceiveStats`].
    pub async fn receive_stats(
        &self,
        member_id: &str,
        kind: MediaStreamKind,
    ) -> Option<ReceiveStats> {
        // `remote_track` hands back an owned `Arc`, so the track-map lock is
        // released before the transport's (potentially slow) stats round trip.
        let track = self.remote_track(member_id, kind)?;
        track.receive_stats().await
    }

    /// Emit [`CallEvent::Ended`] and close every pooled peer-focus connection.
    /// The adopted own-focus connection is not closed here — its owner (the
    /// caller of [`CallEngine::adopt_own_connection`]) closes it and gets the
    /// result. Resolves once the engine has processed the shutdown.
    pub async fn shutdown(&self) {
        let (ack, done) = oneshot::channel();
        if self.messages.send(ActorMessage::Shutdown { ack }).is_ok() {
            let _ = done.await;
        }
    }
}

/// A pooled connection to one focus.
struct ManagedConnection {
    /// Backend that opened (and re-opens) this connection. `None` for the
    /// adopted own-focus connection, which the engine never re-opens.
    backend: Option<Arc<dyn MediaTransport>>,
    /// Members whose media lives on this focus, per the latest snapshot.
    members: HashSet<String>,
    /// Whether this is the focus we publish on.
    is_own: bool,
    state: ConnState,
    /// Bumped every time a live connection is installed; events carrying an
    /// older generation belong to a replaced connection and are dropped.
    generation: u64,
    /// Bumped whenever the member set changes; invalidates pending idle-close
    /// timers.
    idle_generation: u64,
}

enum ConnState {
    Connecting {
        attempt: u32,
    },
    Up {
        // Arc, not Box: publish/apply-constraints calls run in spawned tasks
        // that need shared ownership while the entry stays in the pool.
        connection: Arc<dyn TransportConnection>,
    },
    Backoff {
        attempt: u32,
    },
}

struct Actor {
    transports: Vec<Arc<dyn MediaTransport>>,
    own_member_id: String,
    ctx: ConnectionContext,
    own_connection_key: Option<String>,
    events_tx: broadcast::Sender<CallEvent>,
    participants_tx: watch::Sender<Vec<Participant>>,
    tracks: TrackMap,
    /// Handed to connection forwarders and timers so everything funnels into
    /// the same mailbox.
    messages_tx: mpsc::UnboundedSender<ActorMessage>,
    roster: Vec<Participant>,
    /// The latest membership snapshot, kept for pool reconciliation.
    members_snapshot: Vec<JoinedMembership>,
    /// Transport identity → `member_id`.
    identity_map: HashMap<String, String>,
    /// `member_id` → transport identity (the reverse of `identity_map`),
    /// for pushing constraints at a member's connection.
    member_identities: HashMap<String, String>,
    /// Latest constraints per stream, with a generation counter that
    /// invalidates superseded debounce timers.
    constraints: HashMap<(String, MediaStreamKind), (MediaConstraints, u64)>,
    /// Media that arrived before its membership, flushed when it lands.
    pending_tracks: PendingTracks,
    /// Last frame-encryption state reported per member.
    ///
    /// Only so the log line can name the transition. A bare "is MissingKey"
    /// cannot be read: a momentary drop during a rotation and a permanent failure
    /// produce the same line, and only the previous state separates them.
    encryption_states: HashMap<String, FrameEncryptionState>,
    /// Our own live publications, so a mute can reach the transport and the
    /// roster can be corrected in one place.
    local_tracks: HashMap<MediaStreamKind, Arc<dyn LocalTrackHandle>>,
    /// Key indices installed per member, in the order they were imported.
    ///
    /// Kept so a frame-encryption failure can say whether *any* key reached this
    /// participant. Without it a `MissingKey` is unattributable from outside: a
    /// key that never arrived and a rotation still in flight look identical.
    installed_keys: HashMap<String, Vec<u8>>,
    /// Imported key indices awaiting their membership, in arrival order.
    ///
    /// A `Vec`, not a single index: at join a member can be handed more than one
    /// index before their sticky membership lands (their first key plus a
    /// rotation, say), and keeping only the latest silently dropped the others —
    /// so the host saw one `KeyImported` where the core had imported several, and
    /// could not tell which index the transport actually held.
    pending_keys: HashMap<String, Vec<u8>>,
    /// `member_id` → when they raised their hand, as the core last reported.
    /// Kept for members not yet on the roster too: the core learns of a hand
    /// and of the membership it belongs to through different channels, and a
    /// hand that arrives first must not be lost.
    raised_hands: HashMap<String, u64>,
    pool: HashMap<String, ManagedConnection>,
    connection_generation: u64,
    /// Connections currently impaired (reconnecting or failing to connect);
    /// media is reported degraded while this is non-empty.
    degraded_keys: HashSet<String>,
    ended: bool,
}

/// Waits for the raised-hands watch to change; never resolves when there is
/// no such channel, so the `select!` branch simply never fires.
async fn hands_changed(
    hands: &mut Option<watch::Receiver<Vec<RaisedHand>>>,
) -> Result<(), watch::error::RecvError> {
    match hands {
        Some(hands) => hands.changed().await,
        None => std::future::pending().await,
    }
}

/// The next reaction, skipping over any the receiver lagged past; `None`
/// once the channel is closed. Never resolves when there is no channel.
async fn next_reaction(
    reactions: &mut Option<broadcast::Receiver<ReceivedReaction>>,
) -> Option<ReceivedReaction> {
    let Some(reactions) = reactions else {
        return std::future::pending().await;
    };
    loop {
        match reactions.recv().await {
            Ok(reaction) => return Some(reaction),
            // A reaction is a three-second visual; the ones we fell behind on
            // are not worth showing late.
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                log::debug!("skipped {skipped} reaction(s) the engine fell behind on");
            }
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}

impl Actor {
    async fn run(
        mut self,
        mut memberships: watch::Receiver<Vec<JoinedMembership>>,
        mut messages: mpsc::UnboundedReceiver<ActorMessage>,
        mut raised_hands: Option<watch::Receiver<Vec<RaisedHand>>>,
        mut reactions: Option<broadcast::Receiver<ReceivedReaction>>,
    ) {
        // The watch channels always have a value; start from it.
        let initial = memberships.borrow_and_update().clone();
        self.apply_snapshot(initial);
        if let Some(hands) = raised_hands.as_mut() {
            let initial = hands.borrow_and_update().clone();
            self.apply_raised_hands(initial);
        }

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
                changed = hands_changed(&mut raised_hands) => match changed {
                    Ok(()) => {
                        let snapshot = raised_hands
                            .as_mut()
                            .map(|hands| hands.borrow_and_update().clone())
                            .unwrap_or_default();
                        self.apply_raised_hands(snapshot);
                    }
                    Err(_) => raised_hands = None,
                },
                reaction = next_reaction(&mut reactions) => match reaction {
                    Some(reaction) => self.apply_reaction(reaction),
                    None => reactions = None,
                },
                message = messages.recv() => match message {
                    Some(ActorMessage::Shutdown { ack }) => {
                        self.end(EndedReason::Left);
                        let _ = ack.send(());
                        break;
                    }
                    Some(message) => self.handle_message(message),
                    // All senders gone: the engine handle was dropped.
                    None => break,
                },
            }
        }
    }

    fn handle_message(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::AdoptConnection { connection, events } => {
                self.adopt_connection(connection, events);
            }
            ActorMessage::Connection {
                connection_key,
                generation,
                event,
            } => {
                let current = self.pool.get(&connection_key).map(|entry| entry.generation);
                if current != Some(generation) {
                    log::trace!("dropping event of replaced connection {connection_key}");
                    return;
                }
                self.handle_connection_event(&connection_key, event);
            }
            ActorMessage::ConnectionEnded {
                connection_key,
                generation,
            } => {
                self.connection_down(&connection_key, generation, "connection event stream ended");
            }
            ActorMessage::ConnectFinished {
                connection_key,
                attempt,
                result,
            } => {
                self.connect_finished(&connection_key, attempt, result);
            }
            ActorMessage::RetryConnect {
                connection_key,
                attempt,
            } => {
                let Some(entry) = self.pool.get_mut(&connection_key) else {
                    return;
                };
                if !matches!(entry.state, ConnState::Backoff { attempt: a } if a == attempt) {
                    return;
                }
                if entry.members.is_empty() && !entry.is_own {
                    self.pool.remove(&connection_key);
                    self.clear_degraded(&connection_key);
                    return;
                }
                self.start_connect(&connection_key, attempt);
            }
            ActorMessage::CloseIfIdle {
                connection_key,
                idle_generation,
            } => {
                let Some(entry) = self.pool.get(&connection_key) else {
                    return;
                };
                if entry.idle_generation != idle_generation
                    || !entry.members.is_empty()
                    || entry.is_own
                {
                    return;
                }
                log::debug!("closing idle connection {connection_key}");
                if let Some(entry) = self.pool.remove(&connection_key)
                    && let ConnState::Up { connection } = entry.state
                {
                    rt::spawn(async move {
                        if let Err(error) = connection.close().await {
                            log::debug!("closing idle connection failed: {error}");
                        }
                    });
                }
                self.clear_degraded(&connection_key);
            }
            ActorMessage::KeyImported {
                identity,
                key_index,
            } => match self.identity_map.get(&identity) {
                Some(member_id) => {
                    let member_id = member_id.clone();
                    self.record_installed_key(&member_id, key_index);
                    self.emit(CallEvent::KeyImported {
                        member_id,
                        key_index,
                    });
                }
                None => {
                    // To-device keys regularly beat sticky memberships.
                    let pending = self.pending_keys.entry(identity).or_default();
                    if !pending.contains(&key_index) {
                        pending.push(key_index);
                    }
                }
            },
            ActorMessage::KeyDiscarded(report) => {
                log::warn!(
                    "media key index {:?} for member {} refused: {} (from {}/{})",
                    report.key_index,
                    report.member_id,
                    report.reason,
                    report.sender_user_id.as_deref().unwrap_or("<unattributed>"),
                    report.sender_device_id.as_deref().unwrap_or("<unknown>"),
                );
                self.emit(CallEvent::KeyDiscarded {
                    member_id: report.member_id,
                    key_index: report.key_index,
                    sender_user_id: report.sender_user_id,
                    sender_device_id: report.sender_device_id,
                    reason: report.reason,
                });
            }
            ActorMessage::Publish { options, respond } => {
                // Local tracks always go to the focus we announced in our
                // membership — that is where peers subscribe to us.
                let connection = self
                    .pool
                    .values()
                    .find(|entry| entry.is_own)
                    .and_then(|entry| match &entry.state {
                        ConnState::Up { connection } => Some(connection.clone()),
                        _ => None,
                    });
                match connection {
                    Some(connection) => {
                        // The publish itself is awaited off-actor so a slow SFU
                        // does not stall every other message; the actor learns
                        // the outcome through `LocalPublished`, which is what
                        // puts the stream on our own roster entry. Without that
                        // a host sees no event for its own microphone and has to
                        // shadow the state to render itself truthfully.
                        let kind = options.kind;
                        let messages = self.messages_tx.clone();
                        rt::spawn(async move {
                            let outcome = connection.publish(options).await;
                            if let Ok(track) = &outcome {
                                let _ = messages.send(ActorMessage::LocalPublished {
                                    kind,
                                    track: Arc::clone(track),
                                });
                            }
                            let _ = respond.send(outcome);
                        });
                    }
                    None => {
                        let _ = respond.send(Err(TransportError::Closed(
                            "the own-focus connection is not up".into(),
                        )));
                    }
                }
            }
            ActorMessage::LocalPublished { kind, track } => {
                self.local_tracks.insert(kind, track);
                let own_member_id = self.own_member_id.clone();
                self.add_local_stream(&own_member_id, kind);
            }
            ActorMessage::SetLocalMuted {
                kind,
                muted,
                respond,
            } => {
                let outcome = match self.local_tracks.get(&kind) {
                    Some(track) => track.set_muted(muted),
                    None => Err(TransportError::Unsupported(format!(
                        "nothing of kind {kind:?} is published locally"
                    ))),
                };
                if outcome.is_ok() {
                    let own_member_id = self.own_member_id.clone();
                    self.set_member_muted(&own_member_id, kind, muted);
                }
                let _ = respond.send(outcome);
            }
            ActorMessage::Unpublish { kind, respond } => {
                let Some(track) = self.local_tracks.get(&kind) else {
                    let _ = respond.send(Err(TransportError::Unsupported(format!(
                        "nothing of kind {kind:?} is published locally"
                    ))));
                    return;
                };
                // Awaited off-actor like the publish itself, and the actor
                // learns the outcome through `LocalUnpublished`: a retraction
                // can fail while the room is live (the room-closed case comes
                // back as success from the transport), and clearing local
                // state on a failed attempt would leave a still-live
                // publication that nothing can mute or retract anymore.
                let track = Arc::clone(track);
                let messages = self.messages_tx.clone();
                rt::spawn(async move {
                    let outcome = track.unpublish().await;
                    if outcome.is_ok() {
                        let _ = messages.send(ActorMessage::LocalUnpublished { kind, track });
                    }
                    let _ = respond.send(outcome);
                });
            }
            ActorMessage::LocalUnpublished { kind, track } => {
                if self
                    .local_tracks
                    .get(&kind)
                    .is_some_and(|current| Arc::ptr_eq(current, &track))
                {
                    self.local_tracks.remove(&kind);
                    let own_member_id = self.own_member_id.clone();
                    self.remove_stream(&own_member_id, kind);
                }
            }
            ActorMessage::SetConstraints {
                member_id,
                kind,
                constraints,
            } => {
                let entry = self
                    .constraints
                    .entry((member_id.clone(), kind))
                    .or_insert((constraints, 0));
                entry.0 = constraints;
                entry.1 += 1;
                let generation = entry.1;
                let messages = self.messages_tx.clone();
                rt::spawn(async move {
                    rt::sleep(CONSTRAINTS_DEBOUNCE).await;
                    let _ = messages.send(ActorMessage::ApplyConstraints {
                        member_id,
                        kind,
                        generation,
                    });
                });
            }
            ActorMessage::ApplyConstraints {
                member_id,
                kind,
                generation,
            } => {
                // Only the newest timer applies; older ones were superseded.
                if self
                    .constraints
                    .get(&(member_id.clone(), kind))
                    .is_some_and(|(_, current)| *current == generation)
                {
                    self.apply_constraints_now(&member_id, kind);
                }
            }
            // Handled in the run loop (it must break).
            ActorMessage::Shutdown { ack } => {
                let _ = ack.send(());
            }
        }
    }

    /// Push the resolved constraints for one stream to the connection its
    /// member lives on. No-op while the member, its identity, or its
    /// connection is missing — [`Actor::add_stream`] and reconnects re-apply.
    fn apply_constraints_now(&mut self, member_id: &str, kind: MediaStreamKind) {
        let Some((constraints, _)) = self.constraints.get(&(member_id.to_owned(), kind)) else {
            return;
        };
        let resolved = constraints.resolve(kind);
        let Some(identity) = self.member_identities.get(member_id).cloned() else {
            return;
        };
        let connection = self
            .pool
            .values()
            .find(|entry| entry.members.contains(member_id))
            .and_then(|entry| match &entry.state {
                ConnState::Up { connection } => Some(connection.clone()),
                _ => None,
            });
        let Some(connection) = connection else {
            return;
        };
        let kind_copy = kind;
        rt::spawn(async move {
            if let Err(error) = connection
                .apply_constraints(&identity, kind_copy, resolved)
                .await
            {
                log::warn!("applying constraints for {identity} ({kind_copy:?}) failed: {error}");
            }
        });
    }

    /// Re-apply every stored constraint for members living on `key` (used
    /// after a transport-level reconnect: subscription settings are
    /// server-side state of the connection).
    fn reapply_connection_constraints(&mut self, key: &str) {
        let Some(entry) = self.pool.get(key) else {
            return;
        };
        let members = entry.members.clone();
        let keys: Vec<(String, MediaStreamKind)> = self
            .constraints
            .keys()
            .filter(|(member_id, _)| members.contains(member_id))
            .cloned()
            .collect();
        for (member_id, kind) in keys {
            self.apply_constraints_now(&member_id, kind);
        }
    }

    // ---- connection pool ----------------------------------------------

    fn adopt_connection(
        &mut self,
        connection: Box<dyn TransportConnection>,
        events: mpsc::UnboundedReceiver<ConnectionEvent>,
    ) {
        let key = connection.connection_key().to_owned();
        self.connection_generation += 1;
        let generation = self.connection_generation;
        let members = self.members_on_key(&key);
        self.pool.insert(
            key.clone(),
            ManagedConnection {
                backend: None,
                members,
                is_own: true,
                state: ConnState::Up {
                    connection: Arc::from(connection),
                },
                generation,
                idle_generation: 0,
            },
        );
        self.spawn_forwarder(key, generation, events);
    }

    /// Sync the pool with the latest snapshot: update per-connection member
    /// sets, schedule idle closes, open connections for new focus groups.
    fn reconcile_pool(&mut self) {
        let mut desired: HashMap<String, (Arc<dyn MediaTransport>, HashSet<String>)> =
            HashMap::new();
        for member in &self.members_snapshot {
            if let Some((backend, key)) = select_transport(&self.transports, member) {
                desired
                    .entry(key)
                    .or_insert_with(|| (backend, HashSet::new()))
                    .1
                    .insert(member.member_id.clone());
            }
        }

        log::debug!(
            "focus grouping: {}",
            desired
                .iter()
                .map(|(key, (_, members))| format!("{key} -> {} member(s)", members.len()))
                .collect::<Vec<_>>()
                .join(", "),
        );

        let keys: Vec<String> = self.pool.keys().cloned().collect();
        for key in keys {
            let members = desired.remove(&key).map(|(_, m)| m).unwrap_or_default();
            let entry = self.pool.get_mut(&key).expect("pool key just listed");
            entry.members = members;
            // Any member-set change invalidates pending idle timers; an empty
            // set (re)arms one.
            entry.idle_generation += 1;
            if entry.members.is_empty() && !entry.is_own {
                log::debug!("peer focus {key} has no members left; arming the idle timer");
                let idle_generation = entry.idle_generation;
                let messages = self.messages_tx.clone();
                let key = key.clone();
                rt::spawn(async move {
                    rt::sleep(IDLE_GRACE).await;
                    let _ = messages.send(ActorMessage::CloseIfIdle {
                        connection_key: key,
                        idle_generation,
                    });
                });
            }
        }

        for (key, (backend, members)) in desired {
            // The own focus is established by the caller and adopted; never
            // race it with an engine-initiated connect.
            if self.own_connection_key.as_deref() == Some(key.as_str()) {
                continue;
            }
            log::info!(
                "connecting to peer focus {key} for {} member(s)",
                members.len(),
            );
            self.pool.insert(
                key.clone(),
                ManagedConnection {
                    backend: Some(backend),
                    members,
                    is_own: false,
                    state: ConnState::Connecting { attempt: 0 },
                    generation: 0,
                    idle_generation: 0,
                },
            );
            self.start_connect(&key, 0);
        }
    }

    /// Spawn a connect attempt for an existing pool entry.
    fn start_connect(&mut self, key: &str, attempt: u32) {
        let Some(entry) = self.pool.get_mut(key) else {
            return;
        };
        let Some(backend) = entry.backend.clone() else {
            return;
        };
        entry.state = ConnState::Connecting { attempt };

        let ctx = self.ctx.clone();
        let messages = self.messages_tx.clone();
        let key = key.to_owned();
        rt::spawn(async move {
            let result = backend.connect(&key, &ctx).await;
            let _ = messages.send(ActorMessage::ConnectFinished {
                connection_key: key,
                attempt,
                result,
            });
        });
    }

    fn connect_finished(&mut self, key: &str, attempt: u32, result: ConnectOutcome) {
        let close_stray = |result: ConnectOutcome| {
            if let Ok((connection, _events)) = result {
                rt::spawn(async move {
                    let _ = connection.close().await;
                });
            }
        };

        let (still_needed, matches_attempt) = match self.pool.get(key) {
            // The group emptied and was removed while connecting.
            None => (false, false),
            Some(entry) => (
                !entry.members.is_empty() || entry.is_own,
                matches!(entry.state, ConnState::Connecting { attempt: a } if a == attempt),
            ),
        };
        if !matches_attempt {
            close_stray(result);
            return;
        }
        if !still_needed {
            self.pool.remove(key);
            self.clear_degraded(key);
            close_stray(result);
            return;
        }

        match result {
            Ok((connection, events)) => {
                self.connection_generation += 1;
                let generation = self.connection_generation;
                let entry = self.pool.get_mut(key).expect("entry checked above");
                entry.generation = generation;
                entry.state = ConnState::Up {
                    connection: Arc::from(connection),
                };
                log::info!("media connection up: {key}");
                self.spawn_forwarder(key.to_owned(), generation, events);
                self.clear_degraded(key);
            }
            Err(error) => {
                log::warn!("connecting to {key} failed (attempt {attempt}): {error}");
                self.mark_degraded(key);
                let next = attempt + 1;
                let entry = self.pool.get_mut(key).expect("entry checked above");
                entry.state = ConnState::Backoff { attempt: next };
                let delay = backoff_delay(next);
                let messages = self.messages_tx.clone();
                let key = key.to_owned();
                rt::spawn(async move {
                    rt::sleep(delay).await;
                    let _ = messages.send(ActorMessage::RetryConnect {
                        connection_key: key,
                        attempt: next,
                    });
                });
            }
        }
    }

    /// A live connection is gone (transport `Closed` event or its event
    /// stream ending). Own focus ⇒ the call is over; peer focus ⇒ tear down
    /// its members' streams and reconnect while still needed.
    fn connection_down(&mut self, key: &str, generation: u64, message: &str) {
        let Some(entry) = self.pool.get(key) else {
            return;
        };
        if entry.generation != generation || !matches!(entry.state, ConnState::Up { .. }) {
            return;
        }

        if entry.is_own {
            log::warn!("own-focus connection {key} is gone: {message}");
            self.pool.remove(key);
            self.end(EndedReason::ConnectionClosed {
                message: message.to_owned(),
            });
            return;
        }

        log::warn!("peer-focus connection {key} is gone ({message}); reconnecting");
        let members: Vec<String> = entry.members.iter().cloned().collect();
        for member_id in members {
            self.remove_all_streams(&member_id);
        }
        self.mark_degraded(key);

        let entry = self.pool.get_mut(key).expect("entry checked above");
        if entry.members.is_empty() {
            self.pool.remove(key);
            self.clear_degraded(key);
            return;
        }
        entry.state = ConnState::Backoff { attempt: 0 };
        let messages = self.messages_tx.clone();
        let key = key.to_owned();
        rt::spawn(async move {
            rt::sleep(backoff_delay(0)).await;
            let _ = messages.send(ActorMessage::RetryConnect {
                connection_key: key,
                attempt: 0,
            });
        });
    }

    fn spawn_forwarder(
        &self,
        connection_key: String,
        generation: u64,
        mut events: mpsc::UnboundedReceiver<ConnectionEvent>,
    ) {
        let forward = self.messages_tx.clone();
        rt::spawn(async move {
            while let Some(event) = events.recv().await {
                if forward
                    .send(ActorMessage::Connection {
                        connection_key: connection_key.clone(),
                        generation,
                        event,
                    })
                    .is_err()
                {
                    return;
                }
            }
            let _ = forward.send(ActorMessage::ConnectionEnded {
                connection_key,
                generation,
            });
        });
    }

    /// The members whose media lives on `key`, per the latest snapshot.
    fn members_on_key(&self, key: &str) -> HashSet<String> {
        self.members_snapshot
            .iter()
            .filter(|member| {
                select_transport(&self.transports, member)
                    .is_some_and(|(_, member_key)| member_key == key)
            })
            .map(|member| member.member_id.clone())
            .collect()
    }

    // ---- connection events ---------------------------------------------

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
            ConnectionEvent::ActiveSpeakers { speakers } => {
                // Speakers with no known membership are dropped: a level we
                // cannot attribute to a member is not actionable.
                let speakers: Vec<SpeakingMember> = speakers
                    .iter()
                    .filter_map(|speaker| {
                        self.identity_map
                            .get(&speaker.identity)
                            .map(|member_id| SpeakingMember {
                                member_id: member_id.clone(),
                                level: speaker.level,
                            })
                    })
                    .collect();
                self.emit(CallEvent::ActiveSpeakers { speakers });
            }
            ConnectionEvent::EncryptionStateChanged { identity, state } => {
                match self.identity_map.get(&identity).cloned() {
                    Some(member_id) => {
                        let diagnostic = self.encryption_diagnostic(&member_id, state);
                        let previous = self.encryption_states.insert(member_id.clone(), state);
                        if state.is_failure() {
                            log::warn!(
                                "frame encryption {state:?} for {member_id} (was {previous:?}), \
                                 {diagnostic:?}"
                            );
                        } else {
                            log::info!(
                                "frame encryption {state:?} for {member_id} (was {previous:?})"
                            );
                        }
                        self.emit(CallEvent::FrameEncryptionState {
                            member_id,
                            state,
                            diagnostic,
                        });
                    }
                    None => {
                        // No membership to attribute it to; `UnknownParticipant`
                        // already covers that case on its own.
                        log::debug!(
                            "encryption state {state:?} for unmapped identity {identity} on {connection_key}"
                        );
                    }
                }
            }
            ConnectionEvent::Reconnecting => self.mark_degraded(connection_key),
            ConnectionEvent::Reconnected => {
                self.clear_degraded(connection_key);
                // Subscription settings are server-side connection state; a
                // resumed connection may have lost them.
                self.reapply_connection_constraints(connection_key);
            }
            ConnectionEvent::Closed { message } => {
                let generation = self
                    .pool
                    .get(connection_key)
                    .map(|entry| entry.generation)
                    .unwrap_or_default();
                self.connection_down(connection_key, generation, &message);
            }
        }
    }

    // ---- roster ----------------------------------------------------------

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

        for member in &snapshot {
            if self.roster.iter().any(|p| p.member_id == member.member_id) {
                continue;
            }
            self.add_member(member);
        }

        self.members_snapshot = snapshot;
        self.reconcile_pool();
        self.publish_roster();

        log::debug!(
            "roster reconciled: {} participant(s), {} with a transport identity",
            self.roster.len(),
            self.identity_map.len(),
        );
    }

    fn add_member(&mut self, member: &JoinedMembership) {
        let reachable = select_transport(&self.transports, member).is_some();
        let identity = self
            .transports
            .iter()
            .find_map(|backend| backend.remote_identity(member));

        match &identity {
            Some(identity) => log::debug!(
                "member {} ({}) joined the roster as {identity}, reachable={reachable}",
                member.member_id,
                member.sender,
            ),
            // Their media arrives under an identity nothing maps back, so it
            // will be buffered forever rather than surfacing as their stream.
            None => log::warn!(
                "member {} ({}) has no transport identity; their media cannot be attributed",
                member.member_id,
                member.sender,
            ),
        }

        let hand_raised_at_ms = self.raised_hands.get(&member.member_id).copied();
        self.roster.push(Participant {
            member_id: member.member_id.clone(),
            user_id: member.sender.clone(),
            device_id: member.origin.sender_device_id().map(str::to_owned),
            is_local: member.member_id == self.own_member_id,
            reachable,
            streams: Vec::new(),
            hand_raised_at_ms,
        });
        self.emit(CallEvent::ParticipantJoined {
            member_id: member.member_id.clone(),
            user_id: member.sender.clone(),
        });
        if let Some(raised_at_ms) = hand_raised_at_ms {
            self.emit(CallEvent::HandRaised {
                member_id: member.member_id.clone(),
                raised_at_ms,
            });
        }

        if let Some(identity) = identity {
            self.identity_map
                .insert(identity.clone(), member.member_id.clone());
            self.member_identities
                .insert(member.member_id.clone(), identity.clone());

            // Media and keys that arrived before this membership.
            if let Some(pending) = self.pending_tracks.remove(&identity) {
                log::debug!(
                    "flushing {} buffered track(s) onto member {}",
                    pending.len(),
                    member.member_id,
                );
                for (kind, track) in pending {
                    self.add_stream(&member.member_id.clone(), kind, track);
                }
            }
            for key_index in self.pending_keys.remove(&identity).unwrap_or_default() {
                self.record_installed_key(&member.member_id.clone(), key_index);
                self.emit(CallEvent::KeyImported {
                    member_id: member.member_id.clone(),
                    key_index,
                });
            }
        }
    }

    // ---- reactions --------------------------------------------------------

    /// Applies the core's raised-hand set: the roster entries change, and each
    /// hand that went up or down is reported. Hands of members not on the
    /// roster yet are kept for `add_member`.
    fn apply_raised_hands(&mut self, hands: Vec<RaisedHand>) {
        self.raised_hands = hands
            .into_iter()
            .map(|hand| (hand.member_id, hand.raised_at_ms))
            .collect();

        let mut changed = false;
        for index in 0..self.roster.len() {
            let member_id = self.roster[index].member_id.clone();
            let now = self.raised_hands.get(&member_id).copied();
            let before = self.roster[index].hand_raised_at_ms;
            if before == now {
                continue;
            }
            self.roster[index].hand_raised_at_ms = now;
            changed = true;
            match now {
                Some(raised_at_ms) => self.emit(CallEvent::HandRaised {
                    member_id,
                    raised_at_ms,
                }),
                None => self.emit(CallEvent::HandLowered { member_id }),
            }
        }
        if changed {
            self.publish_roster();
        }
    }

    /// Reports a reaction from a member on the roster. One from a member the
    /// roster does not hold is dropped: the core validated it against a
    /// membership, so this is a hand-off race, not a stranger.
    fn apply_reaction(&mut self, reaction: ReceivedReaction) {
        if !self
            .roster
            .iter()
            .any(|participant| participant.member_id == reaction.member_id)
        {
            log::debug!(
                "dropping reaction from {} ({}): not on the roster yet",
                reaction.member_id,
                reaction.sender,
            );
            return;
        }
        self.emit(CallEvent::Reaction {
            member_id: reaction.member_id,
            emoji: reaction.emoji,
            name: reaction.name,
            sound: reaction.sound.asset_name().map(str::to_owned),
        });
    }

    fn record_installed_key(&mut self, member_id: &str, key_index: u8) {
        let installed = self.installed_keys.entry(member_id.to_owned()).or_default();
        if !installed.contains(&key_index) {
            installed.push(key_index);
        }
    }

    /// What we can say about a frame-encryption state, from what we installed.
    fn encryption_diagnostic(
        &self,
        member_id: &str,
        state: FrameEncryptionState,
    ) -> FrameEncryptionDiagnostic {
        if !state.is_failure() {
            return FrameEncryptionDiagnostic::NotApplicable;
        }
        match self.installed_keys.get(member_id) {
            Some(indices) if !indices.is_empty() => FrameEncryptionDiagnostic::KeysInstalled {
                key_indices: indices.clone(),
            },
            _ => FrameEncryptionDiagnostic::NoKeyInstalled,
        }
    }

    fn remove_member(&mut self, member_id: &str) {
        self.roster
            .retain(|participant| participant.member_id != member_id);
        // Read the identity out before dropping the mappings, so the
        // identity-keyed buffers can be cleared too. They used to be left behind
        // on every departure: a stale index could then resurface against a later
        // member that happened to reuse the identity, and in a long-lived process
        // the maps only ever grew.
        let identity = self.member_identities.remove(member_id);
        if let Some(identity) = &identity {
            self.pending_keys.remove(identity);
            self.pending_tracks.remove(identity);
        }
        self.installed_keys.remove(member_id);
        self.encryption_states.remove(member_id);
        self.identity_map.retain(|_, mapped| mapped != member_id);
        // A rejoining member gets a fresh member_id, so their constraints
        // die with the membership.
        self.constraints
            .retain(|(member, _), _| member != member_id);
        self.tracks
            .lock()
            .expect("track map mutex poisoned")
            .retain(|(track_member, _), _| track_member != member_id);
        self.emit(CallEvent::ParticipantLeft {
            member_id: member_id.to_owned(),
        });
    }

    // ---- streams ----------------------------------------------------------

    /// Put one of our own publications on our roster entry.
    ///
    /// Deliberately not `add_stream`: there is no `RemoteTrackHandle` for our
    /// own media (nothing subscribes us to ourselves) and no constraints to
    /// push, so the two share only the roster bookkeeping.
    fn add_local_stream(&mut self, member_id: &str, kind: MediaStreamKind) {
        let Some(participant) = self.roster.iter_mut().find(|p| p.member_id == member_id) else {
            // Our own membership has not come back through the sticky map yet.
            // The publication is live at the transport regardless; the roster
            // catches up when `add_member` runs.
            log::debug!("published {kind:?} before our own membership is known");
            return;
        };
        if participant.streams.iter().any(|stream| stream.kind == kind) {
            return;
        }
        participant.streams.push(StreamState { kind, muted: false });
        self.emit(CallEvent::StreamStarted {
            member_id: member_id.to_owned(),
            kind,
        });
        self.publish_roster();
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

        // A fresh subscription starts with server-default settings; push the
        // stored constraints at it immediately (no debounce — nothing to
        // coalesce with).
        if self.constraints.contains_key(&(member_id.to_owned(), kind)) {
            self.apply_constraints_now(member_id, kind);
        }
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

    fn remove_all_streams(&mut self, member_id: &str) {
        let kinds: Vec<MediaStreamKind> = self
            .roster
            .iter()
            .find(|p| p.member_id == member_id)
            .map(|p| p.streams.iter().map(|stream| stream.kind).collect())
            .unwrap_or_default();
        for kind in kinds {
            self.remove_stream(member_id, kind);
        }
    }

    fn set_muted(&mut self, identity: &str, kind: MediaStreamKind, muted: bool) {
        let Some(member_id) = self.identity_map.get(identity).cloned() else {
            return;
        };
        self.set_member_muted(&member_id, kind, muted);
    }

    /// Mute state keyed by member rather than transport identity, so it serves
    /// our own publications too — those never arrive as transport events, since
    /// nothing subscribes us to ourselves.
    fn set_member_muted(&mut self, member_id: &str, kind: MediaStreamKind, muted: bool) {
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
        let member_id = member_id.to_owned();
        let event = if muted {
            CallEvent::StreamMuted { member_id, kind }
        } else {
            CallEvent::StreamUnmuted { member_id, kind }
        };
        self.emit(event);
        self.publish_roster();
    }

    // ---- health / end ------------------------------------------------------

    fn mark_degraded(&mut self, key: &str) {
        let was_clear = self.degraded_keys.is_empty();
        if self.degraded_keys.insert(key.to_owned()) && was_clear {
            self.emit(CallEvent::MediaConnectionState { degraded: true });
        }
    }

    fn clear_degraded(&mut self, key: &str) {
        if self.degraded_keys.remove(key) && self.degraded_keys.is_empty() {
            self.emit(CallEvent::MediaConnectionState { degraded: false });
        }
    }

    /// Emit [`CallEvent::Ended`] once and close all pooled peer-focus
    /// connections. The adopted own-focus connection is never closed by the
    /// engine (its owner closes it; at this point it is usually already gone).
    fn end(&mut self, reason: EndedReason) {
        if self.ended {
            return;
        }
        self.ended = true;
        self.emit(CallEvent::Ended { reason });
        for (_, entry) in self.pool.drain() {
            if let ConnState::Up { connection } = entry.state
                && !entry.is_own
            {
                rt::spawn(async move {
                    if let Err(error) = connection.close().await {
                        log::debug!("closing connection on call end failed: {error}");
                    }
                });
            }
        }
    }

    fn emit(&self, event: CallEvent) {
        // Err means no subscriber right now; the roster watch still updates.
        let _ = self.events_tx.send(event);
    }

    fn publish_roster(&self) {
        let _ = self.participants_tx.send(self.roster.clone());
    }
}

/// First backend (in preference order) that can serve any of the member's
/// published transports, with the resulting connection key.
fn select_transport(
    transports: &[Arc<dyn MediaTransport>],
    member: &JoinedMembership,
) -> Option<(Arc<dyn MediaTransport>, String)> {
    for backend in transports {
        for transport in &member.transports {
            if let Some(key) = backend.connection_key(transport) {
                return Some((backend.clone(), key));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::time::Duration;

    use futures_core::stream::BoxStream;
    use futures_util::StreamExt;
    use matrix_rtc_core::{EventOrigin, LiveKitTransport, RtcTransport};

    use crate::event::{FrameEncryptionDiagnostic, FrameEncryptionState};
    use matrix_rtc_core::KeyRejection;
    use tokio::sync::mpsc::UnboundedSender;

    use super::*;
    use crate::frame::AudioFrame;
    use crate::local::VideoSourceConfig;
    use crate::transport::{OwnMemberClaims, SpeakingParticipant};

    const OWN_FOCUS: &str = "https://sfu.example.org";
    const PEER_FOCUS: &str = "https://sfu-b.example.org";

    /// Shared observable state of the fake transport: which keys got
    /// connected/closed, and the live event senders per key.
    #[derive(Default)]
    struct TransportState {
        connects: StdMutex<Vec<String>>,
        closes: StdMutex<Vec<String>>,
        senders: StdMutex<HashMap<String, UnboundedSender<ConnectionEvent>>>,
        /// Number of connect attempts (per key) that fail before succeeding.
        fail_attempts: AtomicU32,
        /// `(connection_key, kind)` of every publish call.
        published: StdMutex<Vec<(String, MediaStreamKind)>>,
        /// `(identity, kind, resolved)` of every apply_constraints call.
        applied: StdMutex<Vec<(String, MediaStreamKind, crate::ResolvedConstraints)>>,
        /// `(kind, muted)` of every local mute call that reached the transport.
        local_mutes: StdMutex<Vec<(MediaStreamKind, bool)>>,
        /// Kind of every unpublish call that reached the transport.
        unpublished: StdMutex<Vec<MediaStreamKind>>,
        /// Makes the next unpublish calls fail at the transport.
        fail_unpublish: AtomicBool,
    }

    struct FakeTransport {
        state: Arc<TransportState>,
    }

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

        async fn connect(&self, connection_key: &str, _ctx: &ConnectionContext) -> ConnectOutcome {
            self.state
                .connects
                .lock()
                .unwrap()
                .push(connection_key.to_owned());
            if self.state.fail_attempts.load(Ordering::SeqCst) > 0 {
                self.state.fail_attempts.fetch_sub(1, Ordering::SeqCst);
                return Err(TransportError::Connect("fake failure".into()));
            }
            let (connection, events) = fake_connection(&self.state, connection_key);
            Ok((connection, events))
        }
    }

    struct FakeConnection {
        key: String,
        state: Arc<TransportState>,
    }

    #[async_trait::async_trait]
    impl TransportConnection for FakeConnection {
        fn connection_key(&self) -> &str {
            &self.key
        }

        async fn publish(
            &self,
            options: PublishOptions,
        ) -> Result<Arc<dyn LocalTrackHandle>, TransportError> {
            self.state
                .published
                .lock()
                .unwrap()
                .push((self.key.clone(), options.kind));
            Ok(Arc::new(FakeLocalTrack::new(
                options.kind,
                Arc::clone(&self.state),
            )))
        }

        async fn apply_constraints(
            &self,
            identity: &str,
            kind: MediaStreamKind,
            resolved: crate::ResolvedConstraints,
        ) -> Result<(), TransportError> {
            self.state
                .applied
                .lock()
                .unwrap()
                .push((identity.to_owned(), kind, resolved));
            Ok(())
        }

        async fn close(&self) -> Result<(), TransportError> {
            self.state.closes.lock().unwrap().push(self.key.clone());
            Ok(())
        }
    }

    struct FakeLocalTrack {
        kind: MediaStreamKind,
        state: Arc<TransportState>,
    }

    impl FakeLocalTrack {
        fn new(kind: MediaStreamKind, state: Arc<TransportState>) -> Self {
            Self { kind, state }
        }
    }

    #[async_trait::async_trait]
    impl LocalTrackHandle for FakeLocalTrack {
        fn kind(&self) -> MediaStreamKind {
            self.kind
        }

        fn set_muted(&self, muted: bool) -> Result<(), TransportError> {
            self.state
                .local_mutes
                .lock()
                .unwrap()
                .push((self.kind, muted));
            Ok(())
        }

        async fn unpublish(&self) -> Result<(), TransportError> {
            if self.state.fail_unpublish.load(Ordering::SeqCst) {
                return Err(TransportError::Closed("fake unpublish failure".into()));
            }
            self.state.unpublished.lock().unwrap().push(self.kind);
            Ok(())
        }
    }

    fn fake_connection(
        state: &Arc<TransportState>,
        key: &str,
    ) -> (
        Box<dyn TransportConnection>,
        mpsc::UnboundedReceiver<ConnectionEvent>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        state.senders.lock().unwrap().insert(key.to_owned(), tx);
        (
            Box::new(FakeConnection {
                key: key.to_owned(),
                state: state.clone(),
            }),
            rx,
        )
    }

    struct FakeTrack {
        kind: MediaStreamKind,
        /// What `receive_stats` reports; `None` models a transport with no
        /// counters (the trait default).
        stats: Option<ReceiveStats>,
    }

    impl FakeTrack {
        fn new(kind: MediaStreamKind) -> Self {
            Self { kind, stats: None }
        }
    }

    #[async_trait::async_trait]
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

        async fn receive_stats(&self) -> Option<ReceiveStats> {
            self.stats.clone()
        }
    }

    fn member_on(member_id: &str, user_id: &str, focus: &str) -> JoinedMembership {
        JoinedMembership {
            room_id: "!room:example.org".to_owned(),
            slot_id: "m.call#ROOM".to_owned(),
            sender: user_id.to_owned(),
            origin: EventOrigin::encrypted(Some("DEVICE".to_owned())),
            sticky_key: member_id.to_owned(),
            member_id: member_id.to_owned(),
            membership_event_id: None,
            membership_ts: None,
            application: Some("m.call".to_owned()),
            transports: vec![RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: focus.to_owned(),
            })],
            can_subscribe: vec!["livekit".to_owned()],
        }
    }

    fn member(member_id: &str, user_id: &str) -> JoinedMembership {
        member_on(member_id, user_id, OWN_FOCUS)
    }

    struct Fixture {
        engine: CallEngine,
        events: broadcast::Receiver<CallEvent>,
        memberships: watch::Sender<Vec<JoinedMembership>>,
        raised_hands: watch::Sender<Vec<RaisedHand>>,
        reactions: broadcast::Sender<ReceivedReaction>,
        state: Arc<TransportState>,
    }

    fn fixture() -> Fixture {
        let state = Arc::new(TransportState::default());
        let (memberships, memberships_rx) = watch::channel(Vec::new());
        let (raised_hands, raised_hands_rx) = watch::channel(Vec::new());
        let (reactions, reactions_rx) = broadcast::channel(8);
        let engine = CallEngine::new(
            EngineConfig {
                transports: vec![Arc::new(FakeTransport {
                    state: state.clone(),
                })],
                own_member_id: "own".to_owned(),
                ctx: ConnectionContext {
                    room_id: "!room:example.org".to_owned(),
                    slot_id: "m.call#ROOM".to_owned(),
                    member: OwnMemberClaims {
                        member_id: "own".to_owned(),
                        user_id: "@alice:example.org".to_owned(),
                        device_id: "DEVICE".to_owned(),
                    },
                },
                own_connection_key: Some(OWN_FOCUS.to_owned()),
                raised_hands: Some(raised_hands_rx),
                reactions: Some(reactions_rx),
            },
            memberships_rx,
        );
        let events = engine.subscribe_events();
        Fixture {
            engine,
            events,
            memberships,
            raised_hands,
            reactions,
            state,
        }
    }

    /// A hand and a reaction are keyed by `member_id` like everything else;
    /// the hand lands on the roster entry (even when it was reported before
    /// the membership) and both surface as events.
    #[tokio::test]
    async fn raised_hands_and_reactions_ride_on_the_roster() {
        let mut fx = fixture();

        // The hand is known before the membership: parked, then applied.
        fx.raised_hands
            .send(vec![RaisedHand {
                member_id: "bob".to_owned(),
                sender: "@bob:example.org".to_owned(),
                reaction_event_id: "$hand".to_owned(),
                raised_at_ms: 42,
            }])
            .unwrap();
        fx.memberships
            .send(vec![
                member("own", "@alice:example.org"),
                member("bob", "@bob:example.org"),
            ])
            .unwrap();

        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(next_event(&mut fx.events).await);
        }
        assert!(seen.contains(&CallEvent::HandRaised {
            member_id: "bob".to_owned(),
            raised_at_ms: 42,
        }));
        let bob = fx
            .engine
            .participants()
            .into_iter()
            .find(|p| p.member_id == "bob")
            .unwrap();
        assert_eq!(bob.hand_raised_at_ms, Some(42));

        fx.reactions
            .send(ReceivedReaction {
                member_id: "bob".to_owned(),
                sender: "@bob:example.org".to_owned(),
                emoji: "👏".to_owned(),
                name: "clapping".to_owned(),
                sound: matrix_rtc_core::ReactionSound::Named("clap".to_owned()),
            })
            .unwrap();
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::Reaction {
                member_id: "bob".to_owned(),
                emoji: "👏".to_owned(),
                name: "clapping".to_owned(),
                sound: Some("clap".to_owned()),
            }
        );

        fx.raised_hands.send(Vec::new()).unwrap();
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::HandLowered {
                member_id: "bob".to_owned(),
            }
        );
        let bob = fx
            .engine
            .participants()
            .into_iter()
            .find(|p| p.member_id == "bob")
            .unwrap();
        assert_eq!(bob.hand_raised_at_ms, None);
    }

    async fn next_event(events: &mut broadcast::Receiver<CallEvent>) -> CallEvent {
        tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("timed out waiting for a call event")
            .expect("event channel closed")
    }

    /// Poll until `predicate` holds (paused-clock tests auto-advance timers).
    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(60), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("condition not reached in time");
    }

    /// Adopt an own-focus connection into the engine, returning its sender.
    fn adopt(fx: &Fixture) -> UnboundedSender<ConnectionEvent> {
        let (connection, events) = fake_connection(&fx.state, OWN_FOCUS);
        fx.engine.adopt_own_connection(connection, events);
        wait_sender(fx, OWN_FOCUS)
    }

    fn wait_sender(fx: &Fixture, key: &str) -> UnboundedSender<ConnectionEvent> {
        fx.state
            .senders
            .lock()
            .unwrap()
            .get(key)
            .expect("connection sender should exist")
            .clone()
    }

    fn peer_sender(fx: &Fixture, key: &str) -> Option<UnboundedSender<ConnectionEvent>> {
        fx.state.senders.lock().unwrap().get(key).cloned()
    }

    fn connect_count(fx: &Fixture, key: &str) -> usize {
        fx.state
            .connects
            .lock()
            .unwrap()
            .iter()
            .filter(|k| *k == key)
            .count()
    }

    fn closed(fx: &Fixture, key: &str) -> bool {
        fx.state.closes.lock().unwrap().iter().any(|k| k == key)
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

        // Everyone is on the own focus, which awaits adoption: the engine
        // must not have opened a connection itself.
        assert_eq!(connect_count(&fx, OWN_FOCUS), 0);
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

        let connection = adopt(&fx);
        connection
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
                track: Arc::new(FakeTrack::new(MediaStreamKind::Microphone)),
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

        // Mute, then removal.
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
        let connection = adopt(&fx);

        // Track and key arrive before the sticky membership.
        connection
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Camera,
                track: Arc::new(FakeTrack::new(MediaStreamKind::Camera)),
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

    /// More than one key can be imported before a membership lands — a first key
    /// plus a rotation, which is exactly what a rejoin produces. Keeping only the
    /// latest index left the host believing the transport held one key when it
    /// held several, and unable to tell which.
    #[tokio::test]
    async fn every_early_key_surfaces_once_the_membership_lands() {
        let mut fx = fixture();

        fx.engine.notify_key_imported("id-bob", 0);
        fx.engine.notify_key_imported("id-bob", 1);
        // A repeat of an index already buffered is still just one import.
        fx.engine.notify_key_imported("id-bob", 1);

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
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::KeyImported {
                member_id: "bob".to_owned(),
                key_index: 0,
            }
        );
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::KeyImported {
                member_id: "bob".to_owned(),
                key_index: 1,
            }
        );
    }

    /// A departure must take its identity-keyed buffers with it, or a key
    /// imported for the member who left resurfaces against whoever next occupies
    /// that identity.
    #[tokio::test]
    async fn a_departure_forgets_the_keys_buffered_against_its_identity() {
        let mut fx = fixture();

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

        // Bob leaves, and a key for his identity lands late.
        fx.memberships.send(Vec::new()).unwrap();
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::ParticipantLeft {
                member_id: "bob".to_owned(),
            }
        );
        fx.engine.notify_key_imported("id-bob", 7);

        // Bob rejoins under a fresh member id, as MSC4143 requires. The stale
        // index must not be attributed to the new participation.
        fx.memberships
            .send(vec![member("bob-2", "@bob:example.org")])
            .unwrap();
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::ParticipantJoined {
                member_id: "bob-2".to_owned(),
                user_id: "@bob:example.org".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn a_decryption_failure_is_surfaced_against_the_membership() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined

        let connection = adopt(&fx);
        connection
            .send(ConnectionEvent::EncryptionStateChanged {
                identity: "id-bob".to_owned(),
                state: FrameEncryptionState::MissingKey,
            })
            .unwrap();

        // The host learns which *member* is undecryptable, not which opaque
        // transport identity — and that no key ever reached them, which is the
        // difference between a signalling bug and a rotation in flight.
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::FrameEncryptionState {
                member_id: "bob".to_owned(),
                state: FrameEncryptionState::MissingKey,
                diagnostic: FrameEncryptionDiagnostic::NoKeyInstalled,
            }
        );
    }

    /// The same failure means something different once a key *has* been
    /// installed: the frames are carrying an index we were not given, so the key
    /// path is working and a rotation is simply in flight.
    #[tokio::test]
    async fn a_failure_after_an_import_reports_the_keys_it_holds() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined

        fx.engine.notify_key_imported("id-bob", 4);
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::KeyImported {
                member_id: "bob".to_owned(),
                key_index: 4,
            }
        );

        let connection = adopt(&fx);
        connection
            .send(ConnectionEvent::EncryptionStateChanged {
                identity: "id-bob".to_owned(),
                state: FrameEncryptionState::MissingKey,
            })
            .unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::FrameEncryptionState {
                member_id: "bob".to_owned(),
                state: FrameEncryptionState::MissingKey,
                diagnostic: FrameEncryptionDiagnostic::KeysInstalled {
                    key_indices: vec![4]
                },
            }
        );
    }

    /// A refused key must reach the host, with the reason and the device that
    /// sent it. It is reported against a `member_id` and needs no membership or
    /// transport identity: the core refuses the key before either applies, and a
    /// key naming a member we have never seen is exactly the case worth reporting.
    #[tokio::test]
    async fn a_refused_key_surfaces_with_its_reason() {
        let mut fx = fixture();

        fx.engine.notify_key_discarded(DiscardedKey {
            member_id: "bob".to_owned(),
            key_index: Some(2),
            sender_user_id: Some("@bob:example.org".to_owned()),
            sender_device_id: Some("BOBDEV".to_owned()),
            reason: KeyRejection::NotCrossSigned,
        });

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::KeyDiscarded {
                member_id: "bob".to_owned(),
                key_index: Some(2),
                sender_user_id: Some("@bob:example.org".to_owned()),
                sender_device_id: Some("BOBDEV".to_owned()),
                reason: KeyRejection::NotCrossSigned,
            }
        );
    }

    /// A recovery carries no diagnostic — there is nothing to explain, and an
    /// `Ok` that still named a reason would read as a lingering fault.
    #[tokio::test]
    async fn a_recovered_stream_reports_no_diagnostic() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined

        let connection = adopt(&fx);
        connection
            .send(ConnectionEvent::EncryptionStateChanged {
                identity: "id-bob".to_owned(),
                state: FrameEncryptionState::Ok,
            })
            .unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::FrameEncryptionState {
                member_id: "bob".to_owned(),
                state: FrameEncryptionState::Ok,
                diagnostic: FrameEncryptionDiagnostic::NotApplicable,
            }
        );
    }

    /// "Who is talking" and "how loud" arrive in the same transport event, so
    /// they must leave together: a host that gets only the identities has to
    /// meter the PCM itself, decoding audio it may not otherwise need to answer
    /// a question the SFU already answered.
    #[tokio::test]
    async fn active_speakers_carry_their_audio_level() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined

        let connection = adopt(&fx);
        connection
            .send(ConnectionEvent::ActiveSpeakers {
                speakers: vec![
                    SpeakingParticipant {
                        identity: "id-bob".to_owned(),
                        level: 0.75,
                    },
                    // No membership maps to this one, so it cannot be attributed
                    // and is dropped rather than reported against nobody.
                    SpeakingParticipant {
                        identity: "id-ghost".to_owned(),
                        level: 0.9,
                    },
                ],
            })
            .unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::ActiveSpeakers {
                speakers: vec![SpeakingMember {
                    member_id: "bob".to_owned(),
                    level: 0.75,
                }],
            }
        );
    }

    #[tokio::test]
    async fn an_encryption_state_for_an_unmapped_identity_is_dropped() {
        let mut fx = fixture();
        let connection = adopt(&fx);
        connection
            .send(ConnectionEvent::EncryptionStateChanged {
                identity: "id-nobody".to_owned(),
                state: FrameEncryptionState::DecryptionFailed,
            })
            .unwrap();
        // Followed by something we can wait for, to prove the first produced
        // no event rather than merely being slower.
        connection
            .send(ConnectionEvent::ActiveSpeakers {
                speakers: Vec::new(),
            })
            .unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::ActiveSpeakers {
                speakers: Vec::new(),
            }
        );
    }

    #[tokio::test]
    async fn receive_stats_come_from_the_subscribed_track() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await;

        // RTP arriving, nothing decoding: the exact case that is invisible at
        // the frame level, because frames are produced either way.
        let reported = ReceiveStats {
            packets_received: 900,
            bytes_received: 120_000,
            frames_decoded: 0,
            concealed_samples: 48_000,
            total_samples_received: 48_000,
            ..ReceiveStats::default()
        };
        let connection = adopt(&fx);
        connection
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
                track: Arc::new(FakeTrack {
                    kind: MediaStreamKind::Microphone,
                    stats: Some(reported.clone()),
                }),
            })
            .unwrap();
        let _ = next_event(&mut fx.events).await; // StreamStarted

        assert_eq!(
            fx.engine
                .receive_stats("bob", MediaStreamKind::Microphone)
                .await,
            Some(reported)
        );
    }

    #[tokio::test]
    async fn the_receive_stats_future_is_send() {
        // uniffi's async exports require `Future + Send`. A `!Send` future
        // here would only break the FFI crate, which cannot be built without
        // libwebrtc — so assert it in the crate that can.
        fn assert_send<T: Send>(_: T) {}
        let fx = fixture();
        assert_send(fx.engine.receive_stats("bob", MediaStreamKind::Microphone));
    }

    #[tokio::test]
    async fn receive_stats_are_none_without_a_subscribed_stream() {
        let fx = fixture();
        assert_eq!(
            fx.engine
                .receive_stats("bob", MediaStreamKind::Microphone)
                .await,
            None
        );
    }

    #[tokio::test]
    async fn receive_stats_are_none_when_the_transport_reports_nothing() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await;

        let connection = adopt(&fx);
        connection
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
                // `FakeTrack::new` leaves the trait default in place.
                track: Arc::new(FakeTrack::new(MediaStreamKind::Microphone)),
            })
            .unwrap();
        let _ = next_event(&mut fx.events).await;

        assert_eq!(
            fx.engine
                .receive_stats("bob", MediaStreamKind::Microphone)
                .await,
            None
        );
    }

    #[tokio::test]
    async fn departure_cleans_up_roster_and_tracks() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("bob", "@bob:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await;

        let connection = adopt(&fx);
        connection
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
                track: Arc::new(FakeTrack::new(MediaStreamKind::Microphone)),
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
    async fn closed_own_connection_ends_the_call() {
        let mut fx = fixture();
        let connection = adopt(&fx);
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
        let connection = adopt(&fx);
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

    #[tokio::test(start_paused = true)]
    async fn peer_focus_gets_its_own_connection() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![
                member("own", "@alice:example.org"),
                member_on("bob", "@bob:example.org", PEER_FOCUS),
            ])
            .unwrap();
        let _ = next_event(&mut fx.events).await;
        let _ = next_event(&mut fx.events).await;

        // The engine connects to bob's focus by itself...
        wait_until(|| peer_sender(&fx, PEER_FOCUS).is_some()).await;
        assert_eq!(connect_count(&fx, PEER_FOCUS), 1);
        // ...but never to the own focus (that one is adopted).
        assert_eq!(connect_count(&fx, OWN_FOCUS), 0);

        // Media flowing over the peer connection maps onto the roster.
        peer_sender(&fx, PEER_FOCUS)
            .unwrap()
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
                track: Arc::new(FakeTrack::new(MediaStreamKind::Microphone)),
            })
            .unwrap();
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamStarted {
                member_id: "bob".to_owned(),
                kind: MediaStreamKind::Microphone,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_peer_connect_retries_with_backoff() {
        let fx = fixture();
        fx.state.fail_attempts.store(2, Ordering::SeqCst);

        fx.memberships
            .send(vec![member_on("bob", "@bob:example.org", PEER_FOCUS)])
            .unwrap();

        // Two failures, then success on the third attempt (timers auto-advance
        // under the paused clock).
        wait_until(|| peer_sender(&fx, PEER_FOCUS).is_some()).await;
        assert_eq!(connect_count(&fx, PEER_FOCUS), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn peer_connection_loss_drops_streams_and_reconnects() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member_on("bob", "@bob:example.org", PEER_FOCUS)])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined

        wait_until(|| peer_sender(&fx, PEER_FOCUS).is_some()).await;
        peer_sender(&fx, PEER_FOCUS)
            .unwrap()
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Microphone,
                track: Arc::new(FakeTrack::new(MediaStreamKind::Microphone)),
            })
            .unwrap();
        let _ = next_event(&mut fx.events).await; // StreamStarted

        // The peer focus dies: bob's streams stop, media degrades, the call
        // does NOT end, and a reconnect follows.
        peer_sender(&fx, PEER_FOCUS)
            .unwrap()
            .send(ConnectionEvent::Closed {
                message: "gone".to_owned(),
            })
            .unwrap();

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamStopped {
                member_id: "bob".to_owned(),
                kind: MediaStreamKind::Microphone,
            }
        );
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::MediaConnectionState { degraded: true }
        );
        wait_until(|| connect_count(&fx, PEER_FOCUS) >= 2).await;
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::MediaConnectionState { degraded: false }
        );
        assert!(
            fx.engine
                .participants()
                .iter()
                .any(|p| p.member_id == "bob")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn idle_peer_connection_closes_after_grace() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member_on("bob", "@bob:example.org", PEER_FOCUS)])
            .unwrap();
        let _ = next_event(&mut fx.events).await;
        wait_until(|| peer_sender(&fx, PEER_FOCUS).is_some()).await;

        // Bob leaves; the connection lingers for the grace period, then closes.
        fx.memberships.send(Vec::new()).unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantLeft
        assert!(!closed(&fx, PEER_FOCUS));
        wait_until(|| closed(&fx, PEER_FOCUS)).await;
        assert_eq!(connect_count(&fx, PEER_FOCUS), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn returning_member_cancels_idle_close() {
        let mut fx = fixture();
        let bob = || member_on("bob", "@bob:example.org", PEER_FOCUS);
        fx.memberships.send(vec![bob()]).unwrap();
        let _ = next_event(&mut fx.events).await;
        wait_until(|| peer_sender(&fx, PEER_FOCUS).is_some()).await;

        // Bob flaps: leaves and returns within the grace period.
        fx.memberships.send(Vec::new()).unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantLeft
        tokio::time::sleep(IDLE_GRACE / 2).await;
        fx.memberships.send(vec![bob()]).unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined

        // Well past the original grace deadline the connection is still the
        // first one, never closed or re-opened.
        tokio::time::sleep(IDLE_GRACE * 2).await;
        assert!(!closed(&fx, PEER_FOCUS));
        assert_eq!(connect_count(&fx, PEER_FOCUS), 1);
    }

    /// A host must be able to render itself from the same roster it renders
    /// everyone else from. Publishing raised no event, so its own entry lacked
    /// the microphone it was actively capturing — and alone in a call nothing
    /// later prompted a re-read to correct it.
    #[tokio::test]
    async fn publishing_puts_the_stream_on_our_own_roster_entry() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("own", "@own:example.org")])
            .unwrap();
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::ParticipantJoined {
                member_id: "own".to_owned(),
                user_id: "@own:example.org".to_owned(),
            }
        );
        let _connection = adopt(&fx);

        fx.engine
            .publish(PublishOptions::microphone())
            .await
            .expect("publish should succeed");

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamStarted {
                member_id: "own".to_owned(),
                kind: MediaStreamKind::Microphone,
            }
        );
        let own = fx
            .engine
            .participants()
            .into_iter()
            .find(|p| p.is_local)
            .expect("we are on our own roster");
        assert_eq!(own.streams.len(), 1);
        assert_eq!(own.streams[0].kind, MediaStreamKind::Microphone);
        assert!(!own.streams[0].muted);
    }

    /// Muting must reach the transport (so peers are told, rather than seeing a
    /// sender that merely stopped) *and* the roster, so the host does not have to
    /// keep its own copy of the answer.
    #[tokio::test]
    async fn muting_ourselves_reaches_the_transport_and_the_roster() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("own", "@own:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined
        let _connection = adopt(&fx);

        fx.engine
            .publish(PublishOptions::microphone())
            .await
            .expect("publish should succeed");
        let _ = next_event(&mut fx.events).await; // StreamStarted

        fx.engine
            .set_local_muted(MediaStreamKind::Microphone, true)
            .await
            .expect("muting a live publication should succeed");

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamMuted {
                member_id: "own".to_owned(),
                kind: MediaStreamKind::Microphone,
            }
        );
        assert_eq!(
            *fx.state.local_mutes.lock().unwrap(),
            vec![(MediaStreamKind::Microphone, true)],
            "the mute must reach the transport, or peers just see a stalled sender",
        );
        assert!(
            fx.engine
                .participants()
                .into_iter()
                .find(|p| p.is_local)
                .expect("we are on our own roster")
                .streams[0]
                .muted
        );
    }

    /// Muting something we never published is an error, not a silent no-op that
    /// leaves the host believing it is muted.
    #[tokio::test]
    async fn muting_an_unpublished_kind_fails() {
        let fx = fixture();
        let _connection = adopt(&fx);

        assert!(
            fx.engine
                .set_local_muted(MediaStreamKind::Camera, true)
                .await
                .is_err()
        );
    }

    /// Unpublishing must reach the transport (so peers drop the stream rather
    /// than rendering an empty tile — the stopped-screen-share case) *and*
    /// remove it from our own roster entry.
    #[tokio::test]
    async fn unpublishing_removes_the_stream_from_our_roster_and_reaches_the_transport() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("own", "@own:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined
        let _connection = adopt(&fx);

        fx.engine
            .publish(PublishOptions::screen_share(VideoSourceConfig {
                width: 1920,
                height: 1080,
            }))
            .await
            .expect("publish should succeed");
        let _ = next_event(&mut fx.events).await; // StreamStarted

        fx.engine
            .unpublish(MediaStreamKind::ScreenShare)
            .await
            .expect("unpublishing a live publication should succeed");

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamStopped {
                member_id: "own".to_owned(),
                kind: MediaStreamKind::ScreenShare,
            }
        );
        assert_eq!(
            *fx.state.unpublished.lock().unwrap(),
            vec![MediaStreamKind::ScreenShare],
            "the unpublish must reach the transport, or peers keep the stream",
        );
        assert!(
            fx.engine
                .participants()
                .into_iter()
                .find(|p| p.is_local)
                .expect("we are on our own roster")
                .streams
                .is_empty()
        );
    }

    /// Unpublishing something we never published is an error, mirroring mute.
    #[tokio::test]
    async fn unpublishing_an_unpublished_kind_fails() {
        let fx = fixture();
        let _connection = adopt(&fx);

        assert!(
            fx.engine
                .unpublish(MediaStreamKind::ScreenShare)
                .await
                .is_err()
        );
    }

    /// A fresh publish of the same kind after an unpublish must come back on
    /// the roster — nothing of the retracted publication may linger.
    #[tokio::test]
    async fn republishing_after_unpublish_puts_the_stream_back() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("own", "@own:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined
        let _connection = adopt(&fx);

        fx.engine
            .publish(PublishOptions::microphone())
            .await
            .expect("publish should succeed");
        let _ = next_event(&mut fx.events).await; // StreamStarted
        fx.engine
            .unpublish(MediaStreamKind::Microphone)
            .await
            .expect("unpublish should succeed");
        let _ = next_event(&mut fx.events).await; // StreamStopped

        fx.engine
            .publish(PublishOptions::microphone())
            .await
            .expect("re-publishing after an unpublish should succeed");

        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamStarted {
                member_id: "own".to_owned(),
                kind: MediaStreamKind::Microphone,
            }
        );
        assert_eq!(fx.state.published.lock().unwrap().len(), 2);
        let own = fx
            .engine
            .participants()
            .into_iter()
            .find(|p| p.is_local)
            .expect("we are on our own roster");
        assert_eq!(own.streams.len(), 1);
        assert_eq!(own.streams[0].kind, MediaStreamKind::Microphone);
    }

    /// A failed retraction keeps the publication: the transport already maps
    /// the room-closed case to success, so a failure means the room is live
    /// and the track is still published at the SFU. Dropping local state then
    /// would leave a stream peers keep rendering but that nothing can mute or
    /// retract anymore — for a screen share, a privacy bug.
    #[tokio::test]
    async fn unpublish_keeps_the_publication_when_the_transport_errors() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member("own", "@own:example.org")])
            .unwrap();
        let _ = next_event(&mut fx.events).await; // ParticipantJoined
        let _connection = adopt(&fx);

        fx.engine
            .publish(PublishOptions::microphone())
            .await
            .expect("publish should succeed");
        let _ = next_event(&mut fx.events).await; // StreamStarted

        fx.state.fail_unpublish.store(true, Ordering::SeqCst);
        assert!(
            fx.engine
                .unpublish(MediaStreamKind::Microphone)
                .await
                .is_err()
        );

        // The stream is still on our roster and the publication still usable.
        assert_eq!(
            fx.engine
                .participants()
                .into_iter()
                .find(|p| p.is_local)
                .expect("we are on our own roster")
                .streams
                .len(),
            1
        );
        fx.engine
            .set_local_muted(MediaStreamKind::Microphone, true)
            .await
            .expect("the engine still holds the publication");
        let _ = next_event(&mut fx.events).await; // StreamMuted

        // A retry once the transport recovers retracts it for real.
        fx.state.fail_unpublish.store(false, Ordering::SeqCst);
        fx.engine
            .unpublish(MediaStreamKind::Microphone)
            .await
            .expect("the retry should succeed");
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::StreamStopped {
                member_id: "own".to_owned(),
                kind: MediaStreamKind::Microphone,
            }
        );
        assert_eq!(
            *fx.state.unpublished.lock().unwrap(),
            vec![MediaStreamKind::Microphone],
        );
    }

    #[tokio::test]
    async fn publish_targets_the_own_focus_connection() {
        let fx = fixture();

        // Before the own connection is adopted, publishing fails.
        assert!(
            fx.engine
                .publish(PublishOptions::microphone())
                .await
                .is_err()
        );

        let _own = adopt(&fx);
        let handle = fx
            .engine
            .publish(PublishOptions::microphone())
            .await
            .expect("publish should succeed once the own focus is up");
        assert_eq!(handle.kind(), MediaStreamKind::Microphone);
        assert_eq!(
            fx.state.published.lock().unwrap().as_slice(),
            &[(OWN_FOCUS.to_owned(), MediaStreamKind::Microphone)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn constraints_are_debounced_and_apply_the_latest_value() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member_on("bob", "@bob:example.org", PEER_FOCUS)])
            .unwrap();
        let _ = next_event(&mut fx.events).await;
        wait_until(|| peer_sender(&fx, PEER_FOCUS).is_some()).await;

        // Two rapid updates: only the final state reaches the transport.
        fx.engine.set_constraints(
            "bob",
            MediaStreamKind::Camera,
            MediaConstraints {
                visible: false,
                ..Default::default()
            },
        );
        fx.engine.set_constraints(
            "bob",
            MediaStreamKind::Camera,
            MediaConstraints {
                visible: true,
                detail: crate::VideoDetail::Quality(crate::QualityLimit::Low),
                ..Default::default()
            },
        );

        wait_until(|| !fx.state.applied.lock().unwrap().is_empty()).await;
        // Give any stale timer a chance to (wrongly) fire as well.
        tokio::time::sleep(CONSTRAINTS_DEBOUNCE * 3).await;

        let applied = fx.state.applied.lock().unwrap().clone();
        assert_eq!(applied.len(), 1, "debounce should coalesce to one apply");
        let (identity, kind, resolved) = &applied[0];
        assert_eq!(identity, "id-bob");
        assert_eq!(*kind, MediaStreamKind::Camera);
        assert_eq!(resolved.demand, crate::StreamDemand::Active);
        assert_eq!(
            resolved.detail,
            crate::VideoDetail::Quality(crate::QualityLimit::Low)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn constraints_reapply_on_stream_start_and_reconnect() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member_on("bob", "@bob:example.org", PEER_FOCUS)])
            .unwrap();
        let _ = next_event(&mut fx.events).await;
        wait_until(|| peer_sender(&fx, PEER_FOCUS).is_some()).await;

        fx.engine.set_constraints(
            "bob",
            MediaStreamKind::Camera,
            MediaConstraints {
                low_bandwidth: true,
                ..Default::default()
            },
        );
        wait_until(|| fx.state.applied.lock().unwrap().len() == 1).await;

        // The stream appearing re-applies immediately (fresh subscriptions
        // start from server defaults)...
        peer_sender(&fx, PEER_FOCUS)
            .unwrap()
            .send(ConnectionEvent::TrackAdded {
                identity: "id-bob".to_owned(),
                kind: MediaStreamKind::Camera,
                track: Arc::new(FakeTrack::new(MediaStreamKind::Camera)),
            })
            .unwrap();
        wait_until(|| fx.state.applied.lock().unwrap().len() == 2).await;
        // low_bandwidth folds video to a pause (subscription kept).
        assert_eq!(
            fx.state.applied.lock().unwrap()[1].2.demand,
            crate::StreamDemand::Paused
        );

        // ...and so does a transport-level reconnect.
        peer_sender(&fx, PEER_FOCUS)
            .unwrap()
            .send(ConnectionEvent::Reconnected)
            .unwrap();
        wait_until(|| fx.state.applied.lock().unwrap().len() == 3).await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_emits_left_and_closes_peer_connections() {
        let mut fx = fixture();
        fx.memberships
            .send(vec![member_on("bob", "@bob:example.org", PEER_FOCUS)])
            .unwrap();
        let _ = next_event(&mut fx.events).await;
        wait_until(|| peer_sender(&fx, PEER_FOCUS).is_some()).await;
        let own = adopt(&fx);

        fx.engine.shutdown().await;
        assert_eq!(
            next_event(&mut fx.events).await,
            CallEvent::Ended {
                reason: EndedReason::Left,
            }
        );
        wait_until(|| closed(&fx, PEER_FOCUS)).await;
        // The adopted own connection is left to its owner.
        assert!(!closed(&fx, OWN_FOCUS));
        drop(own);
    }
}
