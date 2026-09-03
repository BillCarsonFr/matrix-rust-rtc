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

//! Element Call reactions and the raised hand.
//!
//! Neither is in the spec. Both are ordinary room events that *relate to the
//! reacting member's own membership event*, which is how a receiver ties a
//! reaction to a participant tile without trusting anything in the content:
//!
//! - An emoji reaction is an `io.element.call.reaction` event with an
//!   `m.reference` relation to the sender's `m.rtc.member` event, plus `emoji`
//!   and `name`. It is transient: Element Call shows it for three seconds and
//!   ignores further reactions from the same member while one is showing. The
//!   `name` selects a sound; an unknown name plays a generic one.
//! - A raised hand is a plain `m.reaction` annotation of the membership event
//!   with key [`RAISED_HAND_KEY`]. There is no "lowered" event: the annotation
//!   is redacted instead.
//!
//! Only the *protocol* lives here — building and validating the events, the
//! anti-spam window, the raised-hand set — and it is host-agnostic on purpose:
//! playing a sound and hiding an emoji after three seconds are the host's,
//! which is why a received reaction carries a sound *hint* rather than audio.
//!
//! # The membership event id moves
//!
//! A sticky membership is re-sent before it expires, and the new event replaces
//! the old one in the sticky map; matrix-js-sdk's `CallMembership.eventId`
//! follows it. Element Call then drops a raised hand whose membership event has
//! moved on and re-queries the relations of the *new* event. Two consequences:
//!
//! - Our own raised hand is re-annotated onto the new membership event after
//!   every refresh (and the old annotation redacted), or Element Call peers
//!   would lower it for us. See [`crate::RtcSession::heartbeat`].
//! - As a receiver we are more lenient than Element Call: a hand stays raised
//!   for as long as the member is in the call, whichever of their membership
//!   events it was annotated on. A member's reaction is validated against every
//!   membership event id the session has seen for them, not only the latest.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{broadcast, watch};
use unicode_segmentation::UnicodeSegmentation;

use crate::error::CommandError;
use crate::event::EventOrigin;
use crate::session::JoinedMembership;

/// Event type of an Element Call emoji reaction.
pub const REACTION_EVENT_TYPE: &str = "io.element.call.reaction";

/// Event type of the raised-hand annotation: the ordinary Matrix reaction.
pub const ANNOTATION_EVENT_TYPE: &str = "m.reaction";

/// The annotation key Element Call uses for a raised hand: U+1F590 U+FE0F,
/// "raised hand with fingers splayed" with the emoji presentation selector.
/// Byte-exact — a peer compares strings, not meanings.
pub const RAISED_HAND_KEY: &str = "🖐️";

/// How long Element Call shows a reaction, and ignores further ones from the
/// same member (`REACTION_ACTIVE_TIME_MS`).
pub const DEFAULT_REACTION_ACTIVE_MS: u64 = 3_000;

/// How many membership event ids are remembered per member for validating a
/// reaction against an older one of their events. Refreshes are half an hour
/// apart by default, so this covers hours of a call; the bound only exists so
/// a member cannot grow the set without limit.
const KNOWN_MEMBERSHIP_EVENTS_PER_MEMBER: usize = 8;

/// One entry of Element Call's reaction catalogue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReactionKind {
    /// The `name` sent on the wire, and the key a sound is looked up by.
    pub name: &'static str,
    /// The emoji Element Call sends for it.
    pub emoji: &'static str,
    /// The sound asset Element Call plays for it, if any, by base name
    /// (`party` → `party.ogg`). `None` is silent — not the generic sound.
    pub sound: Option<&'static str>,
}

/// Element Call's reaction catalogue, in the order its picker shows them.
///
/// The names are the interoperable part: a peer matches a sound by `name`, so
/// a reaction sent with a name from this list plays the same sound on Element
/// Call as it does here. The `drum` reaction's asset is called `baduntss`.
pub const KNOWN_REACTIONS: &[ReactionKind] = &[
    ReactionKind {
        name: "thumbsup",
        emoji: "👍",
        sound: None,
    },
    ReactionKind {
        name: "party",
        emoji: "🎉",
        sound: Some("party"),
    },
    ReactionKind {
        name: "clapping",
        emoji: "👏",
        sound: Some("clap"),
    },
    ReactionKind {
        name: "dog",
        emoji: "🐶",
        sound: Some("dog"),
    },
    ReactionKind {
        name: "cat",
        emoji: "🐱",
        sound: Some("cat"),
    },
    ReactionKind {
        name: "lightbulb",
        emoji: "💡",
        sound: Some("lightbulb"),
    },
    ReactionKind {
        name: "crickets",
        emoji: "🦗",
        sound: Some("crickets"),
    },
    ReactionKind {
        name: "thumbsdown",
        emoji: "👎",
        sound: None,
    },
    ReactionKind {
        name: "dizzy",
        emoji: "😵‍💫",
        sound: None,
    },
    ReactionKind {
        name: "ok",
        emoji: "👌",
        sound: None,
    },
    ReactionKind {
        name: "heart",
        emoji: "🥰",
        sound: None,
    },
    ReactionKind {
        name: "laugh",
        emoji: "😄",
        sound: None,
    },
    ReactionKind {
        name: "deer",
        emoji: "🦌",
        sound: Some("deer"),
    },
    ReactionKind {
        name: "rock",
        emoji: "🤘",
        sound: Some("rock"),
    },
    ReactionKind {
        name: "wave",
        emoji: "👋",
        sound: Some("wave"),
    },
    ReactionKind {
        name: "drum",
        emoji: "🥁",
        sound: Some("baduntss"),
    },
];

/// Base name of the sound Element Call plays for a reaction whose `name` it
/// does not know.
pub const GENERIC_SOUND: &str = "generic";

/// The catalogue entry for a reaction `name`, if it is one Element Call knows.
pub fn reaction_kind(name: &str) -> Option<&'static ReactionKind> {
    KNOWN_REACTIONS.iter().find(|kind| kind.name == name)
}

/// What a host should play for a received reaction.
///
/// A hint, not audio: the SDK neither ships nor mixes sound. The host bundles
/// the assets under the base names in [`KNOWN_REACTIONS`] plus
/// [`GENERIC_SOUND`], and decides for itself whether reaction sounds are on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReactionSound {
    /// A known reaction with no sound. Silent.
    None,
    /// A known reaction with a sound, by asset base name.
    Named(String),
    /// A reaction whose `name` is not in the catalogue. Element Call plays its
    /// generic reaction sound for these.
    Generic,
}

impl ReactionSound {
    /// The asset base name to play, or `None` for silence.
    pub fn asset_name(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Named(name) => Some(name),
            Self::Generic => Some(GENERIC_SOUND),
        }
    }
}

/// The sound Element Call plays for a reaction `name`.
pub fn sound_for(name: &str) -> ReactionSound {
    match reaction_kind(name) {
        Some(ReactionKind {
            sound: Some(sound), ..
        }) => ReactionSound::Named((*sound).to_owned()),
        Some(_) => ReactionSound::None,
        None => ReactionSound::Generic,
    }
}

/// The first grapheme cluster of `emoji`, which is all Element Call displays.
///
/// One emoji can be several code points (`😵‍💫` is three, joined), so this is
/// not "the first char"; it is what a user sees as one symbol. Trimmed, so a
/// leading space cannot smuggle an empty reaction through.
pub fn first_grapheme(emoji: &str) -> &str {
    emoji.trim().graphemes(true).next().unwrap_or("")
}

/// How a session handles reactions. `Default` is what Element Call does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReactionsConfig {
    /// Whether reactions are handled at all. Off, inbound reactions and raised
    /// hands are ignored and sending returns [`ReactionError::Disabled`].
    pub enabled: bool,
    /// How long a received reaction counts as active, in milliseconds. Further
    /// reactions from the same member inside this window are dropped, which is
    /// the anti-spam rule Element Call applies. Also how long a host should
    /// keep the emoji on screen.
    pub active_window_ms: u64,
    /// The least time between two reactions we send, in milliseconds. A send
    /// inside it fails with [`ReactionError::Cooldown`] rather than reaching
    /// the homeserver, since peers would drop it anyway.
    pub send_cooldown_ms: u64,
}

impl Default for ReactionsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            active_window_ms: DEFAULT_REACTION_ACTIVE_MS,
            send_cooldown_ms: DEFAULT_REACTION_ACTIVE_MS,
        }
    }
}

/// One message-like room event, as a host hands it to the core.
///
/// The reactions intake takes every event the host cares to forward and picks
/// out the two types it reads; anything else is ignored. Redactions are not
/// events here — a redaction only names its target, and hosts already resolve
/// that per room version — so they arrive through
/// [`crate::RtcSessionManager::on_event_redacted`] instead.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawTimelineEvent {
    /// Room the event was sent to.
    pub room_id: String,
    /// The event's id.
    pub event_id: String,
    /// The event's sender.
    pub sender: String,
    /// How the event reached us. Compared against the membership's origin: a
    /// reaction from a different device than the membership's is logged, and
    /// accepted, as Element Call accepts it.
    #[serde(default)]
    pub origin: EventOrigin,
    /// The event type: [`REACTION_EVENT_TYPE`] or [`ANNOTATION_EVENT_TYPE`].
    pub event_type: String,
    /// The event's `origin_server_ts`, which is when a hand counts as raised.
    pub origin_server_ts: u64,
    /// The event's whole `content` object, decrypted.
    pub content: Value,
}

/// Content of an `io.element.call.reaction` event.
pub fn build_reaction_content(membership_event_id: &str, emoji: &str, name: &str) -> Value {
    json!({
        "m.relates_to": {
            "rel_type": "m.reference",
            "event_id": membership_event_id,
        },
        "emoji": emoji,
        "name": name,
    })
}

/// Content of the `m.reaction` that raises a hand.
pub fn build_raised_hand_content(membership_event_id: &str) -> Value {
    json!({
        "m.relates_to": {
            "rel_type": "m.annotation",
            "event_id": membership_event_id,
            "key": RAISED_HAND_KEY,
        },
    })
}

/// What a timeline event says, once read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ParsedTimelineEvent {
    /// An emoji reaction to the membership event `target`.
    Reaction {
        target: String,
        emoji: String,
        name: String,
    },
    /// A raised hand on the membership event `target`.
    RaisedHand { target: String },
    /// Not a reaction of either kind, or one too malformed to act on.
    Irrelevant,
}

/// Reads a timeline event, without judging who sent it: that needs the roster.
pub(crate) fn parse_timeline_event(event: &RawTimelineEvent) -> ParsedTimelineEvent {
    let relation = event.content.get("m.relates_to");
    let rel_type = relation
        .and_then(|relation| relation.get("rel_type"))
        .and_then(Value::as_str);
    let target = relation
        .and_then(|relation| relation.get("event_id"))
        .and_then(Value::as_str);

    match event.event_type.as_str() {
        REACTION_EVENT_TYPE => {
            let (Some("m.reference"), Some(target)) = (rel_type, target) else {
                return ParsedTimelineEvent::Irrelevant;
            };
            let emoji = event
                .content
                .get("emoji")
                .and_then(Value::as_str)
                .map(first_grapheme)
                .unwrap_or("");
            if emoji.is_empty() {
                return ParsedTimelineEvent::Irrelevant;
            }
            let name = event
                .content
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("");
            ParsedTimelineEvent::Reaction {
                target: target.to_owned(),
                emoji: emoji.to_owned(),
                name: name.to_owned(),
            }
        }
        ANNOTATION_EVENT_TYPE => {
            let key = relation
                .and_then(|relation| relation.get("key"))
                .and_then(Value::as_str);
            match (rel_type, target, key) {
                (Some("m.annotation"), Some(target), Some(RAISED_HAND_KEY)) => {
                    ParsedTimelineEvent::RaisedHand {
                        target: target.to_owned(),
                    }
                }
                _ => ParsedTimelineEvent::Irrelevant,
            }
        }
        _ => ParsedTimelineEvent::Irrelevant,
    }
}

/// A member whose hand is up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaisedHand {
    /// `member.id` of the membership the hand belongs to.
    pub member_id: String,
    /// The member's user id.
    pub sender: String,
    /// The `m.reaction` event that raised it — the one to redact to lower it,
    /// if it is ours.
    pub reaction_event_id: String,
    /// When it was raised: the reaction's `origin_server_ts`. Sort by this to
    /// queue speakers in the order they asked.
    pub raised_at_ms: u64,
}

/// An emoji reaction a member just sent.
///
/// Transient: nothing here says when it ends. A host shows it for
/// [`ReactionsConfig::active_window_ms`] — the same window the session uses to
/// drop repeats — and plays `sound` if it plays reaction sounds at all.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceivedReaction {
    /// `member.id` of the reacting membership.
    pub member_id: String,
    /// The reacting user.
    pub sender: String,
    /// The emoji to show: the first grapheme of what was sent.
    pub emoji: String,
    /// The reaction's `name`, verbatim. Empty when the sender gave none.
    pub name: String,
    /// What to play for it.
    pub sound: ReactionSound,
}

/// A membership event whose annotations the host should fetch.
///
/// The initial raised-hand state of a call is not in the timeline we have
/// seen; it is in the relations of each member's membership event. The session
/// lists what it has not looked up yet, the host answers each with the
/// `/relations` of that event (`rel_type=m.annotation`,
/// `event_type=m.reaction`), fed back through
/// [`crate::RtcSessionManager::on_relations_received`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationLookup {
    /// The member whose event it is.
    pub member_id: String,
    /// Their current membership event.
    pub membership_event_id: String,
}

/// Why a reaction or hand could not be sent.
#[derive(Debug, Error)]
pub enum ReactionError {
    /// Reactions are turned off for this session.
    #[error("reactions are disabled for this session")]
    Disabled,
    /// Not joined, so there is no membership event to relate to.
    #[error("not joined: nothing to relate the reaction to")]
    NotJoined,
    /// No `(room, slot)` session exists.
    #[error("no such session")]
    NoSession,
    /// Sent too soon after our previous reaction.
    #[error("reaction sent too soon; {remaining_ms} ms of cooldown remain")]
    Cooldown {
        /// Milliseconds until a reaction can be sent again.
        remaining_ms: u64,
    },
    /// The emoji was empty once trimmed to its first grapheme.
    #[error("the reaction has no emoji")]
    EmptyEmoji,
    /// The host failed to send or redact.
    #[error("command failed: {0}")]
    Command(#[from] CommandError),
}

/// Milliseconds since the Unix epoch, injectable for tests.
pub(crate) type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Our own raised hand, and which membership event it is annotated on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnRaisedHand {
    /// The `m.reaction` we sent; redacted to lower the hand.
    pub(crate) reaction_event_id: String,
    /// The membership event it annotates. Once our membership event moves on
    /// (a sticky refresh), the hand has to be raised again on the new one.
    pub(crate) annotated_membership_event_id: String,
}

/// Everything one session knows about reactions.
///
/// Pure state: it never sends. The session decides *when* to send and feeds the
/// outcome back in, which keeps this testable without a command sender and
/// keeps the ordering rules (relate to the membership event id *as of now*) in
/// one place, the session.
pub(crate) struct ReactionsState {
    config: ReactionsConfig,
    clock: Clock,
    /// Raised hands by `member_id`. A `BTreeMap` so the published order is
    /// stable across identical states.
    raised_hands: BTreeMap<String, RaisedHand>,
    raised_hands_tx: watch::Sender<Vec<RaisedHand>>,
    reactions_tx: broadcast::Sender<ReceivedReaction>,
    /// When each member last reacted, for the active window.
    last_reaction_at: HashMap<String, u64>,
    /// Every membership event id seen for each member of the roster, latest
    /// last. A reaction is validated against any of them.
    known_membership_events: HashMap<String, Vec<String>>,
    /// Membership event ids whose relations have been looked up (or asked
    /// for; a failed lookup is the host's to retry by asking again).
    relations_fetched: HashSet<String>,
    /// When we last sent a reaction.
    last_sent_ms: Option<u64>,
    own_raised_hand: Option<OwnRaisedHand>,
}

/// How many reactions a slow subscriber may fall behind before it starts
/// missing some. A reaction is a three-second visual; lagging further than
/// this is not worth buffering for.
const REACTIONS_CHANNEL_CAPACITY: usize = 64;

impl ReactionsState {
    pub(crate) fn new() -> Self {
        let (raised_hands_tx, _) = watch::channel(Vec::new());
        let (reactions_tx, _) = broadcast::channel(REACTIONS_CHANNEL_CAPACITY);
        Self {
            config: ReactionsConfig::default(),
            clock: Arc::new(crate::own_membership::now_ms),
            raised_hands: BTreeMap::new(),
            raised_hands_tx,
            reactions_tx,
            last_reaction_at: HashMap::new(),
            known_membership_events: HashMap::new(),
            relations_fetched: HashSet::new(),
            last_sent_ms: None,
            own_raised_hand: None,
        }
    }

    pub(crate) fn configure(&mut self, config: ReactionsConfig) {
        self.config = config;
    }

    pub(crate) fn config(&self) -> &ReactionsConfig {
        &self.config
    }

    #[cfg(test)]
    pub(crate) fn set_clock(&mut self, clock: Clock) {
        self.clock = clock;
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    pub(crate) fn subscribe_raised_hands(&self) -> watch::Receiver<Vec<RaisedHand>> {
        self.raised_hands_tx.subscribe()
    }

    pub(crate) fn subscribe_reactions(&self) -> broadcast::Receiver<ReceivedReaction> {
        self.reactions_tx.subscribe()
    }

    /// The raised hands, oldest first.
    pub(crate) fn raised_hands(&self) -> Vec<RaisedHand> {
        let mut hands: Vec<RaisedHand> = self.raised_hands.values().cloned().collect();
        hands.sort_by(|a, b| {
            a.raised_at_ms
                .cmp(&b.raised_at_ms)
                .then_with(|| a.member_id.cmp(&b.member_id))
        });
        hands
    }

    fn publish_raised_hands(&self) {
        self.raised_hands_tx.send_replace(self.raised_hands());
    }

    /// Brings the per-member bookkeeping in line with the roster.
    ///
    /// Members gone from the roster lose their hand and their history; members
    /// whose membership event moved on get the new id remembered, so a hand
    /// raised on the old one stays valid and a hand raised on the new one is
    /// accepted too. Publishes the hands if any were dropped.
    pub(crate) fn sync_roster(&mut self, members: &[JoinedMembership]) {
        let present: HashSet<&str> = members.iter().map(|m| m.member_id.as_str()).collect();

        let before = self.raised_hands.len();
        self.raised_hands
            .retain(|member_id, _| present.contains(member_id.as_str()));
        self.last_reaction_at
            .retain(|member_id, _| present.contains(member_id.as_str()));
        self.known_membership_events
            .retain(|member_id, _| present.contains(member_id.as_str()));

        for member in members {
            let Some(event_id) = &member.membership_event_id else {
                continue;
            };
            let known = self
                .known_membership_events
                .entry(member.member_id.clone())
                .or_default();
            if known.last() != Some(event_id) {
                known.retain(|id| id != event_id);
                known.push(event_id.clone());
                if known.len() > KNOWN_MEMBERSHIP_EVENTS_PER_MEMBER {
                    known.remove(0);
                }
            }
        }

        if self.raised_hands.len() != before {
            self.publish_raised_hands();
        }
    }

    /// The member a relation to `target` belongs to, if `sender` is that
    /// member and `target` is one of their membership events.
    ///
    /// The sender check is the whole of the trust model, and it is enough: a
    /// membership event's sender is homeserver-authenticated, so nobody can
    /// react *as* another member. A reaction from a different device of the
    /// same user is logged and let through, as Element Call lets it through.
    fn resolve_target<'a>(
        &self,
        target: &str,
        event: &RawTimelineEvent,
        members: &'a [JoinedMembership],
    ) -> Option<&'a JoinedMembership> {
        let member = members.iter().find(|member| {
            member.membership_event_id.as_deref() == Some(target)
                || self
                    .known_membership_events
                    .get(&member.member_id)
                    .is_some_and(|known| known.iter().any(|id| id == target))
        })?;

        if member.sender != event.sender {
            log::warn!(
                "ignoring {} {} from {}: it relates to a membership event of {}",
                event.event_type,
                event.event_id,
                event.sender,
                member.sender,
            );
            return None;
        }

        if let (Some(member_device), Some(event_device)) = (
            member.origin.sender_device_id(),
            event.origin.sender_device_id(),
        ) && member_device != event_device
        {
            log::warn!(
                "{} {} from {} was sent by device {event_device}, but their membership by \
                 {member_device}; accepting it anyway",
                event.event_type,
                event.event_id,
                event.sender,
            );
        }

        Some(member)
    }

    /// Applies one timeline event.
    ///
    /// `backfill` marks events fetched from the relations of a membership event
    /// rather than seen live. Those may be arbitrarily old, so only raised hands
    /// are taken from them — replaying somebody's applause from an hour ago
    /// would be wrong.
    pub(crate) fn ingest(
        &mut self,
        event: &RawTimelineEvent,
        members: &[JoinedMembership],
        backfill: bool,
    ) {
        if !self.config.enabled {
            return;
        }

        match parse_timeline_event(event) {
            ParsedTimelineEvent::Irrelevant => {}
            ParsedTimelineEvent::Reaction {
                target,
                emoji,
                name,
            } => {
                if backfill {
                    return;
                }
                let Some(member) = self.resolve_target(&target, event, members) else {
                    return;
                };
                let now = self.now();
                if let Some(last) = self.last_reaction_at.get(&member.member_id)
                    && now.saturating_sub(*last) < self.config.active_window_ms
                {
                    log::debug!(
                        "dropping reaction {} from {}: one of theirs is still active",
                        event.event_id,
                        member.member_id,
                    );
                    return;
                }
                self.last_reaction_at.insert(member.member_id.clone(), now);
                log::debug!(
                    "reaction {emoji} ({name}) from {} ({})",
                    member.sender,
                    member.member_id,
                );
                // A send fails only when nobody is listening, which is fine.
                let _ = self.reactions_tx.send(ReceivedReaction {
                    member_id: member.member_id.clone(),
                    sender: member.sender.clone(),
                    emoji,
                    sound: sound_for(&name),
                    name,
                });
            }
            ParsedTimelineEvent::RaisedHand { target } => {
                let Some(member) = self.resolve_target(&target, event, members) else {
                    return;
                };
                let hand = RaisedHand {
                    member_id: member.member_id.clone(),
                    sender: member.sender.clone(),
                    reaction_event_id: event.event_id.clone(),
                    raised_at_ms: event.origin_server_ts,
                };
                match self.raised_hands.get(&member.member_id) {
                    // Our own local echo, or a replay of an event already
                    // applied: nothing moved.
                    Some(existing) if existing.reaction_event_id == hand.reaction_event_id => {}
                    // Two annotations for one member — the re-annotation after a
                    // sticky refresh, or a peer raising twice. The earlier one
                    // is when they *asked*, which is what the ordering is for;
                    // the later event id is the one a redaction will name.
                    Some(existing) => {
                        let raised_at_ms = existing.raised_at_ms.min(hand.raised_at_ms);
                        let newest = if hand.raised_at_ms >= existing.raised_at_ms {
                            hand
                        } else {
                            existing.clone()
                        };
                        self.raised_hands.insert(
                            member.member_id.clone(),
                            RaisedHand {
                                raised_at_ms,
                                ..newest
                            },
                        );
                        self.publish_raised_hands();
                    }
                    None => {
                        log::info!("hand raised by {} ({})", member.sender, member.member_id);
                        self.raised_hands.insert(member.member_id.clone(), hand);
                        self.publish_raised_hands();
                    }
                }
            }
        }
    }

    /// Lowers whichever hand `event_id` raised, if any. Returns whether it was
    /// ours.
    pub(crate) fn on_event_redacted(&mut self, event_id: &str) -> bool {
        let lowered: Vec<String> = self
            .raised_hands
            .iter()
            .filter(|(_, hand)| hand.reaction_event_id == event_id)
            .map(|(member_id, _)| member_id.clone())
            .collect();
        for member_id in &lowered {
            log::info!("hand lowered by {member_id}");
            self.raised_hands.remove(member_id);
        }
        if !lowered.is_empty() {
            self.publish_raised_hands();
        }

        let was_ours = self
            .own_raised_hand
            .as_ref()
            .is_some_and(|own| own.reaction_event_id == event_id);
        if was_ours {
            self.own_raised_hand = None;
        }
        was_ours
    }

    /// Records that `target`'s relations were fetched and applies them.
    pub(crate) fn on_relations_received(
        &mut self,
        target: &str,
        events: &[RawTimelineEvent],
        members: &[JoinedMembership],
    ) {
        self.relations_fetched.insert(target.to_owned());
        for event in events {
            self.ingest(event, members, true);
        }
    }

    /// Membership events whose annotations have not been fetched yet.
    ///
    /// Our own membership is excluded: we know whether our hand is up.
    pub(crate) fn pending_relation_lookups(
        &self,
        members: &[JoinedMembership],
        own_member_id: Option<&str>,
    ) -> Vec<RelationLookup> {
        if !self.config.enabled {
            return Vec::new();
        }
        members
            .iter()
            .filter(|member| Some(member.member_id.as_str()) != own_member_id)
            .filter_map(|member| {
                let event_id = member.membership_event_id.as_ref()?;
                (!self.relations_fetched.contains(event_id)).then(|| RelationLookup {
                    member_id: member.member_id.clone(),
                    membership_event_id: event_id.clone(),
                })
            })
            .collect()
    }

    /// Whether we may send a reaction now.
    pub(crate) fn check_send_allowed(&self) -> Result<(), ReactionError> {
        if !self.config.enabled {
            return Err(ReactionError::Disabled);
        }
        if let Some(last) = self.last_sent_ms {
            let elapsed = self.now().saturating_sub(last);
            if elapsed < self.config.send_cooldown_ms {
                return Err(ReactionError::Cooldown {
                    remaining_ms: self.config.send_cooldown_ms - elapsed,
                });
            }
        }
        Ok(())
    }

    pub(crate) fn record_sent(&mut self) {
        self.last_sent_ms = Some(self.now());
    }

    pub(crate) fn own_raised_hand(&self) -> Option<&OwnRaisedHand> {
        self.own_raised_hand.as_ref()
    }

    /// Records that we raised our hand with `reaction_event_id` on
    /// `membership_event_id`, and shows it locally right away rather than
    /// waiting for the echo.
    pub(crate) fn set_own_raised_hand(
        &mut self,
        own_member_id: &str,
        own_user_id: &str,
        reaction_event_id: String,
        membership_event_id: String,
    ) {
        let raised_at_ms = self
            .raised_hands
            .get(own_member_id)
            .map(|hand| hand.raised_at_ms)
            .unwrap_or_else(|| self.now());
        self.raised_hands.insert(
            own_member_id.to_owned(),
            RaisedHand {
                member_id: own_member_id.to_owned(),
                sender: own_user_id.to_owned(),
                reaction_event_id: reaction_event_id.clone(),
                raised_at_ms,
            },
        );
        self.own_raised_hand = Some(OwnRaisedHand {
            reaction_event_id,
            annotated_membership_event_id: membership_event_id,
        });
        self.publish_raised_hands();
    }

    /// Forgets our hand, locally lowering it.
    pub(crate) fn clear_own_raised_hand(&mut self, own_member_id: &str) {
        self.own_raised_hand = None;
        if self.raised_hands.remove(own_member_id).is_some() {
            self.publish_raised_hands();
        }
    }

    /// Forgets everything about our own participation. Peers' hands stay: the
    /// session outlives a leave and its roster does too.
    pub(crate) fn reset_own(&mut self) {
        self.own_raised_hand = None;
        self.last_sent_ms = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(event_type: &str, sender: &str, content: Value) -> RawTimelineEvent {
        RawTimelineEvent {
            room_id: "!room:example.org".to_owned(),
            event_id: "$reaction".to_owned(),
            sender: sender.to_owned(),
            origin: EventOrigin::Unknown,
            event_type: event_type.to_owned(),
            origin_server_ts: 1_000,
            content,
        }
    }

    #[test]
    fn reaction_content_is_what_element_call_sends() {
        let content = build_reaction_content("$member", "👏", "clapping");
        assert_eq!(
            content,
            json!({
                "m.relates_to": { "rel_type": "m.reference", "event_id": "$member" },
                "emoji": "👏",
                "name": "clapping",
            })
        );
    }

    #[test]
    fn raised_hand_content_is_what_element_call_sends() {
        let content = build_raised_hand_content("$member");
        assert_eq!(
            content,
            json!({
                "m.relates_to": {
                    "rel_type": "m.annotation",
                    "event_id": "$member",
                    "key": "🖐️",
                },
            })
        );
        // The exact bytes matter: a peer compares strings.
        assert_eq!(RAISED_HAND_KEY.encode_utf16().count(), 3);
        assert_eq!(
            RAISED_HAND_KEY.chars().collect::<Vec<_>>(),
            ['\u{1F590}', '\u{FE0F}']
        );
    }

    #[test]
    fn our_own_content_parses_back() {
        let reaction = event(
            REACTION_EVENT_TYPE,
            "@alice:example.org",
            build_reaction_content("$member", "🎉", "party"),
        );
        assert_eq!(
            parse_timeline_event(&reaction),
            ParsedTimelineEvent::Reaction {
                target: "$member".to_owned(),
                emoji: "🎉".to_owned(),
                name: "party".to_owned(),
            }
        );

        let hand = event(
            ANNOTATION_EVENT_TYPE,
            "@alice:example.org",
            build_raised_hand_content("$member"),
        );
        assert_eq!(
            parse_timeline_event(&hand),
            ParsedTimelineEvent::RaisedHand {
                target: "$member".to_owned()
            }
        );
    }

    #[test]
    fn only_the_first_grapheme_of_the_emoji_is_kept() {
        assert_eq!(first_grapheme("😵‍💫👍"), "😵‍💫");
        assert_eq!(first_grapheme("  👍 "), "👍");
        assert_eq!(first_grapheme("   "), "");

        let reaction = event(
            REACTION_EVENT_TYPE,
            "@alice:example.org",
            json!({
                "m.relates_to": { "rel_type": "m.reference", "event_id": "$member" },
                "emoji": "👍👍👍 spam",
                "name": "thumbsup",
            }),
        );
        assert_eq!(
            parse_timeline_event(&reaction),
            ParsedTimelineEvent::Reaction {
                target: "$member".to_owned(),
                emoji: "👍".to_owned(),
                name: "thumbsup".to_owned(),
            }
        );
    }

    #[test]
    fn malformed_and_foreign_events_are_irrelevant() {
        let no_emoji = event(
            REACTION_EVENT_TYPE,
            "@alice:example.org",
            json!({ "m.relates_to": { "rel_type": "m.reference", "event_id": "$member" } }),
        );
        assert_eq!(
            parse_timeline_event(&no_emoji),
            ParsedTimelineEvent::Irrelevant
        );

        let wrong_relation = event(
            REACTION_EVENT_TYPE,
            "@alice:example.org",
            json!({
                "m.relates_to": { "rel_type": "m.annotation", "event_id": "$member" },
                "emoji": "👍",
            }),
        );
        assert_eq!(
            parse_timeline_event(&wrong_relation),
            ParsedTimelineEvent::Irrelevant
        );

        let thumbs_up_annotation = event(
            ANNOTATION_EVENT_TYPE,
            "@alice:example.org",
            json!({
                "m.relates_to": { "rel_type": "m.annotation", "event_id": "$member", "key": "👍" },
            }),
        );
        assert_eq!(
            parse_timeline_event(&thumbs_up_annotation),
            ParsedTimelineEvent::Irrelevant
        );

        let message = event(
            "m.room.message",
            "@alice:example.org",
            json!({ "body": "🖐️" }),
        );
        assert_eq!(
            parse_timeline_event(&message),
            ParsedTimelineEvent::Irrelevant
        );
    }

    #[test]
    fn sounds_follow_element_calls_table() {
        assert_eq!(
            sound_for("clapping"),
            ReactionSound::Named("clap".to_owned())
        );
        assert_eq!(
            sound_for("drum"),
            ReactionSound::Named("baduntss".to_owned())
        );
        assert_eq!(sound_for("thumbsup"), ReactionSound::None);
        assert_eq!(sound_for("something-new"), ReactionSound::Generic);
        assert_eq!(sound_for(""), ReactionSound::Generic);
        assert_eq!(ReactionSound::Generic.asset_name(), Some("generic"));
        assert_eq!(ReactionSound::None.asset_name(), None);
    }

    // ---- Through the session ----

    use std::sync::atomic::{AtomicU64, Ordering};

    use tokio::sync::broadcast::error::TryRecvError;

    use crate::commands::MockCommandSender;
    use crate::event::{RawStickyEvent, RawStickyEventContent};
    use crate::join::{JoinSessionParams, LeaveSessionParams};
    use crate::session::{CallMembershipEvent, MemberInfo, Membership, RtcSession};
    use crate::transport::{LiveKitTransport, RtcTransport};

    const ROOM: &str = "!room:example.org";
    const SLOT: &str = "m.call#ROOM";
    const ALICE: &str = "@alice:example.org";
    const BOB: &str = "@bob:example.org";
    const BOB_MEMBER: &str = "bob-member-1";

    /// A clock the test moves by hand.
    struct TestClock(Arc<AtomicU64>);

    impl TestClock {
        fn new(start: u64) -> Self {
            Self(Arc::new(AtomicU64::new(start)))
        }

        fn clock(&self) -> Clock {
            let time = self.0.clone();
            Arc::new(move || time.load(Ordering::SeqCst))
        }

        fn advance(&self, ms: u64) {
            self.0.fetch_add(ms, Ordering::SeqCst);
        }
    }

    fn member_event(
        sender: &str,
        device: &str,
        member_id: &str,
        event_id: &str,
    ) -> CallMembershipEvent {
        RawStickyEvent {
            room_id: ROOM.to_owned(),
            event_id: Some(event_id.to_owned()),
            sender: sender.to_owned(),
            origin: EventOrigin::encrypted(Some(device.to_owned())),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: SLOT.to_owned(),
                sticky_key: member_id.to_owned(),
                member: MemberInfo {
                    id: Some(member_id.to_owned()),
                    membership: Some(Membership::Join),
                },
                application: crate::session::ApplicationInfo {
                    application_type: Some("m.call".to_owned()),
                    extra: Default::default(),
                },
                transports: None,
                leave_reason: None,
                created_ts: None,
            },
        }
        .try_into_call_membership_event()
        .expect("a join-shaped event converts")
    }

    fn join_params() -> JoinSessionParams {
        JoinSessionParams::new(
            ALICE.to_owned(),
            "ALICEDEV".to_owned(),
            ROOM.to_owned(),
            SLOT.to_owned(),
            "m.call".to_owned(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://sfu.example.org".to_owned(),
            }),
        )
    }

    /// Alice joined (her membership event is `$sticky-1`), Bob in the roster
    /// with membership event `$bob-member-1`.
    async fn joined_session(
        params: JoinSessionParams,
    ) -> (RtcSession<MockCommandSender>, Arc<MockCommandSender>) {
        let sender = Arc::new(MockCommandSender::new());
        let mut session = RtcSession::with_command_sender(sender.clone());
        let own_member_id = params.membership_id();
        let params = JoinSessionParams {
            membership_id: Some(own_member_id.clone()),
            ..params
        };
        session.join(params).await.expect("join succeeds");
        assert_eq!(
            session.own_membership_event_id().as_deref(),
            Some("$sticky-1")
        );
        session
            .set_current_state(vec![
                member_event(ALICE, "ALICEDEV", &own_member_id, "$sticky-1"),
                member_event(BOB, "BOBDEV", BOB_MEMBER, "$bob-member-1"),
            ])
            .await;
        assert_eq!(session.member_count(), 2);
        (session, sender)
    }

    fn timeline_event(
        event_id: &str,
        sender: &str,
        event_type: &str,
        ts: u64,
        content: Value,
    ) -> RawTimelineEvent {
        RawTimelineEvent {
            room_id: ROOM.to_owned(),
            event_id: event_id.to_owned(),
            sender: sender.to_owned(),
            origin: EventOrigin::Unknown,
            event_type: event_type.to_owned(),
            origin_server_ts: ts,
            content,
        }
    }

    fn bob_reacts(event_id: &str, target: &str, emoji: &str, name: &str) -> RawTimelineEvent {
        timeline_event(
            event_id,
            BOB,
            REACTION_EVENT_TYPE,
            1_000,
            build_reaction_content(target, emoji, name),
        )
    }

    fn bob_raises(event_id: &str, target: &str, ts: u64) -> RawTimelineEvent {
        timeline_event(
            event_id,
            BOB,
            ANNOTATION_EVENT_TYPE,
            ts,
            build_raised_hand_content(target),
        )
    }

    fn hands(session: &RtcSession<MockCommandSender>) -> Vec<(&'static str, String)> {
        session
            .raised_hands()
            .into_iter()
            .map(|hand| {
                let who = if hand.sender == BOB { "bob" } else { "alice" };
                (who, hand.reaction_event_id)
            })
            .collect()
    }

    #[tokio::test]
    async fn a_peers_reaction_is_surfaced_with_its_sound() {
        let (mut session, _) = joined_session(join_params()).await;
        let mut reactions = session.subscribe_reactions();

        session.on_timeline_event(&bob_reacts("$r1", "$bob-member-1", "👏", "clapping"));

        let received = reactions.try_recv().expect("one reaction");
        assert_eq!(received.member_id, BOB_MEMBER);
        assert_eq!(received.sender, BOB);
        assert_eq!(received.emoji, "👏");
        assert_eq!(received.name, "clapping");
        assert_eq!(received.sound, ReactionSound::Named("clap".to_owned()));
    }

    #[tokio::test]
    async fn a_reaction_is_only_accepted_from_the_member_it_relates_to() {
        let (mut session, _) = joined_session(join_params()).await;
        let mut reactions = session.subscribe_reactions();

        // Carol reacting "as" Bob.
        let mut forged = bob_reacts("$r1", "$bob-member-1", "👏", "clapping");
        forged.sender = "@carol:example.org".to_owned();
        session.on_timeline_event(&forged);
        // Bob relating to an event that is nobody's membership.
        session.on_timeline_event(&bob_reacts("$r2", "$not-a-membership", "👏", "clapping"));
        // Bob raising a hand on Alice's membership.
        session.on_timeline_event(&bob_raises("$h1", "$sticky-1", 5));

        assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);
        assert!(session.raised_hands().is_empty());
    }

    #[tokio::test]
    async fn a_repeat_inside_the_active_window_is_dropped() {
        let (mut session, _) = joined_session(join_params()).await;
        let clock = TestClock::new(10_000);
        session.set_reactions_clock(clock.clock());
        let mut reactions = session.subscribe_reactions();

        session.on_timeline_event(&bob_reacts("$r1", "$bob-member-1", "👏", "clapping"));
        clock.advance(1_000);
        session.on_timeline_event(&bob_reacts("$r2", "$bob-member-1", "🎉", "party"));
        assert_eq!(reactions.try_recv().unwrap().emoji, "👏");
        assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);

        clock.advance(2_000);
        session.on_timeline_event(&bob_reacts("$r3", "$bob-member-1", "🎉", "party"));
        assert_eq!(reactions.try_recv().unwrap().emoji, "🎉");
    }

    #[tokio::test]
    async fn sending_relates_to_our_membership_and_honours_the_cooldown() {
        let (mut session, sender) = joined_session(join_params()).await;
        let clock = TestClock::new(10_000);
        session.set_reactions_clock(clock.clock());

        let event_id = session
            .send_reaction("🎉 and more", "party")
            .await
            .expect("first reaction goes out");
        assert_eq!(event_id, "$room-1");
        let sent = sender.room_events.lock().unwrap().clone();
        assert_eq!(
            sent,
            vec![(
                ROOM.to_owned(),
                REACTION_EVENT_TYPE.to_owned(),
                build_reaction_content("$sticky-1", "🎉", "party"),
            )]
        );

        clock.advance(1_000);
        match session.send_reaction("👏", "clapping").await {
            Err(ReactionError::Cooldown { remaining_ms }) => assert_eq!(remaining_ms, 2_000),
            other => panic!("expected a cooldown, got {other:?}"),
        }
        assert_eq!(sender.room_events.lock().unwrap().len(), 1);

        clock.advance(2_000);
        session
            .send_reaction("👏", "clapping")
            .await
            .expect("the cooldown has passed");
        assert_eq!(sender.room_events.lock().unwrap().len(), 2);

        assert!(matches!(
            session.send_reaction("   ", "nothing").await,
            Err(ReactionError::EmptyEmoji)
        ));
    }

    #[tokio::test]
    async fn raising_is_idempotent_and_lowering_redacts_the_annotation() {
        let (mut session, sender) = joined_session(join_params()).await;
        let mut watch = session.subscribe_raised_hands();

        session.raise_hand().await.expect("raise");
        let sent = sender.room_events.lock().unwrap().clone();
        assert_eq!(
            sent,
            vec![(
                ROOM.to_owned(),
                ANNOTATION_EVENT_TYPE.to_owned(),
                build_raised_hand_content("$sticky-1"),
            )]
        );
        // Shown locally at once, before any echo.
        assert_eq!(hands(&session), vec![("alice", "$room-1".to_owned())]);
        assert!(watch.has_changed().unwrap());
        assert_eq!(watch.borrow_and_update().len(), 1);

        session.raise_hand().await.expect("raise again");
        assert_eq!(
            sender.room_events.lock().unwrap().len(),
            1,
            "nothing re-sent"
        );

        // The echo of our own annotation changes nothing.
        session.on_timeline_event(&timeline_event(
            "$room-1",
            ALICE,
            ANNOTATION_EVENT_TYPE,
            2_000,
            build_raised_hand_content("$sticky-1"),
        ));
        assert_eq!(hands(&session), vec![("alice", "$room-1".to_owned())]);
        assert!(!watch.has_changed().unwrap());

        session.lower_hand().await.expect("lower");
        assert_eq!(
            sender.redactions.lock().unwrap().clone(),
            vec![(ROOM.to_owned(), "$room-1".to_owned(), None)]
        );
        assert!(session.raised_hands().is_empty());

        session.lower_hand().await.expect("lowering twice is fine");
        assert_eq!(sender.redactions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_peers_hand_stays_across_their_refresh_and_goes_with_them() {
        let (mut session, _) = joined_session(join_params()).await;
        let own_member_id = session.own_member_id().unwrap().to_owned();

        // Before anything was fetched, Bob's membership event wants a lookup
        // and ours does not.
        assert_eq!(
            session.pending_relation_lookups(),
            vec![RelationLookup {
                member_id: BOB_MEMBER.to_owned(),
                membership_event_id: "$bob-member-1".to_owned(),
            }]
        );
        session.on_relations_received("$bob-member-1", &[]);
        assert!(session.pending_relation_lookups().is_empty());

        session.on_timeline_event(&bob_raises("$h1", "$bob-member-1", 5_000));
        assert_eq!(hands(&session), vec![("bob", "$h1".to_owned())]);

        // Bob's sticky refresh moves his membership event on.
        session
            .set_current_state(vec![
                member_event(ALICE, "ALICEDEV", &own_member_id, "$sticky-1"),
                member_event(BOB, "BOBDEV", BOB_MEMBER, "$bob-member-2"),
            ])
            .await;
        assert_eq!(
            hands(&session),
            vec![("bob", "$h1".to_owned())],
            "a hand outlives a refresh"
        );
        assert_eq!(
            session.pending_relation_lookups(),
            vec![RelationLookup {
                member_id: BOB_MEMBER.to_owned(),
                membership_event_id: "$bob-member-2".to_owned(),
            }],
            "the new event's annotations are looked up"
        );

        // A reaction relating to the previous event is still his.
        let mut reactions = session.subscribe_reactions();
        session.on_timeline_event(&bob_reacts("$r1", "$bob-member-1", "🐶", "dog"));
        assert_eq!(reactions.try_recv().unwrap().member_id, BOB_MEMBER);

        // Bob leaves: hand gone.
        session
            .set_current_state(vec![member_event(
                ALICE,
                "ALICEDEV",
                &own_member_id,
                "$sticky-1",
            )])
            .await;
        assert!(session.raised_hands().is_empty());
    }

    #[tokio::test]
    async fn backfill_restores_hands_but_never_replays_reactions() {
        let (mut session, _) = joined_session(join_params()).await;
        let mut reactions = session.subscribe_reactions();

        session.on_relations_received(
            "$bob-member-1",
            &[
                bob_reacts("$old-reaction", "$bob-member-1", "👏", "clapping"),
                bob_raises("$old-hand", "$bob-member-1", 100),
            ],
        );

        assert_eq!(hands(&session), vec![("bob", "$old-hand".to_owned())]);
        assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    #[tokio::test]
    async fn a_redaction_lowers_the_hand_it_raised() {
        let (mut session, _) = joined_session(join_params()).await;
        session.on_timeline_event(&bob_raises("$h1", "$bob-member-1", 5_000));
        assert_eq!(hands(&session).len(), 1);

        session.on_event_redacted("$something-else");
        assert_eq!(hands(&session).len(), 1);

        session.on_event_redacted("$h1");
        assert!(session.raised_hands().is_empty());
    }

    #[tokio::test]
    async fn hands_are_ordered_by_when_they_were_raised() {
        let (mut session, _) = joined_session(join_params()).await;
        session.raise_hand().await.expect("raise");
        // Bob's hand went up before ours, by the server's clock.
        session.on_timeline_event(&bob_raises("$h1", "$bob-member-1", 1));

        let order: Vec<&str> = hands(&session).iter().map(|(who, _)| *who).collect();
        assert_eq!(order, vec!["bob", "alice"]);
    }

    #[tokio::test]
    async fn the_hand_follows_our_membership_event_across_a_refresh() {
        // A zero lifetime makes every heartbeat refresh the sticky membership.
        let params = JoinSessionParams {
            sticky_duration_ms: Some(0),
            ..join_params()
        };
        let (mut session, sender) = joined_session(params).await;
        session.raise_hand().await.expect("raise");
        assert_eq!(
            session
                .own_raised_hand()
                .unwrap()
                .annotated_membership_event_id,
            "$sticky-1"
        );

        assert!(session.heartbeat().await);

        assert_eq!(
            session.own_membership_event_id().as_deref(),
            Some("$sticky-2"),
            "the heartbeat refreshed the membership"
        );
        let sent = sender.room_events.lock().unwrap().clone();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[1].2, build_raised_hand_content("$sticky-2"));
        assert_eq!(
            sender.redactions.lock().unwrap().clone(),
            vec![(ROOM.to_owned(), "$room-1".to_owned(), None)],
            "the annotation on the old membership event is redacted"
        );
        assert_eq!(hands(&session), vec![("alice", "$room-2".to_owned())]);
        let own = session.own_raised_hand().unwrap();
        assert_eq!(own.annotated_membership_event_id, "$sticky-2");
        assert_eq!(own.reaction_event_id, "$room-2");

        // Lowering redacts the current annotation, not the superseded one.
        session.lower_hand().await.expect("lower");
        assert_eq!(sender.redactions.lock().unwrap()[1].1, "$room-2");
    }

    #[tokio::test]
    async fn leaving_lowers_our_hand_first() {
        let (mut session, sender) = joined_session(join_params()).await;
        session.raise_hand().await.expect("raise");

        session
            .leave(LeaveSessionParams::new())
            .await
            .expect("leave");

        assert_eq!(
            sender.redactions.lock().unwrap().clone(),
            vec![(ROOM.to_owned(), "$room-1".to_owned(), None)]
        );
        assert!(session.own_raised_hand().is_none());
        assert!(matches!(
            session.raise_hand().await,
            Err(ReactionError::NotJoined)
        ));
    }

    #[tokio::test]
    async fn disabled_reactions_neither_send_nor_receive() {
        let params = JoinSessionParams {
            reactions: Some(ReactionsConfig {
                enabled: false,
                ..ReactionsConfig::default()
            }),
            ..join_params()
        };
        let (mut session, sender) = joined_session(params).await;
        let mut reactions = session.subscribe_reactions();

        assert!(matches!(
            session.send_reaction("👏", "clapping").await,
            Err(ReactionError::Disabled)
        ));
        assert!(matches!(
            session.raise_hand().await,
            Err(ReactionError::Disabled)
        ));
        assert!(sender.room_events.lock().unwrap().is_empty());

        session.on_timeline_event(&bob_reacts("$r1", "$bob-member-1", "👏", "clapping"));
        session.on_timeline_event(&bob_raises("$h1", "$bob-member-1", 5_000));
        assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);
        assert!(session.raised_hands().is_empty());
        assert!(session.pending_relation_lookups().is_empty());
    }

    /// Two slots in one room: a reaction names no slot, so the manager offers
    /// it to both sessions and only the one holding the member keeps it.
    #[tokio::test]
    async fn the_manager_routes_a_rooms_reactions_to_the_session_holding_the_member() {
        use crate::manager::RtcSessionManager;

        let sender = Arc::new(MockCommandSender::new());
        let mut manager = RtcSessionManager::with_command_sender(sender);
        let other_slot = "m.call#OTHER";

        let in_room_slot = join_params();
        let in_other_slot = JoinSessionParams {
            slot_id: other_slot.to_owned(),
            ..join_params()
        };
        let alice_a = in_room_slot.membership_id();
        let alice_b = in_other_slot.membership_id();
        manager
            .join(JoinSessionParams {
                membership_id: Some(alice_a.clone()),
                ..in_room_slot
            })
            .await
            .expect("join slot A");
        manager
            .join(JoinSessionParams {
                membership_id: Some(alice_b.clone()),
                ..in_other_slot
            })
            .await
            .expect("join slot B");

        let mut bob_in_a = member_event(BOB, "BOBDEV", BOB_MEMBER, "$bob-member-1");
        let CallMembershipEvent::Joined(joined) = &mut bob_in_a else {
            unreachable!()
        };
        let bob_raw = RawStickyEvent {
            room_id: ROOM.to_owned(),
            event_id: joined.membership_event_id.clone(),
            sender: BOB.to_owned(),
            origin: joined.origin.clone(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: SLOT.to_owned(),
                sticky_key: BOB_MEMBER.to_owned(),
                member: MemberInfo {
                    id: Some(BOB_MEMBER.to_owned()),
                    membership: Some(Membership::Join),
                },
                application: crate::session::ApplicationInfo {
                    application_type: Some("m.call".to_owned()),
                    extra: Default::default(),
                },
                transports: None,
                leave_reason: None,
                created_ts: None,
            },
        };
        manager
            .set_current_sticky_state(ROOM, vec![bob_raw])
            .await
            .expect("state applies");
        assert_eq!(manager.member_count(ROOM, SLOT), Some(1));
        assert_eq!(manager.member_count(ROOM, other_slot), Some(0));

        let mut slot_a = manager.subscribe_reactions(ROOM, SLOT).unwrap();
        let mut slot_b = manager.subscribe_reactions(ROOM, other_slot).unwrap();

        manager.on_room_timeline_events(
            ROOM,
            &[
                bob_reacts("$r1", "$bob-member-1", "👏", "clapping"),
                bob_raises("$h1", "$bob-member-1", 5_000),
            ],
        );

        assert_eq!(slot_a.try_recv().unwrap().member_id, BOB_MEMBER);
        assert_eq!(slot_b.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(manager.raised_hands(ROOM, SLOT).unwrap().len(), 1);
        assert!(manager.raised_hands(ROOM, other_slot).unwrap().is_empty());
        assert_eq!(
            manager.pending_relation_lookups(ROOM),
            vec![RelationLookup {
                member_id: BOB_MEMBER.to_owned(),
                membership_event_id: "$bob-member-1".to_owned(),
            }]
        );

        manager.on_event_redacted(ROOM, "$h1");
        assert!(manager.raised_hands(ROOM, SLOT).unwrap().is_empty());

        assert!(matches!(
            manager.raise_hand(ROOM, "m.call#NOWHERE").await,
            Err(ReactionError::NoSession)
        ));
    }

    #[test]
    fn the_catalogue_has_unique_names_and_emoji() {
        let names: HashSet<&str> = KNOWN_REACTIONS.iter().map(|kind| kind.name).collect();
        assert_eq!(names.len(), KNOWN_REACTIONS.len());
        let emoji: HashSet<&str> = KNOWN_REACTIONS.iter().map(|kind| kind.emoji).collect();
        assert_eq!(emoji.len(), KNOWN_REACTIONS.len());
        for kind in KNOWN_REACTIONS {
            assert_eq!(
                first_grapheme(kind.emoji),
                kind.emoji,
                "{} is one grapheme",
                kind.name
            );
        }
    }
}
