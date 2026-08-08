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

//! The Element Call membership format from *before* MSC4354: MatrixRTC
//! membership as `org.matrix.msc3401.call.member` **room state**.
//!
//! One generation older than [`super::element_call`], which handles the sticky
//! dialect. Both are Element Call; the difference is where the membership
//! lives. A deployment speaks one or the other, never both, so the two files
//! barely interact and either can be deleted without touching the other.
//!
//! Depends on nothing but `serde_json`, for the same reasons as its sibling: the
//! whole translation is unit-testable against captured payloads, and deleting it
//! cannot break a signature anywhere else.
//!
//! # Why this one is opt-in in *both* directions
//!
//! Reading the sticky dialect needs no flag because every rule there is "the
//! modern field is absent and the legacy one is present" — a spec-shaped event
//! comes out byte-identical, so there is nothing a flag would protect. Nothing
//! about this module is like that. It reads a *different event type*, in a
//! *different part of the room*, with a lifetime the content states rather than
//! the homeserver enforcing. Left on everywhere, any room that ever hosted an
//! old Element Call — which is most rooms that ever hosted one at all — would
//! show a call that ended months ago.
//!
//! And unlike the sticky dialect, the outbound half cannot be additive. That one
//! adds legacy fields beside the spec ones so a single event serves both
//! readers. Here the membership is not a timeline event at all, our SFU identity
//! is derived differently, and the token comes from a different endpoint. A call
//! joined this way is visible to this generation of Element Call and to nobody
//! else, which is why it lives behind
//! [`ElementCallCompat::StateEvents`](super::ElementCallCompat::StateEvents).
//!
//! # The format, field by field
//!
//! State key `_{user}_{device}_{application}{call_id}`, e.g.
//! `_@alice:example.io_V5cP8FErcB_m.call`. Inbound it is **not parsed**:
//! everything it encodes is also in the content or in the event's own
//! homeserver-stamped `sender`, and the grammar has two legal spellings (with
//! and without the leading underscore). Trusting the content and the `sender`
//! avoids the question entirely.
//!
//! | | Element Call, pre-sticky | MSC4143 today |
//! |---|---|---|
//! | carrier | `org.matrix.msc3401.call.member` state event | `m.rtc.member` sticky event |
//! | session | `application: "m.call"` (a string) + `call_id` + `scope` | `slot_id: "m.call#ROOM"` |
//! | member id | `membershipID` | `member.id` (== the sticky key) |
//! | device | `device_id` in the content, self-asserted | the device that encrypted the event |
//! | join | any non-empty content | `member.membership: "join"` |
//! | leave | content becomes `{}` | `membership: "leave"` + `leave_reason` |
//! | lifetime | `created_ts + expires`, checked by every reader | the homeserver's sticky TTL |
//! | transport | `foci_preferred` + `focus_active.focus_selection` | `transports.{published,can_subscribe}` |
//! | SFU identity | the plain `membershipID` string | `base64(SHA256([user, device, member_id]))` |
//!
//! # Two things the translation has to resolve, because nothing downstream can
//!
//! **Expiry.** A sticky entry lapses at the homeserver and simply stops being
//! reported, so the core has no expiry logic at all — nowhere to put a deadline
//! and no timer to fire it. Here the deadline is in the content, so an expired
//! membership has to be dropped *here* or it becomes a permanent ghost. The
//! deadline is `created_ts + expires`, falling back to the event's
//! `origin_server_ts` when the content states no `created_ts` — which is exactly
//! the first join of a session, where the client cannot know the timestamp yet.
//! Ruma reads it the same way (`MembershipData::expires_ts`).
//!
//! **The active focus.** `focus_active.focus_selection: "oldest_membership"`
//! means every member uses the *oldest* member's first preferred focus, so which
//! SFU a peer is on is a property of the room's whole membership rather than of
//! that peer's own event. The core has no such concept and must not grow one —
//! the MSC4195 model is that a membership names its own transports — so the
//! cross-member resolution happens here and each translated membership comes out
//! already naming the one transport it actually publishes on. Getting this wrong
//! is silent: we connect to a different SFU and hear nobody.
//!
//! # Not handled: the `memberships` array
//!
//! The original MSC3401 content was one state event per *user* holding an array
//! of per-device memberships (ruma's `LegacyMembershipContent`). That is two
//! generations back, not one, and no deployment we test against still writes it.
//! Such a content is dropped with a log line rather than half-supported.
//!
//! # Why not ruma's `CallMemberEventContent`
//!
//! It exists (`unstable-msc3401` is on) and it is the wrong tool. It is
//! `#[serde(untagged)]` over three shapes, and the one that matches this format,
//! `SessionMembershipData`, has **no `membershipID` field** — so the member id
//! the entire roster and every media key hangs on would be silently dropped. It
//! also makes `expires`, `foci_preferred` and `focus_active` required, so a
//! content missing any of them fails the whole parse instead of degrading. Raw
//! JSON is both more permissive and more honest here.

use std::{
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};

use super::element_call::{is_leave, legacy_session};

/// The state event type carrying a pre-sticky Element Call membership.
pub const STATE_MEMBER_EVENT_TYPE: &str = "org.matrix.msc3401.call.member";

/// The `call_id` sentinel an MSC4143 slot id uses for the room-wide session,
/// where this dialect uses an empty string. The same convention
/// [`legacy_session`] reverses.
const ROOM_CALL_ID: &str = "ROOM";

/// The lifetime to assume when a content states no `expires`: four hours, the JS
/// SDK's own default. Ruma makes the field required; old builds omitted it.
const DEFAULT_EXPIRES_MS: u64 = 4 * 60 * 60 * 1000;

/// The `m.call.intent` to publish when the core supplies none, which is always:
/// MSC4143 dropped the field, so nothing upstream of here has an opinion.
///
/// `audio` and `video` are the two values (ruma's `CallIntent`, snake_case).
/// `video` because every caller of this crate publishes a camera track.
const DEFAULT_CALL_INTENT: &str = "video";

/// Wall-clock milliseconds since the Unix epoch.
///
/// Exposed so the expiry check takes its clock as an argument and the tests can
/// pin it. Callers should read it once per snapshot, not once per event, so two
/// members are never judged against two different "now"s.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The LiveKit participant identity this generation uses: the plain
/// `{user_id}:{device_id}` string, with no MSC4195 hashing anywhere.
///
/// The legacy authorisation service mints it from the OpenID-verified user plus
/// the `device_id` in the `/sfu/get` request, which is why this generation's
/// identities are unhashed and why we cannot choose the shape.
///
/// It is also the legacy `membershipID`, which is why in this mode our
/// `member.id`, our `membershipID` and our SFU identity are all one string — see
/// [`super`] for why that coincidence is load-bearing.
pub fn participant_identity(user_id: &str, device_id: &str) -> String {
    format!("{user_id}:{device_id}")
}

/// One `org.matrix.msc3401.call.member` state event: the content, plus the three
/// event-level fields the translation needs and the content does not carry.
#[derive(Clone, Debug)]
pub struct StateMemberEvent {
    /// The event's `sender`. Homeserver-authenticated, and the only trustworthy
    /// identity in the whole event.
    pub sender: String,
    /// The event's `state_key`, for log lines and tie-breaking only — see the
    /// module docs on why it is not parsed.
    pub state_key: String,
    /// The event's `origin_server_ts`: the fallback deadline base for a content
    /// with no `created_ts`, and the clamp for one that states an implausible
    /// future `created_ts`.
    pub origin_server_ts: u64,
    /// The raw `content` object.
    pub content: Value,
}

/// A pre-sticky membership rendered in the current MSC4143 shape.
///
/// Exactly the three things the caller cannot get out of `content` on its own.
/// Everything else — the slot id, the member id, the application, the transports
/// — is inside `content`, where the core's own `RawStickyEventContent` reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateMembership {
    /// The state event's sender, verbatim.
    pub sender: String,
    /// `content.device_id`. Self-asserted and unauthenticated: state events
    /// carry no decryption metadata anywhere in the SDK, and Element Call as a
    /// widget could not read it if they did. Becomes `EventOrigin::Claimed`.
    pub claimed_device_id: String,
    /// MSC4143 `m.rtc.member` content, ready to deserialize as the core's
    /// `RawStickyEventContent`.
    pub content: Value,
}

/// One surviving membership, after the per-event rules and before the
/// cross-member focus resolution.
struct Parsed<'a> {
    sender: &'a str,
    state_key: &'a str,
    device_id: String,
    member_id: String,
    slot_id: String,
    application_type: String,
    /// `min(created_ts, origin_server_ts)`, or `origin_server_ts` when the
    /// content states no `created_ts`. Both the expiry base and the
    /// `oldest_membership` ordering key — the same value ruma documents as "the
    /// time a member has joined": a membership re-sent to extend its lifetime
    /// keeps `created_ts` and gains a later `origin_server_ts`, so the minimum
    /// is `created_ts`, and a first join has no `created_ts` at all, so it is
    /// `origin_server_ts`. Taking the minimum is also what stops a peer with a
    /// fast clock from looking alive longer than it is.
    joined_at: u64,
    /// `content["m.call.intent"]`, if stated.
    intent: Option<&'a Value>,
    /// `foci_preferred[0]`, verbatim.
    own_focus: Option<&'a Value>,
    /// Whether `focus_active.focus_selection` asks for multi-SFU, i.e. that this
    /// member publishes on its own focus rather than the oldest member's.
    prefers_own_focus: bool,
}

/// Translate a room's whole `org.matrix.msc3401.call.member` state into MSC4143
/// member contents, dropping every entry that is not a live membership.
///
/// Takes the batch rather than one event, because two of its rules are not
/// per-event: `focus_selection: "oldest_membership"` resolves against the oldest
/// membership *of the same slot*, and expiry needs one clock reading for the
/// whole set.
///
/// Dropped, each for its own reason:
///
/// - an empty content (`{}`) — this dialect's leave. Dropping it *is* the leave:
///   the bridge hands the core the complete membership every tick, so an entry
///   that contributes nothing is an entry whose member is gone. Same reasoning
///   as [`MemberContent::BareLeave`](super::MemberContent::BareLeave);
/// - a content past `created_ts + expires` (see the module docs);
/// - a content with no `application` string, or no `device_id`. Without a device
///   there is nothing to bind the member to and no media key can travel in
///   either direction, which makes the roster entry actively misleading;
/// - a `memberships` array (two generations back; see the module docs).
///
/// The order of the output follows the input; nothing downstream depends on it.
pub fn translate_state_memberships(
    events: &[StateMemberEvent],
    now_ms: u64,
) -> Vec<StateMembership> {
    let parsed: Vec<Parsed<'_>> = events
        .iter()
        .filter_map(|event| parse(event, now_ms))
        .collect();

    parsed
        .iter()
        .map(|member| {
            let mut application = json!({ "type": member.application_type });
            if let Some(intent) = member.intent
                && let Some(application) = application.as_object_mut()
            {
                application.insert("m.call.intent".to_owned(), intent.clone());
            }

            let mut content = json!({
                "slot_id": member.slot_id,
                "msc4354_sticky_key": member.member_id,
                "member": { "id": member.member_id, "membership": "join" },
                "application": application,
            });

            // The focus object goes through **verbatim**: `RawRtcTransport`
            // flattens what it does not know, so `livekit_alias` rides along
            // harmlessly and `livekit_service_url` lands exactly where
            // `RawRtcTransport::into_typed` looks for it. Nothing is reshaped,
            // so nothing can be reshaped wrongly.
            if let Some(focus) = resolve_focus(member, &parsed)
                && let Some(object) = content.as_object_mut()
            {
                let can_subscribe: Vec<Value> = focus
                    .get("type")
                    .and_then(Value::as_str)
                    .map(|transport_type| vec![Value::String(transport_type.to_owned())])
                    .unwrap_or_default();
                object.insert(
                    "transports".to_owned(),
                    json!({ "published": [focus], "can_subscribe": can_subscribe }),
                );
            }

            StateMembership {
                sender: member.sender.to_owned(),
                claimed_device_id: member.device_id.clone(),
                content,
            }
        })
        .collect()
}

/// Apply the per-event rules, or explain in the log why this event contributes
/// no membership.
fn parse<'a>(event: &'a StateMemberEvent, now_ms: u64) -> Option<Parsed<'a>> {
    let object = event.content.as_object()?;

    // The leave. Not worth a log line: it is the normal way a call ends, and in
    // a room with any history there are more of these than joins.
    if object.is_empty() {
        return None;
    }

    if object.contains_key("memberships") {
        log::debug!(
            "ignoring a `memberships` array from {} ({}): that is two Element Call \
             generations back and is not supported",
            event.sender,
            event.state_key,
        );
        return None;
    }

    let application_type = object
        .get("application")
        .and_then(Value::as_str)
        .filter(|application| !application.is_empty());
    let Some(application_type) = application_type else {
        log::debug!(
            "ignoring a pre-sticky membership from {} ({}): it names no application",
            event.sender,
            event.state_key,
        );
        return None;
    };

    let device_id = object
        .get("device_id")
        .and_then(Value::as_str)
        .filter(|device_id| !device_id.is_empty());
    let Some(device_id) = device_id else {
        log::debug!(
            "ignoring a pre-sticky membership from {} ({}): it names no device, so nothing \
             could be bound to it and no media key could travel in either direction",
            event.sender,
            event.state_key,
        );
        return None;
    };

    // See `Parsed::joined_at` for why this is a minimum rather than a preference.
    let joined_at = match object.get("created_ts").and_then(Value::as_u64) {
        Some(created_ts) => created_ts.min(event.origin_server_ts),
        None => event.origin_server_ts,
    };
    let expires = object
        .get("expires")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_EXPIRES_MS);
    if joined_at.saturating_add(expires) <= now_ms {
        log::debug!(
            "ignoring a pre-sticky membership from {} ({}): it expired {}ms ago",
            event.sender,
            event.state_key,
            now_ms.saturating_sub(joined_at.saturating_add(expires)),
        );
        return None;
    }

    let call_id = object
        .get("call_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let slot_id = format!(
        "{application_type}#{}",
        if call_id.is_empty() {
            ROOM_CALL_ID
        } else {
            call_id
        }
    );

    // `membershipID` is what Element Call addresses its media keys to. The
    // fallback is the JS SDK's own, and is the same string
    // `participant_identity` builds — deliberately, since an inbound key with no
    // `member` object is bound by exactly that shape.
    let member_id = object
        .get("membershipID")
        .and_then(Value::as_str)
        .filter(|member_id| !member_id.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| participant_identity(&event.sender, device_id));

    Some(Parsed {
        sender: &event.sender,
        state_key: &event.state_key,
        device_id: device_id.to_owned(),
        member_id,
        slot_id,
        application_type: application_type.to_owned(),
        joined_at,
        intent: object.get("m.call.intent"),
        own_focus: object
            .get("foci_preferred")
            .and_then(Value::as_array)
            .and_then(|foci| foci.first()),
        prefers_own_focus: object
            .get("focus_active")
            .and_then(|focus| focus.get("focus_selection"))
            .and_then(Value::as_str)
            == Some("multi_sfu"),
    })
}

/// The focus a member actually publishes on.
///
/// `oldest_membership` — the historical default, and the only value ruma even
/// names — points every member at the first preferred focus of the oldest
/// membership in the same slot. `multi_sfu` means each member keeps its own.
/// Either way, if the chosen side names no focus we fall back to the other
/// rather than declaring a member who publishes nowhere: a peer we can hear on
/// the wrong SFU is recoverable, a peer we never try to reach is not.
///
/// The ordering is `joined_at` ascending, ties broken by `state_key`. The tie
/// break is arbitrary and only there so two clients reading the same room reach
/// the same answer; two memberships sharing a millisecond is not a case that
/// happens.
///
/// Note this runs over the *survivors* only — `parse` has already dropped the
/// expired ones — so a long-dead membership cannot pin the whole room to an SFU
/// nobody is on any more.
fn resolve_focus<'a>(member: &Parsed<'a>, all: &[Parsed<'a>]) -> Option<&'a Value> {
    if member.prefers_own_focus && member.own_focus.is_some() {
        return member.own_focus;
    }

    let oldest = all
        .iter()
        .filter(|candidate| candidate.slot_id == member.slot_id)
        .min_by(|a, b| {
            a.joined_at
                .cmp(&b.joined_at)
                .then_with(|| a.state_key.cmp(b.state_key))
        })
        .and_then(|oldest| oldest.own_focus);

    oldest.or(member.own_focus)
}

/// The outbound half: renders our own membership as a pre-sticky Element Call
/// room state event.
///
/// Wholesale replacement, not an addition. The sticky dialect can rewrite a join
/// additively because both readers look at the same event; here the membership is
/// not even a timeline event, so there is nothing to be additive with. See the
/// module docs.
#[derive(Clone, Debug)]
pub struct ElementCallStateDialect {
    own_user_id: String,
    own_device_id: String,
    room_id: String,
    slot_id: String,
    /// Pinned on the first join and never moved.
    ///
    /// The core re-sends our membership every half sticky-duration
    /// (`refresh_sticky_if_due`), which for room state is a no-op we neither
    /// need nor want to suppress. Pinning makes every re-send **byte-identical**,
    /// so Synapse either drops the duplicate or accepts it and moves nothing but
    /// `origin_server_ts` — and since `created_ts` is present, peers compute the
    /// same deadline either way. A `created_ts` stamped afresh each time would
    /// silently extend our validity past what peers computed when they read us.
    ///
    /// Shared so a clone of the command sender agrees with the original.
    created_ts: Arc<OnceLock<u64>>,
}

impl ElementCallStateDialect {
    /// Builds the dialect for our own membership in `slot_id`.
    ///
    /// `room_id` is needed for the `livekit_alias` this generation expects on a
    /// focus; see [`Self::member_content`].
    pub fn new(
        own_user_id: impl Into<String>,
        own_device_id: impl Into<String>,
        room_id: impl Into<String>,
        slot_id: impl Into<String>,
    ) -> Self {
        Self {
            own_user_id: own_user_id.into(),
            own_device_id: own_device_id.into(),
            room_id: room_id.into(),
            slot_id: slot_id.into(),
            created_ts: Arc::new(OnceLock::new()),
        }
    }

    /// `_{user_id}_{device_id}_{application}{call_id}`.
    ///
    /// The leading underscore is not decoration: Synapse rejects a state key
    /// that looks like a user id from anyone but that user, so every client of
    /// this generation sends the underscore-prefixed form. (MSC3757 lifts the
    /// restriction, but only in room versions nobody runs.) Only the user id is
    /// ever read back out of it; the rest is per-device uniqueness.
    pub fn state_key(&self) -> String {
        let (application, call_id) = legacy_session(&self.slot_id);
        format!(
            "_{}_{}_{application}{call_id}",
            self.own_user_id, self.own_device_id,
        )
    }

    /// Our `membershipID`, which in this generation is also our `member.id` and
    /// our SFU participant identity.
    pub fn membership_id(&self) -> String {
        participant_identity(&self.own_user_id, &self.own_device_id)
    }

    /// The device we publish, for the shared key-message builder.
    pub fn own_device_id(&self) -> &str {
        &self.own_device_id
    }

    /// The slot we joined, for the shared key-message builder — it reconstructs
    /// the legacy `session` object from it.
    pub fn slot_id(&self) -> &str {
        &self.slot_id
    }

    /// Translate an MSC4143 `m.rtc.member` content into pre-sticky Element Call
    /// state-event content.
    ///
    /// A leave returns `{}`, which is the whole protocol for it — this dialect
    /// has no `membership` field to set to `"leave"`.
    ///
    /// `foci_preferred` is lifted out of the content the core already built, not
    /// injected from the caller, so the dialect needs nothing that is not known
    /// when the command sender is constructed. The `livekit_alias` we add is the
    /// Matrix room id, because that is what we pass as `room` to `/sfu/get` and
    /// the legacy service derives the LiveKit room name from it alone. **That
    /// value and the `/sfu/get` `room` field must stay equal**, or the two
    /// clients land in different LiveKit rooms and never see each other while
    /// both connections look perfectly healthy.
    pub fn member_content(&self, spec: &Value) -> Value {
        // A spec leave, or anything that is not a membership we can dress up.
        // `slot_id` is the tell: the core's leave content still carries one, but
        // an already-legacy content does not.
        if is_leave(spec) || spec.get("slot_id").is_none() {
            return json!({});
        }

        let (application, call_id) = legacy_session(&self.slot_id);
        let created_ts = *self.created_ts.get_or_init(now_ms);

        let mut content = json!({
            "application": application,
            "call_id": call_id,
            "scope": "m.room",
            "device_id": self.own_device_id,
            "membershipID": self.membership_id(),
            "created_ts": created_ts,
            "expires": DEFAULT_EXPIRES_MS,
            // We publish on exactly one focus, so which selection mode we
            // declare only matters for how peers read *us*. `multi_sfu` says
            // "use the focus I name", which is true and is what the transports
            // below state; `oldest_membership` would invite a peer to look at
            // somebody else's event for our SFU.
            "focus_active": { "type": "livekit", "focus_selection": "multi_sfu" },
            "foci_preferred": self.foci_preferred(spec),
        });

        // Required, not optional: this generation validates it and logs
        // "RTC membership has invalid m.call.intent" when it is absent, after
        // which its consensus-intent calculation has nothing to work with. The
        // core carries no intent of its own (MSC4143 dropped it), so absent
        // means "we publish video", which is what this crate's callers do.
        if let Some(object) = content.as_object_mut() {
            let intent = spec
                .pointer("/application/m.call.intent")
                .cloned()
                .unwrap_or_else(|| Value::String(DEFAULT_CALL_INTENT.to_owned()));
            object.insert("m.call.intent".to_owned(), intent);
        }

        content
    }

    /// `transports.published` → `foci_preferred`, adding the `livekit_alias`
    /// this generation requires on a focus.
    fn foci_preferred(&self, spec: &Value) -> Value {
        let published = spec
            .pointer("/transports/published")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();

        let foci: Vec<Value> = published
            .iter()
            .filter(|focus| focus.get("type").and_then(Value::as_str) == Some("livekit"))
            .map(|focus| {
                let mut focus = focus.clone();
                if let Some(object) = focus.as_object_mut() {
                    object
                        .entry("livekit_alias")
                        .or_insert_with(|| Value::String(self.room_id.clone()));
                }
                focus
            })
            .collect();

        Value::Array(foci)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A join as observed from Element Call on the JS SDK, pre-sticky.
    const EC_JOIN: &str = r#"{
        "application": "m.call",
        "call_id": "",
        "scope": "m.room",
        "device_id": "V5cP8FErcB",
        "membershipID": "@alice:example.io:V5cP8FErcB",
        "expires": 14400000,
        "created_ts": 1749000000000,
        "m.call.intent": "video",
        "focus_active": { "type": "livekit", "focus_selection": "multi_sfu" },
        "foci_preferred": [
            {
                "type": "livekit",
                "livekit_alias": "!room:example.io",
                "livekit_service_url": "https://mrtc.example.io/livekit/jwt"
            }
        ]
    }"#;

    /// The leave: the content becomes an empty object.
    const EC_LEAVE: &str = "{}";

    /// Two generations back: one state event per user, holding an array.
    const EC_ARRAY_CONTENT: &str = r#"{ "memberships": [ { "application": "m.call" } ] }"#;

    /// Ten minutes after `EC_JOIN`'s `created_ts`, so it is comfortably live.
    const NOW: u64 = 1_749_000_600_000;
    const CREATED: u64 = 1_749_000_000_000;

    fn content(json: &str) -> Value {
        serde_json::from_str(json).expect("valid json")
    }

    /// A `StateMemberEvent` with `origin_server_ts` equal to the fixture's
    /// `created_ts`, which is the ordinary case.
    fn event(json: &str) -> StateMemberEvent {
        event_at(json, CREATED)
    }

    fn event_at(json: &str, origin_server_ts: u64) -> StateMemberEvent {
        StateMemberEvent {
            sender: "@alice:example.io".to_owned(),
            state_key: "_@alice:example.io_V5cP8FErcB_m.call".to_owned(),
            origin_server_ts,
            content: content(json),
        }
    }

    /// Translate a single event, at `NOW`.
    fn one(json: &str) -> Option<StateMembership> {
        translate_state_memberships(&[event(json)], NOW).pop()
    }

    /// `EC_JOIN` with `patch` applied to the top level; a `Value::Null` removes.
    fn join_with(patch: &[(&str, Value)]) -> Value {
        let mut value = content(EC_JOIN);
        let object = value.as_object_mut().unwrap();
        for (key, replacement) in patch {
            if replacement.is_null() {
                object.remove(*key);
            } else {
                object.insert((*key).to_owned(), replacement.clone());
            }
        }
        value
    }

    fn translate_values(contents: &[(&str, u64, Value)], now: u64) -> Vec<StateMembership> {
        let events: Vec<StateMemberEvent> = contents
            .iter()
            .map(|(state_key, origin_server_ts, content)| StateMemberEvent {
                sender: "@alice:example.io".to_owned(),
                state_key: (*state_key).to_owned(),
                origin_server_ts: *origin_server_ts,
                content: content.clone(),
            })
            .collect();
        translate_state_memberships(&events, now)
    }

    #[test]
    fn a_join_becomes_msc4143_member_content() {
        let membership = one(EC_JOIN).expect("a live join");

        assert_eq!(membership.sender, "@alice:example.io");
        assert_eq!(membership.claimed_device_id, "V5cP8FErcB");
        assert_eq!(
            membership.content,
            json!({
                "slot_id": "m.call#ROOM",
                "msc4354_sticky_key": "@alice:example.io:V5cP8FErcB",
                "member": { "id": "@alice:example.io:V5cP8FErcB", "membership": "join" },
                "application": { "type": "m.call", "m.call.intent": "video" },
                "transports": {
                    "published": [{
                        "type": "livekit",
                        "livekit_alias": "!room:example.io",
                        "livekit_service_url": "https://mrtc.example.io/livekit/jwt"
                    }],
                    "can_subscribe": ["livekit"]
                }
            })
        );
    }

    /// The focus is passed through untouched: `livekit_alias` is not ours to
    /// interpret, and the core drops it at `into_typed`, not here.
    #[test]
    fn the_focus_object_is_passed_through_verbatim() {
        let membership = one(EC_JOIN).unwrap();

        assert_eq!(
            membership
                .content
                .pointer("/transports/published/0/livekit_alias")
                .unwrap(),
            "!room:example.io"
        );
    }

    #[test]
    fn the_empty_call_id_becomes_the_room_sentinel() {
        let room_wide = one(EC_JOIN).unwrap();
        assert_eq!(room_wide.content["slot_id"], "m.call#ROOM");

        let named = translate_values(
            &[("a", CREATED, join_with(&[("call_id", json!("standup"))]))],
            NOW,
        )
        .pop()
        .unwrap();
        assert_eq!(named.content["slot_id"], "m.call#standup");

        // And the sentinel round-trips through the convention the outbound half
        // uses, so a peer lands in the session we are actually in.
        assert_eq!(legacy_session("m.call#ROOM"), ("m.call", ""));
        assert_eq!(legacy_session("m.call#standup"), ("m.call", "standup"));
    }

    #[test]
    fn an_empty_content_is_dropped_as_a_leave() {
        assert!(one(EC_LEAVE).is_none());
    }

    #[test]
    fn a_memberships_array_is_dropped() {
        assert!(one(EC_ARRAY_CONTENT).is_none());
    }

    #[test]
    fn a_membership_with_no_device_id_is_dropped() {
        assert!(
            translate_values(
                &[("a", CREATED, join_with(&[("device_id", Value::Null)]))],
                NOW
            )
            .is_empty()
        );
        assert!(
            translate_values(
                &[("a", CREATED, join_with(&[("device_id", json!(""))]))],
                NOW
            )
            .is_empty()
        );
    }

    #[test]
    fn a_membership_with_no_application_is_dropped() {
        assert!(
            translate_values(
                &[("a", CREATED, join_with(&[("application", Value::Null)]))],
                NOW
            )
            .is_empty()
        );
        // An object rather than a string is the *modern* shape, and is not this
        // dialect; refusing it keeps the two generations from crossing.
        assert!(
            translate_values(
                &[(
                    "a",
                    CREATED,
                    join_with(&[("application", json!({ "type": "m.call" }))])
                )],
                NOW
            )
            .is_empty()
        );
    }

    #[test]
    fn the_expiry_boundary_is_exclusive_of_the_deadline() {
        let deadline = CREATED + 14_400_000;

        assert!(translate_values(&[("a", CREATED, content(EC_JOIN))], deadline - 1).len() == 1);
        assert!(translate_values(&[("a", CREATED, content(EC_JOIN))], deadline).is_empty());
    }

    /// The first join of a session states no `created_ts` — the client cannot
    /// know it yet — so the deadline is measured from the event itself.
    #[test]
    fn a_first_join_expires_from_origin_server_ts() {
        let first_join = join_with(&[("created_ts", Value::Null)]);

        // Stamped long ago: expired.
        assert!(translate_values(&[("a", 1_000, first_join.clone())], NOW).is_empty());
        // Stamped just now: live.
        assert_eq!(
            translate_values(&[("a", NOW - 1_000, first_join)], NOW).len(),
            1
        );
    }

    #[test]
    fn a_membership_with_no_expires_gets_four_hours() {
        let no_expires = join_with(&[("expires", Value::Null)]);

        assert_eq!(
            translate_values(
                &[("a", CREATED, no_expires.clone())],
                CREATED + DEFAULT_EXPIRES_MS - 1
            )
            .len(),
            1
        );
        assert!(
            translate_values(&[("a", CREATED, no_expires)], CREATED + DEFAULT_EXPIRES_MS)
                .is_empty()
        );
    }

    /// A peer with a fast clock must not get to outlive its own event.
    #[test]
    fn a_created_ts_after_the_event_is_clamped_to_the_event() {
        let skewed = join_with(&[("created_ts", json!(NOW + 60_000))]);

        // origin_server_ts is old, so despite the future `created_ts` this is
        // judged from the server's stamp and has expired.
        assert!(translate_values(&[("a", 1_000, skewed)], NOW).is_empty());
    }

    #[test]
    fn the_membership_id_falls_back_to_user_and_device() {
        let membership = translate_values(
            &[("a", CREATED, join_with(&[("membershipID", Value::Null)]))],
            NOW,
        )
        .pop()
        .unwrap();

        assert_eq!(
            membership.content["member"]["id"],
            "@alice:example.io:V5cP8FErcB"
        );
        // The two must never diverge: the core keys the roster by one and binds
        // media keys by the other.
        assert_eq!(
            membership.content["member"]["id"],
            membership.content["msc4354_sticky_key"]
        );
    }

    /// Ten and five minutes before `NOW`; both comfortably inside the four-hour
    /// default lifetime, so these tests exercise focus resolution and not expiry.
    const JOINED_EARLIER: u64 = NOW - 600_000;
    const JOINED_LATER: u64 = NOW - 300_000;

    fn focus(url: &str) -> Value {
        json!({ "type": "livekit", "livekit_service_url": url })
    }

    /// One member, with `created_ts` and `origin_server_ts` agreeing.
    ///
    /// Keeping them equal matters: `joined_at` is the *minimum* of the two, so a
    /// fixture that moves one and not the other silently backdates the
    /// membership into expiry and every focus assertion below it becomes
    /// vacuous.
    fn focus_member(
        state_key: &'static str,
        joined_at: u64,
        selection: &str,
        url: &str,
    ) -> (&'static str, u64, Value) {
        let content = join_with(&[
            ("created_ts", json!(joined_at)),
            (
                "focus_active",
                json!({ "type": "livekit", "focus_selection": selection }),
            ),
            ("foci_preferred", json!([focus(url)])),
        ]);
        (state_key, joined_at, content)
    }

    fn published_url(membership: &StateMembership) -> Option<&str> {
        membership
            .content
            .pointer("/transports/published/0/livekit_service_url")
            .and_then(Value::as_str)
    }

    /// Fed newest-first, so a naive "first in the list wins" would pass the
    /// wrong focus and this test would catch it.
    #[test]
    fn oldest_membership_uses_the_oldest_members_focus() {
        let members = translate_values(
            &[
                focus_member("b", JOINED_LATER, "oldest_membership", "https://new"),
                focus_member("a", JOINED_EARLIER, "oldest_membership", "https://old"),
            ],
            NOW,
        );

        assert_eq!(members.len(), 2);
        for member in &members {
            assert_eq!(published_url(member), Some("https://old"));
        }
    }

    #[test]
    fn oldest_membership_is_resolved_per_slot() {
        let room_wide = focus_member("a", JOINED_LATER, "oldest_membership", "https://room");
        let (key, ts, mut standup) =
            focus_member("b", JOINED_EARLIER, "oldest_membership", "https://standup");
        standup["call_id"] = json!("standup");

        let members = translate_values(&[room_wide, (key, ts, standup)], NOW);

        // The older `standup` membership must not lend its focus to the
        // room-wide session it is not part of.
        assert_eq!(published_url(&members[0]), Some("https://room"));
        assert_eq!(published_url(&members[1]), Some("https://standup"));
    }

    #[test]
    fn multi_sfu_keeps_each_members_own_focus() {
        let members = translate_values(
            &[
                focus_member("a", JOINED_EARLIER, "multi_sfu", "https://one"),
                focus_member("b", JOINED_LATER, "multi_sfu", "https://two"),
            ],
            NOW,
        );

        assert_eq!(published_url(&members[0]), Some("https://one"));
        assert_eq!(published_url(&members[1]), Some("https://two"));
    }

    /// A member we never try to reach is worse than one we reach on the wrong
    /// SFU, so an empty `foci_preferred` borrows rather than publishing nowhere.
    #[test]
    fn a_member_with_no_focus_of_its_own_borrows_one() {
        let (key, ts, mut without) = focus_member("b", JOINED_LATER, "multi_sfu", "https://unused");
        without["foci_preferred"] = json!([]);

        let members = translate_values(
            &[
                focus_member("a", JOINED_EARLIER, "multi_sfu", "https://one"),
                (key, ts, without),
            ],
            NOW,
        );

        assert_eq!(published_url(&members[1]), Some("https://one"));
    }

    #[test]
    fn a_membership_with_no_focus_anywhere_still_appears() {
        let mut without = content(EC_JOIN);
        without["foci_preferred"] = json!([]);

        let membership = translate_values(&[("a", CREATED, without)], NOW)
            .pop()
            .expect("the member is still in the call, it just publishes nowhere");
        assert!(membership.content.get("transports").is_none());
    }

    /// The subtlest bug this file can have: a long-dead membership pinning every
    /// live member to an SFU nobody is on any more.
    #[test]
    fn an_expired_member_does_not_get_to_be_the_oldest() {
        let (key, ts, mut ancient) = focus_member("a", 1_000, "oldest_membership", "https://dead");
        ancient["expires"] = json!(1_000);
        let live = focus_member("b", JOINED_LATER, "oldest_membership", "https://alive");

        let members = translate_values(&[(key, ts, ancient), live], NOW);

        assert_eq!(members.len(), 1);
        assert_eq!(published_url(&members[0]), Some("https://alive"));
    }

    /// Two clients reading the same room must reach the same answer, whatever
    /// order the state arrived in — hence the `state_key` tie-break on an
    /// identical `joined_at`.
    #[test]
    fn the_translation_is_deterministic_under_reordering() {
        let a = focus_member("a", JOINED_EARLIER, "oldest_membership", "https://a");
        let b = focus_member("b", JOINED_EARLIER, "oldest_membership", "https://b");

        let forwards = translate_values(&[a.clone(), b.clone()], NOW);
        let backwards = translate_values(&[b, a], NOW);

        assert_eq!(published_url(&forwards[0]), published_url(&backwards[1]));
        assert_eq!(published_url(&forwards[0]), Some("https://a"));
    }

    // -- the outbound half -------------------------------------------------

    /// The content the core hands us for a join, as `RawStickyEventContent`
    /// serializes it.
    const SPEC_JOIN: &str = r#"{
        "slot_id": "m.call#ROOM",
        "msc4354_sticky_key": "xyzABCDEF0123",
        "member": { "id": "xyzABCDEF0123", "membership": "join" },
        "application": { "type": "m.call", "m.call.intent": "video" },
        "transports": {
            "published": [{ "type": "livekit", "livekit_service_url": "https://sfu.example.com" }],
            "can_subscribe": ["livekit"]
        }
    }"#;

    /// The core's leave, and the delayed leave it arms at join time.
    const SPEC_LEAVE: &str = r#"{
        "slot_id": "m.call#ROOM",
        "msc4354_sticky_key": "xyzABCDEF0123",
        "member": { "id": "xyzABCDEF0123", "membership": "leave" },
        "leave_reason": { "code": "m.delayed_leave", "reason": "Dead man's switch" }
    }"#;

    fn dialect() -> ElementCallStateDialect {
        ElementCallStateDialect::new(
            "@alice:example.io",
            "V5cP8FErcB",
            "!room:example.io",
            "m.call#ROOM",
        )
    }

    #[test]
    fn the_state_key_is_the_underscore_prefixed_per_device_form() {
        assert_eq!(
            dialect().state_key(),
            "_@alice:example.io_V5cP8FErcB_m.call"
        );

        let named = ElementCallStateDialect::new(
            "@alice:example.io",
            "V5cP8FErcB",
            "!room:example.io",
            "m.call#standup",
        );
        assert_eq!(
            named.state_key(),
            "_@alice:example.io_V5cP8FErcB_m.callstandup"
        );
    }

    #[test]
    fn the_membership_id_is_the_participant_identity() {
        assert_eq!(dialect().membership_id(), "@alice:example.io:V5cP8FErcB");
        assert_eq!(
            dialect().membership_id(),
            participant_identity("@alice:example.io", "V5cP8FErcB")
        );
    }

    #[test]
    fn a_spec_join_becomes_pre_sticky_state_content() {
        let dialect = dialect();
        let legacy = dialect.member_content(&content(SPEC_JOIN));

        assert_eq!(legacy["application"], "m.call");
        assert_eq!(legacy["call_id"], "");
        assert_eq!(legacy["scope"], "m.room");
        assert_eq!(legacy["device_id"], "V5cP8FErcB");
        assert_eq!(legacy["membershipID"], "@alice:example.io:V5cP8FErcB");
        assert_eq!(legacy["expires"], DEFAULT_EXPIRES_MS);
        assert_eq!(legacy["m.call.intent"], "video");
        assert_eq!(legacy["focus_active"]["focus_selection"], "multi_sfu");
        assert_eq!(
            legacy["foci_preferred"][0]["livekit_service_url"],
            "https://sfu.example.com"
        );
        // The alias this generation expects, and the value `/sfu/get` must be
        // given as `room`.
        assert_eq!(
            legacy["foci_preferred"][0]["livekit_alias"],
            "!room:example.io"
        );
        assert!(legacy["created_ts"].is_u64());
        // No spec field survives: this event serves one dialect only.
        assert!(legacy.get("slot_id").is_none());
        assert!(legacy.get("member").is_none());
        assert!(legacy.get("transports").is_none());
    }

    #[test]
    fn a_spec_leave_becomes_the_empty_object() {
        assert_eq!(dialect().member_content(&content(SPEC_LEAVE)), json!({}));
    }

    /// An already-legacy content (no `slot_id`) must not be dressed up again
    /// into something that looks like a fresh membership.
    #[test]
    fn content_without_a_slot_id_becomes_the_empty_object() {
        assert_eq!(dialect().member_content(&json!({})), json!({}));
    }

    /// The regression test for the `created_ts` pinning: the core re-sends our
    /// membership every half sticky-duration, and a content that differed each
    /// time would move the deadline peers computed for us.
    #[test]
    fn the_same_join_renders_byte_identically_every_time() {
        let dialect = dialect();
        let spec = content(SPEC_JOIN);

        let first = dialect.member_content(&spec);
        let second = dialect.member_content(&spec);

        assert_eq!(first, second);
        // Including across a clone, which is how the command sender holds it.
        assert_eq!(first, dialect.clone().member_content(&spec));
    }

    /// This generation validates `m.call.intent` and logs
    /// "RTC membership has invalid m.call.intent" when it is missing — which it
    /// always was, because the core carries no intent for us to copy.
    #[test]
    fn a_join_always_states_a_call_intent() {
        let legacy = dialect().member_content(&content(SPEC_JOIN));
        assert_eq!(legacy["m.call.intent"], DEFAULT_CALL_INTENT);

        // An intent the core did supply wins over the default.
        let mut spec = content(SPEC_JOIN);
        spec["application"]["m.call.intent"] = json!("audio");
        assert_eq!(dialect().member_content(&spec)["m.call.intent"], "audio");
    }

    #[test]
    fn a_non_livekit_transport_is_not_offered_as_a_focus() {
        let mut spec = content(SPEC_JOIN);
        spec["transports"]["published"] = json!([{ "type": "something-else" }]);

        let legacy = dialect().member_content(&spec);
        assert_eq!(legacy["foci_preferred"], json!([]));
    }
}
