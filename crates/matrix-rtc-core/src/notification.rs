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

//! MatrixRTC notifications (`m.rtc.notification`, MSC4075).
//!
//! Membership says who is *in* a session; it does not say who should be
//! *summoned* to one. MSC4075 adds a timeline event for that, carrying the
//! ring-or-notify intent and an `m.reference` relation back to the sender's own
//! `m.rtc.member` event — which is what lets a receiver check the session is
//! real before it makes a noise.
//!
//! Only the sending half lives here: building the content. Who sends it and
//! when is [`crate::session`]'s call (the first member to join, and only if the
//! host asked for it via [`JoinSessionParams::notify`]); the receiving rules —
//! push rules, lifetime expiry, ring acknowledgements — are not implemented.
//!
//! The MSC and the deployed ecosystem disagree about where the call fields go,
//! so [`build_notification_content`] writes them in both places; its docs say
//! why.
//!
//! [`JoinSessionParams::notify`]: crate::JoinSessionParams::notify

use serde_json::{Map, Value, json};

/// Event type for MatrixRTC notifications (MSC4075).
///
/// The stable id, like every other type the core names; the wire spelling is a
/// host-layer concern (see [`crate::wire_event_type`]).
pub const NOTIFICATION_EVENT_TYPE: &str = "m.rtc.notification";

/// Default `lifetime` for a ring, in milliseconds. MSC4075's recommended value.
pub const DEFAULT_RING_LIFETIME_MS: u64 = 30_000;

/// The longest `lifetime` MSC4075 says a ring SHOULD carry (2 minutes).
///
/// Receivers are told to cap at this value anyway, so asking for more only
/// means the sender and the receiver disagree about when the ring ends.
pub const MAX_RING_LIFETIME_MS: u64 = 120_000;

/// What kind of noise the notification asks recipients to make (MSC4075).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotificationType {
    /// Ring audibly, for `lifetime` milliseconds.
    Ring,
    /// Show a visual indication only.
    Notification,
}

impl NotificationType {
    /// The MSC4075 `notification_type` value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ring => "ring",
            Self::Notification => "notification",
        }
    }
}

/// Who the notification targets, as the Client-Server API's `m.mentions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mentions {
    /// Users named individually.
    pub user_ids: Vec<String>,
    /// Whether the whole room is targeted. Note that `notifications.room` in
    /// the room's power levels may gate this.
    pub room: bool,
}

impl Default for Mentions {
    /// Everyone in the room, which is what a call in a room means.
    fn default() -> Self {
        Self {
            user_ids: Vec::new(),
            room: true,
        }
    }
}

/// What the host asks for when it joins.
///
/// `None` on [`JoinSessionParams::notify`] means "join quietly" — which is what
/// joining a call someone else started does. Element Call makes the same
/// distinction by only passing a notification type when the app *starts* a
/// call.
///
/// [`JoinSessionParams::notify`]: crate::JoinSessionParams::notify
#[derive(Clone, Debug)]
pub struct NotifyConfig {
    /// Ring, or notify silently.
    pub notification_type: NotificationType,

    /// MSC4196 `m.call.intent`, e.g. `"audio"` or `"video"`. Omitted from the
    /// event when `None`.
    pub intent: Option<String>,

    /// How long the ring stays valid, in milliseconds.
    ///
    /// Defaults to [`DEFAULT_RING_LIFETIME_MS`] and is clamped to
    /// [`MAX_RING_LIFETIME_MS`]. Ignored by receivers for
    /// [`NotificationType::Notification`], but still sent — Element Call sends
    /// it either way, and a receiver that keys off it is not made wrong by its
    /// presence.
    pub lifetime_ms: Option<u64>,

    /// Who to notify. Defaults to the whole room.
    pub mentions: Mentions,
}

impl NotifyConfig {
    /// Ring the room, with the default lifetime.
    pub fn ring() -> Self {
        Self::new(NotificationType::Ring)
    }

    /// Notify the room without ringing.
    pub fn notification() -> Self {
        Self::new(NotificationType::Notification)
    }

    fn new(notification_type: NotificationType) -> Self {
        Self {
            notification_type,
            intent: None,
            lifetime_ms: None,
            mentions: Mentions::default(),
        }
    }

    /// The lifetime to put on the wire: the configured value or the default,
    /// clamped to what MSC4075 says a receiver will honour.
    pub fn lifetime_ms(&self) -> u64 {
        let requested = self.lifetime_ms.unwrap_or(DEFAULT_RING_LIFETIME_MS);
        if requested > MAX_RING_LIFETIME_MS {
            log::warn!(
                "notification lifetime_ms {requested} exceeds the {MAX_RING_LIFETIME_MS} \
                 receivers cap at; using the maximum so both ends agree on when the ring ends",
            );
            return MAX_RING_LIFETIME_MS;
        }
        requested
    }
}

/// How long the homeserver should keep the notification in the sticky map.
///
/// MSC4075: at least twice the `lifetime`, because a ring acknowledgement can
/// extend the ring past its original deadline and the event has to still be
/// there when it does.
pub fn notification_sticky_duration_ms(lifetime_ms: u64) -> u64 {
    lifetime_ms.saturating_mul(2)
}

/// Builds content for an `m.rtc.notification` event.
///
/// `member_event_id` is the event id of *our own* `m.rtc.member` event: the
/// relation is what ties the notification to a session a receiver can verify,
/// and MSC4075 requires it, which is why a notification cannot be sent before
/// the membership.
///
/// # Why `notification_type` appears twice
///
/// MSC4075 as written nests the call fields under `application`. Nothing
/// deployed reads them there: Element Call puts `notification_type`,
/// `sender_ts`, `lifetime` and `m.call.intent` at the top level of the content,
/// and so does ruma's `RtcNotificationEventContent` — which is what
/// matrix-rust-sdk hands a mobile client, and which *requires* all three at the
/// top level, so a purely nested event fails to deserialize there and rings
/// nobody.
///
/// So both are written. `application` is the shape the MSC requires and a
/// spec-current receiver will look for; the top-level copies are what every
/// receiver that exists today reads. They are the same values, and a reader of
/// either shape ignores the other as unknown fields.
///
/// `device_id` stays inside `application` alone: it exists for ring
/// acknowledgements, which nothing here sends or reads yet, and a top-level
/// `device_id` means something else entirely in Element Call's key messages.
pub fn build_notification_content(
    config: &NotifyConfig,
    application_type: &str,
    sender: &str,
    device_id: &str,
    member_event_id: &str,
    sender_ts_ms: u64,
) -> Value {
    let notification_type = json!(config.notification_type.as_str());
    let lifetime = json!(config.lifetime_ms());

    let mut application = Map::new();
    application.insert("type".to_owned(), json!(application_type));
    application.insert("notification_type".to_owned(), notification_type.clone());
    application.insert("sender_ts".to_owned(), json!(sender_ts_ms));
    application.insert("lifetime".to_owned(), lifetime.clone());
    application.insert("device_id".to_owned(), json!(device_id));
    if let Some(intent) = &config.intent {
        application.insert("m.call.intent".to_owned(), json!(intent));
    }

    let mut content = Map::new();
    content.insert("application".to_owned(), Value::Object(application));
    content.insert("notification_type".to_owned(), notification_type);
    content.insert("sender_ts".to_owned(), json!(sender_ts_ms));
    content.insert("lifetime".to_owned(), lifetime);
    if let Some(intent) = &config.intent {
        content.insert("m.call.intent".to_owned(), json!(intent));
    }
    // MSC1767 fallback, for receivers that do not know this application type.
    // Deliberately unlocalised: a client that can render the call reads
    // `application` instead.
    content.insert(
        "m.text".to_owned(),
        json!([{ "body": format!("Session started by {sender}") }]),
    );
    content.insert(
        "m.mentions".to_owned(),
        json!({ "user_ids": config.mentions.user_ids, "room": config.mentions.room }),
    );
    content.insert(
        "m.relates_to".to_owned(),
        json!({ "rel_type": "m.reference", "event_id": member_event_id }),
    );

    Value::Object(content)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(config: &NotifyConfig) -> Value {
        build_notification_content(
            config,
            "m.call",
            "@alice:example.org",
            "DEVICEID",
            "$member_event",
            1_752_583_130_365,
        )
    }

    /// MSC4075 as written: everything under `application`, plus the three
    /// required siblings.
    #[test]
    fn ring_carries_every_field_the_msc_requires() {
        let mut config = NotifyConfig::ring();
        config.intent = Some("video".to_owned());
        let content = content(&config);

        assert_eq!(
            content["application"],
            json!({
                "type": "m.call",
                "notification_type": "ring",
                "sender_ts": 1_752_583_130_365u64,
                "lifetime": DEFAULT_RING_LIFETIME_MS,
                "device_id": "DEVICEID",
                "m.call.intent": "video",
            })
        );
        assert_eq!(
            content["m.text"],
            json!([{ "body": "Session started by @alice:example.org" }])
        );
        assert_eq!(
            content["m.mentions"],
            json!({ "user_ids": [], "room": true })
        );
        assert_eq!(
            content["m.relates_to"],
            json!({ "rel_type": "m.reference", "event_id": "$member_event" })
        );
    }

    /// ruma's `RtcNotificationEventContent` — what matrix-rust-sdk hands a
    /// mobile client — has no `application` object and takes these three as
    /// required top-level fields. Nest them only and the event fails to
    /// deserialize there, so nothing rings.
    #[test]
    fn ring_also_carries_the_fields_at_the_top_level() {
        let mut config = NotifyConfig::ring();
        config.intent = Some("video".to_owned());
        let content = content(&config);

        assert_eq!(content["notification_type"], "ring");
        assert_eq!(content["sender_ts"], 1_752_583_130_365u64);
        assert_eq!(content["lifetime"], DEFAULT_RING_LIFETIME_MS);
        assert_eq!(content["m.call.intent"], "video");
        // Only meaningful for ring acknowledgements, and the top level is where
        // Element Call's key messages mean something else by it.
        assert!(content.get("device_id").is_none());
    }

    #[test]
    fn intent_is_omitted_from_both_shapes_when_unset() {
        let content = content(&NotifyConfig::notification());

        assert_eq!(content["application"]["notification_type"], "notification");
        assert_eq!(content["notification_type"], "notification");
        assert!(content["application"].get("m.call.intent").is_none());
        assert!(content.get("m.call.intent").is_none());
    }

    #[test]
    fn explicit_mentions_are_used() {
        let mut config = NotifyConfig::ring();
        config.mentions = Mentions {
            user_ids: vec!["@bob:example.org".to_owned()],
            room: false,
        };

        assert_eq!(
            content(&config)["m.mentions"],
            json!({ "user_ids": ["@bob:example.org"], "room": false })
        );
    }

    /// Sending a lifetime past the cap only makes the two ends disagree about
    /// when the ring stops, since receivers cap it anyway.
    #[test]
    fn lifetime_is_clamped_to_what_receivers_honour() {
        let mut config = NotifyConfig::ring();
        config.lifetime_ms = Some(MAX_RING_LIFETIME_MS + 1);

        assert_eq!(config.lifetime_ms(), MAX_RING_LIFETIME_MS);
        let content = content(&config);
        assert_eq!(content["application"]["lifetime"], MAX_RING_LIFETIME_MS);
        assert_eq!(content["lifetime"], MAX_RING_LIFETIME_MS);
    }

    #[test]
    fn sticky_duration_leaves_room_for_acknowledgements() {
        assert_eq!(
            notification_sticky_duration_ms(DEFAULT_RING_LIFETIME_MS),
            2 * DEFAULT_RING_LIFETIME_MS
        );
    }
}
