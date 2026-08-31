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

//! Glue between `matrix-rtc-core` and a `matrix_sdk::Client`.
//!
//! This is what makes the Rust stack a first-class MatrixRTC participant against
//! a real homeserver, using the experimental MSC4354 sticky-event support:
//!
//! - [`SdkCommandSender`] implements [`RtcCommandSender`], turning the core's
//!   outbound commands (join/leave sticky events, dead man's switch delayed
//!   events) into Matrix Client-Server requests.
//! - [`run_sticky_bridge`] feeds the SDK's live sticky events into an
//!   [`RtcSessionManager`], so the core discovers every peer's `m.rtc.member`
//!   membership.
//!
//! Requires the `matrix-sdk` feature.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use matrix_sdk::deserialized_responses::RawAnySyncOrStrippedState;
use matrix_sdk::ruma::api::client::delayed_events::update_delayed_event::UpdateAction;
use matrix_sdk::ruma::api::client::delayed_events::{
    DelayParameters, delayed_message_event, delayed_state_event, update_delayed_event,
};
use matrix_sdk::ruma::api::client::state::{get_state_events, send_state_event};
use matrix_sdk::ruma::api::error::ErrorKind;
use matrix_sdk::ruma::events::{
    AnyMessageLikeEventContent, AnyStateEventContent, AnyToDeviceEventContent,
    MessageLikeEventType, StateEventType,
};
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{DeviceId, OwnedUserId, RoomId, TransactionId, UserId};
use matrix_sdk::{Client, Room, RoomMemberships};
use matrix_sdk_base::crypto::CollectStrategy;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::sync::broadcast::error::RecvError;

use matrix_rtc_core::{
    CommandError, EventOrigin, RawSlotEvent, RawSlotEventContent, RawStickyEvent,
    RawStickyEventContent, RtcCommandSender, RtcSessionManager, SLOT_EVENT_TYPE, ToDeviceDelivery,
    ToDeviceRecipient,
};

use crate::compat::{
    MemberContent, MemberEventRoute, OutboundDialect, element_call, element_call_state,
};

// The sticky duration for `m.rtc.member` now comes from the core
// (`JoinSessionParams::sticky_duration_ms`), which re-sends the membership at
// half that interval to stay in the map. It used to be a constant here, which
// meant nothing knew when the entry would lapse.
//
// NOTE: the delayed leave is a plain (non-sticky) delayed event, so it clears
// nothing from the sticky map when it fires — crash cleanup relies entirely on
// the membership's own sticky TTL expiring. That makes the dead man's switch
// ceremonial today, and it is why a crashed client lingers as a ghost for up to
// `sticky_duration_ms` rather than `keep_alive_timeout_ms`.
//
// It is *not* an HTTP-level limitation. MSC4140 has no endpoint of its own: both
// `org.matrix.msc4140.delay` and `org.matrix.msc4354.sticky_duration_ms` are
// query parameters on the ordinary
// `PUT /_matrix/client/v3/rooms/{room}/send/{type}/{txn}`, so a sticky delayed
// event is just both at once. The blocker is ruma's types —
// `delayed_message_event::Request` has no sticky field and
// `send_message_event::Request` has no delay field, so neither can express both.
//
// Two things to settle upstream before relying on it:
//   * Neither MSC states that the two compose (MSC4354 only hints: they "may be
//     combined ... to provide heartbeat semantics (e.g required for MatrixRTC)").
//   * `origin_server_ts` for a delayed event must be the *fire* time, not the
//     scheduling time. MSC4354 resolves sticky conflicts by highest
//     `origin_server_ts + duration` ("last to expire wins"), so a leave stamped
//     at scheduling time would expire *before* the join it is meant to replace
//     and would never take effect. MSC4140 only implies fire time, in a footnote
//     about event IDs.

fn command_error(error: impl std::fmt::Display) -> CommandError {
    CommandError::from_message(error.to_string())
}

/// [`command_error`] for the three MSC4140 endpoints, which get one distinction
/// the others do not need: "this homeserver will never do this".
///
/// Worth telling apart because the caller's response differs in kind. An
/// ordinary failure is retried on the next heartbeat; a refusal makes
/// [`matrix_rtc_core::OwnMembershipMachine`] stop asking and fall back to
/// keeping its membership alive by lifetime alone, for the rest of the session.
///
/// Two answers mean it. `M_UNRECOGNIZED` is a homeserver built without the
/// unstable endpoint. `M_FORBIDDEN` is one that has it and has switched it off —
/// matrix.org's "Sending delayed events has been disallowed". Reading
/// `M_FORBIDDEN` this way is safe even for the *other* thing it can mean (we are
/// not allowed to send in this room at all): the membership send that follows
/// would fail for the same reason and surface it properly, so the worst case is
/// a shorter membership lifetime on a join that fails anyway.
fn delayed_command_error(error: matrix_sdk::HttpError) -> CommandError {
    match error.client_api_error_kind() {
        Some(ErrorKind::Unrecognized | ErrorKind::Forbidden) => {
            CommandError::DelayedEventsNotSupported(error.to_string())
        }
        _ => command_error(error),
    }
}

/// The sole conversion from a core event-type string to the wire type.
///
/// Every send in this bridge routes through this so all paths (sticky send,
/// delayed leave) agree on the identifier the homeserver sees. Ruma owns the
/// mapping — e.g. the core's `m.rtc.member` becomes the MSC4143 id
/// `org.matrix.msc4143.rtc.member`, and it follows the stable id automatically
/// once ruma flips. Bypassing this (passing a raw string to `send_raw`) is the
/// footgun it exists to remove: the paths would then serialize differently.
fn wire_event_type(event_type: String) -> MessageLikeEventType {
    MessageLikeEventType::from(event_type)
}

/// An [`RtcCommandSender`] backed by a `matrix_sdk::Client`.
///
/// Clone-cheap: holds only a `Client` (itself an `Arc` inside).
#[derive(Clone)]
pub struct SdkCommandSender {
    client: Client,
    /// Which MatrixRTC generation everything this sender puts on the wire is
    /// rendered for. [`OutboundDialect::None`] for a spec-current call, which is
    /// every non-compat one. Opt-in; see [`crate::compat`].
    compat: OutboundDialect,
}

impl SdkCommandSender {
    /// Create a command sender for the given logged-in client, speaking current
    /// MSC4143 and nothing else.
    pub fn new(client: Client) -> Self {
        Self::with_compat(client, OutboundDialect::None)
    }

    /// Create a command sender that renders its output for an older Element Call
    /// generation.
    ///
    /// [`OutboundDialect::Sticky`] keeps a join MSC4143-valid — the legacy fields
    /// are added alongside — so it costs spec-current peers nothing; only leaves
    /// and media keys go out in the legacy dialect alone, because neither can be
    /// expressed in both at once.
    ///
    /// [`OutboundDialect::State`] is not additive at all: the membership becomes
    /// an `org.matrix.msc3401.call.member` room state event, which a spec-current
    /// peer does not look at.
    ///
    /// Temporary: see [`crate::compat`].
    pub fn with_compat(client: Client, compat: OutboundDialect) -> Self {
        Self { client, compat }
    }

    fn room(&self, room_id: &str) -> Result<Room, CommandError> {
        let room_id = RoomId::parse(room_id).map_err(command_error)?;
        self.client
            .get_room(&room_id)
            .ok_or_else(|| CommandError::from_message(format!("room {room_id} not found")))
    }
}

/// Bounded request behaviour for RTC signalling sends.
///
/// The SDK default retries retryable failures without limit, which turns a
/// wedged homeserver into an indefinitely hanging send (observed: a leave
/// awaiting forever while synapse was down with sqlite I/O errors). MatrixRTC
/// state is time-critical — peers act on our membership within seconds — so
/// this stays bounded rather than becoming the SDK's unlimited default.
///
/// The limit is attempts, not retries. Two was too few to survive a rate limit:
/// joining sends a delayed leave *and* a membership, so N clients joining is 2N
/// events, and synapse's `rc_message` defaults to a burst of 10 and 0.2/s after
/// that. The second attempt landed inside the same closed window and the join
/// failed outright, discarding a fleet that had already half-joined.
///
/// Five gives a rate limit room to clear while still failing a genuinely dead
/// homeserver in a readable time. The waits are not ours: on `M_LIMIT_EXCEEDED`
/// the SDK uses the server's own `Retry-After` when it sends one, so the delay
/// is what the homeserver asked for rather than a number we invented.
///
/// Retrying is not a substitute for pacing. A client joining N devices faster
/// than the server's sustained rate will exhaust any attempt count; the retry is
/// there to absorb a burst, not to outlast a limiter (see `--ramp-ms` in the
/// load generator).
fn rtc_request_config() -> matrix_sdk::config::RequestConfig {
    matrix_sdk::config::RequestConfig::new()
        .timeout(Duration::from_secs(15))
        .retry_limit(5)
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl RtcCommandSender for SdkCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        let room = self.room(&room_id)?;
        let content = self.compat.rewrite_notification(&event_type, content);
        match self
            .compat
            .route_member_event(event_type, content, Some(duration_ms))
        {
            MemberEventRoute::Sticky {
                event_type,
                content,
            } => {
                let event_type = wire_event_type(event_type).to_string();
                // The core's value, not a constant of ours: it schedules the
                // refresh against exactly this lifetime, so substituting one here
                // would break the refresh. The core clamps to
                // `MAX_STICKY_DURATION_MS` for the same reason the SDK does —
                // anything longer comes back as an hour, and a refresh scheduled
                // against the longer figure would fire after the entry had
                // already lapsed.
                let duration_ms = u32::try_from(duration_ms).unwrap_or(u32::MAX);
                let response = room
                    .send_raw(&event_type, &content)
                    .with_sticky_duration_ms(duration_ms)
                    .with_request_config(rtc_request_config())
                    .await
                    .map_err(command_error)?;
                Ok(response.response.event_id.to_string())
            }
            // `duration_ms` is dropped on purpose: room state has no TTL, and in
            // this dialect the lifetime is stated inside the content instead
            // (`created_ts` + `expires`). The core's periodic re-send still
            // fires and is harmless — the content is byte-identical, so the
            // homeserver either drops the duplicate or accepts one that changes
            // nothing a peer reads.
            //
            // Not `Room::send_state_event_raw`: that has no
            // `with_request_config`, so it would inherit the SDK's unlimited
            // retries — exactly the hang `rtc_request_config` exists to prevent,
            // and this membership is every bit as time-critical as the sticky one.
            MemberEventRoute::State {
                event_type,
                state_key,
                content,
            } => {
                let raw = serde_json::value::to_raw_value(&content).map_err(command_error)?;
                let request = send_state_event::v3::Request::new_raw(
                    room.room_id().to_owned(),
                    StateEventType::from(event_type),
                    state_key,
                    Raw::<AnyStateEventContent>::from_json(raw),
                );
                let response = self
                    .client
                    .send(request)
                    .with_request_config(rtc_request_config())
                    .await
                    .map_err(command_error)?;
                Ok(response.event_id.to_string())
            }
        }
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        let room_id = RoomId::parse(&room_id).map_err(command_error)?;
        let delay = DelayParameters::Timeout {
            timeout: Duration::from_millis(delay_ms),
        };

        // The delayed leave is a member event like any other, and a peer that
        // cannot read it is a peer we stay visible to forever — so it goes
        // through the same routing as the join it is paired with.
        // No lifetime: the delayed leave's legacy content is `{}`, which has
        // nowhere to carry a deadline and needs none.
        let delay_id = match self.compat.route_member_event(event_type, content, None) {
            MemberEventRoute::Sticky {
                event_type,
                content,
            } => {
                let raw = serde_json::value::to_raw_value(&content).map_err(command_error)?;
                let request = delayed_message_event::unstable::Request::new_raw(
                    room_id,
                    TransactionId::new(),
                    wire_event_type(event_type),
                    delay,
                    Raw::<AnyMessageLikeEventContent>::from_json(raw),
                );
                self.client
                    .send(request)
                    .with_request_config(rtc_request_config())
                    .await
                    .map_err(delayed_command_error)?
                    .delay_id
            }
            // Worth knowing: this arm gives the *legacy* path a dead man's switch
            // that the modern one still lacks. The comment above on delayed
            // events notes that a delayed sticky leave clears nothing from the
            // sticky map, so crash cleanup really rides on the sticky TTL. A
            // delayed **state** event with `{}` content genuinely empties our
            // membership, which is what MSC4140 was built for and what this
            // generation of Element Call relies on.
            //
            // NOTE the argument order: `state_key` comes *before* `event_type`
            // here, unlike every other ruma send.
            MemberEventRoute::State {
                event_type,
                state_key,
                content,
            } => {
                let raw = serde_json::value::to_raw_value(&content).map_err(command_error)?;
                let request = delayed_state_event::unstable::Request::new_raw(
                    room_id,
                    state_key,
                    StateEventType::from(event_type),
                    delay,
                    Raw::<AnyStateEventContent>::from_json(raw),
                );
                self.client
                    .send(request)
                    .with_request_config(rtc_request_config())
                    .await
                    .map_err(delayed_command_error)?
                    .delay_id
            }
        };

        // Returns the MSC4140 delay id, which is what restart/cancel take — and
        // both of those are id-based, so they need no dialect of their own.
        Ok(delay_id)
    }

    async fn restart_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        // MSC4140's "heartbeat ping": resets the timer to now + the original
        // delay, keeping the same delay id. One request, and never a moment
        // with no delayed leave armed.
        let request =
            update_delayed_event::unstable_v1::Request::new(delay_id, UpdateAction::Restart);
        self.client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(delayed_command_error)?;
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        let request =
            update_delayed_event::unstable_v1::Request::new(delay_id, UpdateAction::Cancel);
        self.client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(delayed_command_error)?;
        Ok(())
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let room = self.room(&room_id)?;
        // Normalise through ruma, which registers `m.rtc.slot` as an alias of
        // the MSC4143 id and stringifies back to whichever it currently treats
        // as primary — `org.matrix.msc4143.rtc.slot` today, the stable id once
        // ruma flips after FCP. Same mechanism as `wire_event_type`.
        let event_type = StateEventType::from(event_type).to_string();
        let response = room
            .send_state_event_raw(&event_type, &state_key, &content)
            .await
            .map_err(command_error)?;
        Ok(response.event_id.to_string())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        // Unlike a member event, a to-device message cannot carry both dialects
        // at once — the type is one or the other — so in compat mode the media
        // key goes out in the legacy dialect alone.
        let (message_type, content) = self
            .compat
            .rewrite_key_message(&message_type, &content)
            .unwrap_or((message_type, content));

        // MSC4143 encryption-key distribution: the media key goes out as an
        // Olm-encrypted to-device message to exactly the devices that published
        // the memberships. There is deliberately no `"*"` fan-out: media keys
        // must not reach devices outside the call, and for our own user a
        // fan-out would include this device, which Olm cannot encrypt to.
        //
        // One SDK call for the whole batch — `encrypt_and_send_raw_to_device`
        // takes a device list and answers with the ones it could not reach, which
        // is exactly the per-recipient outcome the core needs.
        let encryption = self.client.encryption();
        let mut devices = Vec::with_capacity(recipients.len());
        let mut unknown = Vec::new();
        // Users we have already forced a `/keys/query` for in this batch, so a
        // room full of one user's devices costs one query rather than N.
        let mut queried: HashSet<OwnedUserId> = HashSet::new();

        for recipient in &recipients {
            let user = match UserId::parse(&recipient.user_id) {
                Ok(user) => user,
                Err(error) => {
                    unknown.push(ToDeviceDelivery::failed(
                        recipient.clone(),
                        format!("unparseable user id: {error}"),
                    ));
                    continue;
                }
            };
            let device_id = <&DeviceId>::from(recipient.device_id.as_str());

            let mut found = encryption.get_device(&user, device_id).await;

            // Not in the crypto store yet. That is routine on the *first* key we
            // ever send a peer, and it is much more likely on the pre-sticky
            // path: there our membership is an unencrypted state event, so
            // nothing has forced a `/keys/query` for this user and the media key
            // is the first thing we ever try to encrypt to them. (On the spec
            // path the membership is an encrypted room event, whose megolm
            // pre-share does the query for us — which is why this was invisible
            // until the state dialect existed.)
            //
            // So ask, once per user per batch, instead of reporting the peer
            // unreachable and waiting for a later rollout to retry.
            if matches!(found, Ok(None)) && queried.insert(user.clone()) {
                log::debug!(
                    "{user}/{device_id} is not in the crypto store; querying keys before \
                     giving up on delivering a media key to them",
                );
                if let Err(error) = encryption.request_user_identity(&user).await {
                    log::warn!("keys query for {user} failed: {error}");
                }
                found = encryption.get_device(&user, device_id).await;
            }

            match found {
                Ok(Some(device)) => devices.push(device),
                // Not an error for the batch: the other recipients still get the
                // key, and this one is reported unserved so it is retried once
                // the device is known.
                Ok(None) => unknown.push(ToDeviceDelivery::failed(
                    recipient.clone(),
                    "no such known device",
                )),
                Err(error) => unknown.push(ToDeviceDelivery::failed(
                    recipient.clone(),
                    format!("could not look up the device: {error}"),
                )),
            }
        }

        if devices.is_empty() {
            return Ok(unknown);
        }

        let raw: Raw<AnyToDeviceEventContent> =
            Raw::new(&content).map_err(command_error)?.cast_unchecked();
        let failures = encryption
            .encrypt_and_send_raw_to_device(
                devices.iter().collect(),
                &message_type,
                raw,
                // MSC4153: refuse to hand keys to unverified identities.
                CollectStrategy::IdentityBasedStrategy,
            )
            .await
            .map_err(command_error)?;

        let mut deliveries = unknown;
        for device in &devices {
            let recipient =
                ToDeviceRecipient::new(device.user_id().as_str(), device.device_id().as_str());
            let failed = failures
                .iter()
                .any(|(user, dev)| user == device.user_id() && dev == device.device_id());
            deliveries.push(if failed {
                ToDeviceDelivery::failed(recipient, "the homeserver did not accept the message")
            } else {
                ToDeviceDelivery::sent(recipient)
            });
        }

        if !failures.is_empty() {
            log::warn!(
                "to-device {message_type}: {} of {} device(s) did not receive the key",
                failures.len(),
                devices.len(),
            );
        }
        Ok(deliveries)
    }
}

/// An origin built from a device the member event only claims, or `None` when
/// it claimed none. Never used where decryption named a device.
fn claimed_origin(claimed_device: &Option<String>) -> Option<EventOrigin> {
    claimed_device.as_ref().map(EventOrigin::claimed)
}

/// Snapshot the room's live `m.rtc.member` sticky events as core DTOs.
///
/// The sending device comes from the event's decryption metadata, which is the
/// only place MSC4143 leaves it: the proposal removed the self-asserted
/// `member.claimed_device_id`, and key distribution targets "the devices that
/// were used to encrypt these member events". An event with no such metadata
/// falls back to the device it claims, if any (see [`crate::compat`]), and
/// otherwise names none — in which case that member can neither be sent a key
/// nor have one accepted from them.
fn snapshot(room: &Room) -> Vec<RawStickyEvent> {
    let room_id = room.room_id().to_string();
    room.live_sticky_events()
        .into_iter()
        .filter_map(|entry| {
            let event_type = entry.key.event_type.clone();
            if event_type != "m.rtc.member" && event_type != "org.matrix.msc4143.rtc.member" {
                return None;
            }
            // A member event we cannot parse is a member who never appears in
            // the call, so say so — silently dropping it here is indistinguishable
            // from the peer never having joined, and that ambiguity has cost real
            // debugging time.
            //
            // Parsed as raw JSON first so the pre-2026 dialect can be normalised
            // away before anything typed sees it. That step is unconditional: it
            // only ever fills in a modern field that is absent, so a spec-shaped
            // event reaches `RawStickyEventContent` byte-identical either way.
            let mut value: Value = match entry.raw().get_field("content") {
                Ok(Some(content)) => content,
                Ok(None) => {
                    log::warn!(
                        "[{room_id}] ignoring an {event_type} sticky with no content object \
                         (sticky key {})",
                        entry.key.sticky_key.as_deref().unwrap_or("<none>"),
                    );
                    return None;
                }
                Err(error) => {
                    log::warn!(
                        "[{room_id}] ignoring an unparseable {event_type} sticky (sticky key \
                         {}): {error}. That member will not appear in the call.",
                        entry.key.sticky_key.as_deref().unwrap_or("<none>"),
                    );
                    return None;
                }
            };

            if element_call::normalize_member_content(&mut value) == MemberContent::BareLeave {
                // A pre-2026 leave: content is the sticky key and nothing else,
                // so there is no slot to file it under. Dropping it *is* the
                // leave — the live set below is applied whole, and a member who
                // contributes no event is a member who is gone.
                log::debug!(
                    "[{room_id}] pre-2026 leave for sticky key {}; treating the member as gone",
                    entry.key.sticky_key.as_deref().unwrap_or("<none>"),
                );
                return None;
            }
            let claimed_device = element_call::claimed_device_id(&value);

            let content: RawStickyEventContent = match serde_json::from_value(value) {
                Ok(content) => content,
                Err(error) => {
                    log::warn!(
                        "[{room_id}] ignoring an unparseable {event_type} sticky (sticky key \
                         {}): {error}. That member will not appear in the call.",
                        entry.key.sticky_key.as_deref().unwrap_or("<none>"),
                    );
                    return None;
                }
            };

            // The sticky map only files plaintext and successfully decrypted
            // events, so the presence of decryption metadata is exactly whether
            // this arrived encrypted — never "we don't know".
            //
            // A device the event merely *claims* is the last resort, never a
            // preference: it is consulted only where decryption produced none,
            // and it is what makes a pre-2026 Element Call peer usable at all
            // (the widget API gives that client no decryption metadata, so a
            // self-asserted device is the only one it can state). See
            // `EventOrigin::Claimed`.
            let origin = match entry.encryption_info() {
                Some(info) => {
                    let device = info.sender_device.as_ref().map(|device| device.to_string());
                    match device {
                        Some(device) => EventOrigin::encrypted(Some(device)),
                        // Olm messages carry the sender's device keys, so a
                        // decrypted event should always name one. Worth saying
                        // out loud here: downstream this member cannot be bound
                        // to a device, so their media keys get rejected.
                        None => {
                            log::warn!(
                                "decrypted {} from {} resolved to no sending device",
                                event_type,
                                entry.key.sender,
                            );
                            claimed_origin(&claimed_device)
                                .unwrap_or_else(|| EventOrigin::encrypted(None))
                        }
                    }
                }
                None => claimed_origin(&claimed_device).unwrap_or(EventOrigin::Cleartext),
            };
            Some(RawStickyEvent {
                room_id: room_id.clone(),
                sender: entry.key.sender.to_string(),
                origin,
                event_type,
                content,
            })
        })
        .collect()
}

/// Snapshot the room's `m.rtc.slot` state as core DTOs, or `None` when the
/// state could not be determined this tick.
///
/// The state key is the slot id. Events whose content will not parse are still
/// reported, with empty content, so the slot resolves closed rather than
/// vanishing — an unreadable slot event is not an open slot.
///
/// This asks the homeserver (`GET /rooms/{id}/state`) instead of the SDK's
/// state store: the store only holds event types listed in sliding sync's
/// `required_state`, which does not (yet) include the MSC4143 slot type, so
/// the store reports every room as slotless — and feeding that into the core
/// closes the slot and drops every member. For the same reason a failed fetch
/// returns `None` (skip this tick's update) rather than "no slots". Once the
/// SDK's sliding sync config carries the slot type, this can go back to
/// reading the store.
///
/// Filtering happens on the raw `type` string, so the stable and unstable ids
/// are accepted alike — same as the member path, and immune to which of the
/// two ruma currently treats as primary.
async fn slot_snapshot(room: &Room) -> Option<Vec<RawSlotEvent>> {
    let room_id = room.room_id().to_string();
    let request = get_state_events::v3::Request::new(room.room_id().to_owned());
    let response = match room.client().send(request).await {
        Ok(response) => response,
        Err(error) => {
            log::warn!("failed to fetch room state for m.rtc.slot: {error}");
            return None;
        }
    };

    let slots = response
        .room_state
        .into_iter()
        .filter_map(|raw| {
            let event_type = raw.get_field::<String>("type").ok().flatten()?;
            if event_type != SLOT_EVENT_TYPE && event_type != "org.matrix.msc4143.rtc.slot" {
                return None;
            }

            let state_key = raw.get_field::<String>("state_key").ok().flatten();
            let content = raw
                .get_field::<RawSlotEventContent>("content")
                .ok()
                .flatten()
                .unwrap_or_default();

            Some(RawSlotEvent {
                room_id: room_id.clone(),
                slot_id: state_key?,
                content,
            })
        })
        .collect::<Vec<_>>();

    log::debug!(
        "[{room_id}] room state fetched: {} m.rtc.slot event(s): {:?}",
        slots.len(),
        slots.iter().map(|slot| &slot.slot_id).collect::<Vec<_>>(),
    );

    Some(slots)
}

/// Snapshot the users currently joined to the room.
async fn joined_members_snapshot(room: &Room) -> Vec<String> {
    match room.members(RoomMemberships::JOIN).await {
        Ok(members) => members
            .into_iter()
            .map(|member| member.user_id().to_string())
            .collect(),
        Err(error) => {
            log::warn!("failed to read room members: {error}");
            Vec::new()
        }
    }
}

/// Snapshot the room's pre-sticky Element Call memberships —
/// `org.matrix.msc3401.call.member` room state — as core DTOs.
///
/// Reads the **SDK state store**, which works here and famously does not for
/// `m.rtc.slot`: `(StateEventType::CallMember, "*")` is in `matrix-sdk-ui`'s
/// `DEFAULT_REQUIRED_STATE`, so sliding sync already puts these events in the
/// store, while the MSC4143 slot type is not and [`slot_snapshot`] has to go over
/// HTTP for it. No request, and nothing to fail this tick.
///
/// `StateEventType::CallMember` rather than the string, deliberately: if ruma's
/// `unstable-msc3401` ever goes away the failure becomes a compile error instead
/// of an empty roster. The debug line below is the other half of that — a silent
/// "zero events" is exactly what a vanished `required_state` entry would look
/// like.
///
/// The typed accessors next door (`Room::active_room_call_participants`) are not
/// usable: `RoomInfo::rtc_member_events` is `pub(crate)`, it drops the sender, and
/// it drops empty-content (left) events outright — so a member who left would be
/// indistinguishable from one who never existed. Raw JSON it is, which is also
/// what [`element_call_state`] wants; see its docs for why ruma's typed content is
/// the wrong tool even where it is reachable.
///
/// Returns an empty vec on failure. Unlike the slot fetch there is nothing to
/// distinguish "could not read" from "nobody is in the call" — but the cost of
/// guessing wrong is one tick of a stale roster, not a closed slot that drops
/// every member, so it is not worth a third state.
///
/// Temporary; see [`crate::compat`].
async fn element_call_state_snapshot(room: &Room) -> Vec<RawStickyEvent> {
    let room_id = room.room_id().to_string();
    let raw = match room.get_state_events(StateEventType::CallMember).await {
        Ok(raw) => raw,
        Err(error) => {
            log::warn!(
                "[{room_id}] could not read {} state: {error}. Members of a pre-sticky \
                 Element Call session will not appear.",
                element_call_state::STATE_MEMBER_EVENT_TYPE,
            );
            return Vec::new();
        }
    };

    let events: Vec<element_call_state::StateMemberEvent> = raw
        .into_iter()
        .filter_map(|state| {
            // Stripped state belongs to a room we are only invited to, and it
            // carries no `origin_server_ts` — which is the fallback deadline this
            // translation needs. We are never joined to such a room, so there is
            // nothing to lose by skipping it.
            let RawAnySyncOrStrippedState::Sync(raw) = state else {
                return None;
            };
            Some(element_call_state::StateMemberEvent {
                sender: raw.get_field::<String>("sender").ok().flatten()?,
                state_key: raw.get_field::<String>("state_key").ok().flatten()?,
                origin_server_ts: raw.get_field::<u64>("origin_server_ts").ok().flatten()?,
                content: raw.get_field::<Value>("content").ok().flatten()?,
            })
        })
        .collect();

    // One clock reading for the whole set, so two members are never judged
    // against two different "now"s.
    let translated =
        element_call_state::translate_state_memberships(&events, element_call_state::now_ms());
    log::debug!(
        "[{room_id}] {} {} state event(s) translated to {} live membership(s)",
        events.len(),
        element_call_state::STATE_MEMBER_EVENT_TYPE,
        translated.len(),
    );

    translated
        .into_iter()
        .filter_map(|membership| {
            let content: RawStickyEventContent = match serde_json::from_value(membership.content) {
                Ok(content) => content,
                Err(error) => {
                    log::warn!(
                        "[{room_id}] a translated pre-sticky membership from {} does not parse as \
                         MSC4143 content ({error}). That is a bug in the translation, not in the \
                         peer; that member will not appear in the call.",
                        membership.sender,
                    );
                    return None;
                }
            };
            Some(RawStickyEvent {
                room_id: room_id.clone(),
                sender: membership.sender,
                // The self-asserted device, which is all such a peer can state
                // and all we can read — state events carry no decryption
                // metadata anywhere in the SDK. Ranked below an authenticated
                // device everywhere it matters (it never satisfies the
                // encrypted-room rule), but it is what lets a media key travel
                // in either direction at all.
                origin: EventOrigin::claimed(membership.claimed_device_id),
                // Not the type that was on the wire. What the core accepts is an
                // MSC4143 membership, and after the translation that is exactly
                // what this is; carrying the MSC3401 type here would only make
                // `check_convertible` reject it.
                event_type: "m.rtc.member".to_owned(),
                content,
            })
        })
        .collect()
}

/// Push the room state that gates MSC4143 membership into `manager`.
///
/// `state_membership` says the room's membership lives in room state rather than
/// in sticky events, which changes what we can honestly say about slots — see
/// below.
async fn feed_room_state(
    room: &Room,
    manager: &Arc<Mutex<RtcSessionManager<SdkCommandSender>>>,
    state_membership: bool,
) {
    // Skipped entirely in state mode, so don't pay for it either.
    let slots = if state_membership {
        None
    } else {
        slot_snapshot(room).await
    };
    let members = joined_members_snapshot(room).await;
    let encrypted = room
        .latest_encryption_state()
        .await
        .map(|state| state.is_encrypted());
    let room_id = room.room_id().as_str();

    let mut manager = manager.lock().await;
    // Encryption first: it decides how the slots that follow resolve.
    match encrypted {
        Ok(encrypted) => {
            manager
                .on_room_encryption_received(room_id, encrypted)
                .await
        }
        Err(error) => log::warn!("failed to read room encryption state: {error}"),
    }
    // A `None` snapshot means either the fetch failed or we are in state mode.
    //
    // Failed fetch: leave the slot knowledge as it was rather than reporting an
    // empty (all-closed) room.
    //
    // State mode: a pre-sticky Element Call room has no `m.rtc.slot` at all — the
    // concept postdates that generation — so reporting "no slots" would resolve
    // every session closed and drop every member, us included. Saying nothing
    // leaves the condition `SlotKnowledge::Unsupplied`, which is what the core
    // means by "nothing has told us about the slot" and is the honest answer:
    // unknowable, not closed. Encryption still reaches the core above, and with
    // no slot to negotiate from the local `EncryptionConfig` stands, which
    // defaults to managing media keys — so E2EE stays on, as that generation
    // expects.
    if let Some(slots) = slots {
        manager.on_room_slots_received(room_id, slots).await;
    }
    manager.on_room_members_received(room_id, members).await;
}

/// One pass of the bridge: re-read the room state that gates membership, then
/// hand the core the room's complete current membership.
///
/// Both halves run every tick because both can change at any time — a slot can
/// close, a member can leave the room — and MSC4143 requires clients to respect
/// the latest state.
async fn tick(
    room: &Room,
    manager: &Arc<Mutex<RtcSessionManager<SdkCommandSender>>>,
    state_membership: bool,
) {
    let room_id = room.room_id().as_str();

    feed_room_state(room, manager, state_membership).await;

    // The live set *is* the current state, so hand it over whole. This used to
    // keep a copy of the previous set and diff it to turn TTL expiries into
    // synthetic leaves, because the core merged rather than replaced;
    // `set_current_sticky_state` does that now, and correctly for a slot whose
    // last member expired — such a slot contributes no events at all, so a diff
    // over events could never have noticed it.
    let mut current = snapshot(room);
    let from_sticky = current.len();

    // Which is also why the two membership sources are concatenated into **one**
    // list for **one** call. `set_current_sticky_state` replaces the whole room:
    // handed the sticky set and then the state set, each call would wipe the
    // other's members and the roster would flicker between the two halves of the
    // call. If a future edit ever needs a third source, it joins this list — it
    // does not get a call of its own.
    if state_membership {
        // Sticky wins on a collision. An Element Call build mid-transition can
        // write both, and the same human twice in the roster means two receive
        // streams and two key exchanges for one peer.
        let seen: Vec<String> = current
            .iter()
            .map(|event| event.content.sticky_key.clone())
            .collect();
        current.extend(
            element_call_state_snapshot(room)
                .await
                .into_iter()
                .filter(|event| !seen.contains(&event.content.sticky_key)),
        );
    }

    log::debug!(
        "[{room_id}] tick: {from_sticky} sticky + {} pre-sticky state membership(s)",
        current.len() - from_sticky,
    );

    if let Err(error) = manager
        .lock()
        .await
        .set_current_sticky_state(room_id, current)
        .await
    {
        log::warn!("[{room_id}] failed to apply the current membership: {error}");
    }
}

/// Whether a wake source's `recv()` means "keep bridging".
///
/// A lagged receiver still means something changed, and the next tick re-reads
/// everything anyway; a closed one is the end of the room.
fn keep_bridging<T>(result: Result<T, RecvError>) -> bool {
    matches!(result, Ok(_) | Err(RecvError::Lagged(_)))
}

/// How often to re-tick in state mode with nothing else to wake us.
///
/// A pre-sticky membership expires by a deadline inside its own content, with no
/// event to announce it, so in a room that has gone quiet nothing would prompt us
/// to notice. Far below that dialect's four-hour default, and only ever paid in a
/// mode that runs against a test deployment.
const STATE_MEMBERSHIP_POLL: Duration = Duration::from_secs(30);

/// Feed a room's live membership into `manager` until the room is dropped.
///
/// Seeds the manager with the current snapshot, then re-derives the whole
/// membership on every wake. The core's session model is idempotent, so
/// re-feeding the full set each tick is safe; connected members surface as joins,
/// disconnects and TTL expiries as leaves.
///
/// `state_membership` additionally reads pre-sticky Element Call membership out of
/// `org.matrix.msc3401.call.member` room state, for interoperating with Element
/// Call builds older than MSC4354. Opt-in; see [`crate::compat`] and delete the
/// parameter with it.
///
/// Intended to be `tokio::spawn`ed. Returns when a wake source closes.
pub async fn run_sticky_bridge(
    room: Room,
    manager: Arc<Mutex<RtcSessionManager<SdkCommandSender>>>,
    state_membership: bool,
) {
    let room_id = room.room_id().to_string();
    log::info!("[{room_id}] sticky bridge started");

    let mut sticky = room.subscribe_to_sticky_events();

    // Second wake source, and only in state mode: that dialect keeps membership
    // in room state, so such a call produces **no sticky traffic whatsoever** — a
    // bridge waiting on `sticky` alone would seed once and then sleep through the
    // entire call. This is also the room-state subscription the NOTE below used to
    // ask for, arriving by the side door.
    //
    // Deliberately not subscribed when the mode is off. A tick costs a
    // `GET /rooms/{id}/state` (see `slot_snapshot`) and room updates arrive on
    // every sync that touches the room, so subscribing unconditionally would put
    // an HTTP round trip on every sync of every call.
    let mut room_updates = state_membership.then(|| room.subscribe_to_updates());

    // Seed before waiting on anything. `tick` feeds the gating room state before
    // the members, so a member is never briefly considered joined to a slot that
    // room state says is closed.
    tick(&room, &manager, state_membership).await;

    loop {
        // NOTE: without `state_membership` this still only re-checks when sticky
        // traffic arrives, so a slot closing in an otherwise idle room is noticed
        // late. Fixing that for the spec path means subscribing to room updates
        // there too — and paying the state fetch per sync, or throttling it
        // first. Still a follow-up.
        let woken = match &mut room_updates {
            Some(room_updates) => tokio::select! {
                result = sticky.recv() => keep_bridging(result),
                result = room_updates.recv() => keep_bridging(result),
                _ = tokio::time::sleep(STATE_MEMBERSHIP_POLL) => true,
            },
            None => keep_bridging(sticky.recv().await),
        };
        if !woken {
            break;
        }

        tick(&room, &manager, state_membership).await;
    }

    log::info!("[{room_id}] sticky bridge stopped");
}

#[cfg(test)]
mod tests {
    use matrix_rtc_core::{CallMembershipEvent, RtcTransport};

    use super::*;

    /// A join as observed from Element Call on the JS SDK, pre-sticky.
    const EC_STATE_JOIN: &str = r#"{
        "application": "m.call",
        "call_id": "",
        "scope": "m.room",
        "device_id": "V5cP8FErcB",
        "membershipID": "@alice:example.io:V5cP8FErcB",
        "expires": 14400000,
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

    /// The seam `compat::element_call_state` cannot test itself.
    ///
    /// That module is `serde_json`-only on purpose, so nothing inside it can
    /// assert that what it emits actually parses as a core type and projects to a
    /// *joined* member. This is the assertion that the translation and the core's
    /// `RawStickyEventContent` agree, and it is the one that would break first if
    /// either side moved.
    #[test]
    fn a_translated_pre_sticky_membership_is_a_joined_core_membership() {
        let now = element_call_state::now_ms();
        let events = [element_call_state::StateMemberEvent {
            sender: "@alice:example.io".to_owned(),
            state_key: "_@alice:example.io_V5cP8FErcB_m.call".to_owned(),
            // No `created_ts` in the fixture — the first join of a session — so
            // the lifetime is measured from here.
            origin_server_ts: now,
            content: serde_json::from_str(EC_STATE_JOIN).expect("valid json"),
        }];

        let mut translated = element_call_state::translate_state_memberships(&events, now);
        assert_eq!(translated.len(), 1, "a live join must survive translation");
        let membership = translated.pop().unwrap();

        let content: RawStickyEventContent = serde_json::from_value(membership.content)
            .expect("the translation must produce parseable MSC4143 content");
        let event = RawStickyEvent {
            room_id: "!room:example.io".to_owned(),
            sender: membership.sender,
            origin: EventOrigin::claimed(membership.claimed_device_id),
            event_type: "m.rtc.member".to_owned(),
            content,
        };

        let joined = match event
            .try_into_call_membership_event()
            .expect("converts to a membership event")
        {
            CallMembershipEvent::Joined(joined) => joined,
            CallMembershipEvent::Left(_) => panic!("a join must not project as a leave"),
        };

        assert_eq!(joined.slot_id, "m.call#ROOM");
        assert_eq!(joined.sender, "@alice:example.io");
        assert_eq!(joined.member_id, "@alice:example.io:V5cP8FErcB");
        // The core keys the roster by one and binds media keys by the other.
        assert_eq!(joined.sticky_key, joined.member_id);
        assert_eq!(joined.application.as_deref(), Some("m.call"));
        assert_eq!(joined.can_subscribe, vec!["livekit".to_owned()]);
        // The claimed device is what a media key gets addressed to; without it
        // `remote_identity` returns `None` and the peer's media is unattributable.
        assert_eq!(joined.origin.sender_device_id(), Some("V5cP8FErcB"));
        // ...and it must not read as cleartext-in-an-encrypted-room, or
        // `join_condition` would drop it.
        assert_eq!(joined.origin.was_encrypted(), None);

        match joined.transports.as_slice() {
            [RtcTransport::LiveKit(livekit)] => assert_eq!(
                livekit.livekit_service_url,
                "https://mrtc.example.io/livekit/jwt"
            ),
            other => panic!("expected exactly one livekit transport, got {other:?}"),
        }
    }

    /// The leave, end to end: an empty content contributes no event at all, and
    /// because the bridge replaces the whole membership every tick, contributing
    /// nothing *is* leaving.
    #[test]
    fn a_pre_sticky_leave_contributes_no_membership() {
        let now = element_call_state::now_ms();
        let events = [element_call_state::StateMemberEvent {
            sender: "@alice:example.io".to_owned(),
            state_key: "_@alice:example.io_V5cP8FErcB_m.call".to_owned(),
            origin_server_ts: now,
            content: serde_json::json!({}),
        }];

        assert!(
            element_call_state::translate_state_memberships(&events, now).is_empty(),
            "an empty content is this dialect's leave"
        );
    }
}
