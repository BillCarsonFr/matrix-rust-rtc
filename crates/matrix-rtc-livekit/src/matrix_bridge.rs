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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use matrix_sdk::ruma::api::client::delayed_events::update_delayed_event::UpdateAction;
use matrix_sdk::ruma::api::client::delayed_events::{
    DelayParameters, delayed_message_event, update_delayed_event,
};
use matrix_sdk::ruma::api::client::state::get_state_events;
use matrix_sdk::ruma::events::{
    AnyMessageLikeEventContent, AnyToDeviceEventContent, MessageLikeEventType, StateEventType,
};
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{DeviceId, RoomId, TransactionId, UserId};
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

use crate::compat::{ElementCallDialect, MemberContent, element_call};

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
    /// When set, everything this sender puts on the wire is also rendered in
    /// the pre-2026 Element Call dialect. Opt-in; see [`crate::compat`].
    compat: Option<ElementCallDialect>,
}

impl SdkCommandSender {
    /// Create a command sender for the given logged-in client.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            compat: None,
        }
    }

    /// Create a command sender that also speaks the pre-2026 Element Call
    /// dialect, for interoperating with clients that have not caught up with
    /// the 2026 MSC4143 rewrite.
    ///
    /// Member events stay MSC4143-valid — the legacy fields are added alongside
    /// — so this costs spec-current peers nothing. Media keys, whose message
    /// type cannot be two things at once, go out in the legacy dialect *only*.
    ///
    /// Temporary: see [`crate::compat`].
    pub fn with_element_call_compat(client: Client, dialect: ElementCallDialect) -> Self {
        Self {
            client,
            compat: Some(dialect),
        }
    }

    fn room(&self, room_id: &str) -> Result<Room, CommandError> {
        let room_id = RoomId::parse(room_id).map_err(command_error)?;
        self.client
            .get_room(&room_id)
            .ok_or_else(|| CommandError::from_message(format!("room {room_id} not found")))
    }

    /// Render an outgoing room event in the legacy dialect as well, when one is
    /// configured. No-op otherwise, which is every non-compat call.
    fn apply_compat(&self, event_type: &str, content: &mut Value) {
        if let Some(dialect) = &self.compat
            && ElementCallDialect::is_member_event(event_type)
        {
            dialect.add_member_aliases(content);
        }
    }
}

/// Bounded request behaviour for RTC signalling sends.
///
/// The SDK default retries retryable failures without limit, which turns a
/// wedged homeserver into an indefinitely hanging send (observed: a leave
/// awaiting forever while synapse was down with sqlite I/O errors). MatrixRTC
/// state is time-critical — peers act on our membership within seconds — so
/// fail after a couple of attempts and let the caller decide.
fn rtc_request_config() -> matrix_sdk::config::RequestConfig {
    matrix_sdk::config::RequestConfig::new()
        .timeout(Duration::from_secs(15))
        .retry_limit(2)
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl RtcCommandSender for SdkCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        mut content: Value,
        duration_ms: u64,
    ) -> Result<(), CommandError> {
        let room = self.room(&room_id)?;
        self.apply_compat(&event_type, &mut content);
        let event_type = wire_event_type(event_type).to_string();
        // The core's value, not a constant of ours: it schedules the refresh
        // against exactly this lifetime, so substituting one here would break
        // the refresh. The core clamps to `MAX_STICKY_DURATION_MS` for the same
        // reason the SDK does — anything longer comes back as an hour, and a
        // refresh scheduled against the longer figure would fire after the entry
        // had already lapsed.
        let duration_ms = u32::try_from(duration_ms).unwrap_or(u32::MAX);
        room.send_raw(&event_type, &content)
            .with_sticky_duration_ms(duration_ms)
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        Ok(())
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        mut content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        let room_id = RoomId::parse(&room_id).map_err(command_error)?;
        // The delayed leave is a member event like any other, and a peer that
        // cannot read it is a peer we stay visible to forever.
        self.apply_compat(&event_type, &mut content);
        let raw = serde_json::value::to_raw_value(&content).map_err(command_error)?;
        let request = delayed_message_event::unstable::Request::new_raw(
            room_id,
            TransactionId::new(),
            wire_event_type(event_type),
            DelayParameters::Timeout {
                timeout: Duration::from_millis(delay_ms),
            },
            Raw::<AnyMessageLikeEventContent>::from_json(raw),
        );
        let response = self
            .client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        // Returns the MSC4140 delay id, which is what restart/cancel take.
        Ok(response.delay_id)
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
            .map_err(command_error)?;
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
            .map_err(command_error)?;
        Ok(())
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<(), CommandError> {
        let room = self.room(&room_id)?;
        // Normalise through ruma, which registers `m.rtc.slot` as an alias of
        // the MSC4143 id and stringifies back to whichever it currently treats
        // as primary — `org.matrix.msc4143.rtc.slot` today, the stable id once
        // ruma flips after FCP. Same mechanism as `wire_event_type`.
        let event_type = StateEventType::from(event_type).to_string();
        room.send_state_event_raw(&event_type, &state_key, &content)
            .await
            .map_err(command_error)?;
        Ok(())
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
        let (message_type, content) = match &self.compat {
            Some(dialect) => dialect
                .rewrite_key_message(&message_type, &content)
                .unwrap_or((message_type, content)),
            None => (message_type, content),
        };

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
            match encryption.get_device(&user, device_id).await {
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

/// Push the room state that gates MSC4143 membership into `manager`.
async fn feed_room_state(room: &Room, manager: &Arc<Mutex<RtcSessionManager<SdkCommandSender>>>) {
    let slots = slot_snapshot(room).await;
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
    // A `None` snapshot means the fetch failed; leave the slot knowledge as it
    // was rather than reporting an empty (all-closed) room.
    if let Some(slots) = slots {
        manager.on_room_slots_received(room_id, slots).await;
    }
    manager.on_room_members_received(room_id, members).await;
}

/// Feed a room's live sticky events into `manager` until the room is dropped.
///
/// Seeds the manager with the current membership snapshot, then re-derives the
/// membership on every sticky-event broadcast by diffing the live set against
/// the last one. Connected members surface as joins, disconnect events and
/// TTL expiries as leaves — the core's session model is idempotent, so
/// re-feeding the full live set each tick is safe.
///
/// Intended to be `tokio::spawn`ed. Returns when the broadcast channel closes.
pub async fn run_sticky_bridge(
    room: Room,
    manager: Arc<Mutex<RtcSessionManager<SdkCommandSender>>>,
) {
    let room_id = room.room_id().to_string();
    log::info!("[{room_id}] sticky bridge started");

    let mut receiver = room.subscribe_to_sticky_events();

    // Seed the room state that gates membership before any member event is
    // applied, so members are never briefly considered joined to a slot that
    // room state says is closed.
    feed_room_state(&room, &manager).await;

    let initial = snapshot(&room);
    log::debug!(
        "[{room_id}] sticky bridge seeding with {} live event(s)",
        initial.len(),
    );
    if let Err(error) = manager
        .lock()
        .await
        .set_current_sticky_state(&room_id, initial)
        .await
    {
        log::warn!("failed to apply the initial sticky state: {error}");
    }

    while let Ok(_) | Err(RecvError::Lagged(_)) = receiver.recv().await {
        // Re-read room state each tick: a slot can close or a member can leave
        // the room at any time, and MSC4143 requires clients to respect the
        // latest state. NOTE: this only re-checks when sticky traffic arrives.
        // Reacting promptly to a slot change in an otherwise idle room needs a
        // room-state subscription of its own; that is a follow-up.
        feed_room_state(&room, &manager).await;

        // The live set *is* the current state, so hand it over whole. This used
        // to keep a copy of the previous set and diff it to turn TTL expiries
        // into synthetic leaves, because the core merged rather than replaced;
        // `set_current_sticky_state` does that now, and correctly for a slot
        // whose last member expired — such a slot contributes no events at all,
        // so a diff over events could never have noticed it.
        let current = snapshot(&room);
        log::debug!("[{room_id}] sticky tick: {} live event(s)", current.len());

        if let Err(error) = manager
            .lock()
            .await
            .set_current_sticky_state(&room_id, current)
            .await
        {
            log::warn!("failed to apply the current sticky state: {error}");
        }
    }

    log::info!("[{room_id}] sticky bridge stopped");
}
