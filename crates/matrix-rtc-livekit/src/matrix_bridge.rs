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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use matrix_sdk::config::RequestConfig;
use matrix_sdk::encryption::identities::Device;
use matrix_sdk::ruma::api::client::delayed_events::update_delayed_event::UpdateAction;
use matrix_sdk::ruma::api::client::delayed_events::{
    DelayParameters, delayed_message_event, update_delayed_event,
};
use matrix_sdk::ruma::api::client::state::{get_state_events, send_state_event};
use matrix_sdk::ruma::events::{
    AnyMessageLikeEventContent, AnyStateEventContent, AnyToDeviceEventContent,
    MessageLikeEventType, StateEventType,
};
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{DeviceId, OwnedDeviceId, OwnedUserId, RoomId, TransactionId, UserId};
use matrix_sdk::{Client, Room, RoomMemberships, RoomState};
use matrix_sdk_base::crypto::CollectStrategy;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use matrix_rtc_core::{
    CommandError, EventOrigin, RawSlotEvent, RawSlotEventContent, RawStickyEvent,
    RawStickyEventContent, RtcCommandSender, RtcSessionManager, SLOT_EVENT_TYPE,
    StickyEventsUpdate,
};

/// Sticky duration for `m.rtc.member` events, clamped to one hour by the SDK.
///
/// The join event lives for this long; liveness while joined relies on the dead
/// man's switch (a delayed leave restarted by heartbeats), and a graceful leave
/// sends an explicit disconnect sticky. NOTE: the delayed leave is currently a
/// plain (non-sticky) delayed event, so crash cleanup relies on this TTL
/// expiring; making the delayed leave itself sticky is a follow-up.
const STICKY_DURATION_MS: u32 = 60 * 60 * 1000;

/// Hard wall-clock cap on one keep-alive command.
///
/// [`keepalive_request_config`] bounds the SDK's retrying, with one gap it cannot
/// close: when a homeserver answers `M_LIMIT_EXCEEDED` with an explicit
/// `retry_after`, the SDK honours that value verbatim, above any configured
/// backoff cap. A 60-second `retry_after` on a heartbeat is a disconnection, so
/// the whole command gets a deadline comfortably inside the keep-alive timeout
/// (`matrix_rtc_core::DEFAULT_KEEP_ALIVE_TIMEOUT_MS`, 30 s). Giving up early
/// leaves the *existing* delayed leave in place and the next beat 15 s later
/// tries again, which is strictly better than blocking past the deadline.
const KEEPALIVE_DEADLINE: Duration = Duration::from_secs(10);

/// Minimum gap between two full room-state fetches in [`run_sticky_bridge`].
///
/// Membership churn broadcasts once per sticky event, and each tick costs a
/// `GET /rooms/{id}/state` plus a member read. A mass join would fire N of them
/// back to back and rate-limit the very session it is trying to track, so ticks
/// arriving inside this window are coalesced into one fetch.
const ROOM_STATE_MIN_INTERVAL: Duration = Duration::from_secs(1);

fn command_error(error: impl std::fmt::Display) -> CommandError {
    CommandError::from_message(error.to_string())
}

/// Request policy for the bridge's ordinary traffic: member stickies, slot state
/// writes, room-state reads.
///
/// The SDK's default is a poor fit for RTC signalling in both directions. With
/// `retry_limit: None` it retries transient failures (429 / 5xx) for up to
/// fifteen minutes, long after the call has moved on — *and* it declines to
/// retry plain network errors at all, because the network-failure arm is only
/// armed when a retry limit is set. Setting one fixes both halves: a dropped
/// connection is retried, and the transient budget is bounded.
fn rtc_request_config() -> RequestConfig {
    RequestConfig::default()
        .timeout(Duration::from_secs(10))
        .retry_limit(4)
        .max_retry_time(Duration::from_secs(5))
}

/// Request policy for the dead man's switch (delayed-event) commands.
///
/// Deliberately tighter than [`rtc_request_config`]: these run on the heartbeat,
/// and every second spent retrying is a second closer to the delayed leave
/// firing and dropping us from the call. Three attempts of three seconds with
/// the backoff capped at one second fits inside [`KEEPALIVE_DEADLINE`].
fn keepalive_request_config() -> RequestConfig {
    RequestConfig::default()
        .timeout(Duration::from_secs(3))
        .retry_limit(3)
        .max_retry_time(Duration::from_secs(1))
}

/// Run a keep-alive command under [`KEEPALIVE_DEADLINE`].
async fn with_keepalive_deadline<T>(
    what: &str,
    command: impl Future<Output = Result<T, CommandError>>,
) -> Result<T, CommandError> {
    match tokio::time::timeout(KEEPALIVE_DEADLINE, command).await {
        Ok(result) => result,
        Err(_) => Err(CommandError::from_message(format!(
            "{what} did not complete within {}s",
            KEEPALIVE_DEADLINE.as_secs()
        ))),
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
}

impl SdkCommandSender {
    /// Create a command sender for the given logged-in client.
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    fn room(&self, room_id: &str) -> Result<Room, CommandError> {
        let room_id = RoomId::parse(room_id).map_err(command_error)?;
        self.client
            .get_room(&room_id)
            .ok_or_else(|| CommandError::from_message(format!("room {room_id} not found")))
    }
}

#[async_trait(?Send)]
impl RtcCommandSender for SdkCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<(), CommandError> {
        let room = self.room(&room_id)?;
        let event_type = wire_event_type(event_type).to_string();
        // Retrying is safe here: the transaction id is minted once, when the
        // request is built, so every attempt carries the same one and a
        // homeserver that already accepted the event dedupes the resend rather
        // than filing a second membership.
        room.send_raw(&event_type, &content)
            .with_sticky_duration_ms(STICKY_DURATION_MS)
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        Ok(())
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        let room_id = RoomId::parse(&room_id).map_err(command_error)?;
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
        let response = with_keepalive_deadline("scheduling the delayed leave", async {
            self.client
                .send(request)
                .with_request_config(keepalive_request_config())
                .await
                .map_err(command_error)
        })
        .await?;
        // The trait's "event_id" is the delay_id used to restart/cancel it.
        Ok(response.delay_id)
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        event_id: String,
    ) -> Result<(), CommandError> {
        // `event_id` is the delay_id returned by `send_delayed_event`.
        let request =
            update_delayed_event::unstable_v1::Request::new(event_id, UpdateAction::Cancel);
        with_keepalive_deadline("cancelling the delayed leave", async {
            self.client
                .send(request)
                .with_request_config(keepalive_request_config())
                .await
                .map_err(command_error)
        })
        .await?;
        Ok(())
    }

    async fn restart_delayed_event(
        &self,
        _room_id: String,
        event_id: String,
    ) -> Result<(), CommandError> {
        // MSC4140's `restart`: resets the timer, keeps the delay id, and — unlike
        // cancel + reschedule — never leaves the membership without a scheduled
        // leave. The core falls back to the two-step path if this errors, which
        // is what a homeserver answers for a delay id it has already fired.
        let request =
            update_delayed_event::unstable_v1::Request::new(event_id, UpdateAction::Restart);
        with_keepalive_deadline("restarting the delayed leave", async {
            self.client
                .send(request)
                .with_request_config(keepalive_request_config())
                .await
                .map_err(command_error)
        })
        .await?;
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
        if room.state() != RoomState::Joined {
            return Err(CommandError::from_message(format!(
                "cannot send {event_type} to {}: room not joined",
                room.room_id()
            )));
        }
        // Built by hand rather than via `Room::send_state_event_raw`, which only
        // takes a `RequestConfig` under the SDK's
        // `experimental-encrypted-state-events` feature; without it, it is a
        // plain `async fn` on the client default. This is the same request it
        // would build, with our retry policy attached.
        let raw = serde_json::value::to_raw_value(&content).map_err(command_error)?;
        let request = send_state_event::v3::Request::new_raw(
            room.room_id().to_owned(),
            event_type.into(),
            state_key,
            Raw::<AnyStateEventContent>::from_json(raw),
        );
        self.client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        user_id: String,
        device_id: String,
        message_type: String,
        content: Value,
    ) -> Result<(), CommandError> {
        // MSC4143 encryption-key distribution: send the media key as an
        // Olm-encrypted to-device message to the target device(s).
        let user = UserId::parse(&user_id).map_err(command_error)?;
        let encryption = self.client.encryption();

        // Resolve the recipient devices. `"*"` (used by the core to target every
        // device of a user) fans out to all of the user's known devices.
        let devices: Vec<Device> = if device_id == "*" {
            encryption
                .get_user_devices(&user)
                .await
                .map_err(command_error)?
                .devices()
                .collect()
        } else {
            let dev_id = <&DeviceId>::from(device_id.as_str());
            match encryption
                .get_device(&user, dev_id)
                .await
                .map_err(command_error)?
            {
                Some(device) => vec![device],
                None => {
                    log::warn!(
                        "no known device {device_id} for {user_id}; dropping to-device \
                         {message_type}"
                    );
                    return Ok(());
                }
            }
        };
        if devices.is_empty() {
            log::warn!("no devices to send to-device {message_type} to {user_id}");
            return Ok(());
        }

        let raw: Raw<AnyToDeviceEventContent> =
            Raw::new(&content).map_err(command_error)?.cast_unchecked();

        // No retry policy of our own here: the SDK sends `/sendToDevice` under
        // its own `short_retry` config (three attempts, transient errors and
        // network failures alike) and then, rather than failing, *reports* the
        // devices it could not reach. All that is missing is saying so.
        let attempted = devices.len();
        let failures = encryption
            .encrypt_and_send_raw_to_device(
                devices.iter().collect::<Vec<&Device>>(),
                &message_type,
                raw,
                // `AllDevices` sends to every device regardless of verification/cross-
                // signing state. A production integration should prefer
                // `IdentityBasedStrategy` (MSC4153) to refuse sending keys to
                // unverified identities.
                CollectStrategy::IdentityBasedStrategy,
            )
            .await
            .map_err(command_error)?;

        if failures.is_empty() {
            return Ok(());
        }

        // Reaching no device at all means this member is not getting the key,
        // which the core must hear about as an error — the alternative is a
        // participant that stays silently undecryptable. A partial failure still
        // delivered the key, so it is reported without failing the distribution.
        //
        // Note the reported failures also include devices deliberately *withheld*
        // by `IdentityBasedStrategy` (unverified identity), indistinguishable
        // here from a delivery failure.
        if failures.len() >= attempted {
            return Err(CommandError::from_message(format!(
                "to-device {message_type}: no device of {user_id} received the key ({})",
                format_failures(&failures)
            )));
        }
        log::warn!(
            "to-device {message_type}: {} of {attempted} device(s) did not receive the key: {}",
            failures.len(),
            format_failures(&failures)
        );
        Ok(())
    }
}

fn format_failures(failures: &[(OwnedUserId, OwnedDeviceId)]) -> String {
    failures
        .iter()
        .map(|(user, device)| format!("{user}:{device}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Identifies one sticky entry across snapshots: `(sender, type, sticky_key)`.
type StickyId = (String, String, String);

fn sticky_id(event: &RawStickyEvent) -> StickyId {
    (
        event.sender.clone(),
        event.event_type.clone(),
        event.content.sticky_key.clone(),
    )
}

/// Snapshot the room's live `m.rtc.member` sticky events as core DTOs.
///
/// The sending device comes from the event's decryption metadata, which is the
/// only place MSC4143 leaves it: the proposal removed the self-asserted
/// `member.claimed_device_id`, and key distribution targets "the devices that
/// were used to encrypt these member events". Cleartext events have no such
/// metadata, so `sender_device_id` stays `None` and key delivery falls back to
/// all of the sender's devices.
fn snapshot(room: &Room) -> Vec<RawStickyEvent> {
    let room_id = room.room_id().to_string();
    room.live_sticky_events()
        .into_iter()
        .filter_map(|entry| {
            let event_type = entry.key.event_type.clone();
            if event_type != "m.rtc.member" && event_type != "org.matrix.msc4143.rtc.member" {
                return None;
            }
            let content: RawStickyEventContent = entry.raw().get_field("content").ok().flatten()?;
            // The sticky map only files plaintext and successfully decrypted
            // events, so the presence of decryption metadata is exactly whether
            // this arrived encrypted — never "we don't know".
            let origin = match entry.encryption_info() {
                Some(info) => {
                    let device = info.sender_device.as_ref().map(|device| device.to_string());
                    if device.is_none() {
                        // Olm messages carry the sender's device keys, so a
                        // decrypted event should always name one. Worth saying
                        // out loud here: downstream this member cannot be bound
                        // to a device, so their media keys get rejected.
                        log::warn!(
                            "decrypted {} from {} resolved to no sending device",
                            event_type,
                            entry.key.sender,
                        );
                    }
                    EventOrigin::encrypted(device)
                }
                None => EventOrigin::Cleartext,
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
    let response = match room
        .client()
        .send(request)
        .with_request_config(rtc_request_config())
        .await
    {
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
        .collect();

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
    let mut receiver = room.subscribe_to_sticky_events();

    // Seed the room state that gates membership before any member event is
    // applied, so members are never briefly considered joined to a slot that
    // room state says is closed.
    feed_room_state(&room, &manager).await;

    let mut known: HashMap<StickyId, RawStickyEvent> = HashMap::new();
    let initial = snapshot(&room);
    for event in &initial {
        known.insert(sticky_id(event), event.clone());
    }
    if let Err(error) = manager
        .lock()
        .await
        .on_sticky_events_snapshot_received(initial)
        .await
    {
        log::warn!("failed to apply initial sticky snapshot: {error}");
    }

    let mut last_state_fetch = tokio::time::Instant::now();

    while let Ok(_) | Err(RecvError::Lagged(_)) = receiver.recv().await {
        // Coalesce bursts. Churn broadcasts once per sticky event, and the state
        // read below is two requests; a mass join would fire them N times over.
        // Waiting out the remainder of the window and *then* draining whatever
        // piled up collapses the burst into one round trip. Note this waits
        // rather than skips: dropping the fetch would let the sticky diff be
        // applied against stale slot knowledge, which is exactly the ordering
        // the seed above is careful to avoid.
        let since_fetch = last_state_fetch.elapsed();
        if since_fetch < ROOM_STATE_MIN_INTERVAL {
            tokio::time::sleep(ROOM_STATE_MIN_INTERVAL - since_fetch).await;
        }
        // Drain the rest of the burst; the snapshot below covers all of it. A
        // closed channel drops out here too, and is left for the outer `recv()`
        // to observe one harmless pass from now.
        while let Ok(_) | Err(TryRecvError::Lagged(_)) = receiver.try_recv() {}

        // Re-read room state each tick: a slot can close or a member can leave
        // the room at any time, and MSC4143 requires clients to respect the
        // latest state. NOTE: this only re-checks when sticky traffic arrives.
        // Reacting promptly to a slot change in an otherwise idle room needs a
        // room-state subscription of its own; that is a follow-up.
        last_state_fetch = tokio::time::Instant::now();
        feed_room_state(&room, &manager).await;

        let current = snapshot(&room);
        let current_ids: HashMap<StickyId, RawStickyEvent> =
            current.iter().map(|e| (sticky_id(e), e.clone())).collect();

        // Entries that disappeared from the live set (TTL expiry) become
        // leaves; entries still present (joins and explicit disconnects)
        // are re-applied via `added`.
        let removed = known
            .iter()
            .filter(|(id, _)| !current_ids.contains_key(*id))
            .map(|(_, event)| event.clone())
            .collect();

        let update = StickyEventsUpdate {
            added: current,
            updated: Vec::new(),
            removed,
        };

        if let Err(error) = manager
            .lock()
            .await
            .on_sticky_events_update_received(update)
            .await
        {
            log::warn!("failed to apply sticky update: {error}");
        }

        known = current_ids;
    }
}
