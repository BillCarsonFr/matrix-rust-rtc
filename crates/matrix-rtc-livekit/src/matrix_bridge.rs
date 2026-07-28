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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use matrix_sdk::encryption::identities::Device;
use matrix_sdk::ruma::api::client::delayed_events::update_delayed_event::UpdateAction;
use matrix_sdk::ruma::api::client::delayed_events::{
    DelayParameters, delayed_message_event, update_delayed_event,
};
use matrix_sdk::ruma::events::{
    AnyMessageLikeEventContent, AnyToDeviceEventContent, MessageLikeEventType,
};
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{DeviceId, RoomId, TransactionId, UserId};
use matrix_sdk::{Client, Room};
use matrix_sdk_base::crypto::CollectStrategy;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::sync::broadcast::error::RecvError;

use matrix_rtc_core::{
    CommandError, RawStickyEvent, RawStickyEventContent, RtcCommandSender, RtcSessionManager,
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
        room.send_raw(&event_type, &content)
            .with_sticky_duration_ms(STICKY_DURATION_MS)
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
        let response = self.client.send(request).await.map_err(command_error)?;
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
        self.client.send(request).await.map_err(command_error)?;
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
        let recipients: Vec<&Device> = devices.iter().collect();

        let raw: Raw<AnyToDeviceEventContent> =
            Raw::new(&content).map_err(command_error)?.cast_unchecked();

        // `AllDevices` sends to every device regardless of verification/cross-
        // signing state, which the throwaway logins in the e2e test lack. A
        // production integration should prefer `IdentityBasedStrategy` (MSC4153)
        // to refuse sending keys to unverified identities.
        let failures = encryption
            .encrypt_and_send_raw_to_device(
                recipients,
                &message_type,
                raw,
                CollectStrategy::AllDevices,
            )
            .await
            .map_err(command_error)?;
        if !failures.is_empty() {
            log::warn!(
                "to-device {message_type}: {} device(s) did not receive the key",
                failures.len()
            );
        }
        Ok(())
    }
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
fn snapshot(room: &Room) -> Vec<RawStickyEvent> {
    let room_id = room.room_id().to_string();
    room.live_sticky_events()
        .into_iter()
        .filter_map(|entry| {
            let event_type = entry.key.event_type.clone();
            if event_type != "m.rtc.member" && event_type != "org.matrix.msc4143.rtc.member" {
                return None;
            }
            let content: RawStickyEventContent = entry.event.get_field("content").ok().flatten()?;
            Some(RawStickyEvent {
                room_id: room_id.clone(),
                sender: entry.key.sender.to_string(),
                event_type,
                content,
            })
        })
        .collect()
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

    while let Ok(_) | Err(RecvError::Lagged(_)) = receiver.recv().await {
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
