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

//! Native UniFFI bindings for the MatrixRTC core.
//!
//! This module defines UniFFI-facing DTOs and object wrappers, then converts
//! them into core DTOs so `matrix-rtc-core` stays decoupled from FFI-specific
//! types and binding-tooling concerns.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tokio::sync::watch;

use matrix_rtc_core::{
    EventConversionError, JoinedMembership as CoreJoinedMembership, RawStickyEvent,
    RtcSessionManager,
};
mod commands;
mod logging;
mod runtime;
pub use commands::{
    CommandSenderCallback, CommandSenderError, FfiCommandSender, FfiJoinSessionParams,
    FfiLeaveSessionParams, FfiToDeviceDelivery, FfiToDeviceRecipient, FfiTransportConfig,
};
pub use logging::{
    RtcLogConfig, RtcLogLevel, RtcLogRecord, RtcLogSink, dropped_log_record_count, log_event,
    setup_logging,
};

/// Participants with observable frame streams, publishing, and constraints —
/// see the module docs. Pulls the LiveKit client (libwebrtc): default off.
#[cfg(feature = "media")]
pub mod media;

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum MatrixRtcFfiError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("internal lock poisoned")]
    InternalLockPoisoned,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct StickyEvent {
    pub room_id: String,
    pub sender: String,
    /// Device that sent the event, from its decryption metadata. MSC4143 has no
    /// self-asserted device field, so the host must supply this for key
    /// distribution to target a single device.
    pub sender_device_id: Option<String>,
    /// Whether the event arrived encrypted; MSC4143 requires member events to be
    /// encrypted in encrypted rooms. `None` if unknown — which is not the same
    /// as `false`, which would drop the member in an encrypted room.
    pub was_encrypted: Option<bool>,
    pub event_type: String,
    pub slot_id: String,
    pub sticky_key: String,
    pub application_type: Option<String>,
    pub member_id: Option<String>,
    /// MSC4143 `member.membership`: "join" or "leave".
    pub membership: Option<String>,
    pub leave_reason: Option<FfiLeaveReason>,
    /// The raw MSC4143 `content.transports` object as JSON (`{"published":
    /// [...], "can_subscribe": [...]}`), passed through untyped so
    /// transport-specific fields survive the boundary. Without it the member
    /// projects with no transports — media layers then treat them as
    /// unreachable.
    pub transports_json: Option<String>,
}

/// MSC4143 `leave_reason`: a machine-readable `code` plus an optional
/// human-readable `reason`.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiLeaveReason {
    pub code: String,
    pub reason: Option<String>,
}

impl From<FfiLeaveReason> for matrix_rtc_core::LeaveReason {
    fn from(value: FfiLeaveReason) -> Self {
        matrix_rtc_core::LeaveReason {
            code: matrix_rtc_core::LeaveCode::from_code(&value.code),
            reason: value.reason,
        }
    }
}

/// An `m.rtc.slot` state event, with its content as a JSON string.
///
/// The content is passed as JSON rather than a typed record so that
/// application- and mechanism-specific fields survive the FFI boundary.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SlotEvent {
    /// The event's state key, which is the slot id.
    pub slot_id: String,
    /// The raw `m.rtc.slot` content as JSON.
    pub content_json: String,
}

impl SlotEvent {
    fn into_core(self, room_id: &str) -> Result<matrix_rtc_core::RawSlotEvent, MatrixRtcFfiError> {
        let content = serde_json::from_str(&self.content_json).map_err(|error| {
            MatrixRtcFfiError::InvalidInput(format!("invalid m.rtc.slot content: {error}"))
        })?;

        Ok(matrix_rtc_core::RawSlotEvent {
            room_id: room_id.to_owned(),
            slot_id: self.slot_id,
            content,
        })
    }
}

/// A decrypted `m.rtc.encryption_key` to-device message, fed into the core so
/// peers' media keys reach the encryption manager (and, with the `media`
/// feature, the frame decryptors).
///
/// The host's Matrix SDK receives and decrypts the to-device event; the
/// `sender_*` fields come from its decryption metadata, not the payload.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiReceivedEncryptionKey {
    /// The `room_id` carried in the message content.
    pub room_id: String,
    /// The `member_id` carried in the message content.
    pub member_id: String,
    /// The key material, encoded per the message's `format`.
    pub key_b64: String,
    /// The rolling key index (0-255).
    pub key_index: u8,
    /// Whether the to-device message arrived Olm-encrypted. MSC4143 requires
    /// it; the core discards cleartext keys.
    pub was_encrypted: bool,
    /// User the message was decrypted as coming from (required when
    /// `was_encrypted`).
    pub sender_user_id: Option<String>,
    /// Device the message was decrypted as coming from, when attributable.
    pub sender_device_id: Option<String>,
    /// Whether that device is cross-signed (MSC4153).
    pub sender_is_cross_signed: bool,
}

impl FfiReceivedEncryptionKey {
    fn into_core(self) -> Result<matrix_rtc_core::ReceivedEncryptionKey, MatrixRtcFfiError> {
        let origin = if self.was_encrypted {
            matrix_rtc_core::KeyOrigin::Encrypted {
                sender_user_id: self.sender_user_id.ok_or_else(|| {
                    MatrixRtcFfiError::InvalidInput(
                        "an encrypted key needs its sender_user_id".into(),
                    )
                })?,
                sender_device_id: self.sender_device_id,
                sender_is_cross_signed: self.sender_is_cross_signed,
            }
        } else {
            matrix_rtc_core::KeyOrigin::Cleartext
        };
        Ok(matrix_rtc_core::ReceivedEncryptionKey {
            origin,
            room_id: self.room_id,
            member_id: self.member_id,
            key_b64: self.key_b64,
            key_index: self.key_index,
        })
    }
}

/// A transport a member publishes media on (MSC4143 `transports.published`).
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiRtcTransport {
    /// MSC4195 LiveKit transport.
    LiveKit { livekit_service_url: String },
    /// A transport this SDK does not know; kept for forward compatibility.
    Unsupported { transport_type: String },
}

impl From<&matrix_rtc_core::RtcTransport> for FfiRtcTransport {
    fn from(transport: &matrix_rtc_core::RtcTransport) -> Self {
        match transport {
            matrix_rtc_core::RtcTransport::LiveKit(livekit) => FfiRtcTransport::LiveKit {
                livekit_service_url: livekit.livekit_service_url.clone(),
            },
            matrix_rtc_core::RtcTransport::Unsupported(unsupported) => {
                FfiRtcTransport::Unsupported {
                    transport_type: unsupported.transport_type.clone(),
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct JoinedMembership {
    pub room_id: String,
    pub slot_id: String,
    pub sender: String,
    pub sender_device_id: Option<String>,
    pub sticky_key: String,
    pub member_id: String,
    pub application: Option<String>,
    /// Transports this member publishes media on (MSC4143).
    pub transports: Vec<FfiRtcTransport>,
    /// Transport types this member can subscribe to (MSC4143).
    pub can_subscribe: Vec<String>,
}
#[derive(uniffi::Object)]
pub struct RtcSessionManagerHandle {
    /// `Arc` so a heartbeat driver can hold a `Weak` to it without keeping the
    /// manager alive past the handle.
    inner: Arc<Mutex<RtcSessionManager<FfiCommandSender>>>,
    /// One driver per joined session, keyed by `(room_id, slot_id)`. Dropping
    /// the entry stops its thread.
    heartbeats: Mutex<HashMap<(String, String), HeartbeatDriver>>,
}

/// How often the keep-alive is driven.
///
/// Three ticks inside the 30 s default delayed-leave timeout, so a skipped tick
/// (the manager was busy) or one slow round trip cannot let the switch fire.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Owns the thread that drives one session's keep-alive.
///
/// A plain thread rather than a `tokio::spawn`: the beat needs `&mut` on the
/// manager across an await, so its future holds a `std::sync::MutexGuard` and is
/// therefore `!Send`. Blocking on it from a dedicated thread sidesteps that,
/// and sleeping on the channel means a stop takes effect immediately instead of
/// at the end of the current interval.
struct HeartbeatDriver {
    /// Dropped to ask the thread to stop; it observes the disconnect.
    _stop: mpsc::Sender<()>,
}

/// Runs one session's keep-alive until the session ends or the handle goes away.
fn run_heartbeat(
    manager: Weak<Mutex<RtcSessionManager<FfiCommandSender>>>,
    room_id: String,
    slot_id: String,
    stop: mpsc::Receiver<()>,
) {
    loop {
        match stop.recv_timeout(HEARTBEAT_INTERVAL) {
            // The driver was dropped (leave, rejoin, or the handle died).
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }

        let Some(manager) = manager.upgrade() else {
            log::debug!("[{room_id}/{slot_id}] heartbeat: manager gone, stopping");
            break;
        };

        // Holding the guard across the await is the point: `heartbeat` needs
        // `&mut` on the manager for its whole duration, and the manager is
        // behind a `std::sync::Mutex` because the sync FFI entry points share
        // it — an async mutex is not an option. Safe because this future is
        // driven by `block_on` on this dedicated thread and never spawned, so
        // it is never required to be `Send`.
        #[allow(clippy::await_holding_lock)]
        let still_joined = crate::runtime::block_on(async {
            let mut guard = match manager.try_lock() {
                Ok(guard) => guard,
                // An FFI call holds the manager. Skip rather than queue behind
                // it: the next tick is 10 s away and the switch has 30 s.
                Err(std::sync::TryLockError::WouldBlock) => {
                    log::debug!("[{room_id}/{slot_id}] heartbeat: manager busy, skipping a tick");
                    return true;
                }
                Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                    log::error!("[{room_id}/{slot_id}] heartbeat: recovering a poisoned manager");
                    manager.clear_poison();
                    poisoned.into_inner()
                }
            };
            guard.heartbeat(&room_id, &slot_id).await
        });

        // `false` means the session is gone or has left — `leave` takes the
        // membership machine, so a beat racing a leave is a no-op and lands
        // here. Nothing left to keep alive.
        if !still_joined {
            log::debug!("[{room_id}/{slot_id}] heartbeat: no longer joined, stopping");
            break;
        }
    }
}

struct SubscriptionState {
    receiver: watch::Receiver<Vec<CoreJoinedMembership>>,
    initial_pending: bool,
}

#[derive(uniffi::Object)]
pub struct MembershipSnapshotSubscription {
    state: Mutex<SubscriptionState>,
}

#[uniffi::export]
impl RtcSessionManagerHandle {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(Mutex::new(RtcSessionManager::new())),
            heartbeats: Mutex::new(HashMap::new()),
        })
    }

    /// Sets the command sender for this manager.
    ///
    /// This must be called before join/leave operations to enable sending events
    /// back to the Matrix room. The callback will be invoked by the core to send
    /// membership events (join, leave, keep-alive) for all sessions.
    ///
    /// # Arguments
    /// * `callback` - Native implementation of CommandSenderCallback
    pub fn set_command_sender(
        &self,
        callback: Box<dyn CommandSenderCallback>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!("manager: command sender installed");
        let command_sender = FfiCommandSender::new(Arc::from(callback));
        let mut manager = lock_mutex(&self.inner)?;
        manager.set_command_sender(command_sender);
        Ok(())
    }

    /// Apply the **complete** current sticky state for one room.
    ///
    /// Replaces, rather than merges: a member absent from `events` is gone, and
    /// an empty list clears the room.
    ///
    /// That is what makes expiry work. A member does not only leave by sending a
    /// leave event — an MSC4354 sticky entry lapses when its owner stops
    /// refreshing it, which is exactly what a crashed client does, and the lapse
    /// produces no event to feed in. A merging call would keep that member in
    /// the call for good and leave every host to diff snapshots itself.
    ///
    /// The usual flow is this call once with the SDK's current sticky map for
    /// the room, and again whenever it changes. It is the only way membership
    /// reaches the core, and safe to call as often as you like — re-asserting
    /// the full state is also how you resynchronise after a reconnect.
    pub fn set_current_sticky_state(
        &self,
        room_id: String,
        events: Vec<StickyEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::debug!(
            "manager: [{room_id}] current sticky state in, {} events ({})",
            events.len(),
            describe_events(&events),
        );
        trace_sticky_events("current", &events);

        let mut manager = lock_mutex(&self.inner)?;
        crate::runtime::block_on(async {
            manager
                .set_current_sticky_state(&room_id, to_core_events(events))
                .await
                .map_err(map_conversion_error)
        })
    }

    /// Applies a room's complete `m.rtc.slot` state.
    ///
    /// Calling this is what makes the MSC4143 open-slot condition apply to the
    /// room; until then it cannot be evaluated and is not enforced. Any slot in
    /// the room not present in `slots` is treated as closed, so always pass the
    /// full set — an empty list included.
    pub fn on_room_slots_received(
        &self,
        room_id: String,
        slots: Vec<SlotEvent>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::debug!(
            "manager: [{room_id}] room slots in: {:?}",
            slots.iter().map(|slot| &slot.slot_id).collect::<Vec<_>>(),
        );

        let mapped = slots
            .into_iter()
            .map(|slot| slot.into_core(&room_id))
            .collect::<Result<Vec<_>, _>>()
            .inspect_err(|err| log::warn!("manager: [{room_id}] rejected a slot event: {err}"))?;

        let mut manager = lock_mutex(&self.inner)?;
        crate::runtime::block_on(async {
            manager.on_room_slots_received(&room_id, mapped).await;
            Ok(())
        })
    }

    /// Sets the users currently joined to a room.
    ///
    /// MSC4143 only counts a member event while its sender is still joined to
    /// the room; until this is called that condition is not enforced.
    pub fn on_room_members_received(
        &self,
        room_id: String,
        joined_user_ids: Vec<String>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::debug!(
            "manager: [{room_id}] room members in: {} joined",
            joined_user_ids.len(),
        );
        log::trace!("manager: [{room_id}] joined users: {joined_user_ids:?}");

        let mut manager = lock_mutex(&self.inner)?;
        crate::runtime::block_on(async {
            manager
                .on_room_members_received(&room_id, joined_user_ids)
                .await;
            Ok(())
        })
    }

    /// Reports whether a room is end-to-end encrypted.
    ///
    /// MSC4143 requires RTC encryption in encrypted rooms and forbids it
    /// elsewhere, so this changes how the room's slots resolve and whether
    /// cleartext member events count.
    pub fn on_room_encryption_received(
        &self,
        room_id: String,
        encrypted: bool,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!("manager: [{room_id}] room encryption in: encrypted={encrypted}");

        let mut manager = lock_mutex(&self.inner)?;
        crate::runtime::block_on(async {
            manager
                .on_room_encryption_received(&room_id, encrypted)
                .await;
            Ok(())
        })
    }

    /// Feeds a decrypted `m.rtc.encryption_key` to-device message to every
    /// session of its room. Call for each such message the host's sync
    /// delivers; without it peers' media never becomes decryptable.
    pub fn receive_encryption_key(
        &self,
        key: FfiReceivedEncryptionKey,
    ) -> Result<(), MatrixRtcFfiError> {
        // Never the key material itself — only what decides whether it is
        // accepted. Its length is enough to spot a truncated or empty key.
        log::debug!(
            "manager: [{}] encryption key in: member={} index={} len={} encrypted={} sender={:?}/{:?} cross_signed={}",
            key.room_id,
            key.member_id,
            key.key_index,
            key.key_b64.len(),
            key.was_encrypted,
            key.sender_user_id,
            key.sender_device_id,
            key.sender_is_cross_signed,
        );

        let received = key.into_core()?;
        let manager = lock_mutex(&self.inner)?;
        crate::runtime::block_on(async {
            manager
                .receive_encryption_key(received)
                .await
                .map_err(|error| {
                    log::warn!("manager: encryption key rejected: {error}");
                    MatrixRtcFfiError::InvalidInput(error.to_string())
                })
        })
    }

    /// A JSON dump of everything the manager and its sessions currently
    /// believe: sessions, room state per room, and every candidate member with
    /// the reason it is or is not projected as joined.
    ///
    /// For bug reports and for answering "what does Rust think the state is
    /// right now?" without a debugger. Contains no key material.
    pub fn debug_snapshot(&self) -> Result<String, MatrixRtcFfiError> {
        let manager = lock_mutex(&self.inner)?;
        Ok(manager.debug_snapshot().to_string())
    }

    pub fn session_count(&self) -> Result<u64, MatrixRtcFfiError> {
        let manager = lock_mutex(&self.inner)?;
        Ok(manager.session_count() as u64)
    }

    pub fn member_count(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<u64>, MatrixRtcFfiError> {
        let manager = lock_mutex(&self.inner)?;
        Ok(manager
            .member_count(&room_id, &slot_id)
            .map(|count| count as u64))
    }

    /// Observe the joined roster of one session.
    ///
    /// Returns `None` if no session exists for `(room_id, slot_id)` — a session
    /// appears when the first member event for that slot is fed in, or when this
    /// manager joins it.
    ///
    /// This is the roster the media layer works from, and the counterpart to
    /// [`Self::member_count`], which answers only "how many". Polling the count
    /// once a second was the previous way to notice a change: the wrong shape for
    /// a value that moves a handful of times per call, and useless for learning
    /// *who* moved. A host that wants to be told a call has started watches this.
    ///
    /// The subscription yields the current roster on its first
    /// `nextSnapshot()` and then only on change, so a host can attach at any
    /// point without missing the state it attached to.
    pub fn subscribe_membership_snapshots(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<Arc<MembershipSnapshotSubscription>>, MatrixRtcFfiError> {
        let manager = lock_mutex(&self.inner)?;
        Ok(manager
            .subscribe_membership_snapshots(&room_id, &slot_id)
            .map(|receiver| {
                Arc::new(MembershipSnapshotSubscription {
                    state: Mutex::new(SubscriptionState {
                        receiver,
                        initial_pending: true,
                    }),
                })
            }))
    }

    /// Joins a session, returning the `member.id` it joined as.
    ///
    /// The SDK generates that id; hosts do not supply one. MSC4143 requires a
    /// fresh `member.id` on every join, and reusing one is silently destructive:
    /// the MSC4195 participant identity is derived from it, so a repeat join
    /// keeps the identity peers already hold a key for while our key index
    /// restarts at 0 — every peer then decrypts our media with the previous
    /// call's key and never recovers.
    ///
    /// Hosts that need the id later (nothing in this API does — see
    /// [`connect_media_session`](crate::media::connect_media_session)) can read
    /// it back with [`Self::own_member_id`] instead of storing it, which cannot
    /// go stale across a rejoin.
    pub fn join(&self, params: FfiJoinSessionParams) -> Result<String, MatrixRtcFfiError> {
        log::info!("manager: join requested {}", params.summary());

        // Kept for the keep-alive driver, which outlives `params`.
        let room_id = params.room_id.clone();
        let slot_id = params.slot_id.clone();

        let mut core_params = params.into_core().map_err(|e| {
            log::warn!("manager: join rejected before it started: {e}");
            MatrixRtcFfiError::InvalidInput(e.to_string())
        })?;
        let member_id = matrix_rtc_core::generate_member_id();
        core_params.membership_id = Some(member_id.clone());

        // Hold the guard across the join, rather than swapping a placeholder
        // `RtcSessionManager::new()` in and unlocking for the duration. That
        // pattern had two failure modes: any call landing during the join —
        // `member_count`, or a sticky snapshot — operated on the *placeholder*,
        // so events arriving in that window were applied to it and then thrown
        // away when the real manager was restored; and a panic mid-join dropped
        // the real manager during unwind, silently leaving the empty one behind
        // with no poison flag to signal it.
        //
        // Serialising instead is safe because no host callback re-enters a
        // handle: the command sender only sends events outward. If that ever
        // changes, this becomes a deadlock rather than lost state.
        let mut manager = lock_mutex(&self.inner)?;

        let result = crate::runtime::block_on(async {
            manager.join(core_params).await.map_err(|e| {
                log::warn!("manager: join failed: {e}");
                MatrixRtcFfiError::InvalidInput(e.to_string())
            })
        });

        if result.is_ok() {
            log::info!("manager: join succeeded as {member_id}");
            // Drop the manager lock before starting the driver: its first act
            // is to try_lock, and holding it here would make that first tick a
            // guaranteed skip.
            drop(manager);
            self.start_heartbeat(room_id, slot_id);
        }

        result.map(|()| member_id)
    }

    /// Our `member.id` in one session, or `None` if there is no such session or
    /// it has not joined.
    ///
    /// Changes on every join (MSC4143), so read it when needed rather than
    /// caching what [`Self::join`] returned.
    pub fn own_member_id(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<String>, MatrixRtcFfiError> {
        let manager = lock_mutex(&self.inner)?;
        Ok(manager.own_member_id(&room_id, &slot_id))
    }

    /// Restarts the keep-alive for one session: reschedules the delayed leave,
    /// and re-sends the membership if its sticky entry is halfway to expiring.
    ///
    /// **Hosts do not need to call this** — [`Self::join`] starts a driver that
    /// does it every 10 seconds, and [`Self::leave`] stops it. It is exported
    /// for hosts that would rather drive the keep-alive from their own scheduler
    /// (a foreground service, a workmanager job), and for tests.
    ///
    /// Returns `false` if there is no joined session for `(room_id, slot_id)`,
    /// which means there is nothing to keep alive.
    pub fn heartbeat(&self, room_id: String, slot_id: String) -> Result<bool, MatrixRtcFfiError> {
        let mut manager = lock_mutex(&self.inner)?;
        Ok(crate::runtime::block_on(async {
            manager.heartbeat(&room_id, &slot_id).await
        }))
    }

    /// Starts (or replaces) the keep-alive driver for one session.
    fn start_heartbeat(&self, room_id: String, slot_id: String) {
        let (stop, stop_rx) = mpsc::channel();
        let manager = Arc::downgrade(&self.inner);
        let key = (room_id.clone(), slot_id.clone());

        let thread = std::thread::Builder::new()
            .name(format!("matrix-rtc-heartbeat-{room_id}"))
            .spawn(move || run_heartbeat(manager, room_id, slot_id, stop_rx));

        match thread {
            Ok(_) => {
                log::info!(
                    "manager: keep-alive driver started for [{}/{}] every {}s",
                    key.0,
                    key.1,
                    HEARTBEAT_INTERVAL.as_secs(),
                );
                // Replaces any previous driver for this session; dropping the
                // old sender stops its thread.
                match lock_mutex(&self.heartbeats) {
                    Ok(mut drivers) => {
                        drivers.insert(key, HeartbeatDriver { _stop: stop });
                    }
                    Err(error) => log::error!("manager: could not register keep-alive: {error}"),
                }
            }
            Err(error) => log::error!(
                "manager: could not start the keep-alive driver for [{}/{}]: {error}. The \
                 membership will be cleaned up by the delayed leave unless the host drives \
                 `heartbeat()` itself.",
                key.0,
                key.1,
            ),
        }
    }

    /// Stops the keep-alive driver for one session, if any.
    fn stop_heartbeat(&self, room_id: &str, slot_id: &str) {
        let key = (room_id.to_owned(), slot_id.to_owned());
        match lock_mutex(&self.heartbeats) {
            Ok(mut drivers) => {
                if drivers.remove(&key).is_some() {
                    log::debug!("manager: keep-alive driver stopped for [{room_id}/{slot_id}]");
                }
            }
            Err(error) => log::error!("manager: could not stop the keep-alive: {error}"),
        }
    }

    pub fn leave(
        &self,
        room_id: String,
        slot_id: String,
        params: FfiLeaveSessionParams,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!(
            "manager: leave requested [{room_id}/{slot_id}] reason={:?}",
            params.leave_reason,
        );

        let core_params = params.into_core();

        // Stop the keep-alive first, so it cannot re-arm a delayed leave after
        // the leave below cancels it. A beat already in flight is harmless: it
        // holds the manager lock we are about to take, and once `leave` has
        // taken the membership machine any later beat is a no-op.
        self.stop_heartbeat(&room_id, &slot_id);

        // Held across the leave, for the reasons in `join` above. It matters
        // most here: losing the manager to a placeholder mid-leave would drop
        // the very state needed to depart, leaving the membership live until the
        // dead man's switch expired.
        let mut manager = lock_mutex(&self.inner)?;

        let result = crate::runtime::block_on(async {
            manager
                .leave(room_id, slot_id, core_params)
                .await
                .map_err(|e| {
                    log::warn!("manager: leave failed: {e}");
                    MatrixRtcFfiError::InvalidInput(e.to_string())
                })
        });

        if result.is_ok() {
            log::info!("manager: leave succeeded");
        }

        result
    }
}

#[uniffi::export]
impl MembershipSnapshotSubscription {
    pub fn next_snapshot(&self) -> Result<Option<Vec<JoinedMembership>>, MatrixRtcFfiError> {
        let mut state = lock_mutex(&self.state)?;

        let snapshot = if state.initial_pending {
            state.initial_pending = false;
            Some(state.receiver.borrow().clone())
        } else {
            match state.receiver.has_changed() {
                Ok(true) => Some(state.receiver.borrow_and_update().clone()),
                Ok(false) | Err(_) => None,
            }
        };

        Ok(snapshot.map(|members| {
            members
                .into_iter()
                .map(to_ffi_joined_membership)
                .collect::<Vec<_>>()
        }))
    }
}

fn to_core_event(event: StickyEvent) -> matrix_rtc_core::RawStickyEvent {
    use matrix_rtc_core::{
        ApplicationInfo, MemberInfo, Membership, RawStickyEvent, RawStickyEventContent,
    };
    use std::collections::BTreeMap;

    // Map FFI types to MSC4143-compliant core types
    let application = ApplicationInfo {
        application_type: event.application_type,
        extra: BTreeMap::new(),
    };

    let member = MemberInfo {
        id: event.member_id,
        membership: event.membership.map(|m| match m.as_str() {
            "join" => Membership::Join,
            "leave" => Membership::Leave,
            _ => Membership::Unknown(m),
        }),
    };

    // A malformed transports object degrades that member to "no transports"
    // (media layers see them as unreachable) rather than failing the whole
    // sticky batch — it is host-supplied JSON, not protocol input we control.
    let transports = event.transports_json.and_then(|json| {
        serde_json::from_str(&json)
            .map_err(|error| {
                log::warn!("ignoring malformed transports JSON on sticky event: {error}");
            })
            .ok()
    });

    RawStickyEvent {
        room_id: event.room_id,
        sender: event.sender,
        origin: match event.was_encrypted {
            Some(true) => matrix_rtc_core::EventOrigin::encrypted(event.sender_device_id),
            Some(false) => matrix_rtc_core::EventOrigin::Cleartext,
            None => matrix_rtc_core::EventOrigin::Unknown,
        },
        event_type: event.event_type,
        content: RawStickyEventContent {
            slot_id: event.slot_id,
            sticky_key: event.sticky_key,
            application,
            member,
            transports,
            leave_reason: event.leave_reason.map(Into::into),
        },
    }
}

fn to_core_events(events: Vec<StickyEvent>) -> Vec<RawStickyEvent> {
    events.into_iter().map(to_core_event).collect()
}

fn to_ffi_joined_membership(member: CoreJoinedMembership) -> JoinedMembership {
    JoinedMembership {
        room_id: member.room_id,
        slot_id: member.slot_id,
        sender: member.sender,
        sender_device_id: member.origin.sender_device_id().map(str::to_owned),
        sticky_key: member.sticky_key,
        member_id: member.member_id,
        application: member.application,
        transports: member.transports.iter().map(Into::into).collect(),
        can_subscribe: member.can_subscribe,
    }
}

fn map_conversion_error(err: EventConversionError) -> MatrixRtcFfiError {
    log::warn!("rejected a sticky event batch: {err}");
    MatrixRtcFfiError::InvalidInput(err.to_string())
}

/// Compact `(room, slot)` census of a sticky batch: which sessions it touched,
/// and how many events each got.
fn describe_events(events: &[StickyEvent]) -> String {
    if events.is_empty() {
        return "none".to_owned();
    }

    let mut counts: Vec<(String, usize)> = Vec::new();
    for event in events {
        let key = format!("{}/{}", event.room_id, event.slot_id);
        match counts.iter_mut().find(|(seen, _)| *seen == key) {
            Some((_, count)) => *count += 1,
            None => counts.push((key, 1)),
        }
    }

    counts
        .iter()
        .map(|(key, count)| format!("{key} x{count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Per-event detail for a sticky batch, for when a member is dropped for a
/// reason the summary cannot show.
fn trace_sticky_events(kind: &str, events: &[StickyEvent]) {
    if !log::log_enabled!(log::Level::Trace) {
        return;
    }

    for event in events {
        log::trace!(
            "sticky {kind}: [{}/{}] type={} sender={} device={:?} sticky_key={} membership={:?} encrypted={:?} transports={}",
            event.room_id,
            event.slot_id,
            event.event_type,
            event.sender,
            event.sender_device_id,
            event.sticky_key,
            event.membership,
            event.was_encrypted,
            event.transports_json.is_some(),
        );
    }
}

/// Locks `mutex`, recovering from poisoning rather than propagating it.
///
/// A panic anywhere inside a handle method used to poison the handle for the
/// rest of the process: every later call — `member_count`, and critically
/// `leave` — returned [`MatrixRtcFfiError::InternalLockPoisoned`] forever, so
/// the host could not even depart the session and its membership stayed live
/// until the dead man's switch expired. One panic permanently disabling the
/// manager, including the ability to leave it, is worse than the panic.
///
/// So we take the guard anyway and clear the flag. The state behind it may have
/// been mid-mutation when the panic unwound, which is why this logs at error
/// level: it is a bug worth reporting, not a condition to handle silently.
///
/// Still returns `Result` — the signature is what ~20 call sites and the
/// `media` module expect, and it keeps room for a future fallible lock — but
/// the error path is now unreachable.
fn lock_mutex<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, MatrixRtcFfiError> {
    match mutex.lock() {
        Ok(guard) => Ok(guard),
        Err(poisoned) => {
            log::error!(
                "recovering a poisoned lock: an earlier call panicked and its state may be \
                 inconsistent. Please report this with the panic that preceded it."
            );
            mutex.clear_poison();
            Ok(poisoned.into_inner())
        }
    }
}

uniffi::setup_scaffolding!();

#[cfg(test)]
mod tests {
    use super::*;

    /// The manual entry point exists and reports honestly when there is nothing
    /// to keep alive, rather than pretending it beat something.
    #[test]
    fn heartbeat_reports_no_session_when_not_joined() {
        let manager = RtcSessionManagerHandle::new();
        assert!(
            !manager
                .heartbeat("!room:example.org".to_owned(), "m.call#ROOM".to_owned())
                .expect("the call itself should succeed"),
        );
    }

    /// A driver is only registered by a successful join, and `leave` must clear
    /// it even for a session that was never joined — otherwise a failed join
    /// would leave a thread beating forever.
    #[test]
    fn stopping_an_unknown_heartbeat_is_harmless() {
        let manager = RtcSessionManagerHandle::new();
        manager.stop_heartbeat("!room:example.org", "m.call#ROOM");
        assert!(lock_mutex(&manager.heartbeats).unwrap().is_empty());
    }

    fn join_event() -> StickyEvent {
        StickyEvent {
            room_id: "!room:example.org".to_owned(),
            sender: "@alice:example.org".to_owned(),
            sender_device_id: Some("DEVICEID".to_owned()),
            was_encrypted: Some(true),
            event_type: "m.rtc.member".to_owned(),
            slot_id: "m.call#ROOM".to_owned(),
            sticky_key: "alice-device-a".to_owned(),
            application_type: Some("m.call".to_owned()),
            member_id: Some("alice-device-a".to_owned()),
            membership: Some("join".to_owned()),
            leave_reason: None,
            transports_json: Some(
                r#"{"published":[{"type":"livekit","livekit_service_url":"https://sfu.example.org"}],"can_subscribe":["livekit"]}"#
                    .to_owned(),
            ),
        }
    }

    #[test]
    fn ffi_snapshot_entrypoint_accepts_join_event() {
        let manager = RtcSessionManagerHandle::new();

        let result =
            manager.set_current_sticky_state("!room:example.org".to_owned(), vec![join_event()]);

        assert!(result.is_ok());
    }

    #[test]
    fn transports_round_trip_through_the_ffi() {
        let manager = RtcSessionManagerHandle::new();
        manager
            .set_current_sticky_state("!room:example.org".to_owned(), vec![join_event()])
            .unwrap();
        let subscription = manager
            .subscribe_membership_snapshots(
                "!room:example.org".to_owned(),
                "m.call#ROOM".to_owned(),
            )
            .unwrap()
            .expect("the member event should have created the session");

        let joined = subscription.next_snapshot().unwrap().unwrap();
        assert_eq!(
            joined[0].transports,
            vec![FfiRtcTransport::LiveKit {
                livekit_service_url: "https://sfu.example.org".to_owned(),
            }]
        );
        assert_eq!(joined[0].can_subscribe, vec!["livekit".to_owned()]);
    }

    /// The roster is observable from the *manager* handle, which is the one a
    /// host drives and the one the media layer needs. Previously it existed only
    /// on `RtcSessionHandle`, so a host running the manager could see
    /// `memberCount` but never who was in the call — and polling a count once a
    /// second was the only way to notice a change.
    #[test]
    fn the_manager_publishes_the_roster_of_a_session() {
        let manager = RtcSessionManagerHandle::new();
        let (room_id, slot_id) = ("!room:example.org".to_owned(), "m.call#ROOM".to_owned());

        assert!(
            manager
                .subscribe_membership_snapshots(room_id.clone(), slot_id.clone())
                .unwrap()
                .is_none(),
            "no session for that slot yet, so nothing to observe"
        );

        manager
            .set_current_sticky_state(room_id.clone(), vec![join_event()])
            .expect("the current state should be accepted");

        let subscription = manager
            .subscribe_membership_snapshots(room_id, slot_id)
            .unwrap()
            .expect("the member event should have created the session");

        // Attaching late still yields the state attached to, not silence.
        let initial = subscription
            .next_snapshot()
            .unwrap()
            .expect("the first call reports the current roster");
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].sender, "@alice:example.org");

        assert_eq!(
            subscription.next_snapshot().unwrap(),
            None,
            "and then only on change"
        );
    }

    /// A panic inside one handle method must not disable the handle forever.
    /// It used to: the poisoned mutex made every later call — `leave` included —
    /// return `InternalLockPoisoned`, so the host could not depart the session
    /// and its membership stayed live until the dead man's switch expired.
    #[test]
    fn a_poisoned_lock_is_recovered_rather_than_propagated() {
        let mutex = Mutex::new(0_u32);

        let panicked = std::panic::catch_unwind(|| {
            let mut guard = mutex.lock().unwrap();
            *guard = 1;
            panic!("simulates a panic while the guard is held");
        });
        assert!(panicked.is_err());
        assert!(mutex.is_poisoned(), "the mutex should now be poisoned");

        // The value the panicking call had written is still there, and the lock
        // is usable again.
        let guard = lock_mutex(&mutex).expect("a poisoned lock must still be usable");
        assert_eq!(*guard, 1);
        drop(guard);
        assert!(
            !mutex.is_poisoned(),
            "the poison flag should have been cleared"
        );
    }
}
