//! The live [`Session`]: one `RoomState` behind a mutex, seeded from the
//! driver's `read_*` calls and fed from its streams by a pump task.
//!
//! # Lock model
//!
//! `UnboundedReceiver::recv()` borrows the receiver across an await, so the
//! pump cannot hold the state mutex while waiting. Instead the pump is one
//! `poll_fn`: lock, `poll_recv` both receivers and poll the expiry sleep,
//! ingest everything that is ready, publish, unlock, return `Pending` (the
//! wakers were registered under the lock). [`Session::snapshot`] locks and
//! loops `try_recv` — **drain-on-read** — so a read right after an `emit`
//! is fresh even on wasm, where the pump only runs after the current JS task
//! yields. Both paths go through the same lock and the same `ingest`, so an
//! event is processed exactly once whoever drains it. Listener callbacks
//! (`watch` wake-ups) still arrive one microtask later.

use super::dispatch::{
    self, LEGACY_MEMBER_EVENT_TYPE, MEMBER_EVENT_TYPES, ROOM_ENCRYPTION_EVENT_TYPE,
    ROOM_MEMBER_EVENT_TYPE, SLOT_EVENT_TYPES,
};
use super::state::RoomState;
use super::{ElementCallCompat, SessionConfig, SessionSnapshot};
use crate::driver::{RoomEventsDriver, StateKeySelector};
use crate::executor;
use crate::types::RawMatrixEvent;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use tokio::sync::mpsc::{UnboundedReceiver, error::TryRecvError};
use tokio::sync::watch;

/// How many `m.rtc.member` timeline events the seed asks for, per type.
const MEMBER_READ_LIMIT: u32 = 200;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type BoxSleep = Pin<Box<dyn Future<Output = ()> + Send>>;
#[cfg(target_arch = "wasm32")]
pub(crate) type BoxSleep = Pin<Box<dyn Future<Output = ()>>>;

/// The clock the session runs on. Injected so tests never sleep.
pub(crate) trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
    fn sleep(&self, ms: u64) -> BoxSleep;
}

/// `executor`'s clock and timers.
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        executor::now_ms()
    }

    fn sleep(&self, ms: u64) -> BoxSleep {
        Box::pin(executor::sleep_ms(ms))
    }
}

struct Live {
    room_id: String,
    slot_id: String,
    config: SessionConfig,
    state: RoomState,
    /// `None` once the channel finished or the session was dropped.
    room_rx: Option<UnboundedReceiver<RawMatrixEvent>>,
    state_rx: Option<UnboundedReceiver<Vec<RawMatrixEvent>>>,
    snapshot_tx: watch::Sender<SessionSnapshot>,
    clock: Arc<dyn Clock>,
    closed: bool,
    pump_waker: Option<Waker>,
}

impl Live {
    fn tag(&self) -> String {
        format!("{}/{}", self.room_id, self.slot_id)
    }

    fn ingest(&mut self, event: &RawMatrixEvent, now: u64) {
        if let Some(room_id) = dispatch::room_id(event)
            && room_id != self.room_id
        {
            log::debug!("[{}] ignoring an event for another room ({room_id})", self.tag());
            return;
        }
        let ingest = dispatch::classify(event, &self.config, now);
        self.state.ingest(ingest, now);
    }

    fn ingest_batch(&mut self, events: &[RawMatrixEvent], now: u64) {
        for event in events {
            self.ingest(event, now);
        }
    }

    /// Ingest everything that is ready on both streams. With a context the
    /// receivers register its waker; without one this is a plain drain.
    fn drain(&mut self, mut cx: Option<&mut Context<'_>>, now: u64) {
        while let Some(event) = next_ready(&mut self.room_rx, cx.as_deref_mut()) {
            self.ingest(&event, now);
        }
        while let Some(batch) = next_ready(&mut self.state_rx, cx.as_deref_mut()) {
            // A state batch is ingested whole and published once.
            self.ingest_batch(&batch, now);
        }
    }

    /// One transition without the timer: what `snapshot()` runs.
    fn refresh(&mut self) {
        if self.closed {
            return;
        }
        self.state.start_transition();
        let now = self.clock.now_ms();
        self.drain(None, now);
        self.state.expire(now);
        self.publish();
    }

    /// Publish the projection if it differs from the current value.
    fn publish(&mut self) {
        let snapshot = self.state.project(&self.slot_id);
        let tag = self.tag();
        self.snapshot_tx.send_if_modified(|current| {
            if *current == snapshot {
                log::trace!("[{tag}] snapshot unchanged");
                return false;
            }
            let ids = |s: &SessionSnapshot| s.members.iter().map(|m| m.member_id.clone()).collect::<Vec<_>>();
            let before = ids(current);
            let after = ids(&snapshot);
            log::info!(
                "[{tag}] session changed: {} -> {} joined (+{:?} -{:?}), {} excluded, slot={:?}",
                before.len(),
                after.len(),
                after.iter().filter(|id| !before.contains(id)).collect::<Vec<_>>(),
                before.iter().filter(|id| !after.contains(id)).collect::<Vec<_>>(),
                snapshot.excluded_candidates.len(),
                snapshot.slot_state.as_ref().map(|s| if s.is_open() { "Open" } else { "Closed" }),
            );
            *current = snapshot;
            true
        });
    }
}

/// The next ready item, or `None` (registering `cx`'s waker when given). A
/// finished channel is dropped so it is not polled again.
fn next_ready<T>(rx: &mut Option<UnboundedReceiver<T>>, cx: Option<&mut Context<'_>>) -> Option<T> {
    let receiver = rx.as_mut()?;
    match cx {
        Some(cx) => match receiver.poll_recv(cx) {
            Poll::Ready(Some(item)) => Some(item),
            Poll::Ready(None) => {
                *rx = None;
                None
            }
            Poll::Pending => None,
        },
        None => match receiver.try_recv() {
            Ok(item) => Some(item),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                *rx = None;
                None
            }
        },
    }
}

fn lock(inner: &Mutex<Live>) -> MutexGuard<'_, Live> {
    inner.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A **live** single-`(room_id, slot_id)` RTC session: constructed with the
/// [`RoomEventsDriver`] slice, it seeds itself from `read_state` /
/// `read_events` and then consumes the driver's live streams. All reads go
/// through [`SessionSnapshot`].
pub struct Session {
    room_id: String,
    slot_id: String,
    inner: Arc<Mutex<Live>>,
}

impl Session {
    /// Subscribes to the driver's room-event and state-update streams
    /// **before** seeding (so nothing is missed between read and subscribe),
    /// then seeds and pumps in a detached task (`executor::spawn`).
    ///
    /// Reads are *drain-on-read*: [`Self::snapshot`] first processes whatever
    /// the driver has already emitted, so a getter right after an `emit` is
    /// fresh on every platform. Change notifications (`subscribe()`) arrive
    /// from the pump, one task/microtask later.
    pub fn new(
        room_id: String,
        slot_id: String,
        driver: Arc<dyn RoomEventsDriver>,
        config: SessionConfig,
    ) -> Self {
        Self::with_clock(room_id, slot_id, driver, config, Arc::new(SystemClock))
    }

    pub(crate) fn with_clock(
        room_id: String,
        slot_id: String,
        driver: Arc<dyn RoomEventsDriver>,
        config: SessionConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let room_rx = driver.subscribe_room_events();
        let state_rx = driver.subscribe_state_updates();
        let (snapshot_tx, _initial_rx) = watch::channel(SessionSnapshot {
            room_id: room_id.clone(),
            slot_id: slot_id.clone(),
            ..SessionSnapshot::default()
        });
        let live = Live {
            room_id: room_id.clone(),
            slot_id: slot_id.clone(),
            config,
            state: RoomState::for_live(&room_id),
            room_rx: Some(room_rx),
            state_rx: Some(state_rx),
            snapshot_tx,
            clock,
            closed: false,
            pump_waker: None,
        };
        log::info!("[{room_id}/{slot_id}] session created (compat {:?})", config.compat);
        let inner = Arc::new(Mutex::new(live));
        executor::spawn(run(inner.clone(), driver));
        Self { room_id, slot_id, inner }
    }

    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    pub fn slot_id(&self) -> &str {
        &self.slot_id
    }

    /// The current value, after draining whatever the driver has emitted.
    pub fn snapshot(&self) -> SessionSnapshot {
        let mut live = lock(&self.inner);
        live.refresh();
        live.snapshot_tx.borrow().clone()
    }

    /// Reactive stream: current value + every change.
    pub fn subscribe(&self) -> watch::Receiver<SessionSnapshot> {
        let mut live = lock(&self.inner);
        live.refresh();
        live.snapshot_tx.subscribe()
    }

    /// Per-candidate verdicts and current state as JSON, for bug reports.
    pub fn debug_snapshot(&self) -> serde_json::Value {
        let mut live = lock(&self.inner);
        live.refresh();
        let mut debug = live.state.debug_json(&self.slot_id);
        debug["compat"] = serde_json::Value::String(format!("{:?}", live.config.compat));
        debug["streams_open"] = serde_json::json!({
            "room_events": live.room_rx.is_some(),
            "state_updates": live.state_rx.is_some(),
        });
        debug
    }
}

impl Drop for Session {
    /// Closes this session's stream receivers (the driver's fan-out sees the
    /// subscriber go) and ends the pump.
    fn drop(&mut self) {
        let mut live = lock(&self.inner);
        live.closed = true;
        live.room_rx = None;
        live.state_rx = None;
        if let Some(waker) = live.pump_waker.take() {
            waker.wake();
        }
        log::info!("[{}/{}] session dropped", self.room_id, self.slot_id);
    }
}

/// The pump: seed, publish once, then wait on both streams and the expiry
/// timer until the session is dropped.
async fn run(inner: Arc<Mutex<Live>>, driver: Arc<dyn RoomEventsDriver>) {
    seed(&inner, &driver).await;
    {
        let mut live = lock(&inner);
        if live.closed {
            return;
        }
        live.publish();
        log::debug!("[{}] seeded", live.tag());
    }

    let mut timer: Option<(u64, BoxSleep)> = None;
    std::future::poll_fn(move |cx| {
        let mut live = lock(&inner);
        if live.closed {
            return Poll::Ready(());
        }
        live.pump_waker = Some(cx.waker().clone());
        live.state.start_transition();
        loop {
            let now = live.clock.now_ms();
            live.drain(Some(cx), now);
            live.state.expire(now);
            match live.state.next_expiry() {
                None => {
                    timer = None;
                    break;
                }
                Some(deadline) => {
                    if timer.as_ref().is_none_or(|(armed, _)| *armed != deadline) {
                        log::trace!("[{}] expiry timer armed for {deadline} (in {}ms)", live.tag(), deadline.saturating_sub(now));
                        timer = Some((deadline, live.clock.sleep(deadline.saturating_sub(now))));
                    }
                    let Some((_, sleep)) = timer.as_mut() else { unreachable!() };
                    if sleep.as_mut().poll(cx).is_ready() {
                        // Fired: expire against a fresh `now` and re-arm.
                        timer = None;
                        continue;
                    }
                    break;
                }
            }
        }
        live.publish();
        Poll::Pending
    })
    .await;
    log::debug!("session pump ended");
}

/// Seed from room state and the recent timeline. Every read failure is
/// logged and leaves that condition **unenforced**.
async fn seed(inner: &Arc<Mutex<Live>>, driver: &Arc<dyn RoomEventsDriver>) {
    let (config, tag) = {
        let live = lock(inner);
        (live.config, live.tag())
    };
    let ingest = |events: &[RawMatrixEvent]| {
        let mut live = lock(inner);
        let now = live.clock.now_ms();
        live.ingest_batch(events, now);
    };

    match driver.read_state(ROOM_ENCRYPTION_EVENT_TYPE.to_owned(), StateKeySelector::Key(String::new())).await {
        Ok(events) => ingest(&events),
        Err(error) => log::warn!("[{tag}] read_state({ROOM_ENCRYPTION_EVENT_TYPE}) failed: {error}; the encryption condition is unenforced"),
    }

    let mut slot_events = Vec::new();
    let mut slots_supplied = false;
    for event_type in SLOT_EVENT_TYPES {
        match driver.read_state(event_type.to_owned(), StateKeySelector::Any).await {
            Ok(events) => {
                slots_supplied = true;
                slot_events.extend(events);
            }
            Err(error) => log::warn!("[{tag}] read_state({event_type}) failed: {error}"),
        }
    }
    if slots_supplied {
        lock(inner).state.supply_slot_state();
        ingest(&slot_events);
    } else {
        log::warn!("[{tag}] no slot state could be read; the open-slot condition is unenforced");
    }

    match driver.read_state(ROOM_MEMBER_EVENT_TYPE.to_owned(), StateKeySelector::Any).await {
        // A room the caller can read has at least the caller in it, so an
        // empty answer is a host with no member state to offer, not an empty
        // room; enforcing it would exclude everyone.
        Ok(events) if events.is_empty() => {
            log::warn!("[{tag}] read_state({ROOM_MEMBER_EVENT_TYPE}) returned nothing; the sender-in-room condition is unenforced")
        }
        Ok(events) => {
            lock(inner).state.supply_room_members();
            ingest(&events);
        }
        Err(error) => log::warn!("[{tag}] read_state({ROOM_MEMBER_EVENT_TYPE}) failed: {error}; the sender-in-room condition is unenforced"),
    }

    for event_type in MEMBER_EVENT_TYPES {
        match driver.read_events(event_type.to_owned(), None, MEMBER_READ_LIMIT).await {
            Ok(events) => ingest(&events),
            Err(error) => log::warn!("[{tag}] read_events({event_type}) failed: {error}"),
        }
    }

    if config.compat == ElementCallCompat::StateEvents {
        match driver.read_state(LEGACY_MEMBER_EVENT_TYPE.to_owned(), StateKeySelector::Any).await {
            Ok(events) => ingest(&events),
            Err(error) => log::warn!("[{tag}] read_state({LEGACY_MEMBER_EVENT_TYPE}) failed: {error}"),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::driver::DriverError;
    use crate::session::test_support::*;
    use crate::session::{JoinExclusionReason, SlotState};
    use crate::types::EventOrigin;
    use serde_json::json;

    const NOW: u64 = 1_700_000_000_000;

    fn encrypted(device: &str) -> EventOrigin {
        EventOrigin::Encrypted { sender_device_id: Some(device.to_owned()) }
    }

    /// A driver whose room has this slot open (an empty slot read means
    /// "supplied, none" and would close it).
    fn driver_with_open_slot() -> Arc<FakeRoomEventsDriver> {
        Arc::new(FakeRoomEventsDriver::new().with_state(SLOT_EVENT_TYPES[0], vec![raw(slot_open_event(NOW), EventOrigin::Unknown)]))
    }

    fn session(driver: &Arc<FakeRoomEventsDriver>, clock: &FakeClock, compat: ElementCallCompat) -> Session {
        let driver: Arc<dyn RoomEventsDriver> = driver.clone();
        Session::with_clock(ROOM_ID.into(), SLOT_ID.into(), driver, SessionConfig { compat }, Arc::new(clock.clone()))
    }

    fn ids(snapshot: &SessionSnapshot) -> Vec<String> {
        snapshot.members.iter().map(|m| m.member_id.clone()).collect()
    }

    /// Wait until the pump has seeded (the driver logged every read).
    fn wait_seeded(driver: &FakeRoomEventsDriver) {
        wait_until(|| driver.log().iter().filter(|l| l.starts_with("read_")).count() >= 6);
    }

    #[test]
    fn subscribes_before_reading_and_the_seed_publishes_once() {
        let clock = FakeClock::new(NOW);
        let driver = Arc::new(
            FakeRoomEventsDriver::new()
                .with_state(ROOM_ENCRYPTION_EVENT_TYPE, vec![raw(room_encryption_event(NOW), EventOrigin::Unknown)])
                .with_state(SLOT_EVENT_TYPES[0], vec![raw(slot_event(SLOT_ID, json!({ "status": "open", "application": { "type": "m.call" }, "encryption": { "type": "m.per_member" } }), NOW), EventOrigin::Unknown)])
                .with_state(ROOM_MEMBER_EVENT_TYPE, vec![raw(room_member_event("@a:x", "join", NOW), EventOrigin::Unknown)])
                .with_events(MEMBER_EVENT_TYPES[0], vec![
                    raw(member_join_event("@a:x", "m-a", NOW), encrypted("A")),
                    raw(member_join_event("@gone:x", "m-gone", NOW), encrypted("G")),
                ]),
        );
        let session = session(&driver, &clock, ElementCallCompat::Off);
        let mut rx = session.subscribe();
        wait_seeded(&driver);
        let log = driver.log();
        let first_read = log.iter().position(|l| l.starts_with("read_")).unwrap();
        assert!(log[..first_read].contains(&"subscribe_room_events".to_owned()));
        assert!(log[..first_read].contains(&"subscribe_state_updates".to_owned()));

        // `subscribe()` happened before seeding could finish or after — either
        // way the receiver ends on the seeded value with at most one change.
        let snapshot = block_on(async {
            if rx.borrow().members.is_empty() {
                wait_for_change(&mut rx, 5_000).await;
            }
            rx.borrow_and_update().clone()
        });
        assert_eq!(ids(&snapshot), vec!["m-a"]);
        assert_eq!(snapshot.excluded_candidates[0].1, JoinExclusionReason::SenderNotInRoom);
        assert!(snapshot.slot_state.unwrap().is_open());
        assert_eq!(snapshot.negotiated_encryption, Some(true));
        assert!(!rx.has_changed().unwrap(), "one publish at the end of seeding, not per read");
    }

    #[test]
    fn a_failed_read_leaves_that_condition_unenforced() {
        let clock = FakeClock::new(NOW);
        let driver = Arc::new(
            FakeRoomEventsDriver::new()
                .with_state_error(SLOT_EVENT_TYPES[0], DriverError::Http("500".into()))
                .with_state_error(SLOT_EVENT_TYPES[1], DriverError::Http("500".into()))
                .with_state_error(ROOM_MEMBER_EVENT_TYPE, DriverError::Unauthorized("no".into()))
                .with_events(MEMBER_EVENT_TYPES[0], vec![raw(member_join_event("@a:x", "m-a", NOW), encrypted("A"))]),
        );
        let session = session(&driver, &clock, ElementCallCompat::Off);
        wait_seeded(&driver);
        let snapshot = session.snapshot();
        assert_eq!(ids(&snapshot), vec!["m-a"]);
        assert_eq!(snapshot.slot_state, None);
        assert!(snapshot.excluded_candidates.is_empty());
    }

    #[test]
    fn a_live_member_event_updates_the_snapshot_and_wakes_subscribers() {
        let clock = FakeClock::new(NOW);
        let driver = driver_with_open_slot();
        let session = session(&driver, &clock, ElementCallCompat::Off);
        wait_seeded(&driver);
        let mut rx = session.subscribe();
        assert!(driver.emit_room(raw(member_join_event("@a:x", "m-a", NOW), encrypted("A"))));
        block_on(wait_for_change(&mut rx, 5_000));
        assert_eq!(ids(&rx.borrow_and_update()), vec!["m-a"]);
        assert_eq!(ids(&session.snapshot()), vec!["m-a"]);
    }

    #[test]
    fn a_state_batch_is_applied_atomically() {
        let clock = FakeClock::new(NOW);
        let driver = driver_with_open_slot();
        let session = session(&driver, &clock, ElementCallCompat::Off);
        wait_seeded(&driver);
        driver.emit_room(raw(member_join_event("@a:x", "m-a", NOW), EventOrigin::Cleartext));
        let mut rx = session.subscribe();
        assert_eq!(ids(&rx.borrow_and_update()), vec!["m-a"]);
        // Slot closed + room encrypted, in one batch: one wake-up.
        assert!(driver.emit_state(vec![
            raw(slot_closed_event(NOW), EventOrigin::Unknown),
            raw(room_encryption_event(NOW), EventOrigin::Unknown),
        ]));
        block_on(wait_for_change(&mut rx, 5_000));
        let snapshot = rx.borrow_and_update().clone();
        assert!(snapshot.members.is_empty());
        assert_eq!(snapshot.slot_state, Some(SlotState::Closed));
        assert_eq!(snapshot.excluded_candidates[0].1, JoinExclusionReason::SlotClosed);
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!rx.has_changed().unwrap(), "exactly one publish for the batch");
    }

    #[test]
    fn events_for_another_slot_do_not_change_this_session_but_room_conditions_do() {
        let clock = FakeClock::new(NOW);
        // Slot reads fail: slot state stays unsupplied until a slot event arrives.
        let driver = Arc::new(
            FakeRoomEventsDriver::new()
                .with_state_error(SLOT_EVENT_TYPES[0], DriverError::Http("500".into()))
                .with_state_error(SLOT_EVENT_TYPES[1], DriverError::Http("500".into())),
        );
        let session = session(&driver, &clock, ElementCallCompat::Off);
        wait_seeded(&driver);
        driver.emit_room(raw(member_join_event("@a:x", "m-a", NOW), encrypted("A")));
        let mut other = member_join_event("@b:x", "m-b", NOW);
        other["content"]["slot_id"] = json!("m.whiteboard#ROOM");
        driver.emit_room(raw(other, encrypted("B")));
        assert_eq!(ids(&session.snapshot()), vec!["m-a"]);
        // The other slot's state event supplies slot state for the room, so
        // this slot (with no event) is now closed.
        driver.emit_room(raw(slot_event("m.whiteboard#ROOM", json!({ "status": "open", "application": { "type": "m.whiteboard" } }), NOW), EventOrigin::Unknown));
        let snapshot = session.snapshot();
        assert!(snapshot.members.is_empty());
        assert_eq!(snapshot.slot_state, Some(SlotState::Closed));
        // Events for another room are ignored entirely.
        let mut foreign = slot_open_event(NOW);
        foreign["room_id"] = json!("!other:x");
        driver.emit_room(raw(foreign, EventOrigin::Unknown));
        assert_eq!(session.snapshot().slot_state, Some(SlotState::Closed));
    }

    #[test]
    fn expiry_is_driven_by_the_timer_and_rearms() {
        let clock = FakeClock::new(NOW);
        let driver = driver_with_open_slot();
        let session = session(&driver, &clock, ElementCallCompat::Off);
        wait_seeded(&driver);
        let mut rx = session.subscribe();

        let mut short = member_join_event("@a:x", "m-a", NOW);
        short["msc4354_sticky"] = json!({ "duration_ms": 1_000 });
        driver.emit_room(raw(short, encrypted("A")));
        block_on(wait_for_change(&mut rx, 5_000));
        assert_eq!(ids(&rx.borrow_and_update()), vec!["m-a"]);
        wait_until(|| clock.sleepers() == 1);

        // An earlier-expiring event re-arms the timer.
        let mut shorter = member_join_event("@b:x", "m-b", NOW);
        shorter["msc4354_sticky"] = json!({ "duration_ms": 500 });
        driver.emit_room(raw(shorter, encrypted("B")));
        block_on(wait_for_change(&mut rx, 5_000));
        wait_until(|| clock.earliest_deadline() == Some(NOW + 500));

        clock.advance(500);
        block_on(wait_for_change(&mut rx, 5_000));
        let snapshot = rx.borrow_and_update().clone();
        assert_eq!(ids(&snapshot), vec!["m-a"]);
        assert_eq!(snapshot.excluded_candidates.iter().map(|(m, r)| (m.member_id.as_str(), *r)).collect::<Vec<_>>(), vec![("m-b", JoinExclusionReason::Expired)]);
        wait_until(|| clock.earliest_deadline() == Some(NOW + 1_000));

        // A refresh (same key, later end_time) extends it.
        let mut refresh = member_join_event("@a:x", "m-a", NOW + 500);
        refresh["msc4354_sticky"] = json!({ "duration_ms": 2_000 });
        driver.emit_room(raw(refresh, encrypted("A")));
        wait_until(|| clock.earliest_deadline() == Some(NOW + 2_500));
        clock.advance(1_000);
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(ids(&session.snapshot()), vec!["m-a"], "still there at the old deadline");
        clock.advance(1_000);
        // The transition after an expiry republishes once more to clear the
        // one-shot `Expired` entry, so wait for the roster itself.
        block_on(async {
            loop {
                wait_for_change(&mut rx, 5_000).await;
                if rx.borrow_and_update().members.is_empty() {
                    break;
                }
            }
        });
    }

    #[test]
    fn drain_on_read_makes_a_fresh_emit_visible_without_yielding() {
        let clock = FakeClock::new(NOW);
        let driver = driver_with_open_slot();
        let session = session(&driver, &clock, ElementCallCompat::Off);
        wait_seeded(&driver);
        let mut rx = session.subscribe();
        // Hold the lock-free fast path honest: emit then read immediately.
        driver.emit_room(raw(member_join_event("@a:x", "m-a", NOW), encrypted("A")));
        assert_eq!(ids(&session.snapshot()), vec!["m-a"]);
        // The pump later finds nothing to do and publishes nothing extra:
        // exactly one change is observable, from the drain-on-read publish.
        block_on(wait_for_change(&mut rx, 5_000));
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!rx.has_changed().unwrap());
    }

    #[test]
    fn dropping_the_session_closes_the_streams_and_ends_the_pump() {
        let clock = FakeClock::new(NOW);
        let driver = driver_with_open_slot();
        let session = session(&driver, &clock, ElementCallCompat::Off);
        wait_seeded(&driver);
        assert!(driver.emit_room(raw(member_join_event("@a:x", "m-a", NOW), encrypted("A"))));
        let inner = session.inner.clone();
        drop(session);
        assert!(!driver.emit_room(raw(member_join_event("@b:x", "m-b", NOW), encrypted("B"))));
        assert!(!driver.emit_state(vec![]));
        // The pump released its Arc.
        wait_until(|| Arc::strong_count(&inner) == 1);
    }

    #[test]
    fn unchanged_inputs_never_publish() {
        let clock = FakeClock::new(NOW);
        let driver = driver_with_open_slot();
        let session = session(&driver, &clock, ElementCallCompat::Off);
        wait_seeded(&driver);
        let ev = raw(member_join_event("@a:x", "m-a", NOW), encrypted("A"));
        driver.emit_room(ev.clone());
        let mut rx = session.subscribe();
        assert_eq!(ids(&rx.borrow_and_update()), vec!["m-a"]);
        driver.emit_room(ev.clone());
        driver.emit_room(ev);
        driver.emit_state(vec![raw(slot_open_event(NOW), EventOrigin::Unknown)]);
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!rx.has_changed().unwrap(), "re-asserting the open slot changes nothing");
        assert_eq!(session.debug_snapshot()["joined"], json!(["m-a"]));
        driver.emit_state(vec![raw(slot_closed_event(NOW), EventOrigin::Unknown)]);
        block_on(wait_for_change(&mut rx, 5_000)); // closing it is a change
        assert!(rx.borrow_and_update().members.is_empty());
    }

    #[test]
    fn legacy_members_are_seeded_with_state_events_compat() {
        let clock = FakeClock::new(NOW);
        let driver = Arc::new(FakeRoomEventsDriver::new().with_state(
            LEGACY_MEMBER_EVENT_TYPE,
            vec![raw(msc3401_member_event("@a:x", "DEV", NOW - 1_000, NOW - 1_000), EventOrigin::Unknown)],
        ));
        let driver_dyn: Arc<dyn RoomEventsDriver> = driver.clone();
        let session = Session::with_clock(
            ROOM_ID.into(),
            crate::session::LEGACY_SLOT_ID.into(),
            driver_dyn,
            SessionConfig { compat: ElementCallCompat::StateEvents },
            Arc::new(clock.clone()),
        );
        wait_until(|| driver.log().iter().any(|l| l == &format!("read_state:{LEGACY_MEMBER_EVENT_TYPE}")));
        wait_until(|| !session.snapshot().members.is_empty());
        let snapshot = session.snapshot();
        assert_eq!(ids(&snapshot), vec!["@a:x:DEV"]);
        assert_eq!(snapshot.slot_state, None);
    }
}
