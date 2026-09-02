//! Test fixtures: JSON builders mirroring `web-test-app/src/mockDriver.ts`
//! field-for-field (one source of truth per language; `MOCK_DRIVER_JOIN_FIXTURE`
//! is a verbatim copy of the TS builder's output and a test parses it), a
//! scriptable `RoomEventsDriver`, and a controllable clock so no test sleeps
//! for real.

use crate::driver::{DriverError, RoomEventsDriver, StateKeySelector};
use crate::session::live::{BoxSleep, Clock};
use crate::types::{EventOrigin, RawMatrixEvent};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

pub(crate) const ROOM_ID: &str = "!room:example.org";
pub(crate) const SLOT_ID: &str = "m.call#ROOM";
pub(crate) const LK_SERVICE_URL: &str = "https://lk.example.org";

/// `memberJoinEvent({ userId: "@remote:example.org", deviceId: "RDEV", memberId: "m-1" })`
/// as `mockDriver.ts` emits it (timestamp and event id fixed).
pub(crate) const MOCK_DRIVER_JOIN_FIXTURE: &str = r#"{
  "type": "m.rtc.member",
  "sender": "@remote:example.org",
  "event_id": "$ev-0",
  "room_id": "!room:example.org",
  "origin_server_ts": 1700000000000,
  "msc4354_sticky": { "duration_ms": 240000 },
  "content": {
    "slot_id": "m.call#ROOM",
    "msc4354_sticky_key": "m-1",
    "member": { "id": "m-1", "membership": "join" },
    "application": { "type": "m.call" },
    "transports": {
      "published": [{ "type": "livekit", "livekit_service_url": "https://lk.example.org" }],
      "can_subscribe": ["livekit"]
    }
  }
}"#;

static EVENT_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_event_id() -> String {
    format!("$ev-{}", EVENT_COUNTER.fetch_add(1, Ordering::Relaxed))
}

fn base(event_type: &str, sender: &str, ts: u64, content: Value) -> Value {
    json!({
        "type": event_type,
        "sender": sender,
        "event_id": next_event_id(),
        "room_id": ROOM_ID,
        "origin_server_ts": ts,
        "content": content,
    })
}

pub(crate) fn member_join_event(user_id: &str, member_id: &str, ts: u64) -> Value {
    member_join_event_with(user_id, member_id, ts, LK_SERVICE_URL)
}

pub(crate) fn member_join_event_with(user_id: &str, member_id: &str, ts: u64, lk_service_url: &str) -> Value {
    let mut event = base(
        "m.rtc.member",
        user_id,
        ts,
        json!({
            "slot_id": SLOT_ID,
            "msc4354_sticky_key": member_id,
            "member": { "id": member_id, "membership": "join" },
            "application": { "type": "m.call" },
            "transports": {
                "published": [{ "type": "livekit", "livekit_service_url": lk_service_url }],
                "can_subscribe": ["livekit"],
            },
        }),
    );
    event["msc4354_sticky"] = json!({ "duration_ms": 240_000 });
    event
}

pub(crate) fn member_leave_event(user_id: &str, member_id: &str, ts: u64) -> Value {
    let mut event = base(
        "m.rtc.member",
        user_id,
        ts,
        json!({
            "slot_id": SLOT_ID,
            "msc4354_sticky_key": member_id,
            "member": { "id": member_id, "membership": "leave" },
            "leave_reason": { "code": "m.user_hangup" },
        }),
    );
    event["msc4354_sticky"] = json!({ "duration_ms": 240_000 });
    event
}

/// The 2025 dialect's leave / an MSC4354 removal: the sticky key alone.
pub(crate) fn member_bare_leave_event(user_id: &str, member_id: &str, ts: u64) -> Value {
    let mut event = base("m.rtc.member", user_id, ts, json!({ "msc4354_sticky_key": member_id }));
    event["msc4354_sticky"] = json!({ "duration_ms": 240_000 });
    event
}

pub(crate) fn slot_event(slot_id: &str, content: Value, ts: u64) -> Value {
    let mut event = base("m.rtc.slot", "@admin:example.org", ts, content);
    event["state_key"] = json!(slot_id);
    event
}

pub(crate) fn slot_open_event(ts: u64) -> Value {
    slot_event(SLOT_ID, json!({ "status": "open", "application": { "type": "m.call" } }), ts)
}

pub(crate) fn slot_closed_event(ts: u64) -> Value {
    slot_event(SLOT_ID, json!({ "status": "closed", "application": { "type": "m.call" } }), ts)
}

pub(crate) fn room_member_event(user_id: &str, membership: &str, ts: u64) -> Value {
    let mut event = base("m.room.member", user_id, ts, json!({ "membership": membership }));
    event["state_key"] = json!(user_id);
    event
}

pub(crate) fn room_encryption_event(ts: u64) -> Value {
    let mut event = base("m.room.encryption", "@admin:example.org", ts, json!({ "algorithm": "m.megolm.v1.aes-sha2" }));
    event["state_key"] = json!("");
    event
}

/// A pre-sticky Element Call room-state membership (4 h lifetime).
pub(crate) fn msc3401_member_event(user_id: &str, device_id: &str, created_ts: u64, origin_server_ts: u64) -> Value {
    let mut event = base(
        "org.matrix.msc3401.call.member",
        user_id,
        origin_server_ts,
        json!({
            "application": "m.call",
            "call_id": "",
            "scope": "m.room",
            "device_id": device_id,
            "membershipID": format!("{user_id}:{device_id}"),
            "expires": 14_400_000,
            "created_ts": created_ts,
            "m.call.intent": "video",
            "focus_active": { "type": "livekit", "focus_selection": "oldest_membership" },
            "foci_preferred": [{ "type": "livekit", "livekit_service_url": LK_SERVICE_URL, "livekit_alias": ROOM_ID }],
        }),
    );
    event["state_key"] = json!(format!("_{user_id}_{device_id}_m.call"));
    event
}

/// Re-home an event into another room.
pub(crate) fn in_room(mut event: Value, room_id: &str) -> Value {
    event["room_id"] = json!(room_id);
    event
}

pub(crate) fn raw(event: Value, origin: EventOrigin) -> RawMatrixEvent {
    RawMatrixEvent { event, origin }
}

// -- fake driver -------------------------------------------------------------

type Scripted = HashMap<String, Result<Vec<RawMatrixEvent>, DriverError>>;

/// A `RoomEventsDriver` with scripted `read_*` results and hand-fed
/// streams. Unknown event types read as empty.
#[derive(Default)]
pub(crate) struct FakeRoomEventsDriver {
    state: Mutex<Scripted>,
    events: Mutex<Scripted>,
    log: Mutex<Vec<String>>,
    room_txs: Mutex<Vec<UnboundedSender<RawMatrixEvent>>>,
    state_txs: Mutex<Vec<UnboundedSender<Vec<RawMatrixEvent>>>>,
}

impl FakeRoomEventsDriver {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_state(self, event_type: &str, events: Vec<RawMatrixEvent>) -> Self {
        self.state.lock().unwrap().insert(event_type.to_owned(), Ok(events));
        self
    }

    pub(crate) fn with_state_error(self, event_type: &str, error: DriverError) -> Self {
        self.state.lock().unwrap().insert(event_type.to_owned(), Err(error));
        self
    }

    pub(crate) fn with_events(self, event_type: &str, events: Vec<RawMatrixEvent>) -> Self {
        self.events.lock().unwrap().insert(event_type.to_owned(), Ok(events));
        self
    }

    /// Every call in order: `subscribe_*`, `read_state:<type>`, `read_events:<type>`.
    pub(crate) fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// Send on the room stream; `false` once no subscriber is left.
    pub(crate) fn emit_room(&self, event: RawMatrixEvent) -> bool {
        let mut txs = self.room_txs.lock().unwrap();
        txs.retain(|tx| tx.send(event.clone()).is_ok());
        !txs.is_empty()
    }

    pub(crate) fn emit_state(&self, batch: Vec<RawMatrixEvent>) -> bool {
        let mut txs = self.state_txs.lock().unwrap();
        txs.retain(|tx| tx.send(batch.clone()).is_ok());
        !txs.is_empty()
    }

    fn record(&self, line: String) {
        self.log.lock().unwrap().push(line);
    }
}

#[async_trait::async_trait]
impl RoomEventsDriver for FakeRoomEventsDriver {
    async fn read_events(
        &self,
        event_type: String,
        _state_key: Option<StateKeySelector>,
        _limit: u32,
    ) -> Result<Vec<RawMatrixEvent>, DriverError> {
        self.record(format!("read_events:{event_type}"));
        self.events.lock().unwrap().get(&event_type).cloned().unwrap_or_else(|| Ok(Vec::new()))
    }

    async fn read_state(
        &self,
        event_type: String,
        _state_key: StateKeySelector,
    ) -> Result<Vec<RawMatrixEvent>, DriverError> {
        self.record(format!("read_state:{event_type}"));
        self.state.lock().unwrap().get(&event_type).cloned().unwrap_or_else(|| Ok(Vec::new()))
    }

    fn subscribe_room_events(&self) -> UnboundedReceiver<RawMatrixEvent> {
        self.record("subscribe_room_events".into());
        let (tx, rx) = unbounded_channel();
        self.room_txs.lock().unwrap().push(tx);
        rx
    }

    fn subscribe_state_updates(&self) -> UnboundedReceiver<Vec<RawMatrixEvent>> {
        self.record("subscribe_state_updates".into());
        let (tx, rx) = unbounded_channel();
        self.state_txs.lock().unwrap().push(tx);
        rx
    }
}

// -- fake clock ----------------------------------------------------------------

#[derive(Default)]
struct FakeClockInner {
    now: u64,
    next_id: u64,
    /// Pending sleeps by id: deadline + the waker to call once `now` reaches it.
    sleepers: HashMap<u64, (u64, Option<Waker>)>,
}

/// A clock that only moves when told to. Sleeps resolve when `advance`
/// carries `now` past their deadline.
#[derive(Clone)]
pub(crate) struct FakeClock {
    inner: Arc<Mutex<FakeClockInner>>,
}

impl FakeClock {
    pub(crate) fn new(now: u64) -> Self {
        Self { inner: Arc::new(Mutex::new(FakeClockInner { now, ..FakeClockInner::default() })) }
    }

    pub(crate) fn advance(&self, ms: u64) {
        let wakers: Vec<Waker> = {
            let mut inner = self.inner.lock().unwrap();
            inner.now += ms;
            inner.sleepers.values_mut().filter_map(|(_, waker)| waker.take()).collect()
        };
        for waker in wakers {
            waker.wake();
        }
    }

    /// Sleeps that have been created and not yet dropped.
    pub(crate) fn sleepers(&self) -> usize {
        self.inner.lock().unwrap().sleepers.len()
    }

    pub(crate) fn earliest_deadline(&self) -> Option<u64> {
        self.inner.lock().unwrap().sleepers.values().map(|(d, _)| *d).min()
    }
}

impl Clock for FakeClock {
    fn now_ms(&self) -> u64 {
        self.inner.lock().unwrap().now
    }

    fn sleep(&self, ms: u64) -> BoxSleep {
        let mut inner = self.inner.lock().unwrap();
        let deadline = inner.now + ms;
        let id = inner.next_id;
        inner.next_id += 1;
        inner.sleepers.insert(id, (deadline, None));
        Box::pin(FakeSleep { clock: self.inner.clone(), deadline, id })
    }
}

struct FakeSleep {
    clock: Arc<Mutex<FakeClockInner>>,
    deadline: u64,
    id: u64,
}

impl Future for FakeSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let (deadline, id) = (self.deadline, self.id);
        let mut inner = self.clock.lock().unwrap();
        if inner.now >= deadline {
            if let Some(entry) = inner.sleepers.get_mut(&id) {
                entry.1 = None;
            }
            return Poll::Ready(());
        }
        if let Some(entry) = inner.sleepers.get_mut(&id) {
            entry.1 = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for FakeSleep {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.clock.lock() {
            inner.sleepers.remove(&self.id);
        }
    }
}

// -- async helpers -------------------------------------------------------------

/// Run a future to completion on a fresh current-thread runtime (with a
/// timer, for `wait_for_change`). `watch` channels need no runtime driver,
/// so this works against the pump running on `executor`'s thread.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(future)
}

/// Await the next `changed()` on a watch receiver, panicking after `timeout_ms`.
pub(crate) async fn wait_for_change<T>(rx: &mut tokio::sync::watch::Receiver<T>, timeout_ms: u64) {
    tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), rx.changed())
        .await
        .expect("timed out waiting for a snapshot change")
        .expect("the snapshot sender was dropped");
}

/// Spin (briefly sleeping) until `condition` holds, panicking after 5 s.
pub(crate) fn wait_until(mut condition: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while !condition() {
        assert!(start.elapsed() < std::time::Duration::from_secs(5), "condition never became true");
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}
