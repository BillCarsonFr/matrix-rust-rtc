//! Black-box tests of `ParticipationManager` through a mock `MatrixDriver`
//! that models a homeserver: it records outbound calls, echoes accepted
//! sticky/state events back into the room-event stream (as sync would),
//! answers `read_*` from a scripted room state, mints tokens, and hosts
//! scripted remote peers that answer our media key with theirs. Nothing here
//! reaches into the crate's modules — only `matrix_rtc`'s public surface.

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use matrix_rtc::connections::participant_identity;
use matrix_rtc::driver::*;
use matrix_rtc::encryption::SendMachineConfig;
use matrix_rtc::participation::{MembershipState, SessionMembership, Status};
use matrix_rtc::types::*;
use matrix_rtc::{
    ElementCallCompat, JoinParams, OwnIdentity, ParticipationConfig, ParticipationManager,
};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

const ROOM: &str = "!room:example.org";
const SLOT: &str = "m.call#ROOM";
const ME: &str = "@me:example.org";
const MY_DEVICE: &str = "MYDEV";
const LK: &str = "https://lk.example.org";

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// The mock homeserver
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Call {
    Sticky {
        event_type: String,
        content: Value,
        duration_ms: u64,
    },
    State {
        event_type: String,
        state_key: String,
        content: Value,
    },
    Delayed {
        content: Value,
        delay_ms: u64,
        sticky_duration_ms: Option<u64>,
    },
    DelayedState {
        state_key: String,
        content: Value,
        delay_ms: u64,
    },
    Restart(String),
    Cancel(String),
    Delegate(String),
    ToDevice {
        recipients: Vec<ToDeviceRecipient>,
        event_type: String,
        content: Value,
    },
    GetTransports,
    GetToken(LivekitTokenRequest),
}

/// A scripted remote participant: answers our media key with its own.
#[derive(Clone)]
struct Peer {
    user_id: String,
    device_id: String,
    member_id: String,
    key: Vec<u8>,
}

#[derive(Default)]
struct Mock {
    calls: Mutex<Vec<Call>>,
    room_events: Mutex<Vec<UnboundedSender<RawMatrixEvent>>>,
    state_updates: Mutex<Vec<UnboundedSender<Vec<RawMatrixEvent>>>>,
    to_device: Mutex<Vec<UnboundedSender<ToDeviceMessage>>>,
    /// Room state the seed reads (`read_state`).
    state: Mutex<Vec<Value>>,
    peers: Mutex<Vec<Peer>>,
    counter: AtomicU64,
    refuse_delayed: bool,
}

impl Mock {
    fn new(state: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(state),
            ..Self::default()
        })
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn record(&self, call: Call) {
        self.calls.lock().unwrap().push(call);
    }

    fn next_id(&self) -> String {
        format!("$ev{}", self.counter.fetch_add(1, Ordering::Relaxed))
    }

    fn emit_room_event(&self, event: Value, origin: EventOrigin) {
        self.room_events.lock().unwrap().retain(|tx| {
            tx.send(RawMatrixEvent {
                event: event.clone(),
                origin: origin.clone(),
            })
            .is_ok()
        });
    }

    fn emit_state_update(&self, events: Vec<Value>) {
        let batch: Vec<RawMatrixEvent> = events
            .into_iter()
            .map(|event| RawMatrixEvent {
                event,
                origin: EventOrigin::Cleartext,
            })
            .collect();
        self.state_updates
            .lock()
            .unwrap()
            .retain(|tx| tx.send(batch.clone()).is_ok());
    }

    fn emit_to_device(&self, message: ToDeviceMessage) {
        self.to_device
            .lock()
            .unwrap()
            .retain(|tx| tx.send(message.clone()).is_ok());
    }

    fn add_peer(&self, peer: Peer) {
        self.peers.lock().unwrap().push(peer);
    }

    /// The peer joins the session (publishing on `LK`).
    fn peer_joins(&self, peer: &Peer) {
        self.emit_room_event(
            member_join_event(&peer.user_id, &peer.member_id, LK, 240_000),
            EventOrigin::Encrypted {
                sender_device_id: Some(peer.device_id.clone()),
            },
        );
    }

    fn peer_leaves(&self, peer: &Peer) {
        self.emit_room_event(
            member_leave_event(&peer.user_id, &peer.member_id),
            EventOrigin::Encrypted {
                sender_device_id: Some(peer.device_id.clone()),
            },
        );
    }

    fn peer_sends_key(&self, peer: &Peer, index: u8) {
        self.emit_to_device(ToDeviceMessage {
            event_type: "m.rtc.encryption_key".into(),
            sender: peer.user_id.clone(),
            content: json!({
                "room_id": ROOM,
                "member_id": peer.member_id,
                "media_key": { "index": index, "key": STANDARD_NO_PAD.encode(&peer.key) },
                "format": 0,
            }),
            origin: EventOrigin::Encrypted {
                sender_device_id: Some(peer.device_id.clone()),
            },
            sender_cross_signed: Some(true),
        });
    }

    fn sticky_sends(&self) -> Vec<Value> {
        self.calls()
            .into_iter()
            .filter_map(|c| match c {
                Call::Sticky { content, .. } => Some(content),
                _ => None,
            })
            .collect()
    }

    fn to_device_sends(&self) -> Vec<(Vec<ToDeviceRecipient>, Value)> {
        self.calls()
            .into_iter()
            .filter_map(|c| match c {
                Call::ToDevice {
                    recipients,
                    content,
                    ..
                } => Some((recipients, content)),
                _ => None,
            })
            .collect()
    }
}

fn subscribe<T>(list: &Mutex<Vec<UnboundedSender<T>>>) -> UnboundedReceiver<T> {
    let (tx, rx) = unbounded_channel();
    list.lock().unwrap().push(tx);
    rx
}

#[async_trait]
impl OwnMembershipDriver for Mock {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<SendEventResponse, DriverError> {
        self.record(Call::Sticky {
            event_type: event_type.clone(),
            content: content.clone(),
            duration_ms,
        });
        let event_id = self.next_id();
        // The homeserver echoes our event through sync.
        self.emit_room_event(
            json!({ "type": event_type, "sender": ME, "event_id": event_id, "room_id": room_id,
                    "origin_server_ts": now(), "msc4354_sticky": { "duration_ms": duration_ms }, "content": content }),
            EventOrigin::Encrypted { sender_device_id: Some(MY_DEVICE.into()) },
        );
        Ok(SendEventResponse {
            event_id: Some(event_id),
            delay_id: None,
        })
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<SendEventResponse, DriverError> {
        self.record(Call::State {
            event_type: event_type.clone(),
            state_key: state_key.clone(),
            content: content.clone(),
        });
        let event_id = self.next_id();
        self.emit_room_event(
            json!({ "type": event_type, "sender": ME, "event_id": event_id, "room_id": room_id, "state_key": state_key,
                    "origin_server_ts": now(), "content": content }),
            EventOrigin::Cleartext,
        );
        Ok(SendEventResponse {
            event_id: Some(event_id),
            delay_id: None,
        })
    }

    async fn send_delayed_event(
        &self,
        _room_id: String,
        _event_type: String,
        content: Value,
        delay_ms: u64,
        sticky_duration_ms: Option<u64>,
    ) -> Result<String, DriverError> {
        self.record(Call::Delayed {
            content,
            delay_ms,
            sticky_duration_ms,
        });
        if self.refuse_delayed {
            return Err(DriverError::Unsupported("M_UNRECOGNIZED".into()));
        }
        Ok(format!(
            "delay-{}",
            self.counter.fetch_add(1, Ordering::Relaxed)
        ))
    }

    async fn send_delayed_state_event(
        &self,
        _room_id: String,
        _event_type: String,
        state_key: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, DriverError> {
        self.record(Call::DelayedState {
            state_key,
            content,
            delay_ms,
        });
        Ok(format!(
            "delay-{}",
            self.counter.fetch_add(1, Ordering::Relaxed)
        ))
    }

    async fn restart_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), DriverError> {
        self.record(Call::Restart(delay_id));
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), DriverError> {
        self.record(Call::Cancel(delay_id));
        Ok(())
    }

    async fn delegate_livekit_delayed_leave(
        &self,
        request: DelegatedDelayedLeaveRequest,
    ) -> Result<(), DriverError> {
        self.record(Call::Delegate(request.delay_id));
        Ok(())
    }
}

#[async_trait]
impl ToDeviceSendDriver for Mock {
    async fn send_to_device(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        event_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, DriverError> {
        self.record(Call::ToDevice {
            recipients: recipients.clone(),
            event_type,
            content,
        });
        // Scripted peers answer with their own key (index 0).
        let peers = self.peers.lock().unwrap().clone();
        for recipient in &recipients {
            if let Some(peer) = peers
                .iter()
                .find(|p| p.user_id == recipient.user_id && p.device_id == recipient.device_id)
            {
                self.peer_sends_key(peer, 0);
            }
        }
        Ok(recipients
            .into_iter()
            .map(|recipient| ToDeviceDelivery {
                recipient,
                error: None,
            })
            .collect())
    }
}

impl ToDeviceDriver for Mock {
    fn subscribe_to_device_events(&self) -> UnboundedReceiver<ToDeviceMessage> {
        subscribe(&self.to_device)
    }
}

#[async_trait]
impl RoomEventsDriver for Mock {
    async fn read_events(
        &self,
        _event_type: String,
        _state_key: Option<StateKeySelector>,
        _limit: u32,
    ) -> Result<Vec<RawMatrixEvent>, DriverError> {
        Ok(Vec::new())
    }

    async fn read_state(
        &self,
        event_type: String,
        state_key: StateKeySelector,
    ) -> Result<Vec<RawMatrixEvent>, DriverError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e["type"] == event_type)
            .filter(|e| match &state_key {
                StateKeySelector::Any => true,
                StateKeySelector::Key(k) => e["state_key"] == *k,
            })
            .map(|e| RawMatrixEvent {
                event: e.clone(),
                origin: EventOrigin::Cleartext,
            })
            .collect())
    }

    fn subscribe_room_events(&self) -> UnboundedReceiver<RawMatrixEvent> {
        subscribe(&self.room_events)
    }

    fn subscribe_state_updates(&self) -> UnboundedReceiver<Vec<RawMatrixEvent>> {
        subscribe(&self.state_updates)
    }
}

#[async_trait]
impl TokenDriver for Mock {
    async fn get_rtc_transports(&self) -> Result<Vec<RtcTransport>, DriverError> {
        self.record(Call::GetTransports);
        Ok(vec![lk_transport(LK)])
    }

    async fn get_livekit_token(
        &self,
        request: LivekitTokenRequest,
    ) -> Result<LivekitTokenResponse, DriverError> {
        let url = request.url.clone();
        self.record(Call::GetToken(request));
        Ok(LivekitTokenResponse {
            jwt: format!("jwt-for-{url}"),
            url: Some(url.replace("https", "wss")),
        })
    }
}

// ---------------------------------------------------------------------------
// Event fixtures (the wire shapes the session reads)
// ---------------------------------------------------------------------------

fn lk_transport(url: &str) -> RtcTransport {
    RtcTransport {
        transport_type: "livekit".into(),
        properties: json!({ "livekit_service_url": url }),
    }
}

fn member_join_event(user_id: &str, member_id: &str, url: &str, duration_ms: u64) -> Value {
    json!({
        "type": "m.rtc.member", "sender": user_id, "event_id": format!("$join-{member_id}-{}", now()), "room_id": ROOM,
        "origin_server_ts": now(), "msc4354_sticky": { "duration_ms": duration_ms },
        "content": {
            "slot_id": SLOT, "msc4354_sticky_key": member_id,
            "member": { "id": member_id, "membership": "join" },
            "application": { "type": "m.call" },
            "transports": { "published": [{ "type": "livekit", "livekit_service_url": url }], "can_subscribe": ["livekit"] },
        },
    })
}

fn member_leave_event(user_id: &str, member_id: &str) -> Value {
    json!({
        "type": "m.rtc.member", "sender": user_id, "event_id": format!("$leave-{member_id}-{}", now()), "room_id": ROOM,
        "origin_server_ts": now(), "msc4354_sticky": { "duration_ms": 240_000 },
        "content": {
            "slot_id": SLOT, "msc4354_sticky_key": member_id,
            "member": { "id": member_id, "membership": "leave" },
            "leave_reason": { "code": "leave" },
        },
    })
}

fn slot_event(status: &str, encrypted: bool) -> Value {
    let mut content = json!({ "status": status, "application": { "type": "m.call" } });
    if encrypted {
        content["encryption"] = json!({ "type": "m.per_member" });
    }
    json!({ "type": "m.rtc.slot", "sender": "@admin:example.org", "event_id": "$slot", "room_id": ROOM,
            "state_key": SLOT, "origin_server_ts": now(), "content": content })
}

fn room_encryption_event() -> Value {
    json!({ "type": "m.room.encryption", "sender": "@admin:example.org", "event_id": "$enc", "room_id": ROOM,
            "state_key": "", "origin_server_ts": now(), "content": { "algorithm": "m.megolm.v1.aes-sha2" } })
}

fn peer(n: u32) -> Peer {
    Peer {
        user_id: format!("@peer{n}:example.org"),
        device_id: format!("PEERDEV{n}"),
        member_id: format!("m-peer-{n}"),
        key: vec![n as u8; 32],
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn config(compat: ElementCallCompat) -> ParticipationConfig {
    ParticipationConfig {
        compat,
        // Fast rotations so `LeftWithKeys` settles within a test.
        rotation: SendMachineConfig {
            shared_per_minute_to_device_contingent: 1_000_000,
            use_key_delay_ms: 50,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn manager(mock: &Arc<Mock>) -> ParticipationManager {
    ParticipationManager::new(
        ROOM.into(),
        SLOT.into(),
        OwnIdentity {
            user_id: ME.into(),
            device_id: MY_DEVICE.into(),
        },
        mock.clone(),
        config(ElementCallCompat::Off),
    )
}

fn params() -> JoinParams {
    JoinParams {
        sticky_duration_ms: 240_000,
        keep_alive_timeout_ms: 60_000,
        ..JoinParams::new("m.call")
    }
}

fn receive_only() -> TransportIntent {
    TransportIntent::ReceiveOnly {
        can_subscribe: vec!["livekit".into()],
    }
}

fn publish() -> TransportIntent {
    TransportIntent::Publish(lk_transport(LK))
}

/// Encrypted room, encrypted slot open.
fn encrypted_room() -> Vec<Value> {
    vec![room_encryption_event(), slot_event("open", true)]
}

fn open_room() -> Vec<Value> {
    vec![slot_event("open", false)]
}

async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

trait Coarse {
    fn coarse(&self) -> &'static str;
}

impl Coarse for Status {
    fn coarse(&self) -> &'static str {
        match self {
            Status::Disconnected => "Disconnected",
            Status::Joining(_) => "Joining",
            Status::Connected(_) => "Connected",
            Status::Leaving(_) => "Leaving",
        }
    }
}

fn own_member(m: &ParticipationManager) -> Option<SessionMembership> {
    m.memberships().into_iter().find(|m| m.member.user_id == ME)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn starts_disconnected_with_nothing() {
    let mock = Mock::new(open_room());
    let m = manager(&mock);
    assert_eq!(m.status(), Status::Disconnected);
    assert!(m.memberships().is_empty());
    assert!(m.connections().is_empty());
    assert!(m.key_map().is_empty());
    assert!(matches!(
        m.leave(None).await,
        Err(matrix_rtc::own_membership::LeaveError::NotJoined)
    ));
}

#[tokio::test]
async fn a_remote_member_join_shows_up_as_a_joined_membership_before_we_join() {
    let mock = Mock::new(open_room());
    let m = manager(&mock);
    let p = peer(1);
    mock.peer_joins(&p);
    // Drain-on-read: the getter is fresh right after the emit.
    let memberships = m.memberships();
    assert_eq!(memberships.len(), 1);
    assert_eq!(memberships[0].member.member_id, "m-peer-1");
    assert_eq!(memberships[0].state, MembershipState::Joined);
    assert_eq!(memberships[0].connections, vec![LK.to_owned()]);
    assert_eq!(
        memberships[0].transport_identity,
        Some(participant_identity(&p.user_id, &p.device_id, &p.member_id))
    );
    // No token for it: we are not in the call.
    assert!(m.connections().is_empty());
}

#[tokio::test]
async fn join_arms_the_delayed_leave_before_the_membership_and_our_echo_lists_us() {
    let mock = Mock::new(open_room());
    let m = manager(&mock);
    m.join(receive_only(), params()).await.unwrap();

    let calls = mock.calls();
    let delayed_at = calls
        .iter()
        .position(|c| matches!(c, Call::Delayed { .. }))
        .expect("delayed leave armed");
    let sticky_at = calls
        .iter()
        .position(|c| matches!(c, Call::Sticky { .. }))
        .expect("join sent");
    assert!(delayed_at < sticky_at, "dead man's switch first: {calls:?}");
    match &calls[delayed_at] {
        Call::Delayed {
            content,
            delay_ms,
            sticky_duration_ms,
        } => {
            assert_eq!(*delay_ms, 60_000);
            assert_eq!(
                *sticky_duration_ms,
                Some(240_000),
                "the delayed leave is sticky too"
            );
            assert_eq!(content["leave_reason"]["code"], "delayed_leave");
        }
        _ => unreachable!(),
    }
    match &calls[sticky_at] {
        Call::Sticky {
            event_type,
            content,
            duration_ms,
        } => {
            assert_eq!(event_type, "org.matrix.msc4143.rtc.member");
            assert_eq!(*duration_ms, 240_000);
            assert_eq!(content["member"]["membership"], "join");
            assert_eq!(content["application"]["type"], "m.call");
            assert!(
                content["transports"]["published"]
                    .as_array()
                    .unwrap()
                    .is_empty(),
                "receive-only publishes nothing"
            );
            assert_eq!(content["transports"]["can_subscribe"], json!(["livekit"]));
        }
        _ => unreachable!(),
    }
    assert!(
        !calls.iter().any(|c| matches!(c, Call::GetToken(_))),
        "receive-only mints no token"
    );

    // The homeserver echoed our event: we are in the roster like anybody else.
    let me = own_member(&m).expect("our own membership");
    assert_eq!(me.state, MembershipState::Joined);
    assert!(me.connections.is_empty());
    assert!(matches!(m.status(), Status::Connected(_)));
}

#[tokio::test]
async fn a_publishing_member_mints_its_token_advertises_its_transport_and_holds_its_connection() {
    let mock = Mock::new(open_room());
    let m = manager(&mock);
    m.join(publish(), params()).await.unwrap();

    let calls = mock.calls();
    let token_at = calls
        .iter()
        .position(|c| matches!(c, Call::GetToken(_)))
        .expect("token minted");
    let delayed_at = calls
        .iter()
        .position(|c| matches!(c, Call::Delayed { .. }))
        .unwrap();
    assert!(
        token_at < delayed_at,
        "the token exists before anything is published: {calls:?}"
    );
    let Call::GetToken(request) = &calls[token_at] else {
        unreachable!()
    };
    assert_eq!(request.url, LK);
    assert_eq!(request.slot_id, SLOT);
    assert!(!request.legacy_sfu_get);
    assert_eq!(request.member["claimed_user_id"], ME);
    assert_eq!(request.member["claimed_device_id"], MY_DEVICE);
    let member_id = request.member["id"].as_str().unwrap().to_owned();

    let sticky = &mock.sticky_sends()[0];
    assert_eq!(sticky["member"]["id"], member_id);
    assert_eq!(
        sticky["transports"]["published"],
        json!([{ "type": "livekit", "livekit_service_url": LK }])
    );

    // Our connection is there as soon as join resolves.
    let connections = m.connections();
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].connection.service_url, LK);
    assert_eq!(connections[0].connection.ws_url, "wss://lk.example.org");
    assert_eq!(connections[0].connection.jwt_token, format!("jwt-for-{LK}"));
    let me = own_member(&m).unwrap();
    assert_eq!(me.connections, vec![LK.to_owned()]);
    assert_eq!(
        me.transport_identity,
        Some(participant_identity(ME, MY_DEVICE, &member_id))
    );
    // …and connections() lists us on it.
    wait_for("we are a member of our connection", || {
        m.connections()[0]
            .members
            .iter()
            .any(|mm| mm.member_id == member_id)
    })
    .await;
}

#[tokio::test]
async fn slot_encryption_turns_key_distribution_on() {
    let mock = Mock::new(encrypted_room());
    let m = manager(&mock);
    let p = peer(1);
    mock.add_peer(p.clone());
    mock.peer_joins(&p);
    m.join(receive_only(), params()).await.unwrap();

    wait_for("our key sent to the peer's device", || {
        !mock.to_device_sends().is_empty()
    })
    .await;
    let (recipients, content) = &mock.to_device_sends()[0];
    assert_eq!(
        recipients,
        &[ToDeviceRecipient {
            user_id: p.user_id.clone(),
            device_id: p.device_id.clone()
        }]
    );
    assert_eq!(content["room_id"], ROOM);
    assert_eq!(content["media_key"]["index"], 0);
    let own_id = content["member_id"].as_str().unwrap().to_owned();

    // The peer answered: both keys are in the map.
    wait_for("both keys", || m.key_map().len() == 2).await;
    let map = m.key_map();
    assert_eq!(map[&p.member_id][0].key, p.key);
    assert_eq!(map[&own_id][0].index, 0);
    wait_for("connected with keys", || matches!(m.status(), Status::Connected(c) if matches!(c.encryption, matrix_rtc::encryption::Status::Connected { .. }))).await;
}

#[tokio::test]
async fn absent_slot_encryption_turns_key_distribution_off() {
    let mock = Mock::new(open_room());
    let m = manager(&mock);
    let p = peer(1);
    mock.add_peer(p.clone());
    mock.peer_joins(&p);
    m.join(receive_only(), params()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        mock.to_device_sends().is_empty(),
        "no keys in an unencrypted call"
    );
    assert!(m.key_map().is_empty());
    // An inbound key is ignored too.
    mock.peer_sends_key(&p, 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(m.key_map().is_empty());
    match m.status() {
        Status::Connected(c) => assert!(matches!(
            c.encryption,
            matrix_rtc::encryption::Status::Connected { .. }
        )),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn our_own_echo_does_not_trigger_a_key_send_to_ourselves() {
    let mock = Mock::new(encrypted_room());
    let m = manager(&mock);
    m.join(receive_only(), params()).await.unwrap();
    wait_for("we are listed", || own_member(&m).is_some()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(mock.to_device_sends().is_empty());
    // Alone: our own key is in the map, nobody else's.
    assert_eq!(m.key_map().len(), 1);
}

#[tokio::test]
async fn a_member_that_left_while_holding_our_key_stays_left_with_keys_until_the_rotation() {
    let mock = Mock::new(encrypted_room());
    let m = manager(&mock);
    let p = peer(1);
    mock.add_peer(p.clone());
    m.join(receive_only(), params()).await.unwrap();
    mock.peer_joins(&p);
    wait_for("key exchanged", || m.key_map().len() == 2).await;

    mock.peer_leaves(&p);
    let gone = m
        .memberships()
        .into_iter()
        .find(|mm| mm.member.member_id == p.member_id)
        .expect("still listed");
    assert_eq!(gone.state, MembershipState::LeftWithKeys);
    assert!(gone.connections.is_empty());

    // The rotation (jittered grace, then the 50 ms use delay) settles: the
    // peer drops out, our key has a new index, the peer's key is forgotten.
    wait_for("rotation settles", || {
        !m.memberships()
            .iter()
            .any(|mm| mm.member.user_id == p.user_id)
    })
    .await;
    let map = m.key_map();
    let own = map
        .values()
        .find(|ring| ring.len() == 2)
        .expect("own key ring with both indexes");
    assert_eq!(own.iter().map(|k| k.index).collect::<Vec<_>>(), vec![0, 1]);
    // No rotation batch went out: nobody left to send to.
    assert_eq!(mock.to_device_sends().len(), 1);
}

#[tokio::test]
async fn a_rejoin_in_the_same_process_distributes_a_key_to_the_incumbent() {
    let mock = Mock::new(encrypted_room());
    let m = manager(&mock);
    let p = peer(1);
    mock.add_peer(p.clone());
    mock.peer_joins(&p);
    m.join(receive_only(), params()).await.unwrap();
    wait_for("first key", || mock.to_device_sends().len() == 1).await;
    m.leave(None).await.unwrap();
    assert!(m.key_map().is_empty(), "leaving forgets every key");
    assert_eq!(m.status(), Status::Disconnected);

    m.join(receive_only(), params()).await.unwrap();
    wait_for("second key", || mock.to_device_sends().len() == 2).await;
    let sends = mock.to_device_sends();
    assert_ne!(
        sends[0].1["member_id"], sends[1].1["member_id"],
        "a fresh member id per join"
    );
    assert_eq!(sends[1].0[0].user_id, p.user_id);
}

#[tokio::test]
async fn a_member_whose_sticky_entry_expired_is_dropped() {
    let mock = Mock::new(open_room());
    let m = manager(&mock);
    mock.emit_room_event(
        member_join_event("@short:example.org", "m-short", LK, 300),
        EventOrigin::Encrypted {
            sender_device_id: Some("S".into()),
        },
    );
    assert_eq!(m.memberships().len(), 1);
    wait_for("expiry", || m.memberships().is_empty()).await;
}

#[tokio::test]
async fn leave_sends_the_leave_then_cancels_the_delay_and_clears_everything() {
    let mock = Mock::new(encrypted_room());
    let m = manager(&mock);
    m.join(publish(), params()).await.unwrap();
    assert_eq!(m.connections().len(), 1);
    m.leave(Some(LeaveReason::new("m.user_hangup", None)))
        .await
        .unwrap();

    let calls = mock.calls();
    let leave_at = calls.iter().position(|c| matches!(c, Call::Sticky { content, .. } if content["member"]["membership"] == "leave")).unwrap();
    let Call::Sticky { content, .. } = &calls[leave_at] else {
        unreachable!()
    };
    assert_eq!(content["leave_reason"]["code"], "m.user_hangup");
    assert!(matches!(&calls[leave_at + 1], Call::Cancel(_)));
    assert_eq!(m.status(), Status::Disconnected);
    assert!(m.key_map().is_empty());
    assert!(m.connections().is_empty());
    // Our echoed leave removed us from the roster.
    assert!(own_member(&m).is_none());
}

#[tokio::test]
async fn a_slot_closed_under_us_ends_the_participation() {
    let mock = Mock::new(encrypted_room());
    let m = manager(&mock);
    m.join(publish(), params()).await.unwrap();
    wait_for("own key", || m.key_map().len() == 1).await;
    mock.emit_state_update(vec![slot_event("closed", true)]);
    wait_for("left", || m.status() == Status::Disconnected).await;
    let leave = mock
        .sticky_sends()
        .into_iter()
        .find(|c| c["member"]["membership"] == "leave")
        .expect("leave sent");
    assert_eq!(leave["leave_reason"]["code"], "slot_closed");
    wait_for("keys and tokens gone", || {
        m.key_map().is_empty() && m.connections().is_empty()
    })
    .await;
}

#[tokio::test]
async fn callbacks_fire_on_change_with_the_getter_values() {
    let mock = Mock::new(encrypted_room());
    let m = manager(&mock);
    let memberships: Arc<Mutex<Vec<Vec<SessionMembership>>>> = Arc::default();
    let statuses: Arc<Mutex<Vec<Status>>> = Arc::default();
    let connections: Arc<Mutex<Vec<usize>>> = Arc::default();
    let keys: Arc<Mutex<Vec<(String, u8)>>> = Arc::default();
    {
        let sink = memberships.clone();
        m.on_memberships_change(Box::new(move |list| {
            sink.lock().unwrap().push(list.to_vec())
        }));
        let sink = statuses.clone();
        m.on_status_change(Box::new(move |status| {
            sink.lock().unwrap().push(status.clone())
        }));
        let sink = connections.clone();
        m.on_connections_change(Box::new(move |list| sink.lock().unwrap().push(list.len())));
        let sink = keys.clone();
        m.on_key_map_change(Box::new(move |_map, change| {
            sink.lock()
                .unwrap()
                .push((change.member_id.clone(), change.key.index))
        }));
    }
    let p = peer(1);
    mock.add_peer(p.clone());
    mock.peer_joins(&p);
    m.join(publish(), params()).await.unwrap();
    wait_for("membership callback caught up", || {
        memberships.lock().unwrap().last() == Some(&m.memberships()) && m.memberships().len() == 2
    })
    .await;
    wait_for("status callback saw Connected", || {
        matches!(statuses.lock().unwrap().last(), Some(Status::Connected(_)))
    })
    .await;
    wait_for("connections callback", || {
        connections.lock().unwrap().last() == Some(&1)
    })
    .await;
    wait_for("key callbacks: own key and the peer's", || {
        let k = keys.lock().unwrap();
        k.iter().any(|(id, _)| id == &p.member_id) && k.len() >= 2
    })
    .await;
    // Publish-on-change (the watch coalesces fast transitions, so `Joining`
    // may or may not have been observed).
    let seen = statuses.lock().unwrap().clone();
    assert!(
        seen.windows(2).all(|w| w[0] != w[1]),
        "publish-on-change: {seen:?}"
    );
    assert_eq!(
        statuses.lock().unwrap().last().map(Status::coarse),
        Some("Connected")
    );
}

#[tokio::test]
async fn a_homeserver_without_delayed_events_still_lets_us_join_with_a_short_membership() {
    let mock = Arc::new(Mock {
        state: Mutex::new(open_room()),
        refuse_delayed: true,
        ..Mock::default()
    });
    let m = manager(&mock);
    m.join(receive_only(), params()).await.unwrap();
    let calls = mock.calls();
    assert!(matches!(
        calls.iter().find(|c| matches!(c, Call::Sticky { .. })),
        Some(Call::Sticky {
            duration_ms: 300_000,
            ..
        })
    ));
    match m.status() {
        Status::Connected(c) => assert!(!c.own_membership.delayed_leave_supported),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn state_events_compat_publishes_room_state_with_the_legacy_identity() {
    let mock = Mock::new(vec![]);
    let m = ParticipationManager::new(
        ROOM.into(),
        "".into(),
        OwnIdentity {
            user_id: ME.into(),
            device_id: MY_DEVICE.into(),
        },
        mock.clone(),
        config(ElementCallCompat::StateEvents),
    );
    m.join(publish(), params()).await.unwrap();
    let calls = mock.calls();
    let Some(Call::GetToken(request)) = calls.iter().find(|c| matches!(c, Call::GetToken(_)))
    else {
        panic!("{calls:?}")
    };
    assert!(request.legacy_sfu_get);
    assert_eq!(request.member["id"], format!("{ME}:{MY_DEVICE}"));
    assert!(
        matches!(calls.iter().find(|c| matches!(c, Call::DelayedState { .. })), Some(Call::DelayedState { state_key, .. }) if state_key == &format!("_{ME}_{MY_DEVICE}_m.call"))
    );
    let Some(Call::State {
        event_type,
        content,
        ..
    }) = calls.iter().find(|c| matches!(c, Call::State { .. }))
    else {
        panic!()
    };
    assert_eq!(event_type, "org.matrix.msc3401.call.member");
    assert_eq!(content["foci_preferred"][0]["livekit_alias"], ROOM);
    // The echoed state event lands in the legacy slot with the legacy identity.
    let me = own_member(&m).expect("listed via the state echo");
    assert_eq!(me.member.member_id, format!("{ME}:{MY_DEVICE}"));
    assert_eq!(me.transport_identity, Some(format!("{ME}:{MY_DEVICE}")));
    assert_eq!(me.member.device_attribution, DeviceAttribution::Claimed);
    m.leave(None).await.unwrap();
    assert!(matches!(mock.calls().last(), Some(Call::Cancel(_))));
    assert!(own_member(&m).is_none());
}

#[tokio::test]
async fn slot_administration_sends_slot_state() {
    let mock = Mock::new(vec![]);
    let m = manager(&mock);
    assert!(
        m.open_slot("m.whiteboard", false).await.is_err(),
        "slot id must start with the application type"
    );
    m.open_slot("m.call", true).await.unwrap();
    let Some(Call::State {
        event_type,
        state_key,
        content,
    }) = mock.calls().last().cloned()
    else {
        panic!()
    };
    assert_eq!(event_type, "org.matrix.msc4143.rtc.slot");
    assert_eq!(state_key, SLOT);
    assert_eq!(
        content,
        json!({ "status": "open", "application": { "type": "m.call" }, "encryption": { "type": "m.per_member" } })
    );
    // The echo opened our slot: a join is accepted…
    m.join(receive_only(), params()).await.unwrap();
    m.leave(None).await.unwrap();
    // …and closing it refuses the next one (the echo is drained by `join`).
    m.close_slot().await.unwrap();
    assert!(matches!(
        m.join(receive_only(), params()).await,
        Err(matrix_rtc::own_membership::JoinError::SlotClosed)
    ));
}

#[tokio::test]
async fn dropping_the_manager_stops_every_pump_and_releases_the_driver() {
    let mock = Mock::new(encrypted_room());
    let m = manager(&mock);
    m.join(publish(), params()).await.unwrap();
    drop(m);
    wait_for("driver released", || Arc::strong_count(&mock) == 1).await;
    // A later emit reaches nobody.
    mock.emit_room_event(
        member_join_event("@late:example.org", "m-late", LK, 240_000),
        EventOrigin::Unknown,
    );
    assert!(mock.room_events.lock().unwrap().is_empty());
}
