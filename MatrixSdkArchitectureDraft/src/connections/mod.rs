//! Maps the session to the SFU connections a host must hold (multi-focus,
//! MSC4195). The connection key is the transport's `livekit_service_url`;
//! tokens are minted lazily through the [`TokenDriver`] slice, one per key,
//! and re-minted before their JWT expires. Nothing is published before we
//! have a member id of our own (the token names it), so the list is empty
//! outside a participation.

use crate::driver::{DriverError, LivekitTokenRequest, TokenDriver};
use crate::executor::{self, now_ms, sleep_ms};
use crate::own_membership::{OwnIdentity, ResolveTransportError};
use crate::session::{ElementCallCompat, SessionSnapshot};
use crate::types::{Member, RtcTransport, TransportIntent};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{Notify, watch};

const LIVEKIT: &str = "livekit";
const SERVICE_URL: &str = "livekit_service_url";
/// Re-mint this long before the JWT's `exp`.
const REFRESH_MARGIN_MS: u64 = 60_000;
/// Back off this long after a failed mint before trying that key again.
const MINT_RETRY_MS: u64 = 5_000;
/// Never re-mint the same key more often than this, whatever `exp` says: a
/// service handing out short-lived tokens must not turn the pump into a
/// busy loop (it shares the executor thread with every other pump).
const MIN_REFRESH_INTERVAL_MS: u64 = 5_000;

/// Everything a host needs to open one LiveKit room connection.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionData {
    /// The connection key: the transport's `livekit_service_url` — what
    /// `SessionMembership::connections` refers to.
    pub service_url: String,
    /// The SFU websocket URL to connect to (from the token response, else
    /// the service URL).
    pub ws_url: String,
    pub jwt_token: String,
    /// From the JWT's `exp`; `None` when it carries none.
    ///
    /// A failed re-mint keeps the *old* token rather than dropping it (a
    /// host that is still connected must not lose its credentials), so
    /// without this a host would only discover staleness through a LiveKit
    /// connect that fails for no stated reason
    /// (`ErrorSurfaceAnalysis.md` §5.2). An expired token is also reported
    /// as a [`ConnectionProblem`].
    pub expires_at_ts: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionWithMembers {
    pub connection: ConnectionData,
    /// The members publishing on this connection.
    pub members: Vec<Member>,
}

/// A connection the session needs that a host cannot currently use.
///
/// A wanted key with no token is simply *absent* from
/// [`ConnectionsManager::connections`], and a stale one is present but
/// unusable — both silent. This names them, with the members whose media is
/// affected and when the next attempt is due.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionProblem {
    pub service_url: String,
    /// Members whose media is unavailable because of it.
    pub member_ids: Vec<String>,
    pub kind: ConnectionProblemKind,
    /// The last mint failure, or a note that none has been attempted yet.
    pub last_error: String,
    /// When the next mint is due. `0` = at the next pump beat.
    pub retry_at_ts: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConnectionProblemKind {
    /// Wanted, never minted — the connection is absent from
    /// [`ConnectionsManager::connections`] entirely.
    NoToken,
    /// Present in `connections()` but its JWT is past `exp`. Kept on
    /// purpose: dropping it would break a host that is still connected.
    TokenExpired,
}

/// `livekit_service_url` of a LiveKit transport, or `None` for other kinds.
pub fn service_url(transport: &RtcTransport) -> Option<&str> {
    (transport.transport_type == LIVEKIT)
        .then(|| {
            transport
                .properties
                .get(SERVICE_URL)
                .and_then(Value::as_str)
        })
        .flatten()
}

/// The connection keys a member publishes on.
pub fn member_service_urls(member: &Member) -> Vec<String> {
    let mut urls: Vec<String> = member
        .transports
        .published
        .iter()
        .filter_map(service_url)
        .map(str::to_owned)
        .collect();
    urls.dedup();
    urls
}

/// MSC4195 pseudonymous participant identity:
/// `base64(SHA256(canonical_json([user_id, device_id, member_id])))`,
/// standard alphabet, unpadded.
pub fn participant_identity(user_id: &str, device_id: &str, member_id: &str) -> String {
    let canonical =
        serde_json::to_string(&[user_id, device_id, member_id]).expect("a string array serialises");
    STANDARD_NO_PAD.encode(Sha256::digest(canonical.as_bytes()))
}

/// The pre-MSC4195 plain `{user}:{device}` identity
/// (`ElementCallCompat::StateEvents`). Delete with that generation.
pub fn legacy_participant_identity(user_id: &str, device_id: &str) -> String {
    format!("{user_id}:{device_id}")
}

/// The identity for a member under the given compat mode; `None` when the
/// member has no device.
pub fn transport_identity(compat: ElementCallCompat, member: &Member) -> Option<String> {
    let device_id = member.device_id.as_deref()?;
    Some(match compat {
        ElementCallCompat::StateEvents => legacy_participant_identity(&member.user_id, device_id),
        _ => participant_identity(&member.user_id, device_id, &member.member_id),
    })
}

/// `exp` (seconds) of an unverified JWT, as ms.
fn jwt_expiry_ms(jwt: &str) -> Option<u64> {
    let payload = jwt.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    claims.get("exp")?.as_u64().map(|s| s * 1000)
}

#[derive(Clone, Debug)]
struct Token {
    jwt: String,
    ws_url: String,
    expires_at_ms: Option<u64>,
    minted_at_ms: u64,
}

impl Token {
    /// When to re-mint: a minute before `exp`, but never sooner than
    /// [`MIN_REFRESH_INTERVAL_MS`] after the last mint. `None` = never.
    fn refresh_at(&self) -> Option<u64> {
        let exp = self.expires_at_ms?;
        Some(
            exp.saturating_sub(REFRESH_MARGIN_MS)
                .max(self.minted_at_ms + MIN_REFRESH_INTERVAL_MS),
        )
    }
}

struct State {
    members: Vec<Member>,
    /// Our member id and the transport we publish on (`None` = receive-only
    /// or not joined).
    own_member_id: Option<String>,
    own_transport: Option<RtcTransport>,
    tokens: BTreeMap<String, Token>,
    /// Keys whose last mint failed, with the time to try again.
    retry_after: BTreeMap<String, u64>,
    /// The last mint failure per key, cleared by a successful mint. The
    /// cause `connections()` used to swallow entirely.
    last_error: BTreeMap<String, String>,
}

impl State {
    fn wanted(&self) -> BTreeSet<String> {
        let mut wanted = BTreeSet::new();
        if self.own_member_id.is_none() {
            return wanted;
        }
        if let Some(url) = self.own_transport.as_ref().and_then(service_url) {
            wanted.insert(url.to_owned());
        }
        for member in &self.members {
            wanted.extend(member_service_urls(member));
        }
        wanted
    }

    fn connections(&self) -> Vec<ConnectionWithMembers> {
        self.wanted()
            .into_iter()
            .filter_map(|key| {
                let token = self.tokens.get(&key)?;
                Some(ConnectionWithMembers {
                    connection: ConnectionData {
                        service_url: key.clone(),
                        ws_url: token.ws_url.clone(),
                        jwt_token: token.jwt.clone(),
                        expires_at_ts: token.expires_at_ms,
                    },
                    members: self
                        .members
                        .iter()
                        .filter(|m| member_service_urls(m).contains(&key))
                        .cloned()
                        .collect(),
                })
            })
            .collect()
    }

    /// The members publishing on one connection key.
    fn members_on(&self, key: &str) -> Vec<String> {
        self.members
            .iter()
            .filter(|m| member_service_urls(m).contains(&key.to_owned()))
            .map(|m| m.member_id.clone())
            .collect()
    }

    /// Every wanted connection a host cannot use right now, sorted by key so
    /// the published vec is stable (it is compared by `PartialEq`).
    fn problems(&self, now: u64) -> Vec<ConnectionProblem> {
        self.wanted()
            .into_iter()
            .filter_map(|key| {
                let kind = match self.tokens.get(&key) {
                    None => ConnectionProblemKind::NoToken,
                    Some(token) if token.expires_at_ms.is_some_and(|exp| exp <= now) => {
                        ConnectionProblemKind::TokenExpired
                    }
                    Some(_) => return None,
                };
                Some(ConnectionProblem {
                    service_url: key.clone(),
                    member_ids: self.members_on(&key),
                    kind,
                    last_error: self
                        .last_error
                        .get(&key)
                        .cloned()
                        .unwrap_or_else(|| "no token has been minted yet".to_owned()),
                    retry_at_ts: self.retry_after.get(&key).copied().unwrap_or(0),
                })
            })
            .collect()
    }

    /// Keys that need a (fresh) token now, and the earliest later deadline.
    fn due(&self, now: u64) -> (Vec<String>, Option<u64>) {
        let mut mint = Vec::new();
        let mut next: Option<u64> = None;
        let mut consider = |ts: u64| next = Some(next.map_or(ts, |n: u64| n.min(ts)));
        for key in self.wanted() {
            if let Some(retry) = self.retry_after.get(&key)
                && *retry > now
            {
                consider(*retry);
                continue;
            }
            match self.tokens.get(&key).map(Token::refresh_at) {
                None => mint.push(key),
                Some(Some(refresh_at)) if refresh_at <= now => mint.push(key),
                Some(Some(refresh_at)) => consider(refresh_at),
                Some(None) => {}
            }
        }
        (mint, next)
    }
}

struct Inner {
    room_id: String,
    slot_id: String,
    own: OwnIdentity,
    compat: ElementCallCompat,
    driver: Arc<dyn TokenDriver>,
    state: Mutex<State>,
    tx: watch::Sender<Vec<ConnectionWithMembers>>,
    /// A separate watch, because a mint failure changes *no connection* —
    /// the key is simply still absent — so the facade's pump would never
    /// wake on `tx` for the one thing it most needs to report.
    problems_tx: watch::Sender<Vec<ConnectionProblem>>,
    /// Poked when the own transport changes.
    wake: Notify,
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn publish(&self) {
        self.publish_problems();
        let connections = self.lock().connections();
        self.tx.send_if_modified(|current| {
            if *current == connections {
                return false;
            }
            log::info!(
                "[{}/{}] connections: {:?}",
                self.room_id,
                self.slot_id,
                connections
                    .iter()
                    .map(|c| (&c.connection.service_url, c.members.len()))
                    .collect::<Vec<_>>()
            );
            *current = connections;
            true
        });
    }

    fn publish_problems(&self) {
        let problems = self.lock().problems(now_ms());
        self.problems_tx.send_if_modified(|current| {
            if *current == problems {
                return false;
            }
            log::info!(
                "[{}/{}] connection problems: {:?}",
                self.room_id,
                self.slot_id,
                problems
                    .iter()
                    .map(|p| (&p.service_url, p.kind))
                    .collect::<Vec<_>>()
            );
            *current = problems;
            true
        });
    }

    async fn mint(&self, key: &str, member_id: String) -> Result<Token, DriverError> {
        let request = LivekitTokenRequest {
            url: key.to_owned(),
            room_id: self.room_id.clone(),
            slot_id: self.slot_id.clone(),
            member: json!({
                "id": member_id,
                "claimed_user_id": self.own.user_id,
                "claimed_device_id": self.own.device_id,
            }),
            legacy_sfu_get: self.compat == ElementCallCompat::StateEvents,
        };
        let response = self.driver.get_livekit_token(request).await?;
        let expires_at_ms = jwt_expiry_ms(&response.jwt);
        log::debug!(
            "[{}] minted a token for {key} (expires {expires_at_ms:?})",
            self.room_id
        );
        Ok(Token {
            jwt: response.jwt,
            ws_url: response.url.unwrap_or_else(|| key.to_owned()),
            expires_at_ms,
            minted_at_ms: now_ms(),
        })
    }

    /// Mint every due token, one at a time; returns the next deadline.
    async fn mint_due(&self) -> Option<u64> {
        loop {
            let (due, next) = self.lock().due(now_ms());
            let Some(key) = due.into_iter().next() else {
                return next;
            };
            let Some(member_id) = self.lock().own_member_id.clone() else {
                return next;
            };
            match self.mint(&key, member_id).await {
                Ok(token) => {
                    let mut state = self.lock();
                    state.retry_after.remove(&key);
                    state.last_error.remove(&key);
                    state.tokens.insert(key, token);
                }
                Err(error) => {
                    log::warn!(
                        "[{}] could not mint a token for {key}: {error}",
                        self.room_id
                    );
                    let mut state = self.lock();
                    state
                        .retry_after
                        .insert(key.clone(), now_ms() + MINT_RETRY_MS);
                    state.last_error.insert(key, error.to_string());
                }
            }
            self.publish();
        }
    }
}

/// Which connections the session requires, with a valid token each.
pub struct ConnectionsManager {
    inner: Arc<Inner>,
    notify: Arc<Notify>,
}

impl ConnectionsManager {
    pub fn new(
        room_id: String,
        slot_id: String,
        own: OwnIdentity,
        compat: ElementCallCompat,
        session: watch::Receiver<SessionSnapshot>,
        driver: Arc<dyn TokenDriver>,
    ) -> Self {
        let (tx, _) = watch::channel(Vec::new());
        let (problems_tx, _) = watch::channel(Vec::new());
        let inner = Arc::new(Inner {
            room_id,
            slot_id,
            own,
            compat,
            driver,
            state: Mutex::new(State {
                members: Vec::new(),
                own_member_id: None,
                own_transport: None,
                tokens: BTreeMap::new(),
                retry_after: BTreeMap::new(),
                last_error: BTreeMap::new(),
            }),
            tx,
            problems_tx,
            wake: Notify::new(),
        });
        let notify = Arc::new(Notify::new());
        executor::spawn(run(Arc::downgrade(&inner), notify.clone(), session));
        Self { inner, notify }
    }

    /// Current value + every change.
    pub fn subscribe(&self) -> watch::Receiver<Vec<ConnectionWithMembers>> {
        self.inner.tx.subscribe()
    }

    pub fn connections(&self) -> Vec<ConnectionWithMembers> {
        self.inner.tx.borrow().clone()
    }

    /// Every wanted connection a host cannot use right now, sorted by
    /// `service_url`. Empty is the healthy case.
    ///
    /// Recomputed against the current clock, so a token that expired since
    /// the last publish shows up here even before the pump next beats.
    pub fn problems(&self) -> Vec<ConnectionProblem> {
        self.inner.lock().problems(now_ms())
    }

    /// Current problems + every change. A failed mint changes no connection,
    /// so this is the only push signal for it.
    pub fn subscribe_problems(&self) -> watch::Receiver<Vec<ConnectionProblem>> {
        self.inner.problems_tx.subscribe()
    }

    /// Resolve the transport we publish on: a `Publish` intent naming a
    /// `livekit_service_url` is used as is, otherwise `GET /rtc/transports`
    /// supplies the first LiveKit transport; its token is minted (MSC4195,
    /// naming `member_id`) and it is recorded as our own — from now on the
    /// connection set includes it. `ReceiveOnly` records the member id only
    /// (tokens for the connections we subscribe to follow the session).
    pub async fn add_own_transport(
        &self,
        member_id: String,
        intent: TransportIntent,
    ) -> Result<RtcTransport, ResolveTransportError> {
        let transport = match intent {
            TransportIntent::Publish(t)
                if service_url(&t).is_some() || t.transport_type != LIVEKIT =>
            {
                t
            }
            TransportIntent::Publish(_) => {
                // Discovery: a failure here means the homeserver advertises
                // nothing usable, which retrying will not change.
                let transports = self
                    .inner
                    .driver
                    .get_rtc_transports()
                    .await
                    .map_err(ResolveTransportError::NoTransport)?;
                transports
                    .into_iter()
                    .find(|t| service_url(t).is_some())
                    .ok_or_else(|| {
                        ResolveTransportError::NoTransport(DriverError::Unsupported(
                            "the homeserver advertises no LiveKit transport".into(),
                        ))
                    })?
            }
            TransportIntent::ReceiveOnly { .. } => {
                let mut state = self.inner.lock();
                state.own_member_id = Some(member_id);
                state.own_transport = None;
                drop(state);
                self.inner.wake.notify_one();
                return Err(ResolveTransportError::NoTransport(DriverError::Other(
                    "receive-only intents publish no transport".into(),
                )));
            }
        };
        // Mint first, record second: the pump never sees our transport
        // without its token (no duplicate mint, no half-published state).
        // A failure here is the *other* story — a transport exists, its
        // token was refused — so it is reported separately.
        let token = match service_url(&transport) {
            Some(key) => Some((
                key.to_owned(),
                self.inner
                    .mint(key, member_id.clone())
                    .await
                    .map_err(ResolveTransportError::TokenRefused)?,
            )),
            None => None,
        };
        {
            let mut state = self.inner.lock();
            state.own_member_id = Some(member_id);
            state.own_transport = Some(transport.clone());
            if let Some((key, token)) = token {
                state.tokens.insert(key, token);
            }
        }
        self.inner.publish();
        self.inner.wake.notify_one();
        Ok(transport)
    }

    /// Record our member id without a transport (receive-only join).
    pub fn set_own_member(&self, member_id: String) {
        let mut state = self.inner.lock();
        state.own_member_id = Some(member_id);
        state.own_transport = None;
        drop(state);
        self.inner.wake.notify_one();
    }

    /// Leaving: forget our transport, member id and every token.
    pub fn clear_own(&self) {
        let mut state = self.inner.lock();
        state.own_member_id = None;
        state.own_transport = None;
        state.tokens.clear();
        state.retry_after.clear();
        state.last_error.clear();
        drop(state);
        self.inner.publish();
        self.inner.wake.notify_one();
    }
}

impl Drop for ConnectionsManager {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

async fn run(
    inner: Weak<Inner>,
    notify: Arc<Notify>,
    mut session: watch::Receiver<SessionSnapshot>,
) {
    let mut session_open = true;
    session.mark_changed();
    loop {
        let Some(strong) = inner.upgrade() else {
            return;
        };
        let next = strong.mint_due().await;
        strong.publish();
        let sleep = async {
            match next {
                Some(ts) => sleep_ms(ts.saturating_sub(now_ms())).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            changed = session.changed(), if session_open => match changed {
                Ok(()) => {
                    let members = session.borrow_and_update().members.clone();
                    strong.lock().members = members;
                }
                Err(_) => session_open = false,
            },
            _ = strong.wake.notified() => {}
            _ = sleep => {}
            _ = notify.notified() => {
                drop(strong);
                if inner.upgrade().is_none() { return }
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::driver::LivekitTokenResponse;
    use crate::types::{DeviceAttribution, MemberTransports};
    use async_trait::async_trait;
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct MockTokens {
        requests: Mutex<Vec<LivekitTokenRequest>>,
        transports: Mutex<Vec<RtcTransport>>,
        exp_secs: Mutex<Option<u64>>,
        /// When set, every mint fails with this error.
        refuse: Mutex<Option<DriverError>>,
    }

    #[async_trait]
    impl TokenDriver for MockTokens {
        async fn get_rtc_transports(&self) -> Result<Vec<RtcTransport>, DriverError> {
            Ok(self.transports.lock().unwrap().clone())
        }
        async fn get_livekit_token(
            &self,
            request: LivekitTokenRequest,
        ) -> Result<LivekitTokenResponse, DriverError> {
            let url = request.url.clone();
            self.requests.lock().unwrap().push(request);
            if let Some(error) = self.refuse.lock().unwrap().clone() {
                return Err(error);
            }
            let jwt = match *self.exp_secs.lock().unwrap() {
                Some(exp) => {
                    let payload = URL_SAFE_NO_PAD.encode(json!({ "exp": exp }).to_string());
                    format!("h.{payload}.s")
                }
                None => format!("jwt-for-{url}"),
            };
            Ok(LivekitTokenResponse {
                jwt,
                url: Some(url.replace("https", "wss")),
            })
        }
    }

    fn lk(url: &str) -> RtcTransport {
        RtcTransport {
            transport_type: LIVEKIT.into(),
            properties: json!({ SERVICE_URL: url }),
        }
    }

    fn member(id: &str, url: Option<&str>) -> Member {
        Member {
            member_id: id.into(),
            user_id: format!("@{id}:x"),
            device_id: Some("D".into()),
            device_attribution: DeviceAttribution::Verified,
            membership_ts: None,
            display_name: None,
            avatar_url: None,
            intent: None,
            application_type: None,
            transports: MemberTransports {
                published: url.map(lk).into_iter().collect(),
                can_subscribe: vec![],
            },
        }
    }

    fn snapshot(members: Vec<Member>) -> SessionSnapshot {
        SessionSnapshot {
            members,
            ..Default::default()
        }
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn wait_until(what: &str, cond: impl FnMut() -> bool) {
        wait_until_within(what, Duration::from_secs(5), cond);
    }

    fn wait_until_within(what: &str, within: Duration, mut cond: impl FnMut() -> bool) {
        let deadline = Instant::now() + within;
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for: {what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn manager(
        driver: Arc<MockTokens>,
        rx: watch::Receiver<SessionSnapshot>,
        compat: ElementCallCompat,
    ) -> ConnectionsManager {
        ConnectionsManager::new(
            "!r:x".into(),
            "m.call#ROOM".into(),
            OwnIdentity {
                user_id: "@me:x".into(),
                device_id: "DEV".into(),
            },
            compat,
            rx,
            driver,
        )
    }

    #[test]
    fn identities_match_msc4195_and_the_legacy_form() {
        let id = participant_identity("@user:matrix.example.com", "DEVICEID", "xyzABCDEF10123");
        assert_eq!(id.len(), 43);
        assert!(!id.contains('='));
        assert_eq!(
            id,
            participant_identity("@user:matrix.example.com", "DEVICEID", "xyzABCDEF10123")
        );
        assert_eq!(legacy_participant_identity("@u:x", "D"), "@u:x:D");
        let m = member("m", None);
        assert_eq!(
            transport_identity(ElementCallCompat::StateEvents, &m),
            Some("@m:x:D".into())
        );
        assert_eq!(
            transport_identity(ElementCallCompat::Off, &m),
            Some(participant_identity("@m:x", "D", "m"))
        );
    }

    #[test]
    fn jwt_expiry_is_read_from_the_payload() {
        let payload = URL_SAFE_NO_PAD.encode(json!({ "exp": 1_700_000_000u64 }).to_string());
        assert_eq!(
            jwt_expiry_ms(&format!("h.{payload}.s")),
            Some(1_700_000_000_000)
        );
        assert_eq!(jwt_expiry_ms("not-a-jwt"), None);
    }

    #[test]
    fn nothing_is_published_before_we_join_then_own_and_member_connections_get_tokens() {
        let driver = Arc::new(MockTokens::default());
        let (tx, rx) = watch::channel(snapshot(vec![member("a", Some("https://a"))]));
        let m = manager(driver.clone(), rx, ElementCallCompat::Off);
        std::thread::sleep(Duration::from_millis(30));
        assert!(m.connections().is_empty(), "no member id of ours yet");
        assert!(driver.requests.lock().unwrap().is_empty());

        let own = block_on(
            m.add_own_transport("m-me".into(), TransportIntent::Publish(lk("https://own"))),
        )
        .unwrap();
        assert_eq!(own, lk("https://own"));
        // Our own connection is there right after the call returns.
        let ours = m.connections();
        assert!(
            ours.iter()
                .any(|c| c.connection.service_url == "https://own"
                    && c.connection.jwt_token == "jwt-for-https://own"
                    && c.connection.ws_url == "wss://own")
        );
        // The member's connection follows from the pump.
        wait_until("member connection", || m.connections().len() == 2);
        let mut rx = m.subscribe();
        let a = m
            .connections()
            .into_iter()
            .find(|c| c.connection.service_url == "https://a")
            .unwrap();
        assert_eq!(
            a.members
                .iter()
                .map(|m| m.member_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a"]
        );
        {
            let requests = driver.requests.lock().unwrap();
            assert_eq!(
                requests[0].member,
                json!({ "id": "m-me", "claimed_user_id": "@me:x", "claimed_device_id": "DEV" })
            );
            assert_eq!(requests[0].slot_id, "m.call#ROOM");
            assert!(!requests[0].legacy_sfu_get);
        }

        // A second member on the same connection re-uses the token.
        tx.send(snapshot(vec![
            member("a", Some("https://a")),
            member("b", Some("https://a")),
            member("c", None),
        ]))
        .unwrap();
        block_on(async { rx.changed().await.unwrap() });
        let a = rx
            .borrow()
            .iter()
            .find(|c| c.connection.service_url == "https://a")
            .unwrap()
            .clone();
        assert_eq!(a.members.len(), 2, "receive-only c is on no connection");
        assert_eq!(driver.requests.lock().unwrap().len(), 2);

        // Leaving empties the list.
        m.clear_own();
        assert!(m.connections().is_empty());
    }

    #[test]
    fn a_bare_publish_intent_is_discovered_and_state_events_use_sfu_get() {
        let driver = Arc::new(MockTokens::default());
        *driver.transports.lock().unwrap() = vec![
            RtcTransport {
                transport_type: "other".into(),
                properties: json!({}),
            },
            lk("https://discovered"),
        ];
        let (_tx, rx) = watch::channel(snapshot(vec![]));
        let m = manager(driver.clone(), rx, ElementCallCompat::StateEvents);
        let own = block_on(m.add_own_transport(
            "@me:x:DEV".into(),
            TransportIntent::Publish(RtcTransport {
                transport_type: LIVEKIT.into(),
                properties: json!({}),
            }),
        ))
        .unwrap();
        assert_eq!(own, lk("https://discovered"));
        assert!(driver.requests.lock().unwrap()[0].legacy_sfu_get);
    }

    #[test]
    fn refresh_is_a_minute_before_exp_but_never_a_busy_loop() {
        let now = 1_000_000_000;
        let token = |exp: Option<u64>| Token {
            jwt: String::new(),
            ws_url: String::new(),
            expires_at_ms: exp,
            minted_at_ms: now,
        };
        assert_eq!(
            token(Some(now + 3_600_000)).refresh_at(),
            Some(now + 3_600_000 - REFRESH_MARGIN_MS)
        );
        // Already inside the margin when minted: wait the minimum interval
        // instead of re-minting at once (that spun the executor thread).
        assert_eq!(
            token(Some(now + 61_000)).refresh_at(),
            Some(now + MIN_REFRESH_INTERVAL_MS)
        );
        assert_eq!(token(None).refresh_at(), None);
    }

    #[test]
    fn a_near_expired_token_is_not_re_minted_in_a_loop() {
        let driver = Arc::new(MockTokens::default());
        *driver.exp_secs.lock().unwrap() = Some(now_ms() / 1000 + 61);
        let (_tx, rx) = watch::channel(snapshot(vec![]));
        let m = manager(driver.clone(), rx, ElementCallCompat::Off);
        block_on(m.add_own_transport("m-me".into(), TransportIntent::Publish(lk("https://own"))))
            .unwrap();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            driver.requests.lock().unwrap().len(),
            1,
            "one mint, then the minimum interval"
        );
        assert_eq!(m.connections().len(), 1);
    }

    /// §5.1/§5.2: a key with no token is *absent* from `connections()` and a
    /// stale one is present but unusable. Both must be nameable, and both
    /// must clear once a mint succeeds.
    #[test]
    fn a_failed_mint_is_a_problem_with_a_cause_and_clears_when_it_succeeds() {
        let driver = Arc::new(MockTokens::default());
        *driver.refuse.lock().unwrap() = Some(DriverError::Unauthorized("openid refused".into()));
        let (_tx, rx) = watch::channel(snapshot(vec![member("a", Some("https://a"))]));
        let m = manager(driver.clone(), rx, ElementCallCompat::Off);
        m.set_own_member("m-me".into());
        wait_until("a problem", || !m.problems().is_empty());

        let problems = m.problems();
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].service_url, "https://a");
        assert_eq!(problems[0].kind, ConnectionProblemKind::NoToken);
        assert_eq!(problems[0].member_ids, vec!["a".to_owned()]);
        assert!(problems[0].last_error.contains("openid refused"));
        assert!(problems[0].retry_at_ts > 0, "a retry is scheduled");
        assert!(
            m.connections().is_empty(),
            "the connection itself stays absent"
        );

        // The mint succeeding clears it — a problem is never terminal.
        // The retry is `MINT_RETRY_MS` out, so allow for it.
        *driver.refuse.lock().unwrap() = None;
        wait_until_within(
            "recovery",
            Duration::from_millis(MINT_RETRY_MS + 5_000),
            || m.problems().is_empty(),
        );
        assert_eq!(m.connections().len(), 1);
    }

    /// A token past its `exp` is kept (the host may still be connected on it)
    /// but flagged, and its expiry is on the connection for a host to check.
    #[test]
    fn an_expired_token_is_still_served_but_reported() {
        let driver = Arc::new(MockTokens::default());
        *driver.exp_secs.lock().unwrap() = Some(now_ms() / 1000 - 10);
        let (_tx, rx) = watch::channel(snapshot(vec![]));
        let m = manager(driver.clone(), rx, ElementCallCompat::Off);
        block_on(m.add_own_transport("m-me".into(), TransportIntent::Publish(lk("https://own"))))
            .unwrap();
        let connections = m.connections();
        assert_eq!(connections.len(), 1, "not dropped from under the host");
        assert!(connections[0].connection.expires_at_ts.unwrap() < now_ms());
        let problems = m.problems();
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].kind, ConnectionProblemKind::TokenExpired);
    }

    #[test]
    fn dropping_the_manager_stops_the_pump() {
        let driver = Arc::new(MockTokens::default());
        let (tx, rx) = watch::channel(snapshot(vec![]));
        let m = manager(driver.clone(), rx, ElementCallCompat::Off);
        drop(m);
        wait_until("pump gone", || tx.receiver_count() == 0);
        assert_eq!(Arc::strong_count(&driver), 1);
    }
}
