//! The one task of the own-membership manager: commands from the host,
//! the session `watch`, the deadline timer and the driver calls. State
//! transitions are the pure [`Machine`](super::machine::Machine), stepped
//! under a short lock never held across an `await`; status is published after
//! the lock is released. Driver calls are awaited inside the loop, one at a
//! time, so nothing of one membership interleaves (a heartbeat restart and a
//! leave's cancel cannot cross).

use super::machine::{Action, Input, Outcome};
use super::wire::Route;
use super::{Inner, TransportResolver};
use crate::driver::{DelegatedDelayedLeaveRequest, OwnMembershipDriver};
use crate::executor::{now_ms, sleep_ms};
use crate::session::SessionSnapshot;
use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Notify, watch};

pub(super) struct Pump {
    pub inner: Weak<Inner>,
    pub notify: Arc<Notify>,
    pub driver: Arc<dyn OwnMembershipDriver>,
    pub resolve_transport: TransportResolver,
    pub session: watch::Receiver<SessionSnapshot>,
    pub commands: UnboundedReceiver<Input>,
    pub room_id: String,
    pub slot_id: String,
}

impl Pump {
    fn step(&self, input: Input) -> Option<Vec<Action>> {
        let inner = self.inner.upgrade()?;
        let actions = inner
            .machine
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .step(input, now_ms());
        inner.publish_status();
        Some(actions)
    }

    pub(super) async fn run(mut self) {
        let mut session_open = true;
        self.session.mark_changed();
        loop {
            let Some(inner) = self.inner.upgrade() else {
                return;
            };
            let wake_at = inner
                .machine
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .next_wake_ts();
            drop(inner);

            let sleep = async {
                match wake_at {
                    Some(ts) => sleep_ms(ts.saturating_sub(now_ms())).await,
                    None => std::future::pending::<()>().await,
                }
            };

            let actions = tokio::select! {
                cmd = self.commands.recv() => match cmd {
                    Some(input) => self.step(input),
                    None => return,
                },
                changed = self.session.changed(), if session_open => {
                    if changed.is_err() {
                        session_open = false;
                        Some(Vec::new())
                    } else {
                        let snapshot = self.session.borrow_and_update().clone();
                        self.step(Input::Session(snapshot))
                    }
                }
                _ = sleep => self.step(Input::Wake),
                _ = self.notify.notified() => {
                    if self.inner.upgrade().is_none() { return }
                    Some(Vec::new())
                }
            };
            let Some(actions) = actions else { return };

            let mut queue: VecDeque<Action> = actions.into();
            while let Some(action) = queue.pop_front() {
                let outcome = self.execute(action).await;
                let Some(more) = self.step(Input::Outcome(outcome)) else {
                    return;
                };
                queue.extend(more);
            }
        }
    }

    async fn execute(&self, action: Action) -> Outcome {
        let room_id = self.room_id.clone();
        match action {
            Action::ResolveTransport { member_id, intent } => {
                Outcome::TransportResolved((self.resolve_transport)(member_id, intent).await)
            }
            Action::ArmDelayedLeave { route, delay_ms } => Outcome::DelayedArmed(match route {
                Route::Sticky {
                    event_type,
                    content,
                    duration_ms,
                } => {
                    self.driver
                        .send_delayed_event(
                            room_id,
                            event_type,
                            content,
                            delay_ms,
                            Some(duration_ms),
                        )
                        .await
                }
                Route::State {
                    event_type,
                    state_key,
                    content,
                } => {
                    self.driver
                        .send_delayed_state_event(room_id, event_type, state_key, content, delay_ms)
                        .await
                }
            }),
            Action::SendMembership { route, kind } => {
                let result = match route {
                    Route::Sticky {
                        event_type,
                        content,
                        duration_ms,
                    } => {
                        self.driver
                            .send_sticky_event(room_id, event_type, content, duration_ms)
                            .await
                    }
                    Route::State {
                        event_type,
                        state_key,
                        content,
                    } => {
                        self.driver
                            .send_state_event(room_id, event_type, state_key, content)
                            .await
                    }
                };
                Outcome::MembershipSent { kind, result }
            }
            Action::RestartDelayedLeave { delay_id } => {
                Outcome::Restarted(self.driver.restart_delayed_event(room_id, delay_id).await)
            }
            Action::CancelDelayedLeave { delay_id } => {
                Outcome::Cancelled(self.driver.cancel_delayed_event(room_id, delay_id).await)
            }
            Action::Delegate { delay_id, member } => Outcome::Delegated(
                self.driver
                    .delegate_livekit_delayed_leave(DelegatedDelayedLeaveRequest {
                        room_id,
                        slot_id: self.slot_id.clone(),
                        member,
                        delay_id,
                    })
                    .await,
            ),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::super::{JoinParams, OwnIdentity, OwnMembershipManager, Status};
    use super::*;
    use crate::driver::{DriverError, SendEventResponse};
    use crate::session::{ElementCallCompat, SlotState};
    use crate::types::{RtcTransport, TransportIntent};
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    #[derive(Debug, Clone, PartialEq)]
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
            delay_ms: u64,
        },
        Restart(String),
        Cancel(String),
        Delegate(String),
    }

    #[derive(Default)]
    struct MockDriver {
        calls: Mutex<Vec<Call>>,
        /// Block every restart for this long (interleaving test).
        slow_restart_ms: u64,
        refuse_delayed: bool,
    }

    impl MockDriver {
        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl OwnMembershipDriver for MockDriver {
        async fn send_sticky_event(
            &self,
            _room_id: String,
            event_type: String,
            content: Value,
            duration_ms: u64,
        ) -> Result<SendEventResponse, DriverError> {
            self.calls.lock().unwrap().push(Call::Sticky {
                event_type,
                content,
                duration_ms,
            });
            Ok(SendEventResponse {
                event_id: Some("$e".into()),
                delay_id: None,
            })
        }
        async fn send_state_event(
            &self,
            _room_id: String,
            event_type: String,
            state_key: String,
            content: Value,
        ) -> Result<SendEventResponse, DriverError> {
            self.calls.lock().unwrap().push(Call::State {
                event_type,
                state_key,
                content,
            });
            Ok(SendEventResponse {
                event_id: Some("$s".into()),
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
            self.calls.lock().unwrap().push(Call::Delayed {
                content,
                delay_ms,
                sticky_duration_ms,
            });
            if self.refuse_delayed {
                Err(DriverError::Unsupported("M_UNRECOGNIZED".into()))
            } else {
                Ok("delay-1".into())
            }
        }
        async fn send_delayed_state_event(
            &self,
            _room_id: String,
            _event_type: String,
            state_key: String,
            _content: Value,
            delay_ms: u64,
        ) -> Result<String, DriverError> {
            self.calls.lock().unwrap().push(Call::DelayedState {
                state_key,
                delay_ms,
            });
            Ok("delay-state".into())
        }
        async fn restart_delayed_event(
            &self,
            _room_id: String,
            delay_id: String,
        ) -> Result<(), DriverError> {
            if self.slow_restart_ms > 0 {
                sleep_ms(self.slow_restart_ms).await;
            }
            self.calls.lock().unwrap().push(Call::Restart(delay_id));
            Ok(())
        }
        async fn cancel_delayed_event(
            &self,
            _room_id: String,
            delay_id: String,
        ) -> Result<(), DriverError> {
            self.calls.lock().unwrap().push(Call::Cancel(delay_id));
            Ok(())
        }
        async fn delegate_livekit_delayed_leave(
            &self,
            request: DelegatedDelayedLeaveRequest,
        ) -> Result<(), DriverError> {
            self.calls
                .lock()
                .unwrap()
                .push(Call::Delegate(request.delay_id));
            Ok(())
        }
    }

    fn resolver(calls: Arc<Mutex<Vec<&'static str>>>) -> TransportResolver {
        Box::new(move |member_id, intent| {
            let calls = calls.clone();
            Box::pin(async move {
                assert_eq!(member_id, "m-1");
                calls.lock().unwrap().push("resolve");
                match intent {
                    TransportIntent::Publish(t) => Ok(RtcTransport {
                        transport_type: t.transport_type,
                        properties: json!({ "livekit_service_url": "https://resolved" }),
                    }),
                    TransportIntent::ReceiveOnly { .. } => {
                        Err(DriverError::Other("not called for receive-only".into()))
                    }
                }
            })
        })
    }

    fn manager(
        driver: Arc<MockDriver>,
        session: watch::Receiver<SessionSnapshot>,
        compat: ElementCallCompat,
        resolves: Arc<Mutex<Vec<&'static str>>>,
    ) -> OwnMembershipManager {
        OwnMembershipManager::new(
            "!room:x".into(),
            "m.call#ROOM".into(),
            OwnIdentity {
                user_id: "@me:x".into(),
                device_id: "DEV".into(),
            },
            session,
            driver,
            compat,
            resolver(resolves),
        )
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for: {what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn params(keep_alive_ms: u64) -> JoinParams {
        JoinParams {
            sticky_duration_ms: 240_000,
            keep_alive_timeout_ms: keep_alive_ms,
            delegate_delayed_leave: true,
            ..JoinParams::new("m.call")
        }
    }

    #[test]
    fn join_runs_the_sequence_in_order_and_replies_after_the_last_action() {
        let driver = Arc::new(MockDriver::default());
        let resolves = Arc::new(Mutex::new(Vec::new()));
        let (_tx, rx) = watch::channel(SessionSnapshot {
            seeded: true,
            ..Default::default()
        });
        let m = manager(driver.clone(), rx, ElementCallCompat::Off, resolves.clone());
        let lk = RtcTransport {
            transport_type: "livekit".into(),
            properties: json!({}),
        };
        block_on(m.join(
            "m-1".into(),
            TransportIntent::Publish(lk),
            params(3_600_000),
        ))
        .unwrap();
        assert_eq!(*resolves.lock().unwrap(), vec!["resolve"]);
        let calls = driver.calls();
        assert!(matches!(
            &calls[0],
            Call::Delayed {
                delay_ms: 3_600_000,
                sticky_duration_ms: Some(240_000),
                ..
            }
        ));
        match &calls[1] {
            Call::Sticky {
                content,
                duration_ms: 240_000,
                event_type,
            } => {
                assert_eq!(event_type, "org.matrix.msc4143.rtc.member");
                assert_eq!(
                    content["transports"]["published"][0]["livekit_service_url"],
                    "https://resolved"
                );
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(calls[2], Call::Delegate("delay-1".into()));
        assert_eq!(calls.len(), 3);
        assert!(matches!(m.status(), Status::Connected(c) if c.delegation_setup_ts.is_some()));
        assert!(m.debug_snapshot()["join_event_id"].is_string());
    }

    #[test]
    fn the_heartbeat_restarts_the_delay_on_the_executor_clock_and_a_leave_does_not_interleave() {
        let driver = Arc::new(MockDriver {
            slow_restart_ms: 30,
            ..MockDriver::default()
        });
        let (_tx, rx) = watch::channel(SessionSnapshot {
            seeded: true,
            ..Default::default()
        });
        let m = manager(driver.clone(), rx, ElementCallCompat::Off, Arc::default());
        let mut status_rx = m.subscribe_status();
        block_on(m.join(
            "m-1".into(),
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                delegate_delayed_leave: false,
                ..params(60)
            },
        ))
        .unwrap();
        // 60 ms timeout → a restart every 20 ms (each taking 30 ms in the mock).
        wait_until("two restarts", || {
            driver
                .calls()
                .iter()
                .filter(|c| matches!(c, Call::Restart(_)))
                .count()
                >= 2
        });
        block_on(async { status_rx.changed().await.unwrap() });
        // Leave while a slow restart is in flight: the leave lands after it.
        block_on(m.leave(None)).unwrap();
        let calls = driver.calls();
        let leave_at = calls.iter().position(|c| matches!(c, Call::Sticky { content, .. } if content["member"]["membership"] == "leave")).unwrap();
        assert!(matches!(calls[leave_at + 1], Call::Cancel(_)));
        assert!(
            calls[leave_at + 2..]
                .iter()
                .all(|c| !matches!(c, Call::Restart(_))),
            "no restart after the leave: {calls:?}"
        );
        assert_eq!(m.status(), Status::NotJoined);
    }

    #[test]
    fn a_session_slot_close_leaves_without_a_host_call() {
        let driver = Arc::new(MockDriver::default());
        let (tx, rx) = watch::channel(SessionSnapshot {
            seeded: true,
            ..Default::default()
        });
        let m = manager(driver.clone(), rx, ElementCallCompat::Off, Arc::default());
        block_on(m.join(
            "m-1".into(),
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                delegate_delayed_leave: false,
                ..params(60_000)
            },
        ))
        .unwrap();
        tx.send(SessionSnapshot {
            seeded: true,
            slot_state: Some(SlotState::Closed),
            ..Default::default()
        })
        .unwrap();
        wait_until("left", || m.status() == Status::NotJoined);
        let calls = driver.calls();
        assert!(calls.iter().any(|c| matches!(c, Call::Sticky { content, .. } if content["leave_reason"]["code"] == "slot_closed")));
        assert!(matches!(calls.last(), Some(Call::Cancel(_))));
    }

    #[test]
    fn a_refusing_homeserver_degrades_and_state_events_use_the_state_routes() {
        let driver = Arc::new(MockDriver {
            refuse_delayed: true,
            ..MockDriver::default()
        });
        let (_tx, rx) = watch::channel(SessionSnapshot {
            seeded: true,
            ..Default::default()
        });
        let m = manager(driver.clone(), rx, ElementCallCompat::Off, Arc::default());
        block_on(m.join(
            "m-1".into(),
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                delegate_delayed_leave: false,
                ..params(60_000)
            },
        ))
        .unwrap();
        assert!(
            matches!(driver.calls()[1], Call::Sticky { duration_ms, .. } if duration_ms == super::super::DEFAULT_DEGRADED_LIFETIME_MS)
        );
        assert!(matches!(m.status(), Status::Connected(c) if !c.delayed_leave_supported));

        let driver = Arc::new(MockDriver::default());
        let (_tx, rx) = watch::channel(SessionSnapshot {
            seeded: true,
            ..Default::default()
        });
        let m = OwnMembershipManager::new(
            "!room:x".into(),
            "".into(),
            OwnIdentity {
                user_id: "@me:x".into(),
                device_id: "DEV".into(),
            },
            rx,
            driver.clone(),
            ElementCallCompat::StateEvents,
            resolver(Arc::default()),
        );
        block_on(m.join(
            "@me:x:DEV".into(),
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                delegate_delayed_leave: false,
                ..params(60_000)
            },
        ))
        .unwrap();
        let calls = driver.calls();
        assert!(
            matches!(&calls[0], Call::DelayedState { state_key, delay_ms: 60_000 } if state_key == "_@me:x_DEV_m.call")
        );
        assert!(
            matches!(&calls[1], Call::State { event_type, .. } if event_type == "org.matrix.msc3401.call.member")
        );
        block_on(m.leave(None)).unwrap();
        assert!(matches!(&driver.calls()[2], Call::State { content, .. } if *content == json!({})));
    }

    #[test]
    fn dropping_the_manager_stops_the_pump_and_releases_the_driver() {
        let driver = Arc::new(MockDriver::default());
        let (tx, rx) = watch::channel(SessionSnapshot::default());
        let m = manager(driver.clone(), rx, ElementCallCompat::Off, Arc::default());
        block_on(m.join(
            "m-1".into(),
            TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
            JoinParams {
                delegate_delayed_leave: false,
                ..params(30)
            },
        ))
        .unwrap();
        drop(m);
        wait_until("driver released", || Arc::strong_count(&driver) == 1);
        assert_eq!(tx.receiver_count(), 0);
        let n = driver.calls().len();
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(driver.calls().len(), n, "nothing is sent after the drop");
    }
}
