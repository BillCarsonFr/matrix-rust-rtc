//! The one task of the encryption machine. Everything that awaits or waits
//! lives here: the session `watch`, the inbound to-device stream, the
//! deadline timer, the driver sends. State transitions themselves are the
//! pure [`SendMachine`](super::send_machine::SendMachine) and
//! [`InboundKeys`](super::inbound::InboundKeys), stepped under a short lock
//! that is never held across an `await`, and callbacks fire after the lock
//! is released (a host callback may call back into the machine).
//!
//! Sends are awaited inside the loop, so session changes that land during a
//! batch coalesce into the next iteration — `watch` keeps only the latest
//! value — which is the PR's "one rollout at a time, once more afterwards".

use super::send_machine::Action;
use super::{Machine, MachineInner, matrix_encryption_event as wire};
use crate::driver::{ToDeviceDriver, ToDeviceMessage, ToDeviceRecipient};
use crate::executor::{now_ms, sleep_ms};
use crate::session::SessionSnapshot;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Notify, watch};

pub(super) struct Pump {
    pub inner: Weak<MachineInner>,
    pub notify: Arc<Notify>,
    pub driver: Arc<dyn ToDeviceDriver>,
    pub session: watch::Receiver<SessionSnapshot>,
    pub to_device: UnboundedReceiver<ToDeviceMessage>,
}

impl Pump {
    pub(super) async fn run(mut self) {
        let mut to_device_open = true;
        // The initial snapshot counts as a change.
        self.session.mark_changed();
        loop {
            let Some(inner) = self.inner.upgrade() else { return };
            let wake_at = inner.state.lock().unwrap().send.next_wake_ts();
            drop(inner);

            let sleep = async {
                match wake_at {
                    Some(ts) => sleep_ms(ts.saturating_sub(now_ms())).await,
                    None => std::future::pending::<()>().await,
                }
            };

            let actions = tokio::select! {
                changed = self.session.changed() => {
                    if changed.is_err() {
                        log::info!("session closed; encryption pump stopping");
                        return;
                    }
                    let snapshot = self.session.borrow_and_update().clone();
                    let Some(inner) = self.inner.upgrade() else { return };
                    Machine::on_session(&inner, snapshot, now_ms())
                }
                _ = sleep => {
                    let Some(inner) = self.inner.upgrade() else { return };
                    Machine::on_wake(&inner, now_ms())
                }
                _ = self.notify.notified() => {
                    // Only the Machine's Drop notifies: exit if it is gone.
                    if self.inner.upgrade().is_none() { return }
                    Vec::new()
                }
                msg = self.to_device.recv(), if to_device_open => {
                    match msg {
                        Some(msg) => {
                            let Some(inner) = self.inner.upgrade() else { return };
                            Machine::on_to_device(&inner, msg, now_ms());
                        }
                        None => to_device_open = false,
                    }
                    Vec::new()
                }
            };

            for action in actions {
                let Some(inner) = self.inner.upgrade() else { return };
                match action {
                    Action::UseOwnKey(key) => Machine::use_own_key(&inner, key),
                    Action::Send { key, to } => {
                        let (event_type, content) = {
                            let state = inner.state.lock().unwrap();
                            let Some(own) = state.send.own() else { continue };
                            (
                                wire::outbound_event_type(state.compat),
                                wire::build_content(
                                    state.compat,
                                    &state.room_id,
                                    &state.slot_id,
                                    &own.member().member_id,
                                    own.device_id(),
                                    &key,
                                    now_ms(),
                                ),
                            )
                        };
                        let recipients: Vec<ToDeviceRecipient> =
                            to.iter().map(|p| p.recipient()).collect();
                        drop(inner);
                        let served = match self
                            .driver
                            .send_to_device(recipients, event_type.to_owned(), content)
                            .await
                        {
                            Ok(deliveries) => to
                                .iter()
                                .filter(|p| {
                                    deliveries.iter().any(|d| {
                                        d.error.is_none()
                                            && d.recipient.user_id == p.member().user_id
                                            && d.recipient.device_id == p.device_id()
                                    })
                                })
                                .cloned()
                                .collect(),
                            Err(e) => {
                                log::warn!("media key batch (index {}) not sent: {e}", key.index);
                                Vec::new()
                            }
                        };
                        let Some(inner) = self.inner.upgrade() else { return };
                        Machine::on_delivered(&inner, key.index, &served, now_ms());
                    }
                }
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::driver::{DriverError, ToDeviceDelivery, ToDeviceSendDriver};
    use crate::encryption::{EncryptionConfig, Machine, MediaKeyChange, SendMachineConfig};
    use crate::session::ElementCallCompat;
    use crate::types::{DeviceAttribution, EventOrigin, Member};
    use async_trait::async_trait;
    use serde_json::Value;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

    struct MockDriver {
        sent: Mutex<Vec<(Vec<ToDeviceRecipient>, String, Value)>>,
        inbound: Mutex<Option<UnboundedSender<ToDeviceMessage>>>,
    }

    #[async_trait]
    impl ToDeviceSendDriver for MockDriver {
        async fn send_to_device(
            &self,
            recipients: Vec<ToDeviceRecipient>,
            event_type: String,
            content: Value,
        ) -> Result<Vec<ToDeviceDelivery>, DriverError> {
            let deliveries = recipients
                .iter()
                .map(|r| ToDeviceDelivery { recipient: r.clone(), error: None })
                .collect();
            self.sent.lock().unwrap().push((recipients, event_type, content));
            Ok(deliveries)
        }
    }

    impl ToDeviceDriver for MockDriver {
        fn subscribe_to_device_events(&self) -> UnboundedReceiver<ToDeviceMessage> {
            let (tx, rx) = unbounded_channel();
            *self.inbound.lock().unwrap() = Some(tx);
            rx
        }
    }

    fn member(user: &str, device: &str) -> Member {
        Member {
            member_id: format!("m-{user}"),
            user_id: user.into(),
            device_id: Some(device.into()),
            device_attribution: DeviceAttribution::Verified,
            membership_ts: None,
            display_name: None,
            avatar_url: None,
            intent: None,
        }
    }

    fn snapshot(members: Vec<Member>) -> SessionSnapshot {
        SessionSnapshot { members, negotiated_encryption: Some(true), ..Default::default() }
    }

    fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for: {what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn session_changes_wakes_and_inbound_keys_flow_through_the_pump() {
        let driver = Arc::new(MockDriver { sent: Mutex::new(Vec::new()), inbound: Mutex::new(None) });
        let (session_tx, session_rx) = watch::channel(snapshot(Vec::new()));
        let changes: Arc<Mutex<Vec<MediaKeyChange>>> = Arc::default();
        let changes_cb = changes.clone();
        let own = member("@own:x", "OWN");
        let bob = member("@bob:x", "BOB");
        let machine = Machine::new(
            driver.clone(),
            "!room:x".into(),
            "m.call#".into(),
            ElementCallCompat::Off,
            session_rx,
            &own,
            true,
            EncryptionConfig::default(),
            SendMachineConfig {
                shared_per_minute_to_device_contingent: 60,
                use_key_delay_ms: 30,
                ..Default::default()
            },
            Box::new(move |_, change| changes_cb.lock().unwrap().push(change.clone())),
        )
        .unwrap();

        // 1. A session with bob: our first key goes to bob's device and is in use.
        session_tx.send(snapshot(vec![own.clone(), bob.clone()])).unwrap();
        wait_until("first key sent", || !driver.sent.lock().unwrap().is_empty());
        {
            let sent = driver.sent.lock().unwrap();
            let (recipients, event_type, content) = &sent[0];
            assert_eq!(recipients, &[ToDeviceRecipient { user_id: "@bob:x".into(), device_id: "BOB".into() }]);
            assert_eq!(event_type, "org.matrix.msc4143.rtc.encryption_key");
            assert_eq!(content["member_id"], "m-@own:x");
            assert_eq!(content["media_key"]["index"], 0);
        }
        wait_until("own key 0 in map", || changes.lock().unwrap().iter().any(|c| c.member_id == "m-@own:x" && c.key.index == 0));
        assert_eq!(machine.key_map()["m-@own:x"].len(), 1);
        wait_until("initial keys distributed", || matches!(machine.status(),
            super::super::Status::Joining { has_distributed_initial_keys: true, .. }));

        // 2. Bob's key arrives over to-device and lands in the map.
        let content = wire::build_content(ElementCallCompat::Off, "!room:x", "", "m-@bob:x", "BOB",
            &super::super::MediaKey { key: vec![9; 32], index: 0, creation_ts_ms: 0 }, 0);
        driver.inbound.lock().unwrap().as_ref().unwrap().send(ToDeviceMessage {
            event_type: "m.rtc.encryption_key".into(),
            sender: "@bob:x".into(),
            content,
            origin: EventOrigin::Encrypted { sender_device_id: Some("BOB".into()) },
            sender_cross_signed: Some(true),
        }).unwrap();
        wait_until("bob's key in map", || machine.key_map().contains_key("m-@bob:x"));
        assert!(matches!(machine.status(), super::super::Status::Connected { .. }));

        // 3. Bob leaves: he is "left with keys" until the timer-driven
        //    rotation; alone, the new key is used at once.
        session_tx.send(snapshot(vec![own.clone()])).unwrap();
        wait_until("own key 1 after the wake", || changes.lock().unwrap().iter().any(|c| c.member_id == "m-@own:x" && c.key.index == 1));
        assert!(matches!(machine.status(),
            super::super::Status::Connected { left_members_with_keys, fully_settled: true, .. } if left_members_with_keys.is_empty()));
        // No batch was sent for the rotation: nobody to send to.
        assert_eq!(driver.sent.lock().unwrap().len(), 1);

        // 4. Dropping the machine is leaving: the pump exits and releases the
        //    driver; a later session change reaches nobody.
        drop(machine);
        wait_until("pump released the driver", || Arc::strong_count(&driver) == 1);
        assert_eq!(session_tx.receiver_count(), 0, "the session subscription is gone");
        assert!(session_tx.send(snapshot(vec![own, bob])).is_err());
        assert_eq!(driver.sent.lock().unwrap().len(), 1);
    }
}
