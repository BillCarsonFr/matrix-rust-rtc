//! Our own media key: when to mint a new one, whom to send it to, when to
//! start encrypting with it. The rotation policy of matrix-js-sdk
//! PR #5505 ("Encryption-Key rotation slow down") as a **pure state
//! machine**: no clock, no timer, no randomness of its own — `now` and the
//! jitter draw are inputs, sends and key switches are outputs
//! ([`Action`]s), and the next deadline is a query ([`next_wake_ts`]).
//! `pump.rs` supplies the time. See README.md for the rules and their origin.
//!
//! [`next_wake_ts`]: SendMachine::next_wake_ts

use super::{MediaKey, Participation, SendMachineConfig};
use crate::types::Member;

/// What the pump has to do after a step.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Send `key` to these participations (one to-device batch).
    Send { key: MediaKey, to: Vec<Participation> },
    /// Start encrypting with this key now.
    UseOwnKey(MediaKey),
}

#[derive(Clone, Debug)]
struct OutboundKey {
    key: MediaKey,
    /// Participations that actually received this key.
    shared_with: Vec<Participation>,
    /// We encrypt with it (the `use_key_delay` has passed).
    in_use: bool,
}

pub struct SendMachine {
    config: SendMachineConfig,
    own: Option<Participation>,
    manage_media_keys: bool,
    /// Recipients as of the last session update (everyone but us).
    current: Vec<Participation>,
    /// Session size as of the last update, including us (`N` in the grace
    /// formula).
    participant_count: usize,
    /// The key being distributed (and used, once `in_use`).
    outbound: Option<OutboundKey>,
    /// The key still encrypting our media while `outbound` propagates.
    superseded: Option<OutboundKey>,
    next_index: u8,
    blocked_until_ts: u64,
    /// A rotation is owed at this instant.
    wake_ts: Option<u64>,
    /// Switch to `outbound` at this instant.
    use_ts: Option<u64>,
    lifetime_deadline_ts: Option<u64>,
    last_rotation_ts: u64,
    initial_keys_distributed: bool,
}

impl SendMachine {
    /// Lives for exactly one participation: `own` is our fresh membership,
    /// `manage_media_keys` the negotiated decision. Dropping it is leaving.
    pub fn new(config: SendMachineConfig, own: Participation, manage_media_keys: bool) -> Self {
        Self {
            config,
            own: Some(own),
            manage_media_keys,
            current: Vec::new(),
            participant_count: 0,
            outbound: None,
            superseded: None,
            next_index: 0,
            blocked_until_ts: 0,
            wake_ts: None,
            use_ts: None,
            lifetime_deadline_ts: None,
            last_rotation_ts: 0,
            initial_keys_distributed: false,
        }
    }

    pub fn config(&self) -> &SendMachineConfig {
        &self.config
    }

    pub fn own(&self) -> Option<&Participation> {
        self.own.as_ref()
    }

    /// PR #5505: `60_000 * N * (N - 1) / contingent`. No floor: a rotation
    /// that falls due while the previous key is still propagating waits for
    /// that key's switch instead (see [`Self::on_wake`]).
    pub fn grace_period_ms(&self, participant_count: usize) -> u64 {
        let n = participant_count as f64;
        let raw = 60_000.0 * n * (n - 1.0).max(0.0)
            / (self.config.shared_per_minute_to_device_contingent.max(1) as f64);
        raw as u64
    }

    /// The session changed. `jitter` is uniform in `[0, 2)`.
    pub fn on_session(&mut self, members: &[Member], now: u64, jitter: f64) -> Vec<Action> {
        let Some(own) = self.own.clone() else { return Vec::new() };
        if !self.manage_media_keys {
            return Vec::new();
        }
        self.participant_count = members.len();
        self.current = members
            .iter()
            .filter(|m| m.member_id != own.member().member_id)
            // Our own stale membership (same user *and* device, older
            // member id) is never a recipient; other devices of ours are.
            .filter(|m| {
                !(m.user_id == own.member().user_id && m.device_id.as_deref() == Some(own.device_id()))
            })
            .filter_map(|m| match Participation::from_member(m) {
                Some(p) => Some(p),
                None => {
                    log::warn!(
                        "member {} of {} has no device id; cannot receive a media key",
                        m.member_id,
                        m.user_id
                    );
                    None
                }
            })
            .collect();

        let mut actions = Vec::new();
        let Some(outbound) = &self.outbound else {
            // First key: everyone gets it, we use it at once, and the
            // jittered block starts. No wake-up: nothing is owed.
            let key = self.mint(now);
            self.outbound = Some(OutboundKey { key: key.clone(), shared_with: Vec::new(), in_use: true });
            self.last_rotation_ts = now;
            self.blocked_until_ts = now + Self::jittered(self.grace_period_ms(self.participant_count), jitter);
            self.lifetime_deadline_ts = self.config.max_key_lifetime_ms.map(|l| now + l);
            actions.push(Action::UseOwnKey(key.clone()));
            if self.current.is_empty() {
                self.initial_keys_distributed = true;
            } else {
                actions.push(Action::Send { key, to: self.current.clone() });
            }
            return actions;
        };

        let left = outbound
            .shared_with
            .iter()
            .any(|shared| !self.current.iter().any(|c| c.same_join(shared)));
        let joined: Vec<Participation> = self
            .current
            .iter()
            .filter(|c| !outbound.shared_with.iter().any(|s| s.same_join(c)))
            .cloned()
            .collect();
        if !left && joined.is_empty() {
            return actions;
        }
        if !joined.is_empty() {
            // Joiners get the current key right away, whatever else happens.
            actions.push(Action::Send { key: outbound.key.clone(), to: joined });
        }
        if now >= self.blocked_until_ts {
            // Not blocked, but every other client saw this change at the same
            // moment: spread the rotations with a jittered block.
            self.blocked_until_ts = now + Self::jittered(self.grace_period_ms(self.participant_count), jitter);
        }
        // Blocked (or just made so): the rotation lands when the block ends.
        // Idempotent — later changes inside the block neither move the
        // deadline nor add one.
        self.wake_ts = Some(self.blocked_until_ts);
        actions
    }

    /// The deadline from [`Self::next_wake_ts`] arrived (or any later time).
    pub fn on_wake(&mut self, now: u64) -> Vec<Action> {
        let mut actions = Vec::new();
        if self.own.is_none() || !self.manage_media_keys {
            return actions;
        }
        if self.use_ts.is_some_and(|t| now >= t) {
            self.use_ts = None;
            if let Some(outbound) = &mut self.outbound {
                outbound.in_use = true;
                self.superseded = None;
                actions.push(Action::UseOwnKey(outbound.key.clone()));
            }
        }
        let rotation_owed = self.wake_ts.is_some_and(|t| now >= t);
        let lifetime_over = self.lifetime_deadline_ts.is_some_and(|t| now >= t);
        if rotation_owed || lifetime_over {
            if let Some(use_ts) = self.use_ts {
                // The previous key is still propagating: never mint over a
                // key that has not come into use. Rotate right after the
                // switch (same `on_wake` call, since the switch runs first).
                self.wake_ts = Some(use_ts);
                return actions;
            }
            if rotation_owed {
                self.wake_ts = None;
            }
            if self.outbound.is_none() {
                self.lifetime_deadline_ts = self.config.max_key_lifetime_ms.map(|l| now + l);
                return actions;
            }
            self.rotate(now, &mut actions);
        }
        actions
    }

    fn rotate(&mut self, now: u64, actions: &mut Vec<Action>) {
        let key = self.mint(now);
        let alone = self.current.is_empty();
        let previous = self.outbound.take();
        // Keep whichever key our media is *actually* encrypted with until the
        // new one is in use; a never-used pending key holds nothing.
        match previous {
            Some(p) if p.in_use => self.superseded = Some(p),
            _ => {}
        }
        self.outbound = Some(OutboundKey { key: key.clone(), shared_with: Vec::new(), in_use: alone });
        self.last_rotation_ts = now;
        // An unjittered block: the next rotation waits a full grace period,
        // and is scheduled only if a change lands in it.
        self.blocked_until_ts = now + self.grace_period_ms(self.participant_count);
        self.lifetime_deadline_ts = self.config.max_key_lifetime_ms.map(|l| now + l);
        self.use_ts = None;
        if alone {
            // Nobody holds the old key: switch at once. (The PR returns
            // before signalling here and keeps encrypting with the old key.)
            self.superseded = None;
            actions.push(Action::UseOwnKey(key));
        } else {
            actions.push(Action::Send { key, to: self.current.clone() });
        }
    }

    /// A batch for `key_index` finished; `served` are the recipients that
    /// actually got it. A rotated key is used `use_key_delay_ms` after at
    /// least one recipient has it; if nobody has, the next change resends it.
    pub fn on_delivered(&mut self, key_index: u8, served: &[Participation], now: u64) {
        let Some(outbound) = &mut self.outbound else { return };
        if outbound.key.index != key_index {
            return;
        }
        for s in served {
            if !outbound.shared_with.iter().any(|x| x.same_join(s)) {
                outbound.shared_with.push(s.clone());
            }
        }
        if !served.is_empty() {
            self.initial_keys_distributed = true;
            if !outbound.in_use && self.use_ts.is_none() {
                self.use_ts = Some(now + self.config.use_key_delay_ms);
            }
        }
    }

    /// Earliest of: owed rotation, pending key switch, lifetime cap.
    pub fn next_wake_ts(&self) -> Option<u64> {
        if self.own.is_none() || !self.manage_media_keys {
            return None;
        }
        [self.wake_ts, self.use_ts, self.lifetime_deadline_ts]
            .into_iter()
            .flatten()
            .min()
    }

    /// Participations that hold the key our media is encrypted with but are
    /// no longer in the session — "left, possibly still listening".
    pub fn left_members_with_keys(&self) -> Vec<Member> {
        let in_use = match (&self.outbound, &self.superseded) {
            (Some(o), _) if o.in_use => Some(o),
            (_, Some(s)) => Some(s),
            (Some(o), None) => Some(o),
            (None, None) => None,
        };
        let Some(in_use) = in_use else { return Vec::new() };
        in_use
            .shared_with
            .iter()
            .filter(|s| !self.current.iter().any(|c| c.same_join(s)))
            .map(|s| s.member().clone())
            .collect()
    }

    pub fn is_settled(&self) -> bool {
        self.wake_ts.is_none() && self.use_ts.is_none()
    }

    pub fn last_rotation_ts(&self) -> u64 {
        self.last_rotation_ts
    }

    pub fn has_distributed_initial_keys(&self) -> bool {
        self.initial_keys_distributed
    }

    pub fn current_key(&self) -> Option<&MediaKey> {
        self.outbound.as_ref().map(|o| &o.key)
    }

    fn jittered(grace_ms: u64, jitter: f64) -> u64 {
        (grace_ms as f64 * jitter.clamp(0.0, 2.0)) as u64
    }

    fn mint(&mut self, now: u64) -> MediaKey {
        let mut bytes = vec![0u8; 32];
        super::fill_random(&mut bytes);
        let index = self.next_index;
        self.next_index = self.next_index.wrapping_add(1);
        MediaKey { key: bytes, index, creation_ts_ms: now }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DeviceAttribution;

    /// The PR's `TEST_CONTINGENT`: grace(2) = 2 s, grace(3) = 6 s, grace(4) = 12 s.
    const CONTINGENT: u32 = 60;
    const USE_DELAY: u64 = 1_000;

    fn config() -> SendMachineConfig {
        SendMachineConfig {
            shared_per_minute_to_device_contingent: CONTINGENT,
            use_key_delay_ms: USE_DELAY,
            ..Default::default()
        }
    }

    fn member(user: &str, device: &str) -> Member {
        Member {
            member_id: format!("m-{user}-{device}"),
            user_id: user.into(),
            device_id: Some(device.into()),
            device_attribution: DeviceAttribution::Verified,
            membership_ts: None,
            display_name: None,
            avatar_url: None,
            intent: None,
        }
    }

    fn p(m: &Member) -> Participation {
        Participation::from_member(m).unwrap()
    }

    fn own() -> Member {
        member("@own:x", "OWN")
    }

    fn joined(config: SendMachineConfig) -> SendMachine {
        SendMachine::new(config, p(&own()), true)
    }

    fn sends(actions: &[Action]) -> Vec<(u8, Vec<String>)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send { key, to } => {
                    Some((key.index, to.iter().map(|p| p.member().member_id.clone()).collect()))
                }
                _ => None,
            })
            .collect()
    }

    fn used(actions: &[Action]) -> Vec<u8> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::UseOwnKey(k) => Some(k.index),
                _ => None,
            })
            .collect()
    }

    fn ids(members: &[Member]) -> Vec<String> {
        members.iter().map(|m| m.member_id.clone()).collect()
    }

    /// Deliver every Send in `actions` successfully.
    fn deliver_all(m: &mut SendMachine, actions: &[Action], now: u64) {
        for a in actions {
            if let Action::Send { key, to } = a {
                m.on_delivered(key.index, to, now);
            }
        }
    }

    #[test]
    fn grace_period_grows_with_participant_count() {
        for (contingent, expected) in [(2000, [1.2, 4.9, 19.9]), (3000, [0.8, 3.3, 13.2]), (5000, [0.5, 1.9, 7.9])] {
            let cfg = SendMachineConfig { shared_per_minute_to_device_contingent: contingent, ..Default::default() };
            let m = joined(cfg);
            for (n, minutes) in [50, 100, 200].into_iter().zip(expected) {
                let got = m.grace_period_ms(n) as f64 / 60_000.0;
                assert!((got - minutes).abs() < 0.1, "{contingent}/{n}: {got} vs {minutes}");
            }
        }
    }

    #[test]
    fn grace_period_has_no_floor() {
        // default contingent: 0 ms alone, 40 ms for two, 120 ms for three
        let m = joined(SendMachineConfig::default());
        assert_eq!((m.grace_period_ms(1), m.grace_period_ms(2), m.grace_period_ms(3)), (0, 40, 120));
    }

    #[test]
    fn a_rotation_owed_while_a_key_switch_is_pending_waits_for_the_switch() {
        let (bob, carl) = (member("@bob:x", "B"), member("@carl:x", "C"));
        let mut m = joined(SendMachineConfig::default()); // grace(3) = 120 ms < use delay
        let a = m.on_session(&[own(), bob.clone(), carl.clone()], 0, 1.0);
        deliver_all(&mut m, &a, 0);
        // carl leaves at 1000: block 120 ms, rotation at 1120
        m.on_session(&[own(), bob.clone()], 1000, 1.0);
        assert_eq!(m.next_wake_ts(), Some(1040));
        let a = m.on_wake(1040);
        assert_eq!(sends(&a), vec![(1, ids(&[bob.clone()]))]);
        deliver_all(&mut m, &a, 1040);
        assert_eq!(m.next_wake_ts(), Some(2040), "switch pending");
        // dave joins at 1100 while key 1 propagates (block from the rotation
        // ended at 1080): gets key 1, jittered block of the 3-session -> 1220
        let dave = member("@dave:x", "D");
        let a = m.on_session(&[own(), bob.clone(), dave.clone()], 1100, 1.0);
        assert_eq!(sends(&a), vec![(1, ids(&[dave.clone()]))]);
        deliver_all(&mut m, &a, 1100);
        assert_eq!(m.next_wake_ts(), Some(1220));
        // ...but a rotation never lands on a key that is not in use yet
        let a = m.on_wake(1220);
        assert!(a.is_empty());
        assert_eq!(m.next_wake_ts(), Some(2040), "deferred to the switch");
        let a = m.on_wake(2040);
        assert_eq!(used(&a), vec![1], "switch first");
        assert_eq!(sends(&a), vec![(2, ids(&[bob, dave]))], "then the owed rotation");
    }

    #[test]
    fn first_key_is_sent_to_everyone_used_immediately_and_schedules_nothing() {
        let (bob, carl) = (member("@bob:x", "B"), member("@carl:x", "C"));
        let mut m = joined(config());
        let actions = m.on_session(&[own(), bob.clone(), carl.clone()], 0, 0.9);
        assert_eq!(used(&actions), vec![0]);
        assert_eq!(sends(&actions), vec![(0, ids(&[bob, carl]))]);
        assert_eq!(m.next_wake_ts(), None);
        assert!(!m.has_distributed_initial_keys());
        deliver_all(&mut m, &actions, 0);
        assert!(m.has_distributed_initial_keys());
        assert!(m.is_settled());
    }

    #[test]
    fn a_leave_in_a_settled_call_rotates_after_grace_times_jitter_to_the_remaining_members_only() {
        // PR: "Should do a full rotation after a jitter delay when a user leaves"
        let (bob, bob2, carl) = (member("@bob:x", "B"), member("@bob:x", "B2"), member("@carl:x", "C"));
        let jitter = 0.9 * 2.0; // the PR pins Math.random() to 0.9
        let mut m = joined(config());
        let all = [own(), bob.clone(), bob2.clone(), carl.clone()];
        let a = m.on_session(&all, 0, jitter);
        deliver_all(&mut m, &a, 0);
        let grace4 = m.grace_period_ms(4);
        let block_end = (grace4 as f64 * jitter) as u64;

        // Carl leaves after the first block expired.
        let t1 = block_end;
        let a = m.on_session(&[own(), bob.clone(), bob2.clone()], t1, jitter);
        assert!(sends(&a).is_empty(), "nothing to send: no joiner");
        let grace3 = m.grace_period_ms(3);
        let due = t1 + (grace3 as f64 * jitter) as u64;
        assert_eq!(m.next_wake_ts(), Some(due));
        assert_eq!(ids(&m.left_members_with_keys()), ids(&[carl]));

        assert!(m.on_wake(due - 1).is_empty());
        let a = m.on_wake(due);
        assert_eq!(sends(&a), vec![(1, ids(&[bob, bob2]))]);
        assert!(used(&a).is_empty(), "not used before the delay");
        // Carl still holds the key in use until we switch.
        assert_eq!(m.left_members_with_keys().len(), 1);

        deliver_all(&mut m, &a, due);
        assert_eq!(m.next_wake_ts(), Some(due + USE_DELAY));
        let a = m.on_wake(due + USE_DELAY);
        assert_eq!(used(&a), vec![1]);
        assert!(m.left_members_with_keys().is_empty());
        assert!(m.is_settled());
        assert_eq!(m.last_rotation_ts(), due);
    }

    #[test]
    fn changes_during_a_block_neither_move_the_deadline_nor_earn_a_second_delay() {
        // PR: "Should rotate consecutively without any extra delay when the
        // membership keeps changing inside the grace period" (jitter factor pinned to 1)
        let (bob, bob2, carl, dave, eve) = (
            member("@bob:x", "B"),
            member("@bob:x", "B2"),
            member("@carl:x", "C"),
            member("@dave:x", "D"),
            member("@eve:x", "E"),
        );
        let mut m = joined(config());
        let a = m.on_session(&[own(), bob.clone(), bob2.clone()], 0, 1.0);
        deliver_all(&mut m, &a, 0);
        let grace3 = m.grace_period_ms(3); // block from key 0 ends at 6 s

        let a = m.on_session(&[own(), bob.clone(), bob2.clone(), carl.clone()], 100, 1.0);
        assert_eq!(sends(&a), vec![(0, ids(&[carl.clone()]))]);
        deliver_all(&mut m, &a, 100);
        let a = m.on_session(&[own(), bob.clone(), bob2.clone(), carl.clone(), dave.clone()], 200, 1.0);
        assert_eq!(sends(&a), vec![(0, ids(&[dave.clone()]))]);
        deliver_all(&mut m, &a, 200);
        assert_eq!(m.next_wake_ts(), Some(grace3), "deadline anchored to key 0");

        assert!(m.on_wake(grace3 - 1).is_empty());
        let a = m.on_wake(grace3);
        assert_eq!(sends(&a), vec![(1, ids(&[bob.clone(), bob2.clone(), carl.clone(), dave.clone()]))]);
        deliver_all(&mut m, &a, grace3);
        let a = m.on_wake(grace3 + USE_DELAY);
        assert_eq!(used(&a), vec![1]);
        assert!(m.is_settled());

        // Eve joins: current key at once, rotation deferred to the end of the
        // unjittered block started by key 1 (grace of the 5-participant session).
        let t = grace3 + USE_DELAY + 100;
        let a = m.on_session(&[own(), bob, bob2, carl, dave, eve.clone()], t, 1.0);
        assert_eq!(sends(&a), vec![(1, ids(&[eve]))]);
        deliver_all(&mut m, &a, t);
        // block was set at rotation time with N=5 (own+4) as counted then
        assert_eq!(m.next_wake_ts(), Some(grace3 + m.grace_period_ms(5)));
        let a = m.on_wake(grace3 + m.grace_period_ms(5));
        assert_eq!(sends(&a)[0].0, 2);
        assert_eq!(sends(&a)[0].1.len(), 5);
    }

    #[test]
    fn several_changes_while_blocked_produce_one_rotation_and_a_not_blocked_change_starts_a_jittered_block() {
        let (bob, carl, dave) = (member("@bob:x", "B"), member("@carl:x", "C"), member("@dave:x", "D"));
        let mut m = joined(config());
        let a = m.on_session(&[own(), bob.clone()], 0, 0.0); // jitter 0: block ends now
        deliver_all(&mut m, &a, 0);
        // Not blocked any more at t=10: a jittered block starts from now.
        let a = m.on_session(&[own(), bob.clone(), carl.clone()], 10, 0.5);
        deliver_all(&mut m, &a, 10);
        let due = 10 + (m.grace_period_ms(3) as f64 * 0.5) as u64;
        assert_eq!(m.next_wake_ts(), Some(due));
        // Blocked: more changes, same deadline.
        let a = m.on_session(&[own(), bob.clone(), carl.clone(), dave.clone()], 20, 1.9);
        deliver_all(&mut m, &a, 20);
        let a = m.on_session(&[own(), bob.clone(), dave.clone()], 30, 1.9);
        assert!(sends(&a).is_empty());
        assert_eq!(m.next_wake_ts(), Some(due));
        let a = m.on_wake(due);
        assert_eq!(sends(&a), vec![(1, ids(&[bob, dave]))]);
        assert_eq!(m.next_wake_ts(), None, "a scheduled wake schedules nothing");
    }

    #[test]
    fn no_change_and_no_wake_produces_no_action() {
        let bob = member("@bob:x", "B");
        let mut m = joined(config());
        let a = m.on_session(&[own(), bob.clone()], 0, 0.5);
        deliver_all(&mut m, &a, 0);
        assert!(m.on_session(&[own(), bob], 5, 0.5).is_empty());
        assert!(m.on_wake(5).is_empty());
        assert_eq!(m.next_wake_ts(), None);
    }

    #[test]
    fn a_changed_membership_ts_counts_as_left_and_joined() {
        let mut bob = member("@bob:x", "B");
        bob.membership_ts = Some(1);
        let mut m = joined(config());
        let a = m.on_session(&[own(), bob.clone()], 0, 0.5);
        deliver_all(&mut m, &a, 0);
        bob.membership_ts = Some(2);
        let a = m.on_session(&[own(), bob.clone()], 10, 0.5);
        assert_eq!(sends(&a), vec![(0, ids(&[bob]))], "rejoin is served the current key");
        assert!(m.next_wake_ts().is_some(), "and the old participation left: rotation owed");
    }

    #[test]
    fn recipient_selection() {
        let mut no_device = member("@nod:x", "X");
        no_device.device_id = None;
        let own_other_device = member("@own:x", "OWN2");
        let mut own_stale = member("@own:x", "OWN");
        own_stale.member_id = "stale".into();
        let mut m = joined(config());
        let a = m.on_session(&[own(), no_device, own_other_device.clone(), own_stale], 0, 0.5);
        assert_eq!(sends(&a), vec![(0, ids(&[own_other_device]))]);
    }

    #[test]
    fn only_served_recipients_enter_shared_with_and_failures_are_retried_with_the_same_key() {
        let (bob, carl) = (member("@bob:x", "B"), member("@carl:x", "C"));
        let mut m = joined(config());
        let a = m.on_session(&[own(), bob.clone(), carl.clone()], 0, 0.5);
        // only bob got it
        m.on_delivered(0, &[p(&bob)], 0);
        // no change in the session, but carl is still owed the key
        let a2 = m.on_session(&[own(), bob.clone(), carl.clone()], 10, 0.5);
        assert_eq!(sends(&a2), vec![(0, ids(&[carl.clone()]))]);
        // carl (never served) leaving causes no rotation
        m.on_session(&[own(), bob.clone()], 20, 0.5);
        let _ = a;
        assert_eq!(m.left_members_with_keys(), Vec::<Member>::new());
    }

    #[test]
    fn an_unreachable_member_leaving_causes_no_rotation() {
        let (bob, carl) = (member("@bob:x", "B"), member("@carl:x", "C"));
        let mut m = joined(config());
        let a = m.on_session(&[own(), bob.clone(), carl.clone()], 0, 0.0);
        m.on_delivered(0, &[p(&bob)], 0);
        let _ = a;
        let a = m.on_session(&[own(), bob], 10, 0.0);
        assert!(a.is_empty());
        assert_eq!(m.next_wake_ts(), None);
    }

    #[test]
    fn a_rotated_key_nobody_received_is_resent_not_used() {
        let bob = member("@bob:x", "B");
        let mut m = joined(config());
        let a = m.on_session(&[own(), bob.clone()], 0, 0.0);
        deliver_all(&mut m, &a, 0);
        let carl = member("@carl:x", "C");
        let a = m.on_session(&[own(), bob.clone(), carl.clone()], 10, 0.0);
        deliver_all(&mut m, &a, 10);
        let a = m.on_wake(10);
        assert_eq!(sends(&a)[0].0, 1);
        m.on_delivered(1, &[], 10); // total failure
        assert_eq!(m.next_wake_ts(), None);
        let a = m.on_session(&[own(), bob, carl], 20, 0.0);
        assert_eq!(sends(&a)[0].0, 1, "same key again");
        assert_eq!(sends(&a)[0].1.len(), 2);
    }

    #[test]
    fn rotating_alone_switches_at_once() {
        let bob = member("@bob:x", "B");
        let mut m = joined(config());
        let a = m.on_session(&[own(), bob], 0, 0.0);
        deliver_all(&mut m, &a, 0);
        m.on_session(&[own()], 10, 0.0);
        let a = m.on_wake(10);
        assert_eq!(used(&a), vec![1]);
        assert!(sends(&a).is_empty());
        assert!(m.is_settled());
    }

    #[test]
    fn key_index_wraps_from_255_to_0() {
        let mut m = joined(config());
        m.next_index = 255;
        let a = m.on_session(&[own()], 0, 0.0);
        assert_eq!(used(&a), vec![255]);
        m.on_session(&[own(), member("@bob:x", "B")], 1, 0.0);
        let a = m.on_wake(1);
        assert_eq!(sends(&a)[0].0, 0);
    }

    #[test]
    fn optional_max_key_lifetime_forces_a_rotation_in_a_quiet_call() {
        let mut cfg = config();
        cfg.max_key_lifetime_ms = Some(3_600_000);
        let bob = member("@bob:x", "B");
        let mut m = joined(cfg);
        let a = m.on_session(&[own(), bob.clone()], 0, 0.5);
        deliver_all(&mut m, &a, 0);
        assert_eq!(m.next_wake_ts(), Some(3_600_000));
        let a = m.on_wake(3_600_000);
        assert_eq!(sends(&a), vec![(1, ids(&[bob]))]);
        deliver_all(&mut m, &a, 3_600_000);
        assert_eq!(m.next_wake_ts(), Some(3_600_000 + USE_DELAY));
    }

    #[test]
    fn not_managing_keys_does_nothing() {
        let mut m = SendMachine::new(config(), p(&own()), false);
        assert!(m.on_session(&[own(), member("@bob:x", "B")], 0, 0.5).is_empty());
        assert_eq!(m.next_wake_ts(), None);
    }
}
