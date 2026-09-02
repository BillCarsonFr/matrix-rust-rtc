//! The simulated call from matrix-js-sdk PR #5505 ("To-device rate in a
//! simulated call"), driven against the pure [`SendMachine`] with a fake
//! clock (test-only module, `cargo test --lib rotation_simulation -- --nocapture`
//! prints the per-size traffic): several call sizes, a sample of simulated clients, one flapping
//! participant, seeded jitter. What it asserts is the point of the whole
//! rotation policy: the call stays inside the contingent, nobody rotates
//! faster than the grace period, rotations are not in lockstep, and once the
//! memberships settle everything goes quiet.

use super::send_machine::{Action, SendMachine};
use super::{Participation, SendMachineConfig, fill_random};
use crate::types::{DeviceAttribution, Member};
use std::collections::HashSet;

const CONTINGENT: u32 = 3000;
const CALL_SIZES: [usize; 3] = [10, 100, 300];
const SIMULATED_CLIENTS: usize = 25;
const JOIN_INTERVAL_MS: u64 = 50;
const RAMP_UP_STEPS: usize = 50;
const CHANGES_PER_INTERVAL: u64 = 4;
const TOGGLING_INTERVALS: u64 = 6;
const QUIET_INTERVALS: u64 = 3;
const SETTLE_INTERVALS: u64 = 3;

fn grace_for(n: usize) -> u64 {
    SendMachine::new(SendMachineConfig::default(), Participation::from_member(&member(0)).unwrap(), true)
        .grace_period_ms(n)
}

struct Lcg(u64);
impl Lcg {
    fn next_jitter(&mut self) -> f64 {
        self.0 = (self.0 * 48271) % 0x7fff_ffff;
        self.0 as f64 / 0x7fff_ffff as f64 * 2.0
    }
}

fn member(i: usize) -> Member {
    Member {
        member_id: format!("m{i}"),
        user_id: format!("@user{i}:example.org"),
        device_id: Some(format!("DEVICE{i}")),
        device_attribution: DeviceAttribution::Verified,
        membership_ts: None,
        display_name: None,
        avatar_url: None,
        intent: None,
        application_type: None,
        transports: Default::default(),
    }
}

#[derive(Clone, Debug)]
struct Share {
    time: u64,
    sender: usize,
    to_device_messages: usize,
    is_rotation: bool,
}

struct Client {
    index: usize,
    machine: SendMachine,
    last_index: Option<u8>,
}

struct Sim {
    now: u64,
    rng: Lcg,
    clients: Vec<Client>,
    members: Vec<Member>,
    shares: Vec<Share>,
    phase_start: u64,
    recording: bool,
}

impl Sim {
    fn handle(&mut self, client: usize, actions: Vec<Action>) {
        let now = self.now;
        let c = &mut self.clients[client];
        for action in actions {
            if let Action::Send { key, to } = action {
                let is_rotation = c.last_index.is_some_and(|i| i != key.index);
                c.last_index = Some(key.index);
                if self.recording {
                    self.shares.push(Share {
                        time: now - self.phase_start,
                        sender: c.index,
                        to_device_messages: to.len(),
                        is_rotation,
                    });
                }
                c.machine.on_delivered(key.index, &to, now);
            }
        }
    }

    fn memberships_changed(&mut self) {
        let members = self.members.clone();
        for i in 0..self.clients.len() {
            let jitter = self.rng.next_jitter();
            let actions = self.clients[i].machine.on_session(&members, self.now, jitter);
            self.handle(i, actions);
        }
    }

    /// Run every timer that falls due up to `target`, in time order.
    fn advance_to(&mut self, target: u64) {
        loop {
            let next = self
                .clients
                .iter()
                .enumerate()
                .filter_map(|(i, c)| c.machine.next_wake_ts().map(|t| (t, i)))
                .filter(|(t, _)| *t <= target)
                .min();
            let Some((t, i)) = next else { break };
            self.now = self.now.max(t);
            let actions = self.clients[i].machine.on_wake(self.now);
            self.handle(i, actions);
        }
        self.now = target;
    }

    fn advance_by(&mut self, ms: u64) {
        let target = self.now + ms;
        self.advance_to(target);
    }
}

struct Result_ {
    n: usize,
    simulated: usize,
    grace: u64,
    toggling_ms: u64,
    toggle_interval_ms: u64,
    phase_ms: u64,
    shares: Vec<Share>,
}

fn simulate(n: usize, seed: &mut Lcg) -> Result_ {
    let grace = grace_for(n);
    let toggle_interval_ms = grace / CHANGES_PER_INTERVAL;
    let toggling_ms = grace * TOGGLING_INTERVALS;
    let quiet_ms = grace * QUIET_INTERVALS;
    let simulated = n.min(SIMULATED_CLIENTS);
    let simulated_indices: HashSet<usize> = (0..simulated).map(|i| i * n / simulated).collect();
    let flapping = Member { member_id: "flapping".into(), ..member(usize::MAX) };

    let mut sim = Sim {
        now: 0,
        rng: Lcg(seed.0),
        clients: Vec::new(),
        members: Vec::new(),
        shares: Vec::new(),
        phase_start: 0,
        recording: false,
    };
    seed.next_jitter();

    let per_step = n.div_ceil(RAMP_UP_STEPS);
    let mut joined = 0;
    while joined < n {
        for i in joined..(joined + per_step).min(n) {
            sim.members.push(member(i));
            if simulated_indices.contains(&i) {
                let machine = SendMachine::new(
                    SendMachineConfig::default(),
                    Participation::from_member(&member(i)).unwrap(),
                    true,
                );
                sim.clients.push(Client { index: i, machine, last_index: None });
            }
        }
        joined += per_step;
        sim.memberships_changed();
        sim.advance_by(JOIN_INTERVAL_MS);
    }
    sim.advance_by(SETTLE_INTERVALS * grace_for(n + 1));
    sim.phase_start = sim.now;
    sim.recording = true;

    let base = sim.members.clone();
    let mut elapsed = 0;
    while elapsed < toggling_ms {
        if sim.members.len() == base.len() {
            sim.members.push(flapping.clone());
        } else {
            sim.members = base.clone();
        }
        sim.memberships_changed();
        sim.advance_by(toggle_interval_ms);
        elapsed += toggle_interval_ms;
    }
    sim.advance_by(quiet_ms);

    Result_ {
        n,
        simulated,
        grace,
        toggling_ms,
        toggle_interval_ms,
        phase_ms: toggling_ms + quiet_ms,
        shares: sim.shares,
    }
}

fn total_messages(r: &Result_) -> f64 {
    r.shares.iter().map(|s| s.to_device_messages).sum::<usize>() as f64 * r.n as f64 / r.simulated as f64
}

fn rotation_gaps(r: &Result_) -> Vec<u64> {
    let senders: HashSet<usize> = r.shares.iter().map(|s| s.sender).collect();
    let mut gaps = Vec::new();
    for sender in senders {
        let times: Vec<u64> = r.shares.iter().filter(|s| s.is_rotation && s.sender == sender).map(|s| s.time).collect();
        gaps.extend(times.windows(2).map(|w| w[1] - w[0]));
    }
    gaps
}

fn mean(v: &[u64]) -> f64 {
    v.iter().sum::<u64>() as f64 / v.len().max(1) as f64
}

fn human(ms: u64) -> String {
    if ms >= 60_000 { format!("{:.1}min", ms as f64 / 60_000.0) } else { format!("{:.1}s", ms as f64 / 1000.0) }
}

#[test]
fn a_call_stays_inside_the_contingent_and_goes_quiet_after_the_last_change() {
    let mut seed_bytes = [0u8; 8];
    fill_random(&mut seed_bytes);
    let seed = 1 + u64::from_le_bytes(seed_bytes) % 0x7fff_fffd;
    println!("jitter seed: {seed}");
    let mut rng = Lcg(seed);

    let results: Vec<Result_> = CALL_SIZES.iter().map(|&n| simulate(n, &mut rng)).collect();

    for r in &results {
        let rotations: Vec<&Share> = r.shares.iter().filter(|s| s.is_rotation).collect();
        let gaps = rotation_gaps(r);
        let last_change = r.toggling_ms - r.toggle_interval_ms;
        let active_window = last_change + 2 * grace_for(r.n + 1) + 1;
        let total = total_messages(r);
        let per_minute = total / active_window as f64 * 60_000.0;
        let distinct: HashSet<u64> = rotations.iter().map(|s| s.time).collect();
        println!(
            "{} participants ({} simulated), grace {}, {:.1} rotations/client over {}: {total:.0} msgs = {per_minute:.0}/min of {CONTINGENT}/min; {} of {} rotations at their own instant",
            r.n, r.simulated, human(r.grace), rotations.len() as f64 / r.simulated as f64, human(r.phase_ms), distinct.len(), rotations.len()
        );

        assert!(per_minute <= CONTINGENT as f64, "{}: {per_minute} msgs/min exceeds the contingent", r.n);
        assert!(gaps.iter().all(|&g| g >= r.grace), "{}: a client rotated faster than its grace period", r.n);
        assert!(mean(&gaps) <= grace_for(r.n + 1) as f64 * 1.5, "{}: mean gap {} too idle", r.n, mean(&gaps));
        assert!(distinct.len() as f64 > rotations.len() as f64 * 0.5, "{}: rotations in lockstep", r.n);
        assert!(active_window < r.phase_ms);
        assert!(r.shares.iter().all(|s| s.time <= active_window), "{}: traffic after everything should be quiet", r.n);
    }

    let interval = |n: usize| mean(&rotation_gaps(results.iter().find(|r| r.n == n).unwrap()));
    assert!(interval(100) > interval(10) * 10.0, "100: {} vs 10: {}", interval(100), interval(10));
    assert!(interval(300) > interval(100) * 5.0, "300: {} vs 100: {}", interval(300), interval(100));
    assert!(results[2].shares.iter().any(|s| s.is_rotation), "a 300 participant call must still rotate");
}
