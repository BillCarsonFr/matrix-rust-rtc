//! The MSC4354 sticky map, following the rules matrix-rust-sdk uses so the
//! two agree on every roster — and so that the map is *convergent*: for a
//! fixed set of events, every arrival order yields the same map. That is
//! what makes the static path order-independent.
//!
//! - key = `(sender, event_type, sticky_key)`.
//! - `end_time = min(origin_server_ts, received_ts) + min(duration_ms, 1h)`;
//!   when the server supplied `unsigned.msc4354_sticky_duration_ttl_ms`,
//!   `received_ts + ttl` is preferred (see [`end_time`]).
//! - an incoming event replaces the current entry only if
//!   `(end_time, event_id)` is strictly greater; otherwise it is ignored.
//! - a removal (content = sticky key only) is subject to the same tie-break
//!   and is kept as a tombstone until it expires — a stale join arriving
//!   after the removal must not resurrect the entry.
//! - already expired on arrival → ignored; no sticky metadata at all →
//!   ignored (admitting it would create an entry that never expires and
//!   cannot be ordered against real ones).

use std::collections::HashMap;

/// MSC4354 caps the duration a client may ask for.
pub(crate) const MAX_DURATION_MS: u64 = 3_600_000;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct StickyKey {
    pub sender: String,
    pub event_type: String,
    pub sticky_key: String,
}

impl std::fmt::Display for StickyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.sender, self.sticky_key)
    }
}

/// When an entry stops being sticky. `None` when the event carries no
/// sticky metadata at all.
pub(crate) fn end_time(
    origin_server_ts: u64,
    duration_ms: Option<u64>,
    ttl_ms: Option<u64>,
    received_ts: u64,
) -> Option<u64> {
    if let Some(ttl) = ttl_ms {
        return Some(received_ts.saturating_add(ttl));
    }
    let duration = duration_ms?.min(MAX_DURATION_MS);
    Some(origin_server_ts.min(received_ts).saturating_add(duration))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// A live value was added, replaced or removed.
    Changed,
    /// Nothing observable moved (ignored, or a tombstone bookkeeping update).
    Unchanged,
}

#[derive(Clone, Debug)]
struct Entry<V> {
    end_time: u64,
    event_id: String,
    /// `None` is a removal tombstone.
    value: Option<V>,
}

#[derive(Clone, Debug)]
pub(crate) struct Map<V> {
    entries: HashMap<StickyKey, Entry<V>>,
}

impl<V> Default for Map<V> {
    fn default() -> Self {
        Self { entries: HashMap::new() }
    }
}

impl<V> Map<V> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Apply one event: `value: None` is a removal. Returns whether a live
    /// value changed.
    pub(crate) fn upsert(
        &mut self,
        key: StickyKey,
        end_time: Option<u64>,
        event_id: &str,
        value: Option<V>,
        now: u64,
    ) -> Outcome {
        let Some(end_time) = end_time else {
            log::debug!("sticky {key}: no sticky metadata (duration or ttl); not admitted to the map");
            return Outcome::Unchanged;
        };
        if end_time <= now {
            log::debug!("sticky {key}: already expired on arrival ({}ms ago); ignored", now - end_time);
            return Outcome::Unchanged;
        }
        if let Some(current) = self.entries.get(&key)
            && (end_time, event_id) <= (current.end_time, current.event_id.as_str())
        {
            log::debug!(
                "sticky {key}: ({end_time}, {event_id}) does not supersede ({}, {}); ignored",
                current.end_time,
                current.event_id
            );
            return Outcome::Unchanged;
        }
        let had_value = self.entries.get(&key).is_some_and(|e| e.value.is_some());
        let has_value = value.is_some();
        self.entries.insert(key, Entry { end_time, event_id: event_id.to_owned(), value });
        if had_value || has_value { Outcome::Changed } else { Outcome::Unchanged }
    }

    /// Remove every entry whose `end_time <= now`; the dropped live values
    /// are returned (tombstones vanish silently).
    pub(crate) fn expire(&mut self, now: u64) -> Vec<V> {
        let due: Vec<StickyKey> =
            self.entries.iter().filter(|(_, e)| e.end_time <= now).map(|(k, _)| k.clone()).collect();
        let mut dropped = Vec::new();
        for key in due {
            if let Some(entry) = self.entries.remove(&key) {
                log::debug!("sticky {key}: expired at {}", entry.end_time);
                if let Some(value) = entry.value {
                    dropped.push(value);
                }
            }
        }
        dropped
    }

    /// The earliest `end_time` in the map (tombstones included: they have
    /// to be cleaned up too).
    pub(crate) fn next_expiry(&self) -> Option<u64> {
        self.entries.values().map(|e| e.end_time).min()
    }

    /// The live values.
    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.values().filter_map(|e| e.value.as_ref())
    }

    /// The live values with their keys and end times, for diagnostics.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&StickyKey, u64, Option<&V>)> {
        self.entries.iter().map(|(k, e)| (k, e.end_time, e.value.as_ref()))
    }

    pub(crate) fn live_len(&self) -> usize {
        self.values().count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(sender: &str, sticky_key: &str) -> StickyKey {
        StickyKey { sender: sender.into(), event_type: "m.rtc.member".into(), sticky_key: sticky_key.into() }
    }

    fn live(map: &Map<&'static str>) -> Vec<&'static str> {
        let mut v: Vec<_> = map.values().copied().collect();
        v.sort();
        v
    }

    #[test]
    fn same_key_replaces_and_different_sender_is_a_different_entry() {
        let mut map = Map::new();
        assert_eq!(map.upsert(key("@a", "k"), Some(100), "$1", Some("a1"), 0), Outcome::Changed);
        assert_eq!(map.upsert(key("@a", "k"), Some(200), "$2", Some("a2"), 0), Outcome::Changed);
        assert_eq!(map.upsert(key("@b", "k"), Some(150), "$3", Some("b1"), 0), Outcome::Changed);
        assert_eq!(live(&map), vec!["a2", "b1"]);
    }

    #[test]
    fn tie_break_is_end_time_then_event_id() {
        let mut map = Map::new();
        map.upsert(key("@a", "k"), Some(100), "$b", Some("first"), 0);
        // lower end_time → ignored
        assert_eq!(map.upsert(key("@a", "k"), Some(50), "$z", Some("older"), 0), Outcome::Unchanged);
        // equal end_time, lower event id → ignored
        assert_eq!(map.upsert(key("@a", "k"), Some(100), "$a", Some("lower"), 0), Outcome::Unchanged);
        // identical → ignored
        assert_eq!(map.upsert(key("@a", "k"), Some(100), "$b", Some("same"), 0), Outcome::Unchanged);
        assert_eq!(live(&map), vec!["first"]);
        // equal end_time, higher event id → wins
        assert_eq!(map.upsert(key("@a", "k"), Some(100), "$c", Some("higher"), 0), Outcome::Changed);
        assert_eq!(live(&map), vec!["higher"]);
        // later end_time → wins
        assert_eq!(map.upsert(key("@a", "k"), Some(101), "$a", Some("later"), 0), Outcome::Changed);
        assert_eq!(live(&map), vec!["later"]);
    }

    #[test]
    fn removal_supersedes_only_when_it_wins_the_tie_break() {
        let mut map = Map::new();
        map.upsert(key("@a", "k"), Some(100), "$1", Some("join"), 0);
        // stale removal keeps the live join
        assert_eq!(map.upsert(key("@a", "k"), Some(90), "$0", None, 0), Outcome::Unchanged);
        assert_eq!(live(&map), vec!["join"]);
        // fresh removal wins
        assert_eq!(map.upsert(key("@a", "k"), Some(110), "$2", None, 0), Outcome::Changed);
        assert!(live(&map).is_empty());
        // a stale join after the removal does not resurrect the entry
        assert_eq!(map.upsert(key("@a", "k"), Some(105), "$1b", Some("late join"), 0), Outcome::Unchanged);
        assert!(live(&map).is_empty());
        // the tombstone still expires
        assert_eq!(map.next_expiry(), Some(110));
        assert!(map.expire(110).is_empty());
        assert_eq!(map.next_expiry(), None);
    }

    #[test]
    fn expired_on_arrival_and_no_metadata_are_ignored() {
        let mut map: Map<&str> = Map::new();
        assert_eq!(map.upsert(key("@a", "k"), Some(100), "$1", Some("x"), 100), Outcome::Unchanged);
        assert_eq!(map.upsert(key("@a", "k"), Some(100), "$1", Some("x"), 200), Outcome::Unchanged);
        assert_eq!(map.upsert(key("@a", "k"), None, "$1", Some("x"), 0), Outcome::Unchanged);
        assert!(live(&map).is_empty());
        assert_eq!(map.next_expiry(), None);
    }

    #[test]
    fn end_time_math() {
        // min(origin_server_ts, received_ts) + duration
        assert_eq!(end_time(1_000, Some(500), None, 2_000), Some(1_500));
        assert_eq!(end_time(3_000, Some(500), None, 2_000), Some(2_500));
        // duration clamped to one hour
        assert_eq!(end_time(1_000, Some(10 * MAX_DURATION_MS), None, 1_000), Some(1_000 + MAX_DURATION_MS));
        // unsigned ttl preferred when present
        assert_eq!(end_time(1_000, Some(500), Some(42), 2_000), Some(2_042));
        assert_eq!(end_time(1_000, None, Some(42), 2_000), Some(2_042));
        // no metadata
        assert_eq!(end_time(1_000, None, None, 2_000), None);
    }

    #[test]
    fn expire_removes_exactly_the_due_entries_and_reports_them() {
        let mut map = Map::new();
        map.upsert(key("@a", "1"), Some(100), "$1", Some("a"), 0);
        map.upsert(key("@b", "2"), Some(200), "$2", Some("b"), 0);
        map.upsert(key("@c", "3"), Some(300), "$3", Some("c"), 0);
        assert_eq!(map.next_expiry(), Some(100));
        assert!(map.expire(99).is_empty());
        let mut dropped = map.expire(200);
        dropped.sort();
        assert_eq!(dropped, vec!["a", "b"]);
        assert_eq!(live(&map), vec!["c"]);
        assert_eq!(map.next_expiry(), Some(300));
    }

    /// Every permutation of a fixed event set yields the same map.
    #[test]
    fn order_independence() {
        // (sender, key, end_time, event_id, value or removal)
        let events: Vec<(&str, &str, u64, &str, Option<&'static str>)> = vec![
            ("@a", "k1", 100, "$a1", Some("a1")),
            ("@a", "k1", 200, "$a2", Some("a2")),
            ("@a", "k1", 150, "$a3", None),
            ("@b", "k1", 120, "$b1", Some("b1")),
            ("@b", "k1", 120, "$b2", Some("b2")),
            ("@b", "k1", 130, "$b0", None),
            ("@c", "k2", 50, "$c1", Some("expired")),
            ("@d", "k3", 300, "$d1", Some("d1")),
            ("@d", "k3", 300, "$d0", Some("d0")),
        ];
        let apply = |order: &[usize]| {
            let mut map = Map::new();
            for &i in order {
                let (s, k, t, id, v) = events[i];
                map.upsert(key(s, k), Some(t), id, v, 60);
            }
            let mut snapshot: Vec<(String, u64, Option<&str>)> =
                map.iter().map(|(k, t, v)| (k.to_string(), t, v.copied())).collect();
            snapshot.sort();
            snapshot
        };
        let reference = apply(&(0..events.len()).collect::<Vec<_>>());
        // Deterministic pseudo-random permutations (LCG), no external crate.
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        for _ in 0..25 {
            let mut order: Vec<usize> = (0..events.len()).collect();
            for i in (1..order.len()).rev() {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let j = (seed >> 33) as usize % (i + 1);
                order.swap(i, j);
            }
            assert_eq!(apply(&order), reference, "order {order:?}");
        }
        // And the reference is what the rules say it should be.
        let expected: Vec<(String, u64, Option<&str>)> = vec![
            ("@a/k1".into(), 200, Some("a2")),
            ("@b/k1".into(), 130, None),
            ("@d/k3".into(), 300, Some("d1")),
        ];
        assert_eq!(reference, expected);
    }
}
