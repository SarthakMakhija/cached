use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_utils::CachePadded;

const TOTAL_STATS: usize = 10;

/// Defines various stats that are measured in the cache.
#[repr(usize)]
#[non_exhaustive]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum StatsType {
    /// Defines the number of hits for the keys
    CacheHits = 0,
    /// Defines the number of misses for the keys
    CacheMisses = 1,
    /// Defines the number of keys added
    KeysAdded = 2,
    /// Defines the number of keys deleted
    KeysDeleted = 3,
    /// Defines the number of keys updated
    KeysUpdated = 4,
    /// Defines the number of keys rejected
    KeysRejected = 5,
    /// Defines the total weight added
    WeightAdded = 6,
    /// Defines the total weight removed
    WeightRemoved = 7,
    /// Defines the total number of gets registered in the frequency counter
    AccessAdded = 8,
    /// Defines the total number of gets dropped
    AccessDropped = 9,
}

impl StatsType {
    const VALUES: [Self; TOTAL_STATS] = [
        Self::CacheHits,
        Self::CacheMisses,
        Self::KeysAdded,
        Self::KeysDeleted,
        Self::KeysUpdated,
        Self::KeysRejected,
        Self::WeightAdded,
        Self::WeightRemoved,
        Self::AccessAdded,
        Self::AccessDropped
    ];
}

/// StatsSummary is view representation of various stats represented by [`StatsType`].
#[derive(Debug, PartialEq)]
pub struct StatsSummary {
    pub stats_by_type: HashMap<StatsType, u64>,
    pub hit_ratio: f64,
}

impl StatsSummary {
    pub(crate) fn new(stats_by_type: HashMap<StatsType, u64>, hit_ratio: f64) -> Self {
        StatsSummary {
            stats_by_type,
            hit_ratio,
        }
    }

    /// Returns an Option&lt;u64&gt; counter corresponding to the [`StatsType`].
    pub fn get(&self, stats_type: &StatsType) -> Option<u64> {
        self.stats_by_type.get(stats_type).copied()
    }
}

#[repr(transparent)]
#[derive(Debug)]
struct Counter(CachePadded<AtomicU64>);

/// ConcurrentStatsCounter measures various stats defined by [`StatsType`].
/// ConcurrentStatsCounter is represented as an array of entries where each entry is an instance of type [`Counter`].
/// Each instance of [`Counter`] is a [`crossbeam_utils::CachePadded`] AtomicU64, to avoid false sharing.
/// ConcurrentStatsCounter provides methods:
    /// to increase the counters corresponding to various stats and
    /// to get the values for these stats
pub(crate) struct ConcurrentStatsCounter {
    entries: [Counter; TOTAL_STATS],
}

impl ConcurrentStatsCounter {
    pub(crate) fn new() -> Self {
        ConcurrentStatsCounter {
            entries: (0..TOTAL_STATS)
                .map(|_index| Counter(CachePadded::new(AtomicU64::new(0))))
                .collect::<Vec<Counter>>()
                .try_into().unwrap()
        }
    }

    pub(crate) fn found_a_hit(&self) { self.add(StatsType::CacheHits, 1); }

    pub(crate) fn found_a_miss(&self) { self.add(StatsType::CacheMisses, 1); }

    pub(crate) fn add_weight(&self, delta: u64) { self.add(StatsType::WeightAdded, delta); }

    pub(crate) fn remove_weight(&self, delta: u64) { self.add(StatsType::WeightRemoved, delta); }

    pub(crate) fn add_access(&self, delta: u64) { self.add(StatsType::AccessAdded, delta); }

    pub(crate) fn drop_access(&self, delta: u64) { self.add(StatsType::AccessDropped, delta); }

    pub(crate) fn add_key(&self) { self.add(StatsType::KeysAdded, 1); }

    pub(crate) fn reject_key(&self) { self.add(StatsType::KeysRejected, 1); }

    pub(crate) fn delete_key(&self) { self.add(StatsType::KeysDeleted, 1); }

    pub(crate) fn update_key(&self) { self.add(StatsType::KeysUpdated, 1); }

    pub(crate) fn hits(&self) -> u64 {
        self.get(&StatsType::CacheHits)
    }

    pub(crate) fn misses(&self) -> u64 {
        self.get(&StatsType::CacheMisses)
    }

    pub(crate) fn keys_added(&self) -> u64 {
        self.get(&StatsType::KeysAdded)
    }

    pub(crate) fn keys_deleted(&self) -> u64 {
        self.get(&StatsType::KeysDeleted)
    }

    pub(crate) fn keys_rejected(&self) -> u64 { self.get(&StatsType::KeysRejected) }

    pub(crate) fn keys_updated(&self) -> u64 { self.get(&StatsType::KeysUpdated) }

    pub(crate) fn weight_added(&self) -> u64 {
        self.get(&StatsType::WeightAdded)
    }

    pub(crate) fn weight_removed(&self) -> u64 { self.get(&StatsType::WeightRemoved) }

    pub(crate) fn access_added(&self) -> u64 { self.get(&StatsType::AccessAdded) }

    pub(crate) fn access_dropped(&self) -> u64 { self.get(&StatsType::AccessDropped) }

    pub(crate) fn hit_ratio(&self) -> f64 {
        let hits = self.hits();
        let misses = self.misses();
        if hits == 0 || misses == 0 {
            return 0.0;
        }
        (hits as f64) / (hits + misses) as f64
    }

    pub(crate) fn clear(&self) {
        for entry in &self.entries {
            entry.0.store(0, Ordering::Release);
        }
    }

    pub(crate) fn summary(&self) -> StatsSummary {
        let mut stats_by_type = HashMap::new();
        for stats_type in StatsType::VALUES.iter().copied() {
            stats_by_type.insert(stats_type, self.get(&stats_type));
        }
        StatsSummary::new(stats_by_type, self.hit_ratio())
    }

    fn add(&self, stats_type: StatsType, count: u64) {
        self.entries[stats_type as usize].0.fetch_add(count, Ordering::AcqRel);
    }

    fn get(&self, stats_type: &StatsType) -> u64 {
        self.entries[*stats_type as usize].0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::cache::stats::{ConcurrentStatsCounter, StatsType};

    #[test]
    fn increase_cache_hits() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.found_a_hit();
        stats_counter.found_a_hit();

        assert_eq!(2, stats_counter.hits());
    }

    #[test]
    fn increase_cache_misses() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.found_a_miss();
        stats_counter.found_a_miss();

        assert_eq!(2, stats_counter.misses());
    }

    #[test]
    fn increase_keys_added() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add_key();
        stats_counter.add_key();

        assert_eq!(2, stats_counter.keys_added());
    }

    #[test]
    fn increase_keys_deleted() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add(StatsType::KeysDeleted, 1);
        stats_counter.add(StatsType::KeysDeleted, 1);

        assert_eq!(2, stats_counter.keys_deleted());
    }

    #[test]
    fn increase_keys_rejected() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.reject_key();
        stats_counter.reject_key();

        assert_eq!(2, stats_counter.keys_rejected());
    }

    #[test]
    fn increase_keys_updated() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.update_key();
        stats_counter.update_key();

        assert_eq!(2, stats_counter.keys_updated());
    }

    #[test]
    fn hit_ratio_as_zero() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add(StatsType::CacheMisses, 1);

        assert_eq!(0.0, stats_counter.hit_ratio());
    }

    #[test]
    fn hit_ratio() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add(StatsType::CacheMisses, 3);
        stats_counter.add(StatsType::CacheHits, 1);

        assert_eq!(0.25, stats_counter.hit_ratio());
    }

    #[test]
    fn weight_added() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add_weight(1);
        stats_counter.add_weight(1);

        assert_eq!(2, stats_counter.weight_added());
    }

    #[test]
    fn weight_removed() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add_weight(1);
        stats_counter.add_weight(1);

        stats_counter.remove_weight(1);
        stats_counter.remove_weight(1);

        assert_eq!(2, stats_counter.weight_removed());
    }

    #[test]
    fn access_added() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add_access(1);
        stats_counter.add_access(1);

        assert_eq!(2, stats_counter.access_added());
    }

    #[test]
    fn access_dropped() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.drop_access(1);
        stats_counter.drop_access(1);

        assert_eq!(2, stats_counter.access_dropped());
    }

    #[test]
    fn clear() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add_key();
        stats_counter.add_key();
        assert_eq!(2, stats_counter.keys_added());

        stats_counter.clear();
        assert_eq!(0, stats_counter.keys_added());
    }

    #[test]
    fn stats_summary_with_all_stats_as_one() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.found_a_hit();
        stats_counter.found_a_miss();
        stats_counter.add_key();
        stats_counter.delete_key();
        stats_counter.update_key();
        stats_counter.reject_key();
        stats_counter.add_weight(1);
        stats_counter.remove_weight(1);
        stats_counter.add_access(1);
        stats_counter.drop_access(1);

        let summary = stats_counter.summary();
        let mut stats_by_type = HashMap::new();
        for stats_type in StatsType::VALUES.iter().copied() {
            stats_by_type.insert(stats_type, 1);
        }

        assert_eq!(0.5, summary.hit_ratio);
        assert_eq!(stats_by_type, summary.stats_by_type);
    }

    #[test]
    fn stats_summary() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.found_a_hit();
        stats_counter.found_a_miss();
        stats_counter.delete_key();
        stats_counter.update_key();
        stats_counter.remove_weight(1);
        stats_counter.add_access(1);
        stats_counter.drop_access(2);

        let summary = stats_counter.summary();
        let mut stats_by_type = HashMap::new();
        stats_by_type.insert(StatsType::CacheHits, 1);
        stats_by_type.insert(StatsType::CacheMisses, 1);
        stats_by_type.insert(StatsType::KeysAdded, 0);
        stats_by_type.insert(StatsType::KeysDeleted, 1);
        stats_by_type.insert(StatsType::KeysUpdated, 1);
        stats_by_type.insert(StatsType::KeysRejected, 0);
        stats_by_type.insert(StatsType::WeightAdded, 0);
        stats_by_type.insert(StatsType::WeightRemoved, 1);
        stats_by_type.insert(StatsType::AccessAdded, 1);
        stats_by_type.insert(StatsType::AccessDropped, 2);

        assert_eq!(0.5, summary.hit_ratio);
        assert_eq!(stats_by_type, summary.stats_by_type);
    }
}

#[cfg(test)]
mod stats_summary_tests {
    use std::collections::HashMap;
    use crate::cache::stats::{StatsSummary, StatsType};

    #[test]
    fn missing_stats() {
        let summary = StatsSummary::new(HashMap::new(), 0.0);
        assert_eq!(None, summary.get(&StatsType::CacheHits));
    }

    #[test]
    fn stats_value_by_its_type() {
        let mut stats_by_type = HashMap::new();
        stats_by_type.insert(StatsType::CacheHits, 1);
        stats_by_type.insert(StatsType::KeysAdded, 5);

        let summary = StatsSummary::new(stats_by_type, 1.0);
        assert_eq!(1, summary.get(&StatsType::CacheHits).unwrap());
        assert_eq!(5, summary.get(&StatsType::KeysAdded).unwrap());
    }
}