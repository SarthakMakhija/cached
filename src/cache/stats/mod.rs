use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_utils::CachePadded;

const TOTAL_STATS: usize = 10;

#[repr(usize)]
pub(crate) enum StatsType {
    CacheHits = 0,
    CacheMisses = 1,
    KeysAdded = 2,
    KeysDeleted = 3,
    KeysEvicted = 4,
    KeysRejected = 5,
    WeightAdded = 6,
    WeightRemoved = 7,
    AccessAdded = 8,
    AccessDropped = 9,
}

#[repr(transparent)]
#[derive(Debug)]
struct Counter(CachePadded<AtomicU64>);

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

    pub(crate) fn hits(&self) -> u64 {
        self.get(StatsType::CacheHits)
    }

    pub(crate) fn misses(&self) -> u64 {
        self.get(StatsType::CacheMisses)
    }

    pub(crate) fn keys_added(&self) -> u64 {
        self.get(StatsType::KeysAdded)
    }

    pub(crate) fn keys_deleted(&self) -> u64 {
        self.get(StatsType::KeysDeleted)
    }

    pub(crate) fn keys_evicted(&self) -> u64 {
        self.get(StatsType::KeysEvicted)
    }

    pub(crate) fn keys_rejected(&self) -> u64 {
        self.get(StatsType::KeysRejected)
    }

    pub(crate) fn weight_added(&self) -> u64 {
        self.get(StatsType::WeightAdded)
    }

    pub(crate) fn weight_removed(&self) -> u64 { self.get(StatsType::WeightRemoved) }

    pub(crate) fn access_added(&self) -> u64 { self.get(StatsType::AccessAdded) }

    pub(crate) fn access_dropped(&self) -> u64 { self.get(StatsType::AccessDropped) }

    pub(crate) fn hit_ratio(&self) -> f64 {
        let hits = self.hits();
        let misses = self.misses();
        if hits == 0 || misses == 0 {
            return 0.0;
        }
        (hits as f64) / (hits + misses) as f64
    }

    //TODO: Confirm ordering
    fn add(&self, stats_type: StatsType, count: u64) {
        self.entries[stats_type as usize].0.fetch_add(count, Ordering::AcqRel);
    }

    //TODO: Confirm ordering
    fn get(&self, stats_type: StatsType) -> u64 {
        self.entries[stats_type as usize].0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
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
        stats_counter.add(StatsType::KeysAdded, 1);
        stats_counter.add(StatsType::KeysAdded, 1);

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
    fn increase_keys_evicted() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add(StatsType::KeysEvicted, 1);
        stats_counter.add(StatsType::KeysEvicted, 1);

        assert_eq!(2, stats_counter.keys_evicted());
    }

    #[test]
    fn increase_keys_rejected() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add(StatsType::KeysRejected, 1);
        stats_counter.add(StatsType::KeysRejected, 1);

        assert_eq!(2, stats_counter.keys_rejected());
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
}