use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_utils::CachePadded;

const TOTAL_STATS: usize = 6;

#[repr(usize)]
pub(crate) enum StatsType {
    CacheHits = 0,
    CacheMisses = 1,
    KeysAdded = 2,
    KeysDeleted = 3,
    KeysEvicted = 4,
    KeysRejected = 5,
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

    pub(crate) fn add(&self, stats_type: StatsType, count: u64) {
        self.entries[stats_type as usize].0.fetch_add(count, Ordering::AcqRel);
    }

    pub(crate) fn get(&self, stats_type: StatsType) -> u64 {
        self.entries[stats_type as usize].0.load(Ordering::Acquire)
    }

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

    pub(crate) fn hit_ratio(&self) -> f64 {
        let hits = self.hits();
        let misses = self.misses();
        if hits == 0 || misses == 0 {
            return 0.0;
        }
        (hits as f64) / (hits + misses) as f64
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::stats::{ConcurrentStatsCounter, StatsType};

    #[test]
    fn increase_cache_hits() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add(StatsType::CacheHits, 1);
        stats_counter.add(StatsType::CacheHits, 1);

        assert_eq!(2, stats_counter.hits());
    }

    #[test]
    fn increase_cache_misses() {
        let stats_counter = ConcurrentStatsCounter::new();
        stats_counter.add(StatsType::CacheMisses, 1);
        stats_counter.add(StatsType::CacheMisses, 1);

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
}