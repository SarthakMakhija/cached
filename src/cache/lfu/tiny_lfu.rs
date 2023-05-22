use log::{debug, info};

use crate::cache::lfu::doorkeeper::DoorKeeper;
use crate::cache::lfu::frequency_counter::FrequencyCounter;
use crate::cache::types::{DoorKeeperCapacity, FrequencyEstimate, KeyHash, TotalCounters};

/// TinyLFU maintains determines the key access frequencies.
/// It contains a `FrequencyCounter` and a `DoorKeeper` where `FrequencyCounter` is an implementation of
/// count-min sketch data structure and `DoorKeeper` is an implementation of bloom filter.
/// More on these data structures is available on the following links:
    /// https://tech-lessons.in/blog/count_min_sketch/
    /// https://tech-lessons.in/blog/bloom_filter/
/// Both these data structures work on the hash of the key. All the methods in these abstraction accept `KeyHash` or a `Vec<KeyHash>`.
pub(crate) struct TinyLFU {
    key_access_frequency: FrequencyCounter,
    door_keeper: DoorKeeper,
    total_increments: u64,
    reset_counters_at: u64,
}

impl TinyLFU {
    pub(crate) fn new(counters: TotalCounters) -> TinyLFU {
        let tiny_lfu = TinyLFU {
            key_access_frequency: FrequencyCounter::new(counters),
            door_keeper: DoorKeeper::new(counters as DoorKeeperCapacity, 0.01),
            total_increments: 0,
            reset_counters_at: counters,
        };
        info!(
            "Initialized TinyLFU with total counters {} ,bloom filter capacity {} and reset_counters_at {}",
            counters, counters, counters);

        tiny_lfu
    }

    pub(crate) fn increment_access(&mut self, key_hashes: Vec<KeyHash>) {
        key_hashes.iter().for_each(|key_hash| self.increment_access_for(*key_hash));
    }

    /// Estimates the frequency of the given key_hash.
    /// If the doorkeeper already contains the key, the access is incremented by 1.
    /// [TinyLFU](https://dgraph.io/blog/refs/TinyLFU%20-%20A%20Highly%20Efficient%20Cache%20Admission%20Policy.pdf)
    /// The key will be added to the doorkeeper if is is accessed.
    /// That means, if the same key is accessed twice, it will be added to the doorkeeper on the first access.
    pub(crate) fn estimate(&self, key_hash: KeyHash) -> FrequencyEstimate {
        let mut estimate = self.key_access_frequency.estimate(key_hash);
        if self.door_keeper.has(&key_hash) {
            estimate += 1;
        }
        estimate
    }

    pub(crate) fn clear(&mut self) {
        debug!("Clearing tinyLFU");
        self.total_increments = 0;
        self.key_access_frequency.clear();
        self.door_keeper.clear();
    }

    /// Increments the access for the given key_hash.
    /// The first access of the key will result in an entry in the doorkeeper and
    /// subsequent accesses will find the key in the doorkeeper and hence increment the access in the `FrequencyCounter`.
    fn increment_access_for(&mut self, key_hash: KeyHash) {
        let added = self.door_keeper.add_if_missing(&key_hash);
        if !added {
            self.key_access_frequency.increment(key_hash);
        }
        self.total_increments += 1;
        if self.total_increments >= self.reset_counters_at {
            self.reset();
        }
    }

    fn reset(&mut self) {
        debug!("Resetting tinyLFU");
        self.total_increments = 0;
        self.key_access_frequency.reset();
        self.door_keeper.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::lfu::tiny_lfu::TinyLFU;

    #[test]
    fn increment_frequency_access_for_keys() {
        let mut tiny_lfu = TinyLFU::new(10);
        tiny_lfu.increment_access(vec![10, 10, 10, 20]);

        assert_eq!(3, tiny_lfu.estimate(10));
        assert_eq!(1, tiny_lfu.estimate(20));
    }

    #[test]
    fn increment_frequency_access_for_keys_if_doorkeeper_already_has_some_keys() {
        let mut tiny_lfu = TinyLFU::new(10);
        tiny_lfu.door_keeper.add_if_missing(&10);

        tiny_lfu.increment_access(vec![10, 10, 10, 20]);

        assert_eq!(4, tiny_lfu.estimate(10));
        assert_eq!(1, tiny_lfu.estimate(20));
    }

    #[test]
    fn total_increments() {
        let mut tiny_lfu = TinyLFU::new(10);
        tiny_lfu.increment_access(vec![10, 10, 10, 20]);

        assert_eq!(4, tiny_lfu.total_increments);
    }

    #[test]
    fn reset() {
        let mut tiny_lfu = TinyLFU::new(2);
        tiny_lfu.increment_access(vec![10, 10]);

        assert_eq!(0, tiny_lfu.total_increments);
    }
}