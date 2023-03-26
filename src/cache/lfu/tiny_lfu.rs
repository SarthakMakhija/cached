use crate::cache::lfu::frequency_counter::FrequencyCounter;

pub(crate) struct TinyLFU {
    key_access_frequency: FrequencyCounter,
    total_increments: u64,
    reset_counters_at: u64,
}

impl TinyLFU {
    pub(crate) fn new(counters: u64) -> TinyLFU {
        TinyLFU {
            key_access_frequency: FrequencyCounter::new(counters),
            total_increments: 0,
            reset_counters_at: counters,
        }
    }

    pub(crate) fn add(&mut self, key_hashes: Vec<u64>) {
        key_hashes.iter().for_each(|key_hash| self.increment_access_for(*key_hash));
    }

    pub(crate) fn estimate(&self, key_hash: u64) -> u8 {
        //TODO: Doorkeeper
        self.key_access_frequency.estimate(key_hash)
    }

    fn increment_access_for(&mut self, key_hash: u64) {
        //TODO: Doorkeeper
        self.key_access_frequency.increment(key_hash);
        self.total_increments += 1;
        if self.total_increments >= self.reset_counters_at {
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.total_increments = 0;
        self.key_access_frequency.reset();
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::lfu::tiny_lfu::TinyLFU;

    #[test]
    fn increment_frequency_access_for_keys() {
        let mut tiny_lfu = TinyLFU::new(10);
        tiny_lfu.add(vec![10, 10, 10, 20]);

        assert_eq!(3, tiny_lfu.estimate(10));
        assert_eq!(1, tiny_lfu.estimate(20));
    }

    #[test]
    fn total_increments() {
        let mut tiny_lfu = TinyLFU::new(10);
        tiny_lfu.add(vec![10, 10, 10, 20]);

        assert_eq!(4, tiny_lfu.total_increments);
    }

    #[test]
    fn reset() {
        let mut tiny_lfu = TinyLFU::new(2);
        tiny_lfu.add(vec![10, 10]);

        assert_eq!(0, tiny_lfu.total_increments);
    }
}