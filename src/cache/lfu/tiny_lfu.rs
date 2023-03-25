use crate::cache::lfu::count_min_sketch::CountMinSketch;

pub(crate) struct TinyLFU {
    key_access_frequency: CountMinSketch,
}

impl TinyLFU {
    pub(crate) fn new(counters: u64) -> TinyLFU {
        return TinyLFU {
            key_access_frequency: CountMinSketch::new(counters)
        };
    }

    pub(crate) fn add(&mut self, key_hashes: Vec<u64>) {
        key_hashes.iter().for_each(|key_hash| self.increment_access_for(*key_hash));
    }

    pub(crate) fn estimate(&self, key_hash: u64) -> u8 {
        //TODO: Doorkeeper
        return self.key_access_frequency.estimate(key_hash);
    }

    fn increment_access_for(&mut self, key_hash: u64) {
        //TODO: Doorkeeper
        self.key_access_frequency.increment(key_hash);
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
}