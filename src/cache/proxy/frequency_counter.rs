use crate::cache::lfu::frequency_counter::FrequencyCounter;
use crate::cache::types::{FrequencyEstimate, KeyHash, TotalCounters};

#[cfg(feature = "bench_testable")]
pub struct ProxyFrequencyCounter {
    frequency_counter: FrequencyCounter
}

#[cfg(feature = "bench_testable")]
impl ProxyFrequencyCounter {
    pub fn new(counters: TotalCounters) -> Self {
        ProxyFrequencyCounter {
            frequency_counter: FrequencyCounter::new(counters)
        }
    }

    pub fn increment(&mut self, key_hash: KeyHash) {
        self.frequency_counter.increment(key_hash);
    }

    pub fn estimate(&self, key_hash: KeyHash) -> FrequencyEstimate {
        self.frequency_counter.estimate(key_hash)
    }
}