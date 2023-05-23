use crate::cache::lfu::frequency_counter::FrequencyCounter;
use crate::cache::types::{FrequencyEstimate, KeyHash, TotalCounters};

/// Proxy representation of the `crate::cache::lfu::frequency_counter::FrequencyCounter`.
/// FrequencyCounter provides behaviors to increment the key access and get an access frequency estimate for
/// a key.
/// ProxyFrequencyCounter provides the same behaviors and ends up delegating to the FrequencyCounter object.
/// ProxyFrequencyCounter is used in `benchmark benches/benchmarks/frequency_counter.rs`
#[cfg(feature = "bench_testable")]
pub struct ProxyFrequencyCounter {
    frequency_counter: FrequencyCounter
}

#[cfg(feature = "bench_testable")]
impl ProxyFrequencyCounter {
    #[cfg(not(tarpaulin_include))]
    pub fn new(counters: TotalCounters) -> Self {
        ProxyFrequencyCounter {
            frequency_counter: FrequencyCounter::new(counters)
        }
    }

    #[cfg(not(tarpaulin_include))]
    pub fn increment(&mut self, key_hash: KeyHash) {
        self.frequency_counter.increment(key_hash);
    }

    #[cfg(not(tarpaulin_include))]
    pub fn estimate(&self, key_hash: KeyHash) -> FrequencyEstimate {
        self.frequency_counter.estimate(key_hash)
    }
}