use std::hash::Hash;
use std::sync::Arc;

use crate::cache::buffer_event::{BufferConsumer, BufferEvent};
use crate::cache::policy::admission_policy::AdmissionPolicy;
use crate::cache::policy::config::CacheWeightConfig;
use crate::cache::stats::ConcurrentStatsCounter;
use crate::cache::types::{TotalCapacity, TotalCounters, TotalShards, Weight};

pub struct ProxyAdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    admission_policy: Arc<AdmissionPolicy<Key>>,
}

impl<Key> ProxyAdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    pub fn new(counters: TotalCounters, capacity: TotalCapacity, shards: TotalShards, total_cache_weight: Weight) -> Self {
        ProxyAdmissionPolicy {
            admission_policy: Arc::new(
                AdmissionPolicy::new(
                    counters,
                    CacheWeightConfig::new(
                        capacity,
                        shards,
                        total_cache_weight,
                    ),
                    Arc::new(ConcurrentStatsCounter::new()),
                )
            )
        }
    }
}

impl<Key> BufferConsumer for ProxyAdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    fn accept(&self, event: BufferEvent) {
        self.admission_policy.accept(event);
    }
}