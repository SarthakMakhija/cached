use crate::cache::types::{TotalCapacity, TotalShards, Weight};

/// CacheWeightConfig defines the following:
/// `capacity`: is the capacity parameter for the DashMap used inside [`crate::cache::policy::cache_weight::CacheWeight`]
///             it defines the total number of keys and their weight which may be a part of the DashMap
/// `shards`:   is used as a `shard` parameter for the DashMap used inside [`crate::cache::policy::cache_weight::CacheWeight`]
/// `total_cache_weight`: defines the maximum weight of the cache and is used inside [`crate::cache::policy::cache_weight::CacheWeight`]
pub(crate) struct CacheWeightConfig {
    capacity: TotalCapacity,
    shards: TotalShards,
    total_cache_weight: Weight
}

impl CacheWeightConfig {
    pub(crate) fn new(capacity: TotalCapacity, shards: TotalShards, total_cache_weight: Weight) -> Self {
        CacheWeightConfig {
            capacity,
            shards,
            total_cache_weight
        }
    }

    pub(crate) fn capacity(&self) -> TotalCapacity { self.capacity }

    pub(crate) fn shards(&self) -> TotalShards { self.shards }

    pub(crate) fn total_cache_weight(&self) -> Weight { self.total_cache_weight }
}

#[cfg(test)]
mod tests {
    use crate::cache::policy::config::CacheWeightConfig;

    #[test]
    fn cache_weight_capacity() {
        let config = CacheWeightConfig::new(16, 4, 200);
        assert_eq!(16, config.capacity());
    }

    #[test]
    fn cache_weight_shards() {
        let config = CacheWeightConfig::new(16, 4, 200);
        assert_eq!(4, config.shards());
    }

    #[test]
    fn total_cache_weight() {
        let config = CacheWeightConfig::new(16, 4, 200);
        assert_eq!(200, config.total_cache_weight());
    }
}