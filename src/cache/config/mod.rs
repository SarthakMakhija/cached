use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use crate::cache::clock::{ClockType, SystemClock};
use crate::cache::config::weight_calculation::Calculation;
use crate::cache::errors::Errors;
use crate::cache::expiration::config::TTLConfig;
use crate::cache::policy::config::CacheWeightConfig;
use crate::cache::pool::{BufferSize, PoolSize};
use crate::cache::types::{IsTimeToLiveSpecified, KeyHash, TotalCapacity, TotalCounters, TotalShards, Weight};
pub(crate) mod weight_calculation;

/// Defines the function for calculating the hash of the incoming key. This hash is used to put the in `crate::cache::policy::cache_weight::CacheWeight`
/// By default, DefaultHasher is used that uses SipHasher13 as the hash function.
pub type HashFn<Key> = dyn Fn(&Key) -> KeyHash + Send + Sync;

/// Defines the function for calculating the weight of the incoming key/value pair.
/// Default is the `perform` function defined in `crate::cache::config::weight_calculation::Calculation`
pub type WeightCalculationFn<Key, Value> = dyn Fn(&Key, &Value, IsTimeToLiveSpecified) -> Weight + Send + Sync;

/// Each put, put_or_update, delete results in a command to `crate::cache::command::command_executor::CommandExecutor`.
/// CommandExecutor reads from an mpsc channel and COMMAND_BUFFER_SIZE defines the size (/buffer) of the command channel that
/// is used by CommandExecutor.
const COMMAND_BUFFER_SIZE: usize = 32 * 1024;

/// Pool represents a ring-buffer that is used to buffer the gets for various keys.
/// Default pool size is 32
const ACCESS_POOL_SIZE: PoolSize = PoolSize(32);

/// Each buffer inside the Pool is a Vec<KeyHash>.
/// Default capacity of the buffer is 64
const ACCESS_BUFFER_SIZE: BufferSize = BufferSize(64);

/// Determines the number of shards to use in the [`dashmap::DashMap`] inside `crate::cache::store::Store`
/// Default is 256
const SHARDS: usize = 256;

/// Determines the frequency at which the `crate::cache::expiration::TTLTicker` runs.
/// Default is every 5 seconds.
const TTL_TICK_DURATION: Duration = Duration::from_secs(5);

/// Defines the config parameters for Cached.
pub struct Config<Key, Value>
    where Key: Hash + 'static,
          Value: 'static {
    pub key_hash_fn: Box<HashFn<Key>>,
    pub weight_calculation_fn: Box<WeightCalculationFn<Key, Value>>,
    pub clock: ClockType,
    pub counters: TotalCounters,
    pub command_buffer_size: usize,
    pub total_cache_weight: Weight,

    pub(crate) access_pool_size: PoolSize,
    pub(crate) access_buffer_size: BufferSize,
    pub(crate) capacity: TotalCapacity,
    pub(crate) shards: TotalShards,

    ttl_tick_duration: Duration,
}

impl<Key, Value> Config<Key, Value>
    where Key: Hash + 'static,
          Value: 'static {
    /// Creates a new instance of TTLConfig
    pub(crate) fn ttl_config(&self) -> TTLConfig {
        TTLConfig::new(self.shards, self.ttl_tick_duration, self.clock.clone_box())
    }

    /// Creates a new instance of CacheWeightConfig
    pub(crate) fn cache_weight_config(&self) -> CacheWeightConfig {
        CacheWeightConfig::new(self.capacity, self.shards, self.total_cache_weight)
    }
}

/// Convenient builder that allows creating an instance of Config
pub struct ConfigBuilder<Key, Value>
    where Key: Hash + 'static,
          Value: 'static {
    key_hash_fn: Box<HashFn<Key>>,
    weight_calculation_fn: Box<WeightCalculationFn<Key, Value>>,
    clock: ClockType,
    counters: TotalCounters,
    capacity: TotalCapacity,
    command_buffer_size: usize,
    access_pool_size: PoolSize,
    access_buffer_size: BufferSize,
    total_cache_weight: Weight,
    shards: TotalShards,
    ttl_tick_duration: Duration,
}

impl<Key, Value> ConfigBuilder<Key, Value>
    where Key: Hash + 'static,
          Value: 'static {

    /// Create a new instance of ConfigBuilder with counters, capacity and cache_weight.
    /// `counters` are used to determine the access (or frequency) of each key.
    ///
    /// If you expect your cache to hold 100_000 elements, then counters should be 10 times 100_000 to
    /// get a close estimate of the access frequency
    ///
    /// `capacity` is used as a parameter for [`dashmap::DashMap`] inside `crate::cache::store::Store`
    ///  It defines the number of items that the cache may store
    ///
    /// `cache_weight` defines the total cache size. cache_weight is treated as the cache size in bytes
    /// If cache_weight is set to 1024, that means the cache should take only 1024 bytes of memory.
    ///
    /// After the cache size is full, any further `put` operation will result in either of the following:
        /// rejection of the incoming key
        /// admission of the incoming key by causing eviction of some existing keys
    pub fn new(counters: TotalCounters, capacity: TotalCapacity, cache_weight: Weight) -> Self {
        assert!(counters > 0, "{}", Errors::TotalCountersGtZero);
        assert!(capacity > 0, "{}", Errors::TotalCapacityGtZero);
        assert!(cache_weight > 0, "{}", Errors::TotalCacheWeightGtZero);

        let key_hash_fn = |key: &Key| -> KeyHash {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        };

        ConfigBuilder {
            key_hash_fn: Box::new(key_hash_fn),
            weight_calculation_fn: Box::new(Calculation::perform),
            clock: SystemClock::boxed(),
            access_pool_size: ACCESS_POOL_SIZE,
            access_buffer_size: ACCESS_BUFFER_SIZE,
            command_buffer_size: COMMAND_BUFFER_SIZE,
            counters,
            capacity,
            total_cache_weight: cache_weight,
            shards: SHARDS,
            ttl_tick_duration: TTL_TICK_DURATION,
        }
    }

    /// Sets the key hash function
    ///
    /// By default, DefaultHasher is used that uses SipHasher13 as the hash function.
    pub fn key_hash_fn(mut self, key_hash: Box<HashFn<Key>>) -> ConfigBuilder<Key, Value> {
        self.key_hash_fn = key_hash;
        self
    }

    /// Sets the weight calculation function
    ///
    /// Weight calculation function calculates the weight of the incoming key/value pair.
    ///
    /// Default is the `perform` function defined in `crate::cache::config::weight_calculation::Calculation`
    pub fn weight_calculation_fn(mut self, weight_calculation: Box<WeightCalculationFn<Key, Value>>) -> ConfigBuilder<Key, Value> {
        self.weight_calculation_fn = weight_calculation;
        self
    }

    /// Sets the clock to be used to get the current time. By default [`crate::cache::clock::SystemClock`] is used.
    pub fn clock(mut self, clock: ClockType) -> ConfigBuilder<Key, Value> {
        self.clock = clock;
        self
    }

    /// Sets the pool size
    ///
    /// Pool represents a ring-buffer that is used to buffer the gets for various keys.
    ///
    /// Default pool size is 32
    pub fn access_pool_size(mut self, pool_size: usize) -> ConfigBuilder<Key, Value> {
        assert!(pool_size > 0, "{}", Errors::PoolSizeGtZero);
        self.access_pool_size = PoolSize(pool_size);
        self
    }

    /// Sets the size of each buffer inside Pool
    ///
    /// Default capacity of the buffer is 64
    pub fn access_buffer_size(mut self, buffer_size: usize) -> ConfigBuilder<Key, Value> {
        assert!(buffer_size > 0, "{}", Errors::BufferSizeGtZero);
        self.access_buffer_size = BufferSize(buffer_size);
        self
    }

    /// Each put, put_or_update, delete results in a command to `crate::cache::command::command_executor::CommandExecutor`.
    ///
    /// CommandExecutor reads from a channel and the default channel size is 32 * 1024
    pub fn command_buffer_size(mut self, command_buffer_size: usize) -> ConfigBuilder<Key, Value> {
        assert!(command_buffer_size > 0, "{}", Errors::CommandBufferSizeGtZero);
        self.command_buffer_size = command_buffer_size;
        self
    }

    /// Sets the number of shards to use in the DashMap inside `crate::cache::store::Store`
    ///
    /// `shards` must be a power of 2 and greater than 1
    pub fn shards(mut self, shards: TotalShards) -> ConfigBuilder<Key, Value> {
        assert!(shards > 1, "{}", Errors::TotalShardsGtOne);
        assert!(shards.is_power_of_two(), "{}", Errors::TotalShardsPowerOf2);
        self.shards = shards;
        self
    }

    /// Sets the duration of the `crate::cache::expiration::TTLTicker`]
    ///
    /// Default is every 5 seconds.
    pub fn ttl_tick_duration(mut self, duration: Duration) -> ConfigBuilder<Key, Value> {
        self.ttl_tick_duration = duration;
        self
    }

    // Builds an instance of Config with the supplied values
    pub fn build(self) -> Config<Key, Value> {
        Config {
            key_hash_fn: self.key_hash_fn,
            weight_calculation_fn: self.weight_calculation_fn,
            clock: self.clock,
            access_pool_size: self.access_pool_size,
            access_buffer_size: self.access_buffer_size,
            command_buffer_size: self.command_buffer_size,
            counters: self.counters,
            capacity: self.capacity,
            total_cache_weight: self.total_cache_weight,
            shards: self.shards,
            ttl_tick_duration: self.ttl_tick_duration,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use crate::cache::clock::ClockType;
    use crate::cache::config::{Config, ConfigBuilder};
    use crate::cache::config::tests::setup::UnixEpochClock;
    use crate::cache::pool::{BufferSize, PoolSize};
    use crate::cache::types::IsTimeToLiveSpecified;

    mod setup {
        use std::time::SystemTime;

        use crate::cache::clock::Clock;

        #[derive(Clone)]
        pub(crate) struct UnixEpochClock;

        impl Clock for UnixEpochClock {
            fn now(&self) -> SystemTime {
                SystemTime::UNIX_EPOCH
            }
        }
    }

    fn test_config_builder() -> ConfigBuilder<&'static str, &'static str>{
        ConfigBuilder::new(100, 10, 100)
    }

    #[test]
    fn key_hash_function() {
        let builder: ConfigBuilder<&str, &str> = test_config_builder();

        let key_hash_fn = Box::new(|_key: &&str| 1);
        let config = builder.key_hash_fn(key_hash_fn).build();

        let key = "topic";
        let hash = (config.key_hash_fn)(&key);

        assert_eq!(1, hash);
    }

    #[test]
    fn weight_calculation_function() {
        let builder: ConfigBuilder<&str, &str> = test_config_builder();

        let weight_calculation_fn = Box::new(|_key: &&str, _value: &&str, _is_time_to_live_specified: IsTimeToLiveSpecified| 10);
        let config = builder.weight_calculation_fn(weight_calculation_fn).build();

        let key = "topic";
        let value = "microservices";
        let weight = (config.weight_calculation_fn)(&key, &value, false);

        assert_eq!(10, weight);
    }

    #[test]
    fn clock() {
        let builder: ConfigBuilder<&str, &str> = test_config_builder();
        let clock: ClockType = Box::new(UnixEpochClock {});

        let config = builder.clock(clock).build();
        assert_eq!(SystemTime::UNIX_EPOCH, config.clock.now());
    }

    #[test]
    fn access_pool_size() {
        let builder: ConfigBuilder<&str, &str> = test_config_builder();
        let config = builder.access_pool_size(32).build();

        assert_eq!(PoolSize(32), config.access_pool_size);
    }

    #[test]
    fn access_buffer_size() {
        let builder: ConfigBuilder<&str, &str> = test_config_builder();
        let config = builder.access_buffer_size(64).build();

        assert_eq!(BufferSize(64), config.access_buffer_size);
    }

    #[test]
    fn command_buffer_size() {
        let builder: ConfigBuilder<&str, &str> = test_config_builder();
        let config = builder.command_buffer_size(1024).build();

        assert_eq!(1024, config.command_buffer_size);
    }

    #[test]
    fn counters() {
        let config: Config<&str, &str> = ConfigBuilder::new(4096, 400, 100).build();

        assert_eq!(4096, config.counters);
    }

    #[test]
    fn total_cache_weight() {
        let builder: ConfigBuilder<&str, &str> = ConfigBuilder::new(100, 10, 1048576);
        let config = builder.build();

        assert_eq!(1048576, config.total_cache_weight);
    }

    #[test]
    fn shards() {
        let builder: ConfigBuilder<&str, &str> = test_config_builder();
        let config = builder.shards(16).build();

        assert_eq!(16, config.shards);
    }

    #[test]
    fn ttl_tick_duration() {
        let builder: ConfigBuilder<&str, &str> = test_config_builder();
        let config = builder.ttl_tick_duration(Duration::from_secs(5)).build();

        assert_eq!(Duration::from_secs(5), config.ttl_tick_duration);
    }

    #[test]
    fn ttl_config() {
        let builder: ConfigBuilder<&str, &str> = test_config_builder();
        let config = builder.ttl_tick_duration(Duration::from_secs(5)).shards(16).build();

        let ttl_config = config.ttl_config();
        assert_eq!(16, ttl_config.shards());
        assert_eq!(Duration::from_secs(5), ttl_config.tick_duration());
    }

    #[test]
    fn cache_weight_config() {
        let builder: ConfigBuilder<&str, &str> = ConfigBuilder::new(100, 10, 200).shards(4);
        let config = builder.build();

        let cache_weight_config = config.cache_weight_config();
        assert_eq!(10, cache_weight_config.capacity());
        assert_eq!(4, cache_weight_config.shards());
        assert_eq!(200, cache_weight_config.total_cache_weight());
    }

    #[test]
    #[should_panic]
    fn access_pool_size_must_be_greater_than_zero() {
        let _: Config<&str, &str> = test_config_builder().access_pool_size(0).build();
    }

    #[test]
    #[should_panic]
    fn access_buffer_size_must_be_greater_than_zero() {
        let _: Config<&str, &str> = test_config_builder().access_buffer_size(0).build();
    }

    #[test]
    #[should_panic]
    fn command_buffer_size_must_be_greater_than_zero() {
        let _: Config<&str, &str> = test_config_builder().command_buffer_size(0).build();
    }

    #[test]
    #[should_panic]
    fn total_counters_must_be_greater_than_zero() {
        let _: Config<&str, &str> = ConfigBuilder::new(0, 10, 10).build();
    }

    #[test]
    #[should_panic]
    fn total_capacity_must_be_greater_than_zero() {
        let _: Config<&str, &str> = ConfigBuilder::new(10, 0, 10).build();
    }

    #[test]
    #[should_panic]
    fn total_cache_weight_must_be_greater_than_zero() {
        let _: Config<&str, &str> = ConfigBuilder::new(100, 100, 0).build();
    }

    #[test]
    #[should_panic]
    fn shards_must_be_greater_than_one() {
        let _: Config<&str, &str> = test_config_builder().shards(1).build();
    }

    #[test]
    #[should_panic]
    fn shards_must_be_power_of_2() {
        let _: Config<&str, &str> = test_config_builder().shards(3).build();
    }
}