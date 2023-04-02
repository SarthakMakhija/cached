use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::cache::clock::{ClockType, SystemClock};
use crate::cache::config::weight_calculation::Calculation;
use crate::cache::pool::{BufferSize, PoolSize};
use crate::cache::types::{KeyHash, TotalCounters, Weight};

pub(crate) mod weight_calculation;

type HashFn<Key> = dyn Fn(&Key) -> KeyHash;
type WeightCalculationFn<Key, Value> = dyn Fn(&Key, &Value) -> Weight;

const COMMAND_BUFFER_SIZE: usize = 32 * 1024;
const ACCESS_POOL_SIZE: PoolSize = PoolSize(30);
const ACCESS_BUFFER_SIZE: BufferSize = BufferSize(64);
const COUNTERS: TotalCounters = 1_000_000;
const TOTAL_CACHE_WEIGHT: Weight = 100_000_000;

pub struct Config<Key, Value>
    where Key: Hash + 'static,
          Value: 'static {
    pub key_hash_fn: Box<HashFn<Key>>,
    pub weight_calculation_fn: Box<WeightCalculationFn<Key, Value>>,
    pub clock: ClockType,
    pub counters: TotalCounters,
    pub command_buffer_size: usize,
    pub(crate) access_pool_size: PoolSize,
    pub(crate) access_buffer_size: BufferSize,
    pub total_cache_weight: Weight,
}

pub struct ConfigBuilder<Key, Value>
    where Key: Hash + 'static,
          Value: 'static {
    key_hash_fn: Box<HashFn<Key>>,
    weight_calculation_fn: Box<WeightCalculationFn<Key, Value>>,
    clock: ClockType,
    counters: TotalCounters,
    command_buffer_size: usize,
    access_pool_size: PoolSize,
    access_buffer_size: BufferSize,
    total_cache_weight: Weight,
}

impl<Key, Value> Default for ConfigBuilder<Key, Value>
    where Key: Hash + 'static,
          Value: 'static {
    fn default() -> Self {
        Self::new()
    }
}

impl<Key, Value> ConfigBuilder<Key, Value>
    where Key: Hash + 'static,
          Value: 'static {
    pub fn new() -> Self {
        let key_hash_fn = |key: &Key| -> KeyHash {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        };

        return ConfigBuilder {
            key_hash_fn: Box::new(key_hash_fn),
            weight_calculation_fn: Box::new(Calculation::perform),
            clock: SystemClock::boxed(),
            access_pool_size: ACCESS_POOL_SIZE,
            access_buffer_size: ACCESS_BUFFER_SIZE,
            command_buffer_size: COMMAND_BUFFER_SIZE,
            counters: COUNTERS,
            total_cache_weight: TOTAL_CACHE_WEIGHT,
        };
    }

    pub fn key_hash_fn(mut self, key_hash: Box<HashFn<Key>>) -> ConfigBuilder<Key, Value> {
        self.key_hash_fn = key_hash;
        self
    }

    pub fn weight_calculation_fn(mut self, weight_calculation: Box<WeightCalculationFn<Key, Value>>) -> ConfigBuilder<Key, Value> {
        self.weight_calculation_fn = weight_calculation;
        self
    }

    pub fn clock(mut self, clock: ClockType) -> ConfigBuilder<Key, Value> {
        self.clock = clock;
        self
    }

    pub fn access_pool_size(mut self, pool_size: usize) -> ConfigBuilder<Key, Value> {
        self.access_pool_size = PoolSize(pool_size);
        self
    }

    pub fn access_buffer_size(mut self, buffer_size: usize) -> ConfigBuilder<Key, Value> {
        self.access_buffer_size = BufferSize(buffer_size);
        self
    }

    pub fn command_buffer_size(mut self, command_buffer_size: usize) -> ConfigBuilder<Key, Value> {
        self.command_buffer_size = command_buffer_size;
        self
    }

    pub fn counters(mut self, counters: TotalCounters) -> ConfigBuilder<Key, Value> {
        self.counters = counters;
        self
    }

    pub fn total_cache_weight(mut self, weight: Weight) -> ConfigBuilder<Key, Value> {
        self.total_cache_weight = weight;
        self
    }

    pub fn build(self) -> Config<Key, Value> {
        Config {
            key_hash_fn: self.key_hash_fn,
            weight_calculation_fn: self.weight_calculation_fn,
            clock: self.clock,
            access_pool_size: self.access_pool_size,
            access_buffer_size: self.access_buffer_size,
            command_buffer_size: self.command_buffer_size,
            counters: self.counters,
            total_cache_weight: self.total_cache_weight,
        }
    }
}