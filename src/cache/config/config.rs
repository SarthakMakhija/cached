use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::cache::clock::{ClockType, SystemClock};
use crate::cache::pool::{BufferSize, PoolSize};

type HashFn<Key> = dyn Fn(&Key) -> u64;

const COMMAND_BUFFER_SIZE: usize = 32 * 1024;
const ACCESS_POOL_SIZE: PoolSize = PoolSize(30);
const ACCESS_BUFFER_SIZE: BufferSize = BufferSize(64);
const COUNTERS: u64 = 1_000_000;

pub struct Config<Key>
    where Key: Hash {
    pub key_hash: Box<HashFn<Key>>,
    pub clock: ClockType,
    pub counters: u64,
    pub command_buffer_size: usize,
    pub(crate) access_pool_size: PoolSize,
    pub(crate) access_buffer_size: BufferSize,
}

pub struct ConfigBuilder<Key>
    where Key: Hash {
    key_hash: Box<HashFn<Key>>,
    clock: ClockType,
    access_pool_size: PoolSize,
    access_buffer_size: BufferSize,
    command_buffer_size: usize,
    counters: u64,
}

impl<Key> Default for ConfigBuilder<Key>
    where Key: Hash {
    fn default() -> Self {
        Self::new()
    }
}

impl<Key> ConfigBuilder<Key>
    where Key: Hash {
    pub fn new() -> Self {
        let key_hash = |key: &Key| -> u64 {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        };

        return ConfigBuilder {
            key_hash: Box::new(key_hash),
            clock: SystemClock::boxed(),
            access_pool_size: ACCESS_POOL_SIZE,
            access_buffer_size: ACCESS_BUFFER_SIZE,
            command_buffer_size: COMMAND_BUFFER_SIZE,
            counters: COUNTERS,
        };
    }

    pub fn key_hash(mut self, key_hash: Box<HashFn<Key>>) -> ConfigBuilder<Key> {
        self.key_hash = key_hash;
        self
    }

    pub fn clock(mut self, clock: ClockType) -> ConfigBuilder<Key> {
        self.clock = clock;
        self
    }

    pub fn access_pool_size(mut self, pool_size: usize) -> ConfigBuilder<Key> {
        self.access_pool_size = PoolSize(pool_size);
        self
    }

    pub fn access_buffer_size(mut self, buffer_size: usize) -> ConfigBuilder<Key> {
        self.access_buffer_size = BufferSize(buffer_size);
        self
    }

    pub fn command_buffer_size(mut self, command_buffer_size: usize) -> ConfigBuilder<Key> {
        self.command_buffer_size = command_buffer_size;
        self
    }

    pub fn counters(mut self, counters: u64) -> ConfigBuilder<Key> {
        self.counters = counters;
        self
    }

    pub fn build(self) -> Config<Key> {
        Config {
            key_hash: self.key_hash,
            clock: self.clock,
            access_pool_size: self.access_pool_size,
            access_buffer_size: self.access_buffer_size,
            command_buffer_size: self.command_buffer_size,
            counters: self.counters,
        }
    }
}