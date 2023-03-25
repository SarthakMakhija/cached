use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use crate::cache::clock::{ClockType, SystemClock};

type HashFn<Key> = dyn Fn(&Key) -> u64;

pub struct Config<Key>
    where Key: Hash {
    pub key_hash: Box<HashFn<Key>>,
    pub clock: ClockType,
}

pub struct ConfigBuilder<Key>
    where Key: Hash {
    key_hash: Box<HashFn<Key>>,
    clock: ClockType,
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
        };
    }

    pub fn key_hash(&mut self, key_hash: Box<HashFn<Key>>) {
        self.key_hash = key_hash;
    }

    pub fn clock(&mut self, clock: ClockType) {
        self.clock = clock;
    }

    pub fn build(self) -> Config<Key> {
        return Config {
            key_hash: self.key_hash,
            clock: self.clock,
        };
    }
}