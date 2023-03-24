use std::hash::Hash;
use std::time::Duration;

use crate::cache::clock::{SystemClock};
use crate::cache::store::Store;

pub struct CacheD<Key, Value>
    where Key: Hash + Eq {
    store: Store<Key, Value>,
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq {
    pub fn new() -> Self {
        return CacheD {
            store: Store::new(SystemClock::boxed()),
        };
    }

    #[cfg(test)]
    pub fn new_with_clock(clock: Box<dyn crate::cache::clock::Clock>) -> Self {
        return CacheD {
            store: Store::new(clock),
        };
    }

    pub fn put(&mut self, key: Key, value: Value) {
        self.store.put(key, value);
    }

    pub fn put_with_ttl(&mut self, key: Key, value: Value, time_to_live: Duration) {
        self.store.put_with_ttl(key, value, time_to_live);
    }

    pub fn get(&self, key: Key) -> Option<&Value> {
        return self.store.get(&key);
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::cached::CacheD;

    mod setup {
        use std::ops::Add;
        use std::time::{Duration, SystemTime};

        use crate::cache::clock::Clock;

        pub(crate) struct FutureClock;

        impl Clock for FutureClock {
            fn now(&self) -> SystemTime {
                return SystemTime::now().add(Duration::from_secs(10));
            }
        }
    }

    #[test]
    fn get_value_for_an_existing_key() {
        let mut cached = CacheD::new();
        cached.put("topic", "microservices");

        let value = cached.get("topic");
        assert_eq!(Some(&"microservices"), value);
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let cached: CacheD<&str, &str> = CacheD::new();

        let value = cached.get("non-existing");
        assert_eq!(None, value);
    }
}