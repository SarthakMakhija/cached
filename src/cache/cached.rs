use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;

use crate::cache::clock::{Clock, SystemClock};
use crate::cache::stored_value::StoredValue;

pub struct CacheD<Key, Value>
    where Key: Hash + Eq {
    store: HashMap<Key, StoredValue<Value>>,
    clock: Box<dyn Clock>,
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq {
    pub fn new() -> Self {
        return CacheD {
            store: HashMap::new(),
            clock: SystemClock::boxed(),
        };
    }

    #[cfg(test)]
    pub fn new_with_clock(clock: Box<dyn Clock>) -> Self {
        return CacheD {
            store: HashMap::new(),
            clock,
        };
    }

    pub fn put(&mut self, key: Key, value: Value) {
        self.store.insert(key, StoredValue::never_expiring(value));
    }

    pub fn put_with_ttl(&mut self, key: Key, value: Value, time_to_live: Duration) {
        self.store.insert(key, StoredValue::expiring(value, time_to_live, &self.clock));
    }

    pub fn get(&self, key: Key) -> Option<&Value> {
        let maybe_value = self.store.get(&key);
        return maybe_value
            .filter(|stored_value| stored_value.is_alive(&self.clock))
            .map(|stored_value| stored_value.value());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use crate::cache::cached::CacheD;
    use crate::cache::cached::tests::setup::FutureClock;
    use crate::cache::clock::SystemClock;

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

    #[test]
    fn get_value_for_an_expired_key() {
        let clock = SystemClock::boxed();
        let mut cached = CacheD::new_with_clock(clock);

        cached.put_with_ttl("topic", "microservices", Duration::from_secs(5));
        cached.clock = Box::new(FutureClock{});

        let value = cached.get("topic");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_an_unexpired_key() {
        let clock = SystemClock::boxed();
        let mut cached = CacheD::new_with_clock(clock);

        cached.put_with_ttl("topic", "microservices", Duration::from_secs(15));
        cached.clock = Box::new(FutureClock{});

        let value = cached.get("topic");
        assert_eq!(Some(&"microservices"), value);
    }
}