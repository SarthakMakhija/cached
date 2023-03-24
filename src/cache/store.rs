use std::hash::Hash;
use std::time::Duration;

use dashmap::DashMap;

use crate::cache::clock::ClockType;
use crate::cache::stored_value::StoredValue;

pub(crate) struct Store<Key, Value>
    where Key: Hash + Eq,
          Value: Clone {
    store: DashMap<Key, StoredValue<Value>>,
    clock: ClockType,
}

impl<Key, Value> Store<Key, Value>
    where Key: Hash + Eq,
          Value: Clone {
    pub(crate) fn new(clock: ClockType) -> Self {
        return Store {
            store: DashMap::new(),
            clock,
        };
    }

    pub(crate) fn put(&self, key: Key, value: Value) {
        self.store.insert(key, StoredValue::never_expiring(value));
    }

    pub(crate) fn put_with_ttl(&self, key: Key, value: Value, time_to_live: Duration) {
        self.store.insert(key, StoredValue::expiring(value, time_to_live, &self.clock));
    }

    pub(crate) fn get(&self, key: &Key) -> Option<Value> {
        let maybe_value = self.store.get(key);
        return maybe_value
            .filter(|stored_value| stored_value.is_alive(&self.clock))
            .map(|stored_value| { stored_value.value().value() });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use setup::FutureClock;

    use crate::cache::clock::SystemClock;
    use crate::cache::store::Store;

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
        let clock = SystemClock::boxed();
        let store = Store::new(clock);
        store.put("topic", "microservices");

        let value = store.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let clock = SystemClock::boxed();
        let store: Store<&str, &str> = Store::new(clock);

        let value = store.get(&"non-existing");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_an_expired_key() {
        let clock = SystemClock::boxed();
        let mut store = Store::new(clock);

        store.put_with_ttl("topic", "microservices", Duration::from_secs(5));
        store.clock = Box::new(FutureClock {});

        let value = store.get(&"topic");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_an_unexpired_key() {
        let clock = SystemClock::boxed();
        let mut store = Store::new(clock);

        store.put_with_ttl("topic", "microservices", Duration::from_secs(15));
        store.clock = Box::new(FutureClock {});

        let value = store.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }
}