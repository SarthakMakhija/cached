use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use crate::cache::clock::ClockType;
use crate::cache::stats::ConcurrentStatsCounter;
use crate::cache::store::key_value_ref::KeyValueRef;
use crate::cache::store::stored_value::StoredValue;
use crate::cache::types::KeyId;

pub mod stored_value;
pub mod key_value_ref;

pub(crate) struct Store<Key, Value>
    where Key: Hash + Eq, {
    store: DashMap<Key, StoredValue<Value>>,
    clock: ClockType,
    stats_counter: Arc<ConcurrentStatsCounter>,
}

impl<Key, Value> Store<Key, Value>
    where Key: Hash + Eq, {
    pub(crate) fn new(clock: ClockType, stats_counter: Arc<ConcurrentStatsCounter>) -> Arc<Store<Key, Value>> {
        Arc::new(Store {
            store: DashMap::new(), //TODO: define capacity
            clock,
            stats_counter
        })
    }

    pub(crate) fn put(&self, key: Key, value: Value, key_id: KeyId) {
        self.store.insert(key, StoredValue::never_expiring(value, key_id));
        self.stats_counter.add_key();
    }

    pub(crate) fn put_with_ttl(&self, key: Key, value: Value, key_id: KeyId, time_to_live: Duration) {
        self.store.insert(key, StoredValue::expiring(value, key_id, time_to_live, &self.clock));
        self.stats_counter.add_key();
    }

    pub(crate) fn delete(&self, key: &Key) -> Option<KeyId> {
        if let Some(pair) = self.store.remove(key) {
            self.stats_counter.delete_key();
            return Some(pair.1.key_id())
        }
        None
    }

    pub(crate) fn mark_deleted(&self, key: &Key) {
        if let Some(mut pair) = self.store.get_mut(key) {
            let stored_value = pair.value_mut();
            stored_value.is_soft_deleted = true;
        }
    }

    pub(crate) fn get_ref(&self, key: &Key) -> Option<KeyValueRef<'_, Key, StoredValue<Value>>> {
        let maybe_value = self.store.get(key);
        maybe_value
            .filter(|stored_value| stored_value.is_alive(&self.clock))
            .map(|key_value_ref| KeyValueRef::new(key_value_ref))
    }
}

impl<Key, Value> Store<Key, Value>
    where Key: Hash + Eq,
          Value: Clone, {
    pub(crate) fn get(&self, key: &Key) -> Option<Value> {
        let maybe_value = self.store.get(key);
        maybe_value
            .filter(|stored_value| stored_value.is_alive(&self.clock))
            .map(|key_value_ref| { key_value_ref.value().value() })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use setup::FutureClock;

    use crate::cache::clock::SystemClock;
    use crate::cache::stats::ConcurrentStatsCounter;
    use crate::cache::store::Store;
    use crate::cache::store::stored_value::StoredValue;
    use crate::cache::store::tests::setup::Name;

    mod setup {
        use std::ops::Add;
        use std::time::{Duration, SystemTime};

        use crate::cache::clock::Clock;

        #[derive(Eq, PartialEq, Debug)]
        pub(crate) struct Name {
            pub(crate) first: String,
            pub(crate) last: String,
        }

        #[derive(Clone)]
        pub(crate) struct FutureClock;

        impl Clock for FutureClock {
            fn now(&self) -> SystemTime {
                SystemTime::now().add(Duration::from_secs(10))
            }
        }
    }

    #[test]
    fn get_value_for_an_existing_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("topic", "microservices", 1);

        let value = store.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn put_a_key_value_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("topic", "microservices", 1);
        assert_eq!(1, store.stats_counter.keys_added());
    }

    #[test]
    fn put_with_ttl() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put_with_ttl("topic", "microservices", 1, Duration::from_millis(5));

        let value = store.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn put_with_ttl_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put_with_ttl("topic", "microservices", 1, Duration::from_millis(5));
        assert_eq!(1, store.stats_counter.keys_added());
    }

    #[test]
    fn put_with_ttl_and_get_the_value_of_an_expired_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put_with_ttl("topic", "microservices", 1, Duration::from_nanos(1));

        let value = store.get(&"topic");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_ref_for_an_existing_key_if_value_is_not_cloneable() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("name", Name { first: "John".to_string(), last: "Mcnamara".to_string() }, 1);

        let key_value_ref = store.get_ref(&"name");
        assert_eq!(&Name { first: "John".to_string(), last: "Mcnamara".to_string() }, key_value_ref.unwrap().value().value_ref());
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        let value: Option<&str> = store.get(&"non-existing");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_an_expired_key() {
        let store = Store::new(Box::new(FutureClock {}), Arc::new(ConcurrentStatsCounter::new()));
        {
            let clock = SystemClock::boxed();
            store.store.insert("topic", StoredValue::expiring("microservices", 1, Duration::from_secs(5), &clock));
        }

        let value = store.get(&"topic");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_an_unexpired_key() {
        let store = Store::new(Box::new(FutureClock {}), Arc::new(ConcurrentStatsCounter::new()));
        {
            let clock = SystemClock::boxed();
            store.store.insert("topic", StoredValue::expiring("microservices", 1, Duration::from_secs(15), &clock));
        }

        let value = store.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn delete_a_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("topic", "microservices", 10);
        let key_id = store.delete(&"topic");

        let value = store.get(&"topic");
        assert_eq!(None, value);
        assert_eq!(Some(10), key_id);
    }

    #[test]
    fn delete_a_key_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("topic", "microservices", 10);

        let _ = store.delete(&"topic");
        assert_eq!(1, store.stats_counter.keys_deleted());
    }

    #[test]
    fn delete_a_non_existing_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        let key_id = store.delete(&"non-existing");

        let value: Option<&str> = store.get(&"non-existing");
        assert_eq!(None, value);
        assert_eq!(None, key_id);
    }

    #[test]
    fn delete_a_non_existing_key_and_do_not_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::<&str, &str>::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        let _ = store.delete(&"non-existing");
        assert_eq!(0, store.stats_counter.keys_deleted());
    }

    #[test]
    fn marks_a_key_deleted() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("topic", "microservices", 10);
        store.mark_deleted(&"topic");

        let value = store.get(&"topic");
        assert_eq!(None, value);
    }
}