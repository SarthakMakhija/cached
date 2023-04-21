use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use dashmap::DashMap;

use crate::cache::clock::ClockType;
use crate::cache::stats::ConcurrentStatsCounter;
use crate::cache::store::key_value_ref::KeyValueRef;
use crate::cache::store::stored_value::StoredValue;
use crate::cache::types::{ExpireAfter, KeyId};

pub mod stored_value;
pub mod key_value_ref;

#[derive(Eq, PartialEq, Debug)]
pub(crate) struct KeyIdExpiry(pub(crate) KeyId, pub(crate) Option<ExpireAfter>);

#[derive(Eq, PartialEq, Debug)]
pub(crate) struct UpdateEligibility(pub(crate) Option<KeyIdExpiry>);

#[derive(Eq, PartialEq, Debug)]
pub(crate) struct UpdateResponse(KeyIdExpiry, ExpireAfter);

impl UpdateResponse {
    pub(crate) fn key_id(&self) -> KeyId {
        self.0.0
    }
    pub(crate) fn existing_expiry(&self) -> Option<ExpireAfter> {
        self.0.1
    }
    pub(crate) fn new_expiry(&self) -> SystemTime {
        self.1
    }
}

const UPDATE_NOT_ELIGIBLE: UpdateEligibility = UpdateEligibility(None);

impl UpdateEligibility {
    pub(crate) fn is_eligible(&self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn is_not_eligible(&self) -> bool {
        !self.is_eligible()
    }
}

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
            stats_counter,
        })
    }

    pub(crate) fn put(&self, key: Key, value: Value, key_id: KeyId) {
        self.store.insert(key, StoredValue::never_expiring(value, key_id));
        self.stats_counter.add_key();
    }

    pub(crate) fn put_with_ttl(&self, key: Key, value: Value, key_id: KeyId, time_to_live: Duration) -> ExpireAfter {
        let stored_value = StoredValue::expiring(value, key_id, time_to_live, &self.clock);
        let expire_after = stored_value.expire_after();

        self.store.insert(key, stored_value);
        self.stats_counter.add_key();

        expire_after.unwrap()
    }

    pub(crate) fn delete(&self, key: &Key) -> Option<KeyIdExpiry> {
        if let Some(pair) = self.store.remove(key) {
            self.stats_counter.delete_key();
            return Some(KeyIdExpiry(pair.1.key_id(), pair.1.expire_after()));
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
        let mapped_value = self.contains(key);
        if mapped_value.is_some() { self.stats_counter.found_a_hit(); } else { self.stats_counter.found_a_miss(); }
        mapped_value
    }

    pub(crate) fn get_key_id(&self, key: &Key) -> Option<KeyId> {
        self.contains(key).map(|key_value_ref| key_value_ref.value().key_id())
    }

    pub(crate) fn update_eligibility(&self, key: &Key) -> UpdateEligibility {
        let mapped_value = self.contains(key);
        mapped_value.map_or(UPDATE_NOT_ELIGIBLE, |key_value_ref| {
            let stored_value = key_value_ref.value();
            UpdateEligibility(Some(KeyIdExpiry(stored_value.key_id(), stored_value.expire_after())))
        })
    }

    pub(crate) fn update_time_to_live(&self, key: &Key, time_to_live: Duration) -> Option<UpdateResponse> {
        if let Some(mut existing) = self.store.get_mut(key) {
            let new_expiry = StoredValue::<Value>::calculate_expiry(time_to_live, &self.clock);
            let response = Some(UpdateResponse(
                KeyIdExpiry(existing.key_id(), existing.expire_after()),
                new_expiry,
            ));
            existing.update_expiry(new_expiry);
            return response;
        }
        None
    }

    fn contains(&self, key: &Key) -> Option<KeyValueRef<Key, StoredValue<Value>>> {
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
        let mapped_value = maybe_value
            .filter(|stored_value| stored_value.is_alive(&self.clock))
            .map(|key_value_ref| { key_value_ref.value().value() });

        if mapped_value.is_some() { self.stats_counter.found_a_hit(); } else { self.stats_counter.found_a_miss(); }
        mapped_value
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::sync::Arc;
    use std::time::Duration;

    use setup::FutureClock;

    use crate::cache::clock::{Clock, SystemClock};
    use crate::cache::stats::ConcurrentStatsCounter;
    use crate::cache::store::{KeyIdExpiry, Store, UPDATE_NOT_ELIGIBLE, UpdateEligibility};
    use crate::cache::store::stored_value::StoredValue;
    use crate::cache::store::tests::setup::{Name, UnixEpochClock};

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

        #[derive(Clone)]
        pub(crate) struct UnixEpochClock;

        impl Clock for UnixEpochClock {
            fn now(&self) -> SystemTime {
                SystemTime::UNIX_EPOCH
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
    fn get_value_for_an_existing_key_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("topic", "microservices", 1);

        let _ = store.get(&"topic");
        assert_eq!(1, store.stats_counter.hits());
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
    fn put_with_ttl_and_get_expire_after() {
        let clock = Box::new(UnixEpochClock {});
        let store = Store::new(clock.clone(), Arc::new(ConcurrentStatsCounter::new()));

        let expire_after = store.put_with_ttl("topic", "microservices", 1, Duration::from_secs(5));
        assert_eq!(clock.now().add(Duration::from_secs(5)), expire_after);
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
    fn get_value_ref_for_an_existing_key_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("name", Name { first: "John".to_string(), last: "Mcnamara".to_string() }, 1);

        let _ = store.get_ref(&"name");
        assert_eq!(1, store.stats_counter.hits());
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        let value: Option<&str> = store.get(&"non-existing");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_a_non_existing_key_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        let _value: Option<&str> = store.get(&"non-existing");
        assert_eq!(1, store.stats_counter.misses());
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
    fn get_value_for_an_expired_key_and_increase_stats() {
        let store = Store::new(Box::new(FutureClock {}), Arc::new(ConcurrentStatsCounter::new()));
        {
            let clock = SystemClock::boxed();
            store.store.insert("topic", StoredValue::expiring("microservices", 1, Duration::from_secs(5), &clock));
        }

        let _ = store.get(&"topic");
        assert_eq!(1, store.stats_counter.misses());
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
    fn get_value_for_an_unexpired_key_and_increase_stats() {
        let store = Store::new(Box::new(FutureClock {}), Arc::new(ConcurrentStatsCounter::new()));
        {
            let clock = SystemClock::boxed();
            store.store.insert("topic", StoredValue::expiring("microservices", 1, Duration::from_secs(15), &clock));
        }

        let _ = store.get(&"topic");
        assert_eq!(1, store.stats_counter.hits());
    }

    #[test]
    fn delete_a_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("topic", "microservices", 10);
        let key_id_expiry = store.delete(&"topic");

        let value = store.get(&"topic");
        assert_eq!(None, value);
        assert_eq!(10, key_id_expiry.unwrap().0);
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

        let key_id_expiry = store.delete(&"non-existing");

        let value: Option<&str> = store.get(&"non-existing");
        assert_eq!(None, value);
        assert_eq!(None, key_id_expiry);
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

    #[test]
    fn can_update() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        store.put("topic", "microservices", 10);
        let update_eligibility = store.update_eligibility(&"topic");

        let expected_update_eligibility = UpdateEligibility(Some(KeyIdExpiry(10, None)));
        assert_eq!(expected_update_eligibility, update_eligibility);
    }

    #[test]
    fn can_non_update() {
        let clock = SystemClock::boxed();
        let store: Arc<Store<&str, &str>> = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));

        let update_eligibility = store.update_eligibility(&"non_existing");
        assert_eq!(UPDATE_NOT_ELIGIBLE, update_eligibility);
    }

    #[test]
    fn update_time_to_live_for_non_existing_key() {
        let clock = SystemClock::boxed();
        let store: Arc<Store<&str, &str>> = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()));
        let response = store.update_time_to_live(&"topic", Duration::from_secs(5));

        assert_eq!(None, response);
    }

    #[test]
    fn update_time_to_live_for_an_existing_key() {
        let clock = Box::new(UnixEpochClock {});
        let store = Store::new(clock.clone(), Arc::new(ConcurrentStatsCounter::new()));

        store.put("topic", "microservices", 10);
        let update_response = store.update_time_to_live(&"topic", Duration::from_secs(5));
        assert!(update_response.unwrap().existing_expiry().is_none());

        let key_value_ref = store.get_ref(&"topic").unwrap();
        let expected_expiry = clock.now().add(Duration::from_secs(5));
        assert_eq!(Some(expected_expiry), key_value_ref.value().expire_after());
    }

    #[test]
    fn update_time_to_live_for_an_existing_key_that_has_an_expiry() {
        let clock = Box::new(UnixEpochClock {});
        let store = Store::new(clock.clone(), Arc::new(ConcurrentStatsCounter::new()));

        store.put_with_ttl("topic", "microservices", 10, Duration::from_secs(300));
        let _ = store.update_time_to_live(&"topic", Duration::from_secs(15));

        let key_value_ref = store.get_ref(&"topic").unwrap();
        let expected_expiry = clock.now().add(Duration::from_secs(15));
        assert_eq!(Some(expected_expiry), key_value_ref.value().expire_after());
    }
}

#[cfg(test)]
mod update_eligibility_tests {
    use crate::cache::store::{KeyIdExpiry, UpdateEligibility};

    #[test]
    fn is_not_update_eligible() {
        let update_eligibility = UpdateEligibility(None);
        assert!(update_eligibility.is_not_eligible());
    }

    #[test]
    fn is_update_eligible() {
        let update_eligibility = UpdateEligibility(Some(KeyIdExpiry(10, None)));
        assert!(update_eligibility.is_eligible());
    }
}