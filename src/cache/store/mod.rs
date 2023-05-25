use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use crate::cache::clock::ClockType;
use crate::cache::stats::ConcurrentStatsCounter;
use crate::cache::store::key_value_ref::KeyValueRef;
use crate::cache::store::stored_value::StoredValue;
use crate::cache::types::{ExpireAfter, KeyId, TotalCapacity, TotalShards};

pub mod stored_value;
pub mod key_value_ref;

/// KeyIdExpiry holds the key id and the optional expiry of the key
#[derive(Eq, PartialEq, Debug)]
pub(crate) struct KeyIdExpiry(pub(crate) KeyId, pub(crate) Option<ExpireAfter>);

/// UpdateResponse holds the existing `KeyIdExpiry`, the new `KeyIdExpiry` and the `Value`
#[derive(Eq, PartialEq, Debug)]
pub(crate) struct UpdateResponse<Value>(Option<KeyIdExpiry>, Option<ExpireAfter>, Option<Value>);

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum TypeOfExpiryUpdate {
    Added(KeyId, ExpireAfter),
    Deleted(KeyId, ExpireAfter),
    Updated(KeyId, ExpireAfter, ExpireAfter),
    Nothing,
}

/// UpdateResponse encapsulates the existing `KeyIdExpiry`, new `KeyIdExpiry` and an optional value.
/// As a part of the `update` operation, an instance of `UpdateResponse` is returned containing:
/// the existing `KeyIdExpiry`, if the key was present in the `Store`,
/// the new `KeyIdExpiry`, if the client made changes to the `time_to_live` of the value
/// the original value if the update did not happen because the key was not present in the Store
impl<Value> UpdateResponse<Value> {
    pub(crate) fn did_update_happen(&self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn existing_expiry(&self) -> Option<ExpireAfter> {
        if self.did_update_happen() { self.0.as_ref().unwrap().1 } else { None }
    }

    pub(crate) fn new_expiry(&self) -> Option<ExpireAfter> {
        self.1
    }

    pub(crate) fn value(self) -> Option<Value> {
        self.2
    }

    pub(crate) fn key_id_or_panic(&self) -> KeyId {
        self.0.as_ref().unwrap().0
    }

    /// Determines the type of expiry update performed by the client.
    /// `TypeOfExpiryUpdate` can be:
    /// "Added" -> meaning `time_to_live` was added as a part of the put_or_update operation
    /// "Deleted" -> meaning `time_to_live` was remove as a part of the put_or_update operation
    /// "Updated" -> meaning `time_to_live` was changed as a part of the put_or_update operation
    /// "Nothing" -> meaning the put_or_update operation had nothing to do with `time_to_live`
    /// Based on the `TypeOfExpiryUpdate`, Cached decides whether an entry needs to be made in `crate::cache::expiration::TTLTicker`
    pub(crate) fn type_of_expiry_update(&self) -> TypeOfExpiryUpdate {
        let existing_expiry = self.existing_expiry();
        let new_expiry = self.1;
        let key_id = self.key_id_or_panic();

        if existing_expiry.is_none() && new_expiry.is_none() {
            TypeOfExpiryUpdate::Nothing
        } else if existing_expiry.is_none() && new_expiry.is_some() {
            TypeOfExpiryUpdate::Added(key_id, new_expiry.unwrap())
        } else if existing_expiry.is_some() && new_expiry.is_none() {
            TypeOfExpiryUpdate::Deleted(key_id, existing_expiry.unwrap())
        } else if existing_expiry.is_some() && new_expiry.is_some() && existing_expiry.ne(&new_expiry) {
            TypeOfExpiryUpdate::Updated(key_id, existing_expiry.unwrap(), new_expiry.unwrap())
        } else {
            TypeOfExpiryUpdate::Nothing
        }
    }
}

/// Store holds the key/value mapping.
/// Value is wrapped in another abstraction `crate::cache::store::stored_value::StoredValue` that contains key_id and expiry, if any, of the key.
pub(crate) struct Store<Key, Value>
    where Key: Hash + Eq, {
    store: DashMap<Key, StoredValue<Value>>,
    clock: ClockType,
    stats_counter: Arc<ConcurrentStatsCounter>,
}

impl<Key, Value> Store<Key, Value>
    where Key: Hash + Eq, {
    /// Create a new instance of Store with `clock`, `stats_counter`, `capacity` and `shards`.
    /// `clock`: Defines the clock to be used to get the current time. By default `crate::cache::clock::SystemClock` is used.
    /// `stats_counter`: Is an instance of `crate::cache::stats::ConcurrentStatsCounter`
    /// `capacity`: Is used as a capacity parameter in DashMap, it defines the number of items that the cache may store
    /// `shards`: Is used as a shards parameter in DashMap
    pub(crate) fn new(clock: ClockType, stats_counter: Arc<ConcurrentStatsCounter>, capacity: TotalCapacity, shards: TotalShards) -> Arc<Store<Key, Value>> {
        Arc::new(Store {
            store: DashMap::with_capacity_and_shard_amount(capacity, shards),
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

    pub(crate) fn update(&self, key: &Key, value: Option<Value>, time_to_live: Option<Duration>, remove_time_to_live: bool) -> UpdateResponse<Value> {
        if let Some(mut existing_value) = self.store.get_mut(key) {
            let existing_expiry = existing_value.expire_after();
            let new_expiry = existing_value.update(value, time_to_live, remove_time_to_live, &self.clock);

            let response = UpdateResponse(
                Some(KeyIdExpiry(existing_value.key_id(), existing_expiry)),
                new_expiry,
                None,
            );
            return response;
        }
        UpdateResponse(None, None, value)
    }

    pub(crate) fn clear(&self) {
        self.store.clear();
    }

    pub(crate) fn stats_counter(&self) -> &Arc<ConcurrentStatsCounter> {
        &self.stats_counter
    }

    pub(crate) fn is_present(&self, key: &Key) -> bool {
        let maybe_value = self.store.get(key);
        maybe_value.is_some()
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
    use crate::cache::store::Store;
    use crate::cache::store::stored_value::StoredValue;
    use crate::cache::store::tests::setup::{Name, UnixEpochClock};
    use crate::cache::types::{TotalCapacity, TotalShards};

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

    const DEFAULT_CAPACITY: TotalCapacity = 16;
    const DEFAULT_SHARDS: TotalShards = 4;

    #[test]
    fn get_value_for_an_existing_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 1);

        let value = store.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn get_value_for_an_existing_key_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 1);

        let _ = store.get(&"topic");
        assert_eq!(1, store.stats_counter.hits());
    }

    #[test]
    fn put_a_key_value_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 1);
        assert_eq!(1, store.stats_counter.keys_added());
    }

    #[test]
    fn put_with_ttl() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put_with_ttl("topic", "microservices", 1, Duration::from_millis(5));

        let value = store.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn put_with_ttl_and_get_expire_after() {
        let clock = Box::new(UnixEpochClock {});
        let store = Store::new(clock.clone(), Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        let expire_after = store.put_with_ttl("topic", "microservices", 1, Duration::from_secs(5));
        assert_eq!(clock.now().add(Duration::from_secs(5)), expire_after);
    }

    #[test]
    fn put_with_ttl_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put_with_ttl("topic", "microservices", 1, Duration::from_millis(5));
        assert_eq!(1, store.stats_counter.keys_added());
    }

    #[test]
    fn put_with_ttl_and_get_the_value_of_an_expired_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put_with_ttl("topic", "microservices", 1, Duration::from_nanos(1));

        let value = store.get(&"topic");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_ref_for_an_existing_key_if_value_is_not_cloneable() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("name", Name { first: "John".to_string(), last: "Mcnamara".to_string() }, 1);

        let key_value_ref = store.get_ref(&"name");
        assert_eq!(&Name { first: "John".to_string(), last: "Mcnamara".to_string() }, key_value_ref.unwrap().value().value_ref());
    }

    #[test]
    fn get_value_ref_for_an_existing_key_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("name", Name { first: "John".to_string(), last: "Mcnamara".to_string() }, 1);

        let _ = store.get_ref(&"name");
        assert_eq!(1, store.stats_counter.hits());
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        let value: Option<&str> = store.get(&"non-existing");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_a_non_existing_key_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        let _value: Option<&str> = store.get(&"non-existing");
        assert_eq!(1, store.stats_counter.misses());
    }

    #[test]
    fn get_value_for_an_expired_key() {
        let store = Store::new(Box::new(FutureClock {}), Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);
        {
            let clock = SystemClock::boxed();
            store.store.insert("topic", StoredValue::expiring("microservices", 1, Duration::from_secs(5), &clock));
        }

        let value = store.get(&"topic");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_an_expired_key_and_increase_stats() {
        let store = Store::new(Box::new(FutureClock {}), Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);
        {
            let clock = SystemClock::boxed();
            store.store.insert("topic", StoredValue::expiring("microservices", 1, Duration::from_secs(5), &clock));
        }

        let _ = store.get(&"topic");
        assert_eq!(1, store.stats_counter.misses());
    }

    #[test]
    fn get_value_for_an_unexpired_key() {
        let store = Store::new(Box::new(FutureClock {}), Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);
        {
            let clock = SystemClock::boxed();
            store.store.insert("topic", StoredValue::expiring("microservices", 1, Duration::from_secs(15), &clock));
        }

        let value = store.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn get_value_for_an_unexpired_key_and_increase_stats() {
        let store = Store::new(Box::new(FutureClock {}), Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);
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
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 10);
        let key_id_expiry = store.delete(&"topic");

        let value = store.get(&"topic");
        assert_eq!(None, value);
        assert_eq!(10, key_id_expiry.unwrap().0);
    }

    #[test]
    fn delete_a_key_and_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 10);

        let _ = store.delete(&"topic");
        assert_eq!(1, store.stats_counter.keys_deleted());
    }

    #[test]
    fn delete_a_non_existing_key() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        let key_id_expiry = store.delete(&"non-existing");

        let value: Option<&str> = store.get(&"non-existing");
        assert_eq!(None, value);
        assert_eq!(None, key_id_expiry);
    }

    #[test]
    fn delete_a_non_existing_key_and_do_not_increase_stats() {
        let clock = SystemClock::boxed();
        let store = Store::<&str, &str>::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        let _ = store.delete(&"non-existing");
        assert_eq!(0, store.stats_counter.keys_deleted());
    }

    #[test]
    fn marks_a_key_deleted() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 10);
        store.mark_deleted(&"topic");

        let value = store.get(&"topic");
        assert_eq!(None, value);
    }

    #[test]
    fn update_time_to_live_for_non_existing_key() {
        let clock = SystemClock::boxed();
        let store: Arc<Store<&str, &str>> = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);
        let response = store.update(&"topic", None, Some(Duration::from_secs(5)), false);

        assert!(!response.did_update_happen());
    }

    #[test]
    fn update_time_to_live_for_an_existing_key() {
        let clock = Box::new(UnixEpochClock {});
        let store = Store::new(clock.clone(), Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 10);
        let update_response = store.update(&"topic", None, Some(Duration::from_secs(5)), false);
        assert!(update_response.existing_expiry().is_none());

        let key_value_ref = store.get_ref(&"topic").unwrap();
        let expected_expiry = clock.now().add(Duration::from_secs(5));
        assert_eq!(Some(expected_expiry), key_value_ref.value().expire_after());
    }

    #[test]
    fn update_time_to_live_for_an_existing_key_that_has_an_expiry() {
        let clock = Box::new(UnixEpochClock {});
        let store = Store::new(clock.clone(), Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put_with_ttl("topic", "microservices", 10, Duration::from_secs(300));
        store.update(&"topic", None, Some(Duration::from_secs(15)), false);

        let key_value_ref = store.get_ref(&"topic").unwrap();
        let expected_expiry = clock.now().add(Duration::from_secs(15));
        assert_eq!(Some(expected_expiry), key_value_ref.value().expire_after());
    }

    #[test]
    fn remove_time_to_live_for_an_existing_key_that_has_an_expiry() {
        let clock = Box::new(UnixEpochClock {});
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put_with_ttl("topic", "microservices", 10, Duration::from_secs(300));
        store.update(&"topic", None, None, true);

        let key_value_ref = store.get_ref(&"topic").unwrap();
        let expected_expiry = None;
        assert_eq!(expected_expiry, key_value_ref.value().expire_after());
    }

    #[test]
    fn update_value_for_an_existing() {
        let clock = Box::new(UnixEpochClock {});
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 10);
        store.update(&"topic", Some("cache"), None, false);

        let key_value_ref = store.get_ref(&"topic").unwrap();

        assert_eq!("cache", key_value_ref.value().value());
    }

    #[test]
    fn clear() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 1);

        store.clear();

        let value = store.get(&"topic");
        assert_eq!(None, value);
    }

    #[test]
    fn is_not_present() {
        let clock = SystemClock::boxed();
        let store: Arc<Store<&str, &str>> = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        let is_present = store.is_present(&"non-existing");
        assert!(!is_present)
    }

    #[test]
    fn is_present() {
        let clock = SystemClock::boxed();
        let store = Store::new(clock, Arc::new(ConcurrentStatsCounter::new()), DEFAULT_CAPACITY, DEFAULT_SHARDS);

        store.put("topic", "microservices", 1);

        let is_present = store.is_present(&"topic");
        assert!(is_present)
    }
}

#[cfg(test)]
mod update_response_tests {
    use std::ops::Add;
    use std::time::Duration;
    use crate::cache::clock::SystemClock;
    use crate::cache::store::{KeyIdExpiry, TypeOfExpiryUpdate, UpdateResponse};

    #[test]
    fn type_of_expiry_update_nothing() {
        let update_response: UpdateResponse<&str> = UpdateResponse(
            Some(KeyIdExpiry(1, None)),
            None,
            None
        );
        assert_eq!(TypeOfExpiryUpdate::Nothing, update_response.type_of_expiry_update());
    }

    #[test]
    fn type_of_expiry_update_added() {
        let new_expiry = SystemClock::boxed().now();
        let update_response: UpdateResponse<&str> = UpdateResponse(
            Some(KeyIdExpiry(1, None)),
            Some(new_expiry),
            None
        );
        assert_eq!(TypeOfExpiryUpdate::Added(1, new_expiry), update_response.type_of_expiry_update());
    }

    #[test]
    fn type_of_expiry_update_updated() {
        let old_expiry = SystemClock::boxed().now().add(Duration::from_secs(5));
        let new_expiry = SystemClock::boxed().now().add(Duration::from_secs(10));

        let update_response: UpdateResponse<&str> = UpdateResponse(
            Some(KeyIdExpiry(1, Some(old_expiry))),
            Some(new_expiry),
            None
        );
        assert_eq!(TypeOfExpiryUpdate::Updated(1, old_expiry, new_expiry), update_response.type_of_expiry_update());
    }

    #[test]
    fn type_of_expiry_update_deleted() {
        let old_expiry = SystemClock::boxed().now().add(Duration::from_secs(5));

        let update_response: UpdateResponse<&str> = UpdateResponse(
            Some(KeyIdExpiry(1, Some(old_expiry))),
            None,
            None
        );
        assert_eq!(TypeOfExpiryUpdate::Deleted(1, old_expiry), update_response.type_of_expiry_update());
    }

    #[test]
    fn type_of_expiry_update_nothing_given_both_the_expire_time_is_same() {
        let now = SystemClock::boxed().now();
        let old_expiry = now.add(Duration::from_secs(5));
        let new_expiry = now.add(Duration::from_secs(5));

        let update_response: UpdateResponse<&str> = UpdateResponse(
            Some(KeyIdExpiry(1, Some(old_expiry))),
            Some(new_expiry),
            None
        );
        assert_eq!(TypeOfExpiryUpdate::Nothing, update_response.type_of_expiry_update());
    }
}