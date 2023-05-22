use std::ops::Add;
use std::time::{Duration, SystemTime};

use crate::cache::clock::ClockType;
use crate::cache::types::{ExpireAfter, KeyId};

/// StoredValue wraps the client provided Value and it is stored as a value in the [`crate::cache::store::Store`].
/// StoredValue encapsulates the `value`, `key_id`, the optional expiry of the key
/// and a flag to identify whether a key is soft deleted
pub struct StoredValue<Value> {
    value: Value,
    key_id: KeyId,
    expire_after: Option<ExpireAfter>,
    pub(crate) is_soft_deleted: bool,
}

impl<Value> StoredValue<Value> {
    pub(crate) fn never_expiring(value: Value, key_id: KeyId) -> Self {
        StoredValue {
            value,
            key_id,
            expire_after: None,
            is_soft_deleted: false,
        }
    }

    pub(crate) fn expiring(value: Value, key_id: KeyId, time_to_live: Duration, clock: &ClockType) -> Self {
        StoredValue {
            value,
            key_id,
            expire_after: Some(Self::calculate_expiry(time_to_live, clock)),
            is_soft_deleted: false,
        }
    }

    pub(crate) fn is_alive(&self, clock: &ClockType) -> bool {
        if self.is_soft_deleted {
            return false;
        }
        if let Some(expire_after) = self.expire_after {
            return !clock.has_passed(&expire_after);
        }
        true
    }

    /// Returns a reference to the value stored inside Store
    pub fn value_ref(&self) -> &Value { &self.value }

    // Returns the KeyId
    pub fn key_id(&self) -> KeyId { self.key_id }

    /// Returns the expiry of the key. It returns:
        /// None: if the expiry is not set
        /// Some: if the expiry is set
    pub fn expire_after(&self) -> Option<ExpireAfter> { self.expire_after }

    pub(crate) fn update(&mut self,
                         value: Option<Value>,
                         time_to_live: Option<Duration>,
                         remove_time_to_live: bool,
                         clock: &ClockType) -> Option<ExpireAfter> {

        if remove_time_to_live {
            self.expire_after = None;
        } else if let Some(time_to_live) = time_to_live {
            self.expire_after = Some(Self::calculate_expiry(time_to_live, clock));
        }

        if let Some(value) = value {
            self.value = value
        }
        self.expire_after
    }

    pub(crate) fn calculate_expiry(time_to_live: Duration, clock: &ClockType) -> SystemTime {
        clock.now().add(time_to_live)
    }
}

impl<Value> StoredValue<Value>
    where Value: Clone {
    /// Returns the cloned value stored inside Store
    pub fn value(&self) -> Value { self.value.clone() }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::time::{Duration, SystemTime};

    use crate::cache::clock::{ClockType, SystemClock};
    use crate::cache::store::stored_value::StoredValue;
    use crate::cache::store::stored_value::tests::setup::{FutureClock, UnixEpochClock};

    mod setup {
        use std::ops::Add;
        use std::time::{Duration, SystemTime};

        use crate::cache::clock::Clock;

        #[derive(Clone)]
        pub(crate) struct FutureClock;

        #[derive(Clone)]
        pub(crate) struct UnixEpochClock;

        impl Clock for FutureClock {
            fn now(&self) -> SystemTime {
                SystemTime::now().add(Duration::from_secs(10))
            }
        }

        impl Clock for UnixEpochClock {
            fn now(&self) -> SystemTime {
                SystemTime::UNIX_EPOCH
            }
        }
    }

    #[test]
    fn value_ref() {
        let stored_value = StoredValue::never_expiring("microservices", 1);
        let value = stored_value.value_ref();
        assert_eq!(&"microservices", value);
    }

    #[test]
    fn value() {
        let stored_value = StoredValue::never_expiring("microservices", 1);
        let value = stored_value.value();
        assert_eq!("microservices", value);
    }

    #[test]
    fn expiration_time() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let stored_value = StoredValue::expiring("SSD", 1, Duration::from_secs(10), &clock);

        assert!(stored_value.expire_after.unwrap().eq(&SystemTime::UNIX_EPOCH.add(Duration::from_secs(10))));
    }

    #[test]
    fn is_alive() {
        let stored_value = StoredValue::never_expiring("storage-engine", 1);

        assert!(stored_value.is_alive(&SystemClock::boxed()));
    }

    #[test]
    fn is_alive_if_not_soft_deleted() {
        let stored_value = StoredValue::never_expiring("storage-engine", 1);

        assert!(stored_value.is_alive(&SystemClock::boxed()));
    }

    #[test]
    fn is_not_alive_if_soft_deleted() {
        let mut stored_value = StoredValue::never_expiring("storage-engine", 1);
        stored_value.is_soft_deleted = true;

        assert!(!stored_value.is_alive(&SystemClock::boxed()));
    }

    #[test]
    fn is_not_alive_if_clock_has_passed() {
        let system_clock = SystemClock::boxed();
        let stored_value = StoredValue::expiring("storage-engine", 1, Duration::from_secs(5), &system_clock);

        let future_clock: ClockType = Box::new(FutureClock {});
        assert!(!stored_value.is_alive(&future_clock));
    }

    #[test]
    fn is_not_alive_if_clock_has_not_passed_but_is_soft_deleted() {
        let system_clock = SystemClock::boxed();
        let mut stored_value = StoredValue::expiring("storage-engine", 1, Duration::from_secs(5), &system_clock);
        stored_value.is_soft_deleted = true;

        assert!(!stored_value.is_alive(&system_clock));
    }

    #[test]
    fn update_and_remove_expiry() {
        let system_clock = SystemClock::boxed();
        let mut stored_value = StoredValue::expiring("storage-engine", 1, Duration::from_secs(5), &system_clock);

        stored_value.update(None, None, true, &system_clock);
        assert!(stored_value.expire_after.is_none());
    }

    #[test]
    fn update_the_value() {
        let system_clock = SystemClock::boxed();
        let mut stored_value = StoredValue::expiring("storage-engine", 1, Duration::from_secs(5), &system_clock);

        stored_value.update(Some("bitcask"), None, false, &system_clock);

        let value = stored_value.value();
        assert_eq!("bitcask", value);
    }

    #[test]
    fn update_the_expiry() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let mut stored_value = StoredValue::expiring("storage-engine", 1, Duration::from_secs(5), &clock);

        stored_value.update(None, Some(Duration::from_secs(300)), false, &clock);

        let expiry_after = stored_value.expire_after.unwrap();
        assert_eq!(clock.now().add(Duration::from_secs(300)), expiry_after);
    }

    #[test]
    fn update_value_and_expiry() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let mut stored_value = StoredValue::expiring("storage-engine", 1, Duration::from_secs(5), &clock);

        stored_value.update(Some("bitcask"), Some(Duration::from_secs(300)), false, &clock);

        let expiry_after = stored_value.expire_after.unwrap();
        assert_eq!(clock.now().add(Duration::from_secs(300)), expiry_after);

        let value = stored_value.value();
        assert_eq!("bitcask", value);
    }

    #[test]
    fn update_value_and_remove_expiry() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let mut stored_value = StoredValue::expiring("storage-engine", 1, Duration::from_secs(5), &clock);

        stored_value.update(Some("bitcask"), None, true, &clock);

        let expiry_after = stored_value.expire_after;
        assert!(expiry_after.is_none());

        let value = stored_value.value();
        assert_eq!("bitcask", value);
    }

    #[test]
    fn update_value_and_remove_expiry_given_updated_time_to_live_is_also_provided() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let mut stored_value = StoredValue::expiring("storage-engine", 1, Duration::from_secs(5), &clock);

        stored_value.update(Some("bitcask"), Some(Duration::from_secs(100)), true, &clock);

        let expiry_after = stored_value.expire_after;
        assert!(expiry_after.is_none());

        let value = stored_value.value();
        assert_eq!("bitcask", value);
    }
}