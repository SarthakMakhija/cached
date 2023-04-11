use std::ops::Add;
use std::time::Duration;

use crate::cache::clock::ClockType;
use crate::cache::types::{ExpireAfter, KeyId};

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
            expire_after: Some(clock.now().add(time_to_live)),
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

    pub fn value_ref(&self) -> &Value { &self.value }

    pub fn key_id(&self) -> KeyId { self.key_id }
}

impl<Value> StoredValue<Value>
    where Value: Clone {
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
}