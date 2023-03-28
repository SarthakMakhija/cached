use std::ops::Add;
use std::time::{Duration, SystemTime};

use crate::cache::clock::ClockType;

pub(crate) struct StoredValue<Value> {
    value: Value,
    expire_after: Option<SystemTime>,
}

impl<Value> StoredValue<Value> {
    pub(crate) fn never_expiring(value: Value) -> Self {
        StoredValue {
            value,
            expire_after: None,
        }
    }

    pub(crate) fn expiring(value: Value, time_to_live: Duration, clock: &ClockType) -> Self {
        StoredValue {
            value,
            expire_after: Some(clock.now().add(time_to_live)),
        }
    }

    pub(crate) fn is_alive(&self, clock: &ClockType) -> bool {
        if let Some(expire_after) = self.expire_after {
            return !clock.has_passed(&expire_after);
        }
        true
    }

    pub(crate) fn value_ref(&self) -> &Value {
        &self.value
    }
}

impl<Value> StoredValue<Value>
    where Value: Clone {

    pub(crate) fn value(&self) -> Value {
        self.value.clone()
    }
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
    fn expiration_time() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let stored_value = StoredValue::expiring("SSD", Duration::from_secs(10), &clock);

        assert!(stored_value.expire_after.unwrap().eq(&SystemTime::UNIX_EPOCH.add(Duration::from_secs(10))));
    }

    #[test]
    fn is_alive() {
        let stored_value = StoredValue::never_expiring("storage-engine");

        assert!(stored_value.is_alive(&SystemClock::boxed()));
    }

    #[test]
    fn is_not_alive() {
        let system_clock = SystemClock::boxed();
        let stored_value = StoredValue::expiring("storage-engine", Duration::from_secs(5), &system_clock);

        let future_clock: ClockType = Box::new(FutureClock {});
        assert!(!stored_value.is_alive(&future_clock));
    }
}