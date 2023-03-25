use std::ops::Add;
use std::time::{Duration, SystemTime};

use crate::cache::clock::ClockType;

pub(crate) struct StoredValue<Value>
    where Value: Clone {
    value: Value,
    expire_after: Option<SystemTime>,
}

impl<Value> StoredValue<Value>
    where Value: Clone {
    pub(crate) fn never_expiring(value: Value) -> Self {
        return StoredValue {
            value,
            expire_after: None,
        };
    }

    pub(crate) fn expiring(value: Value, time_to_live: Duration, clock: &ClockType) -> Self {
        return StoredValue {
            value,
            expire_after: Some(clock.now().add(time_to_live)),
        };
    }

    pub(crate) fn is_alive(&self, clock: &ClockType) -> bool {
        if let Some(expire_after) = self.expire_after {
            return !clock.has_passed(&expire_after);
        }
        return true;
    }

    pub(crate) fn value(&self) -> Value {
        return self.value.clone();
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::time::{Duration, SystemTime};

    use crate::cache::clock::{ClockType, SystemClock};
    use crate::cache::store::stored_value::StoredValue;
    use crate::cache::store::stored_value::tests::setup::{UnixEpochClock, FutureClock};

    mod setup {
        use std::ops::Add;
        use std::time::{Duration, SystemTime};

        use crate::cache::clock::Clock;

        pub(crate) struct FutureClock;

        pub(crate) struct UnixEpochClock;

        impl Clock for FutureClock {
            fn now(&self) -> SystemTime {
                return SystemTime::now().add(Duration::from_secs(10));
            }
        }

        impl Clock for UnixEpochClock {
            fn now(&self) -> SystemTime {
                return SystemTime::UNIX_EPOCH;
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
        assert_eq!(false, stored_value.is_alive(&future_clock));
    }
}