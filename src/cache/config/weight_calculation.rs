use std::time::SystemTime;

use crate::cache::policy::cache_weight::WeightedKey;
use crate::cache::types::{IsTimeToLiveSpecified, KeyId, Weight};

pub(crate) struct Calculation;

const KEY_ID_SIZE: usize = std::mem::size_of::<KeyId>();
const SYSTEM_TIME_SIZE: usize = std::mem::size_of::<SystemTime>();

impl Calculation {
    pub(crate) fn perform<Key, Value>(key: &Key, value: &Value, time_to_live_specified: IsTimeToLiveSpecified) -> Weight {
        let (key_size, value_size) = Self::stored_value_size(key, value);
        let ttl_ticker_entry_size = if time_to_live_specified {
            Self::ttl_ticker_entry_size()
        } else {
            0
        };
        (key_size + value_size + Self::weighted_key_size::<Key>() + ttl_ticker_entry_size) as Weight
    }

    pub(crate) fn ttl_ticker_entry_size() -> usize { KEY_ID_SIZE + SYSTEM_TIME_SIZE }

    fn stored_value_size<Key, Value>(key: &Key, value: &Value) -> (usize, usize) {
        (std::mem::size_of_val(key), std::mem::size_of_val(value))
    }

    fn weighted_key_size<Key>() -> usize { std::mem::size_of::<WeightedKey<Key>>() }
}

#[cfg(test)]
mod tests {
    use crate::cache::config::weight_calculation::Calculation;

    #[test]
    fn perform_weight_calculation_without_time_to_live() {
        let key = "topic";
        let value = "microservices";
        let weight = Calculation::perform(&key, &value, false);

        assert_eq!(64, weight);
    }

    #[test]
    fn perform_weight_calculation_with_time_to_live_string() {
        let key = "topic";
        let value = "microservices";
        let weight = Calculation::perform(&key, &value, true);

        assert_eq!(88, weight);
    }

    #[test]
    fn perform_weight_calculation_without_time_to_live_u64() {
        let key: u64 = 100;
        let value: u64 = 200;
        let weight = Calculation::perform(&key, &value, false);

        assert_eq!(40, weight);
    }

    #[test]
    fn perform_weight_calculation_with_time_to_live() {
        let key: u64 = 100;
        let value: u64 = 200;
        let weight = Calculation::perform(&key, &value, true);

        assert_eq!(64, weight);
    }
}