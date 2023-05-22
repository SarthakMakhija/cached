use std::time::SystemTime;

use crate::cache::policy::cache_weight::WeightedKey;
use crate::cache::types::{IsTimeToLiveSpecified, KeyId, Weight};

const KEY_ID_SIZE: usize = std::mem::size_of::<KeyId>();
const SYSTEM_TIME_SIZE: usize = std::mem::size_of::<SystemTime>();

/// Calculation struct provides a function to perform weight calculation for a key/value pair.
/// The `perform` function is the default [`crate::cache::config::WeightCalculationFn`] in Cached implementation.
pub(crate) struct Calculation;

impl Calculation {
    /// Performs the weight calculation for the provided key/value pair.
    /// Total key/value weight = key_size + value_size + weighted_key_size + Optional<ttl_ticker_entry_size>
    /// Every key/value is stored in [`crate::store::Store`] and its size is denoted by `stored_value_size`
    /// A key id is generated for every key and its weight along with key_hash is stored in [`crate::cache::policy::cache_weight::CacheWeight`]
    /// If time_to_live is specified in the `put` operation, the key_id is also stored in [`crate::cache::expiration::TTLTicker`]
    /// Sum of all these sizes constitutes the weight of a key/value pair.
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