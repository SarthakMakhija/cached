use std::time::SystemTime;
use crate::cache::policy::cache_weight::WeightedKey;
use crate::cache::types::{KeyId, Weight};

pub(crate) struct Calculation;

const KEY_ID_SIZE: usize = std::mem::size_of::<KeyId>();
const SYSTEM_TIME_SIZE: usize = std::mem::size_of::<SystemTime>();

impl Calculation {
    pub(crate) fn perform<Key, Value>(key: &Key, value: &Value) -> Weight {
        let (key_size, value_size) = Self::stored_value_size(key, value);
        (key_size + value_size + Self::weighted_key_size::<Key>() + Self::ttl_ticker_entry_size()) as Weight
    }

    fn stored_value_size<Key, Value>(key: &Key, value: &Value) -> (usize, usize) {
        (std::mem::size_of_val(key), std::mem::size_of_val(value))
    }

    fn weighted_key_size<Key>() -> usize { std::mem::size_of::<WeightedKey<Key>>() }

    fn ttl_ticker_entry_size() -> usize { KEY_ID_SIZE + SYSTEM_TIME_SIZE }
}

#[cfg(test)]
mod tests {
    use crate::cache::config::weight_calculation::Calculation;

    #[test]
    fn perform_weight_calculation() {
        let key = "topic";
        let value = "microservices";
        let weight = Calculation::perform(&key, &value);

        assert_eq!(88, weight);
    }
}