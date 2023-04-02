use crate::cache::policy::cache_weight::WeightedKey;
use crate::cache::types::Weight;

pub(crate) struct Calculation;

impl Calculation {
    pub(crate) fn perform<Key, Value>(key: &Key, value: &Value) -> Weight {
        let key_size = std::mem::size_of_val(key);
        let value_size = std::mem::size_of_val(value);
        let weighted_key_size = std::mem::size_of::<WeightedKey<Key>>();

        (key_size + value_size + weighted_key_size) as Weight
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::config::weight_calculation::Calculation;

    #[test]
    fn perform_weight_calculation() {
        let key = "topic";
        let value = "microservices";
        let weight = Calculation::perform(&key, &value);

        assert_eq!(64, weight);
    }
}