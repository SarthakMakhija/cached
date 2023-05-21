use std::hash::Hash;
use std::time::Duration;

use crate::cache::config::WeightCalculationFn;
use crate::cache::errors::Errors;
use crate::cache::types::Weight;

pub struct UpsertRequest<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {
    pub(crate) key: Key,
    pub(crate) value: Option<Value>,
    pub(crate) weight: Option<Weight>,
    pub(crate) time_to_live: Option<Duration>,
    pub(crate) remove_time_to_live: bool,
}

impl<Key, Value> UpsertRequest<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {
    pub(crate) fn updated_weight(&self, weight_calculation_fn: &WeightCalculationFn<Key, Value>) -> Option<Weight> {
        self.weight.or_else(|| self.value.as_ref().map(|value| {
            if self.time_to_live.is_some() {
                (weight_calculation_fn)(&self.key, value, true)
            } else {
                (weight_calculation_fn)(&self.key, value, false)
            }
        }))
    }
}

pub struct UpsertRequestBuilder<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {
    key: Key,
    value: Option<Value>,
    weight: Option<Weight>,
    time_to_live: Option<Duration>,
    remove_time_to_live: bool,
}

impl<Key, Value> UpsertRequestBuilder<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {
    pub fn new(key: Key) -> UpsertRequestBuilder<Key, Value> {
        UpsertRequestBuilder {
            key,
            value: None,
            weight: None,
            time_to_live: None,
            remove_time_to_live: false,
        }
    }

    pub fn value(mut self, value: Value) -> UpsertRequestBuilder<Key, Value> {
        self.value = Some(value);
        self
    }

    pub fn weight(mut self, weight: Weight) -> UpsertRequestBuilder<Key, Value> {
        assert!(weight > 0, "{}", Errors::KeyWeightGtZero("upsert request builder"));
        self.weight = Some(weight);
        self
    }

    pub fn time_to_live(mut self, time_to_live: Duration) -> UpsertRequestBuilder<Key, Value> {
        self.time_to_live = Some(time_to_live);
        self
    }

    pub fn remove_time_to_live(mut self) -> UpsertRequestBuilder<Key, Value> {
        self.remove_time_to_live = true;
        self
    }

    pub fn build(self) -> UpsertRequest<Key, Value> {
        let valid_upsert = self.value.is_some() || self.weight.is_some() || self.time_to_live.is_some() || self.remove_time_to_live;
        assert!(valid_upsert, "{}", Errors::InvalidUpsert);

        let both_time_to_live_and_remove_time_to_live = self.time_to_live.is_some() && self.remove_time_to_live;
        assert!(!both_time_to_live_and_remove_time_to_live, "{}", Errors::InvalidUpsertEitherTimeToLiveOrRemoveTimeToLive);

        UpsertRequest {
            key: self.key,
            value: self.value,
            weight: self.weight,
            time_to_live: self.time_to_live,
            remove_time_to_live: self.remove_time_to_live,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use crate::cache::config::weight_calculation::Calculation;

    use crate::cache::types::IsTimeToLiveSpecified;
    use crate::cache::upsert::{UpsertRequest, UpsertRequestBuilder};

    #[test]
    #[should_panic]
    fn invalid_upsert_with_weight_as_zero() {
        let _: UpsertRequest<&str, &str> = UpsertRequestBuilder::new("topic").weight(0).build();
    }

    #[test]
    #[should_panic]
    fn invalid_upsert_with_only_key_specified() {
        let _: UpsertRequest<&str, &str> = UpsertRequestBuilder::new("topic").build();
    }

    #[test]
    #[should_panic]
    fn invalid_upsert_with_both_time_to_live_and_remove_time_to_live_specified() {
        let _: UpsertRequest<&str, &str> = UpsertRequestBuilder::new("topic").weight(10).remove_time_to_live().time_to_live(Duration::from_secs(9)).build();
    }

    #[test]
    fn upsert_request_with_key_value() {
        let upsert_request = UpsertRequestBuilder::new("topic").value("microservices").build();

        assert_eq!("topic", upsert_request.key);
        assert_eq!(Some("microservices"), upsert_request.value);
    }

    #[test]
    fn upsert_request_with_weight() {
        let upsert_request = UpsertRequestBuilder::new("topic").value("microservices").weight(10).build();

        assert_eq!(Some(10), upsert_request.weight);
    }

    #[test]
    fn upsert_request_with_time_to_live() {
        let upsert_request = UpsertRequestBuilder::new("topic").value("microservices").time_to_live(Duration::from_secs(10)).build();

        assert_eq!(Some(Duration::from_secs(10)), upsert_request.time_to_live);
    }

    #[test]
    fn upsert_request_remove_time_to_live() {
        let upsert_request = UpsertRequestBuilder::new("topic").value("microservices").remove_time_to_live().build();

        assert!(upsert_request.remove_time_to_live);
    }

    #[test]
    fn updated_weight_if_weight_is_provided() {
        let upsert_request = UpsertRequestBuilder::new("topic").weight(10).build();
        let weight_calculation_fn = Box::new(|_key: &&str, _value: &&str, _is_time_to_live_specified: IsTimeToLiveSpecified| 100);

        assert_eq!(Some(10), upsert_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_value_is_provided() {
        let upsert_request = UpsertRequestBuilder::new("topic").value("cached").build();
        let weight_calculation_fn = Box::new(|_key: &&str, value: &&str, _is_time_to_live_specified: IsTimeToLiveSpecified| value.len() as i64);

        assert_eq!(Some(6), upsert_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_weight_and_value_is_provided() {
        let upsert_request = UpsertRequestBuilder::new("topic").value("cached").weight(22).build();
        let weight_calculation_fn = Box::new(|_key: &&str, value: &&str, _is_time_to_live_specified: IsTimeToLiveSpecified| value.len() as i64);

        assert_eq!(Some(22), upsert_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_neither_weight_nor_value_is_provided() {
        let upsert_request = UpsertRequestBuilder::new("topic").remove_time_to_live().build();
        let weight_calculation_fn = Box::new(|_key: &&str, value: &&str, _is_time_to_live_specified: IsTimeToLiveSpecified| value.len() as i64);

        assert_eq!(None, upsert_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_only_time_to_live_is_provided() {
        let upsert_request: UpsertRequest<&str, &str> = UpsertRequestBuilder::new("topic").time_to_live(Duration::from_secs(500)).build();
        let weight_calculation_fn = Box::new(Calculation::perform);

        assert_eq!(None, upsert_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_value_is_provided_without_time_to_live() {
        let key: u64 = 100;
        let value: u64 = 1000;

        let upsert_request = UpsertRequestBuilder::new(key).value(value).build();
        let weight_calculation_fn = Box::new(Calculation::perform);

        assert_eq!(Some(40), upsert_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_value_is_provided_with_time_to_live() {
        let key: u64 = 100;
        let value: u64 = 1000;

        let upsert_request = UpsertRequestBuilder::new(key).value(value).time_to_live(Duration::from_secs(100)).build();
        let weight_calculation_fn = Box::new(Calculation::perform);

        assert_eq!(Some(64), upsert_request.updated_weight(&weight_calculation_fn));
    }
}