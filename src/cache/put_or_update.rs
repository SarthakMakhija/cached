use std::hash::Hash;
use std::time::Duration;

use crate::cache::config::WeightCalculationFn;
use crate::cache::errors::Errors;
use crate::cache::types::Weight;

/// `PutOrUpdateRequest` encapsulates the values for `put` or an `update` operation.
/// `PutOrUpdateRequest` is a parameter to `put_or_update` method of [`crate::cache::cached::CacheD`].
/// `PutOrUpdateRequest` allows put operation with `value`, `weight` and `time_to_live`.
/// `PutOrUpdateRequest` also provides flexibility in updating just the `value` or the `weight` or the `time_to_live` or all of these for an existing key.
/// `PutOrUpdateRequest` also allows removing the `time_to_live` against an existing key. Either of `time_to_live` or `remove_time_to_live` can be provided.
/// If `PutOrUpdateRequest` results in a `put` operation, the flag `remove_time_to_live` will have no significance.
pub struct PutOrUpdateRequest<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {
    pub(crate) key: Key,
    pub(crate) value: Option<Value>,
    pub(crate) weight: Option<Weight>,
    pub(crate) time_to_live: Option<Duration>,
    pub(crate) remove_time_to_live: bool,
}

impl<Key, Value> PutOrUpdateRequest<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {

    /// Returns the weight in a `PutOrUpdateRequest`.
    /// Weight is either the client provided weight or calculated from the value and presence/absence of `time_to_live`
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

/// PutOrUpdateRequestBuilder allows building `PutOrUpdateRequest`.
pub struct PutOrUpdateRequestBuilder<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {
    key: Key,
    value: Option<Value>,
    weight: Option<Weight>,
    time_to_live: Option<Duration>,
    remove_time_to_live: bool,
}

impl<Key, Value> PutOrUpdateRequestBuilder<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {

    /// Creates a new instance of `PutOrUpdateRequestBuilder` with the specified Key
    pub fn new(key: Key) -> PutOrUpdateRequestBuilder<Key, Value> {
        PutOrUpdateRequestBuilder {
            key,
            value: None,
            weight: None,
            time_to_live: None,
            remove_time_to_live: false,
        }
    }

    /// Sets the value in `PutOrUpdateRequestBuilder`
    pub fn value(mut self, value: Value) -> PutOrUpdateRequestBuilder<Key, Value> {
        self.value = Some(value);
        self
    }

    /// Sets the weight, weight must be greater than zero
    pub fn weight(mut self, weight: Weight) -> PutOrUpdateRequestBuilder<Key, Value> {
        assert!(weight > 0, "{}", Errors::KeyWeightGtZero("PutOrUpdate request builder"));
        self.weight = Some(weight);
        self
    }

    /// Sets the time_to_live
    pub fn time_to_live(mut self, time_to_live: Duration) -> PutOrUpdateRequestBuilder<Key, Value> {
        self.time_to_live = Some(time_to_live);
        self
    }

    /// Marks a flag to remove time_to_live. Either of `time_to_live` or `remove_time_to_live` can be provided
    pub fn remove_time_to_live(mut self) -> PutOrUpdateRequestBuilder<Key, Value> {
        self.remove_time_to_live = true;
        self
    }

    pub fn build(self) -> PutOrUpdateRequest<Key, Value> {
        let valid_put_or_update = self.value.is_some() || self.weight.is_some() || self.time_to_live.is_some() || self.remove_time_to_live;
        assert!(valid_put_or_update, "{}", Errors::InvalidPutOrUpdate);

        let both_time_to_live_and_remove_time_to_live = self.time_to_live.is_some() && self.remove_time_to_live;
        assert!(!both_time_to_live_and_remove_time_to_live, "{}", Errors::InvalidPutOrUpdateEitherTimeToLiveOrRemoveTimeToLive);

        PutOrUpdateRequest {
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
    use crate::cache::put_or_update::{PutOrUpdateRequest, PutOrUpdateRequestBuilder};

    #[test]
    #[should_panic]
    fn invalid_put_or_update_with_weight_as_zero() {
        let _: PutOrUpdateRequest<&str, &str> = PutOrUpdateRequestBuilder::new("topic").weight(0).build();
    }

    #[test]
    #[should_panic]
    fn invalid_put_or_update_with_only_key_specified() {
        let _: PutOrUpdateRequest<&str, &str> = PutOrUpdateRequestBuilder::new("topic").build();
    }

    #[test]
    #[should_panic]
    fn invalid_put_or_update_with_both_time_to_live_and_remove_time_to_live_specified() {
        let _: PutOrUpdateRequest<&str, &str> = PutOrUpdateRequestBuilder::new("topic").weight(10).remove_time_to_live().time_to_live(Duration::from_secs(9)).build();
    }

    #[test]
    fn put_or_update_request_with_key_value() {
        let put_or_update_request = PutOrUpdateRequestBuilder::new("topic").value("microservices").build();

        assert_eq!("topic", put_or_update_request.key);
        assert_eq!(Some("microservices"), put_or_update_request.value);
    }

    #[test]
    fn put_or_update_request_with_weight() {
        let put_or_update_request = PutOrUpdateRequestBuilder::new("topic").value("microservices").weight(10).build();

        assert_eq!(Some(10), put_or_update_request.weight);
    }

    #[test]
    fn put_or_update_request_with_time_to_live() {
        let put_or_update_request = PutOrUpdateRequestBuilder::new("topic").value("microservices").time_to_live(Duration::from_secs(10)).build();

        assert_eq!(Some(Duration::from_secs(10)), put_or_update_request.time_to_live);
    }

    #[test]
    fn put_or_update_request_remove_time_to_live() {
        let put_or_update_request = PutOrUpdateRequestBuilder::new("topic").value("microservices").remove_time_to_live().build();

        assert!(put_or_update_request.remove_time_to_live);
    }

    #[test]
    fn updated_weight_if_weight_is_provided() {
        let put_or_update_request = PutOrUpdateRequestBuilder::new("topic").weight(10).build();
        let weight_calculation_fn = Box::new(|_key: &&str, _value: &&str, _is_time_to_live_specified: IsTimeToLiveSpecified| 100);

        assert_eq!(Some(10), put_or_update_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_value_is_provided() {
        let put_or_update_request = PutOrUpdateRequestBuilder::new("topic").value("cached").build();
        let weight_calculation_fn = Box::new(|_key: &&str, value: &&str, _is_time_to_live_specified: IsTimeToLiveSpecified| value.len() as i64);

        assert_eq!(Some(6), put_or_update_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_weight_and_value_is_provided() {
        let put_or_update_request = PutOrUpdateRequestBuilder::new("topic").value("cached").weight(22).build();
        let weight_calculation_fn = Box::new(|_key: &&str, value: &&str, _is_time_to_live_specified: IsTimeToLiveSpecified| value.len() as i64);

        assert_eq!(Some(22), put_or_update_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_neither_weight_nor_value_is_provided() {
        let put_or_update_request = PutOrUpdateRequestBuilder::new("topic").remove_time_to_live().build();
        let weight_calculation_fn = Box::new(|_key: &&str, value: &&str, _is_time_to_live_specified: IsTimeToLiveSpecified| value.len() as i64);

        assert_eq!(None, put_or_update_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_only_time_to_live_is_provided() {
        let put_or_update_request: PutOrUpdateRequest<&str, &str> = PutOrUpdateRequestBuilder::new("topic").time_to_live(Duration::from_secs(500)).build();
        let weight_calculation_fn = Box::new(Calculation::perform);

        assert_eq!(None, put_or_update_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_value_is_provided_without_time_to_live() {
        let key: u64 = 100;
        let value: u64 = 1000;

        let put_or_update_request = PutOrUpdateRequestBuilder::new(key).value(value).build();
        let weight_calculation_fn = Box::new(Calculation::perform);

        assert_eq!(Some(40), put_or_update_request.updated_weight(&weight_calculation_fn));
    }

    #[test]
    fn updated_weight_if_value_is_provided_with_time_to_live() {
        let key: u64 = 100;
        let value: u64 = 1000;

        let put_or_update_request = PutOrUpdateRequestBuilder::new(key).value(value).time_to_live(Duration::from_secs(100)).build();
        let weight_calculation_fn = Box::new(Calculation::perform);

        assert_eq!(Some(64), put_or_update_request.updated_weight(&weight_calculation_fn));
    }
}