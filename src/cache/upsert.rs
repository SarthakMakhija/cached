use std::hash::Hash;
use std::time::Duration;

use crate::cache::types::Weight;

pub struct UpsertRequest<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {
    key: Key,
    value: Option<Value>,
    weight: Option<Weight>,
    time_to_live: Option<Duration>,
}

pub struct UpsertRequestBuilder<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone,
          Value: Send + Sync {
    key: Key,
    value: Option<Value>,
    weight: Option<Weight>,
    time_to_live: Option<Duration>,
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
        }
    }

    pub fn value(mut self, value: Value) -> UpsertRequestBuilder<Key, Value> {
        self.value = Some(value);
        self
    }

    pub fn weight(mut self, weight: Weight) -> UpsertRequestBuilder<Key, Value> {
        self.weight = Some(weight);
        self
    }

    pub fn time_to_live(mut self, time_to_live: Duration) -> UpsertRequestBuilder<Key, Value> {
        self.time_to_live = Some(time_to_live);
        self
    }

    pub fn build(self) -> UpsertRequest<Key, Value> {
        UpsertRequest {
            key: self.key,
            value: self.value,
            weight: self.weight,
            time_to_live: self.time_to_live,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use crate::cache::upsert::UpsertRequestBuilder;

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
}