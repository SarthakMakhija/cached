use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::command::acknowledgement::CommandAcknowledgement;
use crate::cache::command::command_executor::{CommandExecutor, CommandSendResult};
use crate::cache::command::CommandType;
use crate::cache::config::Config;
use crate::cache::errors::Errors;
use crate::cache::expiration::TTLTicker;
use crate::cache::key_description::KeyDescription;
use crate::cache::policy::admission_policy::AdmissionPolicy;
use crate::cache::pool::Pool;
use crate::cache::stats::{ConcurrentStatsCounter, StatsSummary};
use crate::cache::store::{Store, TypeOfExpiryUpdate};
use crate::cache::store::key_value_ref::KeyValueRef;
use crate::cache::store::stored_value::StoredValue;
use crate::cache::types::{KeyId, Weight};
use crate::cache::unique_id::increasing_id_generator::IncreasingIdGenerator;
use crate::cache::upsert::UpsertRequest;

//TODO: Lifetime 'static?
pub struct CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static {
    config: Config<Key, Value>,
    store: Arc<Store<Key, Value>>,
    command_executor: CommandExecutor<Key, Value>,
    admission_policy: Arc<AdmissionPolicy<Key>>,
    pool: Pool<AdmissionPolicy<Key>>,
    ttl_ticker: Arc<TTLTicker>,
    id_generator: IncreasingIdGenerator,
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static {
    pub fn new(config: Config<Key, Value>) -> Self {
        assert!(config.counters > 0);

        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = Store::new(config.clock.clone_box(), stats_counter.clone(), config.capacity, config.shards);
        let admission_policy = Arc::new(AdmissionPolicy::new(config.counters, config.total_cache_weight, stats_counter.clone()));
        let pool = Pool::new(config.access_pool_size, config.access_buffer_size, admission_policy.clone());
        let ttl_ticker = Self::ttl_ticker(&config, store.clone(), admission_policy.clone());
        let command_buffer_size = config.command_buffer_size;

        CacheD {
            config,
            store: store.clone(),
            command_executor: CommandExecutor::new(store, admission_policy.clone(), stats_counter, ttl_ticker.clone(), command_buffer_size),
            admission_policy,
            pool,
            ttl_ticker,
            id_generator: IncreasingIdGenerator::new(),
        }
    }

    pub fn put(&self, key: Key, value: Value) -> CommandSendResult {
        let weight = (self.config.weight_calculation_fn)(&key, &value);
        assert!(weight > 0, "{}", Errors::WeightCalculationGtZero);
        self.put_with_weight(key, value, weight)
    }

    pub fn put_with_weight(&self, key: Key, value: Value, weight: Weight) -> CommandSendResult {
        assert!(weight > 0, "{}", Errors::KeyWeightGtZero("put_with_weight"));
        self.command_executor.send(CommandType::Put(
            self.key_description(key, weight),
            value,
        ))
    }

    pub fn put_with_ttl(&self, key: Key, value: Value, time_to_live: Duration) -> CommandSendResult {
        let weight = (self.config.weight_calculation_fn)(&key, &value);
        assert!(weight > 0, "{}", Errors::WeightCalculationGtZero);
        self.command_executor.send(CommandType::PutWithTTL(
            self.key_description(key, weight), value, time_to_live)
        )
    }

    pub fn put_with_weight_and_ttl(&self, key: Key, value: Value, weight: Weight, time_to_live: Duration) -> CommandSendResult {
        assert!(weight > 0, "{}", Errors::KeyWeightGtZero("put_with_weight_and_ttl"));
        self.command_executor.send(CommandType::PutWithTTL(
            self.key_description(key, weight), value, time_to_live,
        ))
    }

    pub fn upsert(&self, request: UpsertRequest<Key, Value>) -> CommandSendResult {
        let updated_weight = request.updated_weight(&self.config.weight_calculation_fn);
        let (key, value, time_to_live)
            = (request.key, request.value, request.time_to_live);

        let update_response
            = self.store.update(&key, value, time_to_live, request.remove_time_to_live);

        if !update_response.did_update_happen() {
            let value = update_response.value();
            assert!(value.is_some(), "{}", Errors::UpsertValueMissing);
            assert!(updated_weight.is_some());

            let value = value.unwrap();
            let weight = updated_weight.unwrap();
            assert!(weight > 0, "{}", Errors::KeyWeightGtZero("upsert"));

            return if let Some(time_to_live) = time_to_live {
                self.put_with_weight_and_ttl(key, value, weight, time_to_live)
            } else {
                self.put_with_weight(key, value, weight)
            };
        }

        match update_response.type_of_expiry_update() {
            TypeOfExpiryUpdate::Added(key_id, expiry) =>
                self.ttl_ticker.put(key_id, expiry),
            TypeOfExpiryUpdate::Deleted(key_id, expiry) =>
                self.ttl_ticker.delete(&key_id, &expiry),
            TypeOfExpiryUpdate::Updated(key_id, old_expiry, new_expiry) =>
                self.ttl_ticker.update(key_id, &old_expiry, new_expiry),
            _ => {}
        }

        let key_id = update_response.key_id_or_panic();
        if let Some(weight) = updated_weight {
            assert!(weight > 0, "{}", Errors::KeyWeightGtZero("upsert"));
            return self.command_executor.send(CommandType::UpdateWeight(key_id, weight));
        }
        Ok(CommandAcknowledgement::accepted())
    }

    pub fn delete(&self, key: Key) -> CommandSendResult {
        self.store.mark_deleted(&key);
        self.command_executor.send(CommandType::Delete(key))
    }

    pub fn get_ref(&self, key: &Key) -> Option<KeyValueRef<'_, Key, StoredValue<Value>>> {
        if let Some(value_ref) = self.store.get_ref(key) {
            self.mark_key_accessed(key);
            return Some(value_ref);
        }
        None
    }

    pub fn map_get_ref<MapFn, MappedValue>(&self, key: &Key, map_fn: MapFn) -> Option<MappedValue>
        where MapFn: Fn(&StoredValue<Value>) -> MappedValue {
        if let Some(value_ref) = self.get_ref(key) {
            return Some(map_fn(value_ref.value()));
        }
        None
    }

    pub fn total_weight_used(&self) -> Weight {
        self.admission_policy.weight_used()
    }

    pub fn stats_summary(&self) -> StatsSummary {
        self.store.stats_counter().summary()
    }

    pub fn shutdown(&self) {
        let _ = self.command_executor.shutdown();
        self.admission_policy.shutdown();
        self.ttl_ticker.shutdown();

        self.store.clear();
        self.admission_policy.clear();
        self.ttl_ticker.clear();
    }

    fn mark_key_accessed(&self, key: &Key) {
        self.pool.add((self.config.key_hash_fn)(key));
    }

    fn key_description(&self, key: Key, weight: Weight) -> KeyDescription<Key> {
        let hash = (self.config.key_hash_fn)(&key);
        KeyDescription::new(key, self.id_generator.next(), hash, weight)
    }

    fn ttl_ticker(config: &Config<Key, Value>, store: Arc<Store<Key, Value>>, admission_policy: Arc<AdmissionPolicy<Key>>) -> Arc<TTLTicker> {
        let store_evict_hook = move |key| {
            store.delete(&key);
        };
        let cache_weight_evict_hook = move |key_id: &KeyId| {
            admission_policy.delete_with_hook(key_id, &store_evict_hook);
        };

        TTLTicker::new(config.ttl_config(), cache_weight_evict_hook)
    }
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + Clone + 'static {
    pub fn get(&self, key: &Key) -> Option<Value> {
        if let Some(value) = self.store.get(key) {
            self.mark_key_accessed(key);
            return Some(value);
        }
        None
    }

    pub fn map_get<MapFn, MappedValue>(&self, key: &Key, map_fn: MapFn) -> Option<MappedValue>
        where MapFn: Fn(Value) -> MappedValue {
        if let Some(value) = self.get(key) {
            return Some(map_fn(value));
        }
        None
    }

    pub fn multi_get<'a>(&self, keys: Vec<&'a Key>) -> HashMap<&'a Key, Option<Value>> {
        keys.into_iter().map(|key| (key, self.get(key))).collect::<HashMap<_, _>>()
    }

    pub fn multi_get_iterator<'a>(&'a self, keys: Vec<&'a Key>) -> MultiGetIterator<'a, Key, Value> {
        MultiGetIterator {
            cache: self,
            keys,
        }
    }

    pub fn multi_get_map_iterator<'a, MapFn, MappedValue>(&'a self, keys: Vec<&'a Key>, map_fn: MapFn) -> MultiGetMapIterator<'a, Key, Value, MapFn, MappedValue>
        where MapFn: Fn(Value) -> MappedValue {
        MultiGetMapIterator {
            iterator: MultiGetIterator {
                cache: self,
                keys,
            },
            map_fn,
        }
    }
}

pub struct MultiGetIterator<'a, Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + Clone + 'static {
    cache: &'a CacheD<Key, Value>,
    keys: Vec<&'a Key>,
}

impl<'a, Key, Value> Iterator for MultiGetIterator<'a, Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + Clone + 'static {
    type Item = Option<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.keys.is_empty() {
            return None;
        }
        let key = self.keys.get(0).unwrap();
        let value = self.cache.get(key);

        self.keys.remove(0);
        Some(value)
    }
}

pub struct MultiGetMapIterator<'a, Key, Value, MapFn, MappedValue>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + Clone + 'static,
          MapFn: Fn(Value) -> MappedValue, {
    iterator: MultiGetIterator<'a, Key, Value>,
    map_fn: MapFn,
}

impl<'a, Key, Value, MapFn, MappedValue> Iterator for MultiGetMapIterator<'a, Key, Value, MapFn, MappedValue>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + Clone + 'static,
          MapFn: Fn(Value) -> MappedValue, {
    type Item = Option<MappedValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|optional_value| {
            match optional_value {
                None => None,
                Some(value) => Some((self.map_fn)(value))
            }
        })
    }
}


#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::thread;
    use std::time::Duration;

    use crate::cache::cached::CacheD;
    use crate::cache::cached::tests::setup::UnixEpochClock;
    use crate::cache::clock::ClockType;
    use crate::cache::config::{ConfigBuilder, WeightCalculationFn};
    use crate::cache::stats::StatsType;
    use crate::cache::upsert::{UpsertRequest, UpsertRequestBuilder};

    #[derive(Eq, PartialEq, Debug)]
    struct Name {
        first: String,
        last: String,
    }

    mod setup {
        use std::time::SystemTime;

        use crate::cache::clock::Clock;

        #[derive(Clone)]
        pub(crate) struct UnixEpochClock;

        impl Clock for UnixEpochClock {
            fn now(&self) -> SystemTime {
                SystemTime::UNIX_EPOCH
            }
        }
    }

    fn test_config_builder() -> ConfigBuilder<&'static str, &'static str>{
        ConfigBuilder::new(100, 10, 100)
    }

    #[test]
    #[should_panic]
    fn shards_mut_be_power_of_2_and_greater_than_1() {
        let _: CacheD<&str, &str> = CacheD::new(test_config_builder().shards(1).build());
    }

    #[test]
    #[should_panic]
    fn weight_must_be_greater_than_zero_1() {
        let cached = CacheD::new(test_config_builder().build());
        let _ =
            cached.put_with_weight("topic", "microservices", 0).unwrap();
    }

    #[test]
    #[should_panic]
    fn weight_must_be_greater_than_zero_2() {
        let cached = CacheD::new(test_config_builder().build());
        let _ =
            cached.put_with_weight_and_ttl("topic", "microservices", 0, Duration::from_secs(5)).unwrap();
    }

    #[test]
    #[should_panic]
    fn weight_calculation_fn_must_return_weight_greater_than_zero_1() {
        let weight_calculation: Box<WeightCalculationFn<&str, &str>> = Box::new(|_key, _value| 0);
        let cached = CacheD::new(test_config_builder().weight_calculation_fn(weight_calculation).build());
        let _ =
            cached.put("topic", "microservices").unwrap();
    }

    #[test]
    #[should_panic]
    fn weight_calculation_fn_must_return_weight_greater_than_zero_2() {
        let weight_calculation: Box<WeightCalculationFn<&str, &str>> = Box::new(|_key, _value| 0);
        let cached = CacheD::new(test_config_builder().weight_calculation_fn(weight_calculation).build());
        let _ =
            cached.put_with_ttl("topic", "microservices", Duration::from_secs(5)).unwrap();
    }

    #[test]
    #[should_panic]
    fn upsert_results_in_put_value_must_be_present() {
        let cached = CacheD::new(test_config_builder().build());
        let upsert: UpsertRequest<&str, &str> = UpsertRequestBuilder::new("store").build();
        let _ = cached.upsert(upsert);
    }

    #[test]
    #[should_panic]
    fn upsert_results_in_put_weight_calculation_fn_must_return_weight_greater_than_zero() {
        let weight_calculation: Box<WeightCalculationFn<&str, &str>> = Box::new(|_key, _value| 0);
        let cached = CacheD::new(test_config_builder().weight_calculation_fn(weight_calculation).build());

        let upsert = UpsertRequestBuilder::new("store").value("cached").build();
        let _ = cached.upsert(upsert);
    }

    #[tokio::test]
    #[should_panic]
    async fn upsert_results_in_update_weight_calculation_fn_must_return_weight_greater_than_zero() {
        let weight_calculation: Box<WeightCalculationFn<&str, &str>> = Box::new(|_key, _value| 0);
        let cached = CacheD::new(test_config_builder().weight_calculation_fn(weight_calculation).build());
        cached.put("topic", "microservices").unwrap().handle().await;

        let upsert = UpsertRequestBuilder::new("topic").value("cached").build();
        let _ = cached.upsert(upsert);
    }


    #[tokio::test]
    #[should_panic]
    async fn upsert_results_in_update_weight_must_be_greater_than_zero() {
        let cached = CacheD::new(test_config_builder().build());
        cached.put("topic", "microservices").unwrap().handle().await;

        let upsert = UpsertRequestBuilder::new("topic").value("cached").weight(0).build();
        let _ = cached.upsert(upsert);
    }

    #[tokio::test]
    async fn put_a_key_value_with_weight() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put_with_weight("topic", "microservices", 50).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_eq!(Some(50), cached.admission_policy.weight_of(&key_id));
    }

    #[tokio::test]
    async fn put_a_key_value_with_ttl() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put_with_ttl("topic", "microservices", Duration::from_secs(120)).unwrap();
        acknowledgement.handle().await;

        let value = cached.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[tokio::test]
    async fn put_a_key_value_with_ttl_and_ttl_ticker_evicts_it() {
        let cached = CacheD::new(test_config_builder().shards(2).ttl_tick_duration(Duration::from_millis(10)).build());

        let acknowledgement =
            cached.put_with_ttl("topic", "microservices", Duration::from_millis(20)).unwrap();
        acknowledgement.handle().await;

        let value = cached.get(&"topic");
        assert_eq!(Some("microservices"), value);

        thread::sleep(Duration::from_millis(20));
        assert_eq!(None, cached.get(&"topic"));
    }

    #[tokio::test]
    async fn put_a_key_value_with_weight_and_ttl() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put_with_weight_and_ttl("topic", "microservices", 10, Duration::from_secs(120)).unwrap();
        acknowledgement.handle().await;

        let value = cached.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn get_value_ref_for_a_non_existing_key() {
        let cached: CacheD<&str, &str> = CacheD::new(test_config_builder().build());

        let value = cached.get_ref(&"non-existing");
        assert!(value.is_none());
    }

    #[test]
    fn get_value_ref_for_a_non_existing_key_and_attempt_to_map_it() {
        let cached: CacheD<&str, &str> = CacheD::new(test_config_builder().build());

        let value = cached.map_get_ref(&"non_existing", |stored_value| stored_value.value_ref().to_uppercase());
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn get_value_ref_for_an_existing_key() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        assert_eq!(&"microservices", value.unwrap().value().value_ref());
    }

    #[tokio::test]
    async fn get_value_ref_for_an_existing_key_and_map_it() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let value = cached.map_get_ref(&"topic", |stored_value| stored_value.value_ref().to_uppercase());
        assert_eq!("MICROSERVICES", value.unwrap());
    }

    #[tokio::test]
    async fn get_value_for_an_existing_key() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let value = cached.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[tokio::test]
    async fn get_value_for_an_existing_key_and_map_it() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let value = cached.map_get(&"topic", |value| value.to_uppercase());
        assert_eq!("MICROSERVICES", value.unwrap());
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let cached: CacheD<&str, &str> = CacheD::new(test_config_builder().build());

        let value = cached.get(&"non-existing");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_a_non_existing_key_and_attempt_to_map_it() {
        let cached: CacheD<&str, &str> = CacheD::new(test_config_builder().build());

        let value = cached.map_get(&"topic", |value| value.to_uppercase());
        assert_eq!(None, value);
    }

    #[tokio::test]
    async fn get_value_ref_for_an_existing_key_if_value_is_not_cloneable() {
        let cached = CacheD::new(ConfigBuilder::new(100, 10, 1000).build());

        let acknowledgement =
            cached.put("name", Name { first: "John".to_string(), last: "Mcnamara".to_string() }).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"name");
        assert_eq!(&Name { first: "John".to_string(), last: "Mcnamara".to_string() }, value.unwrap().value().value_ref());
    }

    #[tokio::test]
    async fn delete_a_key() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let key_id = {
            let key_value_ref = cached.get_ref(&"topic").unwrap();
            key_value_ref.value().key_id()
        };

        let acknowledgement =
            cached.delete("topic").unwrap();
        acknowledgement.handle().await;

        let value = cached.get(&"topic");
        assert_eq!(None, value);
        assert!(!cached.admission_policy.contains(&key_id));
    }

    #[tokio::test]
    async fn get_access_frequency() {
        let cached = CacheD::new(ConfigBuilder::new(10, 10, 1000).access_pool_size(1).access_buffer_size(3).build());

        let acknowledgement_topic =
            cached.put("topic", "microservices").unwrap();
        let acknowledgement_disk =
            cached.put("disk", "SSD").unwrap();

        acknowledgement_topic.handle().await;
        acknowledgement_disk.handle().await;

        cached.get(&"topic");
        cached.get(&"disk");
        cached.get(&"topic");
        thread::sleep(Duration::from_secs(2));

        let hasher = &(cached.config.key_hash_fn);
        let policy = cached.admission_policy;

        assert_eq!(2, policy.estimate(hasher(&"topic")));
        assert_eq!(1, policy.estimate(hasher(&"disk")));
    }

    #[tokio::test]
    async fn get_multiple_keys() {
        let cached = CacheD::new(ConfigBuilder::new(100, 10, 1000).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.put("disk", "SSD").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.put("cache", "in-memory").unwrap();
        acknowledgement.handle().await;

        let values = cached.multi_get(vec![&"topic", &"non-existing", &"cache", &"disk"]);

        assert_eq!(&Some("microservices"), values.get(&"topic").unwrap());
        assert_eq!(&None, values.get(&"non-existing").unwrap());
        assert_eq!(&Some("in-memory"), values.get(&"cache").unwrap());
        assert_eq!(&Some("SSD"), values.get(&"disk").unwrap());
    }

    #[tokio::test]
    async fn get_multiple_keys_via_an_iterator() {
        let cached = CacheD::new(ConfigBuilder::new(100, 10, 1000).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.put("disk", "SSD").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.put("cache", "in-memory").unwrap();
        acknowledgement.handle().await;

        let mut iterator = cached.multi_get_iterator(vec![&"topic", &"non-existing", &"cache", &"disk"]);
        assert_eq!(Some("microservices"), iterator.next().unwrap());
        assert_eq!(None, iterator.next().unwrap());
        assert_eq!(Some("in-memory"), iterator.next().unwrap());
        assert_eq!(Some("SSD"), iterator.next().unwrap());
        assert_eq!(None, iterator.next());
    }

    #[tokio::test]
    async fn map_multiple_keys_via_an_iterator() {
        let cached = CacheD::new(ConfigBuilder::new(100, 10, 1000).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.put("disk", "ssd").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.put("cache", "in-memory").unwrap();
        acknowledgement.handle().await;

        let mut iterator = cached.multi_get_map_iterator(vec![&"topic", &"non-existing", &"cache", &"disk"], |value| value.to_uppercase());
        assert_eq!(Some("MICROSERVICES".to_string()), iterator.next().unwrap());
        assert_eq!(None, iterator.next().unwrap());
        assert_eq!(Some("IN-MEMORY".to_string()), iterator.next().unwrap());
        assert_eq!(Some("SSD".to_string()), iterator.next().unwrap());
        assert_eq!(None, iterator.next());
    }

    #[tokio::test]
    async fn upsert_a_non_existing_key_value() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.upsert(UpsertRequestBuilder::new("topic").value("microservices").build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();

        assert_eq!("microservices", stored_value.value());
    }

    #[tokio::test]
    async fn upsert_a_non_existing_key_value_with_weight() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.upsert(UpsertRequestBuilder::new("topic").value("microservices").weight(33).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_eq!(Some(33), cached.admission_policy.weight_of(&key_id));
    }

    #[tokio::test]
    async fn upsert_a_non_existing_key_value_with_time_to_live() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.upsert(UpsertRequestBuilder::new("topic").value("microservices").time_to_live(Duration::from_secs(10)).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();

        assert_eq!(Some(clock.now().add(Duration::from_secs(10))), stored_value.expire_after());
        assert_eq!("microservices", stored_value.value());
    }

    #[tokio::test]
    async fn update_the_value_of_an_existing_key() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.upsert(UpsertRequestBuilder::new("topic").value("storage engine").build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();

        assert_eq!("storage engine", stored_value.value());
    }

    #[tokio::test]
    async fn update_the_weight_of_an_existing_key() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.upsert(UpsertRequestBuilder::new("topic").weight(29).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_eq!(Some(29), cached.pool.get_buffer_consumer().weight_of(&key_id));
    }

    #[tokio::test]
    async fn update_the_time_to_live_of_an_existing_key() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.upsert(UpsertRequestBuilder::new("topic").time_to_live(Duration::from_secs(100)).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_eq!(Some(clock.now().add(Duration::from_secs(100))), stored_value.expire_after());
        assert_eq!(stored_value.expire_after(), cached.ttl_ticker.get(&key_id, &stored_value.expire_after().unwrap()));
    }

    #[tokio::test]
    async fn remove_the_time_to_live_of_an_existing_key() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put_with_ttl("topic", "microservices", Duration::from_secs(10)).unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.upsert(UpsertRequestBuilder::new("topic").remove_time_to_live().build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();

        assert_eq!("microservices", stored_value.value());
        assert_eq!(None, stored_value.expire_after());
    }

    #[tokio::test]
    async fn add_the_time_to_live_of_an_existing_key() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.upsert(UpsertRequestBuilder::new("topic").time_to_live(Duration::from_secs(120)).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_eq!(Some(clock.now().add(Duration::from_secs(120))), stored_value.expire_after());
        assert_eq!(stored_value.expire_after(), cached.ttl_ticker.get(&key_id, &stored_value.expire_after().unwrap()));
    }

    #[tokio::test]
    async fn total_weight_used() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put_with_weight("topic", "microservices", 50).unwrap();
        acknowledgement.handle().await;

        assert_eq!(50, cached.total_weight_used());
    }

    #[tokio::test]
    async fn shutdown() {
        let cached = CacheD::new(test_config_builder().build());

        cached.put_with_weight("topic", "microservices", 50).unwrap().handle().await;
        cached.put("cache", "cached").unwrap().handle().await;

        cached.shutdown();
        thread::sleep(Duration::from_secs(1));

        let put_result = cached.put("storage", "cached");
        assert!(put_result.is_err());

        assert_eq!(0, cached.total_weight_used());
        assert_eq!(None, cached.get(&"topic"));
        assert_eq!(None, cached.get(&"cache"));
    }

    #[tokio::test]
    async fn stats_summary() {
        let cached = CacheD::new(test_config_builder().build());

        cached.put_with_weight("topic", "microservices", 50).unwrap().handle().await;
        cached.put_with_weight("cache", "cached", 10).unwrap().handle().await;
        cached.delete("cache").unwrap().handle().await;

        let _ = cached.get(&"topic");
        let _ = cached.get(&"cache");

        let summary = cached.stats_summary();
        assert_eq!(1, summary.get(&StatsType::CacheMisses).unwrap());
        assert_eq!(1, summary.get(&StatsType::CacheHits).unwrap());
        assert_eq!(60, summary.get(&StatsType::WeightAdded).unwrap());
        assert_eq!(10, summary.get(&StatsType::WeightRemoved).unwrap());
        assert_eq!(2, summary.get(&StatsType::KeysAdded).unwrap());
        assert_eq!(1, summary.get(&StatsType::KeysDeleted).unwrap());

        assert_eq!(0, summary.get(&StatsType::KeysRejected).unwrap());
        assert_eq!(0, summary.get(&StatsType::AccessAdded).unwrap());
        assert_eq!(0, summary.get(&StatsType::AccessDropped).unwrap());
    }
}