use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::command::command_executor::{CommandExecutor, CommandSendResult};
use crate::cache::command::CommandType;
use crate::cache::config::Config;
use crate::cache::key_description::KeyDescription;
use crate::cache::policy::admission_policy::AdmissionPolicy;
use crate::cache::pool::Pool;
use crate::cache::stats::ConcurrentStatsCounter;
use crate::cache::store::key_value_ref::KeyValueRef;
use crate::cache::store::Store;
use crate::cache::store::stored_value::StoredValue;
use crate::cache::types::Weight;
use crate::cache::unique_id::increasing_id_generator::IncreasingIdGenerator;

//TODO: Lifetime 'static?
pub struct CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static {
    config: Config<Key, Value>,
    store: Arc<Store<Key, Value>>,
    command_executor: CommandExecutor<Key, Value>,
    pool: Pool<AdmissionPolicy<Key>>,
    id_generator: IncreasingIdGenerator,
    stats_counter: Arc<ConcurrentStatsCounter>,
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static {
    pub fn new(config: Config<Key, Value>) -> Self {
        assert!(config.counters > 0);

        let store = Store::new((config.clock).clone_box());
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let admission_policy = Arc::new(AdmissionPolicy::new(config.counters, config.total_cache_weight, stats_counter.clone()));
        let pool = Pool::new(config.access_pool_size, config.access_buffer_size, admission_policy.clone());
        let command_buffer_size = config.command_buffer_size;

        CacheD {
            config,
            store: store.clone(),
            command_executor: CommandExecutor::new(store, admission_policy, command_buffer_size),
            pool,
            id_generator: IncreasingIdGenerator::new(),
            stats_counter
        }
    }

    pub fn put(&self, key: Key, value: Value) -> CommandSendResult {
        let weight = (self.config.weight_calculation_fn)(&key, &value);
        self.put_with_weight(key, value, weight)
    }

    pub fn put_with_weight(&self, key: Key, value: Value, weight: Weight) -> CommandSendResult {
        assert!(weight > 0);
        self.command_executor.send(CommandType::Put(
            self.key_description(key, weight),
            value,
        ))
    }

    pub fn put_with_ttl(&self, key: Key, value: Value, time_to_live: Duration) -> CommandSendResult {
        let weight = (self.config.weight_calculation_fn)(&key, &value);
        self.command_executor.send(CommandType::PutWithTTL(
            self.key_description(key, weight),
            value,
            time_to_live)
        )
    }

    pub fn put_with_weight_and_ttl(&self, key: Key, value: Value, weight: Weight, time_to_live: Duration) -> CommandSendResult {
        assert!(weight > 0);
        self.command_executor.send(CommandType::PutWithTTL(
            self.key_description(key, weight),
            value,
            time_to_live,
        ))
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
        self.stats_counter.found_a_miss();
        None
    }

    fn mark_key_accessed(&self, key: &Key) {
        self.pool.add((self.config.key_hash_fn)(key));
        self.stats_counter.found_a_hit();
    }

    fn key_description(&self, key: Key, weight: Weight) -> KeyDescription<Key> {
        let hash = (self.config.key_hash_fn)(&key);
        KeyDescription::new(key, self.id_generator.next(), hash, weight)
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
        self.stats_counter.found_a_miss();
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


#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use crate::cache::cached::CacheD;
    use crate::cache::config::ConfigBuilder;

    #[derive(Eq, PartialEq, Debug)]
    struct Name {
        first: String,
        last: String,
    }

    #[tokio::test]
    async fn put_a_key_value_with_weight() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement =
            cached.put_with_weight("topic", "microservices", 50).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_eq!(Some(50), cached.pool.get_buffer_consumer().weight_of(&key_id));
    }

    #[tokio::test]
    async fn put_a_key_value_with_ttl() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement =
            cached.put_with_ttl("topic", "microservices", Duration::from_secs(120)).unwrap();
        acknowledgement.handle().await;

        let value = cached.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[tokio::test]
    async fn put_a_key_value_with_weight_and_ttl() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement =
            cached.put_with_weight_and_ttl("topic", "microservices", 10,Duration::from_secs(120)).unwrap();
        acknowledgement.handle().await;

        let value = cached.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn get_value_ref_for_a_non_existing_key() {
        let cached: CacheD<&str, &str> = CacheD::new(ConfigBuilder::new().counters(10).build());

        let value = cached.get_ref(&"non-existing");
        assert!(value.is_none());
    }

    #[test]
    fn get_value_ref_for_a_non_existing_key_and_increase_stats() {
        let cached: CacheD<&str, &str> = CacheD::new(ConfigBuilder::new().counters(10).build());

        let _ = cached.get_ref(&"non-existing");
        assert_eq!(1, cached.stats_counter.misses());
    }

    #[tokio::test]
    async fn get_value_ref_for_an_existing_key() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        assert_eq!(&"microservices", value.unwrap().value().value_ref());
    }

    #[tokio::test]
    async fn get_value_ref_for_an_existing_key_and_increase_stats() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let _ = cached.get_ref(&"topic");
        assert_eq!(1, cached.stats_counter.hits());
    }

    #[tokio::test]
    async fn get_value_for_an_existing_key() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let value = cached.get(&"topic");
        assert_eq!(Some("microservices"), value);
    }

    #[tokio::test]
    async fn get_value_for_an_existing_key_and_increase_stats() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let _ = cached.get(&"topic");
        assert_eq!(1, cached.stats_counter.hits());
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let cached: CacheD<&str, &str> = CacheD::new(ConfigBuilder::new().counters(10).build());

        let value = cached.get(&"non-existing");
        assert_eq!(None, value);
    }

    #[test]
    fn get_value_for_a_non_existing_key_and_increase_stats() {
        let cached: CacheD<&str, &str> = CacheD::new(ConfigBuilder::new().counters(10).build());

        let value = cached.get(&"non-existing");
        assert_eq!(None, value);
        assert_eq!(1, cached.stats_counter.misses());
    }

    #[tokio::test]
    async fn get_value_ref_for_an_existing_key_if_value_is_not_cloneable() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement =
            cached.put("name", Name { first: "John".to_string(), last: "Mcnamara".to_string() }).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"name");
        assert_eq!(&Name { first: "John".to_string(), last: "Mcnamara".to_string() }, value.unwrap().value().value_ref());
    }

    #[tokio::test]
    async fn delete_a_key() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

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
        assert!(!cached.pool.get_buffer_consumer().contains(&key_id));
    }

    #[tokio::test]
    async fn get_access_frequency() {
        let cached = CacheD::new(ConfigBuilder::new().access_pool_size(1).access_buffer_size(3).counters(10).build());

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
        let policy = cached.pool.get_buffer_consumer();

        assert_eq!(2, policy.estimate(hasher(&"topic")));
        assert_eq!(1, policy.estimate(hasher(&"disk")));
    }

    #[tokio::test]
    async fn get_multiple_keys() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

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
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

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
}