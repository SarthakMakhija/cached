use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::atomic::Ordering::Acquire;
use std::time::Duration;

use log::info;

use crate::cache::command::acknowledgement::CommandAcknowledgement;
use crate::cache::command::command_executor::{CommandExecutor, CommandSendResult, shutdown_result};
use crate::cache::command::CommandType;
use crate::cache::config::Config;
use crate::cache::config::weight_calculation::Calculation;
use crate::cache::errors::Errors;
use crate::cache::expiration::TTLTicker;
use crate::cache::key_description::KeyDescription;
use crate::cache::policy::admission_policy::AdmissionPolicy;
use crate::cache::pool::Pool;
use crate::cache::put_or_update::PutOrUpdateRequest;
use crate::cache::stats::{ConcurrentStatsCounter, StatsSummary};
use crate::cache::store::{Store, TypeOfExpiryUpdate};
use crate::cache::store::key_value_ref::KeyValueRef;
use crate::cache::store::stored_value::StoredValue;
use crate::cache::types::{KeyId, Weight};
use crate::cache::unique_id::increasing_id_generator::IncreasingIdGenerator;

/// `CacheD` is an LFU based, memory bound cache. Cached provides various methods including `put`, `put_with_weight`, `get`, `get_ref`, `map_get_ref`, `delete`, `put_or_update`.
/// The core abstractions that `CacheD` interacts with include:
/// [`crate::cache::store::Store`]: `Store` holds the key/value mapping.
/// [`crate::cache::command::command_executor::CommandExecutor`]: `CommandExecutor` executes various commands of type [`crate::cache::command::CommandType`]. Each write operation results in a command to `CommandExecutor`.
/// [`crate::cache::policy::admission_policy::AdmissionPolicy`]: `AdmissionPolicy` maintains the weight of each key in the cache and takes a decision on whether a key should be admitted
/// [`crate::cache::expiration::TTLTicker`]: `TTLTicker` removes the expired keys.
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
    is_shutting_down: AtomicBool,
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static {
    /// Creates a new instance of `Cached` with the provided [`crate::cache::config::Config`]
    pub fn new(config: Config<Key, Value>) -> Self {
        assert!(config.counters > 0);

        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = Store::new(config.clock.clone_box(), stats_counter.clone(), config.capacity, config.shards);
        let admission_policy = Arc::new(AdmissionPolicy::new(config.counters, config.cache_weight_config(), stats_counter.clone()));
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
            is_shutting_down: AtomicBool::new(false),
        }
    }

    /// Puts the key/value pair in the cacheD instance and returns an instance of [` crate::cache::command::command_executor::CommandSendResult`] to the clients.
    /// Weight is calculated by the weight calculation function provided as a part of `Config`.
    /// `put` is not an immediate operation. Every invocation of `put` results in [`crate::cache::command::CommandType::Put`] to the `CommandExecutor`
    /// `CommandExecutor` in turn delegates to the `AdmissionPolicy` to perform the put operation.
    /// `AdmissionPolicy` may accept or reject the key/value pair depending on the available cache weight.
    /// See [`crate::cache::policy::admission_policy::AdmissionPolicy`] for more details.
    /// Since, `put` is not an immediate operation, clients can `await` on the response to get the [`crate::cache::command::CommandStatus`]
    /// ```
    /// use cached::cache::cached::CacheD;
    /// use cached::cache::command::CommandStatus;
    /// use cached::cache::config::ConfigBuilder;
    /// #[tokio::main]
    ///  async fn main() {
    ///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());
    ///     let status = cached.put("topic", "microservices").unwrap().handle().await;
    ///     assert_eq!(CommandStatus::Accepted, status);
    /// }
    /// ```
    pub fn put(&self, key: Key, value: Value) -> CommandSendResult {
        let weight = (self.config.weight_calculation_fn)(&key, &value, false);
        assert!(weight > 0, "{}", Errors::WeightCalculationGtZero);
        self.put_with_weight(key, value, weight)
    }

    /// Puts the key/value pair in the cacheD instance and returns an instance of [` crate::cache::command::command_executor::CommandSendResult`] to the clients.
    /// Weight is provided by the clients.
    /// `put_with_weight` is not an immediate operation. Every invocation of `put_with_weight` results in [`crate::cache::command::CommandType::Put`] to the `CommandExecutor`
    /// `CommandExecutor` in turn delegates to the `AdmissionPolicy` to perform the put operation.
    /// `AdmissionPolicy` may accept or reject the key/value pair depending on the available cache weight.
    /// See [`crate::cache::policy::admission_policy::AdmissionPolicy`] for more details.
    /// Since, `put_with_weight` is not an immediate operation, clients can `await` on the response to get the [`crate::cache::command::CommandStatus`]
    /// ```
    /// use cached::cache::cached::CacheD;
    /// use cached::cache::command::CommandStatus;
    /// use cached::cache::config::ConfigBuilder;
    /// #[tokio::main]
    ///  async fn main() {
    ///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());
    ///     let status = cached.put_with_weight("topic", "microservices", 50).unwrap().handle().await;
    ///     assert_eq!(CommandStatus::Accepted, status);
    ///     assert_eq!(50, cached.total_weight_used());
    /// }
    /// ```
    pub fn put_with_weight(&self, key: Key, value: Value, weight: Weight) -> CommandSendResult {
        if self.is_shutting_down() { return shutdown_result(); }

        assert!(weight > 0, "{}", Errors::KeyWeightGtZero("put_with_weight"));
        self.command_executor.send(CommandType::Put(
            self.key_description(key, weight),
            value,
        ))
    }

    /// Puts the key/value pair with `time_to_live` in the cacheD instance and returns an instance of [` crate::cache::command::command_executor::CommandSendResult`] to the clients.
    /// Weight is calculated by the weight calculation function provided as a part of `Config`.
    /// `put_with_ttl` is not an immediate operation. Every invocation of `put_with_ttl` results in [`crate::cache::command::CommandType::PutWithTTL`] to the `CommandExecutor`
    /// `CommandExecutor` in turn delegates to the `AdmissionPolicy` to perform the put operation.
    /// `AdmissionPolicy` may accept or reject the key/value pair depending on the available cache weight.
    /// See [`crate::cache::policy::admission_policy::AdmissionPolicy`] for more details.
    /// Since, `put_with_ttl` is not an immediate operation, clients can `await` on the response to get the [`crate::cache::command::CommandStatus`]
    /// ```
    /// use cached::cache::cached::CacheD;
    /// use cached::cache::command::CommandStatus;
    /// use cached::cache::config::ConfigBuilder;
    /// use std::time::Duration;
    /// #[tokio::main]
    ///  async fn main() {
    ///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());
    ///     let status = cached.put_with_ttl("topic", "microservices", Duration::from_secs(120)).unwrap().handle().await;
    ///     assert_eq!(CommandStatus::Accepted, status);
    /// }
    /// ```
    pub fn put_with_ttl(&self, key: Key, value: Value, time_to_live: Duration) -> CommandSendResult {
        if self.is_shutting_down() { return shutdown_result(); }

        let weight = (self.config.weight_calculation_fn)(&key, &value, true);
        assert!(weight > 0, "{}", Errors::WeightCalculationGtZero);
        self.command_executor.send(CommandType::PutWithTTL(
            self.key_description(key, weight), value, time_to_live)
        )
    }

    /// Puts the key/value pair with `time_to_live` in the cacheD instance and returns an instance of [` crate::cache::command::command_executor::CommandSendResult`] to the clients.
    /// Weight is provided by the clients.
    /// `put_with_weight_and_ttl` is not an immediate operation. Every invocation of `put_with_weight_and_ttl` results in [`crate::cache::command::CommandType::PutWithTTL`] to the `CommandExecutor`
    /// `CommandExecutor` in turn delegates to the `AdmissionPolicy` to perform the put operation.
    /// `AdmissionPolicy` may accept or reject the key/value pair depending on the available cache weight.
    /// See [`crate::cache::policy::admission_policy::AdmissionPolicy`] for more details.
    /// Since, `put_with_weight_and_ttl` is not an immediate operation, clients can `await` on the response to get the [`crate::cache::command::CommandStatus`]
    /// ```
    /// use cached::cache::cached::CacheD;
    /// use cached::cache::command::CommandStatus;
    /// use cached::cache::config::ConfigBuilder;
    /// use std::time::Duration;
    /// #[tokio::main]
    ///  async fn main() {
    ///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());
    ///     let status = cached.put_with_weight_and_ttl("topic", "microservices", 50, Duration::from_secs(120)).unwrap().handle().await;
    ///     assert_eq!(50, cached.total_weight_used());
    ///     assert_eq!(CommandStatus::Accepted, status);
    /// }
    /// ```
    pub fn put_with_weight_and_ttl(&self, key: Key, value: Value, weight: Weight, time_to_live: Duration) -> CommandSendResult {
        if self.is_shutting_down() { return shutdown_result(); }

        assert!(weight > 0, "{}", Errors::KeyWeightGtZero("put_with_weight_and_ttl"));
        self.command_executor.send(CommandType::PutWithTTL(
            self.key_description(key, weight), value, time_to_live,
        ))
    }

    /// Performs a put or an update operation. `PutOrUpdateRequest` is a convenient way to perform put or update operation.
    /// `put_or_update` attempts to perform the update operation on [`crate::cache::store::Store`], this update includes value, weight or time_to_live
    /// If the update operation is successful then the changes made to `TTLTicker` and `AdmissionPolicy`, if applicable.
    /// If the update is not successful then a `put` operation is performed.
    /// ```
    /// use cached::cache::cached::CacheD;
    /// use cached::cache::command::CommandStatus;
    /// use cached::cache::config::ConfigBuilder;
    /// use cached::cache::put_or_update::PutOrUpdateRequestBuilder;
    /// #[tokio::main]
    ///  async fn main() {
    ///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());
    ///     let status = cached.put("topic", "microservices").unwrap().handle().await;
    ///     assert_eq!(CommandStatus::Accepted, status);
    ///     let _ = cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").value("Cached").build()).unwrap().handle().await;
    ///     let value = cached.get(&"topic");
    ///     assert_eq!(Some("Cached"), value);
    /// }
    /// ```
    pub fn put_or_update(&self, request: PutOrUpdateRequest<Key, Value>) -> CommandSendResult {
        if self.is_shutting_down() { return shutdown_result(); }

        let updated_weight = request.updated_weight(&self.config.weight_calculation_fn);
        let (key, value, time_to_live)
            = (request.key, request.value, request.time_to_live);

        let update_response
            = self.store.update(&key, value, time_to_live, request.remove_time_to_live);

        if !update_response.did_update_happen() {
            let value = update_response.value();
            assert!(value.is_some(), "{}", Errors::PutOrUpdateValueMissing);
            assert!(updated_weight.is_some());

            let value = value.unwrap();
            let weight = updated_weight.unwrap();
            assert!(weight > 0, "{}", Errors::KeyWeightGtZero("PutOrUpdate"));

            return if let Some(time_to_live) = time_to_live {
                self.put_with_weight_and_ttl(key, value, weight, time_to_live)
            } else {
                self.put_with_weight(key, value, weight)
            };
        }

        let key_id = update_response.key_id_or_panic();
        let existing_weight = self.admission_policy.weight_of(&key_id).unwrap_or(0);

        let updated_weight = match update_response.type_of_expiry_update() {
            TypeOfExpiryUpdate::Added(key_id, expiry) => {
                self.ttl_ticker.put(key_id, expiry);
                updated_weight.or_else(|| Some(existing_weight + Calculation::ttl_ticker_entry_size() as i64))
            }
            TypeOfExpiryUpdate::Deleted(key_id, expiry) => {
                self.ttl_ticker.delete(&key_id, &expiry);
                updated_weight.or_else(|| Some(existing_weight - Calculation::ttl_ticker_entry_size() as i64))
            }
            TypeOfExpiryUpdate::Updated(key_id, old_expiry, new_expiry) => {
                self.ttl_ticker.update(key_id, &old_expiry, new_expiry);
                updated_weight
            }
            _ => updated_weight,
        };

        if let Some(weight) = updated_weight {
            assert!(weight > 0, "{}", Errors::KeyWeightGtZero("PutOrUpdate"));
            return self.command_executor.send(CommandType::UpdateWeight(key_id, weight));
        }
        Ok(CommandAcknowledgement::accepted())
    }

    /// Deletes the key/value pair from the instance of `CacheD`. Delete is a 2 step process:
    /// 1) Marks the key as deleted in the [`crate::cache::store::Store`]. So, any `get` operations on the key would return None.
    ///    This step is immediate.
    /// 2) Sends a [`crate::cache::command::CommandType::Delete`] to the `CommandExecutor` which cause the key weight to be removed from `AdmissionPolicy`.
    ///    This step may happen at a later point in time.
    /// Since, `delete` is not an immediate operation, clients can `await` on the response to get the [`crate::cache::command::CommandStatus`]
    /// ```
    /// use cached::cache::cached::CacheD;
    /// use cached::cache::command::CommandStatus;
    /// use cached::cache::config::ConfigBuilder;
    /// #[tokio::main]
    ///  async fn main() {
    ///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());
    ///     let status = cached.put("topic", "microservices").unwrap().handle().await;
    ///     assert_eq!(CommandStatus::Accepted, status);
    ///     let _ = cached.delete(&"topic").unwrap().handle().await;
    ///     assert_eq!(None, cached.get(&"topic"));
    /// }
    /// ```
    pub fn delete(&self, key: Key) -> CommandSendResult {
        if self.is_shutting_down() { return shutdown_result(); }

        self.store.mark_deleted(&key);
        self.command_executor.send(CommandType::Delete(key))
    }

    /// Returns an optional reference to the key/value present in the instance of `Cached`.
    /// The reference is wrapped in [`crate::cache::store::key_value_ref::KeyValueRef`].
    /// KeyValueRef contains DashMap's Ref [`dashmap::mapref::one::Ref`] which internally holds a `RwLockReadGuard` for the shard.
    /// Any time `get_ref` method is invoked, the `Store` returns `Option<KeyValueRef<'_, Key, StoredValue<Value>>>`.
    /// If the key is present in the `Store`, `get_ref` will return `Some<KeyValueRef<'_, Key, StoredValue<Value>>>`.
    /// Hence, the invocation of `get_ref` will hold a lock against the shard that contains the key (within the scope of its usage).
    /// ```
    /// use cached::cache::cached::CacheD;
    /// use cached::cache::command::CommandStatus;
    /// use cached::cache::config::ConfigBuilder;
    /// #[tokio::main]
    ///  async fn main() {
    ///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());
    ///     let status = cached.put("topic", "microservices").unwrap().handle().await;
    ///     assert_eq!(CommandStatus::Accepted, status);
    ///     let value = cached.get_ref(&"topic");
    ///     let value_ref = value.unwrap();
    ///     let stored_value = value_ref.value();
    ///     assert_eq!("microservices", stored_value.value());
    /// }
    /// ```
    pub fn get_ref(&self, key: &Key) -> Option<KeyValueRef<'_, Key, StoredValue<Value>>> {
        if self.is_shutting_down() { return None; }

        if let Some(value_ref) = self.store.get_ref(key) {
            self.mark_key_accessed(key);
            return Some(value_ref);
        }
        None
    }

    /// Returns an optional MappedValue for key present in the instance of `Cached`.
    /// The parameter `map_fn` is an instance of `Fn` that takes a reference to [`crate::cache::store::stored_value::StoredValue`] and returns any MappedValue
    /// This is an extension to `get_ref` method.
    /// If the key is present in `Cached`, it returns `Some(MappedValue)`, else returns `None`.
    /// ```
    /// use cached::cache::cached::CacheD;
    /// use cached::cache::command::CommandStatus;
    /// use cached::cache::config::ConfigBuilder;
    /// #[tokio::main]
    ///  async fn main() {
    ///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());
    ///     let status = cached.put("topic", "microservices").unwrap().handle().await;
    ///     assert_eq!(CommandStatus::Accepted, status);
    ///     let value = cached.map_get_ref(&"topic", |stored_value| stored_value.value_ref().to_uppercase());
    ///     assert_eq!("MICROSERVICES", value.unwrap());
    /// }
    /// ```
    pub fn map_get_ref<MapFn, MappedValue>(&self, key: &Key, map_fn: MapFn) -> Option<MappedValue>
        where MapFn: Fn(&StoredValue<Value>) -> MappedValue {
        if self.is_shutting_down() { return None; }

        if let Some(value_ref) = self.get_ref(key) {
            return Some(map_fn(value_ref.value()));
        }
        None
    }

    /// Returns the total weight used in the cache.
    pub fn total_weight_used(&self) -> Weight {
        self.admission_policy.weight_used()
    }

    /// Returns an instance of [`crate::cache::stats::StatsSummary`]
    /// ```
    /// use cached::cache::cached::CacheD;
    /// use cached::cache::config::ConfigBuilder;
    /// use cached::cache::stats::StatsType;
    /// #[tokio::main]
    ///  async fn main() {
    ///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 200).build());
    ///     let _ = cached.put("topic", "microservices").unwrap().handle().await;
    ///     let _ = cached.put("cache", "cached").unwrap().handle().await;
    ///     let _ = cached.get(&"topic");
    ///     let _ = cached.get(&"cache");
    ///     let stats_summary = cached.stats_summary();
    ///     assert_eq!(2, stats_summary.get(&StatsType::CacheHits).unwrap());
    /// }
    /// ```
    pub fn stats_summary(&self) -> StatsSummary {
        self.store.stats_counter().summary()
    }

    /// Shuts down the cache.
    /// Shutdown involves the following:
    /// 1) Marking `is_shutting_down` to true
    /// 2) Sending a [`crate::cache::command::CommandType::Shutdown`] to the [`crate::cache::command::command_executor::CommandExecutor`]
    /// 3) Shutting down [`crate::cache::expiration::TTLTicker`]
    /// 4) Clearing the data inside [`crate::cache::store::Store`]
    /// 5) Clearing the data inside [`crate::cache::policy::admission_policy::AdmissionPolicy`]
    /// 6) Clearing the data inside [`crate::cache::expiration::TTLTicker`]
    /// Any attempt to perform an operation after tge `CacheD` instance is shutdown, will result in an error.
    /// However, there is race condition sort of a scenario here.
    /// Consider that `shutdown()` and `put()` on an instance of `Cached` are invoked at the same time.
    /// Both these operations result in sending different commands to the `CommandExecutor`.
    /// Somehow, the `Shutdown` command goes in before the `put` command.
    /// This also means that the client could have performed `await` operation on response from `put`.
    /// It becomes important to finish the future of the `put` command that has come in at the same time `shutdown` was invoked.
    /// This is how `shutdown` in `CommandExecutor` is handled, it finishes all the futures in the pipiline that are placed after the `Shutdown` command.
    /// Refer to the documentation inside [`crate::cache::command::command_executor::CommandExecutor`].
    pub fn shutdown(&self) {
        if self.is_shutting_down.compare_exchange(false, true, Ordering::Release, Ordering::Relaxed).is_ok() {
            info!("Starting to shutdown cached");
            let _ = self.command_executor.shutdown();
            self.admission_policy.shutdown();
            self.ttl_ticker.shutdown();

            self.store.clear();
            self.admission_policy.clear();
            self.ttl_ticker.clear();
        }
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

    fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Acquire)
    }
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + Clone + 'static {
    pub fn get(&self, key: &Key) -> Option<Value> {
        if self.is_shutting_down() { return None; }

        if let Some(value) = self.store.get(key) {
            self.mark_key_accessed(key);
            return Some(value);
        }
        None
    }

    pub fn map_get<MapFn, MappedValue>(&self, key: &Key, map_fn: MapFn) -> Option<MappedValue>
        where MapFn: Fn(Value) -> MappedValue {
        if self.is_shutting_down() { return None; }

        if let Some(value) = self.get(key) {
            return Some(map_fn(value));
        }
        None
    }

    pub fn multi_get<'a>(&self, keys: Vec<&'a Key>) -> HashMap<&'a Key, Option<Value>> {
        if self.is_shutting_down() { return HashMap::new(); }

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
        if self.keys.is_empty() || self.cache.is_shutting_down() {
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
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::cache::cached::CacheD;
    use crate::cache::config::{ConfigBuilder, WeightCalculationFn};
    use crate::cache::put_or_update::{PutOrUpdateRequest, PutOrUpdateRequestBuilder};
    use crate::cache::stats::StatsType;

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

    fn test_config_builder() -> ConfigBuilder<&'static str, &'static str> {
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
        let weight_calculation: Box<WeightCalculationFn<&str, &str>> = Box::new(|_key, _value, _is_time_to_live_specified| 0);
        let cached = CacheD::new(test_config_builder().weight_calculation_fn(weight_calculation).build());
        let _ =
            cached.put("topic", "microservices").unwrap();
    }

    #[test]
    #[should_panic]
    fn weight_calculation_fn_must_return_weight_greater_than_zero_2() {
        let weight_calculation: Box<WeightCalculationFn<&str, &str>> = Box::new(|_key, _value, _is_time_to_live_specified| 0);
        let cached = CacheD::new(test_config_builder().weight_calculation_fn(weight_calculation).build());
        let _ =
            cached.put_with_ttl("topic", "microservices", Duration::from_secs(5)).unwrap();
    }

    #[test]
    #[should_panic]
    fn put_or_update_results_in_put_value_must_be_present() {
        let cached = CacheD::new(test_config_builder().build());
        let put_or_update: PutOrUpdateRequest<&str, &str> = PutOrUpdateRequestBuilder::new("store").build();
        let _ = cached.put_or_update(put_or_update);
    }

    #[test]
    #[should_panic]
    fn put_or_update_results_in_put_with_weight_calculation_fn_must_return_weight_greater_than_zero() {
        let weight_calculation: Box<WeightCalculationFn<&str, &str>> = Box::new(|_key, _value, _is_time_to_live_specified| 0);
        let cached = CacheD::new(test_config_builder().weight_calculation_fn(weight_calculation).build());

        let put_or_update = PutOrUpdateRequestBuilder::new("store").value("cached").build();
        let _ = cached.put_or_update(put_or_update);
    }

    #[tokio::test]
    #[should_panic]
    async fn put_or_update_results_in_update_with_weight_calculation_fn_must_return_weight_greater_than_zero() {
        let weight_calculation: Box<WeightCalculationFn<&str, &str>> = Box::new(|_key, _value, _is_time_to_live_specified| 0);
        let cached = CacheD::new(test_config_builder().weight_calculation_fn(weight_calculation).build());
        cached.put("topic", "microservices").unwrap().handle().await;

        let put_or_update = PutOrUpdateRequestBuilder::new("topic").value("cached").build();
        let _ = cached.put_or_update(put_or_update);
    }


    #[tokio::test]
    #[should_panic]
    async fn put_or_update_results_in_update_with_weight_must_be_greater_than_zero() {
        let cached = CacheD::new(test_config_builder().build());
        cached.put("topic", "microservices").unwrap().handle().await;

        let put_or_update = PutOrUpdateRequestBuilder::new("topic").value("cached").weight(0).build();
        let _ = cached.put_or_update(put_or_update);
    }

    #[tokio::test]
    async fn put_a_key_value_without_weight_and_ttl() {
        let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());

        let key: u64 = 100;
        let value: u64 = 1000;

        let acknowledgement =
            cached.put(key, value).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&100);
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!(1000, stored_value.value());
        assert_eq!(Some(40), cached.admission_policy.weight_of(&key_id));
    }

    #[tokio::test]
    async fn put_a_key_value_without_weight_with_ttl() {
        let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());

        let key: u64 = 100;
        let value: u64 = 1000;

        let acknowledgement =
            cached.put_with_ttl(key, value, Duration::from_secs(300)).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&100);
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!(1000, stored_value.value());
        assert_eq!(Some(64), cached.admission_policy.weight_of(&key_id));
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
    async fn get_value_for_an_existing_key_if_value_is_not_cloneable_by_passing_an_arc() {
        let cached = CacheD::new(ConfigBuilder::new(100, 10, 1000).build());

        let acknowledgement =
            cached.put("name", Arc::new(Name { first: "John".to_string(), last: "Mcnamara".to_string() })).unwrap();
        acknowledgement.handle().await;

        let value = cached.get(&"name").unwrap();
        assert_eq!("John".to_string(), value.first);
        assert_eq!("Mcnamara".to_string(), value.last);
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
        cached.get(&"disk"); //will cause the drain of the buffer which will have 2 accesses of topic and one for disk

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
    async fn get_multiple_keys_via_an_iterator_given_value_is_not_cloneable() {
        let cached = CacheD::new(ConfigBuilder::new(100, 10, 1000).build());

        let acknowledgement =
            cached.put("captain", Arc::new(Name { first: "John".to_string(), last: "Mcnamara".to_string() })).unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.put("vice-captain", Arc::new(Name { first: "Martin".to_string(), last: "Trolley".to_string() })).unwrap();
        acknowledgement.handle().await;

        let mut iterator = cached.multi_get_iterator(vec![&"captain", &"vice-captain", &"disk"]);
        assert_eq!("John", iterator.next().unwrap().unwrap().first);
        assert_eq!("Martin", iterator.next().unwrap().unwrap().first);
        assert_eq!(None, iterator.next().unwrap());
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
    async fn total_weight_used() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put_with_weight("topic", "microservices", 50).unwrap();
        acknowledgement.handle().await;

        assert_eq!(50, cached.total_weight_used());
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

#[cfg(test)]
mod shutdown_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    use async_std::future::timeout;
    use tokio::time::sleep;

    use crate::cache::cached::CacheD;
    use crate::cache::config::ConfigBuilder;
    use crate::cache::put_or_update::PutOrUpdateRequestBuilder;

    fn test_config_builder() -> ConfigBuilder<&'static str, &'static str> {
        ConfigBuilder::new(100, 10, 100)
    }

    #[test]
    fn put_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.shutdown();

        let put_result = cached.put("storage", "cached");
        assert!(put_result.is_err());
    }

    #[test]
    fn put_with_weight_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.shutdown();

        let put_result = cached.put_with_weight("storage", "cached", 10);
        assert!(put_result.is_err());
    }

    #[test]
    fn put_with_ttl_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.shutdown();

        let put_result = cached.put_with_ttl("storage", "cached", Duration::from_secs(5));
        assert!(put_result.is_err());
    }

    #[test]
    fn put_with_weight_and_ttl_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.shutdown();

        let put_result = cached.put_with_weight_and_ttl("storage", "cached", 10, Duration::from_secs(5));
        assert!(put_result.is_err());
    }

    #[test]
    fn delete_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.shutdown();

        let delete_result = cached.delete("storage");
        assert!(delete_result.is_err());
    }

    #[test]
    fn put_or_update_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.shutdown();

        let put_or_update_result = cached.put_or_update(PutOrUpdateRequestBuilder::new("storage").weight(10).build());
        assert!(put_or_update_result.is_err());
    }

    #[tokio::test]
    async fn get_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.put("storage", "cached").unwrap().handle().await;
        cached.shutdown();

        let get_result = cached.get(&"storage");
        assert_eq!(None, get_result);
    }

    #[tokio::test]
    async fn get_ref_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.put("storage", "cached").unwrap().handle().await;
        cached.shutdown();

        let get_result = cached.get_ref(&"storage");
        assert!(get_result.is_none());
    }

    #[tokio::test]
    async fn map_get_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.put("storage", "cached").unwrap().handle().await;
        cached.shutdown();

        let get_result = cached.map_get(&"storage", |value| value.to_uppercase());
        assert!(get_result.is_none());
    }

    #[tokio::test]
    async fn map_get_ref_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.put("storage", "cached").unwrap().handle().await;
        cached.shutdown();

        let get_result = cached.map_get_ref(&"storage", |stored_value| stored_value.value_ref().to_uppercase());
        assert!(get_result.is_none());
    }

    #[tokio::test]
    async fn multi_get_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.put("storage", "cached").unwrap().handle().await;
        cached.put("topic", "microservices").unwrap().handle().await;

        cached.shutdown();

        let multi_get_result = cached.multi_get(vec![&"storage", &"topic"]);
        assert!(multi_get_result.is_empty());
    }

    #[tokio::test]
    async fn multi_get_iterator_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.put("storage", "cached").unwrap().handle().await;
        cached.put("topic", "microservices").unwrap().handle().await;

        cached.shutdown();

        let mut iterator = cached.multi_get_iterator(vec![&"storage", &"topic"]);
        assert!(iterator.next().is_none());
    }

    #[tokio::test]
    async fn multi_get_map_iterator_after_shutdown() {
        let cached = CacheD::new(test_config_builder().build());
        cached.put("storage", "cached").unwrap().handle().await;
        cached.put("topic", "microservices").unwrap().handle().await;

        cached.shutdown();

        let mut iterator = cached.multi_get_map_iterator(vec![&"storage", &"topic"], |value| { value.to_uppercase() });
        assert!(iterator.next().is_none());
    }

    #[tokio::test]
    async fn shutdown() {
        let cached = CacheD::new(test_config_builder().build());

        cached.put_with_weight("topic", "microservices", 50).unwrap().handle().await;
        cached.put("cache", "cached").unwrap().handle().await;

        cached.shutdown();
        assert!(cached.is_shutting_down.load(Ordering::Acquire));

        let put_result = cached.put("storage", "cached");
        assert!(put_result.is_err());

        assert_eq!(0, cached.total_weight_used());
        assert_eq!(None, cached.get(&"topic"));
        assert_eq!(None, cached.get(&"cache"));
    }

    #[tokio::test]
    async fn concurrent_shutdown() {
        let cached = Arc::new(CacheD::new(test_config_builder().build()));
        cached.put_with_weight("topic", "microservices", 50).unwrap().handle().await;
        cached.put("cache", "cached").unwrap().handle().await;

        let thread_handles = (1..=10).map(|_| {
            thread::spawn({
                let cached = cached.clone();
                move || {
                    cached.shutdown();
                }
            })
        }).collect::<Vec<_>>();
        for handle in thread_handles {
            handle.join().unwrap();
        }

        assert!(cached.is_shutting_down.load(Ordering::Acquire));

        let put_result = cached.put("storage", "cached");
        assert!(put_result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_not_block_on_shutdown() {
        let config_builder = ConfigBuilder::new(1000, 100, 1_000_000);
        let cached = Arc::new(CacheD::new(config_builder.build()));

        let task_handles = (1..=50).map(|index| {
            let cached_clone = cached.clone();
            tokio::spawn(
                async move {
                    let start_index = index * 10;
                    let end_index = start_index + 10;

                    for count in start_index..end_index {
                        let put_result = cached_clone.put(count, count * 10);
                        if let Ok(result) = put_result {
                            timeout(Duration::from_secs(1), result.handle()).await.unwrap();
                        }
                        sleep(Duration::from_millis(2)).await;
                    }
                }
            )
        }).collect::<Vec<_>>();

        let cached_clone = cached.clone();
        let shutdown_handle = tokio::spawn(
            async move {
                sleep(Duration::from_millis(8)).await;
                cached_clone.shutdown();
            }
        );
        for handle in task_handles {
            handle.await.unwrap()
        }
        shutdown_handle.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_not_block_on_shutdown_with_limited_space() {
        let config_builder = ConfigBuilder::new(1000, 100, 1000);
        let cached = Arc::new(CacheD::new(config_builder.build()));

        let task_handles = (1..=50).map(|index| {
            let cached_clone = cached.clone();
            tokio::spawn(
                async move {
                    let start_index = index * 10;
                    let end_index = start_index + 10;

                    for count in start_index..end_index {
                        let put_result = cached_clone.put(count, count * 10);
                        if let Ok(result) = put_result {
                            timeout(Duration::from_secs(1), result.handle()).await.unwrap();
                        }
                        sleep(Duration::from_millis(2)).await;
                    }
                }
            )
        }).collect::<Vec<_>>();

        let cached_clone = cached.clone();
        let shutdown_handle = tokio::spawn(
            async move {
                sleep(Duration::from_millis(8)).await;
                cached_clone.shutdown();
            }
        );
        for handle in task_handles {
            handle.await.unwrap()
        }
        shutdown_handle.await.unwrap();
    }
}

#[cfg(test)]
mod put_or_update_tests {
    use std::ops::Add;
    use std::time::Duration;

    use crate::cache::cached::CacheD;
    use crate::cache::cached::put_or_update_tests::setup::UnixEpochClock;
    use crate::cache::clock::ClockType;
    use crate::cache::config::ConfigBuilder;
    use crate::cache::put_or_update::PutOrUpdateRequestBuilder;
    use crate::cache::types::Weight;

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

    fn test_config_builder() -> ConfigBuilder<&'static str, &'static str> {
        ConfigBuilder::new(100, 10, 100)
    }

    #[tokio::test]
    async fn put_or_update_a_non_existing_key_value() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").value("microservices").build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();

        assert_eq!("microservices", stored_value.value());
    }

    #[tokio::test]
    async fn put_or_update_a_non_existing_key_value_with_weight() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").value("microservices").weight(33).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_eq!(Some(33), cached.admission_policy.weight_of(&key_id));
    }

    #[tokio::test]
    async fn put_or_update_a_non_existing_key_value_with_time_to_live() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").value("microservices").weight(10).time_to_live(Duration::from_secs(10)).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!(Some(clock.now().add(Duration::from_secs(10))), stored_value.expire_after());
        assert_eq!("microservices", stored_value.value());
        assert_eq!(Some(10), cached.admission_policy.weight_of(&key_id));
    }

    #[tokio::test]
    async fn update_the_value_of_an_existing_key() {
        let cached = CacheD::new(test_config_builder().build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").value("storage engine").build()).unwrap();
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
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").weight(29).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_eq!(Some(29), cached.admission_policy.weight_of(&key_id));
    }

    #[tokio::test]
    async fn update_the_time_to_live_of_an_existing_key_with_original_key_not_having_time_to_live() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let original_weight = weight_of(&cached, "topic");

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").time_to_live(Duration::from_secs(100)).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_ne!(original_weight, cached.admission_policy.weight_of(&key_id));

        assert_eq!(Some(clock.now().add(Duration::from_secs(100))), stored_value.expire_after());
        assert_eq!(stored_value.expire_after(), cached.ttl_ticker.get(&key_id, &stored_value.expire_after().unwrap()));
    }

    #[tokio::test]
    async fn remove_the_time_to_live_of_an_existing_key() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put_with_ttl("topic", "microservices", Duration::from_secs(100)).unwrap();
        acknowledgement.handle().await;

        let original_weight = weight_of(&cached, "topic");

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").remove_time_to_live().build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_ne!(original_weight, cached.admission_policy.weight_of(&key_id));

        assert_eq!(None, stored_value.expire_after());
    }

    #[tokio::test]
    async fn add_the_time_to_live_of_an_existing_key() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let original_weight = weight_of(&cached, "topic");

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").time_to_live(Duration::from_secs(120)).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("microservices", stored_value.value());
        assert_ne!(original_weight, cached.admission_policy.weight_of(&key_id));

        assert_eq!(Some(clock.now().add(Duration::from_secs(120))), stored_value.expire_after());
        assert_eq!(stored_value.expire_after(), cached.ttl_ticker.get(&key_id, &stored_value.expire_after().unwrap()));
    }

    #[tokio::test]
    async fn update_the_value_and_time_to_live_of_an_existing_key() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put("topic", "microservices").unwrap();
        acknowledgement.handle().await;

        let original_weight = weight_of(&cached, "topic");

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").value("storage engine").time_to_live(Duration::from_secs(100)).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        assert_eq!("storage engine", stored_value.value());
        assert_ne!(original_weight, cached.admission_policy.weight_of(&key_id));

        assert_eq!(Some(clock.now().add(Duration::from_secs(100))), stored_value.expire_after());
        assert_eq!(stored_value.expire_after(), cached.ttl_ticker.get(&key_id, &stored_value.expire_after().unwrap()));
    }

    #[tokio::test]
    async fn update_the_value_and_remove_time_to_live_of_an_existing_key() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put_with_ttl("topic", "microservices", Duration::from_secs(100)).unwrap();
        acknowledgement.handle().await;

        let original_weight = weight_of(&cached, "topic");

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").value("storage engine").remove_time_to_live().build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        let new_weight = cached.admission_policy.weight_of(&key_id);
        assert_eq!("storage engine", stored_value.value());
        assert_ne!(original_weight, new_weight);
        assert!(new_weight < original_weight);

        assert_eq!(None, stored_value.expire_after());
    }

    #[tokio::test]
    async fn update_the_value_weight_and_remove_time_to_live_of_an_existing_key() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put_with_ttl("topic", "microservices", Duration::from_secs(100)).unwrap();
        acknowledgement.handle().await;

        let original_weight = weight_of(&cached, "topic");

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").value("storage engine").weight(300).remove_time_to_live().build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        let new_weight = cached.admission_policy.weight_of(&key_id);
        assert_eq!("storage engine", stored_value.value());
        assert_ne!(original_weight, new_weight);
        assert_eq!(Some(300), new_weight);

        assert_eq!(None, stored_value.expire_after());
    }

    #[tokio::test]
    async fn update_the_time_to_live_of_an_existing_key() {
        let clock: ClockType = Box::new(UnixEpochClock {});
        let cached = CacheD::new(test_config_builder().clock(clock.clone_box()).build());

        let acknowledgement =
            cached.put_with_ttl("topic", "microservices", Duration::from_secs(100)).unwrap();
        acknowledgement.handle().await;

        let original_weight = weight_of(&cached, "topic");

        let acknowledgement =
            cached.put_or_update(PutOrUpdateRequestBuilder::new("topic").time_to_live(Duration::from_secs(500)).build()).unwrap();
        acknowledgement.handle().await;

        let value = cached.get_ref(&"topic");
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        let new_weight = cached.admission_policy.weight_of(&key_id);
        assert_eq!("microservices", stored_value.value());
        assert_eq!(original_weight, new_weight);
    }

    fn weight_of(cached: &CacheD<&str, &str>, key: &'static str) -> Option<Weight> {
        let value = cached.get_ref(&key);
        let value_ref = value.unwrap();
        let stored_value = value_ref.value();
        let key_id = stored_value.key_id();

        cached.admission_policy.weight_of(&key_id)
    }
}