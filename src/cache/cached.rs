use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::command::acknowledgement::CommandAcknowledgement;
use crate::cache::command::command::CommandType;
use crate::cache::command::command_executor::CommandExecutor;
use crate::cache::config::config::Config;
use crate::cache::policy::admission_policy::AdmissionPolicy;
use crate::cache::pool::Pool;
use crate::cache::store::store::Store;

pub struct CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    config: Config<Key>,
    store: Arc<Store<Key, Value>>,
    command_executor: CommandExecutor<Key, Value>,
    policy: Rc<AdmissionPolicy>,
    pool: Pool<AdmissionPolicy>,
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    pub fn new(config: Config<Key>) -> Self {
        assert!(config.counters > 0);

        let store = Store::new((config.clock).clone_box());
        let admission_policy = Rc::new(AdmissionPolicy::new(config.counters));
        let pool = Pool::new(config.access_pool_size, config.access_buffer_size, admission_policy.clone());
        let command_buffer_size = config.command_buffer_size;

        CacheD {
            config,
            store: store.clone(),
            command_executor: CommandExecutor::new(store, command_buffer_size),
            policy: admission_policy,
            pool,
        }
    }

    pub fn put(&self, key: Key, value: Value) -> Arc<CommandAcknowledgement> {
        self.command_executor.send(CommandType::Put(key, value))
    }

    pub fn put_with_ttl(&self, key: Key, value: Value, time_to_live: Duration) -> Arc<CommandAcknowledgement> {
        self.command_executor.send(CommandType::PutWithTTL(key, value, time_to_live))
    }

    pub fn delete(&self, key: Key) -> Arc<CommandAcknowledgement> {
        self.command_executor.send(CommandType::Delete(key))
    }

    pub fn get(&self, key: Key) -> Option<Value> {
        if let Some(value) = self.store.get(&key) {
            self.mark_key_accessed(&key);
            return Some(value);
        }
        None
    }

    fn mark_key_accessed(&self, key: &Key) {
        self.pool.add((self.config.key_hash)(key));
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use crate::cache::cached::CacheD;
    use crate::cache::config::config::ConfigBuilder;

    #[tokio::test]
    async fn get_value_for_an_existing_key() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement = cached.put("topic", "microservices");
        acknowledgement.handle().await;

        let value = cached.get("topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let cached: CacheD<&str, &str> = CacheD::new(ConfigBuilder::new().counters(10).build());

        let value = cached.get("non-existing");
        assert_eq!(None, value);
    }

    #[tokio::test]
    async fn delete_a_key() {
        let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

        let acknowledgement = cached.put("topic", "microservices");
        acknowledgement.handle().await;

        let acknowledgement = cached.delete("topic");
        acknowledgement.handle().await;

        let value = cached.get("topic");
        assert_eq!(None, value);
    }

    #[tokio::test]
    async fn get_access_frequency() {
        let cached = CacheD::new(ConfigBuilder::new().access_pool_size(1).access_buffer_size(3).counters(10).build());

        let acknowledgement_topic = cached.put("topic", "microservices");
        let acknowledgement_disk = cached.put("disk", "SSD");

        acknowledgement_topic.handle().await;
        acknowledgement_disk.handle().await;

        cached.get("topic");
        cached.get("disk");
        cached.get("topic");
        thread::sleep(Duration::from_secs(2));

        let hasher = &(cached.config.key_hash);
        assert_eq!(2, cached.policy.estimate(hasher(&"topic")));
        assert_eq!(1, cached.policy.estimate(hasher(&"disk")));
    }
}