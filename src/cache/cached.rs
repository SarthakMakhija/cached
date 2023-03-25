use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::command::acknowledgement::CommandAcknowledgement;
use crate::cache::command::command::CommandType;
use crate::cache::command::command_executor::CommandExecutor;
use crate::cache::config::Config;
use crate::cache::policy::admission_policy::AdmissionPolicy;
use crate::cache::pool::{BufferSize, Pool, PoolSize};
use crate::cache::store::store::Store;

pub struct CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    config: Config<Key>,
    store: Arc<Store<Key, Value>>,
    command_sender: CommandExecutor<Key, Value>,
    pool: Pool<AdmissionPolicy>
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {

    pub fn new(config: Config<Key>) -> Self {
        let store = Store::new((&config.clock).clone_box());
        return CacheD {
            config,
            store: store.clone(),
            command_sender: CommandExecutor::new(store),
            pool: Pool::new(PoolSize(1), BufferSize(1), Rc::new(AdmissionPolicy::new(10)))
        };
    }

    pub fn put(&mut self, key: Key, value: Value) -> Arc<CommandAcknowledgement> {
        return self.command_sender.send(CommandType::Put(key, value));
    }

    pub fn put_with_ttl(&mut self, key: Key, value: Value, time_to_live: Duration) -> Arc<CommandAcknowledgement> {
        return self.command_sender.send(CommandType::PutWithTTL(key, value, time_to_live));
    }

    pub fn delete(&mut self, key: Key) -> Arc<CommandAcknowledgement> {
        return self.command_sender.send(CommandType::Delete(key));
    }

    pub fn get(&self, key: Key) -> Option<Value> {
        if let Some(value) =  self.store.get(&key) {
            self.mark_key_accessed(&key);
            return Some(value);
        }
        return None;
    }

    fn mark_key_accessed(&self, key: &Key) {
        self.pool.add((self.config.key_hash)(&key));
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::cached::CacheD;
    use crate::cache::config::ConfigBuilder;

    #[tokio::test]
    async fn get_value_for_an_existing_key() {
        let mut cached = CacheD::new(ConfigBuilder::new().build());

        let acknowledgement = cached.put("topic", "microservices");
        acknowledgement.handle().await;

        let value = cached.get("topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let cached: CacheD<&str, &str> = CacheD::new(ConfigBuilder::new().build());

        let value = cached.get("non-existing");
        assert_eq!(None, value);
    }

    #[tokio::test]
    async fn delete_a_key() {
        let mut cached = CacheD::new(ConfigBuilder::new().build());

        let acknowledgement = cached.put("topic", "microservices");
        acknowledgement.handle().await;

        let acknowledgement = cached.delete("topic");
        acknowledgement.handle().await;

        let value = cached.get("topic");
        assert_eq!(None, value);
    }
}