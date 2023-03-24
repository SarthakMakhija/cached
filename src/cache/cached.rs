use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::clock::SystemClock;
use crate::cache::command::acknowledgement::CommandAcknowledgement;
use crate::cache::command::command::CommandType;
use crate::cache::command::command_executor::CommandExecutor;
use crate::cache::store::store::Store;

pub struct CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    store: Arc<Store<Key, Value>>,
    command_sender: CommandExecutor<Key, Value>,
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    pub fn new() -> Self {
        let store = Store::new(SystemClock::boxed());
        return CacheD {
            store: store.clone(),
            command_sender: CommandExecutor::new(store),
        };
    }

    #[cfg(test)]
    pub fn new_with_clock(clock: crate::cache::clock::ClockType) -> Self {
        let store = Store::new(clock);
        return CacheD {
            store: store.clone(),
            command_sender: CommandExecutor::new(store),
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
        return self.store.get(&key);
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::cached::CacheD;

    mod setup {
        use std::ops::Add;
        use std::time::{Duration, SystemTime};

        use crate::cache::clock::Clock;

        pub(crate) struct FutureClock;

        impl Clock for FutureClock {
            fn now(&self) -> SystemTime {
                return SystemTime::now().add(Duration::from_secs(10));
            }
        }
    }

    #[tokio::test]
    async fn get_value_for_an_existing_key() {
        let mut cached = CacheD::new();

        let acknowledgement = cached.put("topic", "microservices");
        acknowledgement.handle().await;

        let value = cached.get("topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let cached: CacheD<&str, &str> = CacheD::new();

        let value = cached.get("non-existing");
        assert_eq!(None, value);
    }

    #[tokio::test]
    async fn delete_a_key() {
        let mut cached = CacheD::new();

        let acknowledgement = cached.put("topic", "microservices");
        acknowledgement.handle().await;

        let acknowledgement = cached.delete("topic");
        acknowledgement.handle().await;

        let value = cached.get("topic");
        assert_eq!(None, value);
    }
}