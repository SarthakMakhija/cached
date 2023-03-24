use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::clock::SystemClock;
use crate::cache::command::CommandType;
use crate::cache::command_sender::CommandSender;
use crate::cache::store::Store;

pub struct CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    store: Arc<Store<Key, Value>>,
    command_sender: CommandSender<Key, Value>,
}

impl<Key, Value> CacheD<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    pub fn new() -> Self {
        let store = Store::new(SystemClock::boxed());
        return CacheD {
            store: store.clone(),
            command_sender: CommandSender::new(store),
        };
    }

    #[cfg(test)]
    pub fn new_with_clock(clock: crate::cache::clock::ClockType) -> Self {
        let store = Store::new(clock);
        return CacheD {
            store: store.clone(),
            command_sender: CommandSender::new(store),
        };
    }

    pub fn put(&mut self, key: Key, value: Value) {
        self.command_sender.send(CommandType::Put(key, value));
    }

    pub fn put_with_ttl(&mut self, key: Key, value: Value, time_to_live: Duration) {
        self.command_sender.send(CommandType::PutWithTTL(key, value, time_to_live));
    }

    pub fn get(&self, key: Key) -> Option<Value> {
        return self.store.get(&key);
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
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

    #[test]
    fn get_value_for_an_existing_key() {
        let mut cached = CacheD::new();
        cached.put("topic", "microservices");
        thread::sleep(Duration::from_millis(5)); //TODO: Remove sleep

        let value = cached.get("topic");
        assert_eq!(Some("microservices"), value);
    }

    #[test]
    fn get_value_for_a_non_existing_key() {
        let cached: CacheD<&str, &str> = CacheD::new();

        let value = cached.get("non-existing");
        assert_eq!(None, value);
    }
}