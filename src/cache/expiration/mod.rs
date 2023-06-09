use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::tick;
use hashbrown::HashMap;
use log::{debug, info};
use parking_lot::RwLock;

use crate::cache::clock::ClockType;
use crate::cache::expiration::config::TTLConfig;
use crate::cache::types::{ExpireAfter, KeyId};

pub(crate) mod config;

/// `TTLTicker` removes the expired keys.
/// `TTLTicker` is a shared lock based HashMap. Each shard holds a [`parking_lot::RwLock`] protected [`hashbrown::HashMap`]
/// During the event of cache shutdown, keep_running is set to false.
pub(crate) struct TTLTicker {
    shards: Arc<[RwLock<HashMap<KeyId, ExpireAfter>>]>,
    keep_running: Arc<AtomicBool>,
}

impl TTLTicker {
    pub(crate) fn new<EvictHook>(config: TTLConfig, evict_hook: EvictHook) -> Arc<TTLTicker>
        where EvictHook: Fn(&KeyId) + Send + Sync + 'static {
        let ticker = Arc::new(
            TTLTicker {
                shards: (0..config.shards())
                    .map(|_| RwLock::new(HashMap::new())).collect(),
                keep_running: Arc::new(AtomicBool::new(true)),
            }
        );
        ticker.clone().spin(config.tick_duration(), config.clock(), evict_hook);
        ticker
    }

    pub(crate) fn put(self: &Arc<TTLTicker>, key_id: KeyId, expire_after: ExpireAfter) {
        let shard_index = self.shard_index(&expire_after);
        self.shards[shard_index].write().insert(key_id, expire_after);
    }

    pub(crate) fn update(self: &Arc<TTLTicker>, key_id: KeyId, old_expiry: &ExpireAfter, new_expiry: ExpireAfter) {
        {
            let shard_index = self.shard_index(old_expiry);
            self.shards[shard_index].write().remove(&key_id);
        }
        {
            let shard_index = self.shard_index(&new_expiry);
            self.shards[shard_index].write().insert(key_id, new_expiry);
        }
    }

    pub(crate) fn delete(self: &Arc<TTLTicker>, key_id: &KeyId, expire_after: &ExpireAfter) {
        let shard_index = self.shard_index(expire_after);
        self.shards[shard_index].write().remove(key_id);
    }

    pub(crate) fn get(self: &Arc<TTLTicker>, key_id: &KeyId, expire_after: &ExpireAfter) -> Option<ExpireAfter> {
        let shard_index = self.shard_index(expire_after);
        self.shards[shard_index].read().get(key_id).copied()
    }

    pub(crate) fn clear(&self) {
        let _ = &self.shards.iter().for_each(|locked_store| {
            locked_store.write().clear();
        });
    }

    pub(crate) fn shutdown(&self) {
        self.keep_running.store(false, Ordering::Release);
    }

    /// Determines the shard to pick for put, update, delete and get operations based on the time.
    /// Detailed explanation is available in the `spin` method.
    fn shard_index(self: &Arc<TTLTicker>, time: &SystemTime) -> usize {
        let since_the_epoch = time.duration_since(UNIX_EPOCH).expect("Time went backwards");
        since_the_epoch.as_secs() as usize % self.shards.len()
    }

    /// Spins a single thread every tick_duration. tick_duration is a configurable parameter defined in [`crate::cache::config::Config`].
    /// Every time the thread runs, it identifies the shard_index to pick for removing the expired keys.
    /// One way is to lock all the shards and perform the cleanup of the expired keys.
    /// This approach will introduce contention, all the puts with time_to_live might suffer.
    /// Other way is to pick one shard at a time and perform the cleanup.
    /// In order to minimize contention, a single shard is picked based on the current_time.
    /// The current_time `(clock.now())` is converted to a duration `time.duration_since(UNIX_EPOCH)` and
    /// its modulo operation with total number of shards gives the shard_index.
    fn spin<EvictHook>(self: Arc<TTLTicker>, tick_duration: Duration, clock: ClockType, evict_hook: EvictHook)
        where EvictHook: Fn(&KeyId) + Send + Sync + 'static {
        let keep_running = self.keep_running.clone();
        let receiver = tick(tick_duration);

        thread::spawn(move || {
            while let Ok(_instant) = receiver.recv() {
                let now = clock.now();
                let shard_index = self.shard_index(&now);

                self.shards[shard_index].write().retain(|key, expire_after| {
                    let has_not_expired = now.le(expire_after);
                    if !has_not_expired {
                        debug!("Key with id {} has expired", key);
                        (evict_hook)(key);
                    }
                    has_not_expired
                });

                if !keep_running.load(Ordering::Acquire) {
                    info!("Shutting down TTLTicker");
                    drop(receiver);
                    break;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use parking_lot::lock_api::Mutex;

    use crate::cache::clock::{Clock, SystemClock};
    use crate::cache::expiration::config::TTLConfig;
    use crate::cache::expiration::tests::setup::{EvictedKeys, UnixEpochClock};
    use crate::cache::expiration::TTLTicker;
    use crate::cache::types::KeyId;

    mod setup {
        use std::time::SystemTime;

        use parking_lot::Mutex;

        use crate::cache::clock::Clock;
        use crate::cache::types::KeyId;

        #[derive(Clone)]
        pub(crate) struct UnixEpochClock;

        impl Clock for UnixEpochClock {
            fn now(&self) -> SystemTime {
                SystemTime::UNIX_EPOCH
            }
        }

        pub(crate) struct EvictedKeys {
            pub(crate) keys: Mutex<Vec<KeyId>>,
        }
    }

    #[test]
    fn shard_index_0() {
        let clock = Box::new(UnixEpochClock {});
        let no_operation_evict_hook = |_key: &KeyId| {};
        let ticker = TTLTicker::new(TTLConfig::new(4, Duration::from_secs(300), clock.clone()), no_operation_evict_hook);

        let shard_index = ticker.shard_index(&clock.now());
        assert_eq!(0, shard_index);
    }

    #[test]
    fn shard_index_1() {
        let clock = Box::new(UnixEpochClock {});
        let no_operation_evict_hook = |_key: &KeyId| {};
        let ticker = TTLTicker::new(TTLConfig::new(4, Duration::from_secs(300), clock.clone()), no_operation_evict_hook);

        let later_time = clock.now().add(Duration::from_secs(5));
        let shard_index = ticker.shard_index(&later_time);
        assert_eq!(1, shard_index);
    }

    #[test]
    fn put() {
        let clock = Box::new(UnixEpochClock {});
        let no_operation_evict_hook = |_key: &KeyId| {};
        let ticker = TTLTicker::new(TTLConfig::new(4, Duration::from_secs(300), clock.clone()), no_operation_evict_hook);

        let key_id = 10;
        let expire_after = clock.now().add(Duration::from_secs(5));
        ticker.put(key_id, expire_after);

        let stored_value = ticker.get(&key_id, &expire_after).unwrap();
        assert_eq!(expire_after, stored_value);
    }

    #[test]
    fn update() {
        let clock = Box::new(UnixEpochClock {});
        let no_operation_evict_hook = |_key: &KeyId| {};
        let ticker = TTLTicker::new(TTLConfig::new(4, Duration::from_secs(300), clock.clone()), no_operation_evict_hook);

        let key_id = 10;
        let expire_after = clock.now().add(Duration::from_secs(5));
        ticker.put(key_id, expire_after);

        let updated_expiry = clock.now().add(Duration::from_secs(30));
        ticker.update(key_id, &expire_after, updated_expiry);

        let stored_value = ticker.get(&key_id, &updated_expiry).unwrap();
        assert_eq!(updated_expiry, stored_value);
    }

    #[test]
    fn delete() {
        let clock = Box::new(UnixEpochClock {});
        let no_operation_evict_hook = |_key: &KeyId| {};
        let ticker = TTLTicker::new(TTLConfig::new(4, Duration::from_secs(300), clock.clone()), no_operation_evict_hook);

        let key_id = 10;
        let expire_after = clock.now().add(Duration::from_secs(5));
        ticker.put(key_id, expire_after);

        ticker.delete(&key_id, &expire_after);
        let stored_value = ticker.get(&key_id, &expire_after);
        assert!(stored_value.is_none())
    }

    #[test]
    fn delete_an_expired_key() {
        let evicted_keys = Arc::new(EvictedKeys { keys: Mutex::new(Vec::new()) });
        let readonly_evicted_keys = evicted_keys.clone();

        let clock = SystemClock::boxed();
        let evict_hook = move |key_id: &KeyId| { evicted_keys.clone().keys.lock().push(*key_id) };
        let ticker = TTLTicker::new(TTLConfig::new(1, Duration::from_millis(5), clock.clone_box()), evict_hook);

        let key_id = 10;
        let expire_after = clock.now();
        ticker.put(key_id, expire_after);

        thread::sleep(Duration::from_secs(1));

        let stored_value = ticker.get(&key_id, &expire_after);
        assert!(stored_value.is_none());
        assert_eq!(vec![10], *readonly_evicted_keys.keys.lock());
    }

    #[test]
    fn delete_an_expired_key_amongst_multiple_keys() {
        let evicted_keys = Arc::new(EvictedKeys { keys: Mutex::new(Vec::new()) });
        let readonly_evicted_keys = evicted_keys.clone();

        let clock = SystemClock::boxed();
        let evict_hook = move |key_id: &KeyId| { evicted_keys.keys.lock().push(*key_id) };
        let ticker = TTLTicker::new(TTLConfig::new(1, Duration::from_millis(5), clock.clone_box()), evict_hook);

        let expire_after = clock.now();
        ticker.put(40, expire_after);
        ticker.put(50, expire_after.add(Duration::from_secs(300)));

        thread::sleep(Duration::from_secs(1));

        let stored_value = ticker.get(&40, &expire_after);
        assert!(stored_value.is_none());

        let stored_value = ticker.get(&50, &expire_after);
        assert!(stored_value.is_some());

        assert_eq!(vec![40], *readonly_evicted_keys.keys.lock());
    }

    #[test]
    fn shutdown() {
        let clock = SystemClock::boxed();
        let no_operation_evict_hook = |_key: &KeyId| {};
        let ticker = TTLTicker::new(TTLConfig::new(1, Duration::from_millis(5), clock.clone_box()), no_operation_evict_hook);

        let expire_after = clock.now();
        ticker.put(10, expire_after);
        ticker.shutdown();

        thread::sleep(Duration::from_secs(1));

        let stored_value = ticker.get(&10, &expire_after);
        assert!(stored_value.is_none());

        //add after shutdown
        ticker.put(20, clock.now());
        thread::sleep(Duration::from_secs(1));

        let stored_value = ticker.get(&20, &expire_after);
        assert!(stored_value.is_some());
    }

    #[test]
    fn clear() {
        let clock = Box::new(UnixEpochClock {});
        let no_operation_evict_hook = |_key: &KeyId| {};
        let ticker = TTLTicker::new(TTLConfig::new(4, Duration::from_secs(300), clock.clone()), no_operation_evict_hook);

        let key_id = 10;
        let expire_after = clock.now().add(Duration::from_secs(5));
        ticker.put(key_id, expire_after);

        let stored_value = ticker.get(&key_id, &expire_after).unwrap();
        assert_eq!(expire_after, stored_value);

        ticker.clear();

        let stored_value = ticker.get(&key_id, &expire_after);
        assert_eq!(None, stored_value);
    }
}