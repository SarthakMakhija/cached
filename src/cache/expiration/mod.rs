use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::tick;
use hashbrown::HashMap;
use parking_lot::RwLock;

use crate::cache::clock::ClockType;
use crate::cache::types::KeyId;

pub(crate) type ExpireAfter = SystemTime;

pub(crate) struct TTLTicker {
    shards: Arc<[RwLock<HashMap<KeyId, ExpireAfter>>]>,
    keep_running: Arc<AtomicBool>,
}

impl TTLTicker {
    pub(crate) fn new(shards: usize, tick_duration: Duration, clock: ClockType) -> Arc<TTLTicker> {
        let ticker = Arc::new(
            TTLTicker {
                shards: (0..shards)
                    .map(|_| RwLock::new(HashMap::new())) //TODO: Capacity
                    .collect(),
                keep_running: Arc::new(AtomicBool::new(true))
            }
        );
        ticker.clone().spin(tick_duration, clock);
        ticker
    }

    pub(crate) fn add(self: &Arc<TTLTicker>, key_id: KeyId, expire_after: ExpireAfter) {
        let shard_index = self.shard_index(&expire_after);
        self.shards[shard_index].write().insert(key_id, expire_after);
    }

    pub(crate) fn get(self: &Arc<TTLTicker>, key_id: &KeyId, expire_after: &ExpireAfter) -> Option<ExpireAfter> {
        let shard_index = self.shard_index(expire_after);
        self.shards[shard_index].read().get(key_id).copied()
    }

    pub(crate) fn delete(self: &Arc<TTLTicker>, key_id: &KeyId, expire_after: &ExpireAfter) {
        let shard_index = self.shard_index(expire_after);
        self.shards[shard_index].write().remove(key_id);
    }

    pub(crate) fn shutdown(&self) {
        self.keep_running.store(false, Ordering::Release);
    }

    fn shard_index(self: &Arc<TTLTicker>, time: &SystemTime) -> usize {
        let since_the_epoch = time.duration_since(UNIX_EPOCH).expect("Time went backwards");
        (since_the_epoch.as_secs() as usize % self.shards.len()) as usize
    }

    fn spin(self: Arc<TTLTicker>, tick_duration: Duration, clock: ClockType) {
        let keep_running = self.keep_running.clone();
        let receiver = tick(tick_duration);

        thread::spawn(move || {
            while let Ok(_instant) = receiver.recv() {
                let now = clock.now();
                let shard_index = self.shard_index(&now);

                self.shards[shard_index].write().retain(|key, expire_after| {
                    let has_not_expired = now.le(expire_after);
                    if !has_not_expired {
                        println!("key {} has expired", key);
                    }
                    has_not_expired
                });

                if !keep_running.load(Ordering::Acquire) {
                    println!("shutting down TTLTicker");
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
    use std::thread;
    use std::time::Duration;
    use crate::cache::clock::{Clock, SystemClock};

    use crate::cache::expiration::tests::setup::UnixEpochClock;
    use crate::cache::expiration::TTLTicker;

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

    #[test]
    fn shard_index_0() {
        let clock = Box::new(UnixEpochClock {});
        let ticker = TTLTicker::new(4, Duration::from_secs(300), clock.clone());

        let shard_index = ticker.shard_index(&clock.now());
        assert_eq!(0, shard_index);
    }

    #[test]
    fn shard_index_1() {
        let clock = Box::new(UnixEpochClock {});
        let ticker = TTLTicker::new(4, Duration::from_secs(300), clock.clone());

        let later_time = clock.now().add(Duration::from_secs(5));
        let shard_index = ticker.shard_index(&later_time);
        assert_eq!(1, shard_index);
    }

    #[test]
    fn add() {
        let clock = Box::new(UnixEpochClock {});
        let ticker = TTLTicker::new(4, Duration::from_secs(300), clock.clone());

        let key_id = 10;
        let expire_after = clock.now().add(Duration::from_secs(5));
        ticker.add(key_id, expire_after);

        let stored_value = ticker.get(&key_id, &expire_after).unwrap();
        assert_eq!(expire_after, stored_value);
    }

    #[test]
    fn delete() {
        let clock = Box::new(UnixEpochClock {});
        let ticker = TTLTicker::new(4, Duration::from_secs(300), clock.clone());

        let key_id = 10;
        let expire_after = clock.now().add(Duration::from_secs(5));
        ticker.add(key_id, expire_after);

        ticker.delete(&key_id, &expire_after);
        let stored_value = ticker.get(&key_id, &expire_after);
        assert!(stored_value.is_none())
    }

    #[test]
    fn delete_an_expired_key() {
        let clock = SystemClock::boxed();
        let ticker = TTLTicker::new(1, Duration::from_millis(5), clock.clone_box());

        let key_id = 10;
        let expire_after = clock.now();
        ticker.add(key_id, expire_after);

        thread::sleep(Duration::from_secs(1));

        let stored_value = ticker.get(&key_id, &expire_after);
        assert!(stored_value.is_none())
    }

    #[test]
    fn delete_an_expired_key_amongst_multiple_keys() {
        let clock = SystemClock::boxed();
        let ticker = TTLTicker::new(1, Duration::from_millis(5), clock.clone_box());

        let expire_after = clock.now();
        ticker.add(10, expire_after);
        ticker.add(20, expire_after.add(Duration::from_secs(300)));

        thread::sleep(Duration::from_secs(1));

        let stored_value = ticker.get(&10, &expire_after);
        assert!(stored_value.is_none());

        let stored_value = ticker.get(&20, &expire_after);
        assert!(stored_value.is_some())
    }

    #[test]
    fn shutdown() {
        let clock = SystemClock::boxed();
        let ticker = TTLTicker::new(1, Duration::from_millis(5), clock.clone_box());

        let expire_after = clock.now();
        ticker.add(10, expire_after);
        ticker.shutdown();

        thread::sleep(Duration::from_secs(1));

        let stored_value = ticker.get(&10, &expire_after);
        assert!(stored_value.is_none());

        //add after shutdown
        ticker.add(20, clock.now());
        thread::sleep(Duration::from_secs(1));

        let stored_value = ticker.get(&20, &expire_after);
        assert!(stored_value.is_some());
    }
}