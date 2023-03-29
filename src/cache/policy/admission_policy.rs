use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crossbeam_channel::Receiver;
use parking_lot::RwLock;

use crate::cache::lfu::tiny_lfu::TinyLFU;
use crate::cache::policy::cache_weight::CacheWeight;
use crate::cache::pool::BufferConsumer;
use crate::cache::types::{KeyHash, TotalCounters};

pub(crate) struct AdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + 'static, {
    access_frequency: Arc<RwLock<TinyLFU>>,
    cache_weight: CacheWeight<Key>,
    sender: crossbeam_channel::Sender<Vec<KeyHash>>,
    keep_running: Arc<AtomicBool>,
}

impl<Key> AdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + 'static, {
    pub(crate) fn new(counters: TotalCounters) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(10); //TODO: capacity as parameter
        let policy = AdmissionPolicy {
            access_frequency: Arc::new(RwLock::new(TinyLFU::new(counters))),
            cache_weight: CacheWeight::new(100), //TODO: parameter?
            sender,
            keep_running: Arc::new(AtomicBool::new(true)),
        };
        policy.start(receiver);
        policy
    }

    fn start(&self, receiver: Receiver<Vec<KeyHash>>) {
        let keep_running = self.keep_running.clone();
        let access_frequency = self.access_frequency.clone();

        thread::spawn(move || {
            while let Ok(key_hashes) = receiver.recv() {
                { access_frequency.write().add(key_hashes); }
                if !keep_running.load(Ordering::Acquire) {
                    return;
                }
            }
        });
    }

    pub(crate) fn estimate(&self, key_hash: KeyHash) -> u8 {
        return self.access_frequency.read().estimate(key_hash);
    }

    pub(crate) fn shutdown(&self) {
        self.keep_running.store(false, Ordering::Release);
    }
}

impl<Key> BufferConsumer for AdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + 'static, {
    fn accept(&self, key_hashes: Vec<KeyHash>) {
        //TODO: Decide if we need to clone this
        //TODO: Remove unwrap
        self.sender.clone().send(key_hashes).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use crate::cache::policy::admission_policy::AdmissionPolicy;
    use crate::cache::pool::BufferConsumer;

    #[test]
    fn increase_access_frequency() {
        let policy: AdmissionPolicy<&str> = AdmissionPolicy::new(10);
        let key_hashes = vec![10, 14, 116, 19, 19, 10];

        policy.accept(key_hashes);
        thread::sleep(Duration::from_millis(5));

        let actual_frequencies = vec![
            policy.estimate(10),
            policy.estimate(14),
            policy.estimate(116),
            policy.estimate(19),
        ];
        let expected_frequencies = vec![2, 1, 1, 2];

        assert_eq!(expected_frequencies, actual_frequencies);
    }
}