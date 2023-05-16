use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crossbeam_channel::{Receiver, select};
use log::{debug, info, warn};
use parking_lot::RwLock;

use crate::cache::buffer_event::BufferEvent;
use crate::cache::command::CommandStatus;
use crate::cache::key_description::KeyDescription;
use crate::cache::lfu::tiny_lfu::TinyLFU;
use crate::cache::policy::cache_weight::CacheWeight;
use crate::cache::policy::config::CacheWeightConfig;
use crate::cache::pool::BufferConsumer;
use crate::cache::stats::ConcurrentStatsCounter;
use crate::cache::types::{FrequencyEstimate, KeyHash, KeyId, TotalCounters, Weight};

const EVICTION_SAMPLE_SIZE: usize = 5;
const CHANNEL_CAPACITY: usize = 10;

pub(crate) struct AdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    access_frequency: Arc<RwLock<TinyLFU>>,
    cache_weight: CacheWeight<Key>,
    sender: crossbeam_channel::Sender<BufferEvent>,
    keep_running: Arc<AtomicBool>,
    stats_counter: Arc<ConcurrentStatsCounter>,
}

impl<Key> AdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    pub(crate) fn new(counters: TotalCounters, cache_weight_config: CacheWeightConfig, stats_counter: Arc<ConcurrentStatsCounter>) -> Self {
        Self::with_channel_capacity(counters, cache_weight_config, CHANNEL_CAPACITY, stats_counter)
    }

    fn with_channel_capacity(
        counters: TotalCounters,
        cache_weight_config: CacheWeightConfig,
        channel_capacity: usize,
        stats_counter: Arc<ConcurrentStatsCounter>) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(channel_capacity);
        let policy = AdmissionPolicy {
            access_frequency: Arc::new(RwLock::new(TinyLFU::new(counters))),
            cache_weight: CacheWeight::new(cache_weight_config, stats_counter.clone()),
            sender,
            keep_running: Arc::new(AtomicBool::new(true)),
            stats_counter,
        };
        policy.start(receiver);
        policy
    }

    fn start(&self, receiver: Receiver<BufferEvent>) {
        let keep_running = self.keep_running.clone();
        let access_frequency = self.access_frequency.clone();

        thread::spawn(move || {
            while let Ok(event) = receiver.recv() {
                match event {
                    BufferEvent::Full(key_hashes) => {
                        { access_frequency.write().increment_access(key_hashes); }
                    }
                    BufferEvent::Shutdown => {
                        info!("Received Shutdown event in AdmissionPolicy, shutting it down");
                        drop(receiver);
                        break;
                    }
                }
                if !keep_running.load(Ordering::Acquire) {
                    info!("Shutting down AdmissionPolicy");
                    drop(receiver);
                    break;
                }
            }
        });
    }

    pub(crate) fn estimate(&self, key_hash: KeyHash) -> FrequencyEstimate {
        return self.access_frequency.read().estimate(key_hash);
    }

    pub(crate) fn maybe_add<DeleteHook>(&self,
                                        key_description: &KeyDescription<Key>,
                                        delete_hook: &DeleteHook) -> CommandStatus
        where DeleteHook: Fn(Key) {
        if key_description.weight > self.cache_weight.get_max_weight() {
            debug!(
                "Rejecting key with id {} and weight {}, given its weight is greater than the max cache weight {}",
                key_description.id, key_description.weight, self.cache_weight.get_max_weight()
            );
            return CommandStatus::Rejected;
        }
        let (space_left, is_enough_space_available) = self.cache_weight.is_space_available_for(key_description.weight);
        if is_enough_space_available {
            self.cache_weight.add(key_description);
            return CommandStatus::Accepted;
        }
        let status = self.create_space(space_left, key_description, delete_hook);
        if let CommandStatus::Accepted = status {
            self.cache_weight.add(key_description);
        }
        status
    }

    pub(crate) fn update(&self, key_id: &KeyId, weight: Weight) {
        self.cache_weight.update(key_id, weight);
    }

    pub(crate) fn delete(&self, key_id: &KeyId) {
        let no_operation_delete_hook = |_key| {};
        self.delete_with_hook(key_id, &no_operation_delete_hook);
    }

    pub(crate) fn delete_with_hook<DeleteHook>(&self, key_id: &KeyId, delete_hook: &DeleteHook)
        where DeleteHook: Fn(Key) {
        self.cache_weight.delete(key_id, delete_hook);
    }

    pub(crate) fn contains(&self, key_id: &KeyId) -> bool {
        self.cache_weight.contains(key_id)
    }

    pub(crate) fn weight_of(&self, key_id: &KeyId) -> Option<Weight> {
        self.cache_weight.weight_of(key_id)
    }

    pub(crate) fn weight_used(&self) -> Weight {
        self.cache_weight.get_weight_used()
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.sender.clone().send(BufferEvent::Shutdown);
        self.keep_running.store(false, Ordering::Release);
    }

    pub(crate) fn clear(&self) {
        self.cache_weight.clear();
        self.access_frequency.write().clear();
        self.stats_counter.clear();
    }

    fn create_space<DeleteHook>(&self,
                                space_left: Weight,
                                key_description: &KeyDescription<Key>,
                                delete_hook: &DeleteHook) -> CommandStatus
        where DeleteHook: Fn(Key) {
        let frequency_counter = |key_hash| self.estimate(key_hash);

        let incoming_key_access_frequency = self.estimate(key_description.hash);
        let mut space_available = space_left;

        let mut sample = self.cache_weight.sample(EVICTION_SAMPLE_SIZE, frequency_counter);
        while space_available < key_description.weight {
            let sampled_key = sample.min_frequency_key();
            if incoming_key_access_frequency < sampled_key.estimated_frequency {
                debug!(
                    "Rejecting key with id {} and estimated frequency {}, given its frequency is less than the sampled key with frequency {}",
                    key_description.id, incoming_key_access_frequency, sampled_key.estimated_frequency
                );
                return CommandStatus::Rejected;
            }

            self.cache_weight.delete(&sampled_key.id, delete_hook);
            let (fresh_space_available, _)  = self.cache_weight.is_space_available_for(key_description.weight);

            space_available = fresh_space_available;
            let _ = sample.maybe_fill_in();
        }
        CommandStatus::Accepted
    }
}

impl<Key> BufferConsumer for AdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    fn accept(&self, event: BufferEvent) {
        let size = if let BufferEvent::Full(ref key_hashes) = event {
            key_hashes.len()
        } else {
            0
        };
        select! {
            send(self.sender.clone(), event) -> response => {
                match response {
                    Ok(_) => {
                        if size > 0 {self.stats_counter.add_access(size as u64);}
                    },
                    Err(_) => {
                        if size > 0 {self.stats_counter.drop_access(size as u64);}
                    }
                }
            },
            default => {
                if size > 0 {
                    warn!("Dropping key accesses of size {}", size as u64);
                    self.stats_counter.drop_access(size as u64);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use parking_lot::RwLock;

    use crate::cache::buffer_event::BufferEvent;
    use crate::cache::command::CommandStatus;
    use crate::cache::key_description::KeyDescription;
    use crate::cache::policy::admission_policy::AdmissionPolicy;
    use crate::cache::policy::config::CacheWeightConfig;
    use crate::cache::pool::BufferConsumer;
    use crate::cache::stats::ConcurrentStatsCounter;

    struct DeletedKeys<Key> {
        keys: RwLock<Vec<Key>>,
    }

    fn test_cache_weight_config() -> CacheWeightConfig {
        CacheWeightConfig::new(100, 4, 10)
    }

    #[test]
    fn increase_access_and_shutdown() {
        let policy: AdmissionPolicy<&str> = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let key_hashes = vec![10, 14];

        policy.accept(BufferEvent::Full(key_hashes));
        thread::sleep(Duration::from_secs(1));

        policy.shutdown();
        thread::sleep(Duration::from_secs(1));

        let key_hashes = vec![116, 19];
        policy.accept(BufferEvent::Full(key_hashes));

        let actual_frequencies = vec![
            policy.estimate(10),
            policy.estimate(14),
            policy.estimate(116),
            policy.estimate(19),
        ];
        let expected_frequencies = vec![1, 1, 0, 0];
        assert_eq!(expected_frequencies, actual_frequencies);
    }

    #[test]
    fn increase_access_frequency_and_increase_stats() {
        let policy: AdmissionPolicy<&str> = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let key_hashes = vec![10, 14, 116, 19, 19, 10];

        policy.accept(BufferEvent::Full(key_hashes));
        thread::sleep(Duration::from_millis(10));

        let actual_frequencies = vec![
            policy.estimate(10),
            policy.estimate(14),
            policy.estimate(116),
            policy.estimate(19),
        ];
        let expected_frequencies = vec![2, 1, 1, 2];

        assert_eq!(expected_frequencies, actual_frequencies);
        assert_eq!(6, policy.stats_counter.access_added());
    }

    #[test]
    fn drop_access() {
        let policy: AdmissionPolicy<&str> = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let key_hashes = vec![10, 14];

        policy.accept(BufferEvent::Full(key_hashes));
        policy.shutdown();

        thread::sleep(Duration::from_secs(1));

        let key_hashes = vec![116, 19];
        policy.accept(BufferEvent::Full(key_hashes));

        thread::sleep(Duration::from_secs(1));

        let actual_frequencies = vec![
            policy.estimate(10),
            policy.estimate(14),
            policy.estimate(116),
            policy.estimate(19),
        ];

        let expected_frequencies = vec![1, 1, 0, 0];
        assert_eq!(expected_frequencies, actual_frequencies);
        assert_eq!(2, policy.stats_counter.access_added());
        assert_eq!(2, policy.stats_counter.access_dropped());
    }

    #[test]
    fn does_not_add_key_if_its_weight_is_more_than_the_total_cache_weight() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let no_operation_delete_hook = |_key| {};

        assert_eq!(CommandStatus::Rejected,
                   policy.maybe_add(&KeyDescription::new("topic", 1, 3018, 100), &no_operation_delete_hook)
        );
    }

    #[test]
    fn adds_a_key_given_space_is_available() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let no_operation_delete_hook = |_key| {};

        let addition_status = policy.maybe_add(
            &KeyDescription::new("topic", 1, 3018, 5), &no_operation_delete_hook,
        );
        assert_eq!(CommandStatus::Accepted, addition_status);
    }

    #[test]
    fn adds_a_key_even_if_the_space_is_not_available() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let key_hashes = vec![10, 14, 116];
        policy.access_frequency.write().increment_access(key_hashes);

        let deleted_keys = DeletedKeys { keys: RwLock::new(Vec::new()) };
        let delete_hook = |key| { deleted_keys.keys.write().push(key) };

        let status = policy.maybe_add(&KeyDescription::new("topic", 1, 10, 5), &delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        let status = policy.maybe_add(&KeyDescription::new("SSD", 2, 14, 6), &delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        assert!(policy.contains(&2));
        assert_eq!(6, policy.cache_weight.get_weight_used());

        assert!(!policy.contains(&1));
        assert_eq!(vec!["topic"], *deleted_keys.keys.read());
    }

    #[test]
    fn rejects_the_incoming_key_and_has_victims() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let key_hashes = vec![14];
        policy.access_frequency.write().increment_access(key_hashes);

        let deleted_keys = DeletedKeys { keys: RwLock::new(Vec::new()) };
        let delete_hook = |key| { deleted_keys.keys.write().push(key) };

        let status = policy.maybe_add(&KeyDescription::new("topic", 1, 20, 5), &delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        let status = policy.maybe_add(&KeyDescription::new("HDD", 2, 14, 3), &delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        let status = policy.maybe_add(&KeyDescription::new("SSD", 3, 90, 9), &delete_hook);
        assert_eq!(CommandStatus::Rejected, status);

        assert!(policy.contains(&2));
        assert_eq!(3, policy.cache_weight.get_weight_used());

        assert!(!policy.contains(&3));
        assert!(!policy.contains(&1));

        assert_eq!(vec!["topic"], *deleted_keys.keys.read());
    }

    #[test]
    fn updates_the_weight_of_a_key() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let no_operation_delete_hook = |_key| {};

        let addition_status = policy.maybe_add(&KeyDescription::new("topic", 1, 3018, 5), &no_operation_delete_hook);
        assert_eq!(CommandStatus::Accepted, addition_status);
        assert_eq!(5, policy.cache_weight.get_weight_used());

        policy.update(&1, 8);
        assert_eq!(8, policy.cache_weight.get_weight_used());
    }

    #[test]
    fn deletes_a_key() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let no_operation_delete_hook = |_key| {};

        let addition_status = policy.maybe_add(&KeyDescription::new("topic", 1, 3018, 5), &no_operation_delete_hook);
        assert_eq!(CommandStatus::Accepted, addition_status);

        policy.delete(&1);
        assert!(!policy.contains(&1));
    }

    #[test]
    fn deletes_a_key_with_hook() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let deleted_keys = DeletedKeys { keys: RwLock::new(Vec::new()) };
        let delete_hook = |key| { deleted_keys.keys.write().push(key) };

        let addition_status = policy.maybe_add(&KeyDescription::new("topic", 1, 3018, 5), &delete_hook);
        assert_eq!(CommandStatus::Accepted, addition_status);

        policy.delete_with_hook(&1, &delete_hook);
        assert!(!policy.contains(&1));
        assert_eq!("topic", deleted_keys.keys.read()[0]);
    }

    #[test]
    fn contains_a_key() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let no_operation_delete_hook = |_key| {};

        let addition_status = policy.maybe_add(&KeyDescription::new("topic", 1, 3018, 5), &no_operation_delete_hook);
        assert_eq!(CommandStatus::Accepted, addition_status);

        assert!(policy.contains(&1));
    }

    #[test]
    fn does_not_contain_a_key() {
        let policy: AdmissionPolicy<&str> = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));

        assert!(!policy.contains(&1));
    }

    #[test]
    fn weight_of_an_existing_key() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let no_operation_delete_hook = |_key| {};

        let addition_status = policy.maybe_add(&KeyDescription::new("topic", 1, 3018, 5), &no_operation_delete_hook);
        assert_eq!(CommandStatus::Accepted, addition_status);

        assert_eq!(Some(5), policy.weight_of(&1));
    }

    #[test]
    fn weight_of_a_non_existing_key() {
        let policy: AdmissionPolicy<&str> = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));

        assert_eq!(None, policy.weight_of(&1));
    }

    #[test]
    fn gets_the_weight_used() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let key_hashes = vec![10, 14, 116];
        policy.access_frequency.write().increment_access(key_hashes);

        let deleted_keys = DeletedKeys { keys: RwLock::new(Vec::new()) };
        let delete_hook = |key| { deleted_keys.keys.write().push(key) };

        let status = policy.maybe_add(&KeyDescription::new("topic", 1, 10, 5), &delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        let status = policy.maybe_add(&KeyDescription::new("SSD", 2, 14, 5), &delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        assert_eq!(10, policy.weight_used());
    }

    #[test]
    fn gets_the_weight_used_after_rejection() {
        let policy = AdmissionPolicy::new(10, test_cache_weight_config(), Arc::new(ConcurrentStatsCounter::new()));
        let key_hashes = vec![14, 116];
        policy.access_frequency.write().increment_access(key_hashes);

        let deleted_keys = DeletedKeys { keys: RwLock::new(Vec::new()) };
        let delete_hook = |key| { deleted_keys.keys.write().push(key) };

        let status = policy.maybe_add(&KeyDescription::new("topic", 1, 10, 5), &delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        let status = policy.maybe_add(&KeyDescription::new("SSD", 2, 14, 6), &delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        assert_eq!(6, policy.weight_used());
    }

    #[test]
    fn clear() {
        let cache_weight_config = CacheWeightConfig::new(100, 4, 20);
        let policy = AdmissionPolicy::new(10, cache_weight_config, Arc::new(ConcurrentStatsCounter::new()));
        let no_operation_delete_hook = |_key| {};

        let status = policy.maybe_add(&KeyDescription::new("topic", 1, 10, 5), &no_operation_delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        let status = policy.maybe_add(&KeyDescription::new("SSD", 2, 14, 6), &no_operation_delete_hook);
        assert_eq!(CommandStatus::Accepted, status);

        assert_eq!(11, policy.cache_weight.get_weight_used());
        assert!(policy.contains(&1));
        assert!(policy.contains(&2));

        policy.clear();

        assert_eq!(0, policy.cache_weight.get_weight_used());
        assert!(!policy.contains(&1));
        assert!(!policy.contains(&2));
    }
}