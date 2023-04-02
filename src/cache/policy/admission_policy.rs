use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crossbeam_channel::Receiver;
use parking_lot::RwLock;

use crate::cache::command::CommandStatus;
use crate::cache::key_description::KeyDescription;
use crate::cache::lfu::tiny_lfu::TinyLFU;
use crate::cache::policy::cache_weight::CacheWeight;
use crate::cache::pool::BufferConsumer;
use crate::cache::types::{FrequencyEstimate, KeyHash, KeyId, TotalCounters, Weight};

const EVICTION_SAMPLE_SIZE: usize = 5;

pub(crate) struct AdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    access_frequency: Arc<RwLock<TinyLFU>>,
    cache_weight: CacheWeight<Key>,
    sender: crossbeam_channel::Sender<Vec<KeyHash>>,
    keep_running: Arc<AtomicBool>,
}

impl<Key> AdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    pub(crate) fn new(counters: TotalCounters, total_cache_weight: Weight) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(10); //TODO: capacity as parameter
        let policy = AdmissionPolicy {
            access_frequency: Arc::new(RwLock::new(TinyLFU::new(counters))),
            cache_weight: CacheWeight::new(total_cache_weight),
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

    pub(crate) fn estimate(&self, key_hash: KeyHash) -> FrequencyEstimate {
        return self.access_frequency.read().estimate(key_hash);
    }

    pub(crate) fn maybe_add<DeleteHook>(&self,
                                        key_description: &KeyDescription<Key>,
                                        delete_hook: &DeleteHook) -> CommandStatus
        where DeleteHook: Fn(Key) {
        if key_description.weight > self.cache_weight.get_max_weight() {
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

    pub(crate) fn update(&self, key_description: &KeyDescription<Key>) {
        self.cache_weight.update(key_description);
    }

    pub(crate) fn delete(&self, key_id: &KeyId) {
        let no_operation_delete_hook = |_key| {};
        self.cache_weight.delete(key_id, &no_operation_delete_hook);
    }

    pub(crate) fn contains(&self, key_id: &KeyId) -> bool {
        self.cache_weight.contains(key_id)
    }

    pub(crate) fn weight_of(&self, key_id: &KeyId) -> Option<Weight> {
        self.cache_weight.weight_of(key_id)
    }

    pub(crate) fn shutdown(&self) {
        self.keep_running.store(false, Ordering::Release);
    }

    //TODO: should we query the space_available from cache_weight?
    fn create_space<DeleteHook>(&self,
                                space_left: Weight,
                                key_description: &KeyDescription<Key>,
                                delete_hook: &DeleteHook) -> CommandStatus
        where DeleteHook: Fn(Key) {
        let frequency_counter = |key_hash| self.estimate(key_hash);

        let incoming_key_access_frequency = self.estimate(key_description.hash);
        let mut space_available = space_left;

        let mut sample
            = self.cache_weight.sample(EVICTION_SAMPLE_SIZE, frequency_counter);

        while space_available < key_description.weight {
            let sampled_key = sample.min_frequency_key();
            if incoming_key_access_frequency < sampled_key.estimated_frequency {
                return CommandStatus::Rejected;
            }
            self.cache_weight.delete(&sampled_key.id, delete_hook);
            space_available += sampled_key.weight;

            let _ = sample.maybe_fill_in();
        }
        CommandStatus::Accepted
    }
}

impl<Key> BufferConsumer for AdmissionPolicy<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
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

    use parking_lot::RwLock;

    use crate::cache::command::CommandStatus;
    use crate::cache::key_description::KeyDescription;
    use crate::cache::policy::admission_policy::AdmissionPolicy;
    use crate::cache::pool::BufferConsumer;

    struct DeletedKeys<Key> {
        keys: RwLock<Vec<Key>>,
    }

    #[test]
    fn increase_access_frequency() {
        let policy: AdmissionPolicy<&str> = AdmissionPolicy::new(10, 10);
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

    #[test]
    fn does_not_add_key_if_its_weight_is_more_than_the_total_cache_weight() {
        let policy = AdmissionPolicy::new(10, 10);
        let no_operation_delete_hook = |_key| {};

        assert_eq!(CommandStatus::Rejected,
                   policy.maybe_add(&KeyDescription::new("topic", 1, 3018, 100), &no_operation_delete_hook)
        );
    }

    #[test]
    fn adds_a_key_given_space_is_available() {
        let policy = AdmissionPolicy::new(10, 10);
        let no_operation_delete_hook = |_key| {};

        let addition_status = policy.maybe_add(
            &KeyDescription::new("topic", 1, 3018, 5), &no_operation_delete_hook,
        );
        assert_eq!(CommandStatus::Accepted, addition_status);
    }

    #[test]
    fn adds_a_key_given_space_is_not_available() {
        let policy = AdmissionPolicy::new(10, 10);
        let key_hashes = vec![10, 14, 116];
        policy.access_frequency.write().add(key_hashes);

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
        let policy = AdmissionPolicy::new(10, 10);
        let key_hashes = vec![14];
        policy.access_frequency.write().add(key_hashes);

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
        let policy = AdmissionPolicy::new(10, 10);
        let no_operation_delete_hook = |_key| {};

        let addition_status = policy.maybe_add(&KeyDescription::new("topic", 1, 3018, 5), &no_operation_delete_hook);
        assert_eq!(CommandStatus::Accepted, addition_status);
        assert_eq!(5, policy.cache_weight.get_weight_used());

        policy.update(&KeyDescription::new("topic", 1, 3018, 8));
        assert_eq!(8, policy.cache_weight.get_weight_used());
    }

    #[test]
    fn deletes_a_key() {
        let policy = AdmissionPolicy::new(10, 10);
        let no_operation_delete_hook = |_key| {};

        let addition_status = policy.maybe_add(&KeyDescription::new("topic", 1, 3018, 5), &no_operation_delete_hook);
        assert_eq!(CommandStatus::Accepted, addition_status);

        policy.delete(&1);
        assert!(!policy.contains(&1));
    }
}