use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
use parking_lot::RwLock;

use crate::cache::key_description::KeyDescription;
use crate::cache::stats::ConcurrentStatsCounter;
use crate::cache::types::{FrequencyEstimate, KeyHash, KeyId, Weight};

pub(crate) struct WeightedKey<Key> {
    key: Key,
    pub(crate) key_hash: KeyHash,
    weight: Weight,
}

impl<Key> WeightedKey<Key> {
    fn new(key: Key, key_hash: KeyHash, weight: Weight) -> Self {
        WeightedKey {
            key,
            key_hash,
            weight,
        }
    }
}

pub(crate) struct CacheWeight<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    max_weight: Weight,
    weight_used: RwLock<Weight>,
    key_weights: DashMap<KeyId, WeightedKey<Key>>,
    stats_counter: Arc<ConcurrentStatsCounter>,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct SampledKey {
    pub(crate) id: KeyId,
    pub(crate) weight: Weight,
    pub(crate) estimated_frequency: FrequencyEstimate,
}

impl Ord for SampledKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.estimated_frequency, self.weight).cmp(&(self.estimated_frequency, other.weight))
    }
}

impl PartialOrd for SampledKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SampledKey {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for SampledKey {}

impl SampledKey {
    pub(crate) fn new<Key>(frequency: FrequencyEstimate, pair: RefMulti<KeyId, WeightedKey<Key>>) -> Self <> {
        Self::using(*pair.key(), pair.weight, frequency)
    }

    fn using(id: KeyId, key_weight: Weight, frequency: FrequencyEstimate) -> Self <> {
        SampledKey {
            id,
            weight: key_weight,
            estimated_frequency: frequency,
        }
    }
}

pub(crate) struct FrequencyCounterBasedMinHeapSamples<'a, Key, Freq>
    where Freq: Fn(KeyHash) -> FrequencyEstimate {
    source: &'a DashMap<KeyId, WeightedKey<Key>>,
    sample: BinaryHeap<SampledKey>,
    current_sample_key_ids: HashSet<KeyId>,
    sample_size: usize,
    frequency_counter: Freq,
}

impl<'a, Key, Freq> FrequencyCounterBasedMinHeapSamples<'a, Key, Freq>
    where Freq: Fn(KeyHash) -> FrequencyEstimate {
    fn new(
        source: &'a DashMap<KeyId, WeightedKey<Key>>,
        sample_size: usize,
        frequency_counter: Freq) -> Self <> {
        let (sample, current_sample_key_ids) = Self::initial_sample(source, sample_size, &frequency_counter);
        FrequencyCounterBasedMinHeapSamples {
            source,
            sample,
            current_sample_key_ids,
            sample_size,
            frequency_counter,
        }
    }

    pub(crate) fn min_frequency_key(&mut self) -> SampledKey {
        let sampled_key = self.sample.pop().unwrap();
        self.current_sample_key_ids.remove(&sampled_key.id);
        sampled_key
    }

    pub(crate) fn maybe_fill_in(&mut self) -> bool {
        let mut filled_in: bool = false;
        let mut iterator = self.source.iter();

        while self.sample.len() < self.sample_size {
            match iterator.next() {
                Some(pair) => {
                    if !self.current_sample_key_ids.contains(pair.key()) {
                        let frequency = (self.frequency_counter)(pair.key_hash);
                        self.sample.push(SampledKey::new(frequency, pair));
                        filled_in = true;
                    }
                }
                None => {
                    break;
                }
            }
        }
        filled_in
    }

    pub(crate) fn size(&self) -> usize {
        self.sample.len()
    }

    fn initial_sample(
        source: &DashMap<KeyId, WeightedKey<Key>>,
        sample_size: usize,
        frequency_counter: &Freq) -> (BinaryHeap<SampledKey>, HashSet<KeyId>) {
        let mut counter = 0;
        let mut sample = BinaryHeap::new();
        let mut current_sample_key_ids = HashSet::new();

        for pair in source.iter().by_ref() {
            current_sample_key_ids.insert(*pair.key());
            sample.push(SampledKey::new(frequency_counter(pair.value().key_hash), pair));
            counter += 1;

            if counter >= sample_size {
                break;
            }
        }
        (sample, current_sample_key_ids)
    }
}

impl<Key> CacheWeight<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    pub(crate) fn new(max_weight: Weight, stats_counter: Arc<ConcurrentStatsCounter>) -> Self <> {
        CacheWeight {
            max_weight,
            weight_used: RwLock::new(0),
            key_weights: DashMap::new(),
            stats_counter,
        }
    }

    pub(crate) fn get_max_weight(&self) -> Weight {
        self.max_weight
    }

    pub(crate) fn get_weight_used(&self) -> Weight {
        *self.weight_used.read()
    }

    pub(crate) fn is_space_available_for(&self, weight: Weight) -> (Weight, bool) {
        let available = self.max_weight - (*self.weight_used.read());
        (available, available >= weight)
    }

    pub(crate) fn add(&self, key_description: &KeyDescription<Key>) {
        self.key_weights.insert(key_description.id, WeightedKey::new(key_description.clone_key(), key_description.hash, key_description.weight));
        let mut guard = self.weight_used.write();
        *guard += key_description.weight;

        self.stats_counter.add_weight(key_description.weight as u64);
    }

    pub(crate) fn update(&self, key_id: &KeyId, weight: Weight) -> bool {
        if let Some(mut existing) = self.key_weights.get_mut(key_id) {
            {
                let mut guard = self.weight_used.write();
                *guard += weight - existing.weight;
            }
            if weight > existing.weight {
                let difference = weight - existing.weight;
                self.stats_counter.add_weight(difference as u64);
            } else {
                let difference = existing.weight - weight;
                self.stats_counter.add_weight(!(difference - 1) as u64);
            }

            let weighted_key = existing.value_mut();
            weighted_key.weight = weight;

            return true;
        }
        false
    }

    pub(crate) fn delete<DeleteHook>(&self, key_id: &KeyId, delete_hook: &DeleteHook)
        where DeleteHook: Fn(Key) {
        if let Some(weight_by_key_hash) = self.key_weights.remove(key_id) {
            let mut guard = self.weight_used.write();
            *guard -= weight_by_key_hash.1.weight;
            delete_hook(weight_by_key_hash.1.key);

            self.stats_counter.remove_weight(weight_by_key_hash.1.weight as u64);
        }
    }

    pub(crate) fn contains(&self, key_id: &KeyId) -> bool {
        self.key_weights.contains_key(key_id)
    }

    pub(crate) fn weight_of(&self, key_id: &KeyId) -> Option<Weight> {
        self.key_weights.get(key_id).map(|pair| pair.weight)
    }

    pub(crate) fn sample<Freq>(&self, size: usize, frequency_counter: Freq)
                               -> FrequencyCounterBasedMinHeapSamples<'_, Key, Freq>
        where Freq: Fn(KeyHash) -> FrequencyEstimate {
        FrequencyCounterBasedMinHeapSamples::new(&self.key_weights, size, frequency_counter)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;

    use crate::cache::key_description::KeyDescription;
    use crate::cache::policy::cache_weight::CacheWeight;
    use crate::cache::stats::ConcurrentStatsCounter;

    struct DeletedKeys<Key> {
        keys: RwLock<Vec<Key>>,
    }

    #[test]
    fn maximum_cache_weight() {
        let cache_weight: CacheWeight<&str> = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));
        assert_eq!(10, cache_weight.get_max_weight());
    }

    #[test]
    fn space_is_available_for_new_key() {
        let cache_weight: CacheWeight<&str> = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));
        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 3));

        assert!(cache_weight.is_space_available_for(7).1);
    }

    #[test]
    fn space_is_not_available_for_new_key() {
        let cache_weight: CacheWeight<&str> = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));
        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 3));

        assert!(!cache_weight.is_space_available_for(8).1);
    }

    #[test]
    fn add_key_weight() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));
        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 3));

        assert_eq!(3, cache_weight.get_weight_used());
    }

    #[test]
    fn add_key_weight_and_increase_stats() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));
        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 3));

        assert_eq!(3, cache_weight.stats_counter.weight_added());
    }

    #[test]
    fn update_non_existing_key() {
        let cache_weight: CacheWeight<&str> = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        let result = cache_weight.update(&1, 2);
        assert!(!result);
    }

    #[test]
    fn update_an_existing_key() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 3));
        let result = cache_weight.update(&1, 3);

        assert!(result);
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_less() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 3));
        assert_eq!(3, cache_weight.get_weight_used());

        cache_weight.update(&1, 2);
        assert_eq!(2, cache_weight.get_weight_used());
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_less_and_increase_stats() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 3));
        assert_eq!(3, cache_weight.stats_counter.weight_added());

        cache_weight.update(&1, 2);
        assert_eq!(2, cache_weight.stats_counter.weight_added());
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_more() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 4));
        assert_eq!(4, cache_weight.get_weight_used());

        cache_weight.update(&1, 8);
        assert_eq!(8, cache_weight.get_weight_used());
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_more_and_increase_stats() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 4));
        assert_eq!(4, cache_weight.stats_counter.weight_added());

        cache_weight.update(&1, 8);
        assert_eq!(8, cache_weight.stats_counter.weight_added());
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_same() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 4));
        assert_eq!(4, cache_weight.get_weight_used());

        cache_weight.update(&1, 4);
        assert_eq!(4, cache_weight.get_weight_used());
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_same_and_make_no_changes_in_stats() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 4));
        assert_eq!(4, cache_weight.stats_counter.weight_added());

        cache_weight.update(&1, 4);
        assert_eq!(4, cache_weight.stats_counter.weight_added());
    }

    #[test]
    fn delete_key_weight() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 3));
        assert_eq!(3, cache_weight.get_weight_used());

        let deleted_keys = DeletedKeys { keys: RwLock::new(Vec::new()) };
        let delete_hook = |key| { deleted_keys.keys.write().push(key) };
        cache_weight.delete(&1, &delete_hook);

        assert_eq!(vec!["disk"], *deleted_keys.keys.read());
        assert_eq!(0, cache_weight.get_weight_used());
        assert!(!cache_weight.contains(&1));
    }

    #[test]
    fn delete_key_weight_increase_stats() {
        let cache_weight = CacheWeight::new(10, Arc::new(ConcurrentStatsCounter::new()));

        cache_weight.add(&KeyDescription::new("disk", 1, 3040, 3));
        assert_eq!(3, cache_weight.get_weight_used());

        let delete_hook = |_| {};
        cache_weight.delete(&1, &delete_hook);

        assert_eq!(3, cache_weight.stats_counter.weight_removed())
    }
}

#[cfg(test)]
mod frequency_counter_based_min_heap_samples_tests {
    use dashmap::DashMap;

    use crate::cache::policy::cache_weight::{FrequencyCounterBasedMinHeapSamples, SampledKey, WeightedKey};
    use crate::cache::types::KeyId;

    #[test]
    fn equality_of_sampled_keys() {
        let cache: DashMap<KeyId, WeightedKey<&str>> = DashMap::new();
        cache.insert(1, WeightedKey::new("disk", 3040, 3));

        let mut sampled_keys = Vec::new();
        for pair in cache.iter().by_ref() {
            sampled_keys.push(SampledKey::new(10, pair));
        }

        assert_eq!(sampled_keys[0], sampled_keys[0]);
    }

    #[test]
    fn sample_size() {
        let cache: DashMap<KeyId, WeightedKey<&str>> = DashMap::new();
        cache.insert(1, WeightedKey::new("disk", 3040, 3));
        cache.insert(2, WeightedKey::new("topic", 1090, 4));
        cache.insert(3, WeightedKey::new("SSD", 1290, 3));

        let sample = FrequencyCounterBasedMinHeapSamples::new(
            &cache,
            2,
            |_hash| { 1 },
        );

        assert_eq!(2, sample.size());
    }

    #[test]
    fn maybe_fill_in_with_source_having_keys_to_fill() {
        let cache: DashMap<KeyId, WeightedKey<&str>> = DashMap::new();
        cache.insert(1, WeightedKey::new("disk", 3040, 3));
        cache.insert(2, WeightedKey::new("topic", 1090, 4));
        cache.insert(3, WeightedKey::new("SSD", 1290, 3));

        let mut sample = FrequencyCounterBasedMinHeapSamples::new(
            &cache,
            2,
            |_hash| { 1 },
        );

        assert_eq!(2, sample.size());

        let _ = sample.min_frequency_key();
        let _ = sample.maybe_fill_in();

        assert_eq!(2, sample.size());
    }

    #[test]
    fn maybe_fill_in_with_source_not_having_keys_to_fill() {
        let cache: DashMap<KeyId, WeightedKey<&str>> = DashMap::new();
        cache.insert(1, WeightedKey::new("disk", 3040, 3));
        cache.insert(2, WeightedKey::new("topic", 1090, 4));

        let mut sample = FrequencyCounterBasedMinHeapSamples::new(
            &cache,
            2,
            |_hash| { 1 },
        );

        assert_eq!(2, sample.size());
        let _ = sample.min_frequency_key();

        cache.remove(&1);
        cache.remove(&2);
        let _ = sample.maybe_fill_in();

        assert_eq!(1, sample.size());
    }

    #[test]
    fn maybe_fill_in_with_source_having_an_existing_sample_key_to_fill() {
        let cache: DashMap<KeyId, WeightedKey<&str>> = DashMap::new();
        cache.insert(1, WeightedKey::new("disk", 3040, 3));
        cache.insert(2, WeightedKey::new("topic", 1090, 4));

        let mut sample = FrequencyCounterBasedMinHeapSamples::new(
            &cache,
            2,
            |hash| match hash {
                3040 => 1,
                1090 => 2,
                _ => 0
            },
        );

        assert_eq!(2, sample.size());
        let _ = sample.min_frequency_key();

        cache.remove(&1);
        let _ = sample.maybe_fill_in();

        assert_eq!(1, sample.size());
    }

    #[test]
    fn maybe_fill_in_with_the_sample_already_containing_the_source_keys() {
        let cache: DashMap<KeyId, WeightedKey<&str>> = DashMap::new();
        cache.insert(1, WeightedKey::new("disk", 3040, 3));
        cache.insert(2, WeightedKey::new("topic", 1090, 4));

        let mut sample = FrequencyCounterBasedMinHeapSamples::new(
            &cache,
            2,
            |_hash| { 1 },
        );

        assert_eq!(2, sample.size());
        let _ = sample.maybe_fill_in();
        assert_eq!(2, sample.size());
    }

    #[test]
    fn sample_keys_with_distinct_frequencies() {
        let cache: DashMap<KeyId, WeightedKey<&str>> = DashMap::new();
        cache.insert(1, WeightedKey::new("disk", 3040, 3));
        cache.insert(2, WeightedKey::new("topic", 1090, 4));
        cache.insert(3, WeightedKey::new("SSD", 1290, 3));

        let mut sample = FrequencyCounterBasedMinHeapSamples::new(
            &cache,
            3,
            |hash| {
                match hash {
                    3040 => 1,
                    1090 => 2,
                    1290 => 3,
                    _ => 0
                }
            },
        );

        assert_eq!(1, sample.min_frequency_key().estimated_frequency);
        assert_eq!(2, sample.min_frequency_key().estimated_frequency);
        assert_eq!(3, sample.min_frequency_key().estimated_frequency);
    }

    #[test]
    fn sample_keys_with_same_frequencies() {
        let cache: DashMap<KeyId, WeightedKey<&str>> = DashMap::new();
        cache.insert(10, WeightedKey::new("disk", 3040, 5));
        cache.insert(20, WeightedKey::new("topic", 1090, 2));
        cache.insert(30, WeightedKey::new("SSD", 1290, 3));

        let mut sample = FrequencyCounterBasedMinHeapSamples::new(
            &cache,
            3,
            |hash| {
                match hash {
                    3040 => 1,
                    1090 => 2,
                    1290 => 1,
                    _ => 0
                }
            },
        );

        let sampled_key = sample.min_frequency_key();
        assert_eq!(1, sampled_key.estimated_frequency);
        assert_eq!(5, sampled_key.weight);
        assert_eq!(10, sampled_key.id);

        let sampled_key = sample.min_frequency_key();
        assert_eq!(1, sampled_key.estimated_frequency);
        assert_eq!(3, sampled_key.weight);
        assert_eq!(30, sampled_key.id);

        let sampled_key = sample.min_frequency_key();
        assert_eq!(2, sampled_key.estimated_frequency);
        assert_eq!(2, sampled_key.weight);
        assert_eq!(20, sampled_key.id);
    }
}