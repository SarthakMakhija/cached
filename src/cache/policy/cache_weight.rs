use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::hash_map::RandomState;
use std::hash::Hash;

use dashmap::DashMap;
use dashmap::iter::Iter;
use dashmap::mapref::multiple::RefMulti;
use parking_lot::RwLock;

use crate::cache::key_description::KeyDescription;
use crate::cache::types::{KeyHash, KeyId, Weight};

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
}

pub(crate) struct SampledKey {
    pub(crate) id: KeyId,
    pub(crate) hash: KeyHash,
    pub(crate) weight: Weight,
    pub(crate) estimated_frequency: u8, //TODO: type for frequency?
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
    pub(crate) fn new<Key>(pair: RefMulti<KeyId, WeightedKey<Key>>, frequency: u8) -> Self <> {
        Self::using(
            *pair.key(),
            pair.key_hash,
            pair.weight,
            frequency,
        )
    }

    fn using(id: KeyId, key_hash: KeyHash, key_weight: Weight, frequency: u8) -> Self <> {
        SampledKey {
            id,
            hash: key_hash,
            weight: key_weight,
            estimated_frequency: frequency,
        }
    }
}

impl<Key> CacheWeight<Key>
    where Key: Hash + Eq + Send + Sync + Clone + 'static, {
    pub(crate) fn new(max_weight: Weight) -> Self <> {
        CacheWeight {
            max_weight,
            weight_used: RwLock::new(0),
            key_weights: DashMap::new(),
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
        self.key_weights.insert(key_description.id, WeightedKey::new(key_description.key.clone(), key_description.hash, key_description.weight));
        let mut guard = self.weight_used.write();
        *guard += key_description.weight;
    }

    pub(crate) fn update(&self, key_description: &KeyDescription<Key>) -> bool {
        if let Some(mut pair) = self.key_weights.get_mut(&key_description.id) {
            {
                let mut guard = self.weight_used.write();
                *guard += key_description.weight - pair.weight;
            }
            let weighted_key = pair.value_mut();
            weighted_key.key_hash = key_description.hash;
            weighted_key.weight = key_description.weight;

            return true;
        }
        false
    }

    pub(crate) fn delete(&self, key_id: &KeyId) {
        if let Some(weight_by_key_hash) = self.key_weights.remove(key_id) {
            let mut guard = self.weight_used.write();
            *guard -= weight_by_key_hash.1.weight;
        }
    }

    //TODO: can we avoid returning iterator? This would avoid holding the read-lock
    pub(crate) fn sample<Freq>(&self, size: usize, frequency_counter: Freq)
                               -> (Iter<'_, KeyId, WeightedKey<Key>, RandomState, DashMap<KeyId, WeightedKey<Key>>>, BinaryHeap<SampledKey>)
        where Freq: Fn(KeyHash) -> u8 {
        let mut counter = 0;
        let mut sample = BinaryHeap::new();
        let mut iterator = self.key_weights.iter();

        for pair in iterator.by_ref() {
            let frequency = frequency_counter(pair.value().key_hash);
            sample.push(SampledKey::new(pair, frequency));
            counter += 1;

            if counter >= size {
                break;
            }
        }
        (iterator, sample)
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::key_description::KeyDescription;
    use crate::cache::policy::cache_weight::CacheWeight;

    #[test]
    fn maximum_cache_weight() {
        let cache_weight: CacheWeight<&str> = CacheWeight::new(10);
        assert_eq!(10, cache_weight.get_max_weight());
    }

    #[test]
    fn space_is_available_for_new_key() {
        let cache_weight: CacheWeight<&str> = CacheWeight::new(10);
        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 3));

        assert!(cache_weight.is_space_available_for(7).1);
    }

    #[test]
    fn space_is_not_available_for_new_key() {
        let cache_weight: CacheWeight<&str> = CacheWeight::new(10);
        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 3));

        assert!(!cache_weight.is_space_available_for(8).1);
    }

    #[test]
    fn add_key_weight() {
        let cache_weight = CacheWeight::new(10);
        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 3));

        assert_eq!(3, cache_weight.get_weight_used());
    }

    #[test]
    fn update_non_existing_key() {
        let cache_weight = CacheWeight::new(10);

        let result = cache_weight.update(&KeyDescription::new(&"disk", 1, 3040, 2));
        assert!(!result);
    }

    #[test]
    fn update_an_existing_key() {
        let cache_weight = CacheWeight::new(10);

        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 3));
        let result = cache_weight.update(&KeyDescription::new(&"disk", 1, 3040, 2));

        assert!(result);
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_less() {
        let cache_weight = CacheWeight::new(10);

        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 3));
        assert_eq!(3, cache_weight.get_weight_used());

        cache_weight.update(&KeyDescription::new(&"disk", 1, 3040, 2));
        assert_eq!(2, cache_weight.get_weight_used());
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_more() {
        let cache_weight = CacheWeight::new(10);

        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 4));
        assert_eq!(4, cache_weight.get_weight_used());

        cache_weight.update(&KeyDescription::new(&"disk", 1, 3040, 8));
        assert_eq!(8, cache_weight.get_weight_used());
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_same() {
        let cache_weight = CacheWeight::new(10);

        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 4));
        assert_eq!(4, cache_weight.get_weight_used());

        cache_weight.update(&KeyDescription::new(&"disk", 1, 3040, 4));
        assert_eq!(4, cache_weight.get_weight_used());
    }

    #[test]
    fn delete_key_weight() {
        let cache_weight = CacheWeight::new(10);

        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 3));
        assert_eq!(3, cache_weight.get_weight_used());

        cache_weight.delete(&1);
        assert_eq!(0, cache_weight.get_weight_used());
    }

    #[test]
    fn sample_size() {
        let cache_weight = CacheWeight::new(10);

        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 3));
        cache_weight.add(&KeyDescription::new(&"topic", 2, 1090, 4));
        cache_weight.add(&KeyDescription::new(&"SSD", 3, 1290, 3));

        let (mut iterator, sample) = cache_weight.sample(2, |_hash| 10);

        assert_eq!(2, sample.len());
        assert!(iterator.next().is_some())
    }

    #[test]
    fn sample_keys_with_distinct_frequencies() {
        let cache_weight = CacheWeight::new(10);

        cache_weight.add(&KeyDescription::new(&"disk", 1, 3040, 3));
        cache_weight.add(&KeyDescription::new(&"topic", 2, 1090, 4));
        cache_weight.add(&KeyDescription::new(&"SSD", 3, 1290, 3));

        let (_, mut sample) = cache_weight.sample(3, |hash| {
            match hash {
                3040 => 1,
                1090 => 2,
                1290 => 3,
                _ => 0
            }
        });
        assert_eq!(1, sample.pop().unwrap().estimated_frequency);
        assert_eq!(2, sample.pop().unwrap().estimated_frequency);
        assert_eq!(3, sample.pop().unwrap().estimated_frequency);
    }

    #[test]
    fn sample_keys_with_same_frequencies() {
        let cache_weight = CacheWeight::new(10);

        cache_weight.add(&KeyDescription::new(&"disk", 10, 3040, 5));
        cache_weight.add(&KeyDescription::new(&"topic", 20, 1090, 2));
        cache_weight.add(&KeyDescription::new(&"SSD", 30, 1290, 3));

        let (_, mut sample) = cache_weight.sample(3, |hash| {
            match hash {
                3040 => 1,
                1090 => 2,
                1290 => 1,
                _ => 0
            }
        });

        let sampled_key = sample.pop().unwrap();
        assert_eq!(1, sampled_key.estimated_frequency);
        assert_eq!(5, sampled_key.weight);
        assert_eq!(10, sampled_key.id);

        let sampled_key = sample.pop().unwrap();
        assert_eq!(1, sampled_key.estimated_frequency);
        assert_eq!(3, sampled_key.weight);
        assert_eq!(30, sampled_key.id);

        let sampled_key = sample.pop().unwrap();
        assert_eq!(2, sampled_key.estimated_frequency);
        assert_eq!(2, sampled_key.weight);
        assert_eq!(20, sampled_key.id);
    }
}