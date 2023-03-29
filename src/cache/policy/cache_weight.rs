use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::Hash;
use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;

use crate::cache::types::{KeyHash, Weight};

struct WeightByKeyHash {
    weight: Weight,
    key_hash: KeyHash,
}

impl WeightByKeyHash {
    fn new(weight: Weight, key_hash: KeyHash) -> Self {
        WeightByKeyHash {
            weight,
            key_hash,
        }
    }
}

pub(crate) struct CacheWeight<Key>
    where Key: Hash + Eq + Send + Sync + 'static, {
    max_weight: Weight,
    weight_used: Weight,
    key_weights: DashMap<Key, WeightByKeyHash>,
}

pub(crate) struct SampledKey<'a, Key>
    where Key: Eq {
    pair: RefMulti<'a, Key, WeightByKeyHash>,
    estimated_frequency: u8, //TODO: type for frequency?
}

impl<'a, Key> Ord for SampledKey<'a, Key>
    where Key: Eq + Hash {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.estimated_frequency, self.pair.weight).cmp(&(self.estimated_frequency, other.pair.weight))
    }
}

impl<'a, Key> PartialOrd for SampledKey<'a, Key>
    where Key: Eq + Hash {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, Key> PartialEq for SampledKey<'a, Key>
    where Key: Eq + Hash {
    fn eq(&self, other: &Self) -> bool {
        self.pair.key() == other.pair.key()
    }
}

impl<'a, Key> Eq for SampledKey<'a, Key>
    where Key: Eq + Hash{}

impl<'a, Key> SampledKey<'a, Key>
    where Key: Eq {
    fn new(pair: RefMulti<'a, Key, WeightByKeyHash>, frequency: u8) -> Self<> {
        SampledKey {
            pair,
            estimated_frequency: frequency,
        }
    }
}

impl<Key> CacheWeight<Key>
    where Key: Hash + Eq + Send + Sync + 'static, {
    fn new(max_weight: Weight) -> Self <> {
        CacheWeight {
            max_weight,
            weight_used: 0,
            key_weights: DashMap::new(),
        }
    }

    pub(crate) fn get_max_weight(&self) -> Weight {
        self.max_weight
    }

    //TODO: Combine key and key_hash together?
    pub(crate) fn add(&mut self, key: Key, key_hash: KeyHash, weight: Weight) {
        self.weight_used += weight;
        self.key_weights.insert(key, WeightByKeyHash::new(weight, key_hash));
    }

    //TODO: Combine key and key_hash together?
    pub(crate) fn update(&mut self, key: Key, key_hash: KeyHash, weight: Weight) {
        self.key_weights.entry(key).and_modify(|weight_by_key_hash| {
            self.weight_used += weight - weight_by_key_hash.weight;
            *weight_by_key_hash = WeightByKeyHash::new(weight, key_hash);
        });
    }

    pub(crate) fn delete(&mut self, key: &Key) {
        if let Some(weight_by_key_hash) = self.key_weights.remove(key) {
            self.weight_used -= weight_by_key_hash.1.weight
        }
    }

    pub(crate) fn sample<Freq>(&self, size: usize, frequency_counter: Freq) -> BinaryHeap<SampledKey<'_, Key>>
        where Freq: Fn(KeyHash) -> u8 {
        let mut sample = BinaryHeap::new();
        self.key_weights.iter().take(size).for_each(|pair| {
            let frequency = frequency_counter(pair.value().key_hash);
            sample.push(SampledKey::new(pair, frequency));
        });
        sample
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::policy::cache_weight::CacheWeight;

    #[test]
    fn maximum_cache_weight() {
        let cache_weight: CacheWeight<&str> = CacheWeight::new(10);
        assert_eq!(10, cache_weight.get_max_weight());
    }

    #[test]
    fn add_key_weight() {
        let mut cache_weight = CacheWeight::new(10);
        cache_weight.add("disk", 3040, 3);

        assert_eq!(3, cache_weight.weight_used);
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_less() {
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 3040, 3);
        assert_eq!(3, cache_weight.weight_used);

        cache_weight.update("disk", 3040, 2);
        assert_eq!(2, cache_weight.weight_used);
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_more() {
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 3040, 4);
        assert_eq!(4, cache_weight.weight_used);

        cache_weight.update("disk", 3040, 8);
        assert_eq!(8, cache_weight.weight_used);
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_same() {
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 3040, 4);
        assert_eq!(4, cache_weight.weight_used);

        cache_weight.update("disk", 3040, 4);
        assert_eq!(4, cache_weight.weight_used);
    }

    #[test]
    fn delete_key_weight() {
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 3040, 3);
        assert_eq!(3, cache_weight.weight_used);

        cache_weight.delete(&"disk");
        assert_eq!(0, cache_weight.weight_used);
    }

    #[test]
    fn sample_keys_with_distinct_frequencies() {
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 3040, 3);
        cache_weight.add("topic", 1090, 4);
        cache_weight.add("SSD", 1290, 3);

        let mut sample = cache_weight.sample(3, |hash| {
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
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 3040, 5);
        cache_weight.add("topic", 1090, 2);
        cache_weight.add("SSD", 1290, 3);

        let mut sample = cache_weight.sample(3, |hash| {
            match hash {
                3040 => 1,
                1090 => 2,
                1290 => 1,
                _ => 0
            }
        });

        let sampled_key = sample.pop().unwrap();
        assert_eq!(1, sampled_key.estimated_frequency);
        assert_eq!(5, sampled_key.pair.weight);
        assert_eq!(&"disk", sampled_key.pair.key());

        let sampled_key = sample.pop().unwrap();
        assert_eq!(1, sampled_key.estimated_frequency);
        assert_eq!(3, sampled_key.pair.weight);
        assert_eq!(&"SSD", sampled_key.pair.key());

        let sampled_key = sample.pop().unwrap();
        assert_eq!(2, sampled_key.estimated_frequency);
        assert_eq!(2, sampled_key.pair.weight);
        assert_eq!(&"topic", sampled_key.pair.key());
    }
}