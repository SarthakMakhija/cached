use std::collections::HashMap;
use std::hash::Hash;

use crate::cache::types::Weight;

pub(crate) struct CacheWeight<Key>
    where Key: Hash + Eq + Send + Sync + 'static, {
    max_weight: Weight,
    weight_used: Weight,
    cost_by_key: HashMap<Key, Weight>,
}

impl<Key> CacheWeight<Key>
    where Key: Hash + Eq + Send + Sync + 'static, {
    fn new(max_weight: Weight) -> Self <> {
        CacheWeight {
            max_weight,
            weight_used: 0,
            cost_by_key: HashMap::new(),
        }
    }

    pub(crate) fn get_max_weight(&self) -> Weight {
        self.max_weight
    }

    pub(crate) fn add(&mut self, key: Key, weight: Weight) {
        self.weight_used += weight;
        self.cost_by_key.insert(key, weight);
    }

    pub(crate) fn update(&mut self, key: Key, weight: Weight) {
        self.cost_by_key.entry(key).and_modify(|existing_weight| {
            self.weight_used += weight - *existing_weight;
            *existing_weight = weight;
        });
    }

    pub(crate) fn delete(&mut self, key: &Key) {
        if let Some(weight) = self.cost_by_key.remove(key) {
            self.weight_used -= weight;
        }
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
        cache_weight.add("disk", 3);

        assert_eq!(3, cache_weight.weight_used);
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_less() {
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 3);
        assert_eq!(3, cache_weight.weight_used);

        cache_weight.update("disk", 2);
        assert_eq!(2, cache_weight.weight_used);
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_more() {
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 4);
        assert_eq!(4, cache_weight.weight_used);

        cache_weight.update("disk", 8);
        assert_eq!(8, cache_weight.weight_used);
    }

    #[test]
    fn update_key_weight_given_the_updated_weight_is_same() {
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 4);
        assert_eq!(4, cache_weight.weight_used);

        cache_weight.update("disk", 4);
        assert_eq!(4, cache_weight.weight_used);
    }

    #[test]
    fn delete_key_weight() {
        let mut cache_weight = CacheWeight::new(10);

        cache_weight.add("disk", 3);
        assert_eq!(3, cache_weight.weight_used);

        cache_weight.delete(&"disk");
        assert_eq!(0, cache_weight.weight_used);
    }
}