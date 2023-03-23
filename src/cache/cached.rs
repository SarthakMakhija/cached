use std::collections::HashMap;
use std::hash::Hash;

struct CacheD<K, V>
    where K: Hash + Eq {
    store: HashMap<K, V>,
}

impl<K, V> CacheD<K, V>
    where K: Hash + Eq {
    fn new() -> Self {
        return CacheD {
            store: HashMap::new()
        };
    }

    fn put(&mut self, key: K, value: V) {
        self.store.insert(key, value);
    }

    fn get(&self, key: K) -> Option<&V> {
        return self.store.get(&key);
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::cached::CacheD;

    #[test]
    fn get_value_for_a_key() {
        let mut cached = CacheD::new();
        cached.put("topic", "microservices");

        let value = cached.get("topic");
        assert_eq!(Some(&"microservices"), value);
    }
}