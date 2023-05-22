use std::hash::Hash;

use dashmap::mapref::one::Ref;

/// KeyValueRef contains DashMap's Ref [`dashmap::mapref::one::Ref`] which internally holds
/// a `RwLockReadGuard` for the shard.
/// Any time `get_ref` method is invoked, the `Store` returns `Option<KeyValueRef<'_, Key, StoredValue<Value>>>`.
/// If the key is present in the `Store`, `get_ref` will return `Some<KeyValueRef<'_, Key, StoredValue<Value>>>`.
/// Hence, the invocation of `get_ref` will hold a lock against the shard that contains the key (within the scope of its usage).
/// The principle idea behind having this abstraction is to hide DashMap's Ref [`dashmap::mapref::one::Ref`].
pub struct KeyValueRef<'a, Key, Value>
    where Key: Eq + Hash {
    key_value_ref: Ref<'a, Key, Value>,
}

impl<'a, Key, Value> KeyValueRef<'a, Key, Value>
    where Key: Eq + Hash {
    pub(crate) fn new(key_value_ref: Ref<'a, Key, Value>) -> Self <> {
        KeyValueRef {
            key_value_ref
        }
    }

    /// Returns the reference of the key present in the Store
    pub fn key(&self) -> &Key {
        self.key_value_ref.key()
    }

    /// Returns the reference of the value present in the Store
    pub fn value(&self) -> &Value {
        self.key_value_ref.value()
    }
}

#[cfg(test)]
mod tests {
    use dashmap::DashMap;
    use crate::cache::store::key_value_ref::KeyValueRef;

    #[test]
    fn get_key() {
        let key_values = DashMap::new();
        key_values.insert("topic", "microservices");
        let value_ref = key_values.get(&"topic").unwrap();

        let key_value_ref = KeyValueRef::new(value_ref);
        assert_eq!(&"topic", key_value_ref.key());
    }

    #[test]
    fn get_value() {
        let key_values = DashMap::new();
        key_values.insert("topic", "microservices");
        let value_ref = key_values.get(&"topic").unwrap();

        let key_value_ref = KeyValueRef::new(value_ref);
        assert_eq!(&"microservices", key_value_ref.value());
    }
}