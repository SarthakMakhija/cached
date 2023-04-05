use std::hash::Hash;

use dashmap::mapref::one::Ref;

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

    pub fn key(&self) -> &Key {
        self.key_value_ref.key()
    }

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