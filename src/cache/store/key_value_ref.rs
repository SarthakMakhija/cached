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