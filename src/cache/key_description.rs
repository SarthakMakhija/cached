use std::hash::Hash;

use crate::cache::types::{KeyHash, KeyId, Weight};

pub(crate) struct KeyDescription<Key>
    where Key: Hash + Eq + Clone {
    pub(crate) key: Key,
    pub(crate) id: KeyId,
    pub(crate) hash: KeyHash,
    pub(crate) weight: Weight,
}

impl<Key> KeyDescription<Key>
    where Key: Hash + Eq + Clone {
    pub(crate) fn new(key: Key, id: KeyId, hash: KeyHash, weight: Weight) -> Self {
        KeyDescription { key, id, hash, weight }
    }
}