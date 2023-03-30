use std::hash::Hash;

use crate::cache::types::{KeyHash, KeyId, Weight};

pub(crate) struct KeyDescription<'a, Key>
    where Key: Hash + Eq + Clone {
    pub(crate) key: &'a Key,
    pub(crate) id: KeyId,
    pub(crate) hash: KeyHash,
    pub(crate) weight: Weight,
}

impl<'a, Key> KeyDescription<'a, Key>
    where Key: Hash + Eq + Clone {
    pub(crate) fn new(key: &'a Key, id: KeyId, hash: KeyHash, weight: Weight) -> Self {
        KeyDescription { key, id, hash, weight }
    }
}