use std::hash::Hash;

use crate::cache::types::{KeyHash, KeyId, Weight};

/// The description of a key consists of the following:
    /// Key
    /// id of the key represented by id
    /// hash of the key represented by hash
    /// weight of the key represented by weight
pub(crate) struct KeyDescription<Key>
    where Key: Hash + Eq + Clone {
    key: Key,
    pub(crate) id: KeyId,
    pub(crate) hash: KeyHash,
    pub(crate) weight: Weight,
}

impl<Key> KeyDescription<Key>
    where Key: Hash + Eq + Clone {
    pub(crate) fn new(key: Key, id: KeyId, hash: KeyHash, weight: Weight) -> Self {
        KeyDescription { key, id, hash, weight }
    }

    /// Clones the key.
    /// Key needs to be cloned because it is added in 2 structures:
    /// [`crate::cache::store::Store`] and [`crate::cache::policy::cache_weight::CacheWeight`]
    pub(crate) fn clone_key(&self) -> Key { self.key.clone() }
}

#[cfg(test)]
mod tests {
    use crate::cache::key_description::KeyDescription;

    #[test]
    fn clone() {
        let key_description = KeyDescription::new("topic", 1, 1090, 10);
        let cloned = key_description.clone_key();

        assert_eq!(cloned, key_description.key);
    }
}