use bloomfilter::Bloom;

use crate::cache::types::{DoorKeeperCapacity, DoorKeeperFalsePositiveRate, KeyHash};

pub(crate) struct DoorKeeper {
    bloom: Bloom<KeyHash>,
}

impl DoorKeeper {
    pub(crate) fn new(capacity: DoorKeeperCapacity, false_positive: DoorKeeperFalsePositiveRate) -> Self {
        DoorKeeper {
            bloom: Bloom::new_for_fp_rate(capacity, false_positive)
        }
    }

    pub(crate) fn add_if_missing(&mut self, key: &KeyHash) -> bool {
        if !self.has(key) {
            self.bloom.set(key);
            return true;
        }
        false
    }

    pub(crate) fn has(&self, key: &KeyHash) -> bool {
        self.bloom.check(key)
    }

    pub(crate) fn clear(&mut self) {
        self.bloom.clear();
    }

    fn add(&mut self, key: &KeyHash) {
        self.bloom.set(key);
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::lfu::doorkeeper::DoorKeeper;

    #[test]
    fn add_if_missing() {
        let mut door_keeper = DoorKeeper::new(100, 0.01);
        door_keeper.add_if_missing(&200);

        assert!(door_keeper.has(&200));
        door_keeper.clear();
    }

    #[test]
    fn do_not_add_if_not_missing() {
        let mut door_keeper = DoorKeeper::new(100, 0.01);
        door_keeper.add_if_missing(&200);
        let added = door_keeper.add_if_missing(&200);

        assert!(door_keeper.has(&200));
        assert!(!added);
        door_keeper.clear();
    }

    #[test]
    fn add() {
        let mut door_keeper = DoorKeeper::new(100, 0.01);
        door_keeper.add(&200);

        assert!(door_keeper.has(&200));
        door_keeper.clear();
    }

    #[test]
    fn add_multiple_keys() {
        let mut door_keeper = DoorKeeper::new(100, 0.01);
        door_keeper.add(&200);
        door_keeper.add(&100);

        assert!(door_keeper.has(&100));
        assert!(door_keeper.has(&200));
        door_keeper.clear();
    }

    #[test]
    fn does_not_contain_after_clear() {
        let mut door_keeper = DoorKeeper::new(100, 0.01);
        door_keeper.add(&200);
        door_keeper.add(&100);

        door_keeper.clear();
        assert!(!door_keeper.has(&100));
        assert!(!door_keeper.has(&200));
    }
}