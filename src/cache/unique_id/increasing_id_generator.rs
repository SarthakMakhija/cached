use std::sync::atomic::{AtomicU64, Ordering};

/// IncreasingIdGenerator generates ids for the incoming keys.
/// Each id is an [`AtomicU64`]
pub(crate) struct IncreasingIdGenerator {
    id: AtomicU64,
}

impl IncreasingIdGenerator {
    pub(crate) fn new() -> Self {
        IncreasingIdGenerator {
            id: AtomicU64::new(1)
        }
    }

    /// Returns the next id for the incoming key
    /// The first generated id is 1 and each invocation of next results in an increasing id.
    pub(crate) fn next(&self) -> u64 {
        self.id.fetch_add(1, Ordering::AcqRel)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::cache::unique_id::increasing_id_generator::IncreasingIdGenerator;

    #[test]
    fn generate_ids() {
        let id_generator = IncreasingIdGenerator::new();
        assert_eq!(1, id_generator.next());
        assert_eq!(2, id_generator.next());
        assert_eq!(3, id_generator.next());
    }

    #[test]
    fn generate_ids_in_concurrent_world() {
        let id_generator = IncreasingIdGenerator::new();
        thread::scope(|scope| {
            let mut ids =
                (1..=100)
                    .map(|_count| scope.spawn(|| id_generator.next()))
                    .map(|handle| handle.join().unwrap())
                    .collect::<Vec<_>>();
            ids.sort();

            let expected_ids = (1..=100).collect::<Vec<u64>>();
            assert_eq!(expected_ids, ids);
        });
    }
}