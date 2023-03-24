use rand::Rng;

#[repr(transparent)]
#[derive(Debug)]
struct Row(Vec<u8>);

impl Row {
    pub(crate) fn increment_at(&mut self, position: u64) {
        let index = (position / 2) as usize;
        let shift = (position & 0x01) * 4;
        let is_less_than15 = (self.0[index] >> shift) & 0x0f < 0x0f;

        if is_less_than15 {
            self.0[index] = self.0[index] + (1 << shift);
        }
    }

    pub(crate) fn get_at(&self, position: u64) -> u8 {
        let index = (position / 2) as usize;
        let shift = (position & 0x01) * 4;

        return (self.0[index] >> shift) & 0x0f;
    }
}

const ROWS: usize = 4;

pub(crate) struct CountMinSketch {
    matrix: [Row; ROWS],
    seeds: [u64; ROWS],
    total_counters: u64,
}

impl CountMinSketch {
    pub(crate) fn new(counters: u64) -> CountMinSketch {
        let total_counters = Self::next_power_2(counters);
        return CountMinSketch {
            matrix: Self::matrix(total_counters),
            seeds: Self::seeds(),
            total_counters,
        };
    }

    pub(crate) fn increment(&mut self, key_hash: u64) {
        (0..ROWS).for_each(|index| {
            let hash = key_hash ^ self.seeds[index];
            let current_row = &mut self.matrix[index];
            current_row.increment_at(hash % self.total_counters)
        });
    }

    pub(crate) fn estimate(&self, key_hash: u64) -> u8 {
        let mut min = u8::MAX;
        (0..ROWS).for_each(|index| {
            let hash = key_hash ^ self.seeds[index];
            let current_row = &self.matrix[index];
            let current_min = current_row.get_at(hash % self.total_counters);

            if current_min < min {
                min = current_min;
            }
        });
        return min;
    }

    fn next_power_2(counters: u64) -> u64 {
        let mut updated_counters = counters;

        updated_counters = updated_counters - 1;
        updated_counters |= updated_counters >> 1;
        updated_counters |= updated_counters >> 2;
        updated_counters |= updated_counters >> 4;
        updated_counters |= updated_counters >> 8;
        updated_counters |= updated_counters >> 16;
        updated_counters |= updated_counters >> 32;

        updated_counters = updated_counters + 1;
        return updated_counters;
    }

    fn seeds() -> [u64; ROWS] {
        let mut random_number_generator = rand::thread_rng();
        let seeds =
            (0..ROWS)
                .map(|_index| random_number_generator.gen::<u64>())
                .collect::<Vec<u64>>();

        return seeds.try_into().unwrap();
    }

    fn matrix(total_counters: u64) -> [Row; ROWS] {
        let total_counters = (total_counters / 2) as usize;
        let rows =
            (0..ROWS)
                .map(|_index| Row(vec![0; total_counters]))
                .collect::<Vec<Row>>();

        return rows.try_into().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::tiny_lfu::count_min_sketch::CountMinSketch;

    #[test]
    fn total_counters() {
        let count_min_sketch = CountMinSketch::new(18);
        assert_eq!(32, count_min_sketch.total_counters);
    }

    #[test]
    fn increment_one_key_single_time() {
        let mut count_min_sketch = CountMinSketch::new(10);
        count_min_sketch.increment(10);

        let count = count_min_sketch.estimate(10);
        assert_eq!(1, count)
    }

    #[test]
    fn increment_one_key_multiple_times() {
        let mut count_min_sketch = CountMinSketch::new(10);
        count_min_sketch.increment(10);
        count_min_sketch.increment(10);
        count_min_sketch.increment(10);

        let count = count_min_sketch.estimate(10);
        assert_eq!(3, count)
    }

    #[test]
    fn increment_2_keys() {
        let mut count_min_sketch = CountMinSketch::new(10);
        count_min_sketch.increment(10);
        count_min_sketch.increment(10);
        count_min_sketch.increment(15);
        count_min_sketch.increment(15);

        assert_eq!(2, count_min_sketch.estimate(10));
        assert_eq!(2, count_min_sketch.estimate(15));
    }
}