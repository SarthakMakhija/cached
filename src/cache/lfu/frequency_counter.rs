use rand::Rng;

#[repr(transparent)]
#[derive(Debug, PartialEq)]
struct Row(Vec<u8>);

impl Row {
    fn increment_at(&mut self, position: u64) {
        let index = (position / 2) as usize;
        let shift = (position & 0x01) * 4;
        let is_less_than15 = (self.0[index] >> shift) & 0x0f < 0x0f;

        if is_less_than15 {
            self.0[index] = self.0[index] + (1 << shift);
        }
    }

    fn get_at(&self, position: u64) -> u8 {
        let index = (position / 2) as usize;
        let shift = (position & 0x01) * 4;

        return (self.0[index] >> shift) & 0x0f;
    }

    fn half_counters(&mut self) {
        self.0.iter_mut().for_each(|slice| {
            *slice = (*slice >> 1) & 0x77;
        });
    }
}

const ROWS: usize = 4;

//Implementation of count-min sketch
pub(crate) struct FrequencyCounter {
    matrix: [Row; ROWS],
    seeds: [u64; ROWS],
    total_counters: u64,
}

impl FrequencyCounter {
    pub(crate) fn new(counters: u64) -> FrequencyCounter {
        let total_counters = Self::next_power_2(counters);
        return FrequencyCounter {
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

    pub(crate) fn reset(&mut self) {
        (0..ROWS).for_each(|index| {
            let row = &mut self.matrix[index];
            row.half_counters();
        });
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
    use crate::cache::lfu::frequency_counter::{FrequencyCounter, Row};

    #[test]
    fn total_counters() {
        let frequency_counter = FrequencyCounter::new(18);
        assert_eq!(32, frequency_counter.total_counters);
    }

    #[test]
    fn increment_one_key_single_time() {
        let mut frequency_counter = FrequencyCounter::new(10);
        frequency_counter.increment(10);

        let count = frequency_counter.estimate(10);
        assert_eq!(1, count)
    }

    #[test]
    fn increment_one_key_multiple_times() {
        let mut frequency_counter = FrequencyCounter::new(10);
        frequency_counter.increment(10);
        frequency_counter.increment(10);
        frequency_counter.increment(10);

        let count = frequency_counter.estimate(10);
        assert_eq!(3, count)
    }

    #[test]
    fn increment_2_keys() {
        let mut frequency_counter = FrequencyCounter::new(10);
        frequency_counter.increment(10);
        frequency_counter.increment(10);
        frequency_counter.increment(15);
        frequency_counter.increment(15);

        assert_eq!(2, frequency_counter.estimate(10));
        assert_eq!(2, frequency_counter.estimate(15));
    }

    #[test]
    fn reset_count_for_a_row() {
        let mut row = Row(vec![15, 10, 240, 255]);

        row.half_counters();

        assert_eq!(7, row.0[0]);
        assert_eq!(5, row.0[1]);
        assert_eq!(112, row.0[2]); // 240/2 is 120 but it can not be represented without using both the lower and the upper 4 bits of our counter
        assert_eq!(7, row.0[3] & 0x0f); //lower 4 bits
        assert_eq!(7, row.0[3] >> 4 & 0x0f); //upper 4 bits
    }

    #[test]
    fn reset_count() {
        let mut frequency_counter = FrequencyCounter::new(2);
        frequency_counter.matrix[0] = Row(vec![15, 240]);
        frequency_counter.matrix[1] = Row(vec![64, 7]);
        frequency_counter.matrix[2] = Row(vec![192, 10]);
        frequency_counter.matrix[3] = Row(vec![48, 14]);

        frequency_counter.reset();

        assert_eq!(Row(vec![7, 112]), frequency_counter.matrix[0]);
        assert_eq!(Row(vec![32, 3]), frequency_counter.matrix[1]);
        assert_eq!(Row(vec![96, 5]), frequency_counter.matrix[2]);
        assert_eq!(Row(vec![16, 7]), frequency_counter.matrix[3]);
    }
}