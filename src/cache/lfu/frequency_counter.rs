use log::{debug, info};
use rand::Rng;
use crate::cache::types::{FrequencyEstimate, KeyHash, TotalCounters};

const BINARY_ONE: u64 = 0x01;
const MAX_VALUE_LOWER_FOUR_BITS: u8 = 0x0f;
const HALF_COUNTERS_BITS: u8 = 0x77;
const SHIFT_OFFSET: u64 = 4;

#[repr(transparent)]
#[derive(Debug, PartialEq)]
struct Row(Vec<u8>);

impl Row {
    fn increment_at(&mut self, position: u64) {
        // Get the index
        let index = (position / 2) as usize;

        // If the position is an odd number, upper four bits store the counter value,
        // else lower four bits store the counter value
        let shift = (position & BINARY_ONE) * SHIFT_OFFSET;
        let is_less_than15 = (self.0[index] >> shift) & MAX_VALUE_LOWER_FOUR_BITS < MAX_VALUE_LOWER_FOUR_BITS;

        // If the value is less than 15, increment
        if is_less_than15 {
            self.0[index] += 1 << shift;
        }
    }

    fn get_at(&self, position: u64) -> FrequencyEstimate {
        // Get the index
        let index = (position / 2) as usize;

        // If the position is an odd number, the upper four bits store the counter value,
        // else lower four bits store the counter value
        let shift = (position & BINARY_ONE) * SHIFT_OFFSET;

        // Perform the shift (shift would be either 0 or 4)
        // Perform an AND operation with 0x0f, which 00001111
        (self.0[index] >> shift) & MAX_VALUE_LOWER_FOUR_BITS
    }

    fn half_counters(&mut self) {
        self.0.iter_mut().for_each(|slice| {
            *slice = (*slice >> 1) & HALF_COUNTERS_BITS;
        });
    }

    fn clear(&mut self) {
        self.0.iter_mut().for_each(|slice| {
            *slice = 0;
        });
    }
}

const ROWS: usize = 4;

/// FrequencyCounter is an implementation of count-min sketch based on 4 bit counter taken from
/// https://github.com/dgryski/go-tinylfu/blob/master/cm4.go
/// More on 4 bit counter is available [here](https://tech-lessons.in/blog/count_min_sketch/#4-bit-counter)
/// Count-min sketch (CM sketch) is a probabilistic data structure1 used to estimate the frequency of events in a data stream.
/// It relies on hash functions to map events to frequencies, but unlike a hash table, it uses only sublinear space at the expense of over-counting some events due to hash collisions.
pub(crate) struct FrequencyCounter {
    matrix: [Row; ROWS],
    seeds: [u64; ROWS],
    total_counters: TotalCounters,
}

impl FrequencyCounter {
    pub(crate) fn new(counters: TotalCounters) -> FrequencyCounter {
        let total_counters = Self::next_power_2(counters);
        info!("Initializing FrequencyCounter with total counters {}", counters);
        FrequencyCounter {
            matrix: Self::matrix(total_counters),
            seeds: Self::seeds(),
            total_counters,
        }
    }

    pub(crate) fn increment(&mut self, key_hash: KeyHash) {
        (0..ROWS).for_each(|index| {
            let hash = key_hash ^ self.seeds[index];
            let current_row = &mut self.matrix[index];
            current_row.increment_at(hash % self.total_counters)
        });
    }

    pub(crate) fn estimate(&self, key_hash: KeyHash) -> FrequencyEstimate {
        let mut min = u8::MAX;
        (0..ROWS).for_each(|index| {
            let hash = key_hash ^ self.seeds[index];
            let current_row = &self.matrix[index];
            let current_min = current_row.get_at(hash % self.total_counters);

            if current_min < min {
                min = current_min;
            }
        });
        min
    }

    pub(crate) fn reset(&mut self) {
        debug!("Resetting the counters");
        (0..ROWS).for_each(|index| {
            let row = &mut self.matrix[index];
            row.half_counters();
        });
    }

    pub(crate) fn clear(&mut self) {
        (0..ROWS).for_each(|index| {
            let row = &mut self.matrix[index];
            row.clear();
        });
    }

    fn next_power_2(counters: TotalCounters) -> u64 {
        let mut updated_counters = counters;
        updated_counters -= 1;

        updated_counters |= updated_counters >> 1;
        updated_counters |= updated_counters >> 2;
        updated_counters |= updated_counters >> 4;
        updated_counters |= updated_counters >> 8;
        updated_counters |= updated_counters >> 16;
        updated_counters |= updated_counters >> 32;

        updated_counters += 1;
        updated_counters
    }

    fn seeds() -> [u64; ROWS] {
        let mut random_number_generator = rand::thread_rng();
        let seeds =
            (0..ROWS)
                .map(|_index| random_number_generator.gen::<u64>())
                .collect::<Vec<u64>>();

        seeds.try_into().unwrap()
    }

    fn matrix(total_counters: TotalCounters) -> [Row; ROWS] {
        let total_counters = (total_counters / 2) as usize;
        let rows =
            (0..ROWS)
                .map(|_index| Row(vec![0; total_counters]))
                .collect::<Vec<Row>>();

        rows.try_into().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::lfu::frequency_counter::{FrequencyCounter, MAX_VALUE_LOWER_FOUR_BITS, Row};

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
        assert_eq!(7, row.0[3] & MAX_VALUE_LOWER_FOUR_BITS); //lower 4 bits
        assert_eq!(7, row.0[3] >> 4 & MAX_VALUE_LOWER_FOUR_BITS); //upper 4 bits
    }

    #[test]
    fn reset_frequency_counter() {
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

    #[test]
    fn clear_row() {
        let mut row = Row(vec![15, 10, 240, 255]);

        row.clear();

        assert_eq!(0, row.0[0]);
        assert_eq!(0, row.0[1]);
        assert_eq!(0, row.0[2]);
        assert_eq!(0, row.0[3] >> 4 & MAX_VALUE_LOWER_FOUR_BITS);
    }

    #[test]
    fn clear_frequency_counter() {
        let mut frequency_counter = FrequencyCounter::new(2);
        frequency_counter.matrix[0] = Row(vec![15, 240]);
        frequency_counter.matrix[1] = Row(vec![64, 7]);
        frequency_counter.matrix[2] = Row(vec![192, 10]);
        frequency_counter.matrix[3] = Row(vec![48, 14]);

        frequency_counter.clear();

        assert_eq!(Row(vec![0, 0]), frequency_counter.matrix[0]);
        assert_eq!(Row(vec![0, 0]), frequency_counter.matrix[1]);
        assert_eq!(Row(vec![0, 0]), frequency_counter.matrix[2]);
        assert_eq!(Row(vec![0, 0]), frequency_counter.matrix[3]);
    }
}