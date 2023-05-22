use criterion::{Bencher, Criterion, criterion_group, criterion_main};

use cached::cache::proxy::frequency_counter::ProxyFrequencyCounter;
use cached::cache::types::TotalCounters;
use crate::benchmarks::common::distribution;

/// Defines the total size of the distribution that will be used in increment and estimate methods of `FrequencyCounter`.
const CAPACITY: usize = 2 << 20;

/// Defines the total sample size that is used for generating Zipf distribution.
const ITEMS: usize = CAPACITY / 3;

const MASK: usize = CAPACITY - 1;

/// As a part of this benchmark we measure the `FrequencyCounter.increment` and `FrequencyCounter.estimate` methods
/// In order to benchmark the `increment` method, we invoke the `increment` method a total of CAPACITY times by
/// treating each element of the distribution as a KeyHash.
/// In order to benchmark the `estimate` method, we perform `setup` which loads the distribution in the `FrequencyCounter`
/// and then we invoke the `estimate` method a total of CAPACITY times by treating each element of the distribution as a KeyHash.

/// This benchmark also varies the `total_counters` which is CAPACITY, CAPACITY*2, CAPACITY*10

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn increase_frequency(criterion: &mut Criterion) {
    let distribution = distribution(ITEMS as u64, CAPACITY);
    let mut group = criterion.benchmark_group("increase frequency");

    group.bench_function("counters = 2097152", |bencher| {
        increment(&distribution, bencher, 2097152);
    });
    group.bench_function("counters = 2097152 * 2", |bencher| {
        increment(&distribution, bencher, 2097152 * 2);
    });
    group.bench_function("counters = 2097152 * 10", |bencher| {
        increment(&distribution, bencher, 2097152 * 10);
    });

    group.finish();
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn estimate_frequency(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("estimate frequency");

    let mut counter = ProxyFrequencyCounter::new(2097152);
    let distribution = setup(&mut counter);
    group.bench_function("counters = 2097152", |bencher| {
        estimate(&counter, &distribution, bencher);
    });

    let mut counter = ProxyFrequencyCounter::new(2097152 * 2);
    let distribution = setup(&mut counter);
    group.bench_function("counters = 2097152 * 2", |bencher| {
        estimate(&counter, &distribution, bencher);
    });

    let mut counter = ProxyFrequencyCounter::new(2097152 * 10);
    let distribution = setup(&mut counter);
    group.bench_function("counters = 2097152 * 10", |bencher| {
        estimate(&counter, &distribution, bencher);
    });

    group.finish();
}

#[cfg(not(tarpaulin_include))]
fn increment(distribution: &[u64], bencher: &mut Bencher, total_counters: TotalCounters) {
    let mut index = 0;
    let mut counter = ProxyFrequencyCounter::new(total_counters);

    bencher.iter(|| {
        let key_hash = distribution[index & MASK];
        counter.increment(key_hash);
        index += 1;
    })
}

#[cfg(not(tarpaulin_include))]
fn setup(frequency_counter: &mut ProxyFrequencyCounter) -> Vec<u64> {
    let distribution = distribution(ITEMS as u64, CAPACITY);
    for item in &distribution {
        frequency_counter.increment(*item);
    }
    distribution
}

#[cfg(not(tarpaulin_include))]
fn estimate(counter: &ProxyFrequencyCounter, distribution: &[u64], bencher: &mut Bencher) {
    let mut index = 0;

    bencher.iter(|| {
        let key_hash = distribution[index & MASK];
        counter.estimate(key_hash);
        index += 1;
    });
}

criterion_group!(benches, increase_frequency, estimate_frequency);
criterion_main!(benches);