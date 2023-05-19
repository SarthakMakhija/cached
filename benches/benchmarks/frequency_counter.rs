use criterion::{Bencher, Criterion, criterion_group, criterion_main};

use cached::cache::proxy::frequency_counter::ProxyFrequencyCounter;
use cached::cache::types::TotalCounters;
use crate::benchmarks::common::distribution;

const CAPACITY: usize = 2 << 20;
const MASK: usize = CAPACITY - 1;
const ITEMS: usize = CAPACITY / 3;

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