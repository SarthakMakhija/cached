use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::*;
use rand_distr::Zipf;

use cached::cache::proxy::frequency_counter::ProxyFrequencyCounter;
use cached::cache::types::KeyHash;

const SIZE: usize = 2 << 20;
const MASK: usize = SIZE - 1;
const ITEMS: usize = SIZE / 3;

#[cfg(feature = "bench_testable")]
pub fn increase_frequency(criterion: &mut Criterion) {
    let distribution = distribution();
    let mut group = criterion.benchmark_group("increase frequency");

    let mut index = 0;
    group.bench_function("counters = 2097152", |bencher| {
        let mut counter = ProxyFrequencyCounter::new(2097152);
        bencher.iter(|| {
            let key_hash = distribution[index & MASK];
            counter.increment(key_hash as u64);
            index += 1;
        })
    });

    let mut index = 0;
    group.bench_function("counters = 2097152 * 2", |bencher| {
        let mut counter = ProxyFrequencyCounter::new(2097152 * 2);
        bencher.iter(|| {
            let key_hash = distribution[index & MASK];
            counter.increment(key_hash as u64);
            index += 1;
        })
    });

    let mut index = 0;
    group.bench_function("counters = 2097152 * 10", |bencher| {
        let mut counter = ProxyFrequencyCounter::new(2097152 * 10);
        bencher.iter(|| {
            let key_hash = distribution[index & MASK];
            counter.increment(key_hash as u64);
            index += 1;
        })
    });

    group.finish();
}

#[cfg(feature = "bench_testable")]
pub fn estimate_frequency(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("estimate frequency");
    let mut counter = ProxyFrequencyCounter::new(2097152);
    let distribution = setup(&mut counter);

    let mut index = 0;
    group.bench_function("counters = 2097152", |bencher| {
        bencher.iter(|| {
            let key_hash = distribution[index & MASK];
            counter.estimate(key_hash as u64);
            index += 1;
        });
    });

    let mut counter = ProxyFrequencyCounter::new(2097152 * 2);
    let distribution = setup(&mut counter);

    let mut index = 0;
    group.bench_function("counters = 2097152 * 2", |bencher| {
        bencher.iter(|| {
            let key_hash = distribution[index & MASK];
            counter.estimate(key_hash as u64);
            index += 1;
        });
    });

    let mut counter = ProxyFrequencyCounter::new(2097152 * 10);
    let distribution = setup(&mut counter);

    let mut index = 0;
    group.bench_function("counters = 2097152 * 10", |bencher| {
        bencher.iter(|| {
            let key_hash = distribution[index & MASK];
            counter.estimate(key_hash as u64);
            index += 1;
        });
    });

    group.finish();
}

fn setup(frequency_counter: &mut ProxyFrequencyCounter) -> Vec<f64> {
    let distribution = distribution();
    for item in &distribution {
        frequency_counter.increment(*item as KeyHash);
    }
    distribution
}

fn distribution() -> Vec<f64> {
    thread_rng().sample_iter(Zipf::new(ITEMS as u64, 1.5).unwrap()).take(SIZE).collect::<Vec<_>>()
}

criterion_group!(benches, increase_frequency, estimate_frequency);
criterion_main!(benches);