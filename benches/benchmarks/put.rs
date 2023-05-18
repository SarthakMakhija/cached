use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};
use rand::{Rng, thread_rng};
use rand_distr::Zipf;

use cached::cache::cached::CacheD;
use cached::cache::config::ConfigBuilder;
use cached::cache::types::{TotalCounters, Weight};

use crate::benchmarks::common::execute_parallel;

const CAPACITY: usize = 2 << 20;
const COUNTERS: TotalCounters = (CAPACITY * 10) as TotalCounters;
const WEIGHT: Weight = CAPACITY as Weight;

const ITEMS: usize = CAPACITY / 3;
const MASK: usize = CAPACITY - 1;

#[cfg(feature = "bench_testable")]
pub fn put_single_threaded(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());

    let distribution = distribution();
    let mut index = 0;

    criterion.bench_function("Cached.put() | No contention", |bencher| {
        bencher.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = cached.put(distribution[index & MASK], distribution[index & MASK]).unwrap();
                index += 1;
            }
            start.elapsed()
        });
    });
}

#[cfg(feature = "bench_testable")]
pub fn put_8_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Cached.put() | 8 threads", prepare_execution_block(), 8);
}

#[cfg(feature = "bench_testable")]
pub fn put_16_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Cached.put() | 16 threads", prepare_execution_block(), 16);
}

#[cfg(feature = "bench_testable")]
pub fn put_32_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Cached.put() | 32 threads", prepare_execution_block(), 32);
}

fn prepare_execution_block() -> Arc<impl Fn(u64) -> () + Send + Sync + 'static> {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution();

    Arc::new(move |index| {
        let key_index = index as usize;
        let _ = cached.put(distribution[key_index & MASK], distribution[key_index & MASK]).unwrap();
    })
}

fn distribution() -> Vec<u64> {
    thread_rng().sample_iter(Zipf::new(ITEMS as u64, 1.01).unwrap()).take(CAPACITY).map(|value| value as u64).collect::<Vec<_>>()
}

criterion_group!(benches, put_single_threaded, put_8_threads, put_16_threads, put_32_threads);
criterion_main!(benches);