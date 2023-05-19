use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

use cached::cache::proxy::admission_policy::ProxyAdmissionPolicy;
use cached::cache::proxy::pool::ProxyPool;
use cached::cache::types::{TotalCapacity, TotalCounters, TotalShards, Weight};

use crate::benchmarks::common::execute_parallel;

const COUNTERS: TotalCounters = 2 << 20;
const CAPACITY: TotalCapacity = 1_000_000;
const SHARDS: TotalShards = 256;
const CACHE_WEIGHT: Weight = CAPACITY as Weight;

#[cfg(feature = "bench_testable")]
pub fn read_buffer_one_thread(criterion: &mut Criterion) {
    criterion.bench_function("Pool.add() | No contention", |bencher| bencher.iter_custom(|iterations| {
        let consumer = Arc::new(ProxyAdmissionPolicy::<u64>::new(COUNTERS, CAPACITY, SHARDS, CACHE_WEIGHT));
        let pool = ProxyPool::new(32, 64, consumer);

        let start = Instant::now();
        for _index in 0..iterations {
            pool.add(1);
        }
        start.elapsed()
    }));
}

#[cfg(feature = "bench_testable")]
pub fn read_buffer_8_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Pool.add() | 8 threads", prepare_execution_block(), 8);
}

#[cfg(feature = "bench_testable")]
pub fn read_buffer_16_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Pool.add() | 16 threads", prepare_execution_block(), 16);
}

#[cfg(feature = "bench_testable")]
pub fn read_buffer_32_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Pool.add() | 32 threads", prepare_execution_block(), 32);
}

fn prepare_execution_block() -> Arc<impl Fn(u64) + Send + Sync + 'static> {
    let consumer = Arc::new(ProxyAdmissionPolicy::<u64>::new(COUNTERS, CAPACITY, SHARDS, CACHE_WEIGHT));
    let pool = Arc::new(ProxyPool::new(32, 64, consumer));

    Arc::new(move |index| {
        pool.add(index);
    })
}

criterion_group!(benches, read_buffer_one_thread, read_buffer_8_threads, read_buffer_16_threads, read_buffer_32_threads);
criterion_main!(benches);