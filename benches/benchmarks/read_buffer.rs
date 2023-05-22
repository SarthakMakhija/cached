use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

use cached::cache::proxy::admission_policy::ProxyAdmissionPolicy;
use cached::cache::proxy::pool::ProxyPool;
use cached::cache::types::{TotalCapacity, TotalCounters, TotalShards, Weight};

use crate::benchmarks::common::execute_parallel;

/// Defines the total capacity for the storage that is used to holding the weight of each key.
/// For this benchmark, capacity, counters and shards will not play a significant role.
const CAPACITY: TotalCapacity = 1_000_000;

/// Defines the total number of counters used to measure the access frequency.
const COUNTERS: TotalCounters = 2 << 20;

/// Defines the total shards for the storage that is used to holding the weight of each key.
const SHARDS: TotalShards = 256;

/// Defines the total size of the cache.
const CACHE_WEIGHT: Weight = CAPACITY as Weight;

/// This benchmark measures the `pool.add` method.
/// Pool represents a ring-buffer that is used to buffer the gets for various keys.
/// We simulate the contention by running it with 8/16/32 threads. `ProxyAdmissionPolicy` is used as a `BufferConsumer`.

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
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
#[cfg(not(tarpaulin_include))]
pub fn read_buffer_8_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Pool.add() | 8 threads", prepare_execution_block(), 8);
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn read_buffer_16_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Pool.add() | 16 threads", prepare_execution_block(), 16);
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn read_buffer_32_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Pool.add() | 32 threads", prepare_execution_block(), 32);
}

#[cfg(not(tarpaulin_include))]
fn prepare_execution_block() -> Arc<impl Fn(u64) + Send + Sync + 'static> {
    let consumer = Arc::new(ProxyAdmissionPolicy::<u64>::new(COUNTERS, CAPACITY, SHARDS, CACHE_WEIGHT));
    let pool = Arc::new(ProxyPool::new(32, 64, consumer));

    Arc::new(move |index| {
        pool.add(index);
    })
}

criterion_group!(benches, read_buffer_one_thread, read_buffer_8_threads, read_buffer_16_threads, read_buffer_32_threads);
criterion_main!(benches);