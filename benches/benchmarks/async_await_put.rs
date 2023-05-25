use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Builder;

use tinylfu_cached::cache::cached::CacheD;
use tinylfu_cached::cache::config::ConfigBuilder;
use tinylfu_cached::cache::types::{TotalCounters, Weight};
use crate::benchmarks::common::{distribution, execute_async};

/// Defines the total number of key/value pairs that are loaded in the cache
const CAPACITY: usize = 2 << 20;

/// Defines the total number of counters used to measure the access frequency.
/// Its value will not impact this benchmark as we are not accessing the keys
const COUNTERS: TotalCounters = (CAPACITY * 10) as TotalCounters;

/// Defines the total size of the cache. It is kept same as the capacity,
/// however the benchmark inserts keys and values of type u64.
/// Weight of a single u64 key and u64 value without time_to_live is 40 bytes. Check `src/cache/config/weight_calculation.rs`
/// Keeping WEIGHT = CAPACITY will cause some key rejections at the level of AdmissionPolicy and will be more realistic
/// than keeping the total weight as CAPACITY * 40.
const WEIGHT: Weight = CAPACITY as Weight;

/// Defines the total sample size that is used for generating Zipf distribution.
const ITEMS: usize = CAPACITY / 3;

const MASK: usize = CAPACITY - 1;

/// This benchmark differs from `put.rs` benchmark in a way that it performs an `.await` operation on the result of `put` operation.
/// As a part of this benchmark we leverage `to_async` function of `criterion` and use `tokio` as the async runtime.

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn async_put_single_task(criterion: &mut Criterion) {
    criterion.bench_function("Async Cached.put() | No contention", |bencher| {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        bencher.to_async(runtime).iter_custom(|iterations| {
            async move {
                let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());

                let distribution = distribution(ITEMS as u64, CAPACITY);
                let mut index = 0;

                let start = Instant::now();
                for _ in 0..iterations {
                    cached.put(distribution[index & MASK], distribution[index & MASK]).unwrap().handle().await;
                    index += 1;
                }
                start.elapsed()
            }
        });
    });
}

#[cfg(not(tarpaulin_include))]
pub fn async_put_8_tasks(criterion: &mut Criterion) {
    let cached = Arc::new(CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build()));
    let distribution = Arc::new(distribution(ITEMS as u64, CAPACITY));

    let block = move |index| {
        let cached = cached.clone();
        let distribution = distribution.clone();

        async move {
            let key_index = index as usize;
            cached.put(distribution[key_index & MASK], distribution[key_index & MASK]).unwrap().handle().await;
        }
    };
    execute_async(criterion, "Async Cached.put() | 8 tasks", Arc::new(block), 8, 8);
}

#[cfg(not(tarpaulin_include))]
pub fn async_put_16_tasks(criterion: &mut Criterion) {
    let cached = Arc::new(CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build()));
    let distribution = Arc::new(distribution(ITEMS as u64, CAPACITY));

    let block = move |index| {
        let cached = cached.clone();
        let distribution = distribution.clone();

        async move {
            let key_index = index as usize;
            cached.put(distribution[key_index & MASK], distribution[key_index & MASK]).unwrap().handle().await;
        }
    };

    execute_async(criterion, "Async Cached.put() | 16 tasks", Arc::new(block), 8, 16);
}

#[cfg(not(tarpaulin_include))]
pub fn async_put_32_tasks(criterion: &mut Criterion) {
    let cached = Arc::new(CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build()));
    let distribution = Arc::new(distribution(ITEMS as u64, CAPACITY));

    let block = move |index| {
        let cached = cached.clone();
        let distribution = distribution.clone();

        async move {
            let key_index = index as usize;
            cached.put(distribution[key_index & MASK], distribution[key_index & MASK]).unwrap().handle().await;
        }
    };

    execute_async(criterion, "Async Cached.put() | 32 tasks", Arc::new(block), 8, 32);
}

criterion_group!(benches, async_put_single_task, async_put_8_tasks, async_put_16_tasks, async_put_32_tasks);
criterion_main!(benches);