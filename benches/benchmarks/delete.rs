use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

use cached::cache::cached::CacheD;
use cached::cache::config::ConfigBuilder;
use cached::cache::types::{TotalCounters, Weight};

use crate::benchmarks::common::{distribution, execute_parallel, preload_cache};

/// Defines the total number of key/value pairs that are loaded in the cache
const CAPACITY: usize = 2 << 14;

/// Defines the total number of counters used to measure the access frequency.
/// Its value will not impact this benchmark as we are not accessing the keys
const COUNTERS: TotalCounters = (CAPACITY * 10) as TotalCounters;

/// Defines the total size of the cache.
/// It is kept to CAPACITY * 40 because the benchmark inserts keys and values of type u64.
/// Weight of a single u64 key and u64 value without time_to_live is 40 bytes. Check `src/cache/config/weight_calculation.rs`
/// As a part of this benchmark, we preload the cache with the total number of elements = CAPACITY.
/// We want all the elements to be admitted in the cache, hence weight = CAPACITY * 40 bytes.
const WEIGHT: Weight = (CAPACITY * 40) as Weight;

/// Defines the total sample size that is used for generating Zipf distribution.
const ITEMS: usize = CAPACITY / 3;
const MASK: usize = CAPACITY - 1;

/// This benchmark preloads the cache with the total number of elements = CAPACITY.

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn delete_single_threaded(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution(ITEMS as u64, CAPACITY);

    preload_cache(&cached, &distribution, |key| key);

    let mut index = 0;
    criterion.bench_function("Cached.delete() | No contention", |bencher| {
        bencher.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = cached.delete(distribution[index & MASK]).unwrap();
                index += 1;
            }
            start.elapsed()
        });
    });
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn delete_8_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution(ITEMS as u64, CAPACITY);

    preload_cache(&cached, &distribution, |key| key);
    execute_parallel(criterion, "Cached.delete() | 8 threads", prepare_execution_block(cached, Arc::new(distribution)), 8);
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn delete_16_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution(ITEMS as u64, CAPACITY);

    preload_cache(&cached, &distribution, |key| key);
    execute_parallel(criterion, "Cached.delete() | 16 threads", prepare_execution_block(cached, Arc::new(distribution)), 16);
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn delete_32_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution(ITEMS as u64, CAPACITY);

    preload_cache(&cached, &distribution, |key| key);
    execute_parallel(criterion, "Cached.delete() | 32 threads", prepare_execution_block(cached, Arc::new(distribution)), 32);
}

#[cfg(not(tarpaulin_include))]
fn prepare_execution_block(cached: CacheD<u64, u64>, distribution: Arc<Vec<u64>>) -> Arc<impl Fn(u64) + Send + Sync + 'static> {
    Arc::new(move |index| {
        let key_index = index as usize;
        let _ = cached.delete(distribution[key_index & MASK]).unwrap();
    })
}

criterion_group!(benches, delete_single_threaded, delete_8_threads, delete_16_threads, delete_32_threads);
criterion_main!(benches);