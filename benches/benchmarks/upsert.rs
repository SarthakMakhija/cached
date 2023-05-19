use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

use cached::cache::cached::CacheD;
use cached::cache::config::ConfigBuilder;
use cached::cache::types::{TotalCounters, Weight};
use cached::cache::upsert::UpsertRequestBuilder;

use crate::benchmarks::common::{distribution, execute_parallel, preload_cache};

const CAPACITY: usize = 2 << 14;
const COUNTERS: TotalCounters = (CAPACITY * 10) as TotalCounters;
const WEIGHT: Weight = CAPACITY as Weight;

const ITEMS: usize = CAPACITY / 3;
const MASK: usize = CAPACITY - 1;

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn upsert_single_threaded(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution(ITEMS as u64, CAPACITY);

    preload_cache(&cached, &distribution);

    let mut index = 0;
    criterion.bench_function("Cached.upsert() | No contention", |bencher| {
        bencher.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                let key = distribution[index & MASK];
                let _ = cached.upsert(UpsertRequestBuilder::new(key).value(key * 10).build()).unwrap();
                index += 1;
            }
            start.elapsed()
        });
    });
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn upsert_8_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution(ITEMS as u64, CAPACITY);

    preload_cache(&cached, &distribution);
    execute_parallel(criterion, "Cached.upsert() | 8 threads", prepare_execution_block(cached, Arc::new(distribution)), 8);
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn upsert_16_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution(ITEMS as u64, CAPACITY);

    preload_cache(&cached, &distribution);
    execute_parallel(criterion, "Cached.upsert() | 16 threads", prepare_execution_block(cached, Arc::new(distribution)), 16);
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn upsert_32_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution(ITEMS as u64, CAPACITY);

    preload_cache(&cached, &distribution);
    execute_parallel(criterion, "Cached.upsert() | 32 threads", prepare_execution_block(cached, Arc::new(distribution)), 32);
}

#[cfg(not(tarpaulin_include))]
fn prepare_execution_block(cached: CacheD<u64, u64>, distribution: Arc<Vec<u64>>) -> Arc<impl Fn(u64) + Send + Sync + 'static> {
    Arc::new(move |index| {
        let key_index = index as usize;
        let key = distribution[key_index & MASK];

        let _ = cached.upsert(UpsertRequestBuilder::new(key).value(key * 10).build()).unwrap();
    })
}

criterion_group!(benches, upsert_single_threaded, upsert_8_threads, upsert_16_threads, upsert_32_threads);
criterion_main!(benches);