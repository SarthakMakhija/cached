use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

use cached::cache::cached::CacheD;
use cached::cache::config::ConfigBuilder;
use cached::cache::types::{TotalCounters, Weight};

use crate::benchmarks::common::{distribution, execute_parallel};

const CAPACITY: usize = 2 << 14;
const COUNTERS: TotalCounters = (CAPACITY * 10) as TotalCounters;
const WEIGHT: Weight = CAPACITY as Weight;

const ITEMS: usize = CAPACITY / 3;
const MASK: usize = CAPACITY - 1;

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn put_get_single_threaded(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution(ITEMS as u64, CAPACITY);

    let mut index = 0;
    criterion.bench_function("Cached.put/Cached.get() | No contention", |bencher| {
        bencher.iter_custom(|iterations| {
            let start = Instant::now();
            for iteration_index in 0..iterations {
                if iteration_index & 0x01 == 0 {
                    let _ = cached.get(&distribution[index & MASK]);
                } else {
                    let _ = cached.put(distribution[index & MASK], distribution[index & MASK]).unwrap();
                }
                index += 1;
            }
            start.elapsed()
        });
    });
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn put_get_8_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Cached.put/Cached.get() | 8 threads", prepare_execution_block(), 8);
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn put_get_16_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Cached.put/Cached.get() | 16 threads", prepare_execution_block(), 16);
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn put_get_32_threads(criterion: &mut Criterion) {
    execute_parallel(criterion, "Cached.put/Cached.get() | 32 threads", prepare_execution_block(), 32);
}

#[cfg(not(tarpaulin_include))]
fn prepare_execution_block() -> Arc<impl Fn(u64) + Send + Sync + 'static> {
    let cached = Arc::new(CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build()));
    let distribution = distribution(ITEMS as u64, CAPACITY);

    Arc::new(move |index| {
        let key_index = index as usize;
        if index & 0x01 == 0 {
            let _ = cached.get(&distribution[key_index & MASK]);
        } else {
            let _ = cached.put(distribution[key_index & MASK], distribution[key_index & MASK]).unwrap();
        }
    })
}

criterion_group!(benches, put_get_single_threaded, put_get_8_threads, put_get_16_threads, put_get_32_threads);
criterion_main!(benches);