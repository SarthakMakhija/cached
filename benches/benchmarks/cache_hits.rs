use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

use cached::cache::cached::CacheD;
use cached::cache::config::ConfigBuilder;
use cached::cache::types::{TotalCounters, Weight};

use crate::benchmarks::common::{distribution_with_exponent, execute_parallel, preload_cache};

const CAPACITY: usize = 2 << 14;
const COUNTERS: TotalCounters = (CAPACITY * 10) as TotalCounters;
const WEIGHT: Weight = CAPACITY as Weight;

const ITEMS: usize = CAPACITY * 16;
const MASK: usize = CAPACITY - 1;

#[derive(Debug)]
struct HitsMissRecorder {
    hits: AtomicU64,
    miss: AtomicU64,
}

impl HitsMissRecorder {
    #[cfg(not(tarpaulin_include))]
    fn new() -> Self {
        HitsMissRecorder {
            hits: AtomicU64::new(0),
            miss: AtomicU64::new(0),
        }
    }

    #[cfg(not(tarpaulin_include))]
    fn record_hit(&self) { self.hits.fetch_add(1, Ordering::SeqCst); }
    #[cfg(not(tarpaulin_include))]
    fn record_miss(&self) { self.miss.fetch_add(1, Ordering::SeqCst); }
    #[cfg(not(tarpaulin_include))]
    fn ratio(&self) -> f64 {
        (self.hits.load(Ordering::SeqCst) as f64 / (self.hits.load(Ordering::SeqCst) + self.miss.load(Ordering::SeqCst)) as f64) * 100.0
    }
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn cache_hits_single_threaded(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution_with_exponent(ITEMS as u64, CAPACITY, 5.1);

    preload_cache(&cached, &distribution, |key| key);

    let mut index = 0;
    let hit_miss_recorder = HitsMissRecorder::new();
    criterion.bench_function("Cached.get() | No contention", |bencher| {
        bencher.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                let option = cached.get(&distribution[index & MASK]);
                if option.is_some() {
                    hit_miss_recorder.record_hit();
                } else {
                    hit_miss_recorder.record_miss();
                }
                index += 1;
            }
            start.elapsed()
        });
    });
    println!("{:?} %", hit_miss_recorder.ratio());
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn cache_hits_8_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution_with_exponent(ITEMS as u64, CAPACITY, 5.1);
    let hit_miss_recorder = Arc::new(HitsMissRecorder::new());

    preload_cache(&cached, &distribution, |key| key);
    execute_parallel(criterion, "Cached.get() | 8 threads", prepare_execution_block(cached, Arc::new(distribution), hit_miss_recorder.clone()), 8);
    println!("{:?} %", hit_miss_recorder.ratio());
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn cache_hits_16_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution_with_exponent(ITEMS as u64, CAPACITY, 5.1);
    let hit_miss_recorder = Arc::new(HitsMissRecorder::new());

    preload_cache(&cached, &distribution, |key| key);
    execute_parallel(criterion, "Cached.get() | 16 threads", prepare_execution_block(cached, Arc::new(distribution), hit_miss_recorder.clone()), 16);
    println!("{:?} %", hit_miss_recorder.ratio());
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn cache_hits_32_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    let distribution = distribution_with_exponent(ITEMS as u64, CAPACITY, 5.1);
    let hit_miss_recorder = Arc::new(HitsMissRecorder::new());

    preload_cache(&cached, &distribution, |key| key);
    execute_parallel(criterion, "Cached.get() | 32 threads", prepare_execution_block(cached, Arc::new(distribution), hit_miss_recorder.clone()), 32);
    println!("{:?} %", hit_miss_recorder.ratio());
}

#[cfg(not(tarpaulin_include))]
fn prepare_execution_block(cached: CacheD<u64, u64>, distribution: Arc<Vec<u64>>, hit_miss_recorder: Arc<HitsMissRecorder>) -> Arc<impl Fn(u64) + Send + Sync + 'static> {
    Arc::new(move |index| {
        let key_index = index as usize;
        let option = cached.get(&distribution[key_index & MASK]);
        if option.is_some() {
            hit_miss_recorder.record_hit();
        } else {
            hit_miss_recorder.record_miss();
        }
    })
}

criterion_group!(benches, cache_hits_single_threaded, cache_hits_8_threads, cache_hits_16_threads, cache_hits_32_threads);
criterion_main!(benches);