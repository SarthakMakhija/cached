use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Builder;

use tinylfu_cached::cache::cached::CacheD;
use tinylfu_cached::cache::config::ConfigBuilder;
use tinylfu_cached::cache::types::{TotalCounters, Weight};

use crate::benchmarks::common::distribution_with_exponent;

/// Defines the total number of key/value pairs that may be loaded in the cache
const CAPACITY: usize = 100_000;

/// Defines the total number of counters used to measure the access frequency.
const COUNTERS: TotalCounters = (CAPACITY * 10) as TotalCounters;

/// Defines the total size of the cache.
const WEIGHT: Weight = (CAPACITY * 40) as Weight;

/// Defines the total sample size that is used for generating Zipf distribution.
/// Here, ITEMS is 16 times the CAPACITY to provide a larger sample for Zipf distribution.
/// W/C = 16, W denotes the sample size, and C is the cache size (denoted by CAPA)
/// [TinyLFU](https://dgraph.io/blog/refs/TinyLFU%20-%20A%20Highly%20Efficient%20Cache%20Admission%20Policy.pdf)
const ITEMS: usize = CAPACITY * 16;

const MASK: usize = CAPACITY - 1;

/// This benchmark uses 0.7, 0.9, and 1.001 as the Zipf distribution exponent.
/// For now, this benchmark prints the cache-hit ratio on console and the cache-hits.json under results/ is manually prepared.

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
pub fn cache_hits_single_threaded_exponent_1_001(criterion: &mut Criterion) {
    criterion.bench_function("Cached.get() | No contention", |bencher| {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        bencher.to_async(runtime).iter_custom(|iterations| {
            async move {
                let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
                let distribution = distribution_with_exponent(ITEMS as u64, ITEMS, 1.001);

                let hit_miss_recorder = HitsMissRecorder::new();
                let mut index = 0;

                let start = Instant::now();
                for _ in 0..CAPACITY*16 {
                    let option = cached.get(&distribution[index]);
                    if option.is_some() {
                        hit_miss_recorder.record_hit();
                    } else {
                        hit_miss_recorder.record_miss();
                    }
                    cached.put_with_weight(distribution[index], distribution[index], 40).unwrap().handle().await;
                    index += 1;
                }
                cached.shutdown();
                println!("{:?} %", hit_miss_recorder.ratio());
                start.elapsed()
            }
        });
    });
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn cache_hits_single_threaded_exponent_0_9(criterion: &mut Criterion) {
    criterion.bench_function("Cached.get() | No contention", |bencher| {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        bencher.to_async(runtime).iter_custom(|iterations| {
            async move {
                let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
                let distribution = distribution_with_exponent(ITEMS as u64, ITEMS, 0.9);

                let hit_miss_recorder = HitsMissRecorder::new();
                let mut index = 0;

                let start = Instant::now();
                for _ in 0..CAPACITY*16 {
                    let option = cached.get(&distribution[index]);
                    if option.is_some() {
                        hit_miss_recorder.record_hit();
                    } else {
                        hit_miss_recorder.record_miss();
                    }
                    cached.put_with_weight(distribution[index], distribution[index], 40).unwrap().handle().await;
                    index += 1;
                }
                cached.shutdown();
                println!("{:?} %", hit_miss_recorder.ratio());
                start.elapsed()
            }
        });
    });
}

criterion_group!(benches,  cache_hits_single_threaded_exponent_1_001, cache_hits_single_threaded_exponent_0_9);
criterion_main!(benches);