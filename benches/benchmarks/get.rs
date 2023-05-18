use std::ops::Div;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use rand::{Rng, thread_rng};
use rand_distr::Zipf;

use cached::cache::cached::CacheD;
use cached::cache::config::ConfigBuilder;
use cached::cache::types::{TotalCounters, Weight};

const CAPACITY: usize = 2 << 14;
const COUNTERS: TotalCounters = (CAPACITY * 10) as TotalCounters;
const WEIGHT: Weight = CAPACITY as Weight;

const ITEMS: usize = CAPACITY / 3;
const MASK: usize = CAPACITY - 1;

#[cfg(feature = "bench_testable")]
pub fn get_single_threaded(criterion: &mut Criterion) {
    let cached = Arc::new(CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build()));
    let distribution = distribution();

    let cached_clone = cached.clone();
    let distribution_clone = distribution.clone();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            setup(cached_clone, distribution_clone).await;
        });

    let mut index = 0;
    criterion.bench_function("Cached.get() | No contention", |bencher| {
        bencher.iter_custom(|iterations| {
            let start = Instant::now();
            for _ in 0..iterations {
                cached.get(&distribution[index & MASK]);
                index += 1;
            }
            start.elapsed()
        });
    });
}

#[cfg(feature = "bench_testable")]
pub fn get_8_threads(criterion: &mut Criterion) {
    let cached = Arc::new(CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build()));
    let distribution = distribution();

    let cached_clone = cached.clone();
    let distribution_clone = distribution.clone();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            setup(cached_clone, distribution_clone).await;
        });

    get_parallel(criterion, "Cached.get() | 8 threads", cached, Arc::new(distribution), 8);
}

#[cfg(feature = "bench_testable")]
pub fn get_16_threads(criterion: &mut Criterion) {
    let cached = Arc::new(CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build()));
    let distribution = distribution();

    let cached_clone = cached.clone();
    let distribution_clone = distribution.clone();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            setup(cached_clone, distribution_clone).await;
        });

    get_parallel(criterion, "Cached.get() | 16 threads", cached, Arc::new(distribution), 16);
}

#[cfg(feature = "bench_testable")]
pub fn get_32_threads(criterion: &mut Criterion) {
    let cached = Arc::new(CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build()));
    let distribution = distribution();

    let cached_clone = cached.clone();
    let distribution_clone = distribution.clone();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            setup(cached_clone, distribution_clone).await;
        });

    get_parallel(criterion, "Cached.get() | 32 threads", cached, Arc::new(distribution), 32);
}

#[cfg(feature = "bench_testable")]
pub fn get_parallel(
    criterion: &mut Criterion,
    id: &'static str,
    cached: Arc<CacheD<u64, u64>>,
    distribution: Arc<Vec<u64>>,
    thread_count: u8) {

    criterion.bench_function(id, |bencher| bencher.iter_custom(|iterations| {
        let per_thread_iterations = iterations / thread_count as u64;
        let mut current_start = 0;
        let mut current_end = current_start + per_thread_iterations;

        let mut threads = Vec::new();
        for _thread_id in 1..=thread_count {
            threads.push(thread::spawn({
                let cached = cached.clone();
                let distribution = distribution.clone();

                move || {
                    let start = Instant::now();
                    for index in current_start..current_end {
                        let key_index = index as usize & MASK;
                        let _ = cached.get(&distribution[key_index]);
                    }
                    start.elapsed()
                }
            }));
            current_start = current_end;
            current_end += per_thread_iterations;
        }

        let mut total_time = Duration::from_nanos(0);
        for thread in threads {
            let elapsed = thread.join().unwrap();
            total_time += elapsed;
        }
        total_time.div(thread_count as u32)
    }));
}

async fn setup(cached: Arc<CacheD<u64, u64>>, distribution: Vec<u64>) {
    for element in distribution {
        cached.put(element, element).unwrap().handle().await;
    }
}

fn distribution() -> Vec<u64> {
    thread_rng().sample_iter(Zipf::new(ITEMS as u64, 1.01).unwrap()).take(CAPACITY).map(|value| value as u64).collect::<Vec<_>>()
}

criterion_group!(benches, get_single_threaded, get_8_threads, get_16_threads, get_32_threads);
criterion_main!(benches);