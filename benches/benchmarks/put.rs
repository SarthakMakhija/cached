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
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    put_parallel(criterion, "Cached.put() | 8 threads", Arc::new(cached), 8);
}

#[cfg(feature = "bench_testable")]
pub fn put_16_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    put_parallel(criterion, "Cached.put() | 16 threads", Arc::new(cached), 16);
}

#[cfg(feature = "bench_testable")]
pub fn put_32_threads(criterion: &mut Criterion) {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, WEIGHT).build());
    put_parallel(criterion, "Cached.put() | 32 threads", Arc::new(cached), 32);
}

#[cfg(feature = "bench_testable")]
pub fn put_parallel(
    criterion: &mut Criterion,
    id: &'static str,
    cached: Arc<CacheD<u64, u64>>,
    thread_count: u8) {
    criterion.bench_function(id, |bencher| bencher.iter_custom(|iterations| {
        let per_thread_iterations = iterations / thread_count as u64;
        let mut current_start = 0;
        let mut current_end = current_start + per_thread_iterations;
        let distribution = Arc::new(distribution());

        let mut threads = Vec::new();
        for _thread_id in 1..=thread_count {
            threads.push(thread::spawn({
                let cached = cached.clone();
                let distribution = distribution.clone();

                move || {
                    let start = Instant::now();
                    for index in current_start..current_end {
                        let key_index = index as usize;
                        let _ = cached.put(distribution[key_index & MASK], distribution[key_index & MASK]).unwrap();
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


fn distribution() -> Vec<u64> {
    thread_rng().sample_iter(Zipf::new(ITEMS as u64, 1.01).unwrap()).take(CAPACITY).map(|value| value as u64).collect::<Vec<_>>()
}

criterion_group!(benches, put_single_threaded, put_8_threads, put_16_threads, put_32_threads);
criterion_main!(benches);