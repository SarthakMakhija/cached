use std::ops::Div;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use criterion::Criterion;
use rand::{Rng, thread_rng};
use rand_distr::Zipf;

use cached::cache::cached::CacheD;

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn execute_parallel<F>(
    criterion: &mut Criterion,
    id: &'static str,
    block: Arc<F>,
    thread_count: u8)
    where F: Fn(u64) + Send + Sync + 'static {
    criterion.bench_function(id, |bencher| bencher.iter_custom(|iterations| {
        let threads = spawn_threads(block.clone(), thread_count, iterations);

        let mut total_time = Duration::from_nanos(0);
        for thread in threads {
            let elapsed = thread.join().unwrap();
            total_time += elapsed;
        }
        total_time.div(thread_count as u32)
    }));
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn distribution(items: u64, capacity: usize) -> Vec<u64> {
    distribution_with_exponent(items, capacity, 1.01)
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn distribution_with_exponent(items: u64, capacity: usize, exponent: f64) -> Vec<u64> {
    thread_rng().sample_iter(Zipf::new(items, exponent).unwrap()).take(capacity).map(|value| value as u64).collect::<Vec<_>>()
}

#[cfg(feature = "bench_testable")]
#[cfg(not(tarpaulin_include))]
pub fn preload_cache<Value, F>(cached: &CacheD<u64, Value>, distribution: &Vec<u64>, value_generation: F)
    where Value: Send + Sync + 'static,
          F: Fn(u64) -> Value {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            setup(cached, distribution, value_generation).await;
        });
}

#[cfg(not(tarpaulin_include))]
async fn setup<Value, F>(cached: &CacheD<u64, Value>, distribution: &Vec<u64>, value_generation: F)
    where Value: Send + Sync + 'static,
          F: Fn(u64) -> Value {
    for key in distribution {
        let value = value_generation(*key);
        cached.put(*key, value).unwrap().handle().await;
    }
}

#[cfg(not(tarpaulin_include))]
fn spawn_threads<F>(block: Arc<F>, thread_count: u8, iterations: u64) -> Vec<JoinHandle<Duration>> where F: Fn(u64) + Send + Sync + 'static {
    let per_thread_iterations = iterations / thread_count as u64;
    let mut current_start = 0;
    let mut current_end = current_start + per_thread_iterations;

    let mut threads = Vec::new();
    for _thread_id in 1..=thread_count {
        threads.push(thread::spawn({
            let block = block.clone();
            move || {
                let start = Instant::now();
                for index in current_start..current_end {
                    block(index);
                }
                start.elapsed()
            }
        }));
        current_start = current_end;
        current_end += per_thread_iterations;
    }
    threads
}