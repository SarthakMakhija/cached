use std::ops::Div;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};

use cached::cache::buffer_event::{BufferConsumer, BufferEvent};
use cached::cache::proxy::pool::ProxyPool;

struct NoOperationBufferConsumer {}

impl BufferConsumer for NoOperationBufferConsumer {
    fn accept(&self, _event: BufferEvent) {}
}

#[cfg(feature = "bench_testable")]
pub fn read_buffer_one_thread(criterion: &mut Criterion) {
    criterion.bench_function("Pool.add() | No contention", |bencher| bencher.iter_custom(|iterations| {
        let consumer = Arc::new(NoOperationBufferConsumer {});
        let pool = ProxyPool::new(32, 64, consumer);

        let start = Instant::now();
        for _index in 0..iterations {
            pool.add(1);
        }
        start.elapsed()
    }));
}

#[cfg(feature = "bench_testable")]
pub fn read_buffer_8_threads(criterion: &mut Criterion) {
    let pool = Arc::new(ProxyPool::new(32, 64, Arc::new(NoOperationBufferConsumer {})));
    read_buffer_parallel(criterion, "Pool.add() | 8 threads", pool, 8);
}

#[cfg(feature = "bench_testable")]
pub fn read_buffer_16_threads(criterion: &mut Criterion) {
    let pool = Arc::new(ProxyPool::new(32, 64, Arc::new(NoOperationBufferConsumer {})));
    read_buffer_parallel(criterion, "Pool.add() | 16 threads", pool, 16);
}

#[cfg(feature = "bench_testable")]
pub fn read_buffer_32_threads(criterion: &mut Criterion) {
    let pool = Arc::new(ProxyPool::new(32, 64, Arc::new(NoOperationBufferConsumer {})));
    read_buffer_parallel(criterion, "Pool.add() | 32 threads", pool, 32);
}

#[cfg(feature = "bench_testable")]
pub fn read_buffer_parallel<'a, Consumer: BufferConsumer + Send + Sync + 'a + 'static>(
    criterion: &'a mut Criterion,
    id: &'static str,
    pool: Arc<ProxyPool<Consumer>>,
    thread_count: u8) {
    criterion.bench_function(id, |bencher| bencher.iter_custom(|iterations| {
        let per_thread_iterations = iterations / thread_count as u64;
        let mut current_start = 0;
        let mut current_end = current_start + per_thread_iterations;

        let mut threads = Vec::new();
        for thread_id in 1..=thread_count {
            threads.push(thread::spawn({
                let pool = pool.clone();
                move || {
                    let start = Instant::now();
                    for _index in current_start..current_end {
                        pool.add(thread_id as u64);
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

criterion_group!(benches, read_buffer_one_thread, read_buffer_8_threads, read_buffer_16_threads, read_buffer_32_threads);
criterion_main!(benches);