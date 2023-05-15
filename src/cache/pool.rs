use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};
use crossbeam_utils::Backoff;
use log::debug;

use parking_lot::Mutex;
use rand::{Rng, thread_rng};

use crate::cache::buffer_event::BufferEvent;
use crate::cache::types::KeyHash;

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct PoolSize(pub(crate) usize);

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct BufferSize(pub(crate) usize);

pub(crate) struct Pool<Consumer: BufferConsumer> {
    buffers: Vec<Buffer<Consumer>>,
    pool_size: PoolSize,
}

struct Buffer<Consumer: BufferConsumer> {
    key_hashes: UnsafeCell<Vec<KeyHash>>,
    capacity: BufferSize,
    consumer: Arc<Consumer>,
    used_tail: AtomicIsize,
    drain_synchronization: Mutex<()>,
}

pub(crate) trait BufferConsumer {
    fn accept(&self, event: BufferEvent);
}

impl<Consumer> Buffer<Consumer>
    where Consumer: BufferConsumer {
    pub(crate) fn new(capacity: BufferSize, consumer: Arc<Consumer>) -> Self {
        Buffer {
            key_hashes: UnsafeCell::new(Vec::with_capacity(capacity.0)),
            capacity,
            consumer,
            used_tail: AtomicIsize::new(-1),
            drain_synchronization: Mutex::new(()),
        }
    }

    //TODO: Validate memory ordering
    pub(crate) fn add(&self, key_hash: KeyHash) {
        let backoff = Backoff::new();
        loop {
            let local_tail = self.used_tail.load(Ordering::Acquire);
            if local_tail >= (self.capacity.0 - 1) as isize && !self.drain() {
                backoff.spin();
                continue;
            }
            let result = self.used_tail.compare_exchange_weak(local_tail, local_tail + 1, Ordering::Release, Ordering::Relaxed);
            if result.is_ok() {
                let hashes = unsafe { &mut *self.key_hashes.get() };
                hashes.push(key_hash);
                return;
            } else {
                backoff.spin()
            }
        }
    }

    fn drain(&self) -> bool {
        let optional_guard = self.drain_synchronization.try_lock();

        return if let Some(_guard) = optional_guard {
            let local_tail = self.used_tail.load(Ordering::Acquire);
            if local_tail >= (self.capacity.0 - 1) as isize {
                debug!("draining the buffer");
                let key_hashes = unsafe { &mut *self.key_hashes.get() };
                self.consumer.accept(BufferEvent::Full(key_hashes.clone()));
                self.used_tail.store(-1, Ordering::Release);
                key_hashes.clear();
            }
            true
        } else {
            false
        };
    }
}

impl<Consumer> Pool<Consumer>
    where Consumer: BufferConsumer {
    pub(crate) fn new(pool_size: PoolSize, buffer_size: BufferSize, buffer_consumer: Arc<Consumer>) -> Self {
        let buffers = (0..pool_size.0)
            .map(|_| Buffer::new(buffer_size, buffer_consumer.clone()))
            .collect::<Vec<Buffer<Consumer>>>();

        Pool { buffers, pool_size }
    }

    pub(crate) fn add(&self, key_hash: KeyHash) {
        let pool_size = self.pool_size.0;
        let index = thread_rng().gen_range(0..pool_size);
        self.buffers[index].add(key_hash);
    }

    #[cfg(test)]
    pub(crate) fn get_buffer_consumer(&self) -> Arc<Consumer> {
        self.buffers[0].consumer.clone()
    }
}

unsafe impl<Consumer> Send for Pool<Consumer> where Consumer: BufferConsumer {}

unsafe impl<Consumer> Sync for Pool<Consumer> where Consumer: BufferConsumer {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use crate::cache::pool::{BufferSize, Pool, PoolSize};
    use crate::cache::pool::tests::setup::TestBufferConsumer;

    mod setup {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::cache::buffer_event::BufferEvent;
        use crate::cache::pool::BufferConsumer;

        pub(crate) struct TestBufferConsumer {
            pub(crate) total_keys: AtomicUsize,
        }

        impl BufferConsumer for TestBufferConsumer {
            fn accept(&self, event: BufferEvent) {
                if let BufferEvent::Full(keys) = event {
                    self.total_keys.fetch_add(keys.len(), Ordering::SeqCst);
                }
            }
        }
    }

    #[test]
    fn push_keys_in_a_pool_with_1_buffer_of_size_2() {
        let consumer = Arc::new(TestBufferConsumer { total_keys: AtomicUsize::new(0) });
        let pool = Pool::new(
            PoolSize(1),
            BufferSize(2),
            consumer.clone(),
        );
        pool.add(15);
        pool.add(10);
        pool.add(90);

        assert_eq!(2, consumer.total_keys.load(Ordering::SeqCst));
    }

    #[test]
    fn push_keys_in_a_pool_with_1_buffer_of_size_3() {
        let consumer = Arc::new(TestBufferConsumer { total_keys: AtomicUsize::new(0) });
        let pool = Pool::new(
            PoolSize(1),
            BufferSize(3),
            consumer.clone(),
        );
        pool.add(10);
        pool.add(10);
        pool.add(12);
        pool.add(16);

        let total_keys = consumer.total_keys.load(Ordering::SeqCst);
        assert_eq!(3, total_keys);
    }

    #[test]
    fn drain_the_buffer_with_contention_1() {
        let consumer = Arc::new(TestBufferConsumer { total_keys: AtomicUsize::new(0) });
        let pool = Arc::new(Pool::new(
            PoolSize(1),
            BufferSize(8),
            consumer.clone(),
        ));
        for count in 1..=7 {
            pool.add(count);
        }

        let handle = thread::spawn({
            let pool = pool.clone();
            move || {
                pool.add(7);
            }
        });
        let other_handle = thread::spawn({
            let pool = pool.clone();
            move || {
                pool.add(8);
            }
        });

        handle.join().unwrap();
        other_handle.join().unwrap();

        let total_keys = consumer.total_keys.load(Ordering::SeqCst);
        assert_eq!(8, total_keys);
        assert_eq!(1, unsafe { &*pool.buffers[0].key_hashes.get() }.len());
    }

    #[test]
    fn drain_the_buffer_with_contention_2() {
        let consumer = Arc::new(TestBufferConsumer { total_keys: AtomicUsize::new(0) });
        let pool = Arc::new(Pool::new(
            PoolSize(1),
            BufferSize(8),
            consumer.clone(),
        ));

        let handle = thread::spawn({
            let pool = pool.clone();
            move || {
                for count in 1..=8 {
                    pool.add(count);
                }
            }
        });
        let other_handle = thread::spawn({
            let pool = pool.clone();
            move || {
                for count in 9..=16 {
                    pool.add(count);
                }
            }
        });

        handle.join().unwrap();
        other_handle.join().unwrap();

        let total_keys = consumer.total_keys.load(Ordering::SeqCst);
        assert_eq!(8, total_keys);
        assert_eq!(8, unsafe { &*pool.buffers[0].key_hashes.get() }.len());
    }
}