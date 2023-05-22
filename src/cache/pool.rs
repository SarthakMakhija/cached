use std::sync::Arc;

use log::debug;
use parking_lot::RwLock;
use rand::{Rng, thread_rng};

use crate::cache::buffer_event::{BufferConsumer, BufferEvent};
use crate::cache::types::KeyHash;

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct PoolSize(pub(crate) usize);

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct BufferSize(pub(crate) usize);

/// Pool represents a ring-buffer that is used to buffer the gets for various keys.
/// PoolSize is a configurable parameter defined in [`crate::cache::config::Config`].
pub(crate) struct Pool<Consumer: BufferConsumer> {
    buffers: Vec<RwLock<Buffer<Consumer>>>,
    pool_size: PoolSize,
}

/// Each buffer inside the Pool is a Vec<KeyHash>. The capacity of buffer is a configurable parameter.
/// Once the buffer is full, it is drained.
struct Buffer<Consumer: BufferConsumer> {
    key_hashes: Vec<KeyHash>,
    capacity: BufferSize,
    consumer: Arc<Consumer>,
}

impl<Consumer> Buffer<Consumer>
    where Consumer: BufferConsumer {
    pub(crate) fn new(capacity: BufferSize, consumer: Arc<Consumer>) -> Self {
        Buffer {
            key_hashes: Vec::with_capacity(capacity.0),
            capacity,
            consumer,
        }
    }

    /// Adds the key_hash to key_hashes.
    /// Before adding the key_hash, it is checked to see if the buffer needs draining.
    /// If the buffer needs to be drained, an event of type `BufferEvent::Full` is created and sent to the consumer
    pub(crate) fn add(&mut self, key_hash: KeyHash) {
        if self.key_hashes.len() >= self.capacity.0 {
            debug!("Draining the buffer");
            self.consumer.accept(BufferEvent::Full(self.key_hashes.clone()));
            self.key_hashes.clear();
        }
        self.key_hashes.push(key_hash);
    }
}

impl<Consumer> Pool<Consumer>
    where Consumer: BufferConsumer {
    pub(crate) fn new(pool_size: PoolSize, buffer_size: BufferSize, buffer_consumer: Arc<Consumer>) -> Self {
        let buffers = (0..pool_size.0)
            .map(|_| RwLock::new(Buffer::new(buffer_size, buffer_consumer.clone())))
            .collect::<_>();

        Pool { buffers, pool_size }
    }

    /// Adds the key_hash to a random buffer. There are a total of pool_size buffers and the
    /// generated random number lies between 0 and pool_size
    /// After the buffer is picked, a write lock is acquired on the buffer to add the key_hash.
    pub(crate) fn add(&self, key_hash: KeyHash) {
        let pool_size = self.pool_size.0;
        let index = thread_rng().gen_range(0..pool_size);
        self.buffers[index].write().add(key_hash);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use crate::cache::pool::{BufferSize, Pool, PoolSize};
    use crate::cache::pool::tests::setup::TestBufferConsumer;

    mod setup {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::cache::buffer_event::{BufferConsumer, BufferEvent};

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
        assert_eq!(1, pool.buffers[0].read().key_hashes.len());
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
        assert_eq!(8, pool.buffers[0].read().key_hashes.len());
    }
}