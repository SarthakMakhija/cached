use std::sync::Arc;

use parking_lot::RwLock;
use rand::{Rng, thread_rng};
use crate::cache::types::KeyHash;

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct PoolSize(pub(crate) usize);

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct BufferSize(pub(crate) usize);

//TODO: Can we get rid of RWLock? Disruptor?
pub(crate) struct Pool<Consumer: BufferConsumer> {
    buffers: Vec<RwLock<Buffer<Consumer>>>,
    pool_size: PoolSize,
}

struct Buffer<Consumer: BufferConsumer> {
    key_hashes: Vec<KeyHash>,
    capacity: BufferSize,
    consumer: Arc<Consumer>,
}

pub(crate) trait BufferConsumer {
    fn accept(&self, key_hashes: Vec<KeyHash>);
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

    pub(crate) fn add(&mut self, key_hash: KeyHash) {
        self.key_hashes.push(key_hash);

        if self.key_hashes.len() >= self.capacity.0 {
            self.consumer.accept(self.key_hashes.clone());
            self.key_hashes.clear();
        }
    }
}

impl<Consumer> Pool<Consumer>
    where Consumer: BufferConsumer {
    pub(crate) fn new(pool_size: PoolSize, buffer_size: BufferSize, buffer_consumer: Arc<Consumer>) -> Self {
        let buffers = (0..pool_size.0)
            .map(|_| RwLock::new(Buffer::new(buffer_size, buffer_consumer.clone())))
            .collect::<Vec<RwLock<Buffer<Consumer>>>>();

        Pool { buffers, pool_size }
    }

    pub(crate) fn add(&self, key_hash: KeyHash) {
        let pool_size = self.pool_size.0;
        let index = thread_rng().gen_range(0..pool_size);
        self.buffers[index].write().add(key_hash);
    }

    pub(crate) fn get_buffer_consumer(&self) -> Arc<Consumer> {
        self.buffers[0].read().consumer.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::cache::pool::{BufferSize, Pool, PoolSize};
    use crate::cache::pool::tests::setup::TestBufferConsumer;

    mod setup {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::cache::pool::BufferConsumer;

        pub(crate) struct TestBufferConsumer {
            pub(crate) total_keys: AtomicUsize,
        }

        impl BufferConsumer for TestBufferConsumer {
            fn accept(&self, keys: Vec<u64>) {
                self.total_keys.fetch_add(keys.len(), Ordering::SeqCst);
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

        assert_eq!(2, consumer.total_keys.load(Ordering::SeqCst));
    }

    #[test]
    fn push_keys_in_a_pool_with_2_buffers_of_size_2() {
        let consumer = Arc::new(TestBufferConsumer { total_keys: AtomicUsize::new(0) });
        let pool = Pool::new(
            PoolSize(2),
            BufferSize(2),
            consumer.clone(),
        );
        pool.add(10);
        pool.add(10);
        pool.add(12);
        pool.add(16);

        //it is possible that one buffer gets full, it gets flushed to the consumer.
        //for the remaining 2 keys, it is possible that 1 key might go in buffer 0 and the other key might go in buffer 1
        let total_keys = consumer.total_keys.load(Ordering::SeqCst);
        assert!(total_keys == 4 || total_keys == 2);
    }
}