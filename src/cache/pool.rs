use std::rc::Rc;
use parking_lot::RwLock;
use rand::{Rng, thread_rng};

#[repr(transparent)]
#[derive(Copy, Clone)]
pub(crate) struct PoolSize(usize);

#[repr(transparent)]
#[derive(Copy, Clone)]
pub(crate) struct BufferSize(usize);

pub(crate) struct Pool<Key, Consumer: BufferConsumer<Key>> {
    buffers: Vec<RwLock<Buffer<Key, Consumer>>>,
    pool_size: PoolSize,
}

struct Buffer<Key, Consumer: BufferConsumer<Key>> {
    keys: Vec<Key>,
    capacity: BufferSize,
    consumer: Rc<Consumer>,
}

pub(crate) trait BufferConsumer<Key> {
    fn accept(&self, keys: &Vec<Key>);
}

impl<Key, Consumer> Buffer<Key, Consumer>
    where Consumer: BufferConsumer<Key> {
    pub(crate) fn new(capacity: BufferSize, consumer: Rc<Consumer>) -> Self {
        return Buffer {
            keys: Vec::with_capacity(capacity.0),
            capacity,
            consumer,
        };
    }

    pub(crate) fn add(&mut self, key: Key) {
        self.keys.push(key);
        if self.keys.len() >= self.capacity.0 {
            self.consumer.accept(&self.keys);
            self.keys.clear();
        }
    }
}

impl<Key, Consumer> Pool<Key, Consumer>
    where Consumer: BufferConsumer<Key> {
    pub(crate) fn new(pool_size: PoolSize, buffer_size: BufferSize, buffer_consumer: Rc<Consumer>) -> Self {
        let buffers =
            (0..pool_size.0)
                .map(|_| RwLock::new(Buffer::new(buffer_size, buffer_consumer.clone())))
                .collect::<Vec<RwLock<Buffer<Key, Consumer>>>>();

        return Pool { buffers, pool_size };
    }

    pub(crate) fn add(&self, key: Key) {
        let pool_size = self.pool_size.0;
        let index = thread_rng().gen_range(0..pool_size);
        self.buffers[index].write().add(key);
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use crate::cache::pool::{BufferSize, Pool, PoolSize};
    use crate::cache::pool::tests::setup::TestBufferConsumer;

    mod setup {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use crate::cache::pool::BufferConsumer;

        pub(crate) struct TestBufferConsumer {
            pub(crate) total_keys: AtomicUsize
        }

        impl<Key> BufferConsumer<Key> for TestBufferConsumer {
            fn accept(&self, keys: &Vec<Key>) {
                self.total_keys.fetch_add(keys.len(), Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn push_keys_in_a_pool_with_1_buffer_of_size_2() {
        let consumer = Rc::new(TestBufferConsumer {total_keys: AtomicUsize::new(0)});
        let pool = Pool::new(
            PoolSize(1),
            BufferSize(2),
            consumer.clone()
        );
        pool.add("topic");
        pool.add("ssd");

        assert_eq!(2, consumer.total_keys.load(Ordering::SeqCst));
    }

    #[test]
    fn push_keys_in_a_pool_with_2_buffers_of_size_2() {
        let consumer = Rc::new(TestBufferConsumer {total_keys: AtomicUsize::new(0)});
        let pool = Pool::new(
            PoolSize(2),
            BufferSize(2),
            consumer.clone()
        );
        pool.add("topic");
        pool.add("LFU");
        pool.add("LRU");
        pool.add("cache");

        //it is possible that one buffer gets full, it gets flushed to the consumer.
        //for the remaining 2 keys, it is possible that 1 key might go in buffer 0 and the other key might go in buffer 1
        let total_keys = consumer.total_keys.load(Ordering::SeqCst);
        assert!(total_keys == 4 || total_keys == 2);
    }
}