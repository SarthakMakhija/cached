use std::sync::Arc;

use crate::cache::buffer_event::BufferConsumer;
use crate::cache::pool::{BufferSize, Pool, PoolSize};

use crate::cache::types::KeyHash;

#[cfg(feature = "bench_testable")]
pub struct ProxyPool<Consumer: BufferConsumer> {
    pool: Pool<Consumer>,
}

#[cfg(feature = "bench_testable")]
impl<Consumer> ProxyPool<Consumer>
    where Consumer: BufferConsumer {
    pub fn new(pool_size: usize, buffer_size: usize, buffer_consumer: Arc<Consumer>) -> Self {
        ProxyPool { pool: Pool::new(PoolSize(pool_size), BufferSize(buffer_size), buffer_consumer) }
    }

    pub fn add(&self, key_hash: KeyHash) {
        self.pool.add(key_hash)
    }
}