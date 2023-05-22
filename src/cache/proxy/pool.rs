use std::sync::Arc;

use crate::cache::buffer_event::BufferConsumer;
use crate::cache::pool::{BufferSize, Pool, PoolSize};

use crate::cache::types::KeyHash;

/// Proxy representation of the [`crate::cache::pool::Pool`].
/// Pool provides a single behavior to add a key_hash to its buffer after a key has been accessed.
/// ProxyPool provides the same behavior and ends up delegating to the Pool object.
/// ProxyPool is used in `benchmark benches/benchmarks/read_buffer.rs`
#[cfg(feature = "bench_testable")]
pub struct ProxyPool<Consumer: BufferConsumer> {
    pool: Pool<Consumer>,
}

#[cfg(feature = "bench_testable")]
impl<Consumer> ProxyPool<Consumer>
    where Consumer: BufferConsumer {
    #[cfg(not(tarpaulin_include))]
    pub fn new(pool_size: usize, buffer_size: usize, buffer_consumer: Arc<Consumer>) -> Self {
        ProxyPool { pool: Pool::new(PoolSize(pool_size), BufferSize(buffer_size), buffer_consumer) }
    }

    #[cfg(not(tarpaulin_include))]
    pub fn add(&self, key_hash: KeyHash) {
        self.pool.add(key_hash)
    }
}