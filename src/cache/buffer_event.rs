use crate::cache::types::KeyHash;

#[cfg(not(feature = "bench_testable"))]
pub(crate) enum BufferEvent {
    Full(Vec<KeyHash>),
    Shutdown,
}

#[cfg(not(feature = "bench_testable"))]
pub(crate) trait BufferConsumer {
    fn accept(&self, event: BufferEvent);
}

#[cfg(feature = "bench_testable")]
pub enum BufferEvent {
    Full(Vec<KeyHash>),
    Shutdown,
}

#[cfg(feature = "bench_testable")]
pub trait BufferConsumer {
    fn accept(&self, event: BufferEvent);
}