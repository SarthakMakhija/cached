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

/// BufferEvent::Full signifies that a buffer in the [`crate::cache::pool::Pool`] is full
/// and the consumer should accepts the buffer, called draining.
/// During the event of cache shutdown, the consumer of the buffer needs to be shutdown.
/// Buffer::Shutdown signals the consumer of the buffer to shutdown.
/// Currently, `crate::cache::policy::admission_policy::AdmissionPolicy` is the consumer of the buffer.
#[cfg(feature = "bench_testable")]
pub enum BufferEvent {
    Full(Vec<KeyHash>),
    Shutdown,
}

#[cfg(feature = "bench_testable")]
pub trait BufferConsumer {
    fn accept(&self, event: BufferEvent);
}