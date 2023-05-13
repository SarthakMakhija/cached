use crate::cache::types::KeyHash;

pub(crate) enum BufferEvent {
    Full(Vec<KeyHash>),
    Shutdown,
}