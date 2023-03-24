use std::time::Duration;

pub(crate) enum CommandType<Key, Value> {
    Put(Key, Value),
    PutWithTTL(Key, Value, Duration),
}