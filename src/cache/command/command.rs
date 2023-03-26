use std::time::Duration;

pub(crate) enum CommandType<Key, Value> {
    Put(Key, Value),
    PutWithTTL(Key, Value, Duration),
    Delete(Key)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CommandStatus {
    Pending,
    Done
}