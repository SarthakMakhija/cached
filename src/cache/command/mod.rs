use std::time::Duration;

pub mod acknowledgement;
pub(crate) mod command_executor;

pub(crate) enum CommandType<Key, Value> {
    Put(Key, Value),
    PutWithTTL(Key, Value, Duration),
    Delete(Key),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CommandStatus {
    Pending,
    Done,
    Rejected,
}
