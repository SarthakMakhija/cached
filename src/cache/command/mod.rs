use std::hash::Hash;
use std::time::Duration;

use crate::cache::key_description::KeyDescription;

pub mod acknowledgement;
pub(crate) mod command_executor;

pub(crate) enum CommandType<Key, Value>
    where Key: Hash + Eq + Clone {
    Put(KeyDescription<Key>, Value),
    PutWithTTL(KeyDescription<Key>, Value, Duration),
    Delete(Key),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CommandStatus {
    Pending,
    Accepted,
    Rejected,
}
