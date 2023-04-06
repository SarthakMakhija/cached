use std::hash::Hash;
use std::time::Duration;

use crate::cache::key_description::KeyDescription;

pub mod acknowledgement;
pub mod error;
pub(crate) mod command_executor;

pub(crate) enum CommandType<Key, Value>
    where Key: Hash + Eq + Clone {
    Put(KeyDescription<Key>, Value),
    PutWithTTL(KeyDescription<Key>, Value, Duration),
    Delete(Key),
}

impl<Key, Value> CommandType<Key, Value>
    where Key: Hash + Eq + Clone {
    fn description(&self) -> String {
        match self {
            CommandType::Put(_, _) => "Put".to_string(),
            CommandType::PutWithTTL(_, _, _) => "PutWithTTL".to_string(),
            CommandType::Delete(_) => "Delete".to_string()
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CommandStatus {
    Pending,
    Accepted,
    Rejected,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::cache::command::CommandType;
    use crate::cache::key_description::KeyDescription;

    #[test]
    fn command_description_put() {
        let put = CommandType::Put(
            KeyDescription::new(
                "topic", 1, 2090, 10,
            ),
            "microservices");

        assert_eq!("Put", put.description());
    }

    #[test]
    fn command_description_put_with_ttl() {
        let put = CommandType::PutWithTTL(
            KeyDescription::new(
                "topic", 1, 2090, 10,
            ),
            "microservices",
            Duration::from_millis(10),
        );

        assert_eq!("PutWithTTL", put.description());
    }

    #[test]
    fn command_description_delete() {
        let put: CommandType<&str, &str> = CommandType::Delete("topic");

        assert_eq!("Delete", put.description());
    }
}