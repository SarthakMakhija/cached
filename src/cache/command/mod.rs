use std::hash::Hash;
use std::time::Duration;

use crate::cache::key_description::KeyDescription;
use crate::cache::types::{KeyId, Weight};

pub mod acknowledgement;
pub mod error;
pub mod command_executor;

/// CommandType defines various write commands including:
/// Put             : attempts to put the new key/value pair in the cache
/// PutWithTTL      : attempts to put the new key/value pair with time_to_live in the cache
/// Delete          : attempts to delete the key
/// UpdateWeight    : updates the weight of the key. This command is sent as a part of `put_or_update` operation
/// Shutdown        : informs the `crate::cache::command::command_executor::CommandExecutor` that the cache is being shutdown
pub(crate) enum CommandType<Key, Value>
    where Key: Hash + Eq + Clone {
    Put(KeyDescription<Key>, Value),
    PutWithTTL(KeyDescription<Key>, Value, Duration),
    Delete(Key),
    UpdateWeight(KeyId, Weight),
    Shutdown,
}

/// Provides the description of each command
/// `description` is used if there is an error in sending a command to the `crate::cache::command::command_executor::CommandExecutor`
impl<Key, Value> CommandType<Key, Value>
    where Key: Hash + Eq + Clone {
    fn description(&self) -> String {
        match self {
            CommandType::Put(_, _) => "Put".to_string(),
            CommandType::PutWithTTL(_, _, _) => "PutWithTTL".to_string(),
            CommandType::Delete(_) => "Delete".to_string(),
            CommandType::UpdateWeight(_, _) => "UpdateWeight".to_string(),
            CommandType::Shutdown => "Shutdown".to_string(),
        }
    }
}

/// CommandStatus defines the status of each command.
///
/// `Pending`:        the initial status of the command, before a command is acted upon
///
/// `Accepted`:       the command is successfully completed
///
/// `Rejected`:       the command is rejected.
    /// - `Put` may be rejected for various reasons: one reason is: the weight of the the incoming key/value pair is more than the total cache weight
    /// - `Delete` will be rejected if the key to be deleted is not preset in the cache
///
/// `ShuttingDown`:   all the commands that could sneak in while the cache is being shutdown will be returned with `ShuttingDown` status
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CommandStatus {
    Pending,
    Accepted,
    Rejected,
    ShuttingDown,
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
        let delete: CommandType<&str, &str> = CommandType::Delete("topic");

        assert_eq!("Delete", delete.description());
    }

    #[test]
    fn command_description_update_weight() {
        let update_weight: CommandType<&str, &str> = CommandType::UpdateWeight(10, 200);

        assert_eq!("UpdateWeight", update_weight.description());
    }

    #[test]
    fn command_description_shutdown() {
        let shutdown: CommandType<&str, &str> = CommandType::Shutdown;

        assert_eq!("Shutdown", shutdown.description());
    }
}