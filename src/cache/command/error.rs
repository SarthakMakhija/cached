use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

const SHUTDOWN_MESSAGE: &str = "could not accept the command for execution, probably the cache is being shutdown.";

pub struct CommandSendError {
    command_description: String,
}

impl CommandSendError {
    pub(crate) fn new(command_description: String) -> Self {
        CommandSendError {
            command_description
        }
    }

    pub(crate) fn shutdown() -> Self {
        CommandSendError {
            command_description: SHUTDOWN_MESSAGE.to_string()
        }
    }
}

impl Display for CommandSendError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} Command description: {}",
            SHUTDOWN_MESSAGE,
            self.command_description
        )
    }
}

impl Debug for CommandSendError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} Command description: {}",
            SHUTDOWN_MESSAGE,
            self.command_description
        )
    }
}

impl Error for CommandSendError {}

#[cfg(test)]
mod tests {
    use crate::cache::command::error::CommandSendError;

    #[test]
    fn command_send_error_display() {
        let error = CommandSendError::new("put".to_string());
        assert_eq!(
            format!("{}", error),
            "could not accept the command for execution, probably the cache is being shutdown. Command description: put",
        );
    }

    #[test]
    fn command_send_error_debug() {
        let error = CommandSendError::new("put".to_string());
        assert_eq!(
            format!("{:?}", error),
            "could not accept the command for execution, probably the cache is being shutdown. Command description: put",
        );
    }
}