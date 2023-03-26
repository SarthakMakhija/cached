use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crossbeam_channel::Receiver;

use crate::cache::command::acknowledgement::CommandAcknowledgement;
use crate::cache::command::command::{CommandStatus, CommandType};
use crate::cache::store::store::Store;

pub(crate) struct CommandExecutor<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    sender: crossbeam_channel::Sender<CommandAcknowledgementPair<Key, Value>>,
    keep_running: Arc<AtomicBool>,
}

struct CommandAcknowledgementPair<Key, Value> {
    command: CommandType<Key, Value>,
    acknowledgement: Arc<CommandAcknowledgement>,
}

impl<Key, Value> CommandExecutor<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {

    pub(crate) fn new(store: Arc<Store<Key, Value>>, buffer_size: usize) -> Self {
        let (sender, receiver)
            = crossbeam_channel::bounded(buffer_size);
        let command_executor
            = CommandExecutor { sender, keep_running: Arc::new(AtomicBool::new(true)) };

        command_executor.spin(receiver, store);
        command_executor
    }

    fn spin(&self, receiver: Receiver<CommandAcknowledgementPair<Key, Value>>, store: Arc<Store<Key, Value>>) {
        let keep_running = self.keep_running.clone();

        thread::spawn(move || {
            while let Ok(pair) = receiver.recv() {
                let command = pair.command;
                match command {
                    CommandType::Put(key, value) =>
                        store.put(key, value),
                    CommandType::PutWithTTL(key, value, ttl) =>
                        store.put_with_ttl(key, value, ttl),
                    CommandType::Delete(key) =>
                        store.delete(&key)
                }
                pair.acknowledgement.done(CommandStatus::Done);
                if !keep_running.load(Ordering::Acquire) {
                    return;
                }
            }
        });
    }

    //TODO: Remove unwrap
    pub(crate) fn send(&self, command: CommandType<Key, Value>) -> Arc<CommandAcknowledgement> {
        let acknowledgement = CommandAcknowledgement::new();
        self.sender.send(CommandAcknowledgementPair {
            command,
            acknowledgement: acknowledgement.clone(),
        }).unwrap();

        acknowledgement
    }

    pub(crate) fn shutdown(&self) {
        self.keep_running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::cache::clock::SystemClock;
    use crate::cache::command::command::CommandType;
    use crate::cache::command::command_executor::CommandExecutor;
    use crate::cache::store::store::Store;

    #[tokio::test]
    async fn puts_a_key_value() {
        let store = Store::new(SystemClock::boxed());
        let command_executor = CommandExecutor::new(
            store.clone(),
            10,
        );

        let command_acknowledgement = command_executor.send(CommandType::Put(
            "topic",
            "microservices",
        ));
        command_acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(Some("microservices"), store.get(&"topic"));
    }

    #[tokio::test]
    async fn puts_a_couple_of_key_values() {
        let store = Store::new(SystemClock::boxed());
        let command_executor = CommandExecutor::new(
            store.clone(),
            10
        );

        let acknowledgement = command_executor.send(CommandType::Put(
            "topic",
            "microservices",
        ));
        let other_acknowledgment = command_executor.send(CommandType::Put(
            "disk",
            "SSD",
        ));
        acknowledgement.handle().await;
        other_acknowledgment.handle().await;

        command_executor.shutdown();
        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert_eq!(Some("SSD"), store.get(&"disk"));
    }

    #[tokio::test]
    async fn puts_a_key_value_with_ttl() {
        let store = Store::new(SystemClock::boxed());
        let command_executor = CommandExecutor::new(
            store.clone(),
            10
        );

        let acknowledgement = command_executor.send(CommandType::PutWithTTL(
            "topic",
            "microservices",
            Duration::from_secs(10),
        ));
        acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(Some("microservices"), store.get(&"topic"));
    }

    #[tokio::test]
    async fn deletes_a_key() {
        let store = Store::new(SystemClock::boxed());
        let command_executor = CommandExecutor::new(
            store.clone(),
            10
        );

        let acknowledgement = command_executor.send(CommandType::PutWithTTL(
            "topic",
            "microservices",
            Duration::from_secs(10),
        ));
        acknowledgement.handle().await;

        let acknowledgement = command_executor.send(CommandType::Delete("topic"));
        acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(None, store.get(&"topic"));
    }
}