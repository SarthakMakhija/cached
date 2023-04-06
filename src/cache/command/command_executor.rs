use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam_channel::Receiver;

use crate::cache::command::{CommandStatus, CommandType};
use crate::cache::command::acknowledgement::CommandAcknowledgement;
use crate::cache::command::error::CommandSendError;
use crate::cache::key_description::KeyDescription;
use crate::cache::policy::admission_policy::AdmissionPolicy;
use crate::cache::store::Store;

pub type CommandSendResult = Result<Arc<CommandAcknowledgement>, CommandSendError>;

pub(crate) struct CommandExecutor<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static {
    sender: crossbeam_channel::Sender<CommandAcknowledgementPair<Key, Value>>,
    keep_running: Arc<AtomicBool>,
}

struct CommandAcknowledgementPair<Key, Value>
    where Key: Hash + Eq + Clone {
    command: CommandType<Key, Value>,
    acknowledgement: Arc<CommandAcknowledgement>,
}

impl<Key, Value> CommandExecutor<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static {
    pub(crate) fn new(
        store: Arc<Store<Key, Value>>,
        admission_policy: Arc<AdmissionPolicy<Key>>,
        command_channel_size: usize) -> Self {
        let (sender, receiver)
            = crossbeam_channel::bounded(command_channel_size);
        let command_executor
            = CommandExecutor { sender, keep_running: Arc::new(AtomicBool::new(true)) };

        command_executor.spin(receiver, store, admission_policy);
        command_executor
    }

    fn spin(&self,
            receiver: Receiver<CommandAcknowledgementPair<Key, Value>>,
            store: Arc<Store<Key, Value>>,
            admission_policy: Arc<AdmissionPolicy<Key>>) {
        let keep_running = self.keep_running.clone();
        let clone = store.clone();
        let delete_hook = move |key| { clone.delete(&key); };

        thread::spawn(move || {
            while let Ok(pair) = receiver.recv() {
                let command = pair.command;
                let status = match command {
                    CommandType::Put(key_description, value) =>
                        Self::put(&store, &admission_policy, &key_description, &delete_hook, value),
                    CommandType::PutWithTTL(key_description, value, ttl) =>
                        Self::put_with_ttl(&store, &admission_policy, &key_description, &delete_hook, value, ttl),
                    CommandType::Delete(key) =>
                        Self::delete(&store, &admission_policy, &key),
                };
                pair.acknowledgement.done(status);
                if !keep_running.load(Ordering::Acquire) {
                    println!("dropping the receiver ..");
                    drop(receiver);
                    break;
                }
            }
        });
    }

    fn put<DeleteHook>(
        store: &Arc<Store<Key, Value>>,
        admission_policy: &Arc<AdmissionPolicy<Key>>,
        key_description: &KeyDescription<Key>,
        delete_hook: &DeleteHook,
        value: Value) -> CommandStatus
        where DeleteHook: Fn(Key) {
        let status = admission_policy.maybe_add(key_description, delete_hook);
        if let CommandStatus::Accepted = status {
            store.put(key_description.clone_key(), value, key_description.id);
        }
        status
    }

    fn put_with_ttl<DeleteHook>(
        store: &Arc<Store<Key, Value>>,
        admission_policy: &Arc<AdmissionPolicy<Key>>,
        key_description: &KeyDescription<Key>,
        delete_hook: &DeleteHook,
        value: Value,
        ttl: Duration) -> CommandStatus
        where DeleteHook: Fn(Key) {
        let status = admission_policy.maybe_add(key_description, delete_hook);
        if let CommandStatus::Accepted = status {
            store.put_with_ttl(key_description.clone_key(), value, key_description.id, ttl);
        }
        status
    }

    fn delete(
        store: &Arc<Store<Key, Value>>,
        admission_policy: &Arc<AdmissionPolicy<Key>>,
        key: &Key) -> CommandStatus {
        let key_id = store.delete(key);
        if let Some(key_id) = key_id {
            admission_policy.delete(&key_id);
            return CommandStatus::Accepted;
        }
        CommandStatus::Rejected
    }

    pub(crate) fn send(&self, command: CommandType<Key, Value>) -> CommandSendResult {
        let acknowledgement = CommandAcknowledgement::new();
        let send_result = self.sender.send(CommandAcknowledgementPair {
            command,
            acknowledgement: acknowledgement.clone(),
        });

        match send_result {
            Ok(_) => Ok(acknowledgement),
            Err(err) => Err(CommandSendError::new(err.0.command.description()))
        }
    }

    pub(crate) fn shutdown(&self) {
        self.keep_running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::cache::clock::SystemClock;
    use crate::cache::command::{CommandStatus, CommandType};
    use crate::cache::command::command_executor::CommandExecutor;
    use crate::cache::key_description::KeyDescription;
    use crate::cache::policy::admission_policy::AdmissionPolicy;
    use crate::cache::store::Store;

    #[tokio::test]
    async fn puts_a_key_value_and_shutdown() {
        let store = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 100));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            10,
        );
        command_executor.shutdown();

        command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
        )).unwrap().handle().await;

        let send_result = command_executor.send(CommandType::Put(
            KeyDescription::new("disk", 2, 2090, 10),
            "SSD",
        ));

        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert_eq!(None, store.get(&"disk"));
        assert!(send_result.is_err());
    }

    #[tokio::test]
    async fn puts_a_key_value() {
        let store = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 100));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            10,
        );

        let command_acknowledgement = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
        )).unwrap();
        command_acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(Some("microservices"), store.get(&"topic"));
    }

    #[tokio::test]
    async fn key_value_gets_rejected_given_its_weight_is_more_than_the_cache_weight() {
        let store = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 100));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            10,
        );

        let command_acknowledgement = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 200),
            "microservices",
        )).unwrap();
        let status = command_acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(None, store.get(&"topic"));
        assert_eq!(CommandStatus::Rejected, status);
    }

    #[tokio::test]
    async fn puts_a_couple_of_key_values() {
        let store = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 100));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            10,
        );

        let acknowledgement = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
        )).unwrap();
        let other_acknowledgment = command_executor.send(CommandType::Put(
            KeyDescription::new("disk", 2, 2076, 3),
            "SSD",
        )).unwrap();
        acknowledgement.handle().await;
        other_acknowledgment.handle().await;

        command_executor.shutdown();
        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert_eq!(Some("SSD"), store.get(&"disk"));
    }

    #[tokio::test]
    async fn puts_a_key_value_with_ttl() {
        let store = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 100));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            10,
        );

        let acknowledgement = command_executor.send(CommandType::PutWithTTL(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
            Duration::from_secs(10),
        )).unwrap();
        acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(Some("microservices"), store.get(&"topic"));
    }

    #[tokio::test]
    async fn deletes_a_key() {
        let store = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 100));
        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            10,
        );

        let acknowledgement = command_executor.send(CommandType::PutWithTTL(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
            Duration::from_secs(10),
        )).unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            command_executor.send(CommandType::Delete("topic")).unwrap();
        acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(None, store.get(&"topic"));
    }

    #[tokio::test]
    async fn deletion_of_a_non_existing_key_value_gets_rejected() {
        let store: Arc<Store<&str, &str>> = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 100));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            10,
        );

        let acknowledgement =
            command_executor.send(CommandType::Delete("non-existing")).unwrap();
        let status = acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(CommandStatus::Rejected, status);
    }
}

#[cfg(test)]
mod sociable_tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::cache::clock::SystemClock;
    use crate::cache::command::{CommandStatus, CommandType};
    use crate::cache::command::command_executor::CommandExecutor;
    use crate::cache::key_description::KeyDescription;
    use crate::cache::policy::admission_policy::AdmissionPolicy;
    use crate::cache::pool::BufferConsumer;
    use crate::cache::store::Store;

    #[tokio::test]
    async fn puts_a_key_value() {
        let store = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 100));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy.clone(),
            10,
        );

        let key_description = KeyDescription::new("topic", 1, 1029, 10);
        let key_id = key_description.id;
        let command_acknowledgement = command_executor.send(CommandType::Put(
            key_description,
            "microservices",
        )).unwrap();
        command_acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert!(admission_policy.contains(&key_id));
    }

    #[tokio::test]
    async fn puts_a_key_value_by_eliminating_victims() {
        let store = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 10));

        let key_hashes = vec![10, 14, 116];
        admission_policy.accept(key_hashes);
        thread::sleep(Duration::from_secs(1));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy.clone(),
            10,
        );

        let command_acknowledgement = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 10, 5),
            "microservices",
        )).unwrap();
        let status = command_acknowledgement.handle().await;
        assert_eq!(CommandStatus::Accepted, status);

        let command_acknowledgement = command_executor.send(CommandType::Put(
            KeyDescription::new("disk", 2, 14, 6),
            "SSD",
        )).unwrap();
        let status = command_acknowledgement.handle().await;
        assert_eq!(CommandStatus::Accepted, status);

        command_executor.shutdown();

        assert!(admission_policy.contains(&2));
        assert_eq!(Some("SSD"), store.get(&"disk"));

        assert!(!admission_policy.contains(&1));
        assert_eq!(None, store.get(&"topic"));
    }

    #[tokio::test]
    async fn deletes_a_key() {
        let store = Store::new(SystemClock::boxed());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, 100));
        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy.clone(),
            10,
        );

        let acknowledgement = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
        )).unwrap();
        acknowledgement.handle().await;

        let acknowledgement =
            command_executor.send(CommandType::Delete("topic")).unwrap();
        acknowledgement.handle().await;

        command_executor.shutdown();
        assert_eq!(None, store.get(&"topic"));
        assert!(!admission_policy.contains(&1));
    }
}