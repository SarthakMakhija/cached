use std::hash::Hash;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::Receiver;
use log::{error, info};

use crate::cache::command::{CommandStatus, CommandType};
use crate::cache::command::acknowledgement::CommandAcknowledgement;
use crate::cache::command::error::CommandSendError;
use crate::cache::command::RejectionReason::KeyDoesNotExist;
use crate::cache::expiration::TTLTicker;
use crate::cache::key_description::KeyDescription;
use crate::cache::policy::admission_policy::AdmissionPolicy;
use crate::cache::stats::ConcurrentStatsCounter;
use crate::cache::store::Store;

/// Every write operation like `put`, `put_or_update` and `delete` is returned a [`crate::cache::command::command_executor::CommandSendResult`] that
/// wraps an instance of [`crate::cache::command::acknowledgement::CommandAcknowledgement`] and a [`crate::cache::command::error::CommandSendError`]
pub type CommandSendResult = Result<Arc<CommandAcknowledgement>, CommandSendError>;

pub(crate) fn shutdown_result() -> CommandSendResult {
    Err(CommandSendError::shutdown())
}

/// CommandExecutor executes various commands of type `crate::cache::command::CommandType`.
/// CommandExecutor spins a thread when it is instantiated and starts receiving commands from the `crossbeam_channel::Receiver`.
/// The command is wrapped in an abstraction `CommandAcknowledgementPair` that combines `CommandType` and `CommandAcknowledgement`
/// Execution of a command typically involves interacting with `crate::cache::policy::admission_policy::AdmissionPolicy`,
/// `crate::cache::store::Store` and `crate::cache::expiration::TTLTicker`
pub(crate) struct CommandExecutor<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static {
    sender: crossbeam_channel::Sender<CommandAcknowledgementPair<Key, Value>>,
}

struct CommandAcknowledgementPair<Key, Value>
    where Key: Hash + Eq + Clone {
    command: CommandType<Key, Value>,
    acknowledgement: Arc<CommandAcknowledgement>,
}

struct PutParameter<'a, Key, Value, DeleteHook>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static,
          DeleteHook: Fn(Key) {
    store: &'a Arc<Store<Key, Value>>,
    key_description: &'a KeyDescription<Key>,
    delete_hook: &'a DeleteHook,
    value: Value,
    admission_policy: &'a Arc<AdmissionPolicy<Key>>,
    stats_counter: &'a Arc<ConcurrentStatsCounter>,
}

struct PutWithTTLParameter<'a, Key, Value, DeleteHook>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static,
          DeleteHook: Fn(Key) {
    put_parameter: PutParameter<'a, Key, Value, DeleteHook>,
    ttl: Duration,
    ttl_ticker: &'a Arc<TTLTicker>,
}

struct DeleteParameter<'a, Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static {
    store: &'a Arc<Store<Key, Value>>,
    key: &'a Key,
    admission_policy: &'a Arc<AdmissionPolicy<Key>>,
    ttl_ticker: &'a Arc<TTLTicker>,
}

impl<Key, Value> CommandExecutor<Key, Value>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + 'static {
    pub(crate) fn new(
        store: Arc<Store<Key, Value>>,
        admission_policy: Arc<AdmissionPolicy<Key>>,
        stats_counter: Arc<ConcurrentStatsCounter>,
        ttl_ticker: Arc<TTLTicker>,
        command_channel_size: usize) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(command_channel_size);
        let command_executor = CommandExecutor { sender };

        command_executor.spin(receiver, store, admission_policy, stats_counter, ttl_ticker);
        command_executor
    }

    /// Spins a thread when `CommandExecutor` is instantiated.
    /// The thread receives a command wrapped in `CommandAcknowledgementPair` from the [`crossbeam_channel::Receiver<T>`].
    /// It identifies the command and performs an appropriate action.
    /// Execution of a command typically involves interacting with `crate::cache::policy::admission_policy::AdmissionPolicy`,
    /// `crate::cache::store::Store` and `crate::cache::expiration::TTLTicker`.
    /// Handling `Shutdown` command is a little tricky. There exists a race condition kind of a case:
    /// Consider that `shutdown()` and `put()` on an instance of `Cached` are invoked at the same time.
    /// Both these operations result in sending different commands to `CommandExecutor`.
    /// Somehow, the `Shutdown` command goes in before the `put` command.
    /// This also means that the client could have performed `await` operation on the `CommandAcknowledgement` of the `put` command.
    /// It is essential to complete the future that the client is awaiting on. That is what the `Shutdown` command does.
    /// It drains the `receiver` and marks the status of the CommandAcknowledgement as `CommandStatus::ShuttingDown`.
    /// The client(s) awaiting on the future will receive `CommandStatus::ShuttingDown`.
    fn spin(&self,
            receiver: Receiver<CommandAcknowledgementPair<Key, Value>>,
            store: Arc<Store<Key, Value>>,
            admission_policy: Arc<AdmissionPolicy<Key>>,
            stats_counter: Arc<ConcurrentStatsCounter>,
            ttl_ticker: Arc<TTLTicker>) {
        let store_clone = store.clone();
        let delete_hook = move |key| { store_clone.delete(&key); };

        thread::spawn(move || {
            while let Ok(pair) = receiver.recv() {
                let command = pair.command;
                let status = match command {
                    CommandType::Put(key_description, value) =>
                        Self::put(PutParameter {
                            store: &store,
                            key_description: &key_description,
                            delete_hook: &delete_hook,
                            value,
                            admission_policy: &admission_policy,
                            stats_counter: &stats_counter,
                        }),
                    CommandType::PutWithTTL(key_description, value, ttl) =>
                        Self::put_with_ttl(PutWithTTLParameter {
                            put_parameter: PutParameter {
                                store: &store,
                                key_description: &key_description,
                                delete_hook: &delete_hook,
                                value,
                                admission_policy: &admission_policy,
                                stats_counter: &stats_counter,
                            },
                            ttl,
                            ttl_ticker: &ttl_ticker,
                        }),
                    CommandType::UpdateWeight(key_id, weight) => {
                        admission_policy.update(&key_id, weight);
                        CommandStatus::Accepted
                    }
                    CommandType::Delete(key) =>
                        Self::delete(DeleteParameter {
                            store: &store,
                            key: &key,
                            admission_policy: &admission_policy,
                            ttl_ticker: &ttl_ticker,
                        }),
                    CommandType::Shutdown => {
                        info!("Received Shutdown command");
                        pair.acknowledgement.done(CommandStatus::Accepted);
                        for command_acknowledgement_pair in receiver.iter() {
                            command_acknowledgement_pair.acknowledgement.done(CommandStatus::ShuttingDown);
                        }
                        drop(receiver);
                        break;
                    }
                };
                pair.acknowledgement.done(status);
            }
        });
    }

    /// Sends a command to the `CommandExecutor`. Every Command is wrapped in a `CommandAcknowledgementPair`
    /// that allows 2 things:
    /// 1) It allows returning an instance of `CommandAcknowledgement` to the clients, so that they can perform `await`
    /// 2) It allows `CommandExecutor` to change the status of the command inside `CommandAcknowledgement`. This would then finish the `await` at the client's end.
    pub(crate) fn send(&self, command: CommandType<Key, Value>) -> CommandSendResult {
        let acknowledgement = CommandAcknowledgement::new();
        let send_result = self.sender.send(CommandAcknowledgementPair {
            command,
            acknowledgement: acknowledgement.clone(),
        });

        match send_result {
            Ok(_) => Ok(acknowledgement),
            Err(err) => {
                error!("received a SendError while sending command type {}", err.0.command.description());
                Err(CommandSendError::new(err.0.command.description()))
            }
        }
    }

    /// Sends a Shutdown command to the `CommandExecutor`.
    pub(crate) fn shutdown(&self) -> CommandSendResult {
        self.send(CommandType::Shutdown)
    }

    fn put<DeleteHook>(put_parameters: PutParameter<Key, Value, DeleteHook>) -> CommandStatus where DeleteHook: Fn(Key) {
        let status = put_parameters.admission_policy.maybe_add(
            put_parameters.key_description,
            put_parameters.delete_hook,
        );
        if let CommandStatus::Accepted = status {
            put_parameters.store.put(
                put_parameters.key_description.clone_key(),
                put_parameters.value,
                put_parameters.key_description.id,
            );
        } else {
            put_parameters.stats_counter.reject_key();
        }
        status
    }

    fn put_with_ttl<DeleteHook>(put_with_ttl_parameter: PutWithTTLParameter<Key, Value, DeleteHook>) -> CommandStatus where DeleteHook: Fn(Key) {
        let status = put_with_ttl_parameter.put_parameter.admission_policy.maybe_add(
            put_with_ttl_parameter.put_parameter.key_description,
            put_with_ttl_parameter.put_parameter.delete_hook,
        );
        if let CommandStatus::Accepted = status {
            let expiry = put_with_ttl_parameter.put_parameter.store.put_with_ttl(
                put_with_ttl_parameter.put_parameter.key_description.clone_key(),
                put_with_ttl_parameter.put_parameter.value,
                put_with_ttl_parameter.put_parameter.key_description.id,
                put_with_ttl_parameter.ttl,
            );
            put_with_ttl_parameter.ttl_ticker.put(
                put_with_ttl_parameter.put_parameter.key_description.id,
                expiry,
            );
        } else {
            put_with_ttl_parameter.put_parameter.stats_counter.reject_key();
        }
        status
    }

    fn delete(delete_parameter: DeleteParameter<Key, Value>) -> CommandStatus {
        let may_be_key_id_expiry = delete_parameter.store.delete(delete_parameter.key);
        if let Some(key_id_expiry) = may_be_key_id_expiry {
            delete_parameter.admission_policy.delete(&key_id_expiry.0);
            if let Some(expiry) = key_id_expiry.1 {
                delete_parameter.ttl_ticker.delete(&key_id_expiry.0, &expiry);
            }
            return CommandStatus::Accepted;
        }
        CommandStatus::Rejected(KeyDoesNotExist)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::cache::clock::{ClockType, SystemClock};
    use crate::cache::command::{CommandStatus, CommandType};
    use crate::cache::command::command_executor::{CommandExecutor, shutdown_result};
    use crate::cache::command::RejectionReason::{KeyDoesNotExist, KeyWeightIsGreaterThanCacheWeight};
    use crate::cache::expiration::config::TTLConfig;
    use crate::cache::expiration::TTLTicker;
    use crate::cache::key_description::KeyDescription;
    use crate::cache::policy::admission_policy::AdmissionPolicy;
    use crate::cache::policy::config::CacheWeightConfig;
    use crate::cache::stats::ConcurrentStatsCounter;
    use crate::cache::store::Store;

    fn no_action_ttl_ticker() -> Arc<TTLTicker> {
        TTLTicker::new(TTLConfig::new(4, Duration::from_secs(300), SystemClock::boxed()), |_key_id| {})
    }

    fn test_store(clock: ClockType, stats_counter: Arc<ConcurrentStatsCounter>) -> Arc<Store<&'static str, &'static str>> {
        Store::new(clock, stats_counter, 16, 4)
    }

    fn test_cache_weight_config() -> CacheWeightConfig {
        CacheWeightConfig::new(100, 4, 100)
    }

    mod setup {
        use std::time::SystemTime;

        use crate::cache::clock::Clock;

        #[derive(Clone)]
        pub(crate) struct UnixEpochClock;

        impl Clock for UnixEpochClock {
            fn now(&self) -> SystemTime {
                SystemTime::UNIX_EPOCH
            }
        }
    }

    #[test]
    fn result_on_shutdown() {
        let result = shutdown_result();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn puts_a_key_value_after_shutdown_with_delay() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter,
            no_action_ttl_ticker(),
            10,
        );
        command_executor.shutdown().unwrap().handle().await;

        thread::sleep(Duration::from_secs(1));

        let send_result = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
        ));

        assert!(send_result.is_err() || send_result.unwrap().handle().await == CommandStatus::ShuttingDown);
    }

    #[tokio::test]
    async fn puts_a_key_value_after_shutdown() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter,
            no_action_ttl_ticker(),
            10,
        );
        command_executor.shutdown().unwrap().handle().await;

        let send_result = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
        ));
        assert!(send_result.is_err() || send_result.unwrap().handle().await == CommandStatus::ShuttingDown);
    }

    #[tokio::test]
    async fn puts_a_key_value() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter,
            no_action_ttl_ticker(),
            10,
        );

        let command_acknowledgement = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
        )).unwrap();
        command_acknowledgement.handle().await;

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(Some("microservices"), store.get(&"topic"));
    }

    #[tokio::test]
    async fn key_value_gets_rejected_given_its_weight_is_more_than_the_cache_weight() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter.clone(),
            no_action_ttl_ticker(),
            10,
        );

        let command_acknowledgement = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 200),
            "microservices",
        )).unwrap();
        let status = command_acknowledgement.handle().await;

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(None, store.get(&"topic"));
        assert_eq!(CommandStatus::Rejected(KeyWeightIsGreaterThanCacheWeight), status);
    }

    #[tokio::test]
    async fn rejects_a_key_value_and_increase_stats() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter.clone(),
            no_action_ttl_ticker(),
            10,
        );

        let command_acknowledgement = command_executor.send(CommandType::Put(
            KeyDescription::new("topic", 1, 1029, 200),
            "microservices",
        )).unwrap();
        let status = command_acknowledgement.handle().await;

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(CommandStatus::Rejected(KeyWeightIsGreaterThanCacheWeight), status);
        assert_eq!(1, stats_counter.keys_rejected());
    }

    #[tokio::test]
    async fn puts_a_couple_of_key_values() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter,
            no_action_ttl_ticker(),
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

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert_eq!(Some("SSD"), store.get(&"disk"));
    }

    #[tokio::test]
    async fn puts_a_key_value_with_ttl() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let ttl_ticker = no_action_ttl_ticker();
        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter,
            ttl_ticker.clone(),
            10,
        );

        let acknowledgement = command_executor.send(CommandType::PutWithTTL(
            KeyDescription::new("topic", 1, 1029, 10),
            "microservices",
            Duration::from_secs(10),
        )).unwrap();
        acknowledgement.handle().await;

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(Some("microservices"), store.get(&"topic"));

        let expiry = store.get_ref(&"topic").unwrap().value().expire_after().unwrap();
        let expiry_in_ttl_ticker = ttl_ticker.get(&1, &expiry).unwrap();

        assert_eq!(expiry, expiry_in_ttl_ticker);
    }

    #[tokio::test]
    async fn rejects_a_key_value_with_ttl_and_increase_stats() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter.clone(),
            no_action_ttl_ticker(),
            10,
        );

        let acknowledgement = command_executor.send(CommandType::PutWithTTL(
            KeyDescription::new("topic", 1, 1029, 4000),
            "microservices",
            Duration::from_secs(10),
        )).unwrap();
        acknowledgement.handle().await;

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(1, stats_counter.keys_rejected());
    }

    #[tokio::test]
    async fn deletes_a_key() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));
        let ttl_ticker = no_action_ttl_ticker();

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter,
            ttl_ticker.clone(),
            10,
        );

        let acknowledgement = command_executor.send(CommandType::PutWithTTL(
            KeyDescription::new("topic", 10, 1029, 10),
            "microservices",
            Duration::from_secs(10),
        )).unwrap();
        acknowledgement.handle().await;

        let expiry = store.get_ref(&"topic").unwrap().value().expire_after().unwrap();
        let expiry_in_ttl_ticker = ttl_ticker.get(&10, &expiry).unwrap();

        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert_eq!(expiry, expiry_in_ttl_ticker);

        let acknowledgement =
            command_executor.send(CommandType::Delete("topic")).unwrap();
        acknowledgement.handle().await;

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(None, store.get(&"topic"));
        assert_eq!(None, ttl_ticker.get(&10, &expiry));
    }

    #[tokio::test]
    async fn deletion_of_a_non_existing_key_value_gets_rejected() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store= test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy,
            stats_counter,
            no_action_ttl_ticker(),
            10,
        );

        let acknowledgement =
            command_executor.send(CommandType::Delete("non-existing")).unwrap();
        let status = acknowledgement.handle().await;

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(CommandStatus::Rejected(KeyDoesNotExist), status);
    }
}

#[cfg(test)]
mod sociable_tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::cache::buffer_event::{BufferConsumer, BufferEvent};
    use crate::cache::clock::{ClockType, SystemClock};
    use crate::cache::command::{CommandStatus, CommandType};
    use crate::cache::command::command_executor::CommandExecutor;
    use crate::cache::command::command_executor::Store;
    use crate::cache::expiration::config::TTLConfig;
    use crate::cache::expiration::TTLTicker;
    use crate::cache::key_description::KeyDescription;
    use crate::cache::policy::admission_policy::AdmissionPolicy;
    use crate::cache::policy::config::CacheWeightConfig;
    use crate::cache::stats::ConcurrentStatsCounter;

    fn no_action_ttl_ticker() -> Arc<TTLTicker> {
        TTLTicker::new(TTLConfig::new(4, Duration::from_secs(300), SystemClock::boxed()), |_key_id| {})
    }

    fn test_store(clock: ClockType, stats_counter: Arc<ConcurrentStatsCounter>) -> Arc<Store<&'static str, &'static str>> {
        Store::new(clock, stats_counter, 16, 4)
    }

    fn test_cache_weight_config() -> CacheWeightConfig {
        CacheWeightConfig::new(100, 4, 100)
    }

    #[tokio::test]
    async fn puts_a_key_value() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy.clone(),
            stats_counter,
            no_action_ttl_ticker(),
            10,
        );

        let key_description = KeyDescription::new("topic", 1, 1029, 10);
        let key_id = key_description.id;
        let command_acknowledgement = command_executor.send(CommandType::Put(
            key_description,
            "microservices",
        )).unwrap();
        command_acknowledgement.handle().await;

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert!(admission_policy.contains(&key_id));
    }

    #[tokio::test]
    async fn puts_a_key_value_by_eliminating_victims() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let cache_weight_config = CacheWeightConfig::new(100, 4, 10);
        let admission_policy = Arc::new(AdmissionPolicy::new(10, cache_weight_config, stats_counter.clone()));

        let key_hashes = vec![10, 14, 116];
        admission_policy.accept(BufferEvent::Full(key_hashes));
        thread::sleep(Duration::from_secs(1));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy.clone(),
            stats_counter,
            no_action_ttl_ticker(),
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

        command_executor.shutdown().unwrap().handle().await;

        assert!(admission_policy.contains(&2));
        assert_eq!(Some("SSD"), store.get(&"disk"));

        assert!(!admission_policy.contains(&1));
        assert_eq!(None, store.get(&"topic"));
    }

    #[tokio::test]
    async fn deletes_a_key() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));
        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy.clone(),
            stats_counter,
            no_action_ttl_ticker(),
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

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(None, store.get(&"topic"));
        assert!(!admission_policy.contains(&1));
    }

    #[tokio::test]
    async fn updates_the_weight_of_the_key() {
        let stats_counter = Arc::new(ConcurrentStatsCounter::new());
        let store = test_store(SystemClock::boxed(), stats_counter.clone());
        let admission_policy = Arc::new(AdmissionPolicy::new(10, test_cache_weight_config(), stats_counter.clone()));

        let command_executor = CommandExecutor::new(
            store.clone(),
            admission_policy.clone(),
            stats_counter,
            no_action_ttl_ticker(),
            10,
        );

        let key_description = KeyDescription::new("topic", 1, 1029, 10);
        let key_id = key_description.id;
        let command_acknowledgement = command_executor.send(CommandType::Put(
            key_description,
            "microservices",
        )).unwrap();
        command_acknowledgement.handle().await;

        let command_acknowledgement = command_executor.send(CommandType::UpdateWeight(
            1, 20)).unwrap();
        command_acknowledgement.handle().await;

        command_executor.shutdown().unwrap().handle().await;
        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert_eq!(Some(20), admission_policy.weight_of(&key_id));
    }
}