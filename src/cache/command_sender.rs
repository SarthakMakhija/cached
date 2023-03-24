use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crossbeam_channel::Receiver;
use crate::cache::acknowledgement::CommandAcknowledgement;

use crate::cache::command::CommandType;
use crate::cache::store::store::Store;

struct  CommandAcknowledgementPair<Key, Value> {
    command: CommandType<Key, Value>,
    acknowledgement: Arc<CommandAcknowledgement>
}

pub(crate) struct CommandSender<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    sender: crossbeam_channel::Sender<CommandAcknowledgementPair<Key, Value>>,
    keep_running: Arc<AtomicBool>,
}

impl<Key, Value> CommandSender<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {

    pub(crate) fn new(store: Arc<Store<Key, Value>>) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(100);
        let command_sender = CommandSender { sender, keep_running: Arc::new(AtomicBool::new(true)) };
        command_sender.spin(receiver, store);

        return command_sender;
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
                }
                pair.acknowledgement.done();
                if !keep_running.load(Ordering::Acquire) {
                    return;
                }
            }
        });
    }

    pub(crate) fn send(&self, command: CommandType<Key, Value>) -> Arc<CommandAcknowledgement> {
        let acknowledgement = CommandAcknowledgement::new();
        self.sender.send(CommandAcknowledgementPair{command, acknowledgement: acknowledgement.clone() }).unwrap(); //TODO: Remove unwrap
        return acknowledgement;
    }

    pub(crate) fn shutdown(&self) {
        self.keep_running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::cache::clock::SystemClock;
    use crate::cache::command::CommandType;
    use crate::cache::command_sender::CommandSender;
    use crate::cache::store::store::Store;

    #[tokio::test]
    async fn puts_a_key_value() {
        let store = Store::new(SystemClock::boxed());
        let command_sender = CommandSender::new(
            store.clone()
        );
        let command_acknowledgement = command_sender.send(CommandType::Put(
            "topic",
            "microservices"
        ));

        command_acknowledgement.handle().await;
        command_sender.shutdown();

        assert_eq!(Some("microservices"), store.get(&"topic"));
    }

    #[tokio::test]
    async fn puts_a_couple_of_key_values() {
        let store = Store::new(SystemClock::boxed());
        let command_sender = CommandSender::new(
            store.clone()
        );
        let acknowledgement = command_sender.send(CommandType::Put(
            "topic",
            "microservices"
        ));
        let other_acknowledgment  = command_sender.send(CommandType::Put(
            "disk",
            "SSD"
        ));

        acknowledgement.handle().await;
        other_acknowledgment.handle().await;
        command_sender.shutdown();

        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert_eq!(Some("SSD"), store.get(&"disk"));
    }

    #[tokio::test]
    async fn puts_a_key_value_with_ttl() {
        let store = Store::new(SystemClock::boxed());
        let command_sender = CommandSender::new(
            store.clone()
        );

        let acknowledgement = command_sender.send(CommandType::PutWithTTL(
            "topic",
            "microservices",
            Duration::from_secs(10)
        ));

        acknowledgement.handle().await;
        command_sender.shutdown();

        assert_eq!(Some("microservices"), store.get(&"topic"));
    }
}