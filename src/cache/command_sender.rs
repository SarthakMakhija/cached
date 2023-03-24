use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crossbeam_channel::Receiver;

use crate::cache::command::CommandType;
use crate::cache::store::Store;

pub(crate) struct CommandSender<Key, Value>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    sender: crossbeam_channel::Sender<CommandType<Key, Value>>,
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

    fn spin(&self, receiver: Receiver<CommandType<Key, Value>>, store: Arc<Store<Key, Value>>) {
        let keep_running = self.keep_running.clone();
        thread::spawn(move || {
            while let Ok(command) = receiver.recv() {
                match command {
                    CommandType::Put(key, value) =>
                        store.put(key, value),
                    CommandType::PutWithTTL(key, value, ttl) =>
                        store.put_with_ttl(key, value, ttl),
                }
                if !keep_running.load(Ordering::Acquire) {
                    return;
                }
            }
        });
    }

    pub(crate) fn send(&self, command: CommandType<Key, Value>) {
        self.sender.send(command).unwrap(); //TODO: Remove unwrap
    }

    pub(crate) fn shutdown(&self) {
        self.keep_running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use crate::cache::clock::SystemClock;
    use crate::cache::command::CommandType;
    use crate::cache::command_sender::CommandSender;
    use crate::cache::store::Store;

    #[test]
    fn puts_a_key_value() {
        let store = Store::new(SystemClock::boxed());
        let command_sender = CommandSender::new(
            store.clone()
        );
        command_sender.send(CommandType::Put("topic", "microservices"));

        thread::sleep(Duration::from_millis(5)); //TODO: Remove sleep
        command_sender.shutdown();

        assert_eq!(Some("microservices"), store.get(&"topic"));
    }

    #[test]
    fn puts_a_couple_of_key_values() {
        let store = Store::new(SystemClock::boxed());
        let command_sender = CommandSender::new(
            store.clone()
        );
        command_sender.send(CommandType::Put("topic", "microservices"));
        command_sender.send(CommandType::Put("disk", "SSD"));

        thread::sleep(Duration::from_millis(5)); //TODO: Remove sleep
        command_sender.shutdown();

        assert_eq!(Some("microservices"), store.get(&"topic"));
        assert_eq!(Some("SSD"), store.get(&"disk"));
    }

    #[test]
    fn puts_a_key_value_with_ttl() {
        let store = Store::new(SystemClock::boxed());
        let command_sender = CommandSender::new(
            store.clone()
        );
        command_sender.send(CommandType::PutWithTTL("topic", "microservices", Duration::from_secs(10)));

        thread::sleep(Duration::from_millis(5)); //TODO: Remove sleep
        command_sender.shutdown();

        assert_eq!(Some("microservices"), store.get(&"topic"));
    }
}