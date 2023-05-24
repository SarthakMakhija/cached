use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use parking_lot::Mutex;
use crate::cache::command::CommandStatus;

/// The execution of every write operation is returned a `CommandAcknowledgement` wrapped inside [`crate::cache::command::command_executor::CommandSendResult`].
/// `CommandAcknowledgement` provides a handle to the clients to perform `.await` to get the command status.
///
/// ```
/// use tinylfu_cached::cache::cached::CacheD;
/// use tinylfu_cached::cache::command::CommandStatus;
/// use tinylfu_cached::cache::config::ConfigBuilder;
/// #[tokio::main]
///  async fn main() {
///     let cached = CacheD::new(ConfigBuilder::new(100, 10, 100).build());
///     let status = cached.put("topic", "microservices").unwrap().handle().await;
///     assert_eq!(CommandStatus::Accepted, status);
///     let value = cached.get(&"topic");
///     assert_eq!(Some("microservices"), value);
/// }
/// ```
pub struct CommandAcknowledgement {
    handle: CommandAcknowledgementHandle,
}

/// CommandAcknowledgementHandle implements [`std::future::Future`] and returns a [`crate::cache::command::CommandStatus`]
///
/// The initial status in the `CommandAcknowledgementHandle` is `CommandStatus::Pending`
///
/// The status gets updated when the command is executed by the `crate::cache::command::command_executor::CommandExecutor`.
pub struct CommandAcknowledgementHandle {
    done: AtomicBool,
    status: Arc<Mutex<CommandStatus>>,
    waker_state: Arc<Mutex<WakerState>>,
}

pub(crate) struct WakerState {
    waker: Option<Waker>,
}

/// CommandAcknowledgement provides a `handle()` method  that returns a reference to the `CommandAcknowledgementHandle`
impl CommandAcknowledgement {
    pub(crate) fn new() -> Arc<CommandAcknowledgement> {
        Arc::new(
            CommandAcknowledgement {
                handle: CommandAcknowledgementHandle {
                    done: AtomicBool::new(false),
                    status: Arc::new(Mutex::new(CommandStatus::Pending)),
                    waker_state: Arc::new(Mutex::new(WakerState {
                        waker: None
                    })),
                },
            }
        )
    }
    pub(crate) fn accepted() -> Arc<CommandAcknowledgement> {
        Arc::new(
            CommandAcknowledgement {
                handle: CommandAcknowledgementHandle {
                    done: AtomicBool::new(true),
                    status: Arc::new(Mutex::new(CommandStatus::Accepted)),
                    waker_state: Arc::new(Mutex::new(WakerState {
                        waker: None
                    })),
                },
            }
        )
    }

    /// Invokes the `done()` method of `CommandAcknowledgementHandle` which changes the `CommandStatus`
    pub(crate) fn done(&self, status: CommandStatus) {
        self.handle.done(status);
    }

    pub fn handle(&self) -> &CommandAcknowledgementHandle {
        &self.handle
    }
}

impl CommandAcknowledgementHandle {
    /// Marks the flag to indicate that the command execution is done and changes the `CommandStatus`
    pub(crate) fn done(&self, status: CommandStatus) {
        self.done.store(true, Ordering::Release);
        *self.status.lock() = status;
        if let Some(waker) = &self.waker_state.lock().waker {
            waker.wake_by_ref();
        }
    }
}

/// Future implementation for CommandAcknowledgementHandle.
/// The future is complete when the the command is executed by the `crate::cache::command::command_executor::CommandExecutor`.
/// The completion of future returns [`CommandStatus`].
impl Future for &CommandAcknowledgementHandle {
    type Output = CommandStatus;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.waker_state.lock();
        match guard.waker.as_ref() {
            Some(waker) => {
                if !waker.will_wake(context.waker()) {
                    guard.waker = Some(context.waker().clone());
                }
            }
            None => {
                guard.waker = Some(context.waker().clone());
            }
        }
        if self.done.load(Ordering::Acquire) {
            return Poll::Ready(*self.status.lock());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::command::acknowledgement::CommandAcknowledgement;
    use crate::cache::command::CommandStatus;

    #[tokio::test]
    async fn acknowledge() {
        let acknowledgement = CommandAcknowledgement::new();
        tokio::spawn({
            let acknowledgement = acknowledgement.clone();
            async move {
                acknowledgement.done(CommandStatus::Accepted);
            }
        });

        let response = acknowledgement.handle().await;
        assert_eq!(CommandStatus::Accepted, response);
    }

    #[tokio::test]
    async fn accepted() {
        let acknowledgement = CommandAcknowledgement::accepted();
        let response = acknowledgement.handle().await;
        assert_eq!(CommandStatus::Accepted, response);
    }
}