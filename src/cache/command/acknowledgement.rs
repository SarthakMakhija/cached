use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use parking_lot::Mutex;
use crate::cache::command::CommandStatus;

pub struct CommandAcknowledgement {
    handle: CommandAcknowledgementHandle,
}

pub struct CommandAcknowledgementHandle {
    done: AtomicBool,
    status: Arc<Mutex<CommandStatus>>,
    waker_state: Arc<Mutex<WakerState>>,
}

pub(crate) struct WakerState {
    waker: Option<Waker>,
}

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

    pub(crate) fn done(&self, status: CommandStatus) {
        self.handle.done(status);
    }

    pub fn handle(&self) -> &CommandAcknowledgementHandle {
        &self.handle
    }
}

impl CommandAcknowledgementHandle {
    pub(crate) fn done(&self, status: CommandStatus) {
        self.done.store(true, Ordering::Release);
        *self.status.lock() = status;
        if let Some(waker) = &self.waker_state.lock().waker {
            waker.wake_by_ref();
        }
    }
}

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
}