use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

pub struct CommandAcknowledgement {
    handle: CommandAcknowledgementHandle,
}

pub struct CommandAcknowledgementHandle {
    done: AtomicBool,
    waker_state: Arc<Mutex<WakerState>>,
}

pub(crate) struct WakerState {
    waker: Option<Waker>,
}

impl CommandAcknowledgement {
    pub(crate) fn new() -> Arc<CommandAcknowledgement> {
        return Arc::new(
            CommandAcknowledgement {
                handle: CommandAcknowledgementHandle {
                    done: AtomicBool::new(false),
                    waker_state: Arc::new(Mutex::new(WakerState {
                        waker: None
                    })),
                },
            }
        );
    }

    pub(crate) fn done(&self) {
        self.handle.done();
    }

    pub fn handle(&self) -> &CommandAcknowledgementHandle {
        return &self.handle;
    }
}

impl CommandAcknowledgementHandle {
    pub(crate) fn done(&self) {
        self.done.store(true, Ordering::Release);
        if let Some(waker) = &self.waker_state.lock().unwrap().waker {
            waker.wake_by_ref();
        }
    }
}

impl Future for &CommandAcknowledgementHandle {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.waker_state.lock().unwrap();
        match guard.waker.as_ref() {
            Some(waker) => {
                if !waker.will_wake(context.waker()) {
                    (*guard).waker = Some(context.waker().clone());
                }
            }
            None => {
                guard.waker = Some(context.waker().clone());
            }
        }

        if self.done.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        return Poll::Pending;
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::command::acknowledgement::CommandAcknowledgement;

    #[tokio::test]
    async fn acknowledge() {
        let acknowledgement = CommandAcknowledgement::new();
        tokio::spawn({
            let acknowledgement = acknowledgement.clone();
            async move {
                acknowledgement.done();
            }
        });

        let response = acknowledgement.handle().await;
        assert_eq!((), response);
    }
}