use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use crate::TantivyError;

/// `FutureResult` is a handle that makes it possible to wait for the completion
/// of an ongoing task.
///
/// Contrary to some `Future`, it does not need to be polled for the task to
/// progress. Dropping the `FutureResult` does not cancel the task being executed
/// either.
///
/// - In a sync context, you can call `FutureResult::wait()`. The function does not rely on
///   `block_on`.
/// - In an async context, you can call simply use `FutureResult` as a future.
pub struct FutureResult<T> {
    inner: Inner<T>,
}

enum Inner<T> {
    FailedBeforeStart(Option<TantivyError>),
    InProgress {
        receiver: oneshot::Receiver<crate::Result<T>>,
        error_msg_if_failure: &'static str,
    },
}

impl<T> From<TantivyError> for FutureResult<T> {
    fn from(err: TantivyError) -> Self {
        FutureResult {
            inner: Inner::FailedBeforeStart(Some(err)),
        }
    }
}

impl<T> FutureResult<T> {
    pub(crate) fn create(
        error_msg_if_failure: &'static str,
    ) -> (Self, oneshot::Sender<crate::Result<T>>) {
        let (sender, receiver) = oneshot::channel();
        let inner: Inner<T> = Inner::InProgress {
            receiver,
            error_msg_if_failure,
        };
        (FutureResult { inner }, sender)
    }

    /// Blocks until the scheduled result is available.
    ///
    /// In an async context, you should simply use `ScheduledResult` as a future.
    pub fn wait(self) -> crate::Result<T> {
        match self.inner {
            Inner::FailedBeforeStart(err) => Err(err.unwrap()),
            Inner::InProgress {
                receiver,
                error_msg_if_failure,
            } => receiver.recv().unwrap_or_else(|_| {
                Err(crate::TantivyError::SystemError(
                    error_msg_if_failure.to_string(),
                ))
            }),
        }
    }
}

impl<T> Future for FutureResult<T> {
    type Output = crate::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        unsafe {
            match &mut Pin::get_unchecked_mut(self).inner {
                Inner::FailedBeforeStart(err) => Poll::Ready(Err(err.take().unwrap())),
                Inner::InProgress {
                    receiver,
                    error_msg_if_failure,
                } => match Future::poll(Pin::new_unchecked(receiver), cx) {
                    Poll::Ready(oneshot_res) => {
                        let res = oneshot_res.unwrap_or_else(|_| {
                            Err(crate::TantivyError::SystemError(
                                error_msg_if_failure.to_string(),
                            ))
                        });
                        Poll::Ready(res)
                    }
                    Poll::Pending => Poll::Pending,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::FutureResult;
    use crate::TantivyError;

    #[test]
    fn test_scheduled_result_failed_to_schedule() {
        let scheduled_result: FutureResult<()> = FutureResult::from(TantivyError::Poisoned);
        let res = block_on(scheduled_result);
        assert!(matches!(res, Err(TantivyError::Poisoned)));
    }

    #[test]

    fn test_scheduled_result_error() {
        let (scheduled_result, tx): (FutureResult<()>, _) = FutureResult::create("failed");
        drop(tx);
        let res = block_on(scheduled_result);
        assert!(matches!(res, Err(TantivyError::SystemError(_))));
    }

    #[test]
    fn test_scheduled_result_sent_success() {
        let (scheduled_result, tx): (FutureResult<u64>, _) = FutureResult::create("failed");
        tx.send(Ok(2u64)).unwrap();
        assert_eq!(block_on(scheduled_result).unwrap(), 2u64);
    }

    #[test]
    fn test_scheduled_result_sent_error() {
        let (scheduled_result, tx): (FutureResult<u64>, _) = FutureResult::create("failed");
        tx.send(Err(TantivyError::Poisoned)).unwrap();
        let res = block_on(scheduled_result);
        assert!(matches!(res, Err(TantivyError::Poisoned)));
    }
}
