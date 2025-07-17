use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use turso_core::{LimboError, Result};

type ConnectionFuture = Pin<Box<dyn Future<Output = Result<(), LimboError>>>>;
pub(crate) struct FuturesByConnection {
    futures: Vec<Option<ConnectionFuture>>,
}

impl FuturesByConnection {
    pub(crate) fn new(connections_len: usize) -> Self {
        Self {
            futures: (0..connections_len).map(|_| None).collect(),
        }
    }
    pub(crate) fn set_future(
        &mut self,
        connection_index: usize,
        future: Pin<Box<dyn Future<Output = Result<(), LimboError>>>>,
    ) {
        self.futures[connection_index] = Some(future);
    }

    pub(crate) fn poll_at(&mut self, connection_index: usize) -> Result<()> {
        if let Some(mut fut) = self.futures[connection_index].take() {
            fn dummy_raw_waker() -> RawWaker {
                fn no_op(_: *const ()) {}
                fn clone(_: *const ()) -> RawWaker {
                    dummy_raw_waker()
                }
                static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
                RawWaker::new(std::ptr::null(), &VTABLE)
            }
            let waker = unsafe { Waker::from_raw(dummy_raw_waker()) };
            let mut cx = Context::from_waker(&waker);

            // SAFETY: We know fut is a Pin<Box<impl Future>>, so we can get a mutable reference and poll it.
            let poll_result = Future::poll(Pin::as_mut(&mut fut), &mut cx);
            match poll_result {
                Poll::Ready(res) => {
                    tracing::trace!("execute_plan completed immediately: {:?}", res);
                    self.futures[connection_index] = None;
                    return res;
                }
                Poll::Pending => {
                    tracing::trace!("execute_plan yielded (pending)");
                    // Put the future back since it's still pending
                    self.futures[connection_index] = Some(fut);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn connection_without_future(&self, connection_index: usize) -> bool {
        self.futures[connection_index].is_none()
    }
}

// Custom future to yield once
pub(crate) struct YieldOnce {
    yielded: bool,
}

impl std::future::Future for YieldOnce {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.yielded {
            std::task::Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

pub(crate) async fn yield_once() {
    YieldOnce { yielded: false }.await
}
