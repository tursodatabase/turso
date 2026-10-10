use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::types::{IOCompletions, IOResult};
use crate::{LimboError, Result};

pub(crate) struct Coro<Yield> {
    bridge: Rc<Cell<Bridge<Yield>>>,
}

impl<Yield> Coro<Yield> {
    pub(crate) async fn yield_(&mut self, value: Yield) -> Result<()> {
        self.bridge.set(Bridge::Yielded(value));
        Resumed {
            bridge: &self.bridge,
        }
        .await
    }
}

impl<Yield: From<IOCompletions>> Coro<Yield> {
    pub(crate) async fn run_io<T, E: Into<LimboError>>(
        &mut self,
        mut step: impl FnMut() -> std::result::Result<IOResult<T>, E>,
    ) -> Result<T> {
        loop {
            match step().map_err(Into::into)? {
                IOResult::Done(value) => return Ok(value),
                IOResult::IO(io) => self.wait_for_io(io).await?,
            }
        }
    }

    pub(crate) async fn wait_for_io(&mut self, io: IOCompletions) -> Result<()> {
        self.yield_(Yield::from(io)).await
    }
}

pub(crate) enum CoroResume<Yield, Output> {
    Yielded(Yield),
    Completed(Output),
}

pub(crate) struct CoroRunner<Yield, Output> {
    bridge: Rc<Cell<Bridge<Yield>>>,
    future: Option<Pin<Box<dyn Future<Output = Output>>>>,
}

impl<Yield, Output> CoroRunner<Yield, Output> {
    pub(crate) fn new<F>(function: impl FnOnce(Coro<Yield>) -> F) -> Self
    where
        F: Future<Output = Output> + 'static,
    {
        let bridge = Rc::new(Cell::new(Bridge::Empty));
        let future = Box::pin(function(Coro {
            bridge: bridge.clone(),
        }));
        Self {
            bridge,
            future: Some(future),
        }
    }

    pub(crate) fn resume(&mut self) -> CoroResume<Yield, Output> {
        self.poll(Resume::Continue)
    }

    pub(crate) fn cancel(&mut self, err: LimboError) -> CoroResume<Yield, Output> {
        self.poll(Resume::Cancel(err))
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.future.is_none()
    }

    fn poll(&mut self, resume: Resume) -> CoroResume<Yield, Output> {
        let mut future = self
            .future
            .take()
            .expect("coroutine must not run after it returned");
        self.bridge.set(Bridge::Resumed(resume));
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(output) => CoroResume::Completed(output),
            Poll::Pending => {
                self.future = Some(future);
                match self.bridge.replace(Bridge::Empty) {
                    Bridge::Yielded(value) => CoroResume::Yielded(value),
                    Bridge::Empty | Bridge::Resumed(_) => {
                        panic!("coroutine must pause only through Coro::yield_")
                    }
                }
            }
        }
    }
}

enum Bridge<Yield> {
    Empty,
    Yielded(Yield),
    Resumed(Resume),
}

enum Resume {
    Continue,
    Cancel(LimboError),
}

struct Resumed<'a, Yield> {
    bridge: &'a Cell<Bridge<Yield>>,
}

impl<Yield> Future for Resumed<'_, Yield> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        match self.bridge.replace(Bridge::Empty) {
            Bridge::Resumed(Resume::Continue) => Poll::Ready(Ok(())),
            Bridge::Resumed(Resume::Cancel(err)) => Poll::Ready(Err(err)),
            not_resumed => {
                self.bridge.set(not_resumed);
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Coro, CoroResume, CoroRunner};
    use crate::types::{IOCompletions, IOResult};
    use crate::{Completion, LimboError, Result};

    enum Step {
        Value(u32),
        Io,
    }

    impl From<IOCompletions> for Step {
        fn from(_: IOCompletions) -> Self {
            Step::Io
        }
    }

    async fn yield_three_values(mut coro: Coro<Step>) -> Result<u32> {
        for value in 1..=3 {
            coro.yield_(Step::Value(value)).await?;
        }
        Ok(10)
    }

    fn yielded_value(resume: CoroResume<Step, Result<u32>>) -> u32 {
        match resume {
            CoroResume::Yielded(Step::Value(value)) => value,
            CoroResume::Yielded(Step::Io) => panic!("expected a value, got I/O"),
            CoroResume::Completed(_) => panic!("expected a value, got the return value"),
        }
    }

    #[test]
    fn resume_gives_each_yielded_value_then_the_return_value() {
        let mut runner = CoroRunner::new(yield_three_values);
        assert_eq!(yielded_value(runner.resume()), 1);
        assert_eq!(yielded_value(runner.resume()), 2);
        assert_eq!(yielded_value(runner.resume()), 3);
        assert!(!runner.is_finished());
        let CoroResume::Completed(Ok(10)) = runner.resume() else {
            panic!("the function must return after its last yield");
        };
        assert!(runner.is_finished());
    }

    #[test]
    fn cancel_makes_the_paused_yield_return_the_error() {
        let mut runner = CoroRunner::new(yield_three_values);
        assert_eq!(yielded_value(runner.resume()), 1);
        let CoroResume::Completed(Err(LimboError::Interrupt)) =
            runner.cancel(LimboError::Interrupt)
        else {
            panic!("the canceled yield must return the error and stop the function");
        };
        assert!(runner.is_finished());
    }

    #[test]
    fn cancel_before_start_runs_the_function_to_its_first_yield() {
        let mut runner = CoroRunner::new(yield_three_values);
        assert_eq!(yielded_value(runner.cancel(LimboError::Interrupt)), 1);
        let CoroResume::Completed(Err(LimboError::Interrupt)) =
            runner.cancel(LimboError::Interrupt)
        else {
            panic!("the second cancel must stop the function");
        };
    }

    #[test]
    fn runner_is_finished_after_the_function_panics() {
        let mut runner = CoroRunner::new(|mut coro: Coro<Step>| async move {
            coro.yield_(Step::Value(1)).await?;
            panic!("the function panics after its first yield");
            #[allow(unreachable_code)]
            Ok(0)
        });
        assert_eq!(yielded_value(runner.resume()), 1);
        let resume = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runner.resume()));
        assert!(resume.is_err(), "the panic must reach the caller");
        assert!(runner.is_finished());
    }

    #[test]
    fn run_io_pauses_for_each_io_and_returns_the_done_value() {
        let mut runner = CoroRunner::new(|mut coro: Coro<Step>| async move {
            let mut ios_left = 2;
            coro.run_io(|| -> Result<IOResult<u32>> {
                if ios_left == 0 {
                    return Ok(IOResult::Done(7));
                }
                ios_left -= 1;
                Ok(IOResult::IO(IOCompletions(Completion::new_yield())))
            })
            .await
        });
        assert!(matches!(runner.resume(), CoroResume::Yielded(Step::Io)));
        assert!(matches!(runner.resume(), CoroResume::Yielded(Step::Io)));
        let CoroResume::Completed(Ok(7)) = runner.resume() else {
            panic!("run_io must return the value of the step that is done");
        };
    }
}
