//! Runs an `async fn` as a step function that returns [`IOResult`].
//!
//! The async function does not run on an executor. The caller calls
//! [`Resumable::resume`] and passes the context for that one step. The async
//! function reads the context through [`Co::with`] and asks for I/O with
//! [`Co::io`] or [`Co::yield_io`]. A yield parks its completion in the
//! context and returns `Pending`, and `resume` hands the completion to its
//! caller as `IOResult::IO`. The next `resume` call continues the function
//! after the yield.
//!
//! A step that fails does not end the operation. [`Co::io`] parks the error
//! in the context and returns `Pending`, `resume` returns the error, and the
//! next `resume` call runs the same step again. This is the same as a
//! hand-written state machine that returns an error without a state change:
//! the caller decides whether to retry the step or to [`Resumable::cancel`]
//! the operation. Only an error that the async function returns itself ends
//! the operation.
//!
//! The future outlives every step, so it cannot borrow the context. The
//! runner stores a pointer to the context in a slot that it shares with the
//! handle, and only for the duration of one poll. The slot is the only place
//! where this module needs `unsafe`: the deref of that pointer.
//!
//! One [`Runner`] holds one async function at a time. The future lives in a
//! box that is allocated once and reused: a new operation builds its future
//! in place, so no allocation happens per operation.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::types::{IOCompletions, IOResult};

/// Names the context type of a step for every step lifetime. The future
/// names its context through this trait, so it does not carry a lifetime.
pub trait StepContext {
    /// The error type of the operations that run on this context.
    type Error;
    type Ctx<'a>: YieldSlot<Self::Error>;
}

/// Where a step parks what suspends it until `resume` picks it up: the
/// completion of an I/O yield, or the error of a step that failed.
pub trait YieldSlot<E> {
    fn park_io(&mut self, io: IOCompletions);
    fn take_io(&mut self) -> Option<IOCompletions>;
    fn park_err(&mut self, err: E);
    fn take_err(&mut self) -> Option<E>;
}

/// The handle an async function uses to reach its context and to yield.
pub struct Co<C> {
    /// Points to the context of the running step, null between steps.
    ctx: Arc<AtomicPtr<()>>,
    _family: PhantomData<C>,
}

impl<C: StepContext> Co<C> {
    /// Runs `f` with the context of the current step.
    ///
    /// `f` must accept the context for every lifetime, so the reference can
    /// never leave `f` and never lives across an await.
    #[inline(always)]
    pub fn with<R>(&mut self, f: impl for<'a> FnOnce(&mut C::Ctx<'a>) -> R) -> R {
        let ctx = self.ctx.load(Ordering::Relaxed).cast::<C::Ctx<'_>>();
        assert!(
            !ctx.is_null(),
            "context is only available while a step runs"
        );
        // SAFETY: `Runner::resume` stores the pointer from a `&mut C::Ctx`
        // that it holds for the whole step, and clears it before it returns.
        // The runner does not touch the context while it polls the future.
        // The future is the only owner of this handle, `&mut self` rules out
        // a nested call, and `f` cannot keep the reference. So this is the
        // only live reference to the context inside `f`.
        f(unsafe { &mut *ctx })
    }

    /// Calls `f` until it returns `Done`. Each `IO` result is yielded to the
    /// caller of `resume`, and `f` runs again after the I/O completes. An
    /// error is also yielded to the caller of `resume`, and `f` runs again
    /// on the next resume.
    #[inline(always)]
    pub fn io<T, E, F>(&mut self, f: F) -> Io<'_, C, F>
    where
        F: for<'a> FnMut(&mut C::Ctx<'a>) -> Result<IOResult<T>, E>,
        E: Into<C::Error>,
    {
        Io { co: self, f }
    }

    /// Hands `io` to the caller of `resume` and pauses until the next resume.
    #[cfg_attr(not(test), allow(dead_code))]
    #[inline(always)]
    pub fn yield_io(&mut self, io: IOCompletions) -> YieldIo<'_, C> {
        YieldIo {
            co: self,
            io: Some(io),
        }
    }
}

/// Future returned by [`Co::io`]: runs the step function on every poll.
pub struct Io<'co, C, F> {
    co: &'co mut Co<C>,
    f: F,
}

/// The step function is called in place and never pinned, so the future
/// can move.
impl<C, F> Unpin for Io<'_, C, F> {}

impl<C, F, T, E> Future for Io<'_, C, F>
where
    C: StepContext,
    F: for<'a> FnMut(&mut C::Ctx<'a>) -> Result<IOResult<T>, E>,
    E: Into<C::Error>,
{
    type Output = T;

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let Io { co, f } = &mut *self;
        co.with(|ctx| match f(ctx) {
            Ok(IOResult::Done(value)) => Poll::Ready(value),
            Ok(IOResult::IO(io)) => {
                ctx.park_io(io);
                Poll::Pending
            }
            Err(error) => {
                ctx.park_err(error.into());
                Poll::Pending
            }
        })
    }
}

/// Future returned by [`Co::yield_io`]: pending once, then ready.
pub struct YieldIo<'co, C> {
    co: &'co mut Co<C>,
    io: Option<IOCompletions>,
}

impl<C> Unpin for YieldIo<'_, C> {}

impl<C: StepContext> Future for YieldIo<'_, C> {
    type Output = ();

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        let YieldIo { co, io } = &mut *self;
        match io.take() {
            Some(io) => {
                co.with(|ctx| ctx.park_io(io));
                Poll::Pending
            }
            None => Poll::Ready(()),
        }
    }
}

/// A boxed [`Runner`] behind the [`Resumable`] trait.
pub type BoxedResumable<C, Args, Out> = Box<dyn Resumable<C, Args, Out> + Send + Sync>;

/// A step function built from an async function.
pub trait Resumable<C: StepContext, Args, Out> {
    /// True between the first `resume` and the one that returns `Done`, or
    /// the error that ends the operation.
    fn is_active(&self) -> bool;

    /// Starts a new operation with `args` if none is active, then runs it
    /// until it yields for I/O, fails a step, or finishes. `args` is ignored
    /// on a resume. After a failed step the operation stays active, and the
    /// next call runs that step again.
    fn resume(&mut self, ctx: &mut C::Ctx<'_>, args: Args) -> Result<IOResult<Out>, C::Error>;

    /// Drops the future of an active operation.
    fn cancel(&mut self);
}

/// Holds one async function, the handle it borrows, and the slot the two
/// share. Build it with [`Runner::boxed`] and keep the box for reuse.
pub struct Runner<C, F, M> {
    ctx: Arc<AtomicPtr<()>>,
    /// The handle between two operations. The future borrows it through
    /// [`with_handle`] and gives it back when it finishes.
    co: Option<Co<C>>,
    make: M,
    /// Stays in place after the operation finishes: dropping it there would
    /// copy the whole future, so `active` tracks the state instead.
    future: Pin<Box<Option<F>>>,
    active: bool,
}

impl<C, F, M> Runner<C, F, M> {
    /// Boxes a new runner behind the `Resumable` trait. `make` builds the
    /// future of one operation, usually through [`with_handle`].
    pub fn boxed<Args, Out>(make: M) -> BoxedResumable<C, Args, Out>
    where
        C: StepContext,
        F: Future<Output = (Co<C>, Result<Out, C::Error>)>,
        M: Fn(Co<C>, Args) -> F,
        Self: Send + Sync + 'static,
    {
        Box::new(Self {
            ctx: Arc::new(AtomicPtr::new(ptr::null_mut())),
            co: None,
            make,
            future: Box::pin(None),
            active: false,
        })
    }
}

impl<C, Args, Out, F, M> Resumable<C, Args, Out> for Runner<C, F, M>
where
    C: StepContext,
    F: Future<Output = (Co<C>, Result<Out, C::Error>)>,
    M: Fn(Co<C>, Args) -> F,
{
    #[inline(always)]
    fn is_active(&self) -> bool {
        self.active
    }

    fn resume(&mut self, ctx: &mut C::Ctx<'_>, args: Args) -> Result<IOResult<Out>, C::Error> {
        if !self.active {
            let co = self.co.take().unwrap_or_else(|| Co {
                ctx: Arc::clone(&self.ctx),
                _family: PhantomData,
            });
            self.future.as_mut().set(Some((self.make)(co, args)));
            self.active = true;
        }
        let future = self
            .future
            .as_mut()
            .as_pin_mut()
            .expect("an active runner holds a future");
        self.ctx
            .store(ptr::from_mut(ctx).cast::<()>(), Ordering::Relaxed);
        let polled = future.poll(&mut Context::from_waker(Waker::noop()));
        self.ctx.store(ptr::null_mut(), Ordering::Relaxed);
        match polled {
            Poll::Ready((co, result)) => {
                self.co = Some(co);
                self.active = false;
                result.map(IOResult::Done)
            }
            Poll::Pending => {
                if let Some(err) = ctx.take_err() {
                    return Err(err);
                }
                let io = ctx
                    .take_io()
                    .expect("future returned Pending without an I/O yield or a step error");
                Ok(IOResult::IO(io))
            }
        }
    }

    #[inline(always)]
    fn cancel(&mut self) {
        self.future.as_mut().set(None);
        self.active = false;
    }
}

/// Runs `body` with the handle borrowed, then gives the handle back to the
/// runner. A borrowed handle cannot leave `body`, and the runner reuses it
/// for the next operation.
pub async fn with_handle<C, Args, Out, B>(
    mut co: Co<C>,
    args: Args,
    body: B,
) -> (Co<C>, Result<Out, C::Error>)
where
    C: StepContext,
    B: AsyncFnOnce(&mut Co<C>, Args) -> Result<Out, C::Error>,
{
    let result = body(&mut co, args).await;
    (co, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{Completion, CompletionType};
    use crate::types::IOCompletions;

    struct Counting;

    impl StepContext for Counting {
        type Error = &'static str;
        type Ctx<'a> = Counter;
    }

    struct Counter {
        steps: usize,
        io: Option<IOCompletions>,
        err: Option<&'static str>,
    }

    impl Counter {
        fn new(steps: usize) -> Self {
            Self {
                steps,
                io: None,
                err: None,
            }
        }
    }

    impl YieldSlot<&'static str> for Counter {
        fn park_io(&mut self, io: IOCompletions) {
            assert!(self.io.is_none(), "a step parks at most one completion");
            self.io = Some(io);
        }

        fn take_io(&mut self) -> Option<IOCompletions> {
            self.io.take()
        }

        fn park_err(&mut self, err: &'static str) {
            assert!(self.err.is_none(), "a step parks at most one error");
            self.err = Some(err);
        }

        fn take_err(&mut self) -> Option<&'static str> {
            self.err.take()
        }
    }

    fn completion() -> IOCompletions {
        IOCompletions(Completion::new(CompletionType::Yield))
    }

    async fn count_to(co: &mut Co<Counting>, target: usize) -> Result<usize, &'static str> {
        let mut yields = 0;
        while co.with(|counter| counter.steps) < target {
            co.with(|counter| counter.steps += 1);
            co.yield_io(completion()).await;
            yields += 1;
        }
        Ok(yields)
    }

    async fn fail_after_one_yield(co: &mut Co<Counting>, _: usize) -> Result<usize, &'static str> {
        co.yield_io(completion()).await;
        co.with(|counter| counter.steps += 1);
        Err("the function failed")
    }

    /// The step fails until the counter reaches `target`, and counts one
    /// step per call.
    async fn step_until(co: &mut Co<Counting>, target: usize) -> Result<usize, &'static str> {
        let reached = co
            .io(|counter: &mut Counter| {
                counter.steps += 1;
                if counter.steps < target {
                    Err("not yet")
                } else {
                    Ok(IOResult::Done(counter.steps))
                }
            })
            .await;
        Ok(reached)
    }

    #[test]
    fn yields_once_per_step_until_done() {
        let mut runner = Runner::boxed(|co, args| with_handle(co, args, count_to));
        let mut counter = Counter::new(0);
        assert!(!runner.is_active());
        for expected in 1..=3 {
            assert!(matches!(
                runner.resume(&mut counter, 3),
                Ok(IOResult::IO(_))
            ));
            assert!(runner.is_active());
            assert_eq!(counter.steps, expected);
        }
        assert!(matches!(
            runner.resume(&mut counter, 3),
            Ok(IOResult::Done(3))
        ));
        assert!(!runner.is_active());
    }

    #[test]
    fn runner_is_reused_for_the_next_operation() {
        let mut runner = Runner::boxed(|co, args| with_handle(co, args, count_to));
        let mut counter = Counter::new(0);
        assert!(matches!(
            runner.resume(&mut counter, 0),
            Ok(IOResult::Done(0))
        ));
        assert!(matches!(
            runner.resume(&mut counter, 1),
            Ok(IOResult::IO(_))
        ));
        let mut other = Counter::new(5);
        assert!(matches!(
            runner.resume(&mut other, 99),
            Ok(IOResult::Done(1))
        ));
        assert_eq!(counter.steps, 1);
        assert_eq!(other.steps, 5);
    }

    #[test]
    fn error_of_the_function_ends_the_operation() {
        let mut runner = Runner::boxed(|co, args| with_handle(co, args, fail_after_one_yield));
        let mut counter = Counter::new(0);
        assert!(matches!(
            runner.resume(&mut counter, 0),
            Ok(IOResult::IO(_))
        ));
        assert!(matches!(
            runner.resume(&mut counter, 0),
            Err("the function failed")
        ));
        assert!(!runner.is_active());
        assert_eq!(counter.steps, 1);
    }

    #[test]
    fn error_of_a_step_suspends_the_operation_and_the_step_runs_again() {
        let mut runner = Runner::boxed(|co, args| with_handle(co, args, step_until));
        let mut counter = Counter::new(0);
        assert!(matches!(runner.resume(&mut counter, 3), Err("not yet")));
        assert!(runner.is_active());
        assert!(matches!(runner.resume(&mut counter, 3), Err("not yet")));
        assert!(runner.is_active());
        assert!(matches!(
            runner.resume(&mut counter, 3),
            Ok(IOResult::Done(3))
        ));
        assert!(!runner.is_active());
        assert_eq!(counter.steps, 3);
        assert!(counter.err.is_none());
    }

    #[test]
    fn cancel_drops_a_suspended_operation() {
        let mut runner = Runner::boxed(|co, args| with_handle(co, args, count_to));
        let mut counter = Counter::new(0);
        assert!(matches!(
            runner.resume(&mut counter, 2),
            Ok(IOResult::IO(_))
        ));
        runner.cancel();
        assert!(!runner.is_active());
        assert!(matches!(
            runner.resume(&mut counter, 0),
            Ok(IOResult::Done(0))
        ));
    }

    #[test]
    fn cancel_drops_an_operation_suspended_by_a_failed_step() {
        let mut runner = Runner::boxed(|co, args| with_handle(co, args, step_until));
        let mut counter = Counter::new(0);
        assert!(matches!(runner.resume(&mut counter, 3), Err("not yet")));
        runner.cancel();
        assert!(!runner.is_active());
        assert!(matches!(
            runner.resume(&mut counter, 2),
            Ok(IOResult::Done(2))
        ));
    }

    struct Borrowing;

    impl StepContext for Borrowing {
        type Error = &'static str;
        type Ctx<'a> = Borrowed<'a>;
    }

    /// A context that borrows its data for one step only, like the VDBE one.
    struct Borrowed<'a> {
        steps: &'a mut usize,
        io: &'a mut Option<IOCompletions>,
        err: Option<&'static str>,
    }

    impl YieldSlot<&'static str> for Borrowed<'_> {
        fn park_io(&mut self, io: IOCompletions) {
            *self.io = Some(io);
        }

        fn take_io(&mut self) -> Option<IOCompletions> {
            self.io.take()
        }

        fn park_err(&mut self, err: &'static str) {
            self.err = Some(err);
        }

        fn take_err(&mut self) -> Option<&'static str> {
            self.err.take()
        }
    }

    async fn count_borrowed(co: &mut Co<Borrowing>, target: usize) -> Result<usize, &'static str> {
        let mut yields = 0;
        while co.with(|ctx| *ctx.steps) < target {
            co.with(|ctx| *ctx.steps += 1);
            co.yield_io(completion()).await;
            yields += 1;
        }
        Ok(yields)
    }

    #[test]
    fn each_step_gets_its_own_borrowed_context() {
        let mut runner = Runner::boxed(|co, args| with_handle(co, args, count_borrowed));
        let mut first = 0;
        let mut second = 1;
        let mut io = None;
        assert!(matches!(
            runner.resume(
                &mut Borrowed {
                    steps: &mut first,
                    io: &mut io,
                    err: None,
                },
                2
            ),
            Ok(IOResult::IO(_))
        ));
        assert_eq!(first, 1);
        assert!(matches!(
            runner.resume(
                &mut Borrowed {
                    steps: &mut second,
                    io: &mut io,
                    err: None,
                },
                2
            ),
            Ok(IOResult::IO(_))
        ));
        assert_eq!(second, 2);
        assert!(matches!(
            runner.resume(
                &mut Borrowed {
                    steps: &mut second,
                    io: &mut io,
                    err: None,
                },
                2
            ),
            Ok(IOResult::Done(2))
        ));
        assert!(io.is_none());
    }
}
