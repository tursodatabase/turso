//! Runs an `async fn` as a step function.
//!
//! The async function does not run on an executor. The caller calls
//! [`Resumable::resume`] and passes the context for that one step. The async
//! function reads the context through [`Co::with`], asks for I/O with
//! [`Co::io`], and pauses with [`Co::pause`]. A pause makes `resume` return
//! `Pending`, and the caller reads from its own context why the function
//! paused. The next `resume` call continues the function after the pause.
//!
//! The future outlives every step, so it cannot borrow the context. The
//! runner stores a pointer to the context in a slot that it shares with the
//! handle, and only for the duration of one poll. The slot is the only place
//! where this module needs `unsafe`: the deref of that pointer.
//!
//! One [`Runner`] holds one async function at a time. The future lives in a
//! box that is allocated once and reused: a new operation builds its future
//! in place, so no allocation happens per operation. Each operation owns
//! its handle, which shares the slot of the runner.

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
    type Ctx<'a>: YieldSlot;
}

/// Where a step parks the completion of an I/O yield.
pub trait YieldSlot {
    fn park_io(&mut self, io: IOCompletions);
}

/// The handle an async function uses to reach its context and to pause.
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

    /// Calls `f` until it returns `Done`. Each `IO` result is parked in the
    /// context and pauses the function, and `f` runs again after the resume.
    #[inline(always)]
    pub fn io<T, E, F>(&mut self, f: F) -> Io<'_, C, F>
    where
        F: for<'a> FnMut(&mut C::Ctx<'a>) -> Result<IOResult<T>, E>,
    {
        Io { co: self, f }
    }

    /// Pauses the function until the next `resume`.
    #[inline(always)]
    pub fn pause(&mut self) -> Pause {
        Pause { paused: false }
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
{
    type Output = Result<T, E>;

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let Io { co, f } = &mut *self;
        co.with(|ctx| match f(ctx) {
            Ok(IOResult::Done(value)) => Poll::Ready(Ok(value)),
            Ok(IOResult::IO(io)) => {
                ctx.park_io(io);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        })
    }
}

/// Future returned by [`Co::pause`]: pending once, then ready.
pub struct Pause {
    paused: bool,
}

impl Future for Pause {
    type Output = ();

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        if self.paused {
            Poll::Ready(())
        } else {
            self.paused = true;
            Poll::Pending
        }
    }
}

/// A boxed [`Runner`] behind the [`Resumable`] trait.
pub type BoxedResumable<C, Args, Out> = Box<dyn Resumable<C, Args, Out> + Send + Sync>;

/// A step function built from an async function.
pub trait Resumable<C: StepContext, Args, Out> {
    /// True between the first `resume` and the one that returns `Ready`.
    #[cfg_attr(not(test), allow(dead_code))]
    fn is_active(&self) -> bool;

    /// Starts a new operation with `args` if none is active, then runs it
    /// until it pauses or finishes. `args` is ignored on a resume.
    fn resume(&mut self, ctx: &mut C::Ctx<'_>, args: Args) -> Poll<Out>;

    /// Drops the future of an active operation.
    fn cancel(&mut self);
}

/// Holds one async function and the slot it shares with the handle of the
/// function. Build it with [`Runner::boxed`] and keep the box for reuse.
pub struct Runner<C, F, M> {
    ctx: Arc<AtomicPtr<()>>,
    make: M,
    _family: PhantomData<C>,
    /// Stays in place after the operation finishes: dropping it there would
    /// copy the whole future, so `active` tracks the state instead.
    future: Pin<Box<Option<F>>>,
    active: bool,
}

impl<C, F, M> Runner<C, F, M> {
    /// Boxes a new runner behind the `Resumable` trait. `make` builds the
    /// future of one operation from its handle and arguments; an `async fn`
    /// with that signature fits.
    pub fn boxed<Args, Out>(make: M) -> BoxedResumable<C, Args, Out>
    where
        C: StepContext,
        F: Future<Output = Out>,
        M: Fn(Co<C>, Args) -> F,
        Self: Send + Sync + 'static,
    {
        Box::new(Self {
            ctx: Arc::new(AtomicPtr::new(ptr::null_mut())),
            make,
            _family: PhantomData,
            future: Box::pin(None),
            active: false,
        })
    }
}

impl<C, Args, Out, F, M> Resumable<C, Args, Out> for Runner<C, F, M>
where
    C: StepContext,
    F: Future<Output = Out>,
    M: Fn(Co<C>, Args) -> F,
{
    #[inline(always)]
    fn is_active(&self) -> bool {
        self.active
    }

    fn resume(&mut self, ctx: &mut C::Ctx<'_>, args: Args) -> Poll<Out> {
        if !self.active {
            let co = Co {
                ctx: Arc::clone(&self.ctx),
                _family: PhantomData,
            };
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
        if polled.is_ready() {
            self.active = false;
        }
        polled
    }

    #[inline(always)]
    fn cancel(&mut self) {
        self.future.as_mut().set(None);
        self.active = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{Completion, CompletionType};
    use crate::types::IOCompletions;

    struct Counting;

    impl StepContext for Counting {
        type Ctx<'a> = Counter;
    }

    struct Counter {
        steps: usize,
        io: Option<IOCompletions>,
    }

    impl Counter {
        fn new(steps: usize) -> Self {
            Self { steps, io: None }
        }
    }

    impl YieldSlot for Counter {
        fn park_io(&mut self, io: IOCompletions) {
            assert!(self.io.is_none(), "a step parks at most one completion");
            self.io = Some(io);
        }
    }

    fn completion() -> IOCompletions {
        IOCompletions(Completion::new(CompletionType::Yield))
    }

    fn yields(polled: Poll<Result<usize, ()>>, counter: &mut Counter) -> bool {
        polled.is_pending() && counter.io.take().is_some()
    }

    async fn count_to(mut co: Co<Counting>, target: usize) -> Result<usize, ()> {
        let mut yields = 0;
        while co.with(|counter| counter.steps) < target {
            co.with(|counter| {
                counter.steps += 1;
                counter.park_io(completion());
            });
            co.pause().await;
            yields += 1;
        }
        Ok(yields)
    }

    async fn fail_after_one_pause(mut co: Co<Counting>, _: usize) -> Result<usize, ()> {
        co.pause().await;
        co.with(|counter| counter.steps += 1);
        Err(())
    }

    #[test]
    fn pauses_once_per_step_until_done() {
        let mut runner = Runner::boxed(count_to);
        let mut counter = Counter::new(0);
        assert!(!runner.is_active());
        for expected in 1..=3 {
            assert!(yields(runner.resume(&mut counter, 3), &mut counter));
            assert!(runner.is_active());
            assert_eq!(counter.steps, expected);
        }
        assert!(matches!(runner.resume(&mut counter, 3), Poll::Ready(Ok(3))));
        assert!(!runner.is_active());
    }

    #[test]
    fn runner_is_reused_for_the_next_operation() {
        let mut runner = Runner::boxed(count_to);
        let mut counter = Counter::new(0);
        assert!(matches!(runner.resume(&mut counter, 0), Poll::Ready(Ok(0))));
        assert!(yields(runner.resume(&mut counter, 1), &mut counter));
        let mut other = Counter::new(5);
        assert!(matches!(runner.resume(&mut other, 99), Poll::Ready(Ok(1))));
        assert_eq!(counter.steps, 1);
        assert_eq!(other.steps, 5);
    }

    #[test]
    fn a_pause_without_io_is_pending_once() {
        let mut runner = Runner::boxed(fail_after_one_pause);
        let mut counter = Counter::new(0);
        assert!(runner.resume(&mut counter, 0).is_pending());
        assert!(counter.io.is_none());
        assert!(runner.is_active());
        assert!(matches!(
            runner.resume(&mut counter, 0),
            Poll::Ready(Err(()))
        ));
        assert!(!runner.is_active());
        assert_eq!(counter.steps, 1);
    }

    #[test]
    fn cancel_drops_a_paused_operation() {
        let mut runner = Runner::boxed(count_to);
        let mut counter = Counter::new(0);
        assert!(yields(runner.resume(&mut counter, 2), &mut counter));
        runner.cancel();
        assert!(!runner.is_active());
        assert!(matches!(runner.resume(&mut counter, 0), Poll::Ready(Ok(0))));
    }

    struct Borrowing;

    impl StepContext for Borrowing {
        type Ctx<'a> = Borrowed<'a>;
    }

    /// A context that borrows its data for one step only, like the VDBE one.
    struct Borrowed<'a> {
        steps: &'a mut usize,
        io: &'a mut Option<IOCompletions>,
    }

    impl YieldSlot for Borrowed<'_> {
        fn park_io(&mut self, io: IOCompletions) {
            *self.io = Some(io);
        }
    }

    fn step_of(
        steps: &mut usize,
    ) -> impl for<'a> FnMut(&mut Borrowed<'a>) -> Result<IOResult<usize>, ()> + '_ {
        move |ctx| {
            if *ctx.steps < *steps {
                *ctx.steps += 1;
                Ok(IOResult::IO(completion()))
            } else {
                Ok(IOResult::Done(*ctx.steps))
            }
        }
    }

    async fn count_borrowed(mut co: Co<Borrowing>, mut target: usize) -> Result<usize, ()> {
        co.io(step_of(&mut target)).await
    }

    #[test]
    fn each_step_gets_its_own_borrowed_context() {
        let mut runner = Runner::boxed(count_borrowed);
        let mut first = 0;
        let mut second = 1;
        let mut io = None;
        assert!(runner
            .resume(
                &mut Borrowed {
                    steps: &mut first,
                    io: &mut io
                },
                2
            )
            .is_pending());
        assert_eq!(first, 1);
        assert!(io.take().is_some());
        assert!(runner
            .resume(
                &mut Borrowed {
                    steps: &mut second,
                    io: &mut io
                },
                2
            )
            .is_pending());
        assert_eq!(second, 2);
        assert!(io.take().is_some());
        assert!(matches!(
            runner.resume(
                &mut Borrowed {
                    steps: &mut second,
                    io: &mut io
                },
                2
            ),
            Poll::Ready(Ok(2))
        ));
        assert!(io.is_none());
    }
}
