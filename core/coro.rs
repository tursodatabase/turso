//! Runs an `async fn` as a step function that returns [`IOResult`].
//!
//! The async function does not run on an executor. The caller calls
//! [`Resumable::step`] and passes the context for that one step. The async
//! function reads the context through [`Co::with`] and asks for I/O with
//! [`Co::io`] or [`Co::yield_io`]. A yield returns the completion to the
//! caller as `IOResult::IO`. The next `step` call resumes the function after
//! the yield.
//!
//! One [`Runner`] holds one async function at a time. A runner is boxed and
//! pinned once, then reused: `start` builds a new future in place, so no
//! allocation happens per operation.

use std::cell::Cell;
use std::future::Future;
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

use crate::types::{IOCompletions, IOResult};

/// The handle an async function uses to reach its context and to yield.
pub struct Co<Ctx> {
    slot: NonNull<Slot<Ctx>>,
    _ctx: PhantomData<*mut Ctx>,
}

// SAFETY: the handle only points into the runner that owns its future, and
// the two always move together. All access goes through `&mut self`.
unsafe impl<Ctx> Send for Co<Ctx> {}
unsafe impl<Ctx> Sync for Co<Ctx> {}

impl<Ctx> Co<Ctx> {
    /// Runs `f` with the context of the current step.
    ///
    /// The reference is valid only inside `f`, so it can never live across
    /// an await.
    #[inline(always)]
    pub fn with<R>(&mut self, f: impl FnOnce(&mut Ctx) -> R) -> R {
        // SAFETY: the slot lives in the pinned runner that owns this future,
        // and the runner sets the pointer only for the duration of one poll.
        let slot = unsafe { self.slot.as_ref() };
        let mut ctx = slot
            .ctx
            .get()
            .expect("context is only available while a step runs");
        // SAFETY: the runner holds `&mut Ctx` for the whole step and does not
        // touch it while the future runs. `&mut self` prevents a nested call.
        f(unsafe { ctx.as_mut() })
    }

    /// Calls `f` until it returns `Done`. Each `IO` result is yielded to the
    /// caller of `step`, and `f` runs again after the I/O completes.
    #[inline(always)]
    pub fn io<T, E, F>(&mut self, f: F) -> Io<'_, Ctx, F>
    where
        F: FnMut(&mut Ctx) -> Result<IOResult<T>, E>,
    {
        Io { co: self, f }
    }

    /// Hands `io` to the caller of `resume` and pauses until the next resume.
    #[cfg_attr(not(test), allow(dead_code))]
    #[inline(always)]
    pub fn yield_io(&mut self, io: IOCompletions) -> YieldIo<'_, Ctx> {
        YieldIo {
            co: self,
            io: Some(io),
        }
    }
}

/// Future returned by [`Co::io`]: runs the step function on every poll.
pub struct Io<'a, Ctx, F> {
    co: &'a mut Co<Ctx>,
    f: F,
}

impl<Ctx, F, T, E> Future for Io<'_, Ctx, F>
where
    F: FnMut(&mut Ctx) -> Result<IOResult<T>, E>,
{
    type Output = Result<T, E>;

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `f` is called in place and never moved out.
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: see `Co::with`.
        let slot = unsafe { this.co.slot.as_ref() };
        let mut ctx = slot
            .ctx
            .get()
            .expect("context is only available while a step runs");
        // SAFETY: see `Co::with`.
        match (this.f)(unsafe { ctx.as_mut() }) {
            Ok(IOResult::Done(value)) => Poll::Ready(Ok(value)),
            Ok(IOResult::IO(io)) => {
                slot.io.set(Some(io));
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

/// Future returned by [`Co::yield_io`]: pending once, then ready.
pub struct YieldIo<'a, Ctx> {
    co: &'a mut Co<Ctx>,
    io: Option<IOCompletions>,
}

impl<Ctx> Unpin for YieldIo<'_, Ctx> {}

impl<Ctx> Future for YieldIo<'_, Ctx> {
    type Output = ();

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        match self.io.take() {
            Some(io) => {
                // SAFETY: see `Co::with`.
                let slot = unsafe { self.co.slot.as_ref() };
                slot.io.set(Some(io));
                Poll::Pending
            }
            None => Poll::Ready(()),
        }
    }
}

struct Slot<Ctx> {
    ctx: Cell<Option<NonNull<Ctx>>>,
    io: Cell<Option<IOCompletions>>,
}

// SAFETY: the slot is only touched through `Pin<&mut Runner>`, either by
// `step` itself or by the future that `step` polls.
unsafe impl<Ctx> Send for Slot<Ctx> {}
unsafe impl<Ctx> Sync for Slot<Ctx> {}

/// A pinned, boxed [`Runner`] behind the [`Resumable`] trait.
pub type BoxedResumable<Ctx, Args, Out, E> =
    Pin<Box<dyn Resumable<Ctx, Args, Out, E> + Send + Sync>>;

/// A step function built from an async function.
pub trait Resumable<Ctx, Args, Out, E> {
    /// True between the first `resume` and the one that returns `Done` or an
    /// error.
    fn is_active(&self) -> bool;

    /// Starts a new operation with `args` if none is active, then runs it
    /// until it yields for I/O or finishes. `args` is ignored on a resume.
    fn resume(self: Pin<&mut Self>, ctx: &mut Ctx, args: Args) -> Result<IOResult<Out>, E>;

    /// Drops the future of an active operation.
    fn cancel(self: Pin<&mut Self>);
}

/// Holds one async function and the slot it talks to. Create it with
/// [`Runner::new`], pin it in a box, and keep the box for reuse.
pub struct Runner<Ctx, Args, F, M> {
    slot: Slot<Ctx>,
    make: M,
    /// Stays in place after the operation finishes: dropping it there would
    /// copy the whole future, so `active` tracks the state instead.
    future: Option<F>,
    active: bool,
    _args: PhantomData<fn(Args)>,
    _pin: PhantomPinned,
}

impl<Ctx, Args, Out, E, F, M> Runner<Ctx, Args, F, M>
where
    F: Future<Output = Result<Out, E>>,
    M: Fn(Co<Ctx>, Args) -> F,
{
    pub fn new(make: M) -> Self {
        Self {
            slot: Slot {
                ctx: Cell::new(None),
                io: Cell::new(None),
            },
            make,
            future: None,
            active: false,
            _args: PhantomData,
            _pin: PhantomPinned,
        }
    }

    /// Boxes and pins a new runner behind the `Resumable` trait.
    pub fn boxed(make: M) -> BoxedResumable<Ctx, Args, Out, E>
    where
        Self: Send + Sync + 'static,
    {
        Box::pin(Self::new(make))
    }
}

impl<Ctx, Args, Out, E, F, M> Resumable<Ctx, Args, Out, E> for Runner<Ctx, Args, F, M>
where
    F: Future<Output = Result<Out, E>>,
    M: Fn(Co<Ctx>, Args) -> F,
{
    #[inline(always)]
    fn is_active(&self) -> bool {
        self.active
    }

    fn resume(self: Pin<&mut Self>, ctx: &mut Ctx, args: Args) -> Result<IOResult<Out>, E> {
        // SAFETY: `future` is replaced in place, only polled through `Pin`,
        // and never moved out.
        let this = unsafe { self.get_unchecked_mut() };
        if !this.active {
            let co = Co {
                slot: NonNull::from(&this.slot),
                _ctx: PhantomData,
            };
            this.future = Some((this.make)(co, args));
            this.active = true;
        }
        let future = this
            .future
            .as_mut()
            .expect("an active runner holds a future");
        // SAFETY: the runner is pinned, so `future` never moves.
        let future = unsafe { Pin::new_unchecked(future) };
        this.slot.ctx.set(Some(NonNull::from(ctx)));
        let polled = future.poll(&mut Context::from_waker(Waker::noop()));
        this.slot.ctx.set(None);
        match polled {
            Poll::Ready(result) => {
                this.active = false;
                result.map(IOResult::Done)
            }
            Poll::Pending => {
                let io = this
                    .slot
                    .io
                    .take()
                    .expect("future returned Pending without an I/O yield");
                Ok(IOResult::IO(io))
            }
        }
    }

    #[inline(always)]
    fn cancel(self: Pin<&mut Self>) {
        // SAFETY: the future is dropped in place.
        let this = unsafe { self.get_unchecked_mut() };
        this.future = None;
        this.active = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{Completion, CompletionType};
    use crate::types::IOCompletions;

    struct Counter {
        steps: usize,
    }

    fn completion() -> IOCompletions {
        IOCompletions(Completion::new(CompletionType::Yield))
    }

    async fn count_to(mut co: Co<Counter>, target: usize) -> Result<usize, ()> {
        let mut yields = 0;
        while co.with(|counter| counter.steps) < target {
            co.with(|counter| counter.steps += 1);
            co.yield_io(completion()).await;
            yields += 1;
        }
        Ok(yields)
    }

    async fn fail_after_one_yield(mut co: Co<Counter>, _: usize) -> Result<usize, ()> {
        co.yield_io(completion()).await;
        co.with(|counter| counter.steps += 1);
        Err(())
    }

    #[test]
    fn yields_once_per_step_until_done() {
        let mut runner = Runner::boxed(count_to);
        let mut counter = Counter { steps: 0 };
        assert!(!runner.is_active());
        for expected in 1..=3 {
            assert!(matches!(
                runner.as_mut().resume(&mut counter, 3),
                Ok(IOResult::IO(_))
            ));
            assert!(runner.is_active());
            assert_eq!(counter.steps, expected);
        }
        assert!(matches!(
            runner.as_mut().resume(&mut counter, 3),
            Ok(IOResult::Done(3))
        ));
        assert!(!runner.is_active());
    }

    #[test]
    fn runner_is_reused_for_the_next_operation() {
        let mut runner = Runner::boxed(count_to);
        let mut counter = Counter { steps: 0 };
        assert!(matches!(
            runner.as_mut().resume(&mut counter, 0),
            Ok(IOResult::Done(0))
        ));
        assert!(matches!(
            runner.as_mut().resume(&mut counter, 1),
            Ok(IOResult::IO(_))
        ));
        let mut other = Counter { steps: 5 };
        assert!(matches!(
            runner.as_mut().resume(&mut other, 99),
            Ok(IOResult::Done(1))
        ));
        assert_eq!(counter.steps, 1);
        assert_eq!(other.steps, 5);
    }

    #[test]
    fn error_ends_the_operation() {
        let mut runner = Runner::boxed(fail_after_one_yield);
        let mut counter = Counter { steps: 0 };
        assert!(matches!(
            runner.as_mut().resume(&mut counter, 0),
            Ok(IOResult::IO(_))
        ));
        assert!(matches!(runner.as_mut().resume(&mut counter, 0), Err(())));
        assert!(!runner.is_active());
        assert_eq!(counter.steps, 1);
    }

    #[test]
    fn cancel_drops_a_suspended_operation() {
        let mut runner = Runner::boxed(count_to);
        let mut counter = Counter { steps: 0 };
        assert!(matches!(
            runner.as_mut().resume(&mut counter, 2),
            Ok(IOResult::IO(_))
        ));
        runner.as_mut().cancel();
        assert!(!runner.is_active());
        assert!(matches!(
            runner.as_mut().resume(&mut counter, 0),
            Ok(IOResult::Done(0))
        ));
    }
}
