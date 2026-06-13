#[cfg(shuttle)]
use shuttle::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(shuttle)]
use shuttle::sync::{Condvar, Mutex};
use std::future::Future;
use std::sync::Arc;

#[cfg(not(shuttle))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(shuttle))]
use std::sync::Mutex;
#[cfg(not(shuttle))]
use tokio::sync::Notify;

/// A barrier that starts in the down position, comes up when synchronization is requested,
/// and then goes back down until the next synchronization request.
#[derive(Debug)]
pub struct RequestableSync {
    closed: AtomicBool,

    requested: AtomicU64,
    completed: AtomicU64,

    arrive: CloseableBarrier,
    done: CloseableBarrier,
    leave: CloseableBarrier,
}

impl RequestableSync {
    pub fn new(participants: usize) -> Self {
        Self {
            closed: AtomicBool::new(false),
            requested: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            arrive: CloseableBarrier::new(participants),
            done: CloseableBarrier::new(participants),
            leave: CloseableBarrier::new(participants),
        }
    }

    /// Returns `true` if the synchronization could be requested.
    pub fn request_synchronization(&self) -> Result<bool, Closed> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(Closed);
            }

            let requested = self.requested.load(Ordering::Acquire);
            let completed = self.completed.load(Ordering::Acquire);

            if requested != completed {
                return Ok(false);
            }

            let next = requested.checked_add(1).unwrap();

            match self.requested.compare_exchange(
                requested,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(true),
                Err(_) => continue,
            }
        }
    }

    /// Synchronizes with other threads if synchronization was requested via
    /// `request_synchronization`, in which case the following steps happen in sequence:
    ///
    /// 1. Wait on a barrier
    /// 2. Execute `action`
    /// 3. Wait on a second barrier
    ///
    /// This ensures that
    ///
    /// 1. No thread starts executing `action` before all participating threads have entered
    ///    `maybe_rendezvous_and_run`, and
    /// 2. No thread exits `maybe_rendezvous_and_run` before all participating threads have
    ///    finished executing `action`.
    pub async fn maybe_rendezvous_and_run<F, Fut>(&self, action: F) -> Result<(), Closed>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        let target = self.requested.load(Ordering::Acquire);

        if target == self.completed.load(Ordering::Acquire) {
            return Ok(());
        }

        self.arrive.wait().await?;

        action().await;

        let done = self.done.wait().await?;
        if done.is_leader() {
            self.completed.store(target, Ordering::Release);
        }

        self.leave.wait().await?;

        Ok(())
    }

    /// Signal all waiters to stop waiting on the barrier.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);

        self.arrive.close();
        self.done.close();
        self.leave.close();
    }

    /// Returns a guard than, when dropped, will signal all waiters to stop waiting on the barrier.
    #[must_use]
    pub fn close_guard(self: Arc<Self>) -> impl Drop {
        CloseGuard(self)
    }
}

struct CloseGuard(Arc<RequestableSync>);

impl Drop for CloseGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BarrierWaitResult {
    leader: bool,
}

impl BarrierWaitResult {
    fn is_leader(&self) -> bool {
        self.leader
    }
}

#[derive(Debug)]
struct BarrierState {
    generation: u64,
    arrived: usize,
    closed: bool,
}

#[derive(Debug)]
struct CloseableBarrier {
    participants: usize,
    state: Mutex<BarrierState>,
    #[cfg(shuttle)]
    condvar: Condvar,
    #[cfg(not(shuttle))]
    notify: Notify,
}

impl CloseableBarrier {
    fn new(participants: usize) -> Self {
        assert!(participants > 0);

        Self {
            participants,
            state: Mutex::new(BarrierState {
                generation: 0,
                arrived: 0,
                closed: false,
            }),
            #[cfg(shuttle)]
            condvar: Condvar::new(),
            #[cfg(not(shuttle))]
            notify: Notify::new(),
        }
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap();

        if state.closed {
            return;
        }

        state.closed = true;
        state.arrived = 0;
        state.generation = state.generation.wrapping_add(1);

        #[cfg(shuttle)]
        self.condvar.notify_all();
        #[cfg(not(shuttle))]
        self.notify.notify_waiters();
    }

    #[cfg(shuttle)]
    async fn wait(&self) -> Result<BarrierWaitResult, Closed> {
        let mut state = self.state.lock().unwrap();

        if state.closed {
            return Err(Closed);
        }

        let my_generation = state.generation;

        state.arrived += 1;

        if state.arrived == self.participants {
            state.arrived = 0;
            state.generation = state.generation.wrapping_add(1);
            self.condvar.notify_all();

            return Ok(BarrierWaitResult { leader: true });
        }

        loop {
            state = self.condvar.wait(state).unwrap();

            if state.closed {
                return Err(Closed);
            }

            if state.generation != my_generation {
                return Ok(BarrierWaitResult { leader: false });
            }
        }
    }

    #[cfg(not(shuttle))]
    async fn wait(&self) -> Result<BarrierWaitResult, Closed> {
        let my_generation = {
            let mut state = self.state.lock().unwrap();

            if state.closed {
                return Err(Closed);
            }

            let generation = state.generation;

            state.arrived += 1;

            if state.arrived == self.participants {
                state.arrived = 0;
                state.generation = state.generation.wrapping_add(1);
                self.notify.notify_waiters();

                return Ok(BarrierWaitResult { leader: true });
            }

            generation
        };

        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let state = self.state.lock().unwrap();

                if state.closed {
                    return Err(Closed);
                }

                if state.generation != my_generation {
                    return Ok(BarrierWaitResult { leader: false });
                }
            }

            notified.await;
        }
    }
}
