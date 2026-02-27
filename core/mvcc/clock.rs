use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Mutex;

/// No-op callback for use with [`LogicalClock::get_timestamp`] when no
/// action needs to be taken atomically alongside timestamp generation
/// (e.g. for begin timestamps).
pub fn no_op(_: u64) {}

/// Logical clock.
pub trait LogicalClock: Send + Sync {
    /// Generates the next timestamp, calls `f` with it, then returns it.
    ///
    /// Implementations that guard concurrent commit protocols (e.g.
    /// [`MvccClock`]) hold their internal lock across the `f` call, so
    /// that the timestamp is published (e.g. stored as `Preparing(ts)`)
    /// before any other caller can observe a timestamp.
    ///
    /// Pass [`no_op`] when no atomic side-effect is needed (begin timestamps).
    fn get_timestamp<F: FnOnce(u64)>(&self, f: F) -> u64;
    fn reset(&self, ts: u64);
}

/// A node-local clock backed by an atomic counter.
/// Suitable for single-threaded use and tests. Does not provide
/// atomicity between timestamp generation and the `f` callback.
#[derive(Debug, Default)]
pub struct LocalClock {
    ts_sequence: AtomicU64,
}

impl LocalClock {
    pub fn new() -> Self {
        Self {
            ts_sequence: AtomicU64::new(0),
        }
    }
}

impl LogicalClock for LocalClock {
    fn get_timestamp<F: FnOnce(u64)>(&self, f: F) -> u64 {
        let ts = self.ts_sequence.fetch_add(1, Ordering::SeqCst);
        f(ts);
        ts
    }

    fn reset(&self, ts: u64) {
        self.ts_sequence.store(ts, Ordering::SeqCst);
    }
}

/// A mutex-guarded clock for concurrent MVCC use.
///
/// The lock is held across the `f` callback in [`get_timestamp`], ensuring
/// that a commit timestamp is published (e.g. as `Preparing(ts)`) before
/// any other transaction can generate a higher timestamp. This closes the
/// TOCTOU window between timestamp generation and `Preparing` state
/// publication in the commit protocol. See how it is used for `end_ts`
#[derive(Debug, Default)]
pub struct MvccClock {
    inner: Mutex<u64>,
}

impl MvccClock {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(0),
        }
    }

    /// Generate a begin timestamp. No side-effect needed alongside generation.
    pub fn get_begin_timestamp(&self) -> u64 {
        self.get_timestamp(no_op)
    }

    /// Generate a commit timestamp and call `f` with it while the lock is
    /// held, atomically publishing the timestamp before releasing.
    pub fn get_commit_timestamp<F: FnOnce(u64)>(&self, f: F) -> u64 {
        self.get_timestamp(f)
    }
}

impl LogicalClock for MvccClock {
    fn get_timestamp<F: FnOnce(u64)>(&self, f: F) -> u64 {
        let mut guard = self.inner.lock();
        let ts = *guard;
        *guard += 1;
        f(ts);
        ts
    }

    fn reset(&self, ts: u64) {
        *self.inner.lock() = ts;
    }
}
