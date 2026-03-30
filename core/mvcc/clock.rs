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

/// A mutex-guarded clock for concurrent MVCC use.
///
/// The lock is held across the `f` callback in [`get_timestamp`], ensuring
/// that a commit timestamp is published (e.g. stored as `Preparing(ts)`)
/// before any other transaction can generate a higher timestamp. This closes
/// the TOCTOU window between timestamp generation and `Preparing` state
/// publication in the commit protocol.
///
/// ## Speculative reads
///
/// We have speculative reads (and speculative ignores). That is, an active
/// transaction can see changes of another transaction which is in the
/// **preparing** phase. Assuming the other transaction successfully commits,
/// the active transaction continues to make progress. If the other transaction
/// gets aborted, then the active transaction needs to be aborted as well.
///
/// So, say `tx2` starts at `begin_ts(11)` and another transaction `tx1`,
/// started earlier, is now in its preparing phase with `end_ts(10)`. Once the
/// `end_ts` is assigned, that will be the final commit timestamp of that
/// transaction. So `tx2` should see changes made by `tx1`, since `tx1` was
/// committed (in logical time) before `tx2` started.
///
/// Whether `tx2` can see `tx1`'s changes depends on when `tx1` acquired the
/// `end_ts` timestamp during the preparing phase.
///
/// > **Note:** We need speculative reads, otherwise it's difficult to make
/// > the MVCC model work without blocking. I made an attempt in
/// > [turso#5198](https://github.com/tursodatabase/turso/pull/5198) but this
/// > introduced a subtle bug which violated snapshot isolation. So without speculative
/// > reads in the previous example, `tx2` needs to wait till `tx1` is committed or
/// > aborted.
///
/// ### Need for atomicity
///
/// We want to atomically generate `end_ts` and publish `Preparing(end_ts)`
/// while the clock lock is held. This closes the TOCTOU window.
///
/// Consider the example:
///
/// ```text
/// tx1 (Active):    generates end_ts = 10
/// tx2 (Active):    gets begin_ts = 11
/// tx2 (Active):    does queries but does not see changes by tx1 (tx1 is still Active)
/// tx1 (Preparing): stores Preparing(end_ts=10)
/// tx2 (Active):    queries again, now it can see changes by tx1 (tx1 is now Preparing)
/// ```
///
/// **This is a snapshot isolation violation** — `tx2` observes different
/// values for the same rows within the same transaction.
///
/// So we want the following two operations to be atomic:
///
/// ```text
/// let ts = get_timestamp()
/// store Preparing(ts)
/// ```
///
/// `tx2` must get its begin timestamp either **before** or **after** these
/// two operations. If it interleaves, the above bug happens.
///
/// ## Note on the Hekaton paper
///
/// The Hekaton paper doesn't mention this "gotcha". The paper says:
///
/// > "When the transaction has completed its normal processing and requests
/// > to commit, it acquires an end timestamp and switches to the Preparing
/// > state."
///
/// But it doesn't go into more detail about atomicity here.
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
