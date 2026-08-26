use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use turso_core::IO;

use crate::observe::LogWatch;
use crate::spec::{BenchError, StopWhen};
use crate::worker::{Phase, StepOut, Worker};

/// Wakes the dedicated `io.step()` thread. Workers never hold the uring
/// wait lock; they only submit SQEs and notify this handle when parked.
#[derive(Clone)]
pub(crate) struct IoWake {
    need_step: Arc<(Mutex<bool>, Condvar)>,
}

impl IoWake {
    pub(crate) fn wake(&self) {
        let (lock, cv) = &*self.need_step;
        let mut g = lock.lock().unwrap_or_else(|e| e.into_inner());
        if !*g {
            *g = true;
            cv.notify_one();
        }
    }
}

/// Dedicated leader for `UringIO::step()`. Sleeps until a worker parks,
/// then drains the ring. Submitters keep running on other threads.
pub(crate) struct IoPump {
    stop: Arc<AtomicBool>,
    wake: IoWake,
    handle: JoinHandle<Result<(), BenchError>>,
}

impl IoPump {
    pub(crate) fn spawn(io: Arc<dyn IO>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let need_step = Arc::new((Mutex::new(false), Condvar::new()));
        let stop_c = Arc::clone(&stop);
        let need_c = Arc::clone(&need_step);
        let handle = thread::Builder::new()
            .name("io-pump".into())
            .spawn(move || {
                let (lock, cv) = &*need_c;
                loop {
                    if stop_c.load(Ordering::Relaxed) {
                        io.step()?;
                        return Ok(());
                    }
                    {
                        let mut g = lock.lock().unwrap_or_else(|e| e.into_inner());
                        while !*g && !stop_c.load(Ordering::Relaxed) {
                            g = cv.wait(g).unwrap_or_else(|e| e.into_inner());
                        }
                        *g = false;
                    }
                    io.step()?;
                }
            })
            .expect("spawn io-pump");
        Self {
            stop,
            wake: IoWake { need_step },
            handle,
        }
    }

    pub(crate) fn waker(&self) -> &IoWake {
        &self.wake
    }

    pub(crate) fn join(self) -> Result<(), BenchError> {
        self.stop.store(true, Ordering::Relaxed);
        self.wake.wake();
        match self.handle.join() {
            Ok(r) => r,
            Err(_) => Err(BenchError::thread_panicked()),
        }
    }
}

/// Who owns `io.step()` for this sweep.
pub(crate) enum RingOwner<'a> {
    /// This thread becomes uring leader when a worker parks.
    Inline(&'a dyn IO),
    /// Dedicated pump is the only leader. Wake it when parked.
    Pump(&'a IoWake),
}

pub(crate) struct CoopStats {
    pub inserts: u64,
    pub txns: u64,
    pub busy: u64,
    pub busy_snapshots: u64,
    pub schema_updated: u64,
    pub latencies_ns: Vec<u64>,
}

pub(crate) struct ThreadStats {
    pub inserts: u64,
    pub txns: u64,
    pub busy: u64,
    pub busy_snapshots: u64,
    pub schema_updated: u64,
    pub latencies_ns: Vec<u64>,
}

impl From<CoopStats> for ThreadStats {
    fn from(s: CoopStats) -> Self {
        Self {
            inserts: s.inserts,
            txns: s.txns,
            busy: s.busy,
            busy_snapshots: s.busy_snapshots,
            schema_updated: s.schema_updated,
            latencies_ns: s.latencies_ns,
        }
    }
}

#[derive(Clone)]
pub(crate) enum StopClock {
    Duration { deadline: Instant },
    Transactions { target: u64, done: Arc<AtomicU64> },
}

impl StopClock {
    pub(crate) fn from_stop(stop: StopWhen) -> Self {
        match stop {
            StopWhen::Duration(d) => {
                if d.is_zero() {
                    Self::Duration {
                        deadline: Instant::now(),
                    }
                } else {
                    Self::Duration {
                        deadline: Instant::now() + d,
                    }
                }
            }
            StopWhen::Transactions(n) => Self::Transactions {
                target: n.get(),
                done: Arc::new(AtomicU64::new(0)),
            },
        }
    }

    pub(crate) fn hit(&self) -> bool {
        match self {
            StopClock::Duration { deadline } => Instant::now() >= *deadline,
            StopClock::Transactions { target, done } => done.load(Ordering::Relaxed) >= *target,
        }
    }

    pub(crate) fn record_txns(&self, n: u64) {
        if let StopClock::Transactions { done, .. } = self {
            done.fetch_add(n, Ordering::Relaxed);
        }
    }
}

/// Cooperative sweep. One `stmt.step()` per worker per round.
/// Inline ring waits block this thread (and every worker on it).
/// Pump wakes a dedicated leader so other workers keep submitting.
pub(crate) fn drive_coop(
    ring: RingOwner<'_>,
    workers: &mut [Worker],
    stop: &StopClock,
    watch: &mut LogWatch,
) -> Result<CoopStats, BenchError> {
    let mut sweeps = 0u32;
    loop {
        let mut running = 0usize;
        let mut parked = false;
        for w in workers.iter_mut() {
            if stop.hit() && matches!(w.phase, Phase::Begin) {
                continue;
            }
            running += 1;
            let before = w.txns_ok;
            match w.drive()? {
                StepOut::Parked => parked = true,
                StepOut::Ready => {}
            }
            if w.txns_ok > before {
                stop.record_txns(w.txns_ok - before);
            }
        }
        sweeps = sweeps.wrapping_add(1);
        if sweeps % 256 == 0 {
            watch.sample();
        }
        if running == 0 {
            watch.sample();
            break;
        }
        if parked {
            match ring {
                RingOwner::Inline(io) => io.step()?,
                RingOwner::Pump(wake) => {
                    wake.wake();
                    thread::yield_now();
                }
            }
        }
    }
    Ok(CoopStats {
        inserts: workers.iter().map(|w| w.inserts_ok).sum(),
        txns: workers.iter().map(|w| w.txns_ok).sum(),
        busy: workers.iter().map(|w| w.busy).sum(),
        busy_snapshots: workers.iter().map(|w| w.busy_snapshots).sum(),
        schema_updated: workers.iter().map(|w| w.schema_updated).sum(),
        latencies_ns: workers
            .iter()
            .flat_map(|w| w.latencies_ns.iter().copied())
            .collect(),
    })
}
