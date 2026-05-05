use std::{
    cell::{Cell, RefCell},
    fmt::Debug,
    sync::Arc,
};

use rand::Rng as _;
use rand_chacha::ChaCha8Rng;
use tracing::{Level, instrument};
use turso_core::{File, Result};

use crate::runner::{FAULT_ERROR_MSG, clock::SimulatorClock};
pub(crate) struct SimulatorFile {
    pub path: String,
    pub(crate) inner: Arc<dyn File>,
    pub(crate) fault: Cell<bool>,
    pub(crate) locked: Cell<bool>,

    /// Number of `pread` function calls (both success and failures).
    pub(crate) nr_pread_calls: Cell<usize>,

    /// Number of `pread` function calls with injected fault.
    pub(crate) nr_pread_faults: Cell<usize>,

    /// Number of `pwrite` function calls (both success and failures).
    pub(crate) nr_pwrite_calls: Cell<usize>,

    /// Number of `pwrite` function calls with injected fault.
    pub(crate) nr_pwrite_faults: Cell<usize>,

    /// Number of `sync` function calls (both success and failures).
    pub(crate) nr_sync_calls: Cell<usize>,

    /// Number of `sync` function calls with injected fault.
    pub(crate) nr_sync_faults: Cell<usize>,

    pub(crate) page_size: usize,

    pub(crate) rng: RefCell<ChaCha8Rng>,

    pub latency_probability: u8,

    pub sync_completion: RefCell<Option<turso_core::Completion>>,
    pub queued_io: RefCell<Vec<DelayedIo>>,
    pub clock: Arc<SimulatorClock>,
}

type IoOperation = Box<dyn FnOnce(&SimulatorFile) -> Result<turso_core::Completion>>;

pub struct DelayedIo {
    pub time: turso_core::WallClockInstant,
    pub op: IoOperation,
}

impl Debug for DelayedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelayedIo")
            .field("time", &self.time)
            .finish()
    }
}

unsafe impl Send for SimulatorFile {}
unsafe impl Sync for SimulatorFile {}

impl SimulatorFile {
    pub(crate) fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
    }

    pub(crate) fn stats_table(&self) -> String {
        let sum_calls =
            self.nr_pread_calls.get() + self.nr_pwrite_calls.get() + self.nr_sync_calls.get();
        let sum_faults = self.nr_pread_faults.get() + self.nr_pwrite_faults.get();
        let stats_table = [
            "op           calls   faults".to_string(),
            "--------- -------- --------".to_string(),
            format!(
                "pread     {:8} {:8}",
                self.nr_pread_calls.get(),
                self.nr_pread_faults.get()
            ),
            format!(
                "pwrite    {:8} {:8}",
                self.nr_pwrite_calls.get(),
                self.nr_pwrite_faults.get()
            ),
            format!(
                "sync      {:8} {:8}",
                self.nr_sync_calls.get(),
                0 // No fault counter for sync
            ),
            "--------- -------- --------".to_string(),
            format!("total     {sum_calls:8} {sum_faults:8}"),
        ];

        stats_table.join("\n")
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn generate_latency_duration(&self) -> Option<turso_core::WallClockInstant> {
        let mut rng = self.rng.borrow_mut();
        // Chance to introduce some latency
        rng.random_bool(self.latency_probability as f64 / 100.0)
            .then(|| {
                let now = self.clock.now();
                let sum = now + std::time::Duration::from_millis(rng.random_range(5..20));
                sum.into()
            })
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn run_queued_io(&self, now: turso_core::WallClockInstant) -> Result<()> {
        let mut queued_io = self.queued_io.borrow_mut();
        for io in queued_io.extract_if(.., |item| item.time <= now) {
            let _c = (io.op)(self)?;
        }
        Ok(())
    }
}

impl File for SimulatorFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        if self.fault.get() {
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        self.inner.lock_file(exclusive)?;
        self.locked.set(true);
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        if self.fault.get() {
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        self.inner.unlock_file()?;
        self.locked.set(false);
        Ok(())
    }

    fn pread(&self, pos: u64, c: turso_core::Completion) -> Result<turso_core::Completion> {
        self.nr_pread_calls.set(self.nr_pread_calls.get() + 1);
        if self.fault.get() {
            tracing::debug!("pread fault");
            self.nr_pread_faults.set(self.nr_pread_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op = Box::new(move |file: &SimulatorFile| file.inner.pread(pos, cloned_c));
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            Ok(c)
        } else {
            self.inner.pread(pos, c)
        }
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<turso_core::Buffer>,
        c: turso_core::Completion,
    ) -> Result<turso_core::Completion> {
        self.nr_pwrite_calls.set(self.nr_pwrite_calls.get() + 1);
        if self.fault.get() {
            tracing::debug!("pwrite fault");
            self.nr_pwrite_faults.set(self.nr_pwrite_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op = Box::new(move |file: &SimulatorFile| file.inner.pwrite(pos, buffer, cloned_c));
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            Ok(c)
        } else {
            self.inner.pwrite(pos, buffer, c)
        }
    }

    fn sync(
        &self,
        c: turso_core::Completion,
        sync_type: turso_core::io::FileSyncType,
    ) -> Result<turso_core::Completion> {
        self.nr_sync_calls.set(self.nr_sync_calls.get() + 1);
        if self.fault.get() {
            // TODO: Enable this when https://github.com/tursodatabase/turso/issues/2091 is fixed.
            tracing::debug!(
                "ignoring sync fault because it causes false positives with current simulator design"
            );
            self.fault.set(false);
        }
        let c = if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op = Box::new(move |file: &SimulatorFile| -> Result<_> {
                let c = file.inner.sync(cloned_c, sync_type)?;
                *file.sync_completion.borrow_mut() = Some(c.clone());
                Ok(c)
            });
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            c
        } else {
            let c = self.inner.sync(c, sync_type)?;
            *self.sync_completion.borrow_mut() = Some(c.clone());
            c
        };
        Ok(c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<turso_core::Buffer>>,
        c: turso_core::Completion,
    ) -> Result<turso_core::Completion> {
        self.nr_pwrite_calls.set(self.nr_pwrite_calls.get() + 1);
        if self.fault.get() {
            tracing::debug!("pwritev fault");
            self.nr_pwrite_faults.set(self.nr_pwrite_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op =
                Box::new(move |file: &SimulatorFile| file.inner.pwritev(pos, buffers, cloned_c));
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            Ok(c)
        } else {
            let c = self.inner.pwritev(pos, buffers, c)?;
            Ok(c)
        }
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: turso_core::Completion) -> Result<turso_core::Completion> {
        if self.fault.get() {
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        let c = if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op = Box::new(move |file: &SimulatorFile| file.inner.truncate(len, cloned_c));
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            c
        } else {
            self.inner.truncate(len, c)?
        };
        Ok(c)
    }
}

impl Drop for SimulatorFile {
    fn drop(&mut self) {
        if self.locked.get() {
            if let Err(error) = self.inner.unlock_file() {
                tracing::warn!(?error, "failed to unlock simulator file during drop");
            } else {
                self.locked.set(false);
            }
        }
    }
}

struct Latency {}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use turso_core::{Buffer, Completion, LimboError, io::FileSyncType};

    struct CountingFile {
        locks: AtomicUsize,
        unlocks: AtomicUsize,
    }

    impl CountingFile {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                locks: AtomicUsize::new(0),
                unlocks: AtomicUsize::new(0),
            })
        }
    }

    impl File for CountingFile {
        fn lock_file(&self, _exclusive: bool) -> Result<()> {
            self.locks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn unlock_file(&self) -> Result<()> {
            let unlocks = self.unlocks.fetch_add(1, Ordering::SeqCst);
            if unlocks >= self.locks.load(Ordering::SeqCst) {
                return Err(LimboError::LockingError("already unlocked".into()));
            }
            Ok(())
        }

        fn pread(&self, _pos: u64, c: Completion) -> Result<Completion> {
            Ok(c)
        }

        fn pwrite(&self, _pos: u64, _buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            Ok(c)
        }

        fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
            Ok(c)
        }

        fn size(&self) -> Result<u64> {
            Ok(0)
        }

        fn truncate(&self, _len: u64, c: Completion) -> Result<Completion> {
            Ok(c)
        }
    }

    fn simulator_file(inner: Arc<dyn File>) -> SimulatorFile {
        SimulatorFile {
            path: "test.db".into(),
            inner,
            fault: Cell::new(false),
            locked: Cell::new(false),
            nr_pread_calls: Cell::new(0),
            nr_pread_faults: Cell::new(0),
            nr_pwrite_calls: Cell::new(0),
            nr_pwrite_faults: Cell::new(0),
            nr_sync_calls: Cell::new(0),
            nr_sync_faults: Cell::new(0),
            page_size: 4096,
            rng: RefCell::new(ChaCha8Rng::seed_from_u64(0)),
            latency_probability: 0,
            sync_completion: RefCell::new(None),
            queued_io: RefCell::new(Vec::new()),
            clock: Arc::new(SimulatorClock::new(ChaCha8Rng::seed_from_u64(0), 1, 2)),
        }
    }

    #[test]
    fn drop_does_not_unlock_never_locked_file() {
        let inner = CountingFile::new();
        {
            let _file = simulator_file(inner.clone());
        }
        assert_eq!(inner.unlocks.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn drop_does_not_unlock_after_explicit_unlock() {
        let inner = CountingFile::new();
        {
            let file = simulator_file(inner.clone());
            file.lock_file(true).unwrap();
            file.unlock_file().unwrap();
        }
        assert_eq!(inner.unlocks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn drop_unlocks_still_locked_file_once() {
        let inner = CountingFile::new();
        {
            let file = simulator_file(inner.clone());
            file.lock_file(true).unwrap();
        }
        assert_eq!(inner.unlocks.load(Ordering::SeqCst), 1);
    }
}
