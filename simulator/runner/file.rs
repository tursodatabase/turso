use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

use rand::Rng as _;
use rand_chacha::ChaCha8Rng;
use tracing::{instrument, Level};
use turso_core::{Completion, File, Result};

use crate::model::FAULT_ERROR_MSG;
pub(crate) struct SimulatorFile {
    pub(crate) inner: Arc<dyn File>,
    pub(crate) fault: Cell<bool>,

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

    pub latency_probability: usize,

    pub sync_completion: RefCell<Option<Arc<turso_core::Completion>>>,
    pub queued_io: RefCell<Vec<QueuedIo>>,
}

pub struct QueuedIo {
    pub completion: Completion,
    pub time: std::time::Instant,
    pub op: Box<dyn FnOnce() -> Result<Arc<turso_core::Completion>>>,
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
            format!("total     {:8} {:8}", sum_calls, sum_faults),
        ];

        stats_table.join("\n")
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn generate_latency_duration(&self) -> Option<std::time::Instant> {
        let mut rng = self.rng.borrow_mut();
        // Chance to introduce some latency
        rng.gen_bool(self.latency_probability as f64 / 100.0)
            .then(|| {
                std::time::Instant::now() + std::time::Duration::from_millis(rng.gen_range(20..50))
            })
    }
}

impl File for SimulatorFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        if self.fault.get() {
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        if self.fault.get() {
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        self.inner.unlock_file()
    }

    fn pread(
        &self,
        pos: usize,
        c: Arc<turso_core::Completion>,
    ) -> Result<Arc<turso_core::Completion>> {
        self.nr_pread_calls.set(self.nr_pread_calls.get() + 1);
        if self.fault.get() {
            tracing::debug!("pread fault");
            self.nr_pread_faults.set(self.nr_pread_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        if let Some(latency) = self.generate_latency_duration() {
            let op = Box::new(|| self.inner.pread(pos, c.clone()));

            Ok(c)
        } else {
            self.inner.pread(pos, c)
        }
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<turso_core::Buffer>>,
        c: Arc<turso_core::Completion>,
    ) -> Result<Arc<turso_core::Completion>> {
        self.nr_pwrite_calls.set(self.nr_pwrite_calls.get() + 1);
        if self.fault.get() {
            tracing::debug!("pwrite fault");
            self.nr_pwrite_faults.set(self.nr_pwrite_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        if let Some(latency) = self.generate_latency_duration() {
            let op = Box::new(|| self.inner.pwrite(pos, buffer, c.clone()));

            Ok(c)
        } else {
            self.inner.pwrite(pos, buffer, c)
        }
    }

    fn sync(&self, c: Arc<turso_core::Completion>) -> Result<Arc<turso_core::Completion>> {
        self.nr_sync_calls.set(self.nr_sync_calls.get() + 1);
        if self.fault.get() {
            tracing::debug!("sync fault");
            self.nr_sync_faults.set(self.nr_sync_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        let c = if let Some(latency) = self.generate_latency_duration() {
            let op = Box::new(|| self.inner.sync(c.clone()));
            Ok(c)
        } else {
            self.inner.sync(c)
        }?;
        *self.sync_completion.borrow_mut() = Some(c.clone());
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
}

impl Drop for SimulatorFile {
    fn drop(&mut self) {
        self.inner.unlock_file().expect("Failed to unlock file");
    }
}

struct Latency {}
