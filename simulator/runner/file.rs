use std::{
    cell::{Cell, RefCell},
    fmt::Debug,
    sync::Arc,
};

use rand::Rng as _;
use rand_chacha::ChaCha8Rng;
use tracing::{instrument, Level};
use turso_core::{File, Result};

use crate::{model::FAULT_ERROR_MSG, runner::clock::SimulatorClock};
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
    pub queued_io: RefCell<Vec<DelayedIo>>,
    pub clock: Arc<SimulatorClock>,
}

type IoOperation = Box<dyn FnOnce(&SimulatorFile) -> Result<Arc<turso_core::Completion>>>;

pub struct DelayedIo {
    pub time: turso_core::Instant,
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
    fn generate_latency_duration(&self) -> Option<turso_core::Instant> {
        let mut rng = self.rng.borrow_mut();
        // Chance to introduce some latency
        rng.gen_bool(self.latency_probability as f64 / 100.0)
            .then(|| {
                let now = self.clock.now();
                let sum = now + std::time::Duration::from_millis(rng.gen_range(5..20));
                sum.into()
            })
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn run_queued_io(&self, now: turso_core::Instant) -> Result<()> {
        let mut queued_io = self.queued_io.borrow_mut();
        // TODO: as we are not in version 1.87 we cannot use `extract_if`
        // so we have to do something different to achieve the same thing
        // This code was acquired from: https://doc.rust-lang.org/beta/std/vec/struct.Vec.html#method.extract_if
        let range = 0..queued_io.len();
        let mut i = range.start;
        let end_items = queued_io.len() - range.end;

        while i < queued_io.len() - end_items {
            if queued_io[i].time <= now {
                let io = queued_io.remove(i);
                // your code here
                (io.op)(self)?;
            } else {
                i += 1;
            }
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

    fn sync(&self, c: Arc<turso_core::Completion>) -> Result<Arc<turso_core::Completion>> {
        self.nr_sync_calls.set(self.nr_sync_calls.get() + 1);
        if self.fault.get() {
            // TODO: Enable this when https://github.com/tursodatabase/turso/issues/2091 is fixed.
            tracing::debug!("ignoring sync fault because it causes false positives with current simulator design");
            self.fault.set(false);
        }
        let c = if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op = Box::new(|file: &SimulatorFile| -> Result<_> {
                let c = file.inner.sync(cloned_c)?;
                *file.sync_completion.borrow_mut() = Some(c.clone());
                Ok(c)
            });
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            c
        } else {
            let c = self.inner.sync(c)?;
            *self.sync_completion.borrow_mut() = Some(c.clone());
            c
        };
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }

    fn truncate(
        &self,
        len: usize,
        c: Arc<turso_core::Completion>,
    ) -> Result<Arc<turso_core::Completion>> {
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
        self.inner.unlock_file().expect("Failed to unlock file");
    }
}

struct Latency {}
