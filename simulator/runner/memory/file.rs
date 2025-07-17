use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

use rand::{Rng as _, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tracing::{instrument, Level};
use turso_core::{File, Result};

use crate::runner::{
    clock::SimulatorClock,
    memory::io::{CallbackQueue, Fd, Operation, OperationType},
};

pub struct MemorySimFile {
    pub callbacks: CallbackQueue,
    pub fd: Arc<Fd>,
    pub buffer: RefCell<Vec<u8>>,
    // TODO: add fault map later here
    pub closed: Cell<bool>,

    /// Number of `pread` function calls (both success and failures).
    pub nr_pread_calls: Cell<usize>,
    /// Number of `pwrite` function calls (both success and failures).
    pub nr_pwrite_calls: Cell<usize>,
    /// Number of `sync` function calls (both success and failures).
    pub nr_sync_calls: Cell<usize>,

    pub rng: RefCell<ChaCha8Rng>,

    pub latency_probability: usize,
    clock: Arc<SimulatorClock>,
}

type IoOperation = Box<dyn FnOnce(OperationType) -> Result<Arc<turso_core::Completion>>>;

pub struct DelayedIo {
    pub time: turso_core::Instant,
    pub op: IoOperation,
}

unsafe impl Send for MemorySimFile {}
unsafe impl Sync for MemorySimFile {}

impl MemorySimFile {
    pub fn new(
        callbacks: CallbackQueue,
        fd: Fd,
        seed: u64,
        latency_probability: usize,
        clock: Arc<SimulatorClock>,
    ) -> Self {
        Self {
            callbacks,
            fd: Arc::new(fd),
            buffer: RefCell::new(Vec::new()),
            closed: Cell::new(false),
            nr_pread_calls: Cell::new(0),
            nr_pwrite_calls: Cell::new(0),
            nr_sync_calls: Cell::new(0),
            rng: RefCell::new(ChaCha8Rng::seed_from_u64(seed)),
            latency_probability,
            clock,
        }
    }

    pub fn stats_table(&self) -> String {
        let sum_calls =
            self.nr_pread_calls.get() + self.nr_pwrite_calls.get() + self.nr_sync_calls.get();
        let stats_table = [
            "op        calls   ".to_string(),
            "--------- --------".to_string(),
            format!("pread     {:8}", self.nr_pread_calls.get()),
            format!("pwrite    {:8}", self.nr_pwrite_calls.get()),
            format!("sync      {:8}", self.nr_sync_calls.get()),
            "--------- -------- --------".to_string(),
            format!("total     {sum_calls:8}"),
        ];

        stats_table.join("\n")
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn generate_latency(&self) -> Option<turso_core::Instant> {
        let mut rng = self.rng.borrow_mut();
        // Chance to introduce some latency
        rng.gen_bool(self.latency_probability as f64 / 100.0)
            .then(|| {
                let now = self.clock.now();
                let sum = now + std::time::Duration::from_millis(rng.gen_range(5..20));
                sum.into()
            })
    }
}

impl File for MemorySimFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(
        &self,
        pos: usize,
        c: Arc<turso_core::Completion>,
    ) -> Result<Arc<turso_core::Completion>> {
        self.nr_pread_calls.set(self.nr_pread_calls.get() + 1);

        let op = OperationType::Read {
            fd: self.fd.clone(),
            completion: c.clone(),
            offset: pos,
        };
        self.callbacks.lock().push(Operation {
            time: self.generate_latency(),
            op,
        });
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<turso_core::Buffer>>,
        c: Arc<turso_core::Completion>,
    ) -> Result<Arc<turso_core::Completion>> {
        self.nr_pwrite_calls.set(self.nr_pwrite_calls.get() + 1);
        let op = OperationType::Write {
            fd: self.fd.clone(),
            buffer,
            completion: c.clone(),
            offset: pos,
        };
        self.callbacks.lock().push(Operation {
            time: self.generate_latency(),
            op,
        });
        Ok(c)
    }

    fn sync(&self, c: Arc<turso_core::Completion>) -> Result<Arc<turso_core::Completion>> {
        self.nr_sync_calls.set(self.nr_sync_calls.get() + 1);
        let op = OperationType::Sync {
            fd: self.fd.clone(),
            completion: c.clone(),
        };
        self.callbacks.lock().push(Operation {
            time: self.generate_latency(),
            op,
        });
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        // TODO: size operation should also be scheduled. But this requires a change in how we
        // Use this function internally in Turso
        Ok(self.buffer.borrow().len() as u64)
    }
}
