use std::cell::{Cell, RefCell};
use std::sync::Arc;

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Clock, Completion, Instant, OpenFlags, Result, IO};

use crate::{model::FAULT_ERROR_MSG, runner::memory::file::MemorySimFile};

/// File descriptor
pub(super) type Fd = usize;

pub enum Operation {
    Read {
        fd: usize,
        completion: Arc<Completion>,
        offset: usize,
    },
    Write {
        fd: usize,
        completion: Arc<Completion>,
        offset: usize,
    },
    Sync {
        fd: usize,
    },
}

pub struct SimulatorIO {
    pub fault: Cell<bool>,
    pub files: RefCell<Vec<Arc<MemorySimFile>>>,
    pub rng: RefCell<ChaCha8Rng>,
    pub nr_run_once_faults: Cell<usize>,
    pub page_size: usize,
    seed: u64,
    latency_probability: usize,
}

unsafe impl Send for SimulatorIO {}
unsafe impl Sync for SimulatorIO {}

impl SimulatorIO {
    pub fn new(seed: u64, page_size: usize, latency_probability: usize) -> Result<Self> {
        let fault = Cell::new(false);
        let files = RefCell::new(Vec::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let nr_run_once_faults = Cell::new(0);
        Ok(Self {
            fault,
            files,
            rng,
            nr_run_once_faults,
            page_size,
            seed,
            latency_probability,
        })
    }

    pub fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
    }

    pub fn print_stats(&self) {
        tracing::info!("run_once faults: {}", self.nr_run_once_faults.get());
        for file in self.files.borrow().iter() {
            tracing::info!("\n===========================\n{}", file.stats_table());
        }
    }
}

impl Clock for SimulatorIO {
    fn now(&self) -> Instant {
        Instant {
            secs: 1704067200, // 2024-01-01 00:00:00 UTC
            micros: 0,
        }
    }
}

impl IO for SimulatorIO {
    fn open_file(
        &self,
        _path: &str,
        _flags: OpenFlags, // TODO: ignoring open flags for now as we don't test read only mode in the simulator yet
        _direct: bool,
    ) -> Result<Arc<dyn turso_core::File>> {
        let files = self.files.borrow_mut();
        let fd = files.len();
        let file = Arc::new(MemorySimFile::new(fd, self.seed, self.latency_probability));
        self.files.borrow_mut().push(file.clone());
        Ok(file)
    }

    fn wait_for_completion(&self, c: Arc<turso_core::Completion>) -> Result<()> {
        while !c.is_completed() {
            self.run_once()?;
        }
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        if self.fault.get() {
            self.nr_run_once_faults
                .replace(self.nr_run_once_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().next_u64() as i64
    }

    fn get_memory_io(&self) -> Arc<turso_core::MemoryIO> {
        todo!()
    }
}
