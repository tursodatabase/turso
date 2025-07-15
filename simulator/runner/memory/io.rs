use std::cell::{Cell, RefCell};
use std::sync::Arc;

use parking_lot::Mutex;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Clock, Completion, Instant, OpenFlags, Result, IO};

use crate::{model::FAULT_ERROR_MSG, runner::memory::file::MemorySimFile};

/// File descriptor
pub type Fd = usize;

pub enum Operation {
    Read {
        fd: usize,
        completion: Arc<Completion>,
        offset: usize,
    },
    Write {
        fd: usize,
        buffer: Arc<RefCell<turso_core::Buffer>>,
        completion: Arc<Completion>,
        offset: usize,
    },
    Sync {
        fd: usize,
        completion: Arc<Completion>,
    },
}

pub type CallbackQueue = Arc<Mutex<Vec<Operation>>>;

pub struct MemorySimIo {
    callbacks: CallbackQueue,
    pub fault: Cell<bool>,
    pub files: RefCell<Vec<Arc<MemorySimFile>>>,
    pub rng: RefCell<ChaCha8Rng>,
    pub nr_run_once_faults: Cell<usize>,
    pub page_size: usize,
    seed: u64,
    latency_probability: usize,
}

unsafe impl Send for MemorySimIo {}
unsafe impl Sync for MemorySimIo {}

impl MemorySimIo {
    pub fn new(seed: u64, page_size: usize, latency_probability: usize) -> Result<Self> {
        let fault = Cell::new(false);
        let files = RefCell::new(Vec::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let nr_run_once_faults = Cell::new(0);
        Ok(Self {
            callbacks: Arc::new(Mutex::new(Vec::new())),
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

impl Clock for MemorySimIo {
    fn now(&self) -> Instant {
        Instant {
            secs: 1704067200, // 2024-01-01 00:00:00 UTC
            micros: 0,
        }
    }
}

impl IO for MemorySimIo {
    fn open_file(
        &self,
        _path: &str,
        _flags: OpenFlags, // TODO: ignoring open flags for now as we don't test read only mode in the simulator yet
        _direct: bool,
    ) -> Result<Arc<dyn turso_core::File>> {
        let files = self.files.borrow_mut();
        let fd = files.len();
        let file = Arc::new(MemorySimFile::new(
            self.callbacks.clone(),
            fd,
            self.seed,
            self.latency_probability,
        ));
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
        let mut callbacks = self.callbacks.lock();
        let files = self.files.borrow_mut();
        while let Some(callback) = callbacks.pop() {
            match callback {
                Operation::Read {
                    fd,
                    completion,
                    offset,
                } => {
                    let file = &files[fd];
                    let file_buf = file.buffer.borrow_mut();
                    let buffer = completion.as_read().buf.clone();
                    let mut buf = buffer.borrow_mut();
                    let buf = buf.as_mut_slice();
                    // TODO: check for sector faults here

                    buf.copy_from_slice(&file_buf[offset..][0..buf.len()]);
                    completion.complete(buf.len() as i32);
                }
                Operation::Write {
                    fd,
                    buffer,
                    completion,
                    offset,
                } => {
                    let file = &files[fd];
                    let mut file_buf = file.buffer.borrow_mut();
                    let buf = buffer.borrow_mut();
                    let buf = buf.as_slice();
                    let write_size = file_buf.len() - offset;
                    if write_size < buf.len() {
                        file_buf.reserve(write_size);
                    }
                    file_buf[offset..][0..buf.len()].copy_from_slice(buf);
                    completion.complete(buf.len() as i32);
                }
                Operation::Sync { completion, .. } => {
                    // There is no Sync for in memory
                    completion.complete(0);
                }
            }
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
