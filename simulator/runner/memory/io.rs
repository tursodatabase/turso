use std::cell::{Cell, RefCell};
use std::sync::Arc;

use indexmap::IndexMap;
use parking_lot::Mutex;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Clock, Completion, Instant, OpenFlags, Result, IO};

use crate::runner::clock::SimulatorClock;
use crate::runner::SimIO;
use crate::{model::FAULT_ERROR_MSG, runner::memory::file::MemorySimFile};

/// File descriptor
pub type Fd = String;

pub enum OperationType {
    Read {
        fd: Arc<Fd>,
        completion: Arc<Completion>,
        offset: usize,
    },
    Write {
        fd: Arc<Fd>,
        buffer: Arc<RefCell<turso_core::Buffer>>,
        completion: Arc<Completion>,
        offset: usize,
    },
    Sync {
        fd: Arc<Fd>,
        completion: Arc<Completion>,
    },
}

impl OperationType {
    fn get_fd(&self) -> &Fd {
        match self {
            OperationType::Read { fd, .. }
            | OperationType::Write { fd, .. }
            | OperationType::Sync { fd, .. } => fd,
        }
    }
}

pub struct Operation {
    pub time: Option<turso_core::Instant>,
    pub op: OperationType,
}

impl Operation {
    fn do_operation(self, files: &IndexMap<Fd, Arc<MemorySimFile>>) {
        match self.op {
            OperationType::Read {
                fd,
                completion,
                offset,
            } => {
                let file = files.get(fd.as_str()).unwrap();
                let file_buf = file.buffer.borrow_mut();
                let buffer = completion.as_read().buf.clone();
                let buf_size = {
                    let mut buf = buffer.borrow_mut();
                    let buf = buf.as_mut_slice();
                    // TODO: check for sector faults here

                    buf.copy_from_slice(&file_buf[offset..][0..buf.len()]);
                    buf.len() as i32
                };
                completion.complete(buf_size);
            }
            OperationType::Write {
                fd,
                buffer,
                completion,
                offset,
            } => {
                let file = files.get(fd.as_str()).unwrap();
                let buf_size = {
                    let mut file_buf = file.buffer.borrow_mut();
                    let buf = buffer.borrow_mut();
                    let buf = buf.as_slice();
                    let more_space = if file_buf.len() < offset {
                        (offset + buf.len()) - file_buf.len()
                    } else {
                        buf.len().saturating_sub(file_buf.len() - offset)
                    };
                    if more_space > 0 {
                        file_buf.reserve(more_space);
                        for _ in 0..more_space {
                            file_buf.push(0);
                        }
                    }

                    file_buf[offset..][0..buf.len()].copy_from_slice(buf);
                    buf.len() as i32
                };
                completion.complete(buf_size);
            }
            OperationType::Sync { completion, .. } => {
                // There is no Sync for in memory
                completion.complete(0);
            }
        }
    }
}

pub type CallbackQueue = Arc<Mutex<Vec<Operation>>>;

pub struct MemorySimIO {
    callbacks: CallbackQueue,
    timeouts: CallbackQueue,
    pub fault: Cell<bool>,
    pub files: RefCell<IndexMap<Fd, Arc<MemorySimFile>>>,
    pub rng: RefCell<ChaCha8Rng>,
    pub nr_run_once_faults: Cell<usize>,
    pub page_size: usize,
    seed: u64,
    latency_probability: usize,
    clock: Arc<SimulatorClock>,
}

unsafe impl Send for MemorySimIO {}
unsafe impl Sync for MemorySimIO {}

impl MemorySimIO {
    pub fn new(
        seed: u64,
        page_size: usize,
        latency_probability: usize,
        min_tick: u64,
        max_tick: u64,
    ) -> Result<Self> {
        let fault = Cell::new(false);
        let files = RefCell::new(IndexMap::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let nr_run_once_faults = Cell::new(0);
        Ok(Self {
            callbacks: Arc::new(Mutex::new(Vec::new())),
            timeouts: Arc::new(Mutex::new(Vec::new())),
            fault,
            files,
            rng,
            nr_run_once_faults,
            page_size,
            seed,
            latency_probability,
            clock: Arc::new(SimulatorClock::new(
                ChaCha8Rng::seed_from_u64(seed),
                min_tick,
                max_tick,
            )),
        })
    }
}

impl SimIO for MemorySimIO {
    fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
        if fault {
            tracing::debug!("fault injected");
        }
    }

    fn print_stats(&self) {
        tracing::info!("run_once faults: {}", self.nr_run_once_faults.get());
        for file in self.files.borrow().values() {
            tracing::info!("\n===========================\n{}", file.stats_table());
        }
    }

    fn syncing(&self) -> bool {
        let callbacks = self.callbacks.try_lock().unwrap();
        callbacks
            .iter()
            .any(|operation| matches!(operation.op, OperationType::Sync { .. }))
    }

    fn close_files(&self) {
        for file in self.files.borrow().values() {
            file.closed.set(true);
        }
    }
}

impl Clock for MemorySimIO {
    fn now(&self) -> Instant {
        self.clock.now().into()
    }
}

impl IO for MemorySimIO {
    fn open_file(
        &self,
        path: &str,
        _flags: OpenFlags, // TODO: ignoring open flags for now as we don't test read only mode in the simulator yet
        _direct: bool,
    ) -> Result<Arc<dyn turso_core::File>> {
        let mut files = self.files.borrow_mut();
        let fd = path.to_string();
        let file = if let Some(file) = files.get(path) {
            file.closed.set(false);
            file.clone()
        } else {
            let file = Arc::new(MemorySimFile::new(
                self.callbacks.clone(),
                fd.clone(),
                self.seed,
                self.latency_probability,
                self.clock.clone(),
            ));
            files.insert(fd, file.clone());
            file
        };

        Ok(file)
    }

    fn wait_for_completion(&self, c: Arc<turso_core::Completion>) -> Result<()> {
        while !c.is_completed() {
            self.run_once()?;
        }
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        let mut callbacks = self.callbacks.lock();
        let mut timeouts = self.timeouts.lock();
        tracing::trace!(
            callbacks.len = callbacks.len(),
            timeouts.len = timeouts.len()
        );
        if self.fault.get() {
            self.nr_run_once_faults
                .replace(self.nr_run_once_faults.get() + 1);
            // TODO: currently we only deal with single threaded execution in one file
            // When we support multiple db files, we need to only remove callbacks not relevant to the current file
            // and maybe connection
            callbacks.clear();
            timeouts.clear();
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        let files = self.files.borrow_mut();
        let now = self.now();

        callbacks.append(&mut timeouts);

        while let Some(callback) = callbacks.pop() {
            if callback.time.is_none() || callback.time.is_some_and(|time| time < now) {
                callback.do_operation(&files);
            } else {
                timeouts.push(callback);
            }
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().next_u64() as i64
    }

    fn get_memory_io(&self) -> Arc<turso_core::MemoryIO> {
        Arc::new(turso_core::MemoryIO {})
    }
}
