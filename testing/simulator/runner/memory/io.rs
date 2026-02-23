use std::cell::RefCell;
use std::sync::Arc;

use indexmap::IndexMap;
use parking_lot::Mutex;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Clock, Completion, IO, MonotonicInstant, OpenFlags, Result, WallClockInstant};

use crate::runner::SimIO;
use crate::runner::clock::SimulatorClock;
use crate::runner::memory::file::MemorySimFile;

/// File descriptor
pub type Fd = String;

#[derive(Debug)]
pub enum OperationType {
    Read {
        completion: Completion,
        offset: usize,
    },
    Write {
        buffer: Arc<turso_core::Buffer>,
        completion: Completion,
        offset: usize,
    },
    WriteV {
        buffers: Vec<Arc<turso_core::Buffer>>,
        completion: Completion,
        offset: usize,
    },
    Sync {
        completion: Completion,
    },
    Truncate {
        completion: Completion,
        len: usize,
    },
}

impl OperationType {
    fn get_completion(&self) -> &Completion {
        match self {
            OperationType::Read { completion, .. }
            | OperationType::Write { completion, .. }
            | OperationType::WriteV { completion, .. }
            | OperationType::Sync { completion, .. }
            | OperationType::Truncate { completion, .. } => completion,
        }
    }
}

#[derive(Debug)]
pub struct Operation {
    pub time: Option<turso_core::WallClockInstant>,
    pub op: OperationType,
    pub fault: bool,
    pub fd: Arc<Fd>,
}

impl Operation {
    fn do_operation(self, files: &IndexMap<Fd, Arc<MemorySimFile>>) {
        let fd = self.fd;
        match self.op {
            OperationType::Read { completion, offset } => {
                let file = files.get(fd.as_str()).unwrap();
                let file_buf = file.buffer.borrow_mut();
                let buffer = completion.as_read().buf.clone();
                let buf_size = {
                    let buf = buffer.as_mut_slice();
                    // TODO: check for sector faults here

                    buf.copy_from_slice(&file_buf[offset..][0..buf.len()]);
                    buf.len() as i32
                };
                completion.complete(buf_size);
            }
            OperationType::Write {
                buffer,
                completion,
                offset,
            } => {
                let file = files.get(fd.as_str()).unwrap();
                let buf_size = file.write_buf(buffer.as_slice(), offset);
                completion.complete(buf_size as i32);
            }
            OperationType::WriteV {
                buffers,
                completion,
                offset,
            } => {
                if buffers.is_empty() {
                    return;
                }
                let file = files.get(fd.as_str()).unwrap();
                let mut pos = offset;
                let written = buffers.into_iter().fold(0, |written, buffer| {
                    let buf_size = file.write_buf(buffer.as_slice(), pos);
                    pos += buf_size;
                    written + buf_size
                });
                completion.complete(written as i32);
            }
            OperationType::Sync { completion, .. } => {
                // There is no Sync for in memory
                completion.complete(0);
            }
            OperationType::Truncate { completion, len } => {
                let file = files.get(fd.as_str()).unwrap();
                let mut file_buf = file.buffer.borrow_mut();
                file_buf.truncate(len);
                completion.complete(0);
            }
        }
    }
}

pub type CallbackQueue = Arc<Mutex<Vec<Operation>>>;

pub struct MemorySimIO {
    callbacks: CallbackQueue,
    timeouts: CallbackQueue,
    pub files: RefCell<IndexMap<Fd, Arc<MemorySimFile>>>,
    pub rng: RefCell<ChaCha8Rng>,
    #[expect(dead_code)]
    pub page_size: usize,
    seed: u64,
    latency_probability: u8,
    clock: Arc<SimulatorClock>,
}

unsafe impl Send for MemorySimIO {}
unsafe impl Sync for MemorySimIO {}

impl MemorySimIO {
    pub fn new(
        seed: u64,
        page_size: usize,
        latency_probability: u8,
        min_tick: u64,
        max_tick: u64,
    ) -> Self {
        let files = RefCell::new(IndexMap::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        Self {
            callbacks: Arc::new(Mutex::new(Vec::new())),
            timeouts: Arc::new(Mutex::new(Vec::new())),
            files,
            rng,
            page_size,
            seed,
            latency_probability,
            clock: Arc::new(SimulatorClock::new(
                ChaCha8Rng::seed_from_u64(seed),
                min_tick,
                max_tick,
            )),
        }
    }
}

impl SimIO for MemorySimIO {
    fn inject_fault(&self, fault: bool) {
        for file in self.files.borrow().values() {
            file.inject_fault(fault);
        }
        if fault {
            tracing::debug!("fault injected");
        }
    }

    fn inject_fault_selective(&self, faults: &[(&str, bool)]) {
        for (path, file) in self.files.borrow().iter() {
            for (stem, fault) in faults {
                if path.contains(stem) {
                    file.inject_fault(*fault);
                    break;
                }
            }
        }
    }

    fn print_stats(&self) {
        for (path, file) in self.files.borrow().iter() {
            if path.contains("ephemeral") {
                // Files created for ephemeral tables just add noise to the simulator output and aren't by default very interesting to debug
                continue;
            }
            tracing::info!(
                "\n===========================\n\nPath: {}\n{}",
                path,
                file.stats_table()
            );
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

    fn persist_files(&self) -> anyhow::Result<()> {
        let files = self.files.borrow();
        for (file_path, file) in files.iter() {
            if file_path.ends_with(".db") || file_path.ends_with("wal") || file_path.ends_with("lg")
            {
                std::fs::write(file_path, &*file.buffer.borrow())?;
            }
        }
        Ok(())
    }
}

impl Clock for MemorySimIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        MonotonicInstant::now()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
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

    fn step(&self) -> Result<()> {
        let mut callbacks = self.callbacks.lock();
        let mut timeouts = self.timeouts.lock();
        tracing::trace!(
            callbacks.len = callbacks.len(),
            timeouts.len = timeouts.len()
        );
        let files = self.files.borrow_mut();
        let now = self.current_time_wall_clock();

        callbacks.append(&mut timeouts);

        while let Some(callback) = callbacks.pop() {
            let completion = callback.op.get_completion();
            if completion.finished() {
                continue;
            }

            if callback.time.is_none() || callback.time.is_some_and(|time| time < now) {
                if callback.fault {
                    // Inject the fault by aborting the completion
                    tracing::error!("Fault injection: aborting completion");
                    completion.abort();
                    continue;
                }
                callback.do_operation(&files);
            } else {
                timeouts.push(callback);
            }
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().random()
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.rng.borrow_mut().fill_bytes(dest);
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.files.borrow_mut().shift_remove(path);
        Ok(())
    }
}
