use std::cell::{Cell, RefCell};
use std::sync::Arc;

use indexmap::IndexMap;
use parking_lot::Mutex;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Clock, Completion, IO, MonotonicInstant, OpenFlags, Result, WallClockInstant};

use crate::runner::clock::SimulatorClock;
use crate::runner::memory::file::{MemorySimFile, PendingWrite};
use crate::runner::{DurableIOEvent, SimIO};

#[derive(Debug)]
struct CrashState {
    crash_at: u64,
    count: Cell<u64>,
    crashed: Cell<bool>,
}

impl CrashState {
    fn new(crash_at: u64) -> Self {
        Self {
            crash_at,
            count: Cell::new(0),
            crashed: Cell::new(false),
        }
    }

    fn check(&self) -> bool {
        if self.crashed.get() {
            return true;
        }
        let n = self.count.get() + 1;
        self.count.set(n);
        if n == self.crash_at {
            self.crashed.set(true);
            return true;
        }
        false
    }

    fn has_crashed(&self) -> bool {
        self.crashed.get()
    }

    fn count(&self) -> u64 {
        self.count.get()
    }
}

fn check_crash(state: &Option<CrashState>, op: &str, path: &str) -> bool {
    let Some(s) = state else { return false };
    if s.has_crashed() {
        return true;
    }
    if s.check() {
        tracing::warn!("CRASH at IO #{} ({}) file={}", s.count(), op, path);
        return true;
    }
    false
}

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
    fn do_operation(
        self,
        files: &IndexMap<Fd, Arc<MemorySimFile>>,
        durable_events: &RefCell<Vec<DurableIOEvent>>,
        crash_state: &Option<CrashState>,
    ) {
        let fd = self.fd;
        match self.op {
            OperationType::Read { completion, offset } => {
                let file = files.get(fd.as_str()).unwrap();
                let durable = file.durable_buffer.borrow();
                let pending = file.pending_writes.borrow();

                let mut effective_data = durable.clone();
                for write in pending.iter() {
                    write.apply_to(&mut effective_data);
                }

                let buffer = completion.as_read().buf.clone();
                let buf_size = {
                    let buf = buffer.as_mut_slice();
                    // TODO: check for sector faults here
                    let read_end = (offset + buf.len()).min(effective_data.len());
                    if offset < effective_data.len() {
                        let available = read_end - offset;
                        buf[..available].copy_from_slice(&effective_data[offset..read_end]);
                        // Zero-fill any remaining buffer space
                        buf[available..].fill(0);
                    } else {
                        buf.fill(0);
                    }
                    buf.len() as i32
                };
                completion.complete(buf_size);
            }
            OperationType::Write {
                buffer,
                completion,
                offset,
            } => {
                if check_crash(crash_state, "pwrite", fd.as_str()) {
                    completion.abort();
                    return;
                }

                let file = files.get(fd.as_str()).unwrap();
                file.write_buf(buffer.as_slice(), offset);
                completion.complete(buffer.len() as i32);
            }
            OperationType::WriteV {
                buffers,
                completion,
                offset,
            } => {
                assert!(!buffers.is_empty(), "WriteV called with empty buffers");
                if check_crash(crash_state, "pwritev", fd.as_str()) {
                    completion.abort();
                    return;
                }

                let file = files.get(fd.as_str()).unwrap();
                let mut pos = offset;
                let mut total = 0;

                for buffer in buffers {
                    file.write_buf(buffer.as_slice(), pos);
                    pos += buffer.len();
                    total += buffer.len();
                }

                completion.complete(total as i32);
            }
            OperationType::Sync { completion, .. } => {
                if check_crash(crash_state, "fsync", fd.as_str()) {
                    completion.complete(-1);
                    return;
                }

                let file = files.get(fd.as_str()).unwrap();
                let pending_truncate = file
                    .pending_writes
                    .borrow()
                    .iter()
                    .filter_map(|w| match w {
                        PendingWrite::Truncate { len } => Some(*len),
                        _ => None,
                    })
                    .last();

                let had_content = !file.durable_buffer.borrow().is_empty()
                    || file
                        .pending_writes
                        .borrow()
                        .iter()
                        .any(|w| matches!(w, PendingWrite::Write { .. }));

                file.flush_pending();

                let durable_size = file.durable_buffer.borrow().len();
                durable_events.borrow_mut().push(DurableIOEvent::Sync {
                    file_path: fd.to_string(),
                    had_content,
                    durable_size,
                });

                if let Some(len) = pending_truncate {
                    durable_events
                        .borrow_mut()
                        .push(DurableIOEvent::TruncateSynced {
                            file_path: fd.to_string(),
                            new_len: len,
                        });
                }
                completion.complete(0);
            }
            OperationType::Truncate { completion, len } => {
                if check_crash(crash_state, "ftruncate", fd.as_str()) {
                    completion.complete(-1);
                    return;
                }
                let file = files.get(fd.as_str()).unwrap();
                file.pending_writes
                    .borrow_mut()
                    .push(PendingWrite::Truncate { len });
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
    /// Crash state for crash-at-write-N
    crash_state: Option<CrashState>,
    /// Events are pushed on sync/truncate, consumed by take_durable_events
    durable_events: RefCell<Vec<DurableIOEvent>>,
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
        crash_at_io_op: Option<u64>,
    ) -> Self {
        let files = RefCell::new(IndexMap::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let crash_state = crash_at_io_op.map(CrashState::new);

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
            crash_state,
            durable_events: RefCell::new(Vec::new()),
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
        for (path, file) in self.files.borrow().iter() {
            if path.ends_with(".db") || path.ends_with("wal") || path.ends_with("lg") {
                std::fs::write(path, &*file.durable_buffer.borrow())?;
            }
        }
        Ok(())
    }

    fn has_crashed(&self) -> bool {
        self.crash_state.as_ref().is_some_and(|s| s.has_crashed())
    }

    fn discard_all_pending(&self) {
        for file in self.files.borrow().values() {
            file.discard_pending();
        }
    }

    fn take_durable_events(&self) -> Vec<DurableIOEvent> {
        self.durable_events.borrow_mut().drain(..).collect()
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
                callback.do_operation(&files, &self.durable_events, &self.crash_state);
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
