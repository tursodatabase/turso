use std::cell::RefCell;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use indexmap::IndexMap;
use parking_lot::Mutex;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{
    Clock, Completion, CompletionError, IO, LimboError, MonotonicInstant, OpenFlags, Result,
    WallClockInstant,
};

use crate::runner::SimIO;
use crate::runner::clock::SimulatorClock;
use crate::runner::memory::file::MemorySimFile;

/// File descriptor
pub type Fd = String;

#[derive(Debug)]
pub(super) enum FileMutation {
    Write { offset: usize, data: Vec<u8> },
    Truncate { len: usize },
}

impl FileMutation {
    fn apply(&self, file: &mut Vec<u8>) {
        match self {
            Self::Write { offset, data } => {
                write_buf(file, data, *offset);
            }
            Self::Truncate { len } => file.resize(*len, 0),
        }
    }
}

#[derive(Debug)]
struct IssuedMutation {
    sequence: u64,
    status: MutationStatus,
}

#[derive(Debug)]
enum MutationStatus {
    Pending(Completion),
    Succeeded {
        completion_sequence: u64,
        mutation: FileMutation,
    },
    Failed,
}

#[derive(Debug, Default)]
pub(super) struct FileState {
    pub(super) buffer: Vec<u8>,
    durable: Vec<u8>,
    mutations: Vec<IssuedMutation>,
    next_sequence: u64,
    next_completion_sequence: u64,
}

impl FileState {
    pub(super) fn mutation_started(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("durability operation sequence overflow");
        sequence
    }

    pub(super) fn sync_started(&mut self) -> u64 {
        for mutation in &mut self.mutations {
            match &mutation.status {
                MutationStatus::Pending(completion) if completion.failed() => {
                    mutation.status = MutationStatus::Failed;
                }
                MutationStatus::Pending(completion) => {
                    assert!(
                        !completion.succeeded(),
                        "queued file mutation completed without running its operation"
                    );
                    panic!("file sync started before an earlier mutation completed");
                }
                MutationStatus::Succeeded { .. } | MutationStatus::Failed => {}
            }
        }
        self.next_sequence
    }

    pub(super) fn record_mutation(&mut self, sequence: u64, completion: Completion) {
        assert!(
            self.mutations
                .last()
                .is_none_or(|last| last.sequence < sequence),
            "file mutations must be recorded in issue order"
        );
        self.mutations.push(IssuedMutation {
            sequence,
            status: MutationStatus::Pending(completion),
        });
    }

    fn mutation_succeeded(&mut self, sequence: u64, mutation: FileMutation) {
        let index = self
            .mutations
            .binary_search_by_key(&sequence, |mutation| mutation.sequence)
            .expect("completed mutation must have been recorded");
        let completion_sequence = self.next_completion_sequence;
        self.next_completion_sequence = self
            .next_completion_sequence
            .checked_add(1)
            .expect("mutation completion sequence overflow");
        let old_status = std::mem::replace(
            &mut self.mutations[index].status,
            MutationStatus::Succeeded {
                completion_sequence,
                mutation,
            },
        );
        assert!(
            matches!(old_status, MutationStatus::Pending(_)),
            "a file mutation must complete exactly once"
        );
    }

    fn mutation_failed(&mut self, sequence: u64) {
        let index = self
            .mutations
            .binary_search_by_key(&sequence, |mutation| mutation.sequence)
            .expect("failed mutation must have been recorded");
        let old_status =
            std::mem::replace(&mut self.mutations[index].status, MutationStatus::Failed);
        assert!(
            matches!(old_status, MutationStatus::Pending(_)),
            "a file mutation must complete exactly once"
        );
    }

    fn clear_failed_mutation(&mut self, sequence: u64) {
        let Ok(index) = self
            .mutations
            .binary_search_by_key(&sequence, |mutation| mutation.sequence)
        else {
            // A completed sync may already have removed this failed mutation.
            return;
        };
        match &self.mutations[index].status {
            MutationStatus::Pending(completion) => {
                assert!(completion.failed(), "unfinished mutation was discarded");
                self.mutations[index].status = MutationStatus::Failed;
            }
            MutationStatus::Failed => {}
            MutationStatus::Succeeded { .. } => {
                panic!("a successful mutation cannot have a failed completion")
            }
        }
    }

    fn sync_completed(&mut self, boundary: u64) {
        let durable_count = self
            .mutations
            .iter()
            .take_while(|mutation| mutation.sequence < boundary)
            .count();

        let mut succeeded = Vec::with_capacity(durable_count);
        for mutation in self.mutations.drain(..durable_count) {
            match mutation.status {
                MutationStatus::Pending(completion) if completion.failed() => {}
                MutationStatus::Pending(_) => {
                    panic!("file sync completed before an earlier mutation completed")
                }
                MutationStatus::Succeeded {
                    completion_sequence,
                    mutation,
                } => succeeded.push((completion_sequence, mutation)),
                MutationStatus::Failed => {}
            }
        }

        succeeded.sort_unstable_by_key(|(completion_sequence, _)| *completion_sequence);
        for (_, mutation) in succeeded {
            mutation.apply(&mut self.durable);
        }
    }

    fn power_loss(&mut self) {
        self.mutations.clear();
        self.next_sequence = 0;
        self.next_completion_sequence = 0;
        self.buffer.clone_from(&self.durable);
    }
}

#[derive(Debug)]
pub(super) enum OperationType {
    Read {
        completion: Completion,
        offset: usize,
    },
    Write {
        buffer: Arc<turso_core::Buffer>,
        completion: Completion,
        offset: usize,
        sequence: u64,
    },
    WriteV {
        buffers: Vec<Arc<turso_core::Buffer>>,
        completion: Completion,
        offset: usize,
        sequence: u64,
    },
    Sync {
        completion: Completion,
        boundary: u64,
    },
    Truncate {
        completion: Completion,
        len: usize,
        sequence: u64,
    },
}

impl OperationType {
    pub(super) fn get_completion(&self) -> &Completion {
        match self {
            OperationType::Read { completion, .. }
            | OperationType::Write { completion, .. }
            | OperationType::WriteV { completion, .. }
            | OperationType::Sync { completion, .. }
            | OperationType::Truncate { completion, .. } => completion,
        }
    }

    pub(super) fn mutation_sequence(&self) -> Option<u64> {
        match self {
            Self::Write { sequence, .. }
            | Self::WriteV { sequence, .. }
            | Self::Truncate { sequence, .. } => Some(*sequence),
            Self::Read { .. } | Self::Sync { .. } => None,
        }
    }
}

#[derive(Debug)]
pub(super) struct Operation {
    pub(super) time: Option<turso_core::WallClockInstant>,
    pub(super) op: OperationType,
    pub(super) fault: bool,
    pub(super) file: Arc<RefCell<FileState>>,
}

impl Operation {
    fn do_operation(self) {
        match self.op {
            OperationType::Read { completion, offset } => {
                let file = self.file.borrow();
                let buffer = completion.as_read().buf.clone();
                let bytes_read = {
                    let buf = buffer.as_mut_slice();
                    if buf.is_empty() || offset >= file.buffer.len() {
                        0
                    } else {
                        let available = file.buffer.len() - offset;
                        let to_copy = available.min(buf.len());
                        buf[..to_copy].copy_from_slice(&file.buffer[offset..offset + to_copy]);
                        if to_copy < buf.len() {
                            // Keep deterministic behavior for unread tail bytes.
                            buf[to_copy..].fill(0);
                        }
                        to_copy as i32
                    }
                };
                drop(file);
                completion.complete(bytes_read);
            }
            OperationType::Write {
                buffer,
                completion,
                offset,
                sequence,
            } => {
                let mut file = self.file.borrow_mut();
                let buf_size = write_buf(&mut file.buffer, buffer.as_slice(), offset);
                file.mutation_succeeded(
                    sequence,
                    FileMutation::Write {
                        offset,
                        data: buffer.as_slice().to_vec(),
                    },
                );
                drop(file);
                completion.complete(buf_size as i32);
            }
            OperationType::WriteV {
                buffers,
                completion,
                offset,
                sequence,
            } => {
                let mut file = self.file.borrow_mut();
                let mut pos = offset;
                let mut data =
                    Vec::with_capacity(buffers.iter().map(|buffer| buffer.len()).sum::<usize>());
                let written = buffers.into_iter().fold(0, |written, buffer| {
                    data.extend_from_slice(buffer.as_slice());
                    let buf_size = write_buf(&mut file.buffer, buffer.as_slice(), pos);
                    pos += buf_size;
                    written + buf_size
                });
                file.mutation_succeeded(sequence, FileMutation::Write { offset, data });
                drop(file);
                completion.complete(written as i32);
            }
            OperationType::Sync {
                completion,
                boundary,
            } => {
                self.file.borrow_mut().sync_completed(boundary);
                completion.complete(0);
            }
            OperationType::Truncate {
                completion,
                len,
                sequence,
            } => {
                let mut file = self.file.borrow_mut();
                file.buffer.resize(len, 0);
                file.mutation_succeeded(sequence, FileMutation::Truncate { len });
                drop(file);
                completion.complete(0);
            }
        }
    }
}

fn write_buf(file: &mut Vec<u8>, buf: &[u8], offset: usize) -> usize {
    let end = offset + buf.len();
    file.resize(file.len().max(end), 0);
    file[offset..end].copy_from_slice(buf);
    buf.len()
}

pub(super) type CallbackQueue = Arc<Mutex<Vec<Operation>>>;

pub struct MemorySimIO {
    callbacks: CallbackQueue,
    timeouts: CallbackQueue,
    pub files: RefCell<IndexMap<Fd, Arc<MemorySimFile>>>,
    file_states: RefCell<Vec<Weak<RefCell<FileState>>>>,
    generation: Arc<AtomicU64>,
    power_loss_in_progress: AtomicBool,
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
            file_states: RefCell::new(Vec::new()),
            generation: Arc::new(AtomicU64::new(0)),
            power_loss_in_progress: AtomicBool::new(false),
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

    fn ensure_io_allowed(&self) -> Result<()> {
        if self.power_loss_in_progress.load(Ordering::Acquire) {
            return Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "memory simulator I/O is unavailable during power loss",
            )));
        }
        Ok(())
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
        let timeouts = self.timeouts.try_lock().unwrap();
        callbacks.iter().chain(timeouts.iter()).any(|operation| {
            matches!(operation.op, OperationType::Sync { .. })
                && !operation.op.get_completion().finished()
        })
    }

    fn close_files(&self) {
        for file in self.files.borrow().values() {
            file.closed.set(true);
        }
    }

    fn power_loss(&self) -> Result<()> {
        assert!(
            !self.power_loss_in_progress.swap(true, Ordering::AcqRel),
            "power loss cannot be nested"
        );
        self.generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .expect("memory simulator crash generation overflow");
        self.close_files();
        let pending = {
            let mut callbacks = self.callbacks.lock();
            let mut timeouts = self.timeouts.lock();
            callbacks
                .drain(..)
                .chain(timeouts.drain(..))
                .collect::<Vec<_>>()
        };
        for operation in pending {
            let completion = operation.op.get_completion();
            if completion.succeeded() {
                panic!("queued file operation completed without running its operation");
            }
            if !completion.finished() {
                if let Some(sequence) = operation.op.mutation_sequence() {
                    operation.file.borrow_mut().mutation_failed(sequence);
                }
                completion.abort();
            } else {
                assert!(completion.failed());
            }
        }
        assert!(
            self.callbacks.lock().is_empty() && self.timeouts.lock().is_empty(),
            "completion callbacks must not queue new I/O during power loss"
        );
        self.file_states.borrow_mut().retain(|state| {
            let Some(state) = state.upgrade() else {
                return false;
            };
            state.borrow_mut().power_loss();
            true
        });
        self.power_loss_in_progress.store(false, Ordering::Release);
        Ok(())
    }

    fn persist_files(&self) -> anyhow::Result<()> {
        let files = self.files.borrow();
        for (file_path, file) in files.iter() {
            if file_path.ends_with(".db") || file_path.ends_with("wal") || file_path.ends_with("lg")
            {
                std::fs::write(file_path, &file.state.borrow().buffer)?;
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
        self.ensure_io_allowed()?;
        let mut files = self.files.borrow_mut();
        let fd = path.to_string();
        let file = if let Some(file) = files.get(path).cloned() {
            if !file.is_open() {
                let file = Arc::new(file.reopen());
                files.insert(fd, file.clone());
                file
            } else {
                file
            }
        } else {
            let file = Arc::new(MemorySimFile::new(
                self.callbacks.clone(),
                self.seed,
                self.latency_probability,
                self.clock.clone(),
                self.generation.clone(),
            ));
            self.file_states
                .borrow_mut()
                .push(Arc::downgrade(&file.state));
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
        let now = self.current_time_wall_clock();

        callbacks.append(&mut timeouts);

        while let Some(callback) = callbacks.pop() {
            let completion = callback.op.get_completion();
            if completion.finished() {
                assert!(
                    completion.failed(),
                    "queued file operation completed without running its operation"
                );
                if let Some(sequence) = callback.op.mutation_sequence() {
                    callback.file.borrow_mut().clear_failed_mutation(sequence);
                }
                continue;
            }

            if callback.time.is_none() || callback.time.is_some_and(|time| time < now) {
                if callback.fault {
                    // Inject the fault by aborting the completion
                    tracing::error!("Fault injection: aborting completion");
                    if let Some(sequence) = callback.op.mutation_sequence() {
                        callback.file.borrow_mut().mutation_failed(sequence);
                    }
                    completion.abort();
                    continue;
                }
                callback.do_operation();
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
        self.ensure_io_allowed()?;
        self.files.borrow_mut().shift_remove(path);
        Ok(())
    }

    fn file_id(&self, path: &str) -> Result<turso_core::io::FileId> {
        Ok(turso_core::io::FileId::from_path_hash(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering};
    use turso_core::{Buffer, io::FileSyncType};

    fn io() -> MemorySimIO {
        MemorySimIO::new(0, 4096, 0, 1, 2)
    }

    fn write(file: &Arc<dyn turso_core::File>, data: &[u8]) -> Completion {
        file.pwrite(
            0,
            Arc::new(Buffer::new(data.to_vec())),
            Completion::new_write(|_| {}),
        )
        .unwrap()
    }

    fn sync(file: &Arc<dyn turso_core::File>) -> Completion {
        file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
            .unwrap()
    }

    #[test]
    fn empty_pwritev_completes_with_zero_bytes() {
        let io = io();
        let file = io
            .open_file("empty-writev.db", OpenFlags::Create, false)
            .unwrap();
        let written = Arc::new(AtomicI32::new(-1));
        let callback_written = written.clone();
        let completion = file
            .pwritev(
                0,
                Vec::new(),
                Completion::new_write(move |result| {
                    callback_written.store(result.unwrap(), Ordering::Relaxed);
                }),
            )
            .unwrap();

        assert!(completion.succeeded());
        assert_eq!(written.load(Ordering::Relaxed), 0);
    }

    #[test]
    #[should_panic(expected = "file sync started before an earlier mutation completed")]
    fn sync_rejects_an_earlier_write_that_has_not_completed() {
        let io = io();
        let file = io
            .open_file("sync-order.db", OpenFlags::Create, false)
            .unwrap();
        drop(write(&file, b"not-complete"));
        drop(sync(&file));

        io.step().unwrap();
    }

    #[test]
    fn durable_overlapping_writes_keep_completion_order() {
        let io = io();
        let file = io
            .open_file("overlapping-writes.db", OpenFlags::Create, false)
            .unwrap();
        let first_write = write(&file, b"first!");
        let second_write = file
            .pwritev(
                0,
                vec![
                    Arc::new(Buffer::new(b"sec".to_vec())),
                    Arc::new(Buffer::new(b"ond".to_vec())),
                ],
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.step().unwrap();

        // The callback queue is LIFO, so the second write completes first and
        // the first write determines the final live bytes.
        assert!(first_write.succeeded());
        assert!(second_write.succeeded());
        let memory_file = io
            .files
            .borrow()
            .get("overlapping-writes.db")
            .unwrap()
            .clone();
        assert_eq!(memory_file.state.borrow().buffer.as_slice(), b"first!");

        drop(sync(&file));
        io.step().unwrap();
        io.power_loss().unwrap();

        assert_eq!(memory_file.state.borrow().buffer.as_slice(), b"first!");
    }

    #[test]
    fn power_loss_rejects_pre_crash_handles() {
        let io = io();
        let old_file = io
            .open_file("stale-handle.db", OpenFlags::Create, false)
            .unwrap();

        io.power_loss().unwrap();
        let new_file = io
            .open_file("stale-handle.db", OpenFlags::Create, false)
            .unwrap();

        assert!(old_file.size().is_err());
        assert!(
            old_file
                .pwrite(
                    0,
                    Arc::new(Buffer::new(b"stale".to_vec())),
                    Completion::new_write(|_| {}),
                )
                .is_err()
        );
        drop(write(&new_file, b"current"));
        io.step().unwrap();

        let memory_file = io.files.borrow().get("stale-handle.db").unwrap().clone();
        assert_eq!(memory_file.state.borrow().buffer.as_slice(), b"current");
    }

    #[test]
    fn power_loss_rejects_unlinked_handles_and_restores_their_bytes() {
        let io = io();
        let file = io
            .open_file("unlinked.db", OpenFlags::Create, false)
            .unwrap();
        drop(write(&file, b"volatile"));
        io.step().unwrap();
        let memory_file = io.files.borrow().get("unlinked.db").unwrap().clone();
        io.remove_file("unlinked.db").unwrap();

        io.power_loss().unwrap();

        assert!(file.size().is_err());
        assert!(memory_file.state.borrow().buffer.is_empty());
    }

    #[test]
    fn syncing_includes_delayed_syncs() {
        let io = io();
        let file = io
            .open_file("delayed-sync.db", OpenFlags::Create, false)
            .unwrap();
        let completion = sync(&file);
        io.callbacks.lock()[0].time =
            Some(io.current_time_wall_clock() + std::time::Duration::from_secs(1));

        io.step().unwrap();

        assert!(!completion.finished());
        assert!(io.callbacks.lock().is_empty());
        assert_eq!(io.timeouts.lock().len(), 1);
        assert!(io.syncing());
    }

    #[test]
    fn syncing_ignores_an_aborted_sync() {
        let io = io();
        let file = io
            .open_file("aborted-sync.db", OpenFlags::Create, false)
            .unwrap();
        let completion = sync(&file);
        completion.abort();

        assert!(!io.syncing());
        io.step().unwrap();
        assert!(completion.failed());
    }

    #[test]
    fn aborted_write_is_not_made_durable() {
        let io = io();
        let file = io
            .open_file("aborted-write.db", OpenFlags::Create, false)
            .unwrap();
        let write = write(&file, b"aborted");
        write.abort();
        io.step().unwrap();

        let memory_file = io.files.borrow().get("aborted-write.db").unwrap().clone();
        assert!(matches!(
            &memory_file.state.borrow().mutations[0].status,
            MutationStatus::Failed
        ));

        let sync = sync(&file);
        io.step().unwrap();
        assert!(sync.succeeded());
        io.power_loss().unwrap();

        assert!(memory_file.state.borrow().buffer.is_empty());
    }

    #[test]
    fn power_loss_cancels_pending_io_and_restores_durable_bytes() {
        let io = io();
        let file = io
            .open_file("pending.db", OpenFlags::Create, false)
            .unwrap();
        drop(write(&file, b"durable"));
        io.step().unwrap();
        drop(sync(&file));
        io.step().unwrap();

        let pending_write = write(&file, b"volatile");
        let truncate = file.truncate(0, Completion::new_trunc(|_| {})).unwrap();
        io.power_loss().unwrap();

        assert!(pending_write.failed());
        assert!(truncate.failed());
        let memory_file = io.files.borrow().get("pending.db").unwrap().clone();
        assert_eq!(memory_file.state.borrow().buffer.as_slice(), b"durable");

        let file = io
            .open_file("pending.db", OpenFlags::Create, false)
            .unwrap();
        drop(write(&file, b"volatile"));
        io.step().unwrap();
        let pending_sync = sync(&file);
        io.power_loss().unwrap();

        assert!(pending_sync.failed());
        assert_eq!(memory_file.state.borrow().buffer.as_slice(), b"durable");
    }

    #[test]
    fn power_loss_rejects_file_changes_from_an_abort_callback() {
        let io = Arc::new(io());
        let file = io
            .open_file("callback-open.db", OpenFlags::Create, false)
            .unwrap();
        let callback_ran = Arc::new(AtomicBool::new(false));
        let callback_ran_clone = callback_ran.clone();
        let callback_io = io.clone();
        let completion = file
            .pwrite(
                0,
                Arc::new(Buffer::new(b"pending".to_vec())),
                Completion::new_write(move |result| {
                    assert!(result.is_err());
                    assert!(
                        callback_io
                            .open_file("opened-from-callback.db", OpenFlags::Create, false)
                            .is_err()
                    );
                    assert!(callback_io.remove_file("callback-open.db").is_err());
                    callback_ran_clone.store(true, Ordering::Release);
                }),
            )
            .unwrap();

        io.power_loss().unwrap();

        assert!(completion.failed());
        assert!(callback_ran.load(Ordering::Acquire));
        assert!(io.files.borrow().contains_key("callback-open.db"));
        assert!(
            io.open_file("opened-after-crash.db", OpenFlags::Create, false)
                .is_ok()
        );
    }
}
