use super::memory::MemStore;
use super::{Buffer, Clock, Completion, File, OpenFlags, IO};
use crate::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::sync::Mutex;
use crate::FileSyncType;
use crate::Result;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tracing::debug;

/// A memory-backed [`IO`] backend that defers every completion until the next
/// [`IO::step`] call, forcing the engine through its cooperative-yield path on
/// *every* `pread` / `pwrite` / `pwritev` / `sync` / `truncate`.
///
/// This backend performs the identical byte-level data movement as
/// `MemoryIO` (it shares [`MemStore`]) but enqueues the completion instead of
/// signalling it. The completion only becomes `finished()` when `step()` runs,
/// so the engine must return `StepResult::IO`, yield, and re-enter — exercising
/// the resume path behind each yield point.
pub struct MemoryYieldIO {
    files: Arc<Mutex<HashMap<String, Arc<MemoryYieldFile>>>>,
    /// Completions submitted but not yet signalled, drained FIFO by `step()`.
    pending: Arc<Mutex<VecDeque<Deferred>>>,
}

/// A completion awaiting `step()`, together with the byte count it should
/// report. All completion kinds (read/write/sync/truncate) report a single
/// `i32`, so this is all the state we need to defer.
struct Deferred {
    completion: Completion,
    result: i32,
}

impl MemoryYieldIO {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Self {
        debug!("Using IO backend 'memory_yield'");
        Self {
            files: Arc::new(Mutex::new(HashMap::default())),
            pending: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl Default for MemoryYieldIO {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MemoryYieldIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for MemoryYieldIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        let mut files = self.files.lock();
        if !files.contains_key(path) && !flags.contains(OpenFlags::Create) {
            return Err(crate::error::CompletionError::IOError(
                std::io::ErrorKind::NotFound,
                "open",
            )
            .into());
        }
        if !files.contains_key(path) {
            files.insert(
                path.to_string(),
                Arc::new(MemoryYieldFile {
                    path: path.to_string(),
                    store: MemStore::new(),
                    pending: self.pending.clone(),
                }),
            );
        }
        Ok(files
            .get(path)
            .ok_or_else(|| {
                crate::LimboError::InternalError("file should exist after insert".to_string())
            })?
            .clone())
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        let mut files = self.files.lock();
        files.remove(path);
        Ok(())
    }

    fn file_id(&self, path: &str) -> Result<super::FileId> {
        Ok(super::FileId::from_path_hash(path))
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }

    /// Signal every completion that was deferred since the last `step()`.
    ///
    /// We snapshot the queue and release the lock before signalling so that a
    /// completion callback is free to submit follow-up I/O (which lands in the
    /// queue for the *next* step) without deadlocking or being drained within
    /// this same call. Each deferred completion required at least one `step()`
    /// to finish, which means at least one yield occurred per submitted op.
    fn step(&self) -> Result<()> {
        let drained: VecDeque<Deferred> = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };
        for Deferred { completion, result } in drained {
            completion.complete(result);
        }
        Ok(())
    }
}

pub struct MemoryYieldFile {
    path: String,
    store: MemStore,
    pending: Arc<Mutex<VecDeque<Deferred>>>,
}

crate::assert::assert_sync!(MemoryYieldFile);

impl MemoryYieldFile {
    /// Defer `c` so it is signalled on the next `IO::step`, leaving it
    /// `finished() == false` for now and forcing the caller to yield.
    fn defer(&self, c: &Completion, result: i32) {
        self.pending.lock().push_back(Deferred {
            completion: c.clone(),
            result,
        });
    }
}

impl File for MemoryYieldFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        tracing::debug!("pread(path={}): pos={} [deferred]", self.path, pos);
        let n = self.store.read_into(pos, c.as_read().buf());
        self.defer(&c, n);
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        tracing::debug!(
            "pwrite(path={}): pos={}, size={} [deferred]",
            self.path,
            pos,
            buffer.len()
        );
        let n = self.store.write_at(pos, buffer.as_slice());
        self.defer(&c, n as i32);
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        tracing::debug!("sync(path={}) [deferred]", self.path);
        self.defer(&c, 0);
        Ok(c)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        tracing::debug!("truncate(path={}): len={} [deferred]", self.path, len);
        self.store.truncate(len);
        self.defer(&c, 0);
        Ok(c)
    }

    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        tracing::debug!(
            "pwritev(path={}): pos={}, buffers={:?} [deferred]",
            self.path,
            pos,
            buffers.iter().map(|x| x.len()).collect::<Vec<_>>()
        );
        let n = self.store.writev(pos, &buffers);
        self.defer(&c, n);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        tracing::debug!("size(path={}): {}", self.path, self.store.size());
        Ok(self.store.size())
    }

    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        Ok(self.store.has_hole(pos, len))
    }

    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        self.store.punch_hole(pos, len);
        Ok(())
    }
}
