//! A test IO that simulates unreliable storage: the bytes actually on disk
//! can lag behind what the process has written.
//!
//! Real storage only guarantees a write after an fsync of its file; until
//! then the write sits in the OS page cache and may or may not reach the
//! platter. This module tracks that gap for every file: reads and the live
//! database always see all writes (the page-cache view), but each write
//! stays volatile until the next fsync of its file promotes it to durable.
//!
//! A test can then materialize the disk image each kind of failure leaves
//! behind:
//!
//! - Process crash: the OS and its page cache survive, so no write is lost.
//!   Just reopen the database on the same [`UnreliableIo`].
//! - Power loss or kernel crash, strictest outcome: only fsync-acknowledged
//!   bytes survive; every unsynced write is lost.
//!   [`UnreliableIo::durable_files`] returns that image for every file.
//! - Power loss during a specific file's fsync, with torn writeback: arm it
//!   with [`UnreliableIo::arm_crash_on_sync`] and collect
//!   [`UnreliableIo::take_crash_snapshot`]. An arbitrary (alternating)
//!   subset of the target file's unsynced writes survives and all other
//!   files keep only their durable bytes — also a legal outcome, because
//!   the kernel may write back some dirty pages while others never reach
//!   the platter.
//!
//! To reopen a database on a materialized post-crash image, write each
//! file's bytes to disk and open with the regular platform IO.
//!
//! Other kinds of storage misbehavior tests need — injected write or fsync
//! errors, reordered writeback — belong in this module too.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use turso_core::{
    io::FileSyncType, Buffer, Clock, Completion, File, MonotonicInstant, OpenFlags,
    WallClockInstant, IO,
};

/// A single not-yet-durable file mutation.
enum UnsyncedOp {
    Write { pos: usize, data: Vec<u8> },
    Truncate { len: usize },
}

impl UnsyncedOp {
    fn apply(&self, image: &mut Vec<u8>) {
        match self {
            UnsyncedOp::Write { pos, data } => {
                let end = pos + data.len();
                if image.len() < end {
                    image.resize(end, 0);
                }
                image[*pos..end].copy_from_slice(data);
            }
            UnsyncedOp::Truncate { len } => {
                image.resize(*len, 0);
            }
        }
    }
}

#[derive(Default)]
struct FileShadow {
    /// Bytes guaranteed to be on stable storage (covered by an fsync).
    durable: Vec<u8>,
    /// Writes/truncates issued since the last fsync, in submission order.
    unsynced: Vec<UnsyncedOp>,
}

impl FileShadow {
    fn promote_all(&mut self) {
        for op in self.unsynced.drain(..) {
            op.apply(&mut self.durable);
        }
    }
}

/// The captured post-crash disk state.
pub struct CrashSnapshot {
    /// Per-file disk image at the crash point.
    pub files: HashMap<String, Vec<u8>>,
    /// Unsynced writes to the crash-target file that made it to disk.
    pub writes_persisted: usize,
    /// Unsynced writes to the crash-target file that were lost.
    pub writes_dropped: usize,
}

struct UnreliableIoState {
    files: Mutex<HashMap<String, Arc<Mutex<FileShadow>>>>,
    /// When set, the next fsync of this file (with pending writes) is the
    /// simulated power-loss point.
    armed_path: Mutex<Option<String>>,
    snapshot: Mutex<Option<CrashSnapshot>>,
}

impl UnreliableIoState {
    /// Builds the post-crash disk image: durable bytes everywhere, plus an
    /// alternating subset of the unsynced writes to the crash-target file
    /// (torn writeback), and none of the unsynced writes to any other file.
    fn build_crash_snapshot(&self, crash_path: &str) -> CrashSnapshot {
        let files = self.files.lock().unwrap();
        let mut images = HashMap::new();
        let mut persisted = 0usize;
        let mut dropped = 0usize;
        for (path, shadow) in files.iter() {
            let shadow = shadow.lock().unwrap();
            let mut image = shadow.durable.clone();
            if path == crash_path {
                let mut write_idx = 0usize;
                for op in &shadow.unsynced {
                    if matches!(op, UnsyncedOp::Write { .. }) {
                        if write_idx % 2 == 0 {
                            op.apply(&mut image);
                            persisted += 1;
                        } else {
                            dropped += 1;
                        }
                        write_idx += 1;
                    }
                }
            }
            images.insert(path.clone(), image);
        }
        CrashSnapshot {
            files: images,
            writes_persisted: persisted,
            writes_dropped: dropped,
        }
    }
}

pub struct UnreliableIo {
    inner: Arc<dyn IO>,
    state: Arc<UnreliableIoState>,
}

impl Default for UnreliableIo {
    fn default() -> Self {
        Self::new()
    }
}

impl UnreliableIo {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(turso_core::MemoryIO::new()),
            state: Arc::new(UnreliableIoState {
                files: Mutex::new(HashMap::new()),
                armed_path: Mutex::new(None),
                snapshot: Mutex::new(None),
            }),
        }
    }

    /// Settle: pretend everything written so far reached stable storage
    /// (baseline state before the scenario under test).
    pub fn mark_all_durable(&self) {
        for shadow in self.state.files.lock().unwrap().values() {
            shadow.lock().unwrap().promote_all();
        }
    }

    /// Arm the power-loss trigger: the next fsync of `path` that has
    /// pending (unsynced) writes captures the crash snapshot.
    pub fn arm_crash_on_sync(&self, path: &str) {
        *self.state.armed_path.lock().unwrap() = Some(path.to_string());
    }

    pub fn take_crash_snapshot(&self) -> Option<CrashSnapshot> {
        self.state.snapshot.lock().unwrap().take()
    }

    /// Simulate power loss right now: return, for each file, only the bytes
    /// that were fsync-acknowledged. Every unsynced write is lost.
    pub fn durable_files(&self) -> HashMap<String, Vec<u8>> {
        self.state
            .files
            .lock()
            .unwrap()
            .iter()
            .map(|(path, shadow)| (path.clone(), shadow.lock().unwrap().durable.clone()))
            .collect()
    }

    /// True when `path` has writes not yet covered by a successful fsync.
    pub fn has_unsynced_writes(&self, path: &str) -> bool {
        self.state
            .files
            .lock()
            .unwrap()
            .get(path)
            .is_some_and(|shadow| !shadow.lock().unwrap().unsynced.is_empty())
    }
}

impl Clock for UnreliableIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for UnreliableIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        let shadow = self
            .state
            .files
            .lock()
            .unwrap()
            .entry(path.to_string())
            .or_default()
            .clone();
        Ok(Arc::new(UnreliableFile {
            path: path.to_string(),
            inner,
            shadow,
            state: self.state.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.state.files.lock().unwrap().remove(path);
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }

    fn cancel(&self, completions: &[Completion]) -> turso_core::Result<()> {
        self.inner.cancel(completions)
    }

    fn drain_completions(&self, completions: &[Completion]) -> turso_core::Result<()> {
        self.inner.drain_completions(completions)
    }

    fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
        self.inner.file_id(path)
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest);
    }

    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }
}

struct UnreliableFile {
    path: String,
    inner: Arc<dyn File>,
    shadow: Arc<Mutex<FileShadow>>,
    state: Arc<UnreliableIoState>,
}

impl File for UnreliableFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(pos, c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.shadow
            .lock()
            .unwrap()
            .unsynced
            .push(UnsyncedOp::Write {
                pos: pos as usize,
                data: buffer.as_slice().to_vec(),
            });
        self.inner.pwrite(pos, buffer, c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        {
            let mut shadow = self.shadow.lock().unwrap();
            let mut off = pos as usize;
            for buffer in &buffers {
                shadow.unsynced.push(UnsyncedOp::Write {
                    pos: off,
                    data: buffer.as_slice().to_vec(),
                });
                off += buffer.len();
            }
        }
        self.inner.pwritev(pos, buffers, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        // Simulated power loss: crash while the armed file's fsync is in
        // flight, i.e. after its writes were submitted but before any of them
        // were made durable.
        let crash_here = {
            let armed = self.state.armed_path.lock().unwrap();
            armed.as_deref() == Some(self.path.as_str())
                && !self.shadow.lock().unwrap().unsynced.is_empty()
        };
        if crash_here {
            let mut snapshot = self.state.snapshot.lock().unwrap();
            if snapshot.is_none() {
                *snapshot = Some(self.state.build_crash_snapshot(&self.path));
            }
        }
        self.shadow.lock().unwrap().promote_all();
        self.inner.sync(c, sync_type)
    }

    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.shadow
            .lock()
            .unwrap()
            .unsynced
            .push(UnsyncedOp::Truncate { len: len as usize });
        self.inner.truncate(len, c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
}
