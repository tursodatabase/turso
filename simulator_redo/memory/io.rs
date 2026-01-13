//! In-memory IO implementation for simulation.

use std::cell::RefCell;
use std::sync::Arc;

use indexmap::IndexMap;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Clock, IO, MonotonicInstant, OpenFlags, Result, WallClockInstant};

use crate::memory::file::MemorySimFile;

/// File descriptor type (path string).
pub type Fd = String;

/// Trait for simulation IO implementations.
///
/// Extends `turso_core::IO` with simulation-specific functionality.
pub trait SimIO: turso_core::IO {
    /// Close all open files.
    fn close_files(&self);

    /// Persist all database files to the filesystem.
    ///
    /// Writes `.db`, `.wal`, and `.lg` files to disk.
    fn persist_files(&self) -> anyhow::Result<()>;
}

/// In-memory IO implementation for simulation.
///
/// Provides a memory-backed file system for deterministic simulation.
/// Files are stored in memory and can be persisted to disk on demand.
pub struct MemorySimIO {
    /// Open files indexed by path.
    pub files: RefCell<IndexMap<Fd, Arc<MemorySimFile>>>,
    /// Random number generator for deterministic behavior.
    pub rng: RefCell<ChaCha8Rng>,
}

unsafe impl Send for MemorySimIO {}
unsafe impl Sync for MemorySimIO {}

impl MemorySimIO {
    /// Create a new in-memory IO with the given seed and page size.
    pub fn new(seed: u64) -> Self {
        Self {
            files: RefCell::new(IndexMap::new()),
            rng: RefCell::new(ChaCha8Rng::seed_from_u64(seed)),
        }
    }
}

impl SimIO for MemorySimIO {
    fn close_files(&self) {
        for file in self.files.borrow().values() {
            file.closed.set(true);
        }
    }

    fn persist_files(&self) -> anyhow::Result<()> {
        let files = self.files.borrow();
        tracing::debug!("persist_files: {} files tracked", files.len());
        for (file_path, file) in files.iter() {
            let buf = file.buffer.borrow();
            tracing::debug!(
                "  file: {:?}, size: {} bytes",
                file_path,
                buf.len()
            );
            // Only persist database-related files
            if file_path.ends_with(".db")
                || file_path.ends_with("wal")
                || file_path.ends_with("log")
            {
                let path = std::path::Path::new(file_path);
                if path.exists() {
                    std::fs::remove_file(path)?;
                }
                tracing::info!("Persisting {} ({} bytes)", file_path, buf.len());
                std::fs::write(path, &*buf)?;
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
        WallClockInstant::now()
    }
}

impl IO for MemorySimIO {
    fn open_file(
        &self,
        path: &str,
        _flags: OpenFlags,
        _direct: bool,
    ) -> Result<Arc<dyn turso_core::File>> {
        let mut files = self.files.borrow_mut();
        let fd = path.to_string();

        let file = if let Some(file) = files.get(path) {
            file.closed.set(false);
            file.clone()
        } else {
            let file = Arc::new(MemorySimFile::new(fd.clone()));
            files.insert(fd, file.clone());
            file
        };

        Ok(file)
    }

    fn step(&self) -> Result<()> {
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
