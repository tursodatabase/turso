pub mod bugbase;
pub mod cli;
pub mod clock;
pub mod differential;
pub mod doublecheck;
pub mod env;
pub mod execution;
#[expect(dead_code)]
pub mod file;
pub mod io;
pub mod memory;

pub const FAULT_ERROR_MSG: &str = "Injected Fault";

/// IO event that affects durability tracking.
/// Used to precisely track what data is durable after each IO operation.
#[derive(Debug, Clone)]
pub enum DurableIOEvent {
    Sync {
        file_path: String,
        /// True if file had content before sync (for WAL: had frames). else false.
        had_content: bool,
        durable_size: usize,
    },
    TruncateSynced { file_path: String, new_len: usize },
}

pub const WAL_HEADER_SIZE: usize = 32;

/// turso have no shm - todo when we support it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Database,
    Wal,
    Other,
}

impl FileType {
    pub fn from_path(path: &str) -> Self {
        if path.ends_with(".db") {
            FileType::Database
        } else if path.ends_with("-wal") {
            FileType::Wal
        } else {
            FileType::Other
        }
    }
}

pub trait SimIO: turso_core::IO {
    fn inject_fault(&self, fault: bool);

    fn print_stats(&self);

    fn syncing(&self) -> bool;

    fn close_files(&self);

    fn persist_files(&self) -> anyhow::Result<()>;

    fn has_crashed(&self) -> bool {
        false
    }

    /// after this, only durable data remains. Default: no-op.
    fn discard_all_pending(&self) {}

    /// this provides precise tracking of what data is durable after each IO operation.
    fn take_durable_events(&self) -> Vec<DurableIOEvent> {
        Vec::new()
    }
}
