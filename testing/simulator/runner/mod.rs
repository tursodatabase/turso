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
pub const DISK_FULL_ERROR_MSG: &str = "Injected Fault: disk full (ENOSPC)";

pub trait SimIO: turso_core::IO {
    fn inject_fault(&self, fault: bool);

    /// Inject faults selectively per database file.
    /// Each entry in `faults` is `(path_stem, should_fault)`.
    /// Files whose path contains a given stem get that fault setting.
    fn inject_fault_selective(&self, faults: &[(&str, bool)]);

    /// Toggle the "disk full" state for all files managed by this IO.
    /// While true, `pwrite`/`sync` return an ENOSPC-like error so the
    /// simulator can exercise error paths that real hosts hit when the
    /// underlying filesystem fills up. Reads continue to succeed.
    fn inject_disk_full(&self, full: bool);

    /// Whether the disk is currently in the "full" state.
    /// Used by the fault generator to decide whether to free the disk
    /// instead of filling it again.
    fn is_disk_full(&self) -> bool;

    fn print_stats(&self);

    fn syncing(&self) -> bool;

    fn close_files(&self);

    fn persist_files(&self) -> anyhow::Result<()>;
}
