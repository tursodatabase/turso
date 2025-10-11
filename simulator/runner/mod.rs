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

pub trait SimIO: turso_core::IO {
    fn inject_fault(&self, fault: bool);

    fn print_stats(&self);

    fn syncing(&self) -> bool;

    fn close_files(&self);

    fn persist_files(&self) -> anyhow::Result<()>;
}
