pub mod bugbase;
pub mod cli;
pub mod clock;
pub mod differential;
pub mod doublecheck;
pub mod env;
pub mod execution;
#[allow(dead_code)]
pub mod file;
pub mod io;
pub mod memory;
pub mod watch;

pub trait SimIO: turso_core::IO {
    fn inject_fault(&self, fault: bool);

    fn print_stats(&self);

    fn syncing(&self) -> bool;

    fn close_files(&self);
}
