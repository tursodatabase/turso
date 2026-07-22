//! In-memory file system for simulation.
//!
//! Provides a memory-backed IO implementation for deterministic simulation.

pub mod file;
pub mod io;

pub use file::MemorySimFile;
pub use io::{MemorySimIO, SimIO};
