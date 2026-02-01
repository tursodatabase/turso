//! Shuttle-aware sync primitives for deterministic testing.
//!
//! Under Shuttle, this module re-exports Shuttle's sync primitives which are
//! controlled by the scheduler. In production, it re-exports std::sync.

#[cfg(shuttle)]
pub(crate) use shuttle_adapter::*;

#[cfg(not(shuttle))]
pub(crate) use std_adapter::*;

#[cfg(shuttle)]
mod shuttle_adapter {
    pub mod atomic {
        pub use shuttle::sync::atomic::{AtomicU8, Ordering};
    }
    pub use shuttle::sync::Mutex;
    pub use std::sync::Arc;
}

#[cfg(not(shuttle))]
mod std_adapter {
    pub mod atomic {
        pub use std::sync::atomic::{AtomicU8, Ordering};
    }
    pub use std::sync::{Arc, Mutex};
}
