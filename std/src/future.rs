// Copyright 2023-2025 the Limbo authors. All rights reserved. MIT license.

//! Async primitives that switch between tokio and shuttle implementations.
//!
//! This module provides async task spawning and execution primitives that
//! work with both tokio (production) and shuttle (deterministic testing).

#[cfg(shuttle)]
pub use shuttle_adapter::*;

#[cfg(not(shuttle))]
pub use tokio_adapter::*;

#[cfg(shuttle)]
mod shuttle_adapter {
    pub use shuttle::future::{block_on, spawn, yield_now, AbortHandle, JoinError, JoinHandle};
}

#[cfg(not(shuttle))]
mod tokio_adapter {
    pub use tokio::task::{spawn, yield_now, AbortHandle, JoinError, JoinHandle};
}
