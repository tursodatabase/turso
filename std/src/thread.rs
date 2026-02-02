// Copyright 2023-2025 the Limbo authors. All rights reserved. MIT license.

//! Thread primitives that switch between std and shuttle implementations.

#[cfg(shuttle)]
pub use shuttle_adapter::*;

#[cfg(not(shuttle))]
pub use std_adapter::*;

#[cfg(shuttle)]
mod shuttle_adapter {
    pub use shuttle::hint::spin_loop;
    pub use shuttle::thread::{
        current, panicking, park, scope, sleep, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };
    pub use shuttle::thread_local;
}

#[cfg(not(shuttle))]
mod std_adapter {
    pub use std::hint::spin_loop;
    pub use std::thread::{
        current, panicking, park, scope, sleep, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };
    pub use std::thread_local;
}
