// Copyright 2023-2025 the Limbo authors. All rights reserved. MIT license.

//! Standard library primitives for Turso with shuttle support for deterministic testing.
//!
//! This crate provides thread and synchronization primitives that can be swapped
//! between standard library implementations and shuttle implementations for
//! deterministic concurrency testing.

#[cfg(feature = "future")]
pub mod future;
pub mod sync;
pub mod thread;
