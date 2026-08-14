//! Value retirement wrapper.
//!
//! This module provides [`BatchedRetire`], a thin wrapper around seize's retirement
//! that provides a consistent API for retirement and flushing across the codebase.

use seize::{Guard, LocalGuard};

/// Value retirement utilities.
#[derive(Debug)]
pub struct BatchedRetire;

impl BatchedRetire {
    /// Flush pending retirements to start reclamation process.
    #[inline(always)]
    pub fn flush(guard: &LocalGuard<'_>) {
        guard.flush();
    }
}
