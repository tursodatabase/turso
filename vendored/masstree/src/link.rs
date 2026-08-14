//! Pointer marking utilities for concurrent split operations.
//!
//! Provides provenance-safe pointer marking using the LSB for split signaling.

const MARK_BIT: usize = 1;

/// Pointer marking/linking utilities for concurrent split operations.
#[derive(Debug)]
pub struct Linker;

impl Linker {
    /// Set the mark bit to signal split-in-progress (provenance-safe).
    #[inline(always)]
    pub fn mark_ptr<T>(p: *mut T) -> *mut T {
        p.map_addr(|a: usize| a | MARK_BIT)
    }

    /// Clear the mark bit (provenance-safe).
    #[inline(always)]
    pub fn unmark_ptr<T>(p: *mut T) -> *mut T {
        p.map_addr(|a: usize| a & !MARK_BIT)
    }

    /// Check if pointer has mark bit set (split-in-progress).
    #[inline(always)]
    pub fn is_marked<T>(p: *mut T) -> bool {
        p.addr() & MARK_BIT != 0
    }
}
