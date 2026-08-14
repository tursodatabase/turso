//! Standard memory orderings for concurrent node access.
//!
//! These constants ensure consistent ordering usage across the codebase
//! and make the intent clear at each access point.

use std::sync::atomic::Ordering;

/// Ordering for reading node fields during optimistic traversal.
/// Pairs with writer's Release stores.
pub const READ_ORD: Ordering = Ordering::Acquire;

/// Ordering for writing node fields under lock.
/// Pairs with reader's Acquire loads.
pub const WRITE_ORD: Ordering = Ordering::Release;

/// Ordering for CAS success (compare-and-swap).
/// Used for `link_split`, root CAS, etc.
pub const CAS_SUCCESS: Ordering = Ordering::AcqRel;

/// Ordering for CAS failure.
/// Only need to see the current value.
pub const CAS_FAILURE: Ordering = Ordering::Acquire;

/// Ordering for relaxed loads (within locked region).
/// Safe because lock provides synchronization.
pub const RELAXED: Ordering = Ordering::Relaxed;
