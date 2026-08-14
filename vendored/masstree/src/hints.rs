//! Branch prediction hints for performance-critical paths.

#[inline(always)]
#[cold]
const fn cold_path() {}

/// Hint that a condition is likely to be true.
#[inline(always)]
#[must_use]
pub const fn likely(b: bool) -> bool {
    if !b {
        cold_path();
    }

    b
}

/// Hint that a condition is unlikely to be true.
#[inline(always)]
#[must_use]
pub const fn unlikely(b: bool) -> bool {
    if b {
        cold_path();
    }

    b
}
