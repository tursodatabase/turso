//! Software prefetching utilities for cache optimization.

/// Prefetch data for reading into all cache levels (temporal).
#[inline(always)]
#[allow(clippy::missing_const_for_fn)]
pub fn prefetch_read<T>(ptr: *const T) {
    #[cfg(feature = "no-prefetch")]
    {
        let _ = ptr;
    }

    #[cfg(all(not(feature = "no-prefetch"), target_arch = "x86_64"))]
    {
        // SAFETY: _mm_prefetch is always safe to call.
        unsafe {
            std::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), std::arch::x86_64::_MM_HINT_T0);
        }
    }

    #[cfg(all(not(feature = "no-prefetch"), target_arch = "aarch64"))]
    {
        // SAFETY: PRFM is always safe - it's a hint that doesn't fault on invalid addresses.
        unsafe {
            std::arch::asm!(
                "prfm pldl1keep, [{ptr}]",
                ptr = in(reg) ptr,
                options(nostack, preserves_flags),
            );
        }
    }

    #[cfg(not(any(
        feature = "no-prefetch",
        target_arch = "x86_64",
        target_arch = "aarch64"
    )))]
    {
        let _ = ptr;
    }
}

/// Prefetch data for writing into all cache levels.
#[inline(always)]
#[allow(dead_code)]
#[allow(clippy::missing_const_for_fn)]
pub fn prefetch_write<T>(ptr: *mut T) {
    #[cfg(feature = "no-prefetch")]
    #[expect(clippy::needless_return, reason = "Feature gate compatibility")]
    {
        let _ = ptr;
        return;
    }

    #[cfg(all(not(feature = "no-prefetch"), target_arch = "x86_64"))]
    {
        // SAFETY: _mm_prefetch is always safe to call.
        unsafe {
            std::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), std::arch::x86_64::_MM_HINT_ET0);
        }
    }

    #[cfg(all(not(feature = "no-prefetch"), target_arch = "aarch64"))]
    {
        // SAFETY: PRFM is always safe - it's a hint that doesn't fault on invalid addresses.
        unsafe {
            std::arch::asm!(
                "prfm pstl1keep, [{ptr}]",
                ptr = in(reg) ptr,
                options(nostack, preserves_flags),
            );
        }
    }

    #[cfg(not(any(
        feature = "no-prefetch",
        target_arch = "x86_64",
        target_arch = "aarch64"
    )))]
    {
        let _ = ptr;
    }
}
