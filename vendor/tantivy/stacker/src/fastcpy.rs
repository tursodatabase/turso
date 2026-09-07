/// Optimized copy for small sizes. All bounds checks are elided.
/// Avoids call to memcpy
/// Applies unbranched copy trick for sizes 8, 16, 32
///
/// src and dst must be num_bytes long.
#[inline]
pub fn fast_short_slice_copy(src: &[u8], dst: &mut [u8]) {
    #[inline(never)]
    #[cold]
    #[track_caller]
    fn len_mismatch_fail(dst_len: usize, src_len: usize) -> ! {
        panic!(
            "source slice length ({}) does not match destination slice length ({})",
            src_len, dst_len,
        );
    }

    if src.len() != dst.len() {
        len_mismatch_fail(src.len(), dst.len());
    }
    let len = src.len();

    if src.is_empty() {
        return;
    }

    if len < 4 {
        short_copy(src, dst);
        return;
    }

    if len < 8 {
        double_copy_trick::<4>(src, dst);
        return;
    }

    if len <= 16 {
        double_copy_trick::<8>(src, dst);
        return;
    }

    if len <= 32 {
        double_copy_trick::<16>(src, dst);
        return;
    }

    // The code will use the vmovdqu instruction to copy 32 bytes at a time.
    #[cfg(target_feature = "avx")]
    {
        if len <= 64 {
            double_copy_trick::<32>(src, dst);
            return;
        }
    }

    // For larger sizes we use the default, which calls memcpy
    // memcpy does some virtual memory tricks to copy large chunks of memory.
    //
    // The theory should be that the checks above don't cost much relative to the copy call for
    // larger copies.
    // The bounds checks in `copy_from_slice` are elided.
    dst.copy_from_slice(src);
}

#[inline(always)]
fn short_copy(src: &[u8], dst: &mut [u8]) {
    debug_assert_ne!(src.len(), 0);
    debug_assert_eq!(src.len(), dst.len());
    let len = src.len();

    // length 1-3
    dst[0] = src[0];
    if len >= 2 {
        double_copy_trick::<2>(src, dst);
    }
}

#[inline(always)]
fn double_copy_trick<const SIZE: usize>(src: &[u8], dst: &mut [u8]) {
    debug_assert!(src.len() >= SIZE);
    debug_assert!(dst.len() >= SIZE);
    dst[0..SIZE].copy_from_slice(&src[0..SIZE]);
    dst[src.len() - SIZE..].copy_from_slice(&src[src.len() - SIZE..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copy_test<const SIZE: usize>() {
        let src: Vec<u8> = (0..SIZE as u8).collect();
        let mut dst = [0u8; SIZE];
        fast_short_slice_copy(&src, &mut dst);
        assert_eq!(src, dst);
    }

    #[test]
    fn copy_test_n() {
        copy_test::<1>();
        copy_test::<2>();
        copy_test::<3>();
        copy_test::<4>();
        copy_test::<5>();
        copy_test::<6>();
        copy_test::<7>();
        copy_test::<8>();
        copy_test::<9>();
        copy_test::<10>();
        copy_test::<11>();
        copy_test::<31>();
        copy_test::<32>();
        copy_test::<33>();
        copy_test::<47>();
        copy_test::<48>();
        copy_test::<49>();
    }
}
