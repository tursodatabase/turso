use crate::leaf15::WIDTH_15;
use crate::policy::LeafPolicy;
use crate::{LeafNode15, TreeInternode};

pub struct Scalar;

impl Scalar {
    /// Scalar upper bound search (simple loop).
    ///
    /// Fallback for non-x86_64 or when AVX2 is unavailable.
    #[inline(always)]
    pub fn upper_bound_internode_scalar<I: TreeInternode>(search_ikey: u64, node: &I) -> usize {
        let size: usize = node.nkeys();

        for i in 0..size {
            let k: u64 = node.ikey_relaxed(i);

            if search_ikey < k {
                return i;
            }

            if search_ikey == k {
                return i + 1;
            }
        }

        size
    }

    // ============================================================================
    //  Leaf ikey Matching (Scalar)
    // ============================================================================

    /// Find all slots in a [`LeafNode15`] where `ikey == target_ikey`.
    ///
    /// Returns a bitmask where bit `i` is set if `leaf.ikey(i) == target_ikey`.
    #[must_use]
    #[inline(always)]
    pub fn find_ikey_matches_leaf15<P: LeafPolicy>(target_ikey: u64, leaf: &LeafNode15<P>) -> u32 {
        let mut mask: u32 = 0;

        for i in 0..WIDTH_15 {
            if leaf.ikey(i) == target_ikey {
                mask |= 1 << i;
            }
        }

        mask
    }
}
