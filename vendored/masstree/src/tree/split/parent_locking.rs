//! Parent membership validation.

use crate::internode::InternodeNode;

/// Unit struct namespace for parent validation operations.
pub struct ParentLocking;

impl ParentLocking {
    /// Validate that child is still in parent by pointer scan.
    ///
    /// Returns the child's index if found, `None` if the child has been moved
    /// (concurrent split changed the parent).
    #[inline(always)]
    pub fn validate_membership(parent: &InternodeNode, child_ptr: *mut u8) -> Option<usize> {
        let nkeys: usize = parent.nkeys();

        // SAFETY: Parent is locked - no concurrent retirement of children.
        (0..=nkeys).find(|i: &usize| unsafe { parent.child_unguarded(*i) } == child_ptr)
    }
}
