use crate::leaf15::LeafNode15;
use crate::nodeversion::LockGuard;
use crate::policy::LeafPolicy;

use super::{LocalGuard, MassTreeGeneric, TreeAllocator};

/// Strategy trait for unifying the generic and write-through insert paths.
///
/// Two implementations exist:
/// - `GenericInsert`: carries `P::Output` through traversal (standard path)
/// - `WriteThroughInsert`: carries `P::Value`, deferring allocation until storage
///
/// All methods are `#[inline(always)]` to ensure monomorphization produces
/// identical machine code to the original hand-duplicated functions.
pub trait InsertStrategy<P: LeafPolicy, A: TreeAllocator<P>> {
    /// The value type carried through the OCC traversal loop.
    type Input;

    /// The old-value type returned when updating an existing key.
    type OldValue;

    /// Convert input to output for storage. Identity for `GenericInsert`,
    /// calls `P::into_output` for `WriteThroughInsert`.
    fn into_output(input: Self::Input) -> P::Output;

    /// Update an existing value in a locked slot. Returns the old value.
    fn update_existing(
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        slot: usize,
        input: &Self::Input,
        guard: &LocalGuard<'_>,
    ) -> Self::OldValue;
}

/// Standard insert strategy: input is `P::Output` (pre-allocated).
pub struct GenericInsert;

impl<P: LeafPolicy, A: TreeAllocator<P>> InsertStrategy<P, A> for GenericInsert {
    type Input = P::Output;
    type OldValue = P::Output;

    #[inline(always)]
    fn into_output(input: P::Output) -> P::Output {
        input
    }

    #[inline(always)]
    fn update_existing(
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        slot: usize,
        input: &P::Output,
        guard: &LocalGuard<'_>,
    ) -> P::Output {
        tree.update_existing_value(leaf, lock, slot, input, guard)
    }
}

/// Write-through insert strategy: input is `P::Value` (deferred allocation).
///
/// Only used when `P::CAN_WRITE_THROUGH` is true. Updates existing values
/// in place without Box allocation or EBR retirement.
pub struct WriteThroughInsert;

impl<P: LeafPolicy, A: TreeAllocator<P>> InsertStrategy<P, A> for WriteThroughInsert {
    type Input = P::Value;
    type OldValue = P::Value;

    #[inline(always)]
    fn into_output(input: P::Value) -> P::Output {
        P::into_output(input)
    }

    #[inline(always)]
    fn update_existing(
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        slot: usize,
        input: &P::Value,
        _guard: &LocalGuard<'_>,
    ) -> P::Value {
        tree.update_existing_value_write_through(leaf, lock, slot, input)
    }
}
