//! Traits for key comparison in maps.

use core::cmp::Ordering;

use super::equivalent::{Comparable, Equivalent};

/// Key equality trait.
///
/// This trait allows for very flexible comparison of objects. You may
/// borrow/dereference `L` and `R` using `Borrow` or another trait. The trait
/// takes a `self` parameter, which you can use to change how the operands are
/// compared. For example, you can toggle string case-sensitivity on and off at
/// runtime, or you can use `Box<dyn Equivalator<L, R>>` to allow the user of
/// your code to supply a custom comparison function.
///
/// Implementations of `Equivalator` don't affect the inherent `Eq` or `Hash`
/// implementations for a type. Currently there is no companion trait that
/// allows for custom hashing the way `Equivalator` allows for custom
/// comparison.
///
/// See also `Comparator`.
///
/// ## Example
/// ```
/// use turso_core::skiplist::comparator::Equivalator;
///
/// struct MyEquivalator {
///     case_sensitive: bool,
/// }
///
/// impl Equivalator<[u8], [u8]> for MyEquivalator {
///     fn equivalent(&self, lhs: &[u8], rhs: &[u8]) -> bool {
///         if self.case_sensitive {
///             lhs == rhs
///         } else {
///             // Case-insensitive ASCII comparison on raw bytes
///             if lhs.len() != rhs.len() { return false; }
///             for (&c1, &c2) in lhs.iter().zip(rhs.iter()) {
///                 let c1 = if c1 >= 0x61 && c1 <= 0x7a { c1 - 32 } else { c1 };
///                 let c2 = if c2 >= 0x61 && c2 <= 0x7a { c2 - 32 } else { c2 };
///                 if c1 != c2 { return false; }
///             }
///             true
///         }
///     }
/// }
/// ```
pub trait Equivalator<L: ?Sized, R: ?Sized = L> {
    /// Compare `lhs` to `rhs` for equality.
    fn equivalent(&self, lhs: &L, rhs: &R) -> bool;
}

/// Key ordering trait.
///
/// This trait allows for very flexible comparison of objects. You may
/// borrow/dereference `L` and `R` using `Borrow` or another trait. The trait
/// takes a `self` parameter, which you can use to change how the operands are
/// compared. For example, you can toggle string case-sensitivity on and off,
/// or you can use `Box<dyn Comparator<L, R>>` to allow the user of your code
/// to supply a custom comparison function.
///
/// The `Comparator` and `Equivalator` implementations for a comparator must
/// agree on which inputs are equal.
///
/// See also `Equivalator`.
pub trait Comparator<L: ?Sized, R: ?Sized = L>: Equivalator<L, R> {
    /// Compare `lhs` to `rhs` and return their ordering.
    fn compare(&self, lhs: &L, rhs: &R) -> Ordering;
}

/// This comparator uses the `Equivalent` and `Comparable` traits to perform
/// comparisons, which themselves fall back on the standard library `Borrow`
/// and `Ord` traits. When used in a map, this results in the same behavior as
/// the standard `BTreeMap` interface.
#[derive(Clone, Copy, Debug, Default)]
pub struct BasicComparator;

impl<K: ?Sized, Q: ?Sized> Equivalator<K, Q> for BasicComparator
where
    K: Equivalent<Q>,
{
    #[inline]
    fn equivalent(&self, lhs: &K, rhs: &Q) -> bool {
        <K as Equivalent<Q>>::equivalent(lhs, rhs)
    }
}

impl<K: ?Sized, Q: ?Sized> Comparator<K, Q> for BasicComparator
where
    K: Comparable<Q>,
{
    #[inline]
    fn compare(&self, lhs: &K, rhs: &Q) -> Ordering {
        <K as Comparable<Q>>::compare(lhs, rhs)
    }
}
