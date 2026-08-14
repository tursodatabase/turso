//! Entry API for conditional key access
//!
//! Provides [`Entry`], [`OccupiedEntry`], and [`VacantEntry`] types for
//! ergonomic conditional insertion and modification patterns.

use std::fmt::{self as StdFmt, Debug, Formatter};

use seize::LocalGuard;

use crate::{MassTreeGeneric, TreeAllocator, policy::LeafPolicy, tree::RemoveError};

/// Result type for [`OccupiedEntry::try_remove_entry`].
pub type RemoveEntryResult<O> = Result<Option<(Vec<u8>, O)>, RemoveError>;

/// A view into a single entry in a tree, which may either be vacant or occupied.
///
/// # Concurrency
///
/// Classification (Occupied vs Vacant) is a point-in-time snapshot taken
/// without holding a lock. A concurrent insert or remove between
/// classification and the subsequent mutation can cause:
///
/// - `and_modify`: lost updates (computation based on stale value).
/// - `VacantEntry::insert`: silent overwrite of a concurrently inserted key.
///
/// For atomic read-modify-write, use `insert_with_guard` directly or an
/// external synchronization strategy.
///
/// # Example
///
/// ```rust
/// use masstree::MassTree;
///
/// let tree: MassTree<u64> = MassTree::new();
/// let guard = tree.guard();
///
/// // Insert if absent, get value either way
/// let _value = tree.entry_with_guard(b"counter", &guard)
///     .or_insert(0);
///
/// // Modify existing or insert default
/// let _value = tree.entry_with_guard(b"counter", &guard)
///     .and_modify(|v| v + 1)
///     .or_insert(0);
/// ```
pub enum Entry<'t, 'e, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// An occupied entry (key exists in tree at classification time).
    Occupied(OccupiedEntry<'t, 'e, P, A>),

    /// A vacant entry (key does not exist classification time).
    Vacant(VacantEntry<'t, 'e, P, A>),
}

/// A view into an occupied entry in a tree.
pub struct OccupiedEntry<'t, 'e, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// The key (borrowed from caller, zero allocation).
    key: &'e [u8],

    /// The current value (snapshot at entry creation time).
    value: P::Output,

    /// Reference to the tree for insertion.
    tree: &'t MassTreeGeneric<P, A>,

    /// Guard for concurrent access.
    guard: &'e LocalGuard<'t>,
}

/// A view into a vacant entry in a tree.
pub struct VacantEntry<'t, 'e, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// The key (borrowed from caller, zero alloc)
    key: &'e [u8],

    /// Reference to the tree for insertion.
    tree: &'t MassTreeGeneric<P, A>,

    /// Guard for concurrent access.
    guard: &'e LocalGuard<'t>,
}

// ============================================================================
//  Entry Implementation
// ============================================================================

impl<'t, 'e, P, A> Entry<'t, 'e, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Returns the key for this entry.
    #[must_use]
    #[inline(always)]
    pub const fn key(&self) -> &[u8] {
        match self {
            Entry::Occupied(o) => o.key(),

            Entry::Vacant(v) => v.key(),
        }
    }

    /// Inserts a default value if the entry is vacant, then returns
    /// the entry's value.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use masstree::MassTree;
    /// # let tree: MassTree<u64> = MassTree::new();
    /// # let guard = tree.guard();
    /// let value = tree.entry_with_guard(b"key", &guard).or_insert(42);
    /// ```
    /// Returns the entry's value if occupied, or inserts the default and returns it.
    #[inline(always)]
    pub fn or_insert(self, default: P::Value) -> P::Output {
        match self {
            Entry::Occupied(o) => o.into_value(),
            Entry::Vacant(v) => v.insert(default),
        }
    }

    /// Returns the entry's value if occupied, or computes and inserts a default.
    #[inline(always)]
    pub fn or_insert_with<F>(self, default: F) -> P::Output
    where
        F: FnOnce() -> P::Value,
    {
        match self {
            Entry::Occupied(o) => o.into_value(),
            Entry::Vacant(v) => v.insert(default()),
        }
    }

    /// Returns the entry's value if occupied, or computes a default from the key.
    #[inline(always)]
    pub fn or_insert_with_key<F>(self, default: F) -> P::Output
    where
        F: FnOnce(&[u8]) -> P::Value,
    {
        match self {
            Entry::Occupied(o) => o.into_value(),
            Entry::Vacant(v) => {
                let value: P::Value = default(v.key());
                v.insert(value)
            }
        }
    }

    /// Inserts the default value if vacant, returns the entry's value.
    pub fn or_default(self) -> P::Output
    where
        P::Value: Default,
    {
        self.or_insert(Default::default())
    }

    /// Modifies the value if occupied using the provided function.
    ///
    /// The function receives a snapshot of the current value. Under concurrent
    /// writes, the snapshot may be stale and the resulting store can overwrite
    /// a newer value. See [`Entry`] concurrency notes.
    #[must_use]
    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&P::Output) -> P::Value,
    {
        match self {
            Entry::Occupied(mut o) => {
                let new_value: P::Value = f(&o.value);
                let new_output: P::Output = P::into_output(new_value);
                let return_output: P::Output = new_output.clone();

                let _old = o.tree.insert_output_with_guard(o.key, new_output, o.guard);
                o.value = return_output;

                Entry::Occupied(o)
            }

            Entry::Vacant(v) => Entry::Vacant(v),
        }
    }

    /// Inserts a value and returns an `OccupiedEntry`.
    #[inline]
    pub fn insert_entry(self, value: P::Value) -> OccupiedEntry<'t, 'e, P, A> {
        match self {
            Entry::Occupied(mut o) => {
                o.insert(value);
                o
            }

            Entry::Vacant(v) => {
                let key: &[u8] = v.key;
                let tree: &MassTreeGeneric<P, A> = v.tree;
                let guard: &LocalGuard<'_> = v.guard;

                // Convert to output before insert
                let output: P::Output = P::into_output(value);
                let return_output: P::Output = output.clone();
                let _old = tree.insert_output_with_guard(key, output, guard);

                OccupiedEntry {
                    key,
                    value: return_output,
                    tree,
                    guard,
                }
            }
        }
    }

    /// Returns a reference to the value if occupied, or `None` if vacant.
    #[must_use]
    #[inline(always)]
    pub const fn get(&self) -> Option<&P::Output> {
        match self {
            Entry::Occupied(o) => Some(o.get()),

            Entry::Vacant(_) => None,
        }
    }
}

// ============================================================================
//  OccupiedEntry Implementation
// ============================================================================

impl<P, A> OccupiedEntry<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Gets a reference to the key in the entry.
    #[must_use]
    #[inline(always)]
    pub const fn key(&self) -> &[u8] {
        self.key
    }

    /// Gets a reference to the value in the entry.
    #[inline(always)]
    pub const fn get(&self) -> &P::Output {
        &self.value
    }

    /// Converts the [`OccupiedEntry`] into the value
    #[inline(always)]
    pub fn into_value(self) -> P::Output {
        self.value
    }

    /// Sets the value of the entry, and returns the old value.
    #[inline(always)]
    pub fn insert(&mut self, value: P::Value) -> Option<P::Output> {
        let output = P::into_output(value);
        let old = self
            .tree
            .insert_output_with_guard(self.key, output.clone(), self.guard);

        self.value = output;

        old
    }

    /// Removes the entry from the tree and returns the actually removed value.
    ///
    /// # Panics
    ///
    /// Panics if removal fails (extremely rare - only on retry limit exceeded).
    /// Use [`try_remove`](Self::try_remove) for fallible operation.
    #[inline(always)]
    #[expect(
        clippy::panic,
        reason = "Convenience wrapper; use try_remove for fallible version"
    )]
    pub fn remove(self) -> Option<P::Output> {
        match self.try_remove() {
            Ok(value) => value,

            Err(e) => panic!("OccupiedEntry::remove failed: {e:?}"),
        }
    }

    /// Fallible version of [`remove`](Self::remove)
    ///
    /// # Errors
    ///
    /// Returns error if removal failed
    #[inline(always)]
    pub fn try_remove(self) -> Result<Option<P::Output>, RemoveError> {
        self.tree.remove_with_guard(self.key, self.guard)
    }

    /// Removes the entry from the tree and returns the key and removed value.
    ///
    /// # Panics
    ///
    /// Panics if removal fails. Use [`try_remove_entry`](Self::try_remove_entry)
    /// for fallible operation.
    #[inline(always)]
    #[expect(
        clippy::panic,
        reason = "Convenience wrapper; use try_remove_entry for fallible version"
    )]
    pub fn remove_entry(self) -> Option<(Vec<u8>, P::Output)> {
        match self.try_remove_entry() {
            Ok(result) => result,

            Err(e) => panic!("OccupiedEntry::remove_entry failed: {e:?}"),
        }
    }

    /// Fallible version of [`remove_entry`](Self::remove_entry).
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying tree operation encounters a failure.
    #[inline(always)]
    pub fn try_remove_entry(self) -> RemoveEntryResult<P::Output> {
        let key_owned: Vec<u8> = self.key.to_vec();

        match self.tree.remove_with_guard(self.key, self.guard)? {
            Some(value) => Ok(Some((key_owned, value))),
            None => Ok(None),
        }
    }
}

// ============================================================================
//  VacantEntry Implementation
// ============================================================================

impl<P, A> VacantEntry<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Gets a reference to the key that would be used when inserting.
    #[must_use]
    #[inline(always)]
    pub const fn key(&self) -> &[u8] {
        self.key
    }

    /// Consumes the entry and returns the key as an owned [`Vec<u8>`]
    #[must_use]
    #[inline(always)]
    pub fn into_key(self) -> Vec<u8> {
        self.key.to_vec()
    }

    /// Inserts the value into the vacant entry and returns it.
    #[inline(always)]
    pub fn insert(self, value: P::Value) -> P::Output {
        // Convert to output before insert, clone for return value
        let output = P::into_output(value);
        let return_output = output.clone();

        // Insert using internal method that accepts P::Output
        // The return value (old value if any) is ignored since we're in a VacantEntry
        let _old = self
            .tree
            .insert_output_with_guard(self.key, output, self.guard);
        return_output
    }
}

// ============================================================================
//  Debug Implementations
// ============================================================================

impl<P, A> Debug for Entry<'_, '_, P, A>
where
    P: LeafPolicy,
    P::Output: Send + Sync + Debug,
    A: TreeAllocator<P>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        match self {
            Entry::Occupied(o) => f.debug_tuple("Occupied").field(&o.value).finish(),

            Entry::Vacant(v) => f.debug_tuple("Vacant").field(&v.key).finish(),
        }
    }
}

impl<P, A> Debug for OccupiedEntry<'_, '_, P, A>
where
    P: LeafPolicy,
    P::Output: Send + Sync + Debug,
    A: TreeAllocator<P>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("OccupiedEntry")
            .field("key", &self.key)
            .field("value", &self.value)
            .finish()
    }
}

impl<P, A> Debug for VacantEntry<'_, '_, P, A>
where
    P: LeafPolicy,
    P::Output: Send + Sync + Debug,
    A: TreeAllocator<P>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("VacantEntry")
            .field("key", &self.key)
            .finish()
    }
}

// ============================================================================
//  Constructor (internal)
// ============================================================================

impl<'t, 'e, P, A> Entry<'t, 'e, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Create an Entry by looking up the key.
    #[inline]
    pub(crate) fn new(
        tree: &'t MassTreeGeneric<P, A>,
        key: &'e [u8],
        guard: &'e LocalGuard<'t>,
    ) -> Self {
        match tree.get_with_guard(key, guard) {
            Some(value) => Entry::Occupied(OccupiedEntry {
                key,
                value,
                tree,
                guard,
            }),
            None => Entry::Vacant(VacantEntry { key, tree, guard }),
        }
    }
}

