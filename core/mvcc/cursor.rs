use crate::alloc::TursoAllocator;
#[cfg(any(test, injected_yields))]
use crate::mvcc::database::MVTableId;
use crate::mvcc::database::RowVersions;
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::skiplist::{comparator::BasicComparator, map::Entry};
#[cfg(any(test, injected_yields))]
use strum::EnumCount;

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount)]
#[repr(u8)]
pub(crate) enum CursorYieldPoint {
    NextStart,
    NextBtreeAdvance,
    PrevBtreeAdvance,
    SeekStart,
    SeekBtreeProgress,
    /// No cursor yields here any more. The variant keeps the ordinals of the
    /// points after it, which simulator plans store.
    #[allow(dead_code)]
    ExistsBtreeFallback,
    CountProgress,
    AdvanceBtreeForwardProgress,
    AdvanceBtreeBackwardProgress,
}

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for CursorYieldPoint {
    const POINT_COUNT: u8 = Self::COUNT as u8;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

#[cfg(any(test, injected_yields))]
pub(crate) fn cursor_yield_key(tx_id: u64, table_id: MVTableId) -> u64 {
    // ASCII-ish "CURSORCR"
    // any large number will do
    const CURSOR_SELECTION_TAG: u64 = 0x4355_5253_4F52_4352;
    // Mix tx/table identity and add a per-family tag (here Cursor tag), so that we get a nice
    // yield plans
    // 17 here is arbitrary, any number would do.
    tx_id ^ (i64::from(table_id) as u64).rotate_left(17) ^ CURSOR_SELECTION_TAG
}

pub(crate) type MvccEntry<'l, T, A = TursoAllocator> =
    Entry<'l, T, RowVersions<A>, BasicComparator, A>;

pub(crate) type MvccIterator<'l, T, A = TursoAllocator> =
    Box<dyn Iterator<Item = MvccEntry<'l, T, A>> + Send + Sync>;

/// Extends the lifetime of a SkipMap iterator to `'static`.
///
/// # Why a macro instead of a function?
///
/// Rust's `crate::skiplist::map::Entry<'a, K, V>` is *invariant* over `K`, meaning
/// the lifetime `'a` cannot be coerced through a function boundary. When we try to pass
/// `Box<dyn Iterator<Item = Entry<'_, K, V>>>` to a function expecting a generic lifetime,
/// the compiler cannot unify the lifetimes across the function call.
///
/// A macro expands inline at the call site, avoiding the function boundary entirely and
/// allowing the explicit transmute with both source and destination types specified.
///
/// # Safety
///
/// The caller must ensure that the underlying `SkipMap` from which the iterator was created
/// outlives the returned iterator. This is guaranteed when:
/// - For table iterators: The `MvStore.rows` SkipMap is held in an `Arc<MvStore>` that
///   outlives the cursor.
/// - For index iterators: The `MvStore.index_rows` SkipMap is held in an `Arc<MvStore>`
///   that outlives the cursor.
macro_rules! static_iterator_hack {
    ($iter:expr, $key_type:ty) => {
        static_iterator_hack!($iter, $key_type, crate::alloc::TursoAllocator)
    };
    ($iter:expr, $key_type:ty, $alloc:ty) => {
        // SAFETY: See macro documentation above.
        unsafe {
            std::mem::transmute::<
                Box<
                    dyn Iterator<Item = crate::mvcc::cursor::MvccEntry<'_, $key_type, $alloc>>
                        + Send
                        + Sync,
                >,
                Box<
                    dyn Iterator<Item = crate::mvcc::cursor::MvccEntry<'static, $key_type, $alloc>>
                        + Send
                        + Sync,
                >,
            >($iter)
        }
    };
}

pub(crate) use static_iterator_hack;
