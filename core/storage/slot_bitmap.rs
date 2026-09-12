use crate::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::turso_assert;

/// Shared constants for bitmap operations.
const WORD_SHIFT: u32 = 6;
const WORD_BITS: u32 = 64;
const WORD_MASK: u32 = 63;

const ALL_FREE: u64 = u64::MAX;
const ALL_ALLOCATED: u64 = 0u64;

#[inline]
const fn word_and_bit_to_slot(word_idx: usize, bit: u32) -> u32 {
    (word_idx as u32) << WORD_SHIFT | bit
}

#[inline]
const fn slot_to_word_and_bit(slot_idx: u32) -> (usize, u32) {
    ((slot_idx >> WORD_SHIFT) as usize, slot_idx & WORD_MASK)
}

/// Lock-free atomic bitmap for tracking allocated slots in an arena.
///
/// Bit meaning:
/// - 1 = free
/// - 0 = allocated
///
/// `alloc_one` is lock-free (CAS retry bounded by contention, not blocking).
/// `free_one` is wait-free (single `fetch_or`).
pub(super) struct AtomicSlotBitmap {
    words: Box<[AtomicU64]>,
    n_slots: u32,
    /// Performance hint for where to start scanning. Not correctness-critical.
    next_word_hint: AtomicUsize,
}

// SAFETY: All fields are atomics or immutable after construction.
unsafe impl Send for AtomicSlotBitmap {}
unsafe impl Sync for AtomicSlotBitmap {}

impl std::fmt::Debug for AtomicSlotBitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicSlotBitmap")
            .field("n_slots", &self.n_slots)
            .finish()
    }
}

impl AtomicSlotBitmap {
    /// Creates a new `AtomicSlotBitmap` capable of tracking `n_slots` slots.
    /// All slots start as free.
    pub fn new(n_slots: u32) -> Self {
        turso_assert!(n_slots > 0, "number of slots must be non-zero");
        turso_assert!(
            n_slots % WORD_BITS == 0,
            "number of slots in map must be a multiple of 64"
        );
        let n_words = (n_slots / WORD_BITS) as usize;
        let words: Vec<AtomicU64> = (0..n_words).map(|_| AtomicU64::new(ALL_FREE)).collect();
        Self {
            words: words.into_boxed_slice(),
            n_slots,
            next_word_hint: AtomicUsize::new(0),
        }
    }

    /// Returns whether a slot is currently free (snapshot, may be stale).
    pub fn is_free(&self, slot: u32) -> bool {
        if slot >= self.n_slots {
            return false;
        }
        let (word_idx, bit) = slot_to_word_and_bit(slot);
        (self.words[word_idx].load(Ordering::Acquire) & (1u64 << bit)) != 0
    }

    /// Allocates a single free slot from the bitmap. Lock-free.
    ///
    /// Returns `Some(slot_index)` on success, `None` if all slots are allocated.
    pub fn alloc_one(&self) -> Option<u32> {
        let n_words = self.words.len();
        if n_words == 0 {
            return None;
        }

        let hint = self.next_word_hint.load(Ordering::Acquire).min(n_words - 1);

        // Scan from hint to end, then wrap around from 0 to hint.
        for offset in 0..n_words {
            let word_idx = (hint + offset) % n_words;
            let mut word = self.words[word_idx].load(Ordering::Acquire);

            while word != ALL_ALLOCATED {
                let bit = word.trailing_zeros();
                let new_word = word & !(1u64 << bit);

                match self.words[word_idx].compare_exchange_weak(
                    word,
                    new_word,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Update hint: if word is now fully allocated, advance past it.
                        let new_hint = if new_word == ALL_ALLOCATED {
                            (word_idx + 1) % n_words
                        } else {
                            word_idx
                        };
                        self.next_word_hint.store(new_hint, Ordering::Release);
                        return Some(word_and_bit_to_slot(word_idx, bit));
                    }
                    Err(actual) => {
                        // CAS failed — another thread changed this word. Retry with fresh value.
                        word = actual;
                    }
                }
            }
        }
        None
    }

    /// Frees a previously allocated slot. Wait-free (single atomic op).
    pub fn free_one(&self, slot: u32) {
        turso_assert!(slot < self.n_slots, "free_one out of bounds");
        let (word_idx, bit) = slot_to_word_and_bit(slot);
        let mask = 1u64 << bit;
        let old = self.words[word_idx].fetch_or(mask, Ordering::Release);
        debug_assert!((old & mask) == 0, "double-free detected for slot {slot}");
        // If this word is before the current hint, pull the hint back.
        let hint = self.next_word_hint.load(Ordering::Acquire);
        if word_idx < hint {
            self.next_word_hint.store(word_idx, Ordering::Release);
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/storage/slot_bitmap/tests.rs"]
pub mod tests;
