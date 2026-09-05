//! Adaptive quicksort for byte-orderable keys, after US 7,680,791
//! (Callaghan, Li, Waddington): a common prefix skipping quicksort and a
//! most significant digit radix sort that call each other, with a two-byte
//! key substring cache in every item.
//!
//! Every partition knows how many leading bytes all of its keys share (its
//! common prefix), so comparisons start after those bytes. The quicksort
//! step splits a partition into the keys less than, equal to, and greater
//! than a pivot, and while doing so records the common prefix of the "less
//! than" and "greater than" sets. Each of those sets then goes through the
//! radix step, which buckets the keys on the first byte after their common
//! prefix, and each bucket with more than one key goes back to the
//! quicksort step. Both steps keep the input order inside each group, so
//! equal keys come out in insertion order.
//!
//! An item stores the two key bytes that follow the common prefix of its
//! partition. Comparisons read those bytes from the item array and only
//! follow the record pointer when the cached bytes tie. The radix step moves
//! the cache forward when it advances the common prefix by two or more
//! bytes, and leaves it in place when the prefix grows by one byte, because
//! the second cached byte is still useful then.
//!
//! Two additions to the patent: the recursion runs on an explicit stack of
//! steps, so a long chain of partitions never touches the thread stack, and
//! input that is already in order (or in strictly reverse order) is
//! detected by one pass over the items and returned without partitioning.

use std::cmp::Ordering;

use crate::alloc::*;
use crate::{turso_assert, Result};

pub(crate) const CACHE_LEN: usize = 2;

pub(crate) trait AdaptiveSortItem: Copy {
    /// The conditioned key. Follows the record pointer.
    fn key(&self) -> &[u8];
    /// The key length, kept in the item so a comparison that runs into the
    /// end of a key does not follow the record pointer.
    fn key_len(&self) -> usize;
    /// The key substring cache: the key bytes at the common prefix of the
    /// item's partition, zero-filled past the end of the key.
    fn cache(&self) -> [u8; CACHE_LEN];
    fn set_cache(&mut self, cache: [u8; CACHE_LEN]);
}

/// The key substring cache for a key whose partition shares `prefix` bytes.
pub(crate) fn cache_at(key: &[u8], prefix: usize) -> [u8; CACHE_LEN] {
    let mut cache = [0u8; CACHE_LEN];
    let cached = key.get(prefix..).unwrap_or(&[]);
    for (slot, byte) in cache.iter_mut().zip(cached) {
        *slot = *byte;
    }
    cache
}

/// Sorts `items` by key, keeping the input order of equal keys. The caches
/// of the items must hold the first two bytes of their keys.
pub(crate) fn adaptive_sort<T: AdaptiveSortItem>(items: &mut [T]) -> Result<()> {
    let len = items.len();
    if len < 2 {
        return Ok(());
    }
    match input_order(items) {
        InputOrder::Ascending => return Ok(()),
        InputOrder::StrictlyDescending => {
            items.reverse();
            return Ok(());
        }
        InputOrder::Unsorted => {}
    }
    turso_assert!(
        u32::try_from(len).is_ok(),
        "the adaptive sort keeps item positions in u32"
    );
    let mut scratch = items.try_to_vec()?;
    let mut tags = try_vec![0u32; len]?;
    let mut steps: Vec<Step> = Vec::try_with_capacity_ext(64)?;
    steps.try_push(Step::Quick {
        lo: 0,
        hi: len as u32,
        prefix: 0,
        side: Side::Items,
    })?;
    while let Some(step) = steps.pop() {
        match step {
            Step::Quick {
                lo,
                hi,
                prefix,
                side,
            } => {
                let (lo, hi) = (lo as usize, hi as usize);
                let (src, dst) = pick(items, &mut scratch, side);
                quick_step(
                    &mut src[lo..hi],
                    &mut dst[lo..hi],
                    &mut steps,
                    lo,
                    prefix as usize,
                    side.other(),
                )?;
            }
            Step::Radix {
                lo,
                hi,
                index,
                prefix,
                side,
            } => {
                let (lo, hi) = (lo as usize, hi as usize);
                let (src, dst) = pick(items, &mut scratch, side);
                radix_step(
                    &mut src[lo..hi],
                    &mut dst[lo..hi],
                    &mut tags[lo..hi],
                    &mut steps,
                    lo,
                    index as usize,
                    prefix as usize,
                    side.other(),
                )?;
            }
        }
    }
    Ok(())
}

enum InputOrder {
    Ascending,
    StrictlyDescending,
    Unsorted,
}

/// One pass over the items that stops at the first pair out of order, so
/// unsorted input costs a few comparisons. Reverse order only counts when
/// every key is strictly smaller than the one before it: reversing a run
/// with equal keys would swap their insertion order.
fn input_order<T: AdaptiveSortItem>(items: &[T]) -> InputOrder {
    let mut pairs = items.windows(2);
    let descending = pairs
        .next()
        .is_some_and(|pair| compare_items(&pair[0], &pair[1]) == Ordering::Greater);
    if descending {
        if pairs.all(|pair| compare_items(&pair[0], &pair[1]) == Ordering::Greater) {
            InputOrder::StrictlyDescending
        } else {
            InputOrder::Unsorted
        }
    } else if pairs.all(|pair| compare_items(&pair[0], &pair[1]) != Ordering::Greater) {
        InputOrder::Ascending
    } else {
        InputOrder::Unsorted
    }
}

/// Compares two items whose caches hold the first bytes of their keys,
/// following the record pointers only when the cached bytes tie.
fn compare_items<T: AdaptiveSortItem>(a: &T, b: &T) -> Ordering {
    let (a_len, b_len) = (a.key_len(), b.key_len());
    let (a_cache, b_cache) = (a.cache(), b.cache());
    for pos in 0..CACHE_LEN {
        if pos >= a_len || pos >= b_len {
            return a_len.cmp(&b_len);
        }
        if a_cache[pos] != b_cache[pos] {
            return a_cache[pos].cmp(&b_cache[pos]);
        }
    }
    compare_from(a, b.key(), 0).0
}

/// Which of the two buffers holds a partition. Every step reads its
/// partition from one buffer and writes the partitioned groups to the same
/// positions of the other one. `Items` is also where the sorted output ends
/// up, so a group that is finished in the scratch buffer is copied back
/// right away.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Items,
    Scratch,
}

impl Side {
    fn other(self) -> Self {
        match self {
            Side::Items => Side::Scratch,
            Side::Scratch => Side::Items,
        }
    }
}

#[derive(Clone, Copy)]
enum Step {
    /// Quicksort step on `side[lo..hi]`: the keys share `prefix` bytes and
    /// the caches hold the bytes at `prefix`.
    Quick {
        lo: u32,
        hi: u32,
        prefix: u32,
        side: Side,
    },
    /// Radix step on `side[lo..hi]`: bucket on the byte at `index`, while
    /// the caches still hold the bytes at `prefix`.
    Radix {
        lo: u32,
        hi: u32,
        index: u32,
        prefix: u32,
        side: Side,
    },
}

fn pick<'a, T>(items: &'a mut [T], scratch: &'a mut [T], side: Side) -> (&'a mut [T], &'a mut [T]) {
    match side {
        Side::Items => (items, scratch),
        Side::Scratch => (scratch, items),
    }
}

/// Partitions `src` around a pivot into `dst` as [less | equal | greater],
/// each group in input order, and queues the radix steps for the less and
/// greater groups with the common prefix found for them. The equal group is
/// finished, its keys are all the same, so it goes straight to the output
/// buffer. One pass: less keys fill `dst` from the front, greater keys fill
/// it from the back (reversed, then turned around), and equal keys are
/// packed into the front of `src`, which has already been read there.
fn quick_step<T: AdaptiveSortItem>(
    src: &mut [T],
    dst: &mut [T],
    steps: &mut Vec<Step>,
    lo: usize,
    prefix: usize,
    dst_side: Side,
) -> Result<()> {
    let len = src.len();
    let pivot = src[pivot_index(src, prefix)];
    let pivot_key = pivot.key();
    let (mut less, mut equal, mut greater) = (0, 0, 0);
    let mut less_prefix = usize::MAX;
    let mut greater_prefix = usize::MAX;
    let mut i = 0;
    while i < len {
        let item = src[i];
        i += 1;
        let (order, differ_at) = compare_from(&item, pivot_key, prefix);
        match order {
            Ordering::Less => {
                less_prefix = less_prefix.min(differ_at);
                dst[less] = item;
                less += 1;
            }
            Ordering::Equal => {
                src[equal] = item;
                equal += 1;
            }
            Ordering::Greater => {
                greater_prefix = greater_prefix.min(differ_at);
                greater += 1;
                dst[len - greater] = item;
            }
        }
    }
    dst[len - greater..].reverse();
    match dst_side {
        Side::Items => dst[less..less + equal].copy_from_slice(&src[..equal]),
        Side::Scratch => src.copy_within(0..equal, less),
    }
    // Steps run last in, first out: the greater group goes first so that the
    // smaller keys are worked on first, which keeps the working set close.
    push_group(
        steps,
        src,
        dst,
        len - greater,
        len,
        greater_prefix,
        prefix,
        lo,
        dst_side,
    )?;
    push_group(steps, src, dst, 0, less, less_prefix, prefix, lo, dst_side)
}

/// Queues the radix step for a group produced by a quicksort step. A single
/// key is finished and only needs to reach the output buffer.
#[allow(clippy::too_many_arguments)]
fn push_group<T: AdaptiveSortItem>(
    steps: &mut Vec<Step>,
    src: &mut [T],
    dst: &[T],
    start: usize,
    end: usize,
    index: usize,
    prefix: usize,
    lo: usize,
    dst_side: Side,
) -> Result<()> {
    match end - start {
        0 => Ok(()),
        1 => {
            finish(src, dst, start, end, dst_side);
            Ok(())
        }
        _ => Ok(steps.try_push(Step::Radix {
            lo: (lo + start) as u32,
            hi: (lo + end) as u32,
            index: index as u32,
            prefix: prefix as u32,
            side: dst_side,
        })?),
    }
}

/// Moves a finished group to the output buffer. `src` has been read in full
/// by then, so when it is the output buffer the group is copied back over
/// the positions it came from.
fn finish<T: AdaptiveSortItem>(src: &mut [T], dst: &[T], start: usize, end: usize, dst_side: Side) {
    if dst_side == Side::Scratch {
        src[start..end].copy_from_slice(&dst[start..end]);
    }
}

/// The largest sample the pivot is the median of; the patent suggests no
/// more than 20 keys.
const MAX_PIVOT_SAMPLE: usize = 19;

fn pivot_index<T: AdaptiveSortItem>(src: &[T], prefix: usize) -> usize {
    let len = src.len();
    let sample = (len / 64).clamp(1, MAX_PIVOT_SAMPLE) | 1;
    if sample == 1 {
        return len / 2;
    }
    let stride = len / sample;
    let mut chosen = [0usize; MAX_PIVOT_SAMPLE];
    for (i, slot) in chosen[..sample].iter_mut().enumerate() {
        *slot = i * stride + stride / 2;
    }
    for i in 1..sample {
        let index = chosen[i];
        let key = src[index].key();
        let mut j = i;
        while j > 0 && compare_from(&src[chosen[j - 1]], key, prefix).0 == Ordering::Greater {
            chosen[j] = chosen[j - 1];
            j -= 1;
        }
        chosen[j] = index;
    }
    chosen[sample / 2]
}

/// Compares the key of `item` with `other`, both known to share `prefix`
/// bytes, and returns the order and the index of the first byte where they
/// differ. The cached bytes are compared first; the key in memory is only
/// read when they tie.
#[inline]
fn compare_from<T: AdaptiveSortItem>(item: &T, other: &[u8], prefix: usize) -> (Ordering, usize) {
    let len = item.key_len();
    let other_len = other.len();
    let cache = item.cache();
    for (j, cached) in cache.iter().enumerate() {
        let pos = prefix + j;
        if pos >= len || pos >= other_len {
            return (len.cmp(&other_len), pos);
        }
        if *cached != other[pos] {
            return (cached.cmp(&other[pos]), pos);
        }
    }
    let start = prefix + CACHE_LEN;
    let end = len.min(other_len);
    let key = item.key();
    match first_difference(&key[start..end], &other[start..end]) {
        Some(offset) => {
            let pos = start + offset;
            (key[pos].cmp(&other[pos]), pos)
        }
        None => (len.cmp(&other_len), end),
    }
}

/// Index of the first byte where two equally long slices differ.
#[inline]
fn first_difference(a: &[u8], b: &[u8]) -> Option<usize> {
    let len = a.len();
    let mut i = 0;
    while i + 8 <= len {
        let x = u64::from_ne_bytes(a[i..i + 8].try_into().expect("eight bytes"));
        let y = u64::from_ne_bytes(b[i..i + 8].try_into().expect("eight bytes"));
        let diff = x ^ y;
        if diff != 0 {
            let byte = if cfg!(target_endian = "little") {
                diff.trailing_zeros()
            } else {
                diff.leading_zeros()
            } / 8;
            return Some(i + byte as usize);
        }
        i += 8;
    }
    while i < len {
        if a[i] != b[i] {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// A radix tag orders the buckets: keys that end at the partitioning index
/// come first, then for each byte value the keys that end right after it
/// (done) before the keys that go on (more). The new cache bytes of a "more"
/// key ride in the upper half of its tag so the placement pass does not
/// follow the record pointer again.
const TAG_ENDED: u32 = 0;
const TAG_MASK: u32 = 0xFFFF;
const TAG_COUNT: usize = 1 + 2 * 256;
/// Partitions up to this size sort their tags by insertion instead of
/// counting into 513 buckets.
const SMALL_RADIX_STEP: usize = 32;

fn tag_done(byte: u8) -> u32 {
    1 + 2 * byte as u32
}

fn tag_more(byte: u8) -> u32 {
    2 + 2 * byte as u32
}

fn tag_is_more(bucket: u32) -> bool {
    bucket != TAG_ENDED && bucket % 2 == 0
}

fn packed_cache(cache: [u8; CACHE_LEN]) -> u32 {
    ((cache[0] as u32) << 16) | ((cache[1] as u32) << 24)
}

fn unpack_cache(tag: u32) -> [u8; CACHE_LEN] {
    [(tag >> 16) as u8, (tag >> 24) as u8]
}

/// Buckets `src` on the byte at `index` into `dst`, each bucket in input
/// order, and queues a quicksort step for every bucket with more than one
/// key that goes on past `index`. `prefix` says which bytes the caches hold;
/// the patent's three cases for the distance between `index` and `prefix`
/// decide where the bucket byte comes from and whether the caches move:
/// with the byte in the cache the counting pass needs no tags, and only a
/// byte beyond the cache is fetched once and remembered in the tags.
#[allow(clippy::too_many_arguments)]
fn radix_step<T: AdaptiveSortItem>(
    src: &mut [T],
    dst: &mut [T],
    tags: &mut [u32],
    steps: &mut Vec<Step>,
    lo: usize,
    index: usize,
    prefix: usize,
    dst_side: Side,
) -> Result<()> {
    turso_assert!(
        index >= prefix,
        "the cache never runs ahead of the partition index"
    );
    let gap = index - prefix;
    let new_prefix = if gap == 0 { prefix } else { index + 1 };
    let len = src.len();
    if len <= SMALL_RADIX_STEP {
        let mut small_tags = [0u32; SMALL_RADIX_STEP];
        let tags = &mut small_tags[..len];
        for (item, tag) in src.iter().zip(tags.iter_mut()) {
            *tag = radix_tag(item, index, gap);
        }
        place_small(src, dst, tags, gap);
        let mut end = len;
        while end > 0 {
            let bucket = tags[end - 1] & TAG_MASK;
            let mut start = end;
            while start > 0 && (tags[start - 1] & TAG_MASK) == bucket {
                start -= 1;
            }
            push_bucket(
                steps, src, dst, start, end, bucket, new_prefix, lo, dst_side,
            )?;
            end = start;
        }
        return Ok(());
    }
    let mut next = [0u32; TAG_COUNT];
    if gap >= CACHE_LEN {
        for (item, tag) in src.iter().zip(tags.iter_mut()) {
            *tag = radix_tag(item, index, gap);
            next[(*tag & TAG_MASK) as usize] += 1;
        }
        bucket_starts(&mut next);
        for (item, tag) in src.iter().zip(tags.iter()) {
            let bucket = *tag & TAG_MASK;
            let mut placed = *item;
            if tag_is_more(bucket) {
                placed.set_cache(unpack_cache(*tag));
            }
            let slot = &mut next[bucket as usize];
            dst[*slot as usize] = placed;
            *slot += 1;
        }
    } else {
        for item in src.iter() {
            next[cached_bucket(item, index, gap) as usize] += 1;
        }
        bucket_starts(&mut next);
        for item in src.iter() {
            let bucket = cached_bucket(item, index, gap);
            let mut placed = *item;
            if gap != 0 && tag_is_more(bucket) {
                placed.set_cache(cache_at(item.key(), index + 1));
            }
            let slot = &mut next[bucket as usize];
            dst[*slot as usize] = placed;
            *slot += 1;
        }
    }
    for bucket in (0..TAG_COUNT).rev() {
        let end = next[bucket] as usize;
        let start = if bucket == 0 {
            0
        } else {
            next[bucket - 1] as usize
        };
        if end > start {
            push_bucket(
                steps,
                src,
                dst,
                start,
                end,
                bucket as u32,
                new_prefix,
                lo,
                dst_side,
            )?;
        }
    }
    Ok(())
}

/// The bucket of a key when the bucket byte sits in the cache (`gap` is 0
/// or 1).
fn cached_bucket<T: AdaptiveSortItem>(item: &T, index: usize, gap: usize) -> u32 {
    let len = item.key_len();
    if len == index {
        TAG_ENDED
    } else {
        let byte = item.cache()[gap];
        if len == index + 1 {
            tag_done(byte)
        } else {
            tag_more(byte)
        }
    }
}

/// The bucket of a key, with the next two key bytes packed in when the
/// cache must move.
fn radix_tag<T: AdaptiveSortItem>(item: &T, index: usize, gap: usize) -> u32 {
    let len = item.key_len();
    if len == index {
        return TAG_ENDED;
    }
    if gap < CACHE_LEN {
        let bucket = cached_bucket(item, index, gap);
        if gap != 0 && tag_is_more(bucket) {
            bucket | packed_cache(cache_at(item.key(), index + 1))
        } else {
            bucket
        }
    } else {
        let key = item.key();
        if len == index + 1 {
            tag_done(key[index])
        } else {
            tag_more(key[index]) | packed_cache(cache_at(key, index + 1))
        }
    }
}

/// Turns bucket counts into the start position of each bucket.
fn bucket_starts(next: &mut [u32; TAG_COUNT]) {
    let mut start = 0;
    for slot in next.iter_mut() {
        let count = *slot;
        *slot = start;
        start += count;
    }
}

/// Stable insertion sort of a small partition by bucket, moving the tags
/// along so the caller can find the bucket runs in `tags`.
fn place_small<T: AdaptiveSortItem>(src: &[T], dst: &mut [T], tags: &mut [u32], gap: usize) {
    for (slot, (item, tag)) in dst.iter_mut().zip(src.iter().zip(tags.iter())) {
        let mut placed = *item;
        if gap != 0 && tag_is_more(*tag & TAG_MASK) {
            placed.set_cache(unpack_cache(*tag));
        }
        *slot = placed;
    }
    for i in 1..dst.len() {
        let (tag, item) = (tags[i], dst[i]);
        let bucket = tag & TAG_MASK;
        let mut j = i;
        while j > 0 && (tags[j - 1] & TAG_MASK) > bucket {
            tags[j] = tags[j - 1];
            dst[j] = dst[j - 1];
            j -= 1;
        }
        tags[j] = tag;
        dst[j] = item;
    }
}

#[allow(clippy::too_many_arguments)]
fn push_bucket<T: AdaptiveSortItem>(
    steps: &mut Vec<Step>,
    src: &mut [T],
    dst: &[T],
    start: usize,
    end: usize,
    bucket: u32,
    prefix: usize,
    lo: usize,
    dst_side: Side,
) -> Result<()> {
    if tag_is_more(bucket) && end - start > 1 {
        Ok(steps.try_push(Step::Quick {
            lo: (lo + start) as u32,
            hi: (lo + end) as u32,
            prefix: prefix as u32,
            side: dst_side,
        })?)
    } else {
        finish(src, dst, start, end, dst_side);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    #[derive(Clone, Copy, Debug)]
    struct TestItem<'a> {
        key: &'a [u8],
        id: usize,
        cache: [u8; CACHE_LEN],
    }

    impl AdaptiveSortItem for TestItem<'_> {
        fn key(&self) -> &[u8] {
            self.key
        }

        fn key_len(&self) -> usize {
            self.key.len()
        }

        fn cache(&self) -> [u8; CACHE_LEN] {
            self.cache
        }

        fn set_cache(&mut self, cache: [u8; CACHE_LEN]) {
            self.cache = cache;
        }
    }

    fn seed() -> u64 {
        std::env::var("SEED").map_or(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            |v| v.parse().expect("SEED must be a u64"),
        )
    }

    fn check_sorts_like_stable_sort(keys: &[std::vec::Vec<u8>], seed: u64) {
        let mut items: std::vec::Vec<TestItem> = keys
            .iter()
            .enumerate()
            .map(|(id, key)| TestItem {
                key,
                id,
                cache: cache_at(key, 0),
            })
            .collect();
        let mut expected: std::vec::Vec<usize> = (0..keys.len()).collect();
        expected.sort_by(|a, b| keys[*a].cmp(&keys[*b]));
        adaptive_sort(&mut items).unwrap();
        let got: std::vec::Vec<usize> = items.iter().map(|item| item.id).collect();
        assert_eq!(
            got, expected,
            "seed {seed}: sorted order differs from a stable sort"
        );
    }

    fn random_keys(rng: &mut ChaCha8Rng, count: usize) -> std::vec::Vec<std::vec::Vec<u8>> {
        let alphabet_len = 1 + (rng.next_u64() % 4) as usize;
        let alphabet = [0x00u8, 0x61, 0xFF, 0x62];
        let shared_prefix_len = (rng.next_u64() % 70) as usize;
        let shared_prefix: std::vec::Vec<u8> = (0..shared_prefix_len)
            .map(|_| rng.next_u64() as u8)
            .collect();
        let max_len = 1 + (rng.next_u64() % 40) as usize;
        (0..count)
            .map(|_| {
                let mut key = if rng.next_u64() % 8 == 0 {
                    std::vec::Vec::new()
                } else {
                    shared_prefix.clone()
                };
                let len = (rng.next_u64() % max_len as u64) as usize;
                key.extend((0..len).map(|_| {
                    if rng.next_u64() % 3 == 0 {
                        rng.next_u64() as u8
                    } else {
                        alphabet[(rng.next_u64() % alphabet_len as u64) as usize]
                    }
                }));
                key
            })
            .collect()
    }

    #[test]
    fn fuzz_sorts_like_a_stable_sort() {
        let seed = seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        for round in 0..400 {
            let count = match round % 4 {
                0 => (rng.next_u64() % 8) as usize,
                1 => (rng.next_u64() % 64) as usize,
                2 => (rng.next_u64() % 600) as usize,
                _ => (rng.next_u64() % 5000) as usize,
            };
            let mut keys = random_keys(&mut rng, count);
            match round % 5 {
                0 => keys.sort(),
                1 => {
                    keys.sort();
                    keys.reverse();
                }
                _ => {}
            }
            check_sorts_like_stable_sort(&keys, seed);
        }
    }

    #[test]
    fn sorts_many_equal_and_prefixed_keys() {
        let mut keys = std::vec::Vec::new();
        for i in 0..20_000u32 {
            let key: std::vec::Vec<u8> = match i % 5 {
                0 => b"same-key".to_vec(),
                1 => b"same-key-longer".to_vec(),
                2 => b"same".to_vec(),
                3 => std::vec::Vec::new(),
                _ => format!("same-key-{:04}", i % 300).into_bytes(),
            };
            keys.push(key);
        }
        check_sorts_like_stable_sort(&keys, 0);
    }

    #[test]
    fn sorts_keys_with_long_shared_prefix_and_late_differences() {
        let prefix: std::vec::Vec<u8> = std::iter::repeat_n(0xABu8, 3000).collect();
        let keys: std::vec::Vec<std::vec::Vec<u8>> = (0..3000u32)
            .rev()
            .map(|i| {
                let mut key = prefix.clone();
                key.extend_from_slice(&i.to_be_bytes());
                key
            })
            .collect();
        check_sorts_like_stable_sort(&keys, 0);
    }

    #[test]
    fn sorts_ordered_reversed_and_nearly_ordered_input() {
        let sorted: std::vec::Vec<std::vec::Vec<u8>> =
            (0..10_000u64).map(|i| i.to_be_bytes().to_vec()).collect();
        check_sorts_like_stable_sort(&sorted, 0);
        let reversed: std::vec::Vec<std::vec::Vec<u8>> = sorted.iter().rev().cloned().collect();
        check_sorts_like_stable_sort(&reversed, 0);
        // Reversed with equal keys must not be flipped, or equal keys would
        // lose their insertion order.
        let reversed_with_duplicates: std::vec::Vec<std::vec::Vec<u8>> = (0..10_000u64)
            .rev()
            .map(|i| (i / 3).to_be_bytes().to_vec())
            .collect();
        check_sorts_like_stable_sort(&reversed_with_duplicates, 0);
        let mut nearly_sorted = sorted.clone();
        nearly_sorted.swap(9_998, 9_999);
        check_sorts_like_stable_sort(&nearly_sorted, 0);
        let mut ordered_with_duplicates = sorted;
        ordered_with_duplicates[5_000] = ordered_with_duplicates[4_999].clone();
        check_sorts_like_stable_sort(&ordered_with_duplicates, 0);
    }
}
