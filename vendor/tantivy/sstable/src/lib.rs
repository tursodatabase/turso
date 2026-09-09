//! `tantivy_sstable` is a crate that provides a sorted string table data structure.
//!
//! It is used in `tantivy` to store the term dictionary.
//!
//! A `sstable` is a map of sorted `&[u8]` keys to values.
//! The keys are encoded using incremental encoding.
//!
//! Values and keys are compressed using zstd with the default feature flag `zstd-compression`.
//!
//! # Example
//!
//! Here is an example of how to create and search an `sstable`:
//!
//! ```rust
//! use common::OwnedBytes;
//! use tantivy_sstable::{Dictionary, MonotonicU64SSTable};
//!
//! // Create a new sstable in memory.
//! let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
//! builder.insert(b"apple", &1).unwrap();
//! builder.insert(b"banana", &2).unwrap();
//! builder.insert(b"orange", &3).unwrap();
//! let sstable_bytes = builder.finish().unwrap();
//!
//! // Open the sstable.
//! let sstable =
//!     Dictionary::<MonotonicU64SSTable>::from_bytes(OwnedBytes::new(sstable_bytes)).unwrap();
//!
//! // Search for a key.
//! let value = sstable.get(b"banana").unwrap();
//! assert_eq!(value, Some(2));
//!
//! // Search for a non-existent key.
//! let value = sstable.get(b"grape").unwrap();
//! assert_eq!(value, None);
//! ```

use std::io::{self, Write};
use std::ops::Range;

use merge::ValueMerger;

mod block_match_automaton;
mod delta;
mod dictionary;
pub mod merge;
mod streamer;
pub mod value;

mod sstable_index_v3;
pub use sstable_index_v3::{BlockAddr, SSTableIndex, SSTableIndexBuilder, SSTableIndexV3};
mod sstable_index_v2;
pub(crate) mod vint;
pub use dictionary::{Dictionary, TermOrdHit};
pub use streamer::{Streamer, StreamerBuilder};

mod block_reader;
use common::{BinarySerializable, OwnedBytes};
use value::{VecU32ValueReader, VecU32ValueWriter};

pub use self::block_reader::BlockReader;
pub use self::delta::{DeltaReader, DeltaWriter};
pub use self::merge::VoidMerge;
use self::value::{U64MonotonicValueReader, U64MonotonicValueWriter, ValueReader, ValueWriter};
use crate::value::{RangeValueReader, RangeValueWriter};

pub type TermOrdinal = u64;

const DEFAULT_KEY_CAPACITY: usize = 50;
const SSTABLE_VERSION: u32 = 3;

/// Given two byte string returns the length of
/// the longest common prefix.
fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .cloned()
        .zip(right.iter().cloned())
        .take_while(|(left, right)| left == right)
        .count()
}

#[derive(Debug, Copy, Clone)]
pub struct SSTableDataCorruption;

/// SSTable makes it possible to read and write
/// sstables with typed values.
pub trait SSTable: Sized {
    type Value: Clone;
    type ValueReader: ValueReader<Value = Self::Value>;
    type ValueWriter: ValueWriter<Value = Self::Value>;

    fn delta_writer<W: io::Write>(write: W) -> DeltaWriter<W, Self::ValueWriter> {
        DeltaWriter::new(write)
    }

    fn writer<W: io::Write>(wrt: W) -> Writer<W, Self::ValueWriter> {
        Writer::new(wrt)
    }

    fn delta_reader(reader: OwnedBytes) -> DeltaReader<Self::ValueReader> {
        DeltaReader::new(reader)
    }

    fn reader(reader: OwnedBytes) -> Reader<Self::ValueReader> {
        Reader {
            key: Vec::with_capacity(DEFAULT_KEY_CAPACITY),
            delta_reader: Self::delta_reader(reader),
        }
    }

    /// Returns an empty static reader.
    fn create_empty_reader() -> Reader<Self::ValueReader> {
        Self::reader(OwnedBytes::empty())
    }

    fn merge<W: io::Write, M: ValueMerger<Self::Value>>(
        io_readers: Vec<OwnedBytes>,
        w: W,
        merger: M,
    ) -> io::Result<()> {
        let readers: Vec<_> = io_readers.into_iter().map(Self::reader).collect();
        let writer = Self::writer(w);
        merge::merge_sstable::<Self, _, _>(readers, writer, merger)
    }
}

pub struct VoidSSTable;

impl SSTable for VoidSSTable {
    type Value = ();
    type ValueReader = value::VoidValueReader;
    type ValueWriter = value::VoidValueWriter;
}

/// SSTable associated keys to u64
/// sorted in order.
///
/// In other words, two keys `k1` and `k2`
/// such that `k1` <= `k2`, are required to observe
/// `range_sstable[k1] <= range_sstable[k2]`.
pub struct MonotonicU64SSTable;

impl SSTable for MonotonicU64SSTable {
    type Value = u64;

    type ValueReader = U64MonotonicValueReader;

    type ValueWriter = U64MonotonicValueWriter;
}

/// SSTable associating keys to ranges.
/// The range are required to partition the
/// space.
///
/// In other words, two consecutive keys `k1` and `k2`
/// are required to observe
/// `range_sstable[k1].end == range_sstable[k2].start`.
///
/// The first range is not required to start at `0`.
#[derive(Clone, Copy, Debug)]
pub struct RangeSSTable;

impl SSTable for RangeSSTable {
    type Value = Range<u64>;

    type ValueReader = RangeValueReader;

    type ValueWriter = RangeValueWriter;
}

/// SSTable associating keys to Vec<u32>.
pub struct VecU32ValueSSTable;

impl SSTable for VecU32ValueSSTable {
    type Value = Vec<u32>;
    type ValueReader = VecU32ValueReader;
    type ValueWriter = VecU32ValueWriter;
}

/// SSTable reader.
pub struct Reader<TValueReader> {
    key: Vec<u8>,
    delta_reader: DeltaReader<TValueReader>,
}

impl<TValueReader> Reader<TValueReader>
where TValueReader: ValueReader
{
    pub fn advance(&mut self) -> io::Result<bool> {
        if !self.delta_reader.advance()? {
            return Ok(false);
        }
        let common_prefix_len = self.delta_reader.common_prefix_len();
        let suffix = self.delta_reader.suffix();
        let new_len = self.delta_reader.common_prefix_len() + suffix.len();
        self.key.resize(new_len, 0u8);
        self.key[common_prefix_len..].copy_from_slice(suffix);
        Ok(true)
    }

    #[inline(always)]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    #[inline(always)]
    pub fn value(&self) -> &TValueReader::Value {
        self.delta_reader.value()
    }
}

impl<TValueReader> AsRef<[u8]> for Reader<TValueReader> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

pub struct Writer<W, TValueWriter>
where W: io::Write
{
    previous_key: Vec<u8>,
    index_builder: SSTableIndexBuilder,
    delta_writer: DeltaWriter<W, TValueWriter>,
    num_terms: u64,
    first_ordinal_of_the_block: u64,
}

impl<W, TValueWriter> Writer<W, TValueWriter>
where
    W: io::Write,
    TValueWriter: value::ValueWriter,
{
    /// Use `Self::new`. This method only exists to match its
    /// equivalent in fst.
    /// TODO remove this function. (See Issue #1727)
    #[doc(hidden)]
    pub fn create(wrt: W) -> io::Result<Self> {
        Ok(Self::new(wrt))
    }

    /// Creates a new `TermDictionaryBuilder`.
    pub fn new(wrt: W) -> Self {
        Writer {
            previous_key: Vec::with_capacity(DEFAULT_KEY_CAPACITY),
            num_terms: 0u64,
            index_builder: SSTableIndexBuilder::default(),
            delta_writer: DeltaWriter::new(wrt),
            first_ordinal_of_the_block: 0u64,
        }
    }

    /// Set the target block length.
    ///
    /// The delta part of a block will generally be slightly larger than the requested `block_len`,
    /// however this does not account for the length of the Value part of the table.
    pub fn set_block_len(&mut self, block_len: usize) {
        self.delta_writer.set_block_len(block_len)
    }

    /// Returns the last inserted key.
    /// If no key has been inserted yet, or the block was just
    /// flushed, this function returns "".
    #[inline(always)]
    pub(crate) fn last_inserted_key(&self) -> &[u8] {
        &self.previous_key[..]
    }

    /// Inserts a `(key, value)` pair in the term dictionary.
    /// Keys have to be inserted in order.
    ///
    /// # Panics
    ///
    /// Will panics if keys are inserted in an invalid order.
    #[inline]
    pub fn insert<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: &TValueWriter::Value,
    ) -> io::Result<()> {
        self.insert_key(key.as_ref())?;
        self.insert_value(value)?;
        Ok(())
    }

    /// # Warning
    ///
    /// Horribly dangerous internal API. See `.insert(...)`.
    #[doc(hidden)]
    #[inline]
    pub fn insert_key(&mut self, key: &[u8]) -> io::Result<()> {
        // If this is the first key in the block, we use it to
        // shorten the last term in the last block.
        if self.first_ordinal_of_the_block == self.num_terms {
            self.index_builder
                .shorten_last_block_key_given_next_key(key);
        }
        let keep_len = common_prefix_len(&self.previous_key, key);
        let add_len = key.len() - keep_len;
        let increasing_keys = add_len > 0 && (self.previous_key.len() == keep_len)
            || self.previous_key.is_empty()
            || self.previous_key[keep_len] < key[keep_len];
        assert!(
            increasing_keys,
            "Keys should be increasing. ({:?} > {:?})",
            String::from_utf8_lossy(&self.previous_key),
            String::from_utf8_lossy(key),
        );
        self.previous_key.resize(key.len(), 0u8);
        self.previous_key[keep_len..].copy_from_slice(&key[keep_len..]);
        self.delta_writer.write_suffix(keep_len, &key[keep_len..]);
        Ok(())
    }

    /// # Warning
    ///
    /// Horribly dangerous internal API. See `.insert(...)`.
    #[doc(hidden)]
    #[inline]
    pub fn insert_value(&mut self, value: &TValueWriter::Value) -> io::Result<()> {
        self.delta_writer.write_value(value);
        self.num_terms += 1u64;
        self.flush_block_if_required()
    }

    pub fn flush_block_if_required(&mut self) -> io::Result<()> {
        if let Some(byte_range) = self.delta_writer.flush_block_if_required()? {
            self.index_builder.add_block(
                &self.previous_key[..],
                byte_range,
                self.first_ordinal_of_the_block,
            );
            self.first_ordinal_of_the_block = self.num_terms;
            self.previous_key.clear();
        }
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<W> {
        if let Some(byte_range) = self.delta_writer.flush_block()? {
            self.index_builder.add_block(
                &self.previous_key[..],
                byte_range,
                self.first_ordinal_of_the_block,
            );
            self.first_ordinal_of_the_block = self.num_terms;
        }
        let mut wrt = self.delta_writer.finish();
        // add a final empty block as an end marker
        wrt.write_all(&0u32.to_le_bytes())?;

        let offset = wrt.written_bytes();

        let fst_len: u64 = self.index_builder.serialize(&mut wrt)?;
        wrt.write_all(&fst_len.to_le_bytes())?;
        wrt.write_all(&offset.to_le_bytes())?;
        wrt.write_all(&self.num_terms.to_le_bytes())?;

        SSTABLE_VERSION.serialize(&mut wrt)?;

        let wrt = wrt.finish();
        Ok(wrt.into_inner()?)
    }
}

#[cfg(test)]
mod test {
    use std::io;
    use std::ops::Bound;

    use common::OwnedBytes;

    use super::{MonotonicU64SSTable, SSTable, VoidMerge, VoidSSTable, common_prefix_len};

    fn aux_test_common_prefix_len(left: &str, right: &str, expect_len: usize) {
        assert_eq!(
            common_prefix_len(left.as_bytes(), right.as_bytes()),
            expect_len
        );
        assert_eq!(
            common_prefix_len(right.as_bytes(), left.as_bytes()),
            expect_len
        );
    }

    #[test]
    fn test_common_prefix_len() {
        aux_test_common_prefix_len("a", "ab", 1);
        aux_test_common_prefix_len("", "ab", 0);
        aux_test_common_prefix_len("ab", "abc", 2);
        aux_test_common_prefix_len("abde", "abce", 2);
    }

    #[test]
    fn test_long_key_diff() {
        let long_key = (0..1_024).map(|x| (x % 255) as u8).collect::<Vec<_>>();
        let long_key2 = (1..300).map(|x| (x % 255) as u8).collect::<Vec<_>>();
        let mut buffer = vec![];
        {
            let mut sstable_writer = VoidSSTable::writer(&mut buffer);
            assert!(sstable_writer.insert(&long_key[..], &()).is_ok());
            assert!(sstable_writer.insert([0, 3, 4], &()).is_ok());
            assert!(sstable_writer.insert(&long_key2[..], &()).is_ok());
            assert!(sstable_writer.finish().is_ok());
        }
        let buffer = OwnedBytes::new(buffer);
        let mut sstable_reader = VoidSSTable::reader(buffer);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &long_key[..]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[0, 3, 4]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &long_key2[..]);
        assert!(!sstable_reader.advance().unwrap());
    }

    #[test]
    fn test_simple_sstable() {
        let mut buffer = vec![];
        {
            let mut sstable_writer = VoidSSTable::writer(&mut buffer);
            assert!(sstable_writer.insert([17u8], &()).is_ok());
            assert!(sstable_writer.insert([17u8, 18u8, 19u8], &()).is_ok());
            assert!(sstable_writer.insert([17u8, 20u8], &()).is_ok());
            assert!(sstable_writer.finish().is_ok());
        }
        assert_eq!(
            &buffer,
            &[
                // block
                8, 0, 0, 0, // size of block
                0, // compression
                16, 17, 33, 18, 19, 17, 20, // data block
                0, 0, 0, 0, // no more block
                // index
                0, 0, 0, 0, 0, 0, 0, 0, // fst length
                16, 0, 0, 0, 0, 0, 0, 0, // index start offset
                3, 0, 0, 0, 0, 0, 0, 0, // num term
                3, 0, 0, 0, // version
            ]
        );
        let buffer = OwnedBytes::new(buffer);
        let mut sstable_reader = VoidSSTable::reader(buffer);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[17u8]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[17u8, 18u8, 19u8]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[17u8, 20u8]);
        assert!(!sstable_reader.advance().unwrap());
    }

    #[test]
    #[should_panic]
    fn test_simple_sstable_non_increasing_key() {
        let mut buffer = vec![];
        let mut sstable_writer = VoidSSTable::writer(&mut buffer);
        assert!(sstable_writer.insert([17u8], &()).is_ok());
        assert!(sstable_writer.insert([16u8], &()).is_ok());
    }

    #[test]
    fn test_merge_abcd_abe() {
        let mut buffer = Vec::new();
        {
            let mut writer = VoidSSTable::writer(&mut buffer);
            writer.insert(b"abcd", &()).unwrap();
            writer.insert(b"abe", &()).unwrap();
            writer.finish().unwrap();
        }
        let buffer = OwnedBytes::new(buffer);
        let mut output = Vec::new();
        assert!(
            VoidSSTable::merge(vec![buffer.clone(), buffer.clone()], &mut output, VoidMerge)
                .is_ok()
        );
        assert_eq!(&output[..], &buffer[..]);
    }

    #[test]
    fn test_sstable() {
        let mut buffer = Vec::new();
        {
            let mut writer = VoidSSTable::writer(&mut buffer);
            assert_eq!(writer.last_inserted_key(), b"");
            writer.insert(b"abcd", &()).unwrap();
            assert_eq!(writer.last_inserted_key(), b"abcd");
            writer.insert(b"abe", &()).unwrap();
            assert_eq!(writer.last_inserted_key(), b"abe");
            writer.finish().unwrap();
        }
        let buffer = OwnedBytes::new(buffer);
        let mut output = Vec::new();
        assert!(
            VoidSSTable::merge(vec![buffer.clone(), buffer.clone()], &mut output, VoidMerge)
                .is_ok()
        );
        assert_eq!(&output[..], &buffer[..]);
    }

    #[test]
    fn test_sstable_u64() -> io::Result<()> {
        let mut buffer = Vec::new();
        let mut writer = MonotonicU64SSTable::writer(&mut buffer);
        writer.insert(b"abcd", &1u64)?;
        writer.insert(b"abe", &4u64)?;
        writer.insert(b"gogo", &4324234234234234u64)?;
        writer.finish()?;
        let buffer = OwnedBytes::new(buffer);
        let mut reader = MonotonicU64SSTable::reader(buffer);
        assert!(reader.advance()?);
        assert_eq!(reader.key(), b"abcd");
        assert_eq!(reader.value(), &1u64);
        assert!(reader.advance()?);
        assert_eq!(reader.key(), b"abe");
        assert_eq!(reader.value(), &4u64);
        assert!(reader.advance()?);
        assert_eq!(reader.key(), b"gogo");
        assert_eq!(reader.value(), &4324234234234234u64);
        assert!(!reader.advance()?);
        Ok(())
    }

    #[test]
    fn test_sstable_empty() {
        let mut sstable_range_empty = crate::RangeSSTable::create_empty_reader();
        assert!(!sstable_range_empty.advance().unwrap());
    }

    use common::file_slice::FileSlice;
    use proptest::prelude::*;

    use crate::Dictionary;

    fn bound_strategy() -> impl Strategy<Value = Bound<String>> {
        prop_oneof![
            Just(Bound::<String>::Unbounded),
            "[a-c]{0,5}".prop_map(Bound::Included),
            "[a-c]{0,5}".prop_map(Bound::Excluded),
        ]
    }

    fn extract_key(bound: Bound<&String>) -> Option<&str> {
        match bound.as_ref() {
            Bound::Included(key) => Some(key.as_str()),
            Bound::Excluded(key) => Some(key.as_str()),
            Bound::Unbounded => None,
        }
    }

    fn bounds_strategy() -> impl Strategy<Value = (Bound<String>, Bound<String>)> {
        (bound_strategy(), bound_strategy()).prop_filter(
            "Lower bound <= Upper bound",
            |(left, right)| match (extract_key(left.as_ref()), extract_key(right.as_ref())) {
                (None, _) => true,
                (_, None) => true,
                (left, right) => left < right,
            },
        )
    }

    proptest! {
        #[test]
        fn test_proptest_sstable_ranges(words in prop::collection::btree_set("[a-c]{0,6}", 1..100),
            (lower_bound, upper_bound) in bounds_strategy(),
        ) {
            let mut builder = Dictionary::<VoidSSTable>::builder(Vec::new()).unwrap();
            builder.set_block_len(16);
            for word in &words {
                builder.insert(word.as_bytes(), &()).unwrap();
            }
            let buffer: Vec<u8> = builder.finish().unwrap();
            let dictionary: Dictionary<VoidSSTable> = Dictionary::open(FileSlice::from(buffer)).unwrap();
            let mut range_builder = dictionary.range();
            range_builder = match lower_bound.as_ref() {
                Bound::Included(key) => range_builder.ge(key.as_bytes()),
                Bound::Excluded(key) => range_builder.gt(key.as_bytes()),
                Bound::Unbounded => range_builder,
            };
            range_builder = match upper_bound.as_ref() {
                Bound::Included(key) => range_builder.le(key.as_bytes()),
                Bound::Excluded(key) => range_builder.lt(key.as_bytes()),
                Bound::Unbounded => range_builder,
            };
            let mut stream = range_builder.into_stream().unwrap();
            let mut btree_set_range = words.range((lower_bound, upper_bound));
            while stream.advance() {
                let val = btree_set_range.next().unwrap();
                assert_eq!(val.as_bytes(), stream.key());
            }
            assert!(btree_set_range.next().is_none());
        }
    }
}
