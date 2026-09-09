#![allow(clippy::needless_borrows_for_generic_args)]

use std::cmp::Ordering;
use std::io;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use common::bounds::{TransformBound, transform_bound_inner_res};
use common::file_slice::FileSlice;
use common::{BinarySerializable, ByteCount, OwnedBytes};
use futures_util::{StreamExt, TryStreamExt, stream};
use itertools::Itertools;
use tantivy_fst::Automaton;
use tantivy_fst::automaton::AlwaysMatch;

use crate::sstable_index_v3::SSTableIndexV3Empty;
use crate::streamer::{Streamer, StreamerBuilder};
use crate::{
    BlockAddr, DeltaReader, Reader, SSTable, SSTableIndex, SSTableIndexV3, TermOrdinal, VoidSSTable,
};

/// An SSTable is a sorted map that associates sorted `&[u8]` keys
/// to any kind of typed values.
///
/// The SSTable is organized in blocks.
/// In each block, keys and values are encoded separately.
///
/// The keys are encoded using incremental encoding.
/// The values on the other hand, are encoded according to a value-specific
/// codec defined in the TSSTable generic argument.
///
/// Finally, an index is joined to the Dictionary to make it possible,
/// given a key to identify which block contains this key.
///
/// The codec was designed in such a way that the sstable
/// reader is not aware of block, and yet can read any sequence of blocks,
/// as long as the slice of bytes it is given starts and stops at
/// block boundary.
///
/// (See also README.md)
#[derive(Debug, Clone)]
pub struct Dictionary<TSSTable: SSTable = VoidSSTable> {
    pub sstable_slice: FileSlice,
    pub sstable_index: SSTableIndex,
    num_bytes: ByteCount,
    num_terms: u64,
    phantom_data: PhantomData<TSSTable>,
}

impl Dictionary<VoidSSTable> {
    pub fn build_for_tests(terms: &[&str]) -> Dictionary {
        let mut terms = terms.to_vec();
        terms.sort();
        let mut buffer = Vec::new();
        let mut dictionary_writer = Self::builder(&mut buffer).unwrap();
        for term in terms {
            dictionary_writer.insert(term, &()).unwrap();
        }
        dictionary_writer.finish().unwrap();
        Dictionary::from_bytes(OwnedBytes::new(buffer)).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TermOrdHit {
    /// Exact term ord hit
    Exact(TermOrdinal),
    /// Next best term ordinal
    Next(TermOrdinal),
}

impl TermOrdHit {
    fn into_exact(self) -> Option<TermOrdinal> {
        match self {
            TermOrdHit::Exact(ord) => Some(ord),
            TermOrdHit::Next(_) => None,
        }
    }

    fn map<F: FnOnce(TermOrdinal) -> TermOrdinal>(self, f: F) -> Self {
        match self {
            TermOrdHit::Exact(ord) => TermOrdHit::Exact(f(ord)),
            TermOrdHit::Next(ord) => TermOrdHit::Next(f(ord)),
        }
    }
}

impl<TSSTable: SSTable> Dictionary<TSSTable> {
    pub fn builder<W: io::Write>(wrt: W) -> io::Result<crate::Writer<W, TSSTable::ValueWriter>> {
        Ok(TSSTable::writer(wrt))
    }

    pub(crate) fn sstable_reader_block(
        &self,
        block_addr: BlockAddr,
    ) -> io::Result<Reader<TSSTable::ValueReader>> {
        let data = self.sstable_slice.read_bytes_slice(block_addr.byte_range)?;
        Ok(TSSTable::reader(data))
    }

    pub(crate) async fn sstable_delta_reader_for_key_range_async(
        &self,
        key_range: impl RangeBounds<[u8]>,
        limit: Option<u64>,
        automaton: &impl Automaton,
        merge_holes_under_bytes: usize,
    ) -> io::Result<DeltaReader<TSSTable::ValueReader>> {
        let match_all = automaton.will_always_match(&automaton.start());
        if match_all {
            let slice = self.file_slice_for_range(key_range, limit);
            let data = slice.read_bytes_async().await?;
            Ok(TSSTable::delta_reader(data))
        } else {
            let blocks = stream::iter(self.get_block_iterator_for_range_and_automaton(
                key_range,
                automaton,
                merge_holes_under_bytes,
            ));
            let data = blocks
                .map(|block_addr| {
                    self.sstable_slice
                        .read_bytes_slice_async(block_addr.byte_range)
                })
                .buffered(5)
                .try_collect::<Vec<_>>()
                .await?;
            Ok(DeltaReader::from_multiple_blocks(data))
        }
    }

    pub(crate) fn sstable_delta_reader_for_key_range(
        &self,
        key_range: impl RangeBounds<[u8]>,
        limit: Option<u64>,
        automaton: &impl Automaton,
    ) -> io::Result<DeltaReader<TSSTable::ValueReader>> {
        let match_all = automaton.will_always_match(&automaton.start());
        if match_all {
            let slice = self.file_slice_for_range(key_range, limit);
            let data = slice.read_bytes()?;
            Ok(TSSTable::delta_reader(data))
        } else {
            // if operations are sync, we assume latency is almost null, and there is no point in
            // merging across holes
            let blocks = self.get_block_iterator_for_range_and_automaton(key_range, automaton, 0);
            let data = blocks
                .map(|block_addr| self.sstable_slice.read_bytes_slice(block_addr.byte_range))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DeltaReader::from_multiple_blocks(data))
        }
    }

    pub(crate) fn sstable_delta_reader_block(
        &self,
        block_addr: BlockAddr,
    ) -> io::Result<DeltaReader<TSSTable::ValueReader>> {
        let data = self.sstable_slice.read_bytes_slice(block_addr.byte_range)?;
        Ok(TSSTable::delta_reader(data))
    }

    pub(crate) async fn sstable_delta_reader_block_async(
        &self,
        block_addr: BlockAddr,
    ) -> io::Result<DeltaReader<TSSTable::ValueReader>> {
        let data = self
            .sstable_slice
            .read_bytes_slice_async(block_addr.byte_range)
            .await?;
        Ok(TSSTable::delta_reader(data))
    }

    /// This function returns a file slice covering a set of sstable blocks
    /// that include the key range passed in arguments. Optionally returns
    /// only block for up to `limit` matching terms.
    ///
    /// It works by identifying
    /// - `first_block`: the block containing the start boundary key
    /// - `last_block`: the block containing the end boundary key.
    ///
    /// And then returning the range that spans over all blocks between.
    /// and including first_block and last_block, aka:
    /// `[first_block.start_offset .. last_block.end_offset)`
    ///
    /// Technically this function does not provide the tightest fit, as
    /// for simplification, it treats the start bound of the `key_range`
    /// as if it was inclusive, even if it is exclusive.
    /// On the rare edge case where a user asks for `(start_key, end_key]`
    /// and `start_key` happens to be the last key of a block, we return a
    /// slice that is the first block was not necessary.
    pub fn file_slice_for_range(
        &self,
        key_range: impl RangeBounds<[u8]>,
        limit: Option<u64>,
    ) -> FileSlice {
        let first_block_id = match key_range.start_bound() {
            Bound::Included(key) | Bound::Excluded(key) => {
                let Some(first_block_id) = self.sstable_index.locate_with_key(key) else {
                    return FileSlice::empty();
                };
                Some(first_block_id)
            }
            Bound::Unbounded => None,
        };

        let last_block_id = match key_range.end_bound() {
            Bound::Included(key) | Bound::Excluded(key) => self.sstable_index.locate_with_key(key),
            Bound::Unbounded => None,
        };

        let start_bound = if let Some(first_block_id) = first_block_id {
            let Some(block_addr) = self.sstable_index.get_block(first_block_id) else {
                return FileSlice::empty();
            };
            Bound::Included(block_addr.byte_range.start)
        } else {
            Bound::Unbounded
        };

        let last_block_id = if let Some(limit) = limit {
            let second_block_id = first_block_id.map(|id| id + 1).unwrap_or(0);
            if let Some(block_addr) = self.sstable_index.get_block(second_block_id) {
                let ordinal_limit = block_addr.first_ordinal + limit;
                let last_block_limit = self.sstable_index.locate_with_ord(ordinal_limit);
                if let Some(last_block_id) = last_block_id {
                    Some(last_block_id.min(last_block_limit))
                } else {
                    Some(last_block_limit)
                }
            } else {
                last_block_id
            }
        } else {
            last_block_id
        };
        let end_bound = last_block_id
            .and_then(|block_id| self.sstable_index.get_block(block_id))
            .map(|block_addr| Bound::Excluded(block_addr.byte_range.end))
            .unwrap_or(Bound::Unbounded);

        self.sstable_slice.slice((start_bound, end_bound))
    }

    fn get_block_iterator_for_range_and_automaton<'a>(
        &'a self,
        key_range: impl RangeBounds<[u8]>,
        automaton: &'a impl Automaton,
        merge_holes_under_bytes: usize,
    ) -> impl Iterator<Item = BlockAddr> + 'a {
        let lower_bound = match key_range.start_bound() {
            Bound::Included(key) | Bound::Excluded(key) => {
                self.sstable_index.locate_with_key(key).unwrap_or(u64::MAX)
            }
            Bound::Unbounded => 0,
        };

        let upper_bound = match key_range.end_bound() {
            Bound::Included(key) | Bound::Excluded(key) => {
                self.sstable_index.locate_with_key(key).unwrap_or(u64::MAX)
            }
            Bound::Unbounded => u64::MAX,
        };
        let block_range = lower_bound..=upper_bound;
        self.sstable_index
            .get_block_for_automaton(automaton)
            .filter(move |(block_id, _)| block_range.contains(block_id))
            .map(|(_, block_addr)| block_addr)
            .coalesce(move |first, second| {
                if first.byte_range.end + merge_holes_under_bytes >= second.byte_range.start {
                    Ok(BlockAddr {
                        first_ordinal: first.first_ordinal,
                        byte_range: first.byte_range.start..second.byte_range.end,
                    })
                } else {
                    Err((first, second))
                }
            })
    }

    /// Opens a `TermDictionary`.
    pub fn open(term_dictionary_file: FileSlice) -> io::Result<Self> {
        let num_bytes = term_dictionary_file.num_bytes();
        let (main_slice, footer_len_slice) = term_dictionary_file.split_from_end(20);
        let mut footer_len_bytes: OwnedBytes = footer_len_slice.read_bytes()?;
        let index_offset = u64::deserialize(&mut footer_len_bytes)?;
        let num_terms = u64::deserialize(&mut footer_len_bytes)?;
        let version = u32::deserialize(&mut footer_len_bytes)?;
        let (sstable_slice, index_slice) = main_slice.split(index_offset as usize);
        let sstable_index_bytes = index_slice.read_bytes()?;

        let sstable_index = match version {
            2 => SSTableIndex::V2(
                crate::sstable_index_v2::SSTableIndex::load(sstable_index_bytes).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "SSTable corruption")
                })?,
            ),
            3 => {
                let (sstable_index_bytes, mut footerv3_len_bytes) = sstable_index_bytes.rsplit(8);
                let store_offset = u64::deserialize(&mut footerv3_len_bytes)?;
                if store_offset != 0 {
                    SSTableIndex::V3(
                        SSTableIndexV3::load(sstable_index_bytes, store_offset).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "SSTable corruption")
                        })?,
                    )
                } else {
                    // if store_offset is zero, there is no index, so we build a pseudo-index
                    // assuming a single block of sstable covering everything.
                    SSTableIndex::V3Empty(SSTableIndexV3Empty::load(index_offset as usize))
                }
            }
            _ => {
                return Err(io::Error::other(format!(
                    "Unsupported sstable version, expected one of [2, 3], found {version}"
                )));
            }
        };

        Ok(Dictionary {
            sstable_slice,
            sstable_index,
            num_bytes,
            num_terms,
            phantom_data: PhantomData,
        })
    }

    /// Creates a term dictionary from the supplied bytes.
    pub fn from_bytes(owned_bytes: OwnedBytes) -> io::Result<Self> {
        Dictionary::open(FileSlice::new(Arc::new(owned_bytes)))
    }

    /// Creates an empty term dictionary which contains no terms.
    pub fn empty() -> Self {
        let term_dictionary_data: Vec<u8> = Self::builder(Vec::<u8>::new())
            .expect("Creating a TermDictionaryBuilder in a Vec<u8> should never fail")
            .finish()
            .expect("Writing in a Vec<u8> should never fail");
        let empty_dict_file = FileSlice::from(term_dictionary_data);
        Dictionary::open(empty_dict_file).unwrap()
    }

    /// Returns the number of terms in the dictionary.
    /// Term ordinals range from 0 to `num_terms() - 1`.
    pub fn num_terms(&self) -> usize {
        self.num_terms as usize
    }

    /// Returns the total number of bytes used by the dictionary on disk.
    pub fn num_bytes(&self) -> ByteCount {
        self.num_bytes
    }

    /// Decode a DeltaReader up to key, returning the number of terms traversed
    ///
    /// If the key was not found, returns Ok(None).
    /// After calling this function, it is possible to call `DeltaReader::value` to get the
    /// associated value.
    fn decode_up_to_key<K: AsRef<[u8]>>(
        &self,
        key: K,
        sstable_delta_reader: &mut DeltaReader<TSSTable::ValueReader>,
    ) -> io::Result<Option<TermOrdinal>> {
        self.decode_up_to_or_next(key, sstable_delta_reader)
            .map(|hit| hit.into_exact())
    }
    /// Decode a DeltaReader up to key, returning the number of terms traversed
    ///
    /// If the key was not found, it returns the next term id.
    fn decode_up_to_or_next<K: AsRef<[u8]>>(
        &self,
        key: K,
        sstable_delta_reader: &mut DeltaReader<TSSTable::ValueReader>,
    ) -> io::Result<TermOrdHit> {
        let mut term_ord = 0;
        let key_bytes = key.as_ref();
        let mut ok_bytes = 0;
        while sstable_delta_reader.advance()? {
            let prefix_len = sstable_delta_reader.common_prefix_len();
            let suffix = sstable_delta_reader.suffix();

            match prefix_len.cmp(&ok_bytes) {
                Ordering::Less => return Ok(TermOrdHit::Next(term_ord)), /* popped bytes already matched => too far */
                Ordering::Equal => (),
                Ordering::Greater => {
                    // the ok prefix is less than current entry prefix => continue to next elem
                    term_ord += 1;
                    continue;
                }
            }

            // we have ok_bytes byte of common prefix, check if this key adds more
            for (key_byte, suffix_byte) in key_bytes[ok_bytes..].iter().zip(suffix) {
                match suffix_byte.cmp(key_byte) {
                    Ordering::Less => break,          // byte too small
                    Ordering::Equal => ok_bytes += 1, // new matching
                    // byte
                    Ordering::Greater => return Ok(TermOrdHit::Next(term_ord)), // too far
                }
            }

            if ok_bytes == key_bytes.len() {
                if prefix_len + suffix.len() == ok_bytes {
                    return Ok(TermOrdHit::Exact(term_ord));
                } else {
                    // current key is a prefix of current element, not a match
                    return Ok(TermOrdHit::Next(term_ord));
                }
            }

            term_ord += 1;
        }

        Ok(TermOrdHit::Next(term_ord))
    }

    /// Returns the ordinal associated with a given term.
    pub fn term_ord<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TermOrdinal>> {
        let key_bytes = key.as_ref();

        let Some(block_addr) = self.sstable_index.get_block_with_key(key_bytes) else {
            return Ok(None);
        };

        let first_ordinal = block_addr.first_ordinal;
        let mut sstable_delta_reader = self.sstable_delta_reader_block(block_addr)?;
        self.decode_up_to_key(key_bytes, &mut sstable_delta_reader)
            .map(|opt| opt.map(|ord| ord + first_ordinal))
    }

    /// Returns the ordinal associated with a given term or its closest next term_id
    /// The closest next term_id may not exist.
    pub fn term_ord_or_next<K: AsRef<[u8]>>(&self, key: K) -> io::Result<TermOrdHit> {
        let key_bytes = key.as_ref();

        let Some(block_addr) = self.sstable_index.get_block_with_key(key_bytes) else {
            // TODO: Would be more consistent to return last_term id + 1
            return Ok(TermOrdHit::Next(u64::MAX));
        };

        let first_ordinal = block_addr.first_ordinal;
        let mut sstable_delta_reader = self.sstable_delta_reader_block(block_addr)?;
        self.decode_up_to_or_next(key_bytes, &mut sstable_delta_reader)
            .map(|opt| opt.map(|ord| ord + first_ordinal))
    }

    /// Converts strings into a Bound range.
    /// This does handle several special cases if the term is not exactly in the dictionary.
    /// e.g. [bbb, ddd]
    /// lower_bound: Bound::Included(aaa) => Included(0) // "Next" term id
    /// lower_bound: Bound::Excluded(aaa) => Included(0) // "Next" term id + Change the Bounds
    /// lower_bound: Bound::Included(ccc) => Included(1) // "Next" term id
    /// lower_bound: Bound::Excluded(ccc) => Included(1) // "Next" term id + Change the Bounds
    /// lower_bound: Bound::Included(zzz) => Included(2) // "Next" term id
    /// lower_bound: Bound::Excluded(zzz) => Included(2) // "Next" term id + Change the Bounds
    /// For zzz we should have some post processing to return an empty query`
    ///
    /// upper_bound: Bound::Included(aaa) => Excluded(0) // "Next" term id + Change the bounds
    /// upper_bound: Bound::Excluded(aaa) => Excluded(0) // "Next" term id
    /// upper_bound: Bound::Included(ccc) => Excluded(1) // Next term id + Change the bounds
    /// upper_bound: Bound::Excluded(ccc) => Excluded(1) // Next term id
    /// upper_bound: Bound::Included(zzz) => Excluded(2) // Next term id + Change the bounds
    /// upper_bound: Bound::Excluded(zzz) => Excluded(2) // Next term id
    pub fn term_bounds_to_ord<K: AsRef<[u8]>>(
        &self,
        lower_bound: Bound<K>,
        upper_bound: Bound<K>,
    ) -> io::Result<(Bound<TermOrdinal>, Bound<TermOrdinal>)> {
        let lower_bound = transform_bound_inner_res(&lower_bound, |start_bound_bytes| {
            let ord = self.term_ord_or_next(start_bound_bytes)?;
            match ord {
                TermOrdHit::Exact(ord) => Ok(TransformBound::Existing(ord)),
                TermOrdHit::Next(ord) => Ok(TransformBound::NewBound(Bound::Included(ord))), /* Change bounds to included */
            }
        })?;
        let upper_bound = transform_bound_inner_res(&upper_bound, |end_bound_bytes| {
            let ord = self.term_ord_or_next(end_bound_bytes)?;
            match ord {
                TermOrdHit::Exact(ord) => Ok(TransformBound::Existing(ord)),
                TermOrdHit::Next(ord) => Ok(TransformBound::NewBound(Bound::Excluded(ord))), /* Change bounds to excluded */
            }
        })?;
        Ok((lower_bound, upper_bound))
    }

    /// Returns the term associated with a given term ordinal.
    ///
    /// Term ordinals are defined as the position of the term in
    /// the sorted list of terms.
    ///
    /// Returns true if and only if the term has been found.
    ///
    /// Regardless of whether the term is found or not,
    /// the buffer may be modified.
    pub fn ord_to_term(&self, ord: TermOrdinal, bytes: &mut Vec<u8>) -> io::Result<bool> {
        // find block in which the term would be
        let block_addr = self.sstable_index.get_block_with_ord(ord);
        let first_ordinal = block_addr.first_ordinal;

        // then search inside that block only
        let mut sstable_delta_reader = self.sstable_delta_reader_block(block_addr)?;
        for _ in first_ordinal..=ord {
            if !sstable_delta_reader.advance()? {
                return Ok(false);
            }
            bytes.truncate(sstable_delta_reader.common_prefix_len());
            bytes.extend_from_slice(sstable_delta_reader.suffix());
        }
        Ok(true)
    }

    /// Returns the terms for a _sorted_ list of term ordinals.
    ///
    /// Returns true if and only if all terms have been found.
    pub fn sorted_ords_to_term_cb<F: FnMut(&[u8]) -> io::Result<()>>(
        &self,
        mut ords: impl Iterator<Item = TermOrdinal>,
        mut cb: F,
    ) -> io::Result<bool> {
        let Some(mut ord) = ords.next() else {
            return Ok(true);
        };

        // Open the block for the first ordinal.
        let mut bytes = Vec::new();
        let mut current_block_addr = self.sstable_index.get_block_with_ord(ord);
        let mut current_sstable_delta_reader =
            self.sstable_delta_reader_block(current_block_addr.clone())?;
        let mut current_block_ordinal = current_block_addr.first_ordinal;

        loop {
            // move to the ord inside the current block
            while current_block_ordinal <= ord {
                if !current_sstable_delta_reader.advance()? {
                    return Ok(false);
                }
                bytes.truncate(current_sstable_delta_reader.common_prefix_len());
                bytes.extend_from_slice(current_sstable_delta_reader.suffix());
                current_block_ordinal += 1;
            }
            cb(&bytes)?;

            // fetch the next ordinal
            let Some(next_ord) = ords.next() else {
                return Ok(true);
            };

            // advance forward if the new ord is different than the one we just processed
            //
            // this allows the input TermOrdinal iterator to contain duplicates, so long as it's
            // still sorted
            if next_ord < ord {
                panic!("Ordinals were not sorted: received {next_ord} after {ord}");
            } else if next_ord > ord {
                // check if block changed for new term_ord
                let new_block_addr = self.sstable_index.get_block_with_ord(next_ord);
                if new_block_addr != current_block_addr {
                    current_block_addr = new_block_addr;
                    current_block_ordinal = current_block_addr.first_ordinal;
                    current_sstable_delta_reader =
                        self.sstable_delta_reader_block(current_block_addr.clone())?;
                    bytes.clear();
                }
                ord = next_ord;
            } else {
                // The next ord is equal to the previous ord: no need to seek or advance.
            }
        }
    }

    /// Returns the number of terms in the dictionary.
    pub fn term_info_from_ord(&self, term_ord: TermOrdinal) -> io::Result<Option<TSSTable::Value>> {
        // find block in which the term would be
        let block_addr = self.sstable_index.get_block_with_ord(term_ord);
        let first_ordinal = block_addr.first_ordinal;

        // then search inside that block only
        let mut sstable_reader = self.sstable_reader_block(block_addr)?;
        for _ in first_ordinal..=term_ord {
            if !sstable_reader.advance()? {
                return Ok(None);
            }
        }
        Ok(Some(sstable_reader.value().clone()))
    }

    /// Lookups the value corresponding to the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TSSTable::Value>> {
        if let Some(block_addr) = self.sstable_index.get_block_with_key(key.as_ref()) {
            let sstable_reader = self.sstable_delta_reader_block(block_addr)?;
            return self.do_get(key, sstable_reader);
        }
        Ok(None)
    }

    /// Lookups the value corresponding to the key.
    pub async fn get_async<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TSSTable::Value>> {
        if let Some(block_addr) = self.sstable_index.get_block_with_key(key.as_ref()) {
            let sstable_reader = self.sstable_delta_reader_block_async(block_addr).await?;
            return self.do_get(key, sstable_reader);
        }
        Ok(None)
    }

    fn do_get<K: AsRef<[u8]>>(
        &self,
        key: K,
        mut reader: DeltaReader<TSSTable::ValueReader>,
    ) -> io::Result<Option<TSSTable::Value>> {
        if let Some(_ord) = self.decode_up_to_key(key, &mut reader)? {
            Ok(Some(reader.value().clone()))
        } else {
            Ok(None)
        }
    }

    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    pub fn range(&self) -> StreamerBuilder<'_, TSSTable> {
        StreamerBuilder::new(self, AlwaysMatch)
    }

    /// Returns a range builder filtered with a prefix.
    pub fn prefix_range<K: AsRef<[u8]>>(&self, prefix: K) -> StreamerBuilder<'_, TSSTable> {
        let lower_bound = prefix.as_ref();
        let mut upper_bound = lower_bound.to_vec();
        for idx in (0..upper_bound.len()).rev() {
            if upper_bound[idx] == 255 {
                upper_bound.pop();
            } else {
                upper_bound[idx] += 1;
                break;
            }
        }
        let mut builder = self.range().ge(lower_bound);
        if !upper_bound.is_empty() {
            builder = builder.lt(upper_bound);
        }
        builder
    }

    /// A stream of all the sorted terms.
    pub fn stream(&self) -> io::Result<Streamer<'_, TSSTable>> {
        self.range().into_stream()
    }

    /// Returns a search builder, to stream all of the terms
    /// within the Automaton
    pub fn search<'a, A: Automaton + 'a>(
        &'a self,
        automaton: A,
    ) -> StreamerBuilder<'a, TSSTable, A>
    where
        A::State: Clone,
    {
        StreamerBuilder::<TSSTable, A>::new(self, automaton)
    }

    #[doc(hidden)]
    pub async fn warm_up_dictionary(&self) -> io::Result<()> {
        self.sstable_slice.read_bytes_async().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, Range};
    use std::sync::{Arc, Mutex};

    use common::OwnedBytes;

    use super::Dictionary;
    use crate::MonotonicU64SSTable;
    use crate::dictionary::TermOrdHit;

    #[derive(Debug)]
    struct PermissionedHandle {
        bytes: OwnedBytes,
        allowed_range: Mutex<Range<usize>>,
    }

    impl PermissionedHandle {
        fn new(bytes: Vec<u8>) -> Self {
            let bytes = OwnedBytes::new(bytes);
            PermissionedHandle {
                allowed_range: Mutex::new(0..bytes.len()),
                bytes,
            }
        }

        fn restrict(&self, range: Range<usize>) {
            *self.allowed_range.lock().unwrap() = range;
        }
    }

    impl common::HasLen for PermissionedHandle {
        fn len(&self) -> usize {
            self.bytes.len()
        }
    }

    impl common::file_slice::FileHandle for PermissionedHandle {
        fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
            let allowed_range = self.allowed_range.lock().unwrap();
            if !allowed_range.contains(&range.start) || !allowed_range.contains(&(range.end - 1)) {
                return Err(std::io::Error::other(format!(
                    "invalid range, allowed {allowed_range:?}, requested {range:?}"
                )));
            }

            Ok(self.bytes.slice(range))
        }
    }

    fn make_test_sstable() -> (Dictionary<MonotonicU64SSTable>, Arc<PermissionedHandle>) {
        let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();

        // this makes 256k keys, enough to fill multiple blocks.
        for elem in 0..0x3ffff {
            let key = format!("{elem:05X}").into_bytes();
            builder.insert(&key, &elem).unwrap();
        }

        let table = builder.finish().unwrap();
        let table = Arc::new(PermissionedHandle::new(table));
        let slice = common::file_slice::FileSlice::new(table.clone());

        let dictionary = Dictionary::<MonotonicU64SSTable>::open(slice).unwrap();

        // if the last block is id 0, tests are meaningless
        assert_ne!(dictionary.sstable_index.locate_with_ord(u64::MAX), 0);
        assert_eq!(dictionary.num_terms(), 0x3ffff);
        (dictionary, table)
    }

    #[test]
    fn test_term_to_ord_or_next() {
        let dict = {
            let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();

            builder.insert(b"bbb", &1).unwrap();
            builder.insert(b"ddd", &2).unwrap();

            let table = builder.finish().unwrap();
            let table = Arc::new(PermissionedHandle::new(table));
            let slice = common::file_slice::FileSlice::new(table.clone());

            Dictionary::<MonotonicU64SSTable>::open(slice).unwrap()
        };

        assert_eq!(dict.term_ord_or_next(b"aaa").unwrap(), TermOrdHit::Next(0));
        assert_eq!(dict.term_ord_or_next(b"bbb").unwrap(), TermOrdHit::Exact(0));
        assert_eq!(dict.term_ord_or_next(b"bb").unwrap(), TermOrdHit::Next(0));
        assert_eq!(dict.term_ord_or_next(b"bbbb").unwrap(), TermOrdHit::Next(1));
        assert_eq!(dict.term_ord_or_next(b"dd").unwrap(), TermOrdHit::Next(1));
        assert_eq!(dict.term_ord_or_next(b"ddd").unwrap(), TermOrdHit::Exact(1));
        assert_eq!(dict.term_ord_or_next(b"dddd").unwrap(), TermOrdHit::Next(2));

        // This is not u64::MAX because for very small sstables (only one block),
        // we don't store an index, and the pseudo-index always reply that the
        // answer lies in block number 0
        assert_eq!(
            dict.term_ord_or_next(b"zzzzzzz").unwrap(),
            TermOrdHit::Next(2)
        );
    }
    #[test]
    fn test_term_to_ord_or_next_2() {
        let dict = {
            let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();

            let mut term_ord = 0;
            builder.insert(b"bbb", &term_ord).unwrap();

            // Fill blocks in between
            for elem in 0..50_000 {
                term_ord += 1;
                let key = format!("ccccc{elem:05X}").into_bytes();
                builder.insert(&key, &term_ord).unwrap();
            }

            term_ord += 1;
            builder.insert(b"eee", &term_ord).unwrap();

            let table = builder.finish().unwrap();
            let table = Arc::new(PermissionedHandle::new(table));
            let slice = common::file_slice::FileSlice::new(table.clone());

            Dictionary::<MonotonicU64SSTable>::open(slice).unwrap()
        };

        assert_eq!(dict.term_ord(b"bbb").unwrap(), Some(0));
        assert_eq!(dict.term_ord_or_next(b"bbb").unwrap(), TermOrdHit::Exact(0));
        assert_eq!(dict.term_ord_or_next(b"aaa").unwrap(), TermOrdHit::Next(0));
        assert_eq!(dict.term_ord_or_next(b"bb").unwrap(), TermOrdHit::Next(0));
        assert_eq!(dict.term_ord_or_next(b"bbbb").unwrap(), TermOrdHit::Next(1));
        assert_eq!(
            dict.term_ord_or_next(b"ee").unwrap(),
            TermOrdHit::Next(50001)
        );
        assert_eq!(
            dict.term_ord_or_next(b"eee").unwrap(),
            TermOrdHit::Exact(50001)
        );
        assert_eq!(
            dict.term_ord_or_next(b"eeee").unwrap(),
            TermOrdHit::Next(u64::MAX)
        );

        assert_eq!(
            dict.term_ord_or_next(b"zzzzzzz").unwrap(),
            TermOrdHit::Next(u64::MAX)
        );
    }

    #[test]
    fn test_term_bounds_to_ord() {
        let dict = {
            let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();

            builder.insert(b"bbb", &1).unwrap();
            builder.insert(b"ddd", &2).unwrap();

            let table = builder.finish().unwrap();
            let table = Arc::new(PermissionedHandle::new(table));
            let slice = common::file_slice::FileSlice::new(table.clone());

            Dictionary::<MonotonicU64SSTable>::open(slice).unwrap()
        };

        // Test cases for lower_bound
        let test_lower_bound = |bound, expected| {
            assert_eq!(
                dict.term_bounds_to_ord::<&[u8]>(bound, Bound::Included(b"ignored"))
                    .unwrap()
                    .0,
                expected
            );
        };

        test_lower_bound(Bound::Included(b"aaa".as_slice()), Bound::Included(0));
        test_lower_bound(Bound::Excluded(b"aaa".as_slice()), Bound::Included(0));

        test_lower_bound(Bound::Included(b"bbb".as_slice()), Bound::Included(0));
        test_lower_bound(Bound::Excluded(b"bbb".as_slice()), Bound::Excluded(0));

        test_lower_bound(Bound::Included(b"ccc".as_slice()), Bound::Included(1));
        test_lower_bound(Bound::Excluded(b"ccc".as_slice()), Bound::Included(1));

        test_lower_bound(Bound::Included(b"zzz".as_slice()), Bound::Included(2));
        test_lower_bound(Bound::Excluded(b"zzz".as_slice()), Bound::Included(2));

        // Test cases for upper_bound
        let test_upper_bound = |bound, expected| {
            assert_eq!(
                dict.term_bounds_to_ord::<&[u8]>(Bound::Included(b"ignored"), bound,)
                    .unwrap()
                    .1,
                expected
            );
        };
        test_upper_bound(Bound::Included(b"ccc".as_slice()), Bound::Excluded(1));
        test_upper_bound(Bound::Excluded(b"ccc".as_slice()), Bound::Excluded(1));
        test_upper_bound(Bound::Included(b"zzz".as_slice()), Bound::Excluded(2));
        test_upper_bound(Bound::Excluded(b"zzz".as_slice()), Bound::Excluded(2));
        test_upper_bound(Bound::Included(b"ddd".as_slice()), Bound::Included(1));
        test_upper_bound(Bound::Excluded(b"ddd".as_slice()), Bound::Excluded(1));
    }

    #[test]
    fn test_ord_term_conversion() {
        let (dic, slice) = make_test_sstable();

        let block = dic.sstable_index.get_block_with_ord(100_000);
        slice.restrict(block.byte_range);

        let mut res = Vec::new();

        // middle of a block
        assert!(dic.ord_to_term(100_000, &mut res).unwrap());
        assert_eq!(res, format!("{:05X}", 100_000).into_bytes());
        assert_eq!(dic.term_info_from_ord(100_000).unwrap().unwrap(), 100_000);
        assert_eq!(dic.get(&res).unwrap().unwrap(), 100_000);
        assert_eq!(dic.term_ord(&res).unwrap().unwrap(), 100_000);

        // start of a block
        assert!(dic.ord_to_term(block.first_ordinal, &mut res).unwrap());
        assert_eq!(res, format!("{:05X}", block.first_ordinal).into_bytes());
        assert_eq!(
            dic.term_info_from_ord(block.first_ordinal)
                .unwrap()
                .unwrap(),
            block.first_ordinal
        );
        assert_eq!(dic.get(&res).unwrap().unwrap(), block.first_ordinal);
        assert_eq!(dic.term_ord(&res).unwrap().unwrap(), block.first_ordinal);

        // end of a block
        let ordinal = block.first_ordinal - 1;
        let new_range = dic.sstable_index.get_block_with_ord(ordinal).byte_range;
        slice.restrict(new_range);
        assert!(dic.ord_to_term(ordinal, &mut res).unwrap());
        assert_eq!(res, format!("{ordinal:05X}").into_bytes());
        assert_eq!(dic.term_info_from_ord(ordinal).unwrap().unwrap(), ordinal);
        assert_eq!(dic.get(&res).unwrap().unwrap(), ordinal);
        assert_eq!(dic.term_ord(&res).unwrap().unwrap(), ordinal);

        // before first block
        // 1st block must be loaded for key-related operations
        let block = dic.sstable_index.get_block_with_ord(0);
        slice.restrict(block.byte_range);

        assert!(dic.get(b"$$$").unwrap().is_none());
        assert!(dic.term_ord(b"$$$").unwrap().is_none());

        // after last block
        // last block must be loaded for ord related operations
        let ordinal = 0x40000 + 10;
        let new_range = dic.sstable_index.get_block_with_ord(ordinal).byte_range;
        slice.restrict(new_range);
        assert!(!dic.ord_to_term(ordinal, &mut res).unwrap());
        assert!(dic.term_info_from_ord(ordinal).unwrap().is_none());

        // last block isn't required to be loaded for key related operations
        slice.restrict(0..0);
        assert!(dic.get(b"~~~").unwrap().is_none());
        assert!(dic.term_ord(b"~~~").unwrap().is_none());

        slice.restrict(0..slice.bytes.len());
        // between 1000F and 10010, test case where matched prefix > prefix kept
        assert!(dic.term_ord(b"1000G").unwrap().is_none());
        // shorter than 10000, tests prefix case
        assert!(dic.term_ord(b"1000").unwrap().is_none());
    }

    #[test]
    fn test_ords_term() {
        let (dic, _slice) = make_test_sstable();

        // Single term
        let mut terms = Vec::new();
        assert!(
            dic.sorted_ords_to_term_cb(100_000..100_001, |term| {
                terms.push(term.to_vec());
                Ok(())
            })
            .unwrap()
        );
        assert_eq!(terms, vec![format!("{:05X}", 100_000).into_bytes(),]);
        // Single term
        let mut terms = Vec::new();
        assert!(
            dic.sorted_ords_to_term_cb(100_001..100_002, |term| {
                terms.push(term.to_vec());
                Ok(())
            })
            .unwrap()
        );
        assert_eq!(terms, vec![format!("{:05X}", 100_001).into_bytes(),]);
        // both terms
        let mut terms = Vec::new();
        assert!(
            dic.sorted_ords_to_term_cb(100_000..100_002, |term| {
                terms.push(term.to_vec());
                Ok(())
            })
            .unwrap()
        );
        assert_eq!(
            terms,
            vec![
                format!("{:05X}", 100_000).into_bytes(),
                format!("{:05X}", 100_001).into_bytes(),
            ]
        );
        // Test cross block
        let mut terms = Vec::new();
        assert!(
            dic.sorted_ords_to_term_cb(98653..=98655, |term| {
                terms.push(term.to_vec());
                Ok(())
            })
            .unwrap()
        );
        assert_eq!(
            terms,
            vec![
                format!("{:05X}", 98653).into_bytes(),
                format!("{:05X}", 98654).into_bytes(),
                format!("{:05X}", 98655).into_bytes(),
            ]
        );
    }

    #[test]
    fn test_range() {
        let (dic, slice) = make_test_sstable();

        let start = dic
            .sstable_index
            .get_block_with_key(b"10000")
            .unwrap()
            .byte_range;
        let end = dic
            .sstable_index
            .get_block_with_key(b"18000")
            .unwrap()
            .byte_range;
        slice.restrict(start.start..end.end);

        let mut stream = dic.range().ge(b"10000").lt(b"18000").into_stream().unwrap();

        for i in 0x10000..0x18000 {
            assert!(stream.advance());
            assert_eq!(stream.term_ord(), i);
            assert_eq!(stream.value(), &i);
            assert_eq!(stream.key(), format!("{i:05X}").into_bytes());
        }
        assert!(!stream.advance());

        // verify limiting the number of results reduce the size read
        slice.restrict(start.start..(end.end - 1));

        let mut stream = dic
            .range()
            .ge(b"10000")
            .lt(b"18000")
            .limit(0xfff)
            .into_stream()
            .unwrap();

        for i in 0x10000..0x10fff {
            assert!(stream.advance());
            assert_eq!(stream.term_ord(), i);
            assert_eq!(stream.value(), &i);
            assert_eq!(stream.key(), format!("{i:05X}").into_bytes());
        }
        // there might be more successful elements after, though how many is undefined

        slice.restrict(0..slice.bytes.len());

        let mut stream = dic.stream().unwrap();
        for i in 0..0x3ffff {
            assert!(stream.advance());
            assert_eq!(stream.term_ord(), i);
            assert_eq!(stream.value(), &i);
            assert_eq!(stream.key(), format!("{i:05X}").into_bytes());
        }
        assert!(!stream.advance());
    }

    #[test]
    fn test_prefix() {
        let (dic, _slice) = make_test_sstable();
        {
            let mut stream = dic.prefix_range("1").into_stream().unwrap();
            for i in 0x10000..0x20000 {
                assert!(stream.advance());
                assert_eq!(stream.term_ord(), i);
                assert_eq!(stream.value(), &i);
                assert_eq!(stream.key(), format!("{i:05X}").into_bytes());
            }
            assert!(!stream.advance());
        }
        {
            let mut stream = dic.prefix_range("").into_stream().unwrap();
            for i in 0..0x3ffff {
                assert!(stream.advance(), "failed at {i:05X}");
                assert_eq!(stream.term_ord(), i);
                assert_eq!(stream.value(), &i);
                assert_eq!(stream.key(), format!("{i:05X}").into_bytes());
            }
            assert!(!stream.advance());
        }
        {
            let mut stream = dic.prefix_range("0FF").into_stream().unwrap();
            for i in 0x0ff00..=0x0ffff {
                assert!(stream.advance(), "failed at {i:05X}");
                assert_eq!(stream.term_ord(), i);
                assert_eq!(stream.value(), &i);
                assert_eq!(stream.key(), format!("{i:05X}").into_bytes());
            }
            assert!(!stream.advance());
        }
    }

    #[test]
    fn test_prefix_edge() {
        let dict = {
            let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
            builder.insert(&[0, 254], &0).unwrap();
            builder.insert(&[0, 255], &1).unwrap();
            builder.insert(&[0, 255, 12], &2).unwrap();
            builder.insert(&[1], &2).unwrap();
            builder.insert(&[1, 0], &2).unwrap();
            let table = builder.finish().unwrap();
            let table = Arc::new(PermissionedHandle::new(table));
            let slice = common::file_slice::FileSlice::new(table.clone());
            Dictionary::<MonotonicU64SSTable>::open(slice).unwrap()
        };

        let mut stream = dict.prefix_range(&[0, 255]).into_stream().unwrap();
        assert!(stream.advance());
        assert_eq!(stream.key(), &[0, 255]);
        assert!(stream.advance());
        assert_eq!(stream.key(), &[0, 255, 12]);
        assert!(!stream.advance());
    }
}
