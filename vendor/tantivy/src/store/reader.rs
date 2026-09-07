use std::fmt::Display;
use std::io;
use std::iter::Sum;
use std::num::NonZeroUsize;
use std::ops::{AddAssign, Range};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::{BinarySerializable, OwnedBytes};
use lru::LruCache;

use super::footer::DocStoreFooter;
use super::index::SkipIndex;
use super::Decompressor;
use crate::directory::FileSlice;
use crate::error::DataCorruption;
use crate::fastfield::AliveBitSet;
use crate::schema::document::{BinaryDocumentDeserializer, DocumentDeserialize};
use crate::space_usage::StoreSpaceUsage;
use crate::store::index::Checkpoint;
use crate::DocId;
#[cfg(feature = "quickwit")]
use crate::Executor;

pub(crate) const DOCSTORE_CACHE_CAPACITY: usize = 100;

type Block = OwnedBytes;

/// The format version of the document store.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub(crate) enum DocStoreVersion {
    V1 = 1,
    V2 = 2,
}
impl Display for DocStoreVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DocStoreVersion::V1 => write!(f, "V1"),
            DocStoreVersion::V2 => write!(f, "V2"),
        }
    }
}
impl BinarySerializable for DocStoreVersion {
    fn serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        (*self as u32).serialize(writer)
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        Ok(match u32::deserialize(reader)? {
            1 => DocStoreVersion::V1,
            2 => DocStoreVersion::V2,
            v => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid doc store version {v}"),
                ))
            }
        })
    }
}

/// Reads document off tantivy's [`Store`](./index.html)
pub struct StoreReader {
    decompressor: Decompressor,
    doc_store_version: DocStoreVersion,
    data: FileSlice,
    skip_index: Arc<SkipIndex>,
    space_usage: StoreSpaceUsage,
    cache: BlockCache,
}

/// The cache for decompressed blocks.
struct BlockCache {
    cache: Option<Mutex<LruCache<usize, Block>>>,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
}

impl BlockCache {
    fn get_from_cache(&self, pos: usize) -> Option<Block> {
        if let Some(block) = self
            .cache
            .as_ref()
            .and_then(|cache| cache.lock().unwrap().get(&pos).cloned())
        {
            self.cache_hits.fetch_add(1, Ordering::SeqCst);
            return Some(block);
        }
        self.cache_misses.fetch_add(1, Ordering::SeqCst);
        None
    }

    fn put_into_cache(&self, pos: usize, data: Block) {
        if let Some(cache) = self.cache.as_ref() {
            cache.lock().unwrap().put(pos, data);
        }
    }

    fn stats(&self) -> CacheStats {
        CacheStats {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            num_entries: self.len(),
        }
    }

    fn len(&self) -> usize {
        self.cache
            .as_ref()
            .map_or(0, |cache| cache.lock().unwrap().len())
    }

    #[cfg(test)]
    fn peek_lru(&self) -> Option<usize> {
        self.cache
            .as_ref()
            .and_then(|cache| cache.lock().unwrap().peek_lru().map(|(&k, _)| k))
    }
}

#[derive(Debug, Default)]
/// CacheStats for the `StoreReader`.
pub struct CacheStats {
    /// The number of entries in the cache
    pub num_entries: usize,
    /// The number of cache hits.
    pub cache_hits: usize,
    /// The number of cache misses.
    pub cache_misses: usize,
}

impl AddAssign for CacheStats {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            num_entries: self.num_entries + other.num_entries,
            cache_hits: self.cache_hits + other.cache_hits,
            cache_misses: self.cache_misses + other.cache_misses,
        };
    }
}

impl Sum for CacheStats {
    fn sum<I: Iterator<Item = Self>>(mut iter: I) -> Self {
        let mut first = iter.next().unwrap_or_default();
        for el in iter {
            first += el;
        }
        first
    }
}

impl StoreReader {
    /// Opens a store reader
    ///
    /// `cache_num_blocks` sets the number of decompressed blocks to be cached in an LRU.
    /// The size of blocks is configurable, this should be reflexted in the
    pub fn open(store_file: FileSlice, cache_num_blocks: usize) -> io::Result<StoreReader> {
        let (footer, data_and_offset) = DocStoreFooter::extract_footer(store_file)?;

        let (data_file, offset_index_file) = data_and_offset.split(footer.offset as usize);
        let index_data = offset_index_file.read_bytes()?;
        let space_usage =
            StoreSpaceUsage::new(data_file.num_bytes(), offset_index_file.num_bytes());
        let skip_index = SkipIndex::open(index_data);
        Ok(StoreReader {
            decompressor: footer.decompressor,
            doc_store_version: footer.doc_store_version,
            data: data_file,
            cache: BlockCache {
                cache: NonZeroUsize::new(cache_num_blocks)
                    .map(|cache_num_blocks| Mutex::new(LruCache::new(cache_num_blocks))),
                cache_hits: Default::default(),
                cache_misses: Default::default(),
            },
            skip_index: Arc::new(skip_index),
            space_usage,
        })
    }

    pub(crate) fn block_checkpoints(&self) -> impl Iterator<Item = Checkpoint> + '_ {
        self.skip_index.checkpoints()
    }

    pub(crate) fn decompressor(&self) -> Decompressor {
        self.decompressor
    }

    /// Returns the cache hit and miss statistics of the store reader.
    pub(crate) fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    /// Get checkpoint for `DocId`. The checkpoint can be used to load a block containing the
    /// document.
    ///
    /// Advanced API. In most cases use [`get`](Self::get).
    fn block_checkpoint(&self, doc_id: DocId) -> crate::Result<Checkpoint> {
        self.skip_index.seek(doc_id).ok_or_else(|| {
            crate::TantivyError::InvalidArgument(format!("Failed to lookup Doc #{doc_id}."))
        })
    }

    pub(crate) fn block_data(&self) -> io::Result<OwnedBytes> {
        self.data.read_bytes()
    }

    fn get_compressed_block(&self, checkpoint: &Checkpoint) -> io::Result<OwnedBytes> {
        self.data.slice(checkpoint.byte_range.clone()).read_bytes()
    }

    /// Loads and decompresses a block.
    ///
    /// Advanced API. In most cases use [`get`](Self::get).
    fn read_block(&self, checkpoint: &Checkpoint) -> io::Result<Block> {
        let cache_key = checkpoint.byte_range.start;
        if let Some(block) = self.cache.get_from_cache(cache_key) {
            return Ok(block);
        }

        let compressed_block = self.get_compressed_block(checkpoint)?;
        let decompressed_block =
            OwnedBytes::new(self.decompressor.decompress(compressed_block.as_ref())?);

        self.cache
            .put_into_cache(cache_key, decompressed_block.clone());

        Ok(decompressed_block)
    }

    /// Reads a given document.
    ///
    /// Calling `.get(doc)` is relatively costly as it requires
    /// decompressing a compressed block. The store utilizes a LRU cache,
    /// so accessing docs from the same compressed block should be faster.
    /// For that reason a store reader should be kept and reused.
    ///
    /// It should not be called to score documents
    /// for instance.
    pub fn get<D: DocumentDeserialize>(&self, doc_id: DocId) -> crate::Result<D> {
        let mut doc_bytes = self.get_document_bytes(doc_id)?;

        let deserializer =
            BinaryDocumentDeserializer::from_reader(&mut doc_bytes, self.doc_store_version)
                .map_err(crate::TantivyError::from)?;
        D::deserialize(deserializer).map_err(crate::TantivyError::from)
    }

    /// Returns raw bytes of a given document.
    ///
    /// Calling `.get(doc)` is relatively costly as it requires
    /// decompressing a compressed block. The store utilizes a LRU cache,
    /// so accessing docs from the same compressed block should be faster.
    /// For that reason a store reader should be kept and reused.
    pub fn get_document_bytes(&self, doc_id: DocId) -> crate::Result<OwnedBytes> {
        let checkpoint = self.block_checkpoint(doc_id)?;
        let block = self.read_block(&checkpoint)?;
        Self::get_document_bytes_from_block(block, doc_id, &checkpoint)
    }

    /// Advanced API.
    ///
    /// In most cases use [`get_document_bytes`](Self::get_document_bytes).
    fn get_document_bytes_from_block(
        block: OwnedBytes,
        doc_id: DocId,
        checkpoint: &Checkpoint,
    ) -> crate::Result<OwnedBytes> {
        let doc_pos = doc_id - checkpoint.doc_range.start;

        let range = block_read_index(&block, doc_pos)?;
        Ok(block.slice(range))
    }

    /// Iterator over all Documents in their order as they are stored in the doc store.
    /// Use this, if you want to extract all Documents from the doc store.
    /// The `alive_bitset` has to be forwarded from the `SegmentReader` or the results may be wrong.
    pub fn iter<'a: 'b, 'b, D: DocumentDeserialize>(
        &'b self,
        alive_bitset: Option<&'a AliveBitSet>,
    ) -> impl Iterator<Item = crate::Result<D>> + 'b {
        self.iter_raw(alive_bitset).map(|doc_bytes_res| {
            let mut doc_bytes = doc_bytes_res?;

            let deserializer =
                BinaryDocumentDeserializer::from_reader(&mut doc_bytes, self.doc_store_version)
                    .map_err(crate::TantivyError::from)?;
            D::deserialize(deserializer).map_err(crate::TantivyError::from)
        })
    }

    /// Iterator over all raw Documents in their order as they are stored in the doc store.
    /// Use this, if you want to extract all Documents from the doc store.
    /// The `alive_bitset` has to be forwarded from the `SegmentReader` or the results may be wrong.
    pub(crate) fn iter_raw<'a: 'b, 'b>(
        &'b self,
        alive_bitset: Option<&'a AliveBitSet>,
    ) -> impl Iterator<Item = crate::Result<OwnedBytes>> + 'b {
        let last_doc_id = self
            .block_checkpoints()
            .last()
            .map(|checkpoint| checkpoint.doc_range.end)
            .unwrap_or(0);
        let mut checkpoint_block_iter = self.block_checkpoints();
        let mut curr_checkpoint = checkpoint_block_iter.next();
        let mut curr_block = curr_checkpoint
            .as_ref()
            .map(|checkpoint| self.read_block(checkpoint).map_err(|e| e.kind())); // map error in order to enable cloning
        let mut doc_pos = 0;
        (0..last_doc_id)
            .filter_map(move |doc_id| {
                // filter_map is only used to resolve lifetime issues between the two closures on
                // the outer variables

                // check move to next checkpoint
                if doc_id >= curr_checkpoint.as_ref().unwrap().doc_range.end {
                    curr_checkpoint = checkpoint_block_iter.next();
                    curr_block = curr_checkpoint
                        .as_ref()
                        .map(|checkpoint| self.read_block(checkpoint).map_err(|e| e.kind()));
                    doc_pos = 0;
                }

                let alive = alive_bitset
                    .map(|bitset| bitset.is_alive(doc_id))
                    .unwrap_or(true);
                let res = if alive {
                    Some((curr_block.clone(), doc_pos))
                } else {
                    None
                };
                doc_pos += 1;
                res
            })
            .map(move |(block, doc_pos)| {
                let block = block
                    .ok_or_else(|| {
                        DataCorruption::comment_only(
                            "the current checkpoint in the doc store iterator is none, this \
                             should never happen",
                        )
                    })?
                    .map_err(|error_kind| {
                        std::io::Error::new(error_kind, "error when reading block in doc store")
                    })?;

                let range = block_read_index(&block, doc_pos)?;
                Ok(block.slice(range))
            })
    }

    /// Summarize total space usage of this store reader.
    pub fn space_usage(&self) -> StoreSpaceUsage {
        self.space_usage.clone()
    }
}

fn block_read_index(block: &[u8], doc_pos: u32) -> crate::Result<Range<usize>> {
    let doc_pos = doc_pos as usize;
    let size_of_u32 = std::mem::size_of::<u32>();

    let index_len_pos = block.len() - size_of_u32;
    let index_len = u32::deserialize(&mut &block[index_len_pos..])? as usize;

    if doc_pos > index_len {
        return Err(crate::TantivyError::InternalError(
            "Attempted to read doc from wrong block".to_owned(),
        ));
    }

    let index_start = block.len() - (index_len + 1) * size_of_u32;
    let index = &block[index_start..index_start + index_len * size_of_u32];

    let start_offset = u32::deserialize(&mut &index[doc_pos * size_of_u32..])? as usize;
    let end_offset = u32::deserialize(&mut &index[(doc_pos + 1) * size_of_u32..])
        .unwrap_or(index_start as u32) as usize;
    Ok(start_offset..end_offset)
}

#[cfg(feature = "quickwit")]
impl StoreReader {
    /// Advanced API.
    ///
    /// In most cases use [`get_async`](Self::get_async)
    ///
    /// Loads and decompresses a block asynchronously.
    async fn read_block_async(
        &self,
        checkpoint: &Checkpoint,
        executor: &Executor,
    ) -> io::Result<Block> {
        let cache_key = checkpoint.byte_range.start;
        if let Some(block) = self.cache.get_from_cache(checkpoint.byte_range.start) {
            return Ok(block);
        }

        let compressed_block = self
            .data
            .slice(checkpoint.byte_range.clone())
            .read_bytes_async()
            .await?;

        let decompressor = self.decompressor;
        let maybe_decompressed_block = executor
            .spawn_blocking(move || decompressor.decompress(compressed_block.as_ref()))
            .await
            .expect("decompression panicked");
        let decompressed_block = OwnedBytes::new(maybe_decompressed_block?);

        self.cache
            .put_into_cache(cache_key, decompressed_block.clone());

        Ok(decompressed_block)
    }

    /// Reads raw bytes of a given document asynchronously.
    pub async fn get_document_bytes_async(
        &self,
        doc_id: DocId,
        executor: &Executor,
    ) -> crate::Result<OwnedBytes> {
        let checkpoint = self.block_checkpoint(doc_id)?;
        let block = self.read_block_async(&checkpoint, executor).await?;
        Self::get_document_bytes_from_block(block, doc_id, &checkpoint)
    }

    /// Fetches a document asynchronously. Async version of [`get`](Self::get).
    pub async fn get_async<D: DocumentDeserialize>(
        &self,
        doc_id: DocId,
        executor: &Executor,
    ) -> crate::Result<D> {
        let mut doc_bytes = self.get_document_bytes_async(doc_id, executor).await?;

        let deserializer =
            BinaryDocumentDeserializer::from_reader(&mut doc_bytes, self.doc_store_version)
                .map_err(crate::TantivyError::from)?;
        D::deserialize(deserializer).map_err(crate::TantivyError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::directory::RamDirectory;
    use crate::schema::{Field, TantivyDocument, Value};
    use crate::store::tests::write_lorem_ipsum_store;
    use crate::store::Compressor;
    use crate::Directory;

    const BLOCK_SIZE: usize = 16_384;

    fn get_text_field<'a>(doc: &'a TantivyDocument, field: &'a Field) -> Option<&'a str> {
        doc.get_first(*field).and_then(|f| f.as_value().as_str())
    }

    #[test]
    fn test_doc_store_version_ord() {
        assert!(DocStoreVersion::V1 < DocStoreVersion::V2);
    }

    #[test]
    fn test_store_lru_cache() -> crate::Result<()> {
        let directory = RamDirectory::create();
        let path = Path::new("store");
        let writer = directory.open_write(path)?;
        let schema = write_lorem_ipsum_store(writer, 500, Compressor::None, BLOCK_SIZE, true);
        let title = schema.get_field("title").unwrap();
        let store_file = directory.open_read(path)?;
        let store = StoreReader::open(store_file, DOCSTORE_CACHE_CAPACITY)?;

        assert_eq!(store.cache.len(), 0);
        assert_eq!(store.cache_stats().cache_hits, 0);
        assert_eq!(store.cache_stats().cache_misses, 0);

        let doc = store.get(0)?;
        assert_eq!(get_text_field(&doc, &title), Some("Doc 0"));

        assert_eq!(store.cache.len(), 1);
        assert_eq!(store.cache_stats().cache_hits, 0);
        assert_eq!(store.cache_stats().cache_misses, 1);

        assert_eq!(store.cache.peek_lru(), Some(0));

        let doc = store.get(499)?;
        assert_eq!(get_text_field(&doc, &title), Some("Doc 499"));

        assert_eq!(store.cache.len(), 2);
        assert_eq!(store.cache_stats().cache_hits, 0);
        assert_eq!(store.cache_stats().cache_misses, 2);

        assert_eq!(store.cache.peek_lru(), Some(0));

        let doc = store.get(0)?;
        assert_eq!(get_text_field(&doc, &title), Some("Doc 0"));

        assert_eq!(store.cache.len(), 2);
        assert_eq!(store.cache_stats().cache_hits, 1);
        assert_eq!(store.cache_stats().cache_misses, 2);

        assert_eq!(store.cache.peek_lru(), Some(232206));

        Ok(())
    }
}
