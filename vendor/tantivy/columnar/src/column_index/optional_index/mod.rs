use std::io;
use std::sync::Arc;

mod set;
mod set_block;

use common::{BinarySerializable, OwnedBytes, VInt};
pub use set::{SelectCursor, Set, SetCodec};
use set_block::{
    DENSE_BLOCK_NUM_BYTES, DenseBlock, DenseBlockCodec, SparseBlock, SparseBlockCodec,
};

use crate::iterable::Iterable;
use crate::{DocId, RowId};

/// The threshold for for number of elements after which we switch to dense block encoding.
///
/// We simply pick the value that minimize the size of the blocks.
const DENSE_BLOCK_THRESHOLD: u32 =
    set_block::DENSE_BLOCK_NUM_BYTES / std::mem::size_of::<u16>() as u32; //< 5_120

const ELEMENTS_PER_BLOCK: u32 = u16::MAX as u32 + 1;

#[derive(Copy, Clone, Debug)]
struct BlockMeta {
    non_null_rows_before_block: u32,
    start_byte_offset: u32,
    block_variant: BlockVariant,
}

#[derive(Clone, Copy, Debug)]
enum BlockVariant {
    Dense,
    Sparse { num_vals: u16 },
}

impl BlockVariant {
    pub fn empty() -> Self {
        Self::Sparse { num_vals: 0 }
    }
    pub fn num_bytes_in_block(&self) -> u32 {
        match *self {
            BlockVariant::Dense => set_block::DENSE_BLOCK_NUM_BYTES,
            BlockVariant::Sparse { num_vals } => num_vals as u32 * 2,
        }
    }
}

/// This codec is inspired by roaring bitmaps.
/// In the dense blocks, however, in order to accelerate `select`
/// we interleave an offset over two bytes. (more on this lower)
///
/// The lower 16 bits of doc ids are stored as u16 while the upper 16 bits are given by the block
/// id. Each block contains 1<<16 docids.
///
/// # Serialized Data Layout
/// The data starts with the block data. Each block is either dense or sparse encoded, depending on
/// the number of values in the block. A block is sparse when it contains less than
/// DENSE_BLOCK_THRESHOLD (6144) values.
/// [Sparse data block | dense data block, .. #repeat*; Desc: Either a sparse or dense encoded
/// block]
/// ### Sparse block data
/// [u16 LE, .. #repeat*; Desc: Positions with values in a block]
/// ### Dense block data
/// [Dense codec for the whole block; Desc: Similar to a bitvec(0..ELEMENTS_PER_BLOCK) + Metadata
/// for faster lookups. See dense.rs]
///
/// The data is followed by block metadata, to know which area of the raw block data belongs to
/// which block. Only metadata for blocks with elements is recorded to
/// keep the overhead low for scenarios with many very sparse columns. The block metadata consists
/// of the block index and the number of values in the block. Since we don't store empty blocks
/// num_vals is incremented by 1, e.g. 0 means 1 value.
///
/// The last u16 is storing the number of metadata blocks.
/// [u16 LE, .. #repeat*; Desc: Positions with values in a block][(u16 LE, u16 LE), .. #repeat*;
/// Desc: (Block Id u16, Num Elements u16)][u16 LE; Desc: num blocks with values u16]
///
/// # Opening
/// When opening the data layout, the data is expanded to `Vec<SparseCodecBlockVariant>`, where the
/// index is the block index. For each block `byte_start` and `offset` is computed.
#[derive(Clone)]
pub struct OptionalIndex {
    num_docs: RowId,
    num_non_null_docs: RowId,
    block_data: OwnedBytes,
    block_metas: Arc<[BlockMeta]>,
}

impl Iterable<u32> for &OptionalIndex {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        Box::new(self.iter_non_null_docs())
    }
}

impl std::fmt::Debug for OptionalIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("OptionalIndex")
            .field("num_docs", &self.num_docs)
            .field("num_non_null_docs", &self.num_non_null_docs)
            .finish_non_exhaustive()
    }
}

/// Splits a value address into lower and upper 16bits.
/// The lower 16 bits are the value in the block
/// The upper 16 bits are the block index
#[derive(Copy, Debug, Clone)]
struct RowAddr {
    block_id: u16,
    in_block_row_id: u16,
}

#[inline(always)]
fn row_addr_from_row_id(row_id: RowId) -> RowAddr {
    RowAddr {
        block_id: (row_id / ELEMENTS_PER_BLOCK) as u16,
        in_block_row_id: (row_id % ELEMENTS_PER_BLOCK) as u16,
    }
}

enum BlockSelectCursor<'a> {
    Dense(<DenseBlock<'a> as Set<u16>>::SelectCursor<'a>),
    Sparse(<SparseBlock<'a> as Set<u16>>::SelectCursor<'a>),
}

impl BlockSelectCursor<'_> {
    fn select(&mut self, rank: u16) -> u16 {
        match self {
            BlockSelectCursor::Dense(dense_select_cursor) => dense_select_cursor.select(rank),
            BlockSelectCursor::Sparse(sparse_select_cursor) => sparse_select_cursor.select(rank),
        }
    }
}
pub struct OptionalIndexSelectCursor<'a> {
    current_block_cursor: BlockSelectCursor<'a>,
    current_block_id: u16,
    // The current block is guaranteed to contain ranks < end_rank.
    current_block_end_rank: RowId,
    optional_index: &'a OptionalIndex,
    block_doc_idx_start: RowId,
    num_null_rows_before_block: RowId,
}

impl OptionalIndexSelectCursor<'_> {
    fn search_and_load_block(&mut self, rank: RowId) {
        if rank < self.current_block_end_rank {
            // we are already in the right block
            return;
        }
        self.current_block_id = self.optional_index.find_block(rank, self.current_block_id);
        self.current_block_end_rank = self
            .optional_index
            .block_metas
            .get(self.current_block_id as usize + 1)
            .map(|block_meta| block_meta.non_null_rows_before_block)
            .unwrap_or(u32::MAX);
        self.block_doc_idx_start = (self.current_block_id as u32) * ELEMENTS_PER_BLOCK;
        let block_meta = self.optional_index.block_metas[self.current_block_id as usize];
        self.num_null_rows_before_block = block_meta.non_null_rows_before_block;
        let block: Block<'_> = self.optional_index.block(block_meta);
        self.current_block_cursor = match block {
            Block::Dense(dense_block) => BlockSelectCursor::Dense(dense_block.select_cursor()),
            Block::Sparse(sparse_block) => BlockSelectCursor::Sparse(sparse_block.select_cursor()),
        };
    }
}

impl SelectCursor<RowId> for OptionalIndexSelectCursor<'_> {
    fn select(&mut self, rank: RowId) -> RowId {
        self.search_and_load_block(rank);
        let index_in_block = (rank - self.num_null_rows_before_block) as u16;
        self.current_block_cursor.select(index_in_block) as RowId + self.block_doc_idx_start
    }
}

impl Set<RowId> for OptionalIndex {
    type SelectCursor<'b>
        = OptionalIndexSelectCursor<'b>
    where Self: 'b;
    // Check if value at position is not null.
    #[inline]
    fn contains(&self, row_id: RowId) -> bool {
        let RowAddr {
            block_id,
            in_block_row_id,
        } = row_addr_from_row_id(row_id);
        let block_meta = self.block_metas[block_id as usize];
        match self.block(block_meta) {
            Block::Dense(dense_block) => dense_block.contains(in_block_row_id),
            Block::Sparse(sparse_block) => sparse_block.contains(in_block_row_id),
        }
    }

    /// Any value doc_id is allowed.
    /// In particular, doc_id = num_rows.
    #[inline]
    fn rank(&self, doc_id: DocId) -> RowId {
        if doc_id >= self.num_docs() {
            return self.num_non_nulls();
        }
        let RowAddr {
            block_id,
            in_block_row_id,
        } = row_addr_from_row_id(doc_id);
        let block_meta = self.block_metas[block_id as usize];
        let block = self.block(block_meta);

        let block_offset_row_id = match block {
            Block::Dense(dense_block) => dense_block.rank(in_block_row_id),
            Block::Sparse(sparse_block) => sparse_block.rank(in_block_row_id),
        } as u32;
        block_meta.non_null_rows_before_block + block_offset_row_id
    }

    /// Any value doc_id is allowed.
    /// In particular, doc_id = num_rows.
    #[inline]
    fn rank_if_exists(&self, doc_id: DocId) -> Option<RowId> {
        let RowAddr {
            block_id,
            in_block_row_id,
        } = row_addr_from_row_id(doc_id);
        let block_meta = *self.block_metas.get(block_id as usize)?;
        let block = self.block(block_meta);
        let block_offset_row_id = match block {
            Block::Dense(dense_block) => dense_block.rank_if_exists(in_block_row_id),
            Block::Sparse(sparse_block) => sparse_block.rank_if_exists(in_block_row_id),
        }? as u32;
        Some(block_meta.non_null_rows_before_block + block_offset_row_id)
    }

    #[inline]
    fn select(&self, rank: RowId) -> RowId {
        let block_pos = self.find_block(rank, 0);
        let block_doc_idx_start = (block_pos as u32) * ELEMENTS_PER_BLOCK;
        let block_meta = self.block_metas[block_pos as usize];
        let block: Block<'_> = self.block(block_meta);
        let index_in_block = (rank - block_meta.non_null_rows_before_block) as u16;
        let in_block_rank = match block {
            Block::Dense(dense_block) => dense_block.select(index_in_block),
            Block::Sparse(sparse_block) => sparse_block.select(index_in_block),
        };
        block_doc_idx_start + in_block_rank as u32
    }

    fn select_cursor(&self) -> OptionalIndexSelectCursor<'_> {
        OptionalIndexSelectCursor {
            current_block_cursor: BlockSelectCursor::Sparse(
                SparseBlockCodec::open(b"").select_cursor(),
            ),
            current_block_id: 0u16,
            current_block_end_rank: 0u32, //< this is sufficient to force the first load
            optional_index: self,
            block_doc_idx_start: 0u32,
            num_null_rows_before_block: 0u32,
        }
    }
}

impl OptionalIndex {
    pub fn for_test(num_rows: RowId, row_ids: &[RowId]) -> OptionalIndex {
        assert!(
            row_ids
                .last()
                .copied()
                .map(|last_row_id| last_row_id < num_rows)
                .unwrap_or(true)
        );
        let mut buffer = Vec::new();
        serialize_optional_index(&row_ids, num_rows, &mut buffer).unwrap();
        let bytes = OwnedBytes::new(buffer);
        open_optional_index(bytes).unwrap()
    }

    pub fn num_docs(&self) -> RowId {
        self.num_docs
    }

    pub fn num_non_nulls(&self) -> RowId {
        self.num_non_null_docs
    }

    pub fn iter_non_null_docs(&self) -> impl Iterator<Item = RowId> + '_ {
        // TODO optimize. We could iterate over the blocks directly.
        // We use the dense value ids and retrieve the doc ids via select.
        let mut select_batch = self.select_cursor();
        (0..self.num_non_null_docs).map(move |rank| select_batch.select(rank))
    }
    pub fn select_batch(&self, ranks: &mut [RowId]) {
        let mut select_cursor = self.select_cursor();
        for rank in ranks.iter_mut() {
            *rank = select_cursor.select(*rank);
        }
    }

    #[inline]
    fn block(&self, block_meta: BlockMeta) -> Block<'_> {
        let BlockMeta {
            start_byte_offset,
            block_variant,
            ..
        } = block_meta;
        let start_byte_offset = start_byte_offset as usize;
        let bytes = self.block_data.as_slice();
        match block_variant {
            BlockVariant::Dense => Block::Dense(DenseBlockCodec::open(
                &bytes[start_byte_offset..start_byte_offset + DENSE_BLOCK_NUM_BYTES as usize],
            )),
            BlockVariant::Sparse { num_vals } => {
                let end_byte_offset = start_byte_offset + num_vals as usize * 2;
                let sparse_bytes = &bytes[start_byte_offset..end_byte_offset];
                Block::Sparse(SparseBlockCodec::open(sparse_bytes))
            }
        }
    }

    #[inline]
    fn find_block(&self, dense_idx: u32, start_block_pos: u16) -> u16 {
        for block_pos in start_block_pos..self.block_metas.len() as u16 {
            let offset = self.block_metas[block_pos as usize].non_null_rows_before_block;
            if offset > dense_idx {
                return block_pos - 1u16;
            }
        }
        self.block_metas.len() as u16 - 1u16
    }

    // TODO Add a good API for the codec_idx to original_idx translation.
    // The Iterator API is a probably a bad idea
}

#[derive(Copy, Clone)]
enum Block<'a> {
    Dense(DenseBlock<'a>),
    Sparse(SparseBlock<'a>),
}

fn serialize_optional_index_block(block_els: &[u16], out: &mut impl io::Write) -> io::Result<()> {
    let is_sparse = is_sparse(block_els.len() as u32);
    if is_sparse {
        SparseBlockCodec::serialize(block_els.iter().copied(), out)?;
    } else {
        DenseBlockCodec::serialize(block_els.iter().copied(), out)?;
    }
    Ok(())
}

pub fn serialize_optional_index<W: io::Write>(
    non_null_rows: &dyn Iterable<RowId>,
    num_rows: RowId,
    output: &mut W,
) -> io::Result<()> {
    VInt(num_rows as u64).serialize(output)?;

    let mut rows_it = non_null_rows.boxed_iter();
    let mut block_metadata: Vec<SerializedBlockMeta> = Vec::new();
    let mut current_block = Vec::new();

    // This if-statement for the first element ensures that
    // `block_metadata` is not empty in the loop below.
    let Some(idx) = rows_it.next() else {
        output.write_all(&0u16.to_le_bytes())?;
        return Ok(());
    };

    let row_addr = row_addr_from_row_id(idx);

    let mut current_block_id = row_addr.block_id;
    current_block.push(row_addr.in_block_row_id);

    for idx in rows_it {
        let value_addr = row_addr_from_row_id(idx);
        if current_block_id != value_addr.block_id {
            serialize_optional_index_block(&current_block[..], output)?;
            block_metadata.push(SerializedBlockMeta {
                block_id: current_block_id,
                num_non_null_rows: current_block.len() as u32,
            });
            current_block.clear();
            current_block_id = value_addr.block_id;
        }
        current_block.push(value_addr.in_block_row_id);
    }

    // handle last block
    serialize_optional_index_block(&current_block[..], output)?;

    block_metadata.push(SerializedBlockMeta {
        block_id: current_block_id,
        num_non_null_rows: current_block.len() as u32,
    });

    for block in &block_metadata {
        output.write_all(&block.to_bytes())?;
    }

    output.write_all((block_metadata.len() as u16).to_le_bytes().as_ref())?;

    Ok(())
}

const SERIALIZED_BLOCK_META_NUM_BYTES: usize = 4;

#[derive(Clone, Copy, Debug)]
struct SerializedBlockMeta {
    block_id: u16,
    num_non_null_rows: u32, //< takes values in 1..=u16::MAX
}

// TODO unit tests
impl SerializedBlockMeta {
    #[inline]
    fn from_bytes(bytes: [u8; SERIALIZED_BLOCK_META_NUM_BYTES]) -> SerializedBlockMeta {
        let block_id = u16::from_le_bytes(bytes[0..2].try_into().unwrap());
        let num_non_null_rows: u32 =
            u16::from_le_bytes(bytes[2..4].try_into().unwrap()) as u32 + 1u32;
        SerializedBlockMeta {
            block_id,
            num_non_null_rows,
        }
    }

    #[inline]
    fn to_bytes(self) -> [u8; SERIALIZED_BLOCK_META_NUM_BYTES] {
        assert!(self.num_non_null_rows > 0);
        let mut bytes = [0u8; SERIALIZED_BLOCK_META_NUM_BYTES];
        bytes[0..2].copy_from_slice(&self.block_id.to_le_bytes());
        // We don't store empty blocks, therefore we can subtract 1.
        // This way we will be able to use u16 when the number of elements is 1 << 16 or u16::MAX+1
        bytes[2..4].copy_from_slice(&((self.num_non_null_rows - 1u32) as u16).to_le_bytes());
        bytes
    }
}

#[inline]
fn is_sparse(num_rows_in_block: u32) -> bool {
    num_rows_in_block < DENSE_BLOCK_THRESHOLD
}

fn deserialize_optional_index_block_metadatas(
    data: &[u8],
    num_rows: u32,
) -> (Box<[BlockMeta]>, u32) {
    let num_blocks = data.len() / SERIALIZED_BLOCK_META_NUM_BYTES;
    let mut block_metas = Vec::with_capacity(num_blocks + 1);
    let mut start_byte_offset = 0;
    let mut non_null_rows_before_block = 0;
    for block_meta_bytes in data.chunks_exact(SERIALIZED_BLOCK_META_NUM_BYTES) {
        let block_meta_bytes: [u8; SERIALIZED_BLOCK_META_NUM_BYTES] =
            block_meta_bytes.try_into().unwrap();
        let SerializedBlockMeta {
            block_id,
            num_non_null_rows,
        } = SerializedBlockMeta::from_bytes(block_meta_bytes);
        block_metas.resize(
            block_id as usize,
            BlockMeta {
                non_null_rows_before_block,
                start_byte_offset,
                block_variant: BlockVariant::empty(),
            },
        );
        let block_variant = if is_sparse(num_non_null_rows) {
            BlockVariant::Sparse {
                num_vals: num_non_null_rows as u16,
            }
        } else {
            BlockVariant::Dense
        };
        block_metas.push(BlockMeta {
            non_null_rows_before_block,
            start_byte_offset,
            block_variant,
        });
        start_byte_offset += block_variant.num_bytes_in_block();
        non_null_rows_before_block += num_non_null_rows;
    }
    block_metas.resize(
        num_rows.div_ceil(ELEMENTS_PER_BLOCK) as usize,
        BlockMeta {
            non_null_rows_before_block,
            start_byte_offset,
            block_variant: BlockVariant::empty(),
        },
    );
    (block_metas.into_boxed_slice(), non_null_rows_before_block)
}

pub fn open_optional_index(bytes: OwnedBytes) -> io::Result<OptionalIndex> {
    let (mut bytes, num_non_empty_blocks_bytes) = bytes.rsplit(2);
    let num_non_empty_block_bytes =
        u16::from_le_bytes(num_non_empty_blocks_bytes.as_slice().try_into().unwrap());
    let num_docs = VInt::deserialize_u64(&mut bytes)? as u32;
    let block_metas_num_bytes =
        num_non_empty_block_bytes as usize * SERIALIZED_BLOCK_META_NUM_BYTES;
    let (block_data, block_metas) = bytes.rsplit(block_metas_num_bytes);
    let (block_metas, num_non_null_docs) =
        deserialize_optional_index_block_metadatas(block_metas.as_slice(), num_docs);
    let optional_index = OptionalIndex {
        num_docs,
        num_non_null_docs,
        block_data,
        block_metas: block_metas.into(),
    };
    Ok(optional_index)
}

#[cfg(test)]
mod tests;
