use common::{BinarySerializable, VInt};

use crate::directory::OwnedBytes;
use crate::store::index::block::CheckpointBlock;
use crate::store::index::Checkpoint;
use crate::DocId;

pub struct LayerCursor<'a> {
    remaining: &'a [u8],
    block: CheckpointBlock,
    cursor: usize,
}

impl Iterator for LayerCursor<'_> {
    type Item = Checkpoint;

    fn next(&mut self) -> Option<Checkpoint> {
        if self.cursor == self.block.len() {
            if self.remaining.is_empty() {
                return None;
            }
            let (block_mut, remaining_mut) = (&mut self.block, &mut self.remaining);
            block_mut.deserialize(remaining_mut).ok()?;
            self.cursor = 0;
        }
        let res = Some(self.block.get(self.cursor));
        self.cursor += 1;
        res
    }
}

struct Layer {
    data: OwnedBytes,
}

impl Layer {
    fn cursor(&self) -> impl Iterator<Item = Checkpoint> + '_ {
        self.cursor_at_offset(0)
    }

    fn cursor_at_offset(&self, start_offset: usize) -> impl Iterator<Item = Checkpoint> + '_ {
        let data = &self.data.as_slice();
        LayerCursor {
            remaining: &data[start_offset..],
            block: CheckpointBlock::default(),
            cursor: 0,
        }
    }

    fn seek_start_at_offset(&self, target: DocId, offset: usize) -> Option<Checkpoint> {
        self.cursor_at_offset(offset)
            .find(|checkpoint| checkpoint.doc_range.end > target)
    }
}

pub struct SkipIndex {
    layers: Vec<Layer>,
}

impl SkipIndex {
    pub fn open(mut data: OwnedBytes) -> SkipIndex {
        let offsets: Vec<u64> = Vec::<VInt>::deserialize(&mut data)
            .unwrap()
            .into_iter()
            .map(|el| el.0)
            .collect();
        let mut start_offset = 0;
        let mut layers = Vec::new();
        for end_offset in offsets {
            let layer = Layer {
                data: data.slice(start_offset as usize..end_offset as usize),
            };
            layers.push(layer);
            start_offset = end_offset;
        }
        SkipIndex { layers }
    }

    pub(crate) fn checkpoints(&self) -> impl Iterator<Item = Checkpoint> + '_ {
        self.layers
            .last()
            .into_iter()
            .flat_map(|layer| layer.cursor())
    }

    pub fn seek(&self, target: DocId) -> Option<Checkpoint> {
        let first_layer_len = self
            .layers
            .first()
            .map(|layer| layer.data.len())
            .unwrap_or(0);
        let mut cur_checkpoint = Checkpoint {
            doc_range: 0u32..1u32,
            byte_range: 0..first_layer_len,
        };
        for layer in &self.layers {
            if let Some(checkpoint) =
                layer.seek_start_at_offset(target, cur_checkpoint.byte_range.start)
            {
                cur_checkpoint = checkpoint;
            } else {
                return None;
            }
        }
        Some(cur_checkpoint)
    }
}
