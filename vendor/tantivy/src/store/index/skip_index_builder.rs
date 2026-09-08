use std::io;
use std::io::Write;

use common::{BinarySerializable, VInt};

use crate::store::index::block::CheckpointBlock;
use crate::store::index::{Checkpoint, CHECKPOINT_PERIOD};

// Each skip contains iterator over pairs (last doc in block, offset to start of block).

struct LayerBuilder {
    buffer: Vec<u8>,
    pub block: CheckpointBlock,
}

impl LayerBuilder {
    fn finish(self) -> Vec<u8> {
        self.buffer
    }

    fn new() -> LayerBuilder {
        LayerBuilder {
            buffer: Vec::new(),
            block: CheckpointBlock::default(),
        }
    }

    /// Serializes the block, and return a checkpoint representing
    /// the entire block.
    ///
    /// If the block was empty to begin with, simply return `None`.
    fn flush_block(&mut self) -> Option<Checkpoint> {
        if let Some(doc_range) = self.block.doc_interval() {
            let start_offset = self.buffer.len();
            self.block.serialize(&mut self.buffer);
            let end_offset = self.buffer.len();
            self.block.clear();
            Some(Checkpoint {
                doc_range,
                byte_range: start_offset..end_offset,
            })
        } else {
            None
        }
    }

    fn push(&mut self, checkpoint: Checkpoint) {
        self.block.push(checkpoint);
    }

    fn insert(&mut self, checkpoint: Checkpoint) -> Option<Checkpoint> {
        self.push(checkpoint);
        let emit_skip_info = self.block.len() >= CHECKPOINT_PERIOD;
        if emit_skip_info {
            self.flush_block()
        } else {
            None
        }
    }
}

pub struct SkipIndexBuilder {
    layers: Vec<LayerBuilder>,
}

impl SkipIndexBuilder {
    pub fn new() -> SkipIndexBuilder {
        SkipIndexBuilder { layers: Vec::new() }
    }

    fn get_layer(&mut self, layer_id: usize) -> &mut LayerBuilder {
        if layer_id == self.layers.len() {
            let layer_builder = LayerBuilder::new();
            self.layers.push(layer_builder);
        }
        &mut self.layers[layer_id]
    }

    pub fn insert(&mut self, checkpoint: Checkpoint) {
        let mut skip_pointer = Some(checkpoint);
        for layer_id in 0.. {
            if let Some(checkpoint) = skip_pointer {
                skip_pointer = self.get_layer(layer_id).insert(checkpoint);
            } else {
                break;
            }
        }
    }

    pub fn serialize_into<W: Write>(mut self, output: &mut W) -> io::Result<()> {
        let mut last_pointer = None;
        for skip_layer in self.layers.iter_mut() {
            if let Some(checkpoint) = last_pointer {
                skip_layer.push(checkpoint);
            }
            last_pointer = skip_layer.flush_block();
        }
        let layer_buffers: Vec<Vec<u8>> = self
            .layers
            .into_iter()
            .rev()
            .map(|layer| layer.finish())
            .collect();

        let mut layer_offset = 0;
        let mut layer_sizes = Vec::new();
        for layer_buffer in &layer_buffers {
            layer_offset += layer_buffer.len() as u64;
            layer_sizes.push(VInt(layer_offset));
        }
        layer_sizes.serialize(output)?;
        for layer_buffer in layer_buffers {
            output.write_all(&layer_buffer[..])?;
        }
        Ok(())
    }
}

pub(crate) struct AsyncSkipIndexBuilder {
    layers: Vec<AsyncLayer>,
    usable: bool,
}

impl AsyncSkipIndexBuilder {
    pub fn new() -> Self {
        Self {
            layers: Vec::new(),
            usable: true,
        }
    }

    pub async fn insert(
        &mut self,
        checkpoint: Checkpoint,
        directory: &dyn crate::directory::Directory,
    ) -> io::Result<()> {
        self.begin()?;
        let mut pointer = Some(checkpoint);
        for layer_id in 0.. {
            let Some(checkpoint) = pointer else { break };
            if layer_id == self.layers.len() {
                self.layers.push(AsyncLayer {
                    spool: crate::directory::async_spool::AsyncSpool::open(directory).await?,
                    block: CheckpointBlock::default(),
                });
            }
            let layer = &mut self.layers[layer_id];
            layer.block.push(checkpoint);
            pointer = if layer.block.len() >= CHECKPOINT_PERIOD {
                layer.flush().await?
            } else {
                None
            };
        }
        self.usable = true;
        Ok(())
    }

    pub async fn serialize_into(
        mut self,
        directory: &dyn crate::directory::Directory,
        output: &mut dyn crate::directory::AsyncWrite,
    ) -> io::Result<()> {
        self.begin()?;
        let mut pointer = None;
        for layer in &mut self.layers {
            if let Some(checkpoint) = pointer {
                layer.block.push(checkpoint);
            }
            pointer = layer.flush().await?;
        }
        let mut offset = 0;
        let sizes: Vec<VInt> = self
            .layers
            .iter()
            .rev()
            .map(|layer| {
                offset += layer.spool.len();
                VInt(offset)
            })
            .collect();
        let mut header = Vec::new();
        sizes.serialize(&mut header)?;
        output.write_all(&header).await?;
        for layer in self.layers.into_iter().rev() {
            layer.spool.copy_to(directory, output).await?;
        }
        Ok(())
    }

    fn begin(&mut self) -> io::Result<()> {
        if !std::mem::replace(&mut self.usable, false) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Document index output was cancelled or failed",
            ));
        }
        Ok(())
    }
}

struct AsyncLayer {
    spool: crate::directory::async_spool::AsyncSpool,
    block: CheckpointBlock,
}

impl AsyncLayer {
    async fn flush(&mut self) -> io::Result<Option<Checkpoint>> {
        let Some(doc_range) = self.block.doc_interval() else {
            return Ok(None);
        };
        let start = self.spool.len() as usize;
        let mut bytes = Vec::new();
        self.block.serialize(&mut bytes);
        self.spool.append(&bytes).await?;
        self.block.clear();
        Ok(Some(Checkpoint {
            doc_range,
            byte_range: start..self.spool.len() as usize,
        }))
    }
}
