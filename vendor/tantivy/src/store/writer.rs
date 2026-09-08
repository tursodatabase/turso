use std::io;

use common::BinarySerializable;

use super::compressors::Compressor;
use super::StoreReader;
use crate::directory::WritePtr;
use crate::schema::document::{BinaryDocumentSerializer, Document};
use crate::schema::Schema;
use crate::store::store_compressor::BlockCompressor;
use crate::DocId;

/// Write tantivy's [`Store`](./index.html)
///
/// Contrary to the other components of `tantivy`,
/// the store is written to disc as document as being added,
/// as opposed to when the segment is getting finalized.
///
/// The skip list index on the other hand, is built in memory.
pub struct StoreWriter {
    compressor: Compressor,
    block_size: usize,
    num_docs_in_current_block: DocId,
    current_block: Vec<u8>,
    doc_pos: Vec<u32>,
    block_compressor: BlockCompressor,
}

impl StoreWriter {
    /// Create a store writer.
    ///
    /// The store writer will writes blocks on disc as
    /// document are added.
    pub fn new(
        writer: WritePtr,
        compressor: Compressor,
        block_size: usize,
        dedicated_thread: bool,
    ) -> io::Result<StoreWriter> {
        let block_compressor = BlockCompressor::new(compressor, writer, dedicated_thread)?;
        Ok(StoreWriter {
            compressor,
            block_size,
            num_docs_in_current_block: 0,
            doc_pos: Vec::new(),
            current_block: Vec::new(),
            block_compressor,
        })
    }

    pub(crate) fn compressor(&self) -> Compressor {
        self.compressor
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.current_block.capacity() + self.doc_pos.capacity() * std::mem::size_of::<u32>()
    }

    /// Checks if the current block is full, and if so, compresses and flushes it.
    fn check_flush_block(&mut self) -> io::Result<()> {
        // this does not count the VInt storing the index length itself, but it is negligible in
        // front of everything else.
        let index_len = self.doc_pos.len() * std::mem::size_of::<usize>();
        if self.current_block.len() + index_len > self.block_size {
            self.send_current_block_to_compressor()?;
        }
        Ok(())
    }

    /// Flushes current uncompressed block and sends to compressor.
    fn send_current_block_to_compressor(&mut self) -> io::Result<()> {
        // We don't do anything if the current block is empty to begin with.
        if self.current_block.is_empty() {
            return Ok(());
        }

        let size_of_u32 = std::mem::size_of::<u32>();
        self.current_block
            .reserve((self.doc_pos.len() + 1) * size_of_u32);

        for pos in self.doc_pos.iter() {
            pos.serialize(&mut self.current_block)?;
        }
        (self.doc_pos.len() as u32).serialize(&mut self.current_block)?;

        self.block_compressor
            .compress_block_and_write(&self.current_block, self.num_docs_in_current_block)?;
        self.doc_pos.clear();
        self.current_block.clear();
        self.num_docs_in_current_block = 0;
        Ok(())
    }

    /// Store a new document.
    ///
    /// The document id is implicitly the current number
    /// of documents.
    pub fn store<D: Document>(&mut self, document: &D, schema: &Schema) -> io::Result<()> {
        self.doc_pos.push(self.current_block.len() as u32);

        let mut serializer = BinaryDocumentSerializer::new(&mut self.current_block, schema);
        serializer.serialize_doc(document)?;

        self.num_docs_in_current_block += 1;
        self.check_flush_block()?;
        Ok(())
    }

    /// Store bytes of a serialized document.
    ///
    /// The document id is implicitly the current number
    /// of documents.
    pub fn store_bytes(&mut self, serialized_document: &[u8]) -> io::Result<()> {
        self.doc_pos.push(self.current_block.len() as u32);
        self.current_block.extend_from_slice(serialized_document);
        self.num_docs_in_current_block += 1;
        self.check_flush_block()?;
        Ok(())
    }

    /// Stacks a store reader on top of the documents written so far.
    /// This method is an optimization compared to iterating over the documents
    /// in the store and adding them one by one, as the store's data will
    /// not be decompressed and then recompressed.
    pub fn stack(&mut self, store_reader: StoreReader) -> io::Result<()> {
        // We flush the current block first before stacking
        self.send_current_block_to_compressor()?;
        self.block_compressor.stack_reader(store_reader)?;
        Ok(())
    }

    /// Finalized the store writer.
    ///
    /// Compress the last unfinished block if any,
    /// and serializes the skip list index on disc.
    pub fn close(mut self) -> io::Result<()> {
        self.send_current_block_to_compressor()?;
        self.block_compressor.close()?;
        Ok(())
    }
}

/// Runtime-free block output. Retains one uncompressed block and its compressed
/// form; an individual document can exceed the target block size. Checkpoint
/// layers are spooled rather than accumulated as a complete in-memory index.
pub struct AsyncStoreWriter {
    writer: crate::directory::AsyncWritePtr,
    directory: Box<dyn crate::directory::Directory>,
    compressor: Compressor,
    block_size: usize,
    current_block: Vec<u8>,
    doc_pos: Vec<u32>,
    compressed: Vec<u8>,
    index: super::index::AsyncSkipIndexBuilder,
    offset: u64,
    first_doc: DocId,
    usable: bool,
}

impl AsyncStoreWriter {
    /// The directory owns transaction-private scratch cleanup after cancellation.
    pub fn new(
        writer: crate::directory::AsyncWritePtr,
        directory: Box<dyn crate::directory::Directory>,
        compressor: Compressor,
        block_size: usize,
    ) -> Self {
        Self {
            writer,
            directory,
            compressor,
            block_size,
            current_block: Vec::new(),
            doc_pos: Vec::new(),
            compressed: Vec::new(),
            index: super::index::AsyncSkipIndexBuilder::new(),
            offset: 0,
            first_doc: 0,
            usable: true,
        }
    }

    /// Encodes resident values, then awaits any full-block output before accepting
    /// another document. Failure or cancellation invalidates this writer.
    pub async fn store<D: Document>(&mut self, document: &D, schema: &Schema) -> io::Result<()> {
        self.begin()?;
        self.record_position()?;
        BinaryDocumentSerializer::new(&mut self.current_block, schema).serialize_doc(document)?;
        self.flush_if_full().await?;
        self.usable = true;
        Ok(())
    }

    /// Accepts one already serialized document, for merging store readers.
    pub async fn store_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.begin()?;
        self.record_position()?;
        self.current_block.extend_from_slice(bytes);
        self.flush_if_full().await?;
        self.usable = true;
        Ok(())
    }

    /// Completes all block and checkpoint output before finalizing the component.
    pub async fn close(mut self) -> io::Result<()> {
        self.begin()?;
        self.flush_block().await?;
        let footer = super::footer::DocStoreFooter::new(
            self.offset,
            super::Decompressor::from(self.compressor),
            super::DOC_STORE_VERSION,
        );
        self.index
            .serialize_into(self.directory.as_ref(), self.writer.as_mut())
            .await?;
        let mut bytes = Vec::new();
        footer.serialize(&mut bytes)?;
        self.writer.write_all(&bytes).await?;
        self.writer.finish().await
    }

    fn begin(&mut self) -> io::Result<()> {
        if !std::mem::replace(&mut self.usable, false) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Document store output was cancelled or failed",
            ));
        }
        Ok(())
    }

    fn record_position(&mut self) -> io::Result<()> {
        self.doc_pos
            .push(u32::try_from(self.current_block.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Document block exceeds u32 offsets",
                )
            })?);
        Ok(())
    }

    async fn flush_if_full(&mut self) -> io::Result<()> {
        if self.current_block.len() + self.doc_pos.len() * std::mem::size_of::<usize>()
            > self.block_size
        {
            self.flush_block().await?;
        }
        Ok(())
    }

    async fn flush_block(&mut self) -> io::Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }
        let count = u32::try_from(self.doc_pos.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "Too many documents in block")
        })?;
        let next_doc = self.first_doc.checked_add(count).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Too many stored documents")
        })?;
        for position in &self.doc_pos {
            position.serialize(&mut self.current_block)?;
        }
        count.serialize(&mut self.current_block)?;
        self.compressed.clear();
        self.compressor
            .compress_into(&self.current_block, &mut self.compressed)?;
        self.writer.write_all(&self.compressed).await?;
        let end = self.offset + self.compressed.len() as u64;
        self.index
            .insert(
                super::index::Checkpoint {
                    doc_range: self.first_doc..next_doc,
                    byte_range: self.offset as usize..end as usize,
                },
                self.directory.as_ref(),
            )
            .await?;
        self.offset = end;
        self.first_doc = next_doc;
        self.current_block.clear();
        self.doc_pos.clear();
        Ok(())
    }
}

#[cfg(test)]
mod async_tests {
    use super::*;
    use crate::directory::tests::AsyncOutputDirectory;
    use crate::directory::{Directory, RamDirectory};
    use crate::schema::STORED;
    use crate::TantivyDocument;
    use std::path::Path;

    #[test]
    fn async_store_cancellation_and_errors_poison_output() {
        use crate::directory::WriteQueue;
        use std::future::Future;
        use std::task::{Context, Poll};
        for cancel in [false, true] {
            let queue = WriteQueue::new(31.try_into().unwrap());
            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
            let mut opening = Box::pin(queue.open("store".to_owned()));
            assert!(opening.as_mut().poll(&mut cx).is_pending());
            queue.pop().unwrap().complete(Ok(0));
            let Poll::Ready(Ok(write)) = opening.as_mut().poll(&mut cx) else {
                panic!("open incomplete")
            };
            let mut writer = AsyncStoreWriter::new(
                write,
                Box::new(AsyncOutputDirectory::default()),
                Compressor::None,
                0,
            );
            let mut operation = Box::pin(writer.store_bytes(&[0]));
            assert!(operation.as_mut().poll(&mut cx).is_pending());
            let request = queue.pop().unwrap();
            for _ in 0..3 {
                assert!(operation.as_mut().poll(&mut cx).is_pending());
                assert!(queue.pop().is_none());
            }
            if cancel {
                drop(operation);
                assert!(request.is_cancelled());
                request.complete(Ok(1));
            } else {
                request.complete(Err(io::Error::other("injected store failure")));
                let Poll::Ready(Err(error)) = operation.as_mut().poll(&mut cx) else {
                    panic!("error lost")
                };
                assert_eq!(error.kind(), io::ErrorKind::Other);
                drop(operation);
            }
            let mut next = Box::pin(writer.store_bytes(&[0]));
            let Poll::Ready(Err(error)) = next.as_mut().poll(&mut cx) else {
                panic!("reused poisoned writer")
            };
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            drop(next);
            let mut close = Box::pin(writer.close());
            let Poll::Ready(Err(error)) = close.as_mut().poll(&mut cx) else {
                panic!("finalized poisoned writer")
            };
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert!(queue.pop().is_none());
        }
    }

    #[test]
    fn async_store_matches_sync_with_delayed_short_writes() -> crate::Result<()> {
        let mut builder = Schema::builder();
        let text = builder.add_text_field("text", STORED);
        let number = builder.add_i64_field("number", STORED);
        let schema = builder.build();
        let compressors = [
            Compressor::None,
            #[cfg(feature = "lz4-compression")]
            Compressor::Lz4,
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd(Default::default()),
        ];
        for compressor in compressors {
            for block_size in [0, 64, 16_384] {
                for count in [0, 1, 9, 65, 513] {
                    let docs: Vec<TantivyDocument> = (0..count)
                        .map(|id| doc!(text => "value".repeat(id % 17 + 1), number => id as i64))
                        .collect();
                    let path = Path::new("store");
                    let expected = RamDirectory::default();
                    let mut sync = StoreWriter::new(
                        expected.open_write(path)?,
                        compressor,
                        block_size,
                        false,
                    )?;
                    for doc in &docs {
                        sync.store(doc, &schema)?;
                    }
                    sync.close()?;
                    let directory = AsyncOutputDirectory::default();
                    directory.run(async {
                        let write = directory.open_write_async(path).await?;
                        let mut output = AsyncStoreWriter::new(
                            write,
                            Box::new(directory.clone()),
                            compressor,
                            block_size,
                        );
                        for (id, doc) in docs.iter().enumerate() {
                            if id % 2 == 0 {
                                output.store(doc, &schema).await?;
                            } else {
                                let mut bytes = Vec::new();
                                BinaryDocumentSerializer::new(&mut bytes, &schema)
                                    .serialize_doc(doc)?;
                                output.store_bytes(&bytes).await?;
                            }
                        }
                        output.close().await?;
                        crate::Result::Ok(())
                    })?;
                    assert_eq!(
                        directory.ram.atomic_read(path)?,
                        expected.atomic_read(path)?,
                        "count={count} block_size={block_size} compressor={compressor:?}"
                    );
                    directory.run(async {
                        let reader =
                            StoreReader::open_async(directory.open_read_async(path).await?, 0)
                                .await?;
                        for (id, expected) in docs.iter().enumerate() {
                            assert_eq!(
                                &reader.get_async::<TantivyDocument>(id as u32).await?,
                                expected
                            );
                        }
                        crate::Result::Ok(())
                    })?;
                }
            }
        }
        Ok(())
    }
}
