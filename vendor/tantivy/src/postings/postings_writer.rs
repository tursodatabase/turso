use std::io;
use std::marker::PhantomData;
use std::ops::Range;

use stacker::Addr;

use crate::fieldnorm::FieldNormReaders;
use crate::indexer::indexing_term::IndexingTerm;
use crate::indexer::path_to_unordered_id::OrderedPathId;
use crate::postings::recorder::{BufferLender, Recorder};
use crate::postings::{
    FieldSerializer, IndexingContext, InvertedIndexSerializer, PerFieldPostingsWriter,
};
use crate::schema::{Field, Schema, Type};
use crate::tokenizer::{Token, TokenStream, MAX_TOKEN_LEN};
use crate::DocId;

const POSITION_GAP: u32 = 1;

fn make_field_partition(
    term_offsets: &[(Field, OrderedPathId, &[u8], Addr)],
) -> Vec<(Field, Range<usize>)> {
    let term_offsets_it = term_offsets
        .iter()
        .map(|(field, _, _, _)| *field)
        .enumerate();
    let mut prev_field_opt = None;
    let mut fields = vec![];
    let mut offsets = vec![];
    for (offset, field) in term_offsets_it {
        if Some(field) != prev_field_opt {
            prev_field_opt = Some(field);
            fields.push(field);
            offsets.push(offset);
        }
    }
    offsets.push(term_offsets.len());
    let mut field_offsets = vec![];
    for i in 0..fields.len() {
        field_offsets.push((fields[i], offsets[i]..offsets[i + 1]));
    }
    field_offsets
}

/// Serialize the inverted index.
/// It pushes all term, one field at a time, towards the
/// postings serializer.
pub(crate) fn serialize_postings(
    ctx: IndexingContext,
    schema: Schema,
    per_field_postings_writers: &PerFieldPostingsWriter,
    fieldnorm_readers: FieldNormReaders,
    serializer: &mut InvertedIndexSerializer,
) -> crate::Result<()> {
    let term_offsets = sorted_term_offsets(&ctx, &schema);
    let ordered_id_to_path = ctx.path_to_unordered_id.ordered_id_to_path();
    let field_offsets = make_field_partition(&term_offsets);
    for (field, byte_offsets) in field_offsets {
        let postings_writer = per_field_postings_writers.get_for_field(field);
        let fieldnorm_reader = fieldnorm_readers.get_field(field)?;
        let mut field_serializer =
            serializer.new_field(field, postings_writer.total_num_tokens(), fieldnorm_reader)?;
        postings_writer.serialize(
            &term_offsets[byte_offsets],
            &ordered_id_to_path,
            &ctx,
            &mut field_serializer,
        )?;
        field_serializer.close()?;
    }
    Ok(())
}

#[cfg(not(feature = "quickwit"))]
pub(crate) async fn serialize_postings_async(
    ctx: IndexingContext,
    schema: Schema,
    per_field_postings_writers: &PerFieldPostingsWriter,
    fieldnorm_readers: FieldNormReaders,
    serializer: &mut crate::postings::AsyncInvertedIndexSerializer,
) -> crate::Result<()> {
    let term_offsets = sorted_term_offsets(&ctx, &schema);
    let ordered_id_to_path = ctx.path_to_unordered_id.ordered_id_to_path();
    for (field, byte_offsets) in make_field_partition(&term_offsets) {
        let postings_writer = per_field_postings_writers.get_for_field(field);
        let fieldnorm_reader = fieldnorm_readers.get_field_async(field).await?;
        let mut field_serializer = serializer
            .new_field(field, postings_writer.total_num_tokens(), fieldnorm_reader)
            .await?;
        postings_writer
            .serialize_async(
                &term_offsets[byte_offsets],
                &ordered_id_to_path,
                &ctx,
                &mut field_serializer,
            )
            .await?;
        field_serializer.close().await?;
    }
    Ok(())
}

fn sorted_term_offsets<'a>(
    ctx: &'a IndexingContext,
    schema: &Schema,
) -> Vec<(Field, OrderedPathId, &'a [u8], Addr)> {
    // Replace unordered ids by ordered ids to be able to sort
    let unordered_id_to_ordered_id: Vec<OrderedPathId> =
        ctx.path_to_unordered_id.unordered_id_to_ordered_id();

    let mut term_offsets: Vec<(Field, OrderedPathId, &[u8], Addr)> =
        Vec::with_capacity(ctx.term_index.len());
    term_offsets.extend(ctx.term_index.iter().map(|(key, addr)| {
        let field = IndexingTerm::wrap(key).field();
        if schema.get_field_entry(field).field_type().value_type() == Type::Json {
            let byte_range_path = 4..4 + 4;
            let unordered_id = u32::from_be_bytes(key[byte_range_path.clone()].try_into().unwrap());
            let path_id = unordered_id_to_ordered_id[unordered_id as usize];
            (field, path_id, &key[byte_range_path.end..], addr)
        } else {
            (field, 0.into(), &key[4..], addr)
        }
    }));
    // Sort by field, path, and term
    term_offsets.sort_unstable_by(
        |(field1, path_id1, bytes1, _), (field2, path_id2, bytes2, _)| {
            (field1, path_id1, bytes1).cmp(&(field2, path_id2, bytes2))
        },
    );
    term_offsets
}

#[derive(Default, Debug)]
pub(crate) struct IndexingPosition {
    pub num_tokens: u32,
    pub end_position: u32,
}

/// The `PostingsWriter` is in charge of receiving documenting
/// and building a `Segment` in anonymous memory.
///
/// `PostingsWriter` writes in a `MemoryArena`.
pub(crate) trait PostingsWriter: Send + Sync {
    /// Record that a document contains a term at a given position.
    ///
    /// * doc  - the document id
    /// * pos  - the term position (expressed in tokens)
    /// * term - the term
    /// * ctx - Contains a term hashmap and a memory arena to store all necessary posting list
    ///   information.
    fn subscribe(&mut self, doc: DocId, pos: u32, term: &IndexingTerm, ctx: &mut IndexingContext);

    /// Serializes the postings on disk.
    /// The actual serialization format is handled by the `PostingsSerializer`.
    fn serialize(
        &self,
        term_addrs: &[(Field, OrderedPathId, &[u8], Addr)],
        ordered_id_to_path: &[&str],
        ctx: &IndexingContext,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()>;

    #[cfg(not(feature = "quickwit"))]
    fn serialize_async<'a, 'directory: 'a>(
        &'a self,
        term_addrs: &'a [(Field, OrderedPathId, &[u8], Addr)],
        ordered_id_to_path: &'a [&str],
        ctx: &'a IndexingContext,
        serializer: &'a mut crate::postings::AsyncFieldSerializer<'directory>,
    ) -> crate::directory::DirectoryFuture<'a, io::Result<()>>;

    /// Tokenize a text and subscribe all of its token.
    fn index_text(
        &mut self,
        doc_id: DocId,
        token_stream: &mut dyn TokenStream,
        term_buffer: &mut IndexingTerm,
        ctx: &mut IndexingContext,
        indexing_position: &mut IndexingPosition,
    ) {
        let end_of_path_idx = term_buffer.len_bytes();
        let mut num_tokens = 0;
        let mut end_position = indexing_position.end_position;
        token_stream.process(&mut |token: &Token| {
            // We skip all tokens with a len greater than u16.
            if token.text.len() > MAX_TOKEN_LEN {
                warn!(
                    "A token exceeding MAX_TOKEN_LEN ({}>{}) was dropped. Search for \
                     MAX_TOKEN_LEN in the documentation for more information.",
                    token.text.len(),
                    MAX_TOKEN_LEN
                );
                return;
            }
            term_buffer.truncate_value_bytes(end_of_path_idx);
            term_buffer.append_bytes(token.text.as_bytes());
            let start_position = indexing_position.end_position + token.position as u32;
            end_position = end_position.max(start_position + token.position_length as u32);
            self.subscribe(doc_id, start_position, term_buffer, ctx);
            num_tokens += 1;
        });

        indexing_position.end_position = end_position + POSITION_GAP;
        indexing_position.num_tokens += num_tokens;
        term_buffer.truncate_value_bytes(end_of_path_idx);
    }

    fn total_num_tokens(&self) -> u64;
}

/// The `SpecializedPostingsWriter` is just here to remove dynamic
/// dispatch to the recorder information.
#[derive(Default)]
pub(crate) struct SpecializedPostingsWriter<Rec: Recorder> {
    total_num_tokens: u64,
    _recorder_type: PhantomData<Rec>,
}

impl<Rec: Recorder> From<SpecializedPostingsWriter<Rec>> for Box<dyn PostingsWriter> {
    fn from(
        specialized_postings_writer: SpecializedPostingsWriter<Rec>,
    ) -> Box<dyn PostingsWriter> {
        Box::new(specialized_postings_writer)
    }
}

impl<Rec: Recorder> SpecializedPostingsWriter<Rec> {
    #[inline]
    pub(crate) fn serialize_one_term(
        term: &[u8],
        addr: Addr,
        buffer_lender: &mut BufferLender,
        ctx: &IndexingContext,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()> {
        let recorder: Rec = ctx.term_index.read(addr);
        let term_doc_freq = recorder.term_doc_freq().unwrap_or(0u32);
        serializer.new_term(term, term_doc_freq, recorder.has_term_freq())?;
        let mut docs = recorder.recorded_docs(&ctx.arena, buffer_lender);
        while let Some((doc, freq, positions)) = docs.next_doc() {
            serializer.write_doc(doc, freq, positions);
        }
        serializer.close_term()?;
        Ok(())
    }

    #[cfg(not(feature = "quickwit"))]
    pub(crate) async fn serialize_one_term_async(
        term: &[u8],
        addr: Addr,
        buffer_lender: &mut BufferLender,
        ctx: &IndexingContext,
        serializer: &mut crate::postings::AsyncFieldSerializer<'_>,
    ) -> io::Result<()> {
        let recorder: Rec = ctx.term_index.read(addr);
        serializer
            .new_term(
                term,
                recorder.term_doc_freq().unwrap_or(0),
                recorder.has_term_freq(),
            )
            .await?;
        let mut docs = recorder.recorded_docs(&ctx.arena, buffer_lender);
        while let Some((doc, freq, positions)) = docs.next_doc() {
            serializer.write_doc(doc, freq, positions).await?;
        }
        serializer.close_term().await
    }
}

impl<Rec: Recorder> PostingsWriter for SpecializedPostingsWriter<Rec> {
    #[inline]
    fn subscribe(
        &mut self,
        doc: DocId,
        position: u32,
        term: &IndexingTerm,
        ctx: &mut IndexingContext,
    ) {
        debug_assert!(term.serialized_term().len() >= 4);
        self.total_num_tokens += 1;
        let (term_index, arena) = (&mut ctx.term_index, &mut ctx.arena);
        term_index.mutate_or_create(term.serialized_term(), |opt_recorder: Option<Rec>| {
            if let Some(mut recorder) = opt_recorder {
                let current_doc = recorder.current_doc();
                if current_doc != doc {
                    recorder.close_doc(arena);
                    recorder.new_doc(doc, arena);
                }
                recorder.record_position(position, arena);
                recorder
            } else {
                let mut recorder = Rec::default();
                recorder.new_doc(doc, arena);
                recorder.record_position(position, arena);
                recorder
            }
        });
    }

    fn serialize(
        &self,
        term_addrs: &[(Field, OrderedPathId, &[u8], Addr)],
        _ordered_id_to_path: &[&str],
        ctx: &IndexingContext,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()> {
        let mut buffer_lender = BufferLender::default();
        for (_field, _path_id, term, addr) in term_addrs {
            Self::serialize_one_term(term, *addr, &mut buffer_lender, ctx, serializer)?;
        }
        Ok(())
    }

    #[cfg(not(feature = "quickwit"))]
    fn serialize_async<'a, 'directory: 'a>(
        &'a self,
        term_addrs: &'a [(Field, OrderedPathId, &[u8], Addr)],
        _ordered_id_to_path: &'a [&str],
        ctx: &'a IndexingContext,
        serializer: &'a mut crate::postings::AsyncFieldSerializer<'directory>,
    ) -> crate::directory::DirectoryFuture<'a, io::Result<()>> {
        Box::pin(async move {
            let mut buffers = BufferLender::default();
            for (_, _, term, addr) in term_addrs {
                Self::serialize_one_term_async(term, *addr, &mut buffers, ctx, serializer).await?;
            }
            Ok(())
        })
    }

    fn total_num_tokens(&self) -> u64 {
        self.total_num_tokens
    }
}

#[cfg(all(test, not(feature = "quickwit")))]
mod async_tests {
    use super::*;
    use crate::directory::tests::AsyncOutputDirectory;
    use crate::index::SegmentComponent;
    use crate::indexer::operation::AddOperation;
    use crate::indexer::SegmentWriter;
    use crate::postings::AsyncInvertedIndexSerializer;
    use crate::schema::{IndexRecordOption, TextFieldIndexing, TextOptions, INDEXED, TEXT};
    use crate::{Index, TantivyDocument};

    #[test]
    fn async_recorded_postings_match_sync_with_delayed_short_writes() -> crate::Result<()> {
        let mut builder = Schema::builder();
        for (name, option) in [
            ("basic", IndexRecordOption::Basic),
            ("freq", IndexRecordOption::WithFreqs),
            ("positions", IndexRecordOption::WithFreqsAndPositions),
        ] {
            builder.add_text_field(
                name,
                TextOptions::default()
                    .set_indexing_options(TextFieldIndexing::default().set_index_option(option)),
            );
        }
        builder.add_i64_field("number", INDEXED);
        builder.add_json_field("json", TEXT);
        let schema = builder.build();
        let expected_index = Index::create_in_ram(schema.clone());
        let mut expected_segment = expected_index.new_segment();
        let (ctx, writers, norms) = recorded(&schema)?;
        let mut expected = InvertedIndexSerializer::open(&mut expected_segment)?;
        serialize_postings(ctx, schema.clone(), &writers, norms, &mut expected)?;
        expected.close()?;

        let directory = AsyncOutputDirectory::default();
        let index = directory.run(Index::create_async(
            directory.clone(),
            schema.clone(),
            Default::default(),
        ))?;
        let segment = index.new_segment();
        let (ctx, writers, norms) = recorded(&schema)?;
        directory.run(async {
            let mut output = AsyncInvertedIndexSerializer::open(&segment).await?;
            serialize_postings_async(ctx, schema, &writers, norms, &mut output).await?;
            output.close().await?;
            crate::Result::Ok(())
        })?;
        for component in [
            SegmentComponent::Terms,
            SegmentComponent::Postings,
            SegmentComponent::Positions,
        ] {
            let expected = expected_segment.open_read(component)?.read_bytes()?;
            let actual = directory.run(async {
                segment
                    .open_read_async(component)
                    .await?
                    .read_bytes_async()
                    .await
                    .map_err(crate::TantivyError::from)
            })?;
            assert_eq!(actual.as_slice(), expected.as_slice());
        }
        Ok(())
    }

    fn recorded(
        schema: &Schema,
    ) -> crate::Result<(IndexingContext, PerFieldPostingsWriter, FieldNormReaders)> {
        let index = Index::create_in_ram(schema.clone());
        let segment = index.new_segment();
        let mut writer = SegmentWriter::for_segment(8_000_000, segment.clone())?;
        for id in 0..513 {
            let text = format!("common common term{id}");
            let document = TantivyDocument::parse_json(
                schema,
                &serde_json::json!({
                    "basic": text, "freq": text, "positions": text, "number": id - 256,
                    "json": {"text": text, "number": id, "nested": {"text": "nested common"}}
                })
                .to_string(),
            )
            .unwrap();
            writer.add_document(AddOperation {
                document,
                opstamp: id as u64,
            })?;
        }
        writer.fieldnorms_writer.fill_up_to_max_doc(writer.max_doc);
        writer.fieldnorms_writer.serialize(
            writer
                .segment_serializer
                .extract_fieldnorms_serializer()
                .unwrap(),
        )?;
        let norms = FieldNormReaders::open(segment.open_read(SegmentComponent::FieldNorms)?)?;
        writer.segment_serializer.close()?;
        Ok((writer.ctx, writer.per_field_postings_writers, norms))
    }
}
