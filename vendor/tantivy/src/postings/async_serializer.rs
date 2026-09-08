use std::io;

use crate::directory::{AsyncCompositeWrite, ManagedDirectory};
use crate::fieldnorm::FieldNormReader;
use crate::index::{Segment, SegmentComponent};
use crate::positions::AsyncPositionSerializer;
use crate::schema::{Field, IndexRecordOption, Schema};
use crate::termdict::AsyncTermDictionaryBuilder;
use crate::{DocId, Score};

use super::{AsyncPostingsSerializer, TermInfo};

/// Native component output for the FST inverted-index format.
pub struct AsyncInvertedIndexSerializer {
    terms: AsyncCompositeWrite,
    postings: AsyncCompositeWrite,
    positions: AsyncCompositeWrite,
    directory: ManagedDirectory,
    schema: Schema,
    usable: bool,
}

impl AsyncInvertedIndexSerializer {
    /// Opens all inverted-index components through injected output.
    pub async fn open(segment: &Segment) -> crate::Result<Self> {
        Ok(Self {
            terms: AsyncCompositeWrite::wrap(
                segment.open_write_async(SegmentComponent::Terms).await?,
            ),
            postings: AsyncCompositeWrite::wrap(
                segment.open_write_async(SegmentComponent::Postings).await?,
            ),
            positions: AsyncCompositeWrite::wrap(
                segment
                    .open_write_async(SegmentComponent::Positions)
                    .await?,
            ),
            directory: segment.index().directory().clone(),
            schema: segment.schema(),
            usable: true,
        })
    }

    /// Starts a field. Abandoning or failing its serializer poisons the parent.
    pub async fn new_field(
        &mut self,
        field: Field,
        total_num_tokens: u64,
        fieldnorms: Option<FieldNormReader>,
    ) -> io::Result<AsyncFieldSerializer<'_>> {
        begin(&mut self.usable)?;
        self.terms.for_field(field);
        self.postings.for_field(field);
        self.positions.for_field(field);
        self.postings
            .write_all(&total_num_tokens.to_le_bytes())
            .await?;
        let postings_start = self.postings.written_bytes();
        let positions_start = self.positions.written_bytes();
        let mode = self
            .schema
            .get_field_entry(field)
            .field_type()
            .index_record_option()
            .unwrap_or(IndexRecordOption::Basic);
        let average_fieldnorm = fieldnorms
            .as_ref()
            .map(|norms| total_num_tokens as Score / norms.num_docs() as Score)
            .unwrap_or(0.0);
        Ok(AsyncFieldSerializer {
            terms: AsyncTermDictionaryBuilder::new(&self.directory, &mut self.terms).await?,
            directory: &self.directory,
            postings: &mut self.postings,
            positions: &mut self.positions,
            postings_start,
            positions_start,
            mode,
            average_fieldnorm,
            fieldnorms,
            term: None,
            usable: true,
            parent_usable: &mut self.usable,
        })
    }

    /// Awaits component footers and finalization. Publication belongs to the caller.
    pub async fn close(mut self) -> io::Result<()> {
        begin(&mut self.usable)?;
        self.terms.close().await?;
        self.postings.close().await?;
        self.positions.close().await
    }
}

/// A field's sorted terms and document lists. No storage callback is synchronous.
pub struct AsyncFieldSerializer<'a> {
    directory: &'a ManagedDirectory,
    terms: AsyncTermDictionaryBuilder<'a>,
    postings: &'a mut AsyncCompositeWrite,
    positions: &'a mut AsyncCompositeWrite,
    postings_start: u64,
    positions_start: u64,
    mode: IndexRecordOption,
    average_fieldnorm: Score,
    fieldnorms: Option<FieldNormReader>,
    term: Option<AsyncTerm<'a>>,
    usable: bool,
    parent_usable: &'a mut bool,
}

impl AsyncFieldSerializer<'_> {
    /// Opens scratch for one term. The preceding term must be closed first.
    pub async fn new_term(
        &mut self,
        key: &[u8],
        doc_freq: u32,
        record_freq: bool,
    ) -> io::Result<()> {
        begin(&mut self.usable)?;
        assert!(self.term.is_none(), "previous term was not closed");
        let postings = AsyncPostingsSerializer::new(
            self.directory,
            self.average_fieldnorm,
            self.mode,
            self.fieldnorms.clone(),
            doc_freq,
            record_freq,
        )
        .await?;
        let positions = if self.mode.has_positions() {
            Some(AsyncPositionSerializer::new(self.directory).await?)
        } else {
            None
        };
        let postings_start = (self.postings.written_bytes() - self.postings_start) as usize;
        let positions_start = (self.positions.written_bytes() - self.positions_start) as usize;
        self.term = Some(AsyncTerm {
            key: key.to_vec(),
            postings,
            positions,
            info: TermInfo {
                doc_freq: 0,
                postings_range: postings_start..postings_start,
                positions_range: positions_start..positions_start,
            },
        });
        self.usable = true;
        Ok(())
    }

    /// Adds a document, awaiting each completed encoded block's scratch writes.
    pub async fn write_doc(&mut self, doc: DocId, freq: u32, deltas: &[u32]) -> io::Result<()> {
        begin(&mut self.usable)?;
        let term = self.term.as_mut().expect("new_term must precede write_doc");
        term.postings.write_doc(doc, freq).await?;
        if let Some(positions) = &mut term.positions {
            assert_eq!(freq as usize, deltas.len());
            positions.write_positions_delta(deltas).await?;
        }
        term.info.doc_freq += 1;
        self.usable = true;
        Ok(())
    }

    /// Appends one term's scratch streams and commits its dictionary entry.
    pub async fn close_term(&mut self) -> io::Result<()> {
        begin(&mut self.usable)?;
        if let Some(mut term) = self.term.take() {
            term.postings.close_term(self.postings).await?;
            term.info.postings_range.end =
                (self.postings.written_bytes() - self.postings_start) as usize;
            if let Some(positions) = term.positions {
                positions.close_term(self.positions).await?;
                term.info.positions_range.end =
                    (self.positions.written_bytes() - self.positions_start) as usize;
            }
            self.terms.insert(&term.key, &term.info).await?;
        }
        self.usable = true;
        Ok(())
    }

    /// Finalizes the field dictionary and permits the parent to open another field.
    pub async fn close(mut self) -> io::Result<()> {
        self.close_term().await?;
        self.terms.finish().await?;
        *self.parent_usable = true;
        Ok(())
    }
}

struct AsyncTerm<'a> {
    key: Vec<u8>,
    info: TermInfo,
    postings: AsyncPostingsSerializer<'a>,
    positions: Option<AsyncPositionSerializer<'a>>,
}

fn begin(usable: &mut bool) -> io::Result<()> {
    if !std::mem::replace(usable, false) {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "Inverted-index output incomplete or cancelled",
        ));
    }
    Ok(())
}
