use crate::postings::json_postings_writer::JsonPostingsWriter;
use crate::postings::postings_writer::SpecializedPostingsWriter;
use crate::postings::recorder::{DocIdRecorder, TermFrequencyRecorder, TfAndPositionRecorder};
use crate::postings::PostingsWriter;
use crate::schema::{Field, FieldEntry, FieldType, IndexRecordOption, Schema};

pub(crate) struct PerFieldPostingsWriter {
    per_field_postings_writers: Vec<Box<dyn PostingsWriter>>,
}

impl PerFieldPostingsWriter {
    pub fn for_schema(schema: &Schema) -> Self {
        let per_field_postings_writers = schema
            .fields()
            .map(|(_, field_entry)| posting_writer_from_field_entry(field_entry))
            .collect();
        PerFieldPostingsWriter {
            per_field_postings_writers,
        }
    }

    pub(crate) fn get_for_field(&self, field: Field) -> &dyn PostingsWriter {
        self.per_field_postings_writers[field.field_id() as usize].as_ref()
    }

    pub(crate) fn get_for_field_mut(&mut self, field: Field) -> &mut dyn PostingsWriter {
        self.per_field_postings_writers[field.field_id() as usize].as_mut()
    }
}

fn posting_writer_from_field_entry(field_entry: &FieldEntry) -> Box<dyn PostingsWriter> {
    match *field_entry.field_type() {
        FieldType::Str(ref text_options) => text_options
            .get_indexing_options()
            .map(|indexing_options| match indexing_options.index_option() {
                IndexRecordOption::Basic => {
                    SpecializedPostingsWriter::<DocIdRecorder>::default().into()
                }
                IndexRecordOption::WithFreqs => {
                    SpecializedPostingsWriter::<TermFrequencyRecorder>::default().into()
                }
                IndexRecordOption::WithFreqsAndPositions => {
                    SpecializedPostingsWriter::<TfAndPositionRecorder>::default().into()
                }
            })
            .unwrap_or_else(|| SpecializedPostingsWriter::<DocIdRecorder>::default().into()),
        FieldType::U64(_)
        | FieldType::I64(_)
        | FieldType::F64(_)
        | FieldType::Bool(_)
        | FieldType::Date(_)
        | FieldType::Bytes(_)
        | FieldType::IpAddr(_)
        | FieldType::Facet(_) => Box::<SpecializedPostingsWriter<DocIdRecorder>>::default(),
        FieldType::JsonObject(ref json_object_options) => {
            if let Some(text_indexing_option) = json_object_options.get_text_indexing_options() {
                match text_indexing_option.index_option() {
                    IndexRecordOption::Basic => {
                        JsonPostingsWriter::<DocIdRecorder>::default().into()
                    }
                    IndexRecordOption::WithFreqs => {
                        JsonPostingsWriter::<TermFrequencyRecorder>::default().into()
                    }
                    IndexRecordOption::WithFreqsAndPositions => {
                        JsonPostingsWriter::<TfAndPositionRecorder>::default().into()
                    }
                }
            } else {
                JsonPostingsWriter::<DocIdRecorder>::default().into()
            }
        }
    }
}
