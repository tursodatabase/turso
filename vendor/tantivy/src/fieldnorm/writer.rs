use std::cmp::Ordering;
use std::{io, iter};

use super::{fieldnorm_to_id, FieldNormsSerializer};
use crate::schema::{Field, Schema};
use crate::DocId;

/// The `FieldNormsWriter` is in charge of tracking the fieldnorm byte
/// of each document for each field with field norms.
///
/// `FieldNormsWriter` stores a `Vec<u8>` for each tracked field, using a
/// byte per document per field.
pub struct FieldNormsWriter {
    fieldnorms_buffers: Vec<Option<Vec<u8>>>,
}

impl FieldNormsWriter {
    /// Returns the fields that should have field norms computed
    /// according to the given schema.
    pub(crate) fn fields_with_fieldnorm(schema: &Schema) -> Vec<Field> {
        schema
            .fields()
            .filter_map(|(field, field_entry)| {
                if field_entry.is_indexed() && field_entry.has_fieldnorms() {
                    Some(field)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    /// Initialize with state for tracking the field norm fields
    /// specified in the schema.
    pub fn for_schema(schema: &Schema) -> FieldNormsWriter {
        let mut fieldnorms_buffers: Vec<Option<Vec<u8>>> = iter::repeat_with(|| None)
            .take(schema.num_fields())
            .collect();
        for field in FieldNormsWriter::fields_with_fieldnorm(schema) {
            fieldnorms_buffers[field.field_id() as usize] = Some(Vec::with_capacity(1_000));
        }
        FieldNormsWriter { fieldnorms_buffers }
    }

    /// The memory used inclusive childs
    pub fn mem_usage(&self) -> usize {
        self.fieldnorms_buffers
            .iter()
            .flatten()
            .map(|buf| buf.capacity())
            .sum()
    }
    /// Ensure that all documents in 0..max_doc have a byte associated with them
    /// in each of the fieldnorm vectors.
    ///
    /// Will extend with 0-bytes for documents that have not been seen.
    pub fn fill_up_to_max_doc(&mut self, max_doc: DocId) {
        for fieldnorms_buffer_opt in self.fieldnorms_buffers.iter_mut() {
            if let Some(fieldnorms_buffer) = fieldnorms_buffer_opt.as_mut() {
                fieldnorms_buffer.resize(max_doc as usize, 0u8);
            }
        }
    }

    /// Set the fieldnorm byte for the given document for the given field.
    ///
    /// Will internally convert the u32 `fieldnorm` value to the appropriate byte
    /// to approximate the field norm in less space.
    ///
    /// * doc       - the document id
    /// * field     - the field being set
    /// * fieldnorm - the number of terms present in document `doc` in field `field`
    pub fn record(&mut self, doc: DocId, field: Field, fieldnorm: u32) {
        if let Some(fieldnorm_buffer) = self
            .fieldnorms_buffers
            .get_mut(field.field_id() as usize)
            .and_then(Option::as_mut)
        {
            match fieldnorm_buffer.len().cmp(&(doc as usize)) {
                Ordering::Less => {
                    // we fill intermediary `DocId` as  having a fieldnorm of 0.
                    fieldnorm_buffer.resize(doc as usize, 0u8);
                }
                Ordering::Equal => {}
                Ordering::Greater => {
                    panic!("Cannot register a given fieldnorm twice")
                }
            }
            fieldnorm_buffer.push(fieldnorm_to_id(fieldnorm));
        }
    }

    /// Serialize the seen fieldnorm values to the serializer for all fields.
    pub fn serialize(&self, mut fieldnorms_serializer: FieldNormsSerializer) -> io::Result<()> {
        for (field, fieldnorms_buffer) in self.fieldnorms_buffers.iter().enumerate().filter_map(
            |(field_id, fieldnorms_buffer_opt)| {
                fieldnorms_buffer_opt.as_ref().map(|fieldnorms_buffer| {
                    (Field::from_field_id(field_id as u32), fieldnorms_buffer)
                })
            },
        ) {
            fieldnorms_serializer.serialize_field(field, fieldnorms_buffer)?;
        }
        fieldnorms_serializer.close()?;
        Ok(())
    }
}
