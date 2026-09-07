use std::net::Ipv6Addr;

use columnar::MonotonicallyMappableToU128;

use crate::fastfield::FastValue;
use crate::schema::Field;

/// IndexingTerm is used to represent a term during indexing.
/// It's a serialized representation over field and value.
///
/// It actually wraps a `Vec<u8>`. The first 4 bytes are the field.
///
/// We serialize the field, because we index everything in a single
/// global term dictionary during indexing.
#[derive(Clone)]
pub(crate) struct IndexingTerm<B = Vec<u8>>(B)
where B: AsRef<[u8]>;

/// The number of bytes used as metadata by `Term`.
const TERM_METADATA_LENGTH: usize = 4;

impl IndexingTerm {
    /// Create a new Term with a buffer with a given capacity.
    pub fn with_capacity(capacity: usize) -> IndexingTerm {
        let mut data = Vec::with_capacity(TERM_METADATA_LENGTH + capacity);
        data.resize(TERM_METADATA_LENGTH, 0u8);
        IndexingTerm(data)
    }

    /// Panics when the term is not empty... ie: some value is set.
    /// Use `clear_with_field_and_type` in that case.
    ///
    /// Sets field and the type.
    pub(crate) fn set_field(&mut self, field: Field) {
        assert!(self.is_empty());
        self.0[0..4].clone_from_slice(field.field_id().to_be_bytes().as_ref());
    }

    /// Is empty if there are no value bytes.
    pub fn is_empty(&self) -> bool {
        self.0.len() == TERM_METADATA_LENGTH
    }

    /// Removes the value_bytes and set the field
    pub(crate) fn clear_with_field(&mut self, field: Field) {
        self.truncate_value_bytes(0);
        self.set_field(field);
    }

    /// Sets a u64 value in the term.
    ///
    /// U64 are serialized using (8-byte) BigEndian
    /// representation.
    /// The use of BigEndian has the benefit of preserving
    /// the natural order of the values.
    pub fn set_u64(&mut self, val: u64) {
        self.set_fast_value(val);
    }

    /// Sets a `i64` value in the term.
    pub fn set_i64(&mut self, val: i64) {
        self.set_fast_value(val);
    }

    /// Sets a `f64` value in the term.
    pub fn set_f64(&mut self, val: f64) {
        self.set_fast_value(val);
    }

    /// Sets a `bool` value in the term.
    pub fn set_bool(&mut self, val: bool) {
        self.set_fast_value(val);
    }

    fn set_fast_value<T: FastValue>(&mut self, val: T) {
        self.set_bytes(val.to_u64().to_be_bytes().as_ref());
    }

    /// Append a type marker + fast value to a term.
    /// This is used in JSON type to append a fast value after the path.
    ///
    /// It will not clear existing bytes.
    pub fn append_type_and_fast_value<T: FastValue>(&mut self, val: T) {
        self.0.push(T::to_type().to_code());
        let value = val.to_u64();
        self.0.extend(value.to_be_bytes().as_ref());
    }

    /// Sets a `Ipv6Addr` value in the term.
    pub fn set_ip_addr(&mut self, val: Ipv6Addr) {
        self.set_bytes(val.to_u128().to_be_bytes().as_ref());
    }

    /// Sets the value of a `Bytes` field.
    pub fn set_bytes(&mut self, bytes: &[u8]) {
        self.truncate_value_bytes(0);
        self.0.extend(bytes);
    }

    /// Truncates the value bytes of the term. Value and field type stays the same.
    pub fn truncate_value_bytes(&mut self, len: usize) {
        self.0.truncate(len + TERM_METADATA_LENGTH);
    }

    /// The length of the bytes.
    pub fn len_bytes(&self) -> usize {
        self.0.len() - TERM_METADATA_LENGTH
    }

    /// Appends value bytes to the Term.
    ///
    /// This function returns the segment that has just been added.
    #[inline]
    pub fn append_bytes(&mut self, bytes: &[u8]) -> &mut [u8] {
        let len_before = self.0.len();
        self.0.extend_from_slice(bytes);
        &mut self.0[len_before..]
    }
}

impl<B> IndexingTerm<B>
where B: AsRef<[u8]>
{
    /// Wraps serialized term bytes.
    ///
    /// The input buffer is expected to be the concatenation of the big endian encoded field id
    /// followed by the serialized value bytes (type tag + payload).
    #[inline]
    pub fn wrap(serialized_term: B) -> IndexingTerm<B> {
        debug_assert!(serialized_term.as_ref().len() >= TERM_METADATA_LENGTH);
        IndexingTerm(serialized_term)
    }

    /// Returns the field this term belongs to.
    #[inline]
    pub fn field(&self) -> Field {
        let field_id_bytes: [u8; 4] = self.0.as_ref()[..4].try_into().unwrap();
        Field::from_field_id(u32::from_be_bytes(field_id_bytes))
    }

    /// Returns the serialized representation of Term.
    /// This includes field_id, value type and value.
    ///
    /// Do NOT rely on this byte representation in the index.
    /// This value is likely to change in the future.
    #[inline]
    pub fn serialized_term(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[cfg(test)]
mod tests {

    use super::IndexingTerm;
    use crate::schema::*;

    #[test]
    pub fn test_term_str() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("text", STRING);
        let title_field = schema_builder.add_text_field("title", STRING);
        let mut term = IndexingTerm::with_capacity(0);
        term.set_field(title_field);
        term.set_bytes(b"test");
        assert_eq!(term.field(), title_field);
        assert_eq!(term.serialized_term(), b"\x00\x00\x00\x01test".to_vec())
    }

    /// Size (in bytes) of the buffer of a fast value (u64, i64, f64, or date) term.
    /// <field> + <type byte> + <value len>
    ///
    /// - <field> is a big endian encoded u32 field id
    /// - <value> is,  if this is not the json term, a binary representation specific to the type.
    ///   If it is a JSON Term, then it is prepended with the path that leads to this leaf value.
    const FAST_VALUE_TERM_LEN: usize = 4 + 8;

    #[test]
    pub fn test_term_u64() {
        let mut schema_builder = Schema::builder();
        let count_field = schema_builder.add_u64_field("count", INDEXED);
        let mut term = IndexingTerm::with_capacity(0);
        term.set_field(count_field);
        term.set_u64(983u64);
        assert_eq!(term.field(), count_field);
        assert_eq!(term.serialized_term().len(), FAST_VALUE_TERM_LEN);
    }

    #[test]
    pub fn test_term_bool() {
        let mut schema_builder = Schema::builder();
        let bool_field = schema_builder.add_bool_field("bool", INDEXED);
        let term = {
            let mut term = IndexingTerm::with_capacity(0);
            term.set_field(bool_field);
            term.set_bool(true);
            term
        };
        assert_eq!(term.field(), bool_field);
        assert_eq!(term.serialized_term().len(), FAST_VALUE_TERM_LEN);
    }

    #[test]
    pub fn indexing_term_wrap_extracts_field() {
        let field = Field::from_field_id(7u32);
        let mut term = IndexingTerm::with_capacity(0);
        term.set_field(field);
        term.append_bytes(b"abc");

        let wrapped = IndexingTerm::wrap(term.serialized_term());
        assert_eq!(wrapped.field(), field);
        assert_eq!(wrapped.serialized_term(), term.serialized_term());
    }
}
