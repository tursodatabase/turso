use std::hash::Hash;
use std::net::Ipv6Addr;
use std::{fmt, str};

use columnar::MonotonicallyMappableToU128;
use common::json_path_writer::{JSON_END_OF_PATH, JSON_PATH_SEGMENT_SEP_STR};
use common::JsonPathWriter;
use serde::{Deserialize, Serialize};

use super::date_time_options::DATE_TIME_PRECISION_INDEXED;
use super::{Field, Schema};
use crate::fastfield::FastValue;
use crate::json_utils::split_json_path;
use crate::schema::{Facet, Type};
use crate::DateTime;

/// Term represents the value that the token can take.
/// It's a serialized representation over different types.
///
/// A term is composed of Field and the serialized value bytes.
/// The serialized value bytes themselves start with a one byte type tag followed by the payload.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Term {
    field: Field,
    serialized_value_bytes: Vec<u8>,
}

/// The number of bytes used as metadata when serializing a term.
const TERM_TYPE_TAG_LEN: usize = 1;

impl Term {
    /// Takes a serialized term and wraps it as a Term.
    /// First 4 bytes are the field id
    #[deprecated(
        note = "we want to avoid working on the serialized representation directly, replace with \
                typed API calls (add more if needed) or use serde to serialize/deserialize"
    )]
    pub fn wrap(serialized: &[u8]) -> Term {
        let field_id_bytes: [u8; 4] = serialized[0..4].try_into().unwrap();
        let field_id = u32::from_be_bytes(field_id_bytes);
        Term {
            field: Field::from_field_id(field_id),
            serialized_value_bytes: serialized[4..].to_vec(),
        }
    }

    /// Returns the serialized representation of the term.
    /// First 4 bytes are the field id
    #[deprecated(
        note = "we want to avoid working on the serialized representation directly, replace with \
                typed API calls (add more if needed) or use serde to serialize/deserialize"
    )]
    pub fn serialized_term(&self) -> Vec<u8> {
        let mut serialized = Vec::with_capacity(4 + self.serialized_value_bytes.len());
        serialized.extend(self.field.field_id().to_be_bytes().as_ref());
        serialized.extend_from_slice(&self.serialized_value_bytes);
        serialized
    }

    /// Create a new Term with a buffer with a given capacity.
    pub fn with_capacity(capacity: usize) -> Term {
        let mut data = Vec::with_capacity(TERM_TYPE_TAG_LEN + capacity);
        data.resize(TERM_TYPE_TAG_LEN, 0u8);
        Term {
            field: Field::from_field_id(0u32),
            serialized_value_bytes: data,
        }
    }

    /// Creates a term from a json path.
    ///
    /// The json path can address a nested value in a JSON object.
    /// e.g. `{"k8s": {"node": {"id": 5}}}` can be addressed via `k8s.node.id`.
    ///
    /// In case there are dots in the field name, and the `expand_dots_enabled` parameter is not
    /// set they need to be escaped with a backslash.
    /// e.g. `{"k8s.node": {"id": 5}}` can be addressed via `k8s\.node.id`.
    pub fn from_field_json_path(field: Field, json_path: &str, expand_dots_enabled: bool) -> Term {
        let paths = split_json_path(json_path);
        let mut json_path = JsonPathWriter::with_expand_dots(expand_dots_enabled);
        for path in paths {
            json_path.push(&path);
        }
        json_path.set_end();
        let mut term = Term::with_type_and_field(Type::Json, field);

        term.append_bytes(json_path.as_str().as_bytes());

        term
    }

    /// Gets the full path of the field name + optional json path.
    pub fn get_full_path(&self, schema: &Schema) -> String {
        let field = self.field();
        let mut field = schema.get_field_name(field).to_string();
        if let Some(json_path) = self.get_json_path() {
            field.push('.');
            field.push_str(&json_path);
        };
        field
    }

    /// Gets the json path if the type is JSON
    pub fn get_json_path(&self) -> Option<String> {
        let value = self.value();
        if let Some((json_path, _)) = value.as_json() {
            Some(unsafe {
                std::str::from_utf8_unchecked(&json_path[..json_path.len() - 1]).to_string()
            })
        } else {
            None
        }
    }

    pub(crate) fn with_type_and_field(typ: Type, field: Field) -> Term {
        let mut term = Self::with_capacity(8);
        term.set_field_and_type(field, typ);
        term
    }

    fn with_bytes_and_field_and_payload(typ: Type, field: Field, bytes: &[u8]) -> Term {
        let mut term = Self::with_capacity(bytes.len());
        term.set_field_and_type(field, typ);
        term.serialized_value_bytes.extend_from_slice(bytes);
        term
    }

    pub(crate) fn from_fast_value<T: FastValue>(field: Field, val: &T) -> Term {
        let mut term = Self::with_type_and_field(T::to_type(), field);
        term.set_bytes(val.to_u64().to_be_bytes().as_ref());
        term
    }

    /// Panics when the term is not empty... ie: some value is set.
    /// Use `clear_with_field_and_type` in that case.
    ///
    /// Sets field and the type.
    pub(crate) fn set_field_and_type(&mut self, field: Field, typ: Type) {
        assert!(self.is_empty());
        self.field = field;
        self.serialized_value_bytes[0] = typ.to_code();
    }

    /// Is empty if there are no value bytes.
    pub fn is_empty(&self) -> bool {
        self.serialized_value_bytes.len() == TERM_TYPE_TAG_LEN
    }

    /// Builds a term given a field, and a `Ipv6Addr`-value
    pub fn from_field_ip_addr(field: Field, ip_addr: Ipv6Addr) -> Term {
        let mut term = Self::with_type_and_field(Type::IpAddr, field);
        term.set_bytes(ip_addr.to_u128().to_be_bytes().as_ref());
        term
    }

    /// Builds a term given a field, and a `u64`-value
    pub fn from_field_u64(field: Field, val: u64) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a `i64`-value
    pub fn from_field_i64(field: Field, val: i64) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a `f64`-value
    pub fn from_field_f64(field: Field, val: f64) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a `bool`-value
    pub fn from_field_bool(field: Field, val: bool) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a `DateTime` value.
    ///
    /// The contained value may not match the value, due do the truncation used
    /// for indexed data [super::DATE_TIME_PRECISION_INDEXED].
    /// To create a term used for search use `from_field_date_for_search`.
    pub fn from_field_date(field: Field, val: DateTime) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a `DateTime` value to be used in searching the inverted
    /// index.
    /// It truncates the `DateTime` to the precision used in the index
    /// ([super::DATE_TIME_PRECISION_INDEXED]).
    pub fn from_field_date_for_search(field: Field, val: DateTime) -> Term {
        Term::from_fast_value(field, &val.truncate(DATE_TIME_PRECISION_INDEXED))
    }

    /// Creates a `Term` given a facet.
    pub fn from_facet(field: Field, facet: &Facet) -> Term {
        let facet_encoded_str = facet.encoded_str();
        Term::with_bytes_and_field_and_payload(Type::Facet, field, facet_encoded_str.as_bytes())
    }

    /// Builds a term given a field, and a string value
    pub fn from_field_text(field: Field, text: &str) -> Term {
        Term::with_bytes_and_field_and_payload(Type::Str, field, text.as_bytes())
    }

    /// Builds a term bytes.
    pub fn from_field_bytes(field: Field, bytes: &[u8]) -> Term {
        Term::with_bytes_and_field_and_payload(Type::Bytes, field, bytes)
    }

    /// Removes the value_bytes and set the type code.
    pub fn clear_with_type(&mut self, typ: Type) {
        self.truncate_value_bytes(0);
        self.serialized_value_bytes[0] = typ.to_code();
    }

    /// Append a type marker + fast value to a term.
    /// This is used in JSON type to append a fast value after the path.
    ///
    /// It will not clear existing bytes.
    pub fn append_type_and_fast_value<T: FastValue>(&mut self, val: T) {
        self.serialized_value_bytes.push(T::to_type().to_code());
        let value = val.to_u64();
        self.serialized_value_bytes
            .extend(value.to_be_bytes().as_ref());
    }

    /// Append a string type marker + string to a term.
    /// This is used in JSON type to append a str after the path.
    ///
    /// It will not clear existing bytes.
    pub fn append_type_and_str(&mut self, val: &str) {
        self.serialized_value_bytes.push(Type::Str.to_code());
        self.serialized_value_bytes.extend(val.as_bytes().as_ref());
    }

    /// Sets the value of a `Bytes` field.
    pub fn set_bytes(&mut self, bytes: &[u8]) {
        self.truncate_value_bytes(0);
        self.serialized_value_bytes.extend(bytes);
    }

    /// Truncates the value bytes of the term. Value and field type stays the same.
    pub fn truncate_value_bytes(&mut self, len: usize) {
        self.serialized_value_bytes
            .truncate(len + TERM_TYPE_TAG_LEN);
    }

    /// The length of the bytes.
    pub fn len_bytes(&self) -> usize {
        self.serialized_value_bytes.len() - TERM_TYPE_TAG_LEN
    }

    /// Appends value bytes to the Term.
    ///
    /// This function returns the segment that has just been added.
    #[inline]
    pub fn append_bytes(&mut self, bytes: &[u8]) -> &mut [u8] {
        let len_before = self.serialized_value_bytes.len();
        self.serialized_value_bytes.extend_from_slice(bytes);
        &mut self.serialized_value_bytes[len_before..]
    }

    /// Return the type of the term.
    pub fn typ(&self) -> Type {
        self.value().typ()
    }

    /// Returns the field.
    pub fn field(&self) -> Field {
        self.field
    }

    /// Returns the serialized representation of the value.
    /// (this does neither include the field id nor the value type.)
    ///
    /// If the term is a string, its value is utf-8 encoded.
    /// If the term is a u64, its value is encoded according
    /// to `byteorder::BigEndian`.
    pub fn serialized_value_bytes(&self) -> &[u8] {
        &self.serialized_value_bytes[TERM_TYPE_TAG_LEN..]
    }

    /// Returns the value of the term.
    /// address or JSON path + value. (this does not include the field.)
    pub fn value(&self) -> ValueBytes<&[u8]> {
        ValueBytes::wrap(self.serialized_value_bytes.as_ref())
    }
}

/// ValueBytes represents a serialized value.
///
/// The value can be of any type of [`Type`] (e.g. string, u64, f64, bool, date, JSON).
/// The serialized representation matches the lexicographical order of the type.
///
/// The `ValueBytes` format is as follow:
/// `[type code: u8][serialized value]`
///
/// For JSON `ValueBytes` equals to:
/// `[type code=JSON][JSON path][JSON_END_OF_PATH][ValueBytes]`
///
/// The nested ValueBytes in JSON is never of type JSON. (there's no recursion)
#[derive(Clone)]
pub struct ValueBytes<B>(B)
where B: AsRef<[u8]>;

impl<B> ValueBytes<B>
where B: AsRef<[u8]>
{
    /// Wraps a object holding bytes
    pub fn wrap(data: B) -> ValueBytes<B> {
        ValueBytes(data)
    }

    /// Wraps a object holding Vec<u8>
    pub fn to_owned(&self) -> ValueBytes<Vec<u8>> {
        ValueBytes(self.0.as_ref().to_vec())
    }

    fn typ_code(&self) -> u8 {
        self.0.as_ref()[0]
    }

    /// Return the type of the term.
    pub fn typ(&self) -> Type {
        Type::from_code(self.typ_code()).expect("The term has an invalid type code")
    }

    /// Returns the `u64` value stored in a term.
    ///
    /// Returns `None` if the term is not of the u64 type, or if the term byte representation
    /// is invalid.
    pub fn as_u64(&self) -> Option<u64> {
        self.get_fast_type::<u64>()
    }

    fn get_fast_type<T: FastValue>(&self) -> Option<T> {
        if self.typ() != T::to_type() {
            return None;
        }
        let value_bytes = self.raw_value_bytes_payload();
        let value_u64 = u64::from_be_bytes(value_bytes.try_into().ok()?);
        Some(T::from_u64(value_u64))
    }

    /// Returns the `i64` value stored in a term.
    ///
    /// Returns `None` if the term is not of the i64 type, or if the term byte representation
    /// is invalid.
    pub fn as_i64(&self) -> Option<i64> {
        self.get_fast_type::<i64>()
    }

    /// Returns the `f64` value stored in a term.
    ///
    /// Returns `None` if the term is not of the f64 type, or if the term byte representation
    /// is invalid.
    pub fn as_f64(&self) -> Option<f64> {
        self.get_fast_type::<f64>()
    }

    /// Returns the `bool` value stored in a term.
    ///
    /// Returns `None` if the term is not of the bool type, or if the term byte representation
    /// is invalid.
    pub fn as_bool(&self) -> Option<bool> {
        self.get_fast_type::<bool>()
    }

    /// Returns the `Date` value stored in a term.
    ///
    /// Returns `None` if the term is not of the Date type, or if the term byte representation
    /// is invalid.
    pub fn as_date(&self) -> Option<DateTime> {
        self.get_fast_type::<DateTime>()
    }

    /// Returns the text associated with the term.
    ///
    /// Returns `None` if the field is not of string type
    /// or if the bytes are not valid utf-8.
    pub fn as_str(&self) -> Option<&str> {
        if self.typ() != Type::Str {
            return None;
        }
        str::from_utf8(self.raw_value_bytes_payload()).ok()
    }

    /// Returns the facet associated with the term.
    ///
    /// Returns `None` if the field is not of facet type
    /// or if the bytes are not valid utf-8.
    pub fn as_facet(&self) -> Option<Facet> {
        if self.typ() != Type::Facet {
            return None;
        }
        let facet_encode_str = str::from_utf8(self.raw_value_bytes_payload()).ok()?;
        Some(Facet::from_encoded_string(facet_encode_str.to_string()))
    }

    /// Returns the bytes associated with the term.
    ///
    /// Returns `None` if the field is not of bytes type.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if self.typ() != Type::Bytes {
            return None;
        }
        Some(self.raw_value_bytes_payload())
    }

    /// Returns a `Ipv6Addr` value from the term.
    pub fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        if self.typ() != Type::IpAddr {
            return None;
        }
        let ip_u128 = u128::from_be_bytes(self.raw_value_bytes_payload().try_into().ok()?);
        Some(Ipv6Addr::from_u128(ip_u128))
    }

    /// Returns the json path type.
    ///
    /// Returns `None` if the value is not JSON.
    pub fn json_path_type(&self) -> Option<Type> {
        let json_value_bytes = self.as_json_value_bytes()?;

        Some(json_value_bytes.typ())
    }

    /// Returns the json path bytes (including the JSON_END_OF_PATH byte),
    /// and the encoded ValueBytes after the json path.
    ///
    /// Returns `None` if the value is not JSON.
    pub(crate) fn as_json(&self) -> Option<(&[u8], ValueBytes<&[u8]>)> {
        if self.typ() != Type::Json {
            return None;
        }
        let bytes = self.raw_value_bytes_payload();

        let pos = bytes.iter().cloned().position(|b| b == JSON_END_OF_PATH)?;
        // split at pos + 1, so that json_path_bytes includes the JSON_END_OF_PATH byte.
        let (json_path_bytes, term) = bytes.split_at(pos + 1);
        Some((json_path_bytes, ValueBytes::wrap(term)))
    }

    /// Returns the encoded ValueBytes after the json path.
    ///
    /// Returns `None` if the value is not JSON.
    pub(crate) fn as_json_value_bytes(&self) -> Option<ValueBytes<&[u8]>> {
        if self.typ() != Type::Json {
            return None;
        }
        let bytes = self.raw_value_bytes_payload();
        let pos = bytes.iter().cloned().position(|b| b == JSON_END_OF_PATH)?;
        Some(ValueBytes::wrap(&bytes[pos + 1..]))
    }

    /// Returns the raw value of ValueBytes payload, without the type tag.
    pub(crate) fn raw_value_bytes_payload(&self) -> &[u8] {
        &self.0.as_ref()[1..]
    }

    /// Returns the serialized value of ValueBytes payload, without the type tag.
    pub(crate) fn value_bytes_payload(&self) -> Vec<u8> {
        if let Some(value_bytes) = self.as_json_value_bytes() {
            value_bytes.raw_value_bytes_payload().to_vec()
        } else {
            self.raw_value_bytes_payload().to_vec()
        }
    }

    /// Returns the serialized representation of the value bytes including the type tag.
    pub fn as_serialized(&self) -> &[u8] {
        self.0.as_ref()
    }

    fn debug_value_bytes(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let typ = self.typ();
        write!(f, "type={typ:?}, ")?;
        match typ {
            Type::Str => {
                let s = self.as_str();
                write_opt(f, s)?;
            }
            Type::U64 => {
                write_opt(f, self.as_u64())?;
            }
            Type::I64 => {
                write_opt(f, self.as_i64())?;
            }
            Type::F64 => {
                write_opt(f, self.as_f64())?;
            }
            Type::Bool => {
                write_opt(f, self.as_bool())?;
            }
            // TODO pretty print these types too.
            Type::Date => {
                write_opt(f, self.as_date())?;
            }
            Type::Facet => {
                write_opt(f, self.as_facet())?;
            }
            Type::Bytes => {
                write_opt(f, self.as_bytes())?;
            }
            Type::Json => {
                if let Some((path_bytes, sub_value_bytes)) = self.as_json() {
                    // Remove the JSON_END_OF_PATH byte & convert to utf8.
                    let path = str::from_utf8(&path_bytes[..path_bytes.len() - 1])
                        .map_err(|_| std::fmt::Error)?;
                    let path_pretty = path.replace(JSON_PATH_SEGMENT_SEP_STR, ".");
                    write!(f, "path={path_pretty}, ")?;
                    sub_value_bytes.debug_value_bytes(f)?;
                }
            }
            Type::IpAddr => {
                write_opt(f, self.as_ip_addr())?;
            }
        }
        Ok(())
    }
}

fn write_opt<T: std::fmt::Debug>(f: &mut fmt::Formatter, val_opt: Option<T>) -> fmt::Result {
    if let Some(val) = val_opt {
        write!(f, "{val:?}")?;
    }
    Ok(())
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let field_id = self.field.field_id();
        write!(f, "Term(field={field_id}, ")?;
        let value_bytes = ValueBytes::wrap(&self.serialized_value_bytes);
        value_bytes.debug_value_bytes(f)?;
        write!(f, ")",)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::schema::*;

    #[test]
    pub fn test_term_str() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("text", STRING);
        let title_field = schema_builder.add_text_field("title", STRING);
        let term = Term::from_field_text(title_field, "test");
        assert_eq!(term.field(), title_field);
        assert_eq!(term.typ(), Type::Str);
        assert_eq!(term.value().as_str(), Some("test"))
    }

    #[test]
    pub fn test_term_u64() {
        let mut schema_builder = Schema::builder();
        let count_field = schema_builder.add_u64_field("count", INDEXED);
        let term = Term::from_field_u64(count_field, 983u64);
        assert_eq!(term.field(), count_field);
        assert_eq!(term.typ(), Type::U64);
        assert_eq!(term.serialized_value_bytes().len(), 8);
        assert_eq!(term.value().as_u64(), Some(983u64))
    }

    #[test]
    pub fn test_term_bool() {
        let mut schema_builder = Schema::builder();
        let bool_field = schema_builder.add_bool_field("bool", INDEXED);
        let term = Term::from_field_bool(bool_field, true);
        assert_eq!(term.field(), bool_field);
        assert_eq!(term.typ(), Type::Bool);
        assert_eq!(term.serialized_value_bytes().len(), 8);
        assert_eq!(term.value().as_bool(), Some(true))
    }
}
