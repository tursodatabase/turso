//! Document binary deserialization API
//!
//! The deserialization API is strongly inspired by serde's API but with
//! some tweaks, mostly around some of the types being concrete (errors)
//! and some more specific types being visited (Ips, datetime, etc...)
//!
//! The motivation behind this API is to provide a easy to implement and
//! efficient way of deserializing a potentially arbitrarily nested object.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::io;
use std::io::Read;
use std::marker::PhantomData;
use std::net::Ipv6Addr;
use std::sync::Arc;

use columnar::MonotonicallyMappableToU128;
use common::{u64_to_f64, BinarySerializable, DateTime, VInt};

use super::se::BinaryObjectSerializer;
use super::{OwnedValue, Value};
use crate::schema::document::type_codes;
use crate::schema::{Facet, Field};
use crate::store::DocStoreVersion;
use crate::tokenizer::PreTokenizedString;

#[derive(Debug, thiserror::Error, Clone)]
/// An error which occurs while attempting to deserialize a given value
/// by using the provided value visitor.
pub enum DeserializeError {
    #[error("Unsupported Type: {0:?} cannot be deserialized from the given visitor")]
    /// The value cannot be deserialized from the given type.
    UnsupportedType(ValueType),
    #[error("Type Mismatch: Expected {expected:?} but found {actual:?}")]
    /// The value cannot be deserialized from the given type.
    TypeMismatch {
        /// The expected value type.
        expected: ValueType,
        /// The actual value type read.
        actual: ValueType,
    },
    #[error("The value could not be read: {0}")]
    /// The value was unable to be read due to the error.
    CorruptedValue(Arc<io::Error>),
    #[error("{0}")]
    /// A custom error message.
    Custom(String),
    #[error("Version {0}, Max version supported: {1}")]
    /// Unsupported version error.
    UnsupportedVersion(u32, u32),
}

impl DeserializeError {
    /// Creates a new custom deserialize error.
    pub fn custom(msg: impl Display) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl From<io::Error> for DeserializeError {
    fn from(error: io::Error) -> Self {
        Self::CorruptedValue(Arc::new(error))
    }
}

/// The core trait for deserializing a document.
///
/// TODO: Improve docs
pub trait DocumentDeserialize: Sized {
    /// Attempts to deserialize Self from a given document deserializer.
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: DocumentDeserializer<'de>;
}

/// A deserializer that can walk through each entry in the document.
pub trait DocumentDeserializer<'de> {
    /// A indicator as to how many values are in the document.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next field in the document.
    fn next_field<V: ValueDeserialize>(&mut self) -> Result<Option<(Field, V)>, DeserializeError>;
}

/// The core trait for deserializing values.
///
/// TODO: Improve docs
pub trait ValueDeserialize: Sized {
    /// Attempts to deserialize Self from a given value deserializer.
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de>;
}

/// A value deserializer.
pub trait ValueDeserializer<'de> {
    /// Attempts to deserialize a null value from the deserializer.
    fn deserialize_null(self) -> Result<(), DeserializeError>;

    /// Attempts to deserialize a string value from the deserializer.
    fn deserialize_string(self) -> Result<String, DeserializeError>;

    /// Attempts to deserialize a u64 value from the deserializer.
    fn deserialize_u64(self) -> Result<u64, DeserializeError>;

    /// Attempts to deserialize an i64 value from the deserializer.
    fn deserialize_i64(self) -> Result<i64, DeserializeError>;

    /// Attempts to deserialize a f64 value from the deserializer.
    fn deserialize_f64(self) -> Result<f64, DeserializeError>;

    /// Attempts to deserialize a datetime value from the deserializer.
    fn deserialize_datetime(self) -> Result<DateTime, DeserializeError>;

    /// Attempts to deserialize a facet value from the deserializer.
    fn deserialize_facet(self) -> Result<Facet, DeserializeError>;

    /// Attempts to deserialize a bytes value from the deserializer.
    fn deserialize_bytes(self) -> Result<Vec<u8>, DeserializeError>;

    /// Attempts to deserialize an IP address value from the deserializer.
    fn deserialize_ip_address(self) -> Result<Ipv6Addr, DeserializeError>;

    /// Attempts to deserialize a bool value from the deserializer.
    fn deserialize_bool(self) -> Result<bool, DeserializeError>;

    /// Attempts to deserialize a pre-tokenized string value from the deserializer.
    fn deserialize_pre_tokenized_string(self) -> Result<PreTokenizedString, DeserializeError>;

    /// Attempts to deserialize the value using a given visitor.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, DeserializeError>
    where V: ValueVisitor;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The type of the value attempting to be deserialized.
pub enum ValueType {
    /// A null value.
    Null,
    /// A string value.
    String,
    /// A u64 value.
    U64,
    /// A i64 value.
    I64,
    /// A f64 value.
    F64,
    /// A datetime value.
    DateTime,
    /// A facet value.
    Facet,
    /// A bytes value.
    Bytes,
    /// A IP address value.
    IpAddr,
    /// A boolean value.
    Bool,
    /// A pre-tokenized string value.
    PreTokStr,
    /// An array of value.
    Array,
    /// A dynamic object value.
    Object,
    /// A JSON object value. Deprecated.
    #[deprecated(note = "We keep this for backwards compatibility, use Object instead")]
    JSONObject,
}

/// A value visitor for deserializing a document value.
///
/// This is strongly inspired by serde but has a few extra types.
///
/// TODO: Improve docs
pub trait ValueVisitor {
    /// The value produced by the visitor.
    type Value;

    #[inline]
    /// Called when the deserializer visits a string value.
    fn visit_null(&self) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Null))
    }

    #[inline]
    /// Called when the deserializer visits a string value.
    fn visit_string(&self, _val: String) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::String))
    }

    #[inline]
    /// Called when the deserializer visits a u64 value.
    fn visit_u64(&self, _val: u64) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::U64))
    }

    #[inline]
    /// Called when the deserializer visits a i64 value.
    fn visit_i64(&self, _val: i64) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::I64))
    }

    #[inline]
    /// Called when the deserializer visits a f64 value.
    fn visit_f64(&self, _val: f64) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::F64))
    }

    #[inline]
    /// Called when the deserializer visits a bool value.
    fn visit_bool(&self, _val: bool) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Bool))
    }

    #[inline]
    /// Called when the deserializer visits a datetime value.
    fn visit_datetime(&self, _val: DateTime) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::DateTime))
    }

    #[inline]
    /// Called when the deserializer visits an IP address value.
    fn visit_ip_address(&self, _val: Ipv6Addr) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::IpAddr))
    }

    #[inline]
    /// Called when the deserializer visits a facet value.
    fn visit_facet(&self, _val: Facet) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Facet))
    }

    #[inline]
    /// Called when the deserializer visits a bytes value.
    fn visit_bytes(&self, _val: Vec<u8>) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Bytes))
    }

    #[inline]
    /// Called when the deserializer visits a pre-tokenized string value.
    fn visit_pre_tokenized_string(
        &self,
        _val: PreTokenizedString,
    ) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::PreTokStr))
    }

    #[inline]
    /// Called when the deserializer visits an array.
    fn visit_array<'de, A>(&self, _access: A) -> Result<Self::Value, DeserializeError>
    where A: ArrayAccess<'de> {
        Err(DeserializeError::UnsupportedType(ValueType::Array))
    }

    #[inline]
    /// Called when the deserializer visits a object value.
    fn visit_object<'de, A>(&self, _access: A) -> Result<Self::Value, DeserializeError>
    where A: ObjectAccess<'de> {
        Err(DeserializeError::UnsupportedType(ValueType::Object))
    }
}

/// Access to a sequence of values which can be deserialized.
pub trait ArrayAccess<'de> {
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next element in the sequence.
    fn next_element<V: ValueDeserialize>(&mut self) -> Result<Option<V>, DeserializeError>;
}

/// TODO: Improve docs
pub trait ObjectAccess<'de> {
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next key-value pair in the object.
    fn next_entry<V: ValueDeserialize>(&mut self) -> Result<Option<(String, V)>, DeserializeError>;
}

/// The document deserializer used to read the tantivy documents serialized with
/// `BinarySerializable`.
///
/// This acts very similarly to serde's deserialize types and can incrementally
/// deserialize each field of the document from the provided reader (`R`).
///
/// TODO: Switch to slice instead?
pub struct BinaryDocumentDeserializer<'de, R> {
    length: usize,
    position: usize,
    doc_store_version: DocStoreVersion,
    reader: &'de mut R,
}

impl<'de, R> BinaryDocumentDeserializer<'de, R>
where R: Read
{
    /// Attempts to create a new document deserializer from a given reader.
    pub(crate) fn from_reader(
        reader: &'de mut R,
        doc_store_version: DocStoreVersion,
    ) -> Result<Self, DeserializeError> {
        let length = VInt::deserialize(reader)?;

        Ok(Self {
            length: length.val() as usize,
            position: 0,
            doc_store_version,
            reader,
        })
    }

    /// Returns true if the deserializer has deserialized all the entries
    /// within the document.
    fn is_complete(&self) -> bool {
        self.position >= self.length
    }
}

impl<'de, R> DocumentDeserializer<'de> for BinaryDocumentDeserializer<'de, R>
where R: Read
{
    #[inline]
    fn size_hint(&self) -> usize {
        self.length
    }

    fn next_field<V: ValueDeserialize>(&mut self) -> Result<Option<(Field, V)>, DeserializeError> {
        if self.is_complete() {
            return Ok(None);
        }

        let field = Field::deserialize(self.reader).map_err(DeserializeError::from)?;
        let deserializer =
            BinaryValueDeserializer::from_reader(self.reader, self.doc_store_version)?;
        let value = V::deserialize(deserializer)?;

        self.position += 1;

        Ok(Some((field, value)))
    }
}

/// A single value deserializer that deserializes a value serialized with `BinarySerializable`.
/// TODO: Improve docs
pub struct BinaryValueDeserializer<'de, R> {
    value_type: ValueType,
    reader: &'de mut R,
    doc_store_version: DocStoreVersion,
}

impl<'de, R> BinaryValueDeserializer<'de, R>
where R: Read
{
    /// Attempts to create a new value deserializer from a given reader.
    fn from_reader(
        reader: &'de mut R,
        doc_store_version: DocStoreVersion,
    ) -> Result<Self, DeserializeError> {
        let type_code = <u8 as BinarySerializable>::deserialize(reader)?;

        let value_type = match type_code {
            type_codes::TEXT_CODE => ValueType::String,
            type_codes::U64_CODE => ValueType::U64,
            type_codes::I64_CODE => ValueType::I64,
            type_codes::F64_CODE => ValueType::F64,
            type_codes::BOOL_CODE => ValueType::Bool,
            type_codes::DATE_CODE => ValueType::DateTime,
            type_codes::HIERARCHICAL_FACET_CODE => ValueType::Facet,
            type_codes::BYTES_CODE => ValueType::Bytes,
            type_codes::EXT_CODE => {
                let ext_type_code = <u8 as BinarySerializable>::deserialize(reader)?;

                match ext_type_code {
                    type_codes::TOK_STR_EXT_CODE => ValueType::PreTokStr,
                    _ => {
                        return Err(DeserializeError::from(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "No extended field type is associated with code {ext_type_code:?}"
                            ),
                        )))
                    }
                }
            }
            type_codes::IP_CODE => ValueType::IpAddr,
            type_codes::NULL_CODE => ValueType::Null,
            type_codes::ARRAY_CODE => ValueType::Array,
            type_codes::OBJECT_CODE => ValueType::Object,
            #[expect(deprecated)]
            type_codes::JSON_OBJ_CODE => ValueType::JSONObject,
            _ => {
                return Err(DeserializeError::from(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("No field type is associated with code {type_code:?}"),
                )))
            }
        };

        Ok(Self {
            value_type,
            reader,
            doc_store_version,
        })
    }

    fn validate_type(&self, expected_type: ValueType) -> Result<(), DeserializeError> {
        if self.value_type == expected_type {
            Ok(())
        } else {
            Err(DeserializeError::TypeMismatch {
                expected: expected_type,
                actual: self.value_type,
            })
        }
    }
}

impl<'de, R> ValueDeserializer<'de> for BinaryValueDeserializer<'de, R>
where R: Read
{
    fn deserialize_null(self) -> Result<(), DeserializeError> {
        self.validate_type(ValueType::Null)?;
        Ok(())
    }

    fn deserialize_string(self) -> Result<String, DeserializeError> {
        self.validate_type(ValueType::String)?;
        <String as BinarySerializable>::deserialize(self.reader).map_err(DeserializeError::from)
    }

    fn deserialize_u64(self) -> Result<u64, DeserializeError> {
        self.validate_type(ValueType::U64)?;
        <u64 as BinarySerializable>::deserialize(self.reader).map_err(DeserializeError::from)
    }

    fn deserialize_i64(self) -> Result<i64, DeserializeError> {
        self.validate_type(ValueType::I64)?;
        <i64 as BinarySerializable>::deserialize(self.reader).map_err(DeserializeError::from)
    }

    fn deserialize_f64(self) -> Result<f64, DeserializeError> {
        self.validate_type(ValueType::F64)?;
        <u64 as BinarySerializable>::deserialize(self.reader)
            .map(u64_to_f64)
            .map_err(DeserializeError::from)
    }

    fn deserialize_datetime(self) -> Result<DateTime, DeserializeError> {
        self.validate_type(ValueType::DateTime)?;
        match self.doc_store_version {
            DocStoreVersion::V1 => {
                let timestamp_micros = <i64 as BinarySerializable>::deserialize(self.reader)?;
                Ok(DateTime::from_timestamp_micros(timestamp_micros))
            }
            DocStoreVersion::V2 => {
                let timestamp_nanos = <i64 as BinarySerializable>::deserialize(self.reader)?;
                Ok(DateTime::from_timestamp_nanos(timestamp_nanos))
            }
        }
    }

    fn deserialize_facet(self) -> Result<Facet, DeserializeError> {
        self.validate_type(ValueType::Facet)?;
        <Facet as BinarySerializable>::deserialize(self.reader).map_err(DeserializeError::from)
    }

    fn deserialize_bytes(self) -> Result<Vec<u8>, DeserializeError> {
        self.validate_type(ValueType::Bytes)?;
        <Vec<u8> as BinarySerializable>::deserialize(self.reader).map_err(DeserializeError::from)
    }

    fn deserialize_ip_address(self) -> Result<Ipv6Addr, DeserializeError> {
        self.validate_type(ValueType::IpAddr)?;
        <u128 as BinarySerializable>::deserialize(self.reader)
            .map(Ipv6Addr::from_u128)
            .map_err(DeserializeError::from)
    }

    fn deserialize_bool(self) -> Result<bool, DeserializeError> {
        self.validate_type(ValueType::Bool)?;
        <bool as BinarySerializable>::deserialize(self.reader).map_err(DeserializeError::from)
    }

    fn deserialize_pre_tokenized_string(self) -> Result<PreTokenizedString, DeserializeError> {
        self.validate_type(ValueType::PreTokStr)?;
        <PreTokenizedString as BinarySerializable>::deserialize(self.reader)
            .map_err(DeserializeError::from)
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, DeserializeError>
    where V: ValueVisitor {
        match self.value_type {
            ValueType::Null => visitor.visit_null(),
            ValueType::String => {
                let val = self.deserialize_string()?;
                visitor.visit_string(val)
            }
            ValueType::U64 => {
                let val = self.deserialize_u64()?;
                visitor.visit_u64(val)
            }
            ValueType::I64 => {
                let val = self.deserialize_i64()?;
                visitor.visit_i64(val)
            }
            ValueType::F64 => {
                let val = self.deserialize_f64()?;
                visitor.visit_f64(val)
            }
            ValueType::DateTime => {
                let val = self.deserialize_datetime()?;
                visitor.visit_datetime(val)
            }
            ValueType::Facet => {
                let val = self.deserialize_facet()?;
                visitor.visit_facet(val)
            }
            ValueType::Bytes => {
                let val = self.deserialize_bytes()?;
                visitor.visit_bytes(val)
            }
            ValueType::IpAddr => {
                let val = self.deserialize_ip_address()?;
                visitor.visit_ip_address(val)
            }
            ValueType::Bool => {
                let val = self.deserialize_bool()?;
                visitor.visit_bool(val)
            }
            ValueType::PreTokStr => {
                let val = self.deserialize_pre_tokenized_string()?;
                visitor.visit_pre_tokenized_string(val)
            }
            ValueType::Array => {
                let access =
                    BinaryArrayDeserializer::from_reader(self.reader, self.doc_store_version)?;
                visitor.visit_array(access)
            }
            ValueType::Object => {
                let access =
                    BinaryObjectDeserializer::from_reader(self.reader, self.doc_store_version)?;
                visitor.visit_object(access)
            }
            #[allow(deprecated)]
            ValueType::JSONObject => {
                // This is a compatibility layer
                // The implementation is slow, but is temporary anyways
                let mut de = serde_json::Deserializer::from_reader(self.reader);
                let json_map = <serde_json::Map::<String, serde_json::Value> as serde::Deserialize>::deserialize(&mut de).map_err(|err| DeserializeError::Custom(err.to_string()))?;
                let mut out = Vec::new();
                let mut serializer = BinaryObjectSerializer::begin(json_map.len(), &mut out)?;
                for (key, val) in json_map {
                    let val: OwnedValue = val.into();
                    serializer.serialize_entry(&key, (&val).as_value())?;
                }
                serializer.end()?;

                let out_rc = std::rc::Rc::new(out);
                let mut slice: &[u8] = &out_rc;
                let access =
                    BinaryObjectDeserializer::from_reader(&mut slice, self.doc_store_version)?;

                visitor.visit_object(access)
            }
        }
    }
}

/// A deserializer for an array of values serialized with `BinarySerializable`.
/// TODO: Improve docs
pub struct BinaryArrayDeserializer<'de, R> {
    length: usize,
    position: usize,
    reader: &'de mut R,
    doc_store_version: DocStoreVersion,
}

impl<'de, R> BinaryArrayDeserializer<'de, R>
where R: Read
{
    /// Attempts to create a new array deserializer from a given reader.
    fn from_reader(
        reader: &'de mut R,
        doc_store_version: DocStoreVersion,
    ) -> Result<Self, DeserializeError> {
        let length = <VInt as BinarySerializable>::deserialize(reader)?;

        Ok(Self {
            length: length.val() as usize,
            position: 0,
            reader,
            doc_store_version,
        })
    }

    /// Returns true if the deserializer has deserialized all the elements
    /// within the array.
    fn is_complete(&self) -> bool {
        self.position >= self.length
    }
}

impl<'de, R> ArrayAccess<'de> for BinaryArrayDeserializer<'de, R>
where R: Read
{
    #[inline]
    fn size_hint(&self) -> usize {
        self.length
    }

    fn next_element<V: ValueDeserialize>(&mut self) -> Result<Option<V>, DeserializeError> {
        if self.is_complete() {
            return Ok(None);
        }

        let deserializer =
            BinaryValueDeserializer::from_reader(self.reader, self.doc_store_version)?;
        let value = V::deserialize(deserializer)?;

        // Advance the position cursor.
        self.position += 1;

        Ok(Some(value))
    }
}

/// A deserializer for a object consisting of key-value pairs.
pub struct BinaryObjectDeserializer<'de, R> {
    /// The inner deserializer.
    ///
    /// Internally an object is just represented by an array
    /// in the format of `[key, value, key, value, key, value]`.
    inner: BinaryArrayDeserializer<'de, R>,
}

impl<'de, R> BinaryObjectDeserializer<'de, R>
where R: Read
{
    /// Attempts to create a new object deserializer from a given reader.
    fn from_reader(
        reader: &'de mut R,
        doc_store_version: DocStoreVersion,
    ) -> Result<Self, DeserializeError> {
        let inner = BinaryArrayDeserializer::from_reader(reader, doc_store_version)?;
        Ok(Self { inner })
    }
}

impl<'de, R> ObjectAccess<'de> for BinaryObjectDeserializer<'de, R>
where R: Read
{
    #[inline]
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize {
        // We divide by 2 here as we know our elements are going to be
        // in the format of `[key, value, key, value, key, value]`.
        self.inner.size_hint() / 2
    }

    /// Attempts to deserialize the next key-value pair in the object.
    fn next_entry<V: ValueDeserialize>(&mut self) -> Result<Option<(String, V)>, DeserializeError> {
        if self.inner.is_complete() {
            return Ok(None);
        }

        let key = self.inner.next_element::<String>()?.expect(
            "Deserializer should not be empty as it is not marked as complete, this is a bug",
        );
        let value = self.inner.next_element::<V>()?.expect(
            "Deserializer should not be empty as it is not marked as complete, this is a bug",
        );

        Ok(Some((key, value)))
    }
}

// Core type implementations

impl ValueDeserialize for String {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_string()
    }
}

impl ValueDeserialize for u64 {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_u64()
    }
}

impl ValueDeserialize for i64 {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_i64()
    }
}

impl ValueDeserialize for f64 {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_f64()
    }
}

impl ValueDeserialize for DateTime {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_datetime()
    }
}

impl ValueDeserialize for Ipv6Addr {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_ip_address()
    }
}

impl ValueDeserialize for Facet {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_facet()
    }
}

impl ValueDeserialize for Vec<u8> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_bytes()
    }
}

impl ValueDeserialize for PreTokenizedString {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_pre_tokenized_string()
    }
}

// Collections kind of suck, but can't think of a nicer way of doing this generically
// without quite literally cloning serde entirely...

struct VecVisitor<T: ValueDeserialize>(PhantomData<T>);
impl<T: ValueDeserialize> ValueVisitor for VecVisitor<T> {
    type Value = Vec<T>;

    fn visit_array<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
    where A: ArrayAccess<'de> {
        let mut entries = Vec::with_capacity(access.size_hint());
        while let Some(value) = access.next_element()? {
            entries.push(value);
        }
        Ok(entries)
    }
}
impl<T: ValueDeserialize> ValueDeserialize for Vec<T> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_any(VecVisitor(PhantomData))
    }
}

struct BTreeMapVisitor<T: ValueDeserialize>(PhantomData<T>);
impl<T: ValueDeserialize> ValueVisitor for BTreeMapVisitor<T> {
    type Value = BTreeMap<String, T>;

    fn visit_object<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
    where A: ObjectAccess<'de> {
        let mut entries = BTreeMap::new();
        while let Some((key, value)) = access.next_entry()? {
            entries.insert(key, value);
        }
        Ok(entries)
    }
}
impl<T: ValueDeserialize> ValueDeserialize for BTreeMap<String, T> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_any(BTreeMapVisitor(PhantomData))
    }
}

struct HashMapVisitor<T: ValueDeserialize>(PhantomData<T>);
impl<T: ValueDeserialize> ValueVisitor for HashMapVisitor<T> {
    type Value = HashMap<String, T>;

    fn visit_object<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
    where A: ObjectAccess<'de> {
        let mut entries = HashMap::with_capacity(access.size_hint());
        while let Some((key, value)) = access.next_entry()? {
            entries.insert(key, value);
        }
        Ok(entries)
    }
}
impl<T: ValueDeserialize> ValueDeserialize for HashMap<String, T> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_any(HashMapVisitor(PhantomData))
    }
}

struct KeyValuesVecVisitor<T: ValueDeserialize>(PhantomData<T>);
impl<T: ValueDeserialize> ValueVisitor for KeyValuesVecVisitor<T> {
    type Value = Vec<(String, T)>;

    fn visit_object<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
    where A: ObjectAccess<'de> {
        let mut entries = Vec::with_capacity(access.size_hint());
        while let Some(entry) = access.next_entry()? {
            entries.push(entry);
        }
        Ok(entries)
    }
}
impl<T: ValueDeserialize> ValueDeserialize for Vec<(String, T)> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_any(KeyValuesVecVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::Number;
    use tokenizer_api::Token;

    use super::*;
    use crate::schema::document::existing_type_impls::JsonObjectIter;
    use crate::schema::document::se::BinaryValueSerializer;
    use crate::schema::document::{ReferenceValue, ReferenceValueLeaf};
    use crate::store::DOC_STORE_VERSION;

    fn serialize_value<'a>(value: ReferenceValue<'a, &'a serde_json::Value>) -> Vec<u8> {
        let mut writer = Vec::new();

        let mut serializer = BinaryValueSerializer::new(&mut writer);
        serializer.serialize_value(value).expect("Serialize value");

        writer
    }

    fn serialize_owned_value<'a>(value: ReferenceValue<'a, &'a OwnedValue>) -> Vec<u8> {
        let mut writer = Vec::new();

        let mut serializer = BinaryValueSerializer::new(&mut writer);
        serializer.serialize_value(value).expect("Serialize value");

        writer
    }

    fn deserialize_value(buffer: Vec<u8>) -> crate::schema::OwnedValue {
        let mut cursor = Cursor::new(buffer);
        let deserializer =
            BinaryValueDeserializer::from_reader(&mut cursor, DOC_STORE_VERSION).unwrap();
        crate::schema::OwnedValue::deserialize(deserializer).expect("Deserialize value")
    }

    #[test]
    fn test_simple_value_serialize() {
        let result = serialize_value(ReferenceValueLeaf::Null.into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::Null);

        let result = serialize_value(ReferenceValueLeaf::Str("hello, world").into());
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Str(String::from("hello, world"))
        );

        let result = serialize_value(ReferenceValueLeaf::U64(123).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::U64(123));

        let result = serialize_value(ReferenceValueLeaf::I64(-123).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::I64(-123));

        let result = serialize_value(ReferenceValueLeaf::F64(123.3845).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::F64(123.3845));

        let result = serialize_value(ReferenceValueLeaf::Bool(false).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::Bool(false));

        let result =
            serialize_value(ReferenceValueLeaf::Date(DateTime::from_timestamp_micros(100)).into());
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Date(DateTime::from_timestamp_micros(100))
        );

        let facet = Facet::from_text("/hello/world").unwrap();
        let result = serialize_value(ReferenceValueLeaf::Facet(facet.encoded_str()).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::Facet(facet));

        let pre_tok_str = PreTokenizedString {
            text: "hello, world".to_string(),
            tokens: vec![Token::default(), Token::default()],
        };
        let result =
            serialize_value(ReferenceValueLeaf::PreTokStr(pre_tok_str.clone().into()).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::PreTokStr(pre_tok_str));
    }

    #[test]
    fn test_array_serialize() {
        let elements = [serde_json::Value::Null, serde_json::Value::Null];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Array(vec![
                crate::schema::OwnedValue::Null,
                crate::schema::OwnedValue::Null,
            ]),
        );

        let elements = [
            serde_json::Value::String("Hello, world".into()),
            serde_json::Value::String("Some demo".into()),
        ];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Array(vec![
                crate::schema::OwnedValue::Str(String::from("Hello, world")),
                crate::schema::OwnedValue::Str(String::from("Some demo")),
            ]),
        );

        let elements = [];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::Array(vec![]));

        let elements = [
            serde_json::Value::Null,
            serde_json::Value::String("Hello, world".into()),
            serde_json::Value::Number(12345.into()),
        ];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Array(vec![
                crate::schema::OwnedValue::Null,
                crate::schema::OwnedValue::Str(String::from("Hello, world")),
                crate::schema::OwnedValue::I64(12345),
            ]),
        );
    }

    #[test]
    fn test_object_serialize() {
        let mut object = serde_json::Map::new();
        object.insert(
            "my-first-key".into(),
            serde_json::Value::String("Hello".into()),
        );
        object.insert("my-second-key".into(), serde_json::Value::Null);
        object.insert(
            "my-third-key".into(),
            serde_json::Value::Number(Number::from_f64(123.0).unwrap()),
        );
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);

        let mut expected_object = BTreeMap::new();
        expected_object.insert(
            "my-first-key".to_string(),
            crate::schema::OwnedValue::Str(String::from("Hello")),
        );
        expected_object.insert("my-second-key".to_string(), crate::schema::OwnedValue::Null);
        expected_object.insert(
            "my-third-key".to_string(),
            crate::schema::OwnedValue::F64(123.0),
        );
        assert_eq!(
            value,
            crate::schema::OwnedValue::Object(expected_object.into_iter().collect())
        );

        let object = serde_json::Map::new();
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);
        let expected_object = BTreeMap::new();
        assert_eq!(
            value,
            crate::schema::OwnedValue::Object(expected_object.into_iter().collect())
        );

        let mut object = serde_json::Map::new();
        object.insert("my-first-key".into(), serde_json::Value::Null);
        object.insert("my-second-key".into(), serde_json::Value::Null);
        object.insert("my-third-key".into(), serde_json::Value::Null);
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);
        let mut expected_object = BTreeMap::new();
        expected_object.insert("my-first-key".to_string(), crate::schema::OwnedValue::Null);
        expected_object.insert("my-second-key".to_string(), crate::schema::OwnedValue::Null);
        expected_object.insert("my-third-key".to_string(), crate::schema::OwnedValue::Null);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Object(expected_object.into_iter().collect())
        );
    }

    #[test]
    fn test_json_compat() {
        let data = [
            8, 123, 34, 107, 101, 121, 97, 58, 34, 58, 34, 98, 108, 117, 98, 34, 44, 34, 118, 97,
            108, 115, 34, 58, 123, 34, 104, 101, 121, 34, 58, 34, 104, 111, 34, 125, 125,
        ]
        .to_vec();
        let expected = json!({
            "keya:": "blub",
            "vals": {
                "hey": "ho"
            }
        });
        let expected_val: OwnedValue = expected.clone().into();

        let value = deserialize_value(data);
        assert_eq!(value, expected_val);
    }

    #[test]
    fn test_nested_date_precision() {
        let object = OwnedValue::Object(vec![(
            "my-date".into(),
            OwnedValue::Date(DateTime::from_timestamp_nanos(323456)),
        )]);
        let result = serialize_owned_value((&object).as_value());
        let value = deserialize_value(result);
        assert_eq!(value, object);
    }

    #[test]
    fn test_nested_serialize() {
        let mut object = serde_json::Map::new();
        object.insert(
            "my-array".into(),
            serde_json::Value::Array(vec![
                serde_json::Value::Null,
                serde_json::Value::String(String::from("bobby of the sea")),
            ]),
        );
        object.insert(
            "my-object".into(),
            serde_json::Value::Object(
                vec![
                    (
                        "inner-1".to_string(),
                        serde_json::Value::Number((-123i64).into()),
                    ),
                    (
                        "inner-2".to_string(),
                        serde_json::Value::String(String::from("bobby of the sea 2")),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        );
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);

        let mut expected_object = BTreeMap::new();
        expected_object.insert(
            "my-array".to_string(),
            crate::schema::OwnedValue::Array(vec![
                crate::schema::OwnedValue::Null,
                crate::schema::OwnedValue::Str(String::from("bobby of the sea")),
            ]),
        );
        expected_object.insert(
            "my-object".to_string(),
            crate::schema::OwnedValue::Object(
                vec![
                    (
                        "inner-1".to_string(),
                        crate::schema::OwnedValue::I64(-123i64),
                    ),
                    (
                        "inner-2".to_string(),
                        crate::schema::OwnedValue::Str(String::from("bobby of the sea 2")),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        );
        assert_eq!(
            value,
            crate::schema::OwnedValue::Object(expected_object.into_iter().collect())
        );

        // Some more extreme nesting that might behave weirdly
        let mut object = serde_json::Map::new();
        object.insert(
            "my-array".into(),
            serde_json::Value::Array(vec![serde_json::Value::Array(vec![
                serde_json::Value::Array(vec![]),
                serde_json::Value::Array(vec![serde_json::Value::Null]),
            ])]),
        );
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);

        let mut expected_object = BTreeMap::new();
        expected_object.insert(
            "my-array".to_string(),
            OwnedValue::Array(vec![OwnedValue::Array(vec![
                OwnedValue::Array(vec![]),
                OwnedValue::Array(vec![OwnedValue::Null]),
            ])]),
        );
        assert_eq!(
            value,
            OwnedValue::Object(expected_object.into_iter().collect())
        );
    }
}
