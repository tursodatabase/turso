//! Implementations of some of the core traits on various types to improve the ergonomics
//! of the API when providing custom documents.
//!
//! This allows users a bit more freedom and ergonomics if they want a simple API
//! and don't care about some of the more specialised types or only want to customise
//! part of the document structure.
use std::collections::{btree_map, hash_map, BTreeMap, HashMap};
use std::iter::Empty;
use std::net::Ipv6Addr;

use common::DateTime;
use serde_json::Number;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::facet::Facet;
use super::ReferenceValueLeaf;
use crate::schema::document::{
    ArrayAccess, DeserializeError, Document, DocumentDeserialize, DocumentDeserializer,
    ObjectAccess, ReferenceValue, Value, ValueDeserialize, ValueDeserializer, ValueVisitor,
};
use crate::schema::Field;
use crate::tokenizer::PreTokenizedString;

// Serde compatibility support.
pub fn can_be_rfc3339_date_time(text: &str) -> bool {
    if let Some(&first_byte) = text.as_bytes().first() {
        if first_byte.is_ascii_digit() {
            return true;
        }
    }

    false
}

impl<'a> Value<'a> for &'a serde_json::Value {
    type ArrayIter = std::slice::Iter<'a, serde_json::Value>;
    type ObjectIter = JsonObjectIter<'a>;

    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match self {
            serde_json::Value::Null => ReferenceValueLeaf::Null.into(),
            serde_json::Value::Bool(value) => ReferenceValueLeaf::Bool(*value).into(),
            serde_json::Value::Number(number) => {
                if let Some(val) = number.as_i64() {
                    ReferenceValueLeaf::I64(val).into()
                } else if let Some(val) = number.as_u64() {
                    ReferenceValueLeaf::U64(val).into()
                } else if let Some(val) = number.as_f64() {
                    ReferenceValueLeaf::F64(val).into()
                } else {
                    panic!("Unsupported serde_json number {number}");
                }
            }
            serde_json::Value::String(text) => {
                if can_be_rfc3339_date_time(text) {
                    match OffsetDateTime::parse(text, &Rfc3339) {
                        Ok(dt) => {
                            let dt_utc = dt.to_offset(time::UtcOffset::UTC);
                            ReferenceValueLeaf::Date(DateTime::from_utc(dt_utc)).into()
                        }
                        Err(_) => ReferenceValueLeaf::Str(text).into(),
                    }
                } else {
                    ReferenceValueLeaf::Str(text).into()
                }
            }
            serde_json::Value::Array(elements) => ReferenceValue::Array(elements.iter()),
            serde_json::Value::Object(object) => {
                ReferenceValue::Object(JsonObjectIter(object.iter()))
            }
        }
    }
}

impl<'a> Value<'a> for &'a String {
    type ArrayIter = Empty<&'a String>;
    type ObjectIter = Empty<(&'a str, &'a String)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::Str(self))
    }
}

impl<'a> Value<'a> for &'a Facet {
    type ArrayIter = Empty<&'a Facet>;
    type ObjectIter = Empty<(&'a str, &'a Facet)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::Facet(self.encoded_str()))
    }
}

impl<'a> Value<'a> for &'a u64 {
    type ArrayIter = Empty<&'a u64>;
    type ObjectIter = Empty<(&'a str, &'a u64)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::U64(**self))
    }
}

impl<'a> Value<'a> for &'a i64 {
    type ArrayIter = Empty<&'a i64>;
    type ObjectIter = Empty<(&'a str, &'a i64)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::I64(**self))
    }
}
impl<'a> Value<'a> for &'a f64 {
    type ArrayIter = Empty<&'a f64>;
    type ObjectIter = Empty<(&'a str, &'a f64)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::F64(**self))
    }
}
impl<'a> Value<'a> for &'a bool {
    type ArrayIter = Empty<&'a bool>;
    type ObjectIter = Empty<(&'a str, &'a bool)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::Bool(**self))
    }
}
impl<'a> Value<'a> for &'a str {
    type ArrayIter = Empty<&'a str>;
    type ObjectIter = Empty<(&'a str, &'a str)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::Str(self))
    }
}
impl<'a> Value<'a> for &'a &'a str {
    type ArrayIter = Empty<&'a &'a str>;
    type ObjectIter = Empty<(&'a str, &'a &'a str)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::Str(self))
    }
}

impl<'a> Value<'a> for &'a [u8] {
    type ArrayIter = Empty<&'a [u8]>;
    type ObjectIter = Empty<(&'a str, &'a [u8])>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::Bytes(self))
    }
}

impl<'a> Value<'a> for &'a &'a [u8] {
    type ArrayIter = Empty<&'a &'a [u8]>;
    type ObjectIter = Empty<(&'a str, &'a &'a [u8])>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::Bytes(self))
    }
}

impl<'a> Value<'a> for &'a Vec<u8> {
    type ArrayIter = Empty<&'a Vec<u8>>;
    type ObjectIter = Empty<(&'a str, &'a Vec<u8>)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::Bytes(self))
    }
}

impl<'a> Value<'a> for &'a DateTime {
    type ArrayIter = Empty<&'a DateTime>;
    type ObjectIter = Empty<(&'a str, &'a DateTime)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::Date(**self))
    }
}
impl<'a> Value<'a> for &'a Ipv6Addr {
    type ArrayIter = Empty<&'a Ipv6Addr>;
    type ObjectIter = Empty<(&'a str, &'a Ipv6Addr)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::IpAddr(**self))
    }
}
impl<'a> Value<'a> for &'a PreTokenizedString {
    type ArrayIter = Empty<&'a PreTokenizedString>;
    type ObjectIter = Empty<(&'a str, &'a PreTokenizedString)>;
    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        ReferenceValue::Leaf(ReferenceValueLeaf::PreTokStr(Box::new((*self).clone())))
    }
}

impl ValueDeserialize for serde_json::Value {
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        struct SerdeValueVisitor;

        impl ValueVisitor for SerdeValueVisitor {
            type Value = serde_json::Value;

            fn visit_null(&self) -> Result<Self::Value, DeserializeError> {
                Ok(serde_json::Value::Null)
            }

            fn visit_string(&self, val: String) -> Result<Self::Value, DeserializeError> {
                Ok(serde_json::Value::String(val))
            }

            fn visit_u64(&self, val: u64) -> Result<Self::Value, DeserializeError> {
                Ok(serde_json::Value::Number(val.into()))
            }

            fn visit_i64(&self, val: i64) -> Result<Self::Value, DeserializeError> {
                Ok(serde_json::Value::Number(val.into()))
            }

            fn visit_f64(&self, val: f64) -> Result<Self::Value, DeserializeError> {
                let num = Number::from_f64(val).ok_or_else(|| {
                    DeserializeError::custom(format!(
                        "serde_json::Value cannot deserialize float {val}"
                    ))
                })?;
                Ok(serde_json::Value::Number(num))
            }

            fn visit_bool(&self, val: bool) -> Result<Self::Value, DeserializeError> {
                Ok(serde_json::Value::Bool(val))
            }

            fn visit_array<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
            where A: ArrayAccess<'de> {
                let mut elements = Vec::with_capacity(access.size_hint());

                while let Some(value) = access.next_element()? {
                    elements.push(value);
                }

                Ok(serde_json::Value::Array(elements))
            }

            fn visit_object<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
            where A: ObjectAccess<'de> {
                let mut object = serde_json::Map::with_capacity(access.size_hint());

                while let Some((key, value)) = access.next_entry()? {
                    object.insert(key, value);
                }

                Ok(serde_json::Value::Object(object))
            }
        }

        deserializer.deserialize_any(SerdeValueVisitor)
    }
}

/// A wrapper struct for an iterator producing [Value]s.
pub struct JsonObjectIter<'a>(pub(crate) serde_json::map::Iter<'a>);

impl<'a> Iterator for JsonObjectIter<'a> {
    type Item = (&'a str, &'a serde_json::Value);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.0.next()?;
        Some((key, value))
    }
}

// Custom document types

// BTreeMap based documents
impl Document for BTreeMap<Field, crate::schema::OwnedValue> {
    type Value<'a> = &'a crate::schema::OwnedValue;
    type FieldsValuesIter<'a> = FieldCopyingIterator<
        'a,
        btree_map::Iter<'a, Field, crate::schema::OwnedValue>,
        crate::schema::OwnedValue,
    >;

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldCopyingIterator(self.iter())
    }
}
impl DocumentDeserialize for BTreeMap<Field, crate::schema::OwnedValue> {
    fn deserialize<'de, D>(mut deserializer: D) -> Result<Self, DeserializeError>
    where D: DocumentDeserializer<'de> {
        let mut document = BTreeMap::new();

        while let Some((field, value)) = deserializer.next_field()? {
            document.insert(field, value);
        }

        Ok(document)
    }
}

// HashMap based documents
impl Document for HashMap<Field, crate::schema::OwnedValue> {
    type Value<'a> = &'a crate::schema::OwnedValue;
    type FieldsValuesIter<'a> = FieldCopyingIterator<
        'a,
        hash_map::Iter<'a, Field, crate::schema::OwnedValue>,
        crate::schema::OwnedValue,
    >;

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldCopyingIterator(self.iter())
    }
}
impl DocumentDeserialize for HashMap<Field, crate::schema::OwnedValue> {
    fn deserialize<'de, D>(mut deserializer: D) -> Result<Self, DeserializeError>
    where D: DocumentDeserializer<'de> {
        let mut document = HashMap::with_capacity(deserializer.size_hint());

        while let Some((field, value)) = deserializer.next_field()? {
            document.insert(field, value);
        }

        Ok(document)
    }
}

pub struct FieldCopyingIterator<'a, I, V>(I)
where
    V: 'a,
    I: Iterator<Item = (&'a Field, &'a V)>;

impl<'a, I, V> Iterator for FieldCopyingIterator<'a, I, V>
where
    V: 'a,
    I: Iterator<Item = (&'a Field, &'a V)>,
{
    type Item = (Field, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let (field, value) = self.0.next()?;
        Some((*field, value))
    }
}
