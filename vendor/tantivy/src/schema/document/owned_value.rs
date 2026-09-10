use std::collections::BTreeMap;
use std::fmt;
use std::net::Ipv6Addr;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::de::{MapAccess, SeqAccess};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::existing_type_impls::can_be_rfc3339_date_time;
use super::ReferenceValueLeaf;
use crate::schema::document::{
    ArrayAccess, DeserializeError, ObjectAccess, ReferenceValue, Value, ValueDeserialize,
    ValueDeserializer, ValueVisitor,
};
use crate::schema::Facet;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;

/// This is a owned variant of `Value`, that can be passed around without lifetimes.
/// Represents the value of a any field.
/// It is an enum over all over all of the possible field type.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValue {
    /// A null value.
    Null,
    /// The str type is used for any text information.
    Str(String),
    /// Pre-tokenized str type,
    PreTokStr(PreTokenizedString),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Bool value
    Bool(bool),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    /// Facet
    Facet(Facet),
    /// Arbitrarily sized byte array
    Bytes(Vec<u8>),
    /// A set of values.
    Array(Vec<Self>),
    /// Dynamic object value.
    Object(Vec<(String, Self)>),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
}

impl AsRef<OwnedValue> for OwnedValue {
    #[inline]
    fn as_ref(&self) -> &OwnedValue {
        self
    }
}

impl OwnedValue {
    /// Returns a u8 discriminant value for the `OwnedValue` variant.
    ///
    /// This can be used to sort `OwnedValue` instances by their type.
    pub fn discriminant_value(&self) -> u8 {
        match self {
            OwnedValue::Null => 0,
            OwnedValue::Str(_) => 1,
            OwnedValue::PreTokStr(_) => 2,
            // It is key to make sure U64, I64, F64 are grouped together in there, otherwise we
            // might be breaking transivity.
            OwnedValue::U64(_) => 3,
            OwnedValue::I64(_) => 4,
            OwnedValue::F64(_) => 5,
            OwnedValue::Bool(_) => 6,
            OwnedValue::Date(_) => 7,
            OwnedValue::Facet(_) => 8,
            OwnedValue::Bytes(_) => 9,
            OwnedValue::Array(_) => 10,
            OwnedValue::Object(_) => 11,
            OwnedValue::IpAddr(_) => 12,
        }
    }
}

impl<'a> Value<'a> for &'a OwnedValue {
    type ArrayIter = std::slice::Iter<'a, OwnedValue>;
    type ObjectIter = ObjectMapIter<'a>;

    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match self {
            OwnedValue::Null => ReferenceValueLeaf::Null.into(),
            OwnedValue::Str(val) => ReferenceValueLeaf::Str(val).into(),
            OwnedValue::PreTokStr(val) => ReferenceValueLeaf::PreTokStr(val.clone().into()).into(),
            OwnedValue::U64(val) => ReferenceValueLeaf::U64(*val).into(),
            OwnedValue::I64(val) => ReferenceValueLeaf::I64(*val).into(),
            OwnedValue::F64(val) => ReferenceValueLeaf::F64(*val).into(),
            OwnedValue::Bool(val) => ReferenceValueLeaf::Bool(*val).into(),
            OwnedValue::Date(val) => ReferenceValueLeaf::Date(*val).into(),
            OwnedValue::Facet(val) => ReferenceValueLeaf::Facet(val.encoded_str()).into(),
            OwnedValue::Bytes(val) => ReferenceValueLeaf::Bytes(val).into(),
            OwnedValue::IpAddr(val) => ReferenceValueLeaf::IpAddr(*val).into(),
            OwnedValue::Array(array) => ReferenceValue::Array(array.iter()),
            OwnedValue::Object(object) => ReferenceValue::Object(ObjectMapIter(object.iter())),
        }
    }
}

impl ValueDeserialize for OwnedValue {
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        struct Visitor;

        impl ValueVisitor for Visitor {
            type Value = OwnedValue;

            fn visit_null(&self) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::Null)
            }

            fn visit_string(&self, val: String) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::Str(val))
            }

            fn visit_u64(&self, val: u64) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::U64(val))
            }

            fn visit_i64(&self, val: i64) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::I64(val))
            }

            fn visit_f64(&self, val: f64) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::F64(val))
            }

            fn visit_bool(&self, val: bool) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::Bool(val))
            }

            fn visit_datetime(&self, val: DateTime) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::Date(val))
            }

            fn visit_ip_address(&self, val: Ipv6Addr) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::IpAddr(val))
            }

            fn visit_facet(&self, val: Facet) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::Facet(val))
            }

            fn visit_bytes(&self, val: Vec<u8>) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::Bytes(val))
            }

            fn visit_pre_tokenized_string(
                &self,
                val: PreTokenizedString,
            ) -> Result<Self::Value, DeserializeError> {
                Ok(OwnedValue::PreTokStr(val))
            }

            fn visit_array<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
            where A: ArrayAccess<'de> {
                let mut elements = Vec::with_capacity(access.size_hint());

                while let Some(value) = access.next_element()? {
                    elements.push(value);
                }

                Ok(OwnedValue::Array(elements))
            }

            fn visit_object<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
            where A: ObjectAccess<'de> {
                let mut elements = Vec::with_capacity(access.size_hint());

                while let Some((key, value)) = access.next_entry()? {
                    elements.push((key, value));
                }

                Ok(OwnedValue::Object(elements))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl Eq for OwnedValue {}

impl serde::Serialize for OwnedValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        use serde::ser::SerializeMap;
        match *self {
            OwnedValue::Null => serializer.serialize_unit(),
            OwnedValue::Str(ref v) => serializer.serialize_str(v),
            OwnedValue::PreTokStr(ref v) => v.serialize(serializer),
            OwnedValue::U64(u) => serializer.serialize_u64(u),
            OwnedValue::I64(u) => serializer.serialize_i64(u),
            OwnedValue::F64(u) => serializer.serialize_f64(u),
            OwnedValue::Bool(b) => serializer.serialize_bool(b),
            OwnedValue::Date(ref date) => {
                time::serde::rfc3339::serialize(&date.into_utc(), serializer)
            }
            OwnedValue::Facet(ref facet) => facet.serialize(serializer),
            OwnedValue::Bytes(ref bytes) => serializer.serialize_str(&BASE64.encode(bytes)),
            OwnedValue::Object(ref obj) => {
                let mut map = serializer.serialize_map(Some(obj.len()))?;
                for (k, v) in obj {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
            OwnedValue::IpAddr(ref ip_v6) => {
                // Ensure IpV4 addresses get serialized as IpV4, but excluding IpV6 loopback.
                if let Some(ip_v4) = ip_v6.to_ipv4_mapped() {
                    ip_v4.serialize(serializer)
                } else {
                    ip_v6.serialize(serializer)
                }
            }
            OwnedValue::Array(ref array) => array.serialize(serializer),
        }
    }
}

impl<'de> serde::Deserialize<'de> for OwnedValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        struct ValueVisitor;

        impl<'de> serde::de::Visitor<'de> for ValueVisitor {
            type Value = OwnedValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string or u32")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
                Ok(OwnedValue::Bool(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(OwnedValue::I64(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(OwnedValue::U64(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(OwnedValue::F64(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(OwnedValue::Str(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(OwnedValue::Str(v))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where E: serde::de::Error {
                Ok(OwnedValue::Null)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where A: SeqAccess<'de> {
                let mut elements = Vec::with_capacity(seq.size_hint().unwrap_or_default());

                while let Some(value) = seq.next_element()? {
                    elements.push(value);
                }

                Ok(OwnedValue::Array(elements))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where A: MapAccess<'de> {
                let mut object = map.size_hint().map(Vec::with_capacity).unwrap_or_default();
                while let Some((key, value)) = map.next_entry()? {
                    object.push((key, value));
                }
                Ok(OwnedValue::Object(object))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl<'a, V: Value<'a>> From<ReferenceValue<'a, V>> for OwnedValue {
    fn from(val: ReferenceValue<'a, V>) -> OwnedValue {
        match val {
            ReferenceValue::Leaf(leaf) => match leaf {
                ReferenceValueLeaf::Null => OwnedValue::Null,
                ReferenceValueLeaf::Str(val) => OwnedValue::Str(val.to_string()),
                ReferenceValueLeaf::U64(val) => OwnedValue::U64(val),
                ReferenceValueLeaf::I64(val) => OwnedValue::I64(val),
                ReferenceValueLeaf::F64(val) => OwnedValue::F64(val),
                ReferenceValueLeaf::Date(val) => OwnedValue::Date(val),
                ReferenceValueLeaf::Facet(val) => {
                    OwnedValue::Facet(Facet::from_encoded_string(val.to_string()))
                }
                ReferenceValueLeaf::Bytes(val) => OwnedValue::Bytes(val.to_vec()),
                ReferenceValueLeaf::IpAddr(val) => OwnedValue::IpAddr(val),
                ReferenceValueLeaf::Bool(val) => OwnedValue::Bool(val),
                ReferenceValueLeaf::PreTokStr(val) => OwnedValue::PreTokStr(*val.clone()),
            },
            ReferenceValue::Array(val) => {
                OwnedValue::Array(val.map(|v| v.as_value().into()).collect())
            }
            ReferenceValue::Object(val) => OwnedValue::Object(
                val.map(|(k, v)| (k.to_string(), v.as_value().into()))
                    .collect(),
            ),
        }
    }
}

impl From<String> for OwnedValue {
    fn from(s: String) -> OwnedValue {
        OwnedValue::Str(s)
    }
}

impl From<Ipv6Addr> for OwnedValue {
    fn from(v: Ipv6Addr) -> OwnedValue {
        OwnedValue::IpAddr(v)
    }
}

impl From<u64> for OwnedValue {
    fn from(v: u64) -> OwnedValue {
        OwnedValue::U64(v)
    }
}

impl From<i64> for OwnedValue {
    fn from(v: i64) -> OwnedValue {
        OwnedValue::I64(v)
    }
}

impl From<f64> for OwnedValue {
    fn from(v: f64) -> OwnedValue {
        OwnedValue::F64(v)
    }
}

impl From<bool> for OwnedValue {
    fn from(b: bool) -> Self {
        OwnedValue::Bool(b)
    }
}

impl From<DateTime> for OwnedValue {
    fn from(dt: DateTime) -> OwnedValue {
        OwnedValue::Date(dt)
    }
}

impl<'a> From<&'a str> for OwnedValue {
    fn from(s: &'a str) -> OwnedValue {
        OwnedValue::Str(s.to_string())
    }
}

impl<'a> From<&'a [u8]> for OwnedValue {
    fn from(bytes: &'a [u8]) -> OwnedValue {
        OwnedValue::Bytes(bytes.to_vec())
    }
}

impl From<Facet> for OwnedValue {
    fn from(facet: Facet) -> OwnedValue {
        OwnedValue::Facet(facet)
    }
}

impl From<Vec<u8>> for OwnedValue {
    fn from(bytes: Vec<u8>) -> OwnedValue {
        OwnedValue::Bytes(bytes)
    }
}

impl From<PreTokenizedString> for OwnedValue {
    fn from(pretokenized_string: PreTokenizedString) -> OwnedValue {
        OwnedValue::PreTokStr(pretokenized_string)
    }
}

impl From<BTreeMap<String, OwnedValue>> for OwnedValue {
    fn from(object: BTreeMap<String, OwnedValue>) -> OwnedValue {
        let key_values = object.into_iter().collect();
        OwnedValue::Object(key_values)
    }
}

impl From<serde_json::Value> for OwnedValue {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(val) => Self::Bool(val),
            serde_json::Value::Number(number) => {
                if let Some(val) = number.as_i64() {
                    Self::I64(val)
                } else if let Some(val) = number.as_u64() {
                    Self::U64(val)
                } else if let Some(val) = number.as_f64() {
                    Self::F64(val)
                } else {
                    panic!("Unsupported serde_json number {number}");
                }
            }
            serde_json::Value::String(text) => {
                if can_be_rfc3339_date_time(&text) {
                    match OffsetDateTime::parse(&text, &Rfc3339) {
                        Ok(dt) => {
                            let dt_utc = dt.to_offset(time::UtcOffset::UTC);
                            Self::Date(DateTime::from_utc(dt_utc))
                        }
                        Err(_) => Self::Str(text),
                    }
                } else {
                    Self::Str(text)
                }
            }
            serde_json::Value::Array(elements) => {
                let converted_elements = elements.into_iter().map(Self::from).collect();
                Self::Array(converted_elements)
            }
            serde_json::Value::Object(object) => Self::from(object),
        }
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for OwnedValue {
    fn from(map: serde_json::Map<String, serde_json::Value>) -> Self {
        let object: Vec<(String, OwnedValue)> = map
            .into_iter()
            .map(|(key, value)| (key, OwnedValue::from(value)))
            .collect();
        OwnedValue::Object(object)
    }
}

/// A wrapper type for iterating over a serde_json object producing reference values.
pub struct ObjectMapIter<'a>(std::slice::Iter<'a, (String, OwnedValue)>);

impl<'a> Iterator for ObjectMapIter<'a> {
    type Item = (&'a str, &'a OwnedValue);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.0.next()?;
        Some((key.as_str(), value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{BytesOptions, Schema};
    use crate::{Document, TantivyDocument};

    #[test]
    fn test_parse_bytes_doc() {
        let mut schema_builder = Schema::builder();
        let bytes_options = BytesOptions::default();
        let bytes_field = schema_builder.add_bytes_field("my_bytes", bytes_options);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        doc.add_bytes(bytes_field, "this is a test".as_bytes());
        let json_string = doc.to_json(&schema);
        assert_eq!(json_string, r#"{"my_bytes":["dGhpcyBpcyBhIHRlc3Q="]}"#);
    }

    #[test]
    fn test_parse_empty_bytes_doc() {
        let mut schema_builder = Schema::builder();
        let bytes_options = BytesOptions::default();
        let bytes_field = schema_builder.add_bytes_field("my_bytes", bytes_options);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        doc.add_bytes(bytes_field, "".as_bytes());
        let json_string = doc.to_json(&schema);

        assert_eq!(json_string, r#"{"my_bytes":[""]}"#);
    }

    #[test]
    fn test_parse_many_bytes_doc() {
        let mut schema_builder = Schema::builder();
        let bytes_options = BytesOptions::default();
        let bytes_field = schema_builder.add_bytes_field("my_bytes", bytes_options);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        doc.add_bytes(
            bytes_field,
            "A bigger test I guess\nspanning on multiple lines\nhoping this will work".as_bytes(),
        );
        let json_string = doc.to_json(&schema);
        assert_eq!(
            json_string,
            r#"{"my_bytes":["QSBiaWdnZXIgdGVzdCBJIGd1ZXNzCnNwYW5uaW5nIG9uIG11bHRpcGxlIGxpbmVzCmhvcGluZyB0aGlzIHdpbGwgd29yaw=="]}"#
        );
    }

    #[test]
    fn test_serialize_date() {
        let value = OwnedValue::from(DateTime::from_utc(
            OffsetDateTime::parse("1996-12-20T00:39:57+00:00", &Rfc3339).unwrap(),
        ));
        let serialized_value_json = serde_json::to_string_pretty(&value).unwrap();
        assert_eq!(serialized_value_json, r#""1996-12-20T00:39:57Z""#);
        let value = OwnedValue::from(DateTime::from_utc(
            OffsetDateTime::parse("1996-12-20T00:39:57-01:00", &Rfc3339).unwrap(),
        ));
        let serialized_value_json = serde_json::to_string_pretty(&value).unwrap();
        // The time zone information gets lost by conversion into `Value::Date` and
        // implicitly becomes UTC.
        assert_eq!(serialized_value_json, r#""1996-12-20T01:39:57Z""#);
    }
}
