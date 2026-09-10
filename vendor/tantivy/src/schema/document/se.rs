use std::borrow::Cow;
use std::io;
use std::io::Write;

use columnar::MonotonicallyMappableToU128;
use common::{f64_to_u64, BinarySerializable, VInt};

use super::{OwnedValue, ReferenceValueLeaf};
use crate::schema::document::{type_codes, Document, ReferenceValue, Value};
use crate::schema::Schema;

/// A serializer writing documents which implement [`Document`] to a provided writer.
pub struct BinaryDocumentSerializer<'se, W> {
    writer: &'se mut W,
    schema: &'se Schema,
}

impl<'se, W> BinaryDocumentSerializer<'se, W>
where W: Write
{
    /// Creates a new serializer with a provided writer.
    pub(crate) fn new(writer: &'se mut W, schema: &'se Schema) -> Self {
        Self { writer, schema }
    }

    /// Attempts to serialize a given document and write the output
    /// to the writer.
    #[inline]
    pub(crate) fn serialize_doc<D>(&mut self, doc: &D) -> io::Result<()>
    where D: Document {
        let stored_field_values = || {
            doc.iter_fields_and_values()
                .filter(|(field, _)| self.schema.get_field_entry(*field).is_stored())
        };
        let num_field_values = stored_field_values().count();
        let mut actual_length = 0;

        VInt(num_field_values as u64).serialize(self.writer)?;
        for (field, value_access) in stored_field_values() {
            field.serialize(self.writer)?;

            let mut serializer = BinaryValueSerializer::new(self.writer);
            match value_access.as_value() {
                ReferenceValue::Leaf(ReferenceValueLeaf::PreTokStr(pre_tokenized_text)) => {
                    serializer.serialize_value(ReferenceValue::Leaf::<&'_ OwnedValue>(
                        ReferenceValueLeaf::Str(&pre_tokenized_text.text),
                    ))?;
                }
                _ => {
                    serializer.serialize_value(value_access.as_value())?;
                }
            }

            actual_length += 1;
        }

        if num_field_values != actual_length {
            return Err(io::Error::other(format!(
                "Unexpected number of entries written to serializer, expected {num_field_values} \
                 entries, got {actual_length} entries",
            )));
        }

        Ok(())
    }
}

/// A serializer for a single value.
pub struct BinaryValueSerializer<'se, W> {
    writer: &'se mut W,
}

impl<'se, W> BinaryValueSerializer<'se, W>
where W: Write
{
    /// Creates a new serializer with a provided writer.
    pub(crate) fn new(writer: &'se mut W) -> Self {
        Self { writer }
    }

    fn serialize_with_type_code<T: BinarySerializable>(
        &mut self,
        code: u8,
        val: &T,
    ) -> io::Result<()> {
        self.write_type_code(code)?;
        BinarySerializable::serialize(val, self.writer)
    }

    /// Attempts to serialize a given value and write the output
    /// to the writer.
    pub(crate) fn serialize_value<'a, V>(
        &mut self,
        value: ReferenceValue<'a, V>,
    ) -> io::Result<()>
    where
        V: Value<'a>,
    {
        match value {
            ReferenceValue::Leaf(leaf) => match leaf {
                ReferenceValueLeaf::Null => self.write_type_code(type_codes::NULL_CODE),
                ReferenceValueLeaf::Str(val) => {
                    self.serialize_with_type_code(type_codes::TEXT_CODE, &Cow::Borrowed(val))
                }
                ReferenceValueLeaf::U64(val) => {
                    self.serialize_with_type_code(type_codes::U64_CODE, &val)
                }
                ReferenceValueLeaf::I64(val) => {
                    self.serialize_with_type_code(type_codes::I64_CODE, &val)
                }
                ReferenceValueLeaf::F64(val) => {
                    self.serialize_with_type_code(type_codes::F64_CODE, &f64_to_u64(val))
                }
                ReferenceValueLeaf::Date(val) => {
                    self.write_type_code(type_codes::DATE_CODE)?;
                    let timestamp_nanos: i64 = val.into_timestamp_nanos();
                    BinarySerializable::serialize(&timestamp_nanos, self.writer)
                }
                ReferenceValueLeaf::Facet(val) => self.serialize_with_type_code(
                    type_codes::HIERARCHICAL_FACET_CODE,
                    &Cow::Borrowed(val),
                ),
                ReferenceValueLeaf::Bytes(val) => {
                    self.serialize_with_type_code(type_codes::BYTES_CODE, &Cow::Borrowed(val))
                }
                ReferenceValueLeaf::IpAddr(val) => {
                    self.serialize_with_type_code(type_codes::IP_CODE, &val.to_u128())
                }
                ReferenceValueLeaf::Bool(val) => {
                    self.serialize_with_type_code(type_codes::BOOL_CODE, &val)
                }
                ReferenceValueLeaf::PreTokStr(val) => {
                    self.write_type_code(type_codes::EXT_CODE)?;
                    self.serialize_with_type_code(type_codes::TOK_STR_EXT_CODE, &*val)
                }
            },
            ReferenceValue::Array(elements) => {
                self.write_type_code(type_codes::ARRAY_CODE)?;

                // Somewhat unfortunate that we do this here however, writing the
                // length at the end of the complicates things quite considerably.
                let elements: Vec<V> = elements.collect();

                let mut serializer = BinaryArraySerializer::begin(elements.len(), self.writer)?;

                for value in elements {
                    serializer.serialize_value(value.as_value())?;
                }

                serializer.end()
            }
            ReferenceValue::Object(object) => {
                self.write_type_code(type_codes::OBJECT_CODE)?;

                // Somewhat unfortunate that we do this here however, writing the
                // length at the end of the complicates things quite considerably.
                let entries: Vec<(&str, V)> = object.collect();

                let mut serializer = BinaryObjectSerializer::begin(entries.len(), self.writer)?;

                for (key, value) in entries {
                    serializer.serialize_entry(key, value.as_value())?;
                }

                serializer.end()
            }
        }
    }

    fn write_type_code(&mut self, code: u8) -> io::Result<()> {
        code.serialize(self.writer)
    }
}

/// A serializer for writing a sequence of values to a writer.
pub struct BinaryArraySerializer<'se, W> {
    writer: &'se mut W,
    expected_length: usize,
    actual_length: usize,
}

impl<'se, W> BinaryArraySerializer<'se, W>
where W: Write
{
    /// Creates a new array serializer and writes the length of the array to the writer.
    pub(crate) fn begin(length: usize, writer: &'se mut W) -> io::Result<Self> {
        VInt(length as u64).serialize(writer)?;

        Ok(Self {
            writer,
            expected_length: length,
            actual_length: 0,
        })
    }

    /// Attempts to serialize a given value and write the output
    /// to the writer.
    pub(crate) fn serialize_value<'a, V>(
        &mut self,
        value: ReferenceValue<'a, V>,
    ) -> io::Result<()>
    where
        V: Value<'a>,
    {
        let mut serializer = BinaryValueSerializer::new(self.writer);
        serializer.serialize_value(value)?;

        self.actual_length += 1;
        Ok(())
    }

    /// Finishes writing the array to the writer and validates it.
    pub(crate) fn end(self) -> io::Result<()> {
        if self.expected_length != self.actual_length {
            return Err(io::Error::other(format!(
                "Unexpected number of entries written to serializer, expected {} entries, got {} \
                 entries",
                self.expected_length, self.actual_length,
            )));
        }
        Ok(())
    }
}

/// A serializer for writing a set of key-value pairs to a writer.
pub struct BinaryObjectSerializer<'se, W> {
    inner: BinaryArraySerializer<'se, W>,
    expected_length: usize,
    actual_length: usize,
}

impl<'se, W> BinaryObjectSerializer<'se, W>
where W: Write
{
    /// Creates a new object serializer and writes the length of the object to the writer.
    pub(crate) fn begin(length: usize, writer: &'se mut W) -> io::Result<Self> {
        // We mul by 2 here to count the keys and values separately:
        // [("a", 1), ("b", 2)] is actually stored as ["a", 1, "b", 2]
        let inner = BinaryArraySerializer::begin(length * 2, writer)?;

        Ok(Self {
            inner,
            expected_length: length,
            actual_length: 0,
        })
    }

    /// Attempts to serialize a given value and write the output
    /// to the writer.
    pub(crate) fn serialize_entry<'a, V>(
        &mut self,
        key: &'a str,
        value: ReferenceValue<'a, V>,
    ) -> io::Result<()>
    where
        V: Value<'a>,
    {
        // Keys and values are stored inline with one another.
        // Technically this isn't the *most* optimal way of storing the objects
        // as we could avoid writing the extra byte per key. But the gain is
        // largely not worth it for the extra complexity it brings.
        self.inner
            .serialize_value(ReferenceValue::<'a, V>::Leaf(ReferenceValueLeaf::Str(key)))?;
        self.inner.serialize_value(value)?;

        self.actual_length += 1;
        Ok(())
    }

    /// Finishes writing the array to the writer and validates it.
    pub(crate) fn end(self) -> io::Result<()> {
        if self.expected_length != self.actual_length {
            return Err(io::Error::other(format!(
                "Unexpected number of entries written to serializer, expected {} entries, got {} \
                 entries",
                self.expected_length, self.actual_length,
            )));
        }

        // This should never fail if the above statement is valid.
        self.inner.end()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::Number;
    use tokenizer_api::Token;

    use super::*;
    use crate::schema::document::existing_type_impls::JsonObjectIter;
    use crate::schema::{Facet, Field, FAST, STORED, TEXT};
    use crate::tokenizer::PreTokenizedString;

    fn serialize_value<'a>(value: ReferenceValue<'a, &'a serde_json::Value>) -> Vec<u8> {
        let mut writer = Vec::new();

        let mut serializer = BinaryValueSerializer::new(&mut writer);
        serializer.serialize_value(value).expect("Serialize value");

        writer
    }

    /// A macro for defining the expected binary representation
    /// of the serialized values in a somewhat human readable way.
    macro_rules! binary_repr {
        ($( $type_code:expr $(, $ext_code:expr)? => $value:expr $(,)?)*) => {{
            let mut writer = Vec::new();

            $(
                $type_code.serialize(&mut writer).unwrap();

                $(
                    $ext_code.serialize(&mut writer).unwrap();
                )?

                BinarySerializable::serialize(
                    &$value,
                    &mut writer,
                ).unwrap();
            )*

            writer
        }};
        (collection $code:expr, length $len:expr, $( $type_code:expr $(, $ext_code:expr)? => $value:expr $(,)?)*) => {{
            let mut writer = Vec::new();

            $code.serialize(&mut writer).unwrap();
            VInt($len as u64).serialize(&mut writer).unwrap();

            $(
                $type_code.serialize(&mut writer).unwrap();

                $(
                    $ext_code.serialize(&mut writer).unwrap();
                )?

                BinarySerializable::serialize(
                    &$value,
                    &mut writer,
                ).unwrap();
            )*

            writer
        }};
    }

    #[test]
    fn test_simple_value_serialize() {
        let result = serialize_value(ReferenceValueLeaf::Null.into());
        let expected = binary_repr!(
            type_codes::NULL_CODE => (),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValueLeaf::Str("hello, world").into());
        let expected = binary_repr!(
            type_codes::TEXT_CODE => String::from("hello, world"),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValueLeaf::U64(123).into());
        let expected = binary_repr!(
            type_codes::U64_CODE => 123u64,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValueLeaf::I64(-123).into());
        let expected = binary_repr!(
            type_codes::I64_CODE => -123i64,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValueLeaf::F64(123.3845f64).into());
        let expected = binary_repr!(
            type_codes::F64_CODE => f64_to_u64(123.3845f64),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValueLeaf::Bool(false).into());
        let expected = binary_repr!(
            type_codes::BOOL_CODE => false,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let facet = Facet::from_text("/hello/world").unwrap();
        let result = serialize_value(ReferenceValueLeaf::Facet(facet.encoded_str()).into());
        let expected = binary_repr!(
            type_codes::HIERARCHICAL_FACET_CODE => Facet::from_text("/hello/world").unwrap(),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let pre_tok_str = PreTokenizedString {
            text: "hello, world".to_string(),
            tokens: vec![Token::default(), Token::default()],
        };
        let result =
            serialize_value(ReferenceValueLeaf::PreTokStr(pre_tok_str.clone().into()).into());
        let expected = binary_repr!(
            type_codes::EXT_CODE, type_codes::TOK_STR_EXT_CODE => pre_tok_str,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );
    }

    #[test]
    fn test_array_serialize() {
        let elements = [serde_json::Value::Null, serde_json::Value::Null];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let expected = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length elements.len(),
            type_codes::NULL_CODE => (),
            type_codes::NULL_CODE => (),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let elements = [
            serde_json::Value::String("Hello, world".into()),
            serde_json::Value::String("Some demo".into()),
        ];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let expected = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length elements.len(),
            type_codes::TEXT_CODE => String::from("Hello, world"),
            type_codes::TEXT_CODE => String::from("Some demo"),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let elements = [];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let expected = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length elements.len(),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let elements = [
            serde_json::Value::Null,
            serde_json::Value::String("Hello, world".into()),
            serde_json::Value::Number(12345.into()),
        ];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let expected = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length elements.len(),
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("Hello, world"),
            type_codes::I64_CODE => 12345i64,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
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
        let expected = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len() * 2,  // To account for keys counting towards the length
            type_codes::TEXT_CODE => String::from("my-first-key"),
            type_codes::TEXT_CODE => String::from("Hello"),
            type_codes::TEXT_CODE => String::from("my-second-key"),
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("my-third-key"),
            type_codes::F64_CODE => f64_to_u64(123.0),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let object = serde_json::Map::new();
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let expected = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len(),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let mut object = serde_json::Map::new();
        object.insert("my-first-key".into(), serde_json::Value::Null);
        object.insert("my-second-key".into(), serde_json::Value::Null);
        object.insert("my-third-key".into(), serde_json::Value::Null);
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let expected = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len() * 2, // To account for keys counting towards the length
            type_codes::TEXT_CODE => String::from("my-first-key"),
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("my-second-key"),
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("my-third-key"),
            type_codes::NULL_CODE => (),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );
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

        let mut expected = Vec::new();
        let header = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len() * 2,
        );
        expected.extend_from_slice(&header);
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("my-array")));
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 2,
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("bobby of the sea"),
        );
        expected.extend_from_slice(&nested_array);
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("my-object")));
        let nested_object = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length 4,   // 2 keys, 2 values
            type_codes::TEXT_CODE => String::from("inner-1"),
            type_codes::I64_CODE => -123i64,
            type_codes::TEXT_CODE => String::from("inner-2"),
            type_codes::TEXT_CODE => String::from("bobby of the sea 2"),
        );
        expected.extend_from_slice(&nested_object);
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
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

        let mut expected = Vec::new();
        let header = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len() * 2,
        );
        expected.extend_from_slice(&header);
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("my-array")));
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 1,
        );
        expected.extend_from_slice(&nested_array);
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 2,
        );
        expected.extend_from_slice(&nested_array);
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 0,
        );
        expected.extend_from_slice(&nested_array);
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 1,
            type_codes::NULL_CODE => (),
        );
        expected.extend_from_slice(&nested_array);
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );
    }

    #[inline]
    fn serialize_doc<D: Document>(doc: &D, schema: &Schema) -> Vec<u8> {
        let mut writer = Vec::new();

        let mut serializer = BinaryDocumentSerializer::new(&mut writer, schema);
        serializer.serialize_doc(doc).expect("Serialize value");

        writer
    }

    /// A helper macro for generating the expected binary representation of the document.
    macro_rules! expected_doc_data {
        (length $len:expr) => {{
            let mut writer = Vec::new();
            VInt($len as u64).serialize(&mut writer).unwrap();
            writer
        }};
        (length $len:expr, $( $field_id:expr => $value:expr $(,)?)*) => {{
            let mut writer = Vec::new();

            VInt($len as u64).serialize(&mut writer).unwrap();
            $(
                $field_id.serialize(&mut writer).unwrap();
                $value.serialize(&mut writer).unwrap();
            )*

            writer
        }};
    }

    #[test]
    fn test_document_serialize() {
        let mut builder = Schema::builder();
        let name = builder.add_text_field("name", TEXT | STORED);
        let age = builder.add_u64_field("age", FAST | STORED);
        let schema = builder.build();

        let mut document = BTreeMap::new();
        document.insert(name, crate::schema::OwnedValue::Str("ChillFish8".into()));
        document.insert(age, crate::schema::OwnedValue::U64(20));

        let result = serialize_doc(&document, &schema);
        let mut expected = expected_doc_data!(length document.len());
        name.serialize(&mut expected).unwrap();
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("ChillFish8")));
        age.serialize(&mut expected).unwrap();
        expected.extend_from_slice(&binary_repr!(type_codes::U64_CODE => 20u64));
        assert_eq!(
            result, expected,
            "Expected serialized document to match the binary representation"
        );

        let mut builder = Schema::builder();
        let name = builder.add_text_field("name", TEXT | STORED);
        // This should be skipped when serializing.
        let age = builder.add_u64_field("age", FAST);
        let schema = builder.build();

        let mut document = BTreeMap::new();
        document.insert(name, crate::schema::OwnedValue::Str("ChillFish8".into()));
        document.insert(age, crate::schema::OwnedValue::U64(20));

        let result = serialize_doc(&document, &schema);
        let mut expected = expected_doc_data!(length 1);
        name.serialize(&mut expected).unwrap();
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("ChillFish8")));
        assert_eq!(
            result, expected,
            "Expected serialized document to match the binary representation"
        );

        let builder = Schema::builder();
        let schema = builder.build();
        let document = BTreeMap::<Field, crate::schema::OwnedValue>::new();
        let result = serialize_doc(&document, &schema);
        let expected = expected_doc_data!(length document.len());
        assert_eq!(
            result, expected,
            "Expected serialized document to match the binary representation"
        );
    }
}
