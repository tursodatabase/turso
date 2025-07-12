mod cache;
mod error;
pub(crate) mod jsonb;
mod ops;
pub(crate) mod path;

use crate::json::error::Error as JsonError;
pub use crate::json::ops::{
    json_insert, json_patch, json_remove, json_replace, jsonb_insert, jsonb_patch, jsonb_remove,
    jsonb_replace,
};
use crate::json::path::{json_path, JsonPath, PathElement};
use crate::types::{RawSlice, Text, TextRef, TextSubtype, Value, ValueType};
use crate::vdbe::Register;
use crate::{bail_constraint_error, bail_parse_error, LimboError, RefValue};
pub use cache::JsonCacheCell;
use jsonb::{ElementType, Jsonb, JsonbHeader, PathOperationMode, SearchOperation, SetOperation};
use std::borrow::Cow;
use std::str::FromStr;

#[derive(Debug, Clone, Copy)]
pub enum Conv {
    Strict,
    NotStrict,
    ToString,
}

#[cfg(feature = "json")]
pub enum OutputVariant {
    ElementType,
    Binary,
    String,
}

pub fn get_json(json_value: &Value, indent: Option<&str>) -> crate::Result<Value> {
    match json_value {
        Value::Text(ref t) => {
            // optimization: once we know the subtype is a valid JSON, we do not have
            // to go through parsing JSON and serializing it back to string
            if t.subtype == TextSubtype::Json {
                return Ok(json_value.to_owned());
            }

            let json_val = convert_dbtype_to_jsonb(json_value, Conv::Strict)?;
            let json = match indent {
                Some(indent) => json_val.to_string_pretty(Some(indent))?,
                None => json_val.to_string()?,
            };

            Ok(Value::Text(Text::json(json)))
        }
        Value::Blob(b) => {
            let jsonbin = Jsonb::new(b.len(), Some(b));
            jsonbin.is_valid()?;
            Ok(Value::Text(Text {
                value: jsonbin.to_string()?.into_bytes(),
                subtype: TextSubtype::Json,
            }))
        }
        Value::Null => Ok(Value::Null),
        _ => {
            let json_val = convert_dbtype_to_jsonb(json_value, Conv::Strict)?;
            let json = match indent {
                Some(indent) => Value::Text(Text::json(json_val.to_string_pretty(Some(indent))?)),
                None => {
                    let element_type = json_val.is_valid()?;
                    json_string_to_db_type(json_val, element_type, OutputVariant::ElementType)?
                }
            };
            Ok(json)
        }
    }
}

pub fn jsonb(json_value: &Value, cache: &JsonCacheCell) -> crate::Result<Value> {
    let json_conv_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);

    let jsonbin = cache.get_or_insert_with(json_value, json_conv_fn);
    match jsonbin {
        Ok(jsonbin) => Ok(Value::Blob(jsonbin.data())),
        Err(_) => {
            bail_parse_error!("malformed JSON")
        }
    }
}

pub fn convert_dbtype_to_raw_jsonb(data: &Value) -> crate::Result<Vec<u8>> {
    let json = convert_dbtype_to_jsonb(data, Conv::NotStrict)?;
    Ok(json.data())
}

pub fn json_from_raw_bytes_agg(data: &[u8], raw: bool) -> crate::Result<Value> {
    let mut json = Jsonb::from_raw_data(data);
    let el_type = json.is_valid()?;
    json.finalize_unsafe(el_type)?;
    if raw {
        json_string_to_db_type(json, el_type, OutputVariant::Binary)
    } else {
        json_string_to_db_type(json, el_type, OutputVariant::ElementType)
    }
}

pub fn convert_dbtype_to_jsonb(val: &Value, strict: Conv) -> crate::Result<Jsonb> {
    convert_ref_dbtype_to_jsonb(
        &match val {
            Value::Null => RefValue::Null,
            Value::Integer(x) => RefValue::Integer(*x),
            Value::Float(x) => RefValue::Float(*x),
            Value::Text(text) => {
                RefValue::Text(TextRef::create_from(text.as_str().as_bytes(), text.subtype))
            }
            Value::Blob(items) => RefValue::Blob(RawSlice::create_from(items)),
        },
        strict,
    )
}

pub fn convert_ref_dbtype_to_jsonb(val: &RefValue, strict: Conv) -> crate::Result<Jsonb> {
    match val {
        RefValue::Text(text) => {
            let res = if text.subtype == TextSubtype::Json || matches!(strict, Conv::Strict) {
                // Parse directly as JSON if it's already JSON subtype or strict mode is on
                let json = if matches!(strict, Conv::ToString) {
                    let mut str = text.as_str().replace('"', "\\\"");
                    str.insert(0, '"');
                    str.push('"');
                    Jsonb::from_str(&str)
                } else {
                    Jsonb::from_str(text.as_str())
                };
                json
            } else {
                // Handle as a string literal otherwise
                let mut str = text.as_str().replace('"', "\\\"");

                // Quote the string to make it a JSON string
                str.insert(0, '"');
                str.push('"');
                Jsonb::from_str(&str)
            };
            res.map_err(|_| LimboError::ParseError("malformed JSON".to_string()))
        }
        RefValue::Blob(blob) => {
            let json = Jsonb::from_raw_data(blob.to_slice());
            json.is_valid()?;
            Ok(json)
        }
        RefValue::Null => Ok(Jsonb::from_raw_data(
            JsonbHeader::make_null().into_bytes().as_bytes(),
        )),
        RefValue::Float(float) => {
            let mut buff = ryu::Buffer::new();
            Jsonb::from_str(buff.format(*float))
                .map_err(|_| LimboError::ParseError("malformed JSON".to_string()))
        }
        RefValue::Integer(int) => Jsonb::from_str(&int.to_string())
            .map_err(|_| LimboError::ParseError("malformed JSON".to_string())),
    }
}

pub fn curry_convert_dbtype_to_jsonb(strict: Conv) -> impl Fn(&Value) -> crate::Result<Jsonb> {
    move |val| convert_dbtype_to_jsonb(val, strict)
}

pub fn json_array(values: &[Register]) -> crate::Result<Value> {
    let mut json = Jsonb::make_empty_array(values.len());

    for value in values.iter() {
        if matches!(value.get_owned_value(), Value::Blob(_)) {
            crate::bail_constraint_error!("JSON cannot hold BLOB values")
        }
        let value = convert_dbtype_to_jsonb(value.get_owned_value(), Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }
    json.finalize_unsafe(ElementType::ARRAY)?;

    json_string_to_db_type(json, ElementType::ARRAY, OutputVariant::ElementType)
}

pub fn jsonb_array(values: &[Register]) -> crate::Result<Value> {
    let mut json = Jsonb::make_empty_array(values.len());

    for value in values.iter() {
        if matches!(value.get_owned_value(), Value::Blob(_)) {
            crate::bail_constraint_error!("JSON cannot hold BLOB values")
        }
        let value = convert_dbtype_to_jsonb(value.get_owned_value(), Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }
    json.finalize_unsafe(ElementType::ARRAY)?;

    json_string_to_db_type(json, ElementType::ARRAY, OutputVariant::Binary)
}

pub fn json_array_length(
    value: &Value,
    path: Option<&Value>,
    json_cache: &JsonCacheCell,
) -> crate::Result<Value> {
    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(value, make_jsonb_fn)?;

    if path.is_none() {
        let len = json.array_len()?;
        return Ok(Value::Integer(len as i64));
    }

    let path = json_path_from_owned_value(path.expect("We already checked none"), true)?;

    if let Some(path) = path {
        let mut op = SearchOperation::new(json.len() / 2);
        let _ = json.operate_on_path(&path, &mut op);
        if let Ok(len) = op.result().array_len() {
            return Ok(Value::Integer(len as i64));
        }
    }
    Ok(Value::Null)
}

pub fn json_set(args: &[Register], json_cache: &JsonCacheCell) -> crate::Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(args[0].get_owned_value(), make_jsonb_fn)?;
    let other = args[1..].chunks_exact(2);

    for chunk in other {
        let path = json_path_from_owned_value(chunk[0].get_owned_value(), true)?;

        let value = convert_dbtype_to_jsonb(chunk[1].get_owned_value(), Conv::NotStrict)?;
        let mut op = SetOperation::new(value);
        if let Some(path) = path {
            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.is_valid()?;

    json_string_to_db_type(json, el_type, OutputVariant::String)
}

pub fn jsonb_set(args: &[Register], json_cache: &JsonCacheCell) -> crate::Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(args[0].get_owned_value(), make_jsonb_fn)?;
    let other = args[1..].chunks_exact(2);

    for chunk in other {
        let path = json_path_from_owned_value(chunk[0].get_owned_value(), true)?;

        let value = convert_dbtype_to_jsonb(chunk[1].get_owned_value(), Conv::NotStrict)?;
        let mut op = SetOperation::new(value);
        if let Some(path) = path {
            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.is_valid()?;

    json_string_to_db_type(json, el_type, OutputVariant::Binary)
}

/// Implements the -> operator. Always returns a proper JSON value.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_extract(
    value: &Value,
    path: &Value,
    json_cache: &JsonCacheCell,
) -> crate::Result<Value> {
    if let Value::Null = value {
        return Ok(Value::Null);
    }

    if let Some(path) = json_path_from_owned_value(path, false)? {
        let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
        let mut json = json_cache.get_or_insert_with(value, make_jsonb_fn)?;
        let mut op = SearchOperation::new(json.len());
        let res = json.operate_on_path(&path, &mut op);
        let extracted = op.result();
        if res.is_ok() {
            Ok(Value::Text(Text::json(extracted.to_string()?)))
        } else {
            Ok(Value::Null)
        }
    } else {
        Ok(Value::Null)
    }
}

/// Implements the ->> operator. Always returns a SQL representation of the JSON subcomponent.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_shift_extract(
    value: &Value,
    path: &Value,
    json_cache: &JsonCacheCell,
) -> crate::Result<Value> {
    if let Value::Null = value {
        return Ok(Value::Null);
    }
    if let Some(path) = json_path_from_owned_value(path, false)? {
        let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
        let mut json = json_cache.get_or_insert_with(value, make_jsonb_fn)?;
        let mut op = SearchOperation::new(json.len());
        let res = json.operate_on_path(&path, &mut op);
        let extracted = op.result();
        let element_type = match extracted.is_valid() {
            Err(_) => return Ok(Value::Null),
            Ok(el) => el,
        };

        if res.is_ok() {
            Ok(json_string_to_db_type(
                extracted,
                element_type,
                OutputVariant::ElementType,
            )?)
        } else {
            Ok(Value::Null)
        }
    } else {
        Ok(Value::Null)
    }
}

/// Extracts a JSON value from a JSON object or array.
/// If there's only a single path, the return value might be either a TEXT or a database type.
/// https://sqlite.org/json1.html#the_json_extract_function
pub fn json_extract(
    value: &Value,
    paths: &[Register],
    json_cache: &JsonCacheCell,
) -> crate::Result<Value> {
    if let Value::Null = value {
        return Ok(Value::Null);
    }

    if paths.is_empty() {
        return Ok(Value::Null);
    }
    let convert_to_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let jsonb = json_cache.get_or_insert_with(value, convert_to_jsonb)?;
    let (json, element_type) = jsonb_extract_internal(jsonb, paths)?;

    let result = json_string_to_db_type(json, element_type, OutputVariant::ElementType)?;

    Ok(result)
}

pub fn jsonb_extract(
    value: &Value,
    paths: &[Register],
    json_cache: &JsonCacheCell,
) -> crate::Result<Value> {
    if let Value::Null = value {
        return Ok(Value::Null);
    }

    if paths.is_empty() {
        return Ok(Value::Null);
    }
    let convert_to_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let jsonb = json_cache.get_or_insert_with(value, convert_to_jsonb)?;

    let (json, element_type) = jsonb_extract_internal(jsonb, paths)?;
    let result = json_string_to_db_type(json, element_type, OutputVariant::ElementType)?;

    Ok(result)
}

fn jsonb_extract_internal(value: Jsonb, paths: &[Register]) -> crate::Result<(Jsonb, ElementType)> {
    let null = Jsonb::from_raw_data(JsonbHeader::make_null().into_bytes().as_bytes());
    if paths.len() == 1 {
        if let Some(path) = json_path_from_owned_value(paths[0].get_owned_value(), true)? {
            let mut json = value;

            let mut op = SearchOperation::new(json.len());
            let res = json.operate_on_path(&path, &mut op);
            let extracted = op.result();
            let element_type = match extracted.is_valid() {
                Err(_) => return Ok((null, ElementType::NULL)),
                Ok(el) => el,
            };
            if res.is_ok() {
                return Ok((extracted, element_type));
            } else {
                return Ok((null, ElementType::NULL));
            }
        } else {
            return Ok((null, ElementType::NULL));
        }
    }

    let mut json = value;
    let mut result = Jsonb::make_empty_array(json.len());

    // TODO: make an op to avoid creating new json for every path element
    let paths = paths
        .iter()
        .map(|p| json_path_from_owned_value(p.get_owned_value(), true));
    for path in paths {
        if let Some(path) = path? {
            let mut op = SearchOperation::new(json.len());
            let res = json.operate_on_path(&path, &mut op);
            let extracted = op.result();
            if res.is_ok() {
                result.append_to_array_unsafe(&extracted.data());
            } else {
                result.append_to_array_unsafe(JsonbHeader::make_null().into_bytes().as_bytes());
            }
        } else {
            return Ok((null, ElementType::NULL));
        }
    }
    result.finalize_unsafe(ElementType::ARRAY)?;
    Ok((result, ElementType::ARRAY))
}

pub fn json_string_to_db_type(
    json: Jsonb,
    element_type: ElementType,
    flag: OutputVariant,
) -> crate::Result<Value> {
    let mut json_string = json.to_string()?;
    if matches!(flag, OutputVariant::Binary) {
        return Ok(Value::Blob(json.data()));
    }
    match element_type {
        ElementType::ARRAY | ElementType::OBJECT => Ok(Value::Text(Text::json(json_string))),
        ElementType::TEXT | ElementType::TEXT5 | ElementType::TEXTJ | ElementType::TEXTRAW => {
            if matches!(flag, OutputVariant::ElementType) {
                json_string.remove(json_string.len() - 1);
                json_string.remove(0);
                Ok(Value::Text(Text {
                    value: json_string.into_bytes(),
                    subtype: TextSubtype::Json,
                }))
            } else {
                Ok(Value::Text(Text {
                    value: json_string.into_bytes(),
                    subtype: TextSubtype::Text,
                }))
            }
        }
        ElementType::FLOAT5 | ElementType::FLOAT => Ok(Value::Float(
            json_string.parse().expect("Should be valid f64"),
        )),
        ElementType::INT | ElementType::INT5 => {
            let result = i64::from_str(&json_string);
            if let Ok(int) = result {
                Ok(Value::Integer(int))
            } else {
                let res = f64::from_str(&json_string);
                match res {
                    Ok(num) => Ok(Value::Float(num)),
                    Err(_) => Ok(Value::Null),
                }
            }
        }
        ElementType::TRUE => Ok(Value::Integer(1)),
        ElementType::FALSE => Ok(Value::Integer(0)),
        ElementType::NULL => Ok(Value::Null),
        _ => unreachable!(),
    }
}

pub fn json_type(value: &Value, path: Option<&Value>) -> crate::Result<Value> {
    if let Value::Null = value {
        return Ok(Value::Null);
    }
    if path.is_none() {
        let json = convert_dbtype_to_jsonb(value, Conv::Strict)?;
        let element_type = json.is_valid()?;

        return Ok(Value::Text(Text::json(element_type.into())));
    }
    if let Some(path) = json_path_from_owned_value(path.unwrap(), true)? {
        let mut json = convert_dbtype_to_jsonb(value, Conv::Strict)?;

        if let Ok(mut path) = json.navigate_path(&path, PathOperationMode::ReplaceExisting) {
            let target = path.pop().expect("Should exist");
            let element_type = if let Some(el_index) = target.get_array_index() {
                json.element_type_at(el_index)
            } else {
                json.element_type_at(target.field_value_index)
            }?;
            Ok(Value::Text(Text::json(element_type.into())))
        } else {
            Ok(Value::Null)
        }
    } else {
        Ok(Value::Null)
    }
}

fn json_path_from_owned_value(path: &Value, strict: bool) -> crate::Result<Option<JsonPath>> {
    let json_path = if strict {
        match path {
            Value::Text(t) => json_path(t.as_str())?,
            Value::Null => return Ok(None),
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    } else {
        match path {
            Value::Text(t) => {
                if t.as_str().starts_with("$") {
                    json_path(t.as_str())?
                } else {
                    JsonPath {
                        elements: vec![
                            PathElement::Root(),
                            PathElement::Key(Cow::Borrowed(t.as_str()), false),
                        ],
                    }
                }
            }
            Value::Null => return Ok(None),
            Value::Integer(i) => JsonPath {
                elements: vec![
                    PathElement::Root(),
                    PathElement::ArrayLocator(Some(*i as i32)),
                ],
            },
            Value::Float(f) => JsonPath {
                elements: vec![
                    PathElement::Root(),
                    PathElement::Key(Cow::Owned(f.to_string()), false),
                ],
            },
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    };

    Ok(Some(json_path))
}

pub fn json_error_position(json: &Value) -> crate::Result<Value> {
    match json {
        Value::Text(t) => match Jsonb::from_str(t.as_str()) {
            Ok(_) => Ok(Value::Integer(0)),
            Err(JsonError::Message { location, .. }) => {
                if let Some(loc) = location {
                    let one_indexed = loc + 1;
                    Ok(Value::Integer(one_indexed as i64))
                } else {
                    Err(crate::error::LimboError::InternalError(
                        "failed to determine json error position".into(),
                    ))
                }
            }
        },
        Value::Blob(_) => {
            bail_parse_error!("Unsupported")
        }
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Integer(0)),
    }
}

/// Constructs a JSON object from a list of values that represent key-value pairs.
/// The number of values must be even, and the first value of each pair (which represents the map key)
/// must be a TEXT value. The second value of each pair can be any JSON value (which represents the map value)
pub fn json_object(values: &[Register]) -> crate::Result<Value> {
    if values.len() % 2 != 0 {
        bail_constraint_error!("json_object() requires an even number of arguments")
    }
    let mut json = Jsonb::make_empty_obj(values.len() * 50);

    for chunk in values.chunks_exact(2) {
        if chunk[0].get_owned_value().value_type() != ValueType::Text {
            bail_constraint_error!("json_object() labels must be TEXT")
        }
        let key = convert_dbtype_to_jsonb(chunk[0].get_owned_value(), Conv::ToString)?;
        json.append_jsonb_to_end(key.data());
        let value = convert_dbtype_to_jsonb(chunk[1].get_owned_value(), Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }

    json.finalize_unsafe(ElementType::OBJECT)?;

    json_string_to_db_type(json, ElementType::OBJECT, OutputVariant::String)
}

pub fn jsonb_object(values: &[Register]) -> crate::Result<Value> {
    if values.len() % 2 != 0 {
        bail_constraint_error!("json_object() requires an even number of arguments")
    }
    let mut json = Jsonb::make_empty_obj(values.len() * 50);

    for chunk in values.chunks_exact(2) {
        if chunk[0].get_owned_value().value_type() != ValueType::Text {
            bail_constraint_error!("json_object() labels must be TEXT")
        }
        let key = convert_dbtype_to_jsonb(chunk[0].get_owned_value(), Conv::ToString)?;
        json.append_jsonb_to_end(key.data());
        let value = convert_dbtype_to_jsonb(chunk[1].get_owned_value(), Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }

    json.finalize_unsafe(ElementType::OBJECT)?;

    json_string_to_db_type(json, ElementType::OBJECT, OutputVariant::Binary)
}

pub fn is_json_valid(json_value: &Value) -> Value {
    if matches!(json_value, Value::Null) {
        return Value::Null;
    }
    convert_dbtype_to_jsonb(json_value, Conv::Strict)
        .map(|_| Value::Integer(1))
        .unwrap_or(Value::Integer(0))
}

pub fn json_quote(value: &Value) -> crate::Result<Value> {
    match value {
        Value::Text(ref t) => {
            // If X is a JSON value returned by another JSON function,
            // then this function is a no-op
            if t.subtype == TextSubtype::Json {
                // Should just return the json value with no quotes
                return Ok(value.to_owned());
            }

            let mut escaped_value = String::with_capacity(t.value.len() + 4);
            escaped_value.push('"');

            for c in t.as_str().chars() {
                match c {
                    '"' | '\\' | '\n' | '\r' | '\t' | '\u{0008}' | '\u{000c}' => {
                        escaped_value.push('\\');
                        escaped_value.push(c);
                    }
                    c => escaped_value.push(c),
                }
            }
            escaped_value.push('"');

            Ok(Value::build_text(escaped_value))
        }
        // Numbers are unquoted in json
        Value::Integer(ref int) => Ok(Value::Integer(int.to_owned())),
        Value::Float(ref float) => Ok(Value::Float(float.to_owned())),
        Value::Blob(_) => crate::bail_constraint_error!("JSON cannot hold BLOB values"),
        Value::Null => Ok(Value::build_text("null")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_get_json_valid_json5() {
        let input = Value::build_text("{ key: 'value' }");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_infinity() {
        let input = Value::build_text("{ \"key\": Infinity }");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_negative_infinity() {
        let input = Value::build_text("{ \"key\": -Infinity }");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":-9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_nan() {
        let input = Value::build_text("{ \"key\": NaN }");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":null}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_json5() {
        let input = Value::build_text("{ key: value }");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_valid_jsonb() {
        let input = Value::build_text("{\"key\":\"value\"}");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_jsonb() {
        let input = Value::build_text("{key:\"value\"");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_blob_valid_jsonb() {
        let binary_json = vec![124, 55, 104, 101, 121, 39, 121, 111];
        let input = Value::Blob(binary_json);
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains(r#"{"hey":"yo"}"#));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_blob_invalid_jsonb() {
        let binary_json: Vec<u8> = vec![0xA2, 0x62, 0x6B, 0x31, 0x62, 0x76]; // Incomplete binary JSON
        let input = Value::Blob(binary_json);
        let result = get_json(&input, None);
        println!("{result:?}");
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_non_text() {
        let input = Value::Null;
        let result = get_json(&input, None).unwrap();
        if let Value::Null = result {
            // Test passed
        } else {
            panic!("Expected Value::Null");
        }
    }

    #[test]
    fn test_json_array_simple() {
        let text = Register::Value(Value::build_text("value1"));
        let json = Register::Value(Value::Text(Text::json("\"value2\"".to_string())));
        let input = vec![
            text,
            json,
            Register::Value(Value::Integer(1)),
            Register::Value(Value::Float(1.1)),
        ];

        let result = json_array(&input).unwrap();
        if let Value::Text(res) = result {
            assert_eq!(res.as_str(), "[\"value1\",\"value2\",1,1.1]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_json_array_empty() {
        let input = vec![];

        let result = json_array(&input).unwrap();
        if let Value::Text(res) = result {
            assert_eq!(res.as_str(), "[]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_json_array_blob_invalid() {
        let blob = Register::Value(Value::Blob("1".as_bytes().to_vec()));

        let input = vec![blob];

        let result = json_array(&input);

        match result {
            Ok(_) => panic!("Expected error for blob input"),
            Err(e) => assert!(e.to_string().contains("JSON cannot hold BLOB values")),
        }
    }

    #[test]
    fn test_json_array_length() {
        let input = Value::build_text("[1,2,3,4]");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        if let Value::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected Value::Integer");
        }
    }

    #[test]
    fn test_json_array_length_empty() {
        let input = Value::build_text("[]");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        if let Value::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected Value::Integer");
        }
    }

    #[test]
    fn test_json_array_length_root() {
        let input = Value::build_text("[1,2,3,4]");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, Some(&Value::build_text("$")), &json_cache).unwrap();
        if let Value::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected Value::Integer");
        }
    }

    #[test]
    fn test_json_array_length_not_array() {
        let input = Value::build_text("{one: [1,2,3,4]}");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        if let Value::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected Value::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_prop() {
        let input = Value::build_text("{one: [1,2,3,4]}");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&Value::build_text("$.one")), &json_cache).unwrap();
        if let Value::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected Value::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index() {
        let input = Value::build_text("[[1,2,3,4]]");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&Value::build_text("$[0]")), &json_cache).unwrap();
        if let Value::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected Value::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_not_array() {
        let input = Value::build_text("[1,2,3,4]");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&Value::build_text("$[2]")), &json_cache).unwrap();
        if let Value::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected Value::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_bad_prop() {
        let input = Value::build_text("{one: [1,2,3,4]}");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&Value::build_text("$.two")), &json_cache).unwrap();
        assert_eq!(Value::Null, result);
    }

    #[test]
    fn test_json_array_length_simple_json_subtype() {
        let input = Value::build_text("[1,2,3]");
        let json_cache = JsonCacheCell::new();
        let wrapped = get_json(&input, None).unwrap();
        let result = json_array_length(&wrapped, None, &json_cache).unwrap();

        if let Value::Integer(res) = result {
            assert_eq!(res, 3);
        } else {
            panic!("Expected Value::Integer");
        }
    }

    #[test]
    fn test_json_extract_missing_path() {
        let json_cache = JsonCacheCell::new();
        let result = json_extract(
            &Value::build_text("{\"a\":2}"),
            &[Register::Value(Value::build_text("$.x"))],
            &json_cache,
        );

        match result {
            Ok(Value::Null) => (),
            _ => panic!("Expected null result, got: {result:?}"),
        }
    }
    #[test]
    fn test_json_extract_null_path() {
        let json_cache = JsonCacheCell::new();
        let result = json_extract(
            &Value::build_text("{\"a\":2}"),
            &[Register::Value(Value::Null)],
            &json_cache,
        );

        match result {
            Ok(Value::Null) => (),
            _ => panic!("Expected null result, got: {result:?}"),
        }
    }

    #[test]
    fn test_json_path_invalid() {
        let json_cache = JsonCacheCell::new();
        let result = json_extract(
            &Value::build_text("{\"a\":2}"),
            &[Register::Value(Value::Float(1.1))],
            &json_cache,
        );

        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => assert!(e.to_string().contains("JSON path error")),
        }
    }

    #[test]
    fn test_json_error_position_no_error() {
        let input = Value::build_text("[1,2,3]");
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::Integer(0));
    }

    #[test]
    fn test_json_error_position_no_error_more() {
        let input = Value::build_text(r#"{"a":55,"b":72 , }"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::Integer(0));
    }

    #[test]
    fn test_json_error_position_object() {
        let input = Value::build_text(r#"{"a":55,"b":72,,}"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::Integer(16));
    }

    #[test]
    fn test_json_error_position_array() {
        let input = Value::build_text(r#"["a",55,"b",72,,]"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::Integer(16));
    }

    #[test]
    fn test_json_error_position_null() {
        let input = Value::Null;
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_json_error_position_integer() {
        let input = Value::Integer(5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::Integer(0));
    }

    #[test]
    fn test_json_error_position_float() {
        let input = Value::Float(-5.5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::Integer(0));
    }

    #[test]
    fn test_json_object_simple() {
        let key = Register::Value(Value::build_text("key"));
        let value = Register::Value(Value::build_text("value"));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_json_object_multiple_values() {
        let text_key = Value::build_text("text_key");
        let text_value = Value::build_text("text_value");
        let json_key = Value::build_text("json_key");
        let json_value = Value::Text(Text::json(r#"{"json":"value","number":1}"#.to_string()));
        let integer_key = Value::build_text("integer_key");
        let integer_value = Value::Integer(1);
        let float_key = Value::build_text("float_key");
        let float_value = Value::Float(1.1);
        let null_key = Value::build_text("null_key");
        let null_value = Value::Null;

        let input = vec![
            Register::Value(text_key),
            Register::Value(text_value),
            Register::Value(json_key),
            Register::Value(json_value),
            Register::Value(integer_key),
            Register::Value(integer_value),
            Register::Value(float_key),
            Register::Value(float_value),
            Register::Value(null_key),
            Register::Value(null_value),
        ];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(
            json_text.as_str(),
            r#"{"text_key":"text_value","json_key":{"json":"value","number":1},"integer_key":1,"float_key":1.1,"null_key":null}"#
        );
    }

    #[test]
    fn test_json_object_json_value_is_rendered_as_json() {
        let key = Register::Value(Value::build_text("key"));
        let value = Register::Value(Value::Text(Text::json(r#"{"json":"value"}"#.to_string())));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":{"json":"value"}}"#);
    }

    #[test]
    fn test_json_object_json_text_value_is_rendered_as_regular_text() {
        let key = Register::Value(Value::build_text("key"));
        let value = Register::Value(Value::Text(Text::new(r#"{"json":"value"}"#)));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"{\"json\":\"value\"}"}"#);
    }

    #[test]
    fn test_json_object_nested() {
        let key = Register::Value(Value::build_text("key"));
        let value = Register::Value(Value::build_text("value"));
        let input = vec![key, value];

        let parent_key = Register::Value(Value::build_text("parent_key"));
        let parent_value = Register::Value(json_object(&input).unwrap());
        let parent_input = vec![parent_key, parent_value];

        let result = json_object(&parent_input).unwrap();

        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"parent_key":{"key":"value"}}"#);
    }

    #[test]
    fn test_json_object_duplicated_keys() {
        let key = Register::Value(Value::build_text("key"));
        let value = Register::Value(Value::build_text("value"));
        let input = vec![key.clone(), value.clone(), key, value];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value","key":"value"}"#);
    }

    #[test]
    fn test_json_object_empty() {
        let input = vec![];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{}"#);
    }

    #[test]
    fn test_json_object_non_text_key() {
        let key = Register::Value(Value::Integer(1));
        let value = Register::Value(Value::build_text("value"));
        let input = vec![key, value];

        match json_object(&input) {
            Ok(_) => panic!("Expected error for non-TEXT key"),
            Err(e) => assert!(e.to_string().contains("labels must be TEXT")),
        }
    }

    #[test]
    fn test_json_odd_number_of_values() {
        let key = Register::Value(Value::build_text("key"));
        let value = Register::Value(Value::build_text("value"));
        let input = vec![key.clone(), value, key];

        assert!(json_object(&input).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_root_strict() {
        let path = Value::Text(Text::new("$"));

        let result = json_path_from_owned_value(&path, true);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match result.elements[..] {
            [PathElement::Root()] => {}
            _ => panic!("Expected root"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_root_non_strict() {
        let path = Value::Text(Text::new("$"));

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match result.elements[..] {
            [PathElement::Root()] => {}
            _ => panic!("Expected root"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_named_strict() {
        let path = Value::Text(Text::new("field"));

        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_named_non_strict() {
        let path = Value::Text(Text::new("field"));

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::Key(field, false)] if *field == "field" => {}
            _ => panic!("Expected root and field"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_integer_strict() {
        let path = Value::Integer(3);
        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_integer_non_strict() {
        let path = Value::Integer(3);

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::ArrayLocator(index)] if *index == Some(3) => {}
            _ => panic!("Expected root and array locator"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_null_strict() {
        let path = Value::Null;

        let result = json_path_from_owned_value(&path, true);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_owned_value_null_non_strict() {
        let path = Value::Null;

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_owned_value_float_strict() {
        let path = Value::Float(1.23);

        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_float_non_strict() {
        let path = Value::Float(1.23);

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::Key(field, false)] if *field == "1.23" => {}
            _ => panic!("Expected root and field"),
        }
    }

    #[test]
    fn test_json_set_field_empty_object() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("{}")),
                Register::Value(Value::build_text("$.field")),
                Register::Value(Value::build_text("value")),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), r#"{"field":"value"}"#);
    }

    #[test]
    fn test_json_set_replace_field() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text(r#"{"field":"old_value"}"#)),
                Register::Value(Value::build_text("$.field")),
                Register::Value(Value::build_text("new_value")),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap().to_text().unwrap(),
            r#"{"field":"new_value"}"#
        );
    }

    #[test]
    fn test_json_set_set_deeply_nested_key() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("{}")),
                Register::Value(Value::build_text("$.object.doesnt.exist")),
                Register::Value(Value::build_text("value")),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap().to_text().unwrap(),
            r#"{"object":{"doesnt":{"exist":"value"}}}"#
        );
    }

    #[test]
    fn test_json_set_add_value_to_empty_array() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("[]")),
                Register::Value(Value::build_text("$[0]")),
                Register::Value(Value::build_text("value")),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), r#"["value"]"#);
    }

    #[test]
    fn test_json_set_add_value_to_nonexistent_array() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("{}")),
                Register::Value(Value::build_text("$.some_array[0]")),
                Register::Value(Value::Integer(123)),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap().to_text().unwrap(),
            r#"{"some_array":[123]}"#
        );
    }

    #[test]
    fn test_json_set_add_value_to_array() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("[123]")),
                Register::Value(Value::build_text("$[1]")),
                Register::Value(Value::Integer(456)),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "[123,456]");
    }

    #[test]
    fn test_json_set_add_value_to_array_out_of_bounds() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("[123]")),
                Register::Value(Value::build_text("$[200]")),
                Register::Value(Value::Integer(456)),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "[123]");
    }

    #[test]
    fn test_json_set_replace_value_in_array() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("[123]")),
                Register::Value(Value::build_text("$[0]")),
                Register::Value(Value::Integer(456)),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "[456]");
    }

    #[test]
    fn test_json_set_null_path() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("{}")),
                Register::Value(Value::Null),
                Register::Value(Value::Integer(456)),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "{}");
    }

    #[test]
    fn test_json_set_multiple_keys() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("[123]")),
                Register::Value(Value::build_text("$[0]")),
                Register::Value(Value::Integer(456)),
                Register::Value(Value::build_text("$[1]")),
                Register::Value(Value::Integer(789)),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "[456,789]");
    }

    #[test]
    fn test_json_set_add_array_in_nested_object() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("{}")),
                Register::Value(Value::build_text("$.object[0].field")),
                Register::Value(Value::Integer(123)),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap().to_text().unwrap(),
            r#"{"object":[{"field":123}]}"#
        );
    }

    #[test]
    fn test_json_set_add_array_in_array_in_nested_object() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("{}")),
                Register::Value(Value::build_text("$.object[0][0]")),
                Register::Value(Value::Integer(123)),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), r#"{"object":[[123]]}"#);
    }

    #[test]
    fn test_json_set_add_array_in_array_in_nested_object_out_of_bounds() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Register::Value(Value::build_text("{}")),
                Register::Value(Value::build_text("$.object[123].another")),
                Register::Value(Value::build_text("value")),
                Register::Value(Value::build_text("$.field")),
                Register::Value(Value::build_text("value")),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), r#"{"field":"value"}"#,);
    }
}
