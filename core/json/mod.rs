mod de;
mod error;
mod json_cache;
mod json_operations;
mod json_path;
mod jsonb;
mod ser;

use crate::bail_constraint_error;
pub use crate::json::de::from_str;
use crate::json::error::Error as JsonError;
pub use crate::json::json_operations::{
    json_insert, json_patch, json_remove, json_replace, jsonb_insert, jsonb_remove, jsonb_replace,
};
use crate::json::json_path::{json_path, JsonPath, PathElement};
pub use crate::json::ser::to_string;
use crate::types::{OwnedValue, OwnedValueType, Text, TextSubtype};
use crate::vdbe::Register;
use crate::{bail_parse_error, json::de::ordered_object};
pub use json_cache::JsonCacheCell;
use jsonb::{ElementType, Jsonb, JsonbHeader, PathOperationMode, SearchOperation, SetOperation};
use ser::to_string_pretty;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum Val {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Array(Vec<Val>),
    Removed,
    #[serde(with = "ordered_object")]
    Object(Vec<(String, Val)>),
}

#[derive(Debug, Clone, Copy)]
pub enum Conv {
    Strict,
    NotStrict,
    ToString,
}

enum OutputVariant {
    ElementType,
    Binary,
    String,
}

pub fn get_json(json_value: &OwnedValue, indent: Option<&str>) -> crate::Result<OwnedValue> {
    match json_value {
        OwnedValue::Text(ref t) => {
            // optimization: once we know the subtype is a valid JSON, we do not have
            // to go through parsing JSON and serializing it back to string
            if t.subtype == TextSubtype::Json {
                return Ok(json_value.to_owned());
            }

            let json_val = get_json_value(json_value)?;
            let json = match indent {
                Some(indent) => to_string_pretty(&json_val, indent)?,
                None => to_string(&json_val)?,
            };

            Ok(OwnedValue::Text(Text::json(json)))
        }
        OwnedValue::Blob(b) => {
            let jsonbin = Jsonb::new(b.len(), Some(b));
            jsonbin.is_valid()?;
            Ok(OwnedValue::Text(Text {
                value: Arc::new(jsonbin.to_string()?.into_bytes()),
                subtype: TextSubtype::Json,
            }))
        }
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => {
            let json_val = get_json_value(json_value)?;
            let json = match indent {
                Some(indent) => to_string_pretty(&json_val, indent)?,
                None => to_string(&json_val)?,
            };

            Ok(OwnedValue::Text(Text::json(json)))
        }
    }
}

pub fn jsonb(json_value: &OwnedValue, cache: &JsonCacheCell) -> crate::Result<OwnedValue> {
    let json_conv_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);

    let jsonbin = cache.get_or_insert_with(json_value, json_conv_fn);
    match jsonbin {
        Ok(jsonbin) => Ok(OwnedValue::Blob(Arc::new(jsonbin.data()))),
        Err(_) => {
            bail_parse_error!("malformed JSON")
        }
    }
}

fn convert_dbtype_to_jsonb(val: &OwnedValue, strict: Conv) -> crate::Result<Jsonb> {
    match val {
        OwnedValue::Text(text) => {
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
            res
        }
        OwnedValue::Blob(blob) => {
            let json = Jsonb::from_raw_data(blob);
            json.is_valid()?;
            Ok(json)
        }
        OwnedValue::Null => Ok(Jsonb::from_raw_data(
            JsonbHeader::make_null().into_bytes().as_bytes(),
        )),
        OwnedValue::Float(float) => {
            let mut buff = ryu::Buffer::new();
            Jsonb::from_str(buff.format(*float))
        }
        OwnedValue::Integer(int) => Jsonb::from_str(&int.to_string()),
    }
}

pub fn curry_convert_dbtype_to_jsonb(strict: Conv) -> impl Fn(&OwnedValue) -> crate::Result<Jsonb> {
    move |val| convert_dbtype_to_jsonb(val, strict)
}

fn get_json_value(json_value: &OwnedValue) -> crate::Result<Val> {
    match json_value {
        OwnedValue::Text(ref t) => match from_str::<Val>(t.as_str()) {
            Ok(json) => Ok(json),
            Err(_) => {
                crate::bail_parse_error!("malformed JSON")
            }
        },
        OwnedValue::Blob(_) => {
            crate::bail_parse_error!("malformed JSON");
        }
        OwnedValue::Null => Ok(Val::Null),
        OwnedValue::Float(f) => Ok(Val::Float(*f)),
        OwnedValue::Integer(i) => Ok(Val::Integer(*i)),
    }
}

pub fn json_array(values: &[Register]) -> crate::Result<OwnedValue> {
    let mut json = Jsonb::make_empty_array(values.len());

    for value in values.iter() {
        if matches!(value.get_owned_value(), OwnedValue::Blob(_)) {
            crate::bail_constraint_error!("JSON cannot hold BLOB values")
        }
        let value = convert_dbtype_to_jsonb(value.get_owned_value(), Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }
    json.finalize_unsafe(ElementType::ARRAY)?;

    json_string_to_db_type(json, ElementType::ARRAY, OutputVariant::ElementType)
}

pub fn jsonb_array(values: &[Register]) -> crate::Result<OwnedValue> {
    let mut json = Jsonb::make_empty_array(values.len());

    for value in values.iter() {
        if matches!(value.get_owned_value(), OwnedValue::Blob(_)) {
            crate::bail_constraint_error!("JSON cannot hold BLOB values")
        }
        let value = convert_dbtype_to_jsonb(value.get_owned_value(), Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }
    json.finalize_unsafe(ElementType::ARRAY)?;

    json_string_to_db_type(json, ElementType::ARRAY, OutputVariant::Binary)
}

pub fn json_array_length(
    value: &OwnedValue,
    path: Option<&OwnedValue>,
    json_cache: &JsonCacheCell,
) -> crate::Result<OwnedValue> {
    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(value, make_jsonb_fn)?;

    if path.is_none() {
        let len = json.array_len()?;
        return Ok(OwnedValue::Integer(len as i64));
    }

    let path = json_path_from_owned_value(path.expect("We already checked none"), true)?;

    if let Some(path) = path {
        let mut op = SearchOperation::new(json.len() / 2);
        let _ = json.operate_on_path(&path, &mut op);
        if let Ok(len) = op.result().array_len() {
            return Ok(OwnedValue::Integer(len as i64));
        }
    }
    Ok(OwnedValue::Null)
}

pub fn json_set(args: &[Register], json_cache: &JsonCacheCell) -> crate::Result<OwnedValue> {
    if args.is_empty() {
        return Ok(OwnedValue::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(&args[0].get_owned_value(), make_jsonb_fn)?;
    let other = args[1..].chunks_exact(2);

    for chunk in other {
        let path = json_path_from_owned_value(&chunk[0].get_owned_value(), true)?;

        let value = convert_dbtype_to_jsonb(&chunk[1].get_owned_value(), Conv::NotStrict)?;
        let mut op = SetOperation::new(value);
        if let Some(path) = path {
            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.is_valid()?;

    json_string_to_db_type(json, el_type, OutputVariant::String)
}

/// Implements the -> operator. Always returns a proper JSON value.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_extract(
    value: &OwnedValue,
    path: &OwnedValue,
    json_cache: &JsonCacheCell,
) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    if let Some(path) = json_path_from_owned_value(path, false)? {
        let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
        let mut json = json_cache.get_or_insert_with(value, make_jsonb_fn)?;
        let mut op = SearchOperation::new(json.len());
        let res = json.operate_on_path(&path, &mut op);
        let extracted = op.result();
        if res.is_ok() {
            Ok(OwnedValue::Text(Text::json(extracted.to_string()?)))
        } else {
            Ok(OwnedValue::Null)
        }
    } else {
        Ok(OwnedValue::Null)
    }
}

/// Implements the ->> operator. Always returns a SQL representation of the JSON subcomponent.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_shift_extract(
    value: &OwnedValue,
    path: &OwnedValue,
    json_cache: &JsonCacheCell,
) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }
    if let Some(path) = json_path_from_owned_value(path, false)? {
        let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
        let mut json = json_cache.get_or_insert_with(value, make_jsonb_fn)?;
        let mut op = SearchOperation::new(json.len());
        let res = json.operate_on_path(&path, &mut op);
        let extracted = op.result();
        let element_type = match extracted.is_valid() {
            Err(_) => return Ok(OwnedValue::Null),
            Ok(el) => el,
        };

        if res.is_ok() {
            Ok(json_string_to_db_type(
                extracted,
                element_type,
                OutputVariant::ElementType,
            )?)
        } else {
            Ok(OwnedValue::Null)
        }
    } else {
        Ok(OwnedValue::Null)
    }
}

/// Extracts a JSON value from a JSON object or array.
/// If there's only a single path, the return value might be either a TEXT or a database type.
/// https://sqlite.org/json1.html#the_json_extract_function
pub fn json_extract(
    value: &OwnedValue,
    paths: &[Register],
    json_cache: &JsonCacheCell,
) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    if paths.is_empty() {
        return Ok(OwnedValue::Null);
    }
    let convert_to_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let jsonb = json_cache.get_or_insert_with(value, convert_to_jsonb)?;
    let (json, element_type) = jsonb_extract_internal(jsonb, paths)?;

    let result = json_string_to_db_type(json, element_type, OutputVariant::ElementType)?;

    Ok(result)
}

pub fn jsonb_extract(
    value: &OwnedValue,
    paths: &[Register],
    json_cache: &JsonCacheCell,
) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    if paths.is_empty() {
        return Ok(OwnedValue::Null);
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
        if let Some(path) = json_path_from_owned_value(&paths[0].get_owned_value(), true)? {
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

fn json_string_to_db_type(
    json: Jsonb,
    element_type: ElementType,
    flag: OutputVariant,
) -> crate::Result<OwnedValue> {
    let mut json_string = json.to_string()?;
    if matches!(flag, OutputVariant::Binary) {
        return Ok(OwnedValue::Blob(Arc::new(json.data())));
    }
    match element_type {
        ElementType::ARRAY | ElementType::OBJECT => Ok(OwnedValue::Text(Text::json(json_string))),
        ElementType::TEXT | ElementType::TEXT5 | ElementType::TEXTJ | ElementType::TEXTRAW => {
            if matches!(flag, OutputVariant::ElementType) {
                json_string.remove(json_string.len() - 1);
                json_string.remove(0);
                Ok(OwnedValue::Text(Text {
                    value: Arc::new(json_string.into_bytes()),
                    subtype: TextSubtype::Json,
                }))
            } else {
                Ok(OwnedValue::Text(Text {
                    value: Arc::new(json_string.into_bytes()),
                    subtype: TextSubtype::Text,
                }))
            }
        }
        ElementType::FLOAT5 | ElementType::FLOAT => Ok(OwnedValue::Float(
            json_string.parse().expect("Should be valid f64"),
        )),
        ElementType::INT | ElementType::INT5 => {
            let result = i64::from_str(&json_string);
            if let Ok(int) = result {
                Ok(OwnedValue::Integer(int))
            } else {
                let res = f64::from_str(&json_string);
                match res {
                    Ok(num) => Ok(OwnedValue::Float(num)),
                    Err(_) => Ok(OwnedValue::Null),
                }
            }
        }
        ElementType::TRUE => Ok(OwnedValue::Integer(1)),
        ElementType::FALSE => Ok(OwnedValue::Integer(0)),
        ElementType::NULL => Ok(OwnedValue::Null),
        _ => unreachable!(),
    }
}

/// Returns a value with type defined by SQLite documentation:
///   > the SQL datatype of the result is NULL for a JSON null,
///   > INTEGER or REAL for a JSON numeric value,
///   > an INTEGER zero for a JSON false value,
///   > an INTEGER one for a JSON true value,
///   > the dequoted text for a JSON string value,
///   > and a text representation for JSON object and array values.
///
/// https://sqlite.org/json1.html#the_json_extract_function
///
/// *all_as_db* - if true, objects and arrays will be returned as pure TEXT without the JSON subtype
fn convert_json_to_db_type(extracted: &Val, all_as_db: bool) -> crate::Result<OwnedValue> {
    match extracted {
        Val::Removed => Ok(OwnedValue::Null),
        Val::Null => Ok(OwnedValue::Null),
        Val::Float(f) => Ok(OwnedValue::Float(*f)),
        Val::Integer(i) => Ok(OwnedValue::Integer(*i)),
        Val::Bool(b) => {
            if *b {
                Ok(OwnedValue::Integer(1))
            } else {
                Ok(OwnedValue::Integer(0))
            }
        }
        Val::String(s) => Ok(OwnedValue::Text(Text::from_str(s))),
        _ => {
            let json = to_string(&extracted)?;
            if all_as_db {
                Ok(OwnedValue::build_text(&json))
            } else {
                Ok(OwnedValue::Text(Text::json(json)))
            }
        }
    }
}

pub fn json_type(value: &OwnedValue, path: Option<&OwnedValue>) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }
    if path.is_none() {
        let json = convert_dbtype_to_jsonb(value, Conv::Strict)?;
        let element_type = json.is_valid()?;

        return Ok(OwnedValue::Text(Text::json(element_type.into())));
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
            Ok(OwnedValue::Text(Text::json(element_type.into())))
        } else {
            Ok(OwnedValue::Null)
        }
    } else {
        Ok(OwnedValue::Null)
    }
}

fn json_path_from_owned_value(path: &OwnedValue, strict: bool) -> crate::Result<Option<JsonPath>> {
    let json_path = if strict {
        match path {
            OwnedValue::Text(t) => json_path(t.as_str())?,
            OwnedValue::Null => return Ok(None),
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    } else {
        match path {
            OwnedValue::Text(t) => {
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
            OwnedValue::Null => return Ok(None),
            OwnedValue::Integer(i) => JsonPath {
                elements: vec![
                    PathElement::Root(),
                    PathElement::ArrayLocator(Some(*i as i32)),
                ],
            },
            OwnedValue::Float(f) => JsonPath {
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

pub fn json_error_position(json: &OwnedValue) -> crate::Result<OwnedValue> {
    match json {
        OwnedValue::Text(t) => match from_str::<Val>(t.as_str()) {
            Ok(_) => Ok(OwnedValue::Integer(0)),
            Err(JsonError::Message { location, .. }) => {
                if let Some(loc) = location {
                    Ok(OwnedValue::Integer(loc.column as i64))
                } else {
                    Err(crate::error::LimboError::InternalError(
                        "failed to determine json error position".into(),
                    ))
                }
            }
        },
        OwnedValue::Blob(_) => {
            bail_parse_error!("Unsupported")
        }
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => Ok(OwnedValue::Integer(0)),
    }
}

/// Constructs a JSON object from a list of values that represent key-value pairs.
/// The number of values must be even, and the first value of each pair (which represents the map key)
/// must be a TEXT value. The second value of each pair can be any JSON value (which represents the map value)
pub fn json_object(values: &[Register]) -> crate::Result<OwnedValue> {
    if values.len() % 2 != 0 {
        bail_constraint_error!("json_object() requires an even number of arguments")
    }
    let mut json = Jsonb::make_empty_obj(values.len() * 50);

    for chunk in values.chunks_exact(2) {
        if chunk[0].get_owned_value().value_type() != OwnedValueType::Text {
            bail_constraint_error!("json_object() labels must be TEXT")
        }
        let key = convert_dbtype_to_jsonb(&chunk[0].get_owned_value(), Conv::ToString)?;
        json.append_jsonb_to_end(key.data());
        let value = convert_dbtype_to_jsonb(&chunk[1].get_owned_value(), Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }

    json.finalize_unsafe(ElementType::OBJECT)?;

    json_string_to_db_type(json, ElementType::OBJECT, OutputVariant::String)
}

pub fn jsonb_object(values: &[Register]) -> crate::Result<OwnedValue> {
    if values.len() % 2 != 0 {
        bail_constraint_error!("json_object() requires an even number of arguments")
    }
    let mut json = Jsonb::make_empty_obj(values.len() * 50);

    for chunk in values.chunks_exact(2) {
        if chunk[0].get_owned_value().value_type() != OwnedValueType::Text {
            bail_constraint_error!("json_object() labels must be TEXT")
        }
        let key = convert_dbtype_to_jsonb(&chunk[0].get_owned_value(), Conv::ToString)?;
        json.append_jsonb_to_end(key.data());
        let value = convert_dbtype_to_jsonb(&chunk[1].get_owned_value(), Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }

    json.finalize_unsafe(ElementType::OBJECT)?;

    json_string_to_db_type(json, ElementType::OBJECT, OutputVariant::Binary)
}

pub fn is_json_valid(json_value: &OwnedValue) -> OwnedValue {
    if matches!(json_value, OwnedValue::Null) {
        return OwnedValue::Null;
    }
    convert_dbtype_to_jsonb(json_value, Conv::Strict)
        .map(|_| OwnedValue::Integer(1))
        .unwrap_or(OwnedValue::Integer(0))
}

pub fn json_quote(value: &OwnedValue) -> crate::Result<OwnedValue> {
    match value {
        OwnedValue::Text(ref t) => {
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

            Ok(OwnedValue::build_text(&escaped_value))
        }
        // Numbers are unquoted in json
        OwnedValue::Integer(ref int) => Ok(OwnedValue::Integer(int.to_owned())),
        OwnedValue::Float(ref float) => Ok(OwnedValue::Float(float.to_owned())),
        OwnedValue::Blob(_) => crate::bail_constraint_error!("JSON cannot hold BLOB values"),
        OwnedValue::Null => Ok(OwnedValue::build_text("null")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OwnedValue;

    #[test]
    fn test_get_json_valid_json5() {
        let input = OwnedValue::build_text("{ key: 'value' }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_double_single_quotes() {
        let input = OwnedValue::build_text("{ key: ''value'' }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_infinity() {
        let input = OwnedValue::build_text("{ \"key\": Infinity }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_negative_infinity() {
        let input = OwnedValue::build_text("{ \"key\": -Infinity }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":-9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_nan() {
        let input = OwnedValue::build_text("{ \"key\": NaN }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":null}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_json5() {
        let input = OwnedValue::build_text("{ key: value }");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_valid_jsonb() {
        let input = OwnedValue::build_text("{\"key\":\"value\"}");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_jsonb() {
        let input = OwnedValue::build_text("{key:\"value\"");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_blob_valid_jsonb() {
        let binary_json = vec![124, 55, 104, 101, 121, 39, 121, 111];
        let input = OwnedValue::Blob(Arc::new(binary_json));
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains(r#"{"hey":"yo"}"#));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_blob_invalid_jsonb() {
        let binary_json: Vec<u8> = vec![0xA2, 0x62, 0x6B, 0x31, 0x62, 0x76]; // Incomplete binary JSON
        let input = OwnedValue::Blob(Arc::new(binary_json));
        let result = get_json(&input, None);
        println!("{:?}", result);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_non_text() {
        let input = OwnedValue::Null;
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Null = result {
            // Test passed
        } else {
            panic!("Expected OwnedValue::Null");
        }
    }

    #[test]
    fn test_json_array_simple() {
        let text = Register::OwnedValue(OwnedValue::build_text("value1"));
        let json = Register::OwnedValue(OwnedValue::Text(Text::json("\"value2\"".to_string())));
        let input = vec![
            text,
            json,
            Register::OwnedValue(OwnedValue::Integer(1)),
            Register::OwnedValue(OwnedValue::Float(1.1)),
        ];

        let result = json_array(&input).unwrap();
        if let OwnedValue::Text(res) = result {
            assert_eq!(res.as_str(), "[\"value1\",\"value2\",1,1.1]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_json_array_empty() {
        let input = vec![];

        let result = json_array(&input).unwrap();
        if let OwnedValue::Text(res) = result {
            assert_eq!(res.as_str(), "[]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_json_array_blob_invalid() {
        let blob = Register::OwnedValue(OwnedValue::Blob(Arc::new("1".as_bytes().to_vec())));

        let input = vec![blob];

        let result = json_array(&input);

        match result {
            Ok(_) => panic!("Expected error for blob input"),
            Err(e) => assert!(e.to_string().contains("JSON cannot hold BLOB values")),
        }
    }

    #[test]
    fn test_json_array_length() {
        let input = OwnedValue::build_text("[1,2,3,4]");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_empty() {
        let input = OwnedValue::build_text("[]");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_root() {
        let input = OwnedValue::build_text("[1,2,3,4]");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&OwnedValue::build_text("$")), &json_cache).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_not_array() {
        let input = OwnedValue::build_text("{one: [1,2,3,4]}");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_prop() {
        let input = OwnedValue::build_text("{one: [1,2,3,4]}");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&OwnedValue::build_text("$.one")), &json_cache).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index() {
        let input = OwnedValue::build_text("[[1,2,3,4]]");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&OwnedValue::build_text("$[0]")), &json_cache).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_not_array() {
        let input = OwnedValue::build_text("[1,2,3,4]");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&OwnedValue::build_text("$[2]")), &json_cache).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_bad_prop() {
        let input = OwnedValue::build_text("{one: [1,2,3,4]}");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&OwnedValue::build_text("$.two")), &json_cache).unwrap();
        assert_eq!(OwnedValue::Null, result);
    }

    #[test]
    fn test_json_array_length_simple_json_subtype() {
        let input = OwnedValue::build_text("[1,2,3]");
        let json_cache = JsonCacheCell::new();
        let wrapped = get_json(&input, None).unwrap();
        let result = json_array_length(&wrapped, None, &json_cache).unwrap();

        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 3);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_extract_missing_path() {
        let json_cache = JsonCacheCell::new();
        let result = json_extract(
            &OwnedValue::build_text("{\"a\":2}"),
            &[Register::OwnedValue(OwnedValue::build_text("$.x"))],
            &json_cache,
        );

        match result {
            Ok(OwnedValue::Null) => (),
            _ => panic!("Expected null result, got: {:?}", result),
        }
    }
    #[test]
    fn test_json_extract_null_path() {
        let json_cache = JsonCacheCell::new();
        let result = json_extract(
            &OwnedValue::build_text("{\"a\":2}"),
            &[Register::OwnedValue(OwnedValue::Null)],
            &json_cache,
        );

        match result {
            Ok(OwnedValue::Null) => (),
            _ => panic!("Expected null result, got: {:?}", result),
        }
    }

    #[test]
    fn test_json_path_invalid() {
        let json_cache = JsonCacheCell::new();
        let result = json_extract(
            &OwnedValue::build_text("{\"a\":2}"),
            &[Register::OwnedValue(OwnedValue::Float(1.1))],
            &json_cache,
        );

        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => assert!(e.to_string().contains("JSON path error")),
        }
    }

    #[test]
    fn test_json_error_position_no_error() {
        let input = OwnedValue::build_text("[1,2,3]");
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_no_error_more() {
        let input = OwnedValue::build_text(r#"{"a":55,"b":72 , }"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_object() {
        let input = OwnedValue::build_text(r#"{"a":55,"b":72,,}"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(16));
    }

    #[test]
    fn test_json_error_position_array() {
        let input = OwnedValue::build_text(r#"["a",55,"b",72,,]"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(16));
    }

    #[test]
    fn test_json_error_position_null() {
        let input = OwnedValue::Null;
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Null);
    }

    #[test]
    fn test_json_error_position_integer() {
        let input = OwnedValue::Integer(5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_float() {
        let input = OwnedValue::Float(-5.5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_object_simple() {
        let key = Register::OwnedValue(OwnedValue::build_text("key"));
        let value = Register::OwnedValue(OwnedValue::build_text("value"));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_json_object_multiple_values() {
        let text_key = OwnedValue::build_text("text_key");
        let text_value = OwnedValue::build_text("text_value");
        let json_key = OwnedValue::build_text("json_key");
        let json_value = OwnedValue::Text(Text::json(r#"{"json":"value","number":1}"#.to_string()));
        let integer_key = OwnedValue::build_text("integer_key");
        let integer_value = OwnedValue::Integer(1);
        let float_key = OwnedValue::build_text("float_key");
        let float_value = OwnedValue::Float(1.1);
        let null_key = OwnedValue::build_text("null_key");
        let null_value = OwnedValue::Null;

        let input = vec![
            Register::OwnedValue(text_key),
            Register::OwnedValue(text_value),
            Register::OwnedValue(json_key),
            Register::OwnedValue(json_value),
            Register::OwnedValue(integer_key),
            Register::OwnedValue(integer_value),
            Register::OwnedValue(float_key),
            Register::OwnedValue(float_value),
            Register::OwnedValue(null_key),
            Register::OwnedValue(null_value),
        ];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(
            json_text.as_str(),
            r#"{"text_key":"text_value","json_key":{"json":"value","number":1},"integer_key":1,"float_key":1.1,"null_key":null}"#
        );
    }

    #[test]
    fn test_json_object_json_value_is_rendered_as_json() {
        let key = Register::OwnedValue(OwnedValue::build_text("key"));
        let value = Register::OwnedValue(OwnedValue::Text(Text::json(
            r#"{"json":"value"}"#.to_string(),
        )));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":{"json":"value"}}"#);
    }

    #[test]
    fn test_json_object_json_text_value_is_rendered_as_regular_text() {
        let key = Register::OwnedValue(OwnedValue::build_text("key"));
        let value = Register::OwnedValue(OwnedValue::Text(Text::new(r#"{"json":"value"}"#)));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"{\"json\":\"value\"}"}"#);
    }

    #[test]
    fn test_json_object_nested() {
        let key = Register::OwnedValue(OwnedValue::build_text("key"));
        let value = Register::OwnedValue(OwnedValue::build_text("value"));
        let input = vec![key, value];

        let parent_key = Register::OwnedValue(OwnedValue::build_text("parent_key"));
        let parent_value = Register::OwnedValue(json_object(&input).unwrap());
        let parent_input = vec![parent_key, parent_value];

        let result = json_object(&parent_input).unwrap();

        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"parent_key":{"key":"value"}}"#);
    }

    #[test]
    fn test_json_object_duplicated_keys() {
        let key = Register::OwnedValue(OwnedValue::build_text("key"));
        let value = Register::OwnedValue(OwnedValue::build_text("value"));
        let input = vec![key.clone(), value.clone(), key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value","key":"value"}"#);
    }

    #[test]
    fn test_json_object_empty() {
        let input = vec![];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{}"#);
    }

    #[test]
    fn test_json_object_non_text_key() {
        let key = Register::OwnedValue(OwnedValue::Integer(1));
        let value = Register::OwnedValue(OwnedValue::build_text("value"));
        let input = vec![key, value];

        match json_object(&input) {
            Ok(_) => panic!("Expected error for non-TEXT key"),
            Err(e) => assert!(e.to_string().contains("labels must be TEXT")),
        }
    }

    #[test]
    fn test_json_odd_number_of_values() {
        let key = Register::OwnedValue(OwnedValue::build_text("key"));
        let value = Register::OwnedValue(OwnedValue::build_text("value"));
        let input = vec![key.clone(), value, key];

        assert!(json_object(&input).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_root_strict() {
        let path = OwnedValue::Text(Text::new("$"));

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
        let path = OwnedValue::Text(Text::new("$"));

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
        let path = OwnedValue::Text(Text::new("field"));

        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_named_non_strict() {
        let path = OwnedValue::Text(Text::new("field"));

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
        let path = OwnedValue::Integer(3);
        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_integer_non_strict() {
        let path = OwnedValue::Integer(3);

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
        let path = OwnedValue::Null;

        let result = json_path_from_owned_value(&path, true);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_owned_value_null_non_strict() {
        let path = OwnedValue::Null;

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_owned_value_float_strict() {
        let path = OwnedValue::Float(1.23);

        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_float_non_strict() {
        let path = OwnedValue::Float(1.23);

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
                Register::OwnedValue(OwnedValue::build_text("{}")),
                Register::OwnedValue(OwnedValue::build_text("$.field")),
                Register::OwnedValue(OwnedValue::build_text("value")),
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
                Register::OwnedValue(OwnedValue::build_text(r#"{"field":"old_value"}"#)),
                Register::OwnedValue(OwnedValue::build_text("$.field")),
                Register::OwnedValue(OwnedValue::build_text("new_value")),
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
                Register::OwnedValue(OwnedValue::build_text("{}")),
                Register::OwnedValue(OwnedValue::build_text("$.object.doesnt.exist")),
                Register::OwnedValue(OwnedValue::build_text("value")),
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
                Register::OwnedValue(OwnedValue::build_text("[]")),
                Register::OwnedValue(OwnedValue::build_text("$[0]")),
                Register::OwnedValue(OwnedValue::build_text("value")),
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
                Register::OwnedValue(OwnedValue::build_text("{}")),
                Register::OwnedValue(OwnedValue::build_text("$.some_array[0]")),
                Register::OwnedValue(OwnedValue::Integer(123)),
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
                Register::OwnedValue(OwnedValue::build_text("[123]")),
                Register::OwnedValue(OwnedValue::build_text("$[1]")),
                Register::OwnedValue(OwnedValue::Integer(456)),
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
                Register::OwnedValue(OwnedValue::build_text("[123]")),
                Register::OwnedValue(OwnedValue::build_text("$[200]")),
                Register::OwnedValue(OwnedValue::Integer(456)),
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
                Register::OwnedValue(OwnedValue::build_text("[123]")),
                Register::OwnedValue(OwnedValue::build_text("$[0]")),
                Register::OwnedValue(OwnedValue::Integer(456)),
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
                Register::OwnedValue(OwnedValue::build_text("{}")),
                Register::OwnedValue(OwnedValue::Null),
                Register::OwnedValue(OwnedValue::Integer(456)),
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
                Register::OwnedValue(OwnedValue::build_text("[123]")),
                Register::OwnedValue(OwnedValue::build_text("$[0]")),
                Register::OwnedValue(OwnedValue::Integer(456)),
                Register::OwnedValue(OwnedValue::build_text("$[1]")),
                Register::OwnedValue(OwnedValue::Integer(789)),
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
                Register::OwnedValue(OwnedValue::build_text("{}")),
                Register::OwnedValue(OwnedValue::build_text("$.object[0].field")),
                Register::OwnedValue(OwnedValue::Integer(123)),
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
                Register::OwnedValue(OwnedValue::build_text("{}")),
                Register::OwnedValue(OwnedValue::build_text("$.object[0][0]")),
                Register::OwnedValue(OwnedValue::Integer(123)),
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
                Register::OwnedValue(OwnedValue::build_text("{}")),
                Register::OwnedValue(OwnedValue::build_text("$.object[123].another")),
                Register::OwnedValue(OwnedValue::build_text("value")),
                Register::OwnedValue(OwnedValue::build_text("$.field")),
                Register::OwnedValue(OwnedValue::build_text("value")),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), r#"{"field":"value"}"#,);
    }
}
