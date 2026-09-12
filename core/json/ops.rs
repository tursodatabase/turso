use super::{
    convert_dbtype_to_jsonb, curry_convert_dbtype_to_jsonb, json_path_from_db_value,
    json_string_to_db_type,
    jsonb::{DeleteOperation, InsertOperation, ReplaceOperation},
    Conv, JsonCacheCell, OutputVariant,
};
use crate::{
    types::{AsValueRef, Text, Value},
    ValueRef,
};

/// The function follows RFC 7386 JSON Merge Patch semantics:
/// * If the patch is null, the target is replaced with null
/// * If the patch contains a scalar value, the target is replaced with that value
/// * If both target and patch are objects, the patch is recursively applied
/// * null values in the patch result in property removal from the target
pub fn json_patch(
    target: impl AsValueRef,
    patch: impl AsValueRef,
    cache: &JsonCacheCell,
) -> crate::Result<Value> {
    let (target, patch) = (target.as_value_ref(), patch.as_value_ref());
    if matches!(target, ValueRef::Null) || matches!(patch, ValueRef::Null) {
        return Ok(Value::Null);
    }
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut target = cache.get_or_insert_with(target, make_jsonb)?;
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let patch = cache.get_or_insert_with(patch, make_jsonb)?;

    if patch.element_type()? != super::jsonb::ElementType::OBJECT {
        target = patch;
    } else {
        if target.element_type()? != super::jsonb::ElementType::OBJECT {
            target = super::jsonb::Jsonb::make_empty_obj(0)?;
        }
        target.patch(&patch)?;
    }

    let element_type = target.element_type()?;

    // A JSON null patch result stays JSON here: SQLite returns the
    // text 'null', while json_string_to_db_type would map it to SQL
    // NULL the way extraction does.
    if element_type == super::jsonb::ElementType::NULL {
        return Ok(Value::Text(Text::json("null".to_string())));
    }

    json_string_to_db_type(target, element_type, OutputVariant::String)
}

pub fn jsonb_patch(
    target: impl AsValueRef,
    patch: impl AsValueRef,
    cache: &JsonCacheCell,
) -> crate::Result<Value> {
    let (target, patch) = (target.as_value_ref(), patch.as_value_ref());
    if matches!(target, ValueRef::Null) || matches!(patch, ValueRef::Null) {
        return Ok(Value::Null);
    }
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut target = cache.get_or_insert_with(target, make_jsonb)?;
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let patch = cache.get_or_insert_with(patch, make_jsonb)?;

    if patch.element_type()? != super::jsonb::ElementType::OBJECT {
        target = patch;
    } else {
        if target.element_type()? != super::jsonb::ElementType::OBJECT {
            target = super::jsonb::Jsonb::make_empty_obj(0)?;
        }
        target.patch(&patch)?;
    }

    let element_type = target.element_type()?;

    // A JSON null patch result stays JSON here: SQLite returns the
    // JSONB null blob, while json_string_to_db_type would map it to
    // SQL NULL the way extraction does.
    if element_type == super::jsonb::ElementType::NULL {
        return Ok(Value::Blob(target.data()));
    }

    json_string_to_db_type(target, element_type, OutputVariant::Binary)
}

pub fn json_remove<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;
    for arg in args {
        if let ValueRef::Text(s) = arg.as_value_ref() {
            if s.as_str() == "$" {
                return Ok(Value::Null);
            }
        }
        if let Some(path) = json_path_from_db_value(&arg, true)? {
            let mut op = DeleteOperation::new();
            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_string_to_db_type(json, el_type, OutputVariant::String)
}

pub fn jsonb_remove<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;
    for arg in args {
        if let ValueRef::Text(s) = arg.as_value_ref() {
            if s.as_str() == "$" {
                return Ok(Value::Null);
            }
        }
        if let Some(path) = json_path_from_db_value(&arg, true)? {
            let mut op = DeleteOperation::new();
            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    Ok(Value::Blob(json.data()))
}

pub fn json_replace<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;
    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;
        let path = json_path_from_db_value(&first, true)?;

        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;
        let value = convert_dbtype_to_jsonb(&second, Conv::NotStrict)?;
        if let Some(path) = path {
            let mut op = ReplaceOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_string_to_db_type(json, el_type, super::OutputVariant::String)
}

pub fn jsonb_replace<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;
    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;
        let path = json_path_from_db_value(&first, true)?;

        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;
        let value = convert_dbtype_to_jsonb(&second, Conv::NotStrict)?;
        if let Some(path) = path {
            let mut op = ReplaceOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_string_to_db_type(json, el_type, OutputVariant::Binary)
}

pub fn json_insert<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;

    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;
        let path = json_path_from_db_value(&first, true)?;

        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;
        let value = convert_dbtype_to_jsonb(&second, Conv::NotStrict)?;
        if let Some(path) = path {
            let mut op = InsertOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_string_to_db_type(json, el_type, OutputVariant::String)
}

pub fn jsonb_insert<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;

    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;
        let path = json_path_from_db_value(&first, true)?;

        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;
        let value = convert_dbtype_to_jsonb(&second, Conv::NotStrict)?;
        if let Some(path) = path {
            let mut op = InsertOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_string_to_db_type(json, el_type, OutputVariant::Binary)
}

#[cfg(test)]
#[path = "../tests/unit/json/ops/tests.rs"]
mod tests;
