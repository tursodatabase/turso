use crate::types::AsValueRef;
use crate::types::Value;
use crate::types::ValueType;
use crate::vdbe::Register;
use crate::LimboError;
use crate::Result;
use crate::ValueRef;

pub mod operations;
pub mod vector_types;
use vector_types::*;

pub fn parse_vector<'a>(
    value: &'a (impl AsValueRef + 'a),
    type_hint: Option<VectorType>,
) -> Result<Vector<'a>> {
    let value = value.as_value_ref();
    match value.value_type() {
        ValueType::Text => operations::text::vector_from_text(
            type_hint.unwrap_or(VectorType::Float32Dense),
            value.to_text().expect("value must be text"),
        ),
        ValueType::Blob => {
            let Some(blob) = value.to_blob() else {
                return Err(LimboError::ConversionError(
                    "Invalid vector value".to_string(),
                ));
            };
            Vector::from_slice(blob)
        }
        _ => Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        )),
    }
}

pub fn vector32(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector32 requires exactly one argument".to_string(),
        ));
    }
    let value = args[0].get_value();
    let vector = parse_vector(value, Some(VectorType::Float32Dense))?;
    let vector = operations::convert::vector_convert(vector, VectorType::Float32Dense)?;
    Ok(operations::serialize::vector_serialize(vector))
}

pub fn vector32_sparse(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector32_sparse requires exactly one argument".to_string(),
        ));
    }
    let value = args[0].get_value();
    let vector = parse_vector(value, Some(VectorType::Float32Sparse))?;
    let vector = operations::convert::vector_convert(vector, VectorType::Float32Sparse)?;
    Ok(operations::serialize::vector_serialize(vector))
}

pub fn vector64(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector64 requires exactly one argument".to_string(),
        ));
    }
    let value = args[0].get_value();
    let vector = parse_vector(value, Some(VectorType::Float64Dense))?;
    let vector = operations::convert::vector_convert(vector, VectorType::Float64Dense)?;
    Ok(operations::serialize::vector_serialize(vector))
}

pub fn vector_extract(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector_extract requires exactly one argument".to_string(),
        ));
    }

    let value = args[0].get_value().as_value_ref();
    let blob = match value {
        ValueRef::Blob(b) => b,
        _ => {
            return Err(LimboError::ConversionError(
                "Expected blob value".to_string(),
            ))
        }
    };

    if blob.is_empty() {
        return Ok(Value::build_text("[]"));
    }

    let vector = Vector::from_slice(blob)?;
    Ok(Value::build_text(operations::text::vector_to_text(&vector)))
}

pub fn vector_distance_cos(args: &[Register]) -> Result<Value> {
    if args.len() != 2 {
        return Err(LimboError::ConversionError(
            "vector_distance_cos requires exactly two arguments".to_string(),
        ));
    }

    let value_0 = args[0].get_value();
    let value_1 = args[1].get_value();
    let x = parse_vector(value_0, None)?;
    let y = parse_vector(value_1, None)?;
    let dist = operations::distance_cos::vector_distance_cos(&x, &y)?;
    Ok(Value::Float(dist))
}

pub fn vector_distance_l2(args: &[Register]) -> Result<Value> {
    if args.len() != 2 {
        return Err(LimboError::ConversionError(
            "distance_l2 requires exactly two arguments".to_string(),
        ));
    }

    let value_0 = args[0].get_value();
    let value_1 = args[1].get_value();
    let x = parse_vector(value_0, None)?;
    let y = parse_vector(value_1, None)?;
    let dist = operations::distance_l2::vector_distance_l2(&x, &y)?;
    Ok(Value::Float(dist))
}

pub fn vector_distance_jaccard(args: &[Register]) -> Result<Value> {
    if args.len() != 2 {
        return Err(LimboError::ConversionError(
            "distance_jaccard requires exactly two arguments".to_string(),
        ));
    }

    let value_0 = args[0].get_value();
    let value_1 = args[1].get_value();
    let x = parse_vector(value_0, None)?;
    let y = parse_vector(value_1, None)?;
    let dist = operations::jaccard::vector_distance_jaccard(&x, &y)?;
    Ok(Value::Float(dist))
}

pub fn vector_distance_dot(args: &[Register]) -> Result<Value> {
    if args.len() != 2 {
        return Err(LimboError::ConversionError(
            "distance_dot requires exactly two arguments".to_string(),
        ));
    }

    let value_0 = args[0].get_value();
    let value_1 = args[1].get_value();
    let x = parse_vector(value_0, None)?;
    let y = parse_vector(value_1, None)?;
    let dist = operations::distance_dot::vector_distance_dot(&x, &y)?;
    Ok(Value::Float(dist))
}

pub fn vector_concat(args: &[Register]) -> Result<Value> {
    if args.len() != 2 {
        return Err(LimboError::InvalidArgument(
            "concat requires exactly two arguments".into(),
        ));
    }

    let value_0 = args[0].get_value();
    let value_1 = args[1].get_value();
    let x = parse_vector(value_0, None)?;
    let y = parse_vector(value_1, None)?;
    let vector = operations::concat::vector_concat(&x, &y)?;
    Ok(operations::serialize::vector_serialize(vector))
}

pub fn vector_slice(args: &[Register]) -> Result<Value> {
    if args.len() != 3 {
        return Err(LimboError::InvalidArgument(
            "vector_slice requires exactly three arguments".into(),
        ));
    }
    let value_0 = args[0].get_value();
    let value_1 = args[1].get_value().as_value_ref();
    let value_2 = args[2].get_value().as_value_ref();

    let vector = parse_vector(value_0, None)?;

    let start_index = value_1
        .as_int()
        .ok_or_else(|| LimboError::InvalidArgument("start index must be an integer".into()))?;

    let end_index = value_2
        .as_int()
        .ok_or_else(|| LimboError::InvalidArgument("end_index must be an integer".into()))?;

    if start_index < 0 || end_index < 0 {
        return Err(LimboError::InvalidArgument(
            "start index and end_index must be non-negative".into(),
        ));
    }

    let result =
        operations::slice::vector_slice(&vector, start_index as usize, end_index as usize)?;

    Ok(operations::serialize::vector_serialize(result))
}
