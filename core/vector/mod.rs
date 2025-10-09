use crate::types::Value;
use crate::vdbe::Register;
use crate::LimboError;
use crate::Result;

pub mod operations;
pub mod vector_types;
use vector_types::*;

pub fn vector32(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector32 requires exactly one argument".to_string(),
        ));
    }
    let vector = parse_vector(&args[0], Some(VectorType::Float32Dense))?;
    Ok(operations::serialize::vector_serialize(vector))
}

pub fn vector64(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector64 requires exactly one argument".to_string(),
        ));
    }
    let vector = parse_vector(&args[0], Some(VectorType::Float64Dense))?;
    Ok(operations::serialize::vector_serialize(vector))
}

pub fn vector_extract(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector_extract requires exactly one argument".to_string(),
        ));
    }

    let blob = match &args[0].get_value() {
        Value::Blob(b) => b,
        _ => {
            return Err(LimboError::ConversionError(
                "Expected blob value".to_string(),
            ))
        }
    };

    if blob.is_empty() {
        return Ok(Value::build_text("[]"));
    }

    let vector_type = vector_type(blob)?;
    let vector = vector_deserialize(vector_type, blob)?;
    Ok(Value::build_text(operations::text::vector_to_text(&vector)))
}

pub fn vector_distance_cos(args: &[Register]) -> Result<Value> {
    if args.len() != 2 {
        return Err(LimboError::ConversionError(
            "vector_distance_cos requires exactly two arguments".to_string(),
        ));
    }

    let x = parse_vector(&args[0], None)?;
    let y = parse_vector(&args[1], None)?;
    let dist = operations::distance_cos::vector_distance_cos(&x, &y)?;
    Ok(Value::Float(dist))
}

pub fn vector_distance_l2(args: &[Register]) -> Result<Value> {
    if args.len() != 2 {
        return Err(LimboError::ConversionError(
            "distance_l2 requires exactly two arguments".to_string(),
        ));
    }

    let x = parse_vector(&args[0], None)?;
    let y = parse_vector(&args[1], None)?;
    let dist = operations::distance_l2::vector_distance_l2(&x, &y)?;
    Ok(Value::Float(dist))
}

pub fn vector_concat(args: &[Register]) -> Result<Value> {
    if args.len() != 2 {
        return Err(LimboError::InvalidArgument(
            "concat requires exactly two arguments".into(),
        ));
    }

    let x = parse_vector(&args[0], None)?;
    let y = parse_vector(&args[1], None)?;
    let vector = operations::concat::vector_concat(&x, &y)?;
    Ok(operations::serialize::vector_serialize(vector))
}

pub fn vector_slice(args: &[Register]) -> Result<Value> {
    if args.len() != 3 {
        return Err(LimboError::InvalidArgument(
            "vector_slice requires exactly three arguments".into(),
        ));
    }

    let vector = parse_vector(&args[0], None)?;

    let start_index = args[1]
        .get_value()
        .as_int()
        .ok_or_else(|| LimboError::InvalidArgument("start index must be an integer".into()))?;

    let end_index = args[2]
        .get_value()
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
