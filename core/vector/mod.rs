use crate::types::Value;
use crate::vdbe::Register;
use crate::vector::distance::{euclidean::Euclidean, DistanceCalculator};
use crate::LimboError;
use crate::Result;

pub mod distance;
pub mod vector_types;
use vector_types::*;

pub fn vector32(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector32 requires exactly one argument".to_string(),
        ));
    }
    let x = parse_vector(&args[0], Some(VectorType::Float32))?;
    // Extract the Vec<u8> from Value
    if let Value::Blob(data) = vector_serialize_f32(x) {
        Ok(Value::Blob(data))
    } else {
        Err(LimboError::ConversionError(
            "Failed to serialize vector".to_string(),
        ))
    }
}

pub fn vector64(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector64 requires exactly one argument".to_string(),
        ));
    }
    let x = parse_vector(&args[0], Some(VectorType::Float64))?;
    // Extract the Vec<u8> from Value
    if let Value::Blob(data) = vector_serialize_f64(x) {
        Ok(Value::Blob(data))
    } else {
        Err(LimboError::ConversionError(
            "Failed to serialize vector".to_string(),
        ))
    }
}

pub fn vector_extract(args: &[Register]) -> Result<Value> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector_extract requires exactly one argument".to_string(),
        ));
    }

    let blob = match &args[0].get_owned_value() {
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
    Ok(Value::build_text(vector_to_text(&vector)))
}

pub fn vector_distance_cos(args: &[Register]) -> Result<Value> {
    if args.len() != 2 {
        return Err(LimboError::ConversionError(
            "vector_distance_cos requires exactly two arguments".to_string(),
        ));
    }

    let x = parse_vector(&args[0], None)?;
    let y = parse_vector(&args[1], None)?;
    let dist = do_vector_distance_cos(&x, &y)?;
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
    // Validate that both vectors have the same dimensions and type
    if x.dims != y.dims {
        return Err(LimboError::ConversionError(
            "Vectors must have the same dimensions".to_string(),
        ));
    }
    if x.vector_type != y.vector_type {
        return Err(LimboError::ConversionError(
            "Vectors must be of the same type".to_string(),
        ));
    }

    let dist = Euclidean::calculate(&x, &y)?;
    Ok(Value::Float(dist))
}
