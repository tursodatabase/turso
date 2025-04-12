use crate::types::{OwnedValue, OwnedValueType};
use crate::vdbe::Register;
use crate::LimboError;
use crate::Result;

pub mod vector_types;
use vector_types::*;

pub fn vector32(args: &[Register]) -> Result<OwnedValue> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector32 requires exactly one argument".to_string(),
        ));
    }
    let x = parse_vector(&args[0], Some(VectorType::Float32))?;
    // Extract the Vec<u8> from OwnedValue
    let serialized_vector = vector_serialize_f32(x);
    if OwnedValueType::Blob == serialized_vector.value_type() {
        Ok(serialized_vector)
    } else {
        Err(LimboError::ConversionError(
            "Failed to serialize vector".to_string(),
        ))
    }
}

pub fn vector64(args: &[Register]) -> Result<OwnedValue> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector64 requires exactly one argument".to_string(),
        ));
    }
    let x = parse_vector(&args[0], Some(VectorType::Float64))?;
    // Extract the Vec<u8> from OwnedValue
    let serialized_vector = vector_serialize_f64(x);
    if OwnedValueType::Blob == serialized_vector.value_type() {
        Ok(serialized_vector)
    } else {
        Err(LimboError::ConversionError(
            "Failed to serialize vector".to_string(),
        ))
    }
}

pub fn vector_extract(args: &[Register]) -> Result<OwnedValue> {
    if args.len() != 1 {
        return Err(LimboError::ConversionError(
            "vector_extract requires exactly one argument".to_string(),
        ));
    }

    let blob = match args[0].get_owned_value().to_blob() {
        Some(b) => b,
        None => {
            return Err(LimboError::ConversionError(
                "Expected blob value".to_string(),
            ))
        }
    };

    if blob.is_empty() {
        return Ok(OwnedValue::build_text("[]"));
    }

    let vector_type = vector_type(blob)?;
    let vector = vector_deserialize(vector_type, blob)?;
    Ok(OwnedValue::build_text(&vector_to_text(&vector)))
}

pub fn vector_distance_cos(args: &[Register]) -> Result<OwnedValue> {
    if args.len() != 2 {
        return Err(LimboError::ConversionError(
            "vector_distance_cos requires exactly two arguments".to_string(),
        ));
    }

    let x = parse_vector(&args[0], None)?;
    let y = parse_vector(&args[1], None)?;
    let dist = do_vector_distance_cos(&x, &y)?;
    Ok(OwnedValue::float(dist))
}
