use crate::{
    vector::vector_types::{Vector, VectorType},
    LimboError, Result,
};

pub fn vector_to_text(vector: &Vector) -> String {
    let mut text = String::new();
    text.push('[');
    match vector.vector_type {
        VectorType::Float32Dense => {
            let data = vector.as_f32_slice();
            for (i, value) in data.iter().enumerate().take(vector.dims) {
                text.push_str(&value.to_string());
                if i < vector.dims - 1 {
                    text.push(',');
                }
            }
        }
        VectorType::Float64Dense => {
            let data = vector.as_f64_slice();
            for (i, value) in data.iter().enumerate().take(vector.dims) {
                text.push_str(&value.to_string());
                if i < vector.dims - 1 {
                    text.push(',');
                }
            }
        }
    }
    text.push(']');
    text
}

/// Parse a vector in text representation into a Vector.
///
/// The format of a vector in text representation looks as follows:
///
/// ```console
/// [1.0, 2.0, 3.0]
/// ```
pub fn vector_from_text(vector_type: VectorType, text: &str) -> Result<Vector> {
    let text = text.trim();
    let mut chars = text.chars();
    if chars.next() != Some('[') || chars.last() != Some(']') {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }
    let mut data: Vec<u8> = Vec::new();
    let text = &text[1..text.len() - 1];
    if text.trim().is_empty() {
        return Ok(Vector {
            vector_type,
            dims: 0,
            data,
        });
    }
    let xs = text.split(',');
    for x in xs {
        let x = x.trim();
        if x.is_empty() {
            return Err(LimboError::ConversionError(
                "Invalid vector value".to_string(),
            ));
        }
        match vector_type {
            VectorType::Float32Dense => {
                let x = x
                    .parse::<f32>()
                    .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
                if !x.is_finite() {
                    return Err(LimboError::ConversionError(
                        "Invalid vector value".to_string(),
                    ));
                }
                data.extend_from_slice(&x.to_le_bytes());
            }
            VectorType::Float64Dense => {
                let x = x
                    .parse::<f64>()
                    .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
                if !x.is_finite() {
                    return Err(LimboError::ConversionError(
                        "Invalid vector value".to_string(),
                    ));
                }
                data.extend_from_slice(&x.to_le_bytes());
            }
        };
    }
    let dims = vector_type.size_to_dims(data.len());
    Ok(Vector {
        vector_type,
        dims,
        data,
    })
}
