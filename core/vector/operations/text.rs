use crate::{
    vector::vector_types::{Vector, VectorType},
    LimboError, Result,
};

pub fn vector_to_text(vector: &Vector) -> String {
    match vector.vector_type {
        VectorType::Float32Dense => format_text(vector.as_f32_slice().iter()),
        VectorType::Float64Dense => format_text(vector.as_f64_slice().iter()),
        VectorType::Float32Sparse => {
            let mut dense = vec![0.0f32; vector.dims];
            let sparse = vector.as_f32_sparse();
            tracing::info!("{:?}", sparse);
            for (&idx, &value) in sparse.idx.iter().zip(sparse.values.iter()) {
                dense[idx as usize] = value;
            }
            format_text(dense.iter())
        }
        VectorType::Float1Bit => {
            // Each bit → +1 or -1
            let data = vector.as_1bit_data();
            let values: Vec<i8> = (0..vector.dims)
                .map(|i| {
                    if (data[i / 8] >> (i & 7)) & 1 == 1 {
                        1
                    } else {
                        -1
                    }
                })
                .collect();
            format_text(values.iter())
        }
        VectorType::Float8 => {
            // Dequantize: f_i = alpha * q_i + shift
            let (quantized, alpha, shift) = vector.as_f8_data();
            let values: Vec<f32> = quantized
                .iter()
                .map(|&q| alpha * (q as f32) + shift)
                .collect();
            format_text(values.iter())
        }
    }
}

fn format_text<T: std::string::ToString>(values: impl Iterator<Item = T>) -> String {
    let mut text = String::new();
    text.push('[');
    let mut first = true;
    for value in values {
        if !first {
            text.push(',');
        }
        first = false;
        text.push_str(&value.to_string());
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
    let text = &text[1..text.len() - 1];
    if text.trim().is_empty() {
        return Ok(match vector_type {
            VectorType::Float1Bit | VectorType::Float8 => {
                return Err(LimboError::ConversionError(
                    "empty vector not supported for this type".to_string(),
                ));
            }
            _ => Vector {
                vector_type,
                dims: 0,
                owned: Some(Vec::new()),
                refer: None,
            },
        });
    }
    let tokens = text.split(',').map(|x| x.trim());
    match vector_type {
        VectorType::Float32Dense => vector32_from_text(tokens),
        VectorType::Float64Dense => vector64_from_text(tokens),
        VectorType::Float32Sparse => vector32_sparse_from_text(tokens),
        VectorType::Float1Bit => vector_1bit_from_text(tokens),
        VectorType::Float8 => vector_f8_from_text(tokens),
    }
}

fn vector32_from_text<'a>(tokens: impl Iterator<Item = &'a str>) -> Result<Vector<'static>> {
    let mut data = Vec::new();
    for token in tokens {
        let value = token
            .parse::<f32>()
            .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
        if !value.is_finite() {
            return Err(LimboError::ConversionError(
                "Invalid vector value".to_string(),
            ));
        }
        data.extend_from_slice(&value.to_le_bytes());
    }
    Ok(Vector {
        vector_type: VectorType::Float32Dense,
        dims: data.len() / 4,
        owned: Some(data),
        refer: None,
    })
}

fn vector64_from_text<'a>(tokens: impl Iterator<Item = &'a str>) -> Result<Vector<'static>> {
    let mut data = Vec::new();
    for token in tokens {
        let value = token
            .parse::<f64>()
            .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
        if !value.is_finite() {
            return Err(LimboError::ConversionError(
                "Invalid vector value".to_string(),
            ));
        }
        data.extend_from_slice(&value.to_le_bytes());
    }
    Ok(Vector {
        vector_type: VectorType::Float64Dense,
        dims: data.len() / 8,
        owned: Some(data),
        refer: None,
    })
}

fn vector32_sparse_from_text<'a>(tokens: impl Iterator<Item = &'a str>) -> Result<Vector<'static>> {
    let mut idx = Vec::new();
    let mut values = Vec::new();
    let mut dims = 0u32;
    for token in tokens {
        let value = token
            .parse::<f32>()
            .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
        if !value.is_finite() {
            return Err(LimboError::ConversionError(
                "Invalid vector value".to_string(),
            ));
        }

        dims += 1;
        if value == 0.0 {
            continue;
        }
        idx.extend_from_slice(&(dims - 1).to_le_bytes());
        values.extend_from_slice(&value.to_le_bytes());
    }

    values.extend_from_slice(&idx);
    Ok(Vector {
        vector_type: VectorType::Float32Sparse,
        dims: dims as usize,
        owned: Some(values),
        refer: None,
    })
}

fn vector_1bit_from_text<'a>(tokens: impl Iterator<Item = &'a str>) -> Result<Vector<'static>> {
    let mut floats = Vec::new();
    for token in tokens {
        let value = token
            .parse::<f32>()
            .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
        if !value.is_finite() {
            return Err(LimboError::ConversionError(
                "Invalid vector value".to_string(),
            ));
        }
        floats.push(value);
    }
    let dims = floats.len();
    let byte_count = dims.div_ceil(8);
    let mut bits = vec![0u8; byte_count];
    for (i, &f) in floats.iter().enumerate() {
        if f > 0.0 {
            bits[i / 8] |= 1 << (i & 7);
        }
    }
    Ok(Vector::from_1bit(dims, bits))
}

fn vector_f8_from_text<'a>(tokens: impl Iterator<Item = &'a str>) -> Result<Vector<'static>> {
    let mut floats = Vec::new();
    for token in tokens {
        let value = token
            .parse::<f32>()
            .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
        if !value.is_finite() {
            return Err(LimboError::ConversionError(
                "Invalid vector value".to_string(),
            ));
        }
        floats.push(value);
    }
    let dims = floats.len();
    let min_val = floats.iter().cloned().fold(f32::INFINITY, f32::min);
    let max_val = floats.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
    let alpha = (max_val - min_val) / 255.0;
    let shift = min_val;
    let mut quantized = Vec::with_capacity(dims);
    for &f in &floats {
        let q = if alpha == 0.0 {
            0u8
        } else {
            let v = (f - shift) / alpha + 0.5;
            (v as i32).clamp(0, 255) as u8
        };
        quantized.push(q);
    }
    Ok(Vector::from_f8(dims, quantized, alpha, shift))
}
