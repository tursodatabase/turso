use crate::{
    alloc::{TursoAllocExt, TursoIteratorExt, TursoTryWithCapacityExt, TursoVecExt, Vec},
    vector::vector_types::{Vector, VectorType},
    Result, ValueBlob,
};

#[turso_macros::allocation_site(crate::alloc::VectorAllocationSite::Convert)]
pub fn vector_convert(v: Vector, target_type: VectorType) -> Result<Vector> {
    if v.vector_type == target_type {
        return Ok(v);
    }
    match (v.vector_type, target_type) {
        (VectorType::Float32Dense, VectorType::Float64Dense) => Ok(Vector::from_f64(
            v.as_f32_slice().iter().map(|&x| x as f64).try_collect()?,
        )),
        (VectorType::Float64Dense, VectorType::Float32Dense) => Ok(Vector::from_f32(
            v.as_f64_slice().iter().map(|&x| x as f32).try_collect()?,
        )),
        (VectorType::Float32Dense, VectorType::Float32Sparse) => {
            let mut idx: Vec<u32> = TursoAllocExt::new();
            let mut values: Vec<f32> = TursoAllocExt::new();
            for (i, &value) in v.as_f32_slice().iter().enumerate() {
                if value == 0.0 {
                    continue;
                }
                idx.try_push(i as u32)?;
                values.try_push(value)?;
            }
            Ok(Vector::from_f32_sparse(v.dims, values, idx)?)
        }
        (VectorType::Float64Dense, VectorType::Float32Sparse) => {
            let mut idx: Vec<u32> = TursoAllocExt::new();
            let mut values: Vec<f32> = TursoAllocExt::new();
            for (i, &value) in v.as_f64_slice().iter().enumerate() {
                if value == 0.0 {
                    continue;
                }
                idx.try_push(i as u32)?;
                values.try_push(value as f32)?;
            }
            Ok(Vector::from_f32_sparse(v.dims, values, idx)?)
        }
        (VectorType::Float32Sparse, VectorType::Float32Dense) => {
            let sparse = v.as_f32_sparse();
            let mut data = crate::alloc::try_vec![0f32; v.dims]?;
            for (&i, &value) in sparse.idx.iter().zip(sparse.values.iter()) {
                data[i as usize] = value;
            }
            Ok(Vector::from_f32(data))
        }
        (VectorType::Float32Sparse, VectorType::Float64Dense) => {
            let sparse = v.as_f32_sparse();
            let mut data = crate::alloc::try_vec![0f64; v.dims]?;
            for (&i, &value) in sparse.idx.iter().zip(sparse.values.iter()) {
                data[i as usize] = value as f64;
            }
            Ok(Vector::from_f64(data))
        }
        // Float1Bit conversions
        (VectorType::Float32Dense, VectorType::Float1Bit) => {
            let dims = v.dims;
            let byte_count = dims.div_ceil(8);
            let mut bits = crate::alloc::try_vec![0u8; byte_count]?;
            for (i, &val) in v.as_f32_slice().iter().enumerate() {
                if val > 0.0 {
                    bits[i / 8] |= 1 << (i & 7);
                }
            }
            Ok(Vector::from_1bit(dims, bits))
        }
        (VectorType::Float64Dense, VectorType::Float1Bit) => {
            let dims = v.dims;
            let byte_count = dims.div_ceil(8);
            let mut bits = crate::alloc::try_vec![0u8; byte_count]?;
            for (i, &val) in v.as_f64_slice().iter().enumerate() {
                if val > 0.0 {
                    bits[i / 8] |= 1 << (i & 7);
                }
            }
            Ok(Vector::from_1bit(dims, bits))
        }
        (VectorType::Float1Bit, VectorType::Float32Dense) => {
            let data = v.as_1bit_data();
            let floats: Vec<f32> = (0..v.dims)
                .map(|i| {
                    if (data[i / 8] >> (i & 7)) & 1 == 1 {
                        1.0
                    } else {
                        -1.0
                    }
                })
                .try_collect()?;
            Ok(Vector::from_f32(floats))
        }
        (VectorType::Float1Bit, VectorType::Float64Dense) => {
            let data = v.as_1bit_data();
            let floats: Vec<f64> = (0..v.dims)
                .map(|i| {
                    if (data[i / 8] >> (i & 7)) & 1 == 1 {
                        1.0
                    } else {
                        -1.0
                    }
                })
                .try_collect()?;
            Ok(Vector::from_f64(floats))
        }
        // Float8 conversions
        (VectorType::Float32Dense, VectorType::Float8) => {
            convert_floats_to_f8(v.as_f32_slice().iter().copied(), v.dims)
        }
        (VectorType::Float64Dense, VectorType::Float8) => {
            convert_floats_to_f8(v.as_f64_slice().iter().map(|&x| x as f32), v.dims)
        }
        (VectorType::Float8, VectorType::Float32Dense) => {
            let (quantized, alpha, shift) = v.as_f8_data();
            let floats: Vec<f32> = quantized
                .iter()
                .map(|&q| alpha * q as f32 + shift)
                .try_collect()?;
            Ok(Vector::from_f32(floats))
        }
        (VectorType::Float8, VectorType::Float64Dense) => {
            let (quantized, alpha, shift) = v.as_f8_data();
            let floats: Vec<f64> = quantized
                .iter()
                .map(|&q| alpha as f64 * q as f64 + shift as f64)
                .try_collect()?;
            Ok(Vector::from_f64(floats))
        }
        // Cross-conversions via intermediate
        (VectorType::Float1Bit, VectorType::Float8) => {
            let f32_vec = vector_convert(v, VectorType::Float32Dense)?;
            vector_convert(f32_vec, VectorType::Float8)
        }
        (VectorType::Float8, VectorType::Float1Bit) => {
            let f32_vec = vector_convert(v, VectorType::Float32Dense)?;
            vector_convert(f32_vec, VectorType::Float1Bit)
        }
        (VectorType::Float1Bit, VectorType::Float32Sparse)
        | (VectorType::Float8, VectorType::Float32Sparse)
        | (VectorType::Float32Sparse, VectorType::Float1Bit)
        | (VectorType::Float32Sparse, VectorType::Float8) => {
            let f32_vec = vector_convert(v, VectorType::Float32Dense)?;
            vector_convert(f32_vec, target_type)
        }
        _ => unreachable!(
            "unexpected conversion: {:?} -> {:?}",
            v.vector_type, target_type
        ),
    }
}

fn convert_floats_to_f8(
    values: impl Iterator<Item = f32> + Clone,
    dims: usize,
) -> Result<Vector<'static>> {
    if dims == 0 {
        return Ok(Vector::from_f8(0, crate::alloc::vec![], 0.0, 0.0)?);
    }
    let mut min_val = f32::INFINITY;
    let mut max_val = f32::NEG_INFINITY;
    for val in values.clone() {
        if val < min_val {
            min_val = val;
        }
        if val > max_val {
            max_val = val;
        }
    }
    let alpha = (max_val - min_val) / 255.0;
    let shift = min_val;
    let mut quantized = <ValueBlob as TursoTryWithCapacityExt>::try_with_capacity_ext(dims)?;
    for val in values {
        let q = if alpha == 0.0 {
            0u8
        } else {
            let v = (val - shift) / alpha + 0.5;
            (v as i32).clamp(0, 255) as u8
        };
        quantized.push(q);
    }
    Ok(Vector::from_f8(dims, quantized, alpha, shift)?)
}

#[cfg(test)]
#[path = "../../tests/unit/vector/operations/convert/tests.rs"]
mod tests;
