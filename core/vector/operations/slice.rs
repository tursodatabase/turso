use crate::{
    alloc::TursoAllocExt,
    types::value_blob_from_slice,
    vector::vector_types::{Vector, VectorType},
    LimboError, Result, ValueBlob,
};

#[turso_macros::allocation_site(crate::alloc::VectorAllocationSite::Slice)]
pub fn vector_slice(vector: &Vector, start: usize, end: usize) -> Result<Vector<'static>> {
    if start > end {
        return Err(LimboError::InvalidArgument(
            "start index must not be greater than end index".into(),
        ));
    }
    if end > vector.dims || end < start {
        return Err(LimboError::ConversionError(
            "vector_slice range out of bounds".into(),
        ));
    }
    match vector.vector_type {
        VectorType::Float32Dense => Ok(Vector {
            vector_type: vector.vector_type,
            dims: end - start,
            owned: Some(value_blob_from_slice(
                &vector.bin_data()[start * 4..end * 4],
            )?),
            refer: None,
        }),
        VectorType::Float64Dense => Ok(Vector {
            vector_type: vector.vector_type,
            dims: end - start,
            owned: Some(value_blob_from_slice(
                &vector.bin_data()[start * 8..end * 8],
            )?),
            refer: None,
        }),
        VectorType::Float32Sparse => {
            let mut values: ValueBlob = TursoAllocExt::new();
            let mut idx: ValueBlob = TursoAllocExt::new();
            let sparse = vector.as_f32_sparse();
            for (&i, &value) in sparse.idx.iter().zip(sparse.values.iter()) {
                let i = i as usize;
                if i < start || i >= end {
                    continue;
                }
                values.try_reserve(4)?;
                values.extend_from_slice(&value.to_le_bytes());
                idx.try_reserve(4)?;
                idx.extend_from_slice(&((i - start) as u32).to_le_bytes());
            }
            values.try_reserve(idx.len())?;
            values.extend_from_slice(&idx);
            Ok(Vector {
                vector_type: vector.vector_type,
                dims: end - start,
                owned: Some(values),
                refer: None,
            })
        }
        VectorType::Float1Bit | VectorType::Float8 => Err(LimboError::ConversionError(
            "vector_slice is not supported for float1bit/float8 vectors".to_string(),
        )),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/vector/operations/slice/tests.rs"]
mod tests;
