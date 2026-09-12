use crate::{
    alloc::TursoTryWithCapacityExt,
    vector::vector_types::{Vector, VectorType},
    LimboError, Result, ValueBlob,
};

#[turso_macros::allocation_site(crate::alloc::VectorAllocationSite::Concat)]
pub fn vector_concat(v1: &Vector, v2: &Vector) -> Result<Vector<'static>> {
    if v1.vector_type != v2.vector_type {
        return Err(LimboError::ConversionError(
            "Mismatched vector types".into(),
        ));
    }

    let data = match v1.vector_type {
        VectorType::Float32Dense | VectorType::Float64Dense => {
            let mut data = <ValueBlob as TursoTryWithCapacityExt>::try_with_capacity_ext(
                v1.bin_len() + v2.bin_len(),
            )?;
            data.extend_from_slice(v1.bin_data());
            data.extend_from_slice(v2.bin_data());
            data
        }
        VectorType::Float32Sparse => {
            let mut data = <ValueBlob as TursoTryWithCapacityExt>::try_with_capacity_ext(
                v1.bin_len() + v2.bin_len(),
            )?;
            data.extend_from_slice(&v1.bin_data()[..v1.bin_len() / 2]);
            data.extend_from_slice(&v2.bin_data()[..v2.bin_len() / 2]);
            data.extend_from_slice(&v1.bin_data()[v1.bin_len() / 2..]);
            data.extend_from_slice(&v2.bin_data()[v2.bin_len() / 2..]);
            data
        }
        VectorType::Float1Bit | VectorType::Float8 => {
            return Err(LimboError::ConversionError(
                "vector_concat is not supported for float1bit/float8 vectors".to_string(),
            ));
        }
    };

    Ok(Vector {
        vector_type: v1.vector_type,
        dims: v1.dims + v2.dims,
        owned: Some(data),
        refer: None,
    })
}

#[cfg(test)]
#[path = "../../tests/unit/vector/operations/concat/tests.rs"]
mod tests;
