use crate::{
    alloc::{TryReserveError, TursoTryWithCapacityExt, Vec},
    turso_debug_assert,
    types::value_blob_from_slice,
    LimboError, Result, ValueBlob,
};

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum VectorType {
    Float32Dense,
    Float64Dense,
    Float32Sparse,
    Float1Bit,
    Float8,
}

#[derive(Debug)]
pub struct Vector<'a> {
    pub vector_type: VectorType,
    pub dims: usize,
    pub owned: Option<ValueBlob>,
    pub refer: Option<&'a [u8]>,
}

#[derive(Debug)]
pub struct VectorSparse<'a, T: std::fmt::Debug> {
    pub idx: &'a [u32],
    pub values: &'a [T],
}

impl<'a> Vector<'a> {
    /// Returns (VectorType, data_length, dims) from a serialized blob.
    /// `data_length` is the number of bytes of actual vector data (before meta/type bytes).
    /// `dims` is the number of vector dimensions (only meaningful for Float1Bit/Float8 where
    /// it can't be inferred from data length alone; for other types it's set to 0 and
    /// computed later in from_data).
    pub fn vector_type(blob: &[u8]) -> Result<(VectorType, usize, usize)> {
        // Even-sized blobs are always float32.
        if blob.len() % 2 == 0 {
            return Ok((VectorType::Float32Dense, blob.len(), 0));
        }
        // Odd-sized blobs have type byte at the end
        let vector_type = blob[blob.len() - 1];
        /*
        vector types used by LibSQL:
        (see https://github.com/tursodatabase/libsql/blob/a55bf61192bdb89e97568de593c4af5b70d24bde/libsql-sqlite3/src/vectorInt.h#L52)
            #define VECTOR_TYPE_FLOAT32   1
            #define VECTOR_TYPE_FLOAT64   2
            #define VECTOR_TYPE_FLOAT1BIT 3
            #define VECTOR_TYPE_FLOAT8    4
            #define VECTOR_TYPE_FLOAT16   5
            #define VECTOR_TYPE_FLOATB16  6
        */
        match vector_type {
            1 => Ok((VectorType::Float32Dense, blob.len() - 1, 0)),
            2 => Ok((VectorType::Float64Dense, blob.len() - 1, 0)),
            3 => {
                // Float1Bit: [data bytes][optional padding][trailing_bits][0x03]
                let n_blob_size = blob.len() - 1; // without type byte
                if n_blob_size == 0 || n_blob_size % 2 != 0 {
                    return Err(LimboError::ConversionError(
                        "float1bit vector blob length must be even and non-empty".to_string(),
                    ));
                }
                let trailing_bits = blob[n_blob_size - 1] as usize;
                // `trailing_bits` is a raw blob byte, so it can name more padding
                // than the blob holds. Unchecked, `dims` wraps and `from_slice`
                // slices with a bogus length.
                let dims = (n_blob_size * 8)
                    .checked_sub(trailing_bits)
                    .ok_or_else(|| {
                        LimboError::ConversionError(format!(
                            "float1bit vector trailing bits {trailing_bits} exceed blob capacity"
                        ))
                    })?;
                let data_size = dims.div_ceil(8);
                // The trailing-bits byte is counted in `n_blob_size`, so valid data
                // is always strictly shorter than the blob.
                if data_size >= n_blob_size {
                    return Err(LimboError::ConversionError(format!(
                        "float1bit vector needs {data_size} data bytes but blob holds {n_blob_size}"
                    )));
                }
                Ok((VectorType::Float1Bit, data_size, dims))
            }
            4 => {
                // Float8: [quantized bytes][alignment padding][alpha f32][shift f32][padding 0x00][trailing_bytes][0x04]
                let n_blob_size = blob.len() - 1; // without type byte
                if n_blob_size < 2 || n_blob_size % 2 != 0 {
                    return Err(LimboError::ConversionError(
                        "float8 vector blob must have even length >= 2 (excluding type byte)"
                            .to_string(),
                    ));
                }
                let trailing_bytes = blob[n_blob_size - 1] as usize;
                // 8 bytes of alpha/shift plus the padding and trailing markers, so
                // `n_blob_size` must be >= 10 before subtracting `trailing_bytes`.
                let dims = n_blob_size
                    .checked_sub(10)
                    .and_then(|dims| dims.checked_sub(trailing_bytes))
                    .ok_or_else(|| {
                        LimboError::ConversionError(format!(
                            "float8 vector blob of {n_blob_size} bytes is too short for {trailing_bytes} trailing bytes"
                        ))
                    })?;
                // data_size = ALIGN(dims, 4) + 8
                let data_size = n_blob_size - 2;
                Ok((VectorType::Float8, data_size, dims))
            }
            5..=6 => Err(LimboError::ConversionError(
                "unsupported vector type from LibSQL".to_string(),
            )),
            9 => Ok((VectorType::Float32Sparse, blob.len() - 1, 0)),
            _ => Err(LimboError::ConversionError(format!(
                "unknown vector type: {vector_type}"
            ))),
        }
    }
    pub fn from_f32(mut values_f32: Vec<f32>) -> Self {
        let dims = values_f32.len();
        #[cfg(not(nightly))]
        let values = unsafe {
            ValueBlob::from_raw_parts(
                values_f32.as_mut_ptr() as *mut u8,
                values_f32.len() * 4,
                values_f32.capacity() * 4,
            )
        };
        #[cfg(nightly)]
        let values = unsafe {
            ValueBlob::from_raw_parts_in(
                values_f32.as_mut_ptr() as *mut u8,
                values_f32.len() * 4,
                values_f32.capacity() * 4,
                crate::alloc::TursoAllocator,
            )
        };
        std::mem::forget(values_f32);
        Self {
            vector_type: VectorType::Float32Dense,
            dims,
            owned: Some(values),
            refer: None,
        }
    }
    pub fn from_f64(mut values_f64: Vec<f64>) -> Self {
        let dims = values_f64.len();
        #[cfg(not(nightly))]
        let values = unsafe {
            ValueBlob::from_raw_parts(
                values_f64.as_mut_ptr() as *mut u8,
                values_f64.len() * 8,
                values_f64.capacity() * 8,
            )
        };
        #[cfg(nightly)]
        let values = unsafe {
            ValueBlob::from_raw_parts_in(
                values_f64.as_mut_ptr() as *mut u8,
                values_f64.len() * 8,
                values_f64.capacity() * 8,
                crate::alloc::TursoAllocator,
            )
        };
        std::mem::forget(values_f64);
        Self {
            vector_type: VectorType::Float64Dense,
            dims,
            owned: Some(values),
            refer: None,
        }
    }
    #[turso_macros::allocation_site(crate::alloc::VectorAllocationSite::SparseConstruction)]
    pub fn from_f32_sparse(
        dims: usize,
        mut values_f32: Vec<f32>,
        mut idx_u32: Vec<u32>,
    ) -> std::result::Result<Self, TryReserveError> {
        #[cfg(not(nightly))]
        let mut values = unsafe {
            ValueBlob::from_raw_parts(
                values_f32.as_mut_ptr() as *mut u8,
                values_f32.len() * 4,
                values_f32.capacity() * 4,
            )
        };
        #[cfg(nightly)]
        let mut values = unsafe {
            ValueBlob::from_raw_parts_in(
                values_f32.as_mut_ptr() as *mut u8,
                values_f32.len() * 4,
                values_f32.capacity() * 4,
                crate::alloc::TursoAllocator,
            )
        };
        std::mem::forget(values_f32);

        #[cfg(not(nightly))]
        let idx = unsafe {
            ValueBlob::from_raw_parts(
                idx_u32.as_mut_ptr() as *mut u8,
                idx_u32.len() * 4,
                idx_u32.capacity() * 4,
            )
        };
        #[cfg(nightly)]
        let idx = unsafe {
            ValueBlob::from_raw_parts_in(
                idx_u32.as_mut_ptr() as *mut u8,
                idx_u32.len() * 4,
                idx_u32.capacity() * 4,
                crate::alloc::TursoAllocator,
            )
        };
        std::mem::forget(idx_u32);

        values.try_reserve(idx.len())?;
        values.extend_from_slice(&idx);
        Ok(Self {
            vector_type: VectorType::Float32Sparse,
            dims,
            owned: Some(values),
            refer: None,
        })
    }
    fn align4(n: usize) -> usize {
        n.div_ceil(4) * 4
    }

    pub fn from_1bit(dims: usize, bits: ValueBlob) -> Self {
        debug_assert!(bits.len() == dims.div_ceil(8));
        Self {
            vector_type: VectorType::Float1Bit,
            dims,
            owned: Some(bits),
            refer: None,
        }
    }

    #[turso_macros::allocation_site(crate::alloc::VectorAllocationSite::Float8Construction)]
    pub fn from_f8(
        dims: usize,
        quantized: ValueBlob,
        alpha: f32,
        shift: f32,
    ) -> std::result::Result<Self, TryReserveError> {
        let aligned = Self::align4(dims);
        let mut data = <ValueBlob as TursoTryWithCapacityExt>::try_with_capacity_ext(aligned + 8)?;
        data.extend_from_slice(&quantized);
        data.resize(aligned, 0); // alignment padding
        data.extend_from_slice(&alpha.to_le_bytes());
        data.extend_from_slice(&shift.to_le_bytes());
        debug_assert!(data.len() == aligned + 8);
        Ok(Self {
            vector_type: VectorType::Float8,
            dims,
            owned: Some(data),
            refer: None,
        })
    }

    pub fn from_vec(mut blob: ValueBlob) -> Result<Self> {
        let (vector_type, len, explicit_dims) = Self::vector_type(&blob)?;
        blob.truncate(len);
        Self::from_data_with_dims(vector_type, Some(blob), None, explicit_dims)
    }

    #[turso_macros::allocation_site(crate::alloc::VectorAllocationSite::IndexPayloadCopy)]
    pub fn from_slice_owned(blob: &[u8]) -> Result<Vector<'static>> {
        Vector::from_vec(value_blob_from_slice(blob)?)
    }

    pub fn from_slice(blob: &'a [u8]) -> Result<Self> {
        let (vector_type, len, explicit_dims) = Self::vector_type(blob)?;
        Self::from_data_with_dims(vector_type, None, Some(&blob[..len]), explicit_dims)
    }
    pub fn from_data(
        vector_type: VectorType,
        owned: Option<ValueBlob>,
        refer: Option<&'a [u8]>,
    ) -> Result<Self> {
        Self::from_data_with_dims(vector_type, owned, refer, 0)
    }

    fn from_data_with_dims(
        vector_type: VectorType,
        owned: Option<ValueBlob>,
        refer: Option<&'a [u8]>,
        explicit_dims: usize,
    ) -> Result<Self> {
        let owned_slice = owned.as_deref();
        let refer_slice = refer.as_ref().map(|&x| x);
        let data = owned_slice.or(refer_slice).ok_or_else(|| {
            LimboError::InternalError("Vector must have either owned or refer data".to_string())
        })?;
        match vector_type {
            VectorType::Float32Dense => {
                if data.len() % 4 != 0 {
                    return Err(LimboError::InvalidArgument(format!(
                        "f32 dense vector unexpected data length: {}",
                        data.len(),
                    )));
                }
                Ok(Vector {
                    vector_type,
                    dims: data.len() / 4,
                    owned,
                    refer,
                })
            }
            VectorType::Float64Dense => {
                if data.len() % 8 != 0 {
                    return Err(LimboError::InvalidArgument(format!(
                        "f64 dense vector unexpected data length: {}",
                        data.len(),
                    )));
                }
                Ok(Vector {
                    vector_type,
                    dims: data.len() / 8,
                    owned,
                    refer,
                })
            }
            VectorType::Float32Sparse => {
                if data.is_empty() || data.len() % 4 != 0 || (data.len() - 4) % 8 != 0 {
                    return Err(LimboError::InvalidArgument(format!(
                        "f32 sparse vector unexpected data length: {}",
                        data.len(),
                    )));
                }
                let original_len = data.len();
                let dims_bytes = &data[original_len - 4..];
                let dims = u32::from_le_bytes([
                    dims_bytes[0],
                    dims_bytes[1],
                    dims_bytes[2],
                    dims_bytes[3],
                ]) as usize;
                // Layout is [values: n * f32][idx: n * u32][dims: u32]. Every
                // `idx` entry comes from the blob and is used to index a dense
                // `dims` buffer, so check it here rather than in each consumer.
                let entries = (original_len - 4) / 8;
                for entry in data[entries * 4..entries * 8].chunks_exact(4) {
                    let index =
                        u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]) as usize;
                    if index >= dims {
                        return Err(LimboError::InvalidArgument(format!(
                            "f32 sparse vector index {index} out of range for {dims} dims"
                        )));
                    }
                }
                let owned = owned.map(|mut x| {
                    x.truncate(original_len - 4);
                    x
                });
                let refer = refer.map(|x| &x[0..original_len - 4]);
                Ok(Vector {
                    vector_type,
                    dims,
                    owned,
                    refer,
                })
            }
            VectorType::Float1Bit => {
                let expected_len = explicit_dims.div_ceil(8);
                if explicit_dims == 0 || data.len() != expected_len {
                    return Err(LimboError::InvalidArgument(format!(
                        "f1bit vector data length mismatch: got {} expected {} for {} dims",
                        data.len(),
                        expected_len,
                        explicit_dims,
                    )));
                }
                Ok(Vector {
                    vector_type,
                    dims: explicit_dims,
                    owned,
                    refer,
                })
            }
            VectorType::Float8 => {
                if data.len() < 8 {
                    return Err(LimboError::InvalidArgument(format!(
                        "f8 vector data too short: {}",
                        data.len(),
                    )));
                }
                let expected_len = Self::align4(explicit_dims) + 8;
                if explicit_dims == 0 || data.len() != expected_len {
                    return Err(LimboError::InvalidArgument(format!(
                        "f8 vector data length mismatch: got {} expected {} for {} dims",
                        data.len(),
                        expected_len,
                        explicit_dims,
                    )));
                }
                Ok(Vector {
                    vector_type,
                    dims: explicit_dims,
                    owned,
                    refer,
                })
            }
        }
    }

    pub fn bin_len(&self) -> usize {
        let owned = self.owned.as_ref().map(|x| x.len());
        let refer = self.refer.as_ref().map(|x| x.len());
        owned
            .or(refer)
            .expect("Vector invariant: exactly one of owned or refer must be Some")
    }

    pub fn bin_data(&'a self) -> &'a [u8] {
        let owned = self.owned.as_deref();
        let refer = self.refer.as_ref().map(|&x| x);
        owned
            .or(refer)
            .expect("Vector invariant: exactly one of owned or refer must be Some")
    }

    pub fn bin_eject(self) -> std::result::Result<ValueBlob, TryReserveError> {
        match self.owned {
            Some(owned) => Ok(owned),
            None => value_blob_from_slice(
                self.refer
                    .expect("Vector invariant: exactly one of owned or refer must be Some"),
            ),
        }
    }

    /// # Safety
    ///
    /// This method is used to reinterpret the underlying `Vec<u8>` data
    /// as a `&[f32]` slice. This is only valid if:
    /// - The buffer is correctly aligned for `f32`
    /// - The length of the buffer is exactly `dims * size_of::<f32>()`
    pub fn as_f32_slice(&self) -> &[f32] {
        turso_debug_assert!(self.vector_type == VectorType::Float32Dense);
        if self.dims == 0 {
            return &[];
        }

        assert_eq!(
            self.bin_len(),
            self.dims * std::mem::size_of::<f32>(),
            "data length must equal dims * size_of::<f32>()"
        );

        let ptr = self.bin_data().as_ptr();
        let align = std::mem::align_of::<f32>();
        assert_eq!(
            ptr.align_offset(align),
            0,
            "data pointer must be aligned to {align} bytes for f32 access"
        );

        unsafe { std::slice::from_raw_parts(ptr as *const f32, self.dims) }
    }

    /// # Safety
    ///
    /// This method is used to reinterpret the underlying `Vec<u8>` data
    /// as a `&[f64]` slice. This is only valid if:
    /// - The buffer is correctly aligned for `f64`
    /// - The length of the buffer is exactly `dims * size_of::<f64>()`
    pub fn as_f64_slice(&self) -> &[f64] {
        turso_debug_assert!(self.vector_type == VectorType::Float64Dense);
        if self.dims == 0 {
            return &[];
        }

        assert_eq!(
            self.bin_len(),
            self.dims * std::mem::size_of::<f64>(),
            "data length must equal dims * size_of::<f64>()"
        );

        let ptr = self.bin_data().as_ptr();
        let align = std::mem::align_of::<f64>();
        assert_eq!(
            ptr.align_offset(align),
            0,
            "data pointer must be aligned to {align} bytes for f64 access"
        );

        unsafe { std::slice::from_raw_parts(ptr as *const f64, self.dims) }
    }

    pub fn as_f32_sparse(&self) -> VectorSparse<'_, f32> {
        turso_debug_assert!(self.vector_type == VectorType::Float32Sparse);
        let ptr = self.bin_data().as_ptr();
        let align = std::mem::align_of::<f32>();
        assert_eq!(
            ptr.align_offset(align),
            0,
            "data pointer must be aligned to {align} bytes for f32 access"
        );
        let length = self.bin_data().len() / 4 / 2;
        let values = unsafe { std::slice::from_raw_parts(ptr as *const f32, length) };
        let idx = unsafe { std::slice::from_raw_parts((ptr as *const u32).add(length), length) };
        turso_debug_assert!(idx.is_sorted());
        VectorSparse { idx, values }
    }

    /// Returns the raw bit-packed bytes for a Float1Bit vector.
    /// Bit `i` is at byte `i/8`, position `i & 7`.
    pub fn as_1bit_data(&self) -> &[u8] {
        debug_assert!(self.vector_type == VectorType::Float1Bit);
        let data = self.bin_data();
        &data[..self.dims.div_ceil(8)]
    }

    /// Returns (quantized_bytes, alpha, shift) for a Float8 vector.
    /// Dequantization: `f_i = alpha * q_i + shift`
    pub fn as_f8_data(&self) -> (&[u8], f32, f32) {
        debug_assert!(self.vector_type == VectorType::Float8);
        let data = self.bin_data();
        let aligned = Self::align4(self.dims);
        let alpha = f32::from_le_bytes([
            data[aligned],
            data[aligned + 1],
            data[aligned + 2],
            data[aligned + 3],
        ]);
        let shift = f32::from_le_bytes([
            data[aligned + 4],
            data[aligned + 5],
            data[aligned + 6],
            data[aligned + 7],
        ]);
        (&data[..self.dims], alpha, shift)
    }
}

#[cfg(test)]
#[path = "../tests/unit/vector/vector_types/tests.rs"]
pub(crate) mod tests;
