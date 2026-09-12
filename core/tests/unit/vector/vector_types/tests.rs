use crate::alloc::{TursoIteratorExt, TursoVecExt, ALLOC_ERR_MSG};
use crate::vector::operations;

use super::*;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;

// Helper to generate arbitrary vectors of specific type and dimensions
#[derive(Debug, Clone)]
pub struct ArbitraryVector<const DIMS: usize> {
    vector_type: VectorType,
    data: Vec<u8>,
}

/// How to create an arbitrary vector of DIMS dims.
impl<const DIMS: usize> ArbitraryVector<DIMS> {
    fn generate_f32_vector(g: &mut Gen) -> Vec<f32> {
        (0..DIMS)
            .map(|_| {
                loop {
                    // generate zeroes with some probability since we have support for sparse vectors
                    if bool::arbitrary(g) {
                        return 0.0;
                    }
                    let f = f32::arbitrary(g);
                    // f32::arbitrary() can generate "problem values" like NaN, infinity, and very small values
                    // Skip these values
                    if f.is_finite() && f.abs() >= 1e-6 {
                        // Scale to [-1, 1] range
                        return f % 2.0 - 1.0;
                    }
                }
            })
            .try_collect()
            .expect(ALLOC_ERR_MSG)
    }

    fn generate_f64_vector(g: &mut Gen) -> Vec<f64> {
        (0..DIMS)
            .map(|_| {
                loop {
                    // generate zeroes with some probability since we have support for sparse vectors
                    if bool::arbitrary(g) {
                        return 0.0;
                    }
                    let f = f64::arbitrary(g);
                    // f64::arbitrary() can generate "problem values" like NaN, infinity, and very small values
                    // Skip these values
                    if f.is_finite() && f.abs() >= 1e-6 {
                        // Scale to [-1, 1] range
                        return f % 2.0 - 1.0;
                    }
                }
            })
            .try_collect()
            .expect(ALLOC_ERR_MSG)
    }
}

/// Convert an ArbitraryVector to a Vector.
impl<const DIMS: usize> TryFrom<ArbitraryVector<DIMS>> for Vector<'static> {
    type Error = LimboError;

    fn try_from(v: ArbitraryVector<DIMS>) -> Result<Self> {
        Vector::from_data_with_dims(v.vector_type, Some(v.data), None, DIMS)
    }
}

/// Implement the quickcheck Arbitrary trait for ArbitraryVector.
impl<const DIMS: usize> Arbitrary for ArbitraryVector<DIMS> {
    fn arbitrary(g: &mut Gen) -> Self {
        let choice = u8::arbitrary(g) % 4;
        let vector_type = match choice {
            0 => VectorType::Float32Dense,
            1 => VectorType::Float64Dense,
            2 => VectorType::Float1Bit,
            _ => VectorType::Float8,
        };

        let data = match vector_type {
            VectorType::Float32Dense => {
                let floats = Self::generate_f32_vector(g);
                floats
                    .iter()
                    .flat_map(|f| f.to_le_bytes())
                    .try_collect()
                    .expect(ALLOC_ERR_MSG)
            }
            VectorType::Float64Dense => {
                let floats = Self::generate_f64_vector(g);
                floats
                    .iter()
                    .flat_map(|f| f.to_le_bytes())
                    .try_collect()
                    .expect(ALLOC_ERR_MSG)
            }
            VectorType::Float1Bit => {
                // Generate random bits
                let byte_count = DIMS.div_ceil(8);
                let mut bits = crate::alloc::vec![0u8; byte_count];
                for b in bits.iter_mut() {
                    *b = u8::arbitrary(g);
                }
                // Mask off unused bits in the last byte
                if DIMS % 8 != 0 {
                    let mask = (1u8 << (DIMS % 8)) - 1;
                    if let Some(last) = bits.last_mut() {
                        *last &= mask;
                    }
                }
                bits
            }
            VectorType::Float8 => {
                // Generate random quantized values + alpha/shift
                let floats = Self::generate_f32_vector(g);
                let min_val = floats.iter().cloned().fold(f32::INFINITY, f32::min);
                let max_val = floats.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
                let alpha = (max_val - min_val) / 255.0;
                let shift = min_val;
                let aligned = DIMS.div_ceil(4) * 4;
                let mut data: Vec<u8> = TursoVecExt::with_capacity(aligned + 8);
                for &f in &floats {
                    let q = if alpha == 0.0 {
                        0u8
                    } else {
                        ((f - shift) / alpha + 0.5) as u8
                    };
                    data.push(q);
                }
                data.resize(aligned, 0); // alignment padding
                data.extend_from_slice(&alpha.to_le_bytes());
                data.extend_from_slice(&shift.to_le_bytes());
                data
            }
            _ => unreachable!(),
        };

        ArbitraryVector { vector_type, data }
    }
}

#[quickcheck]
fn prop_vector_type_identification_2d(v: ArbitraryVector<2>) -> bool {
    test_vector_type::<2>(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_vector_type_identification_3d(v: ArbitraryVector<3>) -> bool {
    test_vector_type::<3>(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_vector_type_identification_4d(v: ArbitraryVector<4>) -> bool {
    test_vector_type::<4>(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_vector_type_identification_100d(v: ArbitraryVector<100>) -> bool {
    test_vector_type::<100>(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_vector_type_identification_1536d(v: ArbitraryVector<1536>) -> bool {
    test_vector_type::<1536>(v.try_into().expect("generated vector must be valid"))
}

/// Test if the vector type identification is correct for a given vector.
fn test_vector_type<const DIMS: usize>(v: Vector) -> bool {
    let vtype = v.vector_type;
    let value = operations::serialize::vector_serialize(v).unwrap();
    let blob = value.to_blob().unwrap().to_vec();
    match Vector::vector_type(&blob) {
        Ok((detected_type, _, _)) => detected_type == vtype,
        Err(_) => false,
    }
}

#[quickcheck]
fn prop_slice_conversion_safety_2d(v: ArbitraryVector<2>) -> bool {
    test_slice_conversion::<2>(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_slice_conversion_safety_3d(v: ArbitraryVector<3>) -> bool {
    test_slice_conversion::<3>(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_slice_conversion_safety_4d(v: ArbitraryVector<4>) -> bool {
    test_slice_conversion::<4>(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_slice_conversion_safety_100d(v: ArbitraryVector<100>) -> bool {
    test_slice_conversion::<100>(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_slice_conversion_safety_1536d(v: ArbitraryVector<1536>) -> bool {
    test_slice_conversion::<1536>(v.try_into().expect("generated vector must be valid"))
}

/// Test if the slice conversion is safe for a given vector:
/// - The slice length matches the dimensions
/// - The data length is correct (4 bytes per float for f32, 8 bytes per float for f64)
fn test_slice_conversion<const DIMS: usize>(v: Vector) -> bool {
    match v.vector_type {
        VectorType::Float32Dense => {
            let slice = v.as_f32_slice();
            slice.len() == DIMS && (slice.len() * 4 == v.bin_len())
        }
        VectorType::Float64Dense => {
            let slice = v.as_f64_slice();
            slice.len() == DIMS && (slice.len() * 8 == v.bin_len())
        }
        VectorType::Float1Bit => {
            let data = v.as_1bit_data();
            data.len() == DIMS.div_ceil(8) && v.dims == DIMS
        }
        VectorType::Float8 => {
            let (quantized, _alpha, _shift) = v.as_f8_data();
            quantized.len() == DIMS && v.dims == DIMS
        }
        _ => true,
    }
}

#[quickcheck]
fn prop_vector_distance_safety_2d(v1: ArbitraryVector<2>, v2: ArbitraryVector<2>) -> bool {
    test_vector_distance::<2>(
        &v1.try_into().expect("generated vector must be valid"),
        &v2.try_into().expect("generated vector must be valid"),
    )
}

#[quickcheck]
fn prop_vector_distance_safety_3d(v1: ArbitraryVector<3>, v2: ArbitraryVector<3>) -> bool {
    test_vector_distance::<3>(
        &v1.try_into().expect("generated vector must be valid"),
        &v2.try_into().expect("generated vector must be valid"),
    )
}

#[quickcheck]
fn prop_vector_distance_safety_4d(v1: ArbitraryVector<4>, v2: ArbitraryVector<4>) -> bool {
    test_vector_distance::<4>(
        &v1.try_into().expect("generated vector must be valid"),
        &v2.try_into().expect("generated vector must be valid"),
    )
}

#[quickcheck]
fn prop_vector_distance_safety_100d(v1: ArbitraryVector<100>, v2: ArbitraryVector<100>) -> bool {
    test_vector_distance::<100>(
        &v1.try_into().expect("generated vector must be valid"),
        &v2.try_into().expect("generated vector must be valid"),
    )
}

#[quickcheck]
fn prop_vector_distance_safety_1536d(v1: ArbitraryVector<1536>, v2: ArbitraryVector<1536>) -> bool {
    test_vector_distance::<1536>(
        &v1.try_into().expect("generated vector must be valid"),
        &v2.try_into().expect("generated vector must be valid"),
    )
}

/// Test if the vector distance calculation is correct for a given pair of vectors:
/// - Skips cases with invalid input vectors.
/// - Assumes vectors are well-formed (same type and dimension)
fn test_vector_distance<const DIMS: usize>(v1: &Vector, v2: &Vector) -> bool {
    match operations::distance_cos::vector_distance_cos(v1, v2) {
        Ok(distance) => {
            if distance.is_nan() {
                return true;
            }
            match v1.vector_type {
                // Float1Bit cosine distance returns hamming distance [0, dims]
                VectorType::Float1Bit => (0.0 - 1e-6..=DIMS as f64 + 1e-6).contains(&distance),
                // Normal cosine distance [0, 2]
                _ => (0.0 - 1e-6..=2.0 + 1e-6).contains(&distance),
            }
        }
        Err(_) => true,
    }
}

#[test]
fn test_vector_some_cosine_dist() {
    let a = Vector {
        vector_type: VectorType::Float32Dense,
        dims: 2,
        owned: Some(crate::alloc::vec![0, 0, 0, 0, 52, 208, 106, 63]),
        refer: None,
    };
    let b = Vector {
        vector_type: VectorType::Float32Dense,
        dims: 2,
        owned: Some(crate::alloc::vec![0, 0, 0, 0, 58, 100, 45, 192]),
        refer: None,
    };
    assert!((operations::distance_cos::vector_distance_cos(&a, &b).unwrap() - 2.0).abs() <= 1e-6);
}

/// Malformed float1bit/float8 blobs encode a dimension count that would underflow
/// the size arithmetic in `Vector::vector_type`. They must be rejected with an
/// error instead of panicking (debug) or wrapping into a huge `dims` that
/// out-of-bounds slices the blob in `from_slice` (release).
#[test]
fn malformed_1bit_and_f8_blobs_are_rejected() {
    // float1bit, trailing_bits = 0xFF but the blob only holds 16 bits
    assert!(Vector::from_slice(&[0x00, 0xFF, 0x03]).is_err());
    // float1bit, trailing_bits equals the whole blob capacity -> zero dims
    assert!(Vector::from_slice(&[0x00, 0x10, 0x03]).is_err());
    // float8, blob far shorter than the mandatory alpha/shift header
    assert!(Vector::from_slice(&[0x00, 0x00, 0x04]).is_err());
    assert!(Vector::from_slice(&[0x00, 0x00, 0x00, 0x0A, 0x04]).is_err());
}

/// No single-byte perturbation of the trailing marker may panic for any blob
/// length: `vector_type` and `from_slice` must always fall back to an error.
#[test]
fn arbitrary_1bit_and_f8_trailing_bytes_never_panic() {
    for type_byte in [0x03u8, 0x04u8] {
        for len in 1..=24usize {
            for trailing in 0..=255u8 {
                let mut blob = crate::alloc::vec![0u8; len];
                if let Some(last) = blob.last_mut() {
                    *last = trailing;
                }
                blob.push(type_byte);
                // Must not panic; either parses or reports a clean error.
                let _ = Vector::from_slice(&blob);
            }
        }
    }
}

/// Valid float1bit/float8 blobs keep round-tripping through the size arithmetic.
#[test]
fn valid_1bit_and_f8_blobs_still_parse() {
    for dims in 1..=40usize {
        for vector_type in [VectorType::Float1Bit, VectorType::Float8] {
            let text = format!(
                "[{}]",
                (0..dims)
                    .map(|i| ((i % 3) as f32 - 1.0).to_string())
                    .collect::<std::vec::Vec<_>>()
                    .join(",")
            );
            let vector = operations::text::vector_from_text(vector_type, &text).unwrap();
            let blob = operations::serialize::vector_serialize(vector)
                .unwrap()
                .to_blob()
                .unwrap()
                .to_vec();
            let parsed = Vector::from_slice(&blob)
                .unwrap_or_else(|e| panic!("{vector_type:?} with {dims} dims: {e}"));
            assert_eq!(parsed.vector_type, vector_type);
            assert_eq!(parsed.dims, dims);
        }
    }
}

/// Every sparse consumer (vector_to_text, vector_convert) uses `idx` to index a
/// dense buffer of `dims` elements, and both fields come straight from the blob.
/// Out-of-range indices must be rejected while the blob is parsed, not when a
/// particular consumer happens to trip over them.
#[test]
fn sparse_vector_index_out_of_range_is_rejected() {
    // values = [1.0], idx = [0xFFFFFFFF], dims = 1
    let err = Vector::from_slice(&[
        0x00, 0x00, 0x80, 0x3F, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00, 0x09,
    ])
    .expect_err("out of range sparse index must be rejected");
    assert!(matches!(err, LimboError::InvalidArgument(_)), "{err}");

    // values = [1.0], idx = [1], dims = 1 -- one past the end is still out of range
    assert!(Vector::from_slice(&[
        0x00, 0x00, 0x80, 0x3F, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x09,
    ])
    .is_err());
}

/// A well-formed sparse blob keeps parsing and densifying correctly.
#[test]
fn valid_sparse_vector_still_parses() {
    // values = [1.0], idx = [0], dims = 3. Parsed from an owned blob so the
    // pointer is f32-aligned, the way blobs reaching the SQL layer are.
    let vector = Vector::from_slice_owned(&[
        0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x09,
    ])
    .expect("well-formed sparse vector must parse");
    assert_eq!(vector.vector_type, VectorType::Float32Sparse);
    assert_eq!(vector.dims, 3);
    assert_eq!(operations::text::vector_to_text(&vector), "[1,0,0]");
    let dense = operations::convert::vector_convert(vector, VectorType::Float32Dense).unwrap();
    assert_eq!(dense.as_f32_slice(), [1.0, 0.0, 0.0]);
}

#[test]
fn parse_string_vector_zero_length() {
    let vector = operations::text::vector_from_text(VectorType::Float32Dense, "[]").unwrap();
    assert_eq!(vector.dims, 0);
    assert_eq!(vector.vector_type, VectorType::Float32Dense);
}

#[test]
fn parse_string_vector_f8_zero_length() {
    let vector = operations::text::vector_from_text(VectorType::Float8, "[]").unwrap();
    assert_eq!(vector.dims, 0);
    assert_eq!(vector.vector_type, VectorType::Float8);
    let (quantized, alpha, shift) = vector.as_f8_data();
    assert!(quantized.is_empty());
    assert_eq!(alpha, 0.0);
    assert_eq!(shift, 0.0);
}

#[test]
fn test_parse_string_vector_valid_whitespace() {
    let vector = operations::text::vector_from_text(
        VectorType::Float32Dense,
        "  [  1.0  ,  2.0  ,  3.0  ]  ",
    )
    .unwrap();
    assert_eq!(vector.dims, 3);
    assert_eq!(vector.vector_type, VectorType::Float32Dense);
}

#[test]
fn test_parse_string_vector_valid() {
    let vector =
        operations::text::vector_from_text(VectorType::Float32Dense, "[1.0, 2.0, 3.0]").unwrap();
    assert_eq!(vector.dims, 3);
    assert_eq!(vector.vector_type, VectorType::Float32Dense);
}

#[quickcheck]
fn prop_vector_text_roundtrip_2d(v: ArbitraryVector<2>) -> bool {
    test_vector_text_roundtrip(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_vector_text_roundtrip_3d(v: ArbitraryVector<3>) -> bool {
    test_vector_text_roundtrip(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_vector_text_roundtrip_4d(v: ArbitraryVector<4>) -> bool {
    test_vector_text_roundtrip(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_vector_text_roundtrip_100d(v: ArbitraryVector<100>) -> bool {
    test_vector_text_roundtrip(v.try_into().expect("generated vector must be valid"))
}

#[quickcheck]
fn prop_vector_text_roundtrip_1536d(v: ArbitraryVector<1536>) -> bool {
    test_vector_text_roundtrip(v.try_into().expect("generated vector must be valid"))
}

/// Test that a vector can be converted to text and back without loss of precision
fn test_vector_text_roundtrip(v: Vector) -> bool {
    // Convert to text
    let text = operations::text::vector_to_text(&v);

    // Parse back from text
    let parsed = operations::text::vector_from_text(v.vector_type, &text);

    match parsed {
        Ok(parsed_vector) => {
            // Check dimensions match
            if v.dims != parsed_vector.dims {
                return false;
            }

            match v.vector_type {
                VectorType::Float32Dense => {
                    let original = v.as_f32_slice();
                    let parsed = parsed_vector.as_f32_slice();
                    original.iter().zip(parsed.iter()).all(|(a, b)| a == b)
                }
                VectorType::Float64Dense => {
                    let original = v.as_f64_slice();
                    let parsed = parsed_vector.as_f64_slice();
                    original.iter().zip(parsed.iter()).all(|(a, b)| a == b)
                }
                VectorType::Float1Bit => {
                    // 1bit text roundtrip: bits → +1/-1 text → threshold → bits
                    let original = v.as_1bit_data();
                    let parsed_data = parsed_vector.as_1bit_data();
                    original == parsed_data
                }
                VectorType::Float8 => {
                    // Float8 text roundtrip: lossy due to quantization
                    // Just check dims match (re-quantization changes alpha/shift)
                    true
                }
                _ => true,
            }
        }
        Err(_) => false,
    }
}
