use crate::{
    vector::vector_types::{Vector, VectorType},
    Value,
};

pub fn vector_serialize(x: Vector) -> Value {
    match x.vector_type {
        VectorType::Float32Dense => Value::from_blob(x.bin_eject()),
        VectorType::Float64Dense => {
            let mut data = x.bin_eject();
            data.push(2);
            Value::from_blob(data)
        }
        VectorType::Float32Sparse => {
            let dims = x.dims;
            let mut data = x.bin_eject();
            data.extend_from_slice(&(dims as u32).to_le_bytes());
            data.push(9);
            Value::from_blob(data)
        }
        VectorType::Float1Bit => {
            // Format: [data bytes][optional padding][trailing_bits][0x03]
            let dims = x.dims;
            let data_size = dims.div_ceil(8);
            let needs_padding = data_size % 2 == 0;
            let mut blob = x.bin_eject();
            blob.truncate(data_size);
            if needs_padding {
                blob.push(0); // padding
            }
            let blob_size = blob.len() + 2;
            let trailing_bits = (blob_size - 1) * 8 - dims;
            blob.push(trailing_bits as u8);
            blob.push(3); // type byte
            Value::from_blob(blob)
        }
        VectorType::Float8 => {
            // Format: [quantized bytes][alignment padding][alpha f32][shift f32][padding 0x00][trailing_bytes][0x04]
            let dims = x.dims;
            let mut data = x.bin_eject(); // ALIGN(dims, 4) + 8 bytes
            let trailing_bytes = dims.div_ceil(4) * 4 - dims;
            data.push(0); // padding
            data.push(trailing_bytes as u8);
            data.push(4); // type byte
            Value::from_blob(data)
        }
    }
}

#[cfg(all(test, nightly))]
mod tests {
    use super::vector_serialize;
    use crate::{alloc::TursoVecExt, vector::vector_types::Vector, Value, ValueBlob};

    fn assert_float32_move_preserves_allocation(bytes: &[u8]) {
        let mut blob = <ValueBlob as TursoVecExt<u8>>::with_capacity(bytes.len() + 4);
        blob.extend_from_slice(bytes);
        let pointer = blob.as_ptr();
        let capacity = blob.capacity();

        let vector = Vector::from_vec(blob).unwrap();
        let Value::Blob(blob) = vector_serialize(vector) else {
            panic!("expected blob value");
        };

        assert_eq!(blob.as_ptr(), pointer);
        assert_eq!(blob.capacity(), capacity);
        assert_eq!(blob.as_slice(), bytes);
    }

    #[test]
    fn float32_serialization_moves_value_blob_allocation() {
        assert_float32_move_preserves_allocation(&[]);
        assert_float32_move_preserves_allocation(&1.0f32.to_le_bytes());
    }
}
