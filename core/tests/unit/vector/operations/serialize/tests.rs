use super::vector_serialize;
use crate::{alloc::TursoVecExt, vector::vector_types::Vector, Value, ValueBlob};

fn assert_float32_move_preserves_allocation(bytes: &[u8]) {
    let mut blob = <ValueBlob as TursoVecExt<u8>>::with_capacity(bytes.len() + 4);
    blob.extend_from_slice(bytes);
    let pointer = blob.as_ptr();
    let capacity = blob.capacity();

    let vector = Vector::from_vec(blob).unwrap();
    let Value::Blob(blob) = vector_serialize(vector).unwrap() else {
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
