use super::*;

fn shared_bytes(bytes: &[u8]) -> Arc<DynBoxedSlice<u8>> {
    let mut data = <crate::alloc::DynVec<u8> as crate::alloc::TursoVecInExt<
        u8,
        crate::alloc::DynAllocator,
    >>::try_with_capacity_in(bytes.len(), crate::alloc::DynAllocator::default())
    .expect("failed to allocate shared buffer test data");
    data.extend_from_slice(bytes);
    Arc::new(data.into_boxed_slice())
}

#[test]
fn shared_buffer_exposes_arc_bytes() {
    let data = shared_bytes(&[1, 2, 3, 4]);
    let buffer = Buffer::new_shared(data.clone());

    assert_eq!(buffer.len(), 4);
    assert_eq!(buffer.as_slice(), &[1, 2, 3, 4]);
    assert_eq!(buffer.as_ptr(), data.as_ref().as_ptr());
    assert!(!buffer.is_heap());
    assert!(!buffer.is_pooled());
}

#[test]
fn shared_buffer_view_exposes_tail_without_copying() {
    let data = shared_bytes(&[0, 1, 2, 3, 4]);
    let shared = SharedBufferData::new_view(data.clone(), 2);
    let buffer = Buffer::new_shared_data(shared.clone());

    assert_eq!(shared.len(), 3);
    assert_eq!(shared.as_slice(), &[2, 3, 4]);
    assert_eq!(shared.as_ptr(), unsafe { data.as_ref().as_ptr().add(2) });
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[2, 3, 4]);
    assert_eq!(buffer.as_ptr(), shared.as_ptr());
}
