use super::*;

#[test]
fn fragment_chunks_preserve_wire_format() {
    let metadata = [3];
    let mut bytes = Vec::new();
    LogSerializer::new(&mut bytes)
        .write(PortableChangePayload::new(1, 2, &metadata).into_chunks())
        .unwrap();

    assert_eq!(bytes, [5, 8, 1, 16, 2, 3]);
}

#[test]
fn chunk_length_mismatch_does_not_change_buffer() {
    struct WrongLength;

    impl LogChunkStream for WrongLength {
        fn encoded_len(&self) -> Option<usize> {
            Some(0)
        }

        fn copy_to(self, writer: &mut LogChunkWriter) -> Result<()> {
            writer.copy_from_slice(&[1])
        }
    }

    let mut buffer = vec![2];
    let result = LogSerializer::new(&mut buffer).write(WrongLength);

    assert!(result.is_err());
    assert_eq!(buffer, [2]);
}

#[test]
fn insert_preserves_surrounding_bytes() {
    let mut buffer = vec![1, 4];
    LogSerializer::new(&mut buffer).insert(1, [2, 3]).unwrap();

    assert_eq!(buffer, [1, 2, 3, 4]);
}

#[test]
fn portable_extension_insert_preserves_wire_format() {
    let mut buffer = vec![9, 10];
    let payload = PortableChangePayload::new(1, 2, &[3]);
    let record = ExtensionRecord::new(0x1234, 0x5678, payload);

    LogSerializer::new(&mut buffer)
        .insert_portable_extension(1, record)
        .unwrap();

    assert_eq!(
        buffer,
        [9, 0x34, 0x12, 0x78, 0x56, 6, 0, 0, 0, 5, 8, 1, 16, 2, 3, 10,]
    );
}
