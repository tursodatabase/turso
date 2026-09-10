use common::OwnedBytes;
use tantivy_sstable::{Dictionary, MonotonicU64SSTable, VecU32ValueSSTable};

#[test]
fn test_create_and_search_sstable() {
    // Create a new sstable in memory.
    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
    builder.insert(b"apple", &1).unwrap();
    builder.insert(b"banana", &2).unwrap();
    builder.insert(b"orange", &3).unwrap();
    let sstable_bytes = builder.finish().unwrap();

    // Open the sstable.
    let sstable =
        Dictionary::<MonotonicU64SSTable>::from_bytes(OwnedBytes::new(sstable_bytes)).unwrap();

    // Search for a key.
    let value = sstable.get(b"banana").unwrap();
    assert_eq!(value, Some(2));

    // Search for a non-existent key.
    let value = sstable.get(b"blub").unwrap();
    assert_eq!(value, None);
}

#[test]
fn test_custom_value_sstable() {
    // Create a new sstable with custom values.
    let mut builder = Dictionary::<VecU32ValueSSTable>::builder(Vec::new()).unwrap();
    builder.set_block_len(4096); // Ensure both values are in the same block
    builder.insert(b"first", &vec![1, 2, 3]).unwrap();
    builder.insert(b"second", &vec![4, 5]).unwrap();
    let sstable_bytes = builder.finish().unwrap();

    // Open the sstable.
    let sstable =
        Dictionary::<VecU32ValueSSTable>::from_bytes(OwnedBytes::new(sstable_bytes)).unwrap();

    let mut stream = sstable.stream().unwrap();
    assert!(stream.advance());
    assert_eq!(stream.key(), b"first");
    assert_eq!(stream.value(), &vec![1, 2, 3]);

    assert!(stream.advance());
    assert_eq!(stream.key(), b"second");
    assert_eq!(stream.value(), &vec![4, 5]);

    assert!(!stream.advance());
}
