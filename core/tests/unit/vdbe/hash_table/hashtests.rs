use super::*;
use crate::alloc::vec;
use crate::io::Buffer;
use crate::MemoryIO;

#[test]
fn test_hash_table_rejects_custom_collations() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        collations: vec![CollationSeq::custom("hash_table_custom")],
        ..Default::default()
    };
    let err = match HashTable::new(config, io) {
        Ok(_) => panic!("custom-collated hash table should be rejected"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("custom collations are not supported by hash tables"));
}

#[test]
fn test_hash_function_consistency() {
    // Test that the same keys produce the same hash
    let keys1 = vec![
        ValueRef::from_i64(42),
        ValueRef::Text(crate::types::TextRef::new(
            "hello",
            crate::types::TextSubtype::Text,
        )),
    ];
    let keys2 = vec![
        ValueRef::from_i64(42),
        ValueRef::Text(crate::types::TextRef::new(
            "hello",
            crate::types::TextSubtype::Text,
        )),
    ];
    let keys3 = vec![
        ValueRef::from_i64(43),
        ValueRef::Text(crate::types::TextRef::new(
            "hello",
            crate::types::TextSubtype::Text,
        )),
    ];

    let collations = vec![CollationSeq::Binary, CollationSeq::Binary];
    let hash1 = hash_join_key(&keys1, &collations);
    let hash2 = hash_join_key(&keys2, &collations);
    let hash3 = hash_join_key(&keys3, &collations);

    assert_eq!(hash1, hash2);
    assert_ne!(hash1, hash3);
}

#[test]
fn test_hash_function_numeric_equivalence() {
    let collations = vec![CollationSeq::Binary];

    // Zero variants should hash identically
    let h_zero = hash_join_key(&[ValueRef::from_f64(0.0)], &collations);
    let h_neg_zero = hash_join_key(&[ValueRef::from_f64(-0.0)], &collations);
    let h_int_zero = hash_join_key(&[ValueRef::from_i64(0)], &collations);
    assert_eq!(h_zero, h_neg_zero);
    assert_eq!(h_zero, h_int_zero);

    // Integer/float representations of the same numeric value should match
    let h_ten_int = hash_join_key(&[ValueRef::from_i64(10)], &collations);
    let h_ten_float = hash_join_key(&[ValueRef::from_f64(10.0)], &collations);
    assert_eq!(h_ten_int, h_ten_float);

    let h_neg_ten_int = hash_join_key(&[ValueRef::from_i64(-10)], &collations);
    let h_neg_ten_float = hash_join_key(&[ValueRef::from_f64(-10.0)], &collations);
    assert_eq!(h_neg_ten_int, h_neg_ten_float);

    // Positive/negative values should still differ
    assert_ne!(h_ten_int, h_neg_ten_int);
}

#[test]
fn test_keys_equal() {
    let key1 = vec![Value::from_i64(42), Value::Text("hello".to_string().into())];
    let key2 = vec![
        ValueRef::from_i64(42),
        ValueRef::Text(crate::types::TextRef::new(
            "hello",
            crate::types::TextSubtype::Text,
        )),
    ];
    let key3 = vec![
        ValueRef::from_i64(43),
        ValueRef::Text(crate::types::TextRef::new(
            "hello",
            crate::types::TextSubtype::Text,
        )),
    ];

    let collations = vec![CollationSeq::Binary, CollationSeq::Binary];
    assert!(keys_equal(&key1, &key2, &collations));
    assert!(!keys_equal(&key1, &key3, &collations));
}

#[test]
fn test_hash_table_basic() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert some entries (late materialization - only store rowids)
    let key1 = vec![Value::from_i64(1)];
    let _ = ht.insert(key1.clone(), 100, vec![], None).unwrap();

    let key2 = vec![Value::from_i64(2)];
    let _ = ht.insert(key2.clone(), 200, vec![], None).unwrap();

    let _ = ht.finalize_build(None);
    let mut metrics = HashJoinMetrics::default();

    // Probe for key1
    let result = ht.probe(key1, Some(&mut metrics)).unwrap();
    assert!(result.is_some());
    let entry1 = result.unwrap();
    assert_eq!(entry1.key_values[0].as_ref(), ValueRef::from_i64(1));
    assert_eq!(entry1.rowid, 100);

    // Probe for key2
    let result = ht.probe(key2, Some(&mut metrics)).unwrap();
    assert!(result.is_some());
    let entry2 = result.unwrap();
    assert_eq!(entry2.key_values[0].as_ref(), ValueRef::from_i64(2));
    assert_eq!(entry2.rowid, 200);

    // Probe for non-existent key
    let result = ht
        .probe(vec![Value::from_i64(999)], Some(&mut metrics))
        .unwrap();
    assert!(result.is_none());
    assert_eq!(metrics.probe_calls, 3);
}

#[test]
fn test_hash_table_collisions() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 2, // Small number to force collisions
        mem_budget: 1024 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert multiple entries (late materialization - only store rowids)
    for i in 0..10 {
        let key = vec![Value::from_i64(i)];
        let _ = ht.insert(key, i * 100, vec![], None).unwrap();
    }

    let _ = ht.finalize_build(None);

    // Verify all entries can be found
    for i in 0..10 {
        let result = ht.probe(vec![Value::from_i64(i)], None).unwrap();
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key_values[0].as_ref(), ValueRef::from_i64(i));
        assert_eq!(entry.rowid, i * 100);
    }
}

#[test]
fn test_hash_table_duplicate_keys() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert multiple entries with the same key
    let key = vec![Value::from_i64(42)];
    for i in 0..3 {
        let _ = ht.insert(key.clone(), 1000 + i, vec![], None).unwrap();
    }

    let _ = ht.finalize_build(None);

    // Probe should return first match
    let result = ht.probe(key, None).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().rowid, 1000);

    // next_match should return additional matches
    let result2 = ht.next_match().unwrap();
    assert!(result2.is_some());
    assert_eq!(result2.unwrap().rowid, 1001);

    let result3 = ht.next_match().unwrap();
    assert!(result3.is_some());
    assert_eq!(result3.unwrap().rowid, 1002);

    // No more matches
    let result4 = ht.next_match().unwrap();
    assert!(result4.is_none());
}

#[test]
fn test_hash_entry_serialization() {
    // Test that entries serialize and deserialize correctly
    let entry = HashEntry::new(
        12345,
        vec![
            Value::from_i64(42),
            Value::Text("hello".to_string().into()),
            Value::Null,
            Value::from_f64(std::f64::consts::PI),
        ],
        100,
    );

    let mut buf = vec![];
    entry.serialize(&mut buf).unwrap();

    let (deserialized, consumed) = HashEntry::deserialize(&buf).unwrap();
    assert_eq!(consumed, buf.len());
    assert_eq!(deserialized.hash, entry.hash);
    assert_eq!(deserialized.rowid, entry.rowid);
    assert_eq!(deserialized.key_values.len(), entry.key_values.len());

    for (v1, v2) in deserialized.key_values.iter().zip(entry.key_values.iter()) {
        match (v1, v2) {
            (Value::Numeric(Numeric::Integer(i1)), Value::Numeric(Numeric::Integer(i2))) => {
                assert_eq!(i1, i2)
            }
            (Value::Text(t1), Value::Text(t2)) => assert_eq!(t1.as_str(), t2.as_str()),
            (Value::Numeric(Numeric::Float(f1)), Value::Numeric(Numeric::Float(f2))) => {
                assert!((f64::from(*f1) - f64::from(*f2)).abs() < 1e-10)
            }
            (Value::Null, Value::Null) => {}
            _ => panic!("Value type mismatch"),
        }
    }
}

#[test]
fn test_serialize_to_slice_matches_serialize() {
    // Test that serialize_to_slice produces identical output to serialize
    let entry = HashEntry::new_with_payload(
        12345,
        vec![
            Value::from_i64(42),
            Value::Text("hello world".to_string().into()),
            Value::Null,
            Value::from_f64(std::f64::consts::PI),
        ],
        100,
        vec![
            Value::from_slice(&[1, 2, 3, 4, 5]).expect(crate::alloc::ALLOC_ERR_MSG),
            Value::from_i64(-999),
        ],
    );

    // Serialize using the Vec-based method
    let mut vec_buf = vec![];
    entry.serialize(&mut vec_buf).unwrap();

    // Serialize using the slice-based method
    let size = entry.serialized_size();
    assert_eq!(
        size,
        vec_buf.len(),
        "serialized_size must match actual size"
    );

    let mut slice_buf = vec![0u8; size];
    let written = entry.serialize_to_slice(&mut slice_buf);
    assert_eq!(written, size, "bytes written must match serialized_size");

    // Both methods must produce identical output
    assert_eq!(
        vec_buf, slice_buf,
        "serialize and serialize_to_slice must produce identical output"
    );

    // Verify the output is valid by deserializing
    let (deserialized, consumed) = HashEntry::deserialize(&slice_buf).unwrap();
    assert_eq!(consumed, size);
    assert_eq!(deserialized.hash, entry.hash);
    assert_eq!(deserialized.rowid, entry.rowid);
    assert_eq!(deserialized.key_values.len(), entry.key_values.len());
    assert_eq!(
        deserialized.payload_values.len(),
        entry.payload_values.len()
    );
}

#[test]
fn test_partition_from_hash() {
    // Test partition distribution
    let partitioning = Partitioning::new(16);
    let mut counts = [0usize; 16];
    for i in 0u64..10000 {
        let hash = i.wrapping_mul(0x9E3779B97F4A7C15); // Simple hash spreading
        let partition = partitioning.index(hash);
        assert!(partition < counts.len());
        counts[partition] += 1;
    }

    // Check reasonable distribution (each partition should have some entries)
    for count in counts {
        assert!(count > 0, "Each partition should have some entries");
    }
}

#[test]
fn test_spill_chunk_tracking() {
    // Test that SpilledPartition can track multiple chunks
    let mut partition = SpilledPartition::new(5);
    assert_eq!(partition.partition_idx, 5);
    assert!(partition.chunks.is_empty());
    assert_eq!(partition.total_size_bytes(), 0);
    assert_eq!(partition.total_num_entries(), 0);

    // Add first chunk
    partition.add_chunk(0, 1000, 50).unwrap();
    assert_eq!(partition.chunks.len(), 1);
    assert_eq!(partition.total_size_bytes(), 1000);
    assert_eq!(partition.total_num_entries(), 50);

    // Add second chunk
    partition.add_chunk(1000, 500, 25).unwrap();
    assert_eq!(partition.chunks.len(), 2);
    assert_eq!(partition.total_size_bytes(), 1500);
    assert_eq!(partition.total_num_entries(), 75);

    // Check individual chunks
    assert_eq!(partition.chunks[0].file_offset, 0);
    assert_eq!(partition.chunks[0].size_bytes, 1000);
    assert_eq!(partition.chunks[1].file_offset, 1000);
    assert_eq!(partition.chunks[1].size_bytes, 500);
}

#[test]
fn test_partition_count_override() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: Some(64),
    };
    let mut ht = HashTable::new(config, io).unwrap();
    insert_many_force_spill(&mut ht, 0, 1024);
    let _ = ht.finalize_build(None).unwrap();
    assert!(ht.has_spilled());

    let spill_state = ht.spill_state.as_ref().expect("spill state exists");
    assert_eq!(spill_state.partitioning.count, 64);
}

#[test]
fn test_adaptive_partition_count_bounds() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();
    insert_many_force_spill(&mut ht, 0, 1024);
    let _ = ht.finalize_build(None).unwrap();
    assert!(ht.has_spilled());

    let spill_state = ht.spill_state.as_ref().expect("spill state exists");
    let count = spill_state.partitioning.count;
    assert!(count.is_power_of_two());
    assert!(count >= MIN_PARTITIONS);
    assert!(count <= MAX_PARTITIONS);
}

#[test]
fn test_spill_streaming_parse_multiple_chunks() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: Some(16),
    };
    let mut ht = HashTable::new(config, io).unwrap();

    let key = vec![Value::from_i64(1)];
    for i in 0..2048 {
        match ht.insert(key.clone(), i, vec![], None).unwrap() {
            IOResult::Done(()) => {}
            IOResult::IO(_) => panic!("memory IO"),
        }
    }

    match ht.finalize_build(None).unwrap() {
        IOResult::Done(()) => {}
        IOResult::IO(_) => panic!("memory IO"),
    }
    assert!(ht.has_spilled());

    let partition_idx = ht.partition_for_keys(&key).unwrap();
    {
        let spill_state = ht.spill_state.as_ref().expect("spill state exists");
        let partition = spill_state
            .find_partition(partition_idx)
            .expect("partition exists");
        assert!(partition.chunks.len() > 1, "expected multiple spill chunks");
    }

    while let IOResult::IO(_) = ht.load_spilled_partition(partition_idx, None).unwrap() {}
    assert!(ht.is_partition_loaded(partition_idx));

    let entry = ht
        .probe_partition(partition_idx, &key, None)
        .unwrap()
        .unwrap();
    assert_eq!(entry.rowid, 0);

    let mut matches = 1usize;
    while ht.next_match().unwrap().is_some() {
        matches += 1;
    }
    assert_eq!(matches, 2048);
}

#[test]
fn test_load_partition_empty_chunk() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: Some(16),
    };
    let mut ht = HashTable::new(config, io.clone()).unwrap();
    let partitioning = Partitioning::new(16);
    let temp_file = TempFile::with_temp_store(&io, crate::TempStore::Default).unwrap();

    let mut partition = SpilledPartition::new(0);
    partition.add_chunk(0, 0, 0).unwrap();

    let spill_state = SpillState {
        partition_buffers: (0..partitioning.count)
            .map(|_| PartitionBuffer::new())
            .try_collect()
            .unwrap(),
        partitions: vec![partition],
        next_spill_offset: 0,
        temp_file,
        partitioning,
    };
    ht.spill_state = Some(spill_state);
    ht.state = HashTableState::Probing;

    while let IOResult::IO(_) = ht.load_spilled_partition(0, None).unwrap() {}
    assert!(ht.is_partition_loaded(0));
}

#[test]
fn test_load_partition_truncated_chunk() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: Some(16),
    };
    let mut ht = HashTable::new(config, io.clone()).unwrap();

    let entry = HashEntry::new(1, vec![Value::from_i64(1)], 7);
    let mut buf = vec![];
    entry.serialize(&mut buf).unwrap();
    let truncated = &buf[..buf.len() - 1];

    let temp_file = TempFile::with_temp_store(&io, crate::TempStore::Default).unwrap();
    let write_buf = Buffer::new_temporary(truncated.len());
    write_buf.as_mut_slice().copy_from_slice(truncated);
    let write_buf = Arc::new(write_buf);
    let completion = temp_file
        .file
        .pwrite(0, write_buf, Completion::new_write(|_| {}))
        .unwrap();
    assert!(completion.finished(), "memory write should complete");

    let partitioning = Partitioning::new(16);
    let mut partition = SpilledPartition::new(0);
    partition.add_chunk(0, truncated.len(), 1).unwrap();

    let spill_state = SpillState {
        partition_buffers: (0..partitioning.count)
            .map(|_| PartitionBuffer::new())
            .try_collect()
            .unwrap(),
        partitions: vec![partition],
        next_spill_offset: truncated.len() as u64,
        temp_file,
        partitioning,
    };
    ht.spill_state = Some(spill_state);
    ht.state = HashTableState::Probing;

    let mut saw_err = false;
    loop {
        match ht.load_spilled_partition(0, None) {
            Ok(IOResult::Done(())) => break,
            Ok(IOResult::IO(_)) => continue,
            Err(_) => {
                saw_err = true;
                break;
            }
        }
    }

    assert!(saw_err, "truncated chunk should return an error");
}

#[test]
fn test_hash_function_respects_collation_nocase() {
    use crate::types::{TextRef, TextSubtype};

    let keys1 = vec![ValueRef::Text(TextRef::new("Hello", TextSubtype::Text))];
    let keys2 = vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))];

    // Under BINARY: hashes must differ
    let bin_coll = vec![CollationSeq::Binary];
    let h1_bin = hash_join_key(&keys1, &bin_coll);
    let h2_bin = hash_join_key(&keys2, &bin_coll);
    assert_ne!(h1_bin, h2_bin);

    // Under NOCASE: hashes should be equal
    let nocase_coll = vec![CollationSeq::NoCase];
    let h1_nc = hash_join_key(&keys1, &nocase_coll);
    let h2_nc = hash_join_key(&keys2, &nocase_coll);
    assert_eq!(h1_nc, h2_nc);
}

#[test]
fn test_hash_nocase_preserves_non_ascii() {
    use crate::types::{TextRef, TextSubtype};

    // SQLite NOCASE only affects ASCII a-z/A-Z.
    // Non-ASCII characters like ü should hash identically regardless of case conversion.
    let keys1 = vec![ValueRef::Text(TextRef::new("über", TextSubtype::Text))];
    let keys2 = vec![ValueRef::Text(TextRef::new("ÜBER", TextSubtype::Text))];

    // Under NOCASE: ASCII portion differs (b/B), so hashes should differ
    // (because SQLite NOCASE doesn't handle Unicode case folding)
    let nocase_coll = vec![CollationSeq::NoCase];
    let h1 = hash_join_key(&keys1, &nocase_coll);
    let h2 = hash_join_key(&keys2, &nocase_coll);

    // The 'b' and 'B' will be lowercased to 'b', but the 'ü' and 'Ü' are not
    // ASCII so they remain as-is. Since ü != Ü at byte level, hashes will differ.
    // This is correct SQLite NOCASE behavior (ASCII-only case folding).
    assert_ne!(
        h1, h2,
        "non-ASCII chars should not be case-folded by NOCASE"
    );
}

#[test]
fn test_hash_nocase_embedded_nul_matches_equality() {
    use crate::types::{TextRef, TextSubtype};

    let nocase_coll = vec![CollationSeq::NoCase];
    let keys1 = vec![ValueRef::Text(TextRef::new("A\0x", TextSubtype::Text))];
    let keys2 = vec![ValueRef::Text(TextRef::new("a\0y", TextSubtype::Text))];
    let keys3 = vec![ValueRef::Text(TextRef::new("a\0yz", TextSubtype::Text))];

    assert!(values_equal(keys1[0], keys2[0], CollationSeq::NoCase));
    assert_eq!(
        hash_join_key(&keys1, &nocase_coll),
        hash_join_key(&keys2, &nocase_coll)
    );

    assert!(!values_equal(keys1[0], keys3[0], CollationSeq::NoCase));
    assert_ne!(
        hash_join_key(&keys1, &nocase_coll),
        hash_join_key(&keys3, &nocase_coll)
    );
}

#[test]
fn test_values_equal_with_collations() {
    use crate::types::{TextRef, TextSubtype};

    let h1 = ValueRef::Text(TextRef::new("Hello  ", TextSubtype::Text));
    let h2 = ValueRef::Text(TextRef::new("hello", TextSubtype::Text));

    // Binary: case / trailing spaces matter
    assert!(!values_equal(h1, h2, CollationSeq::Binary));

    // NOCASE: case-insensitive but trailing spaces still matter -> likely false
    assert!(!values_equal(h1, h2, CollationSeq::NoCase));

    // RTRIM: ignore trailing spaces, but case is still significant
    let h3 = ValueRef::Text(TextRef::new("Hello", TextSubtype::Text));
    assert!(values_equal(h1, h3, CollationSeq::Rtrim));
}

#[test]
fn test_keys_equal_with_collations() {
    use crate::types::{TextRef, TextSubtype};

    let key1 = vec![Value::Text("Hello".into())];
    let key2 = vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))];

    // Binary: not equal
    assert!(!keys_equal(&key1, &key2, &[CollationSeq::Binary]));

    // NOCASE: equal
    assert!(keys_equal(&key1, &key2, &[CollationSeq::NoCase]));
}

#[test]
fn test_hash_entry_deserialization_truncated() {
    let entry = HashEntry::new(123, vec![Value::from_i64(1), Value::Text("abc".into())], 42);

    let mut buf = vec![];
    entry.serialize(&mut buf).unwrap();

    // Cut off the buffer mid-entry
    let truncated = &buf[..buf.len() - 2];

    let res = HashEntry::deserialize(truncated);
    assert!(
        res.is_err(),
        "truncated buffer should be rejected as corrupt"
    );
}

#[test]
fn test_hash_entry_deserialization_garbage_type_tag() {
    let entry = HashEntry::new(1, vec![Value::from_i64(10)], 7);
    let mut buf = vec![];
    entry.serialize(&mut buf).unwrap();

    // Compute the exact offset of the *first* type tag.
    // Layout: [0..8] hash | [8..16] rowid | varint(num_keys) | type | payload...
    let mut corrupted = buf.clone();

    let mut offset = 16;
    let (_num_keys, varint_len) = read_varint(&corrupted[offset..]).unwrap();
    offset += varint_len;
    corrupted[offset] = 0xFF;

    let res = HashEntry::deserialize(&corrupted);
    assert!(
        res.is_err(),
        "invalid type tag should be rejected as corrupt"
    );
}

fn insert_many_force_spill(ht: &mut HashTable, start: i64, count: i64) {
    for i in 0..count {
        let rowid = start + i;
        let key = vec![Value::from_i64(rowid)];
        let _ = ht.insert(key, rowid, vec![], None);
    }
}

#[test]
fn test_hash_table_spill_and_load_partition_round_trip() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        // very small budget to force spill
        mem_budget: 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        ..Default::default()
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert enough fat rows to exceed budget and force spills
    insert_many_force_spill(&mut ht, 0, 1024);

    let _ = ht.finalize_build(None).unwrap();
    assert!(ht.has_spilled(), "hash table should have spilled");

    // Pick a key and find its partition
    let probe_key = vec![Value::from_i64(10)];
    let partition_idx = ht.partition_for_keys(&probe_key).unwrap();

    // Load that partition into memory
    match ht.load_spilled_partition(partition_idx, None).unwrap() {
        IOResult::Done(()) => {}
        IOResult::IO(_) => panic!("test harness must drive IO completions here"),
    }

    assert!(
        ht.is_partition_loaded(partition_idx),
        "partition must be resident after load_spilled_partition"
    );

    // Probe via partition API
    let entry = ht.probe_partition(partition_idx, &probe_key, None).unwrap();
    assert!(entry.is_some()); // here
    assert_eq!(entry.unwrap().rowid, 10);
}

#[test]
fn test_partition_lru_eviction() {
    let io = Arc::new(MemoryIO::new());
    // tiny mem_budget so only ~1 partition can stay resident
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 8 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert two disjoint key ranges that will hash to different partitions
    insert_many_force_spill(&mut ht, 0, 256);
    insert_many_force_spill(&mut ht, 256, 1024);

    let _ = ht.finalize_build(None).unwrap();
    assert!(ht.has_spilled());

    let key_a = vec![Value::from_i64(1)];
    let key_b = vec![Value::from_i64(10_001)];
    let pa = ht.partition_for_keys(&key_a).unwrap();
    let pb = ht.partition_for_keys(&key_b).unwrap();
    assert_ne!(pa, pb);

    // Load partition A
    while let IOResult::IO(_) = ht.load_spilled_partition(pa, None).unwrap() {}
    assert!(ht.is_partition_loaded(pa));

    // Now load partition B, this should (under tight memory) evict A
    let _ = ht.load_spilled_partition(pb, None).unwrap();
    assert!(ht.is_partition_loaded(pb));

    // Depending on mem_budget and actual entry sizes, A should now be evicted
    // We can't *guarantee* that without knowing exact sizes, but in practice
    // this test will detect regressions in the LRU bookkeeping.
    assert!(
        !ht.is_partition_loaded(pa) || ht.loaded_partitions_mem <= ht.mem_budget,
        "either partition A is evicted, or loaded memory is within budget"
    );
}

#[test]
fn test_probe_partition_with_duplicate_keys() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 8 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    let key = vec![Value::from_i64(42)];
    for i in 0..1024 {
        match ht.insert(key.clone(), 1000 + i, vec![], None).unwrap() {
            IOResult::Done(()) => {}
            IOResult::IO(_) => panic!("memory IO"),
        }
    }
    match ht.finalize_build(None).unwrap() {
        IOResult::Done(()) => {}
        IOResult::IO(_) => panic!("memory IO"),
    }

    assert!(ht.has_spilled());
    let partition_idx = ht.partition_for_keys(&key).unwrap();

    match ht.load_spilled_partition(partition_idx, None).unwrap() {
        IOResult::Done(()) => {}
        IOResult::IO(_) => panic!("memory IO"),
    }
    assert!(ht.is_partition_loaded(partition_idx));

    // First probe should give us the first rowid
    let entry1 = ht
        .probe_partition(partition_idx, &key, None)
        .unwrap()
        .unwrap();
    assert_eq!(entry1.rowid, 1000);

    // Then iterate through the rest with next_match
    for i in 0..1023 {
        let next = ht.next_match().unwrap().unwrap();
        assert_eq!(next.rowid, 1001 + i);
    }
    assert!(ht.next_match().unwrap().is_none());
}

#[test]
fn test_hash_table_with_payload() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert entries with payload values (simulating cached result columns)
    let key1 = vec![Value::from_i64(1)];
    let payload1 = vec![
        Value::Text("Alice".into()),
        Value::from_i64(30),
        Value::from_f64(1000.50),
    ];
    let _ = ht.insert(key1.clone(), 100, payload1, None).unwrap();

    let key2 = vec![Value::from_i64(2)];
    let payload2 = vec![
        Value::Text("Bob".into()),
        Value::from_i64(25),
        Value::from_f64(2000.75),
    ];
    let _ = ht.insert(key2.clone(), 200, payload2, None).unwrap();

    let _ = ht.finalize_build(None);

    // Probe and verify payload is returned correctly
    let result = ht.probe(key1, None).unwrap();
    assert!(result.is_some());
    let entry1 = result.unwrap();
    assert_eq!(entry1.rowid, 100);
    assert!(entry1.has_payload());
    assert_eq!(entry1.payload_values.len(), 3);
    assert_eq!(entry1.payload_values[0], Value::Text("Alice".into()));
    assert_eq!(entry1.payload_values[1], Value::from_i64(30));
    assert_eq!(entry1.payload_values[2], Value::from_f64(1000.50));

    let result = ht.probe(key2, None).unwrap();
    assert!(result.is_some());
    let entry2 = result.unwrap();
    assert_eq!(entry2.rowid, 200);
    assert!(entry2.has_payload());
    assert_eq!(entry2.payload_values[0], Value::Text("Bob".into()));
    assert_eq!(entry2.payload_values[1], Value::from_i64(25));
    assert_eq!(entry2.payload_values[2], Value::from_f64(2000.75));
}

#[test]
fn test_hash_table_payload_with_nulls() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert entry with NULL values in payload
    let key = vec![Value::from_i64(1)];
    let payload = vec![Value::Null, Value::Text("test".into()), Value::Null];
    let _ = ht.insert(key.clone(), 100, payload, None).unwrap();

    let _ = ht.finalize_build(None);

    let result = ht.probe(key, None).unwrap();
    assert!(result.is_some());
    let entry = result.unwrap();
    assert_eq!(entry.payload_values.len(), 3);
    assert_eq!(entry.payload_values[0], Value::Null);
    assert_eq!(entry.payload_values[1], Value::Text("test".into()));
    assert_eq!(entry.payload_values[2], Value::Null);
}

#[test]
fn test_null_keys_are_skipped() {
    // In SQL, NULL = NULL is false (actually NULL which is falsy).
    // Hash joins should skip rows with NULL keys during both insert and probe.
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024 * 1024,
        num_keys: 2,
        collations: vec![CollationSeq::Binary, CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert entry with NULL key - should be silently skipped
    let null_key = vec![Value::Null, Value::from_i64(1)];
    let _ = ht.insert(null_key.clone(), 100, vec![], None).unwrap();

    // Insert entry with non-NULL keys
    let valid_key = vec![Value::from_i64(1), Value::from_i64(2)];
    let _ = ht.insert(valid_key.clone(), 200, vec![], None).unwrap();

    // Insert another entry where second key is NULL
    let null_key2 = vec![Value::from_i64(1), Value::Null];
    let _ = ht.insert(null_key2.clone(), 300, vec![], None).unwrap();

    let _ = ht.finalize_build(None);

    // Only one entry should be in the table (the one with valid keys)
    assert_eq!(ht.num_entries, 1);

    // Probing with NULL key should return None
    let result = ht.probe(null_key, None).unwrap();
    assert!(result.is_none());

    // Probing with valid key should return the entry
    let result = ht.probe(valid_key, None).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().rowid, 200);

    // Probing with NULL in second position should also return None
    let result = ht.probe(null_key2, None).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_hash_table_payload_with_blobs() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert entry with blob payload
    let key = vec![Value::from_i64(1)];
    let blob_data = std::vec![0xDE, 0xAD, 0xBE, 0xEF];
    let payload = vec![
        Value::from_slice(&blob_data).expect(crate::alloc::ALLOC_ERR_MSG),
        Value::from_i64(42),
    ];
    let _ = ht.insert(key.clone(), 100, payload, None).unwrap();

    let _ = ht.finalize_build(None);

    let result = ht.probe(key, None).unwrap();
    assert!(result.is_some());
    let entry = result.unwrap();
    assert_eq!(entry.payload_values.len(), 2);
    assert_eq!(
        entry.payload_values[0],
        Value::from_slice(&blob_data).expect(crate::alloc::ALLOC_ERR_MSG)
    );
    assert_eq!(entry.payload_values[1], Value::from_i64(42));
}

#[test]
fn test_hash_table_payload_duplicate_keys() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        partition_count: None,
    };
    let mut ht = HashTable::new(config, io).unwrap();

    // Insert multiple entries with the same key but different payloads
    let key = vec![Value::from_i64(42)];
    let _ = ht
        .insert(
            key.clone(),
            100,
            vec![Value::Text("first".into()), Value::from_i64(1)],
            None,
        )
        .unwrap();
    let _ = ht
        .insert(
            key.clone(),
            200,
            vec![Value::Text("second".into()), Value::from_i64(2)],
            None,
        )
        .unwrap();
    let _ = ht
        .insert(
            key.clone(),
            300,
            vec![Value::Text("third".into()), Value::from_i64(3)],
            None,
        )
        .unwrap();

    let _ = ht.finalize_build(None);

    // First probe should return first match
    let result = ht.probe(key, None).unwrap();
    assert!(result.is_some());
    let entry1 = result.unwrap();
    assert_eq!(entry1.rowid, 100);
    assert_eq!(entry1.payload_values[0], Value::Text("first".into()));
    assert_eq!(entry1.payload_values[1], Value::from_i64(1));

    // next_match should return subsequent matches with their payloads
    let entry2 = ht.next_match().unwrap().unwrap();
    assert_eq!(entry2.rowid, 200);
    assert_eq!(entry2.payload_values[0], Value::Text("second".into()));
    assert_eq!(entry2.payload_values[1], Value::from_i64(2));

    let entry3 = ht.next_match().unwrap().unwrap();
    assert_eq!(entry3.rowid, 300);
    assert_eq!(entry3.payload_values[0], Value::Text("third".into()));
    assert_eq!(entry3.payload_values[1], Value::from_i64(3));

    // No more matches
    assert!(ht.next_match().unwrap().is_none());
}

#[test]
fn test_hash_entry_payload_serialization() {
    // Test that payload values survive serialization/deserialization
    let entry = HashEntry::new_with_payload(
        12345,
        vec![Value::from_i64(1), Value::Text("key".into())],
        100,
        vec![
            Value::Text("payload_text".into()),
            Value::from_i64(999),
            Value::from_f64(std::f64::consts::PI),
            Value::Null,
            Value::from_slice(&[1, 2, 3, 4]).expect(crate::alloc::ALLOC_ERR_MSG),
        ],
    );

    let mut buf = vec![];
    entry.serialize(&mut buf).unwrap();

    let (deserialized, bytes_consumed) = HashEntry::deserialize(&buf).unwrap();
    assert_eq!(bytes_consumed, buf.len());

    // Verify key values
    assert_eq!(deserialized.hash, entry.hash);
    assert_eq!(deserialized.rowid, entry.rowid);
    assert_eq!(deserialized.key_values.len(), 2);
    assert_eq!(deserialized.key_values[0], Value::from_i64(1));
    assert_eq!(deserialized.key_values[1], Value::Text("key".into()));

    // Verify payload values
    assert_eq!(deserialized.payload_values.len(), 5);
    assert_eq!(
        deserialized.payload_values[0],
        Value::Text("payload_text".into())
    );
    assert_eq!(deserialized.payload_values[1], Value::from_i64(999));
    assert_eq!(
        deserialized.payload_values[2],
        Value::from_f64(std::f64::consts::PI)
    );
    assert_eq!(deserialized.payload_values[3], Value::Null);
    assert_eq!(
        deserialized.payload_values[4],
        Value::from_slice(&[1, 2, 3, 4]).expect(crate::alloc::ALLOC_ERR_MSG)
    );
}

#[test]
fn test_hash_entry_empty_payload() {
    // Test that entries without payload work correctly
    let entry = HashEntry::new(12345, vec![Value::from_i64(1)], 100);

    assert!(!entry.has_payload());
    assert!(entry.payload_values.is_empty());

    // Serialization should still work
    let mut buf = vec![];
    entry.serialize(&mut buf).unwrap();

    let (deserialized, _) = HashEntry::deserialize(&buf).unwrap();
    assert!(!deserialized.has_payload());
    assert!(deserialized.payload_values.is_empty());
    assert_eq!(deserialized.rowid, 100);
}

#[test]
fn test_hash_entry_size_includes_payload() {
    let entry_no_payload = HashEntry::new(12345, vec![Value::from_i64(1)], 100);

    let entry_with_payload = HashEntry::new_with_payload(
        12345,
        vec![Value::from_i64(1)],
        100,
        vec![
            Value::Text("a]long payload string".into()),
            Value::from_i64(42),
        ],
    );

    // Entry with payload should have larger size
    assert!(entry_with_payload.size_bytes() > entry_no_payload.size_bytes());
}

// ── Grace hash join tests ──────────────────────────────────────

/// Helper: build a spilled hash table with given keys and payloads
fn make_spilled_ht_with_payload(
    io: Arc<dyn IO>,
    build_keys: &[(i64, Vec<Value>)], // (rowid, key_values)
    payload: bool,
) -> HashTable {
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024, // tiny, forces spill
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        ..Default::default()
    };
    let mut ht = HashTable::new(config, io).unwrap();
    for (rowid, keys) in build_keys {
        let payload_values = if payload {
            vec![Value::Text(format!("payload_{rowid}").into())]
        } else {
            vec![]
        };
        let _ = ht.insert(keys.clone(), *rowid, payload_values, None);
    }
    let _ = ht.finalize_build(None).unwrap();
    ht
}

/// Helper: run grace processing with the new fine-grained API.
/// Returns (build_rowid, probe_rowid) pairs for all matches found.
fn run_grace_processing(ht: &mut HashTable) -> Vec<(i64, i64)> {
    let _ = ht.finalize_probe_spill(None).unwrap();
    let mut matches = vec![];

    if !ht.grace_begin().unwrap() {
        return matches;
    }

    // Partition loop
    loop {
        match ht.grace_load_current_partition(None).unwrap() {
            IOResult::Done(true) => {}
            IOResult::Done(false) => break,
            _ => panic!("unexpected IO"),
        }

        // Probe entry loop
        loop {
            let entry = match ht.grace_next_probe_entry().unwrap() {
                IOResult::Done(entry) => entry,
                IOResult::IO(_) => panic!("unexpected IO"),
            };
            let Some(entry) = entry else {
                break;
            };

            // Use probe_partition + next_match to find build matches
            let key_values = entry.key_values;
            let probe_rowid = entry.probe_rowid;
            let partition_idx = ht.partition_for_keys(&key_values).unwrap();

            if ht
                .probe_partition(partition_idx, &key_values, None)
                .unwrap()
                .is_some()
            {
                // First match from probe_partition
                let build_entry = ht
                    .probe_partition(partition_idx, &key_values, None)
                    .unwrap();
                // Re-probe to get entry again
                if let Some(build_entry) = build_entry {
                    matches.push((build_entry.rowid, probe_rowid));
                }
                // Get additional matches via next_match
                while let Some(build_entry) = ht.next_match().unwrap() {
                    matches.push((build_entry.rowid, probe_rowid));
                }
            }
        }

        if !ht.grace_advance_partition() {
            break;
        }
    }
    matches
}

#[test]
fn test_grace_basic() {
    let io = Arc::new(MemoryIO::new());
    let build_keys: Vec<(i64, Vec<Value>)> = (0..200)
        .map(|i| (i, vec![Value::from_i64(i)]))
        .try_collect()
        .unwrap();
    let mut ht = make_spilled_ht_with_payload(io, &build_keys, true);
    assert!(ht.has_spilled(), "should have spilled");

    // Buffer probe rows for keys that map to spilled partitions
    let mut buffered = 0;
    for i in 0..200 {
        let key = vec![Value::from_i64(i)];
        let partition_idx = ht.partition_for_keys(&key).unwrap();
        if !ht.is_partition_loaded(partition_idx) {
            let _ = ht.buffer_probe_row(key, i + 1000, None).unwrap();
            buffered += 1;
        }
    }
    assert!(buffered > 0, "should have buffered some probe rows");

    let matches = run_grace_processing(&mut ht);

    // Every buffered probe row should have found a match
    assert_eq!(
        matches.len(),
        buffered,
        "each buffered probe row should match exactly one build row"
    );
    // Verify correctness: build_rowid should equal probe_rowid - 1000
    for (build_rowid, probe_rowid) in &matches {
        assert_eq!(
            *build_rowid,
            probe_rowid - 1000,
            "build_rowid should match probe key"
        );
    }
}

#[test]
fn test_grace_no_spill_noop() {
    let io = Arc::new(MemoryIO::new());
    // Use large budget so nothing spills
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024 * 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: false,
        ..Default::default()
    };
    let mut ht = HashTable::new(config, io).unwrap();
    for i in 0..10 {
        let _ = ht.insert(vec![Value::from_i64(i)], i, vec![], None);
    }
    let _ = ht.finalize_build(None).unwrap();
    assert!(!ht.has_spilled(), "should NOT have spilled");

    // grace_begin should return false since nothing was spilled
    assert!(
        !ht.grace_begin().unwrap(),
        "grace_begin should return false when nothing spilled"
    );
}

#[test]
fn test_grace_duplicate_keys() {
    let io = Arc::new(MemoryIO::new());
    // Insert multiple build rows with same key
    let mut build_keys: Vec<(i64, Vec<Value>)> = vec![];
    for i in 0..100 {
        // 3 build rows per key value
        build_keys.push((i * 3, vec![Value::from_i64(i)]));
        build_keys.push((i * 3 + 1, vec![Value::from_i64(i)]));
        build_keys.push((i * 3 + 2, vec![Value::from_i64(i)]));
    }
    let mut ht = make_spilled_ht_with_payload(io, &build_keys, false);
    assert!(ht.has_spilled());

    // Buffer probe rows
    let mut buffered_keys = vec![];
    for i in 0..100 {
        let key = vec![Value::from_i64(i)];
        let partition_idx = ht.partition_for_keys(&key).unwrap();
        if !ht.is_partition_loaded(partition_idx) {
            let _ = ht.buffer_probe_row(key, i + 500, None).unwrap();
            buffered_keys.push(i);
        }
    }

    let matches = run_grace_processing(&mut ht);

    // Each buffered probe key should match 3 build rows
    assert_eq!(
        matches.len(),
        buffered_keys.len() * 3,
        "each probe key should find 3 matches"
    );
}

#[test]
fn test_grace_empty_partitions() {
    let io = Arc::new(MemoryIO::new());
    // Build with keys 0..100, probe with keys 200..300 (no overlap)
    let build_keys: Vec<(i64, Vec<Value>)> = (0..200)
        .map(|i| (i, vec![Value::from_i64(i)]))
        .try_collect()
        .unwrap();
    let mut ht = make_spilled_ht_with_payload(io, &build_keys, false);
    assert!(ht.has_spilled());

    // Buffer probe rows with non-matching keys
    for i in 1000..1050 {
        let key = vec![Value::from_i64(i)];
        let partition_idx = ht.partition_for_keys(&key).unwrap();
        if !ht.is_partition_loaded(partition_idx) {
            let _ = ht.buffer_probe_row(key, i, None).unwrap();
        }
    }

    let matches = run_grace_processing(&mut ht);
    assert_eq!(
        matches.len(),
        0,
        "non-matching keys should produce no matches"
    );
}

#[test]
fn test_grace_null_keys() {
    let io = Arc::new(MemoryIO::new());
    let build_keys: Vec<(i64, Vec<Value>)> = (0..200)
        .map(|i| (i, vec![Value::from_i64(i)]))
        .try_collect()
        .unwrap();
    let mut ht = make_spilled_ht_with_payload(io, &build_keys, false);
    assert!(ht.has_spilled());

    // Buffer a probe row with NULL key - should be skipped
    let null_key = vec![Value::Null];
    // NULL keys can't match, so we just verify no crash
    let partition_idx = ht.partition_for_keys(&[Value::from_i64(0)]).unwrap();
    if !ht.is_partition_loaded(partition_idx) {
        // Buffer with a valid key to ensure grace processing runs
        let _ = ht
            .buffer_probe_row(vec![Value::from_i64(0)], 999, None)
            .unwrap();
    }
    // Buffer a null key row to the same partition
    let _ = ht.buffer_probe_row(null_key, 888, None).unwrap();

    let _ = ht.finalize_probe_spill(None).unwrap();
    // Should not crash; NULL key entries should return from grace_next_probe_entry
    assert!(
        ht.grace_begin().unwrap(),
        "should have partitions to process"
    );
    loop {
        match ht.grace_load_current_partition(None).unwrap() {
            IOResult::Done(true) => {}
            IOResult::Done(false) => break,
            _ => panic!("unexpected IO"),
        }
        loop {
            let entry = match ht.grace_next_probe_entry().unwrap() {
                IOResult::Done(entry) => entry,
                IOResult::IO(_) => panic!("unexpected IO"),
            };
            if entry.is_none() {
                break;
            }
            // NULL key probe entries are still returned; the VDBE's HashProbe
            // handles NULL skip. Just verify no crash.
        }
        if !ht.grace_advance_partition() {
            break;
        }
    }
}

#[test]
fn test_grace_with_payload() {
    let io = Arc::new(MemoryIO::new());
    let build_keys: Vec<(i64, Vec<Value>)> = (0..200)
        .map(|i| (i, vec![Value::from_i64(i)]))
        .try_collect()
        .unwrap();
    let mut ht = make_spilled_ht_with_payload(io, &build_keys, true);
    assert!(ht.has_spilled());

    // Buffer some probe rows
    let mut buffered = 0;
    for i in 0..200 {
        let key = vec![Value::from_i64(i)];
        let partition_idx = ht.partition_for_keys(&key).unwrap();
        if !ht.is_partition_loaded(partition_idx) {
            let _ = ht.buffer_probe_row(key, i + 1000, None).unwrap();
            buffered += 1;
        }
    }

    let _ = ht.finalize_probe_spill(None).unwrap();
    assert!(
        ht.grace_begin().unwrap(),
        "should have partitions to process"
    );

    let mut match_count = 0;
    loop {
        match ht.grace_load_current_partition(None).unwrap() {
            IOResult::Done(true) => {}
            IOResult::Done(false) => break,
            _ => panic!("unexpected IO"),
        }
        loop {
            let entry = match ht.grace_next_probe_entry().unwrap() {
                IOResult::Done(entry) => entry,
                IOResult::IO(_) => panic!("unexpected IO"),
            };
            let Some(entry) = entry else {
                break;
            };
            let key_values = entry.key_values;
            let partition_idx = ht.partition_for_keys(&key_values).unwrap();
            if let Some(build_entry) = ht
                .probe_partition(partition_idx, &key_values, None)
                .unwrap()
            {
                // Check payload was correctly round-tripped
                let expected_payload = format!("payload_{}", build_entry.rowid);
                assert_eq!(build_entry.payload_values.len(), 1);
                match &build_entry.payload_values[0] {
                    Value::Text(t) => assert_eq!(t.as_str(), expected_payload.as_str()),
                    other => panic!("expected text payload, got {other:?}"),
                }
                match_count += 1;
            }
        }
        if !ht.grace_advance_partition() {
            break;
        }
    }
    assert_eq!(match_count, buffered);
}

#[test]
fn test_grace_unmatched_scan_uses_current_partition() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 4096,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: true,
        partition_count: Some(4),
    };
    let mut ht = HashTable::new(config, io).unwrap();

    for i in 0..400 {
        let key = vec![Value::from_i64(i)];
        let _ = ht.insert(key, i, vec![], None);
    }
    let _ = ht.finalize_build(None).unwrap();
    assert!(ht.has_spilled(), "should have spilled");

    let mut keys_by_partition = std::collections::BTreeMap::<usize, Vec<i64>>::new();
    for i in 0..400 {
        let partition_idx = ht.partition_for_keys(&[Value::from_i64(i)]).unwrap();
        keys_by_partition
            .entry(partition_idx)
            .or_insert_with(|| vec![])
            .push(i);
    }

    let partitions_to_process: Vec<usize> = ht
        .spill_state
        .as_ref()
        .expect("spill state")
        .partitions
        .iter()
        .filter(|partition| !partition.chunks.is_empty())
        .map(|partition| partition.partition_idx)
        .try_collect()
        .unwrap();
    assert!(
        partitions_to_process.len() >= 2,
        "test requires at least two spilled partitions"
    );

    let first_partition = partitions_to_process[0];
    let second_partition = partitions_to_process[1];
    let first_keys = keys_by_partition
        .get(&first_partition)
        .cloned()
        .expect("first partition keys");
    let second_keys = keys_by_partition
        .get(&second_partition)
        .cloned()
        .expect("second partition keys");

    for key in &first_keys {
        let _ = ht
            .buffer_probe_row(vec![Value::from_i64(*key)], key + 10_000, None)
            .unwrap();
    }
    let _ = ht.finalize_probe_spill(None).unwrap();
    assert!(ht.grace_begin().unwrap(), "should enter grace");

    match ht.grace_load_current_partition(None).unwrap() {
        IOResult::Done(true) => {}
        other => panic!("unexpected grace load result: {other:?}"),
    }
    assert_eq!(
        ht.grace_state
            .as_ref()
            .expect("grace state")
            .current_partition_idx(),
        Some(first_partition)
    );

    loop {
        let entry = match ht.grace_next_probe_entry().unwrap() {
            IOResult::Done(entry) => entry,
            IOResult::IO(_) => panic!("unexpected IO"),
        };
        let Some(entry) = entry else {
            break;
        };
        let partition_idx = ht.partition_for_keys(&entry.key_values).unwrap();
        if ht
            .probe_partition(partition_idx, &entry.key_values, None)
            .unwrap()
            .is_some()
        {
            ht.mark_current_matched();
            while ht.next_match().unwrap().is_some() {
                ht.mark_current_matched();
            }
        }
    }

    ht.begin_unmatched_scan();
    assert!(
        ht.next_unmatched().is_none(),
        "all rows in the first partition were matched"
    );

    assert!(
        ht.grace_advance_partition(),
        "should have another partition"
    );
    match ht.grace_load_current_partition(None).unwrap() {
        IOResult::Done(true) => {}
        other => panic!("unexpected grace load result: {other:?}"),
    }
    assert_eq!(
        ht.grace_state
            .as_ref()
            .expect("grace state")
            .current_partition_idx(),
        Some(second_partition)
    );

    match ht.load_spilled_partition(first_partition, None).unwrap() {
        IOResult::Done(()) => {}
        other => panic!("unexpected spill load result: {other:?}"),
    }

    ht.begin_unmatched_scan();
    assert_eq!(
        ht.unmatched_scan_current_partition(),
        Some(second_partition),
        "grace unmatched scan must target the active grace partition"
    );
    match ht.load_spilled_partition(second_partition, None).unwrap() {
        IOResult::Done(()) => {}
        other => panic!("unexpected spill load result: {other:?}"),
    }

    let mut unmatched_rowids = Vec::new();
    while let Some(entry) = ht.next_unmatched() {
        unmatched_rowids.push(entry.rowid);
    }
    unmatched_rowids.sort_unstable();

    let mut expected = second_keys;
    expected.sort_unstable();
    assert_eq!(unmatched_rowids, expected);
}

#[test]
fn test_unmatched_scan_preserves_in_memory_partitions_before_grace() {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: crate::TempStore::Default,
        track_matched: true,
        partition_count: Some(16),
    };
    let mut ht = HashTable::new(config, io).unwrap();

    let mut next_rowid = 0i64;
    while !ht.has_spilled() {
        let _ = ht
            .insert(vec![Value::from_i64(next_rowid)], next_rowid, vec![], None)
            .unwrap();
        next_rowid += 1;
    }

    let partition_keys: std::collections::BTreeMap<usize, Vec<i64>> = (0..4096)
        .map(|i| (ht.partition_for_keys(&[Value::from_i64(i)]).unwrap(), i))
        .fold(
            std::collections::BTreeMap::new(),
            |mut acc, (partition, key)| {
                acc.entry(partition).or_insert_with(|| vec![]).push(key);
                acc
            },
        );

    let hot_partition = ht
        .spill_state
        .as_ref()
        .expect("spill state")
        .partitions
        .first()
        .map(|partition| partition.partition_idx)
        .expect("expected at least one spilled partition");
    let cold_partition = partition_keys
        .keys()
        .copied()
        .find(|partition_idx| {
            ht.spill_state
                .as_ref()
                .expect("spill state")
                .find_partition(*partition_idx)
                .is_none()
        })
        .expect("expected at least one partition without spill chunks yet");
    let hot_key = partition_keys
        .get(&hot_partition)
        .and_then(|keys| keys.first())
        .copied()
        .expect("hot partition key");
    let cold_key = partition_keys
        .get(&cold_partition)
        .and_then(|keys| keys.first())
        .copied()
        .expect("cold partition key");

    for _ in 0..160 {
        let _ = ht
            .insert(vec![Value::from_i64(hot_key)], next_rowid, vec![], None)
            .unwrap();
        next_rowid += 1;
    }
    for _ in 0..6 {
        let _ = ht
            .insert(vec![Value::from_i64(cold_key)], next_rowid, vec![], None)
            .unwrap();
        next_rowid += 1;
    }

    let _ = ht.finalize_build(None).unwrap();
    assert!(ht.has_spilled(), "should have spilled");

    let (spilled_partition, mut expected_unmatched) = {
        let spill_state = ht.spill_state.as_ref().expect("spill state");
        let spilled_partition = spill_state
            .partitions
            .iter()
            .find(|partition| !partition.chunks.is_empty())
            .map(|partition| partition.partition_idx)
            .expect("expected at least one spilled partition");
        let expected_unmatched: Vec<i64> = spill_state
            .partitions
            .iter()
            .filter(|partition| partition.chunks.is_empty())
            .flat_map(|partition| {
                partition
                    .buckets
                    .iter()
                    .flat_map(|bucket| bucket.entries.iter().map(|entry| entry.rowid))
            })
            .try_collect()
            .unwrap();
        (spilled_partition, expected_unmatched)
    };
    assert!(
        !expected_unmatched.is_empty(),
        "expected at least one resident in-memory partition"
    );

    let probe_key = (0..400)
        .map(|i| vec![Value::from_i64(i)])
        .find(|key| ht.partition_for_keys(key).unwrap() == spilled_partition)
        .expect("spilled partition should have at least one key");
    let _ = ht.buffer_probe_row(probe_key, 10_000, None).unwrap();
    assert!(
        ht.has_grace_partitions(),
        "probe buffering should enable grace"
    );

    ht.begin_unmatched_scan();
    let mut actual_unmatched = Vec::new();
    while let Some(entry) = ht.next_unmatched() {
        actual_unmatched.push(entry.rowid);
    }

    expected_unmatched.sort_unstable();
    actual_unmatched.sort_unstable();
    assert_eq!(actual_unmatched, expected_unmatched);
}
