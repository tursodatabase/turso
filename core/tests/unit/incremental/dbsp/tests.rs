use super::*;

#[test]
fn test_zset_merge_with_weights() {
    let mut zset1 = SimpleZSet::new();
    zset1.insert(1, 1); // Row 1 with weight +1
    zset1.insert(2, 1); // Row 2 with weight +1

    let mut zset2 = SimpleZSet::new();
    zset2.insert(2, -1); // Row 2 with weight -1 (delete)
    zset2.insert(3, 1); // Row 3 with weight +1 (insert)

    zset1.merge(&zset2);

    // Row 1: weight 1 (unchanged)
    // Row 2: weight 0 (deleted)
    // Row 3: weight 1 (inserted)
    assert_eq!(zset1.iter().count(), 2); // Only rows 1 and 3
    assert!(zset1.iter().any(|(k, _)| *k == 1));
    assert!(zset1.iter().any(|(k, _)| *k == 3));
    assert!(!zset1.iter().any(|(k, _)| *k == 2)); // Row 2 removed
}

#[test]
fn test_zset_represents_updates_as_delete_plus_insert() {
    let mut zset = SimpleZSet::new();

    // Initial state
    zset.insert(1, 1);

    // Update row 1: delete old + insert new
    zset.insert(1, -1); // Delete old version
    zset.insert(1, 1); // Insert new version

    // Weight should be 1 (not 2)
    let weight = zset.iter().find(|(k, _)| **k == 1).map(|(_, w)| w);
    assert_eq!(weight, Some(1));
}

#[test]
fn test_hashable_row_delta_operations() {
    let mut delta = Delta::new();

    // Test INSERT
    delta.insert(1, vec![Value::from_i64(1), Value::from_i64(100)]);
    assert_eq!(delta.len(), 1);

    // Test UPDATE (DELETE + INSERT) - order matters!
    delta.delete(1, vec![Value::from_i64(1), Value::from_i64(100)]);
    delta.insert(1, vec![Value::from_i64(1), Value::from_i64(200)]);
    assert_eq!(delta.len(), 3); // Should have 3 operations before consolidation

    // Verify order is preserved
    let ops: Vec<_> = delta.changes.iter().collect();
    assert_eq!(ops[0].1, 1); // First insert
    assert_eq!(ops[1].1, -1); // Delete
    assert_eq!(ops[2].1, 1); // Second insert

    // Test consolidation
    delta.consolidate();
    // After consolidation, the first insert and delete should cancel out
    // leaving only the second insert
    assert_eq!(delta.len(), 1);

    let final_row = &delta.changes[0];
    assert_eq!(final_row.0.rowid, 1);
    assert_eq!(
        final_row.0.values,
        vec![Value::from_i64(1), Value::from_i64(200)]
    );
    assert_eq!(final_row.1, 1);
}

#[test]
fn test_duplicate_row_consolidation() {
    let mut delta = Delta::new();

    // Insert same row twice
    delta.insert(2, vec![Value::from_i64(2), Value::from_i64(300)]);
    delta.insert(2, vec![Value::from_i64(2), Value::from_i64(300)]);

    assert_eq!(delta.len(), 2);

    delta.consolidate();
    assert_eq!(delta.len(), 1);

    // Weight should be 2 (sum of both inserts)
    let final_row = &delta.changes[0];
    assert_eq!(final_row.0.rowid, 2);
    assert_eq!(final_row.1, 2);
}
