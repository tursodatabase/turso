use super::*;

#[test]
fn recursive_cte_profile_emits_all_queue_shapes_per_connection() {
    let mut profile = RecursiveCte::new(1, 128);
    let (phase, batches) = profile.next_batch(3);

    assert_eq!(phase, Phase::Run);
    assert_eq!(batches.len(), 3);
    for batch in batches {
        assert_eq!(batch.len(), 4);
        assert!(batch[0].sql.contains("UNION ALL"));
        assert!(batch[1].sql.contains("ORDER BY 2, 1 LIMIT ?"));
        assert!(batch[2].sql.contains("UNION SELECT"));
        assert!(batch[3].sql.contains("infinite"));
        assert!(
            batch
                .iter()
                .all(|item| { item.params == vec![turso::Value::Integer(128)] })
        );
    }

    let (phase, batches) = profile.next_batch(3);
    assert_eq!(phase, Phase::Done);
    assert!(batches.is_empty());
}

#[test]
fn recursive_cte_profile_never_generates_zero_cardinality() {
    let mut profile = RecursiveCte::new(1, 0);
    let (_, batches) = profile.next_batch(1);
    assert!(
        batches[0]
            .iter()
            .all(|item| item.params == vec![turso::Value::Integer(1)])
    );
}
