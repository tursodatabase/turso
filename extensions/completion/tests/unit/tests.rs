use super::*;

#[test]
fn test_best_index_argv_order_both_constraints() {
    // Test when both prefix and wholeline constraints are present
    let constraints = vec![
        usable_constraint(1), // prefix
        usable_constraint(2), // wholeline
    ];

    let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

    // Verify prefix gets argv_index 1 and wholeline gets argv_index 2
    assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // prefix
    assert_eq!(index_info.constraint_usages[1].argv_index, Some(2)); // wholeline
    assert_eq!(index_info.idx_num, 3); // Both bits set (1 | 2)
}

#[test]
fn test_best_index_argv_order_only_wholeline() {
    let constraints = vec![
        usable_constraint(2), // wholeline
    ];

    let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

    // Verify wholeline gets argv_index 1 when prefix is missing
    assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // wholeline
    assert_eq!(index_info.idx_num, 2); // Only bit 1 set
}

#[test]
fn test_best_index_argv_order_only_prefix() {
    let constraints = vec![
        usable_constraint(1), // prefix
    ];

    let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

    // Verify prefix gets argv_index 1
    assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // prefix
    assert_eq!(index_info.idx_num, 1); // Only bit 0 set
}

#[test]
fn test_best_index_argv_order_reverse_constraint_order() {
    // Test when constraints are provided in reverse order (wholeline first, then prefix)
    let constraints = vec![
        usable_constraint(2), // wholeline
        usable_constraint(1), // prefix
    ];

    let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

    // Verify prefix still gets argv_index 1 and wholeline gets argv_index 2 regardless of constraint order
    assert_eq!(index_info.constraint_usages[0].argv_index, Some(2)); // wholeline
    assert_eq!(index_info.constraint_usages[1].argv_index, Some(1)); // prefix
    assert_eq!(index_info.idx_num, 3); // Both bits set (1 | 2)
}

#[test]
fn test_best_index_no_usable_constraints() {
    let constraints = vec![ConstraintInfo {
        column_index: 1,
        op: ConstraintOp::Eq,
        usable: false,
        index: 0,
    }];

    let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

    // Verify no argv_index is assigned
    assert_eq!(index_info.constraint_usages[0].argv_index, None);
    assert_eq!(index_info.idx_num, 0); // No bits set
}

fn usable_constraint(column_index: u32) -> ConstraintInfo {
    ConstraintInfo {
        column_index,
        op: ConstraintOp::Eq,
        usable: true,
        index: 0,
    }
}
