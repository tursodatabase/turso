use super::*;
use rand::rngs::StdRng;
use rand::SeedableRng;

#[test]
fn test_extract_column_refs() {
    // Create a simple expression: a + b
    let expr = Expr::Binary(
        Box::new(Expr::Id(Name::from_string("a"))),
        Operator::Add,
        Box::new(Expr::Id(Name::from_string("b"))),
    );
    let refs = extract_column_refs(&expr);
    assert!(refs.contains("a"));
    assert!(refs.contains("b"));
    assert_eq!(refs.len(), 2);
}

#[test]
fn test_extract_column_refs_expr_name() {
    // Test that Expr::Name is also recognized as a column reference
    let expr = Expr::Binary(
        Box::new(Expr::Name(Name::from_string("a"))),
        Operator::Subtract,
        Box::new(Expr::Name(Name::from_string("b"))),
    );
    let refs = extract_column_refs(&expr);
    assert!(refs.contains("a"), "Should find column 'a' in Expr::Name");
    assert!(refs.contains("b"), "Should find column 'b' in Expr::Name");
    assert_eq!(refs.len(), 2);
}

#[test]
fn test_extract_column_refs_mixed() {
    // Test mixed Expr::Id and Expr::Name
    let expr = Expr::Binary(
        Box::new(Expr::Id(Name::from_string("a"))),
        Operator::Add,
        Box::new(Expr::Name(Name::from_string("b"))),
    );
    let refs = extract_column_refs(&expr);
    assert!(refs.contains("a"));
    assert!(refs.contains("b"));
    assert_eq!(refs.len(), 2);
}

#[test]
fn test_extract_column_refs_normalizes_case() {
    let expr = Expr::Binary(
        Box::new(Expr::Id(Name::from_string("A"))),
        Operator::Add,
        Box::new(Expr::Id(Name::from_string("b"))),
    );
    let refs = extract_column_refs(&expr);
    assert!(refs.contains("a"));
    assert!(refs.contains("b"));
    assert_eq!(refs.len(), 2);
}

#[test]
fn test_generate_column_expr() {
    let mut rng = StdRng::seed_from_u64(42);
    let columns = vec![
        Column {
            name: "col_a".to_string(),
            column_type: ColumnType::Integer,
            constraints: vec![],
        },
        Column {
            name: "col_b".to_string(),
            column_type: ColumnType::Integer,
            constraints: vec![],
        },
        Column {
            name: "col_c".to_string(),
            column_type: ColumnType::Text,
            constraints: vec![],
        },
    ];

    // Generate expression for column at index 1 (col_b)
    let (expr, refs) = generate_column_expr_with_refs(
        &mut rng,
        &columns,
        1, // current column index
        &ColumnType::Integer,
        2,
    );

    // Should not reference itself (col_b at index 1)
    assert!(!refs.contains(&1));

    // Expression should be valid
    assert!(!matches!(expr, Expr::Id(name) if name.as_str() == "col_b"));
}
