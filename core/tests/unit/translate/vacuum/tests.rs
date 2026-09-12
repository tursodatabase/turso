use super::*;

#[test]
fn test_extract_path_from_string_literal() {
    let expr = Expr::Literal(Literal::String("'test.db'".to_string()));
    let path = extract_path_from_expr(&expr).unwrap();
    assert_eq!(path, "test.db");
}

#[test]
fn test_extract_path_from_string_literal_double_quotes() {
    let expr = Expr::Literal(Literal::String("\"test.db\"".to_string()));
    let path = extract_path_from_expr(&expr).unwrap();
    assert_eq!(path, "test.db");
}

#[test]
fn test_extract_path_from_identifier() {
    let expr = Expr::Id(Name::exact("myfile".to_string()));
    let path = extract_path_from_expr(&expr).unwrap();
    assert_eq!(path, "myfile");
}

#[test]
fn test_extract_path_empty_fails() {
    let expr = Expr::Literal(Literal::String("''".to_string()));
    assert!(extract_path_from_expr(&expr).is_err());
}
