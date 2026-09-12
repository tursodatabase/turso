use super::*;

#[test]
fn test_parse_select() {
    let sql = b"SELECT * FROM users";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Stmt::Select(select) => {
            assert_eq!(select.columns.len(), 1);
            assert!(select.from.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_dollar_parameter() {
    let sql = b"SELECT * FROM users WHERE id = $1";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Stmt::Select(select) => {
            assert!(select.where_clause.is_some());
            match &select.where_clause.unwrap() {
                Expr::BinaryOp { right, .. } => {
                    assert!(matches!(right.as_ref(), Expr::DollarParameter(1)));
                }
                _ => panic!("Expected binary operation"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_type_cast() {
    let sql = b"SELECT '123'::integer";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Stmt::Select(select) => {
            assert_eq!(select.columns.len(), 1);
            match &select.columns[0].expr {
                Expr::TypeCast { expr, type_name } => {
                    assert!(matches!(expr.as_ref(), Expr::String(_)));
                    assert_eq!(type_name, "integer");
                }
                _ => panic!("Expected type cast"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_insert_with_returning() {
    let sql =
        b"INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Stmt::Insert(insert) => {
            assert!(insert.columns.is_some());
            assert!(insert.returning.is_some());
            assert_eq!(insert.returning.unwrap().len(), 1);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_on_conflict() {
    let sql =
        b"INSERT INTO users (email) VALUES ('test@example.com') ON CONFLICT (email) DO NOTHING";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Stmt::Insert(insert) => {
            assert!(insert.on_conflict.is_some());
            let conflict = insert.on_conflict.unwrap();
            assert!(matches!(conflict.target, Some(OnConflictTarget::Columns(_))));
            assert!(matches!(conflict.action, OnConflictAction::DoNothing));
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_array() {
    let sql = b"SELECT ARRAY[1, 2, 3]";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Stmt::Select(select) => {
            assert_eq!(select.columns.len(), 1);
            assert!(matches!(&select.columns[0].expr, Expr::Array(_)));
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_json_operators() {
    let sql = b"SELECT data->'field' FROM json_table";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Stmt::Select(select) => {
            assert_eq!(select.columns.len(), 1);
            match &select.columns[0].expr {
                Expr::JsonAccess { op, .. } => {
                    assert_eq!(*op, JsonOperator::Arrow);
                }
                _ => panic!("Expected JSON access"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_distinct_on() {
    let sql = b"SELECT DISTINCT ON (department) name, department FROM employees";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Stmt::Select(select) => {
            assert!(matches!(&select.distinct, Some(Distinct::On(_))));
        }
        _ => panic!("Expected SELECT statement"),
    }
}
