use super::*;

#[test]
fn test_type_mapping() {
    let no_params: &[i64] = &[];
    let s = PgTypeMapping::scalar;
    let a = PgTypeMapping::array;

    // Base types (no Turso custom type)
    assert_eq!(map_pg_type("INTEGER", no_params), Some(s("INTEGER")));
    assert_eq!(map_pg_type("SERIAL", no_params), Some(s("INTEGER")));
    assert_eq!(map_pg_type("REAL", no_params), Some(s("REAL")));
    assert_eq!(map_pg_type("TEXT", no_params), Some(s("TEXT")));
    assert_eq!(map_pg_type("BLOB", no_params), Some(s("BLOB")));

    // Turso custom type equivalents
    assert_eq!(map_pg_type("BOOLEAN", no_params), Some(s("boolean")));
    assert_eq!(map_pg_type("SMALLINT", no_params), Some(s("smallint")));
    assert_eq!(map_pg_type("BIGINT", no_params), Some(s("bigint")));
    assert_eq!(map_pg_type("UUID", no_params), Some(s("uuid")));
    assert_eq!(map_pg_type("DATE", no_params), Some(s("date")));
    assert_eq!(map_pg_type("TIME", no_params), Some(s("time")));
    assert_eq!(map_pg_type("TIMESTAMP", no_params), Some(s("timestamp")));
    assert_eq!(
        map_pg_type("TIMESTAMPTZ", no_params),
        Some(s("timestamptz"))
    );
    assert_eq!(map_pg_type("BYTEA", no_params), Some(s("bytea")));
    assert_eq!(map_pg_type("INET", no_params), Some(s("inet")));
    assert_eq!(map_pg_type("JSON", no_params), Some(s("json")));
    assert_eq!(map_pg_type("JSONB", no_params), Some(s("jsonb")));

    // Parametric types — base name + params separated
    assert_eq!(
        map_pg_type("VARCHAR", &[100]),
        Some(PgTypeMapping::with_params("varchar", vec![100]))
    );
    assert_eq!(map_pg_type("VARCHAR", no_params), Some(s("TEXT")));
    assert_eq!(
        map_pg_type("NUMERIC", &[10, 2]),
        Some(PgTypeMapping::with_params("numeric", vec![10, 2]))
    );
    assert_eq!(
        map_pg_type("NUMERIC", &[10]),
        Some(PgTypeMapping::with_params("numeric", vec![10, 0]))
    );
    assert_eq!(map_pg_type("NUMERIC", no_params), Some(s("REAL")));

    // Network types → custom types
    assert_eq!(map_pg_type("CIDR", no_params), Some(s("cidr")));
    assert_eq!(map_pg_type("MACADDR", no_params), Some(s("macaddr")));
    assert_eq!(map_pg_type("MACADDR8", no_params), Some(s("macaddr8")));

    // Array types → base type + dimensions
    assert_eq!(map_pg_type("INTEGER[]", no_params), Some(a("INTEGER", 1)));
    assert_eq!(map_pg_type("TEXT[]", no_params), Some(a("TEXT", 1)));
    assert_eq!(map_pg_type("TEXT[][]", no_params), Some(a("TEXT", 2)));
    assert_eq!(map_pg_type("BOOLEAN[]", no_params), Some(a("boolean", 1)));
    assert_eq!(map_pg_type("BIGINT[]", no_params), Some(a("bigint", 1)));
    assert_eq!(map_pg_type("VARCHAR[]", no_params), Some(a("TEXT", 1)));
    // PG internal array notation
    assert_eq!(map_pg_type("_int4", no_params), Some(a("INTEGER", 1)));
    assert_eq!(map_pg_type("_text", no_params), Some(a("TEXT", 1)));

    // Unknown types pass through as-is (for user-defined enums etc.)
    assert_eq!(
        map_pg_type("SOMECUSTOMTYPE", no_params),
        Some(s("somecustomtype"))
    );
}

#[test]
fn test_varchar_column_type() {
    let translator = PostgreSQLTranslator::new();
    let sql = "CREATE TABLE t(f1 varchar(4))";
    let parse_result = crate::parse(sql).unwrap();
    let stmt = translator.translate(&parse_result).unwrap();
    if let ast::Stmt::CreateTable { body, .. } = &stmt {
        if let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = body {
            let col = &columns[0];
            let col_type = col.col_type.as_ref().unwrap();
            assert_eq!(col_type.name, "varchar");
            assert!(
                matches!(col_type.size, Some(ast::TypeSize::MaxSize(_))),
                "expected MaxSize, got {:?}",
                col_type.size
            );
        } else {
            panic!("expected ColumnsAndConstraints");
        }
    } else {
        panic!("expected CreateTable, got {stmt:?}");
    }
}

#[test]
fn test_numeric_column_type() {
    let translator = PostgreSQLTranslator::new();
    let sql = "CREATE TABLE t(f1 numeric(10, 2))";
    let parse_result = crate::parse(sql).unwrap();
    let stmt = translator.translate(&parse_result).unwrap();
    if let ast::Stmt::CreateTable { body, .. } = &stmt {
        if let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = body {
            let col = &columns[0];
            let col_type = col.col_type.as_ref().unwrap();
            assert_eq!(col_type.name, "numeric");
            assert!(
                matches!(col_type.size, Some(ast::TypeSize::TypeSize(_, _))),
                "expected TypeSize, got {:?}",
                col_type.size
            );
        } else {
            panic!("expected ColumnsAndConstraints");
        }
    } else {
        panic!("expected CreateTable, got {stmt:?}");
    }
}

#[test]
fn test_unary_plus() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT +42";
    let parse_result = crate::parse(sql).unwrap();
    let stmt = translator.translate(&parse_result).unwrap();
    assert!(matches!(stmt, ast::Stmt::Select(_)));
}

#[test]
fn test_unary_minus() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT -42";
    let parse_result = crate::parse(sql).unwrap();
    let stmt = translator.translate(&parse_result).unwrap();
    assert!(matches!(stmt, ast::Stmt::Select(_)));
}

#[test]
fn test_basic_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users WHERE id = 1";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);

    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        // Check the select body
        if let ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            ..
        } = &select.body.select
        {
            // Should have one result column (*)
            assert_eq!(columns.len(), 1);
            matches!(columns[0], ast::ResultColumn::Star);

            // Should have FROM clause
            assert!(from.is_some());

            // Should have WHERE clause
            assert!(where_clause.is_some());
        } else {
            panic!("Expected OneSelect::Select");
        }
    }
}

#[test]
fn test_table_name_mapping() {
    let translator = PostgreSQLTranslator::new();

    // Test pg_tables passes through as a virtual table (not mapped to sqlite_master)
    let sql = "SELECT * FROM pg_tables";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { from, .. } = &select.body.select {
            if let Some(from_clause) = from {
                if let ast::SelectTable::Table(qualified_name, _, _) = &*from_clause.select {
                    assert_eq!(qualified_name.name.as_str(), "pg_tables");
                } else {
                    panic!("Expected table reference");
                }
            } else {
                panic!("Expected FROM clause");
            }
        } else {
            panic!("Expected OneSelect::Select");
        }
    } else {
        panic!("Expected select query");
    }
}

#[test]
fn test_simple_select_star() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM sqlite_master";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select { columns, from, .. } = &select.body.select {
            // Should have one result column: *
            assert_eq!(columns.len(), 1);
            assert!(
                matches!(columns[0], ast::ResultColumn::Star),
                "Expected ResultColumn::Star but got {:?}",
                columns[0]
            );

            // Should have FROM clause
            if let Some(from_clause) = from {
                if let ast::SelectTable::Table(qualified_name, alias, _) = &*from_clause.select {
                    assert_eq!(qualified_name.name.as_str(), "sqlite_master");
                    assert!(alias.is_none());
                } else {
                    panic!("Expected table reference");
                }
            } else {
                panic!("Expected FROM clause");
            }
        } else {
            panic!("Expected OneSelect::Select");
        }
    } else {
        panic!("Expected select query");
    }
}

#[test]
fn test_column_expressions() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT id, name FROM users";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select { columns, from, .. } = &select.body.select {
            // Should have two result columns: id, name
            assert_eq!(columns.len(), 2);

            // First column should be 'id'
            if let ast::ResultColumn::Expr(expr, alias) = &columns[0] {
                assert!(
                    matches!(**expr, ast::Expr::Id(_)),
                    "Expected Name expression but got {expr:?}"
                );
                if let ast::Expr::Id(name) = &**expr {
                    assert_eq!(name.as_str(), "id");
                }
                assert!(alias.is_none());
            } else {
                panic!("Expected expression result column for first column");
            }

            // Second column should be 'name'
            if let ast::ResultColumn::Expr(expr, alias) = &columns[1] {
                assert!(
                    matches!(**expr, ast::Expr::Id(_)),
                    "Expected Name expression but got {expr:?}"
                );
                if let ast::Expr::Id(name) = &**expr {
                    assert_eq!(name.as_str(), "name");
                }
                assert!(alias.is_none());
            } else {
                panic!("Expected expression result column for second column");
            }

            // Should have FROM clause
            assert!(from.is_some());
        } else {
            panic!("Expected OneSelect::Select");
        }
    } else {
        panic!("Expected select query");
    }
}

#[test]
fn test_qualified_column_expressions() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT users.id, t.name FROM users t";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select { columns, from, .. } = &select.body.select {
            // Should have two result columns: users.id, t.name
            assert_eq!(columns.len(), 2);

            // First column should be 'users.id'
            if let ast::ResultColumn::Expr(expr, alias) = &columns[0] {
                assert!(
                    matches!(**expr, ast::Expr::Qualified(_, _)),
                    "Expected Qualified expression but got {expr:?}"
                );
                if let ast::Expr::Qualified(table_name, col_name) = &**expr {
                    assert_eq!(table_name.as_str(), "users");
                    assert_eq!(col_name.as_str(), "id");
                }
                assert!(alias.is_none());
            } else {
                panic!("Expected expression result column for first qualified column");
            }

            // Second column should be 't.name'
            if let ast::ResultColumn::Expr(expr, alias) = &columns[1] {
                assert!(
                    matches!(**expr, ast::Expr::Qualified(_, _)),
                    "Expected Qualified expression but got {expr:?}"
                );
                if let ast::Expr::Qualified(table_name, col_name) = &**expr {
                    assert_eq!(table_name.as_str(), "t");
                    assert_eq!(col_name.as_str(), "name");
                }
                assert!(alias.is_none());
            } else {
                panic!("Expected expression result column for second qualified column");
            }

            // Should have FROM clause
            assert!(from.is_some());
        } else {
            panic!("Expected OneSelect::Select");
        }
    } else {
        panic!("Expected select query");
    }
}

#[test]
fn test_select_with_where_clause() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users WHERE id = 1";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            ..
        } = &select.body.select
        {
            // Should have SELECT *
            assert_eq!(columns.len(), 1);
            assert!(matches!(columns[0], ast::ResultColumn::Star));

            // Should have FROM clause
            assert!(from.is_some());

            // Should have WHERE clause
            assert!(where_clause.is_some());
            if let Some(where_expr) = where_clause {
                // WHERE id = 1 should be a binary expression
                assert!(
                    matches!(**where_expr, ast::Expr::Binary(_, _, _)),
                    "Expected Binary expression but got {where_expr:?}"
                );
                if let ast::Expr::Binary(left, op, right) = &**where_expr {
                    // Left side should be column 'id'
                    assert!(
                        matches!(**left, ast::Expr::Id(_)),
                        "Expected Name expression for left side"
                    );
                    if let ast::Expr::Id(name) = &**left {
                        assert_eq!(name.as_str(), "id");
                    }

                    // Operator should be Equals
                    assert!(
                        matches!(op, ast::Operator::Equals),
                        "Expected Equals operator"
                    );

                    // Right side should be literal 1
                    assert!(
                        matches!(**right, ast::Expr::Literal(_)),
                        "Expected Literal expression for right side"
                    );
                    if let ast::Expr::Literal(literal) = &**right {
                        assert!(
                            matches!(literal, ast::Literal::Numeric(_)),
                            "Expected numeric literal"
                        );
                        if let ast::Literal::Numeric(num_str) = literal {
                            assert_eq!(num_str, "1");
                        }
                    }
                }
            }
        } else {
            panic!("Expected OneSelect::Select");
        }
    } else {
        panic!("Expected select query");
    }
}

#[test]
fn test_comprehensive_translation() {
    let translator = PostgreSQLTranslator::new();

    // Test various PostgreSQL to Turso AST translations
    let test_cases = vec![
        ("SELECT * FROM sqlite_master", "SELECT * with no WHERE"),
        (
            "SELECT name FROM pg_tables WHERE name = 'users'",
            "SELECT with WHERE and table mapping",
        ),
        (
            "SELECT id, name, age FROM users WHERE age > 18",
            "SELECT multiple columns with WHERE",
        ),
        ("SELECT 'hello', 42 FROM users", "SELECT with literals"),
    ];

    for (sql, description) in test_cases {
        println!("Testing: {description}");
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(
            translated.is_ok(),
            "Failed to translate: {sql} ({description})"
        );

        if let Ok(ast::Stmt::Select(select)) = translated {
            // Verify it's a valid Select AST
            match &select.body.select {
                ast::OneSelect::Select { columns, .. } => {
                    assert!(!columns.is_empty(), "No columns in result for: {sql}");
                }
                _ => panic!("Expected OneSelect::Select for: {sql}"),
            }
        }
    }
}

#[test]
fn test_literal_expressions() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT 'hello', 42, 3.14 FROM users";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            assert_eq!(columns.len(), 3);

            // Check string literal (wrapped in single quotes for Turso AST convention)
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::Literal(ast::Literal::String(s)) = &**expr {
                    assert_eq!(s, "'hello'");
                } else {
                    panic!("Expected string literal");
                }
            } else {
                panic!("Expected expression result column");
            }

            // Check integer literal
            if let ast::ResultColumn::Expr(expr, _) = &columns[1] {
                if let ast::Expr::Literal(ast::Literal::Numeric(n)) = &**expr {
                    assert_eq!(n, "42");
                } else {
                    panic!("Expected numeric literal");
                }
            } else {
                panic!("Expected expression result column");
            }

            // Check float literal
            if let ast::ResultColumn::Expr(expr, _) = &columns[2] {
                if let ast::Expr::Literal(ast::Literal::Numeric(n)) = &**expr {
                    assert_eq!(n, "3.14");
                } else {
                    panic!("Expected numeric literal");
                }
            } else {
                panic!("Expected expression result column");
            }
        } else {
            panic!("Expected OneSelect::Select");
        }
    } else {
        panic!("Expected select query");
    }
}

#[test]
fn test_bool_expr_and_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users WHERE age > 18 AND name = 'John'";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            assert!(where_clause.is_some());
            if let Some(where_expr) = where_clause {
                // Should be a binary AND expression
                assert!(
                    matches!(**where_expr, ast::Expr::Binary(_, ast::Operator::And, _)),
                    "Expected AND expression"
                );
            }
        }
    }
}

#[test]
fn test_bool_expr_or_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users WHERE age > 18 OR name = 'John'";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            assert!(where_clause.is_some());
            if let Some(where_expr) = where_clause {
                // Should be a binary OR expression
                assert!(
                    matches!(**where_expr, ast::Expr::Binary(_, ast::Operator::Or, _)),
                    "Expected OR expression"
                );
            }
        }
    }
}

#[test]
fn test_in_list_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users WHERE type IN ('admin', 'user', 'guest')";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            assert!(where_clause.is_some());
            if let Some(where_expr) = where_clause {
                // Should be an InList expression
                if let ast::Expr::InList { lhs, not, rhs } = &**where_expr {
                    assert!(!not, "Should not be NOT IN");
                    assert_eq!(rhs.len(), 3, "Should have 3 values in the IN list");

                    // Check that lhs is a column reference
                    assert!(
                        matches!(**lhs, ast::Expr::Id(_)),
                        "Left side should be a column name"
                    );

                    // Check that the list values are literals
                    for value in rhs {
                        assert!(
                            matches!(**value, ast::Expr::Literal(_)),
                            "IN list values should be literals"
                        );
                    }
                } else {
                    panic!("Expected InList expression but got: {where_expr:?}");
                }
            }
        }
    }
}

#[test]
fn test_like_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users WHERE name LIKE 'John%'";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            assert!(where_clause.is_some());
            if let Some(where_expr) = where_clause {
                // Should be a Like expression
                if let ast::Expr::Like {
                    lhs,
                    not,
                    op,
                    rhs,
                    escape,
                } = &**where_expr
                {
                    assert!(!not, "Should not be NOT LIKE");
                    assert!(
                        matches!(op, ast::LikeOperator::Like),
                        "Should be LIKE operator"
                    );
                    assert!(escape.is_none(), "No ESCAPE clause expected");

                    // Check left and right expressions
                    assert!(
                        matches!(**lhs, ast::Expr::Id(_)),
                        "Left side should be column name"
                    );
                    assert!(
                        matches!(**rhs, ast::Expr::Literal(_)),
                        "Right side should be literal"
                    );
                } else {
                    panic!("Expected Like expression but got: {where_expr:?}");
                }
            }
        }
    }
}

#[test]
fn test_not_like_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users WHERE name NOT LIKE 'sqlite_%'";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            assert!(where_clause.is_some());
            if let Some(where_expr) = where_clause {
                // Should be a Like expression with NOT
                if let ast::Expr::Like {
                    lhs,
                    not,
                    op,
                    rhs,
                    escape,
                } = &**where_expr
                {
                    assert!(*not, "Should be NOT LIKE");
                    assert!(
                        matches!(op, ast::LikeOperator::Like),
                        "Should be LIKE operator"
                    );
                    assert!(escape.is_none(), "No ESCAPE clause expected");

                    // Check expressions
                    assert!(
                        matches!(**lhs, ast::Expr::Id(_)),
                        "Left side should be column name"
                    );
                    assert!(
                        matches!(**rhs, ast::Expr::Literal(_)),
                        "Right side should be literal"
                    );
                } else {
                    panic!("Expected Like expression but got: {where_expr:?}");
                }
            }
        }
    }
}

#[test]
fn test_complex_schema_query_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT type, name FROM sqlite_schema WHERE type IN ('table', 'index', 'view') AND name NOT LIKE 'sqlite_%'";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result);
    assert!(translated.is_ok());

    if let Ok(ast::Stmt::Select(select)) = translated {
        if let ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            ..
        } = &select.body.select
        {
            // Check columns
            assert_eq!(columns.len(), 2, "Should have 2 columns");

            // Check FROM clause
            assert!(from.is_some(), "Should have FROM clause");

            // Check WHERE clause structure
            assert!(where_clause.is_some(), "Should have WHERE clause");
            if let Some(where_expr) = where_clause {
                // Should be an AND expression
                if let ast::Expr::Binary(left, op, right) = &**where_expr {
                    assert!(matches!(op, ast::Operator::And), "Top level should be AND");

                    // Left side should be IN expression
                    assert!(
                        matches!(**left, ast::Expr::InList { .. }),
                        "Left side should be IN list"
                    );

                    // Right side should be NOT LIKE expression
                    if let ast::Expr::Like { not, .. } = &**right {
                        assert!(*not, "Right side should be NOT LIKE");
                    } else {
                        panic!("Right side should be LIKE expression");
                    }
                } else {
                    panic!("WHERE clause should be Binary expression");
                }
            }
        }
    } else {
        panic!("Translation should succeed");
    }
}

#[test]
fn test_group_by_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT age, COUNT(*) FROM users GROUP BY age";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { group_by, .. } = &select.body.select {
            let gb = group_by.as_ref().expect("Should have GROUP BY");
            assert_eq!(gb.exprs.len(), 1);
            assert!(gb.having.is_none());
        } else {
            panic!("Expected OneSelect::Select");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_group_by_having_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { group_by, .. } = &select.body.select {
            let gb = group_by.as_ref().expect("Should have GROUP BY");
            assert_eq!(gb.exprs.len(), 1);
            assert!(gb.having.is_some(), "Should have HAVING clause");
        } else {
            panic!("Expected OneSelect::Select");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_distinct_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT DISTINCT name FROM users";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { distinctness, .. } = &select.body.select {
            assert!(
                matches!(distinctness, Some(ast::Distinctness::Distinct)),
                "Should have DISTINCT"
            );
        } else {
            panic!("Expected OneSelect::Select");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_limit_offset_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users LIMIT 10 OFFSET 5";
    let parse_result = crate::parse(sql).unwrap();
    let translated = translator.translate(&parse_result).unwrap();

    if let ast::Stmt::Select(select) = translated {
        let limit = select.limit.as_ref().expect("Should have LIMIT");
        assert!(limit.offset.is_some(), "Should have OFFSET");
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_begin_commit_rollback_translation() {
    let translator = PostgreSQLTranslator::new();

    let begin = crate::parse("BEGIN").unwrap();
    assert!(matches!(
        translator.translate(&begin).unwrap(),
        ast::Stmt::Begin { .. }
    ));

    let commit = crate::parse("COMMIT").unwrap();
    assert!(matches!(
        translator.translate(&commit).unwrap(),
        ast::Stmt::Commit { .. }
    ));

    let rollback = crate::parse("ROLLBACK").unwrap();
    assert!(matches!(
        translator.translate(&rollback).unwrap(),
        ast::Stmt::Rollback { .. }
    ));
}

#[test]
fn test_drop_table_translation() {
    let translator = PostgreSQLTranslator::new();

    let sql = "DROP TABLE users";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::DropTable {
        if_exists,
        tbl_name,
    } = translated
    {
        assert!(!if_exists);
        assert_eq!(tbl_name.name.as_str(), "users");
    } else {
        panic!("Expected DropTable");
    }

    let sql = "DROP TABLE IF EXISTS users";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::DropTable { if_exists, .. } = translated {
        assert!(if_exists);
    } else {
        panic!("Expected DropTable");
    }
}

#[test]
fn test_insert_on_conflict_do_nothing() {
    let translator = PostgreSQLTranslator::new();
    let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Insert { body, .. } = translated {
        if let ast::InsertBody::Select(_, upsert) = body {
            let upsert = upsert.expect("Should have ON CONFLICT");
            assert!(matches!(upsert.do_clause, ast::UpsertDo::Nothing));
            let index = upsert.index.as_ref().expect("Should have conflict target");
            assert_eq!(index.targets.len(), 1);
        } else {
            panic!("Expected InsertBody::Select");
        }
    } else {
        panic!("Expected Insert");
    }
}

#[test]
fn test_insert_on_conflict_do_update() {
    let translator = PostgreSQLTranslator::new();
    let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = 'Bob'";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Insert { body, .. } = translated {
        if let ast::InsertBody::Select(_, upsert) = body {
            let upsert = upsert.expect("Should have ON CONFLICT");
            if let ast::UpsertDo::Set { sets, .. } = &upsert.do_clause {
                assert_eq!(sets.len(), 1);
                assert_eq!(sets[0].col_names[0].as_str(), "name");
            } else {
                panic!("Expected UpsertDo::Set");
            }
        } else {
            panic!("Expected InsertBody::Select");
        }
    } else {
        panic!("Expected Insert");
    }
}

#[test]
fn test_insert_returning() {
    let translator = PostgreSQLTranslator::new();
    let sql = "INSERT INTO users (name) VALUES ('Alice') RETURNING id, name";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Insert { returning, .. } = translated {
        assert_eq!(returning.len(), 2);
    } else {
        panic!("Expected Insert");
    }
}

#[test]
fn test_update_returning() {
    let translator = PostgreSQLTranslator::new();
    let sql = "UPDATE users SET name = 'Bob' WHERE id = 1 RETURNING *";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Update(update) = translated {
        assert_eq!(update.returning.len(), 1);
        assert!(matches!(update.returning[0], ast::ResultColumn::Star));
    } else {
        panic!("Expected Update");
    }
}

#[test]
fn test_delete_returning() {
    let translator = PostgreSQLTranslator::new();
    let sql = "DELETE FROM users WHERE id = 1 RETURNING id, name";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Delete { returning, .. } = translated {
        assert_eq!(returning.len(), 2);
    } else {
        panic!("Expected Delete");
    }
}

#[test]
fn test_parameterized_insert() {
    let translator = PostgreSQLTranslator::new();
    let sql = "INSERT INTO users (id, name, age) VALUES ($1, $2, $3)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Insert {
        tbl_name,
        columns,
        body,
        ..
    } = translated
    {
        assert_eq!(tbl_name.name.as_str(), "users");
        assert_eq!(columns.len(), 3);
        // Verify the body has VALUES with $1, $2, $3 parameters
        if let ast::InsertBody::Select(select, _) = body {
            if let ast::OneSelect::Values(rows) = &select.body.select {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 3);
            } else {
                panic!("Expected Values");
            }
        } else {
            panic!("Expected Select body");
        }
    } else {
        panic!("Expected Insert");
    }
}

#[test]
fn test_parameterized_select() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users WHERE age > $1 AND name = $2";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    // Should translate successfully without errors
    assert!(matches!(translated, ast::Stmt::Select(_)));
}

#[test]
fn test_parameterized_update() {
    let translator = PostgreSQLTranslator::new();
    let sql = "UPDATE users SET age = $1 WHERE id = $2";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    assert!(matches!(translated, ast::Stmt::Update { .. }));
}

#[test]
fn test_parameterized_delete() {
    let translator = PostgreSQLTranslator::new();
    let sql = "DELETE FROM users WHERE id = $1";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    assert!(matches!(translated, ast::Stmt::Delete { .. }));
}

#[test]
fn test_insert_with_default_values() {
    let translator = PostgreSQLTranslator::new();
    // Drizzle generates DEFAULT for columns with default values
    let sql = "INSERT INTO users (id, name, verified, jsonb) VALUES ($1, $2, default, default)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Insert { columns, body, .. } = translated {
        // DEFAULT columns should be stripped
        assert_eq!(columns.len(), 2, "should only have id and name columns");
        assert_eq!(columns[0].as_str(), "id");
        assert_eq!(columns[1].as_str(), "name");
        // VALUES should also have 2 entries per row
        if let ast::InsertBody::Select(select, _) = body {
            if let ast::OneSelect::Values(rows) = &select.body.select {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 2, "should only have 2 values per row");
            } else {
                panic!("Expected Values");
            }
        }
    } else {
        panic!("Expected Insert");
    }
}

#[test]
fn test_insert_multi_row_with_defaults() {
    let translator = PostgreSQLTranslator::new();
    let sql = "INSERT INTO t (a, b, c) VALUES ($1, default, $2), ($3, default, $4)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Insert { columns, body, .. } = translated {
        assert_eq!(columns.len(), 2, "column b should be stripped");
        assert_eq!(columns[0].as_str(), "a");
        assert_eq!(columns[1].as_str(), "c");
        if let ast::InsertBody::Select(select, _) = body {
            if let ast::OneSelect::Values(rows) = &select.body.select {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
                assert_eq!(rows[1].len(), 2);
            } else {
                panic!("Expected Values");
            }
        }
    } else {
        panic!("Expected Insert");
    }
}

#[test]
fn test_cte_simple() {
    let translator = PostgreSQLTranslator::new();
    let sql = "WITH sq AS (SELECT 1 AS val) SELECT val FROM sq";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        assert!(select.with.is_some(), "WITH clause should be present");
        let with = select.with.unwrap();
        assert_eq!(with.ctes.len(), 1);
        assert_eq!(with.ctes[0].tbl_name.as_str(), "sq");
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_cte_multiple() {
    let translator = PostgreSQLTranslator::new();
    let sql = "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT x, y FROM a, b";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        let with = select.with.unwrap();
        assert_eq!(with.ctes.len(), 2);
        assert_eq!(with.ctes[0].tbl_name.as_str(), "a");
        assert_eq!(with.ctes[1].tbl_name.as_str(), "b");
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_exists_subquery() {
    let translator = PostgreSQLTranslator::new();
    let sql = r#"SELECT name FROM users WHERE EXISTS (SELECT 1 FROM users WHERE name = 'Alice')"#;
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        let one_select = &select.body.select;
        if let ast::OneSelect::Select { where_clause, .. } = one_select {
            let where_expr = where_clause.as_ref().expect("Should have WHERE clause");
            assert!(
                matches!(where_expr.as_ref(), ast::Expr::Exists(_)),
                "Expected Exists expression"
            );
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_scalar_subquery() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT name FROM users WHERE salary > (SELECT AVG(salary) FROM users)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        let one_select = &select.body.select;
        if let ast::OneSelect::Select { where_clause, .. } = one_select {
            let where_expr = where_clause.as_ref().expect("Should have WHERE clause");
            // Should be BinaryOp(salary > Subquery(...))
            if let ast::Expr::Binary(_, op, rhs) = where_expr.as_ref() {
                assert_eq!(*op, ast::Operator::Greater);
                assert!(
                    matches!(rhs.as_ref(), ast::Expr::Subquery(_)),
                    "Expected Subquery expression on RHS"
                );
            } else {
                panic!("Expected Binary expression, got: {where_expr:?}");
            }
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_in_subquery() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        let one_select = &select.body.select;
        if let ast::OneSelect::Select { where_clause, .. } = one_select {
            let where_expr = where_clause.as_ref().expect("Should have WHERE clause");
            assert!(
                matches!(where_expr.as_ref(), ast::Expr::InSelect { not: false, .. }),
                "Expected InSelect expression"
            );
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_join_subquery() {
    let translator = PostgreSQLTranslator::new();
    let sql = r#"SELECT c.name, sq.cnt FROM cities c LEFT JOIN (SELECT city_id, count(*) as cnt FROM users GROUP BY city_id) sq ON c.id = sq.city_id"#;
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        let one_select = &select.body.select;
        if let ast::OneSelect::Select { from, .. } = one_select {
            let from_clause = from.as_ref().expect("Should have FROM clause");
            assert_eq!(from_clause.joins.len(), 1, "Should have one join");
            let join = &from_clause.joins[0];
            assert!(
                matches!(join.table.as_ref(), ast::SelectTable::Select(_, Some(_))),
                "Join RHS should be a subquery with alias"
            );
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_offset_without_limit() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users OFFSET 5";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        let lim = select
            .limit
            .as_ref()
            .expect("Should have limit (LIMIT -1 OFFSET 5)");
        // LIMIT should be -1 (unlimited)
        if let ast::Expr::Literal(ast::Literal::Numeric(n)) = lim.expr.as_ref() {
            assert_eq!(n, "-1");
        } else {
            panic!("Expected Numeric(-1) limit");
        }
        // OFFSET should be present
        assert!(lim.offset.is_some(), "Should have OFFSET");
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_between_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let w = where_clause.as_ref().expect("should have WHERE");
            assert!(
                matches!(&**w, ast::Expr::Between { not: false, .. }),
                "Expected BETWEEN expression, got: {w:?}"
            );
        } else {
            panic!("Expected Select variant");
        }
    }
}

#[test]
fn test_not_between_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let w = where_clause.as_ref().unwrap();
            assert!(matches!(&**w, ast::Expr::Between { not: true, .. }));
        }
    }
}

#[test]
fn test_is_distinct_from_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM t WHERE a IS DISTINCT FROM b";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let w = where_clause.as_ref().unwrap();
            assert!(
                matches!(&**w, ast::Expr::Binary(_, ast::Operator::IsNot, _)),
                "IS DISTINCT FROM should map to IS NOT, got: {w:?}"
            );
        }
    }
}

#[test]
fn test_is_not_distinct_from_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM t WHERE a IS NOT DISTINCT FROM b";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let w = where_clause.as_ref().unwrap();
            assert!(matches!(&**w, ast::Expr::Binary(_, ast::Operator::Is, _)));
        }
    }
}

#[test]
fn test_nullif_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT NULLIF(a, 0) FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            assert_eq!(columns.len(), 1);
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                    assert_eq!(name.as_str(), "NULLIF");
                    assert_eq!(args.len(), 2);
                } else {
                    panic!("Expected FunctionCall, got: {expr:?}");
                }
            }
        }
    }
}

#[test]
fn test_coalesce_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT COALESCE(a, b, 0) FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                    assert_eq!(name.as_str(), "COALESCE");
                    assert_eq!(args.len(), 3);
                } else {
                    panic!("Expected COALESCE function call");
                }
            }
        }
    }
}

#[test]
fn test_boolean_test_is_true() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM t WHERE active IS TRUE";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let w = where_clause.as_ref().unwrap();
            assert!(
                matches!(&**w, ast::Expr::Binary(_, ast::Operator::Is, _)),
                "IS TRUE should map to IS 1, got: {w:?}"
            );
        }
    }
}

#[test]
fn test_boolean_test_is_false() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM t WHERE active IS FALSE";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let w = where_clause.as_ref().unwrap();
            assert!(matches!(&**w, ast::Expr::Binary(_, ast::Operator::Is, _)));
        }
    }
}

#[test]
fn test_boolean_test_is_unknown() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT * FROM t WHERE x IS UNKNOWN";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let w = where_clause.as_ref().unwrap();
            assert!(
                matches!(&**w, ast::Expr::IsNull(_)),
                "IS UNKNOWN should map to IS NULL"
            );
        }
    }
}

#[test]
fn test_drop_view_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "DROP VIEW IF EXISTS my_view";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    assert!(
        matches!(
            translated,
            ast::Stmt::DropView {
                if_exists: true,
                ..
            }
        ),
        "Expected DropView with IF EXISTS"
    );
}

#[test]
fn test_savepoint_translation() {
    let translator = PostgreSQLTranslator::new();

    let sql = "SAVEPOINT sp1";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::Savepoint { name } = &translated {
        assert_eq!(name.as_str(), "sp1");
    } else {
        panic!("Expected Savepoint, got: {translated:?}");
    }

    let sql = "RELEASE SAVEPOINT sp1";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::Release { name } = &translated {
        assert_eq!(name.as_str(), "sp1");
    } else {
        panic!("Expected Release, got: {translated:?}");
    }

    let sql = "ROLLBACK TO SAVEPOINT sp1";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::Rollback { savepoint_name, .. } = &translated {
        assert_eq!(savepoint_name.as_ref().unwrap().as_str(), "sp1");
    } else {
        panic!("Expected Rollback with savepoint, got: {translated:?}");
    }
}

#[test]
fn test_bitwise_operators() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT a & b, a | b, a << 2, a >> 1 FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            assert_eq!(columns.len(), 4);
            // a & b
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                assert!(matches!(
                    &**expr,
                    ast::Expr::Binary(_, ast::Operator::BitwiseAnd, _)
                ));
            }
            // a | b
            if let ast::ResultColumn::Expr(expr, _) = &columns[1] {
                assert!(matches!(
                    &**expr,
                    ast::Expr::Binary(_, ast::Operator::BitwiseOr, _)
                ));
            }
            // a << 2
            if let ast::ResultColumn::Expr(expr, _) = &columns[2] {
                assert!(matches!(
                    &**expr,
                    ast::Expr::Binary(_, ast::Operator::LeftShift, _)
                ));
            }
            // a >> 1
            if let ast::ResultColumn::Expr(expr, _) = &columns[3] {
                assert!(matches!(
                    &**expr,
                    ast::Expr::Binary(_, ast::Operator::RightShift, _)
                ));
            }
        }
    }
}

#[test]
fn test_current_timestamp_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    let ast::Stmt::Select(selected) = translated else {
        panic!("Expected SELECT statement");
    };

    let ast::OneSelect::Select { columns, .. } = &selected.body.select else {
        panic!("Expected SELECT body");
    };

    let expected = [
        ast::Expr::Literal(ast::Literal::CurrentTimestamp),
        ast::Expr::Literal(ast::Literal::CurrentDate),
        ast::Expr::Literal(ast::Literal::CurrentTime),
    ];

    assert_eq!(columns.len(), expected.len());

    for (col, expected_expr) in columns.iter().zip(&expected) {
        let ast::ResultColumn::Expr(expr, _) = col else {
            panic!("Expected column expression");
        };
        assert_eq!(expr.as_ref(), expected_expr);
    }
}

#[test]
fn test_greatest_least_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT GREATEST(a, b, c), LEAST(x, y) FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            assert_eq!(columns.len(), 2);
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                    assert_eq!(name.as_str(), "MAX");
                    assert_eq!(args.len(), 3);
                } else {
                    panic!("Expected MAX function call");
                }
            }
            if let ast::ResultColumn::Expr(expr, _) = &columns[1] {
                if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                    assert_eq!(name.as_str(), "MIN");
                    assert_eq!(args.len(), 2);
                } else {
                    panic!("Expected MIN function call");
                }
            }
        }
    }
}

#[test]
fn test_truncate_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "TRUNCATE TABLE users";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Delete {
        tbl_name,
        where_clause,
        ..
    } = translated
    {
        assert_eq!(tbl_name.name.as_str(), "users");
        assert!(where_clause.is_none(), "TRUNCATE should have no WHERE");
    } else {
        panic!("Expected Delete for TRUNCATE, got: {translated:?}");
    }
}

#[test]
fn test_create_view_translation() {
    let translator = PostgreSQLTranslator::new();
    let sql = "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::CreateView {
        view_name,
        temporary,
        ..
    } = translated
    {
        assert_eq!(view_name.name.as_str(), "active_users");
        assert!(!temporary);
    } else {
        panic!("Expected CreateView, got: {translated:?}");
    }
}

#[test]
fn test_create_view_with_columns() {
    let translator = PostgreSQLTranslator::new();
    let sql = "CREATE VIEW v (col1, col2) AS SELECT a, b FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::CreateView {
        view_name, columns, ..
    } = translated
    {
        assert_eq!(view_name.name.as_str(), "v");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].col_name.as_str(), "col1");
        assert_eq!(columns[1].col_name.as_str(), "col2");
    } else {
        panic!("Expected CreateView");
    }
}

#[test]
fn test_function_within_group_order_is_translated() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM test";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    let ast::Stmt::Select(select) = translated else {
        panic!("expected select statement");
    };
    let ast::OneSelect::Select { columns, .. } = &select.body.select else {
        panic!("expected select body");
    };
    let ast::ResultColumn::Expr(expr, _) = &columns[0] else {
        panic!("expected result column");
    };
    let ast::Expr::FunctionCall {
        order_by,
        within_group,
        ..
    } = expr.as_ref()
    else {
        panic!("expected function call");
    };
    let [sorted_column] = within_group.as_slice() else {
        panic!("expected single sorted column");
    };
    let ast::Expr::Id(name) = sorted_column.expr.as_ref() else {
        panic!("expected id");
    };

    assert_eq!(name.as_str(), "x");
    assert!(order_by.is_empty());
}

#[test]
fn test_function_argument_order_is_translated() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT array_agg(x ORDER BY y) FROM test";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    let ast::Stmt::Select(select) = translated else {
        panic!("expected select statement");
    };
    let ast::OneSelect::Select { columns, .. } = &select.body.select else {
        panic!("expected select body");
    };
    let ast::ResultColumn::Expr(expr, _) = &columns[0] else {
        panic!("expected result column");
    };
    let ast::Expr::FunctionCall {
        order_by,
        within_group,
        ..
    } = expr.as_ref()
    else {
        panic!("expected function call");
    };
    let [sorted_column] = order_by.as_slice() else {
        panic!("expected single sorted column");
    };
    let ast::Expr::Id(name) = sorted_column.expr.as_ref() else {
        panic!("expected id");
    };

    assert_eq!(name.as_str(), "y");
    assert!(within_group.is_empty());
}

#[test]
fn test_function_passthrough_string_agg() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT string_agg(name, ', ') FROM users";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::FunctionCall { name, .. } = &**expr {
                    assert_eq!(
                        name.as_str(),
                        "string_agg",
                        "string_agg should pass through (resolved in core/function.rs)"
                    );
                } else {
                    panic!("Expected FunctionCall");
                }
            }
        }
    }
}

#[test]
fn test_function_passthrough_concat() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT concat(a, b, c) FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                    assert_eq!(
                        name.as_str(),
                        "concat",
                        "concat should pass through (resolved in core/function.rs)"
                    );
                    assert_eq!(args.len(), 3);
                } else {
                    panic!("Expected FunctionCall, got: {expr:?}");
                }
            }
        }
    }
}

#[test]
fn test_function_passthrough_now() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT now()";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::FunctionCall { name, .. } = &**expr {
                    assert_eq!(
                        name.as_str(),
                        "now",
                        "now() should pass through as registered scalar"
                    );
                } else {
                    panic!("Expected FunctionCall");
                }
            }
        }
    }
}

#[test]
fn test_function_passthrough_char_length() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT char_length(name) FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::FunctionCall { name, .. } = &**expr {
                    assert_eq!(
                        name.as_str(),
                        "char_length",
                        "char_length should pass through (resolved in core/function.rs)"
                    );
                }
            }
        }
    }
}

#[test]
fn test_function_passthrough_gen_random_uuid() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT gen_random_uuid()";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::FunctionCall { name, .. } = &**expr {
                    // gen_random_uuid is registered as a scalar function,
                    // not remapped at the AST level.
                    assert_eq!(name.as_str(), "gen_random_uuid");
                }
            }
        }
    }
}

#[test]
fn test_alter_table_alter_column_type() {
    let translator = PostgreSQLTranslator::new();
    let sql = "ALTER TABLE users ALTER COLUMN age TYPE bigint";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::AlterTable(alter) = translated {
        assert_eq!(alter.name.name.as_str(), "users");
        if let ast::AlterTableBody::AlterColumn { old, .. } = &alter.body {
            assert_eq!(old.as_str(), "age");
        } else {
            panic!("Expected AlterColumn, got: {:?}", alter.body);
        }
    } else {
        panic!("Expected AlterTable");
    }
}

#[test]
fn test_alter_table_set_not_null_unsupported() {
    let translator = PostgreSQLTranslator::new();
    let sql = "ALTER TABLE users ALTER COLUMN name SET NOT NULL";
    let parsed = crate::parse(sql).unwrap();
    let err = translator.translate(&parsed).unwrap_err();
    assert!(
        err.to_string().contains("not supported"),
        "expected unsupported error, got: {err}"
    );
}

#[test]
fn test_alter_table_set_default_unsupported() {
    let translator = PostgreSQLTranslator::new();
    let sql = "ALTER TABLE users ALTER COLUMN created_at SET DEFAULT now()";
    let parsed = crate::parse(sql).unwrap();
    let err = translator.translate(&parsed).unwrap_err();
    assert!(
        err.to_string().contains("not supported"),
        "expected unsupported error, got: {err}"
    );
}

#[test]
fn test_array_contains_operator() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT id FROM posts WHERE tags @> '{\"ORM\"}'";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let wc = where_clause.as_ref().expect("Expected WHERE clause");
            if let ast::Expr::FunctionCall { name, args, .. } = &**wc {
                assert_eq!(name.as_str(), "array_contains_all");
                assert_eq!(args.len(), 2);
            } else {
                panic!("Expected FunctionCall for @>, got: {wc:?}");
            }
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_array_contained_operator() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT id FROM posts WHERE tags <@ '{\"ORM\"}'";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let wc = where_clause.as_ref().expect("Expected WHERE clause");
            if let ast::Expr::FunctionCall { name, .. } = &**wc {
                assert_eq!(name.as_str(), "array_contains_all");
            } else {
                panic!("Expected FunctionCall for <@, got: {wc:?}");
            }
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select statement");
    }
}

/// Extract the WHERE clause of a translated single SELECT.
fn where_clause_of(sql: &str) -> ast::Expr {
    let translator = PostgreSQLTranslator::new();
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    let ast::Stmt::Select(select) = translated else {
        panic!("Expected Select statement");
    };
    let ast::OneSelect::Select { where_clause, .. } = &select.body.select else {
        panic!("Expected Select variant");
    };
    *where_clause
        .as_ref()
        .expect("Expected WHERE clause")
        .clone()
}

#[test]
fn test_eq_any_array_literal_becomes_in_list() {
    let wc = where_clause_of("SELECT id FROM t WHERE id = ANY(ARRAY[1, 2, 3])");
    let ast::Expr::InList { not, rhs, .. } = wc else {
        panic!("Expected InList for = ANY(array literal), got: {wc:?}");
    };
    assert!(!not);
    assert_eq!(rhs.len(), 3);
}

#[test]
fn test_eq_any_param_becomes_array_contains() {
    let wc = where_clause_of("SELECT id FROM t WHERE id = ANY($1)");
    let ast::Expr::FunctionCall { name, args, .. } = wc else {
        panic!("Expected FunctionCall for = ANY(param), got: {wc:?}");
    };
    assert_eq!(name.as_str(), "array_contains");
    assert_eq!(args.len(), 2);
    assert!(
        matches!(&*args[0], ast::Expr::Variable(_)),
        "array argument must come first, got: {:?}",
        args[0]
    );
}

#[test]
fn test_ne_all_array_literal_becomes_not_in_list() {
    let wc = where_clause_of("SELECT id FROM t WHERE id <> ALL(ARRAY[1, 2])");
    let ast::Expr::InList { not, rhs, .. } = wc else {
        panic!("Expected InList for != ALL(array literal), got: {wc:?}");
    };
    assert!(not);
    assert_eq!(rhs.len(), 2);
}

#[test]
fn test_ne_all_param_becomes_not_array_contains() {
    let wc = where_clause_of("SELECT id FROM t WHERE id != ALL($1)");
    let ast::Expr::Unary(ast::UnaryOperator::Not, inner) = wc else {
        panic!("Expected NOT(...) for != ALL(param), got: {wc:?}");
    };
    let ast::Expr::FunctionCall { name, .. } = &*inner else {
        panic!("Expected FunctionCall under NOT, got: {inner:?}");
    };
    assert_eq!(name.as_str(), "array_contains");
}

#[test]
fn test_unsupported_any_all_operators_error() {
    let translator = PostgreSQLTranslator::new();
    for sql in [
        "SELECT 1 WHERE 2 > ANY(ARRAY[1, 5])",
        "SELECT 1 WHERE 2 <> ANY(ARRAY[1, 5])",
        "SELECT 1 WHERE 2 = ALL(ARRAY[2, 2])",
    ] {
        let parsed = crate::parse(sql).unwrap();
        let err = translator.translate(&parsed).unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "expected loud error for {sql}, got: {err}"
        );
    }
}

#[test]
fn test_simple_explain_translates_to_query_plan() {
    let translator = PostgreSQLTranslator::new();
    let parsed = crate::parse("EXPLAIN SELECT 1").unwrap();
    let translated = translator.translate_with_prereqs(&parsed).unwrap();
    assert!(translated.prereqs.is_empty());
    assert!(matches!(translated.cmd, ast::Cmd::ExplainQueryPlan { .. }));
}

#[test]
fn test_explain_options_error() {
    let translator = PostgreSQLTranslator::new();
    for sql in [
        "EXPLAIN ANALYZE SELECT 1",
        "EXPLAIN (VERBOSE) SELECT 1",
        "EXPLAIN (COSTS OFF) SELECT 1",
    ] {
        let parsed = crate::parse(sql).unwrap();
        let err = translator.translate_with_prereqs(&parsed).unwrap_err();
        assert!(
            err.to_string().contains("EXPLAIN options"),
            "expected EXPLAIN option error for {sql}, got: {err}"
        );
    }
}

#[test]
fn test_explain_missing_statement_error() {
    let translator = PostgreSQLTranslator::new();
    for query in [
        None,
        Some(Box::new(pg_query::protobuf::Node { node: None })),
    ] {
        let explain = pg_query::protobuf::ExplainStmt {
            query,
            options: vec![],
        };
        let err = translator.translate_explain(&explain).unwrap_err();
        assert!(
            err.to_string().contains("EXPLAIN missing statement"),
            "expected missing statement error, got: {err}"
        );
    }
}

#[test]
fn test_array_overlaps_operator() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT id FROM posts WHERE tags && '{\"ORM\"}'";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
            let wc = where_clause.as_ref().expect("Expected WHERE clause");
            if let ast::Expr::FunctionCall { name, .. } = &**wc {
                assert_eq!(name.as_str(), "array_overlap");
            } else {
                panic!("Expected FunctionCall for &&, got: {wc:?}");
            }
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_array_constructor() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT ARRAY['a', 'b', 'c']";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            let col = &columns[0];
            if let ast::ResultColumn::Expr(expr, _) = col {
                if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                    assert_eq!(name.as_str(), "array");
                    assert_eq!(args.len(), 3);
                } else {
                    panic!("Expected FunctionCall for ARRAY[...], got: {expr:?}");
                }
            } else {
                panic!("Expected Expr column");
            }
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_array_subscript() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT tags[1] FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            let col = &columns[0];
            if let ast::ResultColumn::Expr(expr, _) = col {
                if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                    assert_eq!(name.as_str(), "array_element");
                    assert_eq!(args.len(), 2);
                } else {
                    panic!("Expected FunctionCall for tags[1], got: {expr:?}");
                }
            } else {
                panic!("Expected Expr column");
            }
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_array_slice() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT tags[1:3] FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();
    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { columns, .. } = &select.body.select {
            let col = &columns[0];
            if let ast::ResultColumn::Expr(expr, _) = col {
                if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                    assert_eq!(name.as_str(), "array_slice");
                    assert_eq!(args.len(), 3);
                } else {
                    panic!("Expected FunctionCall for tags[1:3], got: {expr:?}");
                }
            } else {
                panic!("Expected Expr column");
            }
        } else {
            panic!("Expected Select variant");
        }
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_create_materialized_view() {
    let translator = PostgreSQLTranslator::new();
    let sql = "CREATE MATERIALIZED VIEW totals AS SELECT category, SUM(price) FROM products GROUP BY category";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    match translated {
        ast::Stmt::CreateMaterializedView {
            if_not_exists,
            view_name,
            ..
        } => {
            assert!(!if_not_exists);
            assert_eq!(view_name.name.as_str(), "totals");
        }
        other => panic!("Expected CreateMaterializedView, got: {other:?}"),
    }
}

#[test]
fn test_create_materialized_view_if_not_exists() {
    let translator = PostgreSQLTranslator::new();
    let sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    match translated {
        ast::Stmt::CreateMaterializedView { if_not_exists, .. } => {
            assert!(if_not_exists);
        }
        other => panic!("Expected CreateMaterializedView, got: {other:?}"),
    }
}

#[test]
fn test_drop_materialized_view() {
    let translator = PostgreSQLTranslator::new();
    let sql = "DROP MATERIALIZED VIEW my_view";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    match translated {
        ast::Stmt::DropView {
            if_exists,
            view_name,
        } => {
            assert!(!if_exists);
            assert_eq!(view_name.name.as_str(), "my_view");
        }
        other => panic!("Expected DropView, got: {other:?}"),
    }
}

#[test]
fn test_refresh_materialized_view() {
    let sql = "REFRESH MATERIALIZED VIEW my_view";
    let parsed = crate::parse(sql).unwrap();
    // REFRESH is intercepted in pg_dispatch, but the translator should not error
    // if it encounters it — it falls to the catch-all arm.
    // For this test, just verify parsing succeeds.
    assert!(!parsed.protobuf.nodes().is_empty());
}

// -----------------------------------------------------------------------
// Named windows
// -----------------------------------------------------------------------

#[test]
fn test_named_window_basic() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT SUM(x) OVER w FROM t WINDOW w AS (PARTITION BY y)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select {
            window_clause,
            columns,
            ..
        } = &select.body.select
        {
            // WINDOW clause should have one definition
            assert_eq!(window_clause.len(), 1);
            assert_eq!(window_clause[0].name.as_str(), "w");
            assert_eq!(window_clause[0].window.partition_by.len(), 1);
            assert!(window_clause[0].window.order_by.is_empty());

            // The function should reference the named window
            if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                if let ast::Expr::FunctionCall { filter_over, .. } = &**expr {
                    assert!(
                        matches!(&filter_over.over_clause, Some(ast::Over::Name(n)) if n.as_str() == "w"),
                        "Expected Over::Name(\"w\"), got: {:?}",
                        filter_over.over_clause
                    );
                } else {
                    panic!("Expected FunctionCall");
                }
            }
        } else {
            panic!("Expected Select");
        }
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_order_by_nulls_ordering() {
    let translator = PostgreSQLTranslator::new();

    let cases = [
        ("SELECT * FROM t ORDER BY a", None),
        (
            "SELECT * FROM t ORDER BY a NULLS FIRST",
            Some(ast::NullsOrder::First),
        ),
        (
            "SELECT * FROM t ORDER BY a NULLS LAST",
            Some(ast::NullsOrder::Last),
        ),
        (
            "SELECT * FROM t ORDER BY a DESC NULLS FIRST",
            Some(ast::NullsOrder::First),
        ),
        (
            "SELECT * FROM t ORDER BY a ASC NULLS LAST",
            Some(ast::NullsOrder::Last),
        ),
    ];

    for (sql, expected_nulls) in cases {
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        let ast::Stmt::Select(select) = translated else {
            panic!("expected Select statement for {sql}");
        };
        assert_eq!(select.order_by.len(), 1, "for {sql}");
        assert_eq!(select.order_by[0].nulls, expected_nulls, "for {sql}");
    }
}

#[test]
fn test_compound_select_order_by_nulls_ordering() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a NULLS LAST";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    let ast::Stmt::Select(select) = translated else {
        panic!("expected Select statement");
    };
    assert_eq!(select.body.compounds.len(), 1, "expected one UNION arm");
    assert_eq!(select.order_by.len(), 1);
    assert_eq!(select.order_by[0].nulls, Some(ast::NullsOrder::Last));
}

#[test]
fn test_values_order_by_nulls_ordering() {
    let translator = PostgreSQLTranslator::new();
    let sql = "VALUES (1), (NULL), (2) ORDER BY 1 NULLS LAST";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    let ast::Stmt::Select(select) = translated else {
        panic!("expected Select statement");
    };
    assert!(
        matches!(select.body.select, ast::OneSelect::Values(_)),
        "expected a VALUES body, got {:?}",
        select.body.select
    );
    assert_eq!(select.order_by.len(), 1);
    assert_eq!(select.order_by[0].nulls, Some(ast::NullsOrder::Last));
}

#[test]
fn test_window_order_by_nulls_ordering() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT ROW_NUMBER() OVER (ORDER BY salary NULLS LAST) FROM t";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    let ast::Stmt::Select(select) = translated else {
        panic!("expected Select statement");
    };
    let ast::OneSelect::Select { columns, .. } = &select.body.select else {
        panic!("expected Select body");
    };
    let ast::ResultColumn::Expr(expr, _) = &columns[0] else {
        panic!("expected expression result column");
    };
    let ast::Expr::FunctionCall { filter_over, .. } = expr.as_ref() else {
        panic!("expected function call expression, got {expr:?}");
    };
    let Some(ast::Over::Window(window)) = &filter_over.over_clause else {
        panic!(
            "expected inline OVER window, got {:?}",
            filter_over.over_clause
        );
    };
    assert_eq!(window.order_by.len(), 1);
    assert_eq!(window.order_by[0].nulls, Some(ast::NullsOrder::Last));
}

#[test]
fn test_named_window_with_order_by() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (PARTITION BY dept ORDER BY salary)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
            assert_eq!(window_clause.len(), 1);
            assert_eq!(window_clause[0].name.as_str(), "w");
            assert_eq!(window_clause[0].window.partition_by.len(), 1);
            assert_eq!(window_clause[0].window.order_by.len(), 1);
        }
    }
}

#[test]
fn test_multiple_named_windows() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT SUM(x) OVER w1, AVG(x) OVER w2 FROM t \
                   WINDOW w1 AS (PARTITION BY a), w2 AS (ORDER BY b)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
            assert_eq!(window_clause.len(), 2);
            assert_eq!(window_clause[0].name.as_str(), "w1");
            assert_eq!(window_clause[0].window.partition_by.len(), 1);
            assert!(window_clause[0].window.order_by.is_empty());
            assert_eq!(window_clause[1].name.as_str(), "w2");
            assert!(window_clause[1].window.partition_by.is_empty());
            assert_eq!(window_clause[1].window.order_by.len(), 1);
        }
    }
}

#[test]
fn test_named_window_with_frame() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT SUM(x) OVER w FROM t \
                   WINDOW w AS (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
            assert_eq!(window_clause.len(), 1);
            assert!(window_clause[0].window.frame_clause.is_some());
            let frame = window_clause[0].window.frame_clause.as_ref().unwrap();
            assert_eq!(frame.mode, ast::FrameMode::Rows);
        }
    }
}

#[test]
fn test_named_window_inheritance() {
    let translator = PostgreSQLTranslator::new();
    // w2 inherits from w1 and adds ORDER BY
    let sql = "SELECT SUM(x) OVER w2 FROM t \
                   WINDOW w1 AS (PARTITION BY dept), w2 AS (w1 ORDER BY salary)";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
            assert_eq!(window_clause.len(), 2);
            // w1 has no base
            assert!(window_clause[0].window.base.is_none());
            // w2 inherits from w1
            assert_eq!(
                window_clause[1].window.base.as_ref().map(|n| n.as_str()),
                Some("w1")
            );
            assert_eq!(window_clause[1].window.order_by.len(), 1);
        }
    }
}

#[test]
fn test_named_window_empty_over() {
    let translator = PostgreSQLTranslator::new();
    let sql = "SELECT COUNT(*) OVER w FROM t WINDOW w AS ()";
    let parsed = crate::parse(sql).unwrap();
    let translated = translator.translate(&parsed).unwrap();

    if let ast::Stmt::Select(select) = translated {
        if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
            assert_eq!(window_clause.len(), 1);
            assert_eq!(window_clause[0].name.as_str(), "w");
            assert!(window_clause[0].window.partition_by.is_empty());
            assert!(window_clause[0].window.order_by.is_empty());
            assert!(window_clause[0].window.frame_clause.is_none());
        }
    }
}

// -----------------------------------------------------------------------
// COPY statement translation
// -----------------------------------------------------------------------

#[test]
fn test_copy_translation_is_rejected() {
    let translator = PostgreSQLTranslator::new();
    let sql = "COPY users FROM '/path/to/file.tsv'";
    let parsed = crate::parse(sql).unwrap();
    let err = translator.translate(&parsed).unwrap_err();
    assert!(err
        .to_string()
        .contains("COPY is handled at the postgres frontend layer"));
}

#[test]
fn test_try_extract_copy_from() {
    let parsed = crate::parse("COPY users FROM '/tmp/data.tsv'").unwrap();
    let copy = try_extract_copy_from(&parsed).unwrap();
    assert_eq!(copy.table_name, "users");
    assert!(copy.schema_name.is_none());
    assert!(copy.columns.is_none());
    assert_eq!(copy.filename, "/tmp/data.tsv");
    assert!(!copy.header);
    assert!(copy.delimiter.is_none());
    assert!(copy.null_string.is_none());
}

#[test]
fn test_try_extract_copy_from_not_to() {
    let parsed = crate::parse("COPY users TO '/tmp/out.tsv'").unwrap();
    assert!(try_extract_copy_from(&parsed).is_none());
}

#[test]
fn test_try_extract_copy_from_not_stdin() {
    let parsed = crate::parse("COPY users FROM STDIN").unwrap();
    assert!(try_extract_copy_from(&parsed).is_none());
}

#[test]
fn test_try_extract_copy_from_with_columns() {
    let parsed = crate::parse("COPY users (id, name) FROM '/tmp/data.tsv'").unwrap();
    let copy = try_extract_copy_from(&parsed).unwrap();
    let cols = copy.columns.unwrap();
    assert_eq!(cols, vec!["id", "name"]);
}

#[test]
fn test_create_sequence_cycle() {
    let translator = PostgreSQLTranslator::new();
    let sql = "CREATE SEQUENCE cyc_seq MINVALUE 1 MAXVALUE 3 CYCLE";
    let parse_result = crate::parse(sql).unwrap();
    let stmt = translator.translate(&parse_result).unwrap();
    match stmt {
        ast::Stmt::CreateSequence { cycle, .. } => {
            assert!(cycle, "CYCLE should be true");
        }
        other => panic!("Expected CreateSequence, got {other:?}"),
    }
}

#[test]
fn test_recursive_cte_cycle_clause_rejected() {
    let translator = PostgreSQLTranslator::new();
    let sql = "WITH RECURSIVE walk(dst) AS (
            SELECT 1 UNION ALL SELECT w.dst + 1 FROM walk w WHERE w.dst < 3
        ) CYCLE dst SET is_cycle USING path
        SELECT * FROM walk";
    let parse_result = crate::parse(sql).unwrap();
    let err = translator.translate(&parse_result).unwrap_err();
    assert!(
        err.to_string().contains("CYCLE clause"),
        "expected CYCLE clause rejection, got: {err}"
    );
}

#[test]
fn test_recursive_cte_search_clause_rejected() {
    let translator = PostgreSQLTranslator::new();
    let sql = "WITH RECURSIVE walk(dst) AS (
            SELECT 1 UNION ALL SELECT w.dst + 1 FROM walk w WHERE w.dst < 3
        ) SEARCH DEPTH FIRST BY dst SET ordercol
        SELECT * FROM walk";
    let parse_result = crate::parse(sql).unwrap();
    let err = translator.translate(&parse_result).unwrap_err();
    assert!(
        err.to_string().contains("SEARCH clause"),
        "expected SEARCH clause rejection, got: {err}"
    );
}
