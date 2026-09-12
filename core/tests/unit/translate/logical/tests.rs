use super::*;
use crate::alloc::vec;
use crate::schema::{
    BTreeCharacteristics, BTreeTable, ColDef, Column as SchemaColumn, Schema, Type,
};
use turso_parser::parser::Parser;

fn create_test_schema() -> Schema {
    let mut schema = Schema::new();

    // Create users table
    let columns = vec![
        SchemaColumn::new(
            Some("id".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            None,
            ColDef {
                primary_key: true,
                rowid_alias: true,
                notnull: true,
                ..Default::default()
            },
        ),
        SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
        SchemaColumn::new_default_integer(Some("age".to_string()), "INTEGER".to_string(), None),
        SchemaColumn::new_default_text(Some("email".to_string()), "TEXT".to_string(), None),
    ];
    let users_table = BTreeTable::new(
        2,
        "users".to_string(),
        vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        vec![],
        vec![],
        vec![],
        None,
    );
    schema
        .add_btree_table(Arc::new(users_table))
        .expect("Test setup: failed to add users table");

    // Create orders table
    let columns = vec![
        SchemaColumn::new(
            Some("id".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            None,
            ColDef {
                primary_key: true,
                rowid_alias: true,
                notnull: true,
                ..Default::default()
            },
        ),
        SchemaColumn::new_default_integer(Some("user_id".to_string()), "INTEGER".to_string(), None),
        SchemaColumn::new_default_text(Some("product".to_string()), "TEXT".to_string(), None),
        SchemaColumn::new(
            Some("amount".to_string()),
            "REAL".to_string(),
            None,
            None,
            Type::Real,
            None,
            ColDef::default(),
        ),
    ];
    let orders_table = BTreeTable::new(
        3,
        "orders".to_string(),
        vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        vec![],
        vec![],
        vec![],
        None,
    );
    schema
        .add_btree_table(Arc::new(orders_table))
        .expect("Test setup: failed to add orders table");

    // Create products table
    let columns = vec![
        SchemaColumn::new(
            Some("id".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            None,
            ColDef {
                primary_key: true,
                rowid_alias: true,
                notnull: true,
                ..Default::default()
            },
        ),
        SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
        SchemaColumn::new(
            Some("price".to_string()),
            "REAL".to_string(),
            None,
            None,
            Type::Real,
            None,
            ColDef::default(),
        ),
        SchemaColumn::new_default_integer(
            Some("product_id".to_string()),
            "INTEGER".to_string(),
            None,
        ),
    ];
    let products_table = BTreeTable::new(
        4,
        "products".to_string(),
        vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        vec![],
        vec![],
        vec![],
        None,
    );
    schema
        .add_btree_table(Arc::new(products_table))
        .expect("Test setup: failed to add products table");

    schema
}

fn parse_and_build(sql: &str, schema: &Schema) -> Result<LogicalPlan> {
    let mut parser = Parser::new(sql.as_bytes());
    let cmd = parser
        .next()
        .ok_or_else(|| LimboError::ParseError("Empty statement".to_string()))?
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    match cmd {
        ast::Cmd::Stmt(stmt) => {
            let mut builder = LogicalPlanBuilder::new(schema);
            builder.build_statement(&stmt)
        }
        _ => Err(LimboError::ParseError(
            "Only SQL statements are supported".to_string(),
        )),
    }
}

#[test]
fn test_simple_select() {
    let schema = create_test_schema();
    let sql = "SELECT id, name FROM users";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 2);
            assert!(matches!(proj.exprs[0], LogicalExpr::Column(_)));
            assert!(matches!(proj.exprs[1], LogicalExpr::Column(_)));

            match &*proj.input {
                LogicalPlan::TableScan(scan) => {
                    assert_eq!(scan.table_name, "users");
                }
                _ => panic!("Expected TableScan"),
            }
        }
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_select_with_filter() {
    let schema = create_test_schema();
    let sql = "SELECT name FROM users WHERE age > 18";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 1);

            match &*proj.input {
                LogicalPlan::Filter(filter) => {
                    assert!(matches!(
                        filter.predicate,
                        LogicalExpr::BinaryExpr {
                            op: ast::Operator::Greater,
                            ..
                        }
                    ));

                    match &*filter.input {
                        LogicalPlan::TableScan(scan) => {
                            assert_eq!(scan.table_name, "users");
                        }
                        _ => panic!("Expected TableScan"),
                    }
                }
                _ => panic!("Expected Filter"),
            }
        }
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_aggregate_with_group_by() {
    let schema = create_test_schema();
    let sql = "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Aggregate(agg) => {
            assert_eq!(agg.group_expr.len(), 1);
            assert_eq!(agg.aggr_expr.len(), 1);
            assert_eq!(agg.schema.column_count(), 2);

            assert!(matches!(
                agg.aggr_expr[0],
                LogicalExpr::AggregateFunction {
                    fun: AggFunc::Sum,
                    ..
                }
            ));

            match &*agg.input {
                LogicalPlan::TableScan(scan) => {
                    assert_eq!(scan.table_name, "orders");
                }
                _ => panic!("Expected TableScan"),
            }
        }
        _ => panic!("Expected Aggregate (no projection)"),
    }
}

#[test]
fn test_aggregate_without_group_by() {
    let schema = create_test_schema();
    let sql = "SELECT COUNT(*), MAX(age) FROM users";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Aggregate(agg) => {
            assert_eq!(agg.group_expr.len(), 0);
            assert_eq!(agg.aggr_expr.len(), 2);
            assert_eq!(agg.schema.column_count(), 2);

            assert!(matches!(
                agg.aggr_expr[0],
                LogicalExpr::AggregateFunction {
                    fun: AggFunc::Count,
                    ..
                }
            ));

            assert!(matches!(
                agg.aggr_expr[1],
                LogicalExpr::AggregateFunction {
                    fun: AggFunc::Max,
                    ..
                }
            ));
        }
        _ => panic!("Expected Aggregate (no projection)"),
    }
}

#[test]
fn test_order_by() {
    let schema = create_test_schema();
    let sql = "SELECT name FROM users ORDER BY age DESC, name ASC";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Sort(sort) => {
            assert_eq!(sort.exprs.len(), 2);
            assert!(!sort.exprs[0].asc); // DESC
            assert!(sort.exprs[1].asc); // ASC

            match &*sort.input {
                LogicalPlan::Projection(_) => {}
                _ => panic!("Expected Projection"),
            }
        }
        _ => panic!("Expected Sort"),
    }
}

#[test]
fn test_limit_offset() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users LIMIT 10 OFFSET 5";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Limit(limit) => {
            assert_eq!(limit.fetch, Some(10));
            assert_eq!(limit.skip, Some(5));
        }
        _ => panic!("Expected Limit"),
    }
}

#[test]
fn test_order_by_with_limit() {
    let schema = create_test_schema();
    let sql = "SELECT name FROM users ORDER BY age DESC LIMIT 5";
    let plan = parse_and_build(sql, &schema).unwrap();

    // Should produce: Limit -> Sort -> Projection -> TableScan
    match plan {
        LogicalPlan::Limit(limit) => {
            assert_eq!(limit.fetch, Some(5));
            assert_eq!(limit.skip, None);

            match &*limit.input {
                LogicalPlan::Sort(sort) => {
                    assert_eq!(sort.exprs.len(), 1);
                    assert!(!sort.exprs[0].asc); // DESC

                    match &*sort.input {
                        LogicalPlan::Projection(_) => {}
                        _ => panic!("Expected Projection under Sort"),
                    }
                }
                _ => panic!("Expected Sort under Limit"),
            }
        }
        _ => panic!("Expected Limit at top level"),
    }
}

#[test]
fn test_distinct() {
    let schema = create_test_schema();
    let sql = "SELECT DISTINCT name FROM users";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Distinct(distinct) => match &*distinct.input {
            LogicalPlan::Projection(_) => {}
            _ => panic!("Expected Projection"),
        },
        _ => panic!("Expected Distinct"),
    }
}

#[test]
fn test_union() {
    let schema = create_test_schema();
    let sql = "SELECT id FROM users UNION SELECT user_id FROM orders";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Union(union) => {
            assert!(!union.all);
            assert_eq!(union.inputs.len(), 2);
        }
        _ => panic!("Expected Union"),
    }
}

#[test]
fn test_union_all() {
    let schema = create_test_schema();
    let sql = "SELECT id FROM users UNION ALL SELECT user_id FROM orders";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Union(union) => {
            assert!(union.all);
            assert_eq!(union.inputs.len(), 2);
        }
        _ => panic!("Expected Union"),
    }
}

#[test]
fn test_union_with_order_by() {
    let schema = create_test_schema();
    let sql = "SELECT id, name FROM users UNION SELECT user_id, name FROM orders ORDER BY id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Sort(sort) => {
            assert_eq!(sort.exprs.len(), 1);
            assert!(sort.exprs[0].asc); // Default ASC

            match &*sort.input {
                LogicalPlan::Union(union) => {
                    assert!(!union.all); // UNION (not UNION ALL)
                    assert_eq!(union.inputs.len(), 2);
                }
                _ => panic!("Expected Union under Sort"),
            }
        }
        _ => panic!("Expected Sort at top level"),
    }
}

#[test]
fn test_with_cte() {
    let schema = create_test_schema();
    let sql =
        "WITH active_users AS (SELECT * FROM users WHERE age > 18) SELECT name FROM active_users";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::WithCTE(with) => {
            assert_eq!(with.ctes.len(), 1);
            assert!(with.ctes.contains_key("active_users"));

            let cte = &with.ctes["active_users"];
            match &**cte {
                LogicalPlan::Projection(proj) => match &*proj.input {
                    LogicalPlan::Filter(_) => {}
                    _ => panic!("Expected Filter in CTE"),
                },
                _ => panic!("Expected Projection in CTE"),
            }

            match &*with.body {
                LogicalPlan::Projection(proj) => match &*proj.input {
                    LogicalPlan::CTERef(cte_ref) => {
                        assert_eq!(cte_ref.name, "active_users");
                    }
                    _ => panic!("Expected CTERef"),
                },
                _ => panic!("Expected Projection in body"),
            }
        }
        _ => panic!("Expected WithCTE"),
    }
}

#[test]
fn test_case_expression() {
    let schema = create_test_schema();
    let sql = "SELECT CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END FROM users";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 1);
            assert!(matches!(proj.exprs[0], LogicalExpr::Case { .. }));
        }
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_in_list() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Filter(filter) => match &filter.predicate {
                LogicalExpr::InList { list, negated, .. } => {
                    assert!(!negated);
                    assert_eq!(list.len(), 3);
                }
                _ => panic!("Expected InList"),
            },
            _ => panic!("Expected Filter"),
        },
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_in_subquery() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Filter(filter) => {
                assert!(matches!(filter.predicate, LogicalExpr::InSubquery { .. }));
            }
            _ => panic!("Expected Filter"),
        },
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_exists_subquery() {
    let schema = create_test_schema();
    let sql =
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Filter(filter) => {
                assert!(matches!(filter.predicate, LogicalExpr::Exists { .. }));
            }
            _ => panic!("Expected Filter"),
        },
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_between() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Filter(filter) => match &filter.predicate {
                LogicalExpr::Between { negated, .. } => {
                    assert!(!negated);
                }
                _ => panic!("Expected Between"),
            },
            _ => panic!("Expected Filter"),
        },
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_like() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users WHERE name LIKE 'John%'";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Filter(filter) => match &filter.predicate {
                LogicalExpr::Like {
                    negated, escape, ..
                } => {
                    assert!(!negated);
                    assert!(escape.is_none());
                }
                _ => panic!("Expected Like"),
            },
            _ => panic!("Expected Filter"),
        },
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_is_null() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users WHERE email IS NULL";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Filter(filter) => match &filter.predicate {
                LogicalExpr::IsNull { negated, .. } => {
                    assert!(!negated);
                }
                _ => panic!("Expected IsNull, got: {:?}", filter.predicate),
            },
            _ => panic!("Expected Filter"),
        },
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_is_not_null() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users WHERE email IS NOT NULL";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Filter(filter) => match &filter.predicate {
                LogicalExpr::IsNull { negated, .. } => {
                    assert!(negated);
                }
                _ => panic!("Expected IsNull"),
            },
            _ => panic!("Expected Filter"),
        },
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_values_clause() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'))";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Values(values) => {
                assert_eq!(values.rows.len(), 2);
                assert_eq!(values.rows[0].len(), 2);
            }
            _ => panic!("Expected Values"),
        },
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_complex_expression_with_aggregation() {
    // Test: SELECT sum(id + 2) * 2 FROM orders GROUP BY user_id
    let schema = create_test_schema();

    // Test the complex case: sum((id + 2)) * 2 with parentheses
    let sql = "SELECT sum((id + 2)) * 2 FROM orders GROUP BY user_id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 1);
            match &proj.exprs[0] {
                LogicalExpr::BinaryExpr { left, op, right } => {
                    assert_eq!(*op, BinaryOperator::Multiply);
                    assert!(matches!(**left, LogicalExpr::Column(_)));
                    assert!(matches!(**right, LogicalExpr::Literal(_)));
                }
                _ => panic!("Expected BinaryExpr in projection"),
            }

            match &*proj.input {
                LogicalPlan::Aggregate(agg) => {
                    assert_eq!(agg.group_expr.len(), 1);

                    assert_eq!(agg.aggr_expr.len(), 1);
                    match &agg.aggr_expr[0] {
                        LogicalExpr::AggregateFunction { fun, args, .. } => {
                            assert_eq!(*fun, AggregateFunction::Sum);
                            assert_eq!(args.len(), 1);
                            match &args[0] {
                                LogicalExpr::Column(col) => {
                                    assert!(col.name.starts_with("__agg_arg_proj_"));
                                }
                                _ => panic!(
                                    "Expected Column reference to projected expression in aggregate args, got {:?}",
                                    args[0]
                                ),
                            }
                        }
                        _ => panic!("Expected AggregateFunction"),
                    }

                    match &*agg.input {
                        LogicalPlan::Projection(inner_proj) => {
                            assert!(inner_proj.exprs.len() >= 2);
                            let has_binary_add = inner_proj.exprs.iter().any(|e| {
                                matches!(
                                    e,
                                    LogicalExpr::BinaryExpr {
                                        op: BinaryOperator::Add,
                                        ..
                                    }
                                )
                            });
                            assert!(
                                has_binary_add,
                                "Should have id + 2 expression in inner projection"
                            );
                        }
                        _ => panic!("Expected Projection as input to Aggregate"),
                    }
                }
                _ => panic!("Expected Aggregate under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_function_on_aggregate_result() {
    let schema = create_test_schema();

    let sql = "SELECT abs(sum(id)) FROM orders GROUP BY user_id";
    let plan = parse_and_build(sql, &schema).unwrap();
    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 1);
            match &proj.exprs[0] {
                LogicalExpr::ScalarFunction { fun, args } => {
                    assert_eq!(fun, "abs");
                    assert_eq!(args.len(), 1);
                    assert!(matches!(args[0], LogicalExpr::Column(_)));
                }
                _ => panic!("Expected ScalarFunction in projection"),
            }
        }
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_multiple_aggregates_with_arithmetic() {
    let schema = create_test_schema();

    let sql = "SELECT sum(id) * 2 + count(*) FROM orders GROUP BY user_id";
    let plan = parse_and_build(sql, &schema).unwrap();
    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 1);
            match &proj.exprs[0] {
                LogicalExpr::BinaryExpr { op, .. } => {
                    assert_eq!(*op, BinaryOperator::Add);
                }
                _ => panic!("Expected BinaryExpr"),
            }

            match &*proj.input {
                LogicalPlan::Aggregate(agg) => {
                    assert_eq!(agg.aggr_expr.len(), 2);
                }
                _ => panic!("Expected Aggregate"),
            }
        }
        _ => panic!("Expected Projection"),
    }
}

#[test]
fn test_projection_aggregation_projection() {
    let schema = create_test_schema();

    // This tests: projection -> aggregation -> projection
    // The inner projection computes (id + 2), then we aggregate sum(), then apply abs()
    let sql = "SELECT abs(sum(id + 2)) FROM orders GROUP BY user_id";
    let plan = parse_and_build(sql, &schema).unwrap();

    // Should produce: Projection(abs) -> Aggregate(sum) -> Projection(id + 2) -> TableScan
    match plan {
        LogicalPlan::Projection(outer_proj) => {
            assert_eq!(outer_proj.exprs.len(), 1);

            // Outer projection should apply abs() function
            match &outer_proj.exprs[0] {
                LogicalExpr::ScalarFunction { fun, args } => {
                    assert_eq!(fun, "abs");
                    assert_eq!(args.len(), 1);
                    assert!(matches!(args[0], LogicalExpr::Column(_)));
                }
                _ => panic!("Expected abs() function in outer projection"),
            }

            // Next should be the Aggregate
            match &*outer_proj.input {
                LogicalPlan::Aggregate(agg) => {
                    assert_eq!(agg.group_expr.len(), 1);
                    assert_eq!(agg.aggr_expr.len(), 1);

                    // The aggregate should be summing a column reference
                    match &agg.aggr_expr[0] {
                        LogicalExpr::AggregateFunction { fun, args, .. } => {
                            assert_eq!(*fun, AggregateFunction::Sum);
                            assert_eq!(args.len(), 1);

                            // Should reference the projected column
                            match &args[0] {
                                LogicalExpr::Column(col) => {
                                    assert!(col.name.starts_with("__agg_arg_proj_"));
                                }
                                _ => panic!("Expected column reference in aggregate"),
                            }
                        }
                        _ => panic!("Expected AggregateFunction"),
                    }

                    // Input to aggregate should be a projection computing id + 2
                    match &*agg.input {
                        LogicalPlan::Projection(inner_proj) => {
                            // Should have at least the group column and the computed expression
                            assert!(inner_proj.exprs.len() >= 2);

                            // Check for the id + 2 expression
                            let has_add_expr = inner_proj.exprs.iter().any(|e| {
                                matches!(
                                    e,
                                    LogicalExpr::BinaryExpr {
                                        op: BinaryOperator::Add,
                                        ..
                                    }
                                )
                            });
                            assert!(
                                has_add_expr,
                                "Should have id + 2 expression in inner projection"
                            );
                        }
                        _ => panic!("Expected inner Projection under Aggregate"),
                    }
                }
                _ => panic!("Expected Aggregate under outer Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_group_by_validation_allow_grouped_column() {
    let schema = create_test_schema();

    // Test that grouped columns are allowed
    let sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id";
    let result = parse_and_build(sql, &schema);

    assert!(result.is_ok(), "Should allow grouped column in SELECT");
}

#[test]
fn test_group_by_validation_allow_constants() {
    let schema = create_test_schema();

    // Test that simple constants are allowed even when not grouped
    let sql = "SELECT user_id, 42, COUNT(*) FROM orders GROUP BY user_id";
    let result = parse_and_build(sql, &schema);

    assert!(
        result.is_ok(),
        "Should allow simple constants in SELECT with GROUP BY"
    );

    let sql_complex = "SELECT user_id, (100 + 50) * 2, COUNT(*) FROM orders GROUP BY user_id";
    let result_complex = parse_and_build(sql_complex, &schema);

    assert!(
        result_complex.is_ok(),
        "Should allow complex constant expressions in SELECT with GROUP BY"
    );
}

#[test]
fn test_parenthesized_aggregate_expressions() {
    let schema = create_test_schema();

    let sql = "SELECT 25, (MAX(id) / 3), 39 FROM orders";
    let result = parse_and_build(sql, &schema);

    assert!(
        result.is_ok(),
        "Should handle parenthesized aggregate expressions"
    );

    let plan = result.unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 3);

            assert!(matches!(
                proj.exprs[0],
                LogicalExpr::Literal(Value::Numeric(Numeric::Integer(25)))
            ));

            match &proj.exprs[1] {
                LogicalExpr::BinaryExpr { left, op, right } => {
                    assert_eq!(*op, BinaryOperator::Divide);
                    assert!(matches!(&**left, LogicalExpr::Column(_)));
                    assert!(matches!(
                        &**right,
                        LogicalExpr::Literal(Value::Numeric(Numeric::Integer(3)))
                    ));
                }
                _ => panic!("Expected BinaryExpr for (MAX(id) / 3)"),
            }

            assert!(matches!(
                proj.exprs[2],
                LogicalExpr::Literal(Value::Numeric(Numeric::Integer(39)))
            ));

            match &*proj.input {
                LogicalPlan::Aggregate(agg) => {
                    assert_eq!(agg.aggr_expr.len(), 1);
                    assert!(matches!(
                        agg.aggr_expr[0],
                        LogicalExpr::AggregateFunction {
                            fun: AggFunc::Max,
                            ..
                        }
                    ));
                }
                _ => panic!("Expected Aggregate node under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_duplicate_aggregate_reuse() {
    let schema = create_test_schema();

    let sql = "SELECT (COUNT(*) - 225), 30, COUNT(*) FROM orders";
    let result = parse_and_build(sql, &schema);

    assert!(result.is_ok(), "Should handle duplicate aggregates");

    let plan = result.unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 3);

            match &proj.exprs[0] {
                LogicalExpr::BinaryExpr { left, op, right } => {
                    assert_eq!(*op, BinaryOperator::Subtract);
                    match &**left {
                        LogicalExpr::Column(col) => {
                            assert!(col.name.starts_with("__agg_") || col.name == "COUNT(*)");
                        }
                        _ => panic!("Expected Column reference for COUNT(*)"),
                    }
                    assert!(matches!(
                        &**right,
                        LogicalExpr::Literal(Value::Numeric(Numeric::Integer(225)))
                    ));
                }
                _ => panic!("Expected BinaryExpr for (COUNT(*) - 225)"),
            }

            assert!(matches!(
                proj.exprs[1],
                LogicalExpr::Literal(Value::Numeric(Numeric::Integer(30)))
            ));

            match &proj.exprs[2] {
                LogicalExpr::Column(col) => {
                    assert!(col.name.starts_with("__agg_") || col.name == "COUNT(*)");
                }
                _ => panic!("Expected Column reference for COUNT(*)"),
            }

            match &*proj.input {
                LogicalPlan::Aggregate(agg) => {
                    assert_eq!(
                        agg.aggr_expr.len(),
                        1,
                        "Should have only one COUNT(*) aggregate"
                    );
                    assert!(matches!(
                        agg.aggr_expr[0],
                        LogicalExpr::AggregateFunction {
                            fun: AggFunc::Count,
                            ..
                        }
                    ));
                }
                _ => panic!("Expected Aggregate node under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_aggregate_without_group_by_allow_constants() {
    let schema = create_test_schema();

    // Test that constants are allowed with aggregates even without GROUP BY
    let sql = "SELECT 42, COUNT(*), MAX(amount) FROM orders";
    let result = parse_and_build(sql, &schema);

    assert!(
        result.is_ok(),
        "Should allow simple constants with aggregates without GROUP BY"
    );

    // Test complex constant expressions
    let sql_complex = "SELECT (9 / 6) % 5, COUNT(*), MAX(amount) FROM orders";
    let result_complex = parse_and_build(sql_complex, &schema);

    assert!(
        result_complex.is_ok(),
        "Should allow complex constant expressions with aggregates without GROUP BY"
    );
}

#[test]
fn test_aggregate_without_group_by_creates_aggregate_node() {
    let schema = create_test_schema();

    // Test that aggregate without GROUP BY creates proper Aggregate node
    let sql = "SELECT MAX(amount) FROM orders";
    let plan = parse_and_build(sql, &schema).unwrap();

    // Should be: Aggregate -> TableScan (no projection needed for simple aggregate)
    match plan {
        LogicalPlan::Aggregate(agg) => {
            assert_eq!(agg.group_expr.len(), 0, "Should have no group expressions");
            assert_eq!(
                agg.aggr_expr.len(),
                1,
                "Should have one aggregate expression"
            );
            assert_eq!(
                agg.schema.column_count(),
                1,
                "Schema should have one column"
            );
        }
        _ => panic!("Expected Aggregate at top level (no projection)"),
    }
}

#[test]
fn test_scalar_vs_aggregate_function_classification() {
    let schema = create_test_schema();

    // Test MIN/MAX with 1 argument - should be aggregate
    let sql = "SELECT MIN(amount) FROM orders";
    let plan = parse_and_build(sql, &schema).unwrap();
    match plan {
        LogicalPlan::Aggregate(agg) => {
            assert_eq!(agg.aggr_expr.len(), 1, "MIN(x) should be an aggregate");
            match &agg.aggr_expr[0] {
                LogicalExpr::AggregateFunction { fun, args, .. } => {
                    assert!(matches!(fun, AggFunc::Min));
                    assert_eq!(args.len(), 1);
                }
                _ => panic!("Expected AggregateFunction"),
            }
        }
        _ => panic!("Expected Aggregate node for MIN(x)"),
    }

    // Test MIN/MAX with 2 arguments - should be scalar in projection
    let sql = "SELECT MIN(amount, user_id) FROM orders";
    let plan = parse_and_build(sql, &schema).unwrap();
    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 1, "Should have one projection expression");
            match &proj.exprs[0] {
                LogicalExpr::ScalarFunction { fun, args } => {
                    assert_eq!(
                        fun.to_lowercase(),
                        "min",
                        "MIN(x,y) should be a scalar function"
                    );
                    assert_eq!(args.len(), 2);
                }
                _ => panic!("Expected ScalarFunction for MIN(x,y)"),
            }
        }
        _ => panic!("Expected Projection node for scalar MIN(x,y)"),
    }

    // Test MAX with 3 arguments - should be scalar
    let sql = "SELECT MAX(amount, user_id, id) FROM orders";
    let plan = parse_and_build(sql, &schema).unwrap();
    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 1);
            match &proj.exprs[0] {
                LogicalExpr::ScalarFunction { fun, args } => {
                    assert_eq!(
                        fun.to_lowercase(),
                        "max",
                        "MAX(x,y,z) should be a scalar function"
                    );
                    assert_eq!(args.len(), 3);
                }
                _ => panic!("Expected ScalarFunction for MAX(x,y,z)"),
            }
        }
        _ => panic!("Expected Projection node for scalar MAX(x,y,z)"),
    }

    // Test that MIN with 0 args is treated as scalar (will fail later in execution)
    let sql = "SELECT MIN() FROM orders";
    let plan = parse_and_build(sql, &schema).unwrap();
    match plan {
        LogicalPlan::Projection(proj) => match &proj.exprs[0] {
            LogicalExpr::ScalarFunction { fun, args } => {
                assert_eq!(fun.to_lowercase(), "min");
                assert_eq!(args.len(), 0, "MIN() should be scalar with 0 args");
            }
            _ => panic!("Expected ScalarFunction for MIN()"),
        },
        _ => panic!("Expected Projection for MIN()"),
    }

    // Test other functions that are always aggregate (COUNT, SUM, AVG)
    let sql = "SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders";
    let plan = parse_and_build(sql, &schema).unwrap();
    match plan {
        LogicalPlan::Aggregate(agg) => {
            assert_eq!(agg.aggr_expr.len(), 3, "Should have 3 aggregate functions");
            for expr in &agg.aggr_expr {
                assert!(matches!(expr, LogicalExpr::AggregateFunction { .. }));
            }
        }
        _ => panic!("Expected Aggregate node"),
    }

    // Test scalar functions that are never aggregates (ABS, ROUND, etc.)
    let sql = "SELECT ABS(amount), ROUND(amount), LENGTH(product) FROM orders";
    let plan = parse_and_build(sql, &schema).unwrap();
    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 3, "Should have 3 scalar functions");
            for expr in &proj.exprs {
                match expr {
                    LogicalExpr::ScalarFunction { .. } => {}
                    _ => panic!("Expected all ScalarFunctions"),
                }
            }
        }
        _ => panic!("Expected Projection node for scalar functions"),
    }
}

#[test]
fn test_mixed_aggregate_and_group_columns() {
    let schema = create_test_schema();

    // When selecting both aggregate and grouping columns
    let sql = "SELECT user_id, sum(id) FROM orders GROUP BY user_id";
    let plan = parse_and_build(sql, &schema).unwrap();

    // No projection needed - aggregate outputs exactly what we select
    match plan {
        LogicalPlan::Aggregate(agg) => {
            assert_eq!(agg.group_expr.len(), 1);
            assert_eq!(agg.aggr_expr.len(), 1);
            assert_eq!(agg.schema.column_count(), 2);
        }
        _ => panic!("Expected Aggregate (no projection)"),
    }
}

#[test]
fn test_scalar_function_wrapping_aggregate_no_group_by() {
    // Test: SELECT HEX(SUM(age + 2)) FROM users
    // Expected structure:
    // Projection { exprs: [ScalarFunction(HEX, [Column])] }
    //   -> Aggregate { aggr_expr: [Sum(BinaryExpr(age + 2))], group_expr: [] }
    //     -> Projection { exprs: [BinaryExpr(age + 2)] }
    //       -> TableScan("users")

    let schema = create_test_schema();
    let sql = "SELECT HEX(SUM(age + 2)) FROM users";
    let mut parser = Parser::new(sql.as_bytes());
    let stmt = parser.next().unwrap().unwrap();

    let plan = match stmt {
        ast::Cmd::Stmt(stmt) => {
            let mut builder = LogicalPlanBuilder::new(&schema);
            builder.build_statement(&stmt).unwrap()
        }
        _ => panic!("Expected SQL statement"),
    };

    match &plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 1, "Should have one expression");

            match &proj.exprs[0] {
                LogicalExpr::ScalarFunction { fun, args } => {
                    assert_eq!(fun, "HEX", "Outer function should be HEX");
                    assert_eq!(args.len(), 1, "HEX should have one argument");

                    match &args[0] {
                        LogicalExpr::Column(_) => {}
                        LogicalExpr::AggregateFunction { .. } => {
                            panic!(
                                "Aggregate function should not be embedded in projection! It should be in a separate Aggregate operator"
                            );
                        }
                        _ => panic!(
                            "Expected column reference as argument to HEX, got: {:?}",
                            args[0]
                        ),
                    }
                }
                _ => panic!("Expected ScalarFunction (HEX), got: {:?}", proj.exprs[0]),
            }

            match &*proj.input {
                LogicalPlan::Aggregate(agg) => {
                    assert_eq!(agg.group_expr.len(), 0, "Should have no GROUP BY");
                    assert_eq!(
                        agg.aggr_expr.len(),
                        1,
                        "Should have one aggregate expression"
                    );

                    match &agg.aggr_expr[0] {
                        LogicalExpr::AggregateFunction {
                            fun,
                            args,
                            distinct,
                        } => {
                            assert_eq!(*fun, crate::function::AggFunc::Sum, "Should be SUM");
                            assert!(!distinct, "Should not be DISTINCT");
                            assert_eq!(args.len(), 1, "SUM should have one argument");

                            match &args[0] {
                                LogicalExpr::Column(col) => {
                                    // When aggregate arguments are complex, they get pre-projected
                                    assert!(
                                        col.name.starts_with("__agg_arg_proj_"),
                                        "Should reference pre-projected column, got: {}",
                                        col.name
                                    );
                                }
                                LogicalExpr::BinaryExpr { left, op, right } => {
                                    // Simple case without pre-projection (shouldn't happen with current implementation)
                                    assert_eq!(*op, ast::Operator::Add, "Should be addition");

                                    match (&**left, &**right) {
                                        (
                                            LogicalExpr::Column(col),
                                            LogicalExpr::Literal(val),
                                        ) => {
                                            assert_eq!(
                                                col.name, "age",
                                                "Should reference age column"
                                            );
                                            assert_eq!(
                                                *val,
                                                Value::from_i64(2),
                                                "Should add 2"
                                            );
                                        }
                                        _ => panic!("Expected age + 2"),
                                    }
                                }
                                _ => panic!(
                                    "Expected Column reference or BinaryExpr for aggregate argument, got: {:?}",
                                    args[0]
                                ),
                            }
                        }
                        _ => panic!("Expected AggregateFunction"),
                    }

                    match &*agg.input {
                        LogicalPlan::TableScan(scan) => {
                            assert_eq!(scan.table_name, "users");
                        }
                        LogicalPlan::Projection(proj) => match &*proj.input {
                            LogicalPlan::TableScan(scan) => {
                                assert_eq!(scan.table_name, "users");
                            }
                            _ => panic!("Expected TableScan under projection"),
                        },
                        _ => panic!("Expected TableScan or Projection under Aggregate"),
                    }
                }
                _ => panic!(
                    "Expected Aggregate operator under Projection, got: {:?}",
                    proj.input
                ),
            }
        }
        _ => panic!("Expected Projection as top-level operator, got: {plan:?}"),
    }
}

// ===== JOIN TESTS =====

#[test]
fn test_inner_join() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            match &*proj.input {
                LogicalPlan::Join(join) => {
                    assert_eq!(join.join_type, JoinType::Inner);
                    assert!(!join.on.is_empty(), "Should have join conditions");

                    // Check left input is users
                    match &*join.left {
                        LogicalPlan::TableScan(scan) => {
                            assert_eq!(scan.table_name, "users");
                        }
                        _ => panic!("Expected TableScan for left input"),
                    }

                    // Check right input is orders
                    match &*join.right {
                        LogicalPlan::TableScan(scan) => {
                            assert_eq!(scan.table_name, "orders");
                        }
                        _ => panic!("Expected TableScan for right input"),
                    }
                }
                _ => panic!("Expected Join under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_left_join() {
    let schema = create_test_schema();
    let sql = "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 2); // name and amount
            match &*proj.input {
                LogicalPlan::Join(join) => {
                    assert_eq!(join.join_type, JoinType::Left);
                    assert!(!join.on.is_empty(), "Should have join conditions");
                }
                _ => panic!("Expected Join under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_right_join() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM orders o RIGHT JOIN users u ON o.user_id = u.id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Join(join) => {
                assert_eq!(join.join_type, JoinType::Right);
                assert!(!join.on.is_empty(), "Should have join conditions");
            }
            _ => panic!("Expected Join under Projection"),
        },
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_full_outer_join() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Join(join) => {
                assert_eq!(join.join_type, JoinType::Full);
                assert!(!join.on.is_empty(), "Should have join conditions");
            }
            _ => panic!("Expected Join under Projection"),
        },
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_cross_join() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users CROSS JOIN orders";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Join(join) => {
                assert_eq!(join.join_type, JoinType::Cross);
                assert!(join.on.is_empty(), "Cross join should have no conditions");
                assert!(join.filter.is_none(), "Cross join should have no filter");
            }
            _ => panic!("Expected Join under Projection"),
        },
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_join_with_multiple_conditions() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND u.age > 18";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            match &*proj.input {
                LogicalPlan::Join(join) => {
                    assert_eq!(join.join_type, JoinType::Inner);
                    // Should have at least one equijoin condition
                    assert!(!join.on.is_empty(), "Should have join conditions");
                    // Additional conditions may be in filter
                    // The exact distribution depends on our implementation
                }
                _ => panic!("Expected Join under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_join_using_clause() {
    let schema = create_test_schema();
    // Note: Both tables should have an 'id' column for this to work
    let sql = "SELECT * FROM users JOIN orders USING (id)";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => match &*proj.input {
            LogicalPlan::Join(join) => {
                assert_eq!(join.join_type, JoinType::Inner);
                assert!(
                    !join.on.is_empty(),
                    "USING clause should create join conditions"
                );
            }
            _ => panic!("Expected Join under Projection"),
        },
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_natural_join() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users NATURAL JOIN orders";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            match &*proj.input {
                LogicalPlan::Join(join) => {
                    // Natural join finds common columns (id in this case)
                    // If no common columns, it becomes a cross join
                    assert!(
                        !join.on.is_empty() || join.join_type == JoinType::Cross,
                        "Natural join should either find common columns or become cross join"
                    );
                }
                _ => panic!("Expected Join under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_three_way_join() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users u
                   JOIN orders o ON u.id = o.user_id
                   JOIN products p ON o.product_id = p.id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            match &*proj.input {
                LogicalPlan::Join(join2) => {
                    // Second join (with products)
                    assert_eq!(join2.join_type, JoinType::Inner);
                    match &*join2.left {
                        LogicalPlan::Join(join1) => {
                            // First join (users with orders)
                            assert_eq!(join1.join_type, JoinType::Inner);
                        }
                        _ => panic!("Expected nested Join for three-way join"),
                    }
                }
                _ => panic!("Expected Join under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_mixed_join_types() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users u
                   LEFT JOIN orders o ON u.id = o.user_id
                   INNER JOIN products p ON o.product_id = p.id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            match &*proj.input {
                LogicalPlan::Join(join2) => {
                    // Second join should be INNER
                    assert_eq!(join2.join_type, JoinType::Inner);
                    match &*join2.left {
                        LogicalPlan::Join(join1) => {
                            // First join should be LEFT
                            assert_eq!(join1.join_type, JoinType::Left);
                        }
                        _ => panic!("Expected nested Join"),
                    }
                }
                _ => panic!("Expected Join under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_join_with_filter() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            match &*proj.input {
                LogicalPlan::Filter(filter) => {
                    // WHERE clause creates a Filter above the Join
                    match &*filter.input {
                        LogicalPlan::Join(join) => {
                            assert_eq!(join.join_type, JoinType::Inner);
                        }
                        _ => panic!("Expected Join under Filter"),
                    }
                }
                _ => panic!("Expected Filter under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_join_with_projection() {
    let schema = create_test_schema();
    let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(proj) => {
            assert_eq!(proj.exprs.len(), 2); // u.name and o.amount
            match &*proj.input {
                LogicalPlan::Join(join) => {
                    assert_eq!(join.join_type, JoinType::Inner);
                }
                _ => panic!("Expected Join under Projection"),
            }
        }
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_join_with_aggregation() {
    let schema = create_test_schema();
    let sql = "SELECT u.name, SUM(o.amount)
                   FROM users u JOIN orders o ON u.id = o.user_id
                   GROUP BY u.name";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Aggregate(agg) => {
            assert_eq!(agg.group_expr.len(), 1); // GROUP BY u.name
            assert_eq!(agg.aggr_expr.len(), 1); // SUM(o.amount)
            match &*agg.input {
                LogicalPlan::Join(join) => {
                    assert_eq!(join.join_type, JoinType::Inner);
                }
                _ => panic!("Expected Join under Aggregate"),
            }
        }
        _ => panic!("Expected Aggregate"),
    }
}

#[test]
fn test_join_with_order_by() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Sort(sort) => {
            assert_eq!(sort.exprs.len(), 1);
            assert!(!sort.exprs[0].asc); // DESC
            match &*sort.input {
                LogicalPlan::Projection(proj) => match &*proj.input {
                    LogicalPlan::Join(join) => {
                        assert_eq!(join.join_type, JoinType::Inner);
                    }
                    _ => panic!("Expected Join under Projection"),
                },
                _ => panic!("Expected Projection under Sort"),
            }
        }
        _ => panic!("Expected Sort at top level"),
    }
}

#[test]
fn test_join_in_subquery() {
    let schema = create_test_schema();
    let sql = "SELECT * FROM (
                     SELECT u.id, u.name, o.amount
                     FROM users u JOIN orders o ON u.id = o.user_id
                   ) WHERE amount > 100";
    let plan = parse_and_build(sql, &schema).unwrap();

    match plan {
        LogicalPlan::Projection(outer_proj) => match &*outer_proj.input {
            LogicalPlan::Filter(filter) => match &*filter.input {
                LogicalPlan::Projection(inner_proj) => match &*inner_proj.input {
                    LogicalPlan::Join(join) => {
                        assert_eq!(join.join_type, JoinType::Inner);
                    }
                    _ => panic!("Expected Join in subquery"),
                },
                _ => panic!("Expected Projection for subquery"),
            },
            _ => panic!("Expected Filter"),
        },
        _ => panic!("Expected Projection at top level"),
    }
}

#[test]
fn test_join_ambiguous_column() {
    let schema = create_test_schema();
    // Both users and orders have an 'id' column
    let sql = "SELECT id FROM users JOIN orders ON users.id = orders.user_id";
    let result = parse_and_build(sql, &schema);
    // This might error or succeed depending on how we handle ambiguous columns
    // For now, just check that parsing completes
    match result {
        Ok(_) => {
            // If successful, the implementation handles ambiguous columns somehow
        }
        Err(_) => {
            // If error, the implementation rejects ambiguous columns
        }
    }
}

// Tests for strip_alias function
#[test]
fn test_strip_alias_with_alias() {
    let inner_expr = LogicalExpr::Column(Column::new("test"));
    let aliased = LogicalExpr::Alias {
        expr: Box::new(inner_expr.clone()),
        alias: "my_alias".to_string(),
    };

    let stripped = strip_alias(&aliased);
    assert_eq!(stripped, &inner_expr);
}

#[test]
fn test_strip_alias_without_alias() {
    let expr = LogicalExpr::Column(Column::new("test"));
    let stripped = strip_alias(&expr);
    assert_eq!(stripped, &expr);
}

#[test]
fn test_strip_alias_literal() {
    let expr = LogicalExpr::Literal(Value::from_i64(42));
    let stripped = strip_alias(&expr);
    assert_eq!(stripped, &expr);
}

#[test]
fn test_strip_alias_scalar_function() {
    let expr = LogicalExpr::ScalarFunction {
        fun: "substr".to_string(),
        args: std::vec![
            LogicalExpr::Column(Column::new("name")),
            LogicalExpr::Literal(Value::from_i64(1)),
            LogicalExpr::Literal(Value::from_i64(4)),
        ],
    };
    let stripped = strip_alias(&expr);
    assert_eq!(stripped, &expr);
}

#[test]
fn test_strip_alias_nested_alias() {
    // Test that strip_alias only removes the outermost alias
    let inner_expr = LogicalExpr::Column(Column::new("test"));
    let inner_alias = LogicalExpr::Alias {
        expr: Box::new(inner_expr.clone()),
        alias: "inner_alias".to_string(),
    };
    let outer_alias = LogicalExpr::Alias {
        expr: Box::new(inner_alias.clone()),
        alias: "outer_alias".to_string(),
    };

    let stripped = strip_alias(&outer_alias);
    assert_eq!(stripped, &inner_alias);

    // Stripping again should give us the inner expression
    let double_stripped = strip_alias(stripped);
    assert_eq!(double_stripped, &inner_expr);
}

#[test]
fn test_strip_alias_comparison_with_alias() {
    // Test that two expressions match when one has an alias and one doesn't
    let base_expr = LogicalExpr::ScalarFunction {
        fun: "substr".to_string(),
        args: std::vec![
            LogicalExpr::Column(Column::new("orderdate")),
            LogicalExpr::Literal(Value::from_i64(1)),
            LogicalExpr::Literal(Value::from_i64(4)),
        ],
    };

    let aliased_expr = LogicalExpr::Alias {
        expr: Box::new(base_expr.clone()),
        alias: "year".to_string(),
    };

    // Without strip_alias, they wouldn't match
    assert_ne!(&aliased_expr, &base_expr);

    // With strip_alias, they should match
    assert_eq!(strip_alias(&aliased_expr), &base_expr);
    assert_eq!(strip_alias(&base_expr), &base_expr);
}

#[test]
fn test_strip_alias_binary_expr() {
    let expr = LogicalExpr::BinaryExpr {
        left: Box::new(LogicalExpr::Column(Column::new("a"))),
        op: BinaryOperator::Add,
        right: Box::new(LogicalExpr::Literal(Value::from_i64(1))),
    };
    let stripped = strip_alias(&expr);
    assert_eq!(stripped, &expr);
}

#[test]
fn test_strip_alias_aggregate_function() {
    let expr = LogicalExpr::AggregateFunction {
        fun: AggFunc::Sum,
        args: std::vec![LogicalExpr::Column(Column::new("amount"))],
        distinct: false,
    };
    let stripped = strip_alias(&expr);
    assert_eq!(stripped, &expr);
}

#[test]
fn test_strip_alias_comparison_multiple_expressions() {
    // Test comparing a list of expressions with and without aliases
    let expr1 = LogicalExpr::Column(Column::new("a"));
    let expr2 = LogicalExpr::ScalarFunction {
        fun: "substr".to_string(),
        args: std::vec![
            LogicalExpr::Column(Column::new("b")),
            LogicalExpr::Literal(Value::from_i64(1)),
            LogicalExpr::Literal(Value::from_i64(4)),
        ],
    };

    let aliased1 = LogicalExpr::Alias {
        expr: Box::new(expr1.clone()),
        alias: "col_a".to_string(),
    };
    let aliased2 = LogicalExpr::Alias {
        expr: Box::new(expr2.clone()),
        alias: "year".to_string(),
    };

    let select_exprs = [aliased1, aliased2];
    let group_exprs = [expr1, expr2];

    // Verify that stripping aliases allows matching
    for (select_expr, group_expr) in select_exprs.iter().zip(group_exprs.iter()) {
        assert_eq!(strip_alias(select_expr), group_expr);
    }
}
