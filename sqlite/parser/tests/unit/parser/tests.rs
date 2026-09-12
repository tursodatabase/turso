use super::*;

#[test]
fn test_offset() {
    let s = "SELECT 1; SELECT 1";
    let mut p = Parser::new(s.as_bytes());
    p.next_cmd().unwrap();
    assert_eq!(&s[..p.offset()], "SELECT 1; ");
}

#[test]
fn test_offset_multiple_statements_with_insert_columns() {
    let s = "CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, col_a TEXT, col_b TEXT, col_c TEXT, col_d TEXT); INSERT INTO test (col_b, col_d, col_a, col_c) VALUES ('1', '2', '3', '4'); SELECT * FROM test;";
    let mut p = Parser::new(s.as_bytes());

    p.next_cmd().unwrap();
    let first = p.offset();
    assert_eq!(&s[..first], "CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, col_a TEXT, col_b TEXT, col_c TEXT, col_d TEXT); ");

    p.next_cmd().unwrap();
    let second = p.offset();
    assert_eq!(
        &s[first..second],
        "INSERT INTO test (col_b, col_d, col_a, col_c) VALUES ('1', '2', '3', '4'); "
    );

    p.next_cmd().unwrap();
    let third = p.offset();
    assert_eq!(&s[second..third], "SELECT * FROM test;");
}

#[test]
fn check_constraint_comments_survive_formatting() {
    for (sql, comment) in [
        (
            "CREATE TABLE t (x CHECK(x /* column comment */ > 0))",
            "/* column comment */",
        ),
        (
            "CREATE TABLE t (x, CHECK(x -- table comment\n > 0))",
            "-- table comment",
        ),
    ] {
        let command = Parser::new(sql.as_bytes()).next().unwrap().unwrap();
        let formatted = command.to_string();

        assert!(formatted.contains(comment), "formatted SQL: {formatted}");
        Parser::new(formatted.as_bytes()).next().unwrap().unwrap();
    }
}

#[test]
fn test_variable_index_bounds() {
    for sql in ["SELECT ?0", "SELECT ?250001"] {
        let mut p = Parser::new(sql.as_bytes());
        let err = p.next_cmd().unwrap_err().to_string();
        assert!(
            err.contains("variable number must be between ?1 and ?250000"),
            "unexpected error for {sql}: {err}"
        );
    }

    let mut p = Parser::new("SELECT ?250000".as_bytes());
    assert!(p.next_cmd().is_ok());
}

#[test]
fn test_namespace_qualified_parameter_names() {
    // TCL passes `$::g` and `$ns::var` into SQL; each spelling is one
    // parameter, keyed on its full text, and a repeat reuses its index.
    let sql = "SELECT $ns::var, $::g, $ns::var, :::g, @a::b::";
    let mut p = Parser::new(sql.as_bytes());
    let cmd = p.next_cmd().unwrap().unwrap();
    assert_eq!(cmd.to_string(), format!("{sql};"));
    let index = |name: &str| p.named_variables[name.as_bytes()].get();
    assert_eq!(index("$ns::var"), 1);
    assert_eq!(index("$::g"), 2);
    assert_eq!(index(":::g"), 3);
    assert_eq!(index("@a::b::"), 4);
    assert_eq!(p.named_variables.len(), 4);
}

#[test]
fn test_array_element_parameter_names() {
    // TCL array elements, `$arr(elem)`, are one parameter including the
    // suffix; only one suffix is allowed, so `$a(b)(c)` is a call-like
    // syntax error, as in SQLite.
    let sql = "SELECT $arr(elem), $ns::arr(k), $arr(elem)";
    let mut p = Parser::new(sql.as_bytes());
    let cmd = p.next_cmd().unwrap().unwrap();
    assert_eq!(cmd.to_string(), format!("{sql};"));
    assert_eq!(p.named_variables[b"$arr(elem)".as_slice()].get(), 1);
    assert_eq!(p.named_variables[b"$ns::arr(k)".as_slice()].get(), 2);
    assert_eq!(p.named_variables.len(), 2);

    let mut p = Parser::new(b"SELECT $a(b)(c)");
    assert!(p.next_cmd().is_err());
    let mut p = Parser::new(b"SELECT $a(b c)");
    let err = p.next_cmd().unwrap_err().to_string();
    assert!(err.contains("unrecognized token: \"$a(b\""), "{err}");
}

#[test]
fn test_expect_fail() {
    let testcases = vec![
        "ALTER TABLE my_table ADD COLUMN my_column PRIMARY KEY",
        "ALTER TABLE my_table ADD COLUMN my_column UNIQUE",
        // https://github.com/tursodatabase/turso/issues/7058
        "ALTER TABLE my_table ADD COLUMN my_column DEFAULT 5 AS (1)",
        "ALTER TABLE my_table ADD COLUMN my_column AS (1) DEFAULT 5",
        "CREATE TABLE foo(b DEFAULT 5 AS (a+1))",
        "CREATE TABLE foo(b AS (a+1) DEFAULT 5)",
        "CREATE TEMP TABLE baz.foo(bar)",
        "CREATE TABLE foo(d INT AS (a*abs(b)))",
        "CREATE TABLE foo(d INT AS (a*abs(b)))",
        "CREATE TABLE foo(bar) STRICT",
        "CREATE TABLE foo(bar) WITHOUT ROWID",
        "CREATE VIEW foo(bar, bar) AS SELECT 1, 1",
        "CREATE VIEW foo(bar) AS SELECT 1, 1",
        "CREATE VIEW v AS WITH cte AS (SELECT 1), cte AS (SELECT 1) SELECT 1",
        "DELETE FROM my_table ORDER BY col1",
        "INSERT INTO my_table(bar) DEFAULT VALUES",
        "INSERT INTO my_table(bar, baz, barr) VALUES (1, 1)",
        "UPDATE foo SET bar = 1 ORDER BY bar",
        "CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) RETURNING bar, baz; END",
        "CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) ON CONFLICT (bar, baz) WHERE 1 DO NOTHING RETURNING bar, baz; END",
        "CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) ON CONFLICT DO UPDATE SET (bar, baz) = 1 WHERE 1 RETURNING bar, baz; END",
        "CREATE TRIGGER foo INSERT ON bar BEGIN DELETE FROM foo RETURNING *; END",
        "CREATE TRIGGER foo INSERT ON bar BEGIN UPDATE foo SET bar = 1 RETURNING *; END",
    ];

    for tc in testcases {
        let mut p = Parser::new(tc.as_bytes());
        let result = p.next_cmd();
        assert!(result.is_err(), "Expected error for: {tc}");
    }
}

#[expect(clippy::large_stack_frames)]
#[test]
fn test_parser() {
    let test_cases = vec![
        // begin
        (
            b"BEGIN".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: None,
                name: None,
            })],
        ),
        (
            b"EXPLAIN BEGIN".as_slice(),
            vec![Cmd::Explain(Stmt::Begin {
                typ: None,
                name: None,
            })],
        ),
        (
            b"EXPLAIN QUERY PLAN BEGIN".as_slice(),
            vec![Cmd::ExplainQueryPlan {
                stmt: Stmt::Begin {
                    typ: None,
                    name: None,
                },
                format: EqpFormat::Text,
            }],
        ),
        (
            b"EXPLAIN QUERY PLAN FORMAT=JSON BEGIN".as_slice(),
            vec![Cmd::ExplainQueryPlan {
                stmt: Stmt::Begin {
                    typ: None,
                    name: None,
                },
                format: EqpFormat::Json,
            }],
        ),
        (
            b"explain query plan format = json begin".as_slice(),
            vec![Cmd::ExplainQueryPlan {
                stmt: Stmt::Begin {
                    typ: None,
                    name: None,
                },
                format: EqpFormat::Json,
            }],
        ),
        (
            b"EXPLAIN QUERY PLAN FORMAT=TEXT BEGIN".as_slice(),
            vec![Cmd::ExplainQueryPlan {
                stmt: Stmt::Begin {
                    typ: None,
                    name: None,
                },
                format: EqpFormat::Text,
            }],
        ),
        (
            b"BEGIN TRANSACTION".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: None,
                name: None,
            })],
        ),
        (
            b"BEGIN DEFERRED TRANSACTION".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Deferred),
                name: None,
            })],
        ),
        (
            b"BEGIN IMMEDIATE TRANSACTION".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Immediate),
                name: None,
            })],
        ),
        (
            b"BEGIN EXCLUSIVE TRANSACTION".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Exclusive),
                name: None,
            })],
        ),
        (
            b"BEGIN DEFERRED TRANSACTION my_transaction".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Deferred),
                name: Some(Name::from_string("my_transaction")),
            })],
        ),
        (
            b"BEGIN IMMEDIATE TRANSACTION my_transaction".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Immediate),
                name: Some(Name::from_string("my_transaction")),
            })],
        ),
        (
            b"BEGIN EXCLUSIVE TRANSACTION my_transaction".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Exclusive),
                name: Some(Name::from_string("my_transaction")),
            })],
        ),
        (
            b"BEGIN EXCLUSIVE TRANSACTION 'my_transaction'".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Exclusive),
                name: Some(Name::from_string("'my_transaction'")),
            })],
        ),
        (
            b"BEGIN CONCURRENT TRANSACTION".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Concurrent),
                name: None,
            })],
        ),
        (
            b"BEGIN CONCURRENT TRANSACTION my_transaction".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Concurrent),
                name: Some(Name::from_string("my_transaction")),
            })],
        ),
        (
            b"BEGIN CONCURRENT TRANSACTION 'my_transaction'".as_slice(),
            vec![Cmd::Stmt(Stmt::Begin {
                typ: Some(TransactionType::Concurrent),
                name: Some(Name::from_string("'my_transaction'")),
            })],
        ),
        (
            ";;;BEGIN;BEGIN;;;;;;BEGIN".as_bytes(),
            vec![
                Cmd::Stmt(Stmt::Begin {
                    typ: None,
                    name: None,
                }),
                Cmd::Stmt(Stmt::Begin {
                    typ: None,
                    name: None,
                }),
                Cmd::Stmt(Stmt::Begin {
                    typ: None,
                    name: None,
                }),
            ],
        ),
        // commit
        (
            b"COMMIT".as_slice(),
            vec![Cmd::Stmt(Stmt::Commit { name: None })],
        ),
        (
            b"END".as_slice(),
            vec![Cmd::Stmt(Stmt::Commit { name: None })],
        ),
        (
            b"COMMIT TRANSACTION".as_slice(),
            vec![Cmd::Stmt(Stmt::Commit { name: None })],
        ),
        (
            b"END TRANSACTION".as_slice(),
            vec![Cmd::Stmt(Stmt::Commit { name: None })],
        ),
        (
            b"COMMIT TRANSACTION my_transaction".as_slice(),
            vec![Cmd::Stmt(Stmt::Commit {
                name: Some(Name::from_string("my_transaction")),
            })],
        ),
        (
            b"END TRANSACTION my_transaction".as_slice(),
            vec![Cmd::Stmt(Stmt::Commit {
                name: Some(Name::from_string("my_transaction")),
            })],
        ),
        // Rollback
        (
            b"ROLLBACK".as_slice(),
            vec![Cmd::Stmt(Stmt::Rollback {
                tx_name: None,
                savepoint_name: None,
            })],
        ),
        (
            b"ROLLBACK TO SAVEPOINT my_savepoint".as_slice(),
            vec![Cmd::Stmt(Stmt::Rollback {
                tx_name: None,
                savepoint_name: Some(Name::from_string("my_savepoint")),
            })],
        ),
        (
            b"ROLLBACK TO my_savepoint".as_slice(),
            vec![Cmd::Stmt(Stmt::Rollback {
                tx_name: None,
                savepoint_name: Some(Name::from_string("my_savepoint")),
            })],
        ),
        (
            b"ROLLBACK TRANSACTION my_transaction".as_slice(),
            vec![Cmd::Stmt(Stmt::Rollback {
                tx_name: Some(Name::from_string("my_transaction")),
                savepoint_name: None,
            })],
        ),
        (
            b"ROLLBACK TRANSACTION my_transaction TO my_savepoint".as_slice(),
            vec![Cmd::Stmt(Stmt::Rollback {
                tx_name: Some(Name::from_string("my_transaction")),
                savepoint_name: Some(Name::from_string("my_savepoint")),
            })],
        ),
        // savepoint
        (
            b"SAVEPOINT my_savepoint".as_slice(),
            vec![Cmd::Stmt(Stmt::Savepoint {
                name: Name::from_string("my_savepoint"),
            })],
        ),
        (
            b"SAVEPOINT 'my_savepoint'".as_slice(),
            vec![Cmd::Stmt(Stmt::Savepoint {
                name: Name::from_string("'my_savepoint'"),
            })],
        ),
        // release
        (
            b"RELEASE my_savepoint".as_slice(),
            vec![Cmd::Stmt(Stmt::Release {
                name: Name::from_string("my_savepoint"),
            })],
        ),
        (
            b"RELEASE SAVEPOINT my_savepoint".as_slice(),
            vec![Cmd::Stmt(Stmt::Release {
                name: Name::from_string("my_savepoint"),
            })],
        ),
        (
            b"RELEASE SAVEPOINT 'my_savepoint'".as_slice(),
            vec![Cmd::Stmt(Stmt::Release {
                name: Name::from_string("'my_savepoint'"),
            })],
        ),
        (
            b"RELEASE SAVEPOINT ABORT".as_slice(),
            vec![Cmd::Stmt(Stmt::Release {
                name: Name::from_string("ABORT"),
            })],
        ),
        // test expr operand
        (
            b"SELECT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT (1)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Parenthesized(vec![Box::new(Expr::Literal(
                                Literal::Numeric("1".to_owned()),
                            ))])),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT NULL".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Null)),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT X'ab'".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Blob("X'ab'".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 3.333".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("3.333".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT ?".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Variable(Variable::indexed(1u32.try_into().unwrap()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT ?, :named, ?".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![
                            ResultColumn::Expr(
                                Box::new(Expr::Variable(Variable::indexed(1u32.try_into().unwrap()))),
                                None,
                            ),
                            ResultColumn::Expr(
                                Box::new(Expr::Variable(Variable::named(
                                    ":named".to_owned(),
                                    2u32.try_into().unwrap(),
                                ))),
                                None,
                            ),
                            ResultColumn::Expr(
                                Box::new(Expr::Variable(Variable::indexed(3u32.try_into().unwrap()))),
                                None,
                            ),
                        ],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT ?1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Variable(Variable::indexed(1u32.try_into().unwrap()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT CAST(1 AS INTEGER)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Cast {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                type_name: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                    array_dimensions: 0,
                                }),
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT CAST(1 AS VARCHAR(255))".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Cast {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                type_name: Some(Type {
                                    name: "VARCHAR".to_owned(),
                                    size: Some(TypeSize::MaxSize(Box::new(Expr::Literal(
                                        Literal::Numeric("255".to_owned()),
                                    )))),
                                    array_dimensions: 0,
                                }),
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT CAST(1 AS DECIMAL(10, 5))".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Cast {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                type_name: Some(Type {
                                    name: "DECIMAL".to_owned(),
                                    size: Some(TypeSize::TypeSize(
                                        Box::new(Expr::Literal(Literal::Numeric(
                                            "10".to_owned(),
                                        ))),
                                        Box::new(Expr::Literal(Literal::Numeric(
                                            "5".to_owned(),
                                        ))),
                                    )),
                                    array_dimensions: 0,
                                }),
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT CURRENT_DATE".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::CurrentDate)),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT CURRENT_TIME".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::CurrentTime)),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT CURRENT_TIMESTAMP".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::CurrentTimestamp)),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT NOT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Unary(
                                UnaryOperator::Not,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT NOT 1 + 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Unary(
                                UnaryOperator::Not,
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Add,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT ~1 + 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary(
                                Box::new(Expr::Unary(
                                    UnaryOperator::BitwiseNot,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                Operator::Add,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT +1 + 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary(
                                Box::new(Expr::Unary(
                                    UnaryOperator::Positive,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                Operator::Add,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT -1 + 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary(
                                Box::new(Expr::Unary(
                                    UnaryOperator::Negative,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                Operator::Add,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT EXISTS (SELECT 1)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Exists(Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric(
                                                "1".to_owned(),
                                            ))),
                                            None,
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            })),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT CASE WHEN 1 THEN 2 ELSE 3 END".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Case {
                                base: None,
                                when_then_pairs: vec![(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )],
                                else_expr: Some(Box::new(Expr::Literal(Literal::Numeric(
                                    "3".to_owned(),
                                )))),
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT CASE 4 WHEN 1 THEN 2 ELSE 3 END".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Case {
                                base: Some(Box::new(Expr::Literal(Literal::Numeric(
                                    "4".to_owned(),
                                )))),
                                when_then_pairs: vec![(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )],
                                else_expr: Some(Box::new(Expr::Literal(Literal::Numeric(
                                    "3".to_owned(),
                                )))),
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT CASE 4 WHEN 1 THEN 2 END".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Case {
                                base: Some(Box::new(Expr::Literal(Literal::Numeric(
                                    "4".to_owned(),
                                )))),
                                when_then_pairs: vec![(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )],
                                else_expr: None,
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT (SELECT 1)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Subquery(Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric(
                                                "1".to_owned(),
                                            ))),
                                            None,
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            })),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT RAISE (Ignore)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Raise(ResolveType::Ignore, None)),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT RAISE (FAIL, 'error')".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Raise(
                                ResolveType::Fail,
                                Some(Box::new(Expr::Literal(Literal::String(
                                    "'error'".to_owned(),
                                )))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT RAISE (ROLLBACK, 'error')".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Raise(
                                ResolveType::Rollback,
                                Some(Box::new(Expr::Literal(Literal::String(
                                    "'error'".to_owned(),
                                )))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT RAISE (ABORT, 'error')".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Raise(
                                ResolveType::Abort,
                                Some(Box::new(Expr::Literal(Literal::String(
                                    "'error'".to_owned(),
                                )))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT RAISE (ABORT, 'error')".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Raise(
                                ResolveType::Abort,
                                Some(Box::new(Expr::Literal(Literal::String(
                                    "'error'".to_owned(),
                                )))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT RAISE ('error')".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Raise(
                                ResolveType::Abort,
                                Some(Box::new(Expr::Literal(Literal::String(
                                    "'error'".to_owned(),
                                )))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT col_1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Id(Name::exact("col_1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 'col_1'".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::String("'col_1'".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT tbl_name.col_1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Qualified(
                                Name::exact("tbl_name".to_owned()),
                                Name::exact("col_1".to_owned()),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT schema_name.tbl_name.col_1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::DoublyQualified(
                                Name::exact("schema_name".to_owned()),
                                Name::exact("tbl_name".to_owned()),
                                Name::exact("col_1".to_owned()),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name()".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: None,
                                args: vec![],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: None,
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) FILTER (WHERE x) OVER window_name".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: Some(Box::new(Expr::Id(Name::exact(
                                        "x".to_owned(),
                                    )))),
                                    over_clause: Some(Over::Name(Name::exact(
                                        "window_name".to_owned(),
                                    ))),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (PARTITION BY product)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: None,
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: None,
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: None,
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product ORDER BY test ASC NULLS LAST)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![
                                            SortedColumn {
                                                expr: Box::new(Expr::Id(Name::exact("test".to_owned()))),
                                                order: Some(SortOrder::Asc),
                                                nulls: Some(NullsOrder::Last),
                                            }
                                        ],
                                        frame_clause: None,
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Rows,
                                            start: FrameBound::Preceding(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                            end: Some(FrameBound::CurrentRow),
                                            exclude: None
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Range,
                                            start: FrameBound::Preceding(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                            end: Some(FrameBound::CurrentRow),
                                            exclude: None
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::Preceding(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                            end: Some(FrameBound::CurrentRow),
                                            exclude: None
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN 2 FOLLOWING AND CURRENT ROW)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::Following(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                            end: Some(FrameBound::CurrentRow),
                                            exclude: None
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::UnboundedPreceding,
                                            end: Some(FrameBound::CurrentRow),
                                            exclude: None
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND CURRENT ROW)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::CurrentRow,
                                            end: Some(FrameBound::CurrentRow),
                                            exclude: None
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::CurrentRow,
                                            end: Some(FrameBound::UnboundedFollowing),
                                            exclude: None
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND 1 PRECEDING)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::CurrentRow,
                                            end: Some(FrameBound::Preceding(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                                            )),
                                            exclude: None
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::CurrentRow,
                                            end: Some(FrameBound::Following(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                                            )),
                                            exclude: None
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE NO OTHERS)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::CurrentRow,
                                            end: None,
                                            exclude: Some(FrameExclude::NoOthers)
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE CURRENT ROW)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::CurrentRow,
                                            end: None,
                                            exclude: Some(FrameExclude::CurrentRow)
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE GROUP)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::CurrentRow,
                                            end: None,
                                            exclude: Some(FrameExclude::Group)
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE TIES)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("func_name".to_owned()),
                                distinctness: Some(Distinctness::Distinct),
                                args: vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: Some(Over::Window(Window {
                                        base: Some(Name::exact("test".to_owned())),
                                        partition_by: vec![Box::new(Expr::Id(Name::exact(
                                            "product".to_owned(),
                                        )))],
                                        order_by: vec![],
                                        frame_clause: Some(FrameClause{
                                            mode: FrameMode::Groups,
                                            start: FrameBound::CurrentRow,
                                            end: None,
                                            exclude: Some(FrameExclude::Ties)
                                        }),
                                    })),
                                },
                            }),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        // parse expr
        (
            b"SELECT 1 NOT NULL AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::NotNull(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 IS 1 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
            select: OneSelect::Select {
                distinctness: None,
                columns: vec![ResultColumn::Expr(
                    Box::new(Expr::Binary (
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::Is,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                        )),
                        Operator::And,
                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    )),
                    None,
                )],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 IS NOT 1 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
            select: OneSelect::Select {
                distinctness: None,
                columns: vec![ResultColumn::Expr(
                    Box::new(Expr::Binary (
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::IsNot,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                        )),
                        Operator::And,
                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    )),
                    None,
                )],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 IS NOT DISTINCT FROM 1 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
            select: OneSelect::Select {
                distinctness: None,
                columns: vec![ResultColumn::Expr(
                    Box::new(Expr::Binary (
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::Is,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                        )),
                        Operator::And,
                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    )),
                    None,
                )],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 IS DISTINCT FROM 1 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
            select: OneSelect::Select {
                distinctness: None,
                columns: vec![ResultColumn::Expr(
                    Box::new(Expr::Binary (
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::IsNot,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                        )),
                        Operator::And,
                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    )),
                    None,
                )],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 + 2 * 3".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
            select: OneSelect::Select {
                distinctness: None,
                columns: vec![ResultColumn::Expr(
                    Box::new(Expr::Binary (
                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        Operator::Add,
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                            Operator::Multiply,
                            Box::new(Expr::Literal(Literal::Numeric("3".to_owned())))
                        ))
                    )),
                    None,
                )],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 AND 2 OR 3".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
            select: OneSelect::Select {
                distinctness: None,
                columns: vec![ResultColumn::Expr(
                    Box::new(Expr::Binary (
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))
                        )),
                        Operator::Or,
                        Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                    )),
                    None,
                )],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 = 0 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
            select: OneSelect::Select {
                distinctness: None,
                columns: vec![ResultColumn::Expr(
                    Box::new(Expr::Binary (
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::Equals,
                            Box::new(Expr::Literal(Literal::Numeric("0".to_owned())))
                        )),
                        Operator::And,
                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    )),
                    None,
                )],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 != 0 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
            select: OneSelect::Select {
                distinctness: None,
                columns: vec![ResultColumn::Expr(
                    Box::new(Expr::Binary (
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::NotEquals,
                            Box::new(Expr::Literal(Literal::Numeric("0".to_owned())))
                        )),
                        Operator::And,
                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    )),
                    None,
                )],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 BETWEEN 2 AND 3 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Between {
                                    lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    not: false,
                                    start: Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    end: Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
            compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 NOT BETWEEN 2 AND 3 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Between {
                                    lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    not: true,
                                    start: Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    end: Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 IN (SELECT 1) AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::InSelect {
                                    lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    not: false,
                                    rhs: Select {
                                        with: None,
                                        body: SelectBody {
                                            select: OneSelect::Select {
                                                distinctness: None,
                                                columns: vec![ResultColumn::Expr(
                                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                    None,
                                                )],
                                                from: None,
                                                where_clause: None,
                                                group_by: None,
                                                window_clause: vec![],
                                            },
                                            compounds: vec![],
                                        },
                                        order_by: vec![],
                                        limit: None
                                    },
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 NOT IN (SELECT 1) AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::InSelect {
                                    lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    not: true,
                                    rhs: Select {
                                        with: None,
                                        body: SelectBody {
                                            select: OneSelect::Select {
                                                distinctness: None,
                                                columns: vec![ResultColumn::Expr(
                                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                    None,
                                                )],
                                                from: None,
                                                where_clause: None,
                                                group_by: None,
                                                window_clause: vec![],
                                            },
                                            compounds: vec![],
                                        },
                                        order_by: vec![],
                                        limit: None
                                    },
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 IN (1, 2, 3) AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::InList {
                                    lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    not: false,
                                    rhs: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                    ],
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 IN test(1, 2, 3) AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::InTable {
                                    lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    not: false,
                                    rhs: QualifiedName {
                                        db_name: None,
                                        name: Name::exact("test".to_owned()),
                                        alias: None,
                                    },
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                    ],
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 'test' MATCH 'foo' AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Like {
                                    lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                    not: false,
                                    op: LikeOperator::Match,
                                    rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                    escape: None,
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 'test' NOT MATCH 'foo' AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Like {
                                    lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                    not: true,
                                    op: LikeOperator::Match,
                                    rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                    escape: None,
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 'test' NOT MATCH 'foo' ESCAPE 'bar' AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Like {
                                    lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                    not: true,
                                    op: LikeOperator::Match,
                                    rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                    escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 'test' NOT LIKE 'foo' ESCAPE 'bar' AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Like {
                                    lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                    not: true,
                                    op: LikeOperator::Like,
                                    rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                    escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 'test' NOT GLOB 'foo' ESCAPE 'bar' AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Like {
                                    lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                    not: true,
                                    op: LikeOperator::Glob,
                                    rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                    escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 'test' NOT REGEXP 'foo' ESCAPE 'bar' AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Like {
                                    lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                    not: true,
                                    op: LikeOperator::Regexp,
                                    rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                    escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                }),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 ISNULL AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::IsNull (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 NOTNULL AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::NotNull(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 < 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Less,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 > 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Greater,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 <= 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::LessEquals,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 >= 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::GreaterEquals,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 & 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::BitwiseAnd,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 | 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::BitwiseOr,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 << 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::LeftShift,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 >> 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::RightShift,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 / 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Divide,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 % 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Modulus,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 || 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Concat,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 -> 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::ArrowRight,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 ->> 2 AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::ArrowRightShift,
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 'foo' COLLATE bar AND 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Binary (
                                Box::new(Expr::Collate (
                                    Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                    Name::exact("bar".to_owned()),
                                )),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            )),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        // test select
        (
            b"VALUES (1, 2), (3, 4), (5, 6)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Values(vec![
                        vec![
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                        ],
                        vec![
                            Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                            Box::new(Expr::Literal(Literal::Numeric("4".to_owned()))),
                        ],
                        vec![
                            Box::new(Expr::Literal(Literal::Numeric("5".to_owned()))),
                            Box::new(Expr::Literal(Literal::Numeric("6".to_owned()))),
                        ],
                    ]),
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT *".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Star],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT tbl_name.*".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::TableStar(
                            Name::exact("tbl_name".to_owned()),
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT col_1 OVER".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Id(Name::exact("col_1".to_owned()))),
                            Some(As::Elided(Name::exact("OVER".to_owned()))),
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT col_1 AS OVER".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Id(Name::exact("col_1".to_owned()))),
                            Some(As::As(Name::exact("OVER".to_owned()))),
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"WITH test AS (SELECT 1) SELECT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: Some(With {
                    recursive: false,
                    ctes: vec![
                        CommonTableExpr {
                            tbl_name: Name::exact("test".to_owned()),
                            columns: vec![],
                            materialized: Materialized::Any,
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            None,
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            }
                        },
                    ]
                }),
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"WITH test(col_1) AS MATERIALIZED (SELECT 1 AS col_1) SELECT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: Some(With {
                    recursive: false,
                    ctes: vec![
                        CommonTableExpr {
                            tbl_name: Name::exact("test".to_owned()),
                            columns: vec![
                                IndexedColumn {
                                    col_name: Name::exact("col_1".to_owned()),
                                    collation_name: None,
                                    order: None,
                                },
                            ],
                            materialized: Materialized::Yes,
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Some(As::As(Name::exact("col_1".to_owned()))),
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            }
                        },
                    ]
                }),
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"WITH test(col_1) AS NOT MATERIALIZED (SELECT 1 AS col_1) SELECT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: Some(With {
                    recursive: false,
                    ctes: vec![
                        CommonTableExpr {
                            tbl_name: Name::exact("test".to_owned()),
                            columns: vec![
                                IndexedColumn {
                                    col_name: Name::exact("col_1".to_owned()),
                                    collation_name: None,
                                    order: None,
                                },
                            ],
                            materialized: Materialized::No,
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Some(As::As(Name::exact("col_1".to_owned()))),
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            }
                        },
                    ]
                }),
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"WITH test AS (SELECT 1), test_2 AS (SELECT 1) SELECT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: Some(With {
                    recursive: false,
                    ctes: vec![
                        CommonTableExpr {
                            tbl_name: Name::exact("test".to_owned()),
                            columns: vec![],
                            materialized: Materialized::Any,
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            None
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            }
                        },
                        CommonTableExpr {
                            tbl_name: Name::exact("test_2".to_owned()),
                            columns: vec![],
                            materialized: Materialized::Any,
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            None
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            }
                        },
                    ]
                }),
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 ORDER BY 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![
                    SortedColumn {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        order: None,
                        nulls: None,
                    },
                ],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 ORDER BY 1 DESC NULLS FIRST".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![
                    SortedColumn {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        order: Some(SortOrder::Desc),
                        nulls: Some(NullsOrder::First),
                    },
                ],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 ORDER BY 1 ASC NULLS LAST".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![
                    SortedColumn {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        order: Some(SortOrder::Asc),
                        nulls: Some(NullsOrder::Last),
                    },
                ],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 LIMIT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: Some(Limit {
                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    offset: None,
                }),
            }))],
        ),
        (
            b"SELECT 1 LIMIT 1,2".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: Some(Limit {
                    expr: Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                    offset: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                }),
            }))],
        ),
        (
            b"SELECT 1 LIMIT 1 OFFSET 2".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: Some(Limit {
                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    offset: Some(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                }),
            }))],
        ),
        (
            b"SELECT 1 UNION SELECT 2".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![
                        CompoundSelect {
                            operator: CompoundOperator::Union,
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            }
                        }
                    ],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 UNION ALL SELECT 2".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![
                        CompoundSelect {
                            operator: CompoundOperator::UnionAll,
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            }
                        }
                    ],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 EXCEPT SELECT 2".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![
                        CompoundSelect {
                            operator: CompoundOperator::Except,
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            }
                        }
                    ],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 INTERSECT SELECT 2".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![
                        CompoundSelect {
                            operator: CompoundOperator::Intersect,
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            }
                        }
                    ],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo(1, 2)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::TableCall(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM (SELECT 1)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Select(
                                Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None,
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                },
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM (tbl_name)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Sub(
                                FromClause {
                                    select: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("tbl_name".to_owned()), alias: None },
                                        None,
                                        None
                                    )),
                                    joins: vec![]
                                },
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo INDEXED BY bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                Some(Indexed::IndexedBy(Name::exact("bar".to_owned()))),
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo NOT INDEXED".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                Some(Indexed::NotIndexed),
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo, bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::Comma,
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo, bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::Comma,
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo JOIN bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(None),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo NATURAL JOIN bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(Some(JoinType::NATURAL)),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo CROSS JOIN bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(Some(JoinType::INNER|JoinType::CROSS)),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo LEFT JOIN bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(Some(JoinType::LEFT|JoinType::OUTER)),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo RIGHT JOIN bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(Some(JoinType::RIGHT|JoinType::OUTER)),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo FULL JOIN bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(Some(JoinType::LEFT | JoinType::RIGHT | JoinType::OUTER)),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo INNER JOIN bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(Some(JoinType::INNER)),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo NATURAL INNER JOIN bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(Some(JoinType::NATURAL | JoinType::INNER)),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo NATURAL LEFT OUTER JOIN bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(Some(JoinType::NATURAL | JoinType::LEFT | JoinType::OUTER)),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo JOIN bar ON 1 = 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(None),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: Some(JoinConstraint::On(Box::new(Expr::Binary(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Equals,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )))),
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo JOIN bar USING (col_1)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(None),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: Some(JoinConstraint::Using(vec![
                                        Name::exact("col_1".to_owned()),
                                    ])),
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo JOIN bar bar_alias USING (col_1)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(None),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        Some(As::Elided(Name::exact("bar_alias".to_owned()))),
                                        None,
                                    )),
                                    constraint: Some(JoinConstraint::Using(vec![
                                        Name::exact("col_1".to_owned()),
                                    ])),
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo JOIN bar(1, 2)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(None),
                                    table: Box::new(SelectTable::TableCall(
                                        QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo JOIN (VALUES (1,2), (3, 4))".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(None),
                                    table: Box::new(SelectTable::Select(
                                        Select {
                                            with: None,
                                            body: SelectBody {
                                                select: OneSelect::Values(vec![
                                                vec![
                                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))
                                                ],
                                                vec![
                                                    Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                                    Box::new(Expr::Literal(Literal::Numeric("4".to_owned())))
                                                ],
                                                ]),
                                                compounds: vec![],
                                            },
                                            order_by: vec![],
                                            limit: None,
                                        },
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo JOIN (bar)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(None),
                                    table: Box::new(SelectTable::Sub(
                                        FromClause {
                                            select: Box::new(SelectTable::Table(
                                                QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                                None,
                                                None,
                                            )),
                                            joins: vec![]
                                        },
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo WHERE 1 = 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: Some(Box::new(Expr::Binary(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::Equals,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        ))),
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo GROUP BY 1 = 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: Some(GroupBy {
                            exprs: vec![
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Equals,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                            ],
                            having: None,
                        }),
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo GROUP BY 1 = 1 HAVING 1 = 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: Some(GroupBy {
                            exprs: vec![
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Equals,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                            ],
                            having: Some(Box::new(Expr::Binary(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Equals,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            ))),
                        }),
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT 1 FROM foo GROUP BY 1 = 1 HAVING 1 = 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            None,
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: Some(GroupBy {
                            exprs: vec![
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Equals,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                            ],
                            having: Some(Box::new(Expr::Binary(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Equals,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            ))),
                        }),
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT sum(a) s FROM t HAVING s = 15".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Expr(
                            Box::new(Expr::FunctionCall {
                                name: Name::exact("sum".to_owned()),
                                distinctness: None,
                                args: vec![Box::new(Expr::Id(Name::exact("a".to_owned())))],
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: None,
                                },
                            }),
                            Some(As::Elided(Name::exact("s".to_owned()))),
                        )],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("t".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: Some(GroupBy {
                            exprs: vec![],
                            having: Some(Box::new(Expr::Binary(
                                Box::new(Expr::Id(Name::exact("s".to_owned()))),
                                Operator::Equals,
                                Box::new(Expr::Literal(Literal::Numeric("15".to_owned()))),
                            ))),
                        }),
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT * FROM t0 WINDOW JOIN t0;".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Star],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("t0".to_owned()), alias: None },
                                Some(As::Elided(Name::exact("WINDOW".to_owned()))),
                                None,
                            )),
                            joins: vec![
                                JoinedSelectTable {
                                    operator: JoinOperator::TypedJoin(None),
                                    table: Box::new(SelectTable::Table(
                                        QualifiedName { db_name: None, name: Name::exact("t0".to_owned()), alias: None },
                                        None,
                                        None,
                                    )),
                                    constraint: None,
                                }
                            ]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT * FROM t0 WINDOW window_1 AS (PARTITION BY product)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Star],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("t0".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![
                            WindowDef {
                                name: Name::exact("window_1".to_owned()),
                                window: Window {
                                    base: None,
                                    partition_by: vec![
                                        Box::new(Expr::Id(Name::exact("product".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    frame_clause: None,
                                },
                            }
                        ],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        (
            b"SELECT * FROM t0 WINDOW window_1 AS (PARTITION BY product), window_2 AS (PARTITION BY product_2)".as_slice(),
            vec![Cmd::Stmt(Stmt::Select(Select {
                with: None,
                body: SelectBody {
                    select: OneSelect::Select {
                        distinctness: None,
                        columns: vec![ResultColumn::Star],
                        from: Some(FromClause {
                            select: Box::new(SelectTable::Table(
                                QualifiedName { db_name: None, name: Name::exact("t0".to_owned()), alias: None },
                                None,
                                None,
                            )),
                            joins: vec![]
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![
                            WindowDef {
                                name: Name::exact("window_1".to_owned()),
                                window: Window {
                                    base: None,
                                    partition_by: vec![
                                        Box::new(Expr::Id(Name::exact("product".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    frame_clause: None,
                                },
                            },
                            WindowDef {
                                name: Name::exact("window_2".to_owned()),
                                window: Window {
                                    base: None,
                                    partition_by: vec![
                                        Box::new(Expr::Id(Name::exact("product_2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    frame_clause: None,
                                },
                            }
                        ],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            }))],
        ),
        // parse Analyze
        (
            b"ANALYZE".as_slice(),
            vec![Cmd::Stmt(Stmt::Analyze {
                name: None,
            })],
        ),
        (
            b"ANALYZE foo".as_slice(),
            vec![Cmd::Stmt(Stmt::Analyze {
                name: Some(QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None }),
            })],
        ),
        // parse attach
        (
            b"ATTACH DATABASE 'foo' AS bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Attach {
                expr: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                db_name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                key: None,
            })],
        ),
        (
            b"ATTACH 'foo' AS bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Attach {
                expr: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                db_name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                key: None,
            })],
        ),
        (
            b"ATTACH 'foo' AS bar key baz".as_slice(),
            vec![Cmd::Stmt(Stmt::Attach {
                expr: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                db_name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                key: Some(Box::new(Expr::Id(Name::exact("baz".to_owned())))),
            })],
        ),
        // parse detach
        (
            b"DETACH DATABASE bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Detach {
                name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
            })],
        ),
        (
            b"DETACH bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Detach {
                name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
            })],
        ),
        // parse pragma
        (
            b"PRAGMA foreign_keys = ON".as_slice(),
            vec![Cmd::Stmt(Stmt::Pragma {
                name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Keyword("ON".to_owned()))))),
            })],
        ),
        (
            b"PRAGMA foreign_keys = DELETE".as_slice(),
            vec![Cmd::Stmt(Stmt::Pragma {
                name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Keyword("DELETE".to_owned()))))),
            })],
        ),
        (
            b"PRAGMA foreign_keys = DEFAULT".as_slice(),
            vec![Cmd::Stmt(Stmt::Pragma {
                name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Keyword("DEFAULT".to_owned()))))),
            })],
        ),
        (
            b"PRAGMA foreign_keys".as_slice(),
            vec![Cmd::Stmt(Stmt::Pragma {
                name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                body: None,
            })],
        ),
        (
            b"PRAGMA foreign_keys = 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Pragma {
                name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))))),
            })],
        ),
        (
            b"PRAGMA foreign_keys = test".as_slice(),
            vec![Cmd::Stmt(Stmt::Pragma {
                name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                body: Some(PragmaBody::Equals(Box::new(Expr::Name(Name::exact("test".to_owned()))))),
            })],
        ),
        (
            b"PRAGMA foreign_keys".as_slice(),
            vec![Cmd::Stmt(Stmt::Pragma {
                name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                body: None,
            })],
        ),
        // parse vacuum
        (
            b"VACUUM".as_slice(),
            vec![Cmd::Stmt(Stmt::Vacuum {
                name: None,
                into: None,
            })],
        ),
        (
            b"VACUUM INTO 'foo'".as_slice(),
            vec![Cmd::Stmt(Stmt::Vacuum {
                name: None,
                into: Some(Box::new(Expr::Literal(Literal::String("'foo'".to_owned())))),
            })],
        ),
        (
            b"VACUUM INTO foo".as_slice(),
            vec![Cmd::Stmt(Stmt::Vacuum {
                name: None,
                into: Some(Box::new(Expr::Id(Name::exact("foo".to_owned())))),
            })],
        ),
        (
            b"VACUUM foo".as_slice(),
            vec![Cmd::Stmt(Stmt::Vacuum {
                name: Some(Name::exact("foo".to_owned())),
                into: None,
            })],
        ),
        (
            b"VACUUM foo INTO 'bar'".as_slice(),
            vec![Cmd::Stmt(Stmt::Vacuum {
                name: Some(Name::exact("foo".to_owned())),
                into: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
            })],
        ),
        // parse alter
        (
            b"ALTER TABLE foo RENAME TO bar".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::RenameTo(Name::exact("bar".to_owned())),
            }))],
        ),
        (
            b"ALTER TABLE foo RENAME baz TO bar".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::RenameColumn {
                    old: Name::exact("baz".to_owned()),
                    new: Name::exact("bar".to_owned())
                },
            }))],
        ),
        (
            b"ALTER TABLE foo RENAME COLUMN baz TO bar".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::RenameColumn {
                    old: Name::exact("baz".to_owned()),
                    new: Name::exact("bar".to_owned())
                },
            }))],
        ),
        (
            b"ALTER TABLE foo DROP baz".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::DropColumn(Name::exact("baz".to_owned())),
            }))],
        ),
        (
            b"ALTER TABLE foo DROP COLUMN baz".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::DropColumn(Name::exact("baz".to_owned())),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD baz".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: None,
                    constraints: vec![],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Default(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                            ),
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT (1)".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Default(
                                Box::new(Expr::Parenthesized(vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                ]))
                            ),
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT +1".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Default(
                                Box::new(Expr::Unary(
                                    UnaryOperator::Positive,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                ))
                            ),
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT -1".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Default(
                                Box::new(Expr::Unary(
                                    UnaryOperator::Negative,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                ))
                            ),
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT hello".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Default(
                                Box::new(Expr::Id(Name::exact("hello".to_owned())))
                            ),
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER NULL".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::NotNull {
                                nullable: true,
                                conflict_clause: None,
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT IGNORE".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::NotNull {
                                nullable: false,
                                conflict_clause: Some(ResolveType::Ignore),
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT REPLACE".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::NotNull {
                                nullable: false,
                                conflict_clause: Some(ResolveType::Replace),
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT ROLLBACK".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::NotNull {
                                nullable: false,
                                conflict_clause: Some(ResolveType::Rollback),
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT ROLLBACK".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::NotNull {
                                nullable: false,
                                conflict_clause: Some(ResolveType::Rollback),
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER CHECK (1)".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Check {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                source: Some("1".to_owned()),
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER CHECK (1)".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Check {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                source: Some("1".to_owned()),
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![],
                                    args: vec![]
                                },
                                defer_clause: None
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON INSERT SET NULL".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::exact("test".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                        IndexedColumn {
                                            col_name: Name::exact("test_2".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    args: vec![
                                        RefArg::Match(Name::exact("test_3".to_owned())),
                                        RefArg::OnInsert(RefAct::SetNull),
                                    ]
                                },
                                defer_clause: None
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON UPDATE SET NULL".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::exact("test".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                        IndexedColumn {
                                            col_name: Name::exact("test_2".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    args: vec![
                                        RefArg::Match(Name::exact("test_3".to_owned())),
                                        RefArg::OnUpdate(RefAct::SetNull),
                                    ]
                                },
                                defer_clause: None
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE SET NULL".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::exact("test".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                        IndexedColumn {
                                            col_name: Name::exact("test_2".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    args: vec![
                                        RefArg::Match(Name::exact("test_3".to_owned())),
                                        RefArg::OnDelete(RefAct::SetNull),
                                    ]
                                },
                                defer_clause: None
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE SET DEFAULT".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::exact("test".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                        IndexedColumn {
                                            col_name: Name::exact("test_2".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    args: vec![
                                        RefArg::Match(Name::exact("test_3".to_owned())),
                                        RefArg::OnDelete(RefAct::SetDefault),
                                    ]
                                },
                                defer_clause: None
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE CASCADE".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::exact("test".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                        IndexedColumn {
                                            col_name: Name::exact("test_2".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    args: vec![
                                        RefArg::Match(Name::exact("test_3".to_owned())),
                                        RefArg::OnDelete(RefAct::Cascade),
                                    ]
                                },
                                defer_clause: None
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE RESTRICT".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::exact("test".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                        IndexedColumn {
                                            col_name: Name::exact("test_2".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    args: vec![
                                        RefArg::Match(Name::exact("test_3".to_owned())),
                                        RefArg::OnDelete(RefAct::Restrict),
                                    ]
                                },
                                defer_clause: None
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE NO ACTION".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::exact("test".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                        IndexedColumn {
                                            col_name: Name::exact("test_2".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    args: vec![
                                        RefArg::Match(Name::exact("test_3".to_owned())),
                                        RefArg::OnDelete(RefAct::NoAction),
                                    ]
                                },
                                defer_clause: None
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar DEFERRABLE".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![],
                                    args: vec![]
                                },
                                defer_clause: Some(DeferSubclause {
                                    deferrable: true,
                                    init_deferred: None,
                                })
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar NOT DEFERRABLE INITIALLY IMMEDIATE".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![],
                                    args: vec![]
                                },
                                defer_clause: Some(DeferSubclause {
                                    deferrable: false,
                                    init_deferred: Some(InitDeferredPred::InitiallyImmediate),
                                })
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar NOT DEFERRABLE INITIALLY DEFERRED".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![],
                                    args: vec![]
                                },
                                defer_clause: Some(DeferSubclause {
                                    deferrable: false,
                                    init_deferred: Some(InitDeferredPred::InitiallyDeferred),
                                })
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar NOT DEFERRABLE INITIALLY DEFERRED".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::ForeignKey {
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("bar".to_owned()),
                                    columns: vec![],
                                    args: vec![]
                                },
                                defer_clause: Some(DeferSubclause {
                                    deferrable: false,
                                    init_deferred: Some(InitDeferredPred::InitiallyDeferred),
                                })
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER COLLATE bar".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Collate {
                                collation_name: Name::exact("bar".to_owned()),
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER GENERATED ALWAYS AS (1)".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Generated {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                typ: None,
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER AS (1)".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Generated {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                typ: None,
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ADD COLUMN baz INTEGER AS (1) STORED".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AddColumn(ColumnDefinition {
                    col_name: Name::exact("baz".to_owned()),
                    col_type: Some(Type {
                        name: "INTEGER".to_owned(),
                        size: None,
                        array_dimensions: 0,
                    }),
                    constraints: vec![
                        NamedColumnConstraint {
                            name: None,
                            constraint: ColumnConstraint::Generated {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                typ: Some(GeneratedColumnType::Stored),
                            },
                        },
                    ],
                }),
            }))],
        ),
        (
            b"ALTER TABLE foo ALTER COLUMN bar TO baz INTEGER".as_slice(),
            vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                body: AlterTableBody::AlterColumn {
                    old: Name::exact("bar".to_owned()),
                    new: ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![],
                    },

                },
            }))],
        ),
        // parse create index
        (
            b"CREATE INDEX idx_foo ON foo (bar)".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateIndex {
                unique: false,
                if_not_exists: false,
                idx_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("idx_foo".to_owned()),
                    alias: None,
                },
                tbl_name: Name::exact("foo".to_owned()),
                columns: vec![SortedColumn {
                    expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                    order: None,
                    nulls: None,
                }],
                where_clause: None,
                using: None,
                with_clause: Vec::new(),
            })],
        ),
        (
            b"CREATE UNIQUE INDEX IF NOT EXISTS idx_foo ON foo (bar) WHERE 1".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateIndex {
                unique: true,
                if_not_exists: true,
                idx_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("idx_foo".to_owned()),
                    alias: None,
                },
                tbl_name: Name::exact("foo".to_owned()),
                columns: vec![SortedColumn {
                    expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                    order: None,
                    nulls: None,
                }],
                where_clause: Some(Box::new(
                    Expr::Literal(Literal::Numeric("1".to_owned()))
                )),
                using: None,
                with_clause: Vec::new(),
            })],
        ),
        // parse create table
        (
            b"CREATE TABLE foo (column)".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTable {
                temporary: false,
                if_not_exists: false,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                body: CreateTableBody::ColumnsAndConstraints {
                    columns: vec![
                        ColumnDefinition {
                            col_name: Name::exact("column".to_owned()),
                            col_type: None,
                            constraints: vec![],
                        },
                    ],
                    constraints: vec![],
                    options: TableOptions::empty(),
                },
            })],
        ),
        (
            b"CREATE TABLE foo (a CONSTRAINT c PRIMARY KEY, b, d CONSTRAINT e)".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTable {
                temporary: false,
                if_not_exists: false,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                body: CreateTableBody::ColumnsAndConstraints {
                    columns: vec![
                        ColumnDefinition {
                            col_name: Name::exact("a".to_owned()),
                            col_type: None,
                            constraints: vec![
                                NamedColumnConstraint {
                                    name: Some(Name::exact("c".to_owned())),
                                    constraint: ColumnConstraint::PrimaryKey {
                                        order: None,
                                        conflict_clause: None,
                                        auto_increment: false,
                                    }
                                }
                            ],
                        },
                        ColumnDefinition {
                            col_name: Name::exact("b".to_owned()),
                            col_type: None,
                            constraints: vec![],
                        },
                        ColumnDefinition {
                            col_name: Name::exact("d".to_owned()),
                            col_type: None,
                            constraints: vec![],
                        },
                    ],
                    constraints: vec![],
                    options: TableOptions::empty(),
                },
            })],
        ),
        (
            b"CREATE TABLE foo AS SELECT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTable {
                temporary: false,
                if_not_exists: false,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                body: CreateTableBody::AsSelect(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![
                                ResultColumn::Expr(Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))), None)
                            ],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![]
                    },
                    order_by: vec![],
                    limit: None,
                }),
            })],
        ),
        (
            b"CREATE TEMP TABLE IF NOT EXISTS foo (baz INTEGER, CONSTRAINT tbl_cons PRIMARY KEY (bar AUTOINCREMENT) ON CONFLICT ROLLBACK) STRICT".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTable {
                temporary: true,
                if_not_exists: true,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                body: CreateTableBody::ColumnsAndConstraints {
                    columns: vec![
                        ColumnDefinition {
                            col_name: Name::exact("baz".to_owned()),
                            col_type: Some(Type {
                                name: "INTEGER".to_owned(),
                                size: None,
                                array_dimensions: 0,
                            }),
                            constraints: vec![],
                        },
                    ],
                    constraints: vec![
                        NamedTableConstraint {
                            name: Some(Name::exact("tbl_cons".to_owned())),
                            constraint: TableConstraint::PrimaryKey {
                                columns: vec![
                                    SortedColumn {
                                        expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                                        order: None,
                                        nulls: None,
                                    },
                                ],
                                auto_increment: true,
                                conflict_clause:  Some(ResolveType::Rollback)
                            }
                        },
                    ],
                    options: TableOptions { without_rowid_text: None, strict_text: Some("STRICT".to_string()) },
                },
            })],
        ),
        (
            b"CREATE TEMP TABLE IF NOT EXISTS foo (bar INTEGER PRIMARY KEY, baz INTEGER, UNIQUE (bar) ON CONFLICT ROLLBACK) WITHOUT ROWID".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTable {
                temporary: true,
                if_not_exists: true,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                body: CreateTableBody::ColumnsAndConstraints {
                    columns: vec![
                        ColumnDefinition {
                            col_name: Name::exact("bar".to_owned()),
                            col_type: Some(Type {
                                name: "INTEGER".to_owned(),
                                size: None,
                                array_dimensions: 0,
                            }),
                            constraints: vec![
                                NamedColumnConstraint {
                                    name: None,
                                    constraint: ColumnConstraint::PrimaryKey {
                                        order: None,
                                        conflict_clause: None,
                                        auto_increment: false,
                                    }
                                }
                            ],
                        },
                        ColumnDefinition {
                            col_name: Name::exact("baz".to_owned()),
                            col_type: Some(Type {
                                name: "INTEGER".to_owned(),
                                size: None,
                                array_dimensions: 0,
                            }),
                            constraints: vec![],
                        },
                    ],
                    constraints: vec![
                        NamedTableConstraint {
                            name: None,
                            constraint: TableConstraint::Unique {
                                columns: vec![
                                    SortedColumn {
                                        expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                                        order: None,
                                        nulls: None,
                                    },
                                ],
                                conflict_clause:  Some(ResolveType::Rollback)
                            }
                        },
                    ],
                    options: TableOptions { without_rowid_text: Some("WITHOUT ROWID".to_string()), strict_text: None },
                },
            })],
        ),
        (
            b"CREATE TEMP TABLE IF NOT EXISTS foo (bar, baz INTEGER, CHECK (1))".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTable {
                temporary: true,
                if_not_exists: true,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                body: CreateTableBody::ColumnsAndConstraints {
                    columns: vec![
                        ColumnDefinition {
                            col_name: Name::exact("bar".to_owned()),
                            col_type: None,
                            constraints: vec![],
                        },
                        ColumnDefinition {
                            col_name: Name::exact("baz".to_owned()),
                            col_type: Some(Type {
                                name: "INTEGER".to_owned(),
                                size: None,
                                array_dimensions: 0,
                            }),
                            constraints: vec![],
                        },
                    ],
                    constraints: vec![
                        NamedTableConstraint {
                            name: None,
                            constraint: TableConstraint::Check {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                source: Some("1".to_owned()),
                            },
                        },
                    ],
                    options: TableOptions::empty(),
                },
            })],
        ),
        (
            b"CREATE TEMP TABLE IF NOT EXISTS foo (bar, baz INTEGER, FOREIGN KEY (bar) REFERENCES foo_2(bar_2), CHECK (1))".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTable {
                temporary: true,
                if_not_exists: true,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                body: CreateTableBody::ColumnsAndConstraints {
                    columns: vec![
                        ColumnDefinition {
                            col_name: Name::exact("bar".to_owned()),
                            col_type: None,
                            constraints: vec![],
                        },
                        ColumnDefinition {
                            col_name: Name::exact("baz".to_owned()),
                            col_type: Some(Type {
                                name: "INTEGER".to_owned(),
                                size: None,
                                array_dimensions: 0,
                            }),
                            constraints: vec![],
                        },
                    ],
                    constraints: vec![
                        NamedTableConstraint {
                            name: None,
                            constraint: TableConstraint::ForeignKey {
                                columns: vec![
                                    IndexedColumn {
                                        col_name: Name::exact("bar".to_owned()),
                                        collation_name: None,
                                        order: None,
                                    },
                                ],
                                clause: ForeignKeyClause {
                                    tbl_name: Name::exact("foo_2".to_owned()),
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::exact("bar_2".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    args: vec![],
                                },
                                defer_clause: None,
                            },
                        },
                        NamedTableConstraint {
                            name: None,
                            constraint: TableConstraint::Check {
                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                source: Some("1".to_owned()),
                            },
                        },
                    ],
                    options: TableOptions::empty(),
                },
            })],
        ),
        // parse create trigger
        (
            b"CREATE TRIGGER foo INSERT ON bar BEGIN SELECT 1; END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: false,
                if_not_exists: false,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: None,
                event: TriggerEvent::Insert,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: false,
                when_clause: None,
                commands: vec![
                    TriggerCmd::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    })
                ],
            })],
        ),
        (
            b"CREATE TEMP TRIGGER IF NOT EXISTS foo AFTER UPDATE ON bar FOR EACH ROW WHEN 1 BEGIN SELECT 1; END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: true,
                if_not_exists: true,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: Some(TriggerTime::After),
                event: TriggerEvent::Update,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: true,
                when_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                commands: vec![
                    TriggerCmd::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    })
                ],
            })],
        ),
        (
            b"CREATE TEMP TRIGGER IF NOT EXISTS foo BEFORE DELETE ON bar FOR EACH ROW WHEN 1 BEGIN SELECT 1; END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: true,
                if_not_exists: true,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: Some(TriggerTime::Before),
                event: TriggerEvent::Delete,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: true,
                when_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                commands: vec![
                    TriggerCmd::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    })
                ],
            })],
        ),
        (
            b"CREATE TEMP TRIGGER IF NOT EXISTS foo INSTEAD OF UPDATE OF baz, bar ON bar FOR EACH ROW WHEN 1 BEGIN SELECT 1; END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: true,
                if_not_exists: true,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: Some(TriggerTime::InsteadOf),
                event: TriggerEvent::UpdateOf(vec![
                    Name::exact("baz".to_owned()),
                    Name::exact("bar".to_owned()),
                ]),
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: true,
                when_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                commands: vec![
                    TriggerCmd::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    })
                ],
            })],
        ),
        (
            b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2); END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: false,
                if_not_exists: false,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: None,
                event: TriggerEvent::Insert,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: false,
                when_clause: None,
                commands: vec![
                    TriggerCmd::Insert {
                        or_conflict: None,
                        tbl_name: Name::exact("foo".to_owned()),
                        col_names: vec![],
                        select: Select {
                            with: None,
                            body: SelectBody {
                                select: OneSelect::Values(vec![
                                    vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                ]),
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        },
                        upsert: None,
                        returning: vec![],
                    },
                ],
            })],
        ),
        (
            b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT OR ROLLBACK INTO foo(bar, baz) VALUES (1, 2); END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: false,
                if_not_exists: false,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: None,
                event: TriggerEvent::Insert,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: false,
                when_clause: None,
                commands: vec![
                    TriggerCmd::Insert {
                        or_conflict: Some(ResolveType::Rollback),
                        tbl_name: Name::exact("foo".to_owned()),
                        col_names: vec![
                            Name::exact("bar".to_owned()),
                            Name::exact("baz".to_owned()),
                        ],
                        select: Select {
                            with: None,
                            body: SelectBody {
                                select: OneSelect::Values(vec![
                                    vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                ]),
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        },
                        upsert: None,
                        returning: vec![],
                    },
                ],
            })],
        ),
        (
            b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) ON CONFLICT (bar, baz) DO NOTHING ON CONFLICT DO NOTHING; END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: false,
                if_not_exists: false,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: None,
                event: TriggerEvent::Insert,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: false,
                when_clause: None,
                commands: vec![
                    TriggerCmd::Insert {
                        or_conflict: None,
                        tbl_name: Name::exact("foo".to_owned()),
                        col_names: vec![],
                        select: Select {
                            with: None,
                            body: SelectBody {
                                select: OneSelect::Values(vec![
                                    vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                ]),
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        },
                        upsert: Some(Box::new(Upsert {
                            index: Some(UpsertIndex {
                                targets: vec![
                                    SortedColumn {
                                        expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                                        order: None,
                                        nulls: None
                                    },
                                    SortedColumn {
                                        expr: Box::new(Expr::Id(Name::exact("baz".to_owned()))),
                                        order: None,
                                        nulls: None
                                    },
                                ],
                                where_clause: None,
                            }),
                            do_clause: UpsertDo::Nothing,
                            next: Some(Box::new(Upsert {
                                index: None,
                                do_clause: UpsertDo::Nothing,
                                next: None,
                            })),
                        })),
                        returning: vec![],
                    },
                ],
            })],
        ),
        (
            b"CREATE TRIGGER foo INSERT ON bar BEGIN UPDATE foo SET bar = 1; END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: false,
                if_not_exists: false,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: None,
                event: TriggerEvent::Insert,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: false,
                when_clause: None,
                commands: vec![
                    TriggerCmd::Update {
                        or_conflict: None,
                        tbl_name: Name::exact("foo".to_owned()),
                        sets: vec![
                            Set {
                                col_names: vec![Name::exact("bar".to_owned())],

                                expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            },
                        ],
                        from: None,
                        where_clause: None,
                    },
                ],
            })],
        ),
        (
            b"CREATE TRIGGER foo INSERT ON bar BEGIN DELETE FROM foo; END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: false,
                if_not_exists: false,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: None,
                event: TriggerEvent::Insert,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: false,
                when_clause: None,
                commands: vec![
                    TriggerCmd::Delete {
                        tbl_name: Name::exact("foo".to_owned()),
                        where_clause: None,
                    },
                ],
            })],
        ),
        (
            b"CREATE TRIGGER foo INSERT ON bar BEGIN DELETE FROM foo WHERE 1; END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: false,
                if_not_exists: false,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: None,
                event: TriggerEvent::Insert,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: false,
                when_clause: None,
                commands: vec![
                    TriggerCmd::Delete {
                        tbl_name: Name::exact("foo".to_owned()),
                        where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    },
                ],
            })],
        ),
        (
            b"CREATE TRIGGER foo INSERT ON bar BEGIN SELECT 1; SELECT 1; END".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTrigger {
                temporary: false,
                if_not_exists: false,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                time: None,
                event: TriggerEvent::Insert,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("bar".to_owned()),
                    alias: None,
                },
                for_each_row: false,
                when_clause: None,
                commands: vec![
                    TriggerCmd::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    }),
                    TriggerCmd::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    })
                ],
            })],
        ),
        // parse create view
        (
            b"CREATE VIEW foo(bar) AS SELECT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateView {
                temporary: false,
                if_not_exists: false,
                view_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                columns: vec![
                    IndexedColumn {
                        col_name: Name::exact("bar".to_owned()),
                        collation_name: None,
                        order: None,
                    }
                ],
                select: Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                },
            })],
        ),
        (
            b"CREATE TEMP VIEW IF NOT EXISTS foo(bar) AS SELECT 1".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateView {
                temporary: true,
                if_not_exists: true,
                view_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                columns: vec![
                    IndexedColumn {
                        col_name: Name::exact("bar".to_owned()),
                        collation_name: None,
                        order: None,
                    }
                ],
                select: Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                },
            })],
        ),
        // parse CREATE VIRTUAL TABLE
        (
            b"CREATE VIRTUAL TABLE foo USING bar".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable {
                if_not_exists: false,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                module_name: Name::exact("bar".to_owned()),
                args: vec![],
            }))],
        ),
        (
            b"CREATE VIRTUAL TABLE foo USING bar()".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable{
                if_not_exists: false,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                module_name: Name::exact("bar".to_owned()),
                args: vec![],
            }))],
        ),
        (
            b"CREATE VIRTUAL TABLE IF NOT EXISTS foo USING bar(1, 2, ('hello', (3.333), 'world', (1, 2)))".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable{
                if_not_exists: true,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                module_name: Name::exact("bar".to_owned()),
                args: vec![
                    "1".to_owned(),
                    "2".to_owned(),
                    "('hello', (3.333), 'world', (1, 2))".to_owned(),
                ],
            }))],
        ),
        (
            b"CREATE VIRTUAL TABLE ft USING fts5(x, tokenize = '''porter'' ''ascii''')".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable {
                if_not_exists: false,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("ft".to_owned()),
                    alias: None,
                },
                module_name: Name::exact("fts5".to_owned()),
                args: vec![
                    "x".to_owned(),
                    "tokenize = '''porter'' ''ascii'''".to_owned(),
                ],
            }))],
        ),
        // parse delete
        (
            b"DELETE FROM foo".as_slice(),
            vec![Cmd::Stmt(Stmt::Delete {
                with: None,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None
                },
                indexed: None,
                where_clause: None,
                returning: vec![],
            })],
        ),
        (
            b"WITH test AS (SELECT 1) DELETE FROM foo NOT INDEXED WHERE 1 RETURNING bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Delete {
                with: Some(With {
                    recursive: false,
                    ctes: vec![
                        CommonTableExpr {
                            tbl_name: Name::exact("test".to_owned()),
                            columns: vec![],
                            materialized: Materialized::Any,
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            None,
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                        }
                    ],
                }),
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None
                },
                indexed: Some(Indexed::NotIndexed),
                where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                returning: vec![
                    ResultColumn::Expr(
                        Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                        None,
                    ),
                ],
            })],
        ),
        // parse drop index
        (
            b"DROP INDEX foo".as_slice(),
            vec![Cmd::Stmt(Stmt::DropIndex {
                if_exists: false,
                idx_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
            })],
        ),
        (
            b"DROP INDEX IF EXISTS foo".as_slice(),
            vec![Cmd::Stmt(Stmt::DropIndex {
                if_exists: true,
                idx_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
            })],
        ),
        // parse drop table
        (
            b"DROP TABLE foo".as_slice(),
            vec![Cmd::Stmt(Stmt::DropTable {
                if_exists: false,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
            })],
        ),
        (
            b"DROP TABLE IF EXISTS foo".as_slice(),
            vec![Cmd::Stmt(Stmt::DropTable {
                if_exists: true,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
            })],
        ),
        // parse drop trigger
        (
            b"DROP TRIGGER foo".as_slice(),
            vec![Cmd::Stmt(Stmt::DropTrigger {
                if_exists: false,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
            })],
        ),
        (
            b"DROP TRIGGER IF EXISTS foo".as_slice(),
            vec![Cmd::Stmt(Stmt::DropTrigger {
                if_exists: true,
                trigger_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
            })],
        ),
        // parse drop view
        (
            b"DROP VIEW foo".as_slice(),
            vec![Cmd::Stmt(Stmt::DropView {
                if_exists: false,
                view_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
            })],
        ),
        (
            b"DROP VIEW IF EXISTS foo".as_slice(),
            vec![Cmd::Stmt(Stmt::DropView {
                if_exists: true,
                view_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
            })],
        ),
        // parse insert
        (
            b"INSERT INTO foo VALUES (1, 2)".as_slice(),
            vec![Cmd::Stmt(Stmt::Insert {
                with: None,
                or_conflict: None,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                columns: vec![],
                body: InsertBody::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Values(vec![
                            vec![
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                            ],
                        ]),
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }, None),
                returning: vec![],
            })],
        ),
        (
            b"REPLACE INTO foo VALUES (1, 2)".as_slice(),
            vec![Cmd::Stmt(Stmt::Insert {
                with: None,
                or_conflict: Some(ResolveType::Replace),
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                columns: vec![],
                body: InsertBody::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Values(vec![
                            vec![
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                            ],
                        ]),
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }, None),
                returning: vec![],
            })],
        ),
        (
            b"WITH test AS (SELECT 1) INSERT INTO foo DEFAULT VALUES RETURNING bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Insert {
                with: Some(With {
                    recursive: false,
                    ctes: vec![
                        CommonTableExpr {
                            tbl_name: Name::exact("test".to_owned()),
                            columns: vec![],
                            materialized: Materialized::Any,
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            None,
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                        }
                    ],
                }),
                or_conflict: None,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                columns: vec![],
                body: InsertBody::DefaultValues,
                returning: vec![
                    ResultColumn::Expr(
                        Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                        None,
                    ),
                ],
            })],
        ),
        (
            b"WITH test AS (SELECT 1) REPLACE INTO foo DEFAULT VALUES RETURNING bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Insert {
                with: Some(With {
                    recursive: false,
                    ctes: vec![
                        CommonTableExpr {
                            tbl_name: Name::exact("test".to_owned()),
                            columns: vec![],
                            materialized: Materialized::Any,
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            None,
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                        }
                    ],
                }),
                or_conflict: Some(ResolveType::Replace),
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                columns: vec![],
                body: InsertBody::DefaultValues,
                returning: vec![
                    ResultColumn::Expr(
                        Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                        None,
                    ),
                ],
            })],
        ),
        // parse update
        (
            b"UPDATE foo SET bar = 1".as_slice(),
            vec![Cmd::Stmt(Stmt::Update(Update {
                with: None,
                or_conflict: None,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                indexed: None,
                sets: vec![
                    Set {
                        col_names: vec![
                            Name::exact("bar".to_owned()),
                        ],

                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    }
                ],
                from: None,
                where_clause: None,
                returning: vec![],
            }))],
        ),
        (
            b"WITH test AS (SELECT 1) UPDATE OR REPLACE foo NOT INDEXED SET bar = 1 FROM foo_2 WHERE 1 RETURNING bar".as_slice(),
            vec![Cmd::Stmt(Stmt::Update(Update {
                with: Some(With {
                    recursive: false,
                    ctes: vec![
                        CommonTableExpr {
                            tbl_name: Name::exact("test".to_owned()),
                            columns: vec![],
                            materialized: Materialized::Any,
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Select {
                                        distinctness: None,
                                        columns: vec![ResultColumn::Expr(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            None,
                                        )],
                                        from: None,
                                        where_clause: None,
                                        group_by: None,
                                        window_clause: vec![],
                                    },
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                        }
                    ],
                }),
                or_conflict: Some(ResolveType::Replace),
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None,
                },
                indexed: Some(Indexed::NotIndexed),
                sets: vec![
                    Set {
                        col_names: vec![
                            Name::exact("bar".to_owned()),
                        ],

                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                    }
                ],
                from: Some(FromClause {
                    select: Box::new(SelectTable::Table(
                        QualifiedName {
                            db_name: None,
                            name: Name::exact("foo_2".to_owned()),
                            alias: None,
                        },
                        None,
                        None,
                    )),
                    joins: vec![]
                }),
                where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                returning: vec![
                    ResultColumn::Expr(
                        Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                        None,
                    ),
                ],
            }))],
        ),
        // parse reindex
        (
            b"REINDEX".as_slice(),
            vec![Cmd::Stmt(Stmt::Reindex {
                name: None,
            })],
        ),
        (
            b"REINDEX foo".as_slice(),
            vec![Cmd::Stmt(Stmt::Reindex {
                name: Some(QualifiedName {
                    db_name: None,
                    name: Name::exact("foo".to_owned()),
                    alias: None
                }),
            })],
        ),
        // issue 2875
        (
            b"CREATE TABLE \"settings\" (\"enabled\" INTEGER DEFAULT CURRENT_TIMESTAMP NOT NULL)".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateTable {
                temporary: false,
                if_not_exists: false,
                tbl_name: QualifiedName {
                    db_name: None,
                    name: Name::from_string("\"settings\""),
                    alias: None,
                },
                body: CreateTableBody::ColumnsAndConstraints{
                    columns: vec![
                        ColumnDefinition {
                            col_name: Name::from_string("\"enabled\""),
                            col_type: Some(Type {
                                name: "INTEGER".to_owned(),
                                size: None,
                                array_dimensions: 0,
                            }),
                            constraints: vec![
                                NamedColumnConstraint {
                                    name: None,
                                    constraint: ColumnConstraint::Default(Box::new(Expr::Literal(Literal::CurrentTimestamp))),
                                },
                                NamedColumnConstraint {
                                    name: None,
                                    constraint: ColumnConstraint::NotNull { nullable: false, conflict_clause: None }
                                },
                            ],
                        }
                    ],
                    constraints: vec![],
                    options: TableOptions::empty(),
                },
            })],
        ),
        (
            b"CREATE INDEX t_idx ON t USING custom_index (x) WITH (a = 1, b = 'test', c = x'deadbeef', d = NULL)".as_slice(),
            vec![Cmd::Stmt(Stmt::CreateIndex {
                unique: false,
                if_not_exists: false,
                idx_name: QualifiedName {
                    db_name: None,
                    name: Name::exact("t_idx".to_owned()),
                    alias: None,
                },
                tbl_name: Name::exact("t".to_owned()),
                columns: vec![SortedColumn {
                    expr: Box::new(Expr::Id(Name::exact("x".to_owned()))),
                    order: None,
                    nulls: None,
                }],
                where_clause: None,
                using: Some(Name::exact("custom_index".to_owned())),
                with_clause: vec![
                    (Name::exact("a".to_string()), Box::new(Expr::Literal(Literal::Numeric("1".to_string())))),
                    (Name::exact("b".to_string()), Box::new(Expr::Literal(Literal::String("'test'".to_string())))),
                    (Name::exact("c".to_string()), Box::new(Expr::Literal(Literal::Blob("x'deadbeef'".to_string())))),
                    (Name::exact("d".to_string()), Box::new(Expr::Literal(Literal::Null))),
                ],
            })],
        )
    ];

    for (input, expected) in test_cases {
        let input_str = from_bytes(input);
        let parser = Parser::new(input);
        let mut results = Vec::new();
        for cmd in parser {
            results.push(cmd.unwrap());
        }

        // Compare serialized forms since ImplicitColumnName (display-only
        // metadata) serializes to nothing, making comparison insensitive
        // to its presence.
        let results_str: Vec<String> = results.iter().map(|c| c.to_string()).collect();
        let expected_str: Vec<String> = expected.iter().map(|c| c.to_string()).collect();
        assert_eq!(results_str, expected_str, "Input: {input_str:?}");

        // to_string round-trip tests
        for (i, r) in results.iter().enumerate() {
            let rstring = r.to_string();
            // put new string into parser again
            let result = Parser::new(rstring.as_bytes()).next().unwrap().unwrap();
            let result_str = result.to_string();
            assert_eq!(result_str, expected_str[i], "Input: {rstring:?}");
        }
    }
}

#[test]
fn test_inline_struct_rejected() {
    let sql = b"CREATE TABLE t(s STRUCT(x INT, y TEXT)) STRICT";
    let err = Parser::new(sql).next().unwrap().unwrap_err();
    assert!(err.to_string().contains("inline STRUCT/UNION"));
}

#[test]
fn test_inline_union_rejected() {
    let sql = b"CREATE TABLE t(u UNION(i INT, t TEXT)) STRICT";
    let err = Parser::new(sql).next().unwrap().unwrap_err();
    assert!(err.to_string().contains("inline STRUCT/UNION"));
}

#[test]
fn test_delete_and_update_reject_limit_and_order_by() {
    // Default SQLite builds (without SQLITE_ENABLE_UPDATE_DELETE_LIMIT)
    // reject LIMIT and ORDER BY on DELETE and UPDATE.
    for sql in [
        b"DELETE FROM t LIMIT 1".as_slice(),
        b"DELETE FROM t LIMIT 1 OFFSET 2".as_slice(),
        b"DELETE FROM t ORDER BY x LIMIT 1".as_slice(),
        b"UPDATE t SET x = 1 LIMIT 1".as_slice(),
        b"UPDATE t SET x = 1 ORDER BY x LIMIT 1".as_slice(),
    ] {
        let result = Parser::new(sql).next().unwrap();
        assert!(result.is_err(), "expected parse error for {sql:?}");
    }
}

#[test]
fn test_create_type_as_struct() {
    let sql = b"CREATE TYPE point AS STRUCT(x INT, y INT)";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    if let Cmd::Stmt(Stmt::CreateType {
        type_name, body, ..
    }) = cmd
    {
        assert_eq!(type_name, "point");
        match body {
            CreateTypeBody::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name.to_string(), "x");
                assert_eq!(fields[1].name.to_string(), "y");
            }
            _ => panic!("expected Struct body"),
        }
    } else {
        panic!("expected CreateType");
    }
}

#[test]
fn test_create_type_as_union() {
    let sql = b"CREATE TYPE platform AS UNION(telegram INT, slack TEXT)";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    if let Cmd::Stmt(Stmt::CreateType {
        type_name, body, ..
    }) = cmd
    {
        assert_eq!(type_name, "platform");
        match body {
            CreateTypeBody::Union(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name.to_string(), "telegram");
                assert_eq!(fields[1].name.to_string(), "slack");
            }
            _ => panic!("expected Union body"),
        }
    } else {
        panic!("expected CreateType");
    }
}

#[test]
fn test_dot_notation_still_produces_qualified() {
    let sql = b"SELECT col.field FROM t";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    if let Cmd::Stmt(Stmt::Select(sel)) = cmd {
        if let OneSelect::Select { columns, .. } = &sel.body.select {
            if let ResultColumn::Expr(expr, _) = &columns[0] {
                assert!(
                    matches!(expr.as_ref(), Expr::Qualified(_, _)),
                    "expected Qualified, got: {expr:?}",
                );
            } else {
                panic!("expected Expr");
            }
        } else {
            panic!("expected Select");
        }
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_struct_pack_positional() {
    let sql = b"SELECT struct_pack(1, 'hello')";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    if let Cmd::Stmt(Stmt::Select(sel)) = cmd {
        if let OneSelect::Select { columns, .. } = &sel.body.select {
            if let ResultColumn::Expr(expr, _) = &columns[0] {
                if let Expr::FunctionCall { name, args, .. } = expr.as_ref() {
                    assert_eq!(name.to_string(), "struct_pack");
                    assert_eq!(args.len(), 2);
                } else {
                    panic!("expected FunctionCall");
                }
            } else {
                panic!("expected Expr");
            }
        } else {
            panic!("expected Select");
        }
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_union_value_string_tag() {
    let sql = b"SELECT union_value('i', 42)";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    if let Cmd::Stmt(Stmt::Select(sel)) = cmd {
        if let OneSelect::Select { columns, .. } = &sel.body.select {
            if let ResultColumn::Expr(expr, _) = &columns[0] {
                if let Expr::FunctionCall { name, args, .. } = expr.as_ref() {
                    assert_eq!(name.to_string(), "union_value");
                    assert_eq!(args.len(), 2);
                } else {
                    panic!("expected FunctionCall");
                }
            } else {
                panic!("expected Expr");
            }
        } else {
            panic!("expected Select");
        }
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_parse_create_sequence_defaults() {
    let sql = b"CREATE SEQUENCE foo";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    match cmd {
        Cmd::Stmt(Stmt::CreateSequence {
            if_not_exists,
            seq_name,
            start,
            increment,
            min_value,
            max_value,
            cycle,
        }) => {
            assert!(!if_not_exists);
            assert_eq!(seq_name.name.to_string(), "foo");
            assert_eq!(start, None);
            assert_eq!(increment, None);
            assert_eq!(min_value, None);
            assert_eq!(max_value, None);
            assert!(!cycle);
        }
        _ => panic!("expected CreateSequence"),
    }
}

#[test]
fn test_parse_create_sequence_full() {
    let sql = b"CREATE SEQUENCE foo START WITH 10 INCREMENT BY 5 MINVALUE 0 MAXVALUE 100 CYCLE";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    match cmd {
        Cmd::Stmt(Stmt::CreateSequence {
            if_not_exists,
            start,
            increment,
            min_value,
            max_value,
            cycle,
            ..
        }) => {
            assert!(!if_not_exists);
            assert_eq!(start, Some(10));
            assert_eq!(increment, Some(5));
            assert_eq!(min_value, Some(0));
            assert_eq!(max_value, Some(100));
            assert!(cycle);
        }
        _ => panic!("expected CreateSequence"),
    }
}

#[test]
fn test_parse_create_sequence_if_not_exists() {
    let sql = b"CREATE SEQUENCE IF NOT EXISTS foo";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    match cmd {
        Cmd::Stmt(Stmt::CreateSequence { if_not_exists, .. }) => {
            assert!(if_not_exists);
        }
        _ => panic!("expected CreateSequence"),
    }
}

#[test]
fn test_parse_create_sequence_negative_values() {
    let sql = b"CREATE SEQUENCE foo INCREMENT BY -1 START WITH -1 MINVALUE -100 MAXVALUE -1";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    match cmd {
        Cmd::Stmt(Stmt::CreateSequence {
            start,
            increment,
            min_value,
            max_value,
            ..
        }) => {
            assert_eq!(start, Some(-1));
            assert_eq!(increment, Some(-1));
            assert_eq!(min_value, Some(-100));
            assert_eq!(max_value, Some(-1));
        }
        _ => panic!("expected CreateSequence"),
    }
}

#[test]
fn test_parse_create_sequence_no_cycle() {
    let sql = b"CREATE SEQUENCE foo NO CYCLE";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    match cmd {
        Cmd::Stmt(Stmt::CreateSequence { cycle, .. }) => {
            assert!(!cycle);
        }
        _ => panic!("expected CreateSequence"),
    }
}

#[test]
fn test_parse_drop_sequence() {
    let sql = b"DROP SEQUENCE foo";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    match cmd {
        Cmd::Stmt(Stmt::DropSequence {
            if_exists,
            seq_name,
        }) => {
            assert!(!if_exists);
            assert_eq!(seq_name.name.to_string(), "foo");
        }
        _ => panic!("expected DropSequence"),
    }
}

#[test]
fn test_parse_drop_sequence_if_exists() {
    let sql = b"DROP SEQUENCE IF EXISTS foo";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    match cmd {
        Cmd::Stmt(Stmt::DropSequence { if_exists, .. }) => {
            assert!(if_exists);
        }
        _ => panic!("expected DropSequence"),
    }
}

#[test]
fn test_parse_nextval() {
    let sql = b"SELECT nextval('foo')";
    let cmd = Parser::new(sql).next().unwrap().unwrap();
    if let Cmd::Stmt(Stmt::Select(sel)) = cmd {
        if let OneSelect::Select { columns, .. } = &sel.body.select {
            if let ResultColumn::Expr(expr, _) = &columns[0] {
                if let Expr::FunctionCall { name, .. } = expr.as_ref() {
                    assert_eq!(name.to_string(), "nextval");
                } else {
                    panic!("expected FunctionCall");
                }
            } else {
                panic!("expected Expr");
            }
        } else {
            panic!("expected Select");
        }
    } else {
        panic!("expected Select");
    }
}
