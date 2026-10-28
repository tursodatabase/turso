use super::*;
use access_control::access_control;
use clap::ValueEnum;

#[hegel::test(test_cases = 2000)]
fn inserts_follow_column_types_and_constraints(tc: TestCase) {
    use model::{Column, Constraint, DataType, Identifier};
    use pg_query::{protobuf::a_const::Val, NodeEnum as N};
    let types = tc.draw(
        gs::vecs(gs::sampled_from(&[
            DataType::Integer,
            DataType::BigInt,
            DataType::Text,
            DataType::Boolean,
        ]))
        .min_size(1)
        .max_size(8),
    );
    let primary = tc.draw(gs::integers::<usize>().max_value(types.len()));
    let names = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"];
    let table = Table {
        name: Identifier::new("generated"),
        index: 0,
        columns: types
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| Column {
                name: Identifier::new(names[index]),
                data_type,
                constraints: if index == primary {
                    vec![Constraint::PrimaryKey]
                } else if tc.draw(gs::booleans()) {
                    vec![Constraint::NotNull]
                } else {
                    vec![]
                },
            })
            .collect(),
    };
    let mut statements = vec![Statement::CreateTable(table.clone())];
    statements.extend(tc.draw(data::inserts(table.clone())));
    let sql = Workload(statements).to_string();
    let parsed = pg_query::parse(&sql).unwrap();
    let mut keys = std::collections::BTreeSet::new();
    for statement in parsed.protobuf.stmts {
        let Some(N::InsertStmt(insert)) = statement.stmt.and_then(|s| s.node) else {
            continue;
        };
        let Some(N::SelectStmt(select)) = insert.select_stmt.and_then(|s| s.node) else {
            panic!("expected SELECT or VALUES")
        };
        if select.values_lists.is_empty() {
            panic!("generated INSERT must use VALUES");
        }
        for row in select.values_lists {
            let Some(N::List(row)) = row.node else {
                panic!("expected row")
            };
            assert_eq!(row.items.len(), table.columns.len());
            for (cell, column) in row.items.iter().zip(&table.columns) {
                let Some(N::AConst(value)) = &cell.node else {
                    panic!("expected literal")
                };
                if value.isnull {
                    assert!(column.is_nullable());
                    continue;
                }
                match (&column.data_type, &value.val) {
                    (DataType::Integer | DataType::BigInt, Some(Val::Ival(_))) => {}
                    (DataType::Integer | DataType::BigInt, Some(Val::Fval(value))) => {
                        value.fval.parse::<i64>().unwrap();
                    }
                    (DataType::Text, Some(Val::Sval(value))) => {
                        assert!(!value.sval.contains('\0'));
                    }
                    (DataType::Boolean, Some(Val::Boolval(_))) => {}
                    other => panic!("wrong literal type: {other:?}"),
                }
                if column.is_primary_key() {
                    assert!(keys.insert(serde_json::to_string(&value.val).unwrap()));
                }
            }
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn table_workloads_are_valid_postgres_syntax(tc: TestCase) {
    let sql = tc.draw(workload(Profile::Tables, vec![]));
    pg_query::parse(&sql).unwrap();
}

#[hegel::test(test_cases = 2000)]
fn compatible_workloads_are_valid_postgres_syntax(tc: TestCase) {
    let sql = tc.draw(workload(Profile::Compatible, vec![]));
    pg_query::parse(&sql).unwrap();
}
#[hegel::test(test_cases = 2000)]
fn disabled_features_are_absent_from_compatible_ast(tc: TestCase) {
    let disabled = tc.draw(
        gs::vecs(gs::sampled_from(Feature::value_variants()))
            .max_size(Feature::value_variants().len()),
    );
    let sql = tc.draw(workload(Profile::Compatible, disabled.clone()));
    let parsed = pg_query::parse(&sql).unwrap();
    for statement in parsed.protobuf.stmts {
        use pg_query::{
            protobuf::{AlterTableType as A, ConstrType},
            NodeEnum as N,
        };
        match statement.stmt.unwrap().node.unwrap() {
            N::CreateStmt(create) => {
                if disabled.contains(&Feature::MixedCaseIdentifiers) {
                    assert!(!create
                        .relation
                        .as_ref()
                        .unwrap()
                        .relname
                        .chars()
                        .any(char::is_uppercase));
                    for node in &create.table_elts {
                        let Some(N::ColumnDef(column)) = &node.node else {
                            panic!()
                        };
                        assert!(!column.colname.chars().any(char::is_uppercase));
                    }
                }
                if disabled.contains(&Feature::Defaults) {
                    for node in create.table_elts {
                        let Some(N::ColumnDef(column)) = node.node else {
                            panic!()
                        };
                        assert!(!column.constraints.iter().any(|node| matches!(&node.node,
                            Some(N::Constraint(constraint)) if constraint.contype == ConstrType::ConstrDefault as i32)));
                    }
                }
            }
            N::IndexStmt(index) => {
                assert!(!disabled.contains(&Feature::Indexes));
                if disabled.contains(&Feature::CompositeIndexes) {
                    assert_eq!(index.index_params.len(), 1);
                }
                if disabled.contains(&Feature::UniqueIndexes) {
                    assert!(!index.unique);
                }
                if disabled.contains(&Feature::DescendingIndexes) {
                    for node in &index.index_params {
                        let Some(N::IndexElem(key)) = &node.node else {
                            panic!()
                        };
                        assert_ne!(
                            key.ordering,
                            pg_query::protobuf::SortByDir::SortbyDesc as i32
                        );
                    }
                }
            }
            N::CreatePolicyStmt(_) => assert!(!disabled.contains(&Feature::Policies)),
            N::AlterPolicyStmt(_) => assert!(
                !disabled.contains(&Feature::Policies) && !disabled.contains(&Feature::AlterPolicy)
            ),
            N::DropStmt(_) => assert!(
                !disabled.contains(&Feature::Policies) && !disabled.contains(&Feature::DropPolicy)
            ),
            N::GrantStmt(grant) => assert!(
                !disabled.contains(&Feature::Grants)
                    && (grant.is_grant || !disabled.contains(&Feature::Revokes))
            ),
            N::AlterDefaultPrivilegesStmt(_) => {
                assert!(!disabled.contains(&Feature::DefaultPrivileges))
            }
            N::AlterTableStmt(alter) => {
                for command in alter.cmds {
                    let Some(N::AlterTableCmd(command)) = command.node else {
                        panic!("unexpected ALTER command")
                    };
                    assert!(!disabled.contains(&Feature::Rls));
                    if command.subtype == A::AtForceRowSecurity as i32 {
                        assert!(!disabled.contains(&Feature::ForceRls));
                    }
                }
            }
            _ => {}
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn independent_access_controls_stay_enabled(tc: TestCase) {
    let disabled = tc.draw(
        gs::vecs(gs::sampled_from(Feature::value_variants()))
            .max_size(Feature::value_variants().len()),
    );
    let config = Config {
        profile: Profile::Compatible,
        disabled: disabled.clone(),
    };
    let table = tc.draw(schema::table(0, config.clone()));
    let sql = Workload(tc.draw(access_control(table, config))).to_string();
    let nodes: Vec<_> = pg_query::parse(&sql)
        .unwrap()
        .protobuf
        .stmts
        .into_iter()
        .map(|s| s.stmt.unwrap().node.unwrap())
        .collect();
    assert_eq!(
        nodes
            .iter()
            .any(|node| matches!(node, pg_query::NodeEnum::CreatePolicyStmt(_))),
        !disabled.contains(&Feature::Policies)
    );
    assert_eq!(
        nodes
            .iter()
            .any(|node| matches!(node, pg_query::NodeEnum::GrantStmt(g) if g.is_grant)),
        !disabled.contains(&Feature::Grants)
    );
    assert_eq!(
        nodes
            .iter()
            .any(|node| matches!(node, pg_query::NodeEnum::AlterTableStmt(_))),
        !disabled.contains(&Feature::Rls)
    );
}

#[hegel::test(test_cases = 2000)]
fn workload_orders_steps_and_keeps_access_control_out_of_tables_profile(tc: TestCase) {
    use pg_query::NodeEnum as N;
    let profile = tc.draw(gs::sampled_from(&[Profile::Tables, Profile::Compatible]));
    let disabled = tc.draw(
        gs::vecs(gs::sampled_from(Feature::value_variants()))
            .max_size(Feature::value_variants().len()),
    );
    let sql = tc.draw(workload(profile, disabled));
    let mut last_phase = 0;
    let mut created = false;
    for statement in pg_query::parse(&sql).unwrap().protobuf.stmts {
        let phase = match statement.stmt.unwrap().node.unwrap() {
            N::VariableSetStmt(_) => continue,
            N::CreateStmt(_) => {
                created = true;
                last_phase = 0;
                continue;
            }
            N::InsertStmt(insert) => {
                let Some(N::SelectStmt(select)) = insert.select_stmt.unwrap().node else {
                    panic!()
                };
                assert!(
                    !select.values_lists.is_empty(),
                    "INSERT SELECT must not be generated"
                );
                1
            }
            N::IndexStmt(_) => 2,
            N::UpdateStmt(_) | N::DeleteStmt(_) | N::SelectStmt(_) => {
                panic!("only INSERTs may populate data")
            }
            _ => {
                assert_eq!(profile, Profile::Compatible);
                4
            }
        };
        assert!(created);
        assert!(phase >= last_phase);
        last_phase = phase;
    }
}

#[hegel::test(test_cases = 2000)]
fn rendered_values_preserve_their_contents(tc: TestCase) {
    use model::{Identifier, Row, Value};
    let values = tc.draw(
        gs::vecs(gs::one_of(vec![
            gs::just(Value::Null).boxed(),
            gs::integers::<i64>().map(Value::Integer).boxed(),
            gs::text()
                .max_size(128)
                .map(|s| Value::Text(s.replace('\0', "")))
                .boxed(),
            gs::booleans().map(Value::Boolean).boxed(),
        ]))
        .min_size(1)
        .max_size(12),
    );
    let statement = Statement::Insert {
        table: Identifier::new("t"),
        row: Row(values.clone()),
    };
    let parsed = pg_query::parse(&statement.to_string()).unwrap();
    let pg_query::NodeEnum::InsertStmt(insert) = parsed.protobuf.stmts[0]
        .stmt
        .as_ref()
        .unwrap()
        .node
        .as_ref()
        .unwrap()
    else {
        panic!()
    };
    let pg_query::NodeEnum::SelectStmt(select) =
        insert.select_stmt.as_ref().unwrap().node.as_ref().unwrap()
    else {
        panic!()
    };
    let pg_query::NodeEnum::List(row) = select.values_lists[0].node.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(row.items.len(), values.len());
    for (node, value) in row.items.iter().zip(values) {
        use pg_query::{protobuf::a_const::Val, NodeEnum as N};
        let N::AConst(actual) = node.node.as_ref().unwrap() else {
            panic!()
        };
        match value {
            Value::Null => assert!(actual.isnull),
            Value::Integer(expected) => assert_eq!(integer(node), expected),
            Value::Text(expected) => {
                assert!(!actual.isnull);
                let Some(Val::Sval(actual)) = &actual.val else {
                    panic!()
                };
                assert_eq!(actual.sval, expected);
            }
            Value::Boolean(expected) => {
                let Some(Val::Boolval(actual)) = &actual.val else {
                    panic!()
                };
                assert_eq!(actual.boolval, expected);
            }
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn insert_preserves_quoted_target_identifier(tc: TestCase) {
    use model::{Identifier, Row, Value};
    let text = tc.draw(gs::text().max_size(10)).replace('\0', "");
    let target = format!("target \";{text}");
    let statement = Statement::Insert {
        table: Identifier::new(target.clone()),
        row: Row(vec![Value::Null]),
    };
    let parsed = pg_query::parse(&statement.to_string()).unwrap();
    assert_eq!(parsed.protobuf.stmts.len(), 1);
    let pg_query::NodeEnum::InsertStmt(insert) = parsed.protobuf.stmts[0]
        .stmt
        .as_ref()
        .unwrap()
        .node
        .as_ref()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(insert.relation.as_ref().unwrap().relname, target);
}

#[test]
fn identifier_rejects_names_postgres_cannot_preserve() {
    for name in [String::new(), "nul\0byte".to_owned(), "x".repeat(64)] {
        assert!(std::panic::catch_unwind(|| model::Identifier::new(name)).is_err());
    }
}

#[test]
fn integer_extrema_render_exactly() {
    for value in [i64::MIN, i64::MAX] {
        let sql = format!("INSERT INTO t VALUES ({});", model::Value::Integer(value));
        let parsed = pg_query::parse(&sql).unwrap();
        let pg_query::NodeEnum::InsertStmt(insert) = parsed.protobuf.stmts[0]
            .stmt
            .as_ref()
            .unwrap()
            .node
            .as_ref()
            .unwrap()
        else {
            panic!()
        };
        let pg_query::NodeEnum::SelectStmt(select) =
            insert.select_stmt.as_ref().unwrap().node.as_ref().unwrap()
        else {
            panic!()
        };
        let pg_query::NodeEnum::List(row) = select.values_lists[0].node.as_ref().unwrap() else {
            panic!()
        };
        assert_eq!(integer(&row.items[0]), value);
    }
}

#[test]
fn sql_wrapper_agrees_with_rendered_case_generator() {
    fn collect(typed: bool, profile: Profile, disabled: Vec<Feature>) -> Vec<String> {
        let mut result = Vec::new();
        hegel::Hegel::new(|tc: TestCase| {
            result.push(if typed {
                tc.draw(cases(profile, disabled.clone()).map(|case| case.to_string()))
            } else {
                tc.draw(workload(profile, disabled.clone()))
            });
        })
        .settings(
            hegel::Settings::new()
                .test_cases(30)
                .seed(Some(42))
                .phases([hegel::Phase::Generate]),
        )
        .run();
        result
    }
    for profile in [Profile::Tables, Profile::Compatible] {
        for disabled in [vec![], disabled_features(&[], &[]).unwrap()] {
            assert_eq!(
                collect(true, profile, disabled.clone()),
                collect(false, profile, disabled)
            );
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn access_control_rendering_preserves_typed_fields(tc: TestCase) {
    use model::*;
    use pg_query::{protobuf::AlterTableType as A, NodeEnum as N};
    let config = Config {
        profile: Profile::Compatible,
        disabled: vec![],
    };
    let table = tc.draw(schema::table(0, config.clone()));
    let statements = tc.draw(access_control(table, config));
    let parsed = pg_query::parse(&Workload(statements.clone()).to_string()).unwrap();
    assert_eq!(statements.len(), parsed.protobuf.stmts.len());
    for (expected, actual) in statements.iter().zip(parsed.protobuf.stmts) {
        match (expected, actual.stmt.unwrap().node.unwrap()) {
            (Statement::RowSecurity { table, action }, N::AlterTableStmt(actual)) => {
                assert_eq!(actual.relation.unwrap().relname, table.as_str());
                let N::AlterTableCmd(command) = actual.cmds[0].node.as_ref().unwrap() else {
                    panic!()
                };
                assert_eq!(
                    command.subtype,
                    match action {
                        RlsAction::Enable => A::AtEnableRowSecurity,
                        RlsAction::Force => A::AtForceRowSecurity,
                        RlsAction::Disable => A::AtDisableRowSecurity,
                    } as i32
                );
            }
            (Statement::CreatePolicy(policy), N::CreatePolicyStmt(actual)) => {
                assert_eq!(actual.policy_name, policy.name.as_str());
                assert_eq!(actual.table.unwrap().relname, policy.table.as_str());
                assert_eq!(actual.cmd_name, policy.command.to_string().to_lowercase());
                assert_eq!(
                    actual.permissive,
                    matches!(policy.mode, PolicyMode::Permissive)
                );
                assert_eq!(actual.qual.is_some(), policy.using.is_some());
                assert_eq!(actual.with_check.is_some(), policy.check.is_some());
                if let Some(expected) = &policy.using {
                    check_predicate(expected, actual.qual.as_deref().unwrap());
                }
                if let Some(expected) = &policy.check {
                    check_predicate(expected, actual.with_check.as_deref().unwrap());
                }
            }
            (Statement::AlterPolicy { name, table, using }, N::AlterPolicyStmt(actual)) => {
                assert_eq!(actual.policy_name, name.as_str());
                assert_eq!(actual.table.unwrap().relname, table.as_str());
                check_predicate(using, actual.qual.as_deref().unwrap());
            }
            (Statement::DropPolicy { .. }, N::DropStmt(actual)) => assert_eq!(
                actual.remove_type,
                pg_query::protobuf::ObjectType::ObjectPolicy as i32
            ),
            (
                Statement::Privilege {
                    action, command, ..
                },
                N::GrantStmt(actual),
            ) => {
                assert_eq!(actual.is_grant, matches!(action, PrivilegeAction::Grant));
                check_privilege(*command, &actual);
            }
            (
                Statement::DefaultPrivilege { command, .. },
                N::AlterDefaultPrivilegesStmt(actual),
            ) => check_privilege(*command, &actual.action.unwrap()),
            pair => panic!("wrong statement: {pair:?}"),
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn schema_rendering_preserves_columns_and_constraints(tc: TestCase) {
    use model::*;
    use pg_query::{protobuf::ConstrType as C, NodeEnum as N};
    let defaults = tc.draw(gs::booleans());
    let config = Config {
        profile: Profile::Tables,
        disabled: if defaults {
            vec![]
        } else {
            vec![Feature::Defaults]
        },
    };
    let expected = tc.draw(schema::table(0, config));
    let parsed = pg_query::parse(&Statement::CreateTable(expected.clone()).to_string()).unwrap();
    let N::CreateStmt(actual) = parsed.protobuf.stmts[0]
        .stmt
        .as_ref()
        .unwrap()
        .node
        .as_ref()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        actual.relation.as_ref().unwrap().relname,
        expected.name.as_str()
    );
    assert_eq!(actual.table_elts.len(), expected.columns.len());
    for (column, actual) in expected.columns.iter().zip(&actual.table_elts) {
        let Some(N::ColumnDef(actual)) = &actual.node else {
            panic!()
        };
        assert_eq!(actual.colname, column.name.as_str());
        let Some(N::String(name)) = &actual
            .type_name
            .as_ref()
            .unwrap()
            .names
            .last()
            .unwrap()
            .node
        else {
            panic!()
        };
        assert_eq!(
            name.sval,
            match column.data_type {
                DataType::Integer => "int4",
                DataType::BigInt => "int8",
                DataType::Text => "text",
                DataType::Boolean => "bool",
            }
        );
        assert_eq!(actual.constraints.len(), column.constraints.len());
        for (constraint, node) in column.constraints.iter().zip(&actual.constraints) {
            let Some(N::Constraint(actual)) = &node.node else {
                panic!()
            };
            assert_eq!(
                actual.contype,
                match constraint {
                    Constraint::PrimaryKey => C::ConstrPrimary,
                    Constraint::NotNull => C::ConstrNotnull,
                    Constraint::Default(_) => C::ConstrDefault,
                } as i32
            );
            if let Constraint::Default(value) = constraint {
                assert_eq!(
                    integer(actual.raw_expr.as_deref().unwrap()),
                    i64::from(*value)
                );
            }
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn indexes_preserve_keys_ordering_and_uniqueness(tc: TestCase) {
    use model::*;
    use pg_query::{protobuf::SortByDir, NodeEnum as N};
    let config = Config {
        profile: Profile::Tables,
        disabled: vec![],
    };
    let table = tc.draw(schema::table(0, config.clone()));
    let statements = tc.draw(schema::indexes(table, config));
    let parsed = pg_query::parse(&Workload(statements.clone()).to_string()).unwrap();
    assert_eq!(statements.len(), parsed.protobuf.stmts.len());
    for (statement, actual) in statements.iter().zip(parsed.protobuf.stmts) {
        let Statement::CreateIndex {
            name,
            table,
            keys,
            unique,
        } = statement
        else {
            panic!()
        };
        let N::IndexStmt(actual) = actual.stmt.unwrap().node.unwrap() else {
            panic!()
        };
        assert_eq!(actual.idxname, name.as_str());
        assert_eq!(actual.relation.unwrap().relname, table.as_str());
        assert_eq!(actual.unique, *unique);
        assert_eq!(actual.index_params.len(), keys.len());
        for (expected, node) in keys.iter().zip(&actual.index_params) {
            let Some(N::IndexElem(actual)) = &node.node else {
                panic!()
            };
            assert_eq!(actual.name, expected.column.as_str());
            assert_eq!(
                actual.ordering,
                match expected.direction {
                    SortDirection::Ascending => SortByDir::SortbyAsc,
                    SortDirection::Descending => SortByDir::SortbyDesc,
                } as i32
            );
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn generated_schema_and_index_keys_are_consistent(tc: TestCase) {
    use model::*;
    let disabled = tc.draw(
        gs::vecs(gs::sampled_from(Feature::value_variants()))
            .max_size(Feature::value_variants().len()),
    );
    let config = Config {
        profile: Profile::Compatible,
        disabled,
    };
    let table = tc.draw(schema::table(0, config.clone()));
    assert!((1..=8).contains(&table.columns.len()));
    assert_eq!(
        table
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        table.columns.len()
    );
    assert!(table.columns.iter().filter(|c| c.is_primary_key()).count() <= 1);
    for column in &table.columns {
        for constraint in &column.constraints {
            if matches!(constraint, Constraint::Default(_)) {
                assert!(config.enabled(Feature::Defaults));
                assert!(matches!(
                    column.data_type,
                    DataType::Integer | DataType::BigInt
                ));
            }
        }
    }
    let indexes = tc.draw(schema::indexes(table.clone(), config.clone()));
    assert!(indexes.len() <= 3);
    let mut names = std::collections::BTreeSet::new();
    for index in indexes {
        let Statement::CreateIndex {
            name, keys, unique, ..
        } = index
        else {
            panic!()
        };
        assert!(names.insert(name.as_str().to_owned()));
        assert!((1..=table.columns.len().min(3)).contains(&keys.len()));
        assert_eq!(
            keys.iter()
                .map(|k| k.column.as_str())
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            keys.len()
        );
        let columns: Vec<_> = keys
            .iter()
            .map(|key| {
                table
                    .columns
                    .iter()
                    .find(|c| c.name == key.column)
                    .expect("index column belongs to its table")
            })
            .collect();
        if unique {
            assert!(columns.iter().any(|c| c.is_primary_key()));
        }
        if !config.enabled(Feature::Indexes) {
            panic!("indexes disabled");
        }
    }
}

fn check_privilege(command: model::Command, grant: &pg_query::protobuf::GrantStmt) {
    if command == model::Command::All {
        assert!(grant.privileges.is_empty());
    } else {
        assert_eq!(grant.privileges.len(), 1);
        let Some(pg_query::NodeEnum::AccessPriv(actual)) = &grant.privileges[0].node else {
            panic!()
        };
        assert_eq!(actual.priv_name, command.to_string().to_lowercase());
    }
}

fn check_predicate(predicate: &model::Predicate, node: &pg_query::protobuf::Node) {
    use pg_query::NodeEnum as N;
    if let model::Predicate::IsNull { column, negated } = predicate {
        let Some(N::NullTest(test)) = &node.node else {
            panic!()
        };
        assert_eq!(
            test.nulltesttype,
            if *negated {
                pg_query::protobuf::NullTestType::IsNotNull
            } else {
                pg_query::protobuf::NullTestType::IsNull
            } as i32
        );
        let Some(N::ColumnRef(actual)) = test.arg.as_ref().unwrap().node.as_ref() else {
            panic!()
        };
        let Some(N::String(name)) = &actual.fields[0].node else {
            panic!()
        };
        assert_eq!(name.sval, column.as_str());
        return;
    }
    let model::Predicate::Compare {
        column,
        comparison,
        value,
    } = predicate
    else {
        panic!()
    };
    let Some(N::AExpr(expr)) = &node.node else {
        panic!()
    };
    let Some(N::String(op)) = &expr.name[0].node else {
        panic!()
    };
    assert_eq!(
        op.sval,
        match comparison {
            model::Comparison::GreaterOrEqual => ">=",
            model::Comparison::NotEqual => "<>",
        }
    );
    assert_eq!(integer(expr.rexpr.as_deref().unwrap()), i64::from(*value));
    let Some(N::ColumnRef(actual)) = expr.lexpr.as_ref().unwrap().node.as_ref() else {
        panic!()
    };
    let Some(N::String(name)) = &actual.fields[0].node else {
        panic!()
    };
    assert_eq!(name.sval, column.as_str());
}

fn integer(node: &pg_query::protobuf::Node) -> i64 {
    use pg_query::{protobuf::a_const::Val, NodeEnum as N};
    let Some(N::AConst(value)) = &node.node else {
        panic!()
    };
    match value.val.as_ref().unwrap() {
        Val::Ival(value) => i64::from(value.ival),
        Val::Fval(value) => value.fval.parse().unwrap(),
        _ => panic!("expected integer"),
    }
}

#[hegel::test(test_cases = 2000)]
fn generated_unique_indexes_accept_inserted_rows(tc: TestCase) {
    use model::*;
    let config = Config {
        profile: Profile::Compatible,
        disabled: vec![],
    };
    let table = tc.draw(schema::table(0, config.clone()));
    let inserts = tc.draw(data::inserts(table.clone()));
    let rows: Vec<_> = inserts
        .iter()
        .map(|statement| {
            let Statement::Insert { row, .. } = statement else {
                panic!("only VALUES inserts")
            };
            row
        })
        .collect();
    assert!(rows.len() <= 13);
    if rows.len() == 13 {
        assert!(!table.has_primary_key());
        assert!(rows[..12].iter().any(|row| row.0 == rows[12].0));
    }
    for statement in tc.draw(schema::indexes(table.clone(), config)) {
        let Statement::CreateIndex { keys, unique, .. } = statement else {
            panic!()
        };
        if !unique {
            continue;
        }
        let positions: Vec<_> = keys
            .iter()
            .map(|key| {
                table
                    .columns
                    .iter()
                    .position(|column| column.name == key.column)
                    .unwrap()
            })
            .collect();
        let mut seen = Vec::new();
        for row in &rows {
            let tuple: Vec<_> = positions
                .iter()
                .map(|position| row.0[*position].clone())
                .collect();
            if tuple.contains(&Value::Null) {
                continue;
            }
            assert!(
                !seen.contains(&tuple),
                "generated rows violate UNIQUE index"
            );
            seen.push(tuple);
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn policies_only_reference_compatible_generated_columns(tc: TestCase) {
    use model::*;
    let config = Config {
        profile: Profile::Compatible,
        disabled: vec![],
    };
    let table = tc.draw(schema::table(0, config.clone()));
    let statements = tc.draw(access_control(table.clone(), config));
    for statement in statements {
        let predicates = match statement {
            Statement::CreatePolicy(policy) => policy
                .using
                .into_iter()
                .chain(policy.check)
                .collect::<Vec<_>>(),
            Statement::AlterPolicy { using, .. } => vec![using],
            _ => continue,
        };
        for predicate in predicates {
            match predicate {
                Predicate::Compare { column, .. } => {
                    let column = table
                        .columns
                        .iter()
                        .find(|c| c.name == column)
                        .expect("column belongs to table");
                    assert!(matches!(
                        column.data_type,
                        DataType::Integer | DataType::BigInt
                    ));
                }
                Predicate::IsNull { column, .. } => {
                    let column = table
                        .columns
                        .iter()
                        .find(|c| c.name == column)
                        .expect("column belongs to table");
                    assert!(matches!(
                        column.data_type,
                        DataType::Text | DataType::Boolean
                    ));
                }
            }
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn workload_uses_declared_tables_and_distinct_relation_names(tc: TestCase) {
    use model::*;
    let profile = tc.draw(gs::sampled_from(&[Profile::Tables, Profile::Compatible]));
    let case = tc.draw(cases(profile, vec![]));
    let mut relations = std::collections::BTreeSet::new();
    let mut tables = std::collections::BTreeSet::new();
    for statement in case.0 {
        let table = match statement {
            Statement::StandardConformingStrings | Statement::DefaultPrivilege { .. } => continue,
            Statement::CreateTable(table) => {
                assert!(relations.insert(table.name.as_str().to_owned()));
                assert!(tables.insert(table.name.as_str().to_owned()));
                continue;
            }
            Statement::CreateIndex { name, table, .. } => {
                assert!(relations.insert(name.as_str().to_owned()));
                table
            }
            Statement::CreatePolicy(policy) => policy.table,
            Statement::Insert { table, .. }
            | Statement::RowSecurity { table, .. }
            | Statement::AlterPolicy { table, .. }
            | Statement::DropPolicy { table, .. }
            | Statement::Privilege { table, .. } => table,
        };
        assert!(
            tables.contains(table.as_str()),
            "table must be declared before use"
        );
    }
}

#[test]
fn seeded_campaign_exercises_schema_and_index_variants() {
    use model::*;
    use std::collections::BTreeSet;
    let mut default_signs = BTreeSet::new();
    let mut mixed_case_table = false;
    let mut mixed_case_column = false;
    let mut widths = BTreeSet::new();
    let mut types = BTreeSet::new();
    let mut primary_positions = BTreeSet::new();
    let mut key_counts = BTreeSet::new();
    let mut directions = BTreeSet::new();
    let mut uniqueness = BTreeSet::new();
    hegel::Hegel::new(|tc: TestCase| {
        for statement in tc
            .draw(cases(
                Profile::Compatible,
                disabled_features(&[], &[]).unwrap(),
            ))
            .0
        {
            match statement {
                Statement::CreateTable(table) => {
                    mixed_case_table |= table.name.as_str().chars().any(char::is_uppercase);
                    widths.insert(table.columns.len());
                    for (position, column) in table.columns.iter().enumerate() {
                        mixed_case_column |= column.name.as_str().chars().any(char::is_uppercase);
                        for constraint in &column.constraints {
                            if let Constraint::Default(value) = constraint {
                                default_signs.insert(value.signum());
                            }
                        }
                        types.insert(column.data_type.to_string());
                        if column.is_primary_key() {
                            primary_positions.insert(position);
                        }
                    }
                }
                Statement::CreateIndex { keys, unique, .. } => {
                    key_counts.insert(keys.len());
                    uniqueness.insert(unique);
                    for key in keys {
                        directions.insert(matches!(key.direction, SortDirection::Descending));
                    }
                }
                _ => {}
            }
        }
    })
    .settings(
        hegel::Settings::new()
            .test_cases(300)
            .seed(Some(42))
            .phases([hegel::Phase::Generate]),
    )
    .run();
    assert!(mixed_case_table);
    assert!(mixed_case_column);
    assert_eq!(default_signs, [-1, 0, 1].into_iter().collect());
    assert_eq!(widths, (1..=8).collect());
    assert_eq!(
        types,
        ["INTEGER", "BIGINT", "TEXT", "BOOLEAN"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    );
    assert!(primary_positions.len() > 1);
    assert_eq!(key_counts, (1..=3).collect());
    assert_eq!(directions, [false, true].into_iter().collect());
    assert_eq!(uniqueness, [false, true].into_iter().collect());
}
