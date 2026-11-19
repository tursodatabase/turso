use rustc_hash::FxHashMap as HashMap;
use turso_core::alloc::vec;
use turso_core::alloc::*;
use turso_core::dialect::Dialect;
use turso_core::namespace::NamespaceId;
use turso_core::schema::*;
use turso_core::sync::Arc;
use turso_core::CollationSeq;
use turso_core::LimboError;
use turso_core::Result;
use turso_core::SymbolTable;
use turso_parser::ast::Cmd;
use turso_parser::ast::CreateTableBody;
use turso_parser::ast::SortOrder;
use turso_parser::ast::Stmt;
use turso_sqlite::SqliteDialect;

#[test]
fn a_new_schema_has_only_the_root_namespace_with_the_schema_table() {
    let schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();
    let ids: std::vec::Vec<NamespaceId> = schema.namespace_ids().collect();
    assert_eq!(ids, vec![NamespaceId::ROOT]);
    assert!(schema
        .get_table(NamespaceId::ROOT, SCHEMA_TABLE_NAME)
        .is_some());
}

#[test]
fn test_column_definition_matches_create_table() -> Result<()> {
    for sql in [
        "CREATE TABLE t(a)",
        "CREATE TABLE t(a INTEGER PRIMARY KEY)",
        "CREATE TABLE t(a INT PRIMARY KEY)",
        "CREATE TABLE t(a InTeGeR PRIMARY KEY ASC)",
        "CREATE TABLE t(a INTEGER PRIMARY KEY DESC)",
        "CREATE TABLE t(a BIGINT PRIMARY KEY)",
        "CREATE TABLE t(a TEXT PRIMARY KEY)",
        "CREATE TABLE t(a INTEGER PRIMARY KEY NOT NULL UNIQUE)",
        "CREATE TABLE t(a TEXT UNIQUE NOT NULL ON CONFLICT IGNORE DEFAULT hello COLLATE NOCASE)",
        "CREATE TABLE t(a TEXT NOT NULL NULL)",
        "CREATE TABLE t(a TEXT NULL NOT NULL ON CONFLICT REPLACE DEFAULT 'x')",
        "CREATE TABLE t(a NUMERIC(10, 2) DEFAULT (1 + 2))",
        "CREATE TABLE t(a VARCHAR(40) COLLATE RTRIM)",
        "CREATE TABLE t(a ANY)",
        "CREATE TABLE t(a aNy) STRICT",
        "CREATE TABLE t(a INTEGER[]) STRICT",
        "CREATE TABLE t(a INTEGER[][]) STRICT",
        "CREATE TABLE t(a TEXT, b ANY AS (a))",
        "CREATE TABLE t(a TEXT, b aNy AS (a)) STRICT",
    ] {
        let Some(Cmd::Stmt(Stmt::CreateTable { body, .. })) =
            turso_core::dialect::parse_statement(&SqliteDialect, sql)?
        else {
            panic!("expected CREATE TABLE: {sql}");
        };
        let table = create_table("t", &body, 0, false)?;
        let CreateTableBody::ColumnsAndConstraints { columns, .. } = body else {
            panic!("expected column definitions: {sql}");
        };
        let flags =
            FromDefinitionFlags::empty().with(FromDefinitionFlags::InStrictTable, table.is_strict);
        assert_eq!(columns.len(), table.columns().len(), "{sql}");
        for (definition, expected) in columns.iter().zip(table.columns()) {
            let actual = Column::from_definition(definition, flags)?;
            assert_eq!(actual.name, expected.name, "{sql}");
            assert_eq!(actual.ty_str, expected.ty_str, "{sql}");
            assert_eq!(actual.ty_params, expected.ty_params, "{sql}");
            assert_eq!(actual.default, expected.default, "{sql}");
            assert_eq!(actual.ty(), expected.ty(), "{sql}");
            assert_eq!(actual.affinity(), expected.affinity(), "{sql}");
            assert_eq!(actual.text_collation(), expected.text_collation(), "{sql}");
            assert_eq!(actual.primary_key(), expected.primary_key(), "{sql}");
            assert_eq!(actual.is_rowid_alias(), expected.is_rowid_alias(), "{sql}");
            assert_eq!(actual.notnull(), expected.notnull(), "{sql}");
            assert_eq!(
                actual.explicit_notnull(),
                expected.explicit_notnull(),
                "{sql}"
            );
            assert_eq!(actual.unique(), expected.unique(), "{sql}");
            assert_eq!(actual.hidden(), expected.hidden(), "{sql}");
            assert_eq!(
                actual.notnull_conflict_clause, expected.notnull_conflict_clause,
                "{sql}"
            );
            assert_eq!(
                actual.array_dimensions(),
                expected.array_dimensions(),
                "{sql}"
            );
            match (actual.generated_type(), expected.generated_type()) {
                (
                    GeneratedType::Virtual {
                        original_sql: actual,
                        ..
                    },
                    GeneratedType::Virtual {
                        original_sql: expected,
                        ..
                    },
                ) => assert_eq!(actual, expected, "{sql}"),
                (GeneratedType::NotGenerated, GeneratedType::NotGenerated) => {}
                _ => panic!("generated column types differ: {sql}"),
            }
        }
    }
    Ok(())
}

#[test]
fn test_schema_loading_accepts_generated_columns() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();

    schema
        .handle_schema_row(
            "table",
            "t1",
            "t1",
            2,
            Some("CREATE TABLE t1(a INTEGER, b AS (a*2))"),
            &SymbolTable::default(),
            &mut vec![],
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &|_| None,
            &SqliteDialect,
        )
        .unwrap();

    let table = schema.get_btree_table(NamespaceId::ROOT, "t1").unwrap();
    assert!(table.has_virtual_columns);
}

#[test]
fn test_schema_row_vtab_with_nonzero_root_page_is_corrupt() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();

    let result = schema.handle_schema_row(
        "table",
        "v1",
        "v1",
        2,
        Some("CREATE VIRTUAL TABLE v1 USING somemodule"),
        &SymbolTable::default(),
        &mut vec![],
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &|_| None,
        &SqliteDialect,
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("root_page must be 0 for virtual table v1"));
}

#[test]
fn test_schema_row_with_zero_root_page_takes_virtual_table_path() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();

    // Root page 0 must route to virtual-table handling; with no such
    // module registered, that path fails module resolution instead of
    // creating a B-tree table with an invalid root page.
    let result = schema.handle_schema_row(
        "table",
        "v1",
        "v1",
        0,
        Some("CREATE VIRTUAL TABLE v1 USING nosuchmodule"),
        &SymbolTable::default(),
        &mut vec![],
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &|_| None,
        &SqliteDialect,
    );
    assert!(result.is_err());
    assert!(schema.get_table(NamespaceId::ROOT, "v1").is_none());
}

#[test]
fn test_schema_row_with_zero_root_page_rejects_malformed_sql() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();

    let result = schema.handle_schema_row(
        "table",
        "v1",
        "v1",
        0,
        Some("CREATE VIRTUAL TABLE v1 USING"),
        &SymbolTable::default(),
        &mut vec![],
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &|_| None,
        &SqliteDialect,
    );
    assert!(result.is_err());
    assert!(schema.get_table(NamespaceId::ROOT, "v1").is_none());
}

#[test]
fn test_schema_row_table_with_virtual_table_substring_is_btree() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();

    // A regular table whose SQL merely contains virtual-table syntax
    // (e.g. in a DEFAULT literal) must classify as a B-tree table.
    schema
        .handle_schema_row(
            "table",
            "t1",
            "t1",
            2,
            Some("CREATE TABLE t1(x TEXT DEFAULT 'create virtual table')"),
            &SymbolTable::default(),
            &mut vec![],
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &|_| None,
            &SqliteDialect,
        )
        .unwrap();
    let table = schema.get_btree_table(NamespaceId::ROOT, "t1").unwrap();
    assert_eq!(table.root_page, 2);
}

#[test]
fn test_named_unique_constraint_propagates_through_schema() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();
    let table_sql =
        r#"CREATE TABLE t_u (id INT PRIMARY KEY, code TEXT CONSTRAINT t_u_code_uq UNIQUE)"#;

    schema
        .handle_schema_row(
            "table",
            "t_u",
            "t_u",
            1,
            Some(table_sql),
            &SymbolTable::default(),
            &mut vec![],
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &|_| None,
            &SqliteDialect,
        )
        .unwrap();

    let mut automatic_indices = HashMap::default();
    automatic_indices.insert(
        "t_u".to_string(),
        vec![
            ("sqlite_autoindex_t_u_1".to_string(), 2i64),
            ("sqlite_autoindex_t_u_2".to_string(), 3i64),
        ],
    );

    schema
        .populate_indices(
            &SqliteDialect,
            &SymbolTable::default(),
            vec![],
            automatic_indices,
            false,
        )
        .unwrap();

    let indexes: std::vec::Vec<_> = schema.get_indices(NamespaceId::ROOT, "t_u").collect();
    let unique_idx = indexes
        .iter()
        .find(|idx| !idx.columns.iter().any(|c| c.name == "id"))
        .unwrap();
    assert_eq!(
        unique_idx.constraint_name,
        Some("t_u_code_uq".to_string()),
        "named UNIQUE constraint name must propagate through schema loading"
    );
}

fn table(sql: &str, root_page: i64) -> Arc<BTreeTable> {
    Arc::new(
        turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, root_page, false)
            .unwrap(),
    )
}

#[test]
fn a_namespace_holds_only_its_own_objects() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();
    let other = NamespaceId::new(1);
    schema.add_namespace(other, "other");
    schema
        .add_btree_table(NamespaceId::ROOT, table("CREATE TABLE t (x)", 2))
        .unwrap();
    schema
        .add_btree_table(other, table("CREATE TABLE t (x, y)", 3))
        .unwrap();

    let root_t = schema.get_btree_table(NamespaceId::ROOT, "t").unwrap();
    let other_t = schema.get_btree_table(other, "t").unwrap();
    assert_eq!(root_t.root_page, 2);
    assert_eq!(other_t.root_page, 3);
    assert_eq!(
        schema.get_object_type(other, "t"),
        Some(SchemaObjectType::Table)
    );
    assert!(schema.get_table(other, SCHEMA_TABLE_NAME).is_none());

    schema.remove_table(other, "t");
    assert!(schema.get_table(other, "t").is_none());
    assert!(schema.get_table(NamespaceId::ROOT, "t").is_some());
}

#[test]
fn removing_a_namespace_removes_its_objects() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();
    let other = NamespaceId::new(7);
    schema.add_namespace(other, "other");
    schema
        .add_btree_table(other, table("CREATE TABLE t (x)", 3))
        .unwrap();
    assert!(schema.has_namespace(other));

    assert!(schema.remove_namespace(other));
    assert!(!schema.has_namespace(other));
    let ids: std::vec::Vec<NamespaceId> = schema.namespace_ids().collect();
    assert_eq!(ids, vec![NamespaceId::ROOT]);
}

#[test]
fn a_cloned_schema_keeps_every_namespace() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();
    let other = NamespaceId::new(2);
    schema.add_namespace(other, "other");
    schema
        .add_btree_table(other, table("CREATE TABLE t (x)", 3))
        .unwrap();
    let clone = schema.try_clone().unwrap();
    assert_eq!(clone.get_btree_table(other, "t").unwrap().root_page, 3);
}

#[test]
fn a_foreign_key_finds_its_parent_in_the_parent_namespace() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();
    let other = NamespaceId::new(1);
    schema.add_namespace(other, "other");
    schema
        .add_btree_table(
            NamespaceId::ROOT,
            table("CREATE TABLE p (id INTEGER PRIMARY KEY)", 2),
        )
        .unwrap();
    schema
        .add_btree_table(other, table("CREATE TABLE p (id INTEGER PRIMARY KEY)", 3))
        .unwrap();
    schema
        .add_btree_table(other, table("CREATE TABLE c (x REFERENCES p(id))", 4))
        .unwrap();

    let children = schema
        .resolved_fks_referencing(NamespaceId::ROOT, "p")
        .unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].child_namespace, other);
    assert_eq!(children[0].child_table.name, "c");
    assert!(schema.any_resolved_fks_referencing(NamespaceId::ROOT, "p"));

    assert!(schema
        .resolved_fks_referencing(other, "p")
        .unwrap()
        .is_empty());
    assert!(!schema.any_resolved_fks_referencing(other, "p"));

    let parents = schema.resolved_fks_for_child(other, "c", None).unwrap();
    assert_eq!(parents.len(), 1);
    assert_eq!(parents[0].fk.parent_namespace, NamespaceId::ROOT);
}

#[test]
fn schema_wide_scans_see_every_namespace() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();
    let other = NamespaceId::new(1);
    schema.add_namespace(other, "other");
    schema
        .add_btree_table(other, table("CREATE TABLE t (x UNIQUE)", 3))
        .unwrap();
    let t = schema.get_btree_table(other, "t").unwrap();
    schema
        .add_index(
            other,
            Arc::new(
                Index::from_sql(
                    &SqliteDialect,
                    &SymbolTable::new(),
                    "CREATE INDEX i ON t (x)",
                    4,
                    t.as_ref(),
                    &schema,
                )
                .unwrap(),
            ),
        )
        .unwrap();

    assert!(schema.all_tables().any(|table| table.get_name() == "t"));
    assert!(schema.all_indexes().any(|index| index.name == "i"));
    assert!(schema
        .all_tables_mut()
        .any(|(namespace, name, _)| namespace == other && name == "t"));

    assert!(!schema.has_materialized_views());
    schema.add_materialized_view_dependency(other, "t", "v");
    assert!(schema.has_materialized_views());
}

#[test]
fn tables_of_the_same_name_in_two_namespaces_keep_their_own_statistics() {
    let mut schema = <dyn Dialect>::new_schema(&SqliteDialect, true).unwrap();
    let other = NamespaceId::new(1);
    schema.add_namespace(other, "other");
    schema
        .add_btree_table(NamespaceId::ROOT, table("CREATE TABLE t (x)", 2))
        .unwrap();
    schema
        .add_btree_table(other, table("CREATE TABLE t (x)", 3))
        .unwrap();
    schema
        .analyze_stats
        .table_stats_mut(NamespaceId::ROOT, "t")
        .row_count = Some(10);
    schema.analyze_stats.table_stats_mut(other, "t").row_count = Some(20);

    let row_count = |schema: &Schema, namespace| {
        schema
            .analyze_stats
            .table_stats(namespace, "t")
            .and_then(|stats| stats.row_count)
    };
    assert_eq!(row_count(&schema, NamespaceId::ROOT), Some(10));
    assert_eq!(row_count(&schema, other), Some(20));

    schema.remove_table(other, "t");
    assert_eq!(row_count(&schema, other), None);
    assert_eq!(row_count(&schema, NamespaceId::ROOT), Some(10));
}

#[test]
pub fn test_has_rowid_true() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    assert!(table.has_rowid, "has_rowid should be set to true");
    Ok(())
}

#[test]
pub fn test_has_rowid_false() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    assert!(!table.has_rowid, "has_rowid should be set to false");
    Ok(())
}

#[test]
pub fn test_column_default_collation_is_effective_binary() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a TEXT);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert_eq!(column.collation(), CollationSeq::BINARY);
    assert_eq!(column.text_collation(), None);
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_single_text() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a TEXT PRIMARY KEY, b TEXT);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(
        !column.is_rowid_alias(),
        "column 'a´ has type different than INTEGER so can't be a rowid alias"
    );
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_single_integer() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(
        column.is_rowid_alias(),
        "column 'a´ should be a rowid alias"
    );
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_single_integer_separate_primary_key_definition() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(
        column.is_rowid_alias(),
        "column 'a´ should be a rowid alias"
    );
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_single_integer_separate_primary_key_definition_without_rowid(
) -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a)) WITHOUT ROWID;"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(
        !column.is_rowid_alias(),
        "column 'a´ shouldn't be a rowid alias because table has no rowid"
    );
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_single_integer_without_rowid() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(
        !column.is_rowid_alias(),
        "column 'a´ shouldn't be a rowid alias because table has no rowid"
    );
    Ok(())
}

#[test]
pub fn test_multiple_pk_forbidden() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("table \"t1\" has more than one primary key"))
    );
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_separate_composite_primary_key_definition() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(
        !column.is_rowid_alias(),
        "column 'a´ shouldn't be a rowid alias because table has composite primary key"
    );
    Ok(())
}

#[test]
pub fn test_primary_key_inline_single() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT, c REAL);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(column.primary_key(), "column 'a' should be a primary key");
    let column = table.get_column("b").unwrap().1;
    assert!(
        !column.primary_key(),
        "column 'b' shouldn't be a primary key"
    );
    let column = table.get_column("c").unwrap().1;
    assert!(
        !column.primary_key(),
        "column 'c' shouldn't be a primary key"
    );
    assert_eq!(
        vec![("a".to_string(), SortOrder::Asc)],
        table.primary_key_columns,
        "primary key column names should be ['a']"
    );
    Ok(())
}

#[test]
pub fn test_primary_key_inline_multiple_forbidden() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY, c REAL);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("table \"t1\" has more than one primary key"))
    );
    Ok(())
}

#[test]
pub fn test_conflicting_on_conflict_unique_rejected() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a) ON CONFLICT IGNORE);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("conflicting ON CONFLICT clauses"))
    );
    Ok(())
}

#[test]
pub fn test_conflicting_on_conflict_composite_unique_rejected() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a, b, UNIQUE(a, b) ON CONFLICT FAIL, UNIQUE(a, b) ON CONFLICT IGNORE);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("conflicting ON CONFLICT clauses"))
    );
    Ok(())
}

#[test]
pub fn test_same_on_conflict_unique_allowed() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a) ON CONFLICT FAIL);"#;
    assert!(turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false).is_ok());
    Ok(())
}

#[test]
pub fn test_one_on_conflict_unique_allowed() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a));"#;
    assert!(turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false).is_ok());
    Ok(())
}

#[test]
pub fn test_primary_key_separate_single() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a desc));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(column.primary_key(), "column 'a' should be a primary key");
    let column = table.get_column("b").unwrap().1;
    assert!(
        !column.primary_key(),
        "column 'b' shouldn't be a primary key"
    );
    let column = table.get_column("c").unwrap().1;
    assert!(
        !column.primary_key(),
        "column 'c' shouldn't be a primary key"
    );
    assert_eq!(
        vec![("a".to_string(), SortOrder::Desc)],
        table.primary_key_columns,
        "primary key column names should be ['a']"
    );
    Ok(())
}

#[test]
pub fn test_primary_key_separate_multiple() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a, b desc));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(column.primary_key(), "column 'a' should be a primary key");
    let column = table.get_column("b").unwrap().1;
    assert!(column.primary_key(), "column 'b' shouldn be a primary key");
    let column = table.get_column("c").unwrap().1;
    assert!(
        !column.primary_key(),
        "column 'c' shouldn't be a primary key"
    );
    assert_eq!(
        vec![
            ("a".to_string(), SortOrder::Asc),
            ("b".to_string(), SortOrder::Desc)
        ],
        table.primary_key_columns,
        "primary key column names should be ['a', 'b']"
    );
    Ok(())
}

#[test]
pub fn test_primary_key_separate_single_quoted() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY('a'));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(column.primary_key(), "column 'a' should be a primary key");
    let column = table.get_column("b").unwrap().1;
    assert!(
        !column.primary_key(),
        "column 'b' shouldn't be a primary key"
    );
    let column = table.get_column("c").unwrap().1;
    assert!(
        !column.primary_key(),
        "column 'c' shouldn't be a primary key"
    );
    assert_eq!(
        vec![("a".to_string(), SortOrder::Asc)],
        table.primary_key_columns,
        "primary key column names should be ['a']"
    );
    Ok(())
}

#[test]
pub fn test_primary_key_separate_single_doubly_quoted() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY("a"));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(column.primary_key(), "column 'a' should be a primary key");
    let column = table.get_column("b").unwrap().1;
    assert!(
        !column.primary_key(),
        "column 'b' shouldn't be a primary key"
    );
    let column = table.get_column("c").unwrap().1;
    assert!(
        !column.primary_key(),
        "column 'c' shouldn't be a primary key"
    );
    assert_eq!(
        vec![("a".to_string(), SortOrder::Asc)],
        table.primary_key_columns,
        "primary key column names should be ['a']"
    );
    Ok(())
}

#[test]
pub fn test_default_value() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER DEFAULT 23);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    let default = column.default.clone().unwrap();
    assert_eq!(default.to_string(), "23");
    Ok(())
}

#[test]
pub fn test_col_notnull() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER NOT NULL);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(column.notnull());
    Ok(())
}

#[test]
fn rowid_alias_is_not_nullable() {
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY)",
        "CREATE TABLE t(id INTEGER, PRIMARY KEY(id))",
    ] {
        let table =
            turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 2, false).unwrap();
        let column = table.get_column("id").unwrap().1;
        assert!(column.notnull(), "{sql}");
        assert!(!column.explicit_notnull(), "{sql}");
    }
    let table = turso_core::schema::BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(id INTEGER PRIMARY KEY DESC)",
        2,
        false,
    )
    .unwrap();
    assert!(!table.get_column("id").unwrap().1.notnull());
}

#[test]
pub fn test_col_notnull_negative() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert!(!column.notnull());
    Ok(())
}

#[test]
pub fn test_col_type_string_integer() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a InTeGeR);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let column = table.get_column("a").unwrap().1;
    assert_eq!(column.ty_str, "InTeGeR");
    Ok(())
}

#[test]
fn check_constraint_comments_survive_table_reconstruction() -> Result<()> {
    for (sql, comment) in [
        (
            "CREATE TABLE t (x CHECK(x /* block comment */ > 0))",
            "/* block comment */",
        ),
        (
            "CREATE TABLE t (x CHECK(x > 0 -- line comment\n))",
            "-- line comment",
        ),
    ] {
        let reconstructed =
            turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?
                .to_sql();

        assert!(
            reconstructed.contains(comment),
            "reconstructed SQL: {reconstructed}"
        );
        turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, &reconstructed, 0, false)?;
    }

    Ok(())
}

#[test]
pub fn test_special_column_names() -> Result<()> {
    let tests = [
        ("foobar", "CREATE TABLE t (foobar TEXT)"),
        ("_table_name3", r#"CREATE TABLE t (_table_name3 TEXT)"#),
        ("special name", r#"CREATE TABLE t ("special name" TEXT)"#),
        ("foo&bar", r#"CREATE TABLE t ("foo&bar" TEXT)"#),
        (" name", r#"CREATE TABLE t (" name" TEXT)"#),
    ];

    for (input_column_name, expected_sql) in tests {
        let sql = format!(r#"CREATE TABLE t ("{input_column_name}" TEXT)"#);
        let actual =
            turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, &sql, 0, false)?
                .to_sql();
        assert_eq!(expected_sql, actual);
    }

    Ok(())
}

#[test]
fn test_special_table_names_are_quoted_in_to_sql() -> Result<()> {
    let tests = [
        (
            r#"CREATE TABLE "t t" (x TEXT)"#,
            r#"CREATE TABLE "t t" (x TEXT)"#,
        ),
        (
            r#"CREATE TABLE "123table" (x TEXT)"#,
            r#"CREATE TABLE "123table" (x TEXT)"#,
        ),
        (
            r#"CREATE TABLE "t""t" (x TEXT)"#,
            r#"CREATE TABLE "t""t" (x TEXT)"#,
        ),
    ];

    for (input_sql, expected_sql) in tests {
        let actual =
            turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, input_sql, 0, false)?
                .to_sql();
        assert_eq!(actual, expected_sql);
    }

    Ok(())
}

#[test]
#[should_panic]
fn test_automatic_index_single_column() {
    // Without composite primary keys, we should not have an automatic index on a primary key that is a rowid alias
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
    let table =
        turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false).unwrap();
    let _index = Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        1,
        None,
        &[],
        None,
    )
    .unwrap();
}

#[test]
fn test_automatic_index_composite_key() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let index = Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        2,
        None,
        &[],
        None,
    )?;

    assert_eq!(index.name, "sqlite_autoindex_t1_1");
    assert_eq!(index.table_name, "t1");
    assert_eq!(index.root_page, 2);
    assert!(index.unique);
    assert_eq!(index.columns.len(), 2);
    assert_eq!(index.columns[0].name, "a");
    assert_eq!(index.columns[1].name, "b");
    assert!(matches!(index.columns[0].order, SortOrder::Asc));
    assert!(matches!(index.columns[1].order, SortOrder::Asc));
    Ok(())
}

#[test]
#[should_panic]
fn test_automatic_index_no_primary_key() {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT);"#;
    let table =
        turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false).unwrap();
    Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        1,
        None,
        &[],
        None,
    )
    .unwrap();
}

#[test]
fn test_automatic_index_unique_column() -> Result<()> {
    let sql = r#"CREATE table t1 (x INTEGER, y INTEGER UNIQUE);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let index = Index::automatic_from_unique(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        vec![(1, SortOrder::Asc)],
        None,
        &[],
        None,
    )?;

    assert_eq!(index.name, "sqlite_autoindex_t1_1");
    assert_eq!(index.table_name, "t1");
    assert_eq!(index.root_page, 2);
    assert!(index.unique);
    assert_eq!(index.columns.len(), 1);
    assert_eq!(index.columns[0].name, "y");
    assert!(matches!(index.columns[0].order, SortOrder::Asc));
    Ok(())
}

#[test]
fn test_automatic_index_pkey_unique_column() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (x PRIMARY KEY, y UNIQUE);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let indices = [
        Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            1,
            None,
            &[],
            None,
        )?,
        Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_2".to_string(), 3),
            vec![(1, SortOrder::Asc)],
            None,
            &[],
            None,
        )?,
    ];

    assert_eq!(indices[0].name, "sqlite_autoindex_t1_1");
    assert_eq!(indices[0].table_name, "t1");
    assert_eq!(indices[0].root_page, 2);
    assert!(indices[0].unique);
    assert_eq!(indices[0].columns.len(), 1);
    assert_eq!(indices[0].columns[0].name, "x");
    assert!(matches!(indices[0].columns[0].order, SortOrder::Asc));

    assert_eq!(indices[1].name, "sqlite_autoindex_t1_2");
    assert_eq!(indices[1].table_name, "t1");
    assert_eq!(indices[1].root_page, 3);
    assert!(indices[1].unique);
    assert_eq!(indices[1].columns.len(), 1);
    assert_eq!(indices[1].columns[0].name, "y");
    assert!(matches!(indices[1].columns[0].order, SortOrder::Asc));

    Ok(())
}

#[test]
fn test_automatic_index_pkey_many_unique_columns() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a PRIMARY KEY, b UNIQUE, c, d, UNIQUE(c, d));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let auto_indices = [
        ("sqlite_autoindex_t1_1".to_string(), 2),
        ("sqlite_autoindex_t1_2".to_string(), 3),
        ("sqlite_autoindex_t1_3".to_string(), 4),
    ];
    let indices = vec![
        Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            1,
            None,
            &[],
            None,
        )?,
        Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_2".to_string(), 3),
            vec![(1, SortOrder::Asc)],
            None,
            &[],
            None,
        )?,
        Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_3".to_string(), 4),
            vec![(2, SortOrder::Asc), (3, SortOrder::Asc)],
            None,
            &[],
            None,
        )?,
    ];

    assert!(indices.len() == auto_indices.len());

    for (pos, index) in indices.iter().enumerate() {
        let (index_name, root_page) = &auto_indices[pos];
        assert_eq!(index.name, *index_name);
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, *root_page);
        assert!(index.unique);

        if pos == 0 {
            assert_eq!(index.columns.len(), 1);
            assert_eq!(index.columns[0].name, "a");
        } else if pos == 1 {
            assert_eq!(index.columns.len(), 1);
            assert_eq!(index.columns[0].name, "b");
        } else if pos == 2 {
            assert_eq!(index.columns.len(), 2);
            assert_eq!(index.columns[0].name, "c");
            assert_eq!(index.columns[1].name, "d");
        }

        assert!(matches!(index.columns[0].order, SortOrder::Asc));
    }

    Ok(())
}

#[test]
fn test_automatic_index_unique_set_dedup() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a, b, UNIQUE(a, b), UNIQUE(a, b));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let index = Index::automatic_from_unique(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        vec![(0, SortOrder::Asc), (1, SortOrder::Asc)],
        None,
        &[],
        None,
    )?;

    assert_eq!(index.name, "sqlite_autoindex_t1_1");
    assert_eq!(index.table_name, "t1");
    assert_eq!(index.root_page, 2);
    assert!(index.unique);
    assert_eq!(index.columns.len(), 2);
    assert_eq!(index.columns[0].name, "a");
    assert!(matches!(index.columns[0].order, SortOrder::Asc));
    assert_eq!(index.columns[1].name, "b");
    assert!(matches!(index.columns[1].order, SortOrder::Asc));

    Ok(())
}

#[test]
fn test_automatic_index_primary_key_is_unique() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a primary key unique);"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let index = Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        1,
        None,
        &[],
        None,
    )?;

    assert_eq!(index.name, "sqlite_autoindex_t1_1");
    assert_eq!(index.table_name, "t1");
    assert_eq!(index.root_page, 2);
    assert!(index.unique);
    assert_eq!(index.columns.len(), 1);
    assert_eq!(index.columns[0].name, "a");
    assert!(matches!(index.columns[0].order, SortOrder::Asc));

    Ok(())
}

#[test]
fn test_automatic_index_primary_key_is_unique_and_composite() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a, b, PRIMARY KEY(a, b), UNIQUE(a, b));"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let index = Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        2,
        None,
        &[],
        None,
    )?;

    assert_eq!(index.name, "sqlite_autoindex_t1_1");
    assert_eq!(index.table_name, "t1");
    assert_eq!(index.root_page, 2);
    assert!(index.unique);
    assert_eq!(index.columns.len(), 2);
    assert_eq!(index.columns[0].name, "a");
    assert_eq!(index.columns[1].name, "b");
    assert!(matches!(index.columns[0].order, SortOrder::Asc));

    Ok(())
}

#[test]
fn test_strict_table_to_sql() -> Result<()> {
    let sql = r#"CREATE TABLE test_strict (id INTEGER, name TEXT) STRICT"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;

    // Verify the table is marked as strict
    assert!(table.is_strict);

    // Verify that to_sql() includes the STRICT keyword
    let reconstructed_sql = table.to_sql();
    assert!(
        reconstructed_sql.contains("STRICT"),
        "Reconstructed SQL should contain STRICT keyword: {reconstructed_sql}"
    );
    assert_eq!(
        reconstructed_sql,
        "CREATE TABLE test_strict (id INTEGER, name TEXT) STRICT"
    );

    Ok(())
}

#[test]
fn test_non_strict_table_to_sql() -> Result<()> {
    let sql = r#"CREATE TABLE test_normal (id INTEGER, name TEXT)"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;

    // Verify the table is NOT marked as strict
    assert!(!table.is_strict);

    // Verify that to_sql() does NOT include the STRICT keyword
    let reconstructed_sql = table.to_sql();
    assert!(
        !reconstructed_sql.contains("STRICT"),
        "Non-strict table SQL should not contain STRICT keyword: {reconstructed_sql}"
    );
    assert_eq!(
        reconstructed_sql,
        "CREATE TABLE test_normal (id INTEGER, name TEXT)"
    );

    Ok(())
}

#[test]
fn test_autoincrement_preserved_in_to_sql() -> Result<()> {
    let sql = r#"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, doomed INT, v TEXT)"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;

    assert!(table.has_autoincrement);
    assert_eq!(
        table.to_sql(),
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, doomed INT, v TEXT)"
    );

    Ok(())
}

#[test]
fn test_without_rowid_preserved_in_sql() -> Result<()> {
    let sql = r#"CREATE TABLE t(code TEXT PRIMARY KEY, val TEXT) WITHOUT ROWID"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    assert!(table.get_column("code").unwrap().1.notnull());
    assert_eq!(
        table.to_sql(),
        "CREATE TABLE t (code TEXT PRIMARY KEY, val TEXT) WITHOUT ROWID"
    );
    Ok(())
}

#[test]
fn test_strict_without_rowid_preserved_in_sql() -> Result<()> {
    let sql = r#"CREATE TABLE t(code TEXT PRIMARY KEY, val TEXT) STRICT, WITHOUT ROWID"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    assert!(table.get_column("code").unwrap().1.notnull());
    assert_eq!(
        table.to_sql(),
        "CREATE TABLE t (code TEXT PRIMARY KEY, val TEXT) STRICT, WITHOUT ROWID"
    );
    Ok(())
}

#[test]
fn test_automatic_index_unique_and_a_pk() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a NUMERIC UNIQUE UNIQUE,  b TEXT PRIMARY KEY)"#;
    let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false)?;
    let mut indexes = vec![
        Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            vec![(0, SortOrder::Asc)],
            None,
            &[],
            None,
        )?,
        Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_2".to_string(), 3),
            1,
            None,
            &[],
            None,
        )?,
    ];

    assert!(indexes.len() == 2);
    let index = indexes.pop().unwrap();
    assert_eq!(index.name, "sqlite_autoindex_t1_2");
    assert_eq!(index.table_name, "t1");
    assert_eq!(index.root_page, 3);
    assert!(index.unique);
    assert_eq!(index.columns.len(), 1);
    assert_eq!(index.columns[0].name, "b");
    assert!(matches!(index.columns[0].order, SortOrder::Asc));

    let index = indexes.pop().unwrap();
    assert_eq!(index.name, "sqlite_autoindex_t1_1");
    assert_eq!(index.table_name, "t1");
    assert_eq!(index.root_page, 2);
    assert!(index.unique);
    assert_eq!(index.columns.len(), 1);
    assert_eq!(index.columns[0].name, "a");
    assert!(matches!(index.columns[0].order, SortOrder::Asc));

    Ok(())
}

#[test]
fn gencol_graph_cycle_rejected() {
    // Two-cycle: a := b, b := a. Must be rejected at CREATE TABLE time by Kahn's.
    let err = turso_core::schema::BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(stored, a AS (b) VIRTUAL, b AS (a) VIRTUAL)",
        0,
        false,
    )
    .expect_err("cycle must be rejected");
    assert!(
        err.to_string().contains("circular dependency")
            || err.to_string().contains("cannot reference itself"),
        "unexpected error: {err}"
    );
}

#[test]
fn gencol_graph_three_cycle_rejected() {
    // Three-cycle: a := b, b := c, c := a.
    let err = turso_core::schema::BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(stored, a AS (b) VIRTUAL, b AS (c) VIRTUAL, c AS (a) VIRTUAL)",
        0,
        false,
    )
    .expect_err("cycle must be rejected");
    assert!(err.to_string().contains("circular dependency"));
}

#[test]
fn gencol_graph_self_reference_rejected() {
    let err = turso_core::schema::BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b AS (b) VIRTUAL)",
        0,
        false,
    )
    .expect_err("self-reference must be rejected");
    assert!(err.to_string().contains("cannot reference itself"));
}

#[test]
fn test_named_unique_constraint_stores_constraint_name() {
    let sql = r#"CREATE TABLE t_u (id INT, code TEXT CONSTRAINT "T_U Code UQ" UNIQUE);"#;
    let table =
        turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, sql, 0, false).unwrap();
    let unique_set = table
        .unique_sets
        .iter()
        .find(|us| !us.is_primary_key)
        .unwrap();
    assert_eq!(
        unique_set.constraint_name,
        Some("T_U Code UQ".to_string()),
        "named UNIQUE constraint must store the constraint name"
    );

    let index = Index::automatic_from_unique(
        &table,
        ("sqlite_autoindex_t_u_1".to_string(), 2),
        vec![(1, SortOrder::Asc)],
        None,
        &unique_set.columns,
        unique_set.constraint_name.clone(),
    )
    .unwrap();
    assert_eq!(
        index.constraint_name,
        Some("T_U Code UQ".to_string()),
        "index built from named UNIQUE constraint must carry the constraint name"
    );
}

#[test]
fn named_primary_and_foreign_keys_survive_sql_reconstruction() {
    for original_sql in [
        "CREATE TABLE child (id INTEGER CONSTRAINT \"Custom PK\" PRIMARY KEY, pid INTEGER CONSTRAINT \"Custom FK\" REFERENCES parent(id))",
        "CREATE TABLE child (id TEXT, pid INTEGER, CONSTRAINT \"Custom PK\" PRIMARY KEY (id, pid), CONSTRAINT \"Custom FK\" FOREIGN KEY (pid) REFERENCES parent(id))",
    ] {
        let table = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, original_sql, 1, false).unwrap();
        let sql = table.to_sql();
        let restored = turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, &sql, 1, false).unwrap();
        assert_eq!(restored.primary_key_constraint_name.as_deref(), Some("Custom PK"), "{sql}");
        assert_eq!(restored.foreign_keys[0].constraint_name.as_deref(), Some("Custom FK"), "{sql}");
    }
}

#[test]
fn table_level_pk_and_unique_order_survives_to_sql() {
    for (original_sql, expected_sql) in [
        (
            "CREATE TABLE t (a TEXT, b TEXT, UNIQUE (a), PRIMARY KEY (b))",
            "CREATE TABLE t (a TEXT, b TEXT, UNIQUE (a), PRIMARY KEY (b))",
        ),
        (
            "CREATE TABLE u (a TEXT, b TEXT, PRIMARY KEY (b), UNIQUE (a))",
            "CREATE TABLE u (a TEXT, b TEXT, PRIMARY KEY (b), UNIQUE (a))",
        ),
        (
            "CREATE TABLE v (a TEXT, b TEXT, x TEXT, UNIQUE (a), PRIMARY KEY (b, x))",
            "CREATE TABLE v (a TEXT, b TEXT, x TEXT, UNIQUE (a), PRIMARY KEY (b, x))",
        ),
        (
            "CREATE TABLE w (a TEXT UNIQUE, b TEXT, PRIMARY KEY (b))",
            "CREATE TABLE w (a TEXT UNIQUE, b TEXT, PRIMARY KEY (b))",
        ),
    ] {
        let table =
            turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, original_sql, 2, false)
                .unwrap();
        let sql = table.to_sql();
        assert_eq!(sql, expected_sql);
        let restored =
            turso_core::schema::BTreeTable::from_stored_sql(&SqliteDialect, &sql, 2, false)
                .unwrap();
        assert_eq!(restored.unique_sets, table.unique_sets, "{sql}");
    }
}
