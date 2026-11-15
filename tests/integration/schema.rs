use rustc_hash::FxHashMap as HashMap;
use turso_core::namespace::NamespaceId;
use turso_core::schema::*;
use turso_core::Result;
use turso_core::SymbolTable;
use turso_parser::ast::Cmd;
use turso_parser::ast::CreateTableBody;
use turso_parser::ast::Stmt;
use turso_sqlite::SqliteDialect;

#[test]
fn a_new_schema_has_only_the_root_namespace_with_the_schema_table() {
    let schema = Schema::with_options(true, &SqliteDialect).unwrap();
    let ids: std::vec::Vec<NamespaceId> = schema.namespace_ids().collect();
    assert_eq!(ids, vec![NamespaceId::ROOT]);
    assert!(schema
        .get_table(NamespaceId::ROOT, SCHEMA_TABLE_NAME)
        .is_some());
}

use turso_core::alloc::vec;

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
            turso_core::dialect::engine::parse_statement(sql)?
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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();

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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();

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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();

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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();

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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();

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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
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
        .populate_indices(&SymbolTable::default(), vec![], automatic_indices, false)
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
