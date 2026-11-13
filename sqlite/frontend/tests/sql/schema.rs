use rustc_hash::FxHashMap as HashMap;
#[allow(unused_imports)]
use turso_core::alloc::*;
use turso_core::incremental::view::dbsp_version_marker_table_name;
use turso_core::incremental::view::DBSP_CIRCUIT_VERSION;
use turso_core::namespace::NamespaceId;
use turso_core::schema::*;
use turso_core::security::roles::SessionRole;
#[allow(unused_imports)]
use turso_core::storage::btree::CursorTrait;
use turso_core::sync::Arc;
use turso_core::translate::collate::CollationSeq;
use turso_core::translate::collate::TextCollation;
use turso_core::translate::plan::ColumnMask;
use turso_core::vdbe::affinity::Affinity;
use turso_core::LimboError;
use turso_core::Result;
use turso_core::SymbolTable;
use turso_parser::ast::Cmd;
use turso_parser::ast::CreateTableBody;
use turso_parser::ast::ResolveType;
use turso_parser::ast::SortOrder;
use turso_parser::ast::Stmt;
use turso_sqlite::SqliteDialect;

fn table(sql: &str, root_page: i64) -> Arc<BTreeTable> {
    Arc::new(crate::table_from_sql(sql, root_page).unwrap())
}

#[test]
fn a_new_schema_has_only_the_root_namespace_with_the_schema_table() {
    let schema = Schema::with_options(true, &SqliteDialect).unwrap();
    let ids: std::vec::Vec<NamespaceId> = schema.namespace_ids().collect();
    assert_eq!(ids, vec![NamespaceId::ROOT]);
    assert!(schema
        .get_table(NamespaceId::ROOT, SCHEMA_TABLE_NAME)
        .is_some());
}

#[test]
fn builtin_types_read_as_the_kind_their_decode_gives() {
    use turso_core::{
        translate::expr::expr_type::value_type, ConnectOpts, Database, DatabaseOpts, MemoryIO,
        OpenFlags, IO, MAIN_DB_ID,
    };
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new().with_custom_types(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let syms = conn.syms.read();
    conn.with_schema(MAIN_DB_ID, |schema| {
        assert!(schema.type_registry.values().all(|t| t.is_builtin));
        for (name, type_def) in &schema.type_registry {
            assert_eq!(
                Some(type_def.value_type()),
                value_type(type_def, schema, &syms),
                "built-in type {name}"
            );
        }
    });
}

#[test]
#[should_panic(expected = "kinds of type price were read before they were worked out")]
fn reading_the_kind_of_a_type_not_yet_worked_out_panics() {
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    schema
        .insert_type_from_sql(
            &SqliteDialect,
            "CREATE TYPE price(value any) BASE text ENCODE value DECODE CAST(value AS real)",
        )
        .unwrap();
    schema.get_type_def_unchecked("price").unwrap().value_type();
}

#[test]
fn a_namespace_holds_only_its_own_objects() {
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
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
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
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
fn persisted_materialized_views_are_loaded_in_dependency_order() {
    let mut views = HashMap::default();
    views.insert(
        "third".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW third AS SELECT * FROM second".to_string(),
            5,
        ),
    );
    views.insert(
        "first".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW first AS SELECT * FROM base".to_string(),
            3,
        ),
    );
    views.insert(
        "second".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW second AS SELECT * FROM first".to_string(),
            4,
        ),
    );

    let ordered = Schema::with_options(true, &SqliteDialect)
        .unwrap()
        .order_persisted_materialized_views(&SqliteDialect, views)
        .unwrap();
    assert_eq!(
        ordered
            .into_iter()
            .map(|(name, _)| name)
            .collect::<std::vec::Vec<_>>(),
        vec!["first", "second", "third"]
    );
}

#[test]
fn persisted_materialized_view_cycle_is_corrupt() {
    let mut views = HashMap::default();
    views.insert(
        "left_view".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW left_view AS SELECT * FROM right_view".to_string(),
            3,
        ),
    );
    views.insert(
        "right_view".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW right_view AS SELECT * FROM left_view".to_string(),
            4,
        ),
    );

    let error = Schema::with_options(true, &SqliteDialect)
        .unwrap()
        .order_persisted_materialized_views(&SqliteDialect, views)
        .unwrap_err();
    assert!(matches!(error, LimboError::Corrupt(_)));
}

#[test]
fn incompatible_materialized_view_can_be_removed() {
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    let root = schema.namespace_mut(NamespaceId::ROOT);
    root.incompatible_views.insert("old_view".to_string());
    root.incompatible_materialized_view_roots
        .insert("old_view".to_string(), 42);
    root.table_to_materialized_views
        .insert("base".to_string(), vec!["old_view".to_string()]);

    assert!(schema.materialized_view_exists(NamespaceId::ROOT, "old_view"));
    assert_eq!(
        schema.incompatible_materialized_view_root(NamespaceId::ROOT, "old_view"),
        Some(42)
    );
    schema.remove_view(NamespaceId::ROOT, "old_view").unwrap();
    assert!(!schema.materialized_view_exists(NamespaceId::ROOT, "old_view"));
    assert!(!schema.is_incompatible_view(NamespaceId::ROOT, "old_view"));
    assert!(schema
        .get_dependent_materialized_views(NamespaceId::ROOT, "base")
        .is_empty());
}

#[test]
fn stateless_materialized_view_requires_current_version_marker() -> Result<()> {
    fn add_marker(schema: &mut Schema, name: &str, root_page: i64) -> Result<()> {
        let sql = format!(
            "CREATE TABLE {} (version INTEGER)",
            turso_core::util::quote_identifier(name)
        );
        schema.add_btree_table(
            NamespaceId::ROOT,
            Arc::new(crate::table_from_sql(&sql, root_page)?),
        )
    }

    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    assert!(!schema.has_compatible_materialized_view_storage(NamespaceId::ROOT, "stateless"));

    let old_version = DBSP_CIRCUIT_VERSION - 1;
    let other_view_marker = format!("{DBSP_METADATA_TABLE_PREFIX}{old_version}_stateless__n1");
    add_marker(&mut schema, &other_view_marker, 2)?;
    assert!(
        !schema.dbsp_storage_version_mismatch(NamespaceId::ROOT, "stateless"),
        "a similarly prefixed view must not own this view's version marker"
    );

    let current_marker = dbsp_version_marker_table_name("stateless");
    add_marker(&mut schema, &current_marker, 3)?;
    assert!(schema.has_compatible_materialized_view_storage(NamespaceId::ROOT, "stateless"));

    let old_marker = format!("{DBSP_METADATA_TABLE_PREFIX}{old_version}_stateless");
    add_marker(&mut schema, &old_marker, 4)?;
    assert!(!schema.has_compatible_materialized_view_storage(NamespaceId::ROOT, "stateless"));
    Ok(())
}

use turso_core::alloc::vec;

#[test]
fn test_column_definition_flag_conversion() {
    for bits in 0..=u8::MAX {
        let Some(flags) = FromDefinitionFlags::from_bits(bits) else {
            continue;
        };
        let mut expected = ColDefFlags::empty();
        for (name, _) in flags.iter_names() {
            expected.insert(ColDefFlags::from_name(name).expect("column flag must exist"));
        }
        assert_eq!(ColDefFlags::from_def_flags(&flags), expected, "{flags:?}");
    }
}

#[test]
fn test_from_definition_flags_with() {
    let flags = FromDefinitionFlags::empty().with(FromDefinitionFlags::InStrictTable, true);
    assert_eq!(flags, FromDefinitionFlags::InStrictTable);
    assert_eq!(flags.with(FromDefinitionFlags::InStrictTable, true), flags);
    assert_eq!(flags.with(FromDefinitionFlags::empty(), false), flags);
    assert_eq!(
        flags.with(FromDefinitionFlags::InStrictTable, false),
        FromDefinitionFlags::empty()
    );
    assert_eq!(
        FromDefinitionFlags::empty()
            .with(FromDefinitionFlags::InStrictTable, true)
            .with(FromDefinitionFlags::InStrictTable, false)
            .with(FromDefinitionFlags::InStrictTable, true),
        FromDefinitionFlags::InStrictTable
    );
}

#[test]
fn test_coldef_flags_with() {
    let flags = ColDefFlags::PrimaryKey | ColDefFlags::Hidden;
    assert_eq!(
        flags.with(ColDefFlags::NotNull, true),
        ColDefFlags::PrimaryKey | ColDefFlags::Hidden | ColDefFlags::NotNull
    );
    assert_eq!(
        flags.with(ColDefFlags::PrimaryKey, false),
        ColDefFlags::Hidden
    );
    assert_eq!(flags.with(ColDefFlags::NotNull, false), flags);
    assert_eq!(flags.with(ColDefFlags::empty(), false), flags);
    assert_eq!(
        ColDefFlags::empty()
            .with(ColDefFlags::PrimaryKey | ColDefFlags::RowIdAlias, true)
            .with(ColDefFlags::PrimaryKey, false)
            .with(ColDefFlags::NotNull, true),
        ColDefFlags::RowIdAlias | ColDefFlags::NotNull
    );
}

#[test]
fn test_column_definition_flags_preserve_properties() {
    for bits in 0..=u8::MAX {
        let Some(flags) = ColDefFlags::from_bits(bits) else {
            continue;
        };
        let column = Column::new(
            Some("a".to_string()),
            "ANY".to_string(),
            None,
            None,
            Type::Numeric,
            Some(TextCollation::NoCase),
            ColDef {
                flags,
                notnull_conflict_clause: Some(ResolveType::Ignore),
            },
        );
        for (actual, flag) in [
            (column.primary_key(), ColDefFlags::PrimaryKey),
            (column.is_rowid_alias(), ColDefFlags::RowIdAlias),
            (column.explicit_notnull(), ColDefFlags::ExplicitNotNull),
            (column.unique(), ColDefFlags::Unique),
            (column.hidden(), ColDefFlags::Hidden),
        ] {
            assert_eq!(actual, flags.contains(flag), "{flags:?}: {flag:?}");
        }
        assert_eq!(
            column.notnull(),
            flags.intersects(ColDefFlags::NotNull | ColDefFlags::RowIdAlias),
            "{flags:?}"
        );
        assert_eq!(column.ty(), Type::Numeric, "{flags:?}");
        assert_eq!(
            column.text_collation(),
            Some(TextCollation::NoCase),
            "{flags:?}"
        );
        assert_eq!(
            column.notnull_conflict_clause,
            Some(ResolveType::Ignore),
            "{flags:?}"
        );
        let affinity = if flags.contains(ColDefFlags::InStrictTable) {
            Affinity::Blob
        } else {
            Affinity::Numeric
        };
        assert_eq!(column.affinity(), affinity, "{flags:?}");
    }
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
pub fn test_has_rowid_true() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
    let table = crate::table_from_sql(sql, 0)?;
    assert!(table.has_rowid, "has_rowid should be set to true");
    Ok(())
}

#[test]
pub fn test_has_rowid_false() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
    let table = crate::table_from_sql(sql, 0)?;
    assert!(!table.has_rowid, "has_rowid should be set to false");
    Ok(())
}

#[test]
pub fn test_column_default_collation_is_effective_binary() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a TEXT);"#;
    let table = crate::table_from_sql(sql, 0)?;
    let column = table.get_column("a").unwrap().1;
    assert_eq!(column.collation(), CollationSeq::BINARY);
    assert_eq!(column.text_collation(), None);
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_single_text() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a TEXT PRIMARY KEY, b TEXT);"#;
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("table \"t1\" has more than one primary key"))
    );
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_separate_composite_primary_key_definition() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("table \"t1\" has more than one primary key"))
    );
    Ok(())
}

#[test]
pub fn test_conflicting_on_conflict_unique_rejected() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a) ON CONFLICT IGNORE);"#;
    let table = crate::table_from_sql(sql, 0);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("conflicting ON CONFLICT clauses"))
    );
    Ok(())
}

#[test]
pub fn test_conflicting_on_conflict_composite_unique_rejected() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a, b, UNIQUE(a, b) ON CONFLICT FAIL, UNIQUE(a, b) ON CONFLICT IGNORE);"#;
    let table = crate::table_from_sql(sql, 0);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("conflicting ON CONFLICT clauses"))
    );
    Ok(())
}

#[test]
pub fn test_same_on_conflict_unique_allowed() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a) ON CONFLICT FAIL);"#;
    assert!(crate::table_from_sql(sql, 0).is_ok());
    Ok(())
}

#[test]
pub fn test_one_on_conflict_unique_allowed() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a));"#;
    assert!(crate::table_from_sql(sql, 0).is_ok());
    Ok(())
}

#[test]
pub fn test_primary_key_separate_single() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a desc));"#;
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
    let column = table.get_column("a").unwrap().1;
    let default = column.default.clone().unwrap();
    assert_eq!(default.to_string(), "23");
    Ok(())
}

#[test]
pub fn test_col_notnull() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER NOT NULL);"#;
    let table = crate::table_from_sql(sql, 0)?;
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
        let table = crate::table_from_sql(sql, 2).unwrap();
        let column = table.get_column("id").unwrap().1;
        assert!(column.notnull(), "{sql}");
        assert!(!column.explicit_notnull(), "{sql}");
    }
    let table = crate::table_from_sql("CREATE TABLE t(id INTEGER PRIMARY KEY DESC)", 2).unwrap();
    assert!(!table.get_column("id").unwrap().1.notnull());
}

#[test]
pub fn test_col_notnull_negative() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER);"#;
    let table = crate::table_from_sql(sql, 0)?;
    let column = table.get_column("a").unwrap().1;
    assert!(!column.notnull());
    Ok(())
}

#[test]
pub fn test_col_type_string_integer() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a InTeGeR);"#;
    let table = crate::table_from_sql(sql, 0)?;
    let column = table.get_column("a").unwrap().1;
    assert_eq!(column.ty_str, "InTeGeR");
    Ok(())
}

#[test]
pub fn test_sqlite_schema() -> Result<()> {
    let expected = r#"CREATE TABLE sqlite_schema (type TEXT, name TEXT, tbl_name TEXT, rootpage INT, sql TEXT)"#;
    let actual = sqlite_schema_table()?.to_sql();
    assert_eq!(expected, actual);
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
        let reconstructed = crate::table_from_sql(sql, 0)?.to_sql();

        assert!(
            reconstructed.contains(comment),
            "reconstructed SQL: {reconstructed}"
        );
        crate::table_from_sql(&reconstructed, 0)?;
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
        let actual = crate::table_from_sql(&sql, 0)?.to_sql();
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
        let actual = crate::table_from_sql(input_sql, 0)?.to_sql();
        assert_eq!(actual, expected_sql);
    }

    Ok(())
}

#[test]
#[should_panic]
fn test_automatic_index_single_column() {
    // Without composite primary keys, we should not have an automatic index on a primary key that is a rowid alias
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
    let table = crate::table_from_sql(sql, 0).unwrap();
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0).unwrap();
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
fn test_automatic_index_nonexistent_column() {
    // Create a table with a primary key column that doesn't exist in the table
    let columns = vec![Column::new_default_integer(
        Some("a".to_string()),
        "INT".to_string(),
        None,
    )];
    let logical_to_physical_map = BTreeTable::build_logical_to_physical_map(&columns, &[], true);
    let table = BTreeTable {
        root_page: 0,
        name: "t1".to_string(),
        declared_name: "t1".to_string(),
        has_rowid: true,
        is_strict: false,
        has_autoincrement: false,
        primary_key_columns: vec![("nonexistent".to_string(), SortOrder::Asc)],
        primary_key_is_inline: false,
        columns,
        unique_sets: vec![],
        foreign_keys: vec![],
        check_constraints: vec![],
        rowid_alias_conflict_clause: None,
        primary_key_constraint_name: None,
        has_virtual_columns: false,
        logical_to_physical_map,
        column_dependencies: Default::default(),
    };

    let result = Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        1,
        None,
        &[],
        None,
    );
    assert!(result.is_err());
}

#[test]
fn test_automatic_index_unique_column() -> Result<()> {
    let sql = r#"CREATE table t1 (x INTEGER, y INTEGER UNIQUE);"#;
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;

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
    let table = crate::table_from_sql(sql, 0)?;

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
    let table = crate::table_from_sql(sql, 0)?;

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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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
    let table = crate::table_from_sql(sql, 0)?;
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

fn indices(mask: &ColumnMask) -> Vec<usize> {
    let mut v: Vec<usize> = mask.iter().try_collect().unwrap();
    v.sort_unstable();
    v
}

fn stored(bits: &ColumnMask) -> Vec<usize> {
    let mut v: Vec<usize> = bits.iter().try_collect().unwrap();
    v.sort_unstable();
    v
}

#[test]
fn gencol_graph_no_virtual_columns() -> Result<()> {
    let t = crate::table_from_sql("CREATE TABLE t(a, b)", 0)?;
    assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0]);
    assert_eq!(indices(&t.columns_affected_by_update([0, 1])?), vec![0, 1]);
    assert_eq!(stored(&t.dependencies_of_columns([0])?), vec![0]);
    assert_eq!(stored(&t.dependencies_of_columns([])?), Vec::<usize>::new());
    Ok(())
}

#[test]
fn gencol_graph_linear_chain() -> Result<()> {
    let t = crate::table_from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (b) VIRTUAL)", 0)?;
    // affected-by({a}) = {a, b, c}
    assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0, 1, 2]);
    // affected-by({b}) = {b, c} (b is virtual, but updating it still propagates through dependents)
    assert_eq!(indices(&t.columns_affected_by_update([1])?), vec![1, 2]);
    // deps-of({c}) = {a} (transitive stored deps of virtual c)
    assert_eq!(stored(&t.dependencies_of_columns([2])?), vec![0]);
    // deps-of({b}) = {a}
    assert_eq!(stored(&t.dependencies_of_columns([1])?), vec![0]);
    // deps-of({a}) = {a} (stored target included)
    assert_eq!(stored(&t.dependencies_of_columns([0])?), vec![0]);
    Ok(())
}

#[test]
fn gencol_graph_diamond() -> Result<()> {
    let t = crate::table_from_sql(
        "CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (a) VIRTUAL, d AS (b + c) VIRTUAL)",
        0,
    )?;
    assert_eq!(
        indices(&t.columns_affected_by_update([0])?),
        vec![0, 1, 2, 3]
    );
    assert_eq!(stored(&t.dependencies_of_columns([3])?), vec![0]);
    assert_eq!(stored(&t.dependencies_of_columns([1])?), vec![0]);
    Ok(())
}

#[test]
fn gencol_graph_multiple_stored_roots() -> Result<()> {
    let t = crate::table_from_sql("CREATE TABLE t(a, b, c AS (a + b) VIRTUAL)", 0)?;
    assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0, 2]);
    assert_eq!(indices(&t.columns_affected_by_update([1])?), vec![1, 2]);
    assert_eq!(
        indices(&t.columns_affected_by_update([0, 1])?),
        vec![0, 1, 2]
    );
    assert_eq!(stored(&t.dependencies_of_columns([2])?), vec![0, 1]);
    Ok(())
}

#[test]
fn gencol_graph_empty_input() -> Result<()> {
    let t = crate::table_from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
    assert!(t.columns_affected_by_update(std::iter::empty())?.is_empty());
    assert!(t.dependencies_of_columns(std::iter::empty())?.is_empty());
    Ok(())
}

#[test]
fn gencol_graph_disjoint_components() -> Result<()> {
    let t = crate::table_from_sql(
        "CREATE TABLE t(a, b AS (a) VIRTUAL, c, d AS (c) VIRTUAL)",
        0,
    )?;
    assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0, 1]);
    assert_eq!(indices(&t.columns_affected_by_update([2])?), vec![2, 3]);
    assert_eq!(stored(&t.dependencies_of_columns([1])?), vec![0]);
    assert_eq!(stored(&t.dependencies_of_columns([3])?), vec![2]);
    Ok(())
}

#[test]
fn gencol_graph_deep_chain() -> Result<()> {
    // Build 50-long chain: c0 (stored), c1 := c0, c2 := c1, ... c49 := c48.
    let mut sql = String::from("CREATE TABLE t(c0");
    for i in 1..50 {
        sql.push_str(&format!(", c{i} AS (c{prev}) VIRTUAL", prev = i - 1));
    }
    sql.push(')');
    let t = crate::table_from_sql(&sql, 0)?;
    // affected-by({c0}) = {c0..c49}
    let affected = t.columns_affected_by_update([0])?;
    assert_eq!(affected.count(), 50);
    // deps-of({c49}) = {c0}
    assert_eq!(stored(&t.dependencies_of_columns([49])?), vec![0]);
    Ok(())
}

#[test]
fn gencol_graph_very_deep_chain_no_stack_overflow() -> Result<()> {
    // Validates that the iterative Kahn's + DP don't blow the stack on
    // realistic worst-case generated-column depth.
    let mut sql = String::from("CREATE TABLE t(c0");
    for i in 1..500 {
        sql.push_str(&format!(", c{i} AS (c{prev}) VIRTUAL", prev = i - 1));
    }
    sql.push(')');
    let t = crate::table_from_sql(&sql, 0)?;
    assert_eq!(t.columns_affected_by_update([0])?.count(), 500);
    assert_eq!(stored(&t.dependencies_of_columns([499])?), vec![0]);
    Ok(())
}

#[test]
fn gencol_graph_rowid_sentinel_passthrough() -> Result<()> {
    let t = crate::table_from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
    let affected = t.columns_affected_by_update([ROWID_SENTINEL])?;
    // ROWID_SENTINEL is preserved in the mask flag but does not propagate through the graph
    // (no generated column can depend on ROWID_SENTINEL directly).
    assert!(affected.get(ROWID_SENTINEL));
    assert_eq!(affected.count(), 1);
    Ok(())
}

#[test]
fn gencol_graph_transpose_duality() -> Result<()> {
    let t = crate::table_from_sql(
        "CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (b) VIRTUAL, d AS (a + c) VIRTUAL)",
        0,
    )?;
    let graph = t.column_graph()?;
    // j ∈ dependencies[i] iff i ∈ dependents[j]
    for i in 0..graph.dependencies.len() {
        for j in graph.dependencies[i].iter() {
            assert!(
                graph.dependents[j].get(i),
                "transpose violated: {j} is in dependencies[{i}] but {i} is not in dependents[{j}]"
            );
        }
        for j in graph.dependents[i].iter() {
            assert!(
                graph.dependencies[j].get(i),
                "transpose violated: {j} is in dependents[{i}] but {i} is not in dependencies[{j}]"
            );
        }
    }
    Ok(())
}

#[test]
fn gencol_graph_idempotence() -> Result<()> {
    // affected_by(affected_by(xs)) == affected_by(xs).
    let t = crate::table_from_sql(
        "CREATE TABLE t(a, b, c AS (a) VIRTUAL, d AS (b + c) VIRTUAL)",
        0,
    )?;
    let once = t.columns_affected_by_update([0, 1])?;
    let twice = t.columns_affected_by_update(once.iter())?;
    assert_eq!(indices(&twice), indices(&once));
    Ok(())
}

#[test]
fn gencol_graph_union_monotonicity() -> Result<()> {
    // affected_by(A ∪ B) == affected_by(A) ∪ affected_by(B).
    let t = crate::table_from_sql(
        "CREATE TABLE t(a, b, c AS (a) VIRTUAL, d AS (b) VIRTUAL, e AS (c + d) VIRTUAL)",
        0,
    )?;
    let mut expected = t.columns_affected_by_update([0])?;
    let b_mask = t.columns_affected_by_update([1])?;
    expected.union_with(&b_mask).unwrap();
    let union_mask = t.columns_affected_by_update([0, 1])?;
    assert_eq!(indices(&union_mask), indices(&expected));
    Ok(())
}

#[test]
fn gencol_graph_cycle_rejected() {
    // Two-cycle: a := b, b := a. Must be rejected at CREATE TABLE time by Kahn's.
    let err = crate::table_from_sql(
        "CREATE TABLE t(stored, a AS (b) VIRTUAL, b AS (a) VIRTUAL)",
        0,
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
    let err = crate::table_from_sql(
        "CREATE TABLE t(stored, a AS (b) VIRTUAL, b AS (c) VIRTUAL, c AS (a) VIRTUAL)",
        0,
    )
    .expect_err("cycle must be rejected");
    assert!(err.to_string().contains("circular dependency"));
}

#[test]
fn gencol_graph_self_reference_rejected() {
    let err = crate::table_from_sql("CREATE TABLE t(a, b AS (b) VIRTUAL)", 0)
        .expect_err("self-reference must be rejected");
    assert!(err.to_string().contains("cannot reference itself"));
}

#[test]
#[allow(clippy::redundant_clone)]
fn gencol_graph_clone_invalidates_cache() -> Result<()> {
    // After cloning a BTreeTable, the cache is fresh. Mutating columns on
    // the clone via `columns_mut()` keeps it fresh; `prepare_generated_columns`
    // rebuilds correctly.
    let original = crate::table_from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
    // Force the cache to be populated on the original.
    let _ = original.columns_affected_by_update([0])?;
    assert!(original.peek_column_dependencies().is_some());

    // Clone: ResetOnClone makes the cloned cache empty. We keep a real clone
    // (not a move) because the point of the test is that Clone produces a
    // fresh cache independently from the original.
    let cloned = original.clone();
    assert!(cloned.peek_column_dependencies().is_none());
    // Original's cache is still populated — clone didn't touch it.
    assert!(original.peek_column_dependencies().is_some());

    // The clone still returns correct results — cache rebuilds lazily.
    assert_eq!(
        indices(&cloned.columns_affected_by_update([0])?),
        vec![0, 1]
    );
    assert!(cloned.peek_column_dependencies().is_some());
    Ok(())
}

#[test]
fn gencol_graph_columns_mut_invalidates_cache() -> Result<()> {
    let mut t = crate::table_from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
    // Force the cache to be populated.
    let _ = t.columns_affected_by_update([0])?;
    assert!(t.peek_column_dependencies().is_some());

    // Any access through columns_mut() wipes the cache, even if we don't mutate.
    let _ = t.columns_mut();
    assert!(t.peek_column_dependencies().is_none());
    Ok(())
}

/// `install_sequence_descriptor` must surface an error when the
/// persisted metadata is invalid (e.g. min > max) rather than
/// silently dropping the sequence. The internal backing table is
/// the only persistent record of the sequence; a silent drop would
/// manifest later as a misleading "sequence does not exist" on the
/// next nextval that masks real on-disk corruption.
#[test]
fn install_sequence_descriptor_rejects_invalid_metadata_with_corruption_error() {
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    let bogus = SequenceMetadata {
        // increment of zero is universally invalid; Sequence::new
        // rejects it with a clear error.
        start: 0,
        increment: 0,
        min: 0,
        max: 100,
        cycle: false,
    };
    let result = schema.install_sequence_descriptor("broken_seq", bogus);
    let err = result.expect_err(
        "invalid persisted descriptor must surface as an error, not be silently dropped",
    );
    assert!(
        matches!(err, LimboError::Corrupt(_)),
        "expected Corrupt error for unreadable internal backing table, got: {err:?}",
    );
    assert!(
        schema
            .get_sequence(NamespaceId::ROOT, "broken_seq")
            .is_none(),
        "rejected descriptor must not land in the sequences map",
    );
}

#[test]
fn set_base_affinity_is_consistent_with_accessor() {
    for affinity in [
        Affinity::Blob,
        Affinity::Text,
        Affinity::Numeric,
        Affinity::Integer,
        Affinity::Real,
        Affinity::None,
    ] {
        let mut col = Column::new(
            Some("x".to_string()),
            "BLOB".to_string(),
            None,
            None,
            Type::Blob,
            None,
            ColDef::default(),
        );
        col.override_affinity(affinity);
        assert_eq!(
            col.affinity(),
            affinity,
            "stored {affinity:?} but read back {:?}",
            col.affinity(),
        );
    }
}

#[test]
fn column_metadata_changes_preserve_affinity() {
    for affinity in [
        Affinity::Blob,
        Affinity::Text,
        Affinity::Numeric,
        Affinity::Integer,
        Affinity::Real,
        Affinity::None,
    ] {
        let mut col = Column::new(
            Some("x".to_string()),
            "BLOB".to_string(),
            None,
            None,
            Type::Blob,
            None,
            ColDef::default(),
        );
        col.override_affinity(affinity);
        for enabled in [true, false] {
            col.set_rowid_alias(enabled);
            col.set_notnull(enabled);
            col.set_unique(enabled);
            col.set_hidden(enabled);
            col.set_ty(if enabled { Type::Real } else { Type::Null });
            col.set_collation(enabled.then_some(TextCollation::NoCase));
            col.set_array_dimensions(if enabled { 7 } else { 0 });
            assert_eq!(col.affinity(), affinity);
        }
    }
}

#[test]
fn test_named_unique_constraint_stores_constraint_name() {
    let sql = r#"CREATE TABLE t_u (id INT, code TEXT CONSTRAINT "T_U Code UQ" UNIQUE);"#;
    let table = crate::table_from_sql(sql, 0).unwrap();
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
        let table = crate::table_from_sql(original_sql, 1).unwrap();
        let sql = table.to_sql();
        let restored = crate::table_from_sql(&sql, 1).unwrap();
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
        let table = crate::table_from_sql(original_sql, 2).unwrap();
        let sql = table.to_sql();
        assert_eq!(sql, expected_sql);
        let restored = crate::table_from_sql(&sql, 2).unwrap();
        assert_eq!(restored.unique_sets, table.unique_sets, "{sql}");
    }
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
