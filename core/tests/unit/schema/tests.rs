use super::*;
use crate::alloc::vec;

#[test]
pub fn test_has_rowid_true() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
    let table = BTreeTable::from_sql(sql, 0)?;
    assert!(table.has_rowid, "has_rowid should be set to true");
    Ok(())
}

#[test]
pub fn test_has_rowid_false() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
    let table = BTreeTable::from_sql(sql, 0)?;
    assert!(!table.has_rowid, "has_rowid should be set to false");
    Ok(())
}

#[test]
pub fn test_column_default_collation_is_effective_binary() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a TEXT);"#;
    let table = BTreeTable::from_sql(sql, 0)?;
    let column = table.get_column("a").unwrap().1;
    assert_eq!(column.collation(), CollationSeq::Binary);
    assert_eq!(column.collation_opt(), None);
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_single_text() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a TEXT PRIMARY KEY, b TEXT);"#;
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("table \"t1\" has more than one primary key"))
    );
    Ok(())
}

#[test]
pub fn test_column_is_rowid_alias_separate_composite_primary_key_definition() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("table \"t1\" has more than one primary key"))
    );
    Ok(())
}

#[test]
pub fn test_conflicting_on_conflict_unique_rejected() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a) ON CONFLICT IGNORE);"#;
    let table = BTreeTable::from_sql(sql, 0);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("conflicting ON CONFLICT clauses"))
    );
    Ok(())
}

#[test]
pub fn test_conflicting_on_conflict_composite_unique_rejected() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a, b, UNIQUE(a, b) ON CONFLICT FAIL, UNIQUE(a, b) ON CONFLICT IGNORE);"#;
    let table = BTreeTable::from_sql(sql, 0);
    let error = table.unwrap_err();
    assert!(
        matches!(error, LimboError::ParseError(e) if e.contains("conflicting ON CONFLICT clauses"))
    );
    Ok(())
}

#[test]
pub fn test_same_on_conflict_unique_allowed() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a) ON CONFLICT FAIL);"#;
    assert!(BTreeTable::from_sql(sql, 0).is_ok());
    Ok(())
}

#[test]
pub fn test_one_on_conflict_unique_allowed() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a));"#;
    assert!(BTreeTable::from_sql(sql, 0).is_ok());
    Ok(())
}

#[test]
pub fn test_primary_key_separate_single() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a desc));"#;
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
    let column = table.get_column("a").unwrap().1;
    let default = column.default.clone().unwrap();
    assert_eq!(default.to_string(), "23");
    Ok(())
}

#[test]
pub fn test_col_notnull() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER NOT NULL);"#;
    let table = BTreeTable::from_sql(sql, 0)?;
    let column = table.get_column("a").unwrap().1;
    assert!(column.notnull());
    Ok(())
}

#[test]
pub fn test_col_notnull_negative() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER);"#;
    let table = BTreeTable::from_sql(sql, 0)?;
    let column = table.get_column("a").unwrap().1;
    assert!(!column.notnull());
    Ok(())
}

#[test]
pub fn test_col_type_string_integer() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a InTeGeR);"#;
    let table = BTreeTable::from_sql(sql, 0)?;
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
        let reconstructed = BTreeTable::from_sql(sql, 0)?.to_sql();

        assert!(
            reconstructed.contains(comment),
            "reconstructed SQL: {reconstructed}"
        );
        BTreeTable::from_sql(&reconstructed, 0)?;
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
        let actual = BTreeTable::from_sql(&sql, 0)?.to_sql();
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
        let actual = BTreeTable::from_sql(input_sql, 0)?.to_sql();
        assert_eq!(actual, expected_sql);
    }

    Ok(())
}

#[test]
#[should_panic]
fn test_automatic_index_single_column() {
    // Without composite primary keys, we should not have an automatic index on a primary key that is a rowid alias
    let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
    let table = BTreeTable::from_sql(sql, 0).unwrap();
    let _index = Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        1,
        None,
        &[],
    )
    .unwrap();
}

#[test]
fn test_automatic_index_composite_key() -> Result<()> {
    let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
    let table = BTreeTable::from_sql(sql, 0)?;
    let index = Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        2,
        None,
        &[],
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
    let table = BTreeTable::from_sql(sql, 0).unwrap();
    Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        1,
        None,
        &[],
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
        has_rowid: true,
        is_strict: false,
        has_autoincrement: false,
        primary_key_columns: vec![("nonexistent".to_string(), SortOrder::Asc)],
        columns,
        unique_sets: vec![],
        foreign_keys: vec![],
        check_constraints: vec![],
        rowid_alias_conflict_clause: None,
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
    );
    assert!(result.is_err());
}

#[test]
fn test_automatic_index_unique_column() -> Result<()> {
    let sql = r#"CREATE table t1 (x INTEGER, y INTEGER UNIQUE);"#;
    let table = BTreeTable::from_sql(sql, 0)?;
    let index = Index::automatic_from_unique(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        vec![(1, SortOrder::Asc)],
        None,
        &[],
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
    let table = BTreeTable::from_sql(sql, 0)?;
    let indices = [
        Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            1,
            None,
            &[],
        )?,
        Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_2".to_string(), 3),
            vec![(1, SortOrder::Asc)],
            None,
            &[],
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
        )?,
        Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_2".to_string(), 3),
            vec![(1, SortOrder::Asc)],
            None,
            &[],
        )?,
        Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_3".to_string(), 4),
            vec![(2, SortOrder::Asc), (3, SortOrder::Asc)],
            None,
            &[],
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
    let table = BTreeTable::from_sql(sql, 0)?;
    let index = Index::automatic_from_unique(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        vec![(0, SortOrder::Asc), (1, SortOrder::Asc)],
        None,
        &[],
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
    let table = BTreeTable::from_sql(sql, 0)?;
    let index = Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        1,
        None,
        &[],
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
    let table = BTreeTable::from_sql(sql, 0)?;
    let index = Index::automatic_from_primary_key(
        &table,
        ("sqlite_autoindex_t1_1".to_string(), 2),
        2,
        None,
        &[],
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
    let table = BTreeTable::from_sql(sql, 0)?;

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
    let table = BTreeTable::from_sql(sql, 0)?;

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
    let table = BTreeTable::from_sql(sql, 0)?;

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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
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
    let table = BTreeTable::from_sql(sql, 0)?;
    let mut indexes = vec![
        Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            vec![(0, SortOrder::Asc)],
            None,
            &[],
        )?,
        Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_2".to_string(), 3),
            1,
            None,
            &[],
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
fn test_schema_loading_rejects_gencol_without_flag() {
    let mut schema = Schema::new();
    schema.generated_columns_enabled = false;

    let result = schema.handle_schema_row(
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
        &crate::dialect::SqliteDialect,
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("generated columns"));
}

#[test]
fn test_schema_row_vtab_with_nonzero_root_page_is_corrupt() {
    let mut schema = Schema::new();

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
        &crate::dialect::SqliteDialect,
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("root_page must be 0 for virtual table v1"));
}

#[test]
fn test_schema_row_with_zero_root_page_takes_virtual_table_path() {
    let mut schema = Schema::new();

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
        &crate::dialect::SqliteDialect,
    );
    assert!(result.is_err());
    assert!(schema.get_table("v1").is_none());
}

#[test]
fn test_schema_row_with_zero_root_page_rejects_malformed_sql() {
    let mut schema = Schema::new();

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
        &crate::dialect::SqliteDialect,
    );
    assert!(result.is_err());
    assert!(schema.get_table("v1").is_none());
}

#[test]
fn test_schema_row_table_with_virtual_table_substring_is_btree() {
    let mut schema = Schema::new();

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
            &crate::dialect::SqliteDialect,
        )
        .unwrap();
    let table = schema.get_btree_table("t1").unwrap();
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
    let t = BTreeTable::from_sql("CREATE TABLE t(a, b)", 0)?;
    assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0]);
    assert_eq!(indices(&t.columns_affected_by_update([0, 1])?), vec![0, 1]);
    assert_eq!(stored(&t.dependencies_of_columns([0])?), vec![0]);
    assert_eq!(stored(&t.dependencies_of_columns([])?), Vec::<usize>::new());
    Ok(())
}

#[test]
fn gencol_graph_linear_chain() -> Result<()> {
    let t = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (b) VIRTUAL)", 0)?;
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
    let t = BTreeTable::from_sql(
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
    let t = BTreeTable::from_sql("CREATE TABLE t(a, b, c AS (a + b) VIRTUAL)", 0)?;
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
    let t = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
    assert!(t.columns_affected_by_update(std::iter::empty())?.is_empty());
    assert!(t.dependencies_of_columns(std::iter::empty())?.is_empty());
    Ok(())
}

#[test]
fn gencol_graph_disjoint_components() -> Result<()> {
    let t = BTreeTable::from_sql(
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
    let t = BTreeTable::from_sql(&sql, 0)?;
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
    let t = BTreeTable::from_sql(&sql, 0)?;
    assert_eq!(t.columns_affected_by_update([0])?.count(), 500);
    assert_eq!(stored(&t.dependencies_of_columns([499])?), vec![0]);
    Ok(())
}

#[test]
fn gencol_graph_rowid_sentinel_passthrough() -> Result<()> {
    let t = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
    let affected = t.columns_affected_by_update([ROWID_SENTINEL])?;
    // ROWID_SENTINEL is preserved in the mask flag but does not propagate through the graph
    // (no generated column can depend on ROWID_SENTINEL directly).
    assert!(affected.get(ROWID_SENTINEL));
    assert_eq!(affected.count(), 1);
    Ok(())
}

#[test]
fn gencol_graph_transpose_duality() -> Result<()> {
    let t = BTreeTable::from_sql(
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
    let t = BTreeTable::from_sql(
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
    let t = BTreeTable::from_sql(
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
    let err = BTreeTable::from_sql(
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
    let err = BTreeTable::from_sql(
        "CREATE TABLE t(stored, a AS (b) VIRTUAL, b AS (c) VIRTUAL, c AS (a) VIRTUAL)",
        0,
    )
    .expect_err("cycle must be rejected");
    assert!(err.to_string().contains("circular dependency"));
}

#[test]
fn gencol_graph_self_reference_rejected() {
    let err = BTreeTable::from_sql("CREATE TABLE t(a, b AS (b) VIRTUAL)", 0)
        .expect_err("self-reference must be rejected");
    assert!(err.to_string().contains("cannot reference itself"));
}

#[test]
#[allow(clippy::redundant_clone)]
fn gencol_graph_clone_invalidates_cache() -> Result<()> {
    // After cloning a BTreeTable, the cache is fresh. Mutating columns on
    // the clone via `columns_mut()` keeps it fresh; `prepare_generated_columns`
    // rebuilds correctly.
    let original = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
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
    let mut t = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
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
    let mut schema = Schema::new();
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
        !schema.sequences.contains_key("broken_seq"),
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
