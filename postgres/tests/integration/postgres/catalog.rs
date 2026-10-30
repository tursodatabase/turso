use crate::common::TempDatabase;
use turso_core::Value;

#[turso_macros::test(mvcc)]
fn test_pg_object_catalog_column_counts(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for (table, columns) in [
        ("pg_language", 9),
        ("pg_operator", 15),
        ("pg_opclass", 9),
        ("pg_opfamily", 5),
        ("pg_ts_parser", 8),
        ("pg_ts_template", 5),
        ("pg_ts_dict", 6),
        ("pg_ts_config", 5),
        ("pg_foreign_data_wrapper", 7),
        ("pg_foreign_server", 8),
        ("pg_default_acl", 5),
        ("pg_conversion", 8),
        ("pg_range", 7),
        ("pg_event_trigger", 7),
        ("pg_subscription", 17),
        ("pg_largeobject_metadata", 3),
        ("pg_amop", 9),
        ("pg_amproc", 6),
    ] {
        for name in [table.to_owned(), format!("pg_catalog.{table}")] {
            let stmt = conn.prepare(format!("SELECT * FROM {name}")).unwrap();
            assert_eq!(stmt.num_columns(), columns, "{name}");
        }
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_function_support_catalog_column_counts(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for (table, columns) in [("pg_init_privs", 5), ("pg_cast", 6), ("pg_transform", 5)] {
        for name in [table.to_owned(), format!("pg_catalog.{table}")] {
            let stmt = conn.prepare(format!("SELECT * FROM {name}")).unwrap();
            assert_eq!(stmt.num_columns(), columns, "{name}");
        }
    }
}

#[turso_macros::test(mvcc)]
fn test_physical_catalog_tableoid_is_hidden(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE indexed (id INTEGER PRIMARY KEY, value TEXT UNIQUE DEFAULT 'a')")
        .unwrap();
    assert_eq!(
        conn.prepare("SELECT * FROM pg_class")
            .unwrap()
            .num_columns(),
        33
    );
    assert_eq!(
        conn.prepare("SELECT * FROM pg_namespace")
            .unwrap()
            .num_columns(),
        4
    );
    assert_eq!(
        conn.prepare("SELECT * FROM pg_catalog.pg_depend")
            .unwrap()
            .num_columns(),
        7
    );
    assert!(conn.prepare("DELETE FROM pg_depend").is_err());
    let mut tablespaces = conn.prepare("SELECT * FROM pg_tablespace").unwrap();
    assert_eq!(tablespaces.num_columns(), 5);
    assert!(tablespaces.run_collect_rows().unwrap().is_empty());
}

#[turso_macros::test(mvcc)]
fn test_physical_catalog_boolean_metadata(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE indexed (id INTEGER PRIMARY KEY, value TEXT UNIQUE)")
        .unwrap();
    conn.execute("CREATE TABLE plain (value TEXT)").unwrap();
    let mut booleans = conn
        .prepare(
            "SELECT c.relname, c.relhasindex FROM pg_class c
             LEFT JOIN pg_depend d ON (c.relkind = 'S' AND d.classid = 'pg_class'::regclass
                 AND d.objid = c.oid AND d.objsubid = 0
                 AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i'))
             WHERE c.relname IN ('indexed', 'plain') ORDER BY c.relname",
        )
        .unwrap();
    assert_eq!(booleans.get_column_decltype(1).as_deref(), Some("BOOLEAN"));
    assert_eq!(
        booleans.run_collect_rows().unwrap(),
        vec![
            vec![Value::build_text("indexed"), Value::from_i64(1)],
            vec![Value::build_text("plain"), Value::from_i64(0)],
        ]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_extension_has_no_postgres_extensions(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for table in ["pg_extension", "pg_catalog.pg_extension"] {
        let mut stmt = conn
            .prepare(format!(
                "SELECT x.tableoid, x.oid, x.extname, n.nspname, x.extrelocatable,
                        x.extversion, x.extconfig, x.extcondition
                 FROM {table} x JOIN pg_namespace n ON n.oid = x.extnamespace"
            ))
            .unwrap();
        assert_eq!(stmt.num_columns(), 8);
        assert!(stmt.run_collect_rows().unwrap().is_empty());
    }
    let mut stmt = conn.prepare("SELECT * FROM pg_extension").unwrap();
    assert_eq!(stmt.num_columns(), 8);
    assert!(stmt.run_collect_rows().unwrap().is_empty());
    assert!(conn.prepare("DELETE FROM pg_extension").is_err());
}

#[turso_macros::test(mvcc)]
fn test_pg_settings_tracks_connection_and_reused_statement(db: TempDatabase) {
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    let default = Value::build_text("\"$user\", public");
    let mut setting = conn
        .prepare("SELECT setting, source FROM pg_settings WHERE name = 'search_path'")
        .unwrap();
    assert_eq!(
        setting.run_collect_rows().unwrap(),
        vec![vec![default.clone(), Value::build_text("default")]]
    );
    for value in [" \"My,Schema\", PUBLIC ", ""] {
        conn.execute(format!(
            "SELECT set_config('search_path', '{value}', false)"
        ))
        .unwrap();
        setting.reset().unwrap();
        assert_eq!(
            setting.run_collect_rows().unwrap(),
            vec![vec![Value::build_text(value), Value::build_text("session")]]
        );
        assert_eq!(
            other
                .prepare("SELECT setting FROM pg_settings WHERE name = 'search_path'")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![default.clone()]]
        );
    }
    conn.execute("SET search_path TO public").unwrap();
    setting.reset().unwrap();
    assert_eq!(
        setting.run_collect_rows().unwrap(),
        vec![vec![
            Value::build_text("public"),
            Value::build_text("session")
        ]]
    );
    conn.execute("SELECT set_config('search_path', NULL, false)")
        .unwrap();
    setting.reset().unwrap();
    assert_eq!(
        setting.run_collect_rows().unwrap(),
        vec![vec![default, Value::build_text("default")]]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_settings_omits_unsupported_restrictions(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for table in ["pg_settings", "pg_catalog.pg_settings"] {
        let mut stmt = conn
            .prepare(format!(
                "SELECT set_config(name, 'view, foreign-table', false)
                 FROM {table} WHERE name = 'restrict_nonsystem_relation_kind'"
            ))
            .unwrap();
        assert!(stmt.run_collect_rows().unwrap().is_empty());
    }
    assert!(conn
        .execute("SELECT set_config('restrict_nonsystem_relation_kind', 'view', false)")
        .is_err());
}

/// The DDL that `pg_get_tabledef` reports names tables as a user writes
/// them, not by the names their schema rows store them under, so the DDL
/// can be run again.
#[turso_macros::test(mvcc)]
fn test_pg_get_tabledef_names_tables_as_a_user_writes_them(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE SCHEMA app").unwrap();
    conn.execute("CREATE TABLE \"T\"(x int PRIMARY KEY, y int REFERENCES \"T\"(x))")
        .unwrap();
    conn.execute("CREATE TABLE app.\"U\"(a int REFERENCES \"T\"(x))")
        .unwrap();
    let mut stmt = conn
        .query("SELECT schema_name, table_name, ddl FROM pg_get_tabledef ORDER BY schema_name")
        .unwrap()
        .unwrap();
    let mut rows = Vec::new();
    while let turso_core::StepResult::Row = stmt.step().unwrap() {
        let row = stmt.row().unwrap();
        rows.push(
            (0..3)
                .map(|i| row.get_value(i).to_string())
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(
        rows,
        vec![
            vec![
                "app".to_string(),
                "U".to_string(),
                "CREATE TABLE \"U\"(a int REFERENCES public.\"T\"(x))".to_string()
            ],
            vec![
                "public".to_string(),
                "T".to_string(),
                "CREATE TABLE \"T\"(x int PRIMARY KEY, y int REFERENCES \"T\"(x))".to_string()
            ],
        ]
    );
}

#[turso_macros::test]
fn test_alter_table_preserves_catalog_names_after_reopen(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE SCHEMA app").unwrap();
    conn.execute("CREATE TABLE \"Mixed\"(id int)").unwrap();
    conn.execute("CREATE TABLE app.\"Mixed\"(id int, removed int)")
        .unwrap();
    conn.execute("ALTER TABLE \"Mixed\" ADD COLUMN added int")
        .unwrap();
    conn.execute("ALTER TABLE app.\"Mixed\" DROP COLUMN removed")
        .unwrap();

    let path = db.path.clone();
    let io = db.io.clone();
    drop(conn);
    drop(db);
    let db = turso_pg::open_database_with_io(
        io,
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true),
    )
    .unwrap();
    let conn = turso_pg::Connection::connect(&db).unwrap();
    let mut stmt = conn
        .prepare("SELECT schemaname, tablename FROM pg_tables ORDER BY schemaname")
        .unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![
            vec![Value::build_text("app"), Value::build_text("Mixed")],
            vec![Value::build_text("public"), Value::build_text("Mixed")],
        ]
    );
}

#[turso_macros::test]
fn test_pg_dump_schema_details_survive_reopen(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE \"Mixed Table\" (\"Mixed Key\" TEXT PRIMARY KEY, n INTEGER DEFAULT -1)",
    )
    .unwrap();
    conn.execute("CREATE INDEX descending_key ON \"Mixed Table\" (\"Mixed Key\" DESC)")
        .unwrap();
    conn.execute("ALTER TABLE \"Mixed Table\" ADD COLUMN extra INTEGER")
        .unwrap();
    let path = db.path.clone();
    let io = db.io.clone();
    drop(conn);
    drop(db);
    let db = turso_pg::open_database_with_io(
        io,
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true),
    )
    .unwrap();
    let conn = turso_pg::Connection::connect(&db).unwrap();
    let mut rows = conn
        .query("SELECT indexdef FROM pg_indexes WHERE indexname = 'descending_key'")
        .unwrap()
        .unwrap();
    assert!(matches!(rows.step().unwrap(), turso_core::StepResult::Row));
    assert_eq!(
        rows.row().unwrap().get_value(0).to_string(),
        "CREATE INDEX descending_key ON public.\"Mixed Table\" USING btree (\"Mixed Key\" DESC)"
    );
    let mut rows = conn
        .query("SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef")
        .unwrap()
        .unwrap();
    assert!(matches!(rows.step().unwrap(), turso_core::StepResult::Row));
    assert_eq!(
        rows.row().unwrap().get_value(0).to_string(),
        "'-1'::integer"
    );
}
