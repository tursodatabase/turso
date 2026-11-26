use crate::common::TempDatabase;
use std::sync::Arc;
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
        34
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

/// Opening a database and reading its schema again go through the same
/// steps, so the catalogs must report the same objects after both.
#[test]
fn catalogs_report_the_same_objects_after_open_and_after_the_schema_is_read_again() {
    let db = TempDatabase::builder().build();
    {
        let conn = db.connect_postgres();
        conn.execute("CREATE SCHEMA app").unwrap();
        conn.execute("CREATE SEQUENCE app.ids START WITH 5")
            .unwrap();
        conn.execute("CREATE TYPE mood AS ENUM ('happy', 'sad')")
            .unwrap();
        conn.execute("CREATE ROLE alice").unwrap();
        conn.execute("CREATE TABLE app.docs (id int PRIMARY KEY, feeling mood)")
            .unwrap();
        conn.execute("CREATE INDEX docs_feeling ON app.docs (feeling)")
            .unwrap();
        conn.execute("ALTER TABLE app.docs OWNER TO alice").unwrap();
    }
    let db = db.reopen();
    let conn = db.connect_postgres();

    let after_open = catalog_rows(&conn);
    conn.inner().force_reparse_schema_without_publish().unwrap();
    let after_reading_again = catalog_rows(&conn);

    for name in ["app", "ids", "mood", "alice", "docs", "docs_feeling"] {
        assert!(
            after_open
                .iter()
                .any(|row| row.contains(&Value::build_text(name))),
            "{name} is missing from the catalogs: {after_open:?}"
        );
    }
    assert_eq!(after_open, after_reading_again);
}

/// Every user table needs a row in the classes table. A file where one is
/// missing is corrupt: open must fail, and so must a reload.
#[test]
fn a_table_without_a_class_row_fails_the_load_at_open_and_when_the_schema_is_read_again() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE docs (id int)").unwrap();
    conn.inner()
        .prepare_engine_schema_statement("DELETE FROM pg_class WHERE relname = 'docs'")
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    let expected = format!(
        "relation \"docs\" of namespace {} has no row in pg_class",
        public_namespace(&conn)
    );

    let error = conn
        .inner()
        .force_reparse_schema_without_publish()
        .unwrap_err();
    assert!(
        matches!(&error, turso_core::LimboError::Corrupt(message) if *message == expected),
        "{error:?}"
    );

    conn.close().unwrap();
    drop(conn);
    let TempDatabase { path, io, db } = db;
    drop(db);
    let Err(error) = turso_pg::open_database_with_io(
        io,
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_encryption(true),
    ) else {
        panic!("a database with a table that has no class row opened");
    };
    assert!(error.to_string().contains(&expected), "{error}");
}

/// The id of the namespace of the schema `public`.
fn public_namespace(conn: &turso_pg::Connection) -> u32 {
    conn.inner().with_schema(turso_core::MAIN_DB_ID, |schema| {
        schema
            .namespace_named("public")
            .expect("a PostgreSQL database has the schema public")
            .as_u32()
    })
}

/// What the catalogs report about schemas, relations, sequences, types and
/// roles.
fn catalog_rows(conn: &turso_pg::Connection) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for sql in [
        "SELECT oid, nspname, nspowner FROM pg_namespace ORDER BY oid",
        "SELECT oid, relname, relnamespace, relkind, relowner FROM pg_class ORDER BY oid",
        "SELECT schemaname, sequencename, start_value, increment_by FROM pg_sequences \
         ORDER BY schemaname, sequencename",
        "SELECT oid, typname, typnamespace, typtype FROM pg_type ORDER BY oid",
        "SELECT oid, rolname, rolsuper FROM pg_roles ORDER BY oid",
        "SELECT attrelid, attname, atttypid FROM pg_attribute ORDER BY attrelid, attnum",
    ] {
        let mut stmt = conn.query(sql).unwrap().unwrap();
        rows.extend(stmt.run_collect_rows().unwrap());
    }
    rows
}

/// How the database of
/// [`a_schema_a_role_and_a_table_are_present_after_open`] is stored when it
/// is opened again.
#[derive(Debug, Clone, Copy)]
enum StoredAs {
    /// Without MVCC.
    Wal,
    /// With MVCC, and the rows are only in the logical log. Opening replays
    /// them, and the tables that the open reads have no pages yet.
    MvccLogicalLog,
    /// With MVCC, and a checkpoint wrote the rows to the database file.
    MvccCheckpointed,
}

/// Opening reads the names of the schemas and then the dialect's own tables
/// with statements. The second read must see the schemas of the first.
#[test]
fn a_schema_a_role_and_a_table_are_present_after_open() {
    for stored_as in [
        StoredAs::Wal,
        StoredAs::MvccLogicalLog,
        StoredAs::MvccCheckpointed,
    ] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("main.db");
        let open = || {
            let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
            let file = io
                .open_file(
                    path.to_str().unwrap(),
                    turso_core::OpenFlags::default(),
                    true,
                )
                .unwrap();
            let options = turso_core::OpenOptions::new(Arc::new(turso_pg::PostgresDialect))
                .storage(Arc::new(turso_core::storage::database::DatabaseFile::new(
                    file,
                )))
                .db_opts(
                    turso_core::DatabaseOpts::new().with_mvcc(!matches!(stored_as, StoredAs::Wal)),
                );
            let mut state = turso_pg::OpenDatabaseState::default();
            let db = loop {
                match turso_pg::open_database_async(
                    &mut state,
                    io.clone(),
                    path.to_str().unwrap(),
                    &options,
                    turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser),
                )
                .unwrap()
                {
                    turso_core::IOResult::Done(db) => break db,
                    turso_core::IOResult::IO(completions) => {
                        completions.wait(io.as_ref()).unwrap();
                    }
                }
            };
            turso_pg::Connection::connect(&db).unwrap()
        };
        {
            let conn = open();
            conn.execute("CREATE SCHEMA app").unwrap();
            conn.execute("CREATE ROLE alice").unwrap();
            conn.execute("CREATE TABLE app.docs (id int PRIMARY KEY)")
                .unwrap();
            conn.execute("ALTER TABLE app.docs OWNER TO alice").unwrap();
            if matches!(stored_as, StoredAs::MvccCheckpointed) {
                conn.inner()
                    .execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    .unwrap();
            }
        }
        let log_size = std::fs::metadata(path.with_extension("db-log")).map(|log| log.len());
        match stored_as {
            StoredAs::Wal => assert!(log_size.is_err(), "{stored_as:?}"),
            StoredAs::MvccLogicalLog => assert!(log_size.unwrap() > 0, "{stored_as:?}"),
            StoredAs::MvccCheckpointed => assert_eq!(log_size.unwrap(), 0, "{stored_as:?}"),
        }

        let conn = open();

        let mut stmt = conn
            .query(
                "SELECT schemaname, tablename, tableowner FROM pg_tables WHERE schemaname = 'app'",
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![
                Value::build_text("app"),
                Value::build_text("docs"),
                Value::build_text("alice"),
            ]],
            "{stored_as:?}"
        );
        conn.execute("SET ROLE alice").unwrap();
        conn.execute("INSERT INTO app.docs VALUES (1)").unwrap();
    }
}
