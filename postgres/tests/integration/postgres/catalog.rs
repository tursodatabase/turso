use crate::common::TempDatabase;
use turso_core::Value;

#[turso_macros::test(mvcc)]
fn test_pg_attribute_compression_defaults(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE dump_columns (id INTEGER NOT NULL, payload TEXT DEFAULT 'pending')")
        .unwrap();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(
        conn.prepare(
            "SELECT a.attname, a.attcompression, a.attnotnull, a.atthasdef,
                pg_catalog.format_type(t.oid, a.atttypmod), a.attlen, a.attbyval,
                a.attalign, a.attstorage
            FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
            WHERE c.relname = 'dump_columns' ORDER BY a.attnum"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![
                Value::build_text("id"),
                Value::build_text(""),
                Value::from_i64(1),
                Value::from_i64(0),
                Value::build_text("integer"),
                Value::from_i64(4),
                Value::from_i64(1),
                Value::build_text("i"),
                Value::build_text("p")
            ],
            vec![
                Value::build_text("payload"),
                Value::build_text(""),
                Value::from_i64(0),
                Value::from_i64(1),
                Value::build_text("text"),
                Value::from_i64(-1),
                Value::from_i64(0),
                Value::build_text("i"),
                Value::build_text("x")
            ],
        ]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_index_nulls_are_distinct(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE dump_indexes (id INTEGER, payload TEXT)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX dump_unique ON dump_indexes(payload)")
        .unwrap();
    conn.execute("CREATE INDEX dump_plain ON dump_indexes(id)")
        .unwrap();
    conn.execute("INSERT INTO dump_indexes VALUES (17, NULL), (2, NULL), (9, 'present')")
        .unwrap();
    assert!(conn
        .execute("INSERT INTO dump_indexes VALUES (3, 'present')")
        .is_err());
    assert_eq!(
        conn.prepare(
            "SELECT c.relname, i.indisunique, i.indnullsnotdistinct, i.indnatts, i.indkey
            FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
            WHERE c.relname IN ('dump_unique', 'dump_plain') ORDER BY c.relname"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![
                Value::build_text("dump_plain"),
                Value::from_i64(0),
                Value::from_i64(0),
                Value::from_i64(1),
                Value::build_text("1")
            ],
            vec![
                Value::build_text("dump_unique"),
                Value::from_i64(1),
                Value::from_i64(0),
                Value::from_i64(1),
                Value::build_text("2")
            ],
        ]
    );
}

#[turso_macros::test(mvcc)]
fn test_empty_physical_catalogs_have_hidden_tableoid(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for (table, columns) in [
        ("pg_collation", 12),
        ("pg_policy", 8),
        ("pg_trigger", 18),
        ("pg_statistic_ext", 9),
        ("pg_inherits", 4),
        ("pg_rewrite", 8),
        ("pg_foreign_table", 3),
        ("pg_partitioned_table", 8),
        ("pg_description", 4),
        ("pg_publication", 9),
        ("pg_publication_namespace", 3),
        ("pg_publication_rel", 5),
    ] {
        let mut stmt = conn
            .prepare(format!("SELECT tableoid FROM {table}"))
            .unwrap();
        assert!(stmt.run_collect_rows().unwrap().is_empty(), "{table}");
        assert_eq!(
            conn.prepare(format!("SELECT * FROM {table}"))
                .unwrap()
                .num_columns(),
            columns,
            "{table}"
        );
    }
    assert!(conn.prepare("SELECT tableoid, oid, collname, collnamespace, collowner, collencoding FROM pg_collation")
        .unwrap().run_collect_rows().unwrap().is_empty());
}

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
