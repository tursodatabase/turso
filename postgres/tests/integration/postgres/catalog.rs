use crate::common::TempDatabase;
use turso_core::Value;

#[turso_macros::test(mvcc)]
fn test_pg_language_describes_native_function_languages(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(
        conn.prepare(
            "SELECT p.proname, l.lanname, l.lanispl, l.tableoid FROM pg_proc p
             JOIN pg_language l ON l.oid = p.prolang
             WHERE p.proname IN ('abs', 'pg_is_in_recovery') ORDER BY p.proname",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![
                Value::build_text("abs"),
                Value::build_text("internal"),
                Value::from_i64(0),
                Value::from_i64(2612),
            ],
            vec![
                Value::build_text("pg_is_in_recovery"),
                Value::build_text("c"),
                Value::from_i64(0),
                Value::from_i64(2612),
            ],
        ]
    );
    assert!(conn
        .prepare(
            "SELECT tableoid, oid, lanname, lanpltrusted, lanplcallfoid, laninline,
                    lanvalidator, lanacl, acldefault('l', lanowner) AS acldefault, lanowner
             FROM pg_language WHERE lanispl ORDER BY oid",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .is_empty());
    assert_eq!(
        conn.prepare("SELECT * FROM pg_language")
            .unwrap()
            .num_columns(),
        9
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_unsupported_object_catalogs_are_empty_and_read_only(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for (table, columns) in [
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
            let mut stmt = conn.prepare(format!("SELECT * FROM {name}")).unwrap();
            assert_eq!(stmt.num_columns(), columns, "{name}");
            assert!(stmt.run_collect_rows().unwrap().is_empty(), "{name}");
            let mut oid = conn
                .prepare(format!("SELECT tableoid FROM {name}"))
                .unwrap();
            assert!(oid.run_collect_rows().unwrap().is_empty(), "{name}");
        }
        assert!(conn.prepare(format!("DELETE FROM {table}")).is_err());
    }
    for sql in [
        "SELECT tableoid, oid, oprname, oprnamespace, oprowner, oprkind, oprleft,
                oprright, oprcode::oid AS oprcode FROM pg_operator",
        "SELECT tableoid, oid, opcmethod, opcname, opcnamespace, opcowner FROM pg_opclass",
        "SELECT tableoid, oid, opfmethod, opfname, opfnamespace, opfowner FROM pg_opfamily",
        "SELECT tableoid, oid, prsname, prsnamespace, prsstart::oid, prstoken::oid,
                prsend::oid, prsheadline::oid, prslextype::oid FROM pg_ts_parser",
        "SELECT tableoid, oid, tmplname, tmplnamespace, tmplinit::oid, tmpllexize::oid
         FROM pg_ts_template",
        "SELECT tableoid, oid, dictname, dictnamespace, dictowner, dicttemplate,
                dictinitoption FROM pg_ts_dict",
        "SELECT tableoid, oid, cfgname, cfgnamespace, cfgowner, cfgparser FROM pg_ts_config",
        "SELECT oid, tableoid, defaclrole, defaclnamespace, defaclobjtype, defaclacl,
                CASE WHEN defaclnamespace = 0 THEN acldefault(
                  CASE WHEN defaclobjtype = 'S' THEN 's'::\"char\" ELSE defaclobjtype END,
                  defaclrole) ELSE '{}' END AS acldefault FROM pg_default_acl",
        "SELECT tableoid, oid, conname, connamespace, conowner FROM pg_conversion",
        "SELECT tableoid, oid, castsource, casttarget, castfunc, castcontext, castmethod
         FROM pg_cast c WHERE NOT EXISTS (SELECT 1 FROM pg_range r
           WHERE c.castsource = r.rngtypid AND c.casttarget = r.rngmultitypid) ORDER BY 3,4",
        "SELECT s.tableoid, s.oid, s.subname, s.subowner, s.subconninfo, s.subslotname,
                s.subsynccommit, s.subpublications, s.subbinary, s.substream,
                s.subtwophasestate, s.subdisableonerr, s.subpasswordrequired,
                s.subrunasowner, s.suborigin
         FROM pg_subscription s WHERE s.subdbid = (
           SELECT oid FROM pg_database WHERE datname = current_database())",
        "SELECT oid, lomowner, lomacl, acldefault('L', lomowner) AS acldefault
         FROM pg_largeobject_metadata",
    ] {
        assert!(
            conn.prepare(sql)
                .unwrap()
                .run_collect_rows()
                .unwrap()
                .is_empty(),
            "{sql}"
        );
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
