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
        ("pg_trigger", 19),
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
fn test_pg_trigger_supports_pg_dump_parent_trigger_query(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.tgrelid, t.tgname, t.tgfoid::pg_catalog.regproc AS tgfname,
                    pg_catalog.pg_get_triggerdef(t.oid, false) AS tgdef,
                    t.tgenabled, t.tableoid, t.oid, t.tgparentid <> 0 AS tgispartition
             FROM unnest('{}'::pg_catalog.oid[]) AS src(tbloid)
             JOIN pg_catalog.pg_trigger t ON src.tbloid = t.tgrelid
             LEFT JOIN pg_catalog.pg_trigger u ON u.oid = t.tgparentid
             WHERE ((NOT t.tgisinternal AND t.tgparentid = 0) OR t.tgenabled != u.tgenabled)
             ORDER BY t.tgrelid, t.tgname",
        )
        .unwrap();
    assert!(stmt.run_collect_rows().unwrap().is_empty());
    assert_eq!(
        conn.prepare("SELECT * FROM pg_trigger")
            .unwrap()
            .num_columns(),
        19
    );
    assert_eq!(
        conn.prepare("SELECT pg_get_triggerdef(0), pg_catalog.pg_get_triggerdef(2147483647, false), pg_get_triggerdef(NULL)")
            .unwrap().run_collect_rows().unwrap(),
        vec![vec![Value::Null, Value::Null, Value::Null]]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_dump_publication_membership_query(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert!(conn.prepare(
        "SELECT tableoid, oid, prpubid, prrelid,
                pg_catalog.pg_get_expr(prqual, prrelid) AS prrelqual,
                (CASE WHEN pr.prattrs IS NOT NULL THEN
                    (SELECT array_agg(attname)
                     FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,
                          pg_catalog.pg_attribute
                     WHERE attrelid = pr.prrelid AND attnum = prattrs[s])
                 ELSE NULL END) prattrs
         FROM pg_catalog.pg_publication_rel pr"
    ).unwrap().run_collect_rows().unwrap().is_empty());
}

#[turso_macros::test(mvcc)]
fn test_pg_security_labels_are_empty_and_cannot_be_created(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE label_probe (id INTEGER)")
        .unwrap();
    assert!(conn
        .execute("SECURITY LABEL FOR probe ON TABLE label_probe IS 'private'")
        .is_err());
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for query in [
        "SELECT label, provider, classoid, objoid, objsubid FROM pg_catalog.pg_seclabels ORDER BY classoid, objoid, objsubid",
        "SELECT objoid, classoid, objsubid, objtype, objnamespace, objname, provider, label FROM pg_seclabels",
    ] {
        assert!(conn.prepare(query).unwrap().run_collect_rows().unwrap().is_empty());
    }
    assert_eq!(
        conn.prepare("SELECT * FROM pg_catalog.pg_seclabels")
            .unwrap()
            .num_columns(),
        8
    );
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

#[turso_macros::test(mvcc)]
fn test_logical_primary_key_catalog_and_restore_definitions(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE parent_table (payload TEXT, id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
    )
    .unwrap();
    conn.execute("CREATE INDEX payload_index ON parent_table(payload)")
        .unwrap();
    conn.execute("CREATE TABLE child_table (parent_id INTEGER, CONSTRAINT named_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE)")
        .unwrap();
    assert_eq!(
        conn.prepare("SELECT c.relhastriggers, k.conname, k.confdeltype FROM pg_class c
            JOIN pg_constraint k ON k.conrelid=c.oid WHERE c.relname='child_table' AND k.contype='f'")
            .unwrap().run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(1), Value::build_text("named_fk"),Value::build_text("c")]]
    );

    assert_eq!(
        conn.prepare(
            "SELECT c.relname, c.relhasindex, a.attnotnull, i.indisprimary, i.indisunique,
                    i.indkey, k.conindid = i.indexrelid, pg_get_constraintdef(k.oid),
                    pg_get_indexdef(i.indexrelid)
             FROM pg_class c
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'id'
             JOIN pg_constraint k ON k.conrelid = c.oid AND k.contype = 'p'
             JOIN pg_index i ON i.indexrelid = k.conindid
             WHERE c.relname = 'parent_table'",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![
            Value::build_text("parent_table"),
            Value::from_i64(1),
            Value::from_i64(1),
            Value::from_i64(1),
            Value::from_i64(1),
            Value::build_text("2"),
            Value::from_i64(1),
            Value::build_text("PRIMARY KEY (id)"),
            Value::build_text(
                "CREATE UNIQUE INDEX parent_table_pkey ON public.parent_table USING btree (id)"
            ),
        ]]
    );

    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(
        conn.prepare(
            "SELECT c.relname, i.indisprimary, i.indisunique, pg_get_indexdef(c.oid)
             FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
             WHERE c.relname IN ('payload_index', 'sqlite_autoindex_parent_table_1')
             ORDER BY c.relname",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![
                Value::build_text("payload_index"),
                Value::from_i64(0),
                Value::from_i64(0),
                Value::build_text(
                    "CREATE INDEX payload_index ON public.parent_table USING btree (payload)"
                ),
            ],
            vec![
                Value::build_text("sqlite_autoindex_parent_table_1"),
                Value::from_i64(0),
                Value::from_i64(1),
                Value::build_text(
                    "CREATE UNIQUE INDEX sqlite_autoindex_parent_table_1 ON public.parent_table USING btree (code)"
                ),
            ],
        ]
    );
    assert_eq!(
        conn.prepare(
            "SELECT pg_get_constraintdef(k.oid) FROM pg_constraint k
             JOIN pg_class c ON c.oid = k.conrelid
             WHERE c.relname = 'child_table' AND k.contype = 'f'",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![Value::build_text(
            "FOREIGN KEY (parent_id) REFERENCES public.parent_table(id) ON DELETE CASCADE"
        )]]
    );

    conn.execute("SELECT set_config('search_path', 'public', false)")
        .unwrap();
    conn.execute("CREATE TABLE \"quoted parent\" (\"primary id\" INTEGER PRIMARY KEY, \"payload value\" TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX \"payload index\" ON \"quoted parent\"(\"payload value\")")
        .unwrap();
    conn.execute("CREATE TABLE \"quoted child\" (\"parent id\" INTEGER REFERENCES \"quoted parent\"(\"primary id\"))")
        .unwrap();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(
        conn.prepare(
            "SELECT pg_get_indexdef(c.oid) FROM pg_class c WHERE c.relname = 'payload index'",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![Value::build_text(
            "CREATE INDEX \"payload index\" ON public.\"quoted parent\" USING btree (\"payload value\")"
        )]]
    );
    assert_eq!(
        conn.prepare(
            "SELECT pg_get_constraintdef(k.oid) FROM pg_constraint k
             JOIN pg_class c ON c.oid = k.conrelid
             WHERE c.relname = 'quoted child' AND k.contype = 'f'",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![Value::build_text(
            "FOREIGN KEY (\"parent id\") REFERENCES public.\"quoted parent\"(\"primary id\")"
        )]]
    );
    assert_eq!(
        conn.prepare(
            "SELECT i.relname, k.conname, d.deptype FROM pg_depend d
            JOIN pg_class i ON d.classid=1259 AND i.oid=d.objid
            JOIN pg_constraint k ON d.refclassid=2606 AND k.oid=d.refobjid
            WHERE k.conname='parent_table_pkey'"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![
            Value::build_text("parent_table_pkey"),
            Value::build_text("parent_table_pkey"),
            Value::build_text("i")
        ]]
    );
    conn.execute("SELECT set_config('search_path', 'public', false)")
        .unwrap();
    conn.execute(
        "CREATE TABLE public.physical_keys (payload TEXT, id TEXT PRIMARY KEY, code TEXT UNIQUE)",
    )
    .unwrap();
    conn.execute("CREATE UNIQUE INDEX duplicate_primary ON public.physical_keys(id)")
        .unwrap();
    conn.execute(
        "CREATE UNIQUE INDEX partial_code ON public.physical_keys(code) WHERE code IS NOT NULL",
    )
    .unwrap();
    assert_eq!(
        conn.prepare("SELECT c.relname, i.indisprimary, i.indkey FROM pg_index i
            JOIN pg_class c ON c.oid=i.indexrelid
            WHERE c.relname IN ('duplicate_primary', 'partial_code', 'sqlite_autoindex_physical_keys_1', 'sqlite_autoindex_physical_keys_2')
            ORDER BY c.relname")
            .unwrap().run_collect_rows().unwrap(),
        vec![
            vec![Value::build_text("duplicate_primary"), Value::from_i64(0), Value::build_text("2")],
            vec![Value::build_text("partial_code"), Value::from_i64(0), Value::build_text("3")],
            vec![Value::build_text("sqlite_autoindex_physical_keys_1"), Value::from_i64(1), Value::build_text("2")],
            vec![Value::build_text("sqlite_autoindex_physical_keys_2"), Value::from_i64(0), Value::build_text("3")],
        ]
    );
    assert_eq!(
        conn.prepare(
            "SELECT k.contype, c.relname FROM pg_constraint k JOIN pg_class c ON c.oid=k.conindid
            JOIN pg_class t ON t.oid=k.conrelid WHERE t.relname='physical_keys' ORDER BY k.contype"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![
                Value::build_text("p"),
                Value::build_text("sqlite_autoindex_physical_keys_1")
            ],
            vec![
                Value::build_text("u"),
                Value::build_text("sqlite_autoindex_physical_keys_2")
            ]
        ]
    );
}

#[turso_macros::test(views)]
fn test_dump_catalogs_include_schema_qualified_objects(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE public.dump_probe (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE SCHEMA dump_extra").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE dump_extra.dump_probe (payload TEXT DEFAULT 'extra', extra_id BIGINT PRIMARY KEY)").unwrap();
    conn.execute("CREATE TABLE dump_extra.dump_audit (entry TEXT NOT NULL)")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE SEQUENCE public.dump_counter START 41 INCREMENT 3 MAXVALUE 999")
        .unwrap();
    conn.execute("CREATE VIEW public.dump_names AS SELECT name FROM public.dump_probe")
        .unwrap();
    conn.execute("CREATE TABLE public.pg_user_data (id INTEGER)")
        .unwrap();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(
        conn.prepare("SELECT n.nspname, c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.relkind IN ('r','v','S') ORDER BY n.nspname, c.relname").unwrap().run_collect_rows().unwrap(),
        vec![
            vec![Value::build_text("dump_extra"),Value::build_text("dump_audit"),Value::build_text("r")],
            vec![Value::build_text("dump_extra"),Value::build_text("dump_probe"),Value::build_text("r")],
            vec![Value::build_text("public"),Value::build_text("dump_counter"),Value::build_text("S")],
            vec![Value::build_text("public"),Value::build_text("dump_names"),Value::build_text("v")],
            vec![Value::build_text("public"),Value::build_text("dump_probe"),Value::build_text("r")],
            vec![Value::build_text("public"),Value::build_text("pg_user_data"),Value::build_text("r")],
        ]
    );
    assert_eq!(
        conn.prepare("SELECT n.nspname, a.attname, format_type(a.atttypid,a.atttypmod), a.attnotnull, a.atthasdef
            FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.relname='dump_probe' ORDER BY n.nspname, a.attnum").unwrap().run_collect_rows().unwrap(),
        vec![
            vec![Value::build_text("dump_extra"),Value::build_text("payload"),Value::build_text("text"),Value::from_i64(0),Value::from_i64(1)],
            vec![Value::build_text("dump_extra"),Value::build_text("extra_id"),Value::build_text("bigint"),Value::from_i64(1),Value::from_i64(0)],
            vec![Value::build_text("public"),Value::build_text("id"),Value::build_text("integer"),Value::from_i64(1),Value::from_i64(0)],
            vec![Value::build_text("public"),Value::build_text("name"),Value::build_text("text"),Value::from_i64(0),Value::from_i64(0)],
        ]
    );
    assert_eq!(
        conn.prepare("SELECT n.nspname, i.indkey, pg_get_constraintdef(k.oid), pg_get_indexdef(i.indexrelid)
            FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class c ON c.oid=k.conrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='dump_probe' AND k.contype='p' ORDER BY n.nspname").unwrap().run_collect_rows().unwrap(),
        vec![
            vec![Value::build_text("dump_extra"),Value::build_text("2"),Value::build_text("PRIMARY KEY (extra_id)"),Value::build_text("CREATE UNIQUE INDEX sqlite_autoindex_dump_probe_1 ON dump_extra.dump_probe USING btree (extra_id)")],
            vec![Value::build_text("public"),Value::build_text("1"),Value::build_text("PRIMARY KEY (id)"),Value::build_text("CREATE UNIQUE INDEX dump_probe_pkey ON public.dump_probe USING btree (id)")],
        ]
    );
    assert_eq!(
        conn.prepare("SELECT n.nspname, a.adnum, pg_get_expr(a.adbin,a.adrelid) FROM pg_attrdef a
            JOIN pg_class c ON c.oid=a.adrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='dump_probe'").unwrap().run_collect_rows().unwrap(),
        vec![vec![Value::build_text("dump_extra"), Value::from_i64(1),Value::build_text("'extra'")]]
    );
}

#[turso_macros::test(mvcc, views)]
fn test_dump_view_and_persisted_sequence_queries(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE dump_probe (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO dump_probe VALUES (1,'excluded'),(3,'retained')")
        .unwrap();
    conn.execute("CREATE VIEW dump_names AS SELECT id,name FROM public.dump_probe WHERE id>1")
        .unwrap();
    conn.execute(
        "CREATE VIEW dump_cte AS WITH dump_probe AS (SELECT id,name FROM public.dump_probe)
        SELECT id,name FROM dump_probe WHERE id IN (SELECT id FROM public.dump_probe WHERE id>1)",
    )
    .unwrap();
    conn.execute("CREATE SEQUENCE dump_counter START 41 INCREMENT 3 MAXVALUE 999")
        .unwrap();
    conn.execute(
        "CREATE SEQUENCE descending_counter START -11 INCREMENT -4 MINVALUE -20 MAXVALUE -1",
    )
    .unwrap();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(
        conn.prepare("SELECT format_type(seqtypid,NULL),seqstart,seqincrement,seqmax,seqmin,seqcache,seqcycle
            FROM pg_catalog.pg_sequence WHERE seqrelid=(SELECT oid FROM pg_class WHERE relname='dump_counter' AND relkind='S')")
            .unwrap().run_collect_rows().unwrap(),
        vec![vec![Value::build_text("bigint"),Value::from_i64(41),Value::from_i64(3),Value::from_i64(999),Value::from_i64(1),Value::from_i64(1),Value::from_i64(0)]]
    );
    for name in ["dump_names", "dump_cte"] {
        let definition = conn
            .prepare(format!(
                "SELECT pg_catalog.pg_get_viewdef(oid) FROM pg_class WHERE relname='{name}'"
            ))
            .unwrap()
            .run_collect_rows()
            .unwrap();
        let Value::Text(sql) = &definition[0][0] else {
            panic!("expected view SQL")
        };
        assert!(sql.as_str().ends_with(';'), "{sql}");
        assert_eq!(
            conn.prepare(sql.as_str())
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(3), Value::build_text("retained")]]
        );
    }
    assert_eq!(
        conn.prepare(
            "SELECT v.relname,r.relname FROM pg_depend d
            JOIN pg_class v ON v.oid=d.objid AND d.classid=1259
            JOIN pg_class r ON r.oid=d.refobjid AND d.refclassid=1259
            WHERE v.relkind='v' ORDER BY v.relname"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![
                Value::build_text("dump_cte"),
                Value::build_text("dump_probe")
            ],
            vec![
                Value::build_text("dump_names"),
                Value::build_text("dump_probe")
            ],
        ]
    );
    assert_eq!(
        conn.prepare("SELECT last_value,is_called FROM public.dump_counter")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(41), Value::from_i64(0)]]
    );
    for name in ["dump_counter", "descending_counter"] {
        conn.execute(format!("SELECT nextval('{name}')")).unwrap();
        conn.execute(format!("SELECT nextval('{name}')")).unwrap();
    }
    let io = db.io.clone();
    let path = db.path.clone();
    conn.close().unwrap();
    drop(conn);
    drop(db);
    let database = turso_pg::open_database_with_io(
        io,
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new()
            .with_views(true)
            .with_custom_types(true),
    )
    .unwrap();
    let conn = turso_pg::Connection::connect(&database).unwrap();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    let mut statement = conn
        .prepare("SELECT last_value,is_called FROM public.dump_counter")
        .unwrap();
    conn.execute("CREATE TABLE public.reprepare_check (id INTEGER)")
        .unwrap();
    assert_eq!(
        statement.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(44), Value::from_i64(1)]]
    );
    assert_eq!(
        conn.prepare("SELECT last_value,is_called FROM public.descending_counter")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(-15), Value::from_i64(1)]]
    );
    assert_eq!(
        conn.prepare("SELECT last_value FROM pg_sequences WHERE sequencename='dump_counter'")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(44)]]
    );
    assert_eq!(
        conn.prepare("SELECT nextval('dump_counter')")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(47)]]
    );
}
