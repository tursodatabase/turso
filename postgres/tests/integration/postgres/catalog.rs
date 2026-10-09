use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};

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
        assert!(
            conn.prepare(query)
                .unwrap()
                .run_collect_rows()
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        conn.prepare("SELECT * FROM pg_catalog.pg_seclabels")
            .unwrap()
            .num_columns(),
        8
    );
}

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
fn test_pg_function_support_catalogs_have_no_user_objects(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for (table, columns, projection) in [
        (
            "pg_init_privs",
            5,
            "objoid, classoid, objsubid, privtype, initprivs, tableoid",
        ),
        (
            "pg_cast",
            6,
            "oid, castsource, casttarget, castfunc, castcontext, castmethod, tableoid",
        ),
        (
            "pg_transform",
            5,
            "oid, trftype, trflang, trffromsql, trftosql, tableoid",
        ),
    ] {
        for name in [table.to_owned(), format!("pg_catalog.{table}")] {
            let mut stmt = conn.prepare(format!("SELECT * FROM {name}")).unwrap();
            assert_eq!(stmt.num_columns(), columns, "{name}");
            assert!(stmt.run_collect_rows().unwrap().is_empty(), "{name}");
            assert!(conn
                .prepare(format!("SELECT {projection} FROM {name}"))
                .unwrap()
                .run_collect_rows()
                .unwrap()
                .is_empty());
        }
        assert!(conn.prepare(format!("DELETE FROM {table}")).is_err());
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_dump_does_not_treat_engine_functions_as_user_functions(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(
        conn.prepare(
            "SELECT p.proname, n.nspname FROM pg_proc p
             JOIN pg_namespace n ON p.pronamespace = n.oid
             WHERE p.proname IN ('abs', 'pg_is_in_recovery') ORDER BY p.proname",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![Value::build_text("abs"), Value::build_text("pg_catalog")],
            vec![
                Value::build_text("pg_is_in_recovery"),
                Value::build_text("pg_catalog")
            ],
        ]
    );
    assert!(conn
        .prepare(
            "SELECT p.tableoid, p.oid, p.proname, p.prolang, p.pronargs, p.proargtypes,
                    p.prorettype, p.proacl, acldefault('f', p.proowner) AS acldefault,
                    p.pronamespace, p.proowner
             FROM pg_proc p LEFT JOIN pg_init_privs pip
               ON (p.oid = pip.objoid AND pip.classoid = 'pg_proc'::regclass AND pip.objsubid = 0)
             WHERE p.prokind <> 'a'
               AND NOT EXISTS (SELECT 1 FROM pg_depend
                   WHERE classid = 'pg_proc'::regclass AND objid = p.oid AND deptype = 'i')
               AND (pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')
                    OR EXISTS (SELECT 1 FROM pg_cast
                        WHERE pg_cast.oid > 16383 AND p.oid = pg_cast.castfunc)
                    OR EXISTS (SELECT 1 FROM pg_transform
                        WHERE pg_transform.oid > 16383
                          AND (p.oid = pg_transform.trffromsql OR p.oid = pg_transform.trftosql))
                    OR p.proacl IS DISTINCT FROM pip.initprivs)",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .is_empty());
}

#[turso_macros::test(mvcc)]
fn test_pg_depend_tracks_live_objects(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE dep_parent (id TEXT UNIQUE)")
        .unwrap();
    conn.execute(
        "CREATE TABLE dep_child (parent_id TEXT REFERENCES dep_parent(id), qty INTEGER DEFAULT 17)",
    )
    .unwrap();
    conn.execute("CREATE INDEX dep_child_qty ON dep_child(qty)")
        .unwrap();
    conn.execute("CREATE INDEX dep_parent_id ON dep_parent(id)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX dep_parent_unique_id ON dep_parent(id)")
        .unwrap();
    assert_eq!(
        conn.prepare(
            "SELECT c.relname, n.nspname, d.deptype FROM pg_depend d
                      JOIN pg_class c ON d.classid = 1259 AND c.oid = d.objid
                      JOIN pg_namespace n ON d.refclassid = 2615 AND n.oid = d.refobjid
                      WHERE c.relname IN ('dep_parent', 'dep_child') ORDER BY c.relname"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![
                Value::build_text("dep_child"),
                Value::build_text("public"),
                Value::build_text("n")
            ],
            vec![
                Value::build_text("dep_parent"),
                Value::build_text("public"),
                Value::build_text("n")
            ],
        ]
    );
    assert_eq!(
        conn.prepare(
            "SELECT r.relname, d.refobjsubid, d.deptype FROM pg_depend d
                      JOIN pg_constraint c ON d.classid = 2606 AND c.oid = d.objid
                      JOIN pg_class r ON d.refclassid = 1259 AND r.oid = d.refobjid
                      WHERE c.conname = 'dep_child_parent_id_fkey' AND r.relkind = 'r'
                      ORDER BY r.relname"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![
                Value::build_text("dep_child"),
                Value::from_i64(1),
                Value::build_text("a")
            ],
            vec![
                Value::build_text("dep_parent"),
                Value::from_i64(1),
                Value::build_text("n")
            ],
        ]
    );
    assert_eq!(
        conn.prepare(
            "SELECT r.relname, a.adnum, d.refobjsubid, d.deptype FROM pg_depend d
                      JOIN pg_attrdef a ON d.classid = 2604 AND a.oid = d.objid
                      JOIN pg_class r ON d.refclassid = 1259 AND r.oid = d.refobjid
                      WHERE r.relname = 'dep_child'"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![
            Value::build_text("dep_child"),
            Value::from_i64(2),
            Value::from_i64(2),
            Value::build_text("a")
        ]]
    );
    assert_eq!(
        conn.prepare(
            "SELECT d.refobjsubid, d.deptype FROM pg_depend d
                      JOIN pg_class i ON d.classid = 1259 AND i.oid = d.objid AND i.relkind = 'i'
                      JOIN pg_class r ON d.refclassid = 1259 AND r.oid = d.refobjid
                      WHERE r.relname = 'dep_child'"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![Value::from_i64(2), Value::build_text("a")]]
    );
    assert_eq!(
        conn.prepare(
            "SELECT i.relname, d.refobjsubid, d.deptype FROM pg_depend d
                      JOIN pg_class i ON d.classid = 1259 AND i.oid = d.objid
                      JOIN pg_class r ON d.refclassid = 1259 AND r.oid = d.refobjid
                      WHERE i.relname IN ('dep_parent_id', 'dep_parent_unique_id')
                        AND r.relname = 'dep_parent' ORDER BY i.relname"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![
                Value::build_text("dep_parent_id"),
                Value::from_i64(1),
                Value::build_text("a")
            ],
            vec![
                Value::build_text("dep_parent_unique_id"),
                Value::from_i64(1),
                Value::build_text("a")
            ],
        ]
    );
    assert_eq!(
        conn.prepare(
            "SELECT c.conname, d.deptype FROM pg_depend d
                      JOIN pg_class i ON d.classid = 1259 AND i.oid = d.objid AND i.relkind = 'i'
                      JOIN pg_constraint c ON d.refclassid = 2606 AND c.oid = d.refobjid
                      WHERE c.conname = 'dep_parent_id_key'"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![
            Value::build_text("dep_parent_id_key"),
            Value::build_text("i")
        ]]
    );
    let mut stmt = conn.prepare("SELECT * FROM pg_catalog.pg_depend").unwrap();
    assert_eq!(stmt.num_columns(), 7);
    assert!(!stmt.run_collect_rows().unwrap().is_empty());
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(
        conn.prepare("SELECT tableoid FROM pg_depend LIMIT 1")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(2608)]]
    );
    assert!(conn.prepare("DELETE FROM pg_depend").is_err());
}

#[turso_macros::test(mvcc)]
fn test_pg_depend_for_dump_table_query(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE dump_indexed (id INTEGER PRIMARY KEY, value TEXT UNIQUE)")
        .unwrap();
    conn.execute("CREATE TABLE dump_plain (value TEXT)")
        .unwrap();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT c.tableoid, c.oid, c.relname, c.relnamespace, c.relkind, c.reltype,
                c.relowner, c.relchecks, c.relhasindex, c.relhasrules, c.relpages,
                c.relhastriggers, c.relpersistence, c.reloftype, c.relacl,
                acldefault(CASE WHEN c.relkind = 'S' THEN 's'::\"char\" ELSE 'r'::\"char\" END,
                           c.relowner) AS acldefault,
                CASE WHEN c.relkind = 'f' THEN (SELECT ftserver FROM pg_catalog.pg_foreign_table
                     WHERE ftrelid = c.oid) ELSE 0 END AS foreignserver,
                c.relfrozenxid, tc.relfrozenxid AS tfrozenxid, tc.oid AS toid,
                tc.relpages AS toastpages, tc.reloptions AS toast_reloptions,
                d.refobjid AS owning_tab, d.refobjsubid AS owning_col,
                tsp.spcname AS reltablespace, false AS relhasoids, c.relispopulated,
                c.relreplident, c.relrowsecurity, c.relforcerowsecurity, c.relminmxid,
                tc.relminmxid AS tminmxid,
                array_remove(array_remove(c.reloptions,'check_option=local'),
                             'check_option=cascaded') AS reloptions,
                CASE WHEN 'check_option=local' = ANY(c.reloptions) THEN 'LOCAL'::text
                     WHEN 'check_option=cascaded' = ANY(c.reloptions) THEN 'CASCADED'::text
                     ELSE NULL END AS checkoption,
                am.amname, (d.deptype = 'i') IS TRUE AS is_identity_sequence,
                c.relispartition AS ispartition
         FROM pg_class c
         LEFT JOIN pg_depend d ON (c.relkind = 'S' AND d.classid = 'pg_class'::regclass
             AND d.objid = c.oid AND d.objsubid = 0
             AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i'))
         LEFT JOIN pg_tablespace tsp ON (tsp.oid = c.reltablespace)
         LEFT JOIN pg_am am ON (c.relam = am.oid)
         LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid AND tc.relkind = 't'
                                  AND c.relkind <> 'p')
         WHERE c.relkind IN ('r', 'S', 'v', 'c', 'm', 'f', 'p')
           AND c.relname IN ('dump_indexed', 'dump_plain') ORDER BY c.relname",
        )
        .unwrap();
    assert_eq!(stmt.get_column_decltype(8).as_deref(), Some("BOOLEAN"));
    assert_eq!(
        stmt.run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| {
                [
                    row[0].clone(),
                    row[2].clone(),
                    row[8].clone(),
                    row[15].clone(),
                    row[24].clone(),
                    row[32].clone(),
                    row[33].clone(),
                    row[34].clone(),
                ]
            })
            .collect::<Vec<_>>(),
        vec![
            [
                Value::from_i64(1259),
                Value::build_text("dump_indexed"),
                Value::from_i64(1),
                Value::build_text("{turso=arwdDxt/turso}"),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::build_text("heap")
            ],
            [
                Value::from_i64(1259),
                Value::build_text("dump_plain"),
                Value::from_i64(0),
                Value::build_text("{turso=arwdDxt/turso}"),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::build_text("heap")
            ],
        ]
    );
    let mut tablespaces = conn.prepare("SELECT * FROM pg_tablespace").unwrap();
    assert_eq!(tablespaces.num_columns(), 5);
    assert!(tablespaces.run_collect_rows().unwrap().is_empty());
}

#[turso_macros::test(mvcc)]
fn test_physical_catalog_tableoid_is_hidden(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE indexed (id INTEGER PRIMARY KEY, value TEXT UNIQUE DEFAULT 'a')")
        .unwrap();
    for (catalog, oid) in [
        ("pg_class", 1259),
        ("pg_namespace", 2615),
        ("pg_attribute", 1249),
        ("pg_proc", 1255),
        ("pg_database", 1262),
        ("pg_am", 2601),
        ("pg_type", 1247),
        ("pg_index", 2610),
        ("pg_constraint", 2606),
        ("pg_attrdef", 2604),
    ] {
        assert_eq!(
            conn.prepare(format!("SELECT tableoid FROM pg_catalog.{catalog} LIMIT 1"))
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(oid)]],
            "{catalog}"
        );
    }
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

    conn.execute("CREATE TABLE refresh_schema (value TEXT)")
        .unwrap();
    assert_eq!(
        conn.prepare("SELECT tableoid FROM pg_namespace LIMIT 1")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(2615)]]
    );
}

#[turso_macros::test(mvcc)]
fn test_physical_catalog_boolean_metadata(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE indexed (id INTEGER PRIMARY KEY, value TEXT UNIQUE)")
        .unwrap();
    conn.execute("CREATE TABLE plain (value TEXT)").unwrap();
    let mut booleans = conn
        .prepare(
            "SELECT relname, relhasindex FROM pg_class
                  WHERE relname IN ('indexed', 'plain') ORDER BY relname",
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
fn test_pg_settings_reports_connection_settings(db: TempDatabase) {
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    let sql = "SELECT setting, source, boot_val, reset_val, context, vartype,
                      unit, min_val, max_val, enumvals, sourcefile, sourceline, pending_restart
               FROM pg_catalog.pg_settings WHERE name = 'search_path'";
    let default = Value::build_text("\"$user\", public");
    assert_eq!(
        conn.prepare(sql).unwrap().run_collect_rows().unwrap(),
        vec![vec![
            default.clone(),
            Value::build_text("default"),
            default.clone(),
            default.clone(),
            Value::build_text("user"),
            Value::build_text("string"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::from_i64(0),
        ]]
    );
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
    assert!(conn.prepare("DELETE FROM pg_catalog.pg_settings").is_err());
}

#[turso_macros::test]
fn test_postgres_pg_namespace(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Switch to PostgreSQL dialect

    // Query pg_namespace virtual table
    let mut stmt = conn.prepare("SELECT * FROM pg_namespace").unwrap();

    // Should have at least pg_catalog and public namespaces
    let mut found_pg_catalog = false;
    let mut found_public = false;

    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(nspname) = row.get_value(1) {
                    if nspname.as_str() == "pg_catalog" {
                        found_pg_catalog = true;
                    } else if nspname.as_str() == "public" {
                        found_public = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert!(found_pg_catalog, "pg_catalog namespace not found");
    assert!(found_public, "public namespace not found");
}

#[turso_macros::test]
fn test_postgres_pg_class(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create a test table in SQLite dialect first
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    // Switch to PostgreSQL dialect

    // Query pg_class virtual table
    let mut stmt = conn
        .prepare("SELECT relname, relkind FROM pg_class WHERE relkind = 'r'")
        .unwrap();

    // Should see our users table (once we implement the mapping)
    let mut _found_users_table = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let (Value::Text(relname), Value::Text(relkind)) =
                    (row.get_value(0), row.get_value(1))
                {
                    if relname.as_str() == "users" && relkind.as_str() == "r" {
                        _found_users_table = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    // For now this won't find the users table as we haven't implemented
    // the actual mapping from sqlite_master to pg_class yet
    // This is just testing that the virtual table exists and can be queried
}

#[turso_macros::test]
fn test_postgres_pg_attribute(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Switch to PostgreSQL dialect

    // Query pg_attribute virtual table
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM pg_attribute").unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            if let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) {
                // For now should be 0 since we haven't implemented the mapping yet
                assert_eq!(*count, 0);
            }
        }
        _ => panic!("Expected row from COUNT query"),
    }
}

#[turso_macros::test]
fn test_postgres_pg_tables(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create test tables in SQLite dialect first
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
        .unwrap();

    // Switch to PostgreSQL dialect

    // Query pg_tables — the standard PG way to list tables
    let mut stmt = conn
        .prepare("SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public'")
        .unwrap();

    let mut found_users = false;
    let mut found_orders = false;

    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let Value::Text(schemaname) = row.get_value(0) else {
                    panic!("expected text for schemaname");
                };
                let Value::Text(tablename) = row.get_value(1) else {
                    panic!("expected text for tablename");
                };
                assert_eq!(schemaname.as_str(), "public");
                if tablename.as_str() == "users" {
                    found_users = true;
                } else if tablename.as_str() == "orders" {
                    found_orders = true;
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert!(found_users, "users table not found in pg_tables");
    assert!(found_orders, "orders table not found in pg_tables");
}

#[turso_macros::test]
fn test_postgres_pg_tables_no_internal_tables(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create a user table
    conn.execute("CREATE TABLE mydata (id INTEGER PRIMARY KEY)")
        .unwrap();

    // Switch to PostgreSQL dialect

    // pg_tables should not expose internal sqlite_* tables
    let mut stmt = conn.prepare("SELECT tablename FROM pg_tables").unwrap();

    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let Value::Text(tablename) = row.get_value(0) else {
                    panic!("expected text for tablename");
                };
                assert!(
                    !tablename.as_str().starts_with("sqlite_"),
                    "internal table {} should not appear in pg_tables",
                    tablename.as_str()
                );
            }
            StepResult::Done => break,
            _ => {}
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_type tests
// ──────────────────────────────────────────────────────────────────────

#[turso_macros::test]
fn test_pg_type_has_builtin_types(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Check well-known types exist with correct OIDs
    let cases = [("int4", 23), ("text", 25), ("bool", 16), ("uuid", 2950)];
    for (type_name, expected_oid) in cases {
        let mut stmt = conn
            .prepare(format!(
                "SELECT oid FROM pg_type WHERE typname = '{type_name}'"
            ))
            .unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let Value::Numeric(Numeric::Integer(oid)) = row.get_value(0) else {
                    panic!("expected integer oid for {type_name}");
                };
                assert_eq!(*oid, expected_oid, "wrong OID for {type_name}");
            }
            _ => panic!("{type_name} not found in pg_type"),
        }
    }
}

#[turso_macros::test]
fn test_pg_type_array_types(db: TempDatabase) {
    let conn = db.connect_postgres();

    // _int4 should exist with typelem pointing to int4 (oid=23)
    let mut stmt = conn
        .prepare("SELECT oid, typelem FROM pg_type WHERE typname = '_int4'")
        .unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Numeric(Numeric::Integer(oid)) = row.get_value(0) else {
                panic!("expected integer oid");
            };
            let Value::Numeric(Numeric::Integer(typelem)) = row.get_value(1) else {
                panic!("expected integer typelem");
            };
            assert_eq!(*oid, 1007, "_int4 should have oid 1007");
            assert_eq!(*typelem, 23, "_int4 typelem should be 23 (int4)");
        }
        _ => panic!("_int4 not found in pg_type"),
    }

    // _text should exist with typelem pointing to text (oid=25)
    let mut stmt = conn
        .prepare("SELECT typelem FROM pg_type WHERE typname = '_text'")
        .unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Numeric(Numeric::Integer(typelem)) = row.get_value(0) else {
                panic!("expected integer typelem");
            };
            assert_eq!(*typelem, 25, "_text typelem should be 25 (text)");
        }
        _ => panic!("_text not found in pg_type"),
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_index tests
// ──────────────────────────────────────────────────────────────────────

#[turso_macros::test]
fn test_pg_index_populated(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_items_name ON items(name)")
        .unwrap();

    // Join pg_index with pg_class to get index name
    let mut stmt = conn
        .prepare(
            "SELECT c.relname, i.indkey, i.indisunique, i.indisprimary
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indexrelid
             WHERE c.relname = 'idx_items_name'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(relname) = row.get_value(0) else {
                panic!("expected text relname");
            };
            let Value::Text(indkey) = row.get_value(1) else {
                panic!("expected text indkey");
            };
            let Value::Numeric(Numeric::Integer(indisunique)) = row.get_value(2) else {
                panic!("expected integer indisunique");
            };
            assert_eq!(relname.as_str(), "idx_items_name");
            // name is column 2 (1-based), so indkey should be "2"
            assert_eq!(
                indkey.as_str(),
                "2",
                "indkey should be 2 (name is 2nd column)"
            );
            assert_eq!(*indisunique, 0, "non-unique index");
        }
        _ => panic!("idx_items_name not found in pg_index join pg_class"),
    }
}

#[turso_macros::test]
fn test_pg_index_primary_key(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE pk_test (a TEXT, b TEXT, PRIMARY KEY (a, b))")
        .unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT i.indisprimary, i.indisunique, i.indkey
             FROM pg_index i
             JOIN pg_class ct ON ct.oid = i.indrelid
             WHERE ct.relname = 'pk_test' AND i.indisprimary = 1",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Numeric(Numeric::Integer(indisprimary)) = row.get_value(0) else {
                panic!("expected integer");
            };
            let Value::Numeric(Numeric::Integer(indisunique)) = row.get_value(1) else {
                panic!("expected integer");
            };
            let Value::Text(indkey) = row.get_value(2) else {
                panic!("expected text indkey");
            };
            assert_eq!(*indisprimary, 1);
            assert_eq!(*indisunique, 1);
            assert_eq!(indkey.as_str(), "1 2", "PK columns a=1, b=2");
        }
        _ => panic!("primary key index not found for pk_test"),
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_constraint tests
// ──────────────────────────────────────────────────────────────────────

#[turso_macros::test]
fn test_pg_constraint_pk_and_fk(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.pragma_update("foreign_keys", "ON").unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();

    // Check PK constraint on parent
    let mut stmt = conn
        .prepare(
            "SELECT conname, contype FROM pg_constraint
             JOIN pg_class c ON c.oid = conrelid
             WHERE c.relname = 'parent' AND contype = 'p'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(conname) = row.get_value(0) else {
                panic!("expected text conname");
            };
            let Value::Text(contype) = row.get_value(1) else {
                panic!("expected text contype");
            };
            assert_eq!(conname.as_str(), "parent_pkey");
            assert_eq!(contype.as_str(), "p");
        }
        _ => panic!("PK constraint not found for parent table"),
    }

    // Check FK constraint on child
    let mut stmt = conn
        .prepare(
            "SELECT conname, contype, confdeltype FROM pg_constraint
             JOIN pg_class c ON c.oid = conrelid
             WHERE c.relname = 'child' AND contype = 'f'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(conname) = row.get_value(0) else {
                panic!("expected text conname");
            };
            let Value::Text(contype) = row.get_value(1) else {
                panic!("expected text contype");
            };
            let Value::Text(confdeltype) = row.get_value(2) else {
                panic!("expected text confdeltype");
            };
            assert!(
                conname.as_str().contains("fkey"),
                "FK name should contain 'fkey'"
            );
            assert_eq!(contype.as_str(), "f");
            assert_eq!(confdeltype.as_str(), "c", "ON DELETE CASCADE = 'c'");
        }
        _ => panic!("FK constraint not found for child table"),
    }
}

#[turso_macros::test]
fn test_pg_constraint_check(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE checked (id INTEGER, val INTEGER CHECK(val > 0))")
        .unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT contype, conbin FROM pg_constraint
             JOIN pg_class c ON c.oid = conrelid
             WHERE c.relname = 'checked' AND contype = 'c'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(contype) = row.get_value(0) else {
                panic!("expected text contype");
            };
            assert_eq!(contype.as_str(), "c", "should be CHECK constraint");
            // conbin should contain the check expression
            if let Value::Text(conbin) = row.get_value(1) {
                assert!(
                    conbin.as_str().contains("val") && conbin.as_str().contains("0"),
                    "conbin should reference val and 0, got: {}",
                    conbin.as_str()
                );
            }
        }
        _ => panic!("CHECK constraint not found for checked table"),
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_class index row tests
// ──────────────────────────────────────────────────────────────────────

#[turso_macros::test]
fn test_pg_class_includes_indexes(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE indexed (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_indexed_data ON indexed(data)")
        .unwrap();

    // pg_class should have relkind='i' rows for indexes
    let mut stmt = conn
        .prepare(
            "SELECT relname, relkind, relam FROM pg_class
             WHERE relname = 'idx_indexed_data' AND relkind = 'i'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(relname) = row.get_value(0) else {
                panic!("expected text");
            };
            let Value::Text(relkind) = row.get_value(1) else {
                panic!("expected text");
            };
            let Value::Numeric(Numeric::Integer(relam)) = row.get_value(2) else {
                panic!("expected integer relam");
            };
            assert_eq!(relname.as_str(), "idx_indexed_data");
            assert_eq!(relkind.as_str(), "i");
            assert_eq!(*relam, 403, "index relam should be 403 (btree)");
        }
        _ => panic!("idx_indexed_data not found in pg_class with relkind='i'"),
    }
}

/// Test that schema-qualified pg_catalog references work (e.g. `pg_catalog.pg_class`).
/// psql's `\dt` command sends queries like:
///   SELECT ... FROM pg_catalog.pg_class c
///     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
///   WHERE ... AND pg_catalog.pg_table_is_visible(c.oid)
/// This must not fail with "no such database: pg_catalog".
#[turso_macros::test]
fn test_pg_catalog_schema_qualified_tables(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();

    // pg_catalog.pg_class — the core of psql \dt
    let mut stmt = conn
        .prepare("SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relkind = 'r'")
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(0) {
                    if name.as_str() == "widgets" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "widgets table not found via pg_catalog.pg_class");
    drop(stmt);

    // pg_catalog.pg_namespace
    let mut stmt = conn
        .prepare("SELECT nspname FROM pg_catalog.pg_namespace")
        .unwrap();
    let mut found_public = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(ns) = row.get_value(0) {
                    if ns.as_str() == "public" {
                        found_public = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        found_public,
        "public namespace not found via pg_catalog.pg_namespace"
    );
    drop(stmt);

    // JOIN across schema-qualified catalog tables (simplified \dt query)
    let mut stmt = conn
        .prepare(
            "SELECT n.nspname, c.relname \
             FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind = 'r'",
        )
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(1) {
                    if name.as_str() == "widgets" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        found,
        "widgets not found via pg_catalog.pg_class JOIN pg_catalog.pg_namespace"
    );
}

/// Test that `public.tablename` also resolves correctly (not as an ATTACH db).
#[turso_macros::test]
fn test_public_schema_qualified_tables(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE gadgets (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO gadgets (id, name) VALUES (1, 'phone')")
        .unwrap();

    let mut stmt = conn
        .prepare("SELECT name FROM public.gadgets WHERE id = 1")
        .unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "phone");
        }
        _ => panic!("expected row from public.gadgets"),
    }
}

#[turso_macros::test]
fn test_format_type_expanded(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Basic types
    let cases = vec![
        (16, "boolean"),
        (23, "integer"),
        (25, "text"),
        (114, "json"),
        (3802, "jsonb"),
        (2950, "uuid"),
        (1082, "date"),
        (1114, "timestamp without time zone"),
        (1184, "timestamp with time zone"),
        (1186, "interval"),
        (2278, "void"),
        (2205, "regclass"),
        (2206, "regtype"),
        (1000, "boolean[]"),
        (1007, "integer[]"),
        (1009, "text[]"),
    ];

    for (oid, expected) in cases {
        let mut stmt = conn
            .prepare(format!("SELECT format_type({oid}, -1)"))
            .unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    row.get_value(0).to_string(),
                    expected,
                    "format_type({oid}, -1) should return '{expected}'"
                );
            }
            _ => panic!("expected row for format_type({oid}, -1)"),
        }
    }

    // varchar with typemod
    let mut stmt = conn.prepare("SELECT format_type(1043, 54)").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "character varying(50)");
        }
        _ => panic!("expected row for format_type with typemod"),
    }
}

#[turso_macros::test]
fn test_pg_type_is_visible(db: TempDatabase) {
    let conn = db.connect_postgres();

    let mut stmt = conn.prepare("SELECT pg_type_is_visible(23)").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(*row.get_value(0), Value::from_i64(1));
        }
        _ => panic!("expected row"),
    }
}

#[turso_macros::test]
fn test_lpad_rpad(db: TempDatabase) {
    let conn = db.connect_postgres();

    // lpad with fill char
    let mut stmt = conn.prepare("SELECT lpad('hi', 5, '*')").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "***hi");
        }
        _ => panic!("expected row"),
    }
    drop(stmt);

    // rpad with fill char
    let mut stmt = conn.prepare("SELECT rpad('hi', 5, '-')").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "hi---");
        }
        _ => panic!("expected row"),
    }
    drop(stmt);

    // lpad with default space fill
    let mut stmt = conn.prepare("SELECT lpad('hi', 5)").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "   hi");
        }
        _ => panic!("expected row"),
    }
    drop(stmt);

    // Truncation when string is longer than length
    let mut stmt = conn.prepare("SELECT lpad('hello world', 5)").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "hello");
        }
        _ => panic!("expected row"),
    }
}

#[turso_macros::test]
fn test_pg_get_constraintdef(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE, age INTEGER CHECK(age > 0))")
        .unwrap();

    // Collect all constraint definitions
    let mut stmt = conn
        .prepare("SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint")
        .unwrap();
    let mut defs: Vec<(String, String)> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let name = row.get_value(0).to_string();
                let def = row.get_value(1).to_string();
                defs.push((name, def));
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    // Check we got real definitions, not NULLs
    assert!(
        !defs.is_empty(),
        "expected constraint definitions, got: {defs:?}"
    );
    for (name, def) in &defs {
        assert!(
            !def.is_empty(),
            "constraint '{name}' should have a definition"
        );
    }

    // Find a PK constraint
    let has_pk = defs.iter().any(|(_, d)| d.starts_with("PRIMARY KEY"));
    assert!(
        has_pk,
        "should have a PRIMARY KEY constraint, got: {defs:?}"
    );

    // Find the FK constraint
    let has_fk = defs
        .iter()
        .any(|(_, d)| d.contains("FOREIGN KEY") && d.contains("REFERENCES"));
    assert!(
        has_fk,
        "should have a FOREIGN KEY constraint, got: {defs:?}"
    );
}

#[turso_macros::test]
fn test_pg_get_indexdef(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
        .unwrap();
    conn.execute("CREATE INDEX idx_items_name ON items(name)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_items_price ON items(price)")
        .unwrap();

    // Get index definitions via pg_class (indexes have relkind='i')
    let mut stmt = conn
        .prepare("SELECT pg_get_indexdef(oid) FROM pg_class WHERE relkind = 'i'")
        .unwrap();
    let mut defs: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let def = row.get_value(0).to_string();
                defs.push(def);
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert!(!defs.is_empty(), "expected index definitions");

    let has_name_idx = defs
        .iter()
        .any(|d| d.contains("idx_items_name") && d.contains("items") && d.contains("name"));
    assert!(has_name_idx, "should have idx_items_name definition");

    let has_unique_idx = defs
        .iter()
        .any(|d| d.contains("UNIQUE") && d.contains("idx_items_price"));
    assert!(
        has_unique_idx,
        "should have UNIQUE idx_items_price definition"
    );
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

#[turso_macros::test]
fn test_pg_attrdef_populated(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE defaults_test (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unnamed', score INTEGER DEFAULT 0)")
        .unwrap();

    let mut stmt = conn.prepare("SELECT adnum, adbin FROM pg_attrdef").unwrap();
    let mut rows: Vec<(i64, String)> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let adnum = match row.get_value(0) {
                    Value::Numeric(Numeric::Integer(n)) => *n,
                    _ => panic!("expected integer adnum"),
                };
                let adbin = row.get_value(1).to_string();
                rows.push((adnum, adbin));
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert!(
        rows.len() >= 2,
        "expected at least 2 default values, got {}",
        rows.len()
    );
}

// ---------------------------------------------------------------------------
// Tests for tables created in PG mode (not SQLite mode).
// These catch regressions where PG CREATE TABLE compiles but the bytecode
// is never executed (e.g. when DDL statements with 0 result columns are
// not stepped through).
// ---------------------------------------------------------------------------

#[turso_macros::test]
fn test_pg_create_table_visible_in_pg_tables(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create table purely in PG mode
    conn.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
        .unwrap();

    // Verify it appears in pg_tables
    let mut stmt = conn
        .prepare("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(0) {
                    if name.as_str() == "items" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "table created in PG mode not found in pg_tables");
}

#[turso_macros::test]
fn test_pg_create_table_visible_in_pg_class(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE widgets (id INT, label TEXT)")
        .unwrap();

    let mut stmt = conn
        .prepare("SELECT relname FROM pg_class WHERE relkind = 'r'")
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(0) {
                    if name.as_str() == "widgets" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "table created in PG mode not found in pg_class");
}

#[turso_macros::test]
fn test_pg_create_table_columns_in_pg_attribute(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)")
        .unwrap();

    // Join pg_attribute + pg_class + pg_type to get column info (same query tursopg \d uses)
    let mut stmt = conn
        .prepare(
            "SELECT a.attname, t.typname \
             FROM pg_attribute a \
             JOIN pg_class c ON a.attrelid = c.oid \
             JOIN pg_type t ON a.atttypid = t.oid \
             WHERE c.relname = 'products' AND a.attnum > 0 AND a.attisdropped = 0 \
             ORDER BY a.attnum",
        )
        .unwrap();

    let mut columns = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let name = row.get_value(0).to_string();
                let typ = row.get_value(1).to_string();
                columns.push((name, typ));
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert_eq!(columns.len(), 3, "expected 3 columns, got {columns:?}");
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[1].0, "name");
    assert_eq!(columns[1].1, "text");
    assert_eq!(columns[2].0, "price");
}

#[turso_macros::test]
fn test_pg_create_table_then_insert_and_select(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE kv (k TEXT, v INT)").unwrap();
    conn.execute("INSERT INTO kv VALUES ('hello', 42)").unwrap();

    let mut stmt = conn.prepare("SELECT k, v FROM kv").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(k) = row.get_value(0) else {
                panic!("expected text");
            };
            assert_eq!(k.as_str(), "hello");
            let Value::Numeric(Numeric::Integer(v)) = row.get_value(1) else {
                panic!("expected integer");
            };
            assert_eq!(*v, 42);
        }
        _ => panic!("expected a row"),
    }
    assert!(
        matches!(stmt.step().unwrap(), StepResult::Done),
        "expected exactly one row"
    );
}

#[turso_macros::test]
fn test_pg_create_table_in_pg_database(db: TempDatabase) {
    let conn = db.connect_postgres();

    // pg_database should return at least one row
    let mut stmt = conn.prepare("SELECT datname FROM pg_database").unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                found = true;
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "pg_database should return at least one row");
}
