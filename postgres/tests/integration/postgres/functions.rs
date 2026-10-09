use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};
use turso_pg::PgConnection;

#[turso_macros::test(mvcc)]
fn test_pg_ordered_array_aggregate(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE ordered_items (g INTEGER, v TEXT, k INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO ordered_items VALUES (1,'nine',9), (2,'other',2), (1,NULL,4), (1,'two',2), (1,'last',NULL), (2,'first',1)")
        .unwrap();
    assert_eq!(
        conn.prepare(
            "SELECT array_to_string(array_agg(v ORDER BY k), '|', '<null>'),
            array_to_string(array_agg(v ORDER BY k DESC), '|', '<null>'),
            array_to_string(array_agg(v ORDER BY k DESC NULLS LAST), '|', '<null>')
            FROM ordered_items WHERE g = 1"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![
            Value::build_text("two|<null>|nine|last"),
            Value::build_text("last|nine|<null>|two"),
            Value::build_text("nine|<null>|two|last")
        ]]
    );
    for indexed in [false, true] {
        if indexed {
            conn.execute("CREATE INDEX ordered_groups ON ordered_items(g)")
                .unwrap();
        }
        assert_eq!(
            conn.prepare("SELECT g, array_to_string(array_agg(v ORDER BY k NULLS FIRST) FILTER (WHERE k <> 4 OR k IS NULL), '|', '<null>')
                FROM ordered_items GROUP BY g ORDER BY g")
                .unwrap().run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(1), Value::build_text("last|two|nine")],
                vec![Value::from_i64(2), Value::build_text("first|other")]]
        );
    }
    assert_eq!(
        query_text(
            &conn,
            "SELECT array_to_string(array_agg(v ORDER BY k), '|') FROM ordered_items WHERE g = 3"
        ),
        vec!["NULL"]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_ordered_array_aggregate_correlated_and_collated(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE ordered_words (g INTEGER, v TEXT, k TEXT)")
        .unwrap();
    conn.execute("INSERT INTO ordered_words VALUES (1,'lower','a'), (1,'upper','B'), (1,'tie','a'), (2,'other','z')").unwrap();
    assert_eq!(query_text(&conn,
        "SELECT array_to_string(array_agg(v ORDER BY k COLLATE NOCASE, v DESC), '|') FROM ordered_words WHERE g=1"),
        vec!["tie|lower|upper"]);
    assert_eq!(
        conn.prepare("SELECT g, (SELECT array_to_string(array_agg(v ORDER BY k, v DESC), '|') FROM ordered_words i WHERE i.g = src.g)
            FROM (SELECT 2 AS g UNION ALL SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT 2) src")
            .unwrap().run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(2), Value::build_text("other")],
            vec![Value::from_i64(1), Value::build_text("upper|tie|lower")],
            vec![Value::from_i64(3), Value::Null],
            vec![Value::from_i64(2), Value::build_text("other")]]
    );
    for sql in [
        "SELECT array_agg(DISTINCT v ORDER BY v) FROM ordered_words",
        "SELECT array_agg(v ORDER BY k) OVER () FROM ordered_words",
        "SELECT string_agg(v, ',' ORDER BY k) FROM ordered_words",
        "SELECT array_agg(v ORDER BY ARRAY[1,2]) FROM ordered_words",
    ] {
        assert!(conn.prepare(sql).is_err(), "{sql}");
    }
}

#[test]
fn test_pg_function_column_alias_view_survives_reopen() {
    for mvcc in [false, true] {
        let db = TempDatabase::builder()
            .with_views(true)
            .with_mvcc(mvcc)
            .build();
        let conn = db.connect_postgres();
        conn.execute(
            "CREATE VIEW public.dump_values AS
            SELECT v.value FROM pg_catalog.unnest(ARRAY[9,2]) AS v(value)",
        )
        .unwrap();
        let expected = vec![vec![Value::from_i64(9)], vec![Value::from_i64(2)]];
        assert_eq!(
            conn.prepare("SELECT * FROM public.dump_values")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            expected
        );
        let path = db.path.clone();
        let io = db.io.clone();
        conn.close().unwrap();
        drop(conn);
        drop(db);
        let reopened = turso_pg::open_database_with_io(
            io,
            path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new()
                .with_views(true)
                .with_custom_types(true),
        )
        .unwrap();
        let conn = PgConnection::connect(&reopened).unwrap();
        assert_eq!(
            conn.prepare("SELECT * FROM public.dump_values")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            expected
        );
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_array_casts_preserve_elements(db: TempDatabase) {
    let conn = db.connect_postgres();
    for (input, ty, expected) in [
        ("'{17,NULL,2}'", "pg_catalog.oid[]", "17|<null>|2"),
        ("'{17,NULL,hello}'", "text[]", "17|<null>|hello"),
        ("'{3.5,2.25}'", "float8[]", "3.5|2.25"),
        ("'{}'", "integer[]", ""),
        ("ARRAY[17, 2]", "integer[]", "17|2"),
        ("'{t,NULL,f}'", "boolean[]", "1|<null>|0"),
    ] {
        assert_eq!(
            query_text(
                &conn,
                &format!("SELECT array_to_string({input}::{ty}, '|', '<null>')")
            ),
            vec![expected],
            "{input}::{ty}"
        );
    }
    assert_eq!(
        conn.prepare("SELECT NULL::integer[], NULL::boolean[]")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::Null, Value::Null]]
    );
    assert!(conn
        .prepare("SELECT '{hello}'::integer[]")
        .unwrap()
        .run_collect_rows()
        .is_err());
}

#[turso_macros::test(mvcc)]
fn test_pg_unnest_preserves_values_and_aliases(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for input in ["ARRAY[17, NULL, 2]", "'{17,NULL,2}'::pg_catalog.oid[]"] {
        assert_eq!(
            conn.prepare(format!("SELECT x FROM pg_catalog.unnest({input}) AS t(x)"))
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![
                vec![Value::from_i64(17)],
                vec![Value::Null],
                vec![Value::from_i64(2)],
            ]
        );
    }
    for input in ["NULL", "ARRAY[]", "'{}'::pg_catalog.oid[]"] {
        assert!(conn
            .prepare(format!("SELECT * FROM unnest({input})"))
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .is_empty());
    }
    assert_eq!(
        conn.prepare("SELECT a, b FROM pg_options_to_table(ARRAY['z=17']) AS t(a, b)")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::build_text("z"), Value::build_text("17")]]
    );
    assert_eq!(
        conn.prepare(
            "SELECT a.x, a.option_value, b.option_name
            FROM pg_options_to_table(ARRAY['z=17']) AS a(x),
                 pg_options_to_table(ARRAY['y=9']) AS b"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![
            Value::build_text("z"),
            Value::build_text("17"),
            Value::build_text("y")
        ]]
    );
    conn.execute("CREATE TABLE public.unnest_probe (v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO public.unnest_probe VALUES (17), (2)")
        .unwrap();
    for query in [
        "SELECT p.v FROM unnest('{17}'::pg_catalog.oid[]) AS src(tbloid)
            JOIN public.unnest_probe p ON src.tbloid = p.v",
        "SELECT p.v FROM public.unnest_probe p
            JOIN unnest('{17}'::pg_catalog.oid[]) AS src(tbloid) ON src.tbloid = p.v",
    ] {
        assert_eq!(
            conn.prepare(query).unwrap().run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(17)]]
        );
    }
    conn.execute("CREATE TABLE public.unnest_sets (id INTEGER, vals INTEGER[])")
        .unwrap();
    conn.execute(
        "INSERT INTO public.unnest_sets VALUES (1, ARRAY[17, 2]), (2, ARRAY[9]), (3, ARRAY[])",
    )
    .unwrap();
    assert_eq!(
        conn.prepare(
            "SELECT _turso_function_rows.id, t.x FROM public.unnest_sets _turso_function_rows,
            unnest(_turso_function_rows.vals) AS t(x) ORDER BY 1, 2"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![Value::from_i64(1), Value::from_i64(2)],
            vec![Value::from_i64(1), Value::from_i64(17)],
            vec![Value::from_i64(2), Value::from_i64(9)],
        ]
    );
    assert_eq!(
        conn.prepare("SELECT t.x, u.unnest FROM unnest(ARRAY[17]) AS t(x), unnest(ARRAY[2]) u")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(17), Value::from_i64(2)]]
    );
    assert!(conn
        .prepare("SELECT * FROM unnest(17)")
        .unwrap()
        .run_collect_rows()
        .is_err());
    assert!(conn
        .prepare("SELECT * FROM unnest(ARRAY[17]) AS t(a, b)")
        .is_err());
    assert!(conn.prepare("SELECT e.tableoid, e.oid, evtname, evtenabled, evtevent, evtowner,
        array_to_string(array(SELECT quote_literal(x) FROM unnest(evttags) AS t(x)), ', ') AS evttags,
        e.evtfoid::regproc AS evtfname FROM pg_event_trigger e ORDER BY e.oid")
        .unwrap().run_collect_rows().unwrap().is_empty());
}

#[turso_macros::test(mvcc)]
fn test_pg_array_subquery_preserves_query_results(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE array_items (v INTEGER, group_id INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO array_items VALUES (9, 1), (2, 2), (17, 1), (NULL, 1)")
        .unwrap();
    for (query, expected) in [
        (
            "SELECT v AS number FROM array_items WHERE group_id = 1
             ORDER BY number DESC NULLS LAST LIMIT 3",
            "17|9|<null>",
        ),
        (
            "SELECT DISTINCT group_id FROM array_items ORDER BY group_id DESC",
            "2|1",
        ),
        ("SELECT 17 UNION ALL SELECT 2 ORDER BY 1 DESC", "17|2"),
        ("SELECT v FROM array_items WHERE group_id = 3", ""),
        (
            "WITH _turso_array_rows AS (SELECT 29 AS v) SELECT v FROM _turso_array_rows",
            "29",
        ),
    ] {
        assert_eq!(
            query_text(
                &conn,
                &format!("SELECT array_to_string(ARRAY({query}), '|', '<null>')")
            ),
            vec![expected],
            "{query}"
        );
    }
    assert_eq!(
        query_text(
            &conn,
            "WITH _turso_array_rows AS (SELECT 29 AS v)
        SELECT array_to_string(ARRAY(SELECT v FROM _turso_array_rows), '|')"
        ),
        vec!["29"]
    );
    assert_eq!(
        conn.prepare(
            "SELECT g.group_id, array_to_string(
                ARRAY(SELECT v FROM array_items i WHERE i.group_id = g.group_id
                      ORDER BY v NULLS LAST), '|', '<null>')
             FROM (SELECT 1 AS group_id UNION ALL SELECT 2 UNION ALL SELECT 3) g
             ORDER BY g.group_id",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![
            vec![Value::from_i64(1), Value::build_text("9|17|<null>")],
            vec![Value::from_i64(2), Value::build_text("2")],
            vec![Value::from_i64(3), Value::build_text("")],
        ]
    );
    assert!(conn
        .prepare("SELECT ARRAY(SELECT v, group_id FROM array_items)")
        .is_err());
}

#[turso_macros::test(mvcc)]
fn test_pg_options_to_table_reads_options(db: TempDatabase) {
    let conn = db.connect_postgres();
    for input in [
        "ARRAY['x=a=b', 'z=', 'flag', 'space=two words']",
        "'{\"x=a=b\",\"z=\",\"flag\",\"space=two words\"}'",
    ] {
        assert_eq!(
            conn.prepare(format!(
                "SELECT option_name, option_value FROM pg_catalog.pg_options_to_table({input})"
            ))
            .unwrap()
            .run_collect_rows()
            .unwrap(),
            vec![
                vec![Value::build_text("x"), Value::build_text("a=b")],
                vec![Value::build_text("z"), Value::build_text("")],
                vec![Value::build_text("flag"), Value::Null],
                vec![Value::build_text("space"), Value::build_text("two words")],
            ]
        );
    }
    for input in ["NULL", "ARRAY[]", "'{}'"] {
        assert!(conn
            .prepare(format!("SELECT * FROM pg_options_to_table({input})"))
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .is_empty());
    }
    for input in ["'not an array'", "ARRAY[NULL]", "17"] {
        assert!(conn
            .prepare(format!("SELECT * FROM pg_options_to_table({input})"))
            .unwrap()
            .run_collect_rows()
            .is_err());
    }
    assert_eq!(
        query_text(
            &conn,
            "SELECT array_to_string(ARRAY(
                SELECT quote_ident(option_name) || ' ' || quote_literal(option_value)
                FROM pg_options_to_table(ARRAY['z=a=b', 'a=two words']) ORDER BY option_name
             ), ', ')"
        ),
        vec!["a 'two words', z 'a=b'"]
    );
    conn.execute("CREATE TABLE option_sets (id INTEGER, options TEXT[])")
        .unwrap();
    conn.execute(
        "INSERT INTO option_sets VALUES (1, ARRAY['x=9']), (2, ARRAY['x=17']), (3, ARRAY[])",
    )
    .unwrap();
    assert_eq!(conn.prepare(
        "SELECT s.id, p.option_value FROM option_sets s, pg_options_to_table(s.options) p ORDER BY s.id"
    ).unwrap().run_collect_rows().unwrap(), vec![
        vec![Value::from_i64(1), Value::build_text("9")],
        vec![Value::from_i64(2), Value::build_text("17")],
    ]);
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert!(conn.prepare(
        "SELECT tableoid, oid, fdwname, fdwowner, fdwhandler::pg_catalog.regproc,
                fdwvalidator::pg_catalog.regproc, fdwacl, acldefault('F', fdwowner) AS acldefault,
                array_to_string(ARRAY(SELECT quote_ident(option_name) || ' ' || quote_literal(option_value)
                    FROM pg_options_to_table(fdwoptions) ORDER BY option_name), E',\n    ') AS fdwoptions
         FROM pg_foreign_data_wrapper"
    ).unwrap().run_collect_rows().unwrap().is_empty());
}

fn query_text(conn: &PgConnection, sql: &str) -> Vec<String> {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let mut result = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                match row.get_value(0) {
                    Value::Text(v) => result.push(v.value.to_string()),
                    Value::Null => result.push("NULL".to_string()),
                    other => panic!("expected text, got {other:?}"),
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    result
}

fn query_integer(conn: &PgConnection, sql: &str) -> Vec<i64> {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let mut result = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                match row.get_value(0) {
                    Value::Numeric(Numeric::Integer(v)) => result.push(*v),
                    other => panic!("expected integer, got {other:?}"),
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    result
}

#[turso_macros::test(mvcc)]
fn test_pg_set_config_shares_connection_state(db: TempDatabase) {
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    conn.execute("CREATE TABLE config_items (v INT)").unwrap();
    conn.execute("INSERT INTO config_items VALUES (17)")
        .unwrap();
    assert_eq!(
        query_text(&conn, "SELECT set_config('search_path', 'missing', false)"),
        ["missing"]
    );
    assert!(conn.prepare("SELECT v FROM config_items").is_err());
    assert_eq!(query_integer(&other, "SELECT v FROM config_items"), [17]);

    let clone = conn.clone();
    clone.execute("SET search_path TO public").unwrap();
    assert_eq!(query_integer(&conn, "SELECT v FROM config_items"), [17]);
    assert_eq!(
        conn.inner()
            .prepare("SELECT set_config('search_path', 'missing', false)")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::build_text("missing")]]
    );
    assert!(clone.prepare("SELECT v FROM config_items").is_err());
    assert_eq!(query_integer(&other, "SELECT v FROM config_items"), [17]);
}

#[turso_macros::test(mvcc)]
fn test_pg_set_config_empty_and_quoted_search_paths(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE config_items (v INT)").unwrap();
    conn.execute("INSERT INTO config_items VALUES (17)")
        .unwrap();
    assert_eq!(
        query_text(
            &conn,
            "SELECT set_config('search_path', ' \"missing, public\", PUBLIC ', false)"
        ),
        [" \"missing, public\", PUBLIC "]
    );
    assert_eq!(query_integer(&conn, "SELECT v FROM config_items"), [17]);
    assert_eq!(
        query_text(
            &conn,
            "SELECT set_config('search_path', '\"public, missing\"', false)"
        ),
        ["\"public, missing\""]
    );
    assert!(conn.prepare("SELECT v FROM config_items").is_err());
    assert_eq!(
        query_text(
            &conn,
            "SELECT pg_catalog.set_config('search_path', '', false)"
        ),
        [""]
    );
    assert!(conn.prepare("SELECT v FROM config_items").is_err());
    assert_eq!(
        query_integer(&conn, "SELECT v FROM public.config_items"),
        [17]
    );
    assert_eq!(
        query_integer(&conn, "SELECT COUNT(*) FROM pg_catalog.pg_namespace"),
        [3]
    );
    assert_eq!(
        query_integer(&conn, "SELECT COUNT(*) FROM pg_namespace"),
        [3]
    );
    conn.execute("SET search_path TO public").unwrap();
    assert_eq!(query_integer(&conn, "SELECT v FROM config_items"), [17]);
}

#[turso_macros::test(mvcc)]
fn test_pg_set_config_changes_settings_only_when_executed(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE config_items (v INT)").unwrap();
    conn.execute("INSERT INTO config_items VALUES (17)")
        .unwrap();
    let mut setter = conn
        .prepare("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(query_integer(&conn, "SELECT v FROM config_items"), [17]);
    assert_eq!(
        setter.run_collect_rows().unwrap(),
        vec![vec![Value::build_text("")]]
    );
    assert!(conn.prepare("SELECT v FROM config_items").is_err());
    conn.execute("SET search_path TO public").unwrap();
    setter.reset().unwrap();
    setter.run_ignore_rows().unwrap();
    assert!(conn.prepare("SELECT v FROM config_items").is_err());
    let mut setter = conn.prepare("SET search_path TO public").unwrap();
    assert!(conn.prepare("SELECT v FROM config_items").is_err());
    setter.run_ignore_rows().unwrap();
    assert_eq!(query_integer(&conn, "SELECT v FROM config_items"), [17]);
}

#[turso_macros::test(mvcc)]
fn test_pg_set_config_validates_arguments(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE config_items (v INT)").unwrap();
    conn.execute("INSERT INTO config_items VALUES (17)")
        .unwrap();
    for (sql, message) in [
        (
            "SELECT set_config('statement_timeout', '100', false)",
            "unrecognized configuration parameter",
        ),
        (
            "SELECT set_config(NULL, 'public', false)",
            "SET requires parameter name",
        ),
        (
            "SELECT set_config('search_path', 5, false)",
            "set_config requires a text value",
        ),
        (
            "SELECT set_config('search_path', '', 2)",
            "set_config requires a boolean",
        ),
        (
            "SELECT set_config('search_path', '', true)",
            "transaction-local settings are not supported",
        ),
        (
            "SET LOCAL search_path TO missing",
            "transaction-local settings are not supported",
        ),
        (
            "SELECT set_config('search_path', 'public,', false)",
            "invalid value for parameter",
        ),
        (
            "SELECT set_config('search_path', '\"unclosed', false)",
            "invalid value for parameter",
        ),
        (
            "SELECT set_config('search_path', 'one two', false)",
            "invalid value for parameter",
        ),
    ] {
        let error = conn.execute(sql).unwrap_err().to_string();
        assert!(error.contains(message), "{sql}: {error}");
        assert_eq!(query_integer(&conn, "SELECT v FROM config_items"), [17]);
    }
    for sql in [
        "SELECT set_config('search_path', '')",
        "SELECT set_config('search_path', '', false, false)",
    ] {
        assert!(conn.prepare(sql).is_err(), "{sql}");
    }
    assert_eq!(
        query_text(&conn, "SELECT set_config('SEARCH_PATH', '', NULL)"),
        [""]
    );
    assert_eq!(
        query_text(&conn, "SELECT set_config('search_path', NULL, false)"),
        ["\"$user\", public"]
    );
    assert_eq!(query_integer(&conn, "SELECT v FROM config_items"), [17]);
}

#[turso_macros::test(mvcc)]
fn test_pg_is_in_recovery_returns_temporary_true_text(db: TempDatabase) {
    let conn = db.connect_postgres();
    assert_eq!(
        query_text(
            &conn,
            "SELECT pg_catalog.set_config('search_path', '', false)"
        ),
        [""]
    );
    for sql in [
        "SELECT pg_catalog.pg_is_in_recovery()",
        "SELECT pg_is_in_recovery() AS recovery",
    ] {
        assert_eq!(query_text(&conn, sql), ["t"]);
    }
    assert!(conn.prepare("SELECT pg_is_in_recovery(1)").is_err());

    let raw = db.connect_limbo();
    assert_eq!(
        raw.prepare("SELECT pg_is_in_recovery()")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::build_text("t")]]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_acl_default_matches_postgres_defaults(db: TempDatabase) {
    let conn = db.connect_postgres();
    for (kind, expected) in [
        ("c", "{}"),
        ("n", "{turso=UC/turso}"),
        ("r", "{turso=arwdDxt/turso}"),
        ("s", "{turso=rwU/turso}"),
        ("d", "{=Tc/turso,turso=CTc/turso}"),
        ("f", "{=X/turso,turso=X/turso}"),
        ("F", "{turso=U/turso}"),
        ("S", "{turso=U/turso}"),
        ("l", "{=U/turso,turso=U/turso}"),
        ("L", "{turso=rw/turso}"),
        ("p", "{turso=sA/turso}"),
        ("t", "{turso=C/turso}"),
        ("T", "{=U/turso,turso=U/turso}"),
    ] {
        assert_eq!(
            query_text(&conn, &format!("SELECT acldefault('{kind}', 10)")),
            [expected],
            "object kind {kind}"
        );
    }
    assert_eq!(
        query_text(&conn, "SELECT acldefault('f', 4294967294)"),
        ["{=X/4294967294,4294967294=X/4294967294}"]
    );
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    assert_eq!(
        conn.prepare(
            "SELECT n.tableoid, n.oid, n.nspname, n.nspowner, n.nspacl,
                             acldefault('n', n.nspowner) AS acldefault FROM pg_namespace n
                      WHERE n.nspname = 'public'"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![
            Value::from_i64(2615),
            Value::from_i64(2200),
            Value::build_text("public"),
            Value::from_i64(10),
            Value::Null,
            Value::build_text("{turso=UC/turso}"),
        ]]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_acl_default_null_and_invalid_arguments(db: TempDatabase) {
    let conn = db.connect_postgres();
    for sql in [
        "SELECT acldefault(NULL, 10)",
        "SELECT acldefault('r', NULL)",
    ] {
        assert_eq!(query_text(&conn, sql), ["NULL"], "{sql}");
    }

    for kind in ["?", "", "rr", "r ", " r", "R", "Table", "é"] {
        let error = conn
            .execute(format!("SELECT acldefault('{kind}', 10)"))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("unrecognized object type abbreviation: {kind}")),
            "object kind {kind:?}: {error}"
        );
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_version_is_client_parseable(db: TempDatabase) {
    let conn = db.connect_postgres();

    for sql in [
        "SELECT version()",
        "SELECT pg_catalog.version()",
        "SELECT * FROM version()",
    ] {
        let out = query_text(&conn, sql);
        assert_eq!(out.len(), 1, "{sql} should return one row");
        let version = &out[0];
        // drivers like knex and TypeORM regex-parse this output as /^PostgreSQL ([\d.]+)/,
        // so the string must lead with "PostgreSQL <numeric version>".
        let rest = version
            .strip_prefix("PostgreSQL ")
            .unwrap_or_else(|| panic!("version() must start with 'PostgreSQL ': {version}"));
        let number = rest.split(' ').next().unwrap();
        assert!(
            !number.is_empty() && number.chars().all(|c| c.is_ascii_digit() || c == '.'),
            "version() must lead with a numeric version: {version}"
        );
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_current_database_matches_pg_database(db: TempDatabase) {
    let conn = db.connect_postgres();

    let expected = db.path.file_stem().unwrap().to_str().unwrap().to_string();
    assert_eq!(
        query_text(&conn, "SELECT current_database()"),
        [expected.clone()]
    );
    assert_eq!(
        query_text(&conn, "SELECT * FROM current_database()"),
        [expected.clone()]
    );
    // The bare current_catalog keyword is the same function per PG docs.
    assert_eq!(
        query_text(&conn, "SELECT current_catalog"),
        [expected.clone()]
    );
    // Must agree with what the pg_database catalog reports as datname.
    assert_eq!(
        query_text(&conn, "SELECT datname FROM pg_catalog.pg_database"),
        [expected]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_current_schema_all_syntactic_forms(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Function call, bare SQLValueFunction keyword,
    // function-in-FROM-position form must all resolve.
    assert_eq!(query_text(&conn, "SELECT current_schema()"), ["public"]);
    assert_eq!(query_text(&conn, "SELECT current_schema"), ["public"]);
    assert_eq!(
        query_text(&conn, "SELECT * FROM current_schema()"),
        ["public"]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_derived_result_column_names(db: TempDatabase) {
    let conn = db.connect_postgres();

    // PostgreSQL names unaliased function-call and keyword columns after the
    // function
    for (sql, expected) in [
        ("SELECT version()", "version"),
        ("SELECT pg_catalog.version()", "version"),
        ("SELECT current_schema()", "current_schema"),
        ("SELECT current_schema", "current_schema"),
        ("SELECT current_catalog", "current_catalog"),
        ("SELECT current_user", "current_user"),
        ("SELECT session_user", "session_user"),
        ("SELECT * FROM current_schema()", "current_schema"),
        ("SELECT * FROM current_schema() AS cs", "cs"),
        ("SELECT count(*) FROM pg_catalog.pg_database", "count"),
        ("SELECT version() AS v", "v"),
    ] {
        let stmt = conn.prepare(sql).unwrap();
        assert_eq!(stmt.get_column_name(0), expected, "{sql}");
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_backend_pid_is_positive_integer(db: TempDatabase) {
    let conn = db.connect_postgres();

    let pids = query_integer(&conn, "SELECT pg_backend_pid()");
    assert_eq!(pids.len(), 1);
    assert!(pids[0] > 0, "pg_backend_pid() must be a positive pid");
}

#[turso_macros::test(mvcc)]
fn test_pg_native_function_signatures_and_catalogs(db: TempDatabase) {
    let conn = db.connect_limbo();
    for (sql, expected) in [
        ("SELECT quote_ident('Foo')", Value::build_text("\"Foo\"")),
        ("SELECT booleq(1, 0)", Value::from_i64(0)),
        ("SELECT boolne(1, 0)", Value::from_i64(1)),
        (
            "SELECT format_type(1043)",
            Value::build_text("character varying"),
        ),
        (
            "SELECT format_type(1043, 17)",
            Value::build_text("character varying(13)"),
        ),
        ("SELECT pg_get_expr('x + 7', 1)", Value::build_text("x + 7")),
        (
            "SELECT pg_get_expr('x + 7', 1, 1)",
            Value::build_text("x + 7"),
        ),
        ("SELECT pg_get_expr('x + 7', NULL, 1)", Value::Null),
        ("SELECT length(now(1, 2, 3))", Value::from_i64(23)),
    ] {
        assert_eq!(
            conn.prepare(sql).unwrap().run_collect_rows().unwrap(),
            vec![vec![expected]],
            "{sql}"
        );
    }
    for sql in [
        "SELECT version(1)",
        "SELECT quote_ident()",
        "SELECT format_type()",
        "SELECT format_type(23, -1, 0)",
        "SELECT pg_get_expr('x')",
        "SELECT pg_get_expr('x', 1, 1, 1)",
    ] {
        assert!(conn.prepare(sql).is_err(), "{sql}");
    }
    let rows = conn
        .prepare_internal(
            "SELECT name, narg, flags FROM pragma_function_list WHERE name IN \
         ('format_type', 'pg_get_constraintdef', 'quote_ident', 'now') ORDER BY name, narg",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        [
            ("format_type", 1, 2048),
            ("format_type", 2, 2048),
            ("now", -1, 0),
            ("pg_get_constraintdef", 1, 0),
            ("pg_get_constraintdef", 2, 0),
            ("quote_ident", 1, 2048),
        ]
        .into_iter()
        .map(|(name, count, flags)| vec![
            Value::build_text(name),
            Value::from_i64(count),
            Value::from_i64(flags)
        ])
        .collect::<Vec<_>>()
    );
    let rows = conn
        .prepare(
            "SELECT proname, pronargs, provolatile FROM pg_proc WHERE proname IN \
         ('format_type', 'pg_get_constraintdef', 'quote_ident', 'now') ORDER BY proname, pronargs",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        [
            ("format_type", 1, "i"),
            ("format_type", 2, "i"),
            ("now", -1, "v"),
            ("pg_get_constraintdef", 1, "v"),
            ("pg_get_constraintdef", 2, "v"),
            ("quote_ident", 1, "i"),
        ]
        .into_iter()
        .map(|(name, count, volatility)| vec![
            Value::build_text(name),
            Value::from_i64(count),
            Value::build_text(volatility)
        ])
        .collect::<Vec<_>>()
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_native_timestamps_are_registered_before_connection_wrapping(db: TempDatabase) {
    let conn = db.connect_limbo();
    for name in [
        "now",
        "clock_timestamp",
        "transaction_timestamp",
        "statement_timestamp",
    ] {
        assert!(conn.get_syms_functions().iter().any(
            |(registered, is_agg, argc, deterministic)| registered == name
                && !is_agg
                && *argc == -1
                && !deterministic
        ));
        let rows = conn
            .prepare(format!("SELECT {name}()"))
            .unwrap()
            .run_collect_rows()
            .unwrap();
        let Value::Text(timestamp) = &rows[0][0] else {
            panic!("expected timestamp text for {name}")
        };
        assert_eq!(timestamp.as_str().len(), 23);
        for (index, separator) in [
            (4, b'-'),
            (7, b'-'),
            (10, b' '),
            (13, b':'),
            (16, b':'),
            (19, b'.'),
        ] {
            assert_eq!(timestamp.as_str().as_bytes()[index], separator, "{name}");
        }
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_quote_functions(db: TempDatabase) {
    let conn = db.connect_postgres();

    for (input, expected) in [
        ("SELECT quote_ident('abc')", "abc"),
        ("SELECT quote_ident('a_1')", "a_1"),
        ("SELECT quote_ident('Abc')", "\"Abc\""),
        ("SELECT quote_ident('a b')", "\"a b\""),
        ("SELECT quote_ident('a\"b')", "\"a\"\"b\""),
        // Reserved keywords must be quoted, unreserved ones must not.
        ("SELECT quote_ident('select')", "\"select\""),
        ("SELECT quote_ident('table')", "\"table\""),
        ("SELECT quote_ident('commit')", "commit"),
        ("SELECT quote_ident('')", "\"\""),
    ] {
        assert_eq!(query_text(&conn, input), [expected], "{input}");
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_quote_literal(db: TempDatabase) {
    let conn = db.connect_postgres();

    for (input, expected) in [
        ("SELECT quote_literal('abc')", "'abc'"),
        ("SELECT quote_literal('O''Reilly')", "'O''Reilly'"),
        ("SELECT quote_literal(42)", "'42'"),
        ("SELECT quote_literal('a\\b')", "E'a\\\\b'"),
    ] {
        assert_eq!(query_text(&conn, input), [expected], "{input}");
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_description_stubs_return_null(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (id integer PRIMARY KEY)")
        .unwrap();

    // Comments are not stored, so description lookups report none rather
    // than erroring.
    for sql in [
        "SELECT obj_description(16384, 'pg_class')",
        "SELECT obj_description(16384)",
        "SELECT col_description(16384, 1)",
    ] {
        assert_eq!(query_text(&conn, sql), ["NULL"], "{sql}");
    }
}
