use crate::common::TempDatabase;
use turso_core::{Numeric, Value};
use turso_pg::PgConnection;

#[test]
fn superuser_sees_rows_of_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);

    assert_eq!(
        integers(&conn, "SELECT id FROM docs ORDER BY id"),
        [1, 2, 3]
    );
}

#[test]
fn role_sees_no_rows_of_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    conn.execute("CREATE VIEW all_docs AS SELECT * FROM docs")
        .unwrap();

    conn.execute("SET ROLE alice").unwrap();

    for sql in [
        "SELECT count(*) FROM docs",
        "SELECT count(*) FROM docs WHERE id = 1",
        "SELECT count(*) FROM (SELECT * FROM docs) AS d",
        "WITH d AS (SELECT * FROM docs) SELECT count(*) FROM d",
        "SELECT count(*) FROM all_docs",
        "SELECT count(*) FROM ids WHERE id IN (SELECT id FROM docs)",
        "SELECT (SELECT count(*) FROM docs)",
    ] {
        assert_eq!(integers(&conn, sql), [0], "{sql}");
    }
}

#[test]
fn disabled_row_security_shows_rows_again() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    conn.execute("ALTER TABLE docs DISABLE ROW LEVEL SECURITY")
        .unwrap();

    conn.execute("SET ROLE alice").unwrap();

    assert_eq!(integers(&conn, "SELECT count(*) FROM docs"), [3]);
}

#[test]
fn hidden_rows_are_null_on_right_side_of_left_join() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);

    conn.execute("SET ROLE alice").unwrap();

    assert_eq!(
        texts(
            &conn,
            "SELECT coalesce(docs.owner, 'none') FROM ids LEFT JOIN docs ON docs.id = ids.id ORDER BY ids.id",
        ),
        ["none", "none"]
    );
    for sql in [
        "SELECT * FROM docs FULL JOIN ids ON docs.id = ids.id",
        "SELECT * FROM ids FULL JOIN docs ON docs.id = ids.id",
    ] {
        assert_error_contains(&conn, sql, "FULL JOIN");
    }
}

#[test]
fn role_cannot_write_to_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);

    conn.execute("SET ROLE alice").unwrap();

    for sql in [
        "INSERT INTO docs VALUES (4, 'alice')",
        "UPDATE docs SET owner = 'alice'",
        "DELETE FROM docs",
    ] {
        assert_error_contains(&conn, sql, "permission denied for table docs");
    }
}

#[test]
fn statement_prepared_by_superuser_hides_rows_from_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    let mut stmt = conn.prepare("SELECT count(*) FROM docs").unwrap();

    conn.execute("SET ROLE alice").unwrap();

    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(0)]]);
}

#[test]
fn row_security_is_seen_by_other_connections() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    integers(&other, "SELECT count(*) FROM pg_class");

    create_docs_with_row_security(&conn);
    other.execute("SET ROLE alice").unwrap();

    assert_eq!(integers(&other, "SELECT count(*) FROM docs"), [0]);
}

#[test]
fn row_security_is_kept_after_reopening_the_database() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_postgres();
    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT count(*) FROM docs"), [0]);
}

#[test]
fn drop_table_removes_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);

    conn.execute("DROP TABLE docs").unwrap();

    conn.execute("CREATE TABLE docs (id int)").unwrap();
    conn.execute("INSERT INTO docs VALUES (1)").unwrap();
    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT count(*) FROM docs"), [1]);
}

#[test]
fn rename_is_rejected_only_for_the_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);

    assert_error_contains(
        &conn,
        "ALTER TABLE docs RENAME TO papers",
        "row-level security",
    );

    conn.execute("CREATE SCHEMA s").unwrap();
    conn.execute("CREATE TABLE s.docs (id int)").unwrap();
    conn.execute("ALTER TABLE s.docs RENAME TO papers").unwrap();
}

/// The PostgreSQL frontend has no ATTACH, so the database is attached
/// through a core connection.
#[test]
fn row_security_only_on_tables_of_the_main_database() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_attach(true))
        .build();
    let conn = db.connect_limbo();
    conn.execute("ATTACH ':memory:' AS other").unwrap();
    conn.execute("CREATE TABLE other.t (x int)").unwrap();

    let error = conn
        .execute("ALTER TABLE other.t ENABLE ROW LEVEL SECURITY")
        .unwrap_err();

    assert!(error.to_string().contains("main database"), "{error}");
}

/// Hash joins compute their keys, and multi-index scans their branch
/// conditions, before a table's WHERE terms run. An expression that fails
/// on a hidden row there would reveal it, so such tables use other access
/// methods.
#[test]
fn query_expressions_never_run_on_hidden_rows() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    for sql in [
        "ALTER TABLE docs ADD COLUMN body text",
        "ALTER TABLE docs ADD COLUMN x int",
        "INSERT INTO docs (id, owner) SELECT value, 'bob' FROM generate_series(4, 300) AS value",
        "UPDATE docs SET body = 'not json', x = id % 50",
        "CREATE INDEX docs_x ON docs (x)",
        "CREATE TABLE big (x int)",
        "INSERT INTO big SELECT value % 50 FROM generate_series(1, 5000) AS value",
    ] {
        conn.execute(sql).unwrap();
    }

    conn.execute("SET ROLE alice").unwrap();

    for sql in [
        "SELECT count(*) FROM big LEFT JOIN docs ON json_extract(docs.body, '$.x') = big.x",
        "SELECT count(*) FROM big JOIN docs ON json_extract(docs.body, '$.x') = big.x",
        "SELECT count(*) FROM docs JOIN big ON json_extract(docs.body, '$.x') = big.x",
        "SELECT id FROM docs WHERE id = 1 OR (x = 2 AND json_extract(body, '$.x') = 1)",
    ] {
        conn.execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn owner_policy_shows_rows_owned_by_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);

    conn.execute("SET ROLE alice").unwrap();

    for sql in [
        "SELECT id FROM docs ORDER BY id",
        "SELECT d.id FROM docs AS d ORDER BY d.id",
        "SELECT id FROM docs WHERE id IN (1, 2, 3) ORDER BY id",
        "SELECT docs.id FROM ids JOIN docs ON docs.id >= ids.id WHERE ids.id = 1 ORDER BY docs.id",
        "WITH d AS (SELECT * FROM docs) SELECT id FROM d ORDER BY id",
    ] {
        assert_eq!(integers(&conn, sql), [1, 3], "{sql}");
    }
    assert_eq!(
        integers(&conn, "SELECT count(*) FROM docs WHERE id = 2"),
        [0]
    );
    conn.execute("SET ROLE bob").unwrap();
    assert_eq!(integers(&conn, "SELECT id FROM docs"), [2]);
}

#[test]
fn owner_policy_accepts_supabase_spellings() {
    for using in [
        "current_user = owner",
        "docs.owner = current_user",
        "(SELECT current_user) = owner",
        "((owner) = (SELECT current_user))",
    ] {
        let db = TempDatabase::builder().build();
        let conn = db.connect_postgres();
        create_docs_with_row_security(&conn);
        conn.execute(format!("CREATE POLICY own ON docs USING ({using})"))
            .unwrap();

        conn.execute("SET ROLE alice").unwrap();

        assert_eq!(
            integers(&conn, "SELECT id FROM docs ORDER BY id"),
            [1, 3],
            "{using}"
        );
    }
}

#[test]
fn policies_combine_with_or() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);
    for sql in [
        "ALTER TABLE docs ADD COLUMN editor text",
        "UPDATE docs SET editor = 'alice' WHERE id = 2",
        "CREATE POLICY edit ON docs FOR SELECT USING (editor = current_user)",
        "CREATE POLICY nothing ON docs USING (false)",
    ] {
        conn.execute(sql).unwrap();
    }

    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(
        integers(&conn, "SELECT id FROM docs ORDER BY id"),
        [1, 2, 3]
    );

    conn.execute("RESET ROLE").unwrap();
    conn.execute("CREATE POLICY everything ON docs USING (true)")
        .unwrap();
    conn.execute("SET ROLE bob").unwrap();
    assert_eq!(
        integers(&conn, "SELECT id FROM docs ORDER BY id"),
        [1, 2, 3]
    );
}

#[test]
fn owner_policy_uses_index_on_owner_column() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);
    conn.execute("CREATE INDEX docs_owner ON docs (owner)")
        .unwrap();

    conn.execute("SET ROLE alice").unwrap();

    let plan = texts_of_column(&conn, "EXPLAIN SELECT id FROM docs", 3);
    assert!(
        plan.iter().any(|line| line.contains("docs_owner")),
        "{plan:?}"
    );
    assert_eq!(integers(&conn, "SELECT id FROM docs ORDER BY id"), [1, 3]);
}

#[test]
fn unsupported_policies_are_rejected() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);

    for (sql, expected) in [
        (
            "CREATE POLICY p ON docs AS RESTRICTIVE USING (true)",
            "RESTRICTIVE",
        ),
        (
            "CREATE POLICY p ON docs FOR INSERT WITH CHECK (true)",
            "FOR SELECT",
        ),
        ("CREATE POLICY p ON docs TO alice USING (true)", "TO PUBLIC"),
        (
            "CREATE POLICY p ON docs USING (true) WITH CHECK (true)",
            "WITH CHECK",
        ),
        ("CREATE POLICY p ON docs", "USING"),
        (
            "CREATE POLICY p ON docs USING (owner IN (SELECT current_user))",
            "subqueries in policies are not supported",
        ),
        (
            "CREATE POLICY p ON docs USING (EXISTS (SELECT 1 FROM ids WHERE ids.id = docs.id))",
            "subqueries in policies are not supported",
        ),
        (
            "CREATE POLICY p ON docs USING (missing = current_user)",
            "missing",
        ),
        (
            "CREATE POLICY p ON docs USING (ids.id = 1)",
            "can only refer to the columns of that table",
        ),
        ("CREATE POLICY p ON nope USING (true)", "nope"),
    ] {
        assert_error_contains(&conn, sql, expected);
    }
    conn.execute("CREATE POLICY p ON docs USING (true)")
        .unwrap();
    assert_error_contains(
        &conn,
        "CREATE POLICY p ON docs USING (true)",
        "already exists",
    );
}

#[test]
fn policy_condition_can_be_any_expression_on_the_table() {
    for (using, expected) in [
        ("owner = 'bob'", vec![2]),
        ("owner = current_user AND id > 1", vec![3]),
        ("id IN (1, 2)", vec![1, 2]),
        ("upper(owner) = upper(current_user)", vec![1, 3]),
        ("docs.id % 2 = 1", vec![1, 3]),
        ("true", vec![1, 2, 3]),
        ("false", vec![]),
        ("NULL", vec![]),
    ] {
        let db = TempDatabase::builder().build();
        let conn = db.connect_postgres();
        create_docs_with_row_security(&conn);
        conn.execute(format!("CREATE POLICY p ON docs USING ({using})"))
            .unwrap();

        conn.execute("SET ROLE alice").unwrap();

        assert_eq!(
            integers(&conn, "SELECT d.id FROM docs AS d ORDER BY d.id"),
            expected,
            "{using}"
        );
    }
}

#[test]
fn policy_condition_is_kept_exactly_after_reopening_the_database() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    conn.execute(
        "CREATE POLICY p ON docs USING (owner = (SELECT current_user) AND owner <> 'it''s' AND id < 3)",
    )
    .unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_postgres();
    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT id FROM docs"), [1]);
}

#[test]
fn policies_have_no_effect_without_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);
    conn.execute("ALTER TABLE docs DISABLE ROW LEVEL SECURITY")
        .unwrap();

    conn.execute("SET ROLE alice").unwrap();

    assert_eq!(
        integers(&conn, "SELECT id FROM docs ORDER BY id"),
        [1, 2, 3]
    );
}

#[test]
fn drop_policy_hides_the_rows_it_showed() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);

    conn.execute("DROP POLICY own ON docs").unwrap();

    assert_error_contains(&conn, "DROP POLICY own ON docs", "does not exist");
    conn.execute("DROP POLICY IF EXISTS own ON docs").unwrap();
    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT count(*) FROM docs"), [0]);
}

#[test]
fn policies_are_seen_by_other_connections() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    integers(&other, "SELECT count(*) FROM pg_class");

    create_docs_with_owner_policy(&conn);
    other.execute("SET ROLE alice").unwrap();

    assert_eq!(integers(&other, "SELECT id FROM docs ORDER BY id"), [1, 3]);
}

#[test]
fn policies_are_kept_after_reopening_the_database() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_postgres();
    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT id FROM docs ORDER BY id"), [1, 3]);
}

#[test]
fn columns_used_by_policies_cannot_change() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);

    conn.execute("ALTER TABLE docs ADD COLUMN team int")
        .unwrap();
    conn.execute("CREATE POLICY team ON docs USING (docs.team > 0 OR id = 1)")
        .unwrap();

    for sql in [
        "ALTER TABLE docs RENAME COLUMN owner TO author",
        "ALTER TABLE docs DROP COLUMN owner",
        "ALTER TABLE docs DROP COLUMN team",
        "ALTER TABLE docs RENAME COLUMN id TO key",
        "ALTER TABLE docs RENAME TO papers",
    ] {
        assert_error_contains(&conn, sql, "row-level security");
    }
    for sql in [
        "ALTER TABLE docs ADD COLUMN title text",
        "ALTER TABLE docs RENAME COLUMN title TO name",
        "CREATE SCHEMA s",
        "CREATE TABLE s.docs (id int, owner text)",
        "ALTER TABLE s.docs RENAME COLUMN owner TO author",
    ] {
        conn.execute(sql).unwrap();
    }
}

#[test]
fn drop_table_removes_policies() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);

    conn.execute("DROP TABLE docs").unwrap();

    conn.execute("CREATE TABLE docs (id int, owner text)")
        .unwrap();
    conn.execute("CREATE POLICY own ON docs USING (true)")
        .unwrap();
}

#[test]
fn owner_policy_runs_before_multi_index_branch_conditions() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);
    for sql in [
        "ALTER TABLE docs ADD COLUMN body text",
        "ALTER TABLE docs ADD COLUMN x int",
        "UPDATE docs SET body = '{\"x\": 1}', x = id",
        "UPDATE docs SET body = 'not json' WHERE id = 2",
        "CREATE INDEX docs_x ON docs (x)",
    ] {
        conn.execute(sql).unwrap();
    }

    conn.execute("SET ROLE alice").unwrap();

    assert_eq!(
        integers(
            &conn,
            "SELECT id FROM docs WHERE id = 1 OR (x = 2 AND json_extract(body, '$.x') = 1)"
        ),
        [1]
    );
}

/// A query's own conditions must never run on a hidden row, or an error
/// they raise, such as malformed JSON, would reveal the row. The policy
/// filter is evaluated before them in every query shape.
#[test]
fn query_conditions_never_see_hidden_rows() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);
    for sql in [
        "ALTER TABLE docs ADD COLUMN body text",
        "UPDATE docs SET body = '{\"x\": 1}'",
        "UPDATE docs SET body = 'not json' WHERE id = 2",
    ] {
        conn.execute(sql).unwrap();
    }
    let hidden_row_query = "SELECT id FROM docs WHERE json_extract(body, '$.x') = 1";
    assert_error_contains(&conn, hidden_row_query, "malformed JSON");

    conn.execute("SET ROLE alice").unwrap();

    let queries = [
        hidden_row_query,
        "SELECT id FROM docs WHERE id = 2 AND json_extract(body, '$.x') = 1",
        "SELECT id FROM docs WHERE json_extract(body, '$.x') = 1 AND id > 0",
        "SELECT docs.id FROM ids JOIN docs ON docs.id = ids.id WHERE json_extract(docs.body, '$.x') = 1",
        "SELECT docs.id FROM docs JOIN ids ON docs.id = ids.id WHERE json_extract(docs.body, '$.x') = 1",
        "SELECT ids.id FROM ids LEFT JOIN docs ON docs.id = ids.id AND json_extract(docs.body, '$.x') = 1",
        "SELECT id FROM (SELECT * FROM docs) AS d WHERE json_extract(body, '$.x') = 1",
        "SELECT id FROM docs WHERE EXISTS (SELECT 1 WHERE json_extract(docs.body, '$.x') = 1)",
        "SELECT json_extract(body, '$.x') FROM docs",
        "SELECT id FROM docs ORDER BY json_extract(body, '$.x')",
        "SELECT count(*) FROM docs GROUP BY json_extract(body, '$.x')",
    ];
    for sql in queries {
        conn.execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    conn.execute("RESET ROLE").unwrap();
    conn.execute("CREATE INDEX docs_owner ON docs (owner)")
        .unwrap();
    conn.execute("SET ROLE alice").unwrap();
    for sql in queries {
        conn.execute(sql)
            .unwrap_or_else(|error| panic!("with index, {sql}: {error}"));
    }
}

fn create_docs_with_row_security(conn: &PgConnection) {
    for sql in [
        "CREATE TABLE docs (id int PRIMARY KEY, owner text)",
        "INSERT INTO docs VALUES (1, 'alice'), (2, 'bob'), (3, 'alice')",
        "CREATE TABLE ids (id int)",
        "INSERT INTO ids VALUES (1), (2)",
        "CREATE ROLE alice",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
    ] {
        conn.execute(sql).unwrap();
    }
}

fn create_docs_with_owner_policy(conn: &PgConnection) {
    create_docs_with_row_security(conn);
    conn.execute("CREATE ROLE bob").unwrap();
    conn.execute("CREATE POLICY own ON docs USING (owner = current_user)")
        .unwrap();
}

fn integers(conn: &PgConnection, sql: &str) -> Vec<i64> {
    first_column(conn, sql)
        .into_iter()
        .map(|value| match value {
            Value::Numeric(Numeric::Integer(value)) => value,
            value => panic!("{sql}: expected an integer, got {value:?}"),
        })
        .collect()
}

fn texts(conn: &PgConnection, sql: &str) -> Vec<String> {
    texts_of_column(conn, sql, 0)
}

fn texts_of_column(conn: &PgConnection, sql: &str, column: usize) -> Vec<String> {
    rows(conn, sql)
        .into_iter()
        .map(|row| row[column].to_string())
        .collect()
}

fn first_column(conn: &PgConnection, sql: &str) -> Vec<Value> {
    rows(conn, sql)
        .into_iter()
        .map(|row| row.into_iter().next().unwrap())
        .collect()
}

fn rows(conn: &PgConnection, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.query(sql).unwrap().unwrap();
    stmt.run_collect_rows().unwrap()
}

fn assert_error_contains(conn: &PgConnection, sql: &str, expected: &str) {
    let error = conn.execute(sql).unwrap_err();
    assert!(
        error.to_string().contains(expected),
        "{sql}: expected \"{expected}\", got: {error}"
    );
}
