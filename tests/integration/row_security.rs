use std::sync::Arc;

use turso_core::{Connection, DatabaseOpts};

use crate::common::{ExecRows, TempDatabase};

fn create_docs_with_row_security(conn: &Arc<Connection>) {
    for sql in [
        "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner TEXT)",
        "INSERT INTO docs VALUES (1, 'alice'), (2, 'bob'), (3, 'alice')",
        "CREATE TABLE ids(id INTEGER)",
        "INSERT INTO ids VALUES (1), (2)",
        "CREATE ROLE alice",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
    ] {
        conn.execute(sql).unwrap();
    }
}

fn assert_error_contains(result: turso_core::Result<()>, expected: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(expected),
        "expected \"{expected}\", got: {err}"
    );
}

#[test]
fn test_superuser_sees_rows_of_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM docs ORDER BY id");
    assert_eq!(rows, vec![(1,), (2,), (3,)]);
}

#[test]
fn test_role_sees_no_rows_of_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    conn.execute("CREATE VIEW all_docs AS SELECT * FROM docs")
        .unwrap();
    conn.set_role(Some("alice")).unwrap();
    for sql in [
        "SELECT count(*) FROM docs",
        "SELECT count(*) FROM docs WHERE id = 1",
        "SELECT count(*) FROM (SELECT * FROM docs)",
        "WITH d AS (SELECT * FROM docs) SELECT count(*) FROM d",
        "SELECT count(*) FROM all_docs",
        "SELECT count(*) FROM ids WHERE id IN (SELECT id FROM docs)",
        "SELECT (SELECT count(*) FROM docs)",
    ] {
        let rows: Vec<(i64,)> = conn.exec_rows(sql);
        assert_eq!(rows, vec![(0,)], "{sql}");
    }
}

#[test]
fn test_disabled_row_security_shows_rows_again() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    conn.execute("ALTER TABLE docs DISABLE ROW LEVEL SECURITY")
        .unwrap();
    conn.set_role(Some("alice")).unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM docs");
    assert_eq!(rows, vec![(3,)]);
}

#[test]
fn test_row_security_can_change_on_table_with_materialized_view() {
    let db = TempDatabase::builder().with_views(true).build();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, owner TEXT)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW owners AS SELECT owner FROM docs")
        .unwrap();
    conn.execute("ALTER TABLE docs ENABLE ROW LEVEL SECURITY")
        .unwrap();
    conn.execute("ALTER TABLE docs DISABLE ROW LEVEL SECURITY")
        .unwrap();
}

#[test]
fn test_hidden_rows_are_null_on_right_side_of_left_join() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    conn.set_role(Some("alice")).unwrap();
    let rows: Vec<(i64, String)> = conn.exec_rows(
        "SELECT ids.id, coalesce(docs.owner, 'none') FROM ids LEFT JOIN docs ON docs.id = ids.id ORDER BY ids.id",
    );
    assert_eq!(rows, vec![(1, "none".to_string()), (2, "none".to_string())]);
    assert_error_contains(
        conn.execute("SELECT * FROM docs FULL JOIN ids ON docs.id = ids.id"),
        "FULL JOIN",
    );
    assert_error_contains(
        conn.execute("SELECT * FROM ids FULL JOIN docs ON docs.id = ids.id"),
        "FULL JOIN",
    );
}

#[test]
fn test_role_cannot_write_to_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    conn.set_role(Some("alice")).unwrap();
    for sql in [
        "INSERT INTO docs VALUES (4, 'alice')",
        "UPDATE docs SET owner = 'alice'",
        "DELETE FROM docs",
    ] {
        assert_error_contains(conn.execute(sql), "row-level security");
    }
    conn.execute("INSERT INTO ids SELECT id FROM docs").unwrap();
    conn.execute("UPDATE ids SET id = 0 FROM docs WHERE docs.id = ids.id")
        .unwrap();
    conn.set_role(None).unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM ids ORDER BY id");
    assert_eq!(rows, vec![(1,), (2,)]);
}

#[test]
fn test_role_cannot_open_blob_in_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    for sql in [
        "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner TEXT, payload BLOB)",
        "INSERT INTO docs VALUES (1, 'bob', x'01020304')",
        "CREATE TABLE notes(id INTEGER PRIMARY KEY, payload BLOB)",
        "INSERT INTO notes VALUES (1, x'05060708')",
        "CREATE ROLE alice",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY everything ON docs USING (true)",
    ] {
        conn.execute(sql).unwrap();
    }
    conn.set_role(Some("alice")).unwrap();
    for read_write in [false, true] {
        let err = conn
            .blob_open("docs", "payload", 1, read_write)
            .err()
            .unwrap();
        assert!(err.to_string().contains("row-level security"), "{err}");
    }
    let mut blob = conn.blob_open("notes", "payload", 1, false).unwrap();
    let mut buf = [0u8; 4];
    blob.read(0, &mut buf).unwrap();
    assert_eq!(buf, [5, 6, 7, 8]);
}

#[test]
fn test_foreign_key_action_changes_rows_hidden_from_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    for sql in [
        "PRAGMA foreign_keys = ON",
        "CREATE ROLE alice",
        "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
        "INSERT INTO parent VALUES (1)",
        "INSERT INTO child VALUES (1, 1)",
        "ALTER TABLE child ENABLE ROW LEVEL SECURITY",
    ] {
        conn.execute(sql).unwrap();
    }
    conn.set_role(Some("alice")).unwrap();
    conn.execute("DELETE FROM parent WHERE id = 1").unwrap();
    conn.set_role(None).unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM child");
    assert_eq!(rows, vec![(0,)]);
}

#[test]
fn test_statement_prepared_by_superuser_hides_rows_from_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    let mut stmt = conn.prepare("SELECT count(*) FROM docs").unwrap();
    conn.set_role(Some("alice")).unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows, vec![vec![turso_core::Value::from_i64(0)]]);
}

#[test]
fn test_row_security_survives_reopen_and_is_seen_by_other_connections() {
    for mvcc in [false, true] {
        let db = TempDatabase::builder().with_mvcc(mvcc).build();
        let conn = db.connect_limbo();
        let other = db.connect_limbo();
        let _: Vec<(i64,)> = other.exec_rows("SELECT count(*) FROM sqlite_schema");
        create_docs_with_row_security(&conn);
        other.set_role(Some("alice")).unwrap();
        let rows: Vec<(i64,)> = other.exec_rows("SELECT count(*) FROM docs");
        assert_eq!(rows, vec![(0,)], "mvcc={mvcc}");
        conn.close().unwrap();
        other.close().unwrap();
        let path = db.path.clone();
        drop(db);

        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.set_role(Some("alice")).unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM docs");
        assert_eq!(rows, vec![(0,)], "mvcc={mvcc}");
    }
}

#[test]
fn test_drop_table_removes_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    conn.execute("DROP TABLE docs").unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER)").unwrap();
    conn.execute("INSERT INTO docs VALUES (1)").unwrap();
    conn.set_role(Some("alice")).unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM docs");
    assert_eq!(rows, vec![(1,)]);
}

#[test]
fn test_drop_table_rejected_by_foreign_key_keeps_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    for sql in [
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE notes(doc_id INTEGER REFERENCES docs(id) ON DELETE RESTRICT)",
        "INSERT INTO notes VALUES (1)",
        "BEGIN",
    ] {
        conn.execute(sql).unwrap();
    }
    assert_error_contains(conn.execute("DROP TABLE docs"), "FOREIGN KEY");
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM notes");
    assert_eq!(rows, vec![(1,)]);
    conn.execute("COMMIT").unwrap();
    conn.set_role(Some("alice")).unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM docs");
    assert_eq!(rows, vec![(0,)]);
}

#[test]
fn test_rename_is_rejected_only_for_the_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    assert_error_contains(
        conn.execute("ALTER TABLE docs RENAME TO papers"),
        "row-level security",
    );
    conn.execute("CREATE TEMP TABLE docs(id INTEGER)").unwrap();
    conn.execute("ALTER TABLE temp.docs RENAME TO papers")
        .unwrap();
}

#[test]
fn test_row_security_only_on_tables_of_the_main_database() {
    let db = TempDatabase::builder()
        .with_opts(DatabaseOpts::new().with_attach(true))
        .build();
    let conn = db.connect_limbo();
    conn.execute("ATTACH ':memory:' AS other").unwrap();
    conn.execute("CREATE TABLE other.t(x)").unwrap();
    assert_error_contains(
        conn.execute("ALTER TABLE other.t ENABLE ROW LEVEL SECURITY"),
        "main database",
    );
    assert_error_contains(
        conn.execute("ALTER TABLE sqlite_schema ENABLE ROW LEVEL SECURITY"),
        "may not be modified",
    );
}

#[test]
fn test_attached_database_with_access_control_is_rejected_for_roles() {
    let other = TempDatabase::builder().build();
    let other_conn = other.connect_limbo();
    create_docs_with_row_security(&other_conn);
    other_conn.close().unwrap();
    let other_path = other.path.clone();
    drop(other);

    let db = TempDatabase::builder()
        .with_opts(DatabaseOpts::new().with_attach(true))
        .build();
    let conn = db.connect_limbo();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute(format!("ATTACH '{}' AS other", other_path.display()))
        .unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM other.docs");
    assert_eq!(rows, vec![(3,)]);
    conn.set_role(Some("alice")).unwrap();
    assert_error_contains(conn.execute("SELECT * FROM other.ids"), "access control");
}

#[test]
fn test_role_cannot_read_database_pages() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    conn.set_role(Some("alice")).unwrap();
    assert_error_contains(
        conn.execute("SELECT * FROM sqlite_dbpage"),
        "permission denied",
    );
}

/// Hash joins compute their keys, and multi-index scans their branch
/// conditions, before a table's WHERE terms run. An expression that fails on
/// a hidden row there would reveal it, so such tables use other access
/// methods.
#[test]
fn test_query_expressions_never_run_on_hidden_rows() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_row_security(&conn);
    conn.execute("ALTER TABLE docs ADD COLUMN body TEXT")
        .unwrap();
    conn.execute("ALTER TABLE docs ADD COLUMN x INTEGER")
        .unwrap();
    conn.execute("INSERT INTO docs(id, owner) SELECT value, 'bob' FROM generate_series(4, 300)")
        .unwrap();
    conn.execute("UPDATE docs SET body = 'not json', x = id % 50")
        .unwrap();
    conn.execute("CREATE INDEX docs_x ON docs(x)").unwrap();
    conn.execute("CREATE TABLE big(x INTEGER)").unwrap();
    conn.execute("INSERT INTO big SELECT value % 50 FROM generate_series(1, 5000)")
        .unwrap();
    conn.execute("ANALYZE").unwrap();
    conn.set_role(Some("alice")).unwrap();
    for sql in [
        "SELECT count(*) FROM big LEFT JOIN docs ON json_extract(docs.body, '$.x') = big.x",
        "SELECT count(*) FROM big JOIN docs ON json_extract(docs.body, '$.x') = big.x",
        "SELECT count(*) FROM docs JOIN big ON json_extract(docs.body, '$.x') = big.x",
        "SELECT id FROM docs WHERE id = 1 OR (x = 2 AND json_extract(body, '$.x') = 1)",
    ] {
        conn.execute(sql)
            .unwrap_or_else(|err| panic!("{sql}: {err}"));
    }
}

fn create_docs_with_owner_policy(conn: &Arc<Connection>) {
    create_docs_with_row_security(conn);
    conn.execute("CREATE ROLE bob").unwrap();
    conn.execute("CREATE POLICY own ON docs USING (owner = current_user)")
        .unwrap();
}

fn visible_ids(conn: &Arc<Connection>, sql: &str) -> Vec<i64> {
    let rows: Vec<(i64,)> = conn.exec_rows(sql);
    rows.into_iter().map(|(id,)| id).collect()
}

#[test]
fn test_owner_policy_shows_rows_owned_by_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_owner_policy(&conn);
    conn.set_role(Some("alice")).unwrap();
    for sql in [
        "SELECT id FROM docs ORDER BY id",
        "SELECT d.id FROM docs AS d ORDER BY d.id",
        "SELECT id FROM docs WHERE id IN (1, 2, 3) ORDER BY id",
        "SELECT docs.id FROM ids JOIN docs ON docs.id >= ids.id WHERE ids.id = 1 ORDER BY docs.id",
        "WITH d AS (SELECT * FROM docs) SELECT id FROM d ORDER BY id",
    ] {
        assert_eq!(visible_ids(&conn, sql), vec![1, 3], "{sql}");
    }
    assert_eq!(
        visible_ids(&conn, "SELECT count(*) FROM docs WHERE id = 2"),
        vec![0]
    );
    conn.set_role(Some("bob")).unwrap();
    assert_eq!(visible_ids(&conn, "SELECT id FROM docs"), vec![2]);
}

#[test]
fn test_owner_policy_accepts_supabase_spellings() {
    for using in [
        "current_user = owner",
        "docs.owner = current_user()",
        "(SELECT current_user) = owner",
        "((owner) = (SELECT current_user()))",
    ] {
        let db = TempDatabase::builder().build();
        let conn = db.connect_limbo();
        create_docs_with_row_security(&conn);
        conn.execute(format!("CREATE POLICY own ON docs USING ({using})"))
            .unwrap();
        conn.set_role(Some("alice")).unwrap();
        assert_eq!(
            visible_ids(&conn, "SELECT id FROM docs ORDER BY id"),
            vec![1, 3],
            "{using}"
        );
    }
}

#[test]
fn test_policies_combine_with_or() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_owner_policy(&conn);
    conn.execute("ALTER TABLE docs ADD COLUMN editor TEXT")
        .unwrap();
    conn.execute("UPDATE docs SET editor = 'alice' WHERE id = 2")
        .unwrap();
    conn.execute("CREATE POLICY edit ON docs FOR SELECT USING (editor = current_user)")
        .unwrap();
    conn.execute("CREATE POLICY none ON docs USING (false)")
        .unwrap();
    conn.set_role(Some("alice")).unwrap();
    assert_eq!(
        visible_ids(&conn, "SELECT id FROM docs ORDER BY id"),
        vec![1, 2, 3]
    );
    conn.set_role(None).unwrap();
    conn.execute("CREATE POLICY everything ON docs USING (true)")
        .unwrap();
    conn.set_role(Some("bob")).unwrap();
    assert_eq!(
        visible_ids(&conn, "SELECT id FROM docs ORDER BY id"),
        vec![1, 2, 3]
    );
}

#[test]
fn test_owner_policy_compares_role_names_exactly() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, owner TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'alice'), (2, 'ALICE')")
        .unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("ALTER TABLE docs ENABLE ROW LEVEL SECURITY")
        .unwrap();
    conn.execute("CREATE POLICY own ON docs USING (owner = current_user)")
        .unwrap();
    conn.set_role(Some("alice")).unwrap();
    assert_eq!(visible_ids(&conn, "SELECT id FROM docs"), vec![1]);
}

#[test]
fn test_owner_policy_uses_index_on_owner_column() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_owner_policy(&conn);
    conn.execute("CREATE INDEX docs_owner ON docs(owner)")
        .unwrap();
    conn.set_role(Some("alice")).unwrap();
    let plan: Vec<(i64, i64, i64, String)> =
        conn.exec_rows("EXPLAIN QUERY PLAN SELECT id FROM docs");
    assert!(
        plan.iter()
            .any(|(_, _, _, detail)| detail.contains("docs_owner")),
        "{plan:?}"
    );
    assert_eq!(
        visible_ids(&conn, "SELECT id FROM docs ORDER BY id"),
        vec![1, 3]
    );
}

#[test]
fn test_unsupported_policies_are_rejected() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
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
            "CREATE POLICY p ON docs USING (owner = 'alice')",
            "unsupported policy condition",
        ),
        (
            "CREATE POLICY p ON docs USING (owner = current_user AND id > 1)",
            "unsupported policy condition",
        ),
        (
            "CREATE POLICY p ON docs USING (owner IN (SELECT current_user))",
            "unsupported policy condition",
        ),
        (
            "CREATE POLICY p ON docs USING (missing = current_user)",
            "no such column",
        ),
        ("CREATE POLICY p ON nope USING (true)", "no such table"),
    ] {
        assert_error_contains(conn.execute(sql), expected);
    }
    conn.execute("CREATE POLICY p ON docs USING (true)")
        .unwrap();
    assert_error_contains(
        conn.execute("CREATE POLICY p ON docs USING (true)"),
        "already exists",
    );
}

#[test]
fn test_policy_owner_column_cannot_be_generated() {
    let db = TempDatabase::builder()
        .with_opts(DatabaseOpts::new().with_generated_columns(true))
        .build();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE g(a TEXT, b TEXT AS (a || ''))")
        .unwrap();
    assert_error_contains(
        conn.execute("CREATE POLICY p ON g USING (b = current_user)"),
        "generated column",
    );
}

#[test]
fn test_policies_have_no_effect_without_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_owner_policy(&conn);
    conn.execute("ALTER TABLE docs DISABLE ROW LEVEL SECURITY")
        .unwrap();
    conn.set_role(Some("alice")).unwrap();
    assert_eq!(
        visible_ids(&conn, "SELECT id FROM docs ORDER BY id"),
        vec![1, 2, 3]
    );
}

#[test]
fn test_drop_policy() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_owner_policy(&conn);
    conn.execute("DROP POLICY own ON docs").unwrap();
    assert_error_contains(conn.execute("DROP POLICY own ON docs"), "does not exist");
    conn.execute("DROP POLICY IF EXISTS own ON docs").unwrap();
    conn.set_role(Some("alice")).unwrap();
    assert_eq!(visible_ids(&conn, "SELECT count(*) FROM docs"), vec![0]);
}

#[test]
fn test_policies_survive_reopen_and_are_seen_by_other_connections() {
    for mvcc in [false, true] {
        let db = TempDatabase::builder().with_mvcc(mvcc).build();
        let conn = db.connect_limbo();
        let other = db.connect_limbo();
        let _: Vec<(i64,)> = other.exec_rows("SELECT count(*) FROM sqlite_schema");
        create_docs_with_owner_policy(&conn);
        other.set_role(Some("alice")).unwrap();
        assert_eq!(
            visible_ids(&other, "SELECT id FROM docs ORDER BY id"),
            vec![1, 3]
        );
        conn.close().unwrap();
        other.close().unwrap();
        let path = db.path.clone();
        drop(db);

        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.set_role(Some("alice")).unwrap();
        assert_eq!(
            visible_ids(&conn, "SELECT id FROM docs ORDER BY id"),
            vec![1, 3],
            "mvcc={mvcc}"
        );
    }
}

#[test]
fn test_drop_table_keeps_roles() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    for sql in [
        "CREATE ROLE alice",
        "CREATE TABLE \"\"(x)",
        "ALTER TABLE \"\" ENABLE ROW LEVEL SECURITY",
        "DROP TABLE \"\"",
    ] {
        conn.execute(sql).unwrap();
    }
    let rows: Vec<(String, String)> =
        conn.exec_rows("SELECT kind, name FROM __turso_internal_access_control");
    assert_eq!(rows, vec![("role".to_string(), "alice".to_string())]);
}

#[test]
fn test_policy_owner_column_cannot_change() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_owner_policy(&conn);
    for sql in [
        "ALTER TABLE docs RENAME COLUMN owner TO author",
        "ALTER TABLE docs DROP COLUMN owner",
        "ALTER TABLE docs RENAME TO papers",
    ] {
        assert_error_contains(conn.execute(sql), "row-level security");
    }
    conn.execute("ALTER TABLE docs ADD COLUMN title TEXT")
        .unwrap();
    conn.execute("ALTER TABLE docs RENAME COLUMN title TO name")
        .unwrap();
    conn.execute("CREATE TEMP TABLE docs(id INTEGER, owner TEXT)")
        .unwrap();
    conn.execute("ALTER TABLE temp.docs RENAME COLUMN owner TO author")
        .unwrap();
}

#[test]
fn test_drop_table_removes_policies() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_owner_policy(&conn);
    conn.execute("DROP TABLE docs").unwrap();
    let rows: Vec<(String,)> =
        conn.exec_rows("SELECT kind FROM __turso_internal_access_control WHERE tbl_name = 'docs'");
    assert!(rows.is_empty(), "{rows:?}");
    conn.execute("CREATE TABLE docs(id INTEGER, owner TEXT)")
        .unwrap();
    conn.execute("CREATE POLICY own ON docs USING (true)")
        .unwrap();
}

#[test]
fn test_owner_policy_runs_before_multi_index_branch_conditions() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_owner_policy(&conn);
    conn.execute("ALTER TABLE docs ADD COLUMN body TEXT")
        .unwrap();
    conn.execute("ALTER TABLE docs ADD COLUMN x INTEGER")
        .unwrap();
    conn.execute("UPDATE docs SET body = '{\"x\": 1}', x = id")
        .unwrap();
    conn.execute("UPDATE docs SET body = 'not json' WHERE id = 2")
        .unwrap();
    conn.execute("CREATE INDEX docs_x ON docs(x)").unwrap();
    conn.set_role(Some("alice")).unwrap();
    assert_eq!(
        visible_ids(
            &conn,
            "SELECT id FROM docs WHERE id = 1 OR (x = 2 AND json_extract(body, '$.x') = 1)"
        ),
        vec![1]
    );
}

/// A query's own conditions must never run on a hidden row, or an error they
/// raise, such as malformed JSON, would reveal the row. The policy filter is
/// evaluated before them in every query shape.
#[test]
fn test_query_conditions_never_see_hidden_rows() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    create_docs_with_owner_policy(&conn);
    conn.execute("ALTER TABLE docs ADD COLUMN body TEXT")
        .unwrap();
    conn.execute("UPDATE docs SET body = '{\"x\": 1}'").unwrap();
    conn.execute("UPDATE docs SET body = 'not json' WHERE id = 2")
        .unwrap();
    let hidden_row_query = "SELECT id FROM docs WHERE json_extract(body, '$.x') = 1";
    assert_error_contains(conn.execute(hidden_row_query), "malformed JSON");

    conn.set_role(Some("alice")).unwrap();
    let queries = [
        hidden_row_query,
        "SELECT id FROM docs WHERE id = 2 AND json_extract(body, '$.x') = 1",
        "SELECT id FROM docs WHERE json_extract(body, '$.x') = 1 AND id > 0",
        "SELECT docs.id FROM ids JOIN docs ON docs.id = ids.id WHERE json_extract(docs.body, '$.x') = 1",
        "SELECT docs.id FROM docs JOIN ids ON docs.id = ids.id WHERE json_extract(docs.body, '$.x') = 1",
        "SELECT ids.id FROM ids LEFT JOIN docs ON docs.id = ids.id AND json_extract(docs.body, '$.x') = 1",
        "SELECT id FROM (SELECT * FROM docs) WHERE json_extract(body, '$.x') = 1",
        "SELECT id FROM docs WHERE EXISTS (SELECT 1 WHERE json_extract(docs.body, '$.x') = 1)",
        "SELECT json_extract(body, '$.x') FROM docs",
        "SELECT id FROM docs ORDER BY json_extract(body, '$.x')",
        "SELECT count(*) FROM docs GROUP BY json_extract(body, '$.x')",
    ];
    for sql in queries {
        conn.execute(sql)
            .unwrap_or_else(|err| panic!("{sql}: {err}"));
    }
    conn.set_role(None).unwrap();
    conn.execute("CREATE INDEX docs_owner ON docs(owner)")
        .unwrap();
    conn.set_role(Some("alice")).unwrap();
    for sql in queries {
        conn.execute(sql)
            .unwrap_or_else(|err| panic!("with index, {sql}: {err}"));
    }
}
