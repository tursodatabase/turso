use std::sync::Arc;

use crate::common::TempDatabase;
use parking_lot::RwLock;
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
fn right_join_with_table_with_row_security_is_rejected() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);

    conn.execute("SET ROLE alice").unwrap();

    for sql in [
        "SELECT docs.id FROM ids RIGHT JOIN docs ON docs.id = ids.id",
        "SELECT ids.id FROM docs RIGHT JOIN ids ON docs.id = ids.id",
        "SELECT ids.id FROM ids JOIN ids AS other ON true RIGHT JOIN docs ON true",
    ] {
        assert_error_contains(&conn, sql, "RIGHT JOIN");
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
            "CREATE POLICY p ON docs FOR SELECT USING (true) WITH CHECK (true)",
            "WITH CHECK cannot be applied to SELECT or DELETE",
        ),
        (
            "CREATE POLICY p ON docs FOR DELETE WITH CHECK (true)",
            "WITH CHECK cannot be applied to SELECT or DELETE",
        ),
        (
            "CREATE POLICY p ON docs FOR INSERT USING (true)",
            "only WITH CHECK expression allowed for INSERT",
        ),
        (
            "CREATE POLICY p ON docs FOR INSERT WITH CHECK (missing = 1)",
            "missing",
        ),
        (
            "CREATE POLICY p ON docs TO nobody USING (true)",
            "role \"nobody\" does not exist",
        ),
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
fn policy_applies_only_to_its_roles() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    for sql in [
        "CREATE ROLE bob",
        "CREATE ROLE carol",
        "CREATE POLICY readers ON docs FOR SELECT TO alice, carol USING (true)",
        "CREATE POLICY everyone ON docs TO PUBLIC USING (id = 2)",
    ] {
        conn.execute(sql).unwrap();
    }

    for (role, expected) in [
        ("alice", vec![1, 2, 3]),
        ("bob", vec![2]),
        ("carol", vec![1, 2, 3]),
    ] {
        conn.execute(format!("SET ROLE {role}")).unwrap();
        assert_eq!(
            integers(&conn, "SELECT id FROM docs ORDER BY id"),
            expected,
            "{role}"
        );
    }
}

#[test]
fn statement_prepared_by_one_role_uses_the_policies_of_the_next() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    conn.execute("CREATE ROLE bob").unwrap();
    conn.execute("CREATE POLICY readers ON docs TO alice USING (true)")
        .unwrap();
    conn.execute("SET ROLE alice").unwrap();
    let mut stmt = conn.prepare("SELECT count(*) FROM docs").unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap(), [[Value::from_i64(3)]]);

    conn.execute("SET ROLE bob").unwrap();

    stmt.reset().unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap(), [[Value::from_i64(0)]]);
}

/// The policies a Supabase project writes: the user comes from the request's
/// JWT claims, read by auth.uid(), and the request runs as the
/// `authenticated` or `anon` role.
#[test]
fn supabase_policy_shows_each_user_their_rows() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    for sql in [
        "CREATE TABLE todos (id int PRIMARY KEY, user_id uuid, title text)",
        "INSERT INTO todos VALUES \
            (1, '11111111-1111-1111-1111-111111111111', 'a'), \
            (2, '22222222-2222-2222-2222-222222222222', 'b'), \
            (3, '11111111-1111-1111-1111-111111111111', 'c')",
        "CREATE ROLE anon",
        "CREATE ROLE authenticated",
        "ALTER TABLE todos ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY own ON todos FOR SELECT TO authenticated USING ((SELECT auth.uid()) = user_id)",
    ] {
        conn.execute(sql).unwrap();
    }
    conn.execute("BEGIN").unwrap();
    conn.execute("SET LOCAL ROLE authenticated").unwrap();
    let mut stmt = conn.prepare("SELECT id FROM todos ORDER BY id").unwrap();

    for (user, expected) in [
        ("11111111-1111-1111-1111-111111111111", vec![1, 3]),
        ("22222222-2222-2222-2222-222222222222", vec![2]),
        ("33333333-3333-3333-3333-333333333333", vec![]),
    ] {
        conn.execute(format!(
            "SELECT set_config('request.jwt.claims', '{{\"sub\": \"{user}\", \"role\": \"authenticated\"}}', true)"
        ))
        .unwrap();
        stmt.reset().unwrap();
        let ids: Vec<_> = stmt
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        let expected: Vec<_> = expected.into_iter().map(Value::from_i64).collect();
        assert_eq!(ids, expected, "{user}");
    }
    conn.execute("COMMIT").unwrap();

    conn.execute("SET ROLE anon").unwrap();
    assert_eq!(integers(&conn, "SELECT count(*) FROM todos"), [0]);
}

#[test]
fn restrictive_policies_narrow_the_permissive_ones() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);
    conn.execute("CREATE POLICY first_two ON docs AS RESTRICTIVE USING (id < 3)")
        .unwrap();
    conn.execute("CREATE POLICY not_bob ON docs AS RESTRICTIVE FOR SELECT TO bob USING (false)")
        .unwrap();

    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT id FROM docs ORDER BY id"), [1]);
    conn.execute("SET ROLE bob").unwrap();
    assert_eq!(integers(&conn, "SELECT count(*) FROM docs"), [0]);

    conn.execute("RESET ROLE").unwrap();
    conn.execute("DROP POLICY own ON docs").unwrap();
    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT count(*) FROM docs"), [0]);
}

/// Policies for writes are kept so that scripts written for PostgreSQL run,
/// but roles cannot write to tables with row-level security yet, so they
/// change nothing. They do not show rows to SELECT.
#[test]
fn write_policies_are_kept_but_show_no_rows() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    for sql in [
        "CREATE POLICY ins ON docs FOR INSERT WITH CHECK (owner = current_user)",
        "CREATE POLICY upd ON docs FOR UPDATE USING (true) WITH CHECK (owner = current_user)",
        "CREATE POLICY del ON docs FOR DELETE USING (true)",
        "CREATE POLICY nothing ON docs",
    ] {
        conn.execute(sql).unwrap();
    }
    assert_error_contains(
        &conn,
        "ALTER TABLE docs RENAME COLUMN owner TO author",
        "row-level security",
    );

    conn.execute("SET ROLE alice").unwrap();

    assert_eq!(integers(&conn, "SELECT count(*) FROM docs"), [0]);
    for sql in [
        "INSERT INTO docs VALUES (4, 'alice')",
        "UPDATE docs SET owner = 'alice'",
        "DELETE FROM docs",
    ] {
        assert!(conn.execute(sql).is_err(), "{sql}");
    }
    conn.execute("RESET ROLE").unwrap();
    assert_eq!(integers(&conn, "SELECT count(*) FROM docs"), [3]);
}

#[test]
fn policy_definition_is_kept_after_reopening_the_database() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_row_security(&conn);
    for sql in [
        "CREATE ROLE bob",
        "CREATE POLICY readers ON docs FOR SELECT TO alice, bob USING (true)",
        "CREATE POLICY first_two ON docs AS RESTRICTIVE TO alice USING (id < 3)",
        "CREATE POLICY ins ON docs FOR INSERT TO alice WITH CHECK (true)",
    ] {
        conn.execute(sql).unwrap();
    }
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_postgres();
    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT id FROM docs ORDER BY id"), [1, 2]);
    conn.execute("SET ROLE bob").unwrap();
    assert_eq!(
        integers(&conn, "SELECT id FROM docs ORDER BY id"),
        [1, 2, 3]
    );
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

/// The first ALTER TABLE ... ENABLE ROW LEVEL SECURITY creates the table
/// that stores row-level security, so a rollback must remove it from the
/// schema as well as from the file.
#[test]
fn rolled_back_first_enable_leaves_no_row_security_behind() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    for sql in [
        "CREATE TABLE docs (id int PRIMARY KEY, owner text)",
        "INSERT INTO docs VALUES (1, 'alice'), (2, 'bob')",
        "CREATE ROLE alice",
        "BEGIN",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY own ON docs USING (owner = current_user)",
        "ROLLBACK",
    ] {
        conn.execute(sql).unwrap();
    }

    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT id FROM docs ORDER BY id"), [1, 2]);
    conn.execute("RESET ROLE").unwrap();

    conn.execute("ALTER TABLE docs ENABLE ROW LEVEL SECURITY")
        .unwrap();
    conn.execute("CREATE POLICY own ON docs USING (owner = current_user)")
        .unwrap();
    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(integers(&conn, "SELECT id FROM docs ORDER BY id"), [1]);
}

/// A virtual table that reads pages would show the rows that row-level
/// security hides, whatever its name.
#[test]
fn role_cannot_read_a_virtual_table_that_reads_raw_storage() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    create_docs_with_owner_policy(&conn);
    conn.inner()
        .register_internal_vtab(PageReader {
            reads_raw_storage: true,
        })
        .unwrap();
    conn.inner()
        .register_internal_vtab(PageReader {
            reads_raw_storage: false,
        })
        .unwrap();

    conn.execute("SET ROLE alice").unwrap();

    assert_error_contains(
        &conn,
        "SELECT * FROM raw_page_reader",
        "permission denied: raw_page_reader requires the superuser",
    );
    assert_eq!(
        rows(&conn, "SELECT * FROM page_reader"),
        Vec::<Vec<Value>>::new()
    );
}

/// An empty virtual table that claims to read the database file when
/// `reads_raw_storage` is set.
#[derive(Debug)]
struct PageReader {
    reads_raw_storage: bool,
}

impl turso_core::InternalVirtualTable for PageReader {
    fn name(&self) -> String {
        if self.reads_raw_storage {
            "raw_page_reader".to_string()
        } else {
            "page_reader".to_string()
        }
    }

    fn sql(&self) -> String {
        format!("CREATE TABLE {}(data BLOB)", self.name())
    }

    fn reads_raw_storage(&self) -> bool {
        self.reads_raw_storage
    }

    fn open(
        &self,
        _conn: Arc<turso_core::Connection>,
    ) -> turso_core::Result<Arc<RwLock<dyn turso_core::InternalVirtualTableCursor>>> {
        Ok(Arc::new(RwLock::new(EmptyCursor)))
    }

    fn best_index(
        &self,
        constraints: &[turso_ext::ConstraintInfo],
        _order_by: &[turso_ext::OrderByInfo],
    ) -> Result<turso_ext::IndexInfo, turso_ext::ResultCode> {
        Ok(turso_ext::IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1.0,
            estimated_rows: 0,
            constraint_usages: constraints
                .iter()
                .map(|_| turso_ext::ConstraintUsage {
                    argv_index: None,
                    omit: false,
                })
                .collect(),
        })
    }
}

struct EmptyCursor;

impl turso_core::InternalVirtualTableCursor for EmptyCursor {
    fn next(&mut self) -> Result<bool, turso_core::LimboError> {
        Ok(false)
    }

    fn rowid(&self) -> i64 {
        0
    }

    fn column(&self, _column: usize) -> Result<Value, turso_core::LimboError> {
        unreachable!("the table has no rows")
    }

    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<String>,
        _idx_num: i32,
    ) -> Result<bool, turso_core::LimboError> {
        Ok(false)
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
