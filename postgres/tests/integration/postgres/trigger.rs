use crate::common::TempDatabase;
use turso_core::Value;
use turso_pg::PgConnection;

fn query_rows(conn: &PgConnection, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn create_audit_schema(conn: &PgConnection) {
    conn.execute("CREATE TABLE users (id int PRIMARY KEY, name text)")
        .unwrap();
    conn.execute("CREATE TABLE audit (op text, user_id int)")
        .unwrap();
    conn.execute(
        "CREATE FUNCTION log_user() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN INSERT INTO audit (op, user_id) VALUES (TG_OP, NEW.id); RETURN NEW; END $$",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER log_users AFTER INSERT OR UPDATE ON users \
         FOR EACH ROW EXECUTE FUNCTION log_user()",
    )
    .unwrap();
}

#[turso_macros::test]
fn test_plpgsql_trigger_survives_a_reopen(db: TempDatabase) {
    let path = db.path.to_str().unwrap().to_string();
    let io = db.io.clone();
    {
        let conn = db.connect_postgres();
        create_audit_schema(&conn);
        conn.execute("INSERT INTO users VALUES (1, 'ann')").unwrap();
        conn.close().unwrap();
    }
    drop(db);

    let db = turso_pg::open_database_with_io(
        io,
        &path,
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true),
    )
    .unwrap();
    let conn = PgConnection::connect(&db).unwrap();
    conn.execute("UPDATE users SET name = 'anne' WHERE id = 1")
        .unwrap();
    assert_eq!(
        query_rows(&conn, "SELECT op, user_id FROM audit ORDER BY rowid"),
        vec![
            vec![Value::build_text("INSERT"), Value::from_i64(1)],
            vec![Value::build_text("UPDATE"), Value::from_i64(1)],
        ]
    );
    assert_eq!(
        conn.inner()
            .prepare_sqlite(
                "SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = 'log_users'"
            )
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::build_text(
            "/* turso_frontend:postgres */ CREATE TRIGGER log_users AFTER INSERT OR UPDATE ON users \
             FOR EACH ROW EXECUTE FUNCTION log_user()"
        )]]
    );

    conn.execute("DROP TRIGGER log_users ON users").unwrap();
    conn.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
    conn.execute("UPDATE users SET name = 'bobby' WHERE id = 2")
        .unwrap();
    assert_eq!(
        query_rows(&conn, "SELECT count(*) FROM audit"),
        vec![vec![Value::from_i64(2)]]
    );
}

#[turso_macros::test(mvcc)]
fn test_prepared_statement_uses_the_replaced_trigger_function(db: TempDatabase) {
    let conn = db.connect_postgres();
    create_audit_schema(&conn);
    let mut insert = conn.prepare("INSERT INTO users VALUES (1, 'ann')").unwrap();
    insert.run_ignore_rows().unwrap();
    insert.reset().unwrap();

    let other = db.connect_postgres();
    other
        .execute(
            "CREATE OR REPLACE FUNCTION log_user() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN INSERT INTO audit (op, user_id) VALUES ('replaced', NEW.id); RETURN NEW; END $$",
        )
        .unwrap();
    conn.execute("DELETE FROM users").unwrap();
    insert.run_ignore_rows().unwrap();
    assert_eq!(
        query_rows(&conn, "SELECT op FROM audit ORDER BY rowid"),
        vec![
            vec![Value::build_text("INSERT")],
            vec![Value::build_text("replaced")],
        ]
    );
}

#[turso_macros::test]
fn test_rename_of_a_table_with_a_plpgsql_trigger_is_rejected(db: TempDatabase) {
    let conn = db.connect_postgres();
    create_audit_schema(&conn);
    let err = conn
        .execute("ALTER TABLE users RENAME TO people")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("trigger log_users executes function log_user"),
        "{err}"
    );
    let err = conn
        .execute("ALTER TABLE users RENAME COLUMN name TO full_name")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("trigger log_users executes function log_user"),
        "{err}"
    );
    conn.execute("ALTER TABLE users ADD COLUMN email text")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'ann', 'ann@example.com')")
        .unwrap();
    assert_eq!(
        query_rows(&conn, "SELECT user_id FROM audit"),
        vec![vec![Value::from_i64(1)]]
    );
}

#[turso_macros::test]
fn test_prepared_update_maintains_the_index_of_the_column_the_replaced_function_sets(
    db: TempDatabase,
) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE docs (id int PRIMARY KEY, title text, tag text)")
        .unwrap();
    conn.execute("CREATE INDEX docs_tag ON docs (tag)").unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'a', 'none')")
        .unwrap();
    let create_set_tag = |tag: &str| {
        conn.execute(format!(
            "CREATE OR REPLACE FUNCTION set_tag() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN NEW.tag := '{tag}'; RETURN NEW; END $$"
        ))
        .unwrap();
    };
    create_set_tag("first");
    conn.execute(
        "CREATE TRIGGER docs_set_tag BEFORE UPDATE ON docs FOR EACH ROW EXECUTE FUNCTION set_tag()",
    )
    .unwrap();
    let mut update = conn
        .prepare("UPDATE docs SET title = 'b' WHERE id = 1")
        .unwrap();
    update.run_ignore_rows().unwrap();
    update.reset().unwrap();
    let tags_through_index =
        |tag: &str| query_rows(&conn, &format!("SELECT id FROM docs WHERE tag = '{tag}'"));
    assert_eq!(tags_through_index("first"), vec![vec![Value::from_i64(1)]]);

    create_set_tag("second");
    update.run_ignore_rows().unwrap();
    assert_eq!(tags_through_index("second"), vec![vec![Value::from_i64(1)]]);
    assert!(tags_through_index("first").is_empty());
    assert_eq!(
        query_rows(&conn, "SELECT title, tag FROM docs"),
        vec![vec![Value::build_text("b"), Value::build_text("second")]]
    );
}
