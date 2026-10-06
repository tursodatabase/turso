use crate::common::TempDatabase;
use turso_core::Value;
use turso_pg::PgConnection;

fn rows(conn: &PgConnection, sql: &str) -> Vec<String> {
    let mut stmt = conn.query(sql).unwrap().unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::Null => "NULL".to_string(),
                    value => value.to_string(),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[turso_macros::test(mvcc)]
fn add_and_drop_column_keep_function_defaults_readable(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE s (id serial PRIMARY KEY, a text, b text, \
         ts timestamp DEFAULT now(), c text DEFAULT 'x'::text, \
         e text DEFAULT ('y' || 'z'))",
    )
    .unwrap();
    conn.execute("INSERT INTO s (a, b, ts) VALUES ('one', 'gone', '2024-01-01 10:00:00')")
        .unwrap();
    conn.execute("ALTER TABLE s DROP COLUMN b").unwrap();
    conn.execute("ALTER TABLE s ADD COLUMN d text DEFAULT 'q'")
        .unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO s (a, ts) VALUES ('two', '2024-01-02 10:00:00')")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, a, ts, c, e, d FROM s ORDER BY id"),
        [
            "1|one|2024-01-01 10:00:00|x|yz|q",
            "2|two|2024-01-02 10:00:00|x|yz|q"
        ]
    );
}
