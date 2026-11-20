use turso_core::index_method::fts::FtsIndexAttachment;
use turso_core::index_method::IndexMethodConfiguration;
use turso_core::schema::IndexColumn;
use turso_core::security::roles::SessionRole;
use turso_core::sync::Arc;
use turso_core::CollationSeq;
use turso_core::Connection;
use turso_core::LimboError;
use turso_core::Value;
use turso_sqlite::SqliteDialect;

/// 25 documents inserted by one statement, so the index holds them in one segment in id order:
/// ids 1 to 20 match 'hello' and the rest do not, and every match is in category 0 but the last,
/// id 20, in category 1.
fn documents_in_categories() -> Arc<Connection> {
    let db = turso_core::Database::open(
        Arc::new(turso_core::MemoryIO::new()),
        ":memory:",
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(turso_core::DatabaseOpts::default().with_index_method(true)),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT, cat INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    let rows: Vec<String> = (1..=25i64)
        .map(|id| {
            let body = if id <= 20 {
                "hello ".repeat((id % 3 + 1) as usize) + "world"
            } else {
                "goodbye world".to_string()
            };
            format!("({id}, '{body}', {})", i64::from(id == 20))
        })
        .collect();
    conn.execute(format!("INSERT INTO docs VALUES {}", rows.join(", ")))
        .unwrap();
    conn
}

fn collected(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

/// An FTS pattern with LIMIT applies the LIMIT to the matches it gives out, so it cannot serve a
/// query whose aggregate or DISTINCT comes before the LIMIT: the aggregate counts every match, and
/// the DISTINCT keeps every category.
#[test]
fn fts_limit_applies_after_aggregates_and_distinct() {
    let conn = documents_in_categories();
    assert_eq!(
        collected(
            &conn,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'hello') LIMIT 5"
        ),
        vec![vec![Value::from_i64(20)]]
    );
    let mut categories = collected(
        &conn,
        "SELECT DISTINCT cat FROM docs WHERE fts_match(body, 'hello') LIMIT 2",
    );
    categories.sort_by_key(|row| row[0].as_int());
    assert_eq!(
        categories,
        vec![vec![Value::from_i64(0)], vec![Value::from_i64(1)]]
    );
}

/// Nine documents inserted by one statement, mentioning 'hello' once, twice or three times, so
/// their FTS scores take three values.
fn scored_documents() -> Arc<Connection> {
    let db = turso_core::Database::open(
        Arc::new(turso_core::MemoryIO::new()),
        ":memory:",
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(turso_core::DatabaseOpts::default().with_index_method(true)),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX notes_fts ON notes USING fts(body)")
        .unwrap();
    let rows: Vec<String> = (1..=9i64)
        .map(|id| format!("({id}, '{}world')", "hello ".repeat((id % 3 + 1) as usize)))
        .collect();
    conn.execute(format!("INSERT INTO notes VALUES {}", rows.join(", ")))
        .unwrap();
    conn
}

fn rows_of(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

/// The score the FTS index provides for a query is a computed value with no column collation, so
/// a DISTINCT, a compound SELECT and an IN subquery over it plan and run.
#[test]
fn fts_score_from_the_index_has_no_column_collation() {
    let conn = scored_documents();
    let score = "fts_score(body, 'hello')";
    let matching = "FROM notes WHERE fts_match(body, 'hello')";
    assert_eq!(
        rows_of(&conn, &format!("SELECT DISTINCT {score} {matching}")).len(),
        3
    );
    assert_eq!(
        rows_of(
            &conn,
            &format!("SELECT {score} {matching} UNION SELECT {score} {matching}")
        )
        .len(),
        3
    );
    assert_eq!(
        rows_of(
            &conn,
            &format!(
                "SELECT count(*) FROM (SELECT {score} AS s {matching}) WHERE s IN (SELECT {score} {matching})"
            )
        ),
        vec![vec![Value::from_i64(9)]]
    );
}

#[test]
fn field_weights_reject_non_finite_and_non_positive_values() {
    let mut accepted = Vec::new();
    for weight in [
        "NaN",
        "nan",
        "+NaN",
        "-NaN",
        "inf",
        "+inf",
        "-inf",
        "Infinity",
        "+INFINITY",
        "-infinity",
        "1e39",
        "-1e39",
        "0",
        "-0",
        "-1",
        "1e-46",
    ] {
        let result = FtsIndexAttachment::new(
            &SqliteDialect,
            IndexMethodConfiguration {
                table_name: "w".to_string(),
                index_name: "wx".to_string(),
                columns: turso_core::alloc::vec![
                    IndexColumn::new("title", 0, CollationSeq::BINARY),
                    IndexColumn::new("body", 1, CollationSeq::BINARY)
                ],
                parameters: turso_core::HashMap::from_iter([(
                    "weights".to_string(),
                    Value::from_text(format!("title={weight},body=1")),
                )]),
            },
        );
        match result {
            Ok(_) => accepted.push(weight),
            Err(error) => assert!(
                matches!(error, LimboError::ParseError(_)),
                "{weight}: {error}"
            ),
        }
    }
    assert!(
        accepted.is_empty(),
        "accepted invalid weights: {accepted:?}"
    );
}
