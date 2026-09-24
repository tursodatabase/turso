#![cfg(feature = "fts")]

use turso::{Connection, Database};

const RAW_JSON: &str = "json_fields='properties', json_tokenizer='raw'";

#[tokio::test]
async fn raw_json_preserves_exact_references_and_text_search() {
    let (_db, conn) = docs(RAW_JSON).await;
    insert(&conn, 1, r#"{"person":"PERSON-1","status":"active"}"#).await;
    insert(&conn, 2, r#"{"person":"PERSON 1","status":"inactive"}"#).await;
    hits(&conn, "title:release AND body:meeting", &[1, 2]).await;
    hits(&conn, "properties.person:PERSON-1", &[1]).await;
    hits(&conn, "properties.person:person-1", &[]).await;
    hits(&conn, "title:release AND properties.status:active", &[1]).await;
    hits(
        &conn,
        "properties.status:active OR properties.status:inactive",
        &[1, 2],
    )
    .await;
}

#[tokio::test]
async fn json_indexes_nested_values_arrays_numbers_and_booleans() {
    let (_db, conn) = docs(RAW_JSON).await;
    insert(&conn, 1, r#"{"address":{"city":"London"},"reviewers":["PERSON-1","PERSON-2"],"budget":100,"done":true}"#).await;
    insert(
        &conn,
        2,
        r#"{"address":{"city":"Paris"},"reviewers":[],"budget":20,"done":false}"#,
    )
    .await;
    for query in [
        "properties.address.city:London",
        "properties.reviewers:PERSON-2",
        "properties.budget:[50 TO 200]",
        "properties.done:true",
    ] {
        hits(&conn, query, &[1]).await;
    }
}

#[tokio::test]
async fn unicode_keys_and_escaped_punctuation_remain_distinct() {
    let (_db, conn) = docs(RAW_JSON).await;
    insert(&conn, 1, r#"{"имя":"Анна","first_name":"Анна","имя пользователя":"Анна","person.name":"Анна","a:b":"colon","a\\b":"slash","a\"b":"quote","a[b]":"brackets","emoji😀":"да","ключ-с-дефисом":"да","версия":"1.2.3"}"#).await;
    insert(&conn, 2, r#"{"person":{"name":"Анна"}}"#).await;
    for query in [
        "properties.имя:Анна",
        "properties.first_name:Анна",
        r"properties.имя\ пользователя:Анна",
        r"properties.person\.name:Анна",
        r"properties.a\:b:colon",
        r"properties.a\\\\b:slash",
        r#"properties.a\"b:quote"#,
        r"properties.a\[b\]:brackets",
        "properties.emoji😀:да",
        "properties.ключ-с-дефисом:да",
        r#"properties.версия:"1.2.3""#,
    ] {
        hits(&conn, query, &[1]).await;
    }
    hits(&conn, "properties.person.name:Анна", &[2]).await;
}

#[tokio::test]
async fn null_and_empty_objects_are_allowed_but_invalid_roots_are_rejected() {
    let (_db, conn) = docs(RAW_JSON).await;
    conn.execute("INSERT INTO docs VALUES(1,'SQL null','',NULL)", ())
        .await
        .expect("test operation should succeed");
    insert(&conn, 2, "null").await;
    insert(&conn, 3, "{}").await;
    for invalid in ["invalid", "[]", "[{}]", "123", "true", r#""string""#] {
        let error = conn
            .execute("INSERT INTO docs VALUES(99,'Bad','',?)", [invalid])
            .await
            .expect_err("invalid input should be rejected");
        assert!(
            error
                .to_string()
                .contains("invalid JSON object in FTS column 'properties'"),
            "{error}"
        );
    }
    let error = conn
        .execute("INSERT INTO docs VALUES(99,'Bad','',x'7b7d')", ())
        .await
        .expect_err("invalid input should be rejected");
    assert!(
        error.to_string().contains("requires JSON text or NULL"),
        "{error}"
    );
    assert_eq!(row_count(&conn).await, 3);
    hits(&conn, "properties.person:PERSON-1", &[]).await;
}

#[tokio::test]
async fn invalid_json_rolls_back_the_whole_statement_and_preserves_existing_rows() {
    let (_db, conn) = docs(RAW_JSON).await;
    insert(&conn, 1, r#"{"person":"PERSON-1"}"#).await;
    let sql =
        r#"INSERT INTO docs VALUES(2,'First','','{"person":"PERSON-2"}'),(3,'Bad','','invalid')"#;
    assert!(conn.execute(sql, ()).await.is_err());
    assert_eq!(row_count(&conn).await, 1);
    hits(&conn, "properties.person:PERSON-2", &[]).await;
    assert!(conn
        .execute("UPDATE docs SET properties='invalid' WHERE id=1", ())
        .await
        .is_err());
    hits(&conn, "properties.person:PERSON-1", &[1]).await;
}

#[tokio::test]
async fn invalid_json_options_report_the_problem() {
    let (_db, conn) = database(":memory:").await;
    conn.execute("CREATE TABLE docs(properties TEXT)", ())
        .await
        .expect("test operation should succeed");
    for (options, message) in [
        ("json_fields='unknown'", "unknown FTS JSON column 'unknown'"),
        (
            "json_fields='properties,PROPERTIES'",
            "duplicate FTS JSON column",
        ),
        ("json_fields=''", "empty column name"),
        ("json_fields='properties,'", "empty column name"),
        ("json_fields=42", "comma-separated list"),
        ("json_tokenizer='raw'", "requires json_fields"),
        (
            "json_fields='properties',json_tokenizer=42",
            "json_tokenizer must be text",
        ),
        (
            "json_fields='properties',json_tokenizer='unknown'",
            "unsupported FTS json_tokenizer 'unknown'",
        ),
        (
            "json_fields='properties',json_tokenizer='raw',min_gram=4",
            "require tokenizer",
        ),
    ] {
        let sql = format!("CREATE INDEX bad ON docs USING fts(properties) WITH ({options})");
        let error = conn
            .execute(&sql, ())
            .await
            .expect_err("invalid input should be rejected");
        assert!(error.to_string().contains(message), "{options}: {error}");
    }
}

#[tokio::test]
async fn json_tokenizer_inherits_the_text_tokenizer() {
    let (_db, conn) = docs("json_fields='properties'").await;
    insert(&conn, 1, r#"{"description":"Release planning"}"#).await;
    hits(&conn, "properties.description:release", &[1]).await;
    let (_db, conn) = docs("json_fields='properties',tokenizer='raw'").await;
    insert(&conn, 1, r#"{"description":"Release planning"}"#).await;
    hits(&conn, "properties.description:release", &[]).await;
    hits(&conn, r#"properties.description:"Release planning""#, &[1]).await;
}

#[tokio::test]
async fn json_ngram_accepts_a_custom_window_without_changing_text_tokenization() {
    let (_db, conn) =
        docs("json_fields='properties',json_tokenizer='ngram',min_gram=4,max_gram=4").await;
    insert(&conn, 1, r#"{"tag":"abcdefgh"}"#).await;
    hits(&conn, "properties.tag:abcd", &[1]).await;
    hits(&conn, "properties.tag:abc", &[]).await;
    hits(&conn, "title:release", &[1]).await;
    hits(&conn, "title:rele", &[]).await;
}

#[tokio::test]
async fn json_column_names_are_case_insensitive_and_multiple_columns_work() {
    let (_db, conn) = docs("JSON_FIELDS='PROPERTIES, body',JSON_TOKENIZER='raw'").await;
    conn.execute("INSERT INTO docs VALUES(1,'Release','{\"person\":\"PERSON-2\"}','{\"person\":\"PERSON-1\"}')", ()).await.expect("test operation should succeed");
    hits(
        &conn,
        "body.person:PERSON-2 AND properties.person:PERSON-1",
        &[1],
    )
    .await;
}

#[tokio::test]
async fn json_index_survives_update_rollback_merge_reopen_and_delete() {
    let dir = tempfile::tempdir().expect("test operation should succeed");
    let path = dir.path().join("index.db");
    let path = path.to_str().expect("test operation should succeed");
    let (db, conn) = database(path).await;
    conn.execute(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,body TEXT,properties TEXT)",
        (),
    )
    .await
    .expect("test operation should succeed");
    insert(&conn, 1, r#"{"person":"PERSON-1"}"#).await;
    conn.execute(
        &format!(
            "CREATE INDEX docs_fts ON docs USING fts(title,body,properties) WITH ({RAW_JSON})"
        ),
        (),
    )
    .await
    .expect("test operation should succeed");
    hits(&conn, "properties.person:PERSON-1", &[1]).await;
    conn.execute("BEGIN", ())
        .await
        .expect("test operation should succeed");
    conn.execute(
        "UPDATE docs SET properties=? WHERE id=1",
        [r#"{"person":"PERSON-2"}"#],
    )
    .await
    .expect("test operation should succeed");
    conn.execute("ROLLBACK", ())
        .await
        .expect("test operation should succeed");
    hits(&conn, "properties.person:PERSON-1", &[1]).await;
    hits(&conn, "properties.person:PERSON-2", &[]).await;
    conn.execute(
        "UPDATE docs SET properties=? WHERE id=1",
        [r#"{"person":"PERSON-2"}"#],
    )
    .await
    .expect("test operation should succeed");
    hits(&conn, "properties.person:PERSON-1", &[]).await;
    conn.execute("OPTIMIZE INDEX docs_fts", ())
        .await
        .expect("test operation should succeed");
    drop(conn);
    drop(db);
    let (_db, conn) = database(path).await;
    hits(&conn, "properties.person:PERSON-2", &[1]).await;
    hits(&conn, "title:release", &[1]).await;
    conn.execute("DELETE FROM docs WHERE id=1", ())
        .await
        .expect("test operation should succeed");
    hits(&conn, "properties.person:PERSON-2", &[]).await;
}

#[tokio::test]
async fn text_only_indexes_keep_their_existing_behavior() {
    let (_db, conn) = docs("").await;
    insert(&conn, 1, r#"{"description":"Release planning"}"#).await;
    hits(&conn, "properties:planning", &[1]).await;
    hits(&conn, "title:release", &[1]).await;
}

async fn docs(options: &str) -> (Database, Connection) {
    let (db, conn) = database(":memory:").await;
    conn.execute(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT, properties TEXT)",
        (),
    )
    .await
    .expect("test operation should succeed");
    let options = if options.is_empty() {
        String::new()
    } else {
        format!("WITH ({options})")
    };
    conn.execute(
        &format!("CREATE INDEX docs_fts ON docs USING fts(title,body,properties) {options}"),
        (),
    )
    .await
    .expect("test operation should succeed");
    (db, conn)
}

async fn insert(conn: &Connection, id: i64, properties: &str) {
    conn.execute(
        "INSERT INTO docs VALUES(?1,'Release checklist','Meeting notes',?2)",
        turso::params![id, properties],
    )
    .await
    .expect("test operation should succeed");
}

async fn hits(conn: &Connection, query: &str, expected: &[i64]) {
    let mut rows = conn
        .query(
            "SELECT id FROM docs WHERE fts_match(title,body,properties,?) ORDER BY id",
            [query],
        )
        .await
        .expect("test operation should succeed");
    let mut actual = Vec::new();
    while let Some(row) = rows.next().await.expect("test operation should succeed") {
        actual.push(row.get::<i64>(0).expect("test operation should succeed"));
    }
    assert_eq!(actual, expected, "query: {query}");
}

async fn row_count(conn: &Connection) -> i64 {
    conn.query("SELECT count(*) FROM docs", ())
        .await
        .expect("test operation should succeed")
        .next()
        .await
        .expect("test operation should succeed")
        .expect("test operation should succeed")
        .get::<i64>(0)
        .expect("test operation should succeed")
}

async fn database(path: &str) -> (Database, Connection) {
    let db = turso::Builder::new_local(path)
        .experimental_index_method(true)
        .build()
        .await
        .expect("test operation should succeed");
    let conn = db.connect().expect("test operation should succeed");
    (db, conn)
}
