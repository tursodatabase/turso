use crate::common::TempDatabase;
use turso_core::DatabaseOpts;

fn generated_columns_db() -> TempDatabase {
    TempDatabase::builder()
        .with_opts(DatabaseOpts::new().with_generated_columns(true))
        .build()
}

fn assert_schema_rejected(sql: &str) {
    let db = generated_columns_db();
    let conn = db.connect_limbo();
    let err = conn
        .execute(sql)
        .expect_err("schema statement should be rejected");
    assert!(
        err.to_string().contains("generated")
            || err.to_string().contains("DEFAULT")
            || err.to_string().contains("syntax"),
        "unexpected error: {err}"
    );
}

#[turso_macros::test]
fn test_generated_columns_reject_default_clause(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    assert_schema_rejected("CREATE TABLE t(a, b DEFAULT 5 AS (a + 1))");

    let db = generated_columns_db();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)")?;
    conn.execute("ALTER TABLE t ADD COLUMN b DEFAULT 5 AS (a + 1)")
        .expect_err("ALTER ADD generated column with DEFAULT should be rejected");

    Ok(())
}

#[turso_macros::test]
fn test_generated_columns_reject_unknown_type_tokens(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    assert_schema_rejected("CREATE TABLE t(a, b AS (a) WAT)");
    assert_schema_rejected("CREATE TABLE t(a, b AS (a + 1) HIDDEN, c)");

    Ok(())
}

#[turso_macros::test]
fn test_generated_columns_reject_duplicate_generated_clauses(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    drop(tmp_db);

    assert_schema_rejected("CREATE TABLE t(a, b, c AS (a + 1) AS (a + 2))");

    Ok(())
}
