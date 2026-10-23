use rand::Rng;
use turso_stress::ThreadId;

use crate::{conn::StressConn, sql_logging::SqlLogger, ThreadRng};

pub const TABLES: [&str; 2] = ["stress_seq_log", "stress_autoinc"];

pub fn schema() -> Vec<String> {
    vec![
        "CREATE SEQUENCE IF NOT EXISTS stress_seq".to_string(),
        "CREATE TABLE IF NOT EXISTS stress_seq_log (id TEXT PRIMARY KEY)".to_string(),
        "CREATE TABLE IF NOT EXISTS stress_autoinc (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT)"
            .to_string(),
    ]
}

pub async fn insert_then_advance_sequence(
    conn: &StressConn,
    rng: &mut ThreadRng,
    logger: &SqlLogger,
    thread: &ThreadId,
    thread_idx: usize,
    batch_idx: u64,
    interaction_idx: usize,
) -> Option<String> {
    let key = format!("{thread_idx}-{batch_idx}-{interaction_idx}");
    let insert = format!("INSERT INTO stress_seq_log VALUES ('{key}')");
    if let Err(e) = conn.execute(&insert, ()).await {
        assert_not_corrupt(thread, &insert, e);
        return None;
    }
    let advance = if rng.random_ratio(1, 2) {
        "SELECT nextval('stress_seq')".to_string()
    } else {
        format!("INSERT INTO stress_autoinc (key) VALUES ('{key}')")
    };
    let result = async {
        let mut rows = conn.query(&advance, ()).await?;
        while rows.next().await?.is_some() {}
        Ok(())
    }
    .await;
    logger.log_result(thread, &advance, &result);
    if let Err(e) = result {
        assert_not_corrupt(thread, &advance, e);
        return None;
    }
    match conn.in_transaction() {
        Ok(true) => Some(key),
        _ => None,
    }
}

pub async fn check_row_committed(
    conn: &StressConn,
    key: &str,
    logger: &SqlLogger,
    thread: &ThreadId,
) -> turso::Result<()> {
    let sql = format!("SELECT count(*) FROM stress_seq_log WHERE id = '{key}'");
    if let Err(e) = assert_row_exists(conn, &sql, key, logger, thread).await {
        assert_not_corrupt(thread, &sql, e);
        conn.rollback_if_open().await?;
    }
    Ok(())
}

async fn assert_row_exists(
    conn: &StressConn,
    sql: &str,
    key: &str,
    logger: &SqlLogger,
    thread: &ThreadId,
) -> turso::Result<()> {
    let result = async {
        let mut rows = conn.query(sql, ()).await?;
        let row = rows.next().await?.expect("count query must return a row");
        let count = row.get::<i64>(0)?;
        turso_macros::turso_assert_eq!(count, 1,
            "row inserted before advancing a sequence is missing after a successful COMMIT",
            { "thread": thread, "key": key });
        Ok(())
    }
    .await;
    logger.log_result(thread, sql, &result);
    result
}

fn assert_not_corrupt(thread: &ThreadId, sql: &str, error: turso::Error) {
    if let turso::Error::Corrupt(e) = error {
        turso_macros::turso_assert_unreachable!("corrupt error in sequence workload", { "thread": thread, "error": e, "sql": sql });
    }
}
