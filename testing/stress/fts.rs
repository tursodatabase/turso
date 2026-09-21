use rand::Rng;
use turso_stress::ThreadId;

use crate::{conn::StressConn, sql_logging::SqlLogger, ThreadRng};

pub const TOKENS: &[&str] = &["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];

pub fn schema(tables: usize) -> Vec<String> {
    assert!(tables > 0, "FTS workload requires at least one table");
    (0..tables)
        .flat_map(|table| {
            [
                format!("CREATE TABLE IF NOT EXISTS fts_docs_{table}(id INTEGER PRIMARY KEY, body TEXT)"),
                format!("CREATE INDEX IF NOT EXISTS fts_idx_{table} ON fts_docs_{table} USING fts(body)"),
            ]
        })
        .collect()
}

pub async fn run(
    conn: &StressConn,
    rng: &mut ThreadRng,
    tables: usize,
    logger: &SqlLogger,
    thread: &ThreadId,
) -> turso::Result<()> {
    let table = rng.random_range(0..tables);
    let id = rng.random_range(0..400);
    let result = match rng.random_range(0..5) {
        0 => {
            let body = body(rng);
            conn.execute(
                &format!("INSERT OR REPLACE INTO fts_docs_{table} VALUES ({id}, '{body}')"),
                (),
            )
            .await
            .map(|_| ())
        }
        1 => {
            let body = body(rng);
            conn.execute(
                &format!("UPDATE fts_docs_{table} SET body = '{body}' WHERE id = {id}"),
                (),
            )
            .await
            .map(|_| ())
        }
        2 => conn
            .execute(&format!("DELETE FROM fts_docs_{table} WHERE id = {id}"), ())
            .await
            .map(|_| ()),
        3 => conn
            .execute(&format!("OPTIMIZE INDEX fts_idx_{table}"), ())
            .await
            .map(|_| ()),
        _ => check(conn, table, rng.choose::<&str>(TOKENS), logger, thread).await,
    };
    match result {
        Ok(()) | Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

fn body(rng: &mut ThreadRng) -> String {
    (0..rng.random_range(1..=8))
        .map(|_| *rng.choose(TOKENS))
        .collect::<Vec<_>>()
        .join(" ")
}

pub async fn check(
    conn: &StressConn,
    table: usize,
    token: &str,
    logger: &SqlLogger,
    thread: &ThreadId,
) -> turso::Result<()> {
    let sql = format!(
        "WITH indexed AS (SELECT id FROM fts_docs_{table} WHERE fts_match(body, '{token}')),
         scanned AS (SELECT id FROM fts_docs_{table} WHERE (' ' || body || ' ') LIKE '% {token} %')
         SELECT (SELECT count(*) FROM (SELECT id FROM indexed EXCEPT SELECT id FROM scanned)),
                (SELECT count(*) FROM (SELECT id FROM scanned EXCEPT SELECT id FROM indexed)),
                (SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND name = 'fts_idx_{table}')"
    );
    let result = async {
        let mut rows = conn.query(&sql, ()).await?;
        let row = rows.next().await?.expect("FTS check must return a row");
        let extra = row.get::<i64>(0)?;
        let missing = row.get::<i64>(1)?;
        let indexes = row.get::<i64>(2)?;
        turso_macros::turso_assert!(extra == 0 && missing == 0 && indexes == 1,
            "FTS search disagrees with table scan or index is missing",
            { "table": table, "token": token, "extra": extra, "missing": missing, "indexes": indexes });
        Ok(())
    }.await;
    logger.log_result(thread, &sql, &result);
    result
}

#[cfg(all(test, not(shuttle), not(antithesis)))]
mod tests {
    use super::*;
    use crate::conn::StressDb;
    use crate::opts::Opts;
    use clap::Parser;
    use turso_stress::sync::{Arc, AsyncMutex};

    #[test]
    fn fts_is_opt_in_and_rejects_reference_databases() {
        assert!(!Opts::try_parse_from(["turso_stress"]).unwrap().fts);
        assert!(Opts::try_parse_from(["turso_stress", "--fts"]).unwrap().fts);
        assert!(Opts::try_parse_from(["turso_stress", "--fts", "--db-ref", "ref.db"]).is_err());
    }

    #[tokio::test]
    async fn writes_rollback_optimize_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let logger =
            Arc::new(SqlLogger::new(dir.path().join("fts.sql").to_str().unwrap()).unwrap());
        let db = Arc::new(AsyncMutex::new(StressDb::new(
            dir.path().join("fts.db").to_str().unwrap().to_owned(),
            logger.clone(),
            None,
            true,
        )));
        let thread = ThreadId::new(0);
        let conn = StressDb::connect(&db, thread.clone(), 5000).await.unwrap();
        for sql in schema(2) {
            conn.execute(&sql, ()).await.unwrap();
        }
        for sql in [
            "INSERT INTO fts_docs_0 VALUES (1, 'alpha bravo'), (2, 'bravo'), (3, 'alpha alpha')",
            "UPDATE fts_docs_0 SET body = 'charlie' WHERE id = 1",
            "DELETE FROM fts_docs_0 WHERE id = 3",
            "BEGIN",
            "INSERT INTO fts_docs_0 VALUES (4, 'alpha')",
            "ROLLBACK",
            "BEGIN",
            "SAVEPOINT s",
            "UPDATE fts_docs_0 SET body = 'delta' WHERE id = 1",
            "ROLLBACK TO s",
            "RELEASE s",
            "COMMIT",
            "OPTIMIZE INDEX fts_idx_0",
        ] {
            conn.execute(sql, ()).await.unwrap();
        }
        drop(conn);
        db.lock().await.reset();
        let conn = StressDb::connect(&db, thread.clone(), 5000).await.unwrap();
        for table in 0..2 {
            for token in TOKENS {
                check(&conn, table, token, &logger, &thread).await.unwrap();
            }
        }
        let mut rows = conn
            .query(
                "SELECT id FROM fts_docs_0 WHERE fts_match(body, 'charlie')",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1
        );
        assert!(rows.next().await.unwrap().is_none());
    }
}
