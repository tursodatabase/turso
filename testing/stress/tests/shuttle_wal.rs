#![cfg(shuttle)]

use shuttle::scheduler::PctScheduler;
use turso::{Builder, Value};

fn shuttle_config() -> shuttle::Config {
    turso_stress::shuttle_config()
}

fn check_corruption(e: &turso::Error, ctx: &str) {
    let msg = format!("{e}");
    if matches!(e, turso::Error::Corrupt(_)) || msg.contains("Invalid page type") {
        panic!("CORRUPTION REPRODUCED ({ctx}): {e}");
    }
}

fn is_transient(e: &turso::Error) -> bool {
    let s = format!("{e}");
    s.contains("locked")
        || s.contains("busy")
        || s.contains("Busy")
        || s.contains("snapshot")
        || s.contains("Snapshot")
}

async fn exec(conn: &turso::Connection, sql: &str, ctx: &str) {
    if let Err(e) = conn.execute(sql, ()).await {
        check_corruption(&e, ctx);
        if !is_transient(&e) {
            panic!("unexpected error ({ctx}) running `{sql}`: {e}");
        }
    }
}

async fn drain(conn: &turso::Connection, sql: &str, ctx: &str) -> Option<Value> {
    let mut rows = match conn.query(sql, ()).await {
        Ok(rows) => rows,
        Err(e) => {
            check_corruption(&e, ctx);
            if !is_transient(&e) {
                panic!("unexpected error ({ctx}) preparing `{sql}`: {e}");
            }
            return None;
        }
    };
    let mut first = None;
    loop {
        match rows.next().await {
            Ok(Some(row)) => {
                if first.is_none() {
                    first = Some(row.get_value(0).unwrap());
                }
            }
            Ok(None) => return first,
            Err(e) => {
                check_corruption(&e, ctx);
                if !is_transient(&e) {
                    panic!("unexpected error ({ctx}) stepping `{sql}`: {e}");
                }
                return first;
            }
        }
    }
}

async fn checkpoint_publish_race_scenario() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await
        .unwrap();

    let setup = db.connect().unwrap();
    exec(
        &setup,
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB)",
        "setup",
    )
    .await;
    exec(
        &setup,
        "INSERT INTO t VALUES (1, zeroblob(120000))",
        "setup",
    )
    .await;
    drain(&setup, "PRAGMA wal_checkpoint(TRUNCATE)", "setup").await;
    exec(&setup, "DELETE FROM t WHERE a = 1", "setup").await;
    drain(&setup, "PRAGMA wal_checkpoint(TRUNCATE)", "setup").await;

    exec(
        &setup,
        "INSERT INTO t VALUES (2, randomblob(25000))",
        "setup",
    )
    .await;

    let reader = db.connect().unwrap();
    exec(&reader, "BEGIN", "reader").await;
    drain(&reader, "SELECT count(*) FROM t", "reader").await;

    exec(
        &setup,
        "INSERT INTO t VALUES (3, randomblob(25000))",
        "setup",
    )
    .await;

    let conn_c = db.connect().unwrap();
    let c_task = turso_stress::future::spawn(async move {
        drain(
            &conn_c,
            "PRAGMA wal_checkpoint(PASSIVE)",
            "passive-checkpoint",
        )
        .await;
    });

    let conn_w = db.connect().unwrap();
    let s_task = turso_stress::future::spawn(async move {
        exec(&reader, "COMMIT", "reader-commit").await;
        for _ in 0..20 {
            let busy = drain(
                &conn_w,
                "PRAGMA wal_checkpoint(TRUNCATE)",
                "truncate-checkpoint",
            )
            .await;
            match busy {
                Some(Value::Integer(0)) => break,
                _ => shuttle::future::yield_now().await,
            }
        }
        exec(
            &conn_w,
            "INSERT INTO t SELECT 10 + value, randomblob(1500) FROM generate_series(0, 29)",
            "new-gen-insert",
        )
        .await;
        exec(
            &conn_w,
            "INSERT INTO t VALUES (99, randomblob(15000))",
            "new-gen-insert-overflow",
        )
        .await;
    });

    let _ = c_task.await;
    let _ = s_task.await;

    let verify = db.connect().unwrap();
    drain(&verify, "SELECT count(*) FROM t", "verify-count").await;
    drain(
        &verify,
        "SELECT sum(length(b)), count(*) FROM t",
        "verify-scan",
    )
    .await;
    match drain(&verify, "PRAGMA integrity_check", "verify-integrity").await {
        Some(Value::Text(s)) => {
            assert_eq!(
                s.to_lowercase(),
                "ok",
                "integrity check failed after checkpoint race: {s}"
            );
        }
        other => panic!("integrity_check returned no usable row: {other:?}"),
    }
    exec(
        &verify,
        "INSERT INTO t VALUES (100, randomblob(1000))",
        "verify-write",
    )
    .await;
}

#[test]
fn shuttle_wal_checkpoint_publish_race_pct() {
    let scheduler = PctScheduler::new(3, 2000);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(checkpoint_publish_race_scenario()));
}
