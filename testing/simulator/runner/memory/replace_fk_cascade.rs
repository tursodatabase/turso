use std::sync::Arc;

use anyhow::Result;
use turso_core::{ConnectOpts, Connection, Database, DatabaseOpts, IO, OpenFlags, SqliteDialect};

use crate::runner::memory::io::MemorySimIO;

fn open_sim_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>, String)> {
    let io = Arc::new(MemorySimIO::new(seed, 4096, 100, 1, 5));
    let path = format!("sim_replace_fk_cascade_{seed}.db");
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(ConnectOpts::new())?;
    Ok((conn, io, path))
}

fn query_strings(conn: &Arc<Connection>, sql: &str) -> Result<Vec<String>> {
    let mut rows = conn
        .query(sql)?
        .ok_or_else(|| anyhow::anyhow!("expected rows from {sql}"))?;
    let mut out = Vec::new();
    rows.run_with_row_callback(|row| {
        let mut parts = Vec::new();
        for idx in 0..row.len() {
            parts.push(format!("{}", row.get_value(idx)));
        }
        out.push(parts.join("|"));
        Ok(())
    })?;
    Ok(out)
}

#[test]
fn replace_fk_cascade_trigger_reinsert_aborts_and_preserves_integrity() -> Result<()> {
    for seed in 1..=8 {
        let (conn, io, path) = open_sim_conn(seed)?;
        conn.execute("PRAGMA foreign_keys = 1")?;
        conn.execute("CREATE TABLE p1 (a INT, b INT UNIQUE)")?;
        conn.execute("CREATE TABLE c1 (c INT, d INT REFERENCES p1(b) ON DELETE CASCADE)")?;
        conn.execute(
            "CREATE TRIGGER tr6 AFTER DELETE ON c1 BEGIN INSERT INTO p1 VALUES(4, 1); END",
        )?;
        conn.execute("INSERT INTO p1 VALUES(1, 1)")?;
        conn.execute("INSERT INTO c1 VALUES(2, 1)")?;

        let err = conn
            .execute("REPLACE INTO p1 VALUES(3, 1)")
            .expect_err("REPLACE must fail when FK cascade trigger re-inserts conflicting key");
        assert!(
            err.to_string().contains("UNIQUE constraint failed: p1.b"),
            "unexpected error on seed {seed}: {err}"
        );

        assert_eq!(query_strings(&conn, "PRAGMA integrity_check")?, vec!["ok"]);
        assert_eq!(
            query_strings(&conn, "SELECT a, b FROM p1 ORDER BY rowid")?,
            vec!["1|1"]
        );
        assert_eq!(
            query_strings(&conn, "SELECT c, d FROM c1 ORDER BY rowid")?,
            vec!["2|1"]
        );

        // Reopen on the same simulated storage and re-verify integrity.
        drop(conn);
        let db2 = Database::open_file_with_flags(
            io as Arc<dyn IO>,
            &path,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )?;
        let conn2 = db2.connect(ConnectOpts::new())?;
        assert_eq!(query_strings(&conn2, "PRAGMA integrity_check")?, vec!["ok"]);
        assert_eq!(
            query_strings(&conn2, "SELECT a, b FROM p1 ORDER BY rowid")?,
            vec!["1|1"]
        );
    }
    Ok(())
}

#[test]
fn update_or_replace_fk_cascade_trigger_variants_preserve_integrity() -> Result<()> {
    for seed in 1..=6 {
        let (conn, _io, _path) = open_sim_conn(100 + seed)?;
        conn.execute("PRAGMA foreign_keys = 1")?;
        conn.execute("CREATE TABLE p1 (a INT, b INT UNIQUE)")?;
        conn.execute("CREATE TABLE c1 (c INT, d INT REFERENCES p1(b) ON DELETE CASCADE)")?;
        conn.execute(
            "CREATE TRIGGER tr_reinsert AFTER DELETE ON c1 BEGIN INSERT INTO p1 VALUES(4, 1); END",
        )?;
        conn.execute("INSERT INTO p1 VALUES(1, 1)")?;
        conn.execute("INSERT INTO p1 VALUES(3, 2)")?;
        conn.execute("INSERT INTO c1 VALUES(2, 1)")?;

        let err = conn
            .execute("UPDATE OR REPLACE p1 SET b = 1 WHERE a = 3")
            .expect_err("UPDATE OR REPLACE must fail when cascade trigger re-inserts key");
        assert!(
            err.to_string().contains("UNIQUE constraint failed: p1.b"),
            "unexpected error on seed {seed}: {err}"
        );
        assert_eq!(query_strings(&conn, "PRAGMA integrity_check")?, vec!["ok"]);
        assert_eq!(
            query_strings(&conn, "SELECT a, b FROM p1 ORDER BY rowid")?,
            vec!["1|1", "3|2"]
        );

        // Variant 2: cascade trigger deletes the row currently being updated.
        conn.execute("DROP TRIGGER tr_reinsert")?;
        conn.execute(
            "CREATE TRIGGER tr_del_self AFTER DELETE ON c1 BEGIN DELETE FROM p1 WHERE a = 3; END",
        )?;
        conn.execute("UPDATE OR REPLACE p1 SET b = 1 WHERE a = 3")?;
        assert_eq!(query_strings(&conn, "PRAGMA integrity_check")?, vec!["ok"]);
        assert!(query_strings(&conn, "SELECT a, b FROM p1")?.is_empty());
    }
    Ok(())
}
