use crate::common::rusqlite_integrity_check;
use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO};

#[test]
fn test_autovacuum_readonly_behavior() {
    // (autovacuum_mode, enable_autovacuum_flag, expected_readonly)
    // TODO: Add encrypted case ("NONE", false, false) after fixing https://github.com/tursodatabase/turso/issues/4519
    let test_cases = [
        ("NONE", false, false),
        ("NONE", true, false),
        ("FULL", false, true),
        ("FULL", true, false),
        ("INCREMENTAL", false, true),
        ("INCREMENTAL", true, false),
    ];

    for (autovacuum_mode, enable_autovacuum_flag, expected_readonly) in test_cases {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "auto_vacuum", autovacuum_mode)
                .unwrap();
        }

        let io = Arc::new(PlatformIO::new().unwrap()) as Arc<dyn turso_core::IO>;
        let opts = DatabaseOpts::new().with_autovacuum(enable_autovacuum_flag);
        let db = Database::open_file_with_flags(
            io,
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
        )
        .unwrap();

        assert_eq!(
            db.is_readonly(),
            expected_readonly,
            "autovacuum={autovacuum_mode}, flag={enable_autovacuum_flag}: expected readonly={expected_readonly}"
        );
    }
}

#[test]
fn test_autovacuum_freelist_allocation_updates_header_at_ptrmap_boundary() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("ptrmap-boundary.db");

    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "page_size", 512)?;
        conn.pragma_update(None, "auto_vacuum", "full")?;
        conn.execute("CREATE TABLE seed(x)", [])?;
    }

    let io = Arc::new(PlatformIO::new()?) as Arc<dyn turso_core::IO>;
    let opts = DatabaseOpts::new().with_autovacuum(true);
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
    )?;
    let conn = db.connect()?;

    conn.execute("CREATE TABLE t(x)")?;
    conn.execute("BEGIN")?;
    for start in (1..=91).step_by(10) {
        let values = (start..start + 10)
            .map(|value| format!("({value})"))
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(format!("INSERT INTO t VALUES {values}"))?;
    }
    conn.execute("DELETE FROM t WHERE x IN (1,2,3)")?;
    conn.execute("INSERT INTO t VALUES (101),(102),(103),(104),(105)")?;
    conn.execute("COMMIT")?;
    drop(conn);
    drop(db);

    rusqlite_integrity_check(&db_path)
}
