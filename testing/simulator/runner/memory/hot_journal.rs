use std::sync::Arc;

use anyhow::Result;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, IO, LimboError, OpenFlags, SessionRole,
};
use turso_sqlite::SqliteDialect;

use crate::runner::memory::io::MemorySimIO;

/// SQLite rollback journal magic bytes (0xd9d505f920a163d7)
const SQLITE_JOURNAL_MAGIC: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];

fn make_io(seed: u64) -> Arc<MemorySimIO> {
    Arc::new(MemorySimIO::new(seed, 4096, 25, 1, 4))
}

fn open_conn(io: Arc<MemorySimIO>, path: &str) -> Result<Arc<Connection>> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    Ok(db.connect(ConnectOpts::new(SessionRole::Superuser))?)
}

/// Helper to configure a database as legacy rollback journal mode
/// (write_version and read_version at header offsets 18-19 set to 1).
fn mark_legacy_database(io: &Arc<MemorySimIO>, path: &str) -> Result<()> {
    let db_file = io.open_file(path, OpenFlags::None, false)?;
    let buf = Arc::new(turso_core::Buffer::new(vec![0u8; 100]));
    let c = turso_core::Completion::new_read(buf.clone(), |_| None);
    let _c = db_file.pread(0, c)?;
    io.step()?;

    let mut header_bytes = buf.as_slice()[0..100].to_vec();
    header_bytes[18] = 1;
    header_bytes[19] = 1;
    // Standard SQLite legacy databases have 0 reserved bytes per page (offset 20 = 0).
    // This also instructs Turso that no page checksum extension is present in the file.
    header_bytes[20] = 0;

    let write_buf = Arc::new(turso_core::Buffer::new(header_bytes));
    let c = turso_core::Completion::new_write(|_| {});
    let _c = db_file.pwrite(0, write_buf, c)?;
    io.step()?;
    Ok(())
}

/// Regression test for #11828:
/// A database in legacy rollback journal mode (offsets 18-19 == 1) with an active
/// hot rollback journal (`<path>-journal` containing `SQLITE_JOURNAL_MAGIC`)
/// contains uncommitted, dirty data. Turso does not support rollback journal recovery
/// and must refuse to open the database, rather than ignoring the journal, overwriting
/// the header to WAL mode, and allowing reads/writes over unrecovered dirty state.
#[test]
fn sim_open_refuses_legacy_db_with_hot_rollback_journal() -> Result<()> {
    let io = make_io(42);
    let db_path = "sim_hot_journal.db";
    let journal_path = format!("{db_path}-journal");

    // 1. Create a database using Turso
    {
        let conn = open_conn(io.clone(), db_path)?;
        conn.execute("CREATE TABLE t(x TEXT)")?;
        conn.execute("INSERT INTO t VALUES ('initial')")?;
    }

    // 2. Mark the database as legacy rollback journal mode (offsets 18-19 = 1)
    mark_legacy_database(&io, db_path)?;

    // 3. Create an active hot rollback journal file starting with SQLite journal magic
    {
        let journal_file = io.open_file(&journal_path, OpenFlags::Create, false)?;
        let mut journal_payload = SQLITE_JOURNAL_MAGIC.to_vec();
        journal_payload.extend_from_slice(b"\x00\x00\x00\x01\x12\x34\x56\x78");
        journal_payload.resize(512, 0);

        let buf = Arc::new(turso_core::Buffer::new(journal_payload));
        let c = turso_core::Completion::new_write(|_| {});
        let _c = journal_file.pwrite(0, buf, c)?;
        io.step()?;
    }

    // 4. Attempting to open the database must be rejected because a hot rollback journal exists
    let res = open_conn(io, db_path);
    let err = match res {
        Err(e) => e,
        Ok(_) => {
            panic!("expected Database::open to fail due to hot rollback journal, but it succeeded!")
        }
    };

    let limbo_err = err
        .downcast_ref::<LimboError>()
        .expect("should be LimboError");
    match limbo_err {
        LimboError::Corrupt(msg) => {
            assert!(
                msg.contains("rollback journal"),
                "expected error message to mention rollback journal, got: {msg}"
            );
        }
        other => panic!("expected LimboError::Corrupt, got: {other:?}"),
    }

    Ok(())
}

#[test]
fn sim_open_accepts_legacy_db_without_journal() -> Result<()> {
    let io = make_io(43);
    let db_path = "sim_clean_legacy.db";

    {
        let conn = open_conn(io.clone(), db_path)?;
        conn.execute("CREATE TABLE t(x TEXT)")?;
        conn.execute("INSERT INTO t VALUES ('initial')")?;
    }

    mark_legacy_database(&io, db_path)?;

    // Without a hot journal, opening should succeed and convert to WAL mode
    let conn = open_conn(io.clone(), db_path)?;
    let mut stmt = conn.prepare("SELECT x FROM t")?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            turso_core::StepResult::IO => io.step()?,
            turso_core::StepResult::Row => {
                let row = stmt.row().expect("row");
                rows.push(row.get::<String>(0)?);
            }
            turso_core::StepResult::Done => break,
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(rows, vec!["initial".to_string()]);
    Ok(())
}

#[test]
fn sim_open_accepts_legacy_db_with_empty_journal() -> Result<()> {
    let io = make_io(44);
    let db_path = "sim_empty_journal.db";
    let journal_path = format!("{db_path}-journal");

    {
        let conn = open_conn(io.clone(), db_path)?;
        conn.execute("CREATE TABLE t(x TEXT)")?;
        conn.execute("INSERT INTO t VALUES ('initial')")?;
    }

    mark_legacy_database(&io, db_path)?;

    // Create a 0-byte journal file (not hot)
    {
        let _journal_file = io.open_file(&journal_path, OpenFlags::Create, false)?;
    }

    // A 0-byte journal is not hot, so opening must succeed
    let conn = open_conn(io.clone(), db_path)?;
    let mut stmt = conn.prepare("SELECT x FROM t")?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            turso_core::StepResult::IO => io.step()?,
            turso_core::StepResult::Row => {
                let row = stmt.row().expect("row");
                rows.push(row.get::<String>(0)?);
            }
            turso_core::StepResult::Done => break,
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(rows, vec!["initial".to_string()]);
    Ok(())
}

/// Tests SQLite PRAGMA journal_mode = PERSIST behavior:
/// When SQLite commits in PERSIST mode, it zeroes out the journal header without
/// deleting or truncating the file. Because the header does not match SQLITE_JOURNAL_MAGIC,
/// the journal is not hot, and Turso must allow opening and upgrading to WAL mode.
#[test]
fn sim_open_accepts_legacy_db_with_persist_journal_mode() -> Result<()> {
    let io = make_io(43);
    let db_path = "sim_persist_journal.db";
    let journal_path = format!("{db_path}-journal");

    {
        let conn = open_conn(io.clone(), db_path)?;
        conn.execute("CREATE TABLE t(x TEXT)")?;
        conn.execute("INSERT INTO t VALUES ('persisted_data')")?;
    }

    mark_legacy_database(&io, db_path)?;

    // Create a journal file with a zeroed header (simulating PRAGMA journal_mode = PERSIST)
    {
        let journal_file = io.open_file(&journal_path, OpenFlags::Create, false)?;
        // 512 bytes of zeroes: header magic is zeroed, so not a hot journal
        let zeroed_header = vec![0u8; 512];
        let buf = Arc::new(turso_core::Buffer::new(zeroed_header));
        let c = turso_core::Completion::new_write(|_| {});
        let _c = journal_file.pwrite(0, buf, c)?;
        io.step()?;
    }

    // Opening must succeed because the journal magic is zeroed
    let conn = open_conn(io.clone(), db_path)?;
    let mut stmt = conn.prepare("SELECT x FROM t")?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            turso_core::StepResult::IO => io.step()?,
            turso_core::StepResult::Row => {
                let row = stmt.row().expect("row");
                rows.push(row.get::<String>(0)?);
            }
            turso_core::StepResult::Done => break,
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(rows, vec!["persisted_data".to_string()]);
    Ok(())
}

#[test]
fn sim_open_in_memory_db_does_not_probe_journal() -> Result<()> {
    let io = make_io(46);
    let conn = open_conn(io.clone(), ":memory:")?;
    conn.execute("CREATE TABLE m(v INTEGER)")?;
    conn.execute("INSERT INTO m VALUES (42)")?;

    let mut stmt = conn.prepare("SELECT v FROM m")?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            turso_core::StepResult::IO => io.step()?,
            turso_core::StepResult::Row => {
                let row = stmt.row().expect("row");
                rows.push(row.get::<i64>(0)?);
            }
            turso_core::StepResult::Done => break,
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(rows, vec![42]);
    Ok(())
}
