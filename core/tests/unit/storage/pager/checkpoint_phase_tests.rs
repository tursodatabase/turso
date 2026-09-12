use super::*;
use crate::io::{PlatformIO, IO};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::CheckpointMode;
use crate::sync::atomic::Ordering;
use crate::types::IOResult;
use crate::Database;
use crate::SqliteDialect;

/// Returns an IO backend that supports shared WAL coordination on the host.
/// On Windows the default `PlatformIO` (`WindowsIO`) lacks the byte-locking
/// and mapping primitives, so the experimental IOCP backend is used when
/// the `experimental_win_iocp` feature is enabled.
fn shared_wal_test_io() -> Arc<dyn IO> {
    #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
    {
        Arc::new(crate::WindowsIOCP::new().unwrap())
    }
    #[cfg(not(all(target_os = "windows", feature = "experimental_win_iocp")))]
    {
        Arc::new(PlatformIO::new().unwrap())
    }
}

/// The returned `TempDir` deletes the database directory when it drops, so
/// callers must hold it for as long as they use the database.
fn open_checkpoint_test_database() -> (Arc<Database>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    {
        let connection = rusqlite::Connection::open(&db_path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "wal")
            .unwrap();
    }
    let io = shared_wal_test_io();
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new().with_multiprocess_wal(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    (db, dir)
}

fn db_identity(db_path: &std::path::Path) -> (u32, u32) {
    let bytes = std::fs::read(db_path).unwrap();
    assert!(bytes.len() >= DatabaseHeader::SIZE);
    let db_size_pages = u32::from_be_bytes(bytes[28..32].try_into().unwrap());
    let crc = crc32c::crc32c(&bytes[..DatabaseHeader::SIZE]);
    (db_size_pages, crc)
}

#[test]
fn checkpoint_db_sync_completion_still_leaves_backfill_unpublished_until_proof_install() {
    let (db, dir) = open_checkpoint_test_database();
    let db_path = dir.path().join("test.db");
    let conn = db.connect().unwrap();
    conn.wal_auto_actions_disable();
    conn.execute("create table test(id integer primary key, value blob)")
        .unwrap();
    conn.execute("begin immediate").unwrap();
    for _ in 0..32 {
        conn.execute("insert into test(value) values (randomblob(2048))")
            .unwrap();
    }
    conn.execute("commit").unwrap();
    assert!(
        db.shared_wal
            .read()
            .metadata
            .max_frame
            .load(Ordering::SeqCst)
            > 1,
        "checkpoint setup requires more than one WAL frame"
    );

    let pager = conn.pager.load();
    let mode = CheckpointMode::Passive {
        upper_bound_inclusive: Some(1),
    };

    loop {
        match pager.checkpoint(mode, crate::SyncMode::Full, true).unwrap() {
            IOResult::Done(_) => {
                panic!("checkpoint should not finish before we observe the post-sync gap")
            }
            IOResult::IO(io) => io.wait(pager.io.as_ref()).unwrap(),
        }

        let state = pager.checkpoint_state.read();
        let Some(result) = state.result.as_ref() else {
            continue;
        };
        if matches!(state.phase, CheckpointPhase::ReadDbIdentity { .. })
            && result.db_sync_sent
            && !pager.syncing.load(Ordering::SeqCst)
        {
            break;
        }
    }

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before_publish = authority.snapshot();
    let (db_size_pages, db_header_crc32c) = db_identity(&db_path);
    assert_eq!(
        snapshot_before_publish.nbackfills, 0,
        "DB sync completion alone must not publish positive nbackfills"
    );
    assert!(
        !authority.validate_backfill_proof(
            snapshot_before_publish,
            db_size_pages,
            db_header_crc32c
        ),
        "DB sync completion must still leave the durable backfill proof absent"
    );

    let result = pager
        .io
        .block(|| pager.checkpoint(mode, crate::SyncMode::Full, true))
        .unwrap();
    assert!(
        result.wal_total_backfilled > 0 && !result.everything_backfilled(),
        "resumed checkpoint should complete the partial checkpoint after proof installation"
    );

    let snapshot_after_publish = authority.snapshot();
    let (db_size_pages_after, db_header_crc32c_after) = db_identity(&db_path);
    assert!(
        snapshot_after_publish.nbackfills > 0,
        "proof installation step must publish positive nbackfills"
    );
    assert!(
        authority.validate_backfill_proof(
            snapshot_after_publish,
            db_size_pages_after,
            db_header_crc32c_after
        ),
        "resuming after the post-sync gap must install a valid durable backfill proof"
    );
}
