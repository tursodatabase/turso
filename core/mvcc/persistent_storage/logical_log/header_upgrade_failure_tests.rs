//! Regression test for issue #7992: the in-memory logical-log header must not
//! be published before the on-disk header write succeeds.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::{LogicalLog, LOG_VERSION, LOG_VERSION_V2};
use crate::io::{FileSyncType, MemoryIO};
use crate::mvcc::database::tests::generate_simple_string_row;
use crate::mvcc::database::{LogRecord, PackedTs, RowVersion, TxTimestampOrID, WalPos};
use crate::{Buffer, Completion, CompletionError, File, OpenFlags, Result};

struct HeaderWriteFailingFile {
    inner: Arc<dyn File>,
    fail_header_write: AtomicBool,
}

impl File for HeaderWriteFailingFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        if pos == 0 && self.fail_header_write.load(Ordering::SeqCst) {
            c.error(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "injected header write failure",
            ));
            return Ok(c);
        }
        self.inner.pwrite(pos, buffer, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }
}

fn row_version(rowid: i64, commit_ts: u64) -> RowVersion {
    RowVersion {
        id: rowid as u64,
        begin: PackedTs::pack(Some(TxTimestampOrID::Timestamp(commit_ts))),
        end: PackedTs::pack(None),
        row: generate_simple_string_row((-2).into(), rowid, "visible"),
        btree_resident: false,
        materialized_at: WalPos::ORIGIN,
    }
}

#[test]
fn failed_header_upgrade_write_keeps_in_memory_header_at_old_version() {
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let inner = io
        .open_file("issue_7992.db-log", OpenFlags::Create, false)
        .unwrap();
    let file = Arc::new(HeaderWriteFailingFile {
        inner,
        fail_header_write: AtomicBool::new(false),
    });
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let c = log
        .log_tx(LogRecord::for_test(10, &[row_version(1, 10)], None))
        .unwrap();
    io.wait_for_completion(c).unwrap();
    assert_eq!(log.header().unwrap().version, LOG_VERSION_V2);

    let mut portable_tx = LogRecord::for_test(20, &[row_version(2, 20)], None);
    portable_tx.portable_changes_enabled = true;
    portable_tx.portable_changes = crate::alloc::vec![0x1a, 0x00];

    file.fail_header_write.store(true, Ordering::SeqCst);
    let c = log
        .upgrade_header_for_log_tx(&portable_tx)
        .unwrap()
        .unwrap();
    assert!(io.wait_for_completion(c).is_err());

    assert_eq!(
        log.header().unwrap().version,
        LOG_VERSION_V2,
        "in-memory header advanced to version {LOG_VERSION} although the on-disk header write failed"
    );
}
