//! Regression tests for issue #7993: on the immediate-advance logical-log path
//! (`LogicalLog::truncate` / `truncate_to_zero`), the durable-extent offset and
//! the running CRC must not move past the durable tail until the I/O layer has
//! confirmed the operation. Advancing them right after issuing the request —
//! before the completion fires, and ignoring failure — lets a later read or
//! recovery trust a position (offset + CRC chain state) the data never reached.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::io::{FileSyncType, MemoryIO};
use crate::mvcc::database::tests::generate_simple_string_row;
use crate::mvcc::database::{LogRecord, MVTableId, PackedTs, RowVersion, TxTimestampOrID, WalPos};
use crate::sync::Arc;
use crate::{Buffer, Completion, CompletionError, File, OpenFlags, Result, IO};

use super::logical_log::LogicalLog;

/// File wrapper that forwards all I/O to `inner` but can hold or fail
/// truncate requests, modeling an unconfirmed or failed durable write.
struct FaultTruncateFile {
    inner: Arc<dyn File>,
    hold_truncate: AtomicBool,
    fail_truncate: AtomicBool,
    held: Mutex<Option<(u64, Completion)>>,
}

impl FaultTruncateFile {
    fn new(inner: Arc<dyn File>) -> Self {
        Self {
            inner,
            hold_truncate: AtomicBool::new(false),
            fail_truncate: AtomicBool::new(false),
            held: Mutex::new(None),
        }
    }

    /// Confirm the held truncate: forward it to the inner file so it actually
    /// becomes durable, then fire the caller's completion.
    fn release_held_truncate(&self) {
        let (len, c) = self
            .held
            .lock()
            .unwrap()
            .take()
            .expect("no truncate is being held");
        let inner_c = Completion::new_trunc(|_| {});
        drop(self.inner.truncate(len, inner_c).unwrap());
        c.complete(0);
    }
}

impl File for FaultTruncateFile {
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
        self.inner.pwrite(pos, buffer, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        if self.fail_truncate.load(Ordering::SeqCst) {
            // Injected failure: the bytes never reach durable storage.
            c.error(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "injected truncate failure (issue #7993)",
            ));
            return Ok(c);
        }
        if self.hold_truncate.load(Ordering::SeqCst) {
            // Unconfirmed write: completion is withheld until released.
            *self.held.lock().unwrap() = Some((len, c.clone()));
            return Ok(c);
        }
        self.inner.truncate(len, c)
    }
}

fn row_version(commit_ts: u64) -> RowVersion {
    let table_id: MVTableId = (-100).into();
    RowVersion {
        id: commit_ts,
        begin: PackedTs::pack(Some(TxTimestampOrID::Timestamp(commit_ts))),
        end: PackedTs::pack(None),
        row: generate_simple_string_row(table_id, 1, "issue-7993"),
        btree_resident: false,
        materialized_at: WalPos::ORIGIN,
    }
}

/// Builds a log with one durably committed frame and returns the log, the IO,
/// the fault-injection file handle, and the durable tail `(offset, crc)`.
fn log_with_one_durable_frame(
    name: &str,
) -> (LogicalLog, Arc<dyn IO>, Arc<FaultTruncateFile>, (u64, u32)) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let inner = io.open_file(name, OpenFlags::Create, false).unwrap();
    let fault = Arc::new(FaultTruncateFile::new(inner));
    let file: Arc<dyn File> = fault.clone();
    let mut log = LogicalLog::new(file, io.clone(), None);

    let tx = LogRecord::for_test(10, &[row_version(10)], None);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let durable_tail = (log.offset, log.running_crc);
    assert!(durable_tail.0 > 0, "expected a non-empty durable log");
    (log, io, fault, durable_tail)
}

/// While a truncate is issued but its completion has NOT fired, the offset and
/// running CRC must still describe the durable tail. Today both are mutated
/// immediately after (and partly before) issuing the request.
#[test]
fn issue_7993_truncate_must_not_move_offset_and_crc_before_completion() {
    let (mut log, _io, fault, durable_tail) =
        log_with_one_durable_frame("issue_7993_hold_truncate");

    fault.hold_truncate.store(true, Ordering::SeqCst);
    let (_c, _outcome) = log.truncate(u64::MAX).unwrap();

    assert_eq!(
        (log.offset, log.running_crc),
        durable_tail,
        "durable-extent offset/running CRC moved past the durable tail before \
         the truncate completion fired"
    );

    // Confirm durability so the held completion does not dangle.
    fault.release_held_truncate();
}

/// When the truncate FAILS, the offset and running CRC must stay at the
/// durable tail: the on-disk log still contains the old frames, so publishing
/// the post-truncate state (offset 0, reseeded CRC) trusts a position the
/// operation never reached.
#[test]
fn issue_7993_failed_truncate_must_not_move_offset_and_crc() {
    let (mut log, _io, fault, durable_tail) =
        log_with_one_durable_frame("issue_7993_fail_truncate");

    fault.fail_truncate.store(true, Ordering::SeqCst);
    let (_c, _outcome) = log.truncate(u64::MAX).unwrap();

    assert_eq!(
        (log.offset, log.running_crc),
        durable_tail,
        "durable-extent offset/running CRC moved past the durable tail even \
         though the truncate write failed"
    );
}
