//! Regression test for issue #7991: `discard_pending_log_write` must clear the
//! pending running CRC staged by an aborted deferred logical-log write.
//!
//! A deferred (two-phase) write stages a pending running CRC via
//! `log_tx_deferred_offset`. If the commit aborts before the offset advances,
//! the abort path calls `discard_pending_log_write`, which must discard the
//! staged CRC so no later success path chains the running CRC from a value
//! staged for a write that never confirmed.

use std::sync::Arc;

use crate::io::MemoryIO;
use crate::mvcc::persistent_storage::{DurableStorage, Storage};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::OpenFlags;

#[test]
fn discard_pending_log_write_clears_staged_pending_crc() {
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("issue_7991.db-log", OpenFlags::Create, false)
        .unwrap();
    let storage = Storage::new(file, io.clone(), None);

    // Stage a deferred write: `Storage::log_tx` routes to
    // `log_tx_deferred_offset`, which stages the pending running CRC and does
    // NOT advance the writer offset.
    let mut tx =
        crate::mvcc::database::LogRecord::new(1, crate::alloc::DynAllocator::default()).unwrap();
    storage
        .serialize_database_header(&mut tx, &DatabaseHeader::default())
        .unwrap();
    let (completion, bytes) = storage.log_tx(tx, None).unwrap();
    io.wait_for_completion(completion).unwrap();
    assert!(bytes > 0, "deferred write should have framed bytes");

    // Simulate the abort path of the two-phase commit: the commit failed
    // before the offset advanced, so the staged pending CRC must be discarded.
    storage.discard_pending_log_write().unwrap();

    // The staged pending running CRC must be gone: the success path must now
    // refuse to consume it (it panics on the missing pending slot) instead of
    // silently chaining the running CRC from a write that never confirmed.
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {})); // silence the expected panic
    let advanced = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        storage.advance_logical_log_offset_after_success(bytes)
    }));
    std::panic::set_hook(prev_hook);

    assert!(
        advanced.is_err(),
        "discard_pending_log_write left the staged pending running CRC in place: \
         the success path consumed a CRC staged for an aborted deferred write"
    );
}
