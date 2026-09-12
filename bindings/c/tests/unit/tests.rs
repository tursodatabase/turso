use super::*;
use std::ptr;

/// sqlite3_errmsg must return the bare message SQLite produces — no
/// "Runtime error:" prefix and no "(19)" suffix; those are sqlite3
/// shell decoration, not part of the message.
#[test]
fn test_sqlite3_errmsg_constraint_failure_matches_sqlite() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
        assert_eq!(
            sqlite3_exec(
                db,
                c"CREATE TABLE u(a UNIQUE); INSERT INTO u VALUES (1);".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            SQLITE_OK
        );

        let mut stmt = ptr::null_mut();
        assert_eq!(
            sqlite3_prepare_v2(
                db,
                c"INSERT INTO u VALUES (1)".as_ptr(),
                -1,
                &mut stmt,
                ptr::null_mut(),
            ),
            SQLITE_OK
        );
        assert_eq!(sqlite3_step(stmt), SQLITE_CONSTRAINT);
        let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
        assert_eq!(msg, "UNIQUE constraint failed: u.a");
        sqlite3_finalize(stmt);
        sqlite3_close(db);
    }
}

/// A statement that hit SQLITE_BUSY cannot be run to completion at
/// finalize time while another connection still holds the write lock.
/// sqlite3_finalize must free it anyway and report the error, like
/// SQLite does. Returning early instead leaked the statement and left
/// it counted as an active root statement on the connection forever,
/// so every later "no statements active" check failed — an explicit
/// checkpoint always errored and DETACH reported the database locked.
#[test]
fn test_finalize_frees_statement_stuck_on_busy() {
    unsafe {
        let dir = tempfile::tempdir().unwrap();
        let (writer, blocked, stmt) = prepare_statement_stuck_on_busy(&dir);

        assert_eq!(sqlite3_finalize(stmt), SQLITE_BUSY);

        assert_eq!(
            sqlite3_exec(
                writer,
                c"COMMIT".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut()
            ),
            SQLITE_OK
        );
        // The finalized statement must no longer count as active: an
        // explicit checkpoint refuses to run while a statement is in
        // progress on the connection.
        assert_eq!(sqlite3_wal_checkpoint(blocked, ptr::null()), SQLITE_OK);
        assert_eq!(sqlite3_close(blocked), SQLITE_OK);
        assert_eq!(sqlite3_close(writer), SQLITE_OK);
    }
}

/// Same contract for sqlite3_reset: it must reset the statement even
/// when the pending execution cannot be completed, returning the error
/// of the most recent evaluation. The statement stays usable and stops
/// counting as active once finalized.
#[test]
fn test_reset_resets_statement_stuck_on_busy() {
    unsafe {
        let dir = tempfile::tempdir().unwrap();
        let (writer, blocked, stmt) = prepare_statement_stuck_on_busy(&dir);

        assert_eq!(sqlite3_reset(stmt), SQLITE_BUSY);

        assert_eq!(
            sqlite3_exec(
                writer,
                c"COMMIT".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut()
            ),
            SQLITE_OK
        );
        // The reset must have released the statement's active slot
        // already, before it is stepped again: an explicit checkpoint
        // refuses to run while a statement is in progress.
        assert_eq!(sqlite3_wal_checkpoint(blocked, ptr::null()), SQLITE_OK);
        // The reset statement runs again from the start now that the
        // lock is gone.
        assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
        assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
        assert_eq!(sqlite3_close(blocked), SQLITE_OK);
        assert_eq!(sqlite3_close(writer), SQLITE_OK);
    }
}

/// Opens two connections to the same file, makes `writer` hold the write
/// lock, and returns a statement on the second connection whose step just
/// failed with SQLITE_BUSY.
unsafe fn prepare_statement_stuck_on_busy(
    dir: &tempfile::TempDir,
) -> (*mut sqlite3, *mut sqlite3, *mut sqlite3_stmt) {
    let path = CString::new(dir.path().join("busy.db").to_str().unwrap()).unwrap();
    let mut writer = ptr::null_mut();
    let mut blocked = ptr::null_mut();
    assert_eq!(sqlite3_open(path.as_ptr(), &mut writer), SQLITE_OK);
    assert_eq!(sqlite3_open(path.as_ptr(), &mut blocked), SQLITE_OK);
    assert_eq!(
        sqlite3_exec(
            writer,
            c"CREATE TABLE t(x); BEGIN; INSERT INTO t VALUES (1);".as_ptr(),
            None,
            ptr::null_mut(),
            ptr::null_mut(),
        ),
        SQLITE_OK
    );

    let mut stmt = ptr::null_mut();
    assert_eq!(
        sqlite3_prepare_v2(
            blocked,
            c"INSERT INTO t VALUES (2)".as_ptr(),
            -1,
            &mut stmt,
            ptr::null_mut(),
        ),
        SQLITE_OK
    );
    assert_eq!(sqlite3_step(stmt), SQLITE_BUSY);
    (writer, blocked, stmt)
}

#[test]
fn test_sqlite3_stmt_status_rows_read_written() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

        assert_eq!(
            sqlite3_exec(
                db,
                c"CREATE TABLE t(x); INSERT INTO t VALUES (1), (2);".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            SQLITE_OK
        );

        let mut insert_stmt = ptr::null_mut();
        assert_eq!(
            sqlite3_prepare_v2(
                db,
                c"INSERT INTO t VALUES (3)".as_ptr(),
                -1,
                &mut insert_stmt,
                ptr::null_mut(),
            ),
            SQLITE_OK
        );
        assert_eq!(sqlite3_step(insert_stmt), SQLITE_DONE);
        assert_eq!(
            sqlite3_stmt_status(insert_stmt, LIBSQL_STMTSTATUS_ROWS_WRITTEN, 0),
            1
        );
        assert_eq!(
            sqlite3_stmt_status(insert_stmt, LIBSQL_STMTSTATUS_ROWS_WRITTEN, 1),
            1
        );
        assert_eq!(
            sqlite3_stmt_status(insert_stmt, LIBSQL_STMTSTATUS_ROWS_WRITTEN, 0),
            0
        );
        assert_eq!(sqlite3_finalize(insert_stmt), SQLITE_OK);

        let mut select_stmt = ptr::null_mut();
        assert_eq!(
            sqlite3_prepare_v2(
                db,
                c"SELECT x FROM t ORDER BY x".as_ptr(),
                -1,
                &mut select_stmt,
                ptr::null_mut(),
            ),
            SQLITE_OK
        );
        while sqlite3_step(select_stmt) == SQLITE_ROW {}

        let rows_read = sqlite3_stmt_status(select_stmt, LIBSQL_STMTSTATUS_ROWS_READ, 0);
        assert!(
            rows_read >= 3,
            "expected at least 3 rows read, got {rows_read}"
        );
        let fullscan_steps = sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0);
        assert!(
            fullscan_steps >= 2,
            "expected fullscan steps for table iteration, got {fullscan_steps}"
        );
        assert_eq!(
            sqlite3_stmt_status(select_stmt, LIBSQL_STMTSTATUS_ROWS_READ, 1),
            rows_read
        );
        assert_eq!(
            sqlite3_stmt_status(select_stmt, LIBSQL_STMTSTATUS_ROWS_READ, 0),
            0
        );
        assert_eq!(
            sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_AUTOINDEX, 0),
            0
        );
        assert_eq!(
            sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_RUN, 0),
            0
        );
        assert_eq!(
            sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_FILTER_HIT, 0),
            0
        );
        assert_eq!(
            sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_FILTER_MISS, 0),
            0
        );
        assert_eq!(
            sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_MEMUSED, 0),
            0
        );
        assert_eq!(sqlite3_stmt_status(select_stmt, 9999, 0), 0);
        assert_eq!(sqlite3_finalize(select_stmt), SQLITE_OK);

        assert_eq!(sqlite3_close(db), SQLITE_OK);
    }
}

/// FFI-level transcription of the sqlite3_blob_open conditions exercised by
/// SQLite's e_blobopen.test: misuse handling, the *ppBlob-is-NULL-on-error
/// contract, database-name resolution, and error codes/messages.
#[test]
fn test_blob_open_argument_and_error_contract() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
        assert_eq!(
            sqlite3_exec(
                db,
                c"CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB, n); \
                      INSERT INTO t VALUES (1, x'00112233', 42);"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            SQLITE_OK
        );

        // NULL out-pointer is misuse.
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                c"t".as_ptr(),
                c"data".as_ptr(),
                1,
                0,
                ptr::null_mut()
            ),
            SQLITE_MISUSE
        );

        // Every failure must leave *ppBlob NULL — callers close it unconditionally.
        let garbage = std::ptr::dangling_mut::<ffi::c_void>();

        let mut blob = garbage;
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                ptr::null(),
                c"data".as_ptr(),
                1,
                0,
                &mut blob
            ),
            SQLITE_MISUSE
        );
        assert!(blob.is_null());

        // Only "main" resolves; an attached-style or filename-style name errors.
        let mut blob = garbage;
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"aux".as_ptr(),
                c"t".as_ptr(),
                c"data".as_ptr(),
                1,
                0,
                &mut blob
            ),
            SQLITE_ERROR
        );
        assert!(blob.is_null());

        // No such table / column / rowid: SQLITE_ERROR, NULL handle, errmsg set.
        let mut blob = garbage;
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                c"nosuch".as_ptr(),
                c"data".as_ptr(),
                1,
                0,
                &mut blob
            ),
            SQLITE_ERROR
        );
        assert!(blob.is_null());
        let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
        assert!(msg.contains("no such table"), "errmsg: {msg}");

        let mut blob = garbage;
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                c"t".as_ptr(),
                c"data".as_ptr(),
                99,
                0,
                &mut blob
            ),
            SQLITE_ERROR
        );
        assert!(blob.is_null());
        let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
        assert!(msg.contains("no such rowid"), "errmsg: {msg}");

        // A non-TEXT/BLOB value cannot be opened (e_blobopen R-11683-62380).
        let mut blob = garbage;
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                c"t".as_ptr(),
                c"n".as_ptr(),
                1,
                0,
                &mut blob
            ),
            SQLITE_ERROR
        );
        assert!(blob.is_null());
        let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
        assert!(
            msg.contains("cannot open value of type integer"),
            "errmsg: {msg}"
        );

        assert_eq!(sqlite3_close(db), SQLITE_OK);
    }
}

/// e_blobopen R-50854-53979 / R-03922-41160: flags==0 means read-only
/// (writes fail with SQLITE_READONLY); any non-zero flags value means
/// read-write. Also covers R-34146-30782: indexed columns refuse writing.
#[test]
fn test_blob_open_flags_and_indexed_columns() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
        assert_eq!(
            sqlite3_exec(
                db,
                c"CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB, ix BLOB); \
                      CREATE INDEX t_ix ON t(ix); \
                      INSERT INTO t VALUES (1, x'0011223344556677', x'aa');"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            SQLITE_OK
        );

        // Read-only handle: reads work, writes are SQLITE_READONLY.
        let mut blob = ptr::null_mut();
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                c"t".as_ptr(),
                c"data".as_ptr(),
                1,
                0,
                &mut blob
            ),
            SQLITE_OK
        );
        let mut buf = [0u8; 4];
        assert_eq!(
            sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
            SQLITE_OK
        );
        assert_eq!(buf, [0x00, 0x11, 0x22, 0x33]);
        assert_eq!(
            sqlite3_blob_write(blob, buf.as_ptr().cast(), 4, 0),
            SQLITE_READONLY
        );
        assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);

        // Any non-zero flags value opens read-write.
        for flags in [1, -1, ffi::c_int::MAX, ffi::c_int::MIN] {
            let mut blob = ptr::null_mut();
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"t".as_ptr(),
                    c"data".as_ptr(),
                    1,
                    flags,
                    &mut blob
                ),
                SQLITE_OK
            );
            let payload = [0xABu8, 0xCD];
            assert_eq!(
                sqlite3_blob_write(blob, payload.as_ptr().cast(), 2, 6),
                SQLITE_OK
            );
            assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);
        }

        // Indexed column: read-only open works, read-write open fails.
        let mut blob = ptr::null_mut();
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                c"t".as_ptr(),
                c"ix".as_ptr(),
                1,
                0,
                &mut blob
            ),
            SQLITE_OK
        );
        assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);
        let mut blob = ptr::null_mut();
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                c"t".as_ptr(),
                c"ix".as_ptr(),
                1,
                1,
                &mut blob
            ),
            SQLITE_ERROR
        );
        assert!(blob.is_null());
        let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
        assert!(msg.contains("indexed column"), "errmsg: {msg}");

        assert_eq!(sqlite3_close(db), SQLITE_OK);
    }
}

/// FFI transcription of e_blobwrite/e_blobread range conditions: negative N or
/// offset and out-of-range spans are SQLITE_ERROR (not misuse), every such error
/// leaves the handle usable, and zero-length operations never dereference the
/// data pointer (NULL is legal for them) while still range-checking the offset.
#[test]
fn test_blob_read_write_range_contract() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
        assert_eq!(
            sqlite3_exec(
                db,
                c"CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB); \
                      INSERT INTO t VALUES (1, zeroblob(8));"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            SQLITE_OK
        );
        let mut blob = ptr::null_mut();
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                c"t".as_ptr(),
                c"data".as_ptr(),
                1,
                1,
                &mut blob
            ),
            SQLITE_OK
        );
        assert_eq!(sqlite3_blob_bytes(blob), 8);

        let mut buf = [0u8; 8];
        // Out-of-range spans and negative arguments: SQLITE_ERROR.
        assert_eq!(
            sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 5),
            SQLITE_ERROR
        );
        assert_eq!(
            sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), -1, 0),
            SQLITE_ERROR
        );
        assert_eq!(
            sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, -1),
            SQLITE_ERROR
        );
        assert_eq!(
            sqlite3_blob_write(blob, buf.as_ptr().cast(), 4, 5),
            SQLITE_ERROR
        );
        assert_eq!(
            sqlite3_blob_write(blob, buf.as_ptr().cast(), -1, 0),
            SQLITE_ERROR
        );
        assert_eq!(
            sqlite3_blob_write(blob, buf.as_ptr().cast(), 4, -1),
            SQLITE_ERROR
        );

        // Zero-length operations with a NULL data pointer are legal and still
        // range-check the offset.
        assert_eq!(sqlite3_blob_read(blob, ptr::null_mut(), 0, 8), SQLITE_OK);
        assert_eq!(sqlite3_blob_read(blob, ptr::null_mut(), 0, 9), SQLITE_ERROR);
        assert_eq!(sqlite3_blob_write(blob, ptr::null(), 0, 8), SQLITE_OK);
        assert_eq!(sqlite3_blob_write(blob, ptr::null(), 0, 9), SQLITE_ERROR);
        // A non-zero length with a NULL pointer is misuse.
        assert_eq!(
            sqlite3_blob_read(blob, ptr::null_mut(), 4, 0),
            SQLITE_MISUSE
        );
        assert_eq!(sqlite3_blob_write(blob, ptr::null(), 4, 0), SQLITE_MISUSE);

        // All of the above left the handle usable.
        assert_eq!(
            sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 8, 0),
            SQLITE_OK
        );
        assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);

        // NULL handles: bytes reports 0, close is a no-op, I/O is misuse.
        assert_eq!(sqlite3_blob_bytes(ptr::null_mut()), 0);
        assert_eq!(sqlite3_blob_close(ptr::null_mut()), SQLITE_OK);
        assert_eq!(
            sqlite3_blob_read(ptr::null_mut(), buf.as_mut_ptr().cast(), 1, 0),
            SQLITE_MISUSE
        );
        assert_eq!(
            sqlite3_blob_write(ptr::null_mut(), buf.as_ptr().cast(), 1, 0),
            SQLITE_MISUSE
        );

        assert_eq!(sqlite3_close(db), SQLITE_OK);
    }
}

/// e_blobopen R-50542-62589 at the FFI: writing the handle's row expires it and
/// subsequent operations return SQLITE_ABORT; writes to other rows do not.
#[test]
fn test_blob_expiry_returns_abort() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
        assert_eq!(
            sqlite3_exec(
                db,
                c"CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB); \
                      INSERT INTO t VALUES (1, x'00112233'); \
                      INSERT INTO t VALUES (2, x'ffeeddcc');"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            SQLITE_OK
        );
        let mut blob = ptr::null_mut();
        assert_eq!(
            sqlite3_blob_open(
                db,
                c"main".as_ptr(),
                c"t".as_ptr(),
                c"data".as_ptr(),
                1,
                0,
                &mut blob
            ),
            SQLITE_OK
        );
        let mut buf = [0u8; 4];
        assert_eq!(
            sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
            SQLITE_OK
        );

        // A write to a DIFFERENT row leaves the handle usable.
        assert_eq!(
            sqlite3_exec(
                db,
                c"UPDATE t SET data = x'00000000' WHERE id = 2".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            SQLITE_OK
        );
        assert_eq!(
            sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
            SQLITE_OK
        );
        assert_eq!(buf, [0x00, 0x11, 0x22, 0x33]);

        // A write to the handle's own row expires it: SQLITE_ABORT, sticky.
        assert_eq!(
            sqlite3_exec(
                db,
                c"UPDATE t SET data = x'99999999' WHERE id = 1".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            SQLITE_OK
        );
        assert_eq!(
            sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
            SQLITE_ABORT
        );
        assert_eq!(
            sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
            SQLITE_ABORT
        );
        assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);

        assert_eq!(sqlite3_close(db), SQLITE_OK);
    }
}
