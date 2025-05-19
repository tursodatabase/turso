#![allow(non_camel_case_types)]
#![allow(dead_code)]

use std::ptr;

#[repr(C)]
struct sqlite3 {
    _private: [u8; 0],
}

#[repr(C)]
struct sqlite3_stmt {
    _private: [u8; 0],
}

#[cfg_attr(not(feature = "sqlite3"), link(name = "limbo_sqlite3"))]
#[cfg_attr(feature = "sqlite3", link(name = "sqlite3"))]
extern "C" {
    fn sqlite3_libversion() -> *const libc::c_char;
    fn sqlite3_libversion_number() -> i32;
    fn sqlite3_close(db: *mut sqlite3) -> i32;
    fn sqlite3_open(filename: *const libc::c_char, db: *mut *mut sqlite3) -> i32;
    fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        sql: *const libc::c_char,
        n_bytes: i32,
        stmt: *mut *mut sqlite3_stmt,
        tail: *mut *const libc::c_char,
    ) -> i32;
    fn sqlite3_step(stmt: *mut sqlite3_stmt) -> i32;
    fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> i32;
    fn sqlite3_wal_checkpoint(db: *mut sqlite3, db_name: *const libc::c_char) -> i32;
    fn sqlite3_wal_checkpoint_v2(
        db: *mut sqlite3,
        db_name: *const libc::c_char,
        mode: i32,
        log_size: *mut i32,
        checkpoint_count: *mut i32,
    ) -> i32;
    fn libsql_wal_frame_count(db: *mut sqlite3, p_frame_count: *mut u32) -> i32;
}

const SQLITE_OK: i32 = 0;
const SQLITE_CANTOPEN: i32 = 14;
const SQLITE_DONE: i32 = 101;

const SQLITE_CHECKPOINT_PASSIVE: i32 = 0;
const SQLITE_CHECKPOINT_FULL: i32 = 1;
const SQLITE_CHECKPOINT_RESTART: i32 = 2;
const SQLITE_CHECKPOINT_TRUNCATE: i32 = 3;

#[cfg(not(target_os = "windows"))]
mod tests {
    use super::*;

    #[test]
    fn test_libversion() {
        unsafe {
            let version = sqlite3_libversion();
            assert!(!version.is_null());
        }
    }

    #[test]
    fn test_libversion_number() {
        unsafe {
            let version_num = sqlite3_libversion_number();
            assert!(version_num >= 3042000);
        }
    }

    #[test]
    fn test_open_not_found() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(
                sqlite3_open(c"not-found/local.db".as_ptr(), &mut db),
                SQLITE_CANTOPEN
            );
        }
    }

    #[test]
    fn test_open_existing() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(
                sqlite3_open(c"../testing/testing_clone.db".as_ptr(), &mut db),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_close() {
        unsafe {
            assert_eq!(sqlite3_close(ptr::null_mut()), SQLITE_OK);
        }
    }

    #[test]
    fn test_prepare_misuse() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(
                sqlite3_open(c"../testing/testing_clone.db".as_ptr(), &mut db),
                SQLITE_OK
            );

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT 1".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_wal_checkpoint() {
        unsafe {
            // Test with valid db
            let mut db = ptr::null_mut();
            assert_eq!(
                sqlite3_open(c"../testing/testing_clone.db".as_ptr(), &mut db),
                SQLITE_OK
            );
            assert_eq!(sqlite3_wal_checkpoint(db, ptr::null()), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_wal_checkpoint_v2() {
        unsafe {
            // Test with valid db
            let mut db = ptr::null_mut();
            assert_eq!(
                sqlite3_open(c"../testing/testing_clone.db".as_ptr(), &mut db),
                SQLITE_OK
            );

            let mut log_size = 0;
            let mut checkpoint_count = 0;

            // Test different checkpoint modes
            assert_eq!(
                sqlite3_wal_checkpoint_v2(
                    db,
                    ptr::null(),
                    SQLITE_CHECKPOINT_PASSIVE,
                    &mut log_size,
                    &mut checkpoint_count
                ),
                SQLITE_OK
            );

            assert_eq!(
                sqlite3_wal_checkpoint_v2(
                    db,
                    ptr::null(),
                    SQLITE_CHECKPOINT_FULL,
                    &mut log_size,
                    &mut checkpoint_count
                ),
                SQLITE_OK
            );

            assert_eq!(
                sqlite3_wal_checkpoint_v2(
                    db,
                    ptr::null(),
                    SQLITE_CHECKPOINT_RESTART,
                    &mut log_size,
                    &mut checkpoint_count
                ),
                SQLITE_OK
            );

            assert_eq!(
                sqlite3_wal_checkpoint_v2(
                    db,
                    ptr::null(),
                    SQLITE_CHECKPOINT_TRUNCATE,
                    &mut log_size,
                    &mut checkpoint_count
                ),
                SQLITE_OK
            );

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[cfg(not(feature = "sqlite3"))]
    mod libsql_ext {
        use super::*;

        #[test]
        fn test_wal_frame_count() {
            unsafe {
                let mut db = ptr::null_mut();
                assert_eq!(
                    sqlite3_open(c"../testing/testing_clone.db".as_ptr(), &mut db),
                    SQLITE_OK
                );
                // Ensure that WAL is initially empty.
                let mut frame_count = 0;
                assert_eq!(libsql_wal_frame_count(db, &mut frame_count), SQLITE_OK);
                assert_eq!(frame_count, 0);
                // Create a table and insert a row.
                let mut stmt = ptr::null_mut();
                assert_eq!(
                    sqlite3_prepare_v2(
                        db,
                        c"CREATE TABLE test (id INTEGER PRIMARY KEY)".as_ptr(),
                        -1,
                        &mut stmt,
                        ptr::null_mut()
                    ),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
                let mut stmt = ptr::null_mut();
                assert_eq!(
                    sqlite3_prepare_v2(
                        db,
                        c"INSERT INTO test (id) VALUES (1)".as_ptr(),
                        -1,
                        &mut stmt,
                        ptr::null_mut()
                    ),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
                // Check that WAL has three frames.
                assert_eq!(libsql_wal_frame_count(db, &mut frame_count), SQLITE_OK);
                assert_eq!(frame_count, 3);
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
        }
    }
}
