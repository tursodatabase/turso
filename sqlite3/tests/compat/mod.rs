#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_imports)]
use std::ptr;

#[repr(C)]
struct sqlite3 {
    _private: [u8; 0],
}

#[repr(C)]
struct sqlite3_stmt {
    _private: [u8; 0],
}

// Windows: This entire compat test is excluded because Windows has no system SQLite library
// (unlike Linux which has libsqlite3-dev pre-installed). The sqlite3 feature links against
// native libsqlite3 for comparison testing, which isn't available on Windows without complex
// setup (generating .lib from .def using VS tooling). Since SQLite behavior is platform-
// independent, running these tests on Linux/macOS provides sufficient coverage.
#[cfg(not(target_os = "windows"))]
#[cfg_attr(not(feature = "sqlite3"), link(name = "turso_sqlite3"))]
#[cfg_attr(feature = "sqlite3", link(name = "sqlite3"))]
extern "C" {
    fn sqlite3_libversion() -> *const libc::c_char;
    fn sqlite3_libversion_number() -> i32;
    fn sqlite3_close(db: *mut sqlite3) -> i32;
    fn sqlite3_open(filename: *const libc::c_char, db: *mut *mut sqlite3) -> i32;
    fn sqlite3_db_filename(db: *mut sqlite3, db_name: *const libc::c_char) -> *const libc::c_char;
    fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const libc::c_char,
        callback: Option<
            unsafe extern "C" fn(
                arg1: *mut libc::c_void,
                arg2: libc::c_int,
                arg3: *mut *mut libc::c_char,
                arg4: *mut *mut libc::c_char,
            ) -> libc::c_int,
        >,
        arg: *mut libc::c_void,
        errmsg: *mut *mut libc::c_char,
    ) -> i32;
    fn sqlite3_free(ptr: *mut libc::c_void);
    fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        sql: *const libc::c_char,
        n_bytes: i32,
        stmt: *mut *mut sqlite3_stmt,
        tail: *mut *const libc::c_char,
    ) -> i32;
    fn sqlite3_step(stmt: *mut sqlite3_stmt) -> i32;
    fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> i32;
    fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> i32;
    fn sqlite3_wal_checkpoint(db: *mut sqlite3, db_name: *const libc::c_char) -> i32;
    fn sqlite3_wal_checkpoint_v2(
        db: *mut sqlite3,
        db_name: *const libc::c_char,
        mode: i32,
        log_size: *mut i32,
        checkpoint_count: *mut i32,
    ) -> i32;
    fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, idx: i32) -> i64;
    fn libsql_wal_frame_count(db: *mut sqlite3, p_frame_count: *mut u32) -> i32;
    fn libsql_wal_get_frame(
        db: *mut sqlite3,
        frame_no: u32,
        p_frame: *mut u8,
        frame_len: u32,
    ) -> i32;
    fn libsql_wal_disable_checkpoint(db: *mut sqlite3) -> i32;
    fn sqlite3_column_int(stmt: *mut sqlite3_stmt, idx: i32) -> i64;
    fn sqlite3_next_stmt(db: *mut sqlite3, stmt: *mut sqlite3_stmt) -> *mut sqlite3_stmt;
    fn sqlite3_bind_int(stmt: *mut sqlite3_stmt, idx: i32, val: i64) -> i32;
    fn sqlite3_bind_parameter_count(stmt: *mut sqlite3_stmt) -> i32;
    fn sqlite3_bind_parameter_name(stmt: *mut sqlite3_stmt, idx: i32) -> *const libc::c_char;
    fn sqlite3_bind_parameter_index(stmt: *mut sqlite3_stmt, name: *const libc::c_char) -> i32;
    fn sqlite3_clear_bindings(stmt: *mut sqlite3_stmt) -> i32;
    fn sqlite3_column_name(stmt: *mut sqlite3_stmt, idx: i32) -> *const libc::c_char;
    fn sqlite3_column_table_name(stmt: *mut sqlite3_stmt, idx: i32) -> *const libc::c_char;
    fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> i32;
    fn sqlite3_column_count(stmt: *mut sqlite3_stmt) -> i32;
    fn sqlite3_bind_text(
        stmt: *mut sqlite3_stmt,
        idx: i32,
        text: *const libc::c_char,
        len: i32,
        destructor: Option<unsafe extern "C" fn(*mut libc::c_void)>,
    ) -> i32;
    fn sqlite3_bind_blob(
        stmt: *mut sqlite3_stmt,
        idx: i32,
        blob: *const libc::c_void,
        len: i32,
        destructor: Option<unsafe extern "C" fn(*mut libc::c_void)>,
    ) -> i32;
    fn sqlite3_column_text(stmt: *mut sqlite3_stmt, idx: i32) -> *const libc::c_char;
    fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, idx: i32) -> i64;
    fn sqlite3_column_blob(stmt: *mut sqlite3_stmt, idx: i32) -> *const libc::c_void;
    fn sqlite3_column_type(stmt: *mut sqlite3_stmt, idx: i32) -> i32;
    fn sqlite3_column_decltype(stmt: *mut sqlite3_stmt, idx: i32) -> *const libc::c_char;
    fn sqlite3_get_autocommit(db: *mut sqlite3) -> i32;
    fn sqlite3_changes(db: *mut sqlite3) -> i32;
    fn sqlite3_changes64(db: *mut sqlite3) -> i64;
    fn sqlite3_table_column_metadata(
        db: *mut sqlite3,
        z_db_name: *const libc::c_char,
        z_table_name: *const libc::c_char,
        z_column_name: *const libc::c_char,
        pz_data_type: *mut *const libc::c_char,
        pz_coll_seq: *mut *const libc::c_char,
        p_not_null: *mut libc::c_int,
        p_primary_key: *mut libc::c_int,
        p_autoinc: *mut libc::c_int,
    ) -> i32;
    fn sqlite3_busy_handler(
        db: *mut sqlite3,
        callback: Option<unsafe extern "C" fn(*mut libc::c_void, i32) -> i32>,
        arg: *mut libc::c_void,
    ) -> i32;
    fn sqlite3_busy_timeout(db: *mut sqlite3, ms: i32) -> i32;
}

const SQLITE_OK: i32 = 0;
const SQLITE_ERROR: i32 = 1;
const SQLITE_MISUSE: i32 = 21;
const SQLITE_CANTOPEN: i32 = 14;
const SQLITE_ROW: i32 = 100;
const SQLITE_DONE: i32 = 101;

const SQLITE_CHECKPOINT_PASSIVE: i32 = 0;
const SQLITE_CHECKPOINT_FULL: i32 = 1;
const SQLITE_CHECKPOINT_RESTART: i32 = 2;
const SQLITE_CHECKPOINT_TRUNCATE: i32 = 3;
const SQLITE_INTEGER: i32 = 1;
const SQLITE_FLOAT: i32 = 2;
const SQLITE_ABORT: i32 = 4;
const SQLITE_TEXT: i32 = 3;
const SQLITE3_TEXT: i32 = 3;
const SQLITE_BLOB: i32 = 4;
const SQLITE_NULL: i32 = 5;

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
                sqlite3_open(c"../testing/system/testing_clone.db".as_ptr(), &mut db),
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
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

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
    fn test_sqlite3_bind_int() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE test_bind (id INTEGER PRIMARY KEY, value INTEGER)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_bind (value) VALUES (?)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_bind_int(stmt, 1, 42), SQLITE_OK);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT value FROM test_bind LIMIT 1".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 42);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }
    #[test]
    fn test_sqlite3_bind_parameter_name_and_count() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE test_params (id INTEGER PRIMARY KEY, value TEXT)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_params (id, value) VALUES (?1, ?2)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            let param_count = sqlite3_bind_parameter_count(stmt);
            assert_eq!(param_count, 2);

            println!("parameter count {param_count}");
            let name1 = sqlite3_bind_parameter_name(stmt, 1);
            assert!(!name1.is_null());
            let name1_str = std::ffi::CStr::from_ptr(name1).to_str().unwrap();
            assert_eq!(name1_str, "?1");

            let name2 = sqlite3_bind_parameter_name(stmt, 2);
            assert!(!name2.is_null());
            let name2_str = std::ffi::CStr::from_ptr(name2).to_str().unwrap();
            assert_eq!(name2_str, "?2");

            let invalid_name = sqlite3_bind_parameter_name(stmt, 99);
            assert!(invalid_name.is_null());

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_last_insert_rowid() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = std::ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE test_rowid (value INTEGER)".as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_rowid (value) VALUES (6)".as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let last_rowid = sqlite3_last_insert_rowid(db);
            assert!(last_rowid > 0);
            println!("last insert rowid: {last_rowid}");
            let query = format!("SELECT value FROM test_rowid WHERE rowid = {last_rowid}");
            let query_cstring = std::ffi::CString::new(query).unwrap();

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    query_cstring.as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut(),
                ),
                SQLITE_OK
            );

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let value_int = sqlite3_column_int(stmt, 0);
            assert_eq!(value_int, 6);

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }
    #[test]
    fn test_sqlite3_column_name() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = std::ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE test_cols (id INTEGER PRIMARY KEY, value TEXT)".as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT id, value FROM test_cols".as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut(),
                ),
                SQLITE_OK
            );

            let col_count = sqlite3_column_count(stmt);
            assert_eq!(col_count, 2);

            let name1 = sqlite3_column_name(stmt, 0);
            assert!(!name1.is_null());
            let name1_str = std::ffi::CStr::from_ptr(name1).to_str().unwrap();
            assert_eq!(name1_str, "id");

            let table_name1 = sqlite3_column_table_name(stmt, 0);
            assert!(!table_name1.is_null());
            let table_name1_str = std::ffi::CStr::from_ptr(table_name1).to_str().unwrap();
            assert_eq!(table_name1_str, "test_cols");

            let name2 = sqlite3_column_name(stmt, 1);
            assert!(!name2.is_null());
            let name2_str = std::ffi::CStr::from_ptr(name2).to_str().unwrap();
            assert_eq!(name2_str, "value");

            // will lead to panic
            //let invalid = sqlite3_column_name(stmt, 5);
            //assert!(invalid.is_null());

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn column_text_is_nul_terminated_and_bytes_match() {
        unsafe {
            let mut db = std::ptr::null_mut();
            assert_eq!(
                sqlite3_open(c"../testing/system/testing.db".as_ptr(), &mut db),
                SQLITE_OK
            );
            let mut stmt = std::ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT first_name FROM users ORDER BY rowid ASC LIMIT 1;".as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let p = sqlite3_column_text(stmt, 0);
            assert!(!p.is_null());
            let bytes = sqlite3_column_bytes(stmt, 0) as usize;
            // NUL at [bytes], and no extra counted
            let slice = std::slice::from_raw_parts(p, bytes + 1);
            assert_eq!(slice[bytes], 0);
            assert_eq!(libc::strlen(p), bytes);

            let s = std::ffi::CStr::from_ptr(p).to_str().unwrap();
            assert_eq!(s, "Jamie");
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_bind_text() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE test_bind_text_rs (id INTEGER PRIMARY KEY, value TEXT)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            let destructor = std::mem::transmute::<
                isize,
                Option<unsafe extern "C" fn(*mut std::ffi::c_void)>,
            >(-1isize);
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_bind_text_rs (value) VALUES (?)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            let val = std::ffi::CString::new("hello world").unwrap();
            assert_eq!(
                sqlite3_bind_text(stmt, 1, val.as_ptr(), -1, destructor),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_bind_text_rs (value) VALUES (?)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            let val2 = std::ffi::CString::new("abcdef").unwrap();
            assert_eq!(
                sqlite3_bind_text(stmt, 1, val2.as_ptr(), 3, destructor),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT value FROM test_bind_text_rs ORDER BY id".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let col1_ptr = sqlite3_column_text(stmt, 0);
            assert!(!col1_ptr.is_null());
            let col1_str = std::ffi::CStr::from_ptr(col1_ptr).to_str().unwrap();
            assert_eq!(col1_str, "hello world");

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);

            let col2_ptr = sqlite3_column_text(stmt, 0);
            let col2_len = sqlite3_column_bytes(stmt, 0);
            assert!(!col2_ptr.is_null());

            let col2_slice = std::slice::from_raw_parts(col2_ptr as *const u8, col2_len as usize);
            let col2_str = std::str::from_utf8(col2_slice).unwrap().to_owned();

            assert_eq!(col2_str, "abc");
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_bind_blob() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE test_bind_blob_rs (id INTEGER PRIMARY KEY, data BLOB)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_bind_blob_rs (data) VALUES (?)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            let data1 = b"\x01\x02\x03\x04\x05";
            assert_eq!(
                sqlite3_bind_blob(
                    stmt,
                    1,
                    data1.as_ptr() as *const _,
                    data1.len() as i32,
                    None
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_bind_blob_rs (data) VALUES (?)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            let data2 = b"\xAA\xBB\xCC\xDD";
            assert_eq!(
                sqlite3_bind_blob(stmt, 1, data2.as_ptr() as *const _, 2, None),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT data FROM test_bind_blob_rs ORDER BY id".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let col1_ptr = sqlite3_column_blob(stmt, 0);
            let col1_len = sqlite3_column_bytes(stmt, 0);
            let col1_slice = std::slice::from_raw_parts(col1_ptr as *const u8, col1_len as usize);
            assert_eq!(col1_slice, data1);

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let col2_ptr = sqlite3_column_blob(stmt, 0);
            let col2_len = sqlite3_column_bytes(stmt, 0);
            let col2_slice = std::slice::from_raw_parts(col2_ptr as *const u8, col2_len as usize);
            assert_eq!(col2_slice, &data2[..2]);

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_column_type() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = std::ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
            sqlite3_prepare_v2(
                db,
                c"CREATE TABLE test_types (col_int INTEGER, col_float REAL, col_text TEXT, col_blob BLOB, col_null text)".as_ptr(),
                -1,
                &mut stmt,
                std::ptr::null_mut(),
            ),
            SQLITE_OK
        );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_types VALUES (123, 45.67, 'hello', x'010203', null)"
                        .as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT col_int, col_float, col_text, col_blob, col_null FROM test_types"
                        .as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);

            assert_eq!(sqlite3_column_type(stmt, 0), SQLITE_INTEGER);
            assert_eq!(sqlite3_column_type(stmt, 1), SQLITE_FLOAT);
            assert_eq!(sqlite3_column_type(stmt, 2), SQLITE_TEXT);
            assert_eq!(sqlite3_column_type(stmt, 3), SQLITE_BLOB);
            assert_eq!(sqlite3_column_type(stmt, 4), SQLITE_NULL);

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_column_decltype() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = std::ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
            sqlite3_prepare_v2(
                db,
                c"CREATE TABLE test_decltype (col_int INTEGER, col_float REAL, col_text TEXT, col_blob BLOB, col_null NULL)".as_ptr(),
                -1,
                &mut stmt,
                std::ptr::null_mut(),
            ),
            SQLITE_OK
        );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = std::ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT col_int, col_float, col_text, col_blob, col_null FROM test_decltype"
                        .as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut(),
                ),
                SQLITE_OK
            );

            let expected = [
                Some("INTEGER"),
                Some("REAL"),
                Some("TEXT"),
                Some("BLOB"),
                None,
            ];

            for i in 0..sqlite3_column_count(stmt) {
                let decl = sqlite3_column_decltype(stmt, i);

                if decl.is_null() {
                    assert!(expected[i as usize].is_none());
                } else {
                    let s = std::ffi::CStr::from_ptr(decl)
                        .to_string_lossy()
                        .into_owned();
                    assert_eq!(Some(s.as_str()), expected[i as usize]);
                }
            }

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_multi_statement_dml() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // Multiple DML statements in one exec call
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE bind_text(x TEXT);\
              INSERT INTO bind_text(x) VALUES('TEXT1');\
              INSERT INTO bind_text(x) VALUES('TEXT2');"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            // Verify the data was inserted
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT COUNT(*) FROM bind_text".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 2);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_multi_statement_with_semicolons_in_strings() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // Semicolons inside strings should not split statements
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test_semicolon(x TEXT);\
              INSERT INTO test_semicolon(x) VALUES('value;with;semicolons');\
              INSERT INTO test_semicolon(x) VALUES(\"another;value\");"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            // Verify the values contain semicolons
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT x FROM test_semicolon ORDER BY rowid".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let val1 = std::ffi::CStr::from_ptr(sqlite3_column_text(stmt, 0))
                .to_str()
                .unwrap();
            assert_eq!(val1, "value;with;semicolons");

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let val2 = std::ffi::CStr::from_ptr(sqlite3_column_text(stmt, 0))
                .to_str()
                .unwrap();
            assert_eq!(val2, "another;value");

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_multi_statement_with_escaped_quotes() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // Test escaped quotes
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test_quotes(x TEXT);\
              INSERT INTO test_quotes(x) VALUES('it''s working');\
              INSERT INTO test_quotes(x) VALUES(\"quote\"\"test\"\"\");"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT x FROM test_quotes ORDER BY rowid".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let val1 = std::ffi::CStr::from_ptr(sqlite3_column_text(stmt, 0))
                .to_str()
                .unwrap();
            assert_eq!(val1, "it's working");

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let val2 = std::ffi::CStr::from_ptr(sqlite3_column_text(stmt, 0))
                .to_str()
                .unwrap();
            assert_eq!(val2, "quote\"test\"");

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_with_select_callback() {
        unsafe {
            // Callback that collects results
            unsafe extern "C" fn exec_callback(
                context: *mut std::ffi::c_void,
                n_cols: std::ffi::c_int,
                values: *mut *mut std::ffi::c_char,
                _cols: *mut *mut std::ffi::c_char,
            ) -> std::ffi::c_int {
                let results = &mut *(context as *mut Vec<Vec<String>>);
                let mut row = Vec::new();

                for i in 0..n_cols as isize {
                    let value_ptr = *values.offset(i);
                    let value = if value_ptr.is_null() {
                        String::from("NULL")
                    } else {
                        std::ffi::CStr::from_ptr(value_ptr)
                            .to_str()
                            .unwrap()
                            .to_owned()
                    };
                    row.push(value);
                }
                results.push(row);
                0 // Continue
            }

            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // Setup data
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test_select(id INTEGER, name TEXT);\
              INSERT INTO test_select VALUES(1, 'Alice');\
              INSERT INTO test_select VALUES(2, 'Bob');"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            // Execute SELECT with callback
            let mut results: Vec<Vec<String>> = Vec::new();
            let rc = sqlite3_exec(
                db,
                c"SELECT id, name FROM test_select ORDER BY id".as_ptr(),
                Some(exec_callback),
                &mut results as *mut _ as *mut std::ffi::c_void,
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            assert_eq!(results.len(), 2);
            assert_eq!(results[0], vec!["1", "Alice"]);
            assert_eq!(results[1], vec!["2", "Bob"]);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_multi_statement_mixed_dml_select() {
        unsafe {
            // Callback that counts invocations
            unsafe extern "C" fn count_callback(
                context: *mut std::ffi::c_void,
                _n_cols: std::ffi::c_int,
                _values: *mut *mut std::ffi::c_char,
                _cols: *mut *mut std::ffi::c_char,
            ) -> std::ffi::c_int {
                let count = &mut *(context as *mut i32);
                *count += 1;
                0
            }

            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut callback_count = 0;

            // Mix of DDL/DML/DQL
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE mixed(x INTEGER);\
              INSERT INTO mixed VALUES(1);\
              INSERT INTO mixed VALUES(2);\
              SELECT x FROM mixed;\
              INSERT INTO mixed VALUES(3);\
              SELECT COUNT(*) FROM mixed;"
                    .as_ptr(),
                Some(count_callback),
                &mut callback_count as *mut _ as *mut std::ffi::c_void,
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            // Callback should be called 3 times total:
            // 2 times for first SELECT (2 rows)
            // 1 time for second SELECT (1 row with COUNT)
            assert_eq!(callback_count, 3);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_callback_abort() {
        unsafe {
            // Callback that aborts after first row
            unsafe extern "C" fn abort_callback(
                context: *mut std::ffi::c_void,
                _n_cols: std::ffi::c_int,
                _values: *mut *mut std::ffi::c_char,
                _cols: *mut *mut std::ffi::c_char,
            ) -> std::ffi::c_int {
                let count = &mut *(context as *mut i32);
                *count += 1;
                if *count >= 1 {
                    return 1; // Abort
                }
                0
            }

            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            sqlite3_exec(
                db,
                c"CREATE TABLE test(x INTEGER);\
              INSERT INTO test VALUES(1),(2),(3);"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );

            let mut count = 0;
            let rc = sqlite3_exec(
                db,
                c"SELECT x FROM test".as_ptr(),
                Some(abort_callback),
                &mut count as *mut _ as *mut std::ffi::c_void,
                ptr::null_mut(),
            );

            assert_eq!(rc, SQLITE_ABORT);
            assert_eq!(count, 1); // Only processed one row before aborting

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_error_stops_execution() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut err_msg = ptr::null_mut();

            // Second statement has error, third should not execute
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test(x INTEGER);\
              INSERT INTO nonexistent VALUES(1);\
              CREATE TABLE should_not_exist(y INTEGER);"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                &mut err_msg,
            );

            assert_eq!(rc, SQLITE_ERROR);

            // Verify third statement didn't execute
            let mut stmt = ptr::null_mut();
            let check_rc = sqlite3_prepare_v2(
                db,
                c"SELECT name FROM sqlite_master WHERE type='table' AND name='should_not_exist'"
                    .as_ptr(),
                -1,
                &mut stmt,
                ptr::null_mut(),
            );
            assert_eq!(check_rc, SQLITE_OK);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE); // No rows = table doesn't exist
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            if !err_msg.is_null() {
                sqlite3_free(err_msg as *mut std::ffi::c_void);
            }

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_empty_statements() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // Multiple semicolons and whitespace should be handled gracefully
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test(x INTEGER);;;\n\n;\t;INSERT INTO test VALUES(1);;;".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            // Verify both statements executed
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT x FROM test".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 1);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }
    #[test]
    fn test_exec_with_comments() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // SQL comments shouldn't affect statement splitting
            let rc = sqlite3_exec(
                db,
                c"-- This is a comment\n\
              CREATE TABLE test(x INTEGER); -- inline comment\n\
              INSERT INTO test VALUES(1); -- semicolon in comment ;\n\
              INSERT INTO test VALUES(2) -- end with comment"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            // Verify both inserts worked
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT COUNT(*) FROM test".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 2);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_nested_quotes() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // Mix of quote types and nesting
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test(x TEXT);\
              INSERT INTO test VALUES('single \"double\" inside');\
              INSERT INTO test VALUES(\"double 'single' inside\");\
              INSERT INTO test VALUES('mix;\"quote\";types');"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            // Verify values
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT x FROM test ORDER BY rowid".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let val1 = std::ffi::CStr::from_ptr(sqlite3_column_text(stmt, 0))
                .to_str()
                .unwrap();
            assert_eq!(val1, "single \"double\" inside");

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let val2 = std::ffi::CStr::from_ptr(sqlite3_column_text(stmt, 0))
                .to_str()
                .unwrap();
            assert_eq!(val2, "double 'single' inside");

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let val3 = std::ffi::CStr::from_ptr(sqlite3_column_text(stmt, 0))
                .to_str()
                .unwrap();
            assert_eq!(val3, "mix;\"quote\";types");

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_transaction_rollback() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // Test transaction rollback in multi-statement
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test(x INTEGER);\
              BEGIN TRANSACTION;\
              INSERT INTO test VALUES(1);\
              INSERT INTO test VALUES(2);\
              ROLLBACK;"
                    .as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            // Table should exist but be empty due to rollback
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT COUNT(*) FROM test".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 0); // No rows due to rollback
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_with_pragma() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // Callback to capture pragma results
            unsafe extern "C" fn pragma_callback(
                context: *mut std::ffi::c_void,
                _n_cols: std::ffi::c_int,
                _values: *mut *mut std::ffi::c_char,
                _cols: *mut *mut std::ffi::c_char,
            ) -> std::ffi::c_int {
                let count = &mut *(context as *mut i32);
                *count += 1;
                0
            }

            let mut callback_count = 0;

            // PRAGMA should be treated as DQL when it returns results
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test(x INTEGER);\
              PRAGMA table_info(test);"
                    .as_ptr(),
                Some(pragma_callback),
                &mut callback_count as *mut _ as *mut std::ffi::c_void,
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);
            assert!(callback_count > 0); // PRAGMA should return at least one row

            // PRAGMA without callback should discard row
            let mut err_msg = ptr::null_mut();
            let rc = sqlite3_exec(
                db,
                c"PRAGMA table_info(test)".as_ptr(),
                None,
                ptr::null_mut(),
                &mut err_msg,
            );
            assert_eq!(rc, SQLITE_OK);
            if !err_msg.is_null() {
                sqlite3_free(err_msg as *mut std::ffi::c_void);
            }

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_with_cte() {
        unsafe {
            // Callback that collects results
            unsafe extern "C" fn exec_callback(
                context: *mut std::ffi::c_void,
                n_cols: std::ffi::c_int,
                values: *mut *mut std::ffi::c_char,
                _cols: *mut *mut std::ffi::c_char,
            ) -> std::ffi::c_int {
                let results = &mut *(context as *mut Vec<Vec<String>>);
                let mut row = Vec::new();
                for i in 0..n_cols as isize {
                    let value_ptr = *values.offset(i);
                    let value = if value_ptr.is_null() {
                        String::from("NULL")
                    } else {
                        std::ffi::CStr::from_ptr(value_ptr)
                            .to_str()
                            .unwrap()
                            .to_owned()
                    };
                    row.push(value);
                }
                results.push(row);
                0
            }

            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // CTE should be recognized as DQL
            let mut results: Vec<Vec<String>> = Vec::new();
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test(x INTEGER);\
              INSERT INTO test VALUES(1),(2),(3);\
              WITH cte AS (SELECT x FROM test WHERE x > 1) SELECT * FROM cte;"
                    .as_ptr(),
                Some(exec_callback),
                &mut results as *mut _ as *mut std::ffi::c_void,
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);
            assert_eq!(results.len(), 2); // Should get 2 and 3
            assert_eq!(results[0], vec!["2"]);
            assert_eq!(results[1], vec!["3"]);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_with_returning_clause() {
        unsafe {
            // Callback for RETURNING results
            unsafe extern "C" fn exec_callback(
                context: *mut std::ffi::c_void,
                n_cols: std::ffi::c_int,
                values: *mut *mut std::ffi::c_char,
                _cols: *mut *mut std::ffi::c_char,
            ) -> std::ffi::c_int {
                let results = &mut *(context as *mut Vec<Vec<String>>);
                let mut row = Vec::new();
                for i in 0..n_cols as isize {
                    let value_ptr = *values.offset(i);
                    let value = if value_ptr.is_null() {
                        String::from("NULL")
                    } else {
                        std::ffi::CStr::from_ptr(value_ptr)
                            .to_str()
                            .unwrap()
                            .to_owned()
                    };
                    row.push(value);
                }
                results.push(row);
                0
            }

            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut results: Vec<Vec<String>> = Vec::new();

            // INSERT...RETURNING with callback should capture the returned values
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE test(id INTEGER PRIMARY KEY, x INTEGER);\
              INSERT INTO test(x) VALUES(42) RETURNING id, x;"
                    .as_ptr(),
                Some(exec_callback),
                &mut results as *mut _ as *mut std::ffi::c_void,
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);
            assert_eq!(results.len(), 1);
            assert_eq!(results[0][1], "42"); // x value

            // Add another row for testing
            sqlite3_exec(
                db,
                c"INSERT INTO test(x) VALUES(99)".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );

            // should still delete the row but discard the RETURNING results
            let rc = sqlite3_exec(
                db,
                c"UPDATE test SET id = 3, x = 41 WHERE x=42 RETURNING id".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);

            // Verify the row was actually updated
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT COUNT(*) FROM test WHERE x=42".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 0); // Should be 0 rows with x=42
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            // Verify
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT COUNT(*) FROM test".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 2);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[cfg(not(feature = "sqlite3"))]
    mod libsql_ext {

        use super::*;

        #[test]
        fn test_wal_frame_count() {
            unsafe {
                let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
                let path = temp_file.path();
                let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                let mut db = ptr::null_mut();
                assert_eq!(sqlite3_open(c_path.as_ptr(), &mut db), SQLITE_OK);
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

        #[test]
        fn test_read_frame() {
            unsafe {
                let mut db = ptr::null_mut();
                let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
                let path = temp_file.path();
                let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                assert_eq!(sqlite3_open(c_path.as_ptr(), &mut db), SQLITE_OK);
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
                let mut frame_count = 0;
                assert_eq!(libsql_wal_frame_count(db, &mut frame_count), SQLITE_OK);
                assert_eq!(frame_count, 3);
                for i in 1..frame_count + 1 {
                    let frame_len = 4096 + 24;
                    let mut frame = vec![0; frame_len];
                    assert_eq!(
                        libsql_wal_get_frame(db, i, frame.as_mut_ptr(), frame_len as u32),
                        SQLITE_OK
                    );
                }
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
        }

        #[test]
        fn test_disable_wal_checkpoint() {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            unsafe {
                let mut db = ptr::null_mut();
                let path = temp_file.path();
                let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                assert_eq!(sqlite3_open(c_path.as_ptr(), &mut db), SQLITE_OK);
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
                        c"INSERT INTO test (id) VALUES (0)".as_ptr(),
                        -1,
                        &mut stmt,
                        ptr::null_mut()
                    ),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

                let mut log_size = 0;
                let mut checkpoint_count = 0;

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
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
            let mut wal_path = temp_file.path().to_path_buf();
            assert!(wal_path.set_extension("db-wal"));
            std::fs::remove_file(wal_path.clone()).unwrap();

            {
                let mut db = ptr::null_mut();
                unsafe {
                    let path = temp_file.path();
                    let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                    assert_eq!(sqlite3_open(c_path.as_ptr(), &mut db), SQLITE_OK);
                    assert_eq!(libsql_wal_disable_checkpoint(db), SQLITE_OK);
                    // Insert at least 1000 rows to go over checkpoint threshold.
                    let mut stmt = ptr::null_mut();
                    for i in 1..2000 {
                        let sql =
                            std::ffi::CString::new(format!("INSERT INTO test (id) VALUES ({i})"))
                                .unwrap();
                        assert_eq!(
                            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                            SQLITE_OK
                        );
                        assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                        assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
                    }
                    assert_eq!(sqlite3_close(db), SQLITE_OK);
                }
            }

            // Delete WAL to ensure that we don't load anything from it
            std::fs::remove_file(wal_path).unwrap();
            let mut db = ptr::null_mut();
            unsafe {
                let path = temp_file.path();
                let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                assert_eq!(sqlite3_open(c_path.as_ptr(), &mut db), SQLITE_OK);
                // Insert at least 1000 rows to go over checkpoint threshold.
                let mut stmt = ptr::null_mut();
                assert_eq!(
                    sqlite3_prepare_v2(
                        db,
                        c"SELECT count() FROM test".as_ptr(),
                        -1,
                        &mut stmt,
                        ptr::null_mut()
                    ),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
                let count = sqlite3_column_int64(stmt, 0);
                assert_eq!(count, 1);
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            }
        }

        #[test]
        fn test_get_autocommit() {
            unsafe {
                let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
                let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
                let mut db = ptr::null_mut();
                assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

                // Should be in autocommit mode by default
                assert_eq!(sqlite3_get_autocommit(db), 1);

                // Begin a transaction
                let mut stmt = ptr::null_mut();
                assert_eq!(
                    sqlite3_prepare_v2(db, c"BEGIN".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

                // Should NOT be in autocommit mode during transaction
                assert_eq!(sqlite3_get_autocommit(db), 0);

                // Create a table within the transaction
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

                // Still not in autocommit mode
                assert_eq!(sqlite3_get_autocommit(db), 0);

                // Commit the transaction
                let mut stmt = ptr::null_mut();
                assert_eq!(
                    sqlite3_prepare_v2(db, c"COMMIT".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

                // Should be back in autocommit mode after commit
                assert_eq!(sqlite3_get_autocommit(db), 1);

                // Test with ROLLBACK
                let mut stmt = ptr::null_mut();
                assert_eq!(
                    sqlite3_prepare_v2(db, c"BEGIN".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

                assert_eq!(sqlite3_get_autocommit(db), 0);

                let mut stmt = ptr::null_mut();
                assert_eq!(
                    sqlite3_prepare_v2(db, c"ROLLBACK".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

                // Should be back in autocommit mode after rollback
                assert_eq!(sqlite3_get_autocommit(db), 1);

                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
        }

        #[test]
        fn test_wal_checkpoint() {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            unsafe {
                let mut db = ptr::null_mut();
                let path = temp_file.path();
                let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                assert_eq!(sqlite3_open(c_path.as_ptr(), &mut db), SQLITE_OK);
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
                        c"INSERT INTO test (id) VALUES (0)".as_ptr(),
                        -1,
                        &mut stmt,
                        ptr::null_mut()
                    ),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

                let mut log_size = 0;
                let mut checkpoint_count = 0;

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
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
            let mut wal_path = temp_file.path().to_path_buf();
            assert!(wal_path.set_extension("db-wal"));
            std::fs::remove_file(wal_path.clone()).unwrap();

            {
                let mut db = ptr::null_mut();
                unsafe {
                    let path = temp_file.path();
                    let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                    assert_eq!(sqlite3_open(c_path.as_ptr(), &mut db), SQLITE_OK);
                    // Insert at least 1000 rows to go over checkpoint threshold.
                    let mut stmt = ptr::null_mut();
                    for i in 1..2000 {
                        let sql =
                            std::ffi::CString::new(format!("INSERT INTO test (id) VALUES ({i})"))
                                .unwrap();
                        assert_eq!(
                            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                            SQLITE_OK
                        );
                        assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                        assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
                    }
                    assert_eq!(sqlite3_close(db), SQLITE_OK);
                }
            }

            // Delete WAL to ensure that we don't load anything from it
            std::fs::remove_file(wal_path).unwrap();
            let mut db = ptr::null_mut();
            unsafe {
                let path = temp_file.path();
                let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                assert_eq!(sqlite3_open(c_path.as_ptr(), &mut db), SQLITE_OK);
                // Insert at least 1000 rows to go over checkpoint threshold.
                let mut stmt = ptr::null_mut();
                assert_eq!(
                    sqlite3_prepare_v2(
                        db,
                        c"SELECT count() FROM test".as_ptr(),
                        -1,
                        &mut stmt,
                        ptr::null_mut()
                    ),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
                let count = sqlite3_column_int64(stmt, 0);
                // with a sane `should_checkpoint` method we have no guarantee that all 2000 rows are present, as the checkpoint was
                // triggered by cacheflush on insertions. the pattern will trigger a checkpoint when the wal has > 1000 frames,
                // so it will be triggered but will no longer be triggered on each consecutive
                // write. here we can assert that we have > 1500 rows.
                assert!(count > 1500, "count: {}", count);
                assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            }
        }
    }

    #[test]
    fn test_sqlite3_clear_bindings() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();

            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE person (id INTEGER, name TEXT, age INTEGER)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO person (id, name, age) VALUES (1, 'John', 25), (2, 'Jane', 30)"
                        .as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT * FROM person WHERE id = ? AND age > ?".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );

            // Bind parameters - should find John (id=1, age=25 > 20)
            assert_eq!(sqlite3_bind_int(stmt, 1, 1), SQLITE_OK);
            assert_eq!(sqlite3_bind_int(stmt, 2, 20), SQLITE_OK);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 1);
            assert_eq!(sqlite3_column_int(stmt, 2), 25);

            // Reset and clear bindings, query should return no rows
            assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
            assert_eq!(sqlite3_clear_bindings(stmt), SQLITE_OK);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_bind_parameter_index() {
        const SQLITE_OK: i32 = 0;

        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();

            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT * FROM sqlite_master WHERE name = :table_name AND type = :object_type"
                        .as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );

            let index1 = sqlite3_bind_parameter_index(stmt, c":table_name".as_ptr());
            assert_eq!(index1, 1);

            let index2 = sqlite3_bind_parameter_index(stmt, c":object_type".as_ptr());
            assert_eq!(index2, 2);

            let index3 = sqlite3_bind_parameter_index(stmt, c":nonexistent".as_ptr());
            assert_eq!(index3, 0);

            let index4 = sqlite3_bind_parameter_index(stmt, ptr::null());
            assert_eq!(index4, 0);

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_db_filename() {
        const SQLITE_OK: i32 = 0;

        unsafe {
            // Test with in-memory database
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let filename = sqlite3_db_filename(db, c"main".as_ptr());
            assert!(!filename.is_null());
            let filename_str = std::ffi::CStr::from_ptr(filename).to_str().unwrap();
            assert_eq!(filename_str, "");
            assert_eq!(sqlite3_close(db), SQLITE_OK);

            // Open a file-backed database
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            // Test with "main" database name
            let filename = sqlite3_db_filename(db, c"main".as_ptr());
            assert!(!filename.is_null());
            let filename_pathbuf =
                std::fs::canonicalize(std::ffi::CStr::from_ptr(filename).to_str().unwrap())
                    .unwrap();
            assert_eq!(filename_pathbuf, temp_file.path().canonicalize().unwrap());

            // Test with NULL database name (defaults to main)
            let filename_default = sqlite3_db_filename(db, ptr::null());
            assert!(!filename_default.is_null());
            assert_eq!(filename, filename_default);

            // Test with non-existent database name
            let filename = sqlite3_db_filename(db, c"temp".as_ptr());
            assert!(filename.is_null());

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_next_stmt() {
        const SQLITE_OK: i32 = 0;

        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            // Initially, there should be no prepared statements
            let iter = sqlite3_next_stmt(db, ptr::null_mut());
            assert!(iter.is_null());

            // Prepare first statement
            let mut stmt1: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT 1;".as_ptr(), -1, &mut stmt1, ptr::null_mut()),
                SQLITE_OK
            );
            assert!(!stmt1.is_null());

            // Now there should be one statement
            let iter = sqlite3_next_stmt(db, ptr::null_mut());
            assert_eq!(iter, stmt1);

            // And no more after that
            let iter = sqlite3_next_stmt(db, stmt1);
            assert!(iter.is_null());

            // Prepare second statement
            let mut stmt2: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT 2;".as_ptr(), -1, &mut stmt2, ptr::null_mut()),
                SQLITE_OK
            );
            assert!(!stmt2.is_null());

            // Prepare third statement
            let mut stmt3: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT 3;".as_ptr(), -1, &mut stmt3, ptr::null_mut()),
                SQLITE_OK
            );
            assert!(!stmt3.is_null());

            // Count all statements
            let mut count = 0;
            let mut iter = sqlite3_next_stmt(db, ptr::null_mut());
            while !iter.is_null() {
                count += 1;
                iter = sqlite3_next_stmt(db, iter);
            }
            assert_eq!(count, 3);

            // Finalize the middle statement
            assert_eq!(sqlite3_finalize(stmt2), SQLITE_OK);

            // Count should now be 2
            count = 0;
            iter = sqlite3_next_stmt(db, ptr::null_mut());
            while !iter.is_null() {
                count += 1;
                iter = sqlite3_next_stmt(db, iter);
            }
            assert_eq!(count, 2);

            // Finalize remaining statements
            assert_eq!(sqlite3_finalize(stmt1), SQLITE_OK);
            assert_eq!(sqlite3_finalize(stmt3), SQLITE_OK);

            // Should be no statements left
            let iter = sqlite3_next_stmt(db, ptr::null_mut());
            assert!(iter.is_null());

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_changes() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            // // Initially no changes
            assert_eq!(sqlite3_changes(db), 0);
            assert_eq!(sqlite3_changes64(db), 0);

            // Create a table
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE test_changes (id INTEGER PRIMARY KEY, value TEXT)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            // Still no changes after CREATE TABLE
            assert_eq!(sqlite3_changes(db), 0);
            assert_eq!(sqlite3_changes64(db), 0);

            // Insert a single row
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_changes (value) VALUES ('test1')".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            // Should have 1 change
            assert_eq!(sqlite3_changes(db), 1);
            assert_eq!(sqlite3_changes64(db), 1);

            // Insert multiple rows
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO test_changes (value) VALUES ('test2'), ('test3'), ('test4')"
                        .as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            // Should have 3 changes
            assert_eq!(sqlite3_changes(db), 3);
            assert_eq!(sqlite3_changes64(db), 3);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_table_column_metadata() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            // Create a test table
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE test_metadata (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL)"
                        .as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            // Test column metadata for 'id' column
            let mut data_type: *const libc::c_char = ptr::null();
            let mut coll_seq: *const libc::c_char = ptr::null();
            let mut not_null: libc::c_int = 0;
            let mut primary_key: libc::c_int = 0;
            let mut autoinc: libc::c_int = 0;

            assert_eq!(
                sqlite3_table_column_metadata(
                    db,
                    ptr::null(), // main database
                    c"test_metadata".as_ptr(),
                    c"id".as_ptr(),
                    &mut data_type,
                    &mut coll_seq,
                    &mut not_null,
                    &mut primary_key,
                    &mut autoinc,
                ),
                SQLITE_OK
            );

            // Verify the results
            assert!(!data_type.is_null());
            assert!(!coll_seq.is_null());
            assert_eq!(primary_key, 1); // id is primary key
            assert_eq!(not_null, 0); // INTEGER columns don't have NOT NULL by default
            assert_eq!(autoinc, 0); // not auto-increment

            // Test column metadata for 'name' column
            let mut data_type2: *const libc::c_char = ptr::null();
            let mut coll_seq2: *const libc::c_char = ptr::null();
            let mut not_null2: libc::c_int = 0;
            let mut primary_key2: libc::c_int = 0;
            let mut autoinc2: libc::c_int = 0;

            assert_eq!(
                sqlite3_table_column_metadata(
                    db,
                    ptr::null(), // main database
                    c"test_metadata".as_ptr(),
                    c"name".as_ptr(),
                    &mut data_type2,
                    &mut coll_seq2,
                    &mut not_null2,
                    &mut primary_key2,
                    &mut autoinc2,
                ),
                SQLITE_OK
            );

            // Verify the results
            assert!(!data_type2.is_null());
            assert!(!coll_seq2.is_null());
            assert_eq!(primary_key2, 0); // name is not primary key
            assert_eq!(not_null2, 1); // name has NOT NULL constraint
            assert_eq!(autoinc2, 0); // not auto-increment

            // Test non-existent column
            let mut data_type3: *const libc::c_char = ptr::null();
            let mut coll_seq3: *const libc::c_char = ptr::null();
            let mut not_null3: libc::c_int = 0;
            let mut primary_key3: libc::c_int = 0;
            let mut autoinc3: libc::c_int = 0;

            assert_eq!(
                sqlite3_table_column_metadata(
                    db,
                    ptr::null(), // main database
                    c"test_metadata".as_ptr(),
                    c"nonexistent".as_ptr(),
                    &mut data_type3,
                    &mut coll_seq3,
                    &mut not_null3,
                    &mut primary_key3,
                    &mut autoinc3,
                ),
                SQLITE_ERROR
            );

            // Test non-existent table
            let mut data_type4: *const libc::c_char = ptr::null();
            let mut coll_seq4: *const libc::c_char = ptr::null();
            let mut not_null4: libc::c_int = 0;
            let mut primary_key4: libc::c_int = 0;
            let mut autoinc4: libc::c_int = 0;

            assert_eq!(
                sqlite3_table_column_metadata(
                    db,
                    ptr::null(), // main database
                    c"nonexistent_table".as_ptr(),
                    c"id".as_ptr(),
                    &mut data_type4,
                    &mut coll_seq4,
                    &mut not_null4,
                    &mut primary_key4,
                    &mut autoinc4,
                ),
                SQLITE_ERROR
            );

            // Test rowid column
            let mut data_type5: *const libc::c_char = ptr::null();
            let mut coll_seq5: *const libc::c_char = ptr::null();
            let mut not_null5: libc::c_int = 0;
            let mut primary_key5: libc::c_int = 0;
            let mut autoinc5: libc::c_int = 0;

            assert_eq!(
                sqlite3_table_column_metadata(
                    db,
                    ptr::null(), // main database
                    c"test_metadata".as_ptr(),
                    c"rowid".as_ptr(),
                    &mut data_type5,
                    &mut coll_seq5,
                    &mut not_null5,
                    &mut primary_key5,
                    &mut autoinc5,
                ),
                SQLITE_OK
            );

            // Verify rowid results
            assert!(!data_type5.is_null());
            assert!(!coll_seq5.is_null());
            assert_eq!(primary_key5, 1); // rowid is primary key
            assert_eq!(not_null5, 0);
            assert_eq!(autoinc5, 0);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_busy_timeout() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            // Test setting a positive timeout
            assert_eq!(sqlite3_busy_timeout(db, 1000), SQLITE_OK);

            // Test setting a zero timeout (disables busy handler)
            assert_eq!(sqlite3_busy_timeout(db, 0), SQLITE_OK);

            // Test setting a negative timeout (also disables busy handler)
            assert_eq!(sqlite3_busy_timeout(db, -1), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    /// Busy handler callback that retries up to N times
    unsafe extern "C" fn busy_handler_retry_n(data: *mut libc::c_void, count: i32) -> i32 {
        if data.is_null() {
            return 0;
        }
        let max_retries = *(data as *const i32);
        if count < max_retries {
            1
        } else {
            0
        }
    }

    /// Busy handler callback that never retries
    unsafe extern "C" fn busy_handler_never_retry(_data: *mut libc::c_void, _count: i32) -> i32 {
        0
    }

    /// Busy handler callback that always retries (with safety limit)
    unsafe extern "C" fn busy_handler_always_retry(_data: *mut libc::c_void, count: i32) -> i32 {
        if count < 1000 {
            1
        } else {
            0
        }
    }

    #[test]
    fn test_sqlite3_busy_handler() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            // Test setting a custom busy handler with context
            let mut max_retries: i32 = 3;
            assert_eq!(
                sqlite3_busy_handler(
                    db,
                    Some(busy_handler_retry_n),
                    &mut max_retries as *mut i32 as *mut libc::c_void
                ),
                SQLITE_OK
            );

            // Test clearing the busy handler by passing NULL callback
            assert_eq!(sqlite3_busy_handler(db, None, ptr::null_mut()), SQLITE_OK);

            // Test setting busy handler that never retries
            assert_eq!(
                sqlite3_busy_handler(db, Some(busy_handler_never_retry), ptr::null_mut()),
                SQLITE_OK
            );

            // Test setting busy handler that always retries
            assert_eq!(
                sqlite3_busy_handler(db, Some(busy_handler_always_retry), ptr::null_mut()),
                SQLITE_OK
            );

            // Test that busy_timeout clears a previously set busy_handler
            assert_eq!(
                sqlite3_busy_handler(
                    db,
                    Some(busy_handler_retry_n),
                    &mut max_retries as *mut i32 as *mut libc::c_void
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_busy_timeout(db, 500), SQLITE_OK);

            // Test that busy_handler clears a previously set busy_timeout
            assert_eq!(sqlite3_busy_timeout(db, 1000), SQLITE_OK);
            assert_eq!(
                sqlite3_busy_handler(
                    db,
                    Some(busy_handler_retry_n),
                    &mut max_retries as *mut i32 as *mut libc::c_void
                ),
                SQLITE_OK
            );

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }
}
