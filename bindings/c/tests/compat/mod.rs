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

#[cfg_attr(
    all(not(feature = "sqlite3"), not(target_os = "windows")),
    link(name = "turso_sqlite3")
)]
#[cfg_attr(
    all(
        feature = "sqlite3",
        not(all(target_os = "windows", target_env = "msvc"))
    ),
    link(name = "sqlite3")
)]
#[cfg_attr(
    all(feature = "sqlite3", target_os = "windows", target_env = "msvc"),
    link(name = "sqlite3", kind = "static")
)]
#[cfg_attr(
    all(not(feature = "sqlite3"), target_os = "windows"),
    link(name = "turso_sqlite3", kind = "raw-dylib")
)]
extern "C" {
    fn sqlite3_user_data(context: *mut libc::c_void) -> *mut libc::c_void;
    fn sqlite3_aggregate_context(context: *mut libc::c_void, bytes: i32) -> *mut libc::c_void;
    fn sqlite3_result_error(context: *mut libc::c_void, message: *const libc::c_char, len: i32);
    fn sqlite3_create_function(
        db: *mut sqlite3,
        name: *const libc::c_char,
        n_args: i32,
        enc: i32,
        context: *mut libc::c_void,
        func: Option<unsafe extern "C" fn(*mut libc::c_void, i32, *mut *mut libc::c_void)>,
        step: Option<unsafe extern "C" fn(*mut libc::c_void, i32, *mut *mut libc::c_void)>,
        final_: Option<unsafe extern "C" fn(*mut libc::c_void)>,
    ) -> i32;
    fn sqlite3_create_collation(
        db: *mut sqlite3,
        name: *const libc::c_char,
        enc: i32,
        context: *mut libc::c_void,
        compare: Option<
            unsafe extern "C" fn(
                *mut libc::c_void,
                i32,
                *const libc::c_void,
                i32,
                *const libc::c_void,
            ) -> i32,
        >,
    ) -> i32;
    fn sqlite3_create_collation_v2(
        db: *mut sqlite3,
        name: *const libc::c_char,
        enc: i32,
        context: *mut libc::c_void,
        compare: Option<
            unsafe extern "C" fn(
                *mut libc::c_void,
                i32,
                *const libc::c_void,
                i32,
                *const libc::c_void,
            ) -> i32,
        >,
        destroy: Option<unsafe extern "C" fn(*mut libc::c_void)>,
    ) -> i32;
    fn sqlite3_libversion() -> *const libc::c_char;
    fn sqlite3_libversion_number() -> i32;
    fn sqlite3_close(db: *mut sqlite3) -> i32;
    fn sqlite3_close_v2(db: *mut sqlite3) -> i32;
    fn sqlite3_open(filename: *const libc::c_char, db: *mut *mut sqlite3) -> i32;
    fn sqlite3_db_readonly(db: *mut sqlite3, db_name: *const libc::c_char) -> i32;
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
    fn sqlite3_errcode(db: *mut sqlite3) -> i32;
    fn sqlite3_extended_errcode(db: *mut sqlite3) -> i32;
    fn sqlite3_errmsg(db: *mut sqlite3) -> *const libc::c_char;
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
    fn sqlite3_blob_open(
        db: *mut sqlite3,
        database: *const libc::c_char,
        table: *const libc::c_char,
        column: *const libc::c_char,
        rowid: i64,
        flags: i32,
        output: *mut *mut libc::c_void,
    ) -> i32;
    fn libsql_wal_insert_frame(
        db: *mut sqlite3,
        frame_no: u32,
        frame: *const u8,
        frame_len: u32,
        conflict: *mut i32,
    ) -> i32;
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
    fn sqlite3_total_changes(db: *mut sqlite3) -> i32;
    fn sqlite3_total_changes64(db: *mut sqlite3) -> i64;
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
    fn sqlite3_progress_handler(
        db: *mut sqlite3,
        n: i32,
        callback: Option<unsafe extern "C" fn(*mut libc::c_void) -> i32>,
        arg: *mut libc::c_void,
    );
    fn sqlite3_busy_timeout(db: *mut sqlite3, ms: i32) -> i32;
    fn sqlite3_interrupt(db: *mut sqlite3);
    fn sqlite3_get_table(
        db: *mut sqlite3,
        sql: *const libc::c_char,
        paz_result: *mut *mut *mut libc::c_char,
        pn_row: *mut libc::c_int,
        pn_column: *mut libc::c_int,
        pz_err_msg: *mut *mut libc::c_char,
    ) -> i32;
    fn sqlite3_free_table(az_result: *mut *mut libc::c_char);
    fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, idx: i32) -> i32;
    fn sqlite3_value_type(value: *mut libc::c_void) -> i32;
    fn sqlite3_value_blob(value: *mut libc::c_void) -> *const libc::c_void;
    fn sqlite3_value_bytes(value: *mut libc::c_void) -> i32;
    fn sqlite3_result_int64(context: *mut libc::c_void, val: i64);
    fn sqlite3_create_function_v2(
        db: *mut sqlite3,
        name: *const libc::c_char,
        n_args: i32,
        enc: i32,
        context: *mut libc::c_void,
        func: Option<unsafe extern "C" fn(*mut libc::c_void, i32, *mut *mut libc::c_void)>,
        step: Option<unsafe extern "C" fn()>,
        final_: Option<unsafe extern "C" fn()>,
        destroy: Option<unsafe extern "C" fn(*mut libc::c_void)>,
    ) -> i32;
    fn sqlite3_prepare_v3(
        db: *mut sqlite3,
        sql: *const libc::c_char,
        n_bytes: i32,
        prep_flags: u32,
        stmt: *mut *mut sqlite3_stmt,
        tail: *mut *const libc::c_char,
    ) -> i32;
    fn sqlite3_db_handle(stmt: *mut sqlite3_stmt) -> *mut sqlite3;
    fn sqlite3_stricmp(a: *const libc::c_char, b: *const libc::c_char) -> i32;
    fn sqlite3_strnicmp(a: *const libc::c_char, b: *const libc::c_char, n: i32) -> i32;
    fn sqlite3_value_int(value: *mut libc::c_void) -> i32;
    fn sqlite3_result_int(context: *mut libc::c_void, val: i32);
    fn sqlite3_initialize() -> i32;
    fn sqlite3_open_v2(
        filename: *const libc::c_char,
        db: *mut *mut sqlite3,
        flags: i32,
        z_vfs: *const libc::c_char,
    ) -> i32;
    fn sqlite3_column_value(stmt: *mut sqlite3_stmt, idx: i32) -> *mut libc::c_void;
    fn sqlite3_value_int64(value: *mut libc::c_void) -> i64;
    fn sqlite3_value_double(value: *mut libc::c_void) -> f64;
    fn sqlite3_value_text(value: *mut libc::c_void) -> *const libc::c_char;
    fn sqlite3_value_dup(value: *mut libc::c_void) -> *mut libc::c_void;
    fn sqlite3_value_free(value: *mut libc::c_void);
    fn sqlite3_context_db_handle(context: *mut libc::c_void) -> *mut libc::c_void;
}

const SQLITE_BUSY: i32 = 5;
const SQLITE_CONSTRAINT: i32 = 19;
const SQLITE_OK: i32 = 0;
const SQLITE_ERROR: i32 = 1;
const SQLITE_MISUSE: i32 = 21;
const SQLITE_RANGE: i32 = 25;
const SQLITE_CANTOPEN: i32 = 14;
const SQLITE_INTERRUPT: i32 = 9;
const SQLITE_ROW: i32 = 100;
const SQLITE_DONE: i32 = 101;
const SQLITE_PREPARE_PERSISTENT: u32 = 0x01;

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
const SQLITE_UTF8: i32 = 1;
const SQLITE_OPEN_READWRITE: i32 = 0x00000002;
const SQLITE_OPEN_CREATE: i32 = 0x00000004;
const SQLITE_OPEN_URI: i32 = 0x00000040;

mod tests {
    use super::*;

    struct ScalarData {
        db: *mut sqlite3,
        value: i64,
        destroyed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    unsafe extern "C" fn scalar_value(ctx: *mut libc::c_void, _: i32, _: *mut *mut libc::c_void) {
        let data = &*sqlite3_user_data(ctx).cast::<ScalarData>();
        if sqlite3_context_db_handle(ctx).cast::<sqlite3>() != data.db {
            sqlite3_result_error(ctx, c"wrong callback database".as_ptr(), -1);
        } else {
            sqlite3_result_int64(ctx, data.value);
        }
    }

    unsafe extern "C" fn scalar_destroy(data: *mut libc::c_void) {
        let data = Box::from_raw(data.cast::<ScalarData>());
        data.destroyed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    unsafe fn scalar_register(
        db: *mut sqlite3,
        name: &std::ffi::CStr,
        argc: i32,
        value: i64,
    ) -> (i32, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let destroyed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let data = Box::into_raw(Box::new(ScalarData {
            db,
            value,
            destroyed: destroyed.clone(),
        }));
        (
            sqlite3_create_function_v2(
                db,
                name.as_ptr(),
                argc,
                SQLITE_UTF8,
                data.cast(),
                Some(scalar_value),
                None,
                None,
                Some(scalar_destroy),
            ),
            destroyed,
        )
    }

    unsafe fn scalar_prepare(db: *mut sqlite3, sql: &str) -> *mut sqlite3_stmt {
        let sql = std::ffi::CString::new(sql).unwrap();
        let mut stmt = ptr::null_mut();
        assert_eq!(
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
            SQLITE_OK,
            "{sql:?}: {}",
            std::ffi::CStr::from_ptr(sqlite3_errmsg(db)).to_string_lossy()
        );
        stmt
    }

    unsafe fn scalar_query(db: *mut sqlite3, sql: &str, expected: i64) {
        let stmt = scalar_prepare(db, sql);
        assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
        assert_eq!(sqlite3_column_int64(stmt, 0), expected);
        assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
    }

    #[test]
    fn scalar_registrations_have_no_process_capacity_limit() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            for n in 0..64 {
                let name = std::ffi::CString::new(format!("scalar_many_{n}")).unwrap();
                assert_eq!(
                    scalar_register(db, &name, 0, n).0,
                    SQLITE_OK,
                    "registration {n}"
                );
            }
            for n in 0..64 {
                scalar_query(db, &format!("SELECT scalar_many_{n}()"), n);
            }
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn scalar_signatures_replace_and_delete_independently() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let (rc, zero) = scalar_register(db, c"scalar_overload", 0, 10);
            assert_eq!(rc, SQLITE_OK);
            let (rc, one) = scalar_register(db, c"scalar_overload", 1, 11);
            assert_eq!(rc, SQLITE_OK);
            let (rc, variadic) = scalar_register(db, c"scalar_overload", -1, 99);
            assert_eq!(rc, SQLITE_OK);
            scalar_query(db, "SELECT scalar_overload()", 10);
            scalar_query(db, "SELECT scalar_overload(7)", 11);
            scalar_query(db, "SELECT scalar_overload(7,8)", 99);
            assert_eq!(scalar_register(db, c"scalar_overload", 1, 21).0, SQLITE_OK);
            assert_eq!(one.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(zero.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(variadic.load(std::sync::atomic::Ordering::SeqCst), 0);
            scalar_query(db, "SELECT scalar_overload(7)", 21);
            assert_eq!(
                sqlite3_create_function_v2(
                    db,
                    c"scalar_overload".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    None,
                    None,
                    None,
                    None
                ),
                SQLITE_OK
            );
            let mut deleted = ptr::null_mut();
            assert_ne!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT scalar_overload(7)".as_ptr(),
                    -1,
                    &mut deleted,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            scalar_query(db, "SELECT scalar_overload(7,8)", 99);
            assert_eq!(
                sqlite3_create_function_v2(
                    db,
                    c"scalar_overload".as_ptr(),
                    -1,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    None,
                    None,
                    None,
                    None
                ),
                SQLITE_OK
            );
            let mut stmt = ptr::null_mut();
            assert_ne!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT scalar_overload(7)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            scalar_query(db, "SELECT scalar_overload()", 10);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(zero.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(variadic.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn scalar_fixed_arity_wins_in_both_registration_orders() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            for (name, reverse) in [(c"scalar_order_a", false), (c"scalar_order_b", true)] {
                let mut registrations = vec![(0, 10), (1, 11), (-1, 99)];
                if reverse {
                    registrations.reverse();
                }
                for (argc, value) in registrations {
                    assert_eq!(scalar_register(db, name, argc, value).0, SQLITE_OK);
                }
                let name = name.to_str().unwrap();
                scalar_query(db, &format!("SELECT {name}()"), 10);
                scalar_query(db, &format!("SELECT {name}(7)"), 11);
                scalar_query(db, &format!("SELECT {name}(7,8)"), 99);
            }
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn scalar_and_aggregate_share_registration_signature() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let (rc, destroyed) = scalar_register(db, c"scalar_then_aggregate", 1, 10);
            assert_eq!(rc, SQLITE_OK);
            assert_eq!(
                sqlite3_create_function(
                    db,
                    c"scalar_then_aggregate".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    None,
                    Some(signature_aggregate_step),
                    Some(signature_aggregate_final)
                ),
                SQLITE_OK
            );
            assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
            scalar_query(
                db,
                "WITH data(x) AS (VALUES (1), (2)) SELECT scalar_then_aggregate(x) FROM data",
                77,
            );
            assert_eq!(
                scalar_register(db, c"scalar_then_aggregate", 1, 20).0,
                SQLITE_OK
            );
            scalar_query(db, "SELECT scalar_then_aggregate(1)", 20);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    unsafe extern "C" fn signature_aggregate_step(
        _: *mut libc::c_void,
        _: i32,
        _: *mut *mut libc::c_void,
    ) {
    }
    unsafe extern "C" fn signature_aggregate_final(ctx: *mut libc::c_void) {
        sqlite3_result_int64(ctx, 77);
    }

    #[test]
    fn scalar_failed_registration_destroys_userdata_once() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let (rc, destroyed) = scalar_register(db, c"scalar_rejected", -2, 1);
            assert_ne!(rc, SQLITE_OK);
            assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    unsafe fn reject_aggregate_registration(
        db: *mut sqlite3,
        context: *mut libc::c_void,
        destroy: unsafe extern "C" fn(*mut libc::c_void),
    ) -> i32 {
        sqlite3_create_function_v2(
            db,
            c"aggregate_rejected".as_ptr(),
            -2,
            SQLITE_UTF8,
            context,
            None,
            Some(std::mem::transmute::<
                unsafe extern "C" fn(*mut libc::c_void, i32, *mut *mut libc::c_void),
                unsafe extern "C" fn(),
            >(signature_aggregate_step)),
            Some(std::mem::transmute::<
                unsafe extern "C" fn(*mut libc::c_void),
                unsafe extern "C" fn(),
            >(signature_aggregate_final)),
            Some(destroy),
        )
    }

    unsafe fn reject_partial_aggregate(
        db: *mut sqlite3,
        context: *mut libc::c_void,
        final_only: bool,
        destroy: unsafe extern "C" fn(*mut libc::c_void),
    ) -> i32 {
        let step = std::mem::transmute::<
            unsafe extern "C" fn(*mut libc::c_void, i32, *mut *mut libc::c_void),
            unsafe extern "C" fn(),
        >(signature_aggregate_step);
        let final_ = std::mem::transmute::<
            unsafe extern "C" fn(*mut libc::c_void),
            unsafe extern "C" fn(),
        >(signature_aggregate_final);
        sqlite3_create_function_v2(
            db,
            c"partial_aggregate".as_ptr(),
            1,
            SQLITE_UTF8,
            context,
            None,
            if final_only { None } else { Some(step) },
            if final_only { Some(final_) } else { None },
            Some(destroy),
        )
    }

    #[test]
    fn incomplete_aggregate_registration_destroys_userdata_once() {
        unsafe {
            let mut db = ptr::null_mut();
            let mut calls = 0_i32;
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            for final_only in [false, true] {
                assert_ne!(
                    reject_partial_aggregate(
                        db,
                        (&mut calls as *mut i32).cast(),
                        final_only,
                        count_collation_destroy
                    ),
                    SQLITE_OK
                );
                assert_eq!(calls, if final_only { 2 } else { 1 });
            }
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(calls, 2);
        }
    }

    #[test]
    fn aggregate_failed_registration_destroys_userdata_once() {
        unsafe {
            let mut db = ptr::null_mut();
            let mut calls = 0_i32;
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_ne!(
                reject_aggregate_registration(
                    db,
                    (&mut calls as *mut i32).cast(),
                    count_collation_destroy
                ),
                SQLITE_OK
            );
            assert_eq!(calls, 1);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(calls, 1);
        }
    }

    #[test]
    fn rejected_function_destructor_reentry_child() {
        if std::env::var_os("TURSO_REJECTED_FUNCTION_CHILD").is_none() {
            return;
        }
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let mut probe = CollationOwnerProbe {
                db,
                calls: 0,
                reenter: true,
                closing: false,
            };
            assert_ne!(
                reject_aggregate_registration(
                    db,
                    (&mut probe as *mut CollationOwnerProbe).cast(),
                    probe_collation_destroy
                ),
                SQLITE_OK
            );
            assert_eq!(probe.calls, 1);
            assert_ne!(
                sqlite3_create_function_v2(
                    db,
                    c"scalar_reentry_rejected".as_ptr(),
                    -2,
                    SQLITE_UTF8,
                    (&mut probe as *mut CollationOwnerProbe).cast(),
                    Some(signature_aggregate_step),
                    None,
                    None,
                    Some(probe_collation_destroy)
                ),
                SQLITE_OK
            );
            assert_eq!(probe.calls, 2);
            for final_only in [false, true] {
                assert_ne!(
                    reject_partial_aggregate(
                        db,
                        (&mut probe as *mut CollationOwnerProbe).cast(),
                        final_only,
                        probe_collation_destroy
                    ),
                    SQLITE_OK
                );
            }
            assert_eq!(probe.calls, 4);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(probe.calls, 4);
        }
    }

    #[test]
    fn rejected_function_destructor_reentry_is_bounded() {
        bounded_destructor_child(
            "tests::rejected_function_destructor_reentry_child",
            "TURSO_REJECTED_FUNCTION_CHILD",
        );
    }

    #[test]
    fn successful_function_destructor_reentry_child() {
        if std::env::var_os("TURSO_SUCCESSFUL_FUNCTION_CHILD").is_none() {
            return;
        }
        unsafe {
            let step_raw = std::mem::transmute::<
                unsafe extern "C" fn(*mut libc::c_void, i32, *mut *mut libc::c_void),
                unsafe extern "C" fn(),
            >(signature_aggregate_step);
            let final_raw = std::mem::transmute::<
                unsafe extern "C" fn(*mut libc::c_void),
                unsafe extern "C" fn(),
            >(signature_aggregate_final);
            for mode in 0..4 {
                let mut db = ptr::null_mut();
                assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
                let mut old = CollationOwnerProbe {
                    db,
                    calls: 0,
                    reenter: true,
                    closing: false,
                };
                let mut new = CollationOwnerProbe {
                    db,
                    calls: 0,
                    reenter: true,
                    closing: false,
                };
                let aggregate = mode == 1;
                assert_eq!(
                    sqlite3_create_function_v2(
                        db,
                        c"reentrant".as_ptr(),
                        1,
                        SQLITE_UTF8,
                        (&mut old as *mut CollationOwnerProbe).cast(),
                        (!aggregate).then_some(signature_aggregate_step),
                        aggregate.then_some(step_raw),
                        aggregate.then_some(final_raw),
                        Some(probe_collation_destroy),
                    ),
                    SQLITE_OK
                );
                assert_eq!(
                    sqlite3_create_function_v2(
                        db,
                        if mode == 3 { c"absent" } else { c"reentrant" }.as_ptr(),
                        1,
                        SQLITE_UTF8,
                        (&mut new as *mut CollationOwnerProbe).cast(),
                        (mode == 0).then_some(signature_aggregate_step),
                        aggregate.then_some(step_raw),
                        aggregate.then_some(final_raw),
                        Some(probe_collation_destroy),
                    ),
                    SQLITE_OK
                );
                assert_eq!(old.calls, i32::from(mode != 3));
                #[cfg(not(feature = "sqlite3"))]
                assert_eq!(new.calls, i32::from(mode >= 2));
                assert_eq!(sqlite3_close(db), SQLITE_OK);
                assert_eq!(old.calls, 1);
                assert_eq!(new.calls, 1);
            }
        }
    }

    #[test]
    fn successful_function_destructor_reentry_is_bounded() {
        bounded_destructor_child(
            "tests::successful_function_destructor_reentry_child",
            "TURSO_SUCCESSFUL_FUNCTION_CHILD",
        );
    }

    #[test]
    fn trigger_messages_preserve_engine_prefix_text() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE messages(value)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            for message in [
                "Internal error: user payload",
                "Parse error: user payload",
                "Corrupt database: user payload",
                "Env variable error: user payload",
            ] {
                let sql = std::ffi::CString::new(format!(
                    "CREATE TRIGGER reject BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'{message}'); END"
                )).unwrap();
                assert_eq!(
                    sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                    SQLITE_OK
                );
                let mut error = ptr::null_mut();
                assert_eq!(
                    sqlite3_exec(
                        db,
                        c"INSERT INTO messages VALUES(1)".as_ptr(),
                        None,
                        ptr::null_mut(),
                        &mut error,
                    ),
                    SQLITE_CONSTRAINT
                );
                assert!(!error.is_null());
                assert_eq!(std::ffi::CStr::from_ptr(error).to_str().unwrap(), message);
                assert_eq!(
                    std::ffi::CStr::from_ptr(sqlite3_errmsg(db))
                        .to_str()
                        .unwrap(),
                    message
                );
                sqlite3_free(error.cast());
                assert_eq!(
                    sqlite3_exec(
                        db,
                        c"DROP TRIGGER reject".as_ptr(),
                        None,
                        ptr::null_mut(),
                        ptr::null_mut()
                    ),
                    SQLITE_OK
                );
            }
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn scalar_prepared_replacement_keeps_userdata_alive() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let (rc, old) = scalar_register(db, c"scalar_prepared", 0, 10);
            assert_eq!(rc, SQLITE_OK);
            let stmt = scalar_prepare(db, "SELECT scalar_prepared()");
            let (rc, new) = scalar_register(db, c"scalar_prepared", 0, 20);
            assert_eq!(rc, SQLITE_OK);
            #[cfg(not(feature = "sqlite3"))]
            assert_eq!(old.load(std::sync::atomic::Ordering::SeqCst), 0);
            #[cfg(feature = "sqlite3")]
            assert_eq!(old.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 20);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(old.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(new.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(new.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn scalar_deferred_close_keeps_callback_database_alive() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let (rc, destroyed) = scalar_register(db, c"scalar_deferred", 0, 42);
            assert_eq!(rc, SQLITE_OK);
            let stmt = scalar_prepare(db, "SELECT scalar_deferred()");
            assert_eq!(sqlite3_close(db), SQLITE_BUSY);
            assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(sqlite3_close_v2(db), SQLITE_OK);
            assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 42);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn scalar_deleted_signature_can_be_registered_during_an_active_statement() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let (rc, variadic) = scalar_register(db, c"scalar_deleted_active", -1, 99);
            assert_eq!(rc, SQLITE_OK);
            let (rc, old) = scalar_register(db, c"scalar_deleted_active", 1, 10);
            assert_eq!(rc, SQLITE_OK);
            assert_eq!(
                sqlite3_create_function_v2(
                    db,
                    c"scalar_deleted_active".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    None,
                    None,
                    None,
                    None
                ),
                SQLITE_OK
            );
            assert_eq!(old.load(std::sync::atomic::Ordering::SeqCst), 1);
            let mut deleted = ptr::null_mut();
            assert_ne!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT scalar_deleted_active(7)".as_ptr(),
                    -1,
                    &mut deleted,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            let active = scalar_prepare(db, "SELECT 1 UNION ALL SELECT 2");
            assert_eq!(sqlite3_step(active), SQLITE_ROW);
            assert_eq!(
                sqlite3_create_function_v2(
                    db,
                    c"scalar_deleted_active".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    None,
                    None,
                    None,
                    None
                ),
                SQLITE_OK
            );
            let (rc, new) = scalar_register(db, c"scalar_deleted_active", 1, 20);
            assert_eq!(rc, SQLITE_OK);
            assert_eq!(new.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(sqlite3_step(active), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(active, 0), 2);
            assert_eq!(sqlite3_step(active), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(active), SQLITE_OK);
            scalar_query(db, "SELECT scalar_deleted_active(7)", 20);
            scalar_query(db, "SELECT scalar_deleted_active(7,8)", 99);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(old.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(new.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(variadic.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn scalar_active_replacement_is_busy_and_destroys_rejected_userdata() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let (rc, old) = scalar_register(db, c"scalar_active", 0, 10);
            assert_eq!(rc, SQLITE_OK);
            let stmt = scalar_prepare(
                db,
                "SELECT scalar_active() UNION ALL SELECT scalar_active()",
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let (rc, rejected) = scalar_register(db, c"scalar_active", 0, 20);
            assert_eq!(rc, SQLITE_BUSY);
            assert_eq!(rejected.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(
                sqlite3_create_function_v2(
                    db,
                    c"scalar_active".as_ptr(),
                    0,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    None,
                    None,
                    None,
                    None
                ),
                SQLITE_BUSY
            );
            assert_eq!(old.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 10);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            scalar_query(db, "SELECT scalar_active()", 10);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(old.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn scalar_registrations_are_connection_local() {
        unsafe {
            let (mut a, mut b) = (ptr::null_mut(), ptr::null_mut());
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut a), SQLITE_OK);
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut b), SQLITE_OK);
            let (rc, a_destroyed) = scalar_register(a, c"scalar_shared", 0, 111);
            assert_eq!(rc, SQLITE_OK);
            let prepared = scalar_prepare(a, "SELECT scalar_shared()");
            let (rc, b_destroyed) = scalar_register(b, c"scalar_shared", 0, 222);
            assert_eq!(rc, SQLITE_OK);
            assert_eq!(sqlite3_step(prepared), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(prepared, 0), 111);
            assert_eq!(sqlite3_finalize(prepared), SQLITE_OK);
            scalar_query(a, "SELECT scalar_shared()", 111);
            scalar_query(b, "SELECT scalar_shared()", 222);
            let (rc, replacement) = scalar_register(b, c"scalar_shared", 0, 333);
            assert_eq!(rc, SQLITE_OK);
            assert_eq!(b_destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(a_destroyed.load(std::sync::atomic::Ordering::SeqCst), 0);
            scalar_query(b, "SELECT scalar_shared()", 333);
            assert_eq!(sqlite3_close(b), SQLITE_OK);
            assert_eq!(replacement.load(std::sync::atomic::Ordering::SeqCst), 1);
            scalar_query(a, "SELECT scalar_shared()", 111);
            assert_eq!(sqlite3_close(a), SQLITE_OK);
            assert_eq!(a_destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn turso_version_stays_the_engine_version_after_schema_creation() {
        unsafe {
            for filename in [c":memory:", c"turso-version-test.db"] {
                let temp = tempfile::tempdir().unwrap();
                let path = if filename == c":memory:" {
                    std::ffi::CString::new(":memory:").unwrap()
                } else {
                    std::ffi::CString::new(temp.path().join("version.db").to_str().unwrap())
                        .unwrap()
                };
                let mut db = ptr::null_mut();
                assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);
                for phase in 0..3 {
                    if phase == 1 {
                        assert_eq!(
                            sqlite3_exec(
                                db,
                                c"CREATE TABLE example(id INTEGER)".as_ptr(),
                                None,
                                ptr::null_mut(),
                                ptr::null_mut()
                            ),
                            SQLITE_OK
                        );
                    } else if phase == 2 {
                        assert_eq!(sqlite3_close(db), SQLITE_OK);
                        assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);
                    }
                    let mut statement = ptr::null_mut();
                    assert_eq!(
                        sqlite3_prepare_v2(
                            db,
                            c"SELECT turso_version()".as_ptr(),
                            -1,
                            &mut statement,
                            ptr::null_mut()
                        ),
                        SQLITE_OK
                    );
                    assert_eq!(sqlite3_step(statement), SQLITE_ROW);
                    let version = std::ffi::CStr::from_ptr(
                        sqlite3_column_text(statement, 0) as *const libc::c_char
                    );
                    assert_eq!(
                        version.to_str().unwrap(),
                        env!("CARGO_PKG_VERSION"),
                        "phase {phase}"
                    );
                    assert_eq!(sqlite3_finalize(statement), SQLITE_OK);
                }
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
        }
    }

    #[test]
    fn legacy_scalar_function_registration_executes_callback() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_create_function(
                    db,
                    c"legacy_answer".as_ptr(),
                    0,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    Some(legacy_answer),
                    None,
                    None
                ),
                SQLITE_OK
            );
            let mut statement = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT legacy_answer()".as_ptr(),
                    -1,
                    &mut statement,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(statement), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(statement, 0), 42);
            assert_eq!(sqlite3_finalize(statement), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    unsafe extern "C" fn legacy_answer(
        context: *mut libc::c_void,
        _argc: i32,
        _argv: *mut *mut libc::c_void,
    ) {
        sqlite3_result_int(context, 42);
    }

    #[test]
    fn legacy_collation_registration_passes_context_to_callback() {
        unsafe {
            let mut db = ptr::null_mut();
            let mut calls = 0_i32;
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_create_collation(
                    db,
                    c"legacy_reverse".as_ptr(),
                    SQLITE_UTF8,
                    &mut calls as *mut i32 as *mut libc::c_void,
                    Some(legacy_reverse)
                ),
                SQLITE_OK
            );
            let mut statement = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT 'a' COLLATE legacy_reverse < 'b'".as_ptr(),
                    -1,
                    &mut statement,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(statement), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(statement, 0), 0);
            assert!(calls > 0);
            assert_eq!(sqlite3_finalize(statement), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    unsafe extern "C" fn count_collation_destroy(context: *mut libc::c_void) {
        *(context as *mut i32) += 1;
    }

    #[test]
    fn null_collation_owner_survives_until_close() {
        unsafe {
            let mut db = ptr::null_mut();
            let mut calls = 0_i32;
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_create_collation_v2(
                    db,
                    c"inactive".as_ptr(),
                    SQLITE_UTF8,
                    (&mut calls as *mut i32).cast(),
                    None,
                    Some(count_collation_destroy)
                ),
                SQLITE_OK
            );
            assert_eq!(calls, 0, "registration must retain the new owner");
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(calls, 1);
        }
    }

    struct CollationOwnerProbe {
        db: *mut sqlite3,
        calls: i32,
        reenter: bool,
        closing: bool,
    }

    unsafe extern "C" fn probe_collation_destroy(context: *mut libc::c_void) {
        let probe = &mut *context.cast::<CollationOwnerProbe>();
        probe.calls += 1;
        if probe.reenter {
            assert_eq!(sqlite3_changes(probe.db), 0);
            if probe.closing && !cfg!(feature = "sqlite3") {
                let mut rejected = 0_i32;
                assert_eq!(
                    sqlite3_create_collation_v2(
                        probe.db,
                        c"rejected".as_ptr(),
                        SQLITE_UTF8,
                        (&mut rejected as *mut i32).cast(),
                        None,
                        Some(count_collation_destroy)
                    ),
                    SQLITE_MISUSE
                );
                assert_eq!(rejected, 0);
            }
        }
    }

    unsafe fn register_inactive(probe: &mut CollationOwnerProbe, name: &std::ffi::CStr) {
        assert_eq!(
            sqlite3_create_collation_v2(
                probe.db,
                name.as_ptr(),
                SQLITE_UTF8,
                (probe as *mut CollationOwnerProbe).cast(),
                None,
                Some(probe_collation_destroy)
            ),
            SQLITE_OK
        );
    }

    #[test]
    fn null_collation_deferred_close_retains_owner() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let mut probe = CollationOwnerProbe {
                db,
                calls: 0,
                reenter: false,
                closing: false,
            };
            register_inactive(&mut probe, c"inactive");
            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT 1".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(db), SQLITE_BUSY);
            assert_eq!(probe.calls, 0);
            assert_eq!(sqlite3_close_v2(db), SQLITE_OK);
            assert_eq!(probe.calls, 0);
            #[cfg(not(feature = "sqlite3"))]
            {
                let mut rejected = 0_i32;
                assert_eq!(
                    sqlite3_create_collation_v2(
                        db,
                        c"inactive".as_ptr(),
                        SQLITE_UTF8,
                        (&mut rejected as *mut i32).cast(),
                        None,
                        Some(count_collation_destroy)
                    ),
                    SQLITE_MISUSE
                );
                assert_eq!(rejected, 0);
            }
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(probe.calls, 1);
        }
    }

    #[test]
    fn null_collation_replacement_releases_each_owner() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            let mut first = CollationOwnerProbe {
                db,
                calls: 0,
                reenter: false,
                closing: false,
            };
            let mut second = CollationOwnerProbe {
                db,
                calls: 0,
                reenter: false,
                closing: false,
            };
            register_inactive(&mut first, c"INACTIVE");
            register_inactive(&mut second, c"inactive");
            // SQLite overwrites an already-NULL owner's destructor without calling it.
            #[cfg(not(feature = "sqlite3"))]
            assert_eq!(first.calls, 1);
            #[cfg(feature = "sqlite3")]
            assert_eq!(first.calls, 0);
            assert_eq!(second.calls, 0);
            assert_eq!(
                sqlite3_create_collation(
                    db,
                    c"InAcTiVe".as_ptr(),
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    Some(compare_collation_probe)
                ),
                SQLITE_OK
            );
            #[cfg(not(feature = "sqlite3"))]
            assert_eq!(second.calls, 1);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"SELECT 'a' COLLATE inactive < 'b'".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_create_collation(
                    db,
                    c"inactive".as_ptr(),
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    None
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"SELECT 'a' COLLATE inactive < 'b'".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_ERROR
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    unsafe extern "C" fn compare_collation_probe(
        _: *mut libc::c_void,
        _: i32,
        _: *const libc::c_void,
        _: i32,
        _: *const libc::c_void,
    ) -> i32 {
        -1
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn rejected_collation_registration_keeps_caller_owner() {
        unsafe {
            let mut calls = 0_i32;
            assert_eq!(
                sqlite3_create_collation_v2(
                    ptr::null_mut(),
                    c"inactive".as_ptr(),
                    SQLITE_UTF8,
                    (&mut calls as *mut i32).cast(),
                    None,
                    Some(count_collation_destroy)
                ),
                SQLITE_MISUSE
            );
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            #[cfg(not(feature = "sqlite3"))]
            assert_eq!(
                sqlite3_create_collation_v2(
                    db,
                    ptr::null(),
                    SQLITE_UTF8,
                    (&mut calls as *mut i32).cast(),
                    None,
                    Some(count_collation_destroy)
                ),
                SQLITE_MISUSE
            );
            assert_eq!(calls, 0);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(calls, 0);
        }
    }

    #[test]
    fn null_collation_destructor_reentry_child() {
        if std::env::var_os("TURSO_COLLATION_REENTRY_CHILD").is_none() {
            return;
        }
        unsafe {
            for mode in 0..5 {
                let mut db = ptr::null_mut();
                assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
                let mut probe = CollationOwnerProbe {
                    db,
                    calls: 0,
                    reenter: true,
                    closing: mode != 3,
                };
                if mode == 4 {
                    probe.closing = false;
                    assert_eq!(
                        sqlite3_create_collation_v2(
                            db,
                            c"inactive".as_ptr(),
                            SQLITE_UTF8,
                            (&mut probe as *mut CollationOwnerProbe).cast(),
                            Some(compare_collation_probe),
                            Some(probe_collation_destroy)
                        ),
                        SQLITE_OK
                    );
                    let mut removed_owner = 0_i32;
                    assert_eq!(
                        sqlite3_create_collation_v2(
                            db,
                            c"inactive".as_ptr(),
                            SQLITE_UTF8,
                            (&mut removed_owner as *mut i32).cast(),
                            None,
                            Some(count_collation_destroy)
                        ),
                        SQLITE_OK
                    );
                    assert_eq!(probe.calls, 1);
                    assert_eq!(removed_owner, 0);
                    assert_eq!(sqlite3_close(db), SQLITE_OK);
                    assert_eq!(removed_owner, 1);
                    continue;
                }
                register_inactive(&mut probe, c"inactive");
                assert_eq!(probe.calls, 0);
                if mode == 2 {
                    let mut stmt = ptr::null_mut();
                    assert_eq!(
                        sqlite3_prepare_v2(
                            db,
                            c"SELECT 1".as_ptr(),
                            -1,
                            &mut stmt,
                            ptr::null_mut()
                        ),
                        SQLITE_OK
                    );
                    assert_eq!(sqlite3_close_v2(db), SQLITE_OK);
                    assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
                } else if mode == 3 {
                    assert_eq!(
                        sqlite3_create_collation(
                            db,
                            c"inactive".as_ptr(),
                            SQLITE_UTF8,
                            ptr::null_mut(),
                            Some(compare_collation_probe)
                        ),
                        SQLITE_OK
                    );
                    assert_eq!(sqlite3_close(db), SQLITE_OK);
                } else if mode == 1 {
                    assert_eq!(sqlite3_close_v2(db), SQLITE_OK);
                } else {
                    assert_eq!(sqlite3_close(db), SQLITE_OK);
                }
                #[cfg(not(feature = "sqlite3"))]
                assert_eq!(probe.calls, 1);
                #[cfg(feature = "sqlite3")]
                assert_eq!(probe.calls, if mode == 3 { 0 } else { 1 });
            }
        }
    }

    #[test]
    fn null_collation_destructor_reentry_is_bounded() {
        bounded_destructor_child(
            "tests::null_collation_destructor_reentry_child",
            "TURSO_COLLATION_REENTRY_CHILD",
        );
    }

    fn bounded_destructor_child(test: &str, flag: &str) {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture"])
            .env(flag, "1")
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("destructor reentry timed out: {test}");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    unsafe extern "C" fn legacy_reverse(
        context: *mut libc::c_void,
        left_len: i32,
        left: *const libc::c_void,
        right_len: i32,
        right: *const libc::c_void,
    ) -> i32 {
        *(context as *mut i32) += 1;
        let left = std::slice::from_raw_parts(left as *const u8, left_len as usize);
        let right = std::slice::from_raw_parts(right as *const u8, right_len as usize);
        match right.cmp(left) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }
    }

    #[test]
    fn exec_failure_updates_connection_error_state() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE example(id INTEGER PRIMARY KEY); INSERT INTO example VALUES(1)"
                        .as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"INSERT INTO example VALUES(1)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_CONSTRAINT
            );
            assert_eq!(sqlite3_errcode(db), SQLITE_CONSTRAINT);
            let message = std::ffi::CStr::from_ptr(sqlite3_errmsg(db))
                .to_str()
                .unwrap();
            assert!(message.contains("UNIQUE"), "{message}");
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"SELECT 1".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_errcode(db), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn mvcc_exec_conflict_sets_busy_error_on_connection() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path =
                std::ffi::CString::new(dir.path().join("conflict.db").to_str().unwrap()).unwrap();
            let mut first = ptr::null_mut();
            let mut second = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut first), SQLITE_OK);
            assert_eq!(sqlite3_exec(first, c"PRAGMA journal_mode=mvcc; CREATE TABLE example(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO example VALUES(1,0)".as_ptr(), None, ptr::null_mut(), ptr::null_mut()), SQLITE_OK);
            assert_eq!(sqlite3_open(path.as_ptr(), &mut second), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    first,
                    c"BEGIN CONCURRENT; UPDATE example SET value=1 WHERE id=1".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    second,
                    c"BEGIN CONCURRENT; UPDATE example SET value=2 WHERE id=1".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_BUSY
            );
            assert_eq!(sqlite3_errcode(second), SQLITE_BUSY);
            assert_eq!(
                sqlite3_exec(
                    first,
                    c"COMMIT".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(second), SQLITE_OK);
            assert_eq!(sqlite3_close(first), SQLITE_OK);
        }
    }

    #[test]
    fn aggregate_misuse_returns_error_without_aborting() {
        bounded_destructor_child(
            "tests::aggregate_misuse_child",
            "TURSO_AGGREGATE_MISUSE_CHILD",
        );
    }

    #[test]
    fn aggregate_misuse_child() {
        if std::env::var_os("TURSO_AGGREGATE_MISUSE_CHILD").is_none() {
            return;
        }
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_create_function(
                    db,
                    c"agg".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    None,
                    Some(signature_aggregate_step),
                    Some(signature_aggregate_final),
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t(x); INSERT INTO t VALUES(1),(2)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            for sql in [
                "SELECT max(agg(x)) FROM t",
                "SELECT sum(agg(x)) FROM t",
                "SELECT agg(agg(x)) FROM t",
                "SELECT x FROM t WHERE agg(x)>0",
                "SELECT x FROM t WHERE (SELECT agg(t.x))>0",
                "SELECT sum((SELECT agg(t.x))) FROM t",
            ] {
                let sql = std::ffi::CString::new(sql).unwrap();
                assert_eq!(
                    sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                    SQLITE_ERROR
                );
                let message = std::ffi::CStr::from_ptr(sqlite3_errmsg(db))
                    .to_str()
                    .unwrap();
                assert!(message.contains("misuse of aggregate"), "{message}");
                scalar_query(db, "SELECT agg(x) FROM t", 77);
            }
            scalar_query(db, "SELECT abs(agg(x)) FROM t", 77);
            scalar_query(db, "SELECT max(a) FROM (SELECT agg(x) AS a FROM t)", 77);
            scalar_query(db, "SELECT (SELECT agg(t.x)) FROM t", 77);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE e(x)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            scalar_query(
                db,
                "SELECT coalesce((SELECT agg(t.x) FROM e), -1) FROM t",
                -1,
            );
            scalar_query(db, "SELECT (SELECT agg(t.x) FROM t AS u) FROM t", 77);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn aggregate_step_error_stops_execution_and_finalizes_once() {
        unsafe {
            let mut db = ptr::null_mut();
            let mut calls = [0_i32; 3];
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_create_function(
                    db,
                    c"failing_aggregate".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    calls.as_mut_ptr().cast(),
                    None,
                    Some(failing_aggregate_step),
                    Some(failing_aggregate_final),
                ),
                SQLITE_OK
            );
            let mut statement = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"WITH input(x) AS (VALUES (1), (2)) SELECT failing_aggregate(x) FROM input"
                        .as_ptr(),
                    -1,
                    &mut statement,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(statement), SQLITE_ERROR);
            assert_eq!(sqlite3_errcode(db), SQLITE_ERROR);
            assert!(std::ffi::CStr::from_ptr(sqlite3_errmsg(db))
                .to_str()
                .unwrap()
                .contains("aggregate step failed"));
            assert_eq!(calls, [1, 1, 1]);
            sqlite3_finalize(statement);
            assert_eq!(calls, [1, 1, 1]);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    unsafe extern "C" fn failing_aggregate_step(
        context: *mut libc::c_void,
        _argc: i32,
        _argv: *mut *mut libc::c_void,
    ) {
        let calls = sqlite3_user_data(context).cast::<i32>();
        *calls += 1;
        let state =
            sqlite3_aggregate_context(context, std::mem::size_of::<i32>() as i32).cast::<i32>();
        if !state.is_null() {
            *state += 1;
        }
        sqlite3_result_error(context, c"aggregate step failed".as_ptr(), -1);
    }

    unsafe extern "C" fn failing_aggregate_final(context: *mut libc::c_void) {
        let calls = sqlite3_user_data(context).cast::<i32>();
        *calls.add(1) += 1;
        let state = sqlite3_aggregate_context(context, 0).cast::<i32>();
        *calls.add(2) = if state.is_null() { -1 } else { *state };
        sqlite3_result_int(context, 42);
    }

    #[test]
    fn aggregate_finalizes_when_another_expression_fails() {
        unsafe {
            let mut db = ptr::null_mut();
            let mut calls = [0_i32; 3];
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_create_function(
                    db,
                    c"cleanup_track".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    calls.as_mut_ptr().cast(),
                    None,
                    Some(cleanup_track_step),
                    Some(failing_aggregate_final)
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_create_function(
                    db,
                    c"cleanup_boom".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    Some(cleanup_boom),
                    None,
                    None
                ),
                SQLITE_OK
            );
            let mut statement = ptr::null_mut();
            assert_eq!(sqlite3_prepare_v2(db,
                c"WITH input(x) AS (VALUES(1),(2)) SELECT cleanup_track(x), sum(cleanup_boom(x)) FROM input".as_ptr(),
                -1, &mut statement, ptr::null_mut()), SQLITE_OK);
            assert_eq!(sqlite3_step(statement), SQLITE_ERROR);
            assert!(std::ffi::CStr::from_ptr(sqlite3_errmsg(db))
                .to_str()
                .unwrap()
                .contains("separate scalar failed"));
            sqlite3_finalize(statement);
            assert_eq!(calls, [1, 1, 1]);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(calls, [1, 1, 1]);
        }
    }

    #[test]
    fn aggregate_finalizes_once_after_success_and_reset() {
        unsafe {
            let mut db = ptr::null_mut();
            let mut calls = [0_i32; 3];
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_create_function(
                    db,
                    c"cleanup_success".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    calls.as_mut_ptr().cast(),
                    None,
                    Some(cleanup_track_step),
                    Some(failing_aggregate_final)
                ),
                SQLITE_OK
            );
            let mut statement = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"WITH input(x) AS (VALUES(1),(2)) SELECT cleanup_success(x) FROM input"
                        .as_ptr(),
                    -1,
                    &mut statement,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            for run in 1..=2 {
                assert_eq!(sqlite3_step(statement), SQLITE_ROW);
                assert_eq!(sqlite3_column_int(statement, 0), 42);
                assert_eq!(sqlite3_step(statement), SQLITE_DONE);
                assert_eq!(calls, [run * 2, run, 2]);
                assert_eq!(sqlite3_reset(statement), SQLITE_OK);
                assert_eq!(calls, [run * 2, run, 2]);
            }
            assert_eq!(sqlite3_finalize(statement), SQLITE_OK);
            assert_eq!(calls, [4, 2, 2]);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn aggregate_finalizes_after_interruption_and_can_be_reused() {
        unsafe {
            let mut db = ptr::null_mut();
            let mut calls = [0_i32; 3];
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_create_function(
                    db,
                    c"cleanup_interrupt".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    calls.as_mut_ptr().cast(),
                    None,
                    Some(cleanup_interrupt_step),
                    Some(failing_aggregate_final)
                ),
                SQLITE_OK
            );
            let mut statement = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"WITH RECURSIVE input(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM input WHERE x<10000) SELECT cleanup_interrupt(x) FROM input"
                        .as_ptr(),
                    -1,
                    &mut statement,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            for run in 1..=2 {
                let previous_steps = calls[0];
                assert_eq!(sqlite3_step(statement), SQLITE_INTERRUPT);
                sqlite3_reset(statement);
                assert!(calls[0] > previous_steps);
                assert_eq!(calls[1], run);
                assert_eq!(calls[2], calls[0] - previous_steps);
            }
            let after_reset = calls;
            sqlite3_finalize(statement);
            assert_eq!(calls, after_reset);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    unsafe extern "C" fn cleanup_interrupt_step(
        context: *mut libc::c_void,
        argc: i32,
        argv: *mut *mut libc::c_void,
    ) {
        cleanup_track_step(context, argc, argv);
        sqlite3_interrupt(sqlite3_context_db_handle(context).cast());
    }

    unsafe extern "C" fn cleanup_track_step(
        context: *mut libc::c_void,
        _argc: i32,
        _argv: *mut *mut libc::c_void,
    ) {
        let calls = sqlite3_user_data(context).cast::<i32>();
        *calls += 1;
        let state =
            sqlite3_aggregate_context(context, std::mem::size_of::<i32>() as i32).cast::<i32>();
        if !state.is_null() {
            *state += 1;
        }
    }

    unsafe extern "C" fn cleanup_boom(
        context: *mut libc::c_void,
        _argc: i32,
        _argv: *mut *mut libc::c_void,
    ) {
        sqlite3_result_error(context, c"separate scalar failed".as_ptr(), -1);
    }

    #[test]
    fn test_libversion() {
        unsafe {
            let version = sqlite3_libversion();
            assert!(!version.is_null());
            #[cfg(not(feature = "sqlite3"))]
            assert_eq!(std::ffi::CStr::from_ptr(version), c"3.50.4");
        }
    }

    #[test]
    fn test_libversion_number() {
        unsafe {
            let version_num = sqlite3_libversion_number();
            #[cfg(feature = "sqlite3")]
            assert!(version_num > 0);
            #[cfg(not(feature = "sqlite3"))]
            assert_eq!(version_num, 3050004);
        }
    }

    #[test]
    fn test_open_not_found() {
        unsafe {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("absent/local.db");
            let filename = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            for use_v2 in [false, true] {
                for close_v2 in [false, true] {
                    let mut db = ptr::dangling_mut();
                    let rc = if use_v2 {
                        sqlite3_open_v2(filename.as_ptr(), &mut db, 6, ptr::null())
                    } else {
                        sqlite3_open(filename.as_ptr(), &mut db)
                    };
                    assert_eq!(rc, SQLITE_CANTOPEN);
                    assert!(!db.is_null(), "failed open must return an error handle");
                    assert_ne!(
                        db,
                        ptr::dangling_mut(),
                        "open must replace the output pointer"
                    );
                    assert_eq!(sqlite3_errcode(db), SQLITE_CANTOPEN);
                    assert_eq!(sqlite3_extended_errcode(db), SQLITE_CANTOPEN);
                    let message = std::ffi::CStr::from_ptr(sqlite3_errmsg(db))
                        .to_str()
                        .unwrap();
                    assert!(!message.is_empty());
                    assert_ne!(message, "out of memory");
                    assert_eq!(sqlite3_errcode(db), SQLITE_CANTOPEN);
                    assert_eq!(
                        if close_v2 {
                            sqlite3_close_v2(db)
                        } else {
                            sqlite3_close(db)
                        },
                        SQLITE_OK
                    );
                    assert!(!path.exists());
                    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
                }
            }
        }
    }

    #[test]
    fn test_null_error_introspection() {
        unsafe {
            assert_eq!(sqlite3_errcode(ptr::null_mut()), 7);
            assert_eq!(sqlite3_extended_errcode(ptr::null_mut()), 7);
            assert_eq!(
                std::ffi::CStr::from_ptr(sqlite3_errmsg(ptr::null_mut())),
                c"out of memory"
            );
        }
    }

    #[cfg(not(feature = "sqlite3"))]
    #[test]
    fn test_failed_open_operations() {
        unsafe {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("absent/local.db");
            let filename = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(filename.as_ptr(), &mut db), SQLITE_CANTOPEN);
            assert!(!db.is_null());
            let message = std::ffi::CStr::from_ptr(sqlite3_errmsg(db)).to_owned();
            let mut stmt = ptr::dangling_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT 1".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_MISUSE
            );
            assert!(stmt.is_null());
            for sql in [c"", c"SELECT 1"] {
                assert_eq!(
                    sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                    SQLITE_MISUSE
                );
            }
            assert_eq!(sqlite3_db_readonly(db, c"main".as_ptr()), -1);
            assert_eq!(sqlite3_changes64(db), 0);
            assert_eq!(sqlite3_total_changes64(db), 0);
            assert_eq!(sqlite3_last_insert_rowid(db), 0);
            assert_eq!(sqlite3_get_autocommit(db), 1);
            assert_eq!(
                sqlite3_busy_handler(db, None, ptr::null_mut()),
                SQLITE_MISUSE
            );
            assert_eq!(sqlite3_busy_timeout(db, 1), SQLITE_MISUSE);
            sqlite3_progress_handler(db, 1, None, ptr::null_mut());
            sqlite3_interrupt(db);
            assert_eq!(
                sqlite3_create_collation(db, c"unused".as_ptr(), 1, ptr::null_mut(), None),
                SQLITE_MISUSE
            );
            assert_eq!(
                sqlite3_create_function_v2(
                    db,
                    c"unused".as_ptr(),
                    0,
                    1,
                    ptr::null_mut(),
                    None,
                    None,
                    None,
                    None
                ),
                SQLITE_MISUSE
            );
            assert_eq!(
                sqlite3_table_column_metadata(
                    db,
                    c"main".as_ptr(),
                    c"unused".as_ptr(),
                    ptr::null(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_MISUSE
            );
            let mut blob = ptr::dangling_mut();
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"unused".as_ptr(),
                    c"unused".as_ptr(),
                    1,
                    0,
                    &mut blob
                ),
                SQLITE_MISUSE
            );
            assert!(blob.is_null());
            let (rc, destroyed) = scalar_register(db, c"failed_registration", 0, 1);
            assert_eq!(rc, SQLITE_MISUSE);
            assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(sqlite3_wal_checkpoint(db, ptr::null()), SQLITE_MISUSE);
            let mut frame_count = 0;
            assert_eq!(libsql_wal_frame_count(db, &mut frame_count), SQLITE_MISUSE);
            assert_eq!(
                libsql_wal_get_frame(db, 1, ptr::null_mut(), 0),
                SQLITE_MISUSE
            );
            assert_eq!(libsql_wal_disable_checkpoint(db), SQLITE_MISUSE);
            assert_eq!(
                libsql_wal_insert_frame(db, 1, ptr::null(), 0, ptr::null_mut()),
                SQLITE_MISUSE
            );
            assert_eq!(sqlite3_errcode(db), SQLITE_CANTOPEN);
            assert_eq!(sqlite3_extended_errcode(db), SQLITE_CANTOPEN);
            assert_eq!(
                std::ffi::CStr::from_ptr(sqlite3_errmsg(db)),
                message.as_c_str()
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    #[cfg(not(feature = "sqlite3"))]
    #[test]
    fn test_open_invalid_input_clears_output() {
        unsafe {
            for use_v2 in [false, true] {
                let mut db = ptr::dangling_mut();
                let rc = if use_v2 {
                    sqlite3_open_v2(ptr::null(), &mut db, 6, ptr::null())
                } else {
                    sqlite3_open(ptr::null(), &mut db)
                };
                assert_eq!(rc, SQLITE_MISUSE);
                assert!(db.is_null());
            }
        }
    }

    #[test]
    fn test_open_existing() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(
                sqlite3_open(c"../../testing/system/testing_clone.db".as_ptr(), &mut db),
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

    // Per SQLite spec (https://www.sqlite.org/c3ref/prepare.html):
    //   "If nByte is positive, it is the number of bytes read from zSql."
    // Consumers like rusqlite/libsqlite3-sys pass non-NUL-terminated &str slices with
    // explicit positive nByte. The three tests below cover the contract:
    //   - positive nByte respects the bound (no over-read past the slice)
    //   - negative nByte keeps the read-until-NUL behavior
    //   - NUL within the bound truncates (per spec)
    // Each test actively sets up the failure mode (garbage bytes after the bound, or
    // a "should-not-be-seen" suffix after an embedded NUL) so an over-reading impl
    // would produce a parser error rather than silently passing.

    #[test]
    fn test_prepare_v2_n_byte_positive_no_nul() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            // 8 bytes of valid SQL followed by 4 bytes of garbage. n_byte = 8.
            // An over-reading impl would walk past the bound, hit \xff, and error.
            let sql_and_garbage: &[u8] = b"SELECT 1\xff\xff\xff\xff";
            let mut stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(
                db,
                sql_and_garbage.as_ptr() as *const libc::c_char,
                8,
                &mut stmt,
                ptr::null_mut(),
            );
            assert_eq!(
                rc, SQLITE_OK,
                "prepare_v2 must respect positive n_byte (no over-read)"
            );
            assert!(!stmt.is_null());
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_prepare_v2_n_byte_negative_uses_nul() {
        // Regression: n_byte = -1 path must still read until NUL terminator.
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(db, c"SELECT 1".as_ptr(), -1, &mut stmt, ptr::null_mut());
            assert_eq!(rc, SQLITE_OK);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_prepare_v2_n_byte_truncates_at_embedded_nul() {
        // Spec: "the SQL text is read up to the first zero terminator or the nByte-th
        // byte, whichever comes first." Embedded NUL within the bound must truncate.
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            // NUL at offset 8; bound is 20 (claims more). Reader must stop at NUL.
            // If under-truncating, parser sees "GARBAGE_AFTER_NUL" and errors out.
            let sql: &[u8] = b"SELECT 1\x00GARBAGE_AFTER_NUL";
            let mut stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(
                db,
                sql.as_ptr() as *const libc::c_char,
                20,
                &mut stmt,
                ptr::null_mut(),
            );
            assert_eq!(
                rc, SQLITE_OK,
                "embedded NUL within bound must truncate, not over-read"
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
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
    fn test_sqlite3_bind_int_range_checks() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT ?1".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );

            assert_eq!(sqlite3_bind_int(stmt, 0, 1), SQLITE_RANGE);
            assert_eq!(sqlite3_bind_int(stmt, 2, 1), SQLITE_RANGE);
            assert_eq!(sqlite3_bind_int(stmt, 1, 7), SQLITE_OK);

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
    fn test_sqlite3_bind_parameter_count_sparse_positional() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT ?3".as_ptr(), -1, &mut stmt, ptr::null_mut(),),
                SQLITE_OK
            );

            assert_eq!(sqlite3_bind_parameter_count(stmt), 3);
            assert!(sqlite3_bind_parameter_name(stmt, 1).is_null());
            assert!(sqlite3_bind_parameter_name(stmt, 2).is_null());

            let name3 = sqlite3_bind_parameter_name(stmt, 3);
            assert!(!name3.is_null());
            let name3 = std::ffi::CStr::from_ptr(name3).to_str().unwrap();
            assert_eq!(name3, "?3");

            assert_eq!(sqlite3_bind_int(stmt, 1, 1), SQLITE_OK);
            assert_eq!(sqlite3_bind_int(stmt, 3, 9), SQLITE_OK);

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 9);

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

            for invalid_idx in [-1, 2, 5, i32::MIN, i32::MAX] {
                assert!(sqlite3_column_name(stmt, invalid_idx).is_null());
                assert!(sqlite3_column_table_name(stmt, invalid_idx).is_null());
                assert!(sqlite3_column_decltype(stmt, invalid_idx).is_null());
            }

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_ctas_column_count_is_zero() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE dst AS SELECT 1 AS a, 'x' AS b".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_column_count(stmt), 0);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
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
                sqlite3_open(c"../../testing/system/testing.db".as_ptr(), &mut db),
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

            let col2_slice = std::slice::from_raw_parts(col2_ptr.cast::<u8>(), col2_len as usize);
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

    #[cfg(not(feature = "sqlite3"))]
    #[test]
    fn test_sqlite3_bind_blob_negative_len_is_misuse() {
        unsafe {
            let temp_file = tempfile::NamedTempFile::with_suffix(".db").unwrap();
            let path = std::ffi::CString::new(temp_file.path().to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"CREATE TABLE test_bind_blob_nlen (data BLOB)".as_ptr(),
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
                    c"INSERT INTO test_bind_blob_nlen (data) VALUES (?)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            let data = b"abc\0def";
            // A NULL value ignores n. A pointer with n < 0 is SQLITE_MISUSE.
            assert_eq!(sqlite3_bind_blob(stmt, 1, ptr::null(), -1, None), SQLITE_OK);
            assert_eq!(
                sqlite3_bind_blob(stmt, 1, data.as_ptr() as *const _, -1, None),
                SQLITE_MISUSE
            );
            assert_eq!(
                sqlite3_bind_blob(stmt, 1, data.as_ptr() as *const _, 3, None),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT data FROM test_bind_blob_nlen".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let col = sqlite3_column_blob(stmt, 0);
            let n = sqlite3_column_bytes(stmt, 0);
            let got = std::slice::from_raw_parts(col as *const u8, n as usize);
            assert_eq!(got, b"abc");

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
            assert_eq!(sqlite3_errcode(db), SQLITE_ABORT);
            assert_eq!(
                std::ffi::CStr::from_ptr(sqlite3_errmsg(db)),
                c"query aborted"
            );

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
            assert_eq!(sqlite3_errcode(db), SQLITE_ERROR);
            assert!(!err_msg.is_null());
            assert_eq!(
                std::ffi::CStr::from_ptr(err_msg),
                std::ffi::CStr::from_ptr(sqlite3_errmsg(db))
            );
            sqlite3_free(err_msg as *mut std::ffi::c_void);

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

            // A successful exec clears the error left by the failed one.
            let mut err_msg = ptr::null_mut();
            let rc = sqlite3_exec(
                db,
                c"INSERT INTO test VALUES(1)".as_ptr(),
                None,
                ptr::null_mut(),
                &mut err_msg,
            );
            assert_eq!(rc, SQLITE_OK);
            assert!(err_msg.is_null());
            assert_eq!(sqlite3_errcode(db), SQLITE_OK);
            assert_eq!(
                std::ffi::CStr::from_ptr(sqlite3_errmsg(db)),
                c"not an error"
            );

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

            // Semicolons in comments and in a trigger body do not end a statement.
            let rc = sqlite3_exec(
                db,
                c"CREATE TABLE source(value); CREATE TABLE log(value);\
                  -- comment with ; semicolon\n\
                  CREATE TRIGGER source_log AFTER INSERT ON source BEGIN\
                  \x20INSERT INTO log VALUES (new.value); INSERT INTO log VALUES (new.value + 1); END;\
                  /* block ; comment */ INSERT INTO source VALUES (42); /* ; */"
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
                    c"SELECT group_concat(value) FROM log".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(
                std::ffi::CStr::from_ptr(sqlite3_column_text(stmt, 0) as *const libc::c_char),
                c"42,43"
            );
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
                assert!(count > 1500, "count: {count}");
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

            // Initially no changes
            assert_eq!(sqlite3_changes(db), 0);
            assert_eq!(sqlite3_changes64(db), 0);
            assert_eq!(sqlite3_total_changes(db), 0);
            assert_eq!(sqlite3_total_changes64(db), 0);

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
            assert_eq!(sqlite3_total_changes(db), 0);
            assert_eq!(sqlite3_total_changes64(db), 0);

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
            assert_eq!(sqlite3_total_changes(db), 1);
            assert_eq!(sqlite3_total_changes64(db), 1);

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

            // Statement changes is 3, cumulative total_changes is 1 + 3 = 4
            assert_eq!(sqlite3_changes(db), 3);
            assert_eq!(sqlite3_changes64(db), 3);
            assert_eq!(sqlite3_total_changes(db), 4);
            assert_eq!(sqlite3_total_changes64(db), 4);

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

    struct ProgressContext {
        calls: std::sync::atomic::AtomicI32,
        interrupt_after: i32,
    }

    unsafe extern "C" fn progress_handler_interrupt_after(data: *mut libc::c_void) -> i32 {
        let ctx = &*(data as *const ProgressContext);
        let current = ctx.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if current >= ctx.interrupt_after {
            1
        } else {
            0
        }
    }

    #[test]
    fn test_sqlite3_progress_handler_interrupts_statement() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE numbers(v INTEGER); INSERT INTO numbers(v) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            let ctx = ProgressContext {
                calls: std::sync::atomic::AtomicI32::new(0),
                interrupt_after: 8,
            };
            sqlite3_progress_handler(
                db,
                1,
                Some(progress_handler_interrupt_after),
                &ctx as *const ProgressContext as *mut libc::c_void,
            );

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT count(*) FROM numbers AS a, numbers AS b, numbers AS c, numbers AS d, numbers AS e, numbers AS f, numbers AS g".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            assert_eq!(sqlite3_step(stmt), SQLITE_INTERRUPT);
            assert_eq!(sqlite3_errcode(db), SQLITE_INTERRUPT);
            assert_eq!(std::ffi::CStr::from_ptr(sqlite3_errmsg(db)), c"interrupted");
            assert!(
                ctx.calls.load(std::sync::atomic::Ordering::SeqCst) >= ctx.interrupt_after,
                "progress handler did not fire enough times"
            );

            sqlite3_progress_handler(db, 0, None, ptr::null_mut());
            let finalize_rc = sqlite3_finalize(stmt);
            assert!(
                matches!(finalize_rc, SQLITE_OK | SQLITE_INTERRUPT),
                "unexpected finalize rc: {finalize_rc}"
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_interrupt_without_active_statement_is_ignored() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT 1".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );

            sqlite3_interrupt(db);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_interrupt_active_statement() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE numbers(v INTEGER); INSERT INTO numbers(v) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT a.v FROM numbers AS a, numbers AS b, numbers AS c, numbers AS d, numbers AS e, numbers AS f, numbers AS g, numbers AS h".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            let db_addr = db as usize;
            let interrupter = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                sqlite3_interrupt(db_addr as *mut sqlite3);
            });

            let rc = loop {
                let rc = sqlite3_step(stmt);
                if rc != SQLITE_ROW {
                    break rc;
                }
            };
            interrupter.join().unwrap();
            assert_eq!(rc, SQLITE_INTERRUPT, "expected SQLITE_INTERRUPT, got {rc}");

            let finalize_rc = sqlite3_finalize(stmt);
            assert!(
                matches!(finalize_rc, SQLITE_OK | SQLITE_INTERRUPT),
                "unexpected finalize rc: {finalize_rc}"
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_get_table() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            // Create and populate a table
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t1(id INTEGER, name TEXT)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"INSERT INTO t1 VALUES(1, 'alice')".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"INSERT INTO t1 VALUES(2, 'bob')".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            // Query via sqlite3_get_table
            let mut result: *mut *mut libc::c_char = ptr::null_mut();
            let mut n_row: libc::c_int = 0;
            let mut n_col: libc::c_int = 0;
            let mut err_msg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_get_table(
                    db,
                    c"SELECT id, name FROM t1 ORDER BY id".as_ptr(),
                    &mut result,
                    &mut n_row,
                    &mut n_col,
                    &mut err_msg,
                ),
                SQLITE_OK
            );

            assert_eq!(n_row, 2);
            assert_eq!(n_col, 2);

            // result layout: [col0_name, col1_name, row0_val0, row0_val1, row1_val0, row1_val1]
            let col0 = std::ffi::CStr::from_ptr(*result.add(0));
            let col1 = std::ffi::CStr::from_ptr(*result.add(1));
            assert_eq!(col0.to_str().unwrap(), "id");
            assert_eq!(col1.to_str().unwrap(), "name");

            let r0v0 = std::ffi::CStr::from_ptr(*result.add(2));
            let r0v1 = std::ffi::CStr::from_ptr(*result.add(3));
            assert_eq!(r0v0.to_str().unwrap(), "1");
            assert_eq!(r0v1.to_str().unwrap(), "alice");

            let r1v0 = std::ffi::CStr::from_ptr(*result.add(4));
            let r1v1 = std::ffi::CStr::from_ptr(*result.add(5));
            assert_eq!(r1v0.to_str().unwrap(), "2");
            assert_eq!(r1v1.to_str().unwrap(), "bob");

            sqlite3_free_table(result);

            // Rows with different column counts make the row callback fail;
            // sqlite3_get_table reports the callback's SQLITE_ERROR, not SQLITE_ABORT.
            assert_eq!(
                sqlite3_get_table(
                    db,
                    c"SELECT 1; SELECT 1, 2".as_ptr(),
                    &mut result,
                    &mut n_row,
                    &mut n_col,
                    &mut err_msg,
                ),
                SQLITE_ERROR
            );
            assert!(result.is_null());
            assert_eq!(n_row, 0);
            assert_eq!(n_col, 0);
            assert_eq!(
                std::ffi::CStr::from_ptr(err_msg).to_str().unwrap(),
                "sqlite3_get_table() called with two or more incompatible queries"
            );
            sqlite3_free(err_msg as *mut _);
            assert_eq!(sqlite3_errcode(db), SQLITE_ERROR);
            assert_eq!(
                std::ffi::CStr::from_ptr(sqlite3_errmsg(db))
                    .to_str()
                    .unwrap(),
                "SQL logic error"
            );

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_get_table_empty_result() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t1(id INTEGER)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            let mut result: *mut *mut libc::c_char = ptr::null_mut();
            let mut n_row: libc::c_int = 0;
            let mut n_col: libc::c_int = 0;
            assert_eq!(
                sqlite3_get_table(
                    db,
                    c"SELECT id FROM t1".as_ptr(),
                    &mut result,
                    &mut n_row,
                    &mut n_col,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            assert_eq!(n_row, 0);
            assert_eq!(n_col, 0);

            sqlite3_free_table(result);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_free_table_null() {
        unsafe {
            // Passing null should not crash
            sqlite3_free_table(ptr::null_mut());
        }
    }

    /// Regression test: sqlite3_value_blob returns a dangling pointer (use-after-free).
    ///
    /// The internal `ExtValue::to_blob()` returns `Option<Vec<u8>>`, so
    /// `sqlite3_value_blob` calls `.as_ptr()` on a temporary Vec that is
    /// immediately dropped, leaving the returned pointer dangling.
    ///
    /// This test creates a scalar function that receives a BLOB argument,
    /// reads it via `sqlite3_value_blob` + `sqlite3_value_bytes`, and
    /// verifies the data is correct. On the buggy version, the blob data
    /// is corrupted (reads freed memory).
    #[test]
    fn test_sqlite3_value_blob_use_after_free() {
        /// Scalar function callback: verifies that the first argument is a BLOB
        /// with the expected content [0xDE, 0xAD, 0xBE, 0xEF].
        /// Returns 1 if the blob matches, 0 otherwise.
        unsafe extern "C" fn check_blob_fn(
            ctx: *mut libc::c_void,
            argc: i32,
            argv: *mut *mut libc::c_void,
        ) {
            assert_eq!(argc, 1);
            let value = *argv.add(0);
            assert!(!value.is_null());

            let vtype = sqlite3_value_type(value);
            if vtype != SQLITE_BLOB {
                sqlite3_result_int64(ctx, -1); // wrong type
                return;
            }

            let blob_ptr = sqlite3_value_blob(value);
            let blob_len = sqlite3_value_bytes(value);

            if blob_ptr.is_null() || blob_len != 4 {
                sqlite3_result_int64(ctx, -2); // null ptr or wrong length
                return;
            }

            // Read the blob data
            let data = std::slice::from_raw_parts(blob_ptr as *const u8, blob_len as usize);
            let expected: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
            if data == expected {
                sqlite3_result_int64(ctx, 1); // match
            } else {
                // Print actual bytes for debugging
                eprintln!("blob mismatch: expected {expected:?}, got {data:?}");
                sqlite3_result_int64(ctx, 0); // corrupted data
            }
        }

        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test_value_blob.db");
            let path_cstr = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path_cstr.as_ptr(), &mut db), SQLITE_OK);

            // Register the scalar function
            assert_eq!(
                sqlite3_create_function_v2(
                    db,
                    c"check_blob".as_ptr(),
                    1,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    Some(check_blob_fn),
                    None,
                    None,
                    None,
                ),
                SQLITE_OK,
                "create function failed"
            );

            // Create table and insert a row with a known BLOB
            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK
            );

            // Insert using prepared statement with bound BLOB
            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO t (id, data) VALUES (1, ?1)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            let blob_data: [u8; 4] = [0xDE, 0xAD, 0xBE, 0xEF];
            assert_eq!(
                sqlite3_bind_blob(stmt, 1, blob_data.as_ptr() as *const libc::c_void, 4, None,),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            // Call check_blob(data) via SELECT — this triggers sqlite3_value_blob
            // on the ExtValue passed to the scalar function
            let mut stmt2: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT check_blob(data) FROM t WHERE id = 1".as_ptr(),
                    -1,
                    &mut stmt2,
                    ptr::null_mut(),
                ),
                SQLITE_OK,
                "prepare SELECT with check_blob"
            );
            let rc = sqlite3_step(stmt2);
            assert_eq!(rc, SQLITE_ROW, "expected SQLITE_ROW, got {rc}");

            let result = sqlite3_column_int(stmt2, 0);
            assert_eq!(
                result, 1,
                "check_blob returned {result}: blob data was corrupted (use-after-free in sqlite3_value_blob)"
            );

            assert_eq!(sqlite3_finalize(stmt2), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_prepare_v3() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.db");
            let path_cstr = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path_cstr.as_ptr(), &mut db), SQLITE_OK);

            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK
            );

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v3(
                    db,
                    c"INSERT INTO t1 VALUES (?1, ?2)".as_ptr(),
                    -1,
                    SQLITE_PREPARE_PERSISTENT,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert!(!stmt.is_null());

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_db_handle() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.db");
            let path_cstr = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path_cstr.as_ptr(), &mut db), SQLITE_OK);

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, c"SELECT 1".as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert!(!stmt.is_null());

            let returned_db = sqlite3_db_handle(stmt);
            assert_eq!(returned_db, db);

            let null_db = sqlite3_db_handle(ptr::null_mut());
            assert!(null_db.is_null());

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    /// Test: sqlite3_initialize must not panic when a global tracing subscriber
    /// is already installed.
    #[test]
    fn test_sqlite3_initialize_no_panic_with_existing_subscriber() {
        let _ = tracing_subscriber::fmt::try_init();

        unsafe {
            let rc = sqlite3_initialize();
            assert_eq!(rc, SQLITE_OK);

            let rc2 = sqlite3_initialize();
            assert_eq!(rc2, SQLITE_OK);
        }
    }

    /// Test: URI filename with mode=memory should create an in-memory database.
    #[test]
    fn test_uri_mode_memory() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("memtest.db");
            let uri = format!("file:{}?mode=memory&cache=shared", db_path.display());
            let uri_cstr = std::ffi::CString::new(uri).unwrap();

            let mut db: *mut sqlite3 = ptr::null_mut();
            let rc = sqlite3_open_v2(
                uri_cstr.as_ptr(),
                &mut db,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                ptr::null(),
            );
            assert_eq!(rc, SQLITE_OK, "open_v2 with URI mode=memory failed rc={rc}");
            assert!(!db.is_null());

            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg
                ),
                SQLITE_OK,
                "CREATE TABLE in memory DB failed"
            );
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"INSERT INTO t1 VALUES (1, 'hello')".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg
                ),
                SQLITE_OK,
                "INSERT in memory DB failed"
            );

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT val FROM t1 WHERE id = 1".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let text = sqlite3_column_text(stmt, 0);
            assert!(!text.is_null());
            assert_eq!(std::ffi::CStr::from_ptr(text).to_str().unwrap(), "hello");
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);

            assert!(
                !db_path.exists(),
                "URI mode=memory should NOT create a file on disk"
            );
        }
    }

    /// Test: file::memory: URI should behave like :memory:
    #[test]
    fn test_uri_file_colon_memory() {
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            let rc = sqlite3_open_v2(
                c"file::memory:".as_ptr(),
                &mut db,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                ptr::null(),
            );
            assert_eq!(rc, SQLITE_OK, "open_v2 with file::memory: failed rc={rc}");

            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t1 (id INTEGER PRIMARY KEY)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"INSERT INTO t1 VALUES (42)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);

            let mut db2: *mut sqlite3 = ptr::null_mut();
            let rc = sqlite3_open_v2(
                c"file::memory:".as_ptr(),
                &mut db2,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                ptr::null(),
            );
            assert_eq!(rc, SQLITE_OK);

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(
                db2,
                c"SELECT id FROM t1".as_ptr(),
                -1,
                &mut stmt,
                ptr::null_mut(),
            );
            assert_eq!(
                rc, SQLITE_ERROR,
                "file::memory: data persisted — not truly in-memory (rc={rc})"
            );
            assert_eq!(sqlite3_close(db2), SQLITE_OK);
        }
    }

    #[test]
    fn test_uri_memory_access_modes() {
        unsafe {
            for flags in [1, 2, 6] {
                for (name, readonly) in [
                    (c":memory:", flags == 1),
                    (c"file://", flags == 1),
                    (c"file://localhost", flags == 1),
                    (c"file::memory:?mode=ro", true),
                    (c"file:private-access?mode=memory", false),
                    (c"file:private-access?mode=ro&mode=memory", false),
                ] {
                    let mut db = ptr::null_mut();
                    assert_eq!(
                        sqlite3_open_v2(
                            name.as_ptr(),
                            &mut db,
                            flags | SQLITE_OPEN_URI,
                            ptr::null()
                        ),
                        SQLITE_OK
                    );
                    let expected = if readonly { 8 } else { SQLITE_OK };
                    assert_eq!(
                        sqlite3_exec(
                            db,
                            c"CREATE TABLE t(x)".as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut()
                        ),
                        expected,
                        "{name:?} flags={flags}"
                    );
                    assert_eq!(sqlite3_close(db), SQLITE_OK);
                }
            }
        }
    }

    #[test]
    fn test_uri_empty_filename_stays_private() {
        unsafe {
            let name = c"file:?mode=memory&cache=shared";
            let mut first = ptr::null_mut();
            let mut second = ptr::null_mut();
            for db in [&mut first, &mut second] {
                assert_eq!(
                    sqlite3_open_v2(name.as_ptr(), db, 6 | SQLITE_OPEN_URI, ptr::null()),
                    SQLITE_OK
                );
            }
            assert_eq!(
                sqlite3_exec(
                    first,
                    c"CREATE TABLE private_table(x)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    second,
                    c"SELECT * FROM private_table".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_ERROR
            );
            assert_eq!(sqlite3_close(first), SQLITE_OK);
            assert_eq!(sqlite3_close(second), SQLITE_OK);
        }
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn test_uri_legacy_reader_defers_writer_activation_until_first_write() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("legacy.db");
            let name = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut seed = ptr::null_mut();
            assert_eq!(sqlite3_open(name.as_ptr(), &mut seed), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    seed,
                    c"CREATE TABLE t(x); INSERT INTO t VALUES(1)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(seed), SQLITE_OK);
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[18] = 1;
            bytes[19] = 1;
            std::fs::write(&path, bytes).unwrap();
            std::fs::remove_file(format!("{}-wal", path.display())).unwrap();
            let uri = std::ffi::CString::new(format!("file:{}?mode=ro", path.display())).unwrap();
            let mut reader = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(uri.as_ptr(), &mut reader, 6 | SQLITE_OPEN_URI, ptr::null()),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    reader,
                    c"BEGIN; SELECT * FROM t".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            let mut writer = ptr::null_mut();
            assert_eq!(sqlite3_open(name.as_ptr(), &mut writer), SQLITE_OK);
            assert!(!writer.is_null());
            assert_eq!(
                sqlite3_exec(
                    writer,
                    c"INSERT INTO t VALUES(2)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_BUSY
            );
            assert_eq!(sqlite3_errcode(writer), SQLITE_BUSY);
            assert_eq!(
                sqlite3_exec(
                    reader,
                    c"COMMIT".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    writer,
                    c"INSERT INTO t VALUES(2)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    reader,
                    c"SELECT * FROM t".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(reader), SQLITE_OK);
            assert_eq!(sqlite3_close(writer), SQLITE_OK);
        }
    }

    #[test]
    fn test_uri_access_permissions() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("permissions.db");
            let name = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut seed = ptr::null_mut();
            assert_eq!(sqlite3_open(name.as_ptr(), &mut seed), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    seed,
                    c"CREATE TABLE t(x)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(seed), SQLITE_OK);
            for flags in [1, 2, 6] {
                for (query, mode) in [
                    ("mode=ro", 1),
                    ("mode=rw", 2),
                    ("mode=rwc", 6),
                    ("m%6fde=r%6f", 1),
                    ("mode=ro#&mode=rwc", 1),
                ] {
                    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
                    let uri =
                        std::ffi::CString::new(format!("file:{}?{query}", path.display())).unwrap();
                    let mut db = ptr::null_mut();
                    let expected = if mode > flags { 3 } else { SQLITE_OK };
                    assert_eq!(
                        sqlite3_open_v2(
                            uri.as_ptr(),
                            &mut db,
                            flags | SQLITE_OPEN_URI,
                            ptr::null()
                        ),
                        expected,
                        "flags={flags} {query}"
                    );
                    assert!(!db.is_null());
                    if expected == SQLITE_OK {
                        assert_eq!(
                            sqlite3_db_readonly(db, c"main".as_ptr()),
                            i32::from(mode == 1)
                        );
                    } else {
                        assert_eq!(sqlite3_errcode(db), expected);
                    }
                    assert_eq!(sqlite3_close(db), SQLITE_OK);
                }
            }
            for (query, expected) in [
                ("mode=bad", 1),
                ("mode=rw&mode=rwc", 3),
                ("mode=ro&mode=rw", 3),
                ("cache=bad", 1),
            ] {
                let uri =
                    std::ffi::CString::new(format!("file:{}?{query}", path.display())).unwrap();
                let mut db = ptr::null_mut();
                assert_eq!(
                    sqlite3_open_v2(uri.as_ptr(), &mut db, 6 | SQLITE_OPEN_URI, ptr::null()),
                    expected,
                    "{query}"
                );
                assert!(!db.is_null());
                assert_eq!(sqlite3_errcode(db), expected);
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
            for flags in [0, 3, 4, 5, 7] {
                let mut db = ptr::null_mut();
                assert_eq!(
                    sqlite3_open_v2(name.as_ptr(), &mut db, flags, ptr::null()),
                    SQLITE_MISUSE
                );
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
            for mode in ["ro", "rw", "rwc"] {
                let missing = dir.path().join(format!("missing-{mode}.db"));
                let uri = std::ffi::CString::new(format!("file:{}?mode={mode}", missing.display()))
                    .unwrap();
                let mut db = ptr::null_mut();
                let expected = if mode == "rwc" {
                    SQLITE_OK
                } else {
                    SQLITE_CANTOPEN
                };
                assert_eq!(
                    sqlite3_open_v2(uri.as_ptr(), &mut db, 6 | SQLITE_OPEN_URI, ptr::null()),
                    expected
                );
                assert_eq!(missing.exists(), mode == "rwc");
                assert!(!db.is_null());
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
            let missing = dir.path().join("plain-rw.db");
            let name = std::ffi::CString::new(missing.to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(name.as_ptr(), &mut db, 2, ptr::null()),
                SQLITE_CANTOPEN
            );
            assert!(!missing.exists());
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_default_open_falls_back_to_readonly() {
        use std::os::unix::fs::PermissionsExt;
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("permissions.db");
            let name = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(name.as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t(x); INSERT INTO t VALUES(1)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
            if std::fs::OpenOptions::new().write(true).open(&path).is_ok() {
                eprintln!("requires an unprivileged user whose writes are denied by chmod 0444");
                return;
            }
            for keep_reader in [false, true] {
                let mut reader = ptr::null_mut();
                if keep_reader {
                    assert_eq!(
                        sqlite3_open_v2(name.as_ptr(), &mut reader, 1, ptr::null()),
                        SQLITE_OK
                    );
                    assert_eq!(
                        sqlite3_exec(
                            reader,
                            c"BEGIN; SELECT * FROM t".as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut()
                        ),
                        SQLITE_OK
                    );
                }
                for flags in [None, Some(6), Some(2), Some(1)] {
                    db = ptr::null_mut();
                    let rc = match flags {
                        None => sqlite3_open(name.as_ptr(), &mut db),
                        Some(flags) => sqlite3_open_v2(name.as_ptr(), &mut db, flags, ptr::null()),
                    };
                    assert_eq!(rc, SQLITE_OK, "open flags {flags:?}");
                    assert_eq!(sqlite3_db_readonly(db, c"main".as_ptr()), 1);
                    assert_eq!(
                        sqlite3_exec(
                            db,
                            c"SELECT * FROM t".as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut()
                        ),
                        SQLITE_OK
                    );
                    assert_eq!(
                        sqlite3_exec(
                            db,
                            c"INSERT INTO t VALUES(2)".as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut()
                        ),
                        8
                    );
                    assert_eq!(sqlite3_close(db), SQLITE_OK);
                }
                if !reader.is_null() {
                    assert_eq!(sqlite3_close(reader), SQLITE_OK);
                }
            }
            for flags in [None, Some(6), Some(2), Some(1)] {
                let missing = dir.path().join("missing-parent/db");
                let name = std::ffi::CString::new(missing.to_str().unwrap()).unwrap();
                let rc = match flags {
                    None => sqlite3_open(name.as_ptr(), &mut db),
                    Some(flags) => sqlite3_open_v2(name.as_ptr(), &mut db, flags, ptr::null()),
                };
                assert_eq!(rc, SQLITE_CANTOPEN);
                assert!(!missing.exists());
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
        }
    }

    #[test]
    fn test_uri_readonly_rejects_write() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("readonly.db");
            let name = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut writer = ptr::null_mut();
            assert_eq!(sqlite3_open(name.as_ptr(), &mut writer), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    writer,
                    c"CREATE TABLE t(x); INSERT INTO t VALUES(1)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(writer), SQLITE_OK);
            let uri = std::ffi::CString::new(format!("file:{}?mode=ro", path.display())).unwrap();
            let mut reader = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(
                    uri.as_ptr(),
                    &mut reader,
                    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                    ptr::null()
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_db_readonly(reader, c"main".as_ptr()), 1);
            assert_eq!(
                sqlite3_exec(
                    reader,
                    c"INSERT INTO t VALUES(2)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                8
            );
            let mut log = 123;
            let mut checkpointed = 456;
            assert_eq!(
                sqlite3_wal_checkpoint_v2(reader, ptr::null(), 3, &mut log, &mut checkpointed),
                SQLITE_OK
            );
            assert_eq!((log, checkpointed), (-1, -1));
            assert_eq!(sqlite3_close(reader), SQLITE_OK);
        }
    }

    #[test]
    fn test_uri_readonly_live_handles() {
        unsafe {
            for readonly_first in [false, true] {
                for close_reader_first in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("mixed.db");
                    let name = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                    let mut seed = ptr::null_mut();
                    assert_eq!(sqlite3_open(name.as_ptr(), &mut seed), SQLITE_OK);
                    assert_eq!(
                        sqlite3_exec(
                            seed,
                            c"CREATE TABLE t(x); INSERT INTO t VALUES(1)".as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut()
                        ),
                        SQLITE_OK
                    );
                    assert_eq!(sqlite3_close(seed), SQLITE_OK);
                    let uri = std::ffi::CString::new(format!(
                        "file:{}?m%6fde=r%6f#ignored?mode=rwc",
                        path.display()
                    ))
                    .unwrap();
                    let mut reader = ptr::null_mut();
                    let mut writer = ptr::null_mut();
                    let open_reader = |reader: &mut *mut sqlite3| {
                        sqlite3_open_v2(
                            uri.as_ptr(),
                            reader,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                            ptr::null(),
                        )
                    };
                    if readonly_first {
                        assert_eq!(open_reader(&mut reader), SQLITE_OK);
                        assert_eq!(sqlite3_open(name.as_ptr(), &mut writer), SQLITE_OK);
                    } else {
                        assert_eq!(sqlite3_open(name.as_ptr(), &mut writer), SQLITE_OK);
                        assert_eq!(open_reader(&mut reader), SQLITE_OK);
                    }
                    assert_eq!(sqlite3_db_readonly(reader, c"main".as_ptr()), 1);
                    assert_eq!(sqlite3_db_readonly(writer, c"main".as_ptr()), 0);
                    assert_eq!(
                        sqlite3_exec(
                            reader,
                            c"PRAGMA query_only=OFF".as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut()
                        ),
                        SQLITE_OK
                    );
                    for sql in [
                        c"CREATE TEMP TABLE local(x)",
                        c"INSERT INTO local VALUES(1)",
                    ] {
                        let rc = sqlite3_exec(
                            reader,
                            sql.as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut(),
                        );
                        assert_eq!(
                            rc,
                            SQLITE_OK,
                            "{:?}: {:?}",
                            sql,
                            std::ffi::CStr::from_ptr(sqlite3_errmsg(reader))
                        );
                    }
                    assert_eq!(
                        sqlite3_exec(
                            reader,
                            c"INSERT INTO t VALUES(2)".as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut()
                        ),
                        8
                    );
                    assert_eq!(
                        sqlite3_exec(
                            writer,
                            c"INSERT INTO t VALUES(2)".as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut()
                        ),
                        SQLITE_OK
                    );
                    let mut stmt = ptr::null_mut();
                    assert_eq!(
                        sqlite3_prepare_v2(
                            reader,
                            c"SELECT count(*) FROM t".as_ptr(),
                            -1,
                            &mut stmt,
                            ptr::null_mut()
                        ),
                        SQLITE_OK
                    );
                    assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
                    assert_eq!(sqlite3_column_int64(stmt, 0), 2);
                    assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
                    if close_reader_first {
                        assert_eq!(sqlite3_close(reader), SQLITE_OK);
                        assert_eq!(
                            sqlite3_exec(
                                writer,
                                c"INSERT INTO t VALUES(3)".as_ptr(),
                                None,
                                ptr::null_mut(),
                                ptr::null_mut()
                            ),
                            SQLITE_OK
                        );
                        assert_eq!(sqlite3_close(writer), SQLITE_OK);
                    } else {
                        assert_eq!(sqlite3_close(writer), SQLITE_OK);
                        assert_eq!(
                            sqlite3_exec(
                                reader,
                                c"SELECT * FROM t".as_ptr(),
                                None,
                                ptr::null_mut(),
                                ptr::null_mut()
                            ),
                            SQLITE_OK
                        );
                        assert_eq!(sqlite3_close(reader), SQLITE_OK);
                    }
                }
            }
        }
    }

    /// Test: URI with file: prefix and no query params should open a real file.
    #[test]
    fn test_uri_file_path() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("uri_test.db");
            let uri = format!("file:{}", db_path.to_str().unwrap());
            let uri_cstr = std::ffi::CString::new(uri).unwrap();

            let mut db: *mut sqlite3 = ptr::null_mut();
            let rc = sqlite3_open_v2(
                uri_cstr.as_ptr(),
                &mut db,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                ptr::null(),
            );
            assert_eq!(rc, SQLITE_OK, "open_v2 with file: URI path failed rc={rc}");

            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t1 (id INTEGER PRIMARY KEY)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);

            assert!(
                db_path.exists(),
                "file: URI with path should create a real database file"
            );
        }
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn test_shared_memory_uri_connections() {
        unsafe {
            let uri = c"file:shared_mem_test?mode=memory&cache=shared";
            let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI;

            let mut db1: *mut sqlite3 = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(uri.as_ptr(), &mut db1, flags, ptr::null()),
                SQLITE_OK,
                "first open failed"
            );

            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db1,
                    c"CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK,
                "CREATE TABLE failed"
            );
            assert_eq!(
                sqlite3_exec(
                    db1,
                    c"INSERT INTO t1 VALUES (1, 'from_conn1')".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK,
                "INSERT failed"
            );

            // Second connection to the same URI must see shared state
            let mut db2: *mut sqlite3 = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(uri.as_ptr(), &mut db2, flags, ptr::null()),
                SQLITE_OK,
                "second open failed"
            );

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db2,
                    c"SELECT val FROM t1 WHERE id = 1".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK,
                "SELECT on connection 2 failed — table not shared"
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let text = sqlite3_column_text(stmt, 0);
            assert!(!text.is_null());
            let val = std::ffi::CStr::from_ptr(text).to_str().unwrap();
            assert_eq!(val, "from_conn1");
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            assert_eq!(sqlite3_close(db2), SQLITE_OK);
            assert_eq!(sqlite3_close(db1), SQLITE_OK);
        }
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn test_different_named_memory_dbs_are_independent() {
        unsafe {
            let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI;

            // Open db "alpha"
            let mut db1: *mut sqlite3 = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(
                    c"file:alpha_iso?mode=memory&cache=shared".as_ptr(),
                    &mut db1,
                    flags,
                    ptr::null(),
                ),
                SQLITE_OK
            );
            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db1,
                    c"CREATE TABLE t1 (id INTEGER PRIMARY KEY)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK
            );

            // Open db "beta" — should NOT see t1
            let mut db2: *mut sqlite3 = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(
                    c"file:beta_iso?mode=memory&cache=shared".as_ptr(),
                    &mut db2,
                    flags,
                    ptr::null(),
                ),
                SQLITE_OK
            );
            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(
                db2,
                c"SELECT * FROM t1".as_ptr(),
                -1,
                &mut stmt,
                ptr::null_mut(),
            );
            // t1 only exists in "alpha", not "beta"
            assert_eq!(rc, SQLITE_ERROR);

            assert_eq!(sqlite3_close(db2), SQLITE_OK);
            assert_eq!(sqlite3_close(db1), SQLITE_OK);
        }
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn test_plain_memory_stays_independent() {
        unsafe {
            let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI;

            let mut db1: *mut sqlite3 = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(c"file::memory:".as_ptr(), &mut db1, flags, ptr::null(),),
                SQLITE_OK
            );
            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db1,
                    c"CREATE TABLE t1 (id INTEGER PRIMARY KEY)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK
            );

            // Unnamed :memory: must remain independent
            let mut db2: *mut sqlite3 = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(c"file::memory:".as_ptr(), &mut db2, flags, ptr::null(),),
                SQLITE_OK
            );
            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(
                db2,
                c"SELECT * FROM t1".as_ptr(),
                -1,
                &mut stmt,
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_ERROR, "plain :memory: should not share state");

            assert_eq!(sqlite3_close(db2), SQLITE_OK);
            assert_eq!(sqlite3_close(db1), SQLITE_OK);
        }
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn test_unnamed_memory_cache_shared() {
        unsafe {
            let uri = c"file::memory:?cache=shared";
            let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI;

            let mut db1: *mut sqlite3 = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(uri.as_ptr(), &mut db1, flags, ptr::null()),
                SQLITE_OK,
            );
            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db1,
                    c"CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK,
            );
            assert_eq!(
                sqlite3_exec(
                    db1,
                    c"INSERT INTO t1 VALUES (1, 'shared')".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK,
            );

            // Same URI must share the database
            let mut db2: *mut sqlite3 = ptr::null_mut();
            assert_eq!(
                sqlite3_open_v2(uri.as_ptr(), &mut db2, flags, ptr::null()),
                SQLITE_OK,
            );
            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db2,
                    c"SELECT val FROM t1 WHERE id = 1".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK,
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let text = sqlite3_column_text(stmt, 0);
            assert!(!text.is_null());
            let val = std::ffi::CStr::from_ptr(text).to_str().unwrap();
            assert_eq!(val, "shared");
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            assert_eq!(sqlite3_close(db2), SQLITE_OK);
            assert_eq!(sqlite3_close(db1), SQLITE_OK);
        }
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn test_sqlite3_column_value_and_value_dup_free() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.db");
            let path_cstr = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path_cstr.as_ptr(), &mut db), SQLITE_OK);

            let mut errmsg: *mut libc::c_char = ptr::null_mut();
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t2 (i INTEGER, f REAL, t TEXT, b BLOB);".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"INSERT INTO t2 VALUES (42, 1.5, 'hello', X'DEADBEEF');".as_ptr(),
                    None,
                    ptr::null_mut(),
                    &mut errmsg,
                ),
                SQLITE_OK
            );

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT i, f, t, b FROM t2".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);

            let val0 = sqlite3_column_value(stmt, 0);
            assert!(!val0.is_null());
            assert_eq!(sqlite3_value_type(val0), SQLITE_INTEGER);
            assert_eq!(sqlite3_value_int64(val0), 42);

            let val1 = sqlite3_column_value(stmt, 1);
            assert!(!val1.is_null());
            assert_eq!(sqlite3_value_type(val1), SQLITE_FLOAT);
            assert!((sqlite3_value_double(val1) - 1.5).abs() < 0.001);

            let val2 = sqlite3_column_value(stmt, 2);
            assert!(!val2.is_null());
            assert_eq!(sqlite3_value_type(val2), SQLITE_TEXT);
            let text_ptr = sqlite3_value_text(val2);
            assert!(!text_ptr.is_null());
            let text_len = sqlite3_value_bytes(val2) as usize;
            let text =
                std::str::from_utf8(std::slice::from_raw_parts(text_ptr as *const u8, text_len))
                    .unwrap();
            assert_eq!(text, "hello");

            let val3 = sqlite3_column_value(stmt, 3);
            assert!(!val3.is_null());
            assert_eq!(sqlite3_value_type(val3), SQLITE_BLOB);
            assert_eq!(sqlite3_value_bytes(val3), 4);

            let dup = sqlite3_value_dup(val0);
            assert!(!dup.is_null());
            assert_eq!(sqlite3_value_type(dup), SQLITE_INTEGER);
            assert_eq!(sqlite3_value_int64(dup), 42);

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_value_int64(dup), 42);

            sqlite3_value_free(dup);

            sqlite3_value_free(ptr::null_mut());
            let null_dup = sqlite3_value_dup(ptr::null_mut());
            assert!(null_dup.is_null());

            let null_val = sqlite3_column_value(ptr::null_mut(), 0);
            assert!(null_val.is_null());

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    #[cfg(not(feature = "sqlite3"))]
    fn test_sqlite3_context_db_handle() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.db");
            let path_cstr = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path_cstr.as_ptr(), &mut db), SQLITE_OK);

            use std::sync::atomic::{AtomicPtr, Ordering};
            static CAPTURED_DB: AtomicPtr<libc::c_void> = AtomicPtr::new(ptr::null_mut());

            unsafe extern "C" fn test_func(
                ctx: *mut libc::c_void,
                _argc: i32,
                _argv: *mut *mut libc::c_void,
            ) {
                CAPTURED_DB.store(sqlite3_context_db_handle(ctx), Ordering::SeqCst);
                sqlite3_result_int(ctx, 1);
            }

            assert_eq!(
                sqlite3_create_function_v2(
                    db,
                    c"test_db_handle".as_ptr(),
                    0,
                    SQLITE_UTF8,
                    ptr::null_mut(),
                    Some(test_func),
                    None,
                    None,
                    None,
                ),
                SQLITE_OK
            );

            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT test_db_handle()".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);

            assert_eq!(CAPTURED_DB.load(Ordering::SeqCst), db as *mut libc::c_void);

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let null_handle = sqlite3_context_db_handle(ptr::null_mut());
            assert!(null_handle.is_null());

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_sqlite3_stricmp_and_strnicmp() {
        unsafe {
            // NULL handling
            assert_eq!(sqlite3_stricmp(ptr::null(), ptr::null()), 0);
            assert_eq!(sqlite3_stricmp(ptr::null(), c"abc".as_ptr()), -1);
            assert_eq!(sqlite3_stricmp(c"abc".as_ptr(), ptr::null()), 1);

            // NULL precedence over N <= 0 in strnicmp
            assert_eq!(sqlite3_strnicmp(ptr::null(), c"abc".as_ptr(), 0), -1);
            assert_eq!(sqlite3_strnicmp(c"abc".as_ptr(), ptr::null(), 0), 1);
            assert_eq!(sqlite3_strnicmp(ptr::null(), ptr::null(), 0), 0);

            // N <= 0 returns 0 for non-null strings
            assert_eq!(sqlite3_strnicmp(c"abc".as_ptr(), c"xyz".as_ptr(), 0), 0);
            assert_eq!(sqlite3_strnicmp(c"abc".as_ptr(), c"xyz".as_ptr(), -1), 0);

            // Case-insensitivity (unbounded vs bounded)
            assert_eq!(sqlite3_stricmp(c"Hello".as_ptr(), c"hello".as_ptr()), 0);
            assert_eq!(sqlite3_strnicmp(c"HELLO".as_ptr(), c"hello".as_ptr(), 5), 0);

            // Bounded prefix matching and exact difference on mismatch
            assert_eq!(
                sqlite3_strnicmp(c"abcdef".as_ptr(), c"ABCXYZ".as_ptr(), 3),
                0
            );
            assert_eq!(
                sqlite3_strnicmp(c"abcdef".as_ptr(), c"ABCXYZ".as_ptr(), 4),
                (b'd' as i32) - (b'x' as i32)
            );
            assert_eq!(sqlite3_strnicmp(c"abc".as_ptr(), c"abcd".as_ptr(), 3), 0);

            // Length mismatch return value ('\0' difference)
            assert_eq!(
                sqlite3_stricmp(c"abc".as_ptr(), c"abcd".as_ptr()),
                -(b'd' as i32)
            );
            assert_eq!(
                sqlite3_strnicmp(c"abcd".as_ptr(), c"abc".as_ptr(), 4),
                b'd' as i32
            );

            // Non-ASCII byte comparisons (raw byte difference without folding)
            assert_eq!(sqlite3_stricmp(c"\x80".as_ptr(), c"\x81".as_ptr()), -1);
            assert_eq!(
                sqlite3_strnicmp(c"\xc3\xa9".as_ptr(), c"\xc3\x89".as_ptr(), 2),
                (0xa9_i32) - (0x89_i32)
            );
        }
    }
}
