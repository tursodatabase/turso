//! Randomized differential oracle for the C API.
//!
//! Loads the system libsqlite3 and the freshly built libturso_sqlite3 into
//! the same process with dlopen (RTLD_LOCAL keeps their symbol tables
//! apart), resolves the same set of entry points from each, and drives both
//! through identical randomized call sequences — schemas, inserts with
//! every parameter-marker style, binds in random order with adversarial
//! values, interleaved introspection (sqlite3_sql, sqlite3_expanded_sql,
//! sqlite3_stmt_busy), resets, clear_bindings, and full row readback —
//! comparing every observable in lockstep. Any divergence aborts with the
//! scenario seed and the operation trace.
//!
//! Going through dlopen/dlsym also verifies the dynamic export table of the
//! cdylib itself: a symbol missing from the library fails resolution here
//! before any C consumer trips over it.
//!
//! Reproduce a failure with TURSO_ORACLE_SEED=<seed>; raise the scenario
//! count with TURSO_ORACLE_ITERS=<n> for longer local fuzzing sessions.
//!
//! The turso side is the cdylib in target/debug, which `cargo test` does
//! not relink on its own: run `cargo build -p turso_sqlite3 --features
//! capi` first (CI's c-compat workflow already does) or the oracle tests a
//! stale library.

// Windows is excluded like the compat suite: there is no system SQLite
// library to compare against, and the harness loads both libraries with
// dlopen. SQLite behavior is platform-independent, so Linux/macOS coverage
// suffices.
#![cfg(not(target_os = "windows"))]

use std::ffi::{c_char, c_int, c_void, CStr, CString};

const SQLITE_OK: c_int = 0;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;

/// The destructor slot of sqlite3_bind_text/blob is a function pointer in
/// the C API; SQLITE_TRANSIENT is the all-ones sentinel value of that type.
type BindDestructor = Option<unsafe extern "C" fn(*mut c_void)>;
#[allow(clippy::useless_transmute)]
fn sqlite_transient() -> BindDestructor {
    // SAFETY: reproduces C's ((sqlite3_destructor_type)-1) sentinel; it is
    // never called, only compared against by the library.
    unsafe { std::mem::transmute(-1isize) }
}

macro_rules! api_table {
    ($( $name:ident : $ty:ty ),+ $(,)?) => {
        struct Api {
            label: &'static str,
            $( $name: $ty, )+
        }

        impl Api {
            /// # Safety
            /// `path` must name a library exporting the sqlite3 C API with
            /// the standard signatures.
            unsafe fn load(label: &'static str, path: &str) -> Api {
                let cpath = CString::new(path).unwrap();
                let handle = libc::dlopen(cpath.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL);
                if handle.is_null() {
                    let err = libc::dlerror();
                    let err = if err.is_null() {
                        "unknown".to_string()
                    } else {
                        CStr::from_ptr(err).to_string_lossy().into_owned()
                    };
                    panic!("dlopen({path}) failed: {err}");
                }
                #[allow(unused_assignments)]
                let mut sym: *mut c_void = std::ptr::null_mut();
                $(
                    let cname = CString::new(stringify!($name)).unwrap();
                    sym = libc::dlsym(handle, cname.as_ptr());
                    assert!(
                        !sym.is_null(),
                        "{label}: symbol {} not exported by {path}",
                        stringify!($name)
                    );
                    let $name: $ty = std::mem::transmute(sym);
                )+
                Api { label, $( $name, )+ }
            }
        }
    };
}

api_table! {
    sqlite3_open: unsafe extern "C" fn(*const c_char, *mut *mut c_void) -> c_int,
    sqlite3_close: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_exec: unsafe extern "C" fn(*mut c_void, *const c_char, *mut c_void, *mut c_void, *mut *mut c_char) -> c_int,
    sqlite3_prepare_v2: unsafe extern "C" fn(*mut c_void, *const c_char, c_int, *mut *mut c_void, *mut *const c_char) -> c_int,
    sqlite3_step: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_reset: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_clear_bindings: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_finalize: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_bind_null: unsafe extern "C" fn(*mut c_void, c_int) -> c_int,
    sqlite3_bind_int64: unsafe extern "C" fn(*mut c_void, c_int, i64) -> c_int,
    sqlite3_bind_double: unsafe extern "C" fn(*mut c_void, c_int, f64) -> c_int,
    sqlite3_bind_text: unsafe extern "C" fn(*mut c_void, c_int, *const c_char, c_int, BindDestructor) -> c_int,
    sqlite3_bind_blob: unsafe extern "C" fn(*mut c_void, c_int, *const c_void, c_int, BindDestructor) -> c_int,
    sqlite3_bind_parameter_count: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_bind_parameter_index: unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int,
    sqlite3_bind_parameter_name: unsafe extern "C" fn(*mut c_void, c_int) -> *const c_char,
    sqlite3_column_count: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_column_type: unsafe extern "C" fn(*mut c_void, c_int) -> c_int,
    sqlite3_column_int64: unsafe extern "C" fn(*mut c_void, c_int) -> i64,
    sqlite3_column_double: unsafe extern "C" fn(*mut c_void, c_int) -> f64,
    sqlite3_column_text: unsafe extern "C" fn(*mut c_void, c_int) -> *const u8,
    sqlite3_column_blob: unsafe extern "C" fn(*mut c_void, c_int) -> *const c_void,
    sqlite3_column_bytes: unsafe extern "C" fn(*mut c_void, c_int) -> c_int,
    sqlite3_sql: unsafe extern "C" fn(*mut c_void) -> *const c_char,
    sqlite3_expanded_sql: unsafe extern "C" fn(*mut c_void) -> *mut c_char,
    sqlite3_stmt_busy: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_changes: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_total_changes: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_last_insert_rowid: unsafe extern "C" fn(*mut c_void) -> i64,
    sqlite3_errcode: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_get_autocommit: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_data_count: unsafe extern "C" fn(*mut c_void) -> c_int,
    sqlite3_column_name: unsafe extern "C" fn(*mut c_void, c_int) -> *const c_char,
    sqlite3_libversion_number: unsafe extern "C" fn() -> c_int,
    sqlite3_free: unsafe extern "C" fn(*mut c_void),
}

fn system_sqlite_path() -> &'static str {
    if cfg!(target_os = "macos") {
        "/usr/lib/libsqlite3.dylib"
    } else {
        "libsqlite3.so.0"
    }
}

fn turso_sqlite_path() -> String {
    // current_exe is target/debug/deps/oracle-<hash>; the cdylib sits two
    // levels up in target/debug.
    let exe = std::env::current_exe().unwrap();
    let dir = exe.parent().unwrap().parent().unwrap();
    let name = if cfg!(target_os = "macos") {
        "libturso_sqlite3.dylib"
    } else {
        "libturso_sqlite3.so"
    };
    dir.join(name).to_str().unwrap().to_string()
}

/// SplitMix64: tiny deterministic PRNG, no dependencies.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Val {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// Everything observable about one result column, read through every
/// accessor. Text and blob keep raw bytes and pointer-nullness.
#[derive(Debug, PartialEq)]
struct ColObs {
    declared_type: c_int,
    as_int: i64,
    as_double: f64,
    as_text: Option<Vec<u8>>,
    as_blob: Option<Vec<u8>>,
}

fn random_value(rng: &mut Rng) -> Val {
    match rng.below(5) {
        0 => Val::Null,
        1 => {
            let pool = [
                0i64,
                1,
                -1,
                42,
                i64::MAX,
                i64::MIN,
                1 << 40,
                i32::MAX as i64,
                i32::MIN as i64,
                i32::MAX as i64 + 1,
                i32::MIN as i64 - 1,
            ];
            Val::Int(pool[rng.below(pool.len())])
        }
        2 => {
            // NaN and infinities are deliberately excluded until core's
            // policy for them matches SQLite (bind_double(NaN) stores NULL).
            let pool = [0.0f64, -0.0, 3.5, -2.25, 0.1, 1e15, -1.5e-8, 12345.6789];
            Val::Float(pool[rng.below(pool.len())])
        }
        3 => {
            let pool = [
                "",
                "a",
                "it's",
                "two''quotes",
                "h\u{e9}llo w\u{f6}rld \u{1F680}",
                "line\nbreak\ttab",
                "SELECT ?1 -- not a param",
                "12345",
                "-3.25",
            ];
            let mut s = pool[rng.below(pool.len())].to_string();
            if rng.chance(20) {
                // Stay under SQLITE_TRACE_SIZE_LIMIT territory: reference
                // builds compiled with it (e.g. Apple's) truncate long
                // values in expanded SQL with a "/*+N bytes*/" marker,
                // while stock builds and turso render them in full.
                let max_repeat = (900 / s.len().max(1)).max(1);
                s = s.repeat(rng.below(max_repeat) + 1);
            }
            Val::Text(s)
        }
        _ => {
            let mut b = vec![0u8; rng.below(48)];
            for byte in b.iter_mut() {
                *byte = rng.next() as u8;
            }
            Val::Blob(b)
        }
    }
}

/// The random whitespace/comment filler exercises the marker scanner.
fn random_filler(rng: &mut Rng) -> &'static str {
    [" ", " ", " ", "\n", "\t", " /* c? */ ", " -- ?x\n"][rng.below(7)]
}

/// One randomly chosen parameter marker. Duplicated names and explicit
/// index gaps are legal and exercise the numbering rules.
fn random_marker(rng: &mut Rng, k: usize, names: &mut Vec<String>) -> String {
    match rng.below(5) {
        0 => "?".to_string(),
        1 => format!("?{}", k + 1 + rng.below(3)),
        2 | 3 => {
            let name = format!(":p{}", rng.below(4));
            names.push(name.clone());
            name
        }
        _ => {
            let sig = ["@", "$"][rng.below(2)];
            let name = format!("{sig}q{}", rng.below(4));
            names.push(name.clone());
            name
        }
    }
}

struct Ctx {
    seed: u64,
    trace: Vec<String>,
}

impl Ctx {
    fn log(&mut self, line: String) {
        self.trace.push(line);
    }
    fn check<T: PartialEq + std::fmt::Debug>(&mut self, what: &str, sqlite: T, turso: T) {
        if sqlite != turso {
            panic!(
                "[oracle] seed={} DIVERGENCE at {what}:\n  sqlite: {sqlite:?}\n  turso:  {turso:?}\ntrace:\n  {}",
                self.seed,
                self.trace.join("\n  ")
            );
        }
    }
}

struct Session<'a> {
    api: &'a Api,
    db: *mut c_void,
    stmt: *mut c_void,
}

impl<'a> Session<'a> {
    unsafe fn open(api: &'a Api) -> Session<'a> {
        let mut db = std::ptr::null_mut();
        let path = CString::new(":memory:").unwrap();
        let rc = (api.sqlite3_open)(path.as_ptr(), &mut db);
        assert_eq!(rc, SQLITE_OK, "{}: open :memory:", api.label);
        Session {
            api,
            db,
            stmt: std::ptr::null_mut(),
        }
    }

    unsafe fn exec(&mut self, sql: &CString) -> c_int {
        (self.api.sqlite3_exec)(
            self.db,
            sql.as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    }

    unsafe fn prepare(&mut self, sql: &CString) -> c_int {
        self.finalize();
        (self.api.sqlite3_prepare_v2)(
            self.db,
            sql.as_ptr(),
            -1,
            &mut self.stmt,
            std::ptr::null_mut(),
        )
    }

    unsafe fn finalize(&mut self) {
        if !self.stmt.is_null() {
            (self.api.sqlite3_finalize)(self.stmt);
            self.stmt = std::ptr::null_mut();
        }
    }

    unsafe fn bind(&mut self, idx: c_int, v: &Val) -> c_int {
        let api = self.api;
        match v {
            Val::Null => (api.sqlite3_bind_null)(self.stmt, idx),
            Val::Int(i) => (api.sqlite3_bind_int64)(self.stmt, idx, *i),
            Val::Float(f) => (api.sqlite3_bind_double)(self.stmt, idx, *f),
            Val::Text(s) => (api.sqlite3_bind_text)(
                self.stmt,
                idx,
                s.as_ptr() as *const c_char,
                s.len() as c_int,
                sqlite_transient(),
            ),
            Val::Blob(b) => (api.sqlite3_bind_blob)(
                self.stmt,
                idx,
                b.as_ptr() as *const c_void,
                b.len() as c_int,
                sqlite_transient(),
            ),
        }
    }

    unsafe fn expanded_sql(&mut self) -> Option<String> {
        let p = (self.api.sqlite3_expanded_sql)(self.stmt);
        if p.is_null() {
            return None;
        }
        let s = CStr::from_ptr(p).to_string_lossy().into_owned();
        (self.api.sqlite3_free)(p as *mut c_void);
        Some(s)
    }

    unsafe fn sql(&mut self) -> String {
        let p = (self.api.sqlite3_sql)(self.stmt);
        assert!(!p.is_null());
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }

    /// Reads column `i` through every accessor, not just the one matching
    /// the declared type: SQLite defines conversions for mismatched
    /// accessors (column_text on an integer renders it, column_int64 on
    /// text parses it), and those paths are exactly where a compat layer
    /// diverges. Accessors are invoked in a fixed order on both libraries;
    /// SQLite documents that conversions may mutate the stored value, so
    /// the identical call order keeps the comparison fair. NULL pointers
    /// are observed as None rather than folded into empty buffers.
    unsafe fn column(&mut self, i: c_int) -> ColObs {
        let api = self.api;
        let declared_type = (api.sqlite3_column_type)(self.stmt, i);
        let as_int = (api.sqlite3_column_int64)(self.stmt, i);
        let as_double = (api.sqlite3_column_double)(self.stmt, i);
        let text_ptr = (api.sqlite3_column_text)(self.stmt, i);
        let text_bytes = (api.sqlite3_column_bytes)(self.stmt, i) as usize;
        let as_text = if text_ptr.is_null() {
            None
        } else {
            Some(std::slice::from_raw_parts(text_ptr, text_bytes).to_vec())
        };
        let blob_ptr = (api.sqlite3_column_blob)(self.stmt, i);
        let blob_bytes = (api.sqlite3_column_bytes)(self.stmt, i) as usize;
        let as_blob = if blob_ptr.is_null() {
            None
        } else {
            Some(std::slice::from_raw_parts(blob_ptr as *const u8, blob_bytes).to_vec())
        };
        ColObs {
            declared_type,
            as_int,
            as_double,
            as_text,
            as_blob,
        }
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        unsafe {
            self.finalize();
            (self.api.sqlite3_close)(self.db);
        }
    }
}

unsafe fn compare_introspection(ctx: &mut Ctx, s: &mut Session, t: &mut Session, when: &str) {
    ctx.check(&format!("sql ({when})"), s.sql(), t.sql());
    ctx.check(
        &format!("expanded_sql ({when})"),
        s.expanded_sql(),
        t.expanded_sql(),
    );
    ctx.check(
        &format!("stmt_busy ({when})"),
        (s.api.sqlite3_stmt_busy)(s.stmt),
        (t.api.sqlite3_stmt_busy)(t.stmt),
    );
}

unsafe fn run_scenario(ctx: &mut Ctx, rng: &mut Rng, sqlite: &Api, turso: &Api) {
    let mut s = Session::open(sqlite);
    let mut t = Session::open(turso);

    let create = CString::new("CREATE TABLE t(a,b,c,d,e)").unwrap();
    ctx.check("exec CREATE", s.exec(&create), t.exec(&create));

    for round in 0..4 {
        // Build an INSERT whose five values are a random mix of parameter
        // markers (all styles) and the occasional literal, separated by
        // random whitespace and comments.
        let mut names = Vec::new();
        let mut sql = String::from("INSERT INTO t VALUES(");
        for k in 0..5 {
            if k > 0 {
                sql.push(',');
            }
            sql.push_str(random_filler(rng));
            if rng.chance(20) {
                sql.push_str(["7", "'lit''x'", "x'ff00'", "NULL", "1.5"][rng.below(5)]);
            } else {
                sql.push_str(&random_marker(rng, k, &mut names));
            }
            sql.push_str(random_filler(rng));
        }
        sql.push(')');
        ctx.log(format!("round {round}: {sql:?}"));
        let csql = CString::new(sql).unwrap();

        let rc_s = s.prepare(&csql);
        let rc_t = t.prepare(&csql);
        ctx.check("prepare INSERT rc", rc_s, rc_t);
        if rc_s != SQLITE_OK {
            continue;
        }

        let count = (sqlite.sqlite3_bind_parameter_count)(s.stmt);
        ctx.check(
            "bind_parameter_count",
            count,
            (turso.sqlite3_bind_parameter_count)(t.stmt),
        );

        // Every index reports the same parameter name (None for positional
        // slots and gaps), including one index past the end.
        for i in 0..=count + 1 {
            let name_s = (sqlite.sqlite3_bind_parameter_name)(s.stmt, i);
            let name_t = (turso.sqlite3_bind_parameter_name)(t.stmt, i);
            let name_s = (!name_s.is_null()).then(|| CStr::from_ptr(name_s).to_owned());
            let name_t = (!name_t.is_null()).then(|| CStr::from_ptr(name_t).to_owned());
            ctx.check(&format!("bind_parameter_name({i})"), name_s, name_t);
        }

        compare_introspection(ctx, &mut s, &mut t, "before binds");

        // Bind by name: each library resolves the name with its own
        // bind_parameter_index, so numbering rules are compared too.
        for name in &names {
            if !rng.chance(80) {
                continue;
            }
            let cname = CString::new(name.as_str()).unwrap();
            let idx_s = (sqlite.sqlite3_bind_parameter_index)(s.stmt, cname.as_ptr());
            let idx_t = (turso.sqlite3_bind_parameter_index)(t.stmt, cname.as_ptr());
            ctx.check(&format!("parameter_index({name})"), idx_s, idx_t);
            if idx_s == 0 {
                continue;
            }
            let v = random_value(rng);
            ctx.log(format!("bind {name} (idx {idx_s}) = {v:?}"));
            ctx.check(
                &format!("bind rc ({name})"),
                s.bind(idx_s, &v),
                t.bind(idx_t, &v),
            );
        }

        // Bind a random subset of positional indices, sometimes out of
        // range on purpose: both sides must fail identically.
        for _ in 0..rng.below(6) {
            let idx = if rng.chance(15) {
                [0, count + 1, count + 7][rng.below(3)]
            } else if count > 0 {
                (rng.below(count as usize) + 1) as c_int
            } else {
                continue;
            };
            let v = random_value(rng);
            ctx.log(format!("bind idx {idx} = {v:?}"));
            ctx.check(
                &format!("bind rc (idx {idx})"),
                s.bind(idx, &v),
                t.bind(idx, &v),
            );
        }

        compare_introspection(ctx, &mut s, &mut t, "after binds");

        let rc_s = (sqlite.sqlite3_step)(s.stmt);
        let rc_t = (turso.sqlite3_step)(t.stmt);
        ctx.check("step INSERT rc", rc_s, rc_t);
        compare_introspection(ctx, &mut s, &mut t, "after step");
        if rc_s == SQLITE_DONE {
            ctx.check(
                "changes",
                (sqlite.sqlite3_changes)(s.db),
                (turso.sqlite3_changes)(t.db),
            );
            ctx.check(
                "total_changes",
                (sqlite.sqlite3_total_changes)(s.db),
                (turso.sqlite3_total_changes)(t.db),
            );
            ctx.check(
                "last_insert_rowid",
                (sqlite.sqlite3_last_insert_rowid)(s.db),
                (turso.sqlite3_last_insert_rowid)(t.db),
            );
        } else {
            ctx.check(
                "errcode after failed step",
                (sqlite.sqlite3_errcode)(s.db),
                (turso.sqlite3_errcode)(t.db),
            );
        }
        ctx.check(
            "get_autocommit",
            (sqlite.sqlite3_get_autocommit)(s.db),
            (turso.sqlite3_get_autocommit)(t.db),
        );

        // Lifecycle chaos: double reset and re-binding after reset are
        // legal call orders with defined behavior. Stepping again after
        // SQLITE_DONE is deliberately NOT exercised: stock SQLite
        // auto-resets and re-runs, builds with SQLITE_OMIT_AUTORESET (e.g.
        // Apple's) return SQLITE_MISUSE, and turso returns SQLITE_DONE
        // without re-running — the oracle found this three-way divergence;
        // auto-reset support in core is tracked as follow-up work.
        ctx.check(
            "reset rc",
            (sqlite.sqlite3_reset)(s.stmt),
            (turso.sqlite3_reset)(t.stmt),
        );
        compare_introspection(ctx, &mut s, &mut t, "after reset");
        if rng.chance(20) {
            ctx.log("chaos: double reset".into());
            ctx.check(
                "second reset rc",
                (sqlite.sqlite3_reset)(s.stmt),
                (turso.sqlite3_reset)(t.stmt),
            );
        }
        if rng.chance(20) && count > 0 {
            let idx = (rng.below(count as usize) + 1) as c_int;
            let v = random_value(rng);
            ctx.log(format!("chaos: re-bind idx {idx} = {v:?} after reset"));
            ctx.check("re-bind after reset rc", s.bind(idx, &v), t.bind(idx, &v));
            compare_introspection(ctx, &mut s, &mut t, "after re-bind");
        }
        if rng.chance(50) {
            ctx.check(
                "clear_bindings rc",
                (sqlite.sqlite3_clear_bindings)(s.stmt),
                (turso.sqlite3_clear_bindings)(t.stmt),
            );
            compare_introspection(ctx, &mut s, &mut t, "after clear_bindings");
        }
    }

    // An explicit rowid far above i32 keeps last_insert_rowid honest as a
    // 64-bit value across the ABI.
    if rng.chance(40) {
        let rowid = 5_000_000_000i64 + rng.below(1_000_000) as i64;
        let ins = format!("INSERT INTO t(rowid,a,b,c,d,e) VALUES({rowid},1,2,3,4,5)");
        ctx.log(format!("large rowid insert: {ins}"));
        let cins = CString::new(ins).unwrap();
        ctx.check("exec large-rowid INSERT", s.exec(&cins), t.exec(&cins));
        ctx.check(
            "last_insert_rowid (large)",
            (sqlite.sqlite3_last_insert_rowid)(s.db),
            (turso.sqlite3_last_insert_rowid)(t.db),
        );
    }

    // Read everything back in lockstep and compare types and values. The
    // SELECT is sometimes prepared from a multi-statement buffer with an
    // explicit byte length, comparing the reported tail offset.
    let select_text = "SELECT a,b,c,d,e FROM t ORDER BY rowid";
    if rng.chance(30) {
        let multi = format!("{select_text}; SELECT 2 /* tail */");
        ctx.log(format!("prepare with tail: {multi:?}"));
        let cmulti = CString::new(multi).unwrap();
        let mut tail_s: *const c_char = std::ptr::null();
        let mut tail_t: *const c_char = std::ptr::null();
        s.finalize();
        t.finalize();
        let rc_s = (sqlite.sqlite3_prepare_v2)(s.db, cmulti.as_ptr(), -1, &mut s.stmt, &mut tail_s);
        let rc_t = (turso.sqlite3_prepare_v2)(t.db, cmulti.as_ptr(), -1, &mut t.stmt, &mut tail_t);
        ctx.check("prepare multi rc", rc_s, rc_t);
        let off = |tail: *const c_char| {
            if tail.is_null() {
                -1
            } else {
                tail as isize - cmulti.as_ptr() as isize
            }
        };
        ctx.check("tail offset", off(tail_s), off(tail_t));
    } else {
        let cselect = CString::new(select_text).unwrap();
        ctx.check(
            "prepare SELECT rc",
            s.prepare(&cselect),
            t.prepare(&cselect),
        );
    }
    ctx.check(
        "column_count",
        (sqlite.sqlite3_column_count)(s.stmt),
        (turso.sqlite3_column_count)(t.stmt),
    );
    for i in 0..5 {
        let name_s = (sqlite.sqlite3_column_name)(s.stmt, i);
        let name_t = (turso.sqlite3_column_name)(t.stmt, i);
        let name_s = (!name_s.is_null()).then(|| CStr::from_ptr(name_s).to_owned());
        let name_t = (!name_t.is_null()).then(|| CStr::from_ptr(name_t).to_owned());
        ctx.check(&format!("column_name({i})"), name_s, name_t);
    }
    let mut row = 0;
    loop {
        let rc_s = (sqlite.sqlite3_step)(s.stmt);
        let rc_t = (turso.sqlite3_step)(t.stmt);
        ctx.check(&format!("step SELECT rc (row {row})"), rc_s, rc_t);
        ctx.check(
            &format!("data_count (row {row})"),
            (sqlite.sqlite3_data_count)(s.stmt),
            (turso.sqlite3_data_count)(t.stmt),
        );
        if rc_s != SQLITE_ROW {
            break;
        }
        for i in 0..5 {
            ctx.check(&format!("row {row} col {i}"), s.column(i), t.column(i));
        }
        if rng.chance(25) {
            compare_introspection(ctx, &mut s, &mut t, "mid-select");
        }
        row += 1;
    }
}

#[test]
fn oracle_differential() {
    let seed_base: u64 = std::env::var("TURSO_ORACLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xDEC0DE);
    let iters: u64 = std::env::var("TURSO_ORACLE_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    let turso_path = turso_sqlite_path();
    if !std::path::Path::new(&turso_path).exists() {
        // Generic workspace test runners (e.g. coverage) build test
        // binaries without the cdylib. The oracle's home is the c-compat
        // workflow, which builds it first.
        eprintln!(
            "oracle: SKIPPED — {turso_path} not built; \
             run `cargo build -p turso_sqlite3 --features capi` first"
        );
        return;
    }
    unsafe {
        let sqlite = Api::load("sqlite", system_sqlite_path());
        let turso = Api::load("turso", &turso_path);
        println!(
            "oracle: sqlite {} ({}) vs turso {} ({}), seed_base={seed_base}, iters={iters}",
            (sqlite.sqlite3_libversion_number)(),
            system_sqlite_path(),
            (turso.sqlite3_libversion_number)(),
            turso_sqlite_path(),
        );
        for i in 0..iters {
            let seed = seed_base.wrapping_add(i);
            let mut rng = Rng(seed);
            let mut ctx = Ctx {
                seed,
                trace: Vec::new(),
            };
            run_scenario(&mut ctx, &mut rng, &sqlite, &turso);
        }
    }
}
