use crate::common::{limbo_exec_rows, ExecRows, TempDatabase};
use rusqlite::types::Value as SqliteValue;
use serial_test::serial;
use std::cmp::Ordering;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};
use turso_core::{ContextValue, ContextValueBytes, ContextValueData, ContextValueType, LimboError};
use turso_ext::{Value as ExtValue, ValueType as ExtValueType};

static SCALAR_VALUE_DROPS: AtomicUsize = AtomicUsize::new(0);
static AGG_VALUE_DROPS: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct CallbackCounters {
    calls: AtomicUsize,
    context_drops: AtomicUsize,
    aggregate_inits: AtomicUsize,
    aggregate_steps: AtomicUsize,
    aggregate_finals: AtomicUsize,
    aggregate_drops: AtomicUsize,
}

struct ScalarContext {
    multiplier: i64,
    counters: Arc<CallbackCounters>,
}

struct AggregateContext {
    counters: Arc<CallbackCounters>,
}

struct SumState {
    sum: i64,
    counters: Arc<CallbackCounters>,
}

fn integer_result(value: i64) -> ContextValue {
    ContextValue {
        value_type: ContextValueType::Integer,
        value: ContextValueData { int: value },
    }
}

fn text_result(bytes: &'static [u8]) -> ContextValue {
    ContextValue {
        value_type: ContextValueType::Text,
        value: ContextValueData {
            bytes: ContextValueBytes {
                ptr: bytes.as_ptr(),
                len: bytes.len(),
            },
        },
    }
}

fn error_result(bytes: &'static [u8]) -> ContextValue {
    ContextValue {
        value_type: ContextValueType::Error,
        value: ContextValueData {
            bytes: ContextValueBytes {
                ptr: bytes.as_ptr(),
                len: bytes.len(),
            },
        },
    }
}

unsafe extern "C" fn managed_score(
    context: usize,
    argc: i32,
    argv: *const ExtValue,
    result: *mut ContextValue,
) {
    let ctx = unsafe { &*(context as *const ScalarContext) };
    ctx.counters.calls.fetch_add(1, AtomicOrdering::SeqCst);
    let args = if argc <= 0 || argv.is_null() {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(argv, argc as usize) }
    };

    let int_value = args
        .first()
        .and_then(ExtValue::to_integer)
        .unwrap_or_default();
    let float_value = args
        .get(1)
        .and_then(ExtValue::to_float)
        .map(|value| value as i64)
        .unwrap_or_default();
    let text_len = args
        .get(2)
        .and_then(ExtValue::to_text)
        .map(str::len)
        .unwrap_or_default() as i64;
    let blob_len = args
        .get(3)
        .and_then(ExtValue::to_blob)
        .map(|blob| blob.len())
        .unwrap_or_default() as i64;

    unsafe {
        *result = integer_result((int_value + float_value + text_len + blob_len) * ctx.multiplier);
    }
}

unsafe extern "C" fn managed_label(
    context: usize,
    _argc: i32,
    _argv: *const ExtValue,
    result: *mut ContextValue,
) {
    let ctx = unsafe { &*(context as *const ScalarContext) };
    ctx.counters.calls.fetch_add(1, AtomicOrdering::SeqCst);
    unsafe {
        *result = text_result(b"managed-label");
    }
}

unsafe extern "C" fn managed_fail(
    context: usize,
    _argc: i32,
    _argv: *const ExtValue,
    result: *mut ContextValue,
) {
    let ctx = unsafe { &*(context as *const ScalarContext) };
    ctx.counters.calls.fetch_add(1, AtomicOrdering::SeqCst);
    unsafe {
        *result = error_result(b"managed failure");
    }
}

unsafe extern "C" fn managed_variadic_score(
    context: usize,
    argc: i32,
    argv: *const ExtValue,
    result: *mut ContextValue,
) {
    let ctx = unsafe { &*(context as *const ScalarContext) };
    ctx.counters.calls.fetch_add(1, AtomicOrdering::SeqCst);
    let args = if argc <= 0 || argv.is_null() {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(argv, argc as usize) }
    };

    let null_count = args
        .iter()
        .filter(|arg| arg.value_type() == ExtValueType::Null)
        .count() as i64;
    let score = (argc as i64 * 100)
        + args
            .first()
            .and_then(ExtValue::to_integer)
            .unwrap_or_default()
        + args
            .get(1)
            .and_then(ExtValue::to_float)
            .map(|value| value as i64)
            .unwrap_or_default()
        + args
            .get(2)
            .and_then(ExtValue::to_text)
            .map(str::len)
            .unwrap_or_default() as i64
        + args
            .get(3)
            .and_then(ExtValue::to_blob)
            .map(|blob| blob.len())
            .unwrap_or_default() as i64
        + null_count;
    unsafe {
        *result = integer_result(score * ctx.multiplier);
    }
}

unsafe extern "C" fn drop_scalar_context(context: usize) {
    let context = unsafe { Box::from_raw(context as *mut ScalarContext) };
    context
        .counters
        .context_drops
        .fetch_add(1, AtomicOrdering::SeqCst);
}

unsafe extern "C" fn count_scalar_value_drop(_result: *mut ContextValue) {
    SCALAR_VALUE_DROPS.fetch_add(1, AtomicOrdering::SeqCst);
}

unsafe extern "C" fn managed_sum_init(context: usize) -> usize {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_inits
        .fetch_add(1, AtomicOrdering::SeqCst);
    Box::into_raw(Box::new(SumState {
        sum: 0,
        counters: ctx.counters.clone(),
    })) as usize
}

unsafe extern "C" fn managed_sum_step(
    context: usize,
    aggregate_context: usize,
    argc: i32,
    argv: *const ExtValue,
    result: *mut ContextValue,
) {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_steps
        .fetch_add(1, AtomicOrdering::SeqCst);
    let state = unsafe { &mut *(aggregate_context as *mut SumState) };
    if argc > 0 && !argv.is_null() {
        let args = unsafe { std::slice::from_raw_parts(argv, argc as usize) };
        state.sum += args
            .first()
            .and_then(ExtValue::to_integer)
            .unwrap_or_default();
    }
    unsafe {
        *result = ContextValue::null();
    }
}

unsafe extern "C" fn managed_sum_final(
    context: usize,
    aggregate_context: usize,
    result: *mut ContextValue,
) {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_finals
        .fetch_add(1, AtomicOrdering::SeqCst);
    let state = unsafe { &*(aggregate_context as *const SumState) };
    unsafe {
        *result = integer_result(state.sum);
    }
}

unsafe extern "C" fn managed_variadic_sum_step(
    context: usize,
    aggregate_context: usize,
    argc: i32,
    argv: *const ExtValue,
    result: *mut ContextValue,
) {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_steps
        .fetch_add(1, AtomicOrdering::SeqCst);
    let state = unsafe { &mut *(aggregate_context as *mut SumState) };
    state.sum += argc as i64;
    if argc > 0 && !argv.is_null() {
        let args = unsafe { std::slice::from_raw_parts(argv, argc as usize) };
        state.sum += args.iter().filter_map(ExtValue::to_integer).sum::<i64>();
    }
    unsafe {
        *result = ContextValue::null();
    }
}

unsafe extern "C" fn managed_step_fail(
    context: usize,
    _aggregate_context: usize,
    _argc: i32,
    _argv: *const ExtValue,
    result: *mut ContextValue,
) {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_steps
        .fetch_add(1, AtomicOrdering::SeqCst);
    unsafe {
        *result = error_result(b"aggregate step failure");
    }
}

unsafe extern "C" fn managed_final_fail(
    context: usize,
    _aggregate_context: usize,
    result: *mut ContextValue,
) {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_finals
        .fetch_add(1, AtomicOrdering::SeqCst);
    unsafe {
        *result = error_result(b"aggregate final failure");
    }
}

unsafe extern "C" fn drop_aggregate_context(context: usize) {
    let context = unsafe { Box::from_raw(context as *mut AggregateContext) };
    context
        .counters
        .context_drops
        .fetch_add(1, AtomicOrdering::SeqCst);
}

unsafe extern "C" fn drop_sum_state(aggregate_context: usize) {
    let context = unsafe { Box::from_raw(aggregate_context as *mut SumState) };
    context
        .counters
        .aggregate_drops
        .fetch_add(1, AtomicOrdering::SeqCst);
    drop(context);
}

unsafe extern "C" fn count_aggregate_value_drop(_result: *mut ContextValue) {
    AGG_VALUE_DROPS.fetch_add(1, AtomicOrdering::SeqCst);
}

fn boxed_scalar_context(multiplier: i64, counters: Arc<CallbackCounters>) -> usize {
    Box::into_raw(Box::new(ScalarContext {
        multiplier,
        counters,
    })) as usize
}

fn boxed_aggregate_context(counters: Arc<CallbackCounters>) -> usize {
    Box::into_raw(Box::new(AggregateContext { counters })) as usize
}

#[turso_macros::test]
#[serial]
fn managed_scalar_callbacks_cover_dotnet_create_function_cases(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    SCALAR_VALUE_DROPS.store(0, AtomicOrdering::SeqCst);
    let counters = Arc::new(CallbackCounters::default());
    let conn = tmp_db.connect_limbo();

    conn.register_external_scalar_function(
        "managed_score".to_string(),
        4,
        true,
        boxed_scalar_context(1, counters.clone()),
        managed_score,
        Some(drop_scalar_context),
        None,
    );
    conn.register_external_scalar_function(
        "managed_label".to_string(),
        0,
        true,
        boxed_scalar_context(1, counters.clone()),
        managed_label,
        Some(drop_scalar_context),
        Some(count_scalar_value_drop),
    );
    conn.register_external_scalar_function(
        "managed_fail".to_string(),
        0,
        true,
        boxed_scalar_context(1, counters.clone()),
        managed_fail,
        Some(drop_scalar_context),
        Some(count_scalar_value_drop),
    );

    let score: Vec<(i64,)> = conn.exec_rows("SELECT managed_score(2, 3.5, 'hi', x'010203')");
    assert_eq!(score, vec![(10,)]);
    let label: Vec<(String,)> = conn.exec_rows("SELECT managed_label()");
    assert_eq!(label, vec![("managed-label".to_string(),)]);
    let err = conn.execute("SELECT managed_fail()").unwrap_err();
    assert!(err.to_string().contains("managed failure"));
    assert_eq!(SCALAR_VALUE_DROPS.load(AtomicOrdering::SeqCst), 2);

    conn.register_external_scalar_function(
        "managed_score".to_string(),
        4,
        true,
        boxed_scalar_context(10, counters.clone()),
        managed_score,
        Some(drop_scalar_context),
        None,
    );
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 1);
    let score: Vec<(i64,)> = conn.exec_rows("SELECT managed_score(1, 2.0, 'a', x'00')");
    assert_eq!(score, vec![(50,)]);

    conn.unregister_external_function("managed_score");
    let err = conn
        .prepare("SELECT managed_score(1, 2.0, 'a', x'00')")
        .unwrap_err();
    assert!(err.to_string().contains("no such function"));
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 2);

    conn.unregister_external_function("managed_label");
    conn.unregister_external_function("managed_fail");
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 4);
    assert!(counters.calls.load(AtomicOrdering::SeqCst) >= 4);
    Ok(())
}

#[turso_macros::test]
#[serial]
fn managed_scalar_variadic_callbacks_receive_callsite_arguments(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let counters = Arc::new(CallbackCounters::default());
    let conn = tmp_db.connect_limbo();
    conn.register_external_scalar_function(
        "managed_variadic_score".to_string(),
        -1,
        true,
        boxed_scalar_context(1, counters.clone()),
        managed_variadic_score,
        Some(drop_scalar_context),
        None,
    );

    let score: Vec<(i64,)> =
        conn.exec_rows("SELECT managed_variadic_score(1, 3.5, 'A', x'7E57', NULL)");
    assert_eq!(score, vec![(508,)]);

    conn.unregister_external_function("managed_variadic_score");
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(counters.calls.load(AtomicOrdering::SeqCst), 1);
    Ok(())
}

#[turso_macros::test]
#[serial]
fn managed_aggregate_callbacks_cover_dotnet_create_aggregate_cases(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    AGG_VALUE_DROPS.store(0, AtomicOrdering::SeqCst);
    let counters = Arc::new(CallbackCounters::default());
    let conn = tmp_db.connect_limbo();
    conn.register_external_aggregate_function(
        "managed_sum".to_string(),
        1,
        true,
        boxed_aggregate_context(counters.clone()),
        managed_sum_init,
        managed_sum_step,
        managed_sum_final,
        Some(drop_aggregate_context),
        Some(drop_sum_state),
        Some(count_aggregate_value_drop),
    );

    conn.execute("CREATE TABLE items(category TEXT, value INTEGER)")?;
    conn.execute("INSERT INTO items VALUES ('a', 1), ('a', 2), ('a', 2), ('b', 4), ('b', NULL)")?;

    let grouped: Vec<(String, i64)> = conn.exec_rows(
        "SELECT category, managed_sum(value) FROM items GROUP BY category ORDER BY category",
    );
    assert_eq!(grouped, vec![("a".to_string(), 5), ("b".to_string(), 4)]);

    let filtered_distinct: Vec<(i64,)> =
        conn.exec_rows("SELECT managed_sum(DISTINCT value) FILTER (WHERE value < 3) FROM items");
    assert_eq!(filtered_distinct, vec![(3,)]);

    let empty: Vec<(i64,)> = conn.exec_rows("SELECT managed_sum(value) FROM items WHERE 0");
    assert_eq!(empty, vec![(0,)]);

    conn.unregister_external_function("managed_sum");
    let err = conn
        .prepare("SELECT managed_sum(value) FROM items")
        .unwrap_err();
    assert!(err.to_string().contains("no such function"));
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 1);
    assert!(counters.aggregate_inits.load(AtomicOrdering::SeqCst) >= 4);
    assert_eq!(
        counters.aggregate_inits.load(AtomicOrdering::SeqCst),
        counters.aggregate_finals.load(AtomicOrdering::SeqCst)
    );
    assert_eq!(
        counters.aggregate_inits.load(AtomicOrdering::SeqCst),
        counters.aggregate_drops.load(AtomicOrdering::SeqCst)
    );
    let aggregate_value_drops = AGG_VALUE_DROPS.load(AtomicOrdering::SeqCst);
    assert!(aggregate_value_drops >= counters.aggregate_finals.load(AtomicOrdering::SeqCst));
    assert!(aggregate_value_drops > counters.aggregate_finals.load(AtomicOrdering::SeqCst));
    Ok(())
}

#[turso_macros::test]
#[serial]
fn managed_aggregate_variadic_callbacks_receive_callsite_arg_count(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let counters = Arc::new(CallbackCounters::default());
    let conn = tmp_db.connect_limbo();
    conn.register_external_aggregate_function(
        "managed_variadic_sum".to_string(),
        -1,
        true,
        boxed_aggregate_context(counters.clone()),
        managed_sum_init,
        managed_variadic_sum_step,
        managed_sum_final,
        Some(drop_aggregate_context),
        Some(drop_sum_state),
        None,
    );
    conn.execute("CREATE TABLE variadic_items(first INTEGER, second INTEGER)")?;
    conn.execute("INSERT INTO variadic_items VALUES (1, 2), (3, 4)")?;

    let sum: Vec<(i64,)> =
        conn.exec_rows("SELECT managed_variadic_sum(first, second) FROM variadic_items");
    assert_eq!(sum, vec![(14,)]);

    let empty: Vec<(i64,)> =
        conn.exec_rows("SELECT managed_variadic_sum(first) FROM variadic_items WHERE 0");
    assert_eq!(empty, vec![(0,)]);

    conn.unregister_external_function("managed_variadic_sum");
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(
        counters.aggregate_inits.load(AtomicOrdering::SeqCst),
        counters.aggregate_drops.load(AtomicOrdering::SeqCst)
    );
    Ok(())
}

#[turso_macros::test]
#[serial]
fn managed_aggregate_errors_leave_connection_usable(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let counters = Arc::new(CallbackCounters::default());
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE aggregate_errors(value INTEGER)")?;
    conn.execute("INSERT INTO aggregate_errors VALUES (1)")?;

    conn.register_external_aggregate_function(
        "managed_step_fail".to_string(),
        1,
        true,
        boxed_aggregate_context(counters.clone()),
        managed_sum_init,
        managed_step_fail,
        managed_sum_final,
        Some(drop_aggregate_context),
        Some(drop_sum_state),
        None,
    );
    let err = conn
        .execute("SELECT managed_step_fail(value) FROM aggregate_errors")
        .unwrap_err();
    assert!(err.to_string().contains("aggregate step failure"));
    let usable: Vec<(i64,)> = conn.exec_rows("SELECT 1");
    assert_eq!(usable, vec![(1,)]);
    conn.unregister_external_function("managed_step_fail");

    conn.register_external_aggregate_function(
        "managed_final_fail".to_string(),
        1,
        true,
        boxed_aggregate_context(counters.clone()),
        managed_sum_init,
        managed_sum_step,
        managed_final_fail,
        Some(drop_aggregate_context),
        Some(drop_sum_state),
        None,
    );
    let err = conn
        .execute("SELECT managed_final_fail(value) FROM aggregate_errors")
        .unwrap_err();
    assert!(err.to_string().contains("aggregate final failure"));
    let usable: Vec<(i64,)> = conn.exec_rows("SELECT 1");
    assert_eq!(usable, vec![(1,)]);
    conn.unregister_external_function("managed_final_fail");

    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 2);
    Ok(())
}

unsafe extern "C" fn dotnet_nocase_collation(
    _context: usize,
    left_ptr: *const u8,
    left_len: usize,
    right_ptr: *const u8,
    right_len: usize,
) -> i32 {
    let left = unsafe { std::slice::from_raw_parts(left_ptr, left_len) };
    let right = unsafe { std::slice::from_raw_parts(right_ptr, right_len) };
    let left = String::from_utf8_lossy(left).to_ascii_lowercase();
    let right = String::from_utf8_lossy(right).to_ascii_lowercase();
    match left.cmp(&right) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

#[turso_macros::test]
#[serial]
fn custom_collations_cover_dotnet_create_collation_cases(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.register_external_collation(
        "dotnet_nocase".to_string(),
        0,
        dotnet_nocase_collation,
        None,
    );
    conn.execute("CREATE TABLE names(value TEXT)")?;
    conn.execute("INSERT INTO names VALUES ('beta'), ('ALPHA'), ('alpha'), ('Gamma')")?;

    let ordered: Vec<(String,)> =
        conn.exec_rows("SELECT value FROM names ORDER BY value COLLATE dotnet_nocase, value");
    assert_eq!(
        ordered,
        vec![
            ("ALPHA".to_string(),),
            ("alpha".to_string(),),
            ("beta".to_string(),),
            ("Gamma".to_string(),)
        ]
    );

    let equal_rows: Vec<(String,)> = conn.exec_rows(
        "SELECT value FROM names WHERE value = 'ALPHA' COLLATE dotnet_nocase ORDER BY value",
    );
    assert_eq!(
        equal_rows,
        vec![("ALPHA".to_string(),), ("alpha".to_string(),)]
    );
    let min_max: Vec<(String, String)> = conn.exec_rows(
        "SELECT min(value COLLATE dotnet_nocase), max(value COLLATE dotnet_nocase) FROM names",
    );
    assert_eq!(min_max, vec![("ALPHA".to_string(), "Gamma".to_string())]);

    let err = conn
        .execute("CREATE TABLE bad(value TEXT COLLATE dotnet_nocase)")
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("custom collations are not supported"));
    let err = conn
        .execute("CREATE INDEX bad_idx ON names(value COLLATE dotnet_nocase)")
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("custom collations are not supported"));

    conn.execute("CREATE TABLE left_values(value TEXT)")?;
    conn.execute("CREATE TABLE right_values(value TEXT)")?;
    for id in 0..100 {
        let key = format!("key{:02}", id % 10);
        conn.execute(format!("INSERT INTO left_values VALUES ('{key}')"))?;
        conn.execute(format!("INSERT INTO right_values VALUES ('{key}')"))?;
    }
    let binary_join_sql =
        "SELECT count(*) FROM left_values AS l JOIN right_values AS r ON l.value = r.value";
    let explain_rows = limbo_exec_rows(&conn, &format!("EXPLAIN {binary_join_sql}"));
    let has_hash = explain_rows.iter().any(|row| {
        row.get(1).is_some_and(|value| {
            matches!(value, SqliteValue::Text(op) if op == "HashBuild" || op == "HashProbe")
        })
    });
    assert!(has_hash, "expected equivalent binary join to use hash join");

    conn.execute("DELETE FROM right_values")?;
    for id in 0..100 {
        let key = format!("KEY{:02}", id % 10);
        conn.execute(format!("INSERT INTO right_values VALUES ('{key}')"))?;
    }
    let join_sql = "SELECT count(*) FROM left_values AS l JOIN right_values AS r \
        ON l.value = r.value COLLATE dotnet_nocase";
    let joined: Vec<(i64,)> = conn.exec_rows(join_sql);
    assert_eq!(joined, vec![(1000,)]);
    let explain_rows = limbo_exec_rows(&conn, &format!("EXPLAIN {join_sql}"));
    let has_hash = explain_rows.iter().any(|row| {
        row.get(1).is_some_and(|value| {
            matches!(value, SqliteValue::Text(op) if op == "HashBuild" || op == "HashProbe")
        })
    });
    assert!(
        !has_hash,
        "custom collations must not use binary-hashed joins"
    );

    let other_conn = tmp_db.connect_limbo();
    other_conn.register_external_collation(
        "dotnet_nocase".to_string(),
        0,
        dotnet_nocase_collation,
        None,
    );
    conn.unregister_external_collation("dotnet_nocase");
    let err = conn
        .execute("SELECT 'ok' WHERE 'A' = 'a' COLLATE dotnet_nocase")
        .unwrap_err();
    assert!(err.to_string().contains("no such collation sequence"));
    let rows: Vec<(String,)> =
        other_conn.exec_rows("SELECT 'ok' WHERE 'A' = 'a' COLLATE dotnet_nocase");
    assert_eq!(rows, vec![("ok".to_string(),)]);
    other_conn.unregister_external_collation("dotnet_nocase");

    Ok(())
}

#[turso_macros::test]
fn extension_loading_flag_covers_dotnet_enable_extensions_cases(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let err = conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("runtime extension loading is disabled"));

    let err = conn
        .load_extension("definitely_missing_extension")
        .unwrap_err();
    assert!(matches!(err, LimboError::ExtensionError(_)));
    assert!(!err
        .to_string()
        .contains("runtime extension loading is disabled"));

    conn.set_load_extension_enabled(true);
    let err = conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(matches!(err, LimboError::ExtensionError(_)));
    assert!(err.to_string().contains("Extension file not found"));

    conn.set_load_extension_enabled(false);
    let err = conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("runtime extension loading is disabled"));

    let other_conn = tmp_db.connect_limbo();
    let err = other_conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("runtime extension loading is disabled"));
    Ok(())
}
