use crate::common::{limbo_exec_rows, ExecRows, TempDatabase};
use rusqlite::types::Value as SqliteValue;
use serial_test::serial;
use std::{
    ffi::CString,
    os::raw::c_void,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    },
};
use turso_core::{Connection, LimboError, StepResult};
use turso_ext::{
    AggCtx, ContextDestructor, FinalizeFunction, InitAggFunction, ResultCode, ScalarDerive,
    ScalarFunc, ScalarFunction, StepFunction, Value as ExtValue, ValueDestructor,
    ValueType as ExtValueType,
};

static CTX_CALL_COUNT: AtomicUsize = AtomicUsize::new(0);
static CTX_DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

struct MultiplierState {
    multiplier: i64,
}

impl Drop for MultiplierState {
    fn drop(&mut self) {
        CTX_DROP_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
    }
}

#[derive(ScalarDerive)]
struct CtxMultiply;

impl ScalarFunc for CtxMultiply {
    type State = MultiplierState;
    const NAME: &'static str = "ctx_multiply";

    fn init() -> Self::State {
        MultiplierState { multiplier: 3 }
    }

    fn call(state: &Self::State, args: &[ExtValue]) -> ExtValue {
        CTX_CALL_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
        let n = args
            .first()
            .and_then(ExtValue::to_integer)
            .unwrap_or_default();
        ExtValue::from_integer(n * state.multiplier)
    }
}

#[turso_macros::test]
fn scalar_derive_state_is_shared_across_calls_and_dropped_once(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    CTX_CALL_COUNT.store(0, AtomicOrdering::SeqCst);
    CTX_DROP_COUNT.store(0, AtomicOrdering::SeqCst);

    let conn = tmp_db.connect_limbo();

    // Register via the derive-generated entry point against this connection.
    let api = unsafe { conn._build_turso_ext() };
    let rc = unsafe { register_CtxMultiply(&api as *const _) };
    unsafe { conn._free_extension_ctx(api) };
    assert_eq!(rc, ResultCode::OK);

    // State (multiplier = 3) is applied, and shared across multiple calls.
    let first: Vec<(i64,)> = conn.exec_rows("SELECT ctx_multiply(5)");
    assert_eq!(first, vec![(15,)]);
    let second: Vec<(i64,)> = conn.exec_rows("SELECT ctx_multiply(10)");
    assert_eq!(second, vec![(30,)]);
    assert_eq!(CTX_CALL_COUNT.load(AtomicOrdering::SeqCst), 2);

    // No destructor has fired while the registration is still live.
    assert_eq!(CTX_DROP_COUNT.load(AtomicOrdering::SeqCst), 0);

    // Dropping the connection drops its symbol table, which runs the state
    // destructor exactly once.
    drop(conn);
    assert_eq!(CTX_DROP_COUNT.load(AtomicOrdering::SeqCst), 1);
    Ok(())
}

#[turso_macros::test]
fn sql_extension_loading_is_disabled_by_default(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let err = conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("runtime extension loading is disabled"));
    Ok(())
}

#[turso_macros::test]
fn sql_extension_loading_flag_is_per_connection(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let enabled_conn = tmp_db.connect_limbo();
    let disabled_conn = tmp_db.connect_limbo();

    enabled_conn.set_load_extension_enabled(true);
    let err = enabled_conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(matches!(err, LimboError::ExtensionError(_)));
    assert!(err.to_string().contains("Extension file not found"));

    let err = disabled_conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("runtime extension loading is disabled"));

    enabled_conn.set_load_extension_enabled(false);
    let err = enabled_conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("runtime extension loading is disabled"));
    Ok(())
}

#[turso_macros::test]
fn direct_connection_extension_loading_bypasses_sql_flag(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let err = conn
        .load_extension("definitely_missing_extension")
        .unwrap_err();

    assert!(matches!(err, LimboError::ExtensionError(_)));
    assert!(!err
        .to_string()
        .contains("runtime extension loading is disabled"));
    Ok(())
}

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

unsafe extern "C" fn managed_score(
    context: usize,
    argc: i32,
    argv: *const ExtValue,
    _context_destructor: Option<ContextDestructor>,
    _value_destructor: Option<ValueDestructor>,
) -> ExtValue {
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
    let null_count = args
        .iter()
        .filter(|arg| arg.value_type() == ExtValueType::Null)
        .count() as i64;

    ExtValue::from_integer(
        (int_value + float_value + text_len + blob_len + null_count) * ctx.multiplier,
    )
}

unsafe extern "C" fn managed_result(
    context: usize,
    argc: i32,
    argv: *const ExtValue,
    _context_destructor: Option<ContextDestructor>,
    _value_destructor: Option<ValueDestructor>,
) -> ExtValue {
    let ctx = unsafe { &*(context as *const ScalarContext) };
    ctx.counters.calls.fetch_add(1, AtomicOrdering::SeqCst);
    let args = if argc <= 0 || argv.is_null() {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(argv, argc as usize) }
    };
    let mode = args.first().and_then(ExtValue::to_text).unwrap_or_default();

    match mode {
        "null" => ExtValue::null(),
        "text" => ExtValue::from_text("managed-text".to_string()),
        "blob" => ExtValue::from_blob(vec![0x01, 0x02, 0xFE]),
        "float" => ExtValue::from_float(3.25),
        "error" => ExtValue::error_with_message("managed failure".to_string()),
        _ => ExtValue::error_with_message("unexpected mode".to_string()),
    }
}

unsafe extern "C" fn managed_variadic_score(
    context: usize,
    argc: i32,
    argv: *const ExtValue,
    _context_destructor: Option<ContextDestructor>,
    _value_destructor: Option<ValueDestructor>,
) -> ExtValue {
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
    ExtValue::from_integer(score * ctx.multiplier)
}

unsafe extern "C" fn drop_scalar_context(context: usize) {
    let context = unsafe { Box::from_raw(context as *mut ScalarContext) };
    context
        .counters
        .context_drops
        .fetch_add(1, AtomicOrdering::SeqCst);
}

unsafe extern "C" fn count_scalar_value_drop(result: *mut ExtValue) {
    if !result.is_null() {
        unsafe { std::ptr::read(result).__free_internal_type() };
    }
    SCALAR_VALUE_DROPS.fetch_add(1, AtomicOrdering::SeqCst);
}

fn boxed_scalar_context(multiplier: i64, counters: Arc<CallbackCounters>) -> usize {
    Box::into_raw(Box::new(ScalarContext {
        multiplier,
        counters,
    })) as usize
}

#[allow(clippy::too_many_arguments)]
fn register_context_scalar(
    conn: &Connection,
    name: &str,
    argc: i32,
    deterministic: bool,
    context: usize,
    callback: ScalarFunction,
    context_destructor: Option<ContextDestructor>,
    value_destructor: Option<ValueDestructor>,
) -> anyhow::Result<()> {
    let name = CString::new(name)?;
    let api = unsafe { conn._build_turso_ext() };
    let result = unsafe {
        (api.register_scalar_function)(
            api.ctx,
            name.as_ptr(),
            argc,
            deterministic,
            context,
            callback,
            context_destructor,
            value_destructor,
        )
    };
    unsafe { conn._free_extension_ctx(api) };
    if result != ResultCode::OK {
        anyhow::bail!("managed scalar registration failed: {result}");
    }
    Ok(())
}

fn unregister_extension_function(conn: &Connection, name: &str) -> anyhow::Result<()> {
    let name = CString::new(name)?;
    let api = unsafe { conn._build_turso_ext() };
    let result = unsafe { (api.unregister_function)(api.ctx, name.as_ptr()) };
    unsafe { conn._free_extension_ctx(api) };
    if result != ResultCode::OK {
        anyhow::bail!("extension function unregister failed: {result}");
    }
    Ok(())
}

unsafe extern "C" fn managed_sum_init(context: usize) -> *mut AggCtx {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_inits
        .fetch_add(1, AtomicOrdering::SeqCst);
    let state = Box::into_raw(Box::new(SumState {
        sum: 0,
        counters: ctx.counters.clone(),
    }));
    Box::into_raw(Box::new(AggCtx {
        state: state as *mut c_void,
    }))
}

unsafe fn sum_state<'a>(aggregate_context: *mut AggCtx) -> &'a mut SumState {
    let aggregate_context = unsafe { &mut *aggregate_context };
    unsafe { &mut *(aggregate_context.state as *mut SumState) }
}

unsafe extern "C" fn managed_sum_step(
    context: usize,
    aggregate_context: *mut AggCtx,
    argc: i32,
    argv: *const ExtValue,
) -> ExtValue {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_steps
        .fetch_add(1, AtomicOrdering::SeqCst);
    let state = unsafe { sum_state(aggregate_context) };
    if argc > 0 && !argv.is_null() {
        let args = unsafe { std::slice::from_raw_parts(argv, argc as usize) };
        state.sum += args
            .first()
            .and_then(ExtValue::to_integer)
            .unwrap_or_default();
    }
    ExtValue::null()
}

unsafe extern "C" fn managed_variadic_sum_step(
    context: usize,
    aggregate_context: *mut AggCtx,
    argc: i32,
    argv: *const ExtValue,
) -> ExtValue {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_steps
        .fetch_add(1, AtomicOrdering::SeqCst);
    let state = unsafe { sum_state(aggregate_context) };
    state.sum += argc as i64;
    if argc > 0 && !argv.is_null() {
        let args = unsafe { std::slice::from_raw_parts(argv, argc as usize) };
        state.sum += args.iter().filter_map(ExtValue::to_integer).sum::<i64>();
    }
    ExtValue::null()
}

unsafe extern "C" fn managed_step_fail(
    context: usize,
    _aggregate_context: *mut AggCtx,
    _argc: i32,
    _argv: *const ExtValue,
) -> ExtValue {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_steps
        .fetch_add(1, AtomicOrdering::SeqCst);
    ExtValue::error_with_message("aggregate step failure".to_string())
}

unsafe extern "C" fn managed_sum_final(context: usize, aggregate_context: *mut AggCtx) -> ExtValue {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_finals
        .fetch_add(1, AtomicOrdering::SeqCst);
    let state = unsafe { sum_state(aggregate_context) };
    ExtValue::from_integer(state.sum)
}

unsafe extern "C" fn managed_final_fail(
    context: usize,
    _aggregate_context: *mut AggCtx,
) -> ExtValue {
    let ctx = unsafe { &*(context as *const AggregateContext) };
    ctx.counters
        .aggregate_finals
        .fetch_add(1, AtomicOrdering::SeqCst);
    ExtValue::error_with_message("aggregate final failure".to_string())
}

unsafe extern "C" fn drop_aggregate_context(context: usize) {
    let context = unsafe { Box::from_raw(context as *mut AggregateContext) };
    context
        .counters
        .context_drops
        .fetch_add(1, AtomicOrdering::SeqCst);
}

unsafe extern "C" fn drop_sum_state(aggregate_context: usize) {
    let aggregate_context = unsafe { Box::from_raw(aggregate_context as *mut AggCtx) };
    let state = unsafe { Box::from_raw(aggregate_context.state as *mut SumState) };
    state
        .counters
        .aggregate_drops
        .fetch_add(1, AtomicOrdering::SeqCst);
}

unsafe extern "C" fn count_aggregate_value_drop(result: *mut ExtValue) {
    if !result.is_null() {
        unsafe { std::ptr::read(result).__free_internal_type() };
    }
    AGG_VALUE_DROPS.fetch_add(1, AtomicOrdering::SeqCst);
}

fn boxed_aggregate_context(counters: Arc<CallbackCounters>) -> usize {
    Box::into_raw(Box::new(AggregateContext { counters })) as usize
}

#[allow(clippy::too_many_arguments)]
fn register_context_aggregate(
    conn: &Connection,
    name: &str,
    argc: i32,
    context: usize,
    init: InitAggFunction,
    step: StepFunction,
    finalize: FinalizeFunction,
    context_destructor: Option<ContextDestructor>,
    aggregate_destructor: Option<ContextDestructor>,
    value_destructor: Option<ValueDestructor>,
) -> anyhow::Result<()> {
    let name = CString::new(name)?;
    let api = unsafe { conn._build_turso_ext() };
    let result = unsafe {
        (api.register_aggregate_function)(
            api.ctx,
            name.as_ptr(),
            argc,
            context,
            init,
            step,
            finalize,
            context_destructor,
            aggregate_destructor,
            value_destructor,
        )
    };
    unsafe { conn._free_extension_ctx(api) };
    if result != ResultCode::OK {
        anyhow::bail!("managed aggregate registration failed: {result}");
    }
    Ok(())
}

#[turso_macros::test]
#[serial]
fn managed_scalar_callbacks_cover_fixed_args_metadata_and_invalidation(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let counters = Arc::new(CallbackCounters::default());
    let conn = tmp_db.connect_limbo();

    register_context_scalar(
        &conn,
        "managed_score",
        5,
        true,
        boxed_scalar_context(1, counters.clone()),
        managed_score,
        Some(drop_scalar_context),
        None,
    )?;

    let score: Vec<(i64,)> = conn.exec_rows("SELECT managed_score(2, 3.5, 'hi', x'010203', NULL)");
    assert_eq!(score, vec![(11,)]);

    let function_list: Vec<(String, i64, String, String, i64, i64)> =
        conn.exec_rows("PRAGMA function_list");
    let managed_score_metadata = function_list
        .iter()
        .find(|(name, _, _, _, _, _)| name == "managed_score")
        .expect("managed_score should be listed");
    assert_eq!(managed_score_metadata.2, "s");
    assert_eq!(managed_score_metadata.4, 5);
    assert_ne!(managed_score_metadata.5 & 0x800, 0);

    let mut prepared = conn.prepare("SELECT managed_score(1, 2.0, 'a', x'00', NULL)")?;
    register_context_scalar(
        &conn,
        "managed_score",
        5,
        true,
        boxed_scalar_context(10, counters.clone()),
        managed_score,
        Some(drop_scalar_context),
        None,
    )?;
    match prepared.step()? {
        StepResult::Row => {}
        other => panic!("expected row from managed_score, got {other:?}"),
    }
    assert_eq!(
        prepared
            .row()
            .expect("row should be available after StepResult::Row")
            .get::<i64>(0)?,
        60
    );
    drop(prepared);
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 1);

    assert!(conn.prepare("SELECT managed_score(1)").is_err());
    unregister_extension_function(&conn, "managed_score")?;
    let err = conn
        .prepare("SELECT managed_score(1, 2.0, 'a', x'00', NULL)")
        .unwrap_err();
    assert!(err.to_string().contains("no such function"));
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(counters.calls.load(AtomicOrdering::SeqCst), 2);
    Ok(())
}

#[turso_macros::test]
#[serial]
fn managed_scalar_callbacks_convert_results_and_propagate_errors(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    SCALAR_VALUE_DROPS.store(0, AtomicOrdering::SeqCst);
    let counters = Arc::new(CallbackCounters::default());
    let conn = tmp_db.connect_limbo();

    register_context_scalar(
        &conn,
        "managed_result",
        1,
        false,
        boxed_scalar_context(1, counters.clone()),
        managed_result,
        Some(drop_scalar_context),
        Some(count_scalar_value_drop),
    )?;

    assert_eq!(
        limbo_exec_rows(&conn, "SELECT managed_result('null')"),
        vec![vec![SqliteValue::Null]]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT managed_result('text')"),
        vec![vec![SqliteValue::Text("managed-text".to_string())]]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT managed_result('blob')"),
        vec![vec![SqliteValue::Blob(vec![0x01, 0x02, 0xFE])]]
    );
    let float_value: Vec<(f64,)> = conn.exec_rows("SELECT managed_result('float')");
    assert_eq!(float_value, vec![(3.25,)]);

    let err = conn.execute("SELECT managed_result('error')").unwrap_err();
    assert!(matches!(err, LimboError::ExtensionError(_)));
    assert!(err.to_string().contains("managed failure"));
    assert_eq!(SCALAR_VALUE_DROPS.load(AtomicOrdering::SeqCst), 5);

    unregister_extension_function(&conn, "managed_result")?;
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(counters.calls.load(AtomicOrdering::SeqCst), 5);
    Ok(())
}

#[turso_macros::test]
#[serial]
fn managed_scalar_variadic_callbacks_receive_callsite_arguments(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let counters = Arc::new(CallbackCounters::default());
    let conn = tmp_db.connect_limbo();

    register_context_scalar(
        &conn,
        "managed_variadic_score",
        -1,
        true,
        boxed_scalar_context(1, counters.clone()),
        managed_variadic_score,
        Some(drop_scalar_context),
        None,
    )?;

    let score: Vec<(i64,)> =
        conn.exec_rows("SELECT managed_variadic_score(1, 3.5, 'A', x'7E57', NULL)");
    assert_eq!(score, vec![(508,)]);

    let no_args: Vec<(i64,)> = conn.exec_rows("SELECT managed_variadic_score()");
    assert_eq!(no_args, vec![(0,)]);

    unregister_extension_function(&conn, "managed_variadic_score")?;
    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(counters.calls.load(AtomicOrdering::SeqCst), 2);
    Ok(())
}

#[turso_macros::test]
#[serial]
fn managed_aggregate_callbacks_cover_grouping_distinct_filter_and_empty_sets(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    AGG_VALUE_DROPS.store(0, AtomicOrdering::SeqCst);
    let counters = Arc::new(CallbackCounters::default());
    let conn = tmp_db.connect_limbo();

    register_context_aggregate(
        &conn,
        "managed_sum",
        1,
        boxed_aggregate_context(counters.clone()),
        managed_sum_init,
        managed_sum_step,
        managed_sum_final,
        Some(drop_aggregate_context),
        Some(drop_sum_state),
        Some(count_aggregate_value_drop),
    )?;

    let function_list: Vec<(String, i64, String, String, i64, i64)> =
        conn.exec_rows("PRAGMA function_list");
    let managed_sum_metadata = function_list
        .iter()
        .find(|(name, _, _, _, _, _)| name == "managed_sum")
        .expect("managed_sum should be listed");
    assert_eq!(managed_sum_metadata.2, "a");
    assert_eq!(managed_sum_metadata.4, 1);
    assert_eq!(managed_sum_metadata.5 & 0x800, 0);

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

    unregister_extension_function(&conn, "managed_sum")?;
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

    register_context_aggregate(
        &conn,
        "managed_variadic_sum",
        -1,
        boxed_aggregate_context(counters.clone()),
        managed_sum_init,
        managed_variadic_sum_step,
        managed_sum_final,
        Some(drop_aggregate_context),
        Some(drop_sum_state),
        None,
    )?;
    conn.execute("CREATE TABLE variadic_items(first INTEGER, second INTEGER)")?;
    conn.execute("INSERT INTO variadic_items VALUES (1, 2), (3, 4)")?;

    let sum: Vec<(i64,)> =
        conn.exec_rows("SELECT managed_variadic_sum(first, second) FROM variadic_items");
    assert_eq!(sum, vec![(14,)]);

    let empty: Vec<(i64,)> =
        conn.exec_rows("SELECT managed_variadic_sum(first) FROM variadic_items WHERE 0");
    assert_eq!(empty, vec![(0,)]);

    unregister_extension_function(&conn, "managed_variadic_sum")?;
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

    register_context_aggregate(
        &conn,
        "managed_step_fail",
        1,
        boxed_aggregate_context(counters.clone()),
        managed_sum_init,
        managed_step_fail,
        managed_sum_final,
        Some(drop_aggregate_context),
        Some(drop_sum_state),
        None,
    )?;
    let err = conn
        .execute("SELECT managed_step_fail(value) FROM aggregate_errors")
        .unwrap_err();
    assert!(err.to_string().contains("aggregate step failure"));
    let usable: Vec<(i64,)> = conn.exec_rows("SELECT 1");
    assert_eq!(usable, vec![(1,)]);
    unregister_extension_function(&conn, "managed_step_fail")?;

    register_context_aggregate(
        &conn,
        "managed_final_fail",
        1,
        boxed_aggregate_context(counters.clone()),
        managed_sum_init,
        managed_sum_step,
        managed_final_fail,
        Some(drop_aggregate_context),
        Some(drop_sum_state),
        None,
    )?;
    let err = conn
        .execute("SELECT managed_final_fail(value) FROM aggregate_errors")
        .unwrap_err();
    assert!(err.to_string().contains("aggregate final failure"));
    let usable: Vec<(i64,)> = conn.exec_rows("SELECT 1");
    assert_eq!(usable, vec![(1,)]);
    unregister_extension_function(&conn, "managed_final_fail")?;

    assert_eq!(counters.context_drops.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(
        counters.aggregate_inits.load(AtomicOrdering::SeqCst),
        counters.aggregate_drops.load(AtomicOrdering::SeqCst)
    );
    Ok(())
}
