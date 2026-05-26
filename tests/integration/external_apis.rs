use crate::common::{limbo_exec_rows, ExecRows, TempDatabase};
use rusqlite::types::Value as SqliteValue;
use serial_test::serial;
use std::{
    ffi::CString,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    },
};
use turso_core::{Connection, LimboError, StepResult};
use turso_ext::{
    ContextDestructor, ResultCode, ScalarFunction, Value as ExtValue, ValueDestructor,
    ValueType as ExtValueType,
};

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

#[derive(Default)]
struct CallbackCounters {
    calls: AtomicUsize,
    context_drops: AtomicUsize,
}

struct ScalarContext {
    multiplier: i64,
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
