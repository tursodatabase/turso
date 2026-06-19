use std::{
    any::Any,
    mem::ManuallyDrop,
    path::{Path, PathBuf},
    sync::Arc,
};

use rusqlite::types::Value as SqlValue;
use turso_core::{
    Connection, Database, IO, LimboError, Numeric, OpenFlags, PlatformIO, Statement, Value,
};

use crate::{
    model::{
        ResultSet,
        fts::{
            FtsFeatureTag, FtsLimitPrefixOracle, FtsOracleCheck, FtsOracleKind, FtsSchemaSnapshot,
            FtsTableSnapshot, fts_feature_tag_list,
        },
    },
    runner::env::{SimConnection, SimulatorEnv, simulator_database_opts},
};

enum FuzzerStatementRun<T> {
    Ok(T),
    Error(LimboError),
    Panic(Box<dyn Any + Send>),
}

#[derive(Debug, Clone, PartialEq)]
enum FtsQueryOutcome {
    Rows(Vec<Vec<SqlValue>>),
    Error,
}

struct FtsTempDatabase {
    _dir: Option<tempfile::TempDir>,
    db: Arc<Database>,
}

impl FtsTempDatabase {
    fn new() -> turso_core::Result<Self> {
        let dir = tempfile::tempdir().map_err(|err| LimboError::InternalError(err.to_string()))?;
        let path = dir.path().join(format!("fts-{}.db", rand::random::<u64>()));
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        Self::open(path, Some(dir), io)
    }

    fn open_existing(path: impl AsRef<Path>, io: Arc<dyn IO>) -> turso_core::Result<Self> {
        Self::open(path.as_ref().to_path_buf(), None, io)
    }

    fn open(
        path: PathBuf,
        dir: Option<tempfile::TempDir>,
        io: Arc<dyn IO>,
    ) -> turso_core::Result<Self> {
        let db = Database::open_file_with_flags(
            io,
            path.to_str()
                .ok_or_else(|| LimboError::InvalidArgument("non-UTF8 database path".to_string()))?,
            OpenFlags::default(),
            simulator_database_opts(true),
            None,
        )?;
        Ok(Self { _dir: dir, db })
    }

    fn connect(&self) -> turso_core::Result<Arc<Connection>> {
        self.db.connect()
    }
}

pub(crate) fn execute_fts_sql(conn: &Arc<Connection>, sql: &str) -> ResultSet {
    let rows = conn.query(sql)?;
    let Some(mut rows) = rows else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();

    rows.run_with_row_callback(|row| {
        let mut values = Vec::new();
        for value in row.get_values() {
            values.push(value.into());
        }
        out.push(values);
        Ok(())
    })?;

    Ok(out)
}

pub(crate) fn execute_fts_oracle(
    env: &mut SimulatorEnv,
    conn_index: usize,
    check: &FtsOracleCheck,
) -> turso_core::Result<()> {
    let source_conn = match &env.connections[conn_index] {
        SimConnection::LimboConnection(conn) => conn.clone(),
        SimConnection::SQLiteConnection(_) => {
            return Err(LimboError::InternalError(
                "FTS oracle cannot run against SQLite".to_string(),
            ));
        }
        SimConnection::Disconnected => unreachable!(),
    };

    let actual = query_outcome(&source_conn, &check.verification_sql);

    if let Some(limit_prefix) = &check.limit_prefix {
        let expected = limit_prefix_expected(&source_conn, limit_prefix);
        if actual != expected {
            return Err(oracle_failure(
                FtsOracleKind::LimitPrefix,
                check,
                &expected,
                &actual,
            ));
        }
    }

    if check.rebuild {
        let expected = rebuild_outcome(&check.schema, &source_conn, true, &check.verification_sql);
        if actual != expected {
            return Err(oracle_failure(
                FtsOracleKind::Rebuild,
                check,
                &expected,
                &actual,
            ));
        }
    }

    if check.scalar {
        let expected = rebuild_outcome(&check.schema, &source_conn, false, &check.verification_sql);
        if actual != expected {
            return Err(oracle_failure(
                FtsOracleKind::Scalar,
                check,
                &expected,
                &actual,
            ));
        }
    }

    if check.reopen {
        let io: Arc<dyn IO> = env.io.clone();
        let expected = reopen_outcome(&env.get_db_path(), io, &check.verification_sql);
        if actual != expected {
            return Err(oracle_failure(
                FtsOracleKind::Reopen,
                check,
                &expected,
                &actual,
            ));
        }
    }

    Ok(())
}

fn limit_prefix_expected(
    conn: &Arc<Connection>,
    limit_prefix: &FtsLimitPrefixOracle,
) -> FtsQueryOutcome {
    let full = query_outcome(conn, &limit_prefix.full_sql);
    let FtsQueryOutcome::Rows(full_rows) = full else {
        return full;
    };
    FtsQueryOutcome::Rows(
        full_rows
            .into_iter()
            .skip(limit_prefix.offset)
            .take(limit_prefix.limit)
            .collect(),
    )
}

fn reopen_outcome(path: &Path, io: Arc<dyn IO>, sql: &str) -> FtsQueryOutcome {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        FtsTempDatabase::open_existing(path, io)
    })) {
        Ok(Ok(db)) => match db.connect() {
            Ok(conn) => query_outcome(&conn, sql),
            Err(_) => FtsQueryOutcome::Error,
        },
        Ok(Err(_)) => FtsQueryOutcome::Error,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn rebuild_outcome(
    schema: &FtsSchemaSnapshot,
    source_conn: &Arc<Connection>,
    include_fts_indexes: bool,
    verification_sql: &str,
) -> FtsQueryOutcome {
    let Ok(rebuild_db) = FtsTempDatabase::new() else {
        return FtsQueryOutcome::Error;
    };
    let Ok(rebuild_conn) = rebuild_db.connect() else {
        return FtsQueryOutcome::Error;
    };

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rebuild_schema(schema, source_conn, &rebuild_conn, include_fts_indexes)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return FtsQueryOutcome::Error,
        Err(payload) => std::panic::resume_unwind(payload),
    }

    query_outcome(&rebuild_conn, verification_sql)
}

fn rebuild_schema(
    schema: &FtsSchemaSnapshot,
    source_conn: &Arc<Connection>,
    rebuild_conn: &Arc<Connection>,
    include_fts_indexes: bool,
) -> Result<(), String> {
    for table in &schema.tables {
        exec_ignore_rows(rebuild_conn, &table.create_sql).map_err(|err| err.to_string())?;
        let rows =
            exec_rows(source_conn, &select_table_rows_sql(table)).map_err(|err| err.to_string())?;
        for row in rows {
            exec_ignore_rows(rebuild_conn, insert_row_sql(table, &row))
                .map_err(|err| err.to_string())?;
        }
    }

    for index in &schema.indexes {
        if index.is_fts && !include_fts_indexes {
            continue;
        }
        exec_ignore_rows(rebuild_conn, &index.create_sql).map_err(|err| err.to_string())?;
    }

    Ok(())
}

fn select_table_rows_sql(table: &FtsTableSnapshot) -> String {
    format!(
        "SELECT {} FROM {} ORDER BY rowid",
        table.columns.join(", "),
        table.qualified_name
    )
}

fn insert_row_sql(table: &FtsTableSnapshot, row: &[SqlValue]) -> String {
    format!(
        "INSERT INTO {} ({}) VALUES({})",
        table.qualified_name,
        table.columns.join(", "),
        row.iter()
            .map(sql_value_literal)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn exec_rows(conn: &Arc<Connection>, sql: &str) -> Result<Vec<Vec<SqlValue>>, LimboError> {
    match run_statement_collect_rows(conn, sql) {
        FuzzerStatementRun::Ok(rows) => Ok(rows),
        FuzzerStatementRun::Error(err) => Err(err),
        FuzzerStatementRun::Panic(payload) => std::panic::resume_unwind(payload),
    }
}

fn exec_ignore_rows(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<(), LimboError> {
    match run_statement_ignore_rows(conn, sql.as_ref()) {
        FuzzerStatementRun::Ok(()) => Ok(()),
        FuzzerStatementRun::Error(err) => Err(err),
        FuzzerStatementRun::Panic(payload) => std::panic::resume_unwind(payload),
    }
}

fn query_outcome(conn: &Arc<Connection>, sql: &str) -> FtsQueryOutcome {
    query_outcome_from_run(run_statement_collect_rows(conn, sql))
}

fn query_outcome_from_run(run: FuzzerStatementRun<Vec<Vec<SqlValue>>>) -> FtsQueryOutcome {
    match run {
        FuzzerStatementRun::Ok(rows) => FtsQueryOutcome::Rows(rows),
        FuzzerStatementRun::Error(_) => FtsQueryOutcome::Error,
        FuzzerStatementRun::Panic(payload) => std::panic::resume_unwind(payload),
    }
}

fn run_statement_ignore_rows(conn: &Arc<Connection>, sql: &str) -> FuzzerStatementRun<()> {
    let mut stmt =
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| conn.prepare(sql))) {
            Ok(Ok(stmt)) => ManuallyDrop::new(stmt),
            Ok(Err(err)) => return FuzzerStatementRun::Error(err),
            Err(payload) => return FuzzerStatementRun::Panic(payload),
        };

    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stmt.run_ignore_rows()));
    let run_completed = matches!(run, Ok(Ok(())));
    finish_statement(stmt, run_completed);

    match run {
        Ok(Ok(())) => FuzzerStatementRun::Ok(()),
        Ok(Err(err)) => FuzzerStatementRun::Error(err),
        Err(payload) => FuzzerStatementRun::Panic(payload),
    }
}

fn run_statement_collect_rows(
    conn: &Arc<Connection>,
    sql: &str,
) -> FuzzerStatementRun<Vec<Vec<SqlValue>>> {
    let mut stmt =
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| conn.prepare(sql))) {
            Ok(Ok(stmt)) => ManuallyDrop::new(stmt),
            Ok(Err(err)) => return FuzzerStatementRun::Error(err),
            Err(payload) => return FuzzerStatementRun::Panic(payload),
        };
    let mut rows = Vec::new();

    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        stmt.run_with_row_callback(|row| {
            let row = row.get_values().map(sql_value_from_limbo_value).collect();
            rows.push(row);
            Ok(())
        })
    }));
    let run_completed = matches!(run, Ok(Ok(())));
    finish_statement(stmt, run_completed);

    match run {
        Ok(Ok(())) => FuzzerStatementRun::Ok(rows),
        Ok(Err(err)) => FuzzerStatementRun::Error(err),
        Err(payload) => FuzzerStatementRun::Panic(payload),
    }
}

fn finish_statement(mut stmt: ManuallyDrop<Statement>, run_completed: bool) {
    if run_completed
        || matches!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stmt.reset())),
            Ok(Ok(()))
        )
    {
        // SAFETY: the statement is manually dropped exactly once, after it either
        // completed normally or reset successfully to a droppable state.
        unsafe {
            ManuallyDrop::drop(&mut stmt);
        }
    }
    // If reset fails after an interrupted VM run, dropping can obscure the original
    // fuzzer failure with cleanup fallout. Leak this statement and preserve the
    // original panic/error path.
}

fn sql_value_from_limbo_value(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Numeric(Numeric::Integer(value)) => SqlValue::Integer(*value),
        Value::Numeric(Numeric::Float(value)) => SqlValue::Real(f64::from(*value)),
        Value::Text(value) => SqlValue::Text(value.as_str().to_string()),
        Value::Blob(value) => SqlValue::Blob(value.to_vec()),
    }
}

fn sql_value_literal(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(value) => value.to_string(),
        SqlValue::Real(value) if value.is_finite() => value.to_string(),
        SqlValue::Real(_) => "NULL".to_string(),
        SqlValue::Text(value) => format!("'{}'", value.replace('\'', "''")),
        SqlValue::Blob(bytes) => {
            let mut sql = String::from("X'");
            for byte in bytes {
                sql.push_str(&format!("{byte:02X}"));
            }
            sql.push('\'');
            sql
        }
    }
}

fn oracle_failure(
    kind: FtsOracleKind,
    check: &FtsOracleCheck,
    expected: &FtsQueryOutcome,
    actual: &FtsQueryOutcome,
) -> LimboError {
    LimboError::InternalError(format!(
        "FTS oracle failed: signature={} expected={} actual={} query={}",
        normalized_signature(kind, expected, actual, &check.tags),
        outcome_summary(expected),
        outcome_summary(actual),
        check.verification_sql,
    ))
}

fn normalized_signature(
    kind: FtsOracleKind,
    expected: &FtsQueryOutcome,
    actual: &FtsQueryOutcome,
    tags: &std::collections::BTreeSet<FtsFeatureTag>,
) -> String {
    format!(
        "{}:{}:{}:{}",
        kind.name(),
        outcome_signature(expected),
        outcome_signature(actual),
        fts_feature_tag_list(tags)
    )
}

fn outcome_signature(outcome: &FtsQueryOutcome) -> String {
    match outcome {
        FtsQueryOutcome::Rows(rows) => format!("rows:{}:{:016x}", rows.len(), rows_digest(rows)),
        FtsQueryOutcome::Error => "error".to_string(),
    }
}

fn outcome_summary(outcome: &FtsQueryOutcome) -> String {
    match outcome {
        FtsQueryOutcome::Rows(rows) => {
            let preview_rows = rows
                .iter()
                .take(3)
                .map(|row| row_summary(row))
                .collect::<Vec<_>>()
                .join(", ");
            let more_rows = if rows.len() > 3 {
                format!(", ... +{} rows", rows.len() - 3)
            } else {
                String::new()
            };
            format!(
                "Rows(row_count={}, digest=0x{:016x}, preview=[{}{}])",
                rows.len(),
                rows_digest(rows),
                preview_rows,
                more_rows
            )
        }
        FtsQueryOutcome::Error => "Error".to_string(),
    }
}

fn rows_digest(rows: &[Vec<SqlValue>]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for row in rows {
        hash = fnv1a(hash, b"[");
        for value in row {
            hash = fnv1a(hash, sql_value_summary(value).as_bytes());
            hash = fnv1a(hash, b",");
        }
        hash = fnv1a(hash, b"]");
    }
    hash
}

fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn row_summary(row: &[SqlValue]) -> String {
    let preview_cols = row
        .iter()
        .take(6)
        .map(sql_value_summary)
        .collect::<Vec<_>>()
        .join(", ");
    let more_cols = if row.len() > 6 {
        format!(", ... +{} cols", row.len() - 6)
    } else {
        String::new()
    };
    format!("[{preview_cols}{more_cols}]")
}

fn sql_value_summary(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(value) => value.to_string(),
        SqlValue::Real(value) => value.to_string(),
        SqlValue::Text(value) => {
            let mut preview = value.chars().take(80).collect::<String>();
            if value.chars().nth(80).is_some() {
                preview.push_str("...");
            }
            format!("{preview:?}")
        }
        SqlValue::Blob(bytes) => {
            let mut preview = String::new();
            for byte in bytes.iter().take(8) {
                preview.push_str(&format!("{byte:02X}"));
            }
            if bytes.len() > 8 {
                format!("X'{preview}'...({} bytes)", bytes.len())
            } else {
                format!("X'{preview}'")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_panic_is_not_a_comparable_query_outcome() {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            query_outcome_from_run(FuzzerStatementRun::Panic(Box::new(
                "fts verification panic",
            )));
        }));

        assert!(result.is_err());
    }
}
