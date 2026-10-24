use turso_core::{ConstraintKind, ConstraintViolation, LimboError, Value};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Error(Box<ErrorData>);

#[derive(Debug, Clone)]
struct ErrorData {
    report: ErrorReport,
    source: LimboError,
}

impl Error {
    pub fn core_error(&self) -> &LimboError {
        &self.0.source
    }

    pub fn aborted_transaction() -> Self {
        let message =
            "current transaction is aborted, commands ignored until end of transaction block"
                .to_string();
        Self(Box::new(ErrorData {
            report: ErrorReport {
                sqlstate: SQLSTATE_IN_FAILED_SQL_TRANSACTION,
                message: message.clone(),
                detail: None,
                table_name: None,
                constraint_name: None,
                column_name: None,
            },
            source: LimboError::InternalError(message),
        }))
    }

    pub fn read_write_mode_after_first_query() -> Self {
        let message = "transaction read-write mode must be set before any query".to_string();
        Self(Box::new(ErrorData {
            report: ErrorReport {
                sqlstate: SQLSTATE_ACTIVE_SQL_TRANSACTION,
                message: message.clone(),
                detail: None,
                table_name: None,
                constraint_name: None,
                column_name: None,
            },
            source: LimboError::TxError(message),
        }))
    }
}

impl std::ops::Deref for Error {
    type Target = ErrorReport;

    fn deref(&self) -> &Self::Target {
        &self.0.report
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0.source)
    }
}

impl From<LimboError> for Error {
    fn from(source: LimboError) -> Self {
        Self(Box::new(ErrorData {
            report: ErrorReport::from(&source),
            source,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct ErrorReport {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub table_name: Option<String>,
    pub constraint_name: Option<String>,
    pub column_name: Option<String>,
}

impl From<&LimboError> for ErrorReport {
    fn from(error: &LimboError) -> Self {
        let mut report = Self {
            sqlstate: sqlstate(error),
            message: error.to_string(),
            detail: None,
            table_name: None,
            constraint_name: None,
            column_name: None,
        };
        if let LimboError::ConstraintViolation(violation) = error {
            if let Some(table) = &violation.table_name {
                report.table_name = Some(table.clone());
                report.constraint_name = constraint_name(violation, table);
                if violation.kind == ConstraintKind::NotNull {
                    report.column_name = violation.column_names.first().cloned();
                }
                report.message = constraint_message(violation, &report);
                report.detail = constraint_detail(violation);
            }
        }
        report
    }
}

fn constraint_name(violation: &ConstraintViolation, table: &str) -> Option<String> {
    if let Some(name) = &violation.constraint_name {
        return Some(name.clone());
    }
    match violation.kind {
        ConstraintKind::PrimaryKey => Some(format!("{table}_pkey")),
        ConstraintKind::Unique => {
            if let Some(index) = &violation.index_name {
                if !index.starts_with("sqlite_autoindex_") {
                    return Some(index.clone());
                }
            }
            Some(format!(
                "{}_{}_key",
                table,
                violation.column_names.join("_")
            ))
        }
        ConstraintKind::ForeignKey => Some(format!(
            "{}_{}_fkey",
            table,
            violation.column_names.join("_")
        )),
        ConstraintKind::Check => Some(format!("{table}_check")),
        ConstraintKind::NotNull | ConstraintKind::Other => None,
    }
}

fn constraint_message(violation: &ConstraintViolation, report: &ErrorReport) -> String {
    let table = report.table_name.as_deref().unwrap();
    let constraint = report.constraint_name.as_deref().unwrap_or("");
    match violation.kind {
        ConstraintKind::PrimaryKey | ConstraintKind::Unique => {
            format!("duplicate key value violates unique constraint \"{constraint}\"")
        }
        ConstraintKind::ForeignKey => format!(
            "insert or update on table \"{table}\" violates foreign key constraint \"{constraint}\""
        ),
        ConstraintKind::NotNull => {
            let column = report.column_name.as_deref().unwrap_or("");
            format!("null value in column \"{column}\" of relation \"{table}\" violates not-null constraint")
        }
        ConstraintKind::Check => {
            format!("new row for relation \"{table}\" violates check constraint \"{constraint}\"")
        }
        ConstraintKind::Other => violation.message.clone(),
    }
}

fn constraint_detail(violation: &ConstraintViolation) -> Option<String> {
    let values = violation
        .values
        .iter()
        .map(format_value)
        .collect::<Vec<_>>()
        .join(", ");
    if violation.row_values {
        Some(format!("Failing row contains ({values})."))
    } else if !violation.values.is_empty() {
        let columns = violation.column_names.join(", ");
        if let Some(parent) = &violation.parent_table {
            Some(format!(
                "Key ({columns})=({values}) is not present in table \"{parent}\"."
            ))
        } else {
            Some(format!("Key ({columns})=({values}) already exists."))
        }
    } else {
        None
    }
}

fn format_value(value: &Value) -> String {
    use turso_core::Numeric;
    match value {
        Value::Null => "null".to_string(),
        Value::Numeric(Numeric::Integer(i)) => i.to_string(),
        Value::Numeric(Numeric::Float(f)) => {
            let f = f64::from(*f);
            if f.fract() == 0.0 && f.abs() < 1e15 {
                format!("{}", f as i64)
            } else {
                format!("{f}")
            }
        }
        Value::Text(t) => t.as_str().to_string(),
        Value::Blob(b) => {
            let bytes: &[u8] = b.as_ref();
            format!(
                "\\x{}",
                bytes
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            )
        }
    }
}

const SQLSTATE_UNIQUE_VIOLATION: &str = "23505";
const SQLSTATE_FK_VIOLATION: &str = "23503";
const SQLSTATE_CHECK_VIOLATION: &str = "23514";
const SQLSTATE_NOT_NULL_VIOLATION: &str = "23502";
const SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION: &str = "23000";
const SQLSTATE_SYNTAX_ERROR: &str = "42601";
const SQLSTATE_UNDEFINED_TABLE: &str = "42P01";
const SQLSTATE_UNDEFINED_COLUMN: &str = "42703";
const SQLSTATE_UNDEFINED_FUNCTION: &str = "42883";
const SQLSTATE_DUPLICATE_TABLE: &str = "42P07";
const SQLSTATE_SYNTAX_OR_ACCESS_VIOLATION: &str = "42000";
const SQLSTATE_DATATYPE_MISMATCH: &str = "42804";
const SQLSTATE_DATA_EXCEPTION: &str = "22000";
const SQLSTATE_STRING_DATA_RIGHT_TRUNCATION: &str = "22001";
const SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";
const SQLSTATE_NULL_VALUE_NOT_ALLOWED: &str = "22004";
const SQLSTATE_INVALID_DATETIME_FORMAT: &str = "22007";
const SQLSTATE_DIVISION_BY_ZERO: &str = "22012";
const SQLSTATE_INVALID_TEXT_REPRESENTATION: &str = "22P02";
const SQLSTATE_SEQUENCE_LIMIT_EXCEEDED: &str = "2200H";
const SQLSTATE_INVALID_TRANSACTION_STATE: &str = "25000";
const SQLSTATE_ACTIVE_SQL_TRANSACTION: &str = "25001";
const SQLSTATE_READ_ONLY_SQL_TRANSACTION: &str = "25006";
const SQLSTATE_IN_FAILED_SQL_TRANSACTION: &str = "25P02";
const SQLSTATE_NO_ACTIVE_SQL_TRANSACTION: &str = "25P01";
const SQLSTATE_INVALID_SAVEPOINT_SPECIFICATION: &str = "3B001";
const SQLSTATE_SERIALIZATION_FAILURE: &str = "40001";
const SQLSTATE_DISK_FULL: &str = "53100";
const SQLSTATE_OUT_OF_MEMORY: &str = "53200";
const SQLSTATE_LOCK_NOT_AVAILABLE: &str = "55P03";
const SQLSTATE_QUERY_CANCELED: &str = "57014";
const SQLSTATE_RAISE_EXCEPTION: &str = "P0001";
const SQLSTATE_ASSERT_FAILURE: &str = "P0004";
const SQLSTATE_DATA_CORRUPTED: &str = "XX001";
const SQLSTATE_INTERNAL_ERROR: &str = "XX000";

fn sqlstate(error: &LimboError) -> &'static str {
    match error {
        LimboError::ConstraintViolation(cv) => match cv.kind {
            ConstraintKind::PrimaryKey | ConstraintKind::Unique => SQLSTATE_UNIQUE_VIOLATION,
            ConstraintKind::ForeignKey => SQLSTATE_FK_VIOLATION,
            ConstraintKind::NotNull => SQLSTATE_NOT_NULL_VIOLATION,
            ConstraintKind::Check => SQLSTATE_CHECK_VIOLATION,
            ConstraintKind::Other => SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION,
        },
        LimboError::ForeignKeyConstraint(_) => SQLSTATE_FK_VIOLATION,
        LimboError::Constraint(_) => SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION,
        LimboError::LexerError(_) => SQLSTATE_SYNTAX_ERROR,
        LimboError::ParseError(msg) => parse_error_sqlstate(msg),
        LimboError::SqlError(_) | LimboError::PlanningError(_) => {
            SQLSTATE_SYNTAX_OR_ACCESS_VIOLATION
        }
        LimboError::DivisionByZero => SQLSTATE_DIVISION_BY_ZERO,
        LimboError::InvalidTextRepresentation(_) => SQLSTATE_INVALID_TEXT_REPRESENTATION,
        LimboError::IntegerOverflow => SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
        LimboError::TooBig => SQLSTATE_STRING_DATA_RIGHT_TRUNCATION,
        LimboError::InvalidDate(_)
        | LimboError::InvalidTime(_)
        | LimboError::InvalidModifier(_) => SQLSTATE_INVALID_DATETIME_FORMAT,
        LimboError::ConversionError(_)
        | LimboError::InvalidArgument(_)
        | LimboError::InvalidFormatter(_) => SQLSTATE_DATA_EXCEPTION,
        LimboError::ParseIntError(_) | LimboError::ParseFloatError(_) => {
            SQLSTATE_INVALID_TEXT_REPRESENTATION
        }
        LimboError::NullValue => SQLSTATE_NULL_VALUE_NOT_ALLOWED,
        LimboError::InvalidColumnType | LimboError::InvalidBlobSize(_) => {
            SQLSTATE_DATATYPE_MISMATCH
        }
        LimboError::SequenceExhausted { .. } => SQLSTATE_SEQUENCE_LIMIT_EXCEEDED,
        LimboError::TxError(msg) => tx_error_sqlstate(msg),
        LimboError::NoActiveTransaction { .. } => SQLSTATE_NO_ACTIVE_SQL_TRANSACTION,
        LimboError::TransactionAlreadyActive => SQLSTATE_ACTIVE_SQL_TRANSACTION,
        LimboError::ReadOnly => SQLSTATE_READ_ONLY_SQL_TRANSACTION,
        LimboError::TxTerminated | LimboError::StatementsInProgress(_) => {
            SQLSTATE_INVALID_TRANSACTION_STATE
        }
        LimboError::WriteWriteConflict
        | LimboError::BusySnapshot
        | LimboError::CommitDependencyAborted
        | LimboError::SchemaConflict
        | LimboError::SchemaUpdated
        | LimboError::Conflict(_) => SQLSTATE_SERIALIZATION_FAILURE,
        LimboError::DatabaseFull => SQLSTATE_DISK_FULL,
        LimboError::OutOfMemory => SQLSTATE_OUT_OF_MEMORY,
        LimboError::TableLocked | LimboError::Busy => SQLSTATE_LOCK_NOT_AVAILABLE,
        LimboError::Interrupt => SQLSTATE_QUERY_CANCELED,
        LimboError::Raise(_, _) => SQLSTATE_RAISE_EXCEPTION,
        LimboError::RaiseIgnore => SQLSTATE_ASSERT_FAILURE,
        LimboError::Corrupt(_) | LimboError::NotADB => SQLSTATE_DATA_CORRUPTED,
        _ => SQLSTATE_INTERNAL_ERROR,
    }
}

fn parse_error_sqlstate(msg: &str) -> &'static str {
    if msg.contains("no such table") || msg.contains("table not found") {
        SQLSTATE_UNDEFINED_TABLE
    } else if msg.contains("no such column") || msg.contains("column not found") {
        SQLSTATE_UNDEFINED_COLUMN
    } else if msg.contains("no such function") || msg.contains("no such window") {
        SQLSTATE_UNDEFINED_FUNCTION
    } else if msg.contains("already exists") {
        SQLSTATE_DUPLICATE_TABLE
    } else if msg.contains("query_only mode") {
        SQLSTATE_READ_ONLY_SQL_TRANSACTION
    } else {
        SQLSTATE_SYNTAX_ERROR
    }
}

fn tx_error_sqlstate(msg: &str) -> &'static str {
    if msg.contains("no such savepoint") {
        SQLSTATE_INVALID_SAVEPOINT_SPECIFICATION
    } else {
        SQLSTATE_INVALID_TRANSACTION_STATE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_covers_errors_that_sql_alone_cannot_trigger() {
        for (e, code) in [
            (LimboError::Interrupt, SQLSTATE_QUERY_CANCELED),
            (
                LimboError::WriteWriteConflict,
                SQLSTATE_SERIALIZATION_FAILURE,
            ),
            (LimboError::BusySnapshot, SQLSTATE_SERIALIZATION_FAILURE),
            (
                LimboError::CommitDependencyAborted,
                SQLSTATE_SERIALIZATION_FAILURE,
            ),
            (LimboError::SchemaConflict, SQLSTATE_SERIALIZATION_FAILURE),
            (LimboError::DatabaseFull, SQLSTATE_DISK_FULL),
            (LimboError::OutOfMemory, SQLSTATE_OUT_OF_MEMORY),
            (LimboError::Busy, SQLSTATE_LOCK_NOT_AVAILABLE),
            (LimboError::ReadOnly, SQLSTATE_READ_ONLY_SQL_TRANSACTION),
            (
                LimboError::NoActiveTransaction { op: "commit" },
                SQLSTATE_NO_ACTIVE_SQL_TRANSACTION,
            ),
            (
                LimboError::TransactionAlreadyActive,
                SQLSTATE_ACTIVE_SQL_TRANSACTION,
            ),
            (
                LimboError::Corrupt("page 3".to_string()),
                SQLSTATE_DATA_CORRUPTED,
            ),
            (LimboError::NotADB, SQLSTATE_DATA_CORRUPTED),
            (
                LimboError::InternalError("x".to_string()),
                SQLSTATE_INTERNAL_ERROR,
            ),
            (LimboError::DivisionByZero, SQLSTATE_DIVISION_BY_ZERO),
            (
                LimboError::IntegerOverflow,
                SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            ),
            (
                LimboError::ForeignKeyConstraint("fk".to_string()),
                SQLSTATE_FK_VIOLATION,
            ),
            (
                LimboError::Constraint("c".to_string()),
                SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION,
            ),
        ] {
            assert_eq!(
                ErrorReport::from(&e).sqlstate,
                code,
                "wrong SQLSTATE for {e:?}"
            );
        }
    }

    #[test]
    fn parse_errors_classify_by_message() {
        for (msg, code) in [
            ("no such table: t", SQLSTATE_UNDEFINED_TABLE),
            ("no such column: c", SQLSTATE_UNDEFINED_COLUMN),
            ("no such function: f", SQLSTATE_UNDEFINED_FUNCTION),
            ("table t already exists", SQLSTATE_DUPLICATE_TABLE),
            ("unexpected token", SQLSTATE_SYNTAX_ERROR),
            (
                "Cannot execute write statement in query_only mode",
                SQLSTATE_READ_ONLY_SQL_TRANSACTION,
            ),
        ] {
            let e = LimboError::ParseError(msg.to_string());
            assert_eq!(
                ErrorReport::from(&e).sqlstate,
                code,
                "wrong SQLSTATE for parse error {msg:?}"
            );
        }
    }

    #[test]
    fn transaction_errors_classify_by_message() {
        for (msg, code) in [
            (
                "no such savepoint: sp",
                SQLSTATE_INVALID_SAVEPOINT_SPECIFICATION,
            ),
            ("transaction is busy", SQLSTATE_INVALID_TRANSACTION_STATE),
        ] {
            let e = LimboError::TxError(msg.to_string());
            assert_eq!(
                ErrorReport::from(&e).sqlstate,
                code,
                "wrong SQLSTATE for tx error {msg:?}"
            );
        }
    }
}
