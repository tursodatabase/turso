use turso_core::{ConstraintKind, ConstraintViolation, DialectError, LimboError, Value};
use turso_pg_parser::sqlstate::*;

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

    pub(crate) fn into_core_error(self) -> LimboError {
        self.0.source
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

/// An engine error with the message `message`, which reports SQLSTATE
/// `sqlstate` to the client. Use it for errors that the PostgreSQL frontend
/// raises itself, also in the PostgreSQL functions that the engine runs.
pub(crate) fn sqlstate_error(sqlstate: &'static str, message: impl Into<String>) -> LimboError {
    LimboError::Dialect(Box::new(DialectError {
        code: sqlstate,
        message: message.into(),
    }))
}

/// The error PostgreSQL returns for every statement in an aborted
/// transaction block, until COMMIT, ROLLBACK, or ROLLBACK TO SAVEPOINT
/// ends it.
pub(crate) fn in_failed_sql_transaction_error() -> LimboError {
    sqlstate_error(
        turso_pg_parser::sqlstate::SQLSTATE_IN_FAILED_SQL_TRANSACTION,
        "current transaction is aborted, commands ignored until end of transaction block",
    )
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
                // Core names the table as it is stored; the user knows it by
                // its bare name.
                let table = turso_core::namespace::demangle_name(true, table).1;
                report.table_name = Some(table.to_string());
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
        LimboError::SqlError(msg) => sql_error_sqlstate(msg),
        LimboError::PlanningError(_) => SQLSTATE_SYNTAX_OR_ACCESS_VIOLATION,
        LimboError::DivisionByZero => SQLSTATE_DIVISION_BY_ZERO,
        LimboError::InvalidTextRepresentation(_) => SQLSTATE_INVALID_TEXT_REPRESENTATION,
        LimboError::IntegerOverflow => SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
        LimboError::TooBig => SQLSTATE_STRING_DATA_RIGHT_TRUNCATION,
        LimboError::InvalidDate(_)
        | LimboError::InvalidTime(_)
        | LimboError::InvalidModifier(_) => SQLSTATE_INVALID_DATETIME_FORMAT,
        LimboError::InvalidArgument(msg) => invalid_argument_sqlstate(msg),
        LimboError::ConversionError(_) | LimboError::InvalidFormatter(_) => SQLSTATE_DATA_EXCEPTION,
        LimboError::ParseIntError(_) | LimboError::ParseFloatError(_) => {
            SQLSTATE_INVALID_TEXT_REPRESENTATION
        }
        LimboError::NullValue => SQLSTATE_NULL_VALUE_NOT_ALLOWED,
        LimboError::InvalidColumnType | LimboError::InvalidBlobSize(_) => {
            SQLSTATE_DATATYPE_MISMATCH
        }
        LimboError::SequenceExhausted { .. } => SQLSTATE_SEQUENCE_LIMIT_EXCEEDED,
        LimboError::TxError(msg) => tx_error_sqlstate(msg),
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
        LimboError::Raise(_, msg) => raise_sqlstate(msg),
        LimboError::RaiseIgnore => SQLSTATE_ASSERT_FAILURE,
        LimboError::Corrupt(_) | LimboError::NotADB => SQLSTATE_DATA_CORRUPTED,
        LimboError::Dialect(error) => error.code,
        _ => SQLSTATE_INTERNAL_ERROR,
    }
}

/// The SQLSTATE of a parse error that the engine raised. The rules that look
/// at the start of the message cannot match a name in the message, for
/// example a column named "no such object". The checks for dependent objects
/// come first, because their messages end with the error that the dependent
/// object gives, for example "no such column".
fn parse_error_sqlstate(msg: &str) -> &'static str {
    if (msg.starts_with("cannot drop column") && msg.contains(": referenced in VIEW "))
        || msg.starts_with("cannot drop type")
    {
        SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST
    } else if msg.contains("no such table")
        || msg.contains("table not found")
        || msg.starts_with("no such object")
    {
        SQLSTATE_UNDEFINED_TABLE
    } else if msg.contains("no such column") || msg.contains("column not found") {
        SQLSTATE_UNDEFINED_COLUMN
    } else if msg.contains("no such function") || msg.contains("no such window") {
        SQLSTATE_UNDEFINED_FUNCTION
    } else if msg.contains("already exists") {
        SQLSTATE_DUPLICATE_TABLE
    } else if msg.starts_with("ambiguous column name") {
        SQLSTATE_AMBIGUOUS_COLUMN
    } else if msg.starts_with("misuse of aggregate")
        || msg.starts_with("aggregate functions are not allowed")
    {
        SQLSTATE_GROUPING_ERROR
    } else if msg.starts_with("misuse of window function") {
        SQLSTATE_WINDOWING_ERROR
    } else if msg.starts_with("invalid reference to FROM-clause entry") {
        SQLSTATE_INVALID_COLUMN_REFERENCE
    } else if msg == "malformed JSON" {
        SQLSTATE_INVALID_TEXT_REPRESENTATION
    } else if msg.starts_with("sequence \"") && msg.ends_with("\" does not exist") {
        SQLSTATE_UNDEFINED_TABLE
    } else if (msg.starts_with("schema \"") && msg.ends_with("\" does not exist"))
        || msg == "no schema has been selected to create in"
    {
        SQLSTATE_INVALID_SCHEMA_NAME
    } else {
        SQLSTATE_SYNTAX_ERROR
    }
}

fn invalid_argument_sqlstate(msg: &str) -> &'static str {
    if msg.starts_with("no such database") {
        SQLSTATE_INVALID_SCHEMA_NAME
    } else {
        SQLSTATE_DATA_EXCEPTION
    }
}

/// The SQLSTATE of an error that a statement of the engine raised when it ran.
fn sql_error_sqlstate(msg: &str) -> &'static str {
    if msg.contains("ON CONFLICT DO UPDATE command cannot affect row a second time") {
        SQLSTATE_CARDINALITY_VIOLATION
    } else {
        SQLSTATE_SYNTAX_OR_ACCESS_VIOLATION
    }
}

/// The SQLSTATE of a RAISE. The translator and the types of the engine check
/// values with RAISE, and PostgreSQL gives these checks their own SQLSTATE.
fn raise_sqlstate(msg: &str) -> &'static str {
    if msg == "invalid date value" || msg == "invalid timestamp value" {
        SQLSTATE_INVALID_DATETIME_FORMAT
    } else if msg.starts_with("invalid input value for enum ") {
        SQLSTATE_INVALID_TEXT_REPRESENTATION
    } else if msg.starts_with("cannot call ") || msg.ends_with(" must be an array of objects") {
        SQLSTATE_INVALID_PARAMETER_VALUE
    } else {
        SQLSTATE_RAISE_EXCEPTION
    }
}

fn tx_error_sqlstate(msg: &str) -> &'static str {
    if msg.contains("within a transaction") {
        SQLSTATE_ACTIVE_SQL_TRANSACTION
    } else if msg.contains("no transaction is active") {
        SQLSTATE_NO_ACTIVE_SQL_TRANSACTION
    } else if msg.contains("no such savepoint") {
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
            ("malformed JSON", SQLSTATE_INVALID_TEXT_REPRESENTATION),
            ("sequence \"s.q\" does not exist", SQLSTATE_UNDEFINED_TABLE),
            ("schema \"s\" does not exist", SQLSTATE_INVALID_SCHEMA_NAME),
            (
                "no schema has been selected to create in",
                SQLSTATE_INVALID_SCHEMA_NAME,
            ),
            (
                "Invalid statement: syntax error at or near \"'malformed JSON'\"",
                SQLSTATE_SYNTAX_ERROR,
            ),
            ("unexpected token", SQLSTATE_SYNTAX_ERROR),
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
                "cannot start a transaction within a transaction",
                SQLSTATE_ACTIVE_SQL_TRANSACTION,
            ),
            (
                "cannot commit - no transaction is active",
                SQLSTATE_NO_ACTIVE_SQL_TRANSACTION,
            ),
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
