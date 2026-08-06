// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Maps engine errors to PostgreSQL SQLSTATE codes and, where the message
//! carries enough information, to PostgreSQL's wording. Drivers and ORMs
//! branch on SQLSTATE (23505 to retry an upsert, 42P01 to create a missing
//! table, 40001 to retry a transaction), and the regress golden files
//! compare error text byte-for-byte — both need more than the catch-all
//! XX000 every error used to carry.
//!
//! The mapping is deliberately incremental: the code must be right, the
//! wording is rewritten only when the engine message contains everything
//! PostgreSQL's wording needs. Everything unmapped keeps its engine
//! message under a best-effort code.

use turso_core::LimboError;

/// A PostgreSQL-facing error: SQLSTATE, message, and optionally the
/// 1-based character position clients render as a `LINE n:` caret.
pub struct PgErrorInfo {
    pub code: &'static str,
    pub message: String,
    pub position: Option<usize>,
}

impl PgErrorInfo {
    /// An error the frontend words itself, rather than one mapped from an
    /// engine message.
    pub fn user_error(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(code, message)
    }

    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            position: None,
        }
    }
}

/// Maps an engine error to its SQLSTATE and message. `sql` is the statement
/// that failed; syntax errors use it to locate the reported token.
pub fn pg_error(e: &LimboError, sql: &str) -> PgErrorInfo {
    match e {
        LimboError::ParseError(msg) => parse_error(msg, sql),
        LimboError::Constraint(msg) => constraint_error(msg),
        LimboError::ForeignKeyConstraint(msg) => {
            PgErrorInfo::new("23503", format!("Runtime error: {msg}"))
        }
        LimboError::TxError(msg) => tx_error(msg),
        LimboError::InvalidDate(msg) | LimboError::InvalidTime(msg) => {
            PgErrorInfo::new("22007", format!("Parse error: {msg}"))
        }
        // A message already in PostgreSQL's own wording is surfaced as-is;
        // prefixing it would stop any golden file from matching.
        LimboError::ConversionError(msg) if msg.starts_with("invalid input syntax for type ") => {
            PgErrorInfo::new("22P02", msg.clone())
        }
        LimboError::ConversionError(msg) => {
            PgErrorInfo::new("22P02", format!("Conversion error: {msg}"))
        }
        LimboError::IntegerOverflow => PgErrorInfo::new("22003", e.to_string()),
        LimboError::ReadOnly => PgErrorInfo::new("25006", e.to_string()),
        LimboError::Busy | LimboError::TableLocked => PgErrorInfo::new("55P03", e.to_string()),
        LimboError::BusySnapshot | LimboError::WriteWriteConflict => {
            PgErrorInfo::new("40001", e.to_string())
        }
        LimboError::Corrupt(_) => PgErrorInfo::new("XX001", e.to_string()),
        // The engine raises Interrupt when a CancelRequest flips the
        // connection's interrupt flag mid-statement; drivers branch on
        // this exact SQLSTATE for query timeouts.
        LimboError::Interrupt => PgErrorInfo::new(
            "57014",
            "canceling statement due to user request".to_string(),
        ),
        _ => PgErrorInfo::new("XX000", e.to_string()),
    }
}

/// Translator and name-resolution failures. The engine reports them all as
/// parse errors; PostgreSQL distinguishes syntax from missing objects and
/// unimplemented features, and clients rely on that distinction.
fn parse_error(msg: &str, sql: &str) -> PgErrorInfo {
    // Grammar-level failures come from the real PostgreSQL parser
    // (libpg_query) wrapped in engine prefixes; surface its own wording.
    if let Some(rest) = msg
        .find("syntax error at or near \"")
        .map(|i| &msg[i + "syntax error at or near \"".len()..])
    {
        if let Some(token) = rest.rsplit_once('"').map(|(t, _)| t) {
            return PgErrorInfo {
                code: "42601",
                message: format!("syntax error at or near \"{token}\""),
                position: unique_position(sql, token),
            };
        }
    }
    if msg.contains("syntax error at end of input") {
        return PgErrorInfo {
            code: "42601",
            message: "syntax error at end of input".to_string(),
            // PostgreSQL points one past the last character.
            position: Some(sql.chars().count() + 1),
        };
    }
    if let Some(idx) = msg.find("unrecognized configuration parameter") {
        return PgErrorInfo::new("42704", msg[idx..].to_string());
    }
    if let Some(idx) = msg.find("invalid value for parameter") {
        return PgErrorInfo::new("22023", msg[idx..].to_string());
    }
    // The engine spells this both ways depending on the code path.
    if let Some(name) = msg
        .strip_prefix("no such table: ")
        .or_else(|| msg.strip_prefix("No such table: "))
    {
        return PgErrorInfo::new("42P01", format!("relation \"{name}\" does not exist"));
    }
    // DROP reports missing targets as objects; the kind (table, view, ...)
    // is not in the message, so only the code is mapped.
    if msg.strip_prefix("no such object: ").is_some() {
        return PgErrorInfo::new("42P01", format!("Parse error: {msg}"));
    }
    if let Some(name) = msg.strip_prefix("no such column: ") {
        return PgErrorInfo::new("42703", format!("column \"{name}\" does not exist"));
    }
    if let Some(name) = msg.strip_prefix("no such function: ") {
        return PgErrorInfo::new("42883", format!("function {name} does not exist"));
    }
    if msg.contains("already exists") {
        return PgErrorInfo::new("42P07", format!("Parse error: {msg}"));
    }
    // Messages already in PostgreSQL's own wording surface verbatim.
    if msg.ends_with("is not implemented") {
        return PgErrorInfo::new("0A000", msg.to_string());
    }
    if msg.contains("not supported") || msg.contains("not yet supported") {
        return PgErrorInfo::new("0A000", format!("Parse error: {msg}"));
    }
    PgErrorInfo::new("42601", format!("Parse error: {msg}"))
}

/// The table and columns a UNIQUE or PRIMARY KEY violation collided on.
/// PostgreSQL names the constraint instead, which only the schema knows, so
/// the frontend resolves this into a name (see
/// `PgConnection::pg_error`).
pub struct UniqueViolation<'a> {
    pub table: &'a str,
    pub columns: Vec<&'a str>,
}

/// Picks the table and columns out of a unique-violation message, which the
/// engine spells `UNIQUE constraint failed: t.c` for one column and
/// `UNIQUE constraint failed: t.(a, b)` for several. Returns None for any
/// other shape, including a quoted name containing a dot.
pub fn unique_violation(e: &LimboError) -> Option<UniqueViolation<'_>> {
    let LimboError::Constraint(msg) = e else {
        return None;
    };
    let target = msg
        .strip_prefix("UNIQUE constraint failed: ")
        .or_else(|| msg.strip_prefix("PRIMARY KEY constraint failed: "))?;
    let target = strip_result_code(target);
    let (table, columns) = target.split_once('.')?;
    let columns: Vec<&str> = match columns.strip_prefix('(').and_then(|c| c.strip_suffix(')')) {
        Some(list) => list.split(',').map(str::trim).collect(),
        None => vec![columns],
    };
    if table.is_empty() || columns.iter().any(|c| c.is_empty()) {
        return None;
    }
    Some(UniqueViolation { table, columns })
}

/// PostgreSQL's wording for a unique violation on `constraint`. PostgreSQL
/// also prints a `DETAIL: Key (a)=(1) already exists.` line, which needs the
/// colliding values the engine error does not carry.
pub fn unique_violation_message(constraint: &str) -> String {
    format!("duplicate key value violates unique constraint \"{constraint}\"")
}

/// How a failed CHECK constraint identifies itself in the engine message:
/// its name when it has one, otherwise its expression as the engine prints
/// it. Neither says which table it belongs to, which PostgreSQL's wording
/// needs, so the frontend searches the schema for a constraint that
/// describes itself the same way.
pub fn check_violation(e: &LimboError) -> Option<&str> {
    let LimboError::Constraint(msg) = e else {
        return None;
    };
    let description = strip_result_code(msg.strip_prefix("CHECK constraint failed: ")?);
    (!description.is_empty()).then_some(description)
}

/// PostgreSQL's wording for a check violation. PostgreSQL also prints a
/// `DETAIL: Failing row contains (...)` line, which needs the row the engine
/// error does not carry.
pub fn check_violation_message(table: &str, constraint: &str) -> String {
    format!("new row for relation \"{table}\" violates check constraint \"{constraint}\"")
}

/// Constraint failures arrive as `<KIND> constraint failed: ...` strings.
fn constraint_error(msg: &str) -> PgErrorInfo {
    if msg.contains("UNIQUE constraint failed") || msg.contains("PRIMARY KEY constraint failed") {
        // Reached only when the constraint could not be named — the
        // frontend rewrites this to PostgreSQL's wording when it can.
        return PgErrorInfo::new(
            "23505",
            format!("Runtime error: {}", strip_result_code(msg)),
        );
    }
    if let Some(target) = msg.strip_prefix("NOT NULL constraint failed: ") {
        let target = strip_result_code(target);
        // "t.c" carries everything PostgreSQL's wording needs.
        if let Some((table, column)) = target.split_once('.') {
            return PgErrorInfo::new(
                "23502",
                format!(
                    "null value in column \"{column}\" of relation \"{table}\" \
                     violates not-null constraint"
                ),
            );
        }
        return PgErrorInfo::new("23502", format!("Runtime error: {msg}"));
    }
    if msg.contains("CHECK constraint failed") {
        return PgErrorInfo::new(
            "23514",
            format!("Runtime error: {}", strip_result_code(msg)),
        );
    }
    if msg.contains("FOREIGN KEY constraint failed") {
        return PgErrorInfo::new(
            "23503",
            format!("Runtime error: {}", strip_result_code(msg)),
        );
    }
    if msg.contains("value too long") {
        return PgErrorInfo::new(
            "22001",
            format!("Runtime error: {}", strip_result_code(msg)),
        );
    }
    if msg.contains("divi") && msg.contains("zero") {
        return PgErrorInfo::new("22012", "division by zero".to_string());
    }
    PgErrorInfo::new(
        "23000",
        format!("Runtime error: {}", strip_result_code(msg)),
    )
}

/// Drops the SQLite result code the engine appends to constraint messages
/// ("t.c (19)"), which PostgreSQL never prints.
fn strip_result_code(msg: &str) -> &str {
    let trimmed = msg.trim_end();
    let Some(open) = trimmed.rfind(" (") else {
        return trimmed;
    };
    let code = &trimmed[open + 2..];
    match code.strip_suffix(')') {
        Some(digits) if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) => {
            &trimmed[..open]
        }
        _ => trimmed,
    }
}

/// 1-based character position of `token` in `sql`, only when it occurs
/// exactly once — a wrong caret is worse than none.
fn unique_position(sql: &str, token: &str) -> Option<usize> {
    if token.is_empty() {
        return None;
    }
    let mut occurrences = sql.match_indices(token);
    let (byte_offset, _) = occurrences.next()?;
    if occurrences.next().is_some() {
        return None;
    }
    Some(1 + sql[..byte_offset].chars().count())
}

fn tx_error(msg: &str) -> PgErrorInfo {
    if msg.contains("within a transaction") {
        return PgErrorInfo::new("25001", format!("Transaction error: {msg}"));
    }
    if msg.contains("no transaction") {
        return PgErrorInfo::new("25P01", format!("Transaction error: {msg}"));
    }
    PgErrorInfo::new("25000", format!("Transaction error: {msg}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn constraint(msg: &str) -> LimboError {
        LimboError::Constraint(msg.to_string())
    }

    #[test]
    fn one_column_and_many_column_violations_both_parse() {
        let e = constraint("UNIQUE constraint failed: t.a (19)");
        let v = unique_violation(&e).expect("a unique violation");
        assert_eq!(v.table, "t");
        assert_eq!(v.columns, ["a"]);

        let e = constraint("UNIQUE constraint failed: n.(a, b) (19)");
        let v = unique_violation(&e).expect("a unique violation");
        assert_eq!(v.table, "n");
        assert_eq!(v.columns, ["a", "b"]);
    }

    #[test]
    fn a_primary_key_violation_reads_the_same_as_a_unique_one() {
        let e = constraint("PRIMARY KEY constraint failed: t.a (19)");
        let v = unique_violation(&e).expect("a unique violation");
        assert_eq!(v.table, "t");
        assert_eq!(v.columns, ["a"]);
    }

    #[test]
    fn other_constraint_failures_are_not_unique_violations() {
        assert!(unique_violation(&constraint("CHECK constraint failed: c > 0 (19)")).is_none());
        assert!(unique_violation(&constraint("UNIQUE constraint failed: t (19)")).is_none());
        assert!(unique_violation(&LimboError::ReadOnly).is_none());
    }

    #[test]
    fn a_check_describes_itself_by_name_or_by_expression() {
        assert_eq!(
            check_violation(&constraint("CHECK constraint failed: c_positive (19)")),
            Some("c_positive")
        );
        assert_eq!(
            check_violation(&constraint("CHECK constraint failed: c > 0 (19)")),
            Some("c > 0")
        );
        assert_eq!(
            check_violation(&constraint("UNIQUE constraint failed: t.a (19)")),
            None
        );
    }

    #[test]
    fn only_the_one_result_code_the_engine_appends_is_stripped() {
        assert_eq!(strip_result_code("t.c (19)"), "t.c");
        assert_eq!(strip_result_code("length(c) (19)"), "length(c)");
        // Stripping exactly one group is the inverse of the engine's
        // formatting, so an expression that itself ends in a parenthesized
        // number survives.
        assert_eq!(strip_result_code("c > (1) (19)"), "c > (1)");
        // Nothing to strip.
        assert_eq!(
            strip_result_code("CHECK constraint failed: c > 0"),
            "CHECK constraint failed: c > 0"
        );
        assert_eq!(strip_result_code("plain message"), "plain message");
    }
}
