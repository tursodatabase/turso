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
    if let Some(name) = msg.strip_prefix("no such table: ") {
        return PgErrorInfo::new("42P01", format!("relation \"{name}\" does not exist"));
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

/// Constraint failures arrive as `<KIND> constraint failed: ...` strings.
fn constraint_error(msg: &str) -> PgErrorInfo {
    if msg.contains("UNIQUE constraint failed") || msg.contains("PRIMARY KEY constraint failed") {
        return PgErrorInfo::new("23505", format!("Runtime error: {msg}"));
    }
    if let Some(target) = msg.strip_prefix("NOT NULL constraint failed: ") {
        // "t.c (19)" — the trailing SQLite result code is not part of the name.
        let target = target.split_whitespace().next().unwrap_or(target);
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
        return PgErrorInfo::new("23514", format!("Runtime error: {msg}"));
    }
    if msg.contains("FOREIGN KEY constraint failed") {
        return PgErrorInfo::new("23503", format!("Runtime error: {msg}"));
    }
    if msg.contains("value too long") {
        return PgErrorInfo::new("22001", format!("Runtime error: {msg}"));
    }
    if msg.contains("divi") && msg.contains("zero") {
        return PgErrorInfo::new("22012", "division by zero".to_string());
    }
    PgErrorInfo::new("23000", format!("Runtime error: {msg}"))
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
