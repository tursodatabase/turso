//! The functions that PostgreSQL's bool_and, bool_or and every use.

use crate::error::sqlstate_error;
use crate::functions::Boolean;
use turso_core::{Register, Result, Value};
use turso_pg_parser::sqlstate::SQLSTATE_INTERNAL_ERROR;

pub(crate) fn booland_statefunc(args: &[Register]) -> Option<Boolean> {
    let (state, value) = both_booleans(args)?;
    Some(Boolean(state && value))
}

pub(crate) fn boolor_statefunc(args: &[Register]) -> Option<Boolean> {
    let (state, value) = both_booleans(args)?;
    Some(Boolean(state || value))
}

pub(crate) fn bool_accum(args: &[Register]) -> Result<Value> {
    let mut counts = BoolCounts::from_state(args[0].get_value())?;
    if let Some(value) = boolean(&args[1]) {
        counts.rows += 1;
        counts.trues += i64::from(value);
    }
    counts.into_state()
}

pub(crate) fn bool_accum_inv(args: &[Register]) -> Result<Value> {
    let mut counts = BoolCounts::from_state(args[0].get_value())?;
    if let Some(value) = boolean(&args[1]) {
        counts.rows -= 1;
        counts.trues -= i64::from(value);
    }
    counts.into_state()
}

pub(crate) fn bool_alltrue(args: &[Register]) -> Result<Option<Boolean>> {
    let counts = non_empty_counts(&args[0])?;
    Ok(counts.map(|counts| Boolean(counts.trues == counts.rows)))
}

pub(crate) fn bool_anytrue(args: &[Register]) -> Result<Option<Boolean>> {
    let counts = non_empty_counts(&args[0])?;
    Ok(counts.map(|counts| Boolean(counts.trues > 0)))
}

fn both_booleans(args: &[Register]) -> Option<(bool, bool)> {
    Some((boolean(&args[0])?, boolean(&args[1])?))
}

fn boolean(arg: &Register) -> Option<bool> {
    arg.get_value().as_int().map(|value| value != 0)
}

fn non_empty_counts(state: &Register) -> Result<Option<BoolCounts>> {
    let counts = BoolCounts::from_state(state.get_value())?;
    Ok((counts.rows > 0).then_some(counts))
}

/// The state of bool_accum: how many non-NULL values were added, and how many
/// of them were true. It is kept in a blob of two little-endian 64-bit integers.
struct BoolCounts {
    rows: i64,
    trues: i64,
}

impl BoolCounts {
    fn from_state(state: &Value) -> Result<Self> {
        let bytes = match state {
            Value::Null => return Ok(Self { rows: 0, trues: 0 }),
            Value::Blob(bytes) if bytes.len() == 16 => bytes,
            _ => {
                return Err(sqlstate_error(
                    SQLSTATE_INTERNAL_ERROR,
                    "invalid state of a boolean aggregate",
                    None,
                ))
            }
        };
        let (rows, trues) = bytes.split_at(8);
        Ok(Self {
            rows: i64::from_le_bytes(rows.try_into().expect("8 bytes")),
            trues: i64::from_le_bytes(trues.try_into().expect("8 bytes")),
        })
    }

    fn into_state(self) -> Result<Value> {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.rows.to_le_bytes());
        bytes[8..].copy_from_slice(&self.trues.to_le_bytes());
        Ok(Value::from_slice(&bytes)?)
    }
}
