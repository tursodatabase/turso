use crate::error::sqlstate_error;
use std::sync::Arc;
use turso_core::{
    array_values_from_any,
    native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule},
    types::IOResultOr,
    Connection, IOResult, LimboError, Result, Value,
};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};
use turso_pg_parser::sqlstate::SQLSTATE_NULL_VALUE_NOT_ALLOWED;

#[derive(Debug)]
pub(crate) struct PgOptionsToTable;

impl VirtualTableModule for PgOptionsToTable {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE pg_options_to_table (option_name TEXT, option_value TEXT, options_array TEXT HIDDEN)".into())
    }

    fn create(&self, _args: &[Value]) -> Result<Self> {
        Ok(Self)
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for PgOptionsToTable {
    type Cursor = PgOptionsCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgOptionsCursor {
            rows: Vec::new(),
            position: 0,
            input: Value::Null,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let input = constraints
            .iter()
            .position(|c| c.column_index == 2 && c.op == ConstraintOp::Eq && c.usable);
        let Some(input) = input else {
            return Err(ResultCode::ConstraintViolation);
        };
        Ok(IndexInfo {
            constraint_usages: (0..constraints.len())
                .map(|i| ConstraintUsage {
                    argv_index: (i == input).then_some(1),
                    omit: i == input,
                })
                .collect(),
            estimated_cost: 1.0,
            estimated_rows: 10,
            ..Default::default()
        })
    }
}

pub(crate) struct PgOptionsCursor {
    rows: Vec<[Value; 2]>,
    position: usize,
    input: Value,
}

impl VirtualTableCursor for PgOptionsCursor {
    fn next(&mut self) -> IOResultOr<bool> {
        self.position += 1;
        Ok(IOResult::Done(self.position < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.position as i64
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        let value = match column {
            0 | 1 => self.rows[self.position][column].clone(),
            2 => self.input.clone(),
            _ => unreachable!("invalid pg_options_to_table column"),
        };
        Ok(IOResult::Done(value))
    }

    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> IOResultOr<bool> {
        self.rows.clear();
        self.position = 0;
        let [input] = args else {
            return Err(LimboError::InvalidArgument(
                "pg_options_to_table requires one argument".into(),
            )
            .into());
        };
        self.input = input.clone();
        append_options(input, &mut self.rows)?;
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

fn append_options(input: &Value, rows: &mut Vec<[Value; 2]>) -> Result<()> {
    let values = array_values_from_any(input).ok_or_else(|| {
        LimboError::InvalidArgument("pg_options_to_table requires a text array".into())
    })?;
    for value in values {
        if matches!(value, Value::Blob(_)) {
            append_options(&value, rows)?;
            continue;
        }
        if matches!(value, Value::Null) {
            return Err(sqlstate_error(
                SQLSTATE_NULL_VALUE_NOT_ALLOWED,
                "null array element not allowed in this context",
            ));
        }
        let option = value.to_string();
        let (name, value) = match option.split_once('=') {
            Some((name, value)) => (name, Value::build_text(value.to_owned())),
            None => (option.as_str(), Value::Null),
        };
        rows.push([Value::build_text(name.to_owned()), value]);
    }
    Ok(())
}
