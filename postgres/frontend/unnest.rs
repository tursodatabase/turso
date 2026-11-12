use crate::table_function::one_argument_best_index;
use std::sync::Arc;
use turso_core::{
    array_values_from_any,
    native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule},
    types::IOResultOr,
    Connection, IOResult, LimboError, Result, Value,
};
use turso_ext::{ConstraintInfo, IndexInfo, OrderByInfo, ResultCode};

#[derive(Debug)]
pub(crate) struct Unnest;

impl VirtualTableModule for Unnest {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE unnest (unnest, input HIDDEN)".into())
    }

    fn create(&self, _args: &[Value]) -> Result<Self> {
        Ok(Self)
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for Unnest {
    type Cursor = UnnestCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(UnnestCursor {
            values: Vec::new(),
            position: 0,
            input: Value::Null,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        one_argument_best_index(constraints, 1, 10)
    }
}

pub(crate) struct UnnestCursor {
    values: Vec<Value>,
    position: usize,
    input: Value,
}

impl VirtualTableCursor for UnnestCursor {
    fn next(&mut self) -> IOResultOr<bool> {
        self.position += 1;
        Ok(IOResult::Done(self.position < self.values.len()))
    }

    fn rowid(&self) -> i64 {
        self.position as i64
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        Ok(IOResult::Done(match column {
            0 => self.values[self.position].clone(),
            1 => self.input.clone(),
            _ => unreachable!("invalid unnest column"),
        }))
    }

    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> IOResultOr<bool> {
        self.values.clear();
        self.position = 0;
        let [input] = args else {
            return Err(LimboError::InvalidArgument("unnest requires one array".into()).into());
        };
        self.input = input.clone();
        let mut pending = vec![array_values_from_any(input)
            .ok_or_else(|| LimboError::InvalidArgument("unnest requires an array".into()))?
            .into_iter()];
        while let Some(elements) = pending.last_mut() {
            match elements.next() {
                Some(Value::Blob(blob)) => {
                    let values = array_values_from_any(&Value::Blob(blob)).ok_or_else(|| {
                        LimboError::InvalidArgument("invalid nested array in unnest".into())
                    })?;
                    pending.push(values.into_iter());
                }
                Some(value) => self.values.push(value),
                None => {
                    pending.pop();
                }
            }
        }
        Ok(IOResult::Done(!self.values.is_empty()))
    }
}
