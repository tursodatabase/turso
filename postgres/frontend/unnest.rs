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
use turso_pg_parser::translator::{unnest_column_name, MAX_UNNEST_ARRAYS};

#[derive(Debug)]
pub(crate) struct Unnest;

impl VirtualTableModule for Unnest {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        let elements = (1..=MAX_UNNEST_ARRAYS).map(unnest_column_name);
        let inputs = (1..=MAX_UNNEST_ARRAYS).map(|array| format!("input{array} HIDDEN"));
        let columns: Vec<String> = elements.chain(inputs).collect();
        Ok(format!("CREATE TABLE unnest ({})", columns.join(", ")))
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
            arrays: Vec::new(),
            inputs: Vec::new(),
            row_count: 0,
            position: 0,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let input_of = |constraint: &ConstraintInfo| {
            let is_input = constraint.op == ConstraintOp::Eq
                && (MAX_UNNEST_ARRAYS..2 * MAX_UNNEST_ARRAYS)
                    .contains(&(constraint.column_index as usize));
            is_input.then(|| constraint.column_index as usize - MAX_UNNEST_ARRAYS)
        };
        let all_inputs_usable = constraints
            .iter()
            .all(|constraint| input_of(constraint).is_none() || constraint.usable);
        let input_count = constraints.iter().filter_map(input_of).count();
        let has_each_input_once = (0..input_count)
            .all(|input| constraints.iter().filter_map(input_of).any(|i| i == input));
        if input_count == 0 || !all_inputs_usable || !has_each_input_once {
            return Err(ResultCode::ConstraintViolation);
        }
        Ok(IndexInfo {
            constraint_usages: constraints
                .iter()
                .map(|constraint| {
                    let input = input_of(constraint);
                    ConstraintUsage {
                        argv_index: input.map(|input| input as u32 + 1),
                        omit: input.is_some(),
                    }
                })
                .collect(),
            estimated_cost: 1.0,
            estimated_rows: 10,
            ..Default::default()
        })
    }
}

pub(crate) struct UnnestCursor {
    /// The elements of each array, with the elements of nested arrays in place of them.
    arrays: Vec<Vec<Value>>,
    inputs: Vec<Value>,
    row_count: usize,
    position: usize,
}

impl VirtualTableCursor for UnnestCursor {
    fn next(&mut self) -> IOResultOr<bool> {
        self.position += 1;
        Ok(IOResult::Done(self.position < self.row_count))
    }

    fn rowid(&self) -> i64 {
        self.position as i64
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        let value = if column < MAX_UNNEST_ARRAYS {
            self.arrays
                .get(column)
                .and_then(|elements| elements.get(self.position))
                .cloned()
        } else {
            self.inputs.get(column - MAX_UNNEST_ARRAYS).cloned()
        };
        Ok(IOResult::Done(value.unwrap_or(Value::Null)))
    }

    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> IOResultOr<bool> {
        self.position = 0;
        self.inputs = args.to_vec();
        self.arrays = args.iter().map(flattened_elements).collect::<Result<_>>()?;
        self.row_count = self.arrays.iter().map(Vec::len).max().unwrap_or(0);
        Ok(IOResult::Done(self.row_count > 0))
    }
}

fn flattened_elements(array: &Value) -> Result<Vec<Value>> {
    let mut elements = Vec::new();
    let mut pending = vec![array_values_from_any(
        turso_core::types::NanPolicy::for_dialect(&crate::PostgresDialect),
        array,
    )
    .ok_or_else(|| LimboError::InvalidArgument("unnest requires an array".into()))?
    .into_iter()];
    while let Some(values) = pending.last_mut() {
        match values.next() {
            Some(Value::Blob(blob)) => {
                let nested = array_values_from_any(
                    turso_core::types::NanPolicy::for_dialect(&crate::PostgresDialect),
                    &Value::Blob(blob),
                )
                .ok_or_else(|| {
                    LimboError::InvalidArgument("invalid nested array in unnest".into())
                })?;
                pending.push(nested.into_iter());
            }
            Some(value) => elements.push(value),
            None => {
                pending.pop();
            }
        }
    }
    Ok(elements)
}
