//! Planning shared by the table-valued functions that take one argument,
//! such as `unnest(array)`.

use turso_ext::{ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, ResultCode};

/// The plan of a table-valued function whose one argument is the hidden
/// column `argument_column`. The function cannot run without that
/// argument, so the plan needs an equality constraint on the column and
/// passes its value as the first filter argument.
pub(crate) fn one_argument_best_index(
    constraints: &[ConstraintInfo],
    argument_column: u32,
    estimated_rows: u32,
) -> Result<IndexInfo, ResultCode> {
    let argument = constraints
        .iter()
        .position(|c| c.column_index == argument_column && c.op == ConstraintOp::Eq && c.usable)
        .ok_or(ResultCode::ConstraintViolation)?;
    Ok(IndexInfo {
        constraint_usages: (0..constraints.len())
            .map(|i| ConstraintUsage {
                argv_index: (i == argument).then_some(1),
                omit: i == argument,
            })
            .collect(),
        estimated_cost: 1.0,
        estimated_rows,
        ..Default::default()
    })
}
