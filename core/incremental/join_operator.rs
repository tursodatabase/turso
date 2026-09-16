#![allow(dead_code)]

use crate::coro::{with_handle, Co, Runner, StepContext, YieldSlot};
use crate::incremental::dbsp::Hash128;
use crate::incremental::dbsp::{Delta, DeltaPair, HashableRow};
use crate::incremental::operator::{
    generate_storage_id, ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
    OpRunner,
};
use crate::incremental::persistence::WriteRow;
use crate::numeric::Numeric;
use crate::storage::btree::CursorTrait;
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::types::IOResultOr;
use crate::types::{
    IOCompletions, IOResult, ImmutableRecord, ImmutableRecordRef, SeekKey, SeekOp, SeekResult,
};
use crate::{return_if_io, LimboError, Result, Value};

/// Names [`JoinCtx`] as the context type of the async join operations.
pub struct JoinStep;

impl StepContext for JoinStep {
    type Error = Box<LimboError>;
    type Ctx<'a> = JoinCtx<'a>;
}

/// The context of one step of a join operation: the operator, the state
/// cursors of that step, and the slot for what suspends the step.
pub struct JoinCtx<'a> {
    operator: &'a mut JoinOperator,
    cursors: &'a mut DbspStateCursors,
    io: Option<IOCompletions>,
    err: Option<Box<LimboError>>,
}

impl YieldSlot<Box<LimboError>> for JoinCtx<'_> {
    fn park_io(&mut self, io: IOCompletions) {
        self.io = Some(io);
    }

    fn take_io(&mut self) -> Option<IOCompletions> {
        self.io.take()
    }

    fn park_err(&mut self, err: Box<LimboError>) {
        self.err = Some(err);
    }

    fn take_err(&mut self) -> Option<Box<LimboError>> {
        self.err.take()
    }
}

/// The eval of a join operator as a step function.
pub type JoinEvalOp = OpRunner<JoinStep, DeltaPair, Delta>;

/// The commit of a join operator as a step function.
type JoinCommitOp = OpRunner<JoinStep, DeltaPair, Delta>;

/// The runners of the join operations, boxed on first use and reused.
#[derive(Debug, Default)]
struct JoinOps {
    eval: Option<JoinEvalOp>,
    commit: Option<JoinCommitOp>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

// Helper function to read the next row from the BTree for joins
fn read_next_join_row(
    storage_id: i64,
    join_key: &HashableRow,
    last_element_hash: Option<Hash128>,
    cursors: &mut DbspStateCursors,
) -> IOResultOr<Option<(Hash128, HashableRow, isize)>> {
    // Build the index key: (storage_id, zset_id, element_id)
    // zset_id is the hash of the join key
    let zset_hash = join_key.cached_hash();

    // For iteration, use the last element hash if we have one, or NULL to start
    let index_key_values = match last_element_hash {
        Some(last_hash) => vec![
            Value::from_i64(storage_id),
            zset_hash.to_value()?,
            last_hash.to_value()?,
        ],
        None => vec![
            Value::from_i64(storage_id),
            zset_hash.to_value()?,
            Value::Null, // Start iteration from beginning
        ],
    };

    let index_record = ImmutableRecord::from_values(&index_key_values, index_key_values.len())?;

    // Use GE (>=) for initial seek with NULL, GT (>) for continuation
    let seek_op = if last_element_hash.is_none() {
        SeekOp::GE { eq_only: false }
    } else {
        SeekOp::GT
    };

    let seek_result = return_if_io!(cursors
        .index_cursor
        .seek(SeekKey::IndexKey(index_record.as_record_ref()), seek_op));

    if !matches!(seek_result, SeekResult::Found) {
        return Ok(IOResult::Done(None));
    }

    // Check if we're still in the same (storage_id, zset_id) range
    let current_record = return_if_io!(cursors.index_cursor.record());

    // Extract all needed values from the record before dropping it
    let (found_storage_id, found_zset_hash, element_hash) = if let Some(rec) = current_record {
        let values = rec.get_three_values(0, 1, 2);

        // Index has 4 values: storage_id, zset_id, element_id, rowid (appended by WriteRow)
        if let Ok((v0, v1, v2)) = values {
            let found_storage_id = match &v0.to_owned()? {
                Value::Numeric(Numeric::Integer(id)) => *id,
                _ => return Ok(IOResult::Done(None)),
            };
            let found_zset_hash = match &v1.to_owned()? {
                Value::Blob(blob) => Hash128::from_blob(blob).ok_or_else(|| {
                    crate::LimboError::InternalError("Invalid zset_hash blob".to_string())
                })?,
                _ => return Ok(IOResult::Done(None)),
            };
            let element_hash = match &v2.to_owned()? {
                Value::Blob(blob) => Hash128::from_blob(blob).ok_or_else(|| {
                    crate::LimboError::InternalError("Invalid element_hash blob".to_string())
                })?,
                _ => {
                    return Ok(IOResult::Done(None));
                }
            };
            (found_storage_id, found_zset_hash, element_hash)
        } else {
            return Ok(IOResult::Done(None));
        }
    } else {
        return Ok(IOResult::Done(None));
    };

    // Now we can safely check if we're in the right range
    // If we've moved to a different storage_id or zset_id, we're done
    if found_storage_id != storage_id || found_zset_hash != zset_hash {
        return Ok(IOResult::Done(None));
    }

    // Now get the actual row from the table using the rowid from the index
    let rowid = return_if_io!(cursors.index_cursor.rowid());
    if let Some(rowid) = rowid {
        return_if_io!(cursors
            .table_cursor
            .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));

        let table_record = return_if_io!(cursors.table_cursor.record());
        if let Some(rec) = table_record {
            let table_values = rec.get_two_values(3, 4);
            // Table format: [storage_id, zset_id, element_id, value_blob, weight]
            if let Ok((value_at_3, value_at_4)) = table_values {
                // Deserialize the row from the blob
                let value_at_3 = value_at_3.to_owned()?;
                let blob = match value_at_3 {
                    Value::Blob(ref b) => b,
                    _ => return Ok(IOResult::Done(None)),
                };

                // The blob contains the serialized HashableRow
                // For now, let's deserialize it simply
                let row = deserialize_hashable_row(blob)?;

                let weight = match &value_at_4.to_owned()? {
                    Value::Numeric(Numeric::Integer(w)) => *w as isize,
                    _ => return Ok(IOResult::Done(None)),
                };

                return Ok(IOResult::Done(Some((element_hash, row, weight))));
            }
        }
    }
    Ok(IOResult::Done(None))
}

/// Join operator - performs incremental join between two relations
/// Implements the DBSP formula: δ(R ⋈ S) = (δR ⋈ S) ∪ (R ⋈ δS) ∪ (δR ⋈ δS)
#[derive(Debug)]
pub struct JoinOperator {
    /// Unique operator ID for indexing in persistent storage
    operator_id: i64,
    /// Type of join to perform
    join_type: JoinType,
    /// Column indices for extracting join keys from left input
    left_key_indices: Vec<usize>,
    /// Column indices for extracting join keys from right input
    right_key_indices: Vec<usize>,
    /// Column names from left input
    left_columns: Vec<String>,
    /// Column names from right input
    right_columns: Vec<String>,
    /// Tracker for computation statistics
    tracker: Option<Arc<Mutex<ComputationTracker>>>,

    ops: JoinOps,
}

impl JoinOperator {
    pub fn new(
        operator_id: i64,
        join_type: JoinType,
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
        left_columns: Vec<String>,
        right_columns: Vec<String>,
    ) -> Result<Self> {
        // Check for unsupported join types
        match join_type {
            JoinType::Left => {
                return Err(crate::LimboError::ParseError(
                    "LEFT OUTER JOIN is not yet supported in incremental views".to_string(),
                ))
            }
            JoinType::Right => {
                return Err(crate::LimboError::ParseError(
                    "RIGHT OUTER JOIN is not yet supported in incremental views".to_string(),
                ))
            }
            JoinType::Full => {
                return Err(crate::LimboError::ParseError(
                    "FULL OUTER JOIN is not yet supported in incremental views".to_string(),
                ))
            }
            JoinType::Cross => {
                return Err(crate::LimboError::ParseError(
                    "CROSS JOIN is not yet supported in incremental views".to_string(),
                ))
            }
            JoinType::Inner => {} // Inner join is supported
        }

        let result = Self {
            operator_id,
            join_type,
            left_key_indices,
            right_key_indices,
            left_columns,
            right_columns,
            tracker: None,
            ops: JoinOps::default(),
        };
        Ok(result)
    }

    /// Extract join key from row values using the specified indices
    fn extract_join_key(&self, values: &[Value], indices: &[usize]) -> HashableRow {
        let key_values: Vec<Value> = indices
            .iter()
            .map(|&idx| values.get(idx).cloned().unwrap_or(Value::Null))
            .collect();
        // Use 0 as a dummy rowid for join keys. They don't come from a table,
        // so they don't need a rowid. Their key will be the hash of the row values.
        HashableRow::new(0, key_values)
    }

    /// Generate storage ID for left table
    fn left_storage_id(&self) -> i64 {
        // Use column_index=0 for left side
        generate_storage_id(self.operator_id, 0, 0)
    }

    /// Generate storage ID for right table
    fn right_storage_id(&self) -> i64 {
        // Use column_index=1 for right side
        generate_storage_id(self.operator_id, 1, 0)
    }

    /// SQL-compliant comparison for join keys
    /// Returns true if keys match according to SQL semantics (NULL != NULL)
    fn sql_keys_equal(left_key: &HashableRow, right_key: &HashableRow) -> bool {
        if left_key.values.len() != right_key.values.len() {
            return false;
        }

        for (left_val, right_val) in left_key.values.iter().zip(right_key.values.iter()) {
            // In SQL, NULL never equals NULL
            if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                return false;
            }

            // For non-NULL values, use regular comparison
            if left_val != right_val {
                return false;
            }
        }

        true
    }

    /// Joins the left delta with the right delta in memory: the part of the
    /// join that needs no stored rows.
    fn join_delta_with_delta(&self, deltas: &DeltaPair) -> Delta {
        let mut output = Delta::new();
        for (left_row, left_weight) in &deltas.left.changes {
            let left_key = self.extract_join_key(&left_row.values, &self.left_key_indices);

            for (right_row, right_weight) in &deltas.right.changes {
                let right_key = self.extract_join_key(&right_row.values, &self.right_key_indices);

                if Self::sql_keys_equal(&left_key, &right_key) {
                    if let Some(tracker) = &self.tracker {
                        tracker.lock().record_join_lookup();
                    }
                    combine_rows(
                        left_row,
                        *left_weight as i64,
                        right_row,
                        *right_weight as i64,
                        &mut output,
                    );
                }
            }
        }
        output
    }

    /// Runs one step of the eval: starts a new eval from an `Init` state and
    /// resumes a suspended one from a `Join` state. The runner lives in the
    /// state while the eval waits for I/O, and in the operator otherwise.
    fn step_eval(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> IOResultOr<Delta> {
        match state {
            EvalState::Uninitialized => {
                panic!("Cannot eval JoinOperator with Uninitialized state");
            }
            EvalState::Done => return Ok(IOResult::Done(Delta::new())),
            EvalState::Aggregate(_) => {
                panic!("Aggregate state should not appear in join operator");
            }
            EvalState::Init { .. } | EvalState::Join(_) => {}
        }
        let (mut op, deltas) = match std::mem::replace(state, EvalState::Uninitialized) {
            EvalState::Init { deltas } => {
                (self.ops.eval.take().unwrap_or_else(new_eval_runner), deltas)
            }
            EvalState::Join(op) => (op, DeltaPair::default()),
            _ => unreachable!("checked above"),
        };
        let mut ctx = JoinCtx {
            operator: self,
            cursors,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, deltas);
        if op.is_active() {
            *state = EvalState::Join(op);
        } else {
            *state = EvalState::Done;
            self.ops.eval = Some(op);
        }
        result
    }
}

fn new_eval_runner() -> JoinEvalOp {
    OpRunner::new(Runner::boxed(|co, args| with_handle(co, args, eval_deltas)))
}

/// Evaluates the join of a pair of deltas: the left delta against the right
/// delta in memory, then each delta against the stored rows of the other
/// side.
async fn eval_deltas(co: &mut Co<JoinStep>, deltas: DeltaPair) -> Result<Delta, Box<LimboError>> {
    let mut output = co.with(|ctx| ctx.operator.join_delta_with_delta(&deltas));
    let (left_storage_id, right_storage_id) = co.with(|ctx| {
        (
            ctx.operator.left_storage_id(),
            ctx.operator.right_storage_id(),
        )
    });

    for (left_row, left_weight) in &deltas.left.changes {
        let left_key = co.with(|ctx| {
            ctx.operator
                .extract_join_key(&left_row.values, &ctx.operator.left_key_indices)
        });
        let mut last_row_scanned = None;
        while let Some((element_hash, right_row, right_weight)) = co
            .io(|ctx| {
                read_next_join_row(right_storage_id, &left_key, last_row_scanned, ctx.cursors)
            })
            .await
        {
            combine_rows(
                left_row,
                *left_weight as i64,
                &right_row,
                right_weight as i64,
                &mut output,
            );
            last_row_scanned = Some(element_hash);
        }
    }

    for (right_row, right_weight) in &deltas.right.changes {
        let right_key = co.with(|ctx| {
            ctx.operator
                .extract_join_key(&right_row.values, &ctx.operator.right_key_indices)
        });
        let mut last_row_scanned = None;
        while let Some((element_hash, left_row, left_weight)) = co
            .io(|ctx| {
                read_next_join_row(left_storage_id, &right_key, last_row_scanned, ctx.cursors)
            })
            .await
        {
            combine_rows(
                &left_row,
                left_weight as i64,
                right_row,
                *right_weight as i64,
                &mut output,
            );
            last_row_scanned = Some(element_hash);
        }
    }

    Ok(output)
}

fn new_commit_runner() -> JoinCommitOp {
    OpRunner::new(Runner::boxed(|co, args| {
        with_handle(co, args, commit_deltas)
    }))
}

/// Commits a pair of deltas: evaluates the join, then stores every row of
/// both deltas with its weight so that later evals can join against it.
async fn commit_deltas(co: &mut Co<JoinStep>, deltas: DeltaPair) -> Result<Delta, Box<LimboError>> {
    let output = eval_deltas(co, deltas.clone()).await?;

    for (row, weight) in &deltas.left.changes {
        let (index_key, record_values) = co.with(|ctx| {
            let operator = &ctx.operator;
            let join_key = operator.extract_join_key(&row.values, &operator.left_key_indices);
            stored_row(operator.left_storage_id(), &join_key, row)
        })?;
        let mut write_row = WriteRow::new();
        co.io(|ctx| {
            write_row.write_row(
                ctx.cursors,
                index_key.clone(),
                record_values.clone(),
                *weight,
            )
        })
        .await;
    }

    for (row, weight) in &deltas.right.changes {
        let (index_key, record_values) = co.with(|ctx| {
            let operator = &ctx.operator;
            let join_key = operator.extract_join_key(&row.values, &operator.right_key_indices);
            stored_row(operator.right_storage_id(), &join_key, row)
        })?;
        let mut write_row = WriteRow::new();
        co.io(|ctx| {
            write_row.write_row(
                ctx.cursors,
                index_key.clone(),
                record_values.clone(),
                *weight,
            )
        })
        .await;
    }

    Ok(output)
}

/// The index key and the record of one stored join row. The index key is
/// (storage_id, hash of the join key, hash of the row), and the record adds
/// the serialized row as a blob.
fn stored_row(
    storage_id: i64,
    join_key: &HashableRow,
    row: &HashableRow,
) -> Result<(Vec<Value>, Vec<Value>)> {
    let zset_hash = join_key.cached_hash();
    let element_hash = row.cached_hash();
    let index_key = vec![
        Value::from_i64(storage_id),
        zset_hash.to_value()?,
        element_hash.to_value()?,
    ];
    let row_blob = serialize_hashable_row(row)?;
    let record_values = vec![
        Value::from_i64(storage_id),
        zset_hash.to_value()?,
        element_hash.to_value()?,
        Value::Blob(row_blob),
    ];
    Ok((index_key, record_values))
}

/// Adds the row that joins `left_row` with `right_row` to the output. The
/// rowid of the joined row is the hash of the combined values.
fn combine_rows(
    left_row: &HashableRow,
    left_weight: i64,
    right_row: &HashableRow,
    right_weight: i64,
    output: &mut Delta,
) {
    let mut combined_values = left_row.values.clone();
    combined_values.extend(right_row.values.clone());
    let temp_row = HashableRow::new(0, combined_values.clone());
    let joined_rowid = temp_row.cached_hash().as_i64();
    let joined_row = HashableRow::new(joined_rowid, combined_values);

    let combined_weight = left_weight * right_weight;
    output.changes.push((joined_row, combined_weight as isize));
}

fn deserialize_hashable_row(blob: &[u8]) -> Result<HashableRow> {
    let record = ImmutableRecordRef::from_bin_record(blob);
    let all_values = record.get_values_owned()?;

    if all_values.is_empty() {
        return Err(crate::LimboError::InternalError(
            "HashableRow blob must contain at least rowid".to_string(),
        ));
    }

    // First value is the rowid
    let rowid = match &all_values[0] {
        Value::Numeric(Numeric::Integer(i)) => *i,
        _ => {
            return Err(crate::LimboError::InternalError(
                "First value must be rowid (integer)".to_string(),
            ))
        }
    };

    // Rest are the row values
    // TODO: std boundary conversion; adjust once incremental uses the
    // allocator with fallible allocations everywhere.
    let values = all_values[1..].to_vec();

    Ok(HashableRow::new(rowid, values))
}

fn serialize_hashable_row(row: &HashableRow) -> Result<crate::ValueBlob> {
    use crate::types::ImmutableRecord;

    let mut all_values = Vec::with_capacity(row.values.len() + 1);
    all_values.push(Value::from_i64(row.rowid));
    all_values.extend_from_slice(&row.values);

    let record = ImmutableRecord::from_values(&all_values, all_values.len())?;
    Ok(record.into_payload())
}

impl IncrementalOperator for JoinOperator {
    fn eval(&mut self, state: &mut EvalState, cursors: &mut DbspStateCursors) -> IOResultOr<Delta> {
        self.step_eval(state, cursors)
    }

    fn commit(&mut self, deltas: DeltaPair, cursors: &mut DbspStateCursors) -> IOResultOr<Delta> {
        let mut op = self.ops.commit.take().unwrap_or_else(new_commit_runner);
        let mut ctx = JoinCtx {
            operator: self,
            cursors,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, deltas);
        self.ops.commit = Some(op);
        result
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}
