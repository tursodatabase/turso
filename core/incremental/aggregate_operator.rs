// Aggregate operator for DBSP-style incremental computation

use crate::function::{AggFunc, Func};
use crate::incremental::dbsp::Hash128;
use crate::incremental::dbsp::{Delta, DeltaPair, HashableRow};
use crate::incremental::operator::{
    generate_storage_id, ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
};
use crate::incremental::persistence::{ReadRecord, WriteRow};
use crate::types::{IOResult, ImmutableRecord, RefValue, SeekKey, SeekOp, SeekResult};
use crate::{return_and_restore_if_io, return_if_io, LimboError, Result, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Display};
use std::sync::{Arc, Mutex};

/// Constants for aggregate type encoding in storage IDs (2 bits)
pub const AGG_TYPE_REGULAR: u8 = 0b00; // COUNT/SUM/AVG
pub const AGG_TYPE_MINMAX: u8 = 0b01; // MIN/MAX (BTree ordering gives both)

// Serialization type codes for aggregate functions
const AGG_FUNC_COUNT: i64 = 0;
const AGG_FUNC_SUM: i64 = 1;
const AGG_FUNC_AVG: i64 = 2;
const AGG_FUNC_MIN: i64 = 3;
const AGG_FUNC_MAX: i64 = 4;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum(usize), // Column index
    Avg(usize), // Column index
    Min(usize), // Column index
    Max(usize), // Column index
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Count => write!(f, "COUNT(*)"),
            AggregateFunction::Sum(idx) => write!(f, "SUM(col{idx})"),
            AggregateFunction::Avg(idx) => write!(f, "AVG(col{idx})"),
            AggregateFunction::Min(idx) => write!(f, "MIN(col{idx})"),
            AggregateFunction::Max(idx) => write!(f, "MAX(col{idx})"),
        }
    }
}

impl AggregateFunction {
    /// Get the default output column name for this aggregate function
    #[inline]
    pub fn default_output_name(&self) -> String {
        self.to_string()
    }

    /// Serialize this aggregate function to a Value
    /// Returns a vector of values: [type_code, optional_column_index]
    pub fn to_values(&self) -> Vec<Value> {
        match self {
            AggregateFunction::Count => vec![Value::Integer(AGG_FUNC_COUNT)],
            AggregateFunction::Sum(idx) => {
                vec![Value::Integer(AGG_FUNC_SUM), Value::Integer(*idx as i64)]
            }
            AggregateFunction::Avg(idx) => {
                vec![Value::Integer(AGG_FUNC_AVG), Value::Integer(*idx as i64)]
            }
            AggregateFunction::Min(idx) => {
                vec![Value::Integer(AGG_FUNC_MIN), Value::Integer(*idx as i64)]
            }
            AggregateFunction::Max(idx) => {
                vec![Value::Integer(AGG_FUNC_MAX), Value::Integer(*idx as i64)]
            }
        }
    }

    /// Deserialize an aggregate function from values
    /// Consumes values from the cursor and returns the aggregate function
    pub fn from_values(values: &[Value], cursor: &mut usize) -> Result<Self> {
        let type_code = values
            .get(*cursor)
            .ok_or_else(|| LimboError::InternalError("Missing aggregate type code".into()))?;

        let agg_fn = match type_code {
            Value::Integer(AGG_FUNC_COUNT) => {
                *cursor += 1;
                AggregateFunction::Count
            }
            Value::Integer(AGG_FUNC_SUM) => {
                *cursor += 1;
                let idx = values
                    .get(*cursor)
                    .ok_or_else(|| LimboError::InternalError("Missing SUM column index".into()))?;
                if let Value::Integer(idx) = idx {
                    *cursor += 1;
                    AggregateFunction::Sum(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for SUM column index, got {idx:?}"
                    )));
                }
            }
            Value::Integer(AGG_FUNC_AVG) => {
                *cursor += 1;
                let idx = values
                    .get(*cursor)
                    .ok_or_else(|| LimboError::InternalError("Missing AVG column index".into()))?;
                if let Value::Integer(idx) = idx {
                    *cursor += 1;
                    AggregateFunction::Avg(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for AVG column index, got {idx:?}"
                    )));
                }
            }
            Value::Integer(AGG_FUNC_MIN) => {
                *cursor += 1;
                let idx = values
                    .get(*cursor)
                    .ok_or_else(|| LimboError::InternalError("Missing MIN column index".into()))?;
                if let Value::Integer(idx) = idx {
                    *cursor += 1;
                    AggregateFunction::Min(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for MIN column index, got {idx:?}"
                    )));
                }
            }
            Value::Integer(AGG_FUNC_MAX) => {
                *cursor += 1;
                let idx = values
                    .get(*cursor)
                    .ok_or_else(|| LimboError::InternalError("Missing MAX column index".into()))?;
                if let Value::Integer(idx) = idx {
                    *cursor += 1;
                    AggregateFunction::Max(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for MAX column index, got {idx:?}"
                    )));
                }
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "Unknown aggregate type code: {type_code:?}"
                )))
            }
        };

        Ok(agg_fn)
    }

    /// Create an AggregateFunction from a SQL function and its arguments
    /// Returns None if the function is not a supported aggregate
    pub fn from_sql_function(
        func: &crate::function::Func,
        input_column_idx: Option<usize>,
    ) -> Option<Self> {
        match func {
            Func::Agg(agg_func) => {
                match agg_func {
                    AggFunc::Count | AggFunc::Count0 => Some(AggregateFunction::Count),
                    AggFunc::Sum => input_column_idx.map(AggregateFunction::Sum),
                    AggFunc::Avg => input_column_idx.map(AggregateFunction::Avg),
                    AggFunc::Min => input_column_idx.map(AggregateFunction::Min),
                    AggFunc::Max => input_column_idx.map(AggregateFunction::Max),
                    _ => None, // Other aggregate functions not yet supported in DBSP
                }
            }
            _ => None, // Not an aggregate function
        }
    }
}

/// Information about a column that has MIN/MAX aggregations
#[derive(Debug, Clone)]
pub struct AggColumnInfo {
    /// Index used for storage key generation
    pub index: usize,
    /// Whether this column has a MIN aggregate
    pub has_min: bool,
    /// Whether this column has a MAX aggregate
    pub has_max: bool,
}

// group_key_str -> (group_key, state)
type ComputedStates = HashMap<String, (Vec<Value>, AggregateState)>;
// group_key_str -> (column_index, value_as_hashable_row) -> accumulated_weight
pub type MinMaxDeltas = HashMap<String, HashMap<(usize, HashableRow), isize>>;

#[derive(Debug)]
enum AggregateCommitState {
    Idle,
    Eval {
        eval_state: EvalState,
    },
    PersistDelta {
        delta: Delta,
        computed_states: ComputedStates,
        current_idx: usize,
        write_row: WriteRow,
        min_max_deltas: MinMaxDeltas,
    },
    PersistMinMax {
        delta: Delta,
        min_max_persist_state: MinMaxPersistState,
    },
    Done {
        delta: Delta,
    },
    Invalid,
}

// Aggregate-specific eval states
#[derive(Debug)]
pub enum AggregateEvalState {
    FetchKey {
        delta: Delta, // Keep original delta for merge operation
        current_idx: usize,
        groups_to_read: Vec<(String, Vec<Value>)>, // Changed to Vec for index-based access
        existing_groups: HashMap<String, AggregateState>,
        old_values: HashMap<String, Vec<Value>>,
    },
    FetchData {
        delta: Delta, // Keep original delta for merge operation
        current_idx: usize,
        groups_to_read: Vec<(String, Vec<Value>)>, // Changed to Vec for index-based access
        existing_groups: HashMap<String, AggregateState>,
        old_values: HashMap<String, Vec<Value>>,
        rowid: Option<i64>, // Rowid found by FetchKey (None if not found)
        read_record_state: Box<ReadRecord>,
    },
    RecomputeMinMax {
        delta: Delta,
        existing_groups: HashMap<String, AggregateState>,
        old_values: HashMap<String, Vec<Value>>,
        recompute_state: Box<RecomputeMinMax>,
    },
    Done {
        output: (Delta, ComputedStates),
    },
}

/// Note that the AggregateOperator essentially implements a ZSet, even
/// though the ZSet structure is never used explicitly. The on-disk btree
/// plays the role of the set!
#[derive(Debug)]
pub struct AggregateOperator {
    // Unique operator ID for indexing in persistent storage
    pub operator_id: i64,
    // GROUP BY column indices
    group_by: Vec<usize>,
    // Aggregate functions to compute (including MIN/MAX)
    pub aggregates: Vec<AggregateFunction>,
    // Column names from input
    pub input_column_names: Vec<String>,
    // Map from column index to aggregate info for quick lookup
    pub column_min_max: HashMap<usize, AggColumnInfo>,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,

    // State machine for commit operation
    commit_state: AggregateCommitState,
}

/// State for a single group's aggregates
#[derive(Debug, Clone, Default)]
pub struct AggregateState {
    // For COUNT: just the count
    pub count: i64,
    // For SUM: column_index -> sum value
    pub sums: HashMap<usize, f64>,
    // For AVG: column_index -> (sum, count) for computing average
    pub avgs: HashMap<usize, (f64, i64)>,
    // For MIN: column_index -> minimum value
    pub mins: HashMap<usize, Value>,
    // For MAX: column_index -> maximum value
    pub maxs: HashMap<usize, Value>,
}

impl AggregateEvalState {
    fn process_delta(
        &mut self,
        operator: &mut AggregateOperator,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<(Delta, ComputedStates)>> {
        loop {
            match self {
                AggregateEvalState::FetchKey {
                    delta,
                    current_idx,
                    groups_to_read,
                    existing_groups,
                    old_values,
                } => {
                    if *current_idx >= groups_to_read.len() {
                        // All groups have been fetched, move to RecomputeMinMax
                        // Extract MIN/MAX deltas from the input delta
                        let min_max_deltas = operator.extract_min_max_deltas(delta);

                        let recompute_state = Box::new(RecomputeMinMax::new(
                            min_max_deltas,
                            existing_groups,
                            operator,
                        ));

                        *self = AggregateEvalState::RecomputeMinMax {
                            delta: std::mem::take(delta),
                            existing_groups: std::mem::take(existing_groups),
                            old_values: std::mem::take(old_values),
                            recompute_state,
                        };
                    } else {
                        // Get the current group to read
                        let (group_key_str, _group_key) = &groups_to_read[*current_idx];

                        // Build the key for the index: (operator_id, zset_hash, element_id)
                        // For regular aggregates, use column_index=0 and AGG_TYPE_REGULAR
                        let operator_storage_id =
                            generate_storage_id(operator.operator_id, 0, AGG_TYPE_REGULAR);
                        let zset_hash = operator.generate_group_hash(group_key_str);
                        let element_id = 0i64; // Always 0 for aggregators

                        // Create index key values
                        let index_key_values = vec![
                            Value::Integer(operator_storage_id),
                            zset_hash.to_value(),
                            Value::Integer(element_id),
                        ];

                        // Create an immutable record for the index key
                        let index_record =
                            ImmutableRecord::from_values(&index_key_values, index_key_values.len());

                        // Seek in the index to find if this row exists
                        let seek_result = return_if_io!(cursors.index_cursor.seek(
                            SeekKey::IndexKey(&index_record),
                            SeekOp::GE { eq_only: true }
                        ));

                        let rowid = if matches!(seek_result, SeekResult::Found) {
                            // Found in index, get the table rowid
                            // The btree code handles extracting the rowid from the index record for has_rowid indexes
                            return_if_io!(cursors.index_cursor.rowid())
                        } else {
                            // Not found in index, no existing state
                            None
                        };

                        // Always transition to FetchData
                        let taken_existing = std::mem::take(existing_groups);
                        let taken_old_values = std::mem::take(old_values);
                        let next_state = AggregateEvalState::FetchData {
                            delta: std::mem::take(delta),
                            current_idx: *current_idx,
                            groups_to_read: std::mem::take(groups_to_read),
                            existing_groups: taken_existing,
                            old_values: taken_old_values,
                            rowid,
                            read_record_state: Box::new(ReadRecord::new()),
                        };
                        *self = next_state;
                    }
                }
                AggregateEvalState::FetchData {
                    delta,
                    current_idx,
                    groups_to_read,
                    existing_groups,
                    old_values,
                    rowid,
                    read_record_state,
                } => {
                    // Get the current group to read
                    let (group_key_str, group_key) = &groups_to_read[*current_idx];

                    // Only try to read if we have a rowid
                    if let Some(rowid) = rowid {
                        let key = SeekKey::TableRowId(*rowid);
                        let state = return_if_io!(
                            read_record_state.read_record(key, &mut cursors.table_cursor)
                        );
                        // Process the fetched state
                        if let Some(state) = state {
                            let mut old_row = group_key.clone();
                            old_row.extend(state.to_values(&operator.aggregates));
                            old_values.insert(group_key_str.clone(), old_row);
                            existing_groups.insert(group_key_str.clone(), state.clone());
                        }
                    } else {
                        // No rowid for this group, skipping read
                    }
                    // If no rowid, there's no existing state for this group

                    // Move to next group
                    let next_idx = *current_idx + 1;
                    let taken_existing = std::mem::take(existing_groups);
                    let taken_old_values = std::mem::take(old_values);
                    let next_state = AggregateEvalState::FetchKey {
                        delta: std::mem::take(delta),
                        current_idx: next_idx,
                        groups_to_read: std::mem::take(groups_to_read),
                        existing_groups: taken_existing,
                        old_values: taken_old_values,
                    };
                    *self = next_state;
                }
                AggregateEvalState::RecomputeMinMax {
                    delta,
                    existing_groups,
                    old_values,
                    recompute_state,
                } => {
                    if operator.has_min_max() {
                        // Process MIN/MAX recomputation - this will update existing_groups with correct MIN/MAX
                        return_if_io!(recompute_state.process(existing_groups, operator, cursors));
                    }

                    // Now compute final output with updated MIN/MAX values
                    let (output_delta, computed_states) =
                        operator.merge_delta_with_existing(delta, existing_groups, old_values);

                    *self = AggregateEvalState::Done {
                        output: (output_delta, computed_states),
                    };
                }
                AggregateEvalState::Done { output } => {
                    return Ok(IOResult::Done(output.clone()));
                }
            }
        }
    }
}

impl AggregateState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert the aggregate state to a vector of Values for unified serialization
    /// Format: [count, num_aggregates, (agg_metadata, agg_state)...]
    /// Each aggregate includes its type and column index for proper deserialization
    pub fn to_value_vector(&self, aggregates: &[AggregateFunction]) -> Vec<Value> {
        let mut values = Vec::new();

        // Include count first
        values.push(Value::Integer(self.count));

        // Store number of aggregates
        values.push(Value::Integer(aggregates.len() as i64));

        // Add each aggregate's metadata and state
        for agg in aggregates {
            // First, add the aggregate function metadata (type and column index)
            values.extend(agg.to_values());

            // Then add the state for this aggregate
            match agg {
                AggregateFunction::Count => {
                    // Count state is already stored at the beginning
                }
                AggregateFunction::Sum(col_idx) => {
                    let sum = self.sums.get(col_idx).copied().unwrap_or(0.0);
                    values.push(Value::Float(sum));
                }
                AggregateFunction::Avg(col_idx) => {
                    let (sum, count) = self.avgs.get(col_idx).copied().unwrap_or((0.0, 0));
                    values.push(Value::Float(sum));
                    values.push(Value::Integer(count));
                }
                AggregateFunction::Min(col_idx) => {
                    if let Some(min_val) = self.mins.get(col_idx) {
                        values.push(Value::Integer(1)); // Has value
                        values.push(min_val.clone());
                    } else {
                        values.push(Value::Integer(0)); // No value
                    }
                }
                AggregateFunction::Max(col_idx) => {
                    if let Some(max_val) = self.maxs.get(col_idx) {
                        values.push(Value::Integer(1)); // Has value
                        values.push(max_val.clone());
                    } else {
                        values.push(Value::Integer(0)); // No value
                    }
                }
            }
        }

        values
    }

    /// Reconstruct aggregate state from a vector of Values
    pub fn from_value_vector(values: &[Value]) -> Result<Self> {
        let mut cursor = 0;
        let mut state = Self::new();

        // Read count
        let count = values
            .get(cursor)
            .ok_or_else(|| LimboError::InternalError("Aggregate state missing count".into()))?;
        if let Value::Integer(count) = count {
            state.count = *count;
            cursor += 1;
        } else {
            return Err(LimboError::InternalError(format!(
                "Expected Integer for count, got {count:?}"
            )));
        }

        // Read number of aggregates
        let num_aggregates = values
            .get(cursor)
            .ok_or_else(|| LimboError::InternalError("Missing number of aggregates".into()))?;
        let num_aggregates = match num_aggregates {
            Value::Integer(n) => *n as usize,
            _ => {
                return Err(LimboError::InternalError(format!(
                    "Expected Integer for aggregate count, got {num_aggregates:?}"
                )))
            }
        };
        cursor += 1;

        // Read each aggregate's state with type and column index
        for _ in 0..num_aggregates {
            // Deserialize the aggregate function metadata
            let agg_fn = AggregateFunction::from_values(values, &mut cursor)?;

            // Read the state for this aggregate
            match agg_fn {
                AggregateFunction::Count => {
                    // Count state is already stored at the beginning
                }
                AggregateFunction::Sum(col_idx) => {
                    let sum = values
                        .get(cursor)
                        .ok_or_else(|| LimboError::InternalError("Missing SUM value".into()))?;
                    if let Value::Float(sum) = sum {
                        state.sums.insert(col_idx, *sum);
                        cursor += 1;
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Expected Float for SUM value, got {sum:?}"
                        )));
                    }
                }
                AggregateFunction::Avg(col_idx) => {
                    let sum = values
                        .get(cursor)
                        .ok_or_else(|| LimboError::InternalError("Missing AVG sum value".into()))?;
                    let sum = match sum {
                        Value::Float(f) => *f,
                        _ => {
                            return Err(LimboError::InternalError(format!(
                                "Expected Float for AVG sum, got {sum:?}"
                            )))
                        }
                    };
                    cursor += 1;

                    let count = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing AVG count value".into())
                    })?;
                    let count = match count {
                        Value::Integer(i) => *i,
                        _ => {
                            return Err(LimboError::InternalError(format!(
                                "Expected Integer for AVG count, got {count:?}"
                            )))
                        }
                    };
                    cursor += 1;

                    state.avgs.insert(col_idx, (sum, count));
                }
                AggregateFunction::Min(col_idx) => {
                    let has_value = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing MIN has_value flag".into())
                    })?;
                    if let Value::Integer(has_value) = has_value {
                        cursor += 1;
                        if *has_value == 1 {
                            let min_val = values
                                .get(cursor)
                                .ok_or_else(|| {
                                    LimboError::InternalError("Missing MIN value".into())
                                })?
                                .clone();
                            cursor += 1;
                            state.mins.insert(col_idx, min_val);
                        }
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Expected Integer for MIN has_value flag, got {has_value:?}"
                        )));
                    }
                }
                AggregateFunction::Max(col_idx) => {
                    let has_value = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing MAX has_value flag".into())
                    })?;
                    if let Value::Integer(has_value) = has_value {
                        cursor += 1;
                        if *has_value == 1 {
                            let max_val = values
                                .get(cursor)
                                .ok_or_else(|| {
                                    LimboError::InternalError("Missing MAX value".into())
                                })?
                                .clone();
                            cursor += 1;
                            state.maxs.insert(col_idx, max_val);
                        }
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Expected Integer for MAX has_value flag, got {has_value:?}"
                        )));
                    }
                }
            }
        }

        Ok(state)
    }

    fn to_blob(&self, aggregates: &[AggregateFunction], group_key: &[Value]) -> Vec<u8> {
        let mut all_values = Vec::new();
        // Store the group key size first
        all_values.push(Value::Integer(group_key.len() as i64));
        all_values.extend_from_slice(group_key);
        all_values.extend(self.to_value_vector(aggregates));

        let record = ImmutableRecord::from_values(&all_values, all_values.len());
        record.as_blob().clone()
    }

    pub fn from_blob(blob: &[u8]) -> Result<(Self, Vec<Value>)> {
        let record = ImmutableRecord::from_bin_record(blob.to_vec());
        let ref_values = record.get_values();
        let mut all_values: Vec<Value> = ref_values.into_iter().map(|rv| rv.to_owned()).collect();

        if all_values.is_empty() {
            return Err(LimboError::InternalError(
                "Aggregate state blob is empty".into(),
            ));
        }

        // Read the group key size
        let group_key_count = match &all_values[0] {
            Value::Integer(n) if *n >= 0 => *n as usize,
            Value::Integer(n) => {
                return Err(LimboError::InternalError(format!(
                    "Negative group key count: {n}"
                )))
            }
            other => {
                return Err(LimboError::InternalError(format!(
                    "Expected Integer for group key count, got {other:?}"
                )))
            }
        };

        // Remove the group key count from the values
        all_values.remove(0);

        if all_values.len() < group_key_count {
            return Err(LimboError::InternalError(format!(
                "Blob too short: expected at least {} values for group key, got {}",
                group_key_count,
                all_values.len()
            )));
        }

        // Split into group key and state values
        let group_key = all_values[..group_key_count].to_vec();
        let state_values = &all_values[group_key_count..];

        // Reconstruct the aggregate state
        let state = Self::from_value_vector(state_values)?;

        Ok((state, group_key))
    }

    /// Apply a delta to this aggregate state
    fn apply_delta(
        &mut self,
        values: &[Value],
        weight: isize,
        aggregates: &[AggregateFunction],
        _column_names: &[String], // No longer needed
    ) {
        // Update COUNT
        self.count += weight as i64;

        // Update other aggregates
        for agg in aggregates {
            match agg {
                AggregateFunction::Count => {
                    // Already handled above
                }
                AggregateFunction::Sum(col_idx) => {
                    if let Some(val) = values.get(*col_idx) {
                        let num_val = match val {
                            Value::Integer(i) => *i as f64,
                            Value::Float(f) => *f,
                            _ => 0.0,
                        };
                        *self.sums.entry(*col_idx).or_insert(0.0) += num_val * weight as f64;
                    }
                }
                AggregateFunction::Avg(col_idx) => {
                    if let Some(val) = values.get(*col_idx) {
                        let num_val = match val {
                            Value::Integer(i) => *i as f64,
                            Value::Float(f) => *f,
                            _ => 0.0,
                        };
                        let (sum, count) = self.avgs.entry(*col_idx).or_insert((0.0, 0));
                        *sum += num_val * weight as f64;
                        *count += weight as i64;
                    }
                }
                AggregateFunction::Min(_col_name) | AggregateFunction::Max(_col_name) => {
                    // MIN/MAX cannot be handled incrementally in apply_delta because:
                    //
                    // 1. For insertions: We can't just keep the minimum/maximum value.
                    //    We need to track ALL values to handle future deletions correctly.
                    //
                    // 2. For deletions (retractions): If we delete the current MIN/MAX,
                    //    we need to find the next best value, which requires knowing all
                    //    other values in the group.
                    //
                    // Example: Consider MIN(price) with values [10, 20, 30]
                    // - Current MIN = 10
                    // - Delete 10 (weight = -1)
                    // - New MIN should be 20, but we can't determine this without
                    //   having tracked all values [20, 30]
                    //
                    // Therefore, MIN/MAX processing is handled separately:
                    // - All input values are persisted to the index via persist_min_max()
                    // - When aggregates have MIN/MAX, we unconditionally transition to
                    //   the RecomputeMinMax state machine (see EvalState::RecomputeMinMax)
                    // - RecomputeMinMax checks if the current MIN/MAX was deleted, and if so,
                    //   scans the index to find the new MIN/MAX from remaining values
                    //
                    // This ensures correctness for incremental computation at the cost of
                    // additional I/O for MIN/MAX operations.
                }
            }
        }
    }

    /// Convert aggregate state to output values
    pub fn to_values(&self, aggregates: &[AggregateFunction]) -> Vec<Value> {
        let mut result = Vec::new();

        for agg in aggregates {
            match agg {
                AggregateFunction::Count => {
                    result.push(Value::Integer(self.count));
                }
                AggregateFunction::Sum(col_idx) => {
                    let sum = self.sums.get(col_idx).copied().unwrap_or(0.0);
                    // Return as integer if it's a whole number, otherwise as float
                    if sum.fract() == 0.0 {
                        result.push(Value::Integer(sum as i64));
                    } else {
                        result.push(Value::Float(sum));
                    }
                }
                AggregateFunction::Avg(col_idx) => {
                    if let Some((sum, count)) = self.avgs.get(col_idx) {
                        if *count > 0 {
                            result.push(Value::Float(sum / *count as f64));
                        } else {
                            result.push(Value::Null);
                        }
                    } else {
                        result.push(Value::Null);
                    }
                }
                AggregateFunction::Min(col_idx) => {
                    // Return the MIN value from our state
                    result.push(self.mins.get(col_idx).cloned().unwrap_or(Value::Null));
                }
                AggregateFunction::Max(col_idx) => {
                    // Return the MAX value from our state
                    result.push(self.maxs.get(col_idx).cloned().unwrap_or(Value::Null));
                }
            }
        }

        result
    }
}

impl AggregateOperator {
    pub fn new(
        operator_id: i64,
        group_by: Vec<usize>,
        aggregates: Vec<AggregateFunction>,
        input_column_names: Vec<String>,
    ) -> Self {
        // Build map of column indices to their MIN/MAX info
        let mut column_min_max = HashMap::new();
        let mut storage_indices = HashMap::new();
        let mut current_index = 0;

        // First pass: assign storage indices to unique MIN/MAX columns
        for agg in &aggregates {
            match agg {
                AggregateFunction::Min(col_idx) | AggregateFunction::Max(col_idx) => {
                    storage_indices.entry(*col_idx).or_insert_with(|| {
                        let idx = current_index;
                        current_index += 1;
                        idx
                    });
                }
                _ => {}
            }
        }

        // Second pass: build the column info map
        for agg in &aggregates {
            match agg {
                AggregateFunction::Min(col_idx) => {
                    let storage_index = *storage_indices.get(col_idx).unwrap();
                    let entry = column_min_max.entry(*col_idx).or_insert(AggColumnInfo {
                        index: storage_index,
                        has_min: false,
                        has_max: false,
                    });
                    entry.has_min = true;
                }
                AggregateFunction::Max(col_idx) => {
                    let storage_index = *storage_indices.get(col_idx).unwrap();
                    let entry = column_min_max.entry(*col_idx).or_insert(AggColumnInfo {
                        index: storage_index,
                        has_min: false,
                        has_max: false,
                    });
                    entry.has_max = true;
                }
                _ => {}
            }
        }

        Self {
            operator_id,
            group_by,
            aggregates,
            input_column_names,
            column_min_max,
            tracker: None,
            commit_state: AggregateCommitState::Idle,
        }
    }

    pub fn has_min_max(&self) -> bool {
        !self.column_min_max.is_empty()
    }

    fn eval_internal(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<(Delta, ComputedStates)>> {
        match state {
            EvalState::Uninitialized => {
                panic!("Cannot eval AggregateOperator with Uninitialized state");
            }
            EvalState::Init { deltas } => {
                // Aggregate operators only use left_delta, right_delta must be empty
                assert!(
                    deltas.right.is_empty(),
                    "AggregateOperator expects right_delta to be empty"
                );

                if deltas.left.changes.is_empty() {
                    *state = EvalState::Done;
                    return Ok(IOResult::Done((Delta::new(), HashMap::new())));
                }

                let mut groups_to_read = BTreeMap::new();
                for (row, _weight) in &deltas.left.changes {
                    let group_key = self.extract_group_key(&row.values);
                    let group_key_str = Self::group_key_to_string(&group_key);
                    groups_to_read.insert(group_key_str, group_key);
                }

                let delta = std::mem::take(&mut deltas.left);
                *state = EvalState::Aggregate(Box::new(AggregateEvalState::FetchKey {
                    delta,
                    current_idx: 0,
                    groups_to_read: groups_to_read.into_iter().collect(),
                    existing_groups: HashMap::new(),
                    old_values: HashMap::new(),
                }));
            }
            EvalState::Aggregate(_agg_state) => {
                // Already in progress, continue processing below.
            }
            EvalState::Done => {
                panic!("unreachable state! should have returned");
            }
            EvalState::Join(_) => {
                panic!("Join state should not appear in aggregate operator");
            }
        }

        // Process the delta through the aggregate state machine
        match state {
            EvalState::Aggregate(agg_state) => {
                let result = return_if_io!(agg_state.process_delta(self, cursors));
                Ok(IOResult::Done(result))
            }
            _ => panic!("Invalid state for aggregate processing"),
        }
    }

    fn merge_delta_with_existing(
        &mut self,
        delta: &Delta,
        existing_groups: &mut HashMap<String, AggregateState>,
        old_values: &mut HashMap<String, Vec<Value>>,
    ) -> (Delta, HashMap<String, (Vec<Value>, AggregateState)>) {
        let mut output_delta = Delta::new();
        let mut temp_keys: HashMap<String, Vec<Value>> = HashMap::new();

        // Process each change in the delta
        for (row, weight) in &delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_aggregation();
            }

            // Extract group key
            let group_key = self.extract_group_key(&row.values);
            let group_key_str = Self::group_key_to_string(&group_key);

            let state = existing_groups.entry(group_key_str.clone()).or_default();

            temp_keys.insert(group_key_str.clone(), group_key.clone());

            // Apply the delta to the temporary state
            state.apply_delta(
                &row.values,
                *weight,
                &self.aggregates,
                &self.input_column_names,
            );
        }

        // Generate output delta from temporary states and collect final states
        let mut final_states = HashMap::new();

        for (group_key_str, state) in existing_groups {
            let group_key = temp_keys.get(group_key_str).cloned().unwrap_or_default();

            // Generate synthetic rowid for this group
            let result_key = self.generate_group_rowid(group_key_str);

            if let Some(old_row_values) = old_values.get(group_key_str) {
                let old_row = HashableRow::new(result_key, old_row_values.clone());
                output_delta.changes.push((old_row, -1));
            }

            // Always store the state for persistence (even if count=0, we need to delete it)
            final_states.insert(group_key_str.clone(), (group_key.clone(), state.clone()));

            // Only include groups with count > 0 in the output delta
            if state.count > 0 {
                // Build output row: group_by columns + aggregate values
                let mut output_values = group_key.clone();
                let aggregate_values = state.to_values(&self.aggregates);
                output_values.extend(aggregate_values);

                let output_row = HashableRow::new(result_key, output_values.clone());
                output_delta.changes.push((output_row, 1));
            }
        }
        (output_delta, final_states)
    }

    /// Extract MIN/MAX values from delta changes for persistence to index
    fn extract_min_max_deltas(&self, delta: &Delta) -> MinMaxDeltas {
        let mut min_max_deltas: MinMaxDeltas = HashMap::new();

        for (row, weight) in &delta.changes {
            let group_key = self.extract_group_key(&row.values);
            let group_key_str = Self::group_key_to_string(&group_key);

            for agg in &self.aggregates {
                match agg {
                    AggregateFunction::Min(col_idx) | AggregateFunction::Max(col_idx) => {
                        if let Some(val) = row.values.get(*col_idx) {
                            // Skip NULL values - they don't participate in MIN/MAX
                            if val == &Value::Null {
                                continue;
                            }
                            // Create a HashableRow with just this value
                            // Use 0 as rowid since we only care about the value for comparison
                            let hashable_value = HashableRow::new(0, vec![val.clone()]);
                            let key = (*col_idx, hashable_value);

                            let group_entry =
                                min_max_deltas.entry(group_key_str.clone()).or_default();

                            let value_entry = group_entry.entry(key).or_insert(0);

                            // Accumulate the weight
                            *value_entry += weight;
                        }
                    }
                    _ => {} // Ignore non-MIN/MAX aggregates
                }
            }
        }

        min_max_deltas
    }

    pub fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }

    /// Generate a hash for a group
    /// For no GROUP BY: returns a zero hash
    /// For GROUP BY: returns a 128-bit hash of the group key string
    pub fn generate_group_hash(&self, group_key_str: &str) -> Hash128 {
        if self.group_by.is_empty() {
            Hash128::new(0, 0)
        } else {
            Hash128::hash_str(group_key_str)
        }
    }

    /// Generate a rowid for a group (for output rows)
    /// This is NOT the hash used for storage (that's generate_group_hash which returns full 128-bit).
    /// This is a synthetic rowid used in place of SQLite's rowid for aggregate output rows.
    /// We truncate the 128-bit hash to 64 bits for SQLite rowid compatibility.
    pub fn generate_group_rowid(&self, group_key_str: &str) -> i64 {
        let hash = self.generate_group_hash(group_key_str);
        hash.as_i64()
    }

    /// Extract group key values from a row
    pub fn extract_group_key(&self, values: &[Value]) -> Vec<Value> {
        let mut key = Vec::new();

        for &idx in &self.group_by {
            if let Some(val) = values.get(idx) {
                key.push(val.clone());
            } else {
                key.push(Value::Null);
            }
        }

        key
    }

    /// Convert group key to string for indexing (since Value doesn't implement Hash)
    pub fn group_key_to_string(key: &[Value]) -> String {
        key.iter()
            .map(|v| format!("{v:?}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl IncrementalOperator for AggregateOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        let (delta, _) = return_if_io!(self.eval_internal(state, cursors));
        Ok(IOResult::Done(delta))
    }

    fn commit(
        &mut self,
        mut deltas: DeltaPair,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Aggregate operator only uses left delta, right must be empty
        assert!(
            deltas.right.is_empty(),
            "AggregateOperator expects right delta to be empty in commit"
        );
        let delta = std::mem::take(&mut deltas.left);
        loop {
            // Note: because we std::mem::replace here (without it, the borrow checker goes nuts,
            // because we call self.eval_interval, which requires a mutable borrow), we have to
            // restore the state if we return I/O. So we can't use return_if_io!
            let mut state =
                std::mem::replace(&mut self.commit_state, AggregateCommitState::Invalid);
            match &mut state {
                AggregateCommitState::Invalid => {
                    panic!("Reached invalid state! State was replaced, and not replaced back");
                }
                AggregateCommitState::Idle => {
                    let eval_state = EvalState::from_delta(delta.clone());
                    self.commit_state = AggregateCommitState::Eval { eval_state };
                }
                AggregateCommitState::Eval { ref mut eval_state } => {
                    // Extract input delta before eval for MIN/MAX processing
                    let input_delta = eval_state.extract_delta();

                    // Extract MIN/MAX deltas before any I/O operations
                    let min_max_deltas = self.extract_min_max_deltas(&input_delta);

                    // Create a new eval state with the same delta
                    *eval_state = EvalState::from_delta(input_delta.clone());

                    let (output_delta, computed_states) = return_and_restore_if_io!(
                        &mut self.commit_state,
                        state,
                        self.eval_internal(eval_state, cursors)
                    );

                    self.commit_state = AggregateCommitState::PersistDelta {
                        delta: output_delta,
                        computed_states,
                        current_idx: 0,
                        write_row: WriteRow::new(),
                        min_max_deltas, // Store for later use
                    };
                }
                AggregateCommitState::PersistDelta {
                    delta,
                    computed_states,
                    current_idx,
                    write_row,
                    min_max_deltas,
                } => {
                    let states_vec: Vec<_> = computed_states.iter().collect();

                    if *current_idx >= states_vec.len() {
                        // Use the min_max_deltas we extracted earlier from the input delta
                        self.commit_state = AggregateCommitState::PersistMinMax {
                            delta: delta.clone(),
                            min_max_persist_state: MinMaxPersistState::new(min_max_deltas.clone()),
                        };
                    } else {
                        let (group_key_str, (group_key, agg_state)) = states_vec[*current_idx];

                        // Build the key components for the new table structure
                        // For regular aggregates, use column_index=0 and AGG_TYPE_REGULAR
                        let operator_storage_id =
                            generate_storage_id(self.operator_id, 0, AGG_TYPE_REGULAR);
                        let zset_hash = self.generate_group_hash(group_key_str);
                        let element_id = 0i64;

                        // Determine weight: -1 to delete (cancels existing weight=1), 1 to insert/update
                        let weight = if agg_state.count == 0 { -1 } else { 1 };

                        // Serialize the aggregate state with group key (even for deletion, we need a row)
                        let state_blob = agg_state.to_blob(&self.aggregates, group_key);
                        let blob_value = Value::Blob(state_blob);

                        // Build the aggregate storage format: [operator_id, zset_hash, element_id, value, weight]
                        let operator_id_val = Value::Integer(operator_storage_id);
                        let zset_hash_val = zset_hash.to_value();
                        let element_id_val = Value::Integer(element_id);
                        let blob_val = blob_value.clone();

                        // Create index key - the first 3 columns of our primary key
                        let index_key = vec![
                            operator_id_val.clone(),
                            zset_hash_val.clone(),
                            element_id_val.clone(),
                        ];

                        // Record values (without weight)
                        let record_values =
                            vec![operator_id_val, zset_hash_val, element_id_val, blob_val];

                        return_and_restore_if_io!(
                            &mut self.commit_state,
                            state,
                            write_row.write_row(cursors, index_key, record_values, weight)
                        );

                        let delta = std::mem::take(delta);
                        let computed_states = std::mem::take(computed_states);
                        let min_max_deltas = std::mem::take(min_max_deltas);

                        self.commit_state = AggregateCommitState::PersistDelta {
                            delta,
                            computed_states,
                            current_idx: *current_idx + 1,
                            write_row: WriteRow::new(), // Reset for next write
                            min_max_deltas,
                        };
                    }
                }
                AggregateCommitState::PersistMinMax {
                    delta,
                    min_max_persist_state,
                } => {
                    if !self.has_min_max() {
                        let delta = std::mem::take(delta);
                        self.commit_state = AggregateCommitState::Done { delta };
                    } else {
                        return_and_restore_if_io!(
                            &mut self.commit_state,
                            state,
                            min_max_persist_state.persist_min_max(
                                self.operator_id,
                                &self.column_min_max,
                                cursors,
                                |group_key_str| self.generate_group_hash(group_key_str)
                            )
                        );

                        let delta = std::mem::take(delta);
                        self.commit_state = AggregateCommitState::Done { delta };
                    }
                }
                AggregateCommitState::Done { delta } => {
                    self.commit_state = AggregateCommitState::Idle;
                    let delta = std::mem::take(delta);
                    return Ok(IOResult::Done(delta));
                }
            }
        }
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

/// State machine for recomputing MIN/MAX values after deletion
#[derive(Debug)]
pub enum RecomputeMinMax {
    ProcessElements {
        /// Current column being processed
        current_column_idx: usize,
        /// Columns to process (combined MIN and MAX)
        columns_to_process: Vec<(String, usize, bool)>, // (group_key, column_name, is_min)
        /// MIN/MAX deltas for checking values and weights
        min_max_deltas: MinMaxDeltas,
    },
    Scan {
        /// Columns still to process
        columns_to_process: Vec<(String, usize, bool)>,
        /// Current index in columns_to_process (will resume from here)
        current_column_idx: usize,
        /// MIN/MAX deltas for checking values and weights
        min_max_deltas: MinMaxDeltas,
        /// Current group key being processed
        group_key: String,
        /// Current column name being processed
        column_name: usize,
        /// Whether we're looking for MIN (true) or MAX (false)
        is_min: bool,
        /// The scan state machine for finding the new MIN/MAX
        scan_state: Box<ScanState>,
    },
    Done,
}

impl RecomputeMinMax {
    pub fn new(
        min_max_deltas: MinMaxDeltas,
        existing_groups: &HashMap<String, AggregateState>,
        operator: &AggregateOperator,
    ) -> Self {
        let mut groups_to_check: HashSet<(String, usize, bool)> = HashSet::new();

        // Remember the min_max_deltas are essentially just the only column that is affected by
        // this min/max, in delta (actually ZSet - consolidated delta) format. This makes it easier
        // for us to consume it in here.
        //
        // The most challenging case is the case where there is a retraction, since we need to go
        // back to the index.
        for (group_key_str, values) in &min_max_deltas {
            for ((col_name, hashable_row), weight) in values {
                let col_info = operator.column_min_max.get(col_name);

                let value = &hashable_row.values[0];

                if *weight < 0 {
                    // Deletion detected - check if it's the current MIN/MAX
                    if let Some(state) = existing_groups.get(group_key_str) {
                        // Check for MIN
                        if let Some(current_min) = state.mins.get(col_name) {
                            if current_min == value {
                                groups_to_check.insert((group_key_str.clone(), *col_name, true));
                            }
                        }
                        // Check for MAX
                        if let Some(current_max) = state.maxs.get(col_name) {
                            if current_max == value {
                                groups_to_check.insert((group_key_str.clone(), *col_name, false));
                            }
                        }
                    }
                } else if *weight > 0 {
                    // If it is not found in the existing groups, then we only need to care
                    // about this if this is a new record being inserted
                    if let Some(info) = col_info {
                        if info.has_min {
                            groups_to_check.insert((group_key_str.clone(), *col_name, true));
                        }
                        if info.has_max {
                            groups_to_check.insert((group_key_str.clone(), *col_name, false));
                        }
                    }
                }
            }
        }

        if groups_to_check.is_empty() {
            // No recomputation or initialization needed
            Self::Done
        } else {
            // Convert HashSet to Vec for indexed processing
            let groups_to_check_vec: Vec<_> = groups_to_check.into_iter().collect();
            Self::ProcessElements {
                current_column_idx: 0,
                columns_to_process: groups_to_check_vec,
                min_max_deltas,
            }
        }
    }

    pub fn process(
        &mut self,
        existing_groups: &mut HashMap<String, AggregateState>,
        operator: &AggregateOperator,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<()>> {
        loop {
            match self {
                RecomputeMinMax::ProcessElements {
                    current_column_idx,
                    columns_to_process,
                    min_max_deltas,
                } => {
                    if *current_column_idx >= columns_to_process.len() {
                        *self = RecomputeMinMax::Done;
                        return Ok(IOResult::Done(()));
                    }

                    let (group_key, column_name, is_min) =
                        columns_to_process[*current_column_idx].clone();

                    // Column name is already the index
                    // Get the storage index from column_min_max map
                    let column_info = operator
                        .column_min_max
                        .get(&column_name)
                        .expect("Column should exist in column_min_max map");
                    let storage_index = column_info.index;

                    // Get current value from existing state
                    let current_value = existing_groups.get(&group_key).and_then(|state| {
                        if is_min {
                            state.mins.get(&column_name).cloned()
                        } else {
                            state.maxs.get(&column_name).cloned()
                        }
                    });

                    // Create storage keys for index lookup
                    let storage_id =
                        generate_storage_id(operator.operator_id, storage_index, AGG_TYPE_MINMAX);
                    let zset_hash = operator.generate_group_hash(&group_key);

                    // Get the values for this group from min_max_deltas
                    let group_values = min_max_deltas.get(&group_key).cloned().unwrap_or_default();

                    let columns_to_process = std::mem::take(columns_to_process);
                    let min_max_deltas = std::mem::take(min_max_deltas);

                    let scan_state = if is_min {
                        Box::new(ScanState::new_for_min(
                            current_value,
                            group_key.clone(),
                            column_name,
                            storage_id,
                            zset_hash,
                            group_values,
                        ))
                    } else {
                        Box::new(ScanState::new_for_max(
                            current_value,
                            group_key.clone(),
                            column_name,
                            storage_id,
                            zset_hash,
                            group_values,
                        ))
                    };

                    *self = RecomputeMinMax::Scan {
                        columns_to_process,
                        current_column_idx: *current_column_idx,
                        min_max_deltas,
                        group_key,
                        column_name,
                        is_min,
                        scan_state,
                    };
                }
                RecomputeMinMax::Scan {
                    columns_to_process,
                    current_column_idx,
                    min_max_deltas,
                    group_key,
                    column_name,
                    is_min,
                    scan_state,
                } => {
                    // Find new value using the scan state machine
                    let new_value = return_if_io!(scan_state.find_new_value(cursors));

                    // Update the state with new value (create if doesn't exist)
                    let state = existing_groups.entry(group_key.clone()).or_default();

                    if *is_min {
                        if let Some(min_val) = new_value {
                            state.mins.insert(*column_name, min_val);
                        } else {
                            state.mins.remove(column_name);
                        }
                    } else if let Some(max_val) = new_value {
                        state.maxs.insert(*column_name, max_val);
                    } else {
                        state.maxs.remove(column_name);
                    }

                    // Move to next column
                    let min_max_deltas = std::mem::take(min_max_deltas);
                    let columns_to_process = std::mem::take(columns_to_process);
                    *self = RecomputeMinMax::ProcessElements {
                        current_column_idx: *current_column_idx + 1,
                        columns_to_process,
                        min_max_deltas,
                    };
                }
                RecomputeMinMax::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}

/// State machine for scanning through the index to find new MIN/MAX values
#[derive(Debug)]
pub enum ScanState {
    CheckCandidate {
        /// Current candidate value for MIN/MAX
        candidate: Option<Value>,
        /// Group key being processed
        group_key: String,
        /// Column name being processed
        column_name: usize,
        /// Storage ID for the index seek
        storage_id: i64,
        /// ZSet ID for the group
        zset_hash: Hash128,
        /// Group values from MinMaxDeltas: (column_name, HashableRow) -> weight
        group_values: HashMap<(usize, HashableRow), isize>,
        /// Whether we're looking for MIN (true) or MAX (false)
        is_min: bool,
    },
    FetchNextCandidate {
        /// Current candidate to seek past
        current_candidate: Value,
        /// Group key being processed
        group_key: String,
        /// Column name being processed
        column_name: usize,
        /// Storage ID for the index seek
        storage_id: i64,
        /// ZSet ID for the group
        zset_hash: Hash128,
        /// Group values from MinMaxDeltas: (column_name, HashableRow) -> weight
        group_values: HashMap<(usize, HashableRow), isize>,
        /// Whether we're looking for MIN (true) or MAX (false)
        is_min: bool,
    },
    Done {
        /// The final MIN/MAX value found
        result: Option<Value>,
    },
}

impl ScanState {
    pub fn new_for_min(
        current_min: Option<Value>,
        group_key: String,
        column_name: usize,
        storage_id: i64,
        zset_hash: Hash128,
        group_values: HashMap<(usize, HashableRow), isize>,
    ) -> Self {
        Self::CheckCandidate {
            candidate: current_min,
            group_key,
            column_name,
            storage_id,
            zset_hash,
            group_values,
            is_min: true,
        }
    }

    // Extract a new candidate from the index. It is possible that, when searching,
    // we end up going into a different operator altogether. That means we have
    // exhausted this operator (or group) entirely, and no good candidate was found
    fn extract_new_candidate(
        cursors: &mut DbspStateCursors,
        index_record: &ImmutableRecord,
        seek_op: SeekOp,
        storage_id: i64,
        zset_hash: Hash128,
    ) -> Result<IOResult<Option<Value>>> {
        let seek_result = return_if_io!(cursors
            .index_cursor
            .seek(SeekKey::IndexKey(index_record), seek_op));
        if !matches!(seek_result, SeekResult::Found) {
            return Ok(IOResult::Done(None));
        }

        let record = return_if_io!(cursors.index_cursor.record()).ok_or_else(|| {
            LimboError::InternalError(
                "Record found on the cursor, but could not be read".to_string(),
            )
        })?;

        let values = record.get_values();
        if values.len() < 3 {
            return Ok(IOResult::Done(None));
        }

        let Some(rec_storage_id) = values.first() else {
            return Ok(IOResult::Done(None));
        };
        let Some(rec_zset_hash) = values.get(1) else {
            return Ok(IOResult::Done(None));
        };

        // Check if we're still in the same group
        if let RefValue::Integer(rec_sid) = rec_storage_id {
            if *rec_sid != storage_id {
                return Ok(IOResult::Done(None));
            }
        } else {
            return Ok(IOResult::Done(None));
        }

        // Compare zset_hash as blob
        if let RefValue::Blob(rec_zset_blob) = rec_zset_hash {
            if let Some(rec_hash) = Hash128::from_blob(rec_zset_blob.to_slice()) {
                if rec_hash != zset_hash {
                    return Ok(IOResult::Done(None));
                }
            } else {
                return Ok(IOResult::Done(None));
            }
        } else {
            return Ok(IOResult::Done(None));
        }

        // Get the value (3rd element)
        Ok(IOResult::Done(values.get(2).map(|v| v.to_owned())))
    }

    pub fn new_for_max(
        current_max: Option<Value>,
        group_key: String,
        column_name: usize,
        storage_id: i64,
        zset_hash: Hash128,
        group_values: HashMap<(usize, HashableRow), isize>,
    ) -> Self {
        Self::CheckCandidate {
            candidate: current_max,
            group_key,
            column_name,
            storage_id,
            zset_hash,
            group_values,
            is_min: false,
        }
    }

    pub fn find_new_value(
        &mut self,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Option<Value>>> {
        loop {
            match self {
                ScanState::CheckCandidate {
                    candidate,
                    group_key,
                    column_name,
                    storage_id,
                    zset_hash,
                    group_values,
                    is_min,
                } => {
                    // First, check if we have a candidate
                    if let Some(cand_val) = candidate {
                        // Check if the candidate is retracted (weight <= 0)
                        // Create a HashableRow to look up the weight
                        let hashable_cand = HashableRow::new(0, vec![cand_val.clone()]);
                        let key = (*column_name, hashable_cand);
                        let is_retracted =
                            group_values.get(&key).is_some_and(|weight| *weight <= 0);

                        if is_retracted {
                            // Candidate is retracted, need to fetch next from index
                            *self = ScanState::FetchNextCandidate {
                                current_candidate: cand_val.clone(),
                                group_key: std::mem::take(group_key),
                                column_name: std::mem::take(column_name),
                                storage_id: *storage_id,
                                zset_hash: *zset_hash,
                                group_values: std::mem::take(group_values),
                                is_min: *is_min,
                            };
                            continue;
                        }
                    }

                    // Candidate is valid or we have no candidate
                    // Now find the best value from insertions in group_values
                    let mut best_from_zset = None;
                    for ((col, hashable_val), weight) in group_values.iter() {
                        if col == column_name && *weight > 0 {
                            let value = &hashable_val.values[0];
                            // Skip NULL values - they don't participate in MIN/MAX
                            if value == &Value::Null {
                                continue;
                            }
                            // This is an insertion for our column
                            if let Some(ref current_best) = best_from_zset {
                                if *is_min {
                                    if value.cmp(current_best) == std::cmp::Ordering::Less {
                                        best_from_zset = Some(value.clone());
                                    }
                                } else if value.cmp(current_best) == std::cmp::Ordering::Greater {
                                    best_from_zset = Some(value.clone());
                                }
                            } else {
                                best_from_zset = Some(value.clone());
                            }
                        }
                    }

                    // Compare candidate with best from ZSet, filtering out NULLs
                    let result = match (&candidate, &best_from_zset) {
                        (Some(cand), Some(zset_val)) if cand != &Value::Null => {
                            if *is_min {
                                if zset_val.cmp(cand) == std::cmp::Ordering::Less {
                                    Some(zset_val.clone())
                                } else {
                                    Some(cand.clone())
                                }
                            } else if zset_val.cmp(cand) == std::cmp::Ordering::Greater {
                                Some(zset_val.clone())
                            } else {
                                Some(cand.clone())
                            }
                        }
                        (Some(cand), None) if cand != &Value::Null => Some(cand.clone()),
                        (None, Some(zset_val)) => Some(zset_val.clone()),
                        (Some(cand), Some(_)) if cand == &Value::Null => best_from_zset,
                        _ => None,
                    };

                    *self = ScanState::Done { result };
                }

                ScanState::FetchNextCandidate {
                    current_candidate,
                    group_key,
                    column_name,
                    storage_id,
                    zset_hash,
                    group_values,
                    is_min,
                } => {
                    // Seek to the next value in the index
                    let index_key = vec![
                        Value::Integer(*storage_id),
                        zset_hash.to_value(),
                        current_candidate.clone(),
                    ];
                    let index_record = ImmutableRecord::from_values(&index_key, index_key.len());

                    let seek_op = if *is_min {
                        SeekOp::GT // For MIN, seek greater than current
                    } else {
                        SeekOp::LT // For MAX, seek less than current
                    };

                    let new_candidate = return_if_io!(Self::extract_new_candidate(
                        cursors,
                        &index_record,
                        seek_op,
                        *storage_id,
                        *zset_hash
                    ));

                    *self = ScanState::CheckCandidate {
                        candidate: new_candidate,
                        group_key: std::mem::take(group_key),
                        column_name: std::mem::take(column_name),
                        storage_id: *storage_id,
                        zset_hash: *zset_hash,
                        group_values: std::mem::take(group_values),
                        is_min: *is_min,
                    };
                }

                ScanState::Done { result } => {
                    return Ok(IOResult::Done(result.clone()));
                }
            }
        }
    }
}

/// State machine for persisting Min/Max values to storage
#[derive(Debug)]
pub enum MinMaxPersistState {
    Init {
        min_max_deltas: MinMaxDeltas,
        group_keys: Vec<String>,
    },
    ProcessGroup {
        min_max_deltas: MinMaxDeltas,
        group_keys: Vec<String>,
        group_idx: usize,
        value_idx: usize,
    },
    WriteValue {
        min_max_deltas: MinMaxDeltas,
        group_keys: Vec<String>,
        group_idx: usize,
        value_idx: usize,
        value: Value,
        column_name: usize,
        weight: isize,
        write_row: WriteRow,
    },
    Done,
}

impl MinMaxPersistState {
    pub fn new(min_max_deltas: MinMaxDeltas) -> Self {
        let group_keys: Vec<String> = min_max_deltas.keys().cloned().collect();
        Self::Init {
            min_max_deltas,
            group_keys,
        }
    }

    pub fn persist_min_max(
        &mut self,
        operator_id: i64,
        column_min_max: &HashMap<usize, AggColumnInfo>,
        cursors: &mut DbspStateCursors,
        generate_group_hash: impl Fn(&str) -> Hash128,
    ) -> Result<IOResult<()>> {
        loop {
            match self {
                MinMaxPersistState::Init {
                    min_max_deltas,
                    group_keys,
                } => {
                    let min_max_deltas = std::mem::take(min_max_deltas);
                    let group_keys = std::mem::take(group_keys);
                    *self = MinMaxPersistState::ProcessGroup {
                        min_max_deltas,
                        group_keys,
                        group_idx: 0,
                        value_idx: 0,
                    };
                }
                MinMaxPersistState::ProcessGroup {
                    min_max_deltas,
                    group_keys,
                    group_idx,
                    value_idx,
                } => {
                    // Check if we're past all groups
                    if *group_idx >= group_keys.len() {
                        *self = MinMaxPersistState::Done;
                        continue;
                    }

                    let group_key_str = &group_keys[*group_idx];
                    let values = &min_max_deltas[group_key_str]; // This should always exist

                    // Convert HashMap to Vec for indexed access
                    let values_vec: Vec<_> = values.iter().collect();

                    // Check if we have more values in current group
                    if *value_idx >= values_vec.len() {
                        *group_idx += 1;
                        *value_idx = 0;
                        // Continue to check if we're past all groups now
                        continue;
                    }

                    // Process current value and extract what we need before taking ownership
                    let ((column_name, hashable_row), weight) = values_vec[*value_idx];
                    let column_name = *column_name;
                    let value = hashable_row.values[0].clone(); // Extract the Value from HashableRow
                    let weight = *weight;

                    let min_max_deltas = std::mem::take(min_max_deltas);
                    let group_keys = std::mem::take(group_keys);
                    *self = MinMaxPersistState::WriteValue {
                        min_max_deltas,
                        group_keys,
                        group_idx: *group_idx,
                        value_idx: *value_idx,
                        column_name,
                        value,
                        weight,
                        write_row: WriteRow::new(),
                    };
                }
                MinMaxPersistState::WriteValue {
                    min_max_deltas,
                    group_keys,
                    group_idx,
                    value_idx,
                    value,
                    column_name,
                    weight,
                    write_row,
                } => {
                    // Should have exited in the previous state
                    assert!(*group_idx < group_keys.len());

                    let group_key_str = &group_keys[*group_idx];

                    // Get the column info from the pre-computed map
                    let column_info = column_min_max
                        .get(column_name)
                        .expect("Column should exist in column_min_max map");
                    let column_index = column_info.index;

                    // Build the key components for MinMax storage using new encoding
                    let storage_id =
                        generate_storage_id(operator_id, column_index, AGG_TYPE_MINMAX);
                    let zset_hash = generate_group_hash(group_key_str);

                    // element_id is the actual value for Min/Max
                    let element_id_val = value.clone();

                    // Create index key
                    let index_key = vec![
                        Value::Integer(storage_id),
                        zset_hash.to_value(),
                        element_id_val.clone(),
                    ];

                    // Record values (operator_id, zset_hash, element_id, unused_placeholder)
                    // For MIN/MAX, the element_id IS the value, so we use NULL for the 4th column
                    let record_values = vec![
                        Value::Integer(storage_id),
                        zset_hash.to_value(),
                        element_id_val.clone(),
                        Value::Null, // Placeholder - not used for MIN/MAX
                    ];

                    return_if_io!(write_row.write_row(
                        cursors,
                        index_key.clone(),
                        record_values,
                        *weight
                    ));

                    // Move to next value
                    let min_max_deltas = std::mem::take(min_max_deltas);
                    let group_keys = std::mem::take(group_keys);
                    *self = MinMaxPersistState::ProcessGroup {
                        min_max_deltas,
                        group_keys,
                        group_idx: *group_idx,
                        value_idx: *value_idx + 1,
                    };
                }
                MinMaxPersistState::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}
