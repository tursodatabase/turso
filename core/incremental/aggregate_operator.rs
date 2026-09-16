// Aggregate operator for DBSP-style incremental computation

use crate::coro::{with_handle, Co, Runner, StepContext, YieldSlot};
use crate::incremental::dbsp::Hash128;
use crate::incremental::dbsp::{Delta, DeltaPair, HashableRow};
use crate::incremental::operator::{
    generate_storage_id, ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
    OpRunner,
};
use crate::incremental::persistence::{read_record, write_row, CursorStep};
use crate::numeric::Numeric;
use crate::storage::btree::{BTreeCursor, CursorTrait};
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::translate::plan::ColumnMask;
use crate::types::IOResultOr;
use crate::types::{
    IOCompletions, IOResult, ImmutableRecord, ImmutableRecordRef, SeekKey, SeekOp, SeekResult,
    ValueRef,
};
use crate::{return_if_io, LimboError, Result, Value};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::collections::BTreeMap;
use std::fmt::{self, Display};

// Architecture of the Aggregate Operator
// ========================================
//
// This operator implements SQL aggregations (GROUP BY, DISTINCT, COUNT, SUM, AVG, MIN, MAX)
// using DBSP-style incremental computation. The key insight is that all these operations
// can be expressed as operations on weighted sets (Z-sets) stored in persistent BTrees.
//
// ## Storage Strategy
//
// We use three different storage encodings (identified by 2-bit type codes in storage IDs):
// - **Regular aggregates** (COUNT/SUM/AVG): Store accumulated state as a blob
// - **MIN/MAX aggregates**: Store individual values; BTree ordering gives us min/max efficiently
// - **DISTINCT tracking**: Store distinct values with weights (positive = present, zero = deleted)
//
// ## MIN/MAX Handling
//
// MIN/MAX are special because they're not fully incrementalizable:
// - **Inserts**: Can be computed incrementally (new_min = min(old_min, new_value))
// - **Deletes**: Must recompute from the BTree when the current min/max is deleted
//
// Our approach:
// 1. Store each value with its weight in a BTree (leveraging natural ordering)
// 2. On insert: Simply compare with current min/max (incremental)
// 3. On delete of current min/max: Scan the BTree to find the next min/max
//    - For MIN: scan forward from the beginning to find first value with positive weight
//    - For MAX: scan backward from the end to find last value with positive weight
//
// ## DISTINCT Handling
//
// DISTINCT operations (COUNT(DISTINCT), SUM(DISTINCT), etc.) are implemented using the
// weighted set pattern:
// - Each distinct value is stored with a weight (occurrence count)
// - Weight > 0 means the value exists in the current dataset
// - Weight = 0 means the value has been deleted (we may clean these up)
// - We track transitions: when a value's weight crosses zero (appears/disappears)
//
// ## Plain DISTINCT (SELECT DISTINCT)
//
// A clever reuse of infrastructure: SELECT DISTINCT x, y, z is compiled to:
// - GROUP BY x, y, z (making each unique row combination a group)
// - Empty aggregates vector (no actual aggregations to compute)
// - The groups themselves become the distinct rows
//
// This allows us to reuse all the incremental machinery for DISTINCT without special casing.
// The `is_distinct_only` flag indicates this pattern, where the groups ARE the output rows.
//
// ## State Machines
//
// The operator uses async-ready state machines to handle I/O operations:
// - **Eval state machine**: Fetches existing state, applies deltas, recomputes MIN/MAX
// - **Commit state machine**: Persists updated state back to storage
// - Each state represents a resumption point for when I/O operations yield

/// Constants for aggregate type encoding in storage IDs (2 bits)
pub const AGG_TYPE_REGULAR: u8 = 0b00; // COUNT/SUM/AVG
pub const AGG_TYPE_MINMAX: u8 = 0b01; // MIN/MAX (BTree ordering gives both)
pub const AGG_TYPE_DISTINCT: u8 = 0b10; // DISTINCT values tracking

/// Hash a Value to generate an element_id for DISTINCT storage
/// Uses HashableRow with column_idx as rowid for consistent hashing
fn hash_value(value: &Value, column_idx: usize) -> Hash128 {
    // Use column_idx as rowid to ensure different columns with same value get different hashes
    let row = HashableRow::new(column_idx as i64, vec![value.clone()]);
    row.cached_hash()
}

// Serialization type codes for aggregate functions
const AGG_FUNC_COUNT: i64 = 0;
const AGG_FUNC_SUM: i64 = 1;
const AGG_FUNC_AVG: i64 = 2;
const AGG_FUNC_MIN: i64 = 3;
const AGG_FUNC_MAX: i64 = 4;
const AGG_FUNC_COUNT_DISTINCT: i64 = 5;
const AGG_FUNC_SUM_DISTINCT: i64 = 6;
const AGG_FUNC_AVG_DISTINCT: i64 = 7;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    CountDistinct(usize), // COUNT(DISTINCT column_index)
    Sum(usize),           // Column index
    SumDistinct(usize),   // SUM(DISTINCT column_index)
    Avg(usize),           // Column index
    AvgDistinct(usize),   // AVG(DISTINCT column_index)
    Min(usize),           // Column index
    Max(usize),           // Column index
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Count => write!(f, "COUNT(*)"),
            AggregateFunction::CountDistinct(idx) => write!(f, "COUNT(DISTINCT col{idx})"),
            AggregateFunction::Sum(idx) => write!(f, "SUM(col{idx})"),
            AggregateFunction::SumDistinct(idx) => write!(f, "SUM(DISTINCT col{idx})"),
            AggregateFunction::Avg(idx) => write!(f, "AVG(col{idx})"),
            AggregateFunction::AvgDistinct(idx) => write!(f, "AVG(DISTINCT col{idx})"),
            AggregateFunction::Min(idx) => write!(f, "MIN(col{idx})"),
            AggregateFunction::Max(idx) => write!(f, "MAX(col{idx})"),
        }
    }
}

impl AggregateFunction {
    /// Serialize this aggregate function to a Value
    /// Returns a vector of values: [type_code, optional_column_index]
    pub fn to_values(&self) -> Vec<Value> {
        match self {
            AggregateFunction::Count => vec![Value::Numeric(Numeric::Integer(AGG_FUNC_COUNT))],
            AggregateFunction::CountDistinct(idx) => {
                vec![
                    Value::Numeric(Numeric::Integer(AGG_FUNC_COUNT_DISTINCT)),
                    Value::from_i64(*idx as i64),
                ]
            }
            AggregateFunction::Sum(idx) => {
                vec![
                    Value::Numeric(Numeric::Integer(AGG_FUNC_SUM)),
                    Value::from_i64(*idx as i64),
                ]
            }
            AggregateFunction::SumDistinct(idx) => {
                vec![
                    Value::Numeric(Numeric::Integer(AGG_FUNC_SUM_DISTINCT)),
                    Value::from_i64(*idx as i64),
                ]
            }
            AggregateFunction::Avg(idx) => {
                vec![
                    Value::Numeric(Numeric::Integer(AGG_FUNC_AVG)),
                    Value::from_i64(*idx as i64),
                ]
            }
            AggregateFunction::AvgDistinct(idx) => {
                vec![
                    Value::Numeric(Numeric::Integer(AGG_FUNC_AVG_DISTINCT)),
                    Value::from_i64(*idx as i64),
                ]
            }
            AggregateFunction::Min(idx) => {
                vec![
                    Value::Numeric(Numeric::Integer(AGG_FUNC_MIN)),
                    Value::from_i64(*idx as i64),
                ]
            }
            AggregateFunction::Max(idx) => {
                vec![
                    Value::Numeric(Numeric::Integer(AGG_FUNC_MAX)),
                    Value::from_i64(*idx as i64),
                ]
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
            Value::Numeric(Numeric::Integer(AGG_FUNC_COUNT)) => {
                *cursor += 1;
                AggregateFunction::Count
            }
            Value::Numeric(Numeric::Integer(AGG_FUNC_COUNT_DISTINCT)) => {
                *cursor += 1;
                let idx = values.get(*cursor).ok_or_else(|| {
                    LimboError::InternalError("Missing COUNT(DISTINCT) column index".into())
                })?;
                if let Value::Numeric(Numeric::Integer(idx)) = idx {
                    *cursor += 1;
                    AggregateFunction::CountDistinct(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for COUNT(DISTINCT) column index, got {idx:?}"
                    )));
                }
            }
            Value::Numeric(Numeric::Integer(AGG_FUNC_SUM)) => {
                *cursor += 1;
                let idx = values
                    .get(*cursor)
                    .ok_or_else(|| LimboError::InternalError("Missing SUM column index".into()))?;
                if let Value::Numeric(Numeric::Integer(idx)) = idx {
                    *cursor += 1;
                    AggregateFunction::Sum(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for SUM column index, got {idx:?}"
                    )));
                }
            }
            Value::Numeric(Numeric::Integer(AGG_FUNC_SUM_DISTINCT)) => {
                *cursor += 1;
                let idx = values.get(*cursor).ok_or_else(|| {
                    LimboError::InternalError("Missing SUM(DISTINCT) column index".into())
                })?;
                if let Value::Numeric(Numeric::Integer(idx)) = idx {
                    *cursor += 1;
                    AggregateFunction::SumDistinct(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for SUM(DISTINCT) column index, got {idx:?}"
                    )));
                }
            }
            Value::Numeric(Numeric::Integer(AGG_FUNC_AVG)) => {
                *cursor += 1;
                let idx = values
                    .get(*cursor)
                    .ok_or_else(|| LimboError::InternalError("Missing AVG column index".into()))?;
                if let Value::Numeric(Numeric::Integer(idx)) = idx {
                    *cursor += 1;
                    AggregateFunction::Avg(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for AVG column index, got {idx:?}"
                    )));
                }
            }
            Value::Numeric(Numeric::Integer(AGG_FUNC_AVG_DISTINCT)) => {
                *cursor += 1;
                let idx = values.get(*cursor).ok_or_else(|| {
                    LimboError::InternalError("Missing AVG(DISTINCT) column index".into())
                })?;
                if let Value::Numeric(Numeric::Integer(idx)) = idx {
                    *cursor += 1;
                    AggregateFunction::AvgDistinct(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for AVG(DISTINCT) column index, got {idx:?}"
                    )));
                }
            }
            Value::Numeric(Numeric::Integer(AGG_FUNC_MIN)) => {
                *cursor += 1;
                let idx = values
                    .get(*cursor)
                    .ok_or_else(|| LimboError::InternalError("Missing MIN column index".into()))?;
                if let Value::Numeric(Numeric::Integer(idx)) = idx {
                    *cursor += 1;
                    AggregateFunction::Min(*idx as usize)
                } else {
                    return Err(LimboError::InternalError(format!(
                        "Expected Integer for MIN column index, got {idx:?}"
                    )));
                }
            }
            Value::Numeric(Numeric::Integer(AGG_FUNC_MAX)) => {
                *cursor += 1;
                let idx = values
                    .get(*cursor)
                    .ok_or_else(|| LimboError::InternalError("Missing MAX column index".into()))?;
                if let Value::Numeric(Numeric::Integer(idx)) = idx {
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
                )));
            }
        };

        Ok(agg_fn)
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

/// Type for tracking distinct values within a batch
/// Maps: group_key_str -> (column_idx, HashableRow) -> accumulated_weight
/// HashableRow contains the value with column_idx as rowid for proper hashing
type DistinctDeltas = HashMap<String, HashMap<(usize, HashableRow), isize>>;

/// Return type for merge_delta_with_existing function
type MergeResult = (Delta, HashMap<String, (Vec<Value>, AggregateState)>);

/// Information about distinct value transitions for a single column
#[derive(Debug, Clone)]
pub struct DistinctTransition {
    pub transition_type: TransitionType,
    pub transitioned_value: Value, // The value that was added/removed
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransitionType {
    Added,   // Value added to distinct set
    Removed, // Value removed from distinct set
}

/// Names [`AggregateCtx`] as the context type of the async aggregate
/// operations.
pub struct AggregateStep;

impl StepContext for AggregateStep {
    type Error = Box<LimboError>;
    type Ctx<'a> = AggregateCtx<'a>;
}

/// The context of one step of an aggregate operation: the operator, the
/// state cursors of that step, and the slot for what suspends the step.
pub struct AggregateCtx<'a> {
    operator: &'a mut AggregateOperator,
    cursors: &'a mut DbspStateCursors,
    io: Option<IOCompletions>,
    err: Option<Box<LimboError>>,
}

impl CursorStep for AggregateStep {
    fn cursors<'c, 'a>(ctx: &'c mut AggregateCtx<'a>) -> &'c mut DbspStateCursors {
        ctx.cursors
    }
}

impl YieldSlot<Box<LimboError>> for AggregateCtx<'_> {
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

/// The eval of an aggregate operator as a step function. It returns the
/// output delta and the new state of every group the delta touched.
pub type AggregateEvalOp = OpRunner<AggregateStep, Delta, (Delta, ComputedStates)>;

/// The commit of an aggregate operator as a step function.
type AggregateCommitOp = OpRunner<AggregateStep, Delta, Delta>;

/// The runners of the aggregate operations, boxed on first use and reused.
#[derive(Debug, Default)]
struct AggregateOps {
    eval: Option<AggregateEvalOp>,
    commit: Option<AggregateCommitOp>,
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
    // Set of column indices that have distinct aggregates
    pub distinct_columns: ColumnMask,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,

    ops: AggregateOps,

    // SELECT DISTINCT x,y,z.... with no aggregations.
    is_distinct_only: bool,
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
    // For DISTINCT aggregates: column_index -> computed value
    // These are populated during eval when we scan the BTree (or in-memory map)
    pub distinct_counts: HashMap<usize, i64>,
    pub distinct_sums: HashMap<usize, f64>,

    // Weights of specific distinct values needed for current delta processing
    // (column_index, value) -> weight
    // Populated during FetchKey for values mentioned in the delta
    pub(crate) distinct_value_weights: HashMap<(usize, HashableRow), i64>,
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
        values.push(Value::from_i64(self.count));

        // Store number of aggregates
        values.push(Value::from_i64(aggregates.len() as i64));

        // Add each aggregate's metadata and state
        for agg in aggregates {
            // First, add the aggregate function metadata (type and column index)
            values.extend(agg.to_values());

            // Then add the state for this aggregate
            match agg {
                AggregateFunction::Count => {
                    // Count state is already stored at the beginning
                }
                AggregateFunction::CountDistinct(col_idx) => {
                    // Store the distinct count for this column
                    let count = self.distinct_counts.get(col_idx).copied().unwrap_or(0);
                    values.push(Value::from_i64(count));
                }
                AggregateFunction::Sum(col_idx) => {
                    let sum = self.sums.get(col_idx).copied().unwrap_or(0.0);
                    values.push(Value::from_f64(sum));
                }
                AggregateFunction::SumDistinct(col_idx) => {
                    // Store both the distinct count and sum for this column
                    let count = self.distinct_counts.get(col_idx).copied().unwrap_or(0);
                    let sum = self.distinct_sums.get(col_idx).copied().unwrap_or(0.0);
                    values.push(Value::from_i64(count));
                    values.push(Value::from_f64(sum));
                }
                AggregateFunction::Avg(col_idx) => {
                    let (sum, count) = self.avgs.get(col_idx).copied().unwrap_or((0.0, 0));
                    values.push(Value::from_f64(sum));
                    values.push(Value::from_i64(count));
                }
                AggregateFunction::AvgDistinct(col_idx) => {
                    // Store both the distinct count and sum for this column
                    let count = self.distinct_counts.get(col_idx).copied().unwrap_or(0);
                    let sum = self.distinct_sums.get(col_idx).copied().unwrap_or(0.0);
                    values.push(Value::from_i64(count));
                    values.push(Value::from_f64(sum));
                }
                AggregateFunction::Min(col_idx) => {
                    if let Some(min_val) = self.mins.get(col_idx) {
                        values.push(Value::from_i64(1)); // Has value
                        values.push(min_val.clone());
                    } else {
                        values.push(Value::from_i64(0)); // No value
                    }
                }
                AggregateFunction::Max(col_idx) => {
                    if let Some(max_val) = self.maxs.get(col_idx) {
                        values.push(Value::from_i64(1)); // Has value
                        values.push(max_val.clone());
                    } else {
                        values.push(Value::from_i64(0)); // No value
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
        if let Value::Numeric(Numeric::Integer(count)) = count {
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
            Value::Numeric(Numeric::Integer(n)) => *n as usize,
            _ => {
                return Err(LimboError::InternalError(format!(
                    "Expected Integer for aggregate count, got {num_aggregates:?}"
                )));
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
                AggregateFunction::CountDistinct(col_idx) => {
                    let count = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing COUNT(DISTINCT) value".into())
                    })?;
                    if let Value::Numeric(Numeric::Integer(count)) = count {
                        state.distinct_counts.insert(col_idx, *count);
                        cursor += 1;
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Expected Integer for COUNT(DISTINCT) value, got {count:?}"
                        )));
                    }
                }
                AggregateFunction::SumDistinct(col_idx) => {
                    let count = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing SUM(DISTINCT) count".into())
                    })?;
                    if let Value::Numeric(Numeric::Integer(count)) = count {
                        state.distinct_counts.insert(col_idx, *count);
                        cursor += 1;
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Expected Integer for SUM(DISTINCT) count, got {count:?}"
                        )));
                    }

                    let sum = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing SUM(DISTINCT) sum".into())
                    })?;
                    if let Value::Numeric(Numeric::Float(sum)) = sum {
                        state.distinct_sums.insert(col_idx, f64::from(*sum));
                        cursor += 1;
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Expected Float for SUM(DISTINCT) sum, got {sum:?}"
                        )));
                    }
                }
                AggregateFunction::AvgDistinct(col_idx) => {
                    let count = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing AVG(DISTINCT) count".into())
                    })?;
                    if let Value::Numeric(Numeric::Integer(count)) = count {
                        state.distinct_counts.insert(col_idx, *count);
                        cursor += 1;
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Expected Integer for AVG(DISTINCT) count, got {count:?}"
                        )));
                    }

                    let sum = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing AVG(DISTINCT) sum".into())
                    })?;
                    if let Value::Numeric(Numeric::Float(sum)) = sum {
                        state.distinct_sums.insert(col_idx, f64::from(*sum));
                        cursor += 1;
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Expected Float for AVG(DISTINCT) sum, got {sum:?}"
                        )));
                    }
                }
                AggregateFunction::Sum(col_idx) => {
                    let sum = values
                        .get(cursor)
                        .ok_or_else(|| LimboError::InternalError("Missing SUM value".into()))?;
                    if let Value::Numeric(Numeric::Float(sum)) = sum {
                        state.sums.insert(col_idx, f64::from(*sum));
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
                        Value::Numeric(Numeric::Float(f)) => f64::from(*f),
                        _ => {
                            return Err(LimboError::InternalError(format!(
                                "Expected Float for AVG sum, got {sum:?}"
                            )));
                        }
                    };
                    cursor += 1;

                    let count = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing AVG count value".into())
                    })?;
                    let count = match count {
                        Value::Numeric(Numeric::Integer(i)) => *i,
                        _ => {
                            return Err(LimboError::InternalError(format!(
                                "Expected Integer for AVG count, got {count:?}"
                            )));
                        }
                    };
                    cursor += 1;

                    state.avgs.insert(col_idx, (sum, count));
                }
                AggregateFunction::Min(col_idx) => {
                    let has_value = values.get(cursor).ok_or_else(|| {
                        LimboError::InternalError("Missing MIN has_value flag".into())
                    })?;
                    if let Value::Numeric(Numeric::Integer(has_value)) = has_value {
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
                    if let Value::Numeric(Numeric::Integer(has_value)) = has_value {
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

    fn to_blob(
        &self,
        aggregates: &[AggregateFunction],
        group_key: &[Value],
    ) -> Result<crate::ValueBlob> {
        let mut all_values = Vec::new();
        // Store the group key size first
        all_values.push(Value::from_i64(group_key.len() as i64));
        all_values.extend_from_slice(group_key);
        all_values.extend(self.to_value_vector(aggregates));

        let record = ImmutableRecord::from_values(&all_values, all_values.len())?;
        Ok(record.into_payload())
    }

    pub fn from_blob(blob: &[u8]) -> Result<(Self, Vec<Value>)> {
        let record = ImmutableRecordRef::from_bin_record(blob);
        let mut all_values = record.get_values_owned()?;

        if all_values.is_empty() {
            return Err(LimboError::InternalError(
                "Aggregate state blob is empty".into(),
            ));
        }

        // Read the group key size
        let group_key_count = match &all_values[0] {
            Value::Numeric(Numeric::Integer(n)) if *n >= 0 => *n as usize,
            Value::Numeric(Numeric::Integer(n)) => {
                return Err(LimboError::InternalError(format!(
                    "Negative group key count: {n}"
                )));
            }
            other => {
                return Err(LimboError::InternalError(format!(
                    "Expected Integer for group key count, got {other:?}"
                )));
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
        // TODO: std boundary conversion; adjust once incremental uses the
        // allocator with fallible allocations everywhere.
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
        distinct_transitions: &HashMap<usize, DistinctTransition>,
    ) -> Result<()> {
        // Update COUNT
        self.count += weight as i64;

        // Track which columns have had their distinct counts/sums updated
        // This prevents double-counting when multiple distinct aggregates
        // operate on the same column (e.g., COUNT(DISTINCT col), SUM(DISTINCT col), AVG(DISTINCT col))
        let mut processed_counts = ColumnMask::default();
        let mut processed_sums = ColumnMask::default();

        // Update distinct aggregate state
        for agg in aggregates {
            match agg {
                AggregateFunction::Count => {
                    // Already handled above
                }
                AggregateFunction::CountDistinct(col_idx) => {
                    // Only update count if we haven't processed this column yet
                    if !processed_counts.get(*col_idx) {
                        if let Some(transition) = distinct_transitions.get(col_idx) {
                            let current_count =
                                self.distinct_counts.get(col_idx).copied().unwrap_or(0);
                            let new_count = match transition.transition_type {
                                TransitionType::Added => current_count + 1,
                                TransitionType::Removed => current_count - 1,
                            };
                            self.distinct_counts.insert(*col_idx, new_count);
                            processed_counts.set(*col_idx)?;
                        }
                    }
                }
                AggregateFunction::SumDistinct(col_idx)
                | AggregateFunction::AvgDistinct(col_idx) => {
                    if let Some(transition) = distinct_transitions.get(col_idx) {
                        // Update count if not already processed (needed for AVG)
                        if !processed_counts.get(*col_idx) {
                            let current_count =
                                self.distinct_counts.get(col_idx).copied().unwrap_or(0);
                            let new_count = match transition.transition_type {
                                TransitionType::Added => current_count + 1,
                                TransitionType::Removed => current_count - 1,
                            };
                            self.distinct_counts.insert(*col_idx, new_count);
                            processed_counts.set(*col_idx)?;
                        }

                        // Update sum if not already processed
                        if !processed_sums.get(*col_idx) {
                            let current_sum =
                                self.distinct_sums.get(col_idx).copied().unwrap_or(0.0);
                            let value_as_float = match &transition.transitioned_value {
                                Value::Numeric(Numeric::Integer(i)) => *i as f64,
                                Value::Numeric(Numeric::Float(f)) => f64::from(*f),
                                _ => 0.0,
                            };

                            let new_sum = match transition.transition_type {
                                TransitionType::Added => current_sum + value_as_float,
                                TransitionType::Removed => current_sum - value_as_float,
                            };
                            self.distinct_sums.insert(*col_idx, new_sum);
                            processed_sums.set(*col_idx)?;
                        }
                    }
                }
                AggregateFunction::Sum(col_idx) => {
                    if let Some(val) = values.get(*col_idx) {
                        let num_val = match val {
                            Value::Numeric(Numeric::Integer(i)) => *i as f64,
                            Value::Numeric(Numeric::Float(f)) => f64::from(*f),
                            _ => 0.0,
                        };
                        *self.sums.entry(*col_idx).or_insert(0.0) += num_val * weight as f64;
                    }
                }
                AggregateFunction::Avg(col_idx) => {
                    if let Some(val) = values.get(*col_idx) {
                        let num_val = match val {
                            Value::Numeric(Numeric::Integer(i)) => *i as f64,
                            Value::Numeric(Numeric::Float(f)) => f64::from(*f),
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
        Ok(())
    }

    /// Convert aggregate state to output values
    ///
    /// Note: SQLite returns INTEGER for SUM when all inputs are integers, and REAL when any input is REAL.
    /// However, in an incremental system like DBSP, we cannot track whether all current values are integers
    /// after deletions. For example:
    /// - Initial: SUM(10, 20, 30.5) = 60.5 (REAL)
    /// - After DELETE 30.5: SUM(10, 20) = 30 (SQLite returns INTEGER, but we only know the sum is 30.0)
    ///
    /// Therefore, we always return REAL for SUM operations.
    pub fn to_values(&self, aggregates: &[AggregateFunction]) -> Vec<Value> {
        let mut result = Vec::new();

        for agg in aggregates {
            match agg {
                AggregateFunction::Count => {
                    result.push(Value::from_i64(self.count));
                }
                AggregateFunction::CountDistinct(col_idx) => {
                    // Return the computed DISTINCT count
                    let count = self.distinct_counts.get(col_idx).copied().unwrap_or(0);
                    result.push(Value::from_i64(count));
                }
                AggregateFunction::Sum(col_idx) => {
                    let sum = self.sums.get(col_idx).copied().unwrap_or(0.0);
                    result.push(Value::from_f64(sum));
                }
                AggregateFunction::SumDistinct(col_idx) => {
                    // Return the computed SUM(DISTINCT)
                    let sum = self.distinct_sums.get(col_idx).copied().unwrap_or(0.0);
                    result.push(Value::from_f64(sum));
                }
                AggregateFunction::Avg(col_idx) => {
                    if let Some((sum, count)) = self.avgs.get(col_idx) {
                        if *count > 0 {
                            result.push(Value::from_f64(sum / *count as f64));
                        } else {
                            result.push(Value::Null);
                        }
                    } else {
                        result.push(Value::Null);
                    }
                }
                AggregateFunction::AvgDistinct(col_idx) => {
                    // Compute AVG from SUM(DISTINCT) / COUNT(DISTINCT)
                    let count = self.distinct_counts.get(col_idx).copied().unwrap_or(0);
                    if count > 0 {
                        let sum = self.distinct_sums.get(col_idx).copied().unwrap_or(0.0);
                        let avg = sum / count as f64;
                        // AVG always returns a float value for consistency with SQLite
                        result.push(Value::from_f64(avg));
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
    /// Detect if a distinct value crosses the zero boundary (using pre-fetched weights and batch-accumulated weights)
    fn detect_distinct_value_transition(
        col_idx: usize,
        val: &Value,
        weight: isize,
        existing_state: &AggregateState,
        group_distinct_deltas: Option<&HashMap<(usize, HashableRow), isize>>,
    ) -> Option<DistinctTransition> {
        let hashable_row = HashableRow::new(col_idx as i64, vec![val.clone()]);

        // Get the weight from storage (pre-fetched in AggregateState)
        let storage_count = existing_state
            .distinct_value_weights
            .get(&(col_idx, hashable_row.clone()))
            .copied()
            .unwrap_or(0);

        // Get the accumulated weight from the current batch (before this row)
        let batch_accumulated = if let Some(deltas) = group_distinct_deltas {
            deltas.get(&(col_idx, hashable_row)).copied().unwrap_or(0)
        } else {
            0
        };

        // The old count is storage + batch accumulated so far (before this row)
        let old_count = storage_count + batch_accumulated as i64;
        // The new count includes the current weight
        let new_count = old_count + weight as i64;

        // Detect transitions
        if old_count <= 0 && new_count > 0 {
            // Value added to distinct set
            Some(DistinctTransition {
                transition_type: TransitionType::Added,
                transitioned_value: val.clone(),
            })
        } else if old_count > 0 && new_count <= 0 {
            // Value removed from distinct set
            Some(DistinctTransition {
                transition_type: TransitionType::Removed,
                transitioned_value: val.clone(),
            })
        } else {
            // No transition
            None
        }
    }

    /// Detect distinct value transitions for a single row
    fn detect_distinct_transitions(
        &self,
        row_values: &[Value],
        weight: isize,
        existing_state: &AggregateState,
        group_distinct_deltas: Option<&HashMap<(usize, HashableRow), isize>>,
    ) -> HashMap<usize, DistinctTransition> {
        let mut transitions = HashMap::default();

        // Plain Distinct doesn't track individual values, so no transitions needed
        if self.is_distinct_only {
            // Distinct is handled by the count alone in apply_delta
            return transitions;
        }

        // Process each distinct column
        for col_idx in &self.distinct_columns {
            let val = match row_values.get(col_idx) {
                Some(v) => v,
                None => continue,
            };

            // Skip null values
            if val == &Value::Null {
                continue;
            }

            if let Some(transition) = Self::detect_distinct_value_transition(
                col_idx,
                val,
                weight,
                existing_state,
                group_distinct_deltas,
            ) {
                transitions.insert(col_idx, transition);
            }
        }

        transitions
    }

    pub fn new(
        operator_id: i64,
        group_by: Vec<usize>,
        aggregates: Vec<AggregateFunction>,
        input_column_names: Vec<String>,
    ) -> Result<Self> {
        // Precompute flags for runtime efficiency
        // Plain DISTINCT is indicated by empty aggregates vector
        let is_distinct_only = aggregates.is_empty();

        // Build map of column indices to their MIN/MAX info
        let mut column_min_max = HashMap::default();
        let mut storage_indices = HashMap::default();
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

        // Second pass: build the column info map for MIN/MAX
        for agg in &aggregates {
            match agg {
                AggregateFunction::Min(col_idx) => {
                    let storage_index = *storage_indices.get(col_idx).ok_or_else(|| {
                        LimboError::InternalError(
                            "storage index for MIN column should exist from first pass".to_string(),
                        )
                    })?;
                    let entry = column_min_max.entry(*col_idx).or_insert(AggColumnInfo {
                        index: storage_index,
                        has_min: false,
                        has_max: false,
                    });
                    entry.has_min = true;
                }
                AggregateFunction::Max(col_idx) => {
                    let storage_index = *storage_indices.get(col_idx).ok_or_else(|| {
                        LimboError::InternalError(
                            "storage index for MAX column should exist from first pass".to_string(),
                        )
                    })?;
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

        // Build the distinct columns set
        let mut distinct_columns = ColumnMask::default();
        for agg in &aggregates {
            match agg {
                AggregateFunction::CountDistinct(col_idx)
                | AggregateFunction::SumDistinct(col_idx)
                | AggregateFunction::AvgDistinct(col_idx) => {
                    distinct_columns.set(*col_idx)?;
                }
                _ => {}
            }
        }

        Ok(Self {
            operator_id,
            group_by,
            aggregates,
            input_column_names,
            column_min_max,
            distinct_columns,
            tracker: None,
            ops: AggregateOps::default(),
            is_distinct_only,
        })
    }

    pub fn has_min_max(&self) -> bool {
        !self.column_min_max.is_empty()
    }

    /// Check if this operator has any DISTINCT aggregates or plain DISTINCT
    pub fn has_distinct(&self) -> bool {
        !self.distinct_columns.is_empty() || self.is_distinct_only
    }

    /// Runs one step of the eval: starts a new eval from an `Init` state and
    /// resumes a suspended one from an `Aggregate` state. The runner lives
    /// in the state while the eval waits for I/O, and in the operator
    /// otherwise.
    fn step_eval(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> IOResultOr<(Delta, ComputedStates)> {
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
                    return Ok(IOResult::Done((Delta::new(), HashMap::default())));
                }
            }
            EvalState::Aggregate(_) => {}
            EvalState::Done => {
                panic!("unreachable state! should have returned");
            }
            EvalState::Join(_) => {
                panic!("Join state should not appear in aggregate operator");
            }
        }
        let (mut op, delta) = match std::mem::replace(state, EvalState::Uninitialized) {
            EvalState::Init { mut deltas } => (
                self.ops.eval.take().unwrap_or_else(new_eval_runner),
                std::mem::take(&mut deltas.left),
            ),
            EvalState::Aggregate(op) => (op, Delta::new()),
            _ => unreachable!("checked above"),
        };
        let mut ctx = AggregateCtx {
            operator: self,
            cursors,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, delta);
        if op.is_active() {
            *state = EvalState::Aggregate(op);
        } else {
            *state = EvalState::Done;
            self.ops.eval = Some(op);
        }
        result
    }

    /// The groups the delta touches, in key order, with the key values of
    /// each group.
    fn groups_of(&self, delta: &Delta) -> Vec<(String, Vec<Value>)> {
        let mut groups_to_read = BTreeMap::new();
        for (row, _weight) in &delta.changes {
            let group_key = self.extract_group_key(&row.values);
            let group_key_str = Self::group_key_to_string(&group_key);
            groups_to_read.insert(group_key_str, group_key);
        }
        groups_to_read.into_iter().collect()
    }

    fn merge_delta_with_existing(
        &mut self,
        delta: &Delta,
        existing_groups: &mut HashMap<String, AggregateState>,
        old_values: &mut HashMap<String, Vec<Value>>,
        pre_existing_groups: &HashSet<String>,
    ) -> Result<MergeResult> {
        let mut output_delta = Delta::new();
        let mut temp_keys: HashMap<String, Vec<Value>> = HashMap::default();

        // Track distinct value weights as we process the batch
        let mut batch_distinct_weights: HashMap<String, HashMap<(usize, HashableRow), isize>> =
            HashMap::default();

        // Process each change in the delta
        for (row, weight) in delta.changes.iter() {
            if let Some(tracker) = &self.tracker {
                tracker.lock().record_aggregation();
            }

            // Extract group key
            let group_key = self.extract_group_key(&row.values);
            let group_key_str = Self::group_key_to_string(&group_key);

            // Get or create the state for this group
            let state = existing_groups.entry(group_key_str.clone()).or_default();

            // Get batch weights for this group
            let group_batch_weights = batch_distinct_weights.get(&group_key_str);

            // Detect distinct transitions using the existing state and batch-accumulated weights
            let distinct_transitions = if self.has_distinct() {
                self.detect_distinct_transitions(&row.values, *weight, state, group_batch_weights)
            } else {
                HashMap::default()
            };

            // Update batch weights after detecting transitions
            if self.has_distinct() {
                for col_idx in &self.distinct_columns {
                    if let Some(val) = row.values.get(col_idx) {
                        if val != &Value::Null {
                            let hashable_row = HashableRow::new(col_idx as i64, vec![val.clone()]);
                            let group_entry = batch_distinct_weights
                                .entry(group_key_str.clone())
                                .or_default();
                            let weight_entry =
                                group_entry.entry((col_idx, hashable_row)).or_insert(0);
                            *weight_entry += weight;
                        }
                    }
                }
            }

            temp_keys.insert(group_key_str.clone(), group_key.clone());

            // Apply the delta to the state with pre-computed transitions
            state.apply_delta(
                &row.values,
                *weight,
                &self.aggregates,
                &self.input_column_names,
                &distinct_transitions,
            )?;
        }

        // Generate output delta from temporary states and collect final states
        let mut final_states = HashMap::default();

        for (group_key_str, state) in existing_groups.iter() {
            let group_key = if let Some(key) = temp_keys.get(group_key_str) {
                key.clone()
            } else if let Some(old_row) = old_values.get(group_key_str) {
                // Extract group key from old row (first N columns where N = group_by.len())
                old_row[0..self.group_by.len()].to_vec()
            } else {
                vec![]
            };

            // Generate synthetic rowid for this group
            let result_key = self.generate_group_rowid(group_key_str);

            // Always store the state for persistence (even if count=0, we need to delete it)
            final_states.insert(group_key_str.clone(), (group_key.clone(), state.clone()));

            // Check if we only have DISTINCT (no other aggregates)
            if self.is_distinct_only {
                // For plain DISTINCT, we output each distinct VALUE (not group)
                // state.count tells us how many distinct values have positive weight

                // Check if this group had any values before
                let old_existed = pre_existing_groups.contains(group_key_str);
                let new_exists = state.count > 0;

                if old_existed && !new_exists {
                    // All distinct values removed: output deletion
                    if let Some(old_row_values) = old_values.get(group_key_str) {
                        let old_row = HashableRow::new(result_key, old_row_values.clone());
                        output_delta.changes.push((old_row, -1));
                    } else {
                        // For plain DISTINCT, the old row is just the group key itself
                        let old_row = HashableRow::new(result_key, group_key.clone());
                        output_delta.changes.push((old_row, -1));
                    }
                } else if !old_existed && new_exists {
                    // First distinct value added: output insertion
                    let output_values = group_key.clone();
                    // DISTINCT doesn't add aggregate values - just the group key
                    let output_row = HashableRow::new(result_key, output_values.clone());
                    output_delta.changes.push((output_row, 1));
                }
                // No output if staying positive or staying at zero
            } else {
                // Normal aggregates: output deletions and insertions as before
                if let Some(old_row_values) = old_values.get(group_key_str) {
                    let old_row = HashableRow::new(result_key, old_row_values.clone());
                    output_delta.changes.push((old_row, -1));
                }

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
        }

        Ok((output_delta, final_states))
    }

    /// Extract distinct values from delta changes for batch tracking
    fn extract_distinct_deltas(&self, delta: &Delta) -> DistinctDeltas {
        let mut distinct_deltas: DistinctDeltas = HashMap::default();

        for (row, weight) in &delta.changes {
            let group_key = self.extract_group_key(&row.values);
            let group_key_str = Self::group_key_to_string(&group_key);

            // Get or create entry for this group
            let group_entry = distinct_deltas.entry(group_key_str.clone()).or_default();

            if self.is_distinct_only {
                // For plain DISTINCT, the group itself is what we're tracking
                // We store a single entry that represents "this group exists N times"
                // Use column index 0 with the group_key_str as the value
                // For group key, use 0 as column index
                let key = (
                    0,
                    HashableRow::new(0, vec![Value::Text(group_key_str.clone().into())]),
                );
                let value_entry = group_entry.entry(key).or_insert(0);
                *value_entry += weight;
            } else {
                // For DISTINCT aggregates, track individual column values
                for col_idx in &self.distinct_columns {
                    if let Some(val) = row.values.get(col_idx) {
                        // Skip NULL values
                        if val == &Value::Null {
                            continue;
                        }

                        let key = (col_idx, HashableRow::new(col_idx as i64, vec![val.clone()]));
                        let value_entry = group_entry.entry(key).or_insert(0);
                        *value_entry += weight;
                    }
                }
            }
        }

        distinct_deltas
    }

    /// Extract MIN/MAX values from delta changes for persistence to index
    fn extract_min_max_deltas(&self, delta: &Delta) -> MinMaxDeltas {
        let mut min_max_deltas: MinMaxDeltas = HashMap::default();

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

    /// The index key, the record and the weight of the stored state of one
    /// group. The weight is -1 when the group is gone, and 1 otherwise.
    fn stored_group(
        &self,
        group_key_str: &str,
        group_key: &[Value],
        agg_state: &AggregateState,
    ) -> Result<(Vec<Value>, Vec<Value>, isize)> {
        let operator_storage_id = generate_storage_id(self.operator_id, 0, AGG_TYPE_REGULAR);
        let zset_hash = self.generate_group_hash(group_key_str);
        let element_id = Hash128::new(0, 0);
        let weight = if agg_state.count == 0 { -1 } else { 1 };
        let state_blob = agg_state.to_blob(&self.aggregates, group_key)?;

        let operator_id_val = Value::from_i64(operator_storage_id);
        let zset_hash_val = zset_hash.to_value()?;
        let element_id_val = element_id.to_value()?;
        let index_key = vec![
            operator_id_val.clone(),
            zset_hash_val.clone(),
            element_id_val.clone(),
        ];
        let record_values = vec![
            operator_id_val,
            zset_hash_val,
            element_id_val,
            Value::Blob(state_blob),
        ];
        Ok((index_key, record_values, weight))
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
    fn eval(&mut self, state: &mut EvalState, cursors: &mut DbspStateCursors) -> IOResultOr<Delta> {
        let (delta, _) = return_if_io!(self.step_eval(state, cursors));
        Ok(IOResult::Done(delta))
    }

    fn commit(
        &mut self,
        mut deltas: DeltaPair,
        cursors: &mut DbspStateCursors,
    ) -> IOResultOr<Delta> {
        // Aggregate operator only uses left delta, right must be empty
        assert!(
            deltas.right.is_empty(),
            "AggregateOperator expects right delta to be empty in commit"
        );
        let delta = std::mem::take(&mut deltas.left);
        let mut op = self.ops.commit.take().unwrap_or_else(new_commit_runner);
        let mut ctx = AggregateCtx {
            operator: self,
            cursors,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, delta);
        self.ops.commit = Some(op);
        result
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

fn new_commit_runner() -> AggregateCommitOp {
    OpRunner::new(Runner::boxed(|co, args| {
        with_handle(co, args, commit_delta)
    }))
}

/// Commits a delta: evaluates it, then stores the new state of every
/// group, the MIN/MAX values, and the distinct values.
async fn commit_delta(co: &mut Co<AggregateStep>, delta: Delta) -> Result<Delta, Box<LimboError>> {
    let (min_max_deltas, distinct_deltas) = co.with(|ctx| {
        let operator = &ctx.operator;
        let min_max_deltas = operator.extract_min_max_deltas(&delta);
        let distinct_deltas = if operator.has_distinct() || operator.is_distinct_only {
            operator.extract_distinct_deltas(&delta)
        } else {
            HashMap::default()
        };
        (min_max_deltas, distinct_deltas)
    });

    let (output_delta, computed_states) = eval_delta(co, delta).await?;

    // Plain DISTINCT has no aggregate state: only the distinct values are stored.
    if !co.with(|ctx| ctx.operator.is_distinct_only) {
        for (group_key_str, (group_key, agg_state)) in &computed_states {
            let (index_key, record_values, weight) = co.with(|ctx| {
                ctx.operator
                    .stored_group(group_key_str, group_key, agg_state)
            })?;
            write_row(co, index_key, record_values, weight).await?;
        }
    }

    if co.with(|ctx| ctx.operator.has_min_max()) {
        persist_min_max(co, min_max_deltas).await?;
    }

    if co.with(|ctx| ctx.operator.has_distinct()) {
        persist_distinct_values(co, distinct_deltas).await?;
    }

    Ok(output_delta)
}

/// Stores every MIN/MAX value of the delta with its weight, so that a
/// later recompute can read the values of a group from the index in
/// order.
async fn persist_min_max(
    co: &mut Co<AggregateStep>,
    min_max_deltas: MinMaxDeltas,
) -> Result<(), Box<LimboError>> {
    for (group_key_str, values) in &min_max_deltas {
        for ((column_name, hashable_row), weight) in values {
            let value = hashable_row.values[0].clone();
            let (index_key, record_values) = co.with(|ctx| {
                let operator = &ctx.operator;
                let column_info = operator
                    .column_min_max
                    .get(column_name)
                    .expect("Column should exist in column_min_max map");
                let storage_id =
                    generate_storage_id(operator.operator_id, column_info.index, AGG_TYPE_MINMAX);
                let zset_hash = operator.generate_group_hash(group_key_str);
                // The value is the element id, so the record needs no value column.
                Ok::<_, LimboError>((
                    vec![
                        Value::from_i64(storage_id),
                        zset_hash.to_value()?,
                        value.clone(),
                    ],
                    vec![
                        Value::from_i64(storage_id),
                        zset_hash.to_value()?,
                        value,
                        Value::Null,
                    ],
                ))
            })?;
            write_row(co, index_key, record_values, *weight).await?;
        }
    }
    Ok(())
}

/// Stores every distinct value of the delta with its weight. The weight
/// is stored as a minimal aggregate state blob, so that the record read
/// can parse it.
async fn persist_distinct_values(
    co: &mut Co<AggregateStep>,
    distinct_deltas: DistinctDeltas,
) -> Result<(), Box<LimboError>> {
    for (group_key, group_values) in &distinct_deltas {
        for ((col_idx, hashable_row), weight) in group_values {
            let value = hashable_row.values.first().ok_or_else(|| {
                LimboError::InternalError("hashable_row should have at least one value".to_string())
            })?;
            let (index_key, record_values) = co.with(|ctx| {
                let operator = &ctx.operator;
                let storage_id =
                    generate_storage_id(operator.operator_id, *col_idx, AGG_TYPE_DISTINCT);
                let zset_hash = operator.generate_group_hash(group_key);
                let element_id = hash_value(value, *col_idx);
                let weight_state = AggregateState {
                    count: *weight as i64,
                    ..Default::default()
                };
                let weight_blob = weight_state.to_blob(&[], &[])?;
                Ok::<_, LimboError>((
                    vec![
                        Value::from_i64(storage_id),
                        zset_hash.to_value()?,
                        element_id.to_value()?,
                    ],
                    vec![
                        Value::from_i64(storage_id),
                        zset_hash.to_value()?,
                        element_id.to_value()?,
                        Value::Blob(weight_blob),
                    ],
                ))
            })?;
            write_row(co, index_key, record_values, *weight).await?;
        }
    }
    Ok(())
}

fn new_eval_runner() -> AggregateEvalOp {
    OpRunner::new(Runner::boxed(|co, args| with_handle(co, args, eval_delta)))
}

/// Evaluates a delta: reads the stored state of every group the delta
/// touches, then the distinct values and the MIN/MAX values that state
/// needs, and merges the delta into that state.
///
/// The steps always run in this order, and a step is a no-op when the
/// operator does not need it:
/// - The aggregate state read is skipped for plain DISTINCT, which has no aggregates.
/// - The distinct value read does nothing without distinct columns.
/// - The MIN/MAX recompute does nothing without MIN/MAX aggregates.
async fn eval_delta(
    co: &mut Co<AggregateStep>,
    delta: Delta,
) -> Result<(Delta, ComputedStates), Box<LimboError>> {
    if delta.changes.is_empty() {
        return Ok((Delta::new(), HashMap::default()));
    }
    let groups_to_read = co.with(|ctx| ctx.operator.groups_of(&delta));
    let mut existing_groups: HashMap<String, AggregateState> = HashMap::default();
    let mut old_values: HashMap<String, Vec<Value>> = HashMap::default();
    let mut pre_existing_groups: HashSet<String> = HashSet::default();

    for (group_key_str, group_key) in &groups_to_read {
        let rowid = fetch_group_rowid(co, group_key_str).await?;
        if co.with(|ctx| ctx.operator.is_distinct_only) {
            // The group must exist so that the distinct value read fills it in.
            existing_groups.insert(group_key_str.clone(), AggregateState::default());
        } else if let Some(rowid) = rowid {
            if let Some(state) = read_record(co, rowid).await? {
                let mut old_row = group_key.clone();
                old_row.extend(co.with(|ctx| state.to_values(&ctx.operator.aggregates)));
                old_values.insert(group_key_str.clone(), old_row);
                existing_groups.insert(group_key_str.clone(), state);
                pre_existing_groups.insert(group_key_str.clone());
            }
        }
    }

    let groups_to_fetch = co.with(|ctx| {
        let operator = &ctx.operator;
        distinct_values_to_fetch(
            &delta,
            &operator.distinct_columns,
            |values| operator.extract_group_key(values),
            AggregateOperator::group_key_to_string,
            &existing_groups,
            operator.is_distinct_only,
        )
    });
    fetch_distinct_values(co, groups_to_fetch, &mut existing_groups).await?;

    // For plain DISTINCT, a group with a stored distinct value existed before this delta.
    if co.with(|ctx| ctx.operator.is_distinct_only) {
        for (group_key_str, state) in existing_groups.iter() {
            let has_values = state.distinct_value_weights.values().any(|&w| w > 0);
            if has_values {
                pre_existing_groups.insert(group_key_str.clone());
            }
        }
    }

    if co.with(|ctx| ctx.operator.has_min_max()) {
        let min_max_deltas = co.with(|ctx| ctx.operator.extract_min_max_deltas(&delta));
        recompute_min_max(co, min_max_deltas, &mut existing_groups).await?;
    }

    let (output_delta, computed_states) = co.with(|ctx| {
        ctx.operator.merge_delta_with_existing(
            &delta,
            &mut existing_groups,
            &mut old_values,
            &pre_existing_groups,
        )
    })?;
    Ok((output_delta, computed_states))
}

/// The rowid of the stored state of one group, or None when the group has
/// no stored state.
async fn fetch_group_rowid(
    co: &mut Co<AggregateStep>,
    group_key_str: &str,
) -> Result<Option<i64>, Box<LimboError>> {
    let index_key_values = co.with(|ctx| {
        let operator = &ctx.operator;
        let operator_storage_id = generate_storage_id(operator.operator_id, 0, AGG_TYPE_REGULAR);
        let zset_hash = operator.generate_group_hash(group_key_str);
        let element_id = Hash128::new(0, 0);
        Ok::<_, LimboError>(vec![
            Value::from_i64(operator_storage_id),
            zset_hash.to_value()?,
            element_id.to_value()?,
        ])
    })?;
    let index_record = ImmutableRecord::from_values(&index_key_values, index_key_values.len())?;
    let seek_result = co
        .io(|ctx| {
            ctx.cursors.index_cursor.seek(
                SeekKey::IndexKey(index_record.as_record_ref()),
                SeekOp::GE { eq_only: true },
            )
        })
        .await;
    if !matches!(seek_result, SeekResult::Found) {
        return Ok(None);
    }
    Ok(co.io(|ctx| ctx.cursors.index_cursor.rowid()).await)
}

/// Finds the new MIN or MAX of every column whose current value the delta
/// retracts, and the first MIN or MAX of every column the delta inserts a
/// value into.
async fn recompute_min_max(
    co: &mut Co<AggregateStep>,
    min_max_deltas: MinMaxDeltas,
    existing_groups: &mut HashMap<String, AggregateState>,
) -> Result<(), Box<LimboError>> {
    let columns_to_process =
        co.with(|ctx| min_max_columns_to_check(&min_max_deltas, existing_groups, ctx.operator));

    for (group_key, column_name, is_min) in columns_to_process {
        let (storage_id, zset_hash) = co.with(|ctx| {
            let operator = &ctx.operator;
            let column_info = operator
                .column_min_max
                .get(&column_name)
                .expect("Column should exist in column_min_max map");
            (
                generate_storage_id(operator.operator_id, column_info.index, AGG_TYPE_MINMAX),
                operator.generate_group_hash(&group_key),
            )
        });
        let current_value = existing_groups.get(&group_key).and_then(|state| {
            if is_min {
                state.mins.get(&column_name).cloned()
            } else {
                state.maxs.get(&column_name).cloned()
            }
        });
        let group_values = min_max_deltas.get(&group_key).cloned().unwrap_or_default();

        let new_value = find_min_max(
            co,
            current_value,
            column_name,
            storage_id,
            zset_hash,
            &group_values,
            is_min,
        )
        .await?;

        let state = existing_groups.entry(group_key).or_default();
        let values = if is_min {
            &mut state.mins
        } else {
            &mut state.maxs
        };
        match new_value {
            Some(value) => {
                values.insert(column_name, value);
            }
            None => {
                values.remove(&column_name);
            }
        }
    }
    Ok(())
}

/// The (group, column, is_min) triples whose MIN or MAX the delta can
/// change: a deletion of the current MIN or MAX of a group, or an insert
/// into a column that has MIN or MAX.
fn min_max_columns_to_check(
    min_max_deltas: &MinMaxDeltas,
    existing_groups: &HashMap<String, AggregateState>,
    operator: &AggregateOperator,
) -> Vec<(String, usize, bool)> {
    let mut groups_to_check: HashSet<(String, usize, bool)> = HashSet::default();

    // Remember the min_max_deltas are essentially just the only column that is affected by
    // this min/max, in delta (actually ZSet - consolidated delta) format. This makes it easier
    // for us to consume it in here.
    //
    // The most challenging case is the case where there is a retraction, since we need to go
    // back to the index.
    for (group_key_str, values) in min_max_deltas {
        for ((col_name, hashable_row), weight) in values {
            let col_info = operator.column_min_max.get(col_name);

            let value = &hashable_row.values[0];

            if *weight < 0 {
                // Deletion detected - check if it's the current MIN/MAX
                if let Some(state) = existing_groups.get(group_key_str) {
                    if let Some(current_min) = state.mins.get(col_name) {
                        if current_min == value {
                            groups_to_check.insert((group_key_str.clone(), *col_name, true));
                        }
                    }
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

    groups_to_check.into_iter().collect()
}

/// The MIN or MAX of one column of one group after the delta. Starts from
/// the current value, reads the next candidate from the index while the
/// delta retracts the candidate, then compares the candidate with the
/// values the delta inserts.
async fn find_min_max(
    co: &mut Co<AggregateStep>,
    mut candidate: Option<Value>,
    column_name: usize,
    storage_id: i64,
    zset_hash: Hash128,
    group_values: &HashMap<(usize, HashableRow), isize>,
    is_min: bool,
) -> Result<Option<Value>, Box<LimboError>> {
    while let Some(cand_val) = &candidate {
        let key = (column_name, HashableRow::new(0, vec![cand_val.clone()]));
        let is_retracted = group_values.get(&key).is_some_and(|weight| *weight <= 0);
        if !is_retracted {
            break;
        }
        let index_key = vec![
            Value::from_i64(storage_id),
            zset_hash.to_value()?,
            cand_val.clone(),
        ];
        let index_record = ImmutableRecord::from_values(&index_key, index_key.len())?;
        let seek_op = if is_min { SeekOp::GT } else { SeekOp::LT };
        let seek_result = co
            .io(|ctx| {
                ctx.cursors
                    .index_cursor
                    .seek(SeekKey::IndexKey(index_record.as_record_ref()), seek_op)
            })
            .await;
        candidate = if matches!(seek_result, SeekResult::Found) {
            co.io(|ctx| {
                candidate_of_index_row(&mut ctx.cursors.index_cursor, storage_id, zset_hash)
            })
            .await
        } else {
            None
        };
    }
    Ok(best_min_max(candidate, column_name, group_values, is_min))
}

/// The value of the index row the cursor is on, or None when that row
/// belongs to another operator or group: then this group has no more
/// candidates.
fn candidate_of_index_row(
    cursor: &mut BTreeCursor,
    storage_id: i64,
    zset_hash: Hash128,
) -> IOResultOr<Option<Value>> {
    let record = return_if_io!(cursor.record()).ok_or_else(|| {
        LimboError::InternalError("Record found on the cursor, but could not be read".to_string())
    })?;

    let mut values = record.iter()?;

    let Some(rec_storage_id) = values.next() else {
        return Ok(IOResult::Done(None));
    };

    let Some(rec_zset_hash) = values.next() else {
        return Ok(IOResult::Done(None));
    };

    if let ValueRef::Numeric(Numeric::Integer(rec_sid)) = rec_storage_id? {
        if rec_sid != storage_id {
            return Ok(IOResult::Done(None));
        }
    } else {
        return Ok(IOResult::Done(None));
    }

    if let ValueRef::Blob(rec_zset_blob) = rec_zset_hash? {
        if let Some(rec_hash) = Hash128::from_blob(rec_zset_blob) {
            if rec_hash != zset_hash {
                return Ok(IOResult::Done(None));
            }
        } else {
            return Ok(IOResult::Done(None));
        }
    } else {
        return Ok(IOResult::Done(None));
    }

    let Some(third) = values.next() else {
        return Ok(IOResult::Done(None));
    };

    Ok(IOResult::Done(Some(third?.to_owned()?)))
}

/// The better of the candidate and the best value the delta inserts into
/// the column. NULL values do not take part in MIN/MAX.
fn best_min_max(
    candidate: Option<Value>,
    column_name: usize,
    group_values: &HashMap<(usize, HashableRow), isize>,
    is_min: bool,
) -> Option<Value> {
    let mut best_from_zset: Option<Value> = None;
    for ((col, hashable_val), weight) in group_values.iter() {
        if *col == column_name && *weight > 0 {
            let value = &hashable_val.values[0];
            if value == &Value::Null {
                continue;
            }
            if let Some(ref current_best) = best_from_zset {
                if is_min {
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

    match (&candidate, &best_from_zset) {
        (Some(cand), Some(zset_val)) if cand != &Value::Null => {
            if is_min {
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
    }
}

/// The distinct values whose stored weights the eval of `delta` needs,
/// by group and column. For plain DISTINCT the group itself is the value.
/// DISTINCT aggregates only read values of groups that already exist.
fn distinct_values_to_fetch(
    delta: &Delta,
    distinct_columns: &ColumnMask,
    extract_group_key: impl Fn(&[Value]) -> Vec<Value>,
    group_key_to_string: impl Fn(&[Value]) -> String,
    existing_groups: &HashMap<String, AggregateState>,
    is_plain_distinct: bool,
) -> Vec<(String, HashMap<usize, HashSet<HashableRow>>)> {
    let mut groups_to_fetch: HashMap<String, HashMap<usize, HashSet<HashableRow>>> =
        HashMap::default();

    for (row, _weight) in &delta.changes {
        let group_key = extract_group_key(&row.values);
        let group_key_str = group_key_to_string(&group_key);

        if !is_plain_distinct && !existing_groups.contains_key(&group_key_str) {
            continue;
        }

        let group_entry = groups_to_fetch.entry(group_key_str.clone()).or_default();

        if is_plain_distinct {
            add_plain_distinct_fetch(group_entry, &group_key_str);
        } else {
            add_aggregate_distinct_fetch(group_entry, &row.values, distinct_columns);
        }
    }

    groups_to_fetch.into_iter().collect()
}

/// Add fetch entry for plain DISTINCT - the group itself is the distinct value
fn add_plain_distinct_fetch(
    group_entry: &mut HashMap<usize, HashSet<HashableRow>>,
    group_key_str: &str,
) {
    let group_value = Value::Text(group_key_str.to_string().into());
    group_entry
        .entry(0)
        .or_default()
        .insert(HashableRow::new(0, vec![group_value]));
}

/// Add fetch entries for DISTINCT aggregates - individual column values
fn add_aggregate_distinct_fetch(
    group_entry: &mut HashMap<usize, HashSet<HashableRow>>,
    row_values: &[Value],
    distinct_columns: &ColumnMask,
) {
    for col_idx in distinct_columns {
        if let Some(val) = row_values.get(col_idx) {
            if val != &Value::Null {
                group_entry
                    .entry(col_idx)
                    .or_default()
                    .insert(HashableRow::new(col_idx as i64, vec![val.clone()]));
            }
        }
    }
}

/// Reads the stored weight of every distinct value in `groups_to_fetch`
/// into the state of its group. For plain DISTINCT, the count of a group
/// is the sum of those weights.
async fn fetch_distinct_values(
    co: &mut Co<AggregateStep>,
    groups_to_fetch: Vec<(String, HashMap<usize, HashSet<HashableRow>>)>,
    existing_groups: &mut HashMap<String, AggregateState>,
) -> Result<(), Box<LimboError>> {
    for (group_key, cols_values) in &groups_to_fetch {
        for (col_idx, values) in cols_values {
            for hashable_row in values {
                let value = hashable_row.values.first().ok_or_else(|| {
                    LimboError::InternalError(
                        "hashable_row should have at least one value".to_string(),
                    )
                })?;
                let Some(weight) = read_distinct_weight(co, group_key, *col_idx, value).await?
                else {
                    continue;
                };
                let state = existing_groups.entry(group_key.clone()).or_default();
                state.distinct_value_weights.insert(
                    (
                        *col_idx,
                        HashableRow::new(*col_idx as i64, vec![value.clone()]),
                    ),
                    weight,
                );
            }
        }
    }

    if co.with(|ctx| ctx.operator.is_distinct_only) {
        for state in existing_groups.values_mut() {
            state.count = state.distinct_value_weights.values().sum();
        }
    }
    Ok(())
}

/// The stored weight of one distinct value of a group, or None when the
/// value has no stored row. The row is found through the index, as in
/// the row write.
async fn read_distinct_weight(
    co: &mut Co<AggregateStep>,
    group_key: &str,
    column_idx: usize,
    value: &Value,
) -> Result<Option<i64>, Box<LimboError>> {
    let index_key = co.with(|ctx| {
        let operator = &ctx.operator;
        let storage_id = generate_storage_id(operator.operator_id, column_idx, AGG_TYPE_DISTINCT);
        let zset_hash = operator.generate_group_hash(group_key);
        let element_id = hash_value(value, column_idx);
        Ok::<_, LimboError>(vec![
            Value::from_i64(storage_id),
            zset_hash.to_value()?,
            element_id.to_value()?,
        ])
    })?;
    let index_record = ImmutableRecord::from_values(&index_key, index_key.len())?;

    let seek_result = co
        .io(|ctx| {
            ctx.cursors.index_cursor.seek(
                SeekKey::IndexKey(index_record.as_record_ref()),
                SeekOp::GE { eq_only: true },
            )
        })
        .await;
    if !matches!(seek_result, SeekResult::Found) {
        return Ok(None);
    }
    let Some(rowid) = co.io(|ctx| ctx.cursors.index_cursor.rowid()).await else {
        return Ok(None);
    };
    let table_result = co
        .io(|ctx| {
            ctx.cursors
                .table_cursor
                .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })
        })
        .await;
    if !matches!(table_result, SeekResult::Found) {
        return Ok(None);
    }
    Ok(co
        .io(|ctx| stored_weight(&mut ctx.cursors.table_cursor))
        .await)
}

/// The weight column of the record the table cursor is on, or None when
/// the record has no weight. A weight that is not an integer counts as 0.
fn stored_weight(cursor: &mut BTreeCursor) -> IOResultOr<Option<i64>> {
    let Some(record) = return_if_io!(cursor.record()) else {
        return Ok(IOResult::Done(None));
    };
    let Some(weight) = record.get_value_opt(4) else {
        return Ok(IOResult::Done(None));
    };
    let weight = match weight.to_owned()? {
        Value::Numeric(Numeric::Integer(w)) => w,
        _ => 0,
    };
    Ok(IOResult::Done(Some(weight)))
}
