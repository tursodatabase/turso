// Merge operator for DBSP - combines two delta streams
// Used in recursive CTEs and UNION operations

use crate::incremental::dbsp::{Delta, DeltaPair, HashableRow};
use crate::incremental::operator::{
    ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
};
use crate::types::IOResult;
use crate::Result;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// How the merge operator should handle rowids when combining deltas
#[derive(Debug, Clone)]
pub enum UnionMode {
    /// For UNION (distinct) - hash values only to merge duplicates
    Distinct,
    /// For UNION ALL - include source table name in hash to keep duplicates separate
    All {
        left_table: String,
        right_table: String,
    },
}

/// Merge operator that combines two input deltas into one output delta
/// Handles both recursive CTEs and UNION/UNION ALL operations
#[derive(Debug)]
pub struct MergeOperator {
    operator_id: i64,
    union_mode: UnionMode,
    /// For UNION: tracks seen value hashes with their assigned rowids
    /// For UNION ALL: tracks (source_id, original_rowid) -> assigned_rowid mappings
    seen_rows: HashMap<u64, i64>, // hash -> assigned_rowid
    /// Next rowid to assign for new rows
    next_rowid: i64,
}

impl MergeOperator {
    /// Create a new merge operator with specified union mode
    pub fn new(operator_id: i64, mode: UnionMode) -> Self {
        Self {
            operator_id,
            union_mode: mode,
            seen_rows: HashMap::new(),
            next_rowid: 1,
        }
    }

    /// Transform a delta's rowids based on the union mode with state tracking
    fn transform_delta(&mut self, delta: Delta, is_left: bool) -> Delta {
        match &self.union_mode {
            UnionMode::Distinct => {
                // For UNION distinct, track seen values and deduplicate
                let mut output = Delta::new();
                for (row, weight) in delta.changes {
                    // Hash only the values (not rowid) for deduplication
                    let temp_row = HashableRow::new(0, row.values.clone());
                    let value_hash = temp_row.cached_hash().as_i64() as u64;

                    // Check if we've seen this value before
                    let assigned_rowid =
                        if let Some(&existing_rowid) = self.seen_rows.get(&value_hash) {
                            // Value already seen - use existing rowid
                            existing_rowid
                        } else {
                            // New value - assign new rowid and remember it
                            let new_rowid = self.next_rowid;
                            self.next_rowid += 1;
                            self.seen_rows.insert(value_hash, new_rowid);
                            new_rowid
                        };

                    // Output the row with the assigned rowid
                    let final_row = HashableRow::new(assigned_rowid, temp_row.values);
                    output.changes.push((final_row, weight));
                }
                output
            }
            UnionMode::All {
                left_table,
                right_table,
            } => {
                // For UNION ALL, maintain consistent rowid mapping per source
                let table = if is_left { left_table } else { right_table };
                let mut source_hasher = DefaultHasher::new();
                table.hash(&mut source_hasher);
                let source_id = source_hasher.finish();

                let mut output = Delta::new();
                for (row, weight) in delta.changes {
                    // Create a unique key for this (source, rowid) pair
                    let mut key_hasher = DefaultHasher::new();
                    source_id.hash(&mut key_hasher);
                    row.rowid.hash(&mut key_hasher);
                    let key_hash = key_hasher.finish();

                    // Check if we've seen this (source, rowid) before
                    let assigned_rowid =
                        if let Some(&existing_rowid) = self.seen_rows.get(&key_hash) {
                            // Use existing rowid for this (source, rowid) pair
                            existing_rowid
                        } else {
                            // New row - assign new rowid
                            let new_rowid = self.next_rowid;
                            self.next_rowid += 1;
                            self.seen_rows.insert(key_hash, new_rowid);
                            new_rowid
                        };

                    // Create output row with consistent rowid
                    let final_row = HashableRow::new(assigned_rowid, row.values.clone());
                    output.changes.push((final_row, weight));
                }
                output
            }
        }
    }
}

impl Display for MergeOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.union_mode {
            UnionMode::Distinct => write!(f, "MergeOperator({}, UNION)", self.operator_id),
            UnionMode::All { .. } => write!(f, "MergeOperator({}, UNION ALL)", self.operator_id),
        }
    }
}

impl IncrementalOperator for MergeOperator {
    fn eval(
        &mut self,
        input: &mut EvalState,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        match input {
            EvalState::Init { deltas } => {
                // Extract deltas from the evaluation state
                let delta_pair = std::mem::take(deltas);

                // Transform deltas based on union mode (with state tracking)
                let left_transformed = self.transform_delta(delta_pair.left, true);
                let right_transformed = self.transform_delta(delta_pair.right, false);

                // Merge the transformed deltas
                let mut output = Delta::new();
                output.merge(&left_transformed);
                output.merge(&right_transformed);

                // Move to Done state
                *input = EvalState::Done;

                Ok(IOResult::Done(output))
            }
            EvalState::Aggregate(_) | EvalState::Join(_) | EvalState::Uninitialized => {
                // Merge operator only handles Init state
                unreachable!("MergeOperator only handles Init state")
            }
            EvalState::Done => {
                // Already evaluated
                Ok(IOResult::Done(Delta::new()))
            }
        }
    }

    fn commit(
        &mut self,
        deltas: DeltaPair,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Transform deltas based on union mode
        let left_transformed = self.transform_delta(deltas.left, true);
        let right_transformed = self.transform_delta(deltas.right, false);

        // Merge the transformed deltas
        let mut output = Delta::new();
        output.merge(&left_transformed);
        output.merge(&right_transformed);

        Ok(IOResult::Done(output))
    }

    fn set_tracker(&mut self, _tracker: Arc<Mutex<ComputationTracker>>) {
        // Merge operator doesn't need tracking for now
    }
}
