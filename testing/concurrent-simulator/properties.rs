//! Property-based validation for simulation.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{anyhow, bail};
use turso_core::{LimboError, Value};

use crate::elle::{ElleEventType, ElleOp};
use crate::operations::{OpResult, Operation};

/// A property that can be validated during simulation.
/// Properties observe operations and can validate invariants.
pub trait Property: Send + Sync {
    /// Called when an operation finishes execution.
    /// Can perform validation and return an error if invariant is violated.
    #[allow(clippy::too_many_arguments)]
    fn finish_op(
        &mut self,
        _step: usize,
        _fiber_id: usize,
        _txn_id: Option<u64>,
        _start_exec_id: u64,
        _end_exec_id: u64,
        _op: &Operation,
        _result: &OpResult,
    ) -> anyhow::Result<()>;
}

pub struct SimpleKeysDoNotDisappear {
    /// map which stores moment of start for the transaction which equals to start of the successful BEGIN operation
    /// we use this moment in order to "merge" AdditionMoments from transaction keys to the global scope after COMMIT operation
    pub txn_started_at: HashMap<u64, u64>,
    /// map of simple keys addition moment: TxnId -> (Table, Key) -> AdditionMoment
    /// For every transaction we put information about key addition moment which equals to end of successful INSERT operation
    /// We use None key to represent "commited" state of the database
    /// The "commited" state modified by operations in auto-commit mode (txn_id is None) or after successful COMMIT operation
    pub simple_keys_added_at: HashMap<Option<u64>, HashMap<(String, String), u64>>,
}

impl Default for SimpleKeysDoNotDisappear {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleKeysDoNotDisappear {
    pub fn new() -> Self {
        let mut simple_keys_added_at = HashMap::new();
        simple_keys_added_at.insert(None, HashMap::new());
        Self {
            txn_started_at: HashMap::new(),
            simple_keys_added_at,
        }
    }
}

impl Property for SimpleKeysDoNotDisappear {
    fn finish_op(
        &mut self,
        _step: usize,
        _fiber_id: usize,
        txn_id: Option<u64>,
        start_exec_id: u64,
        end_exec_id: u64,
        op: &Operation,
        result: &OpResult,
    ) -> anyhow::Result<()> {
        let Ok(rows) = result else {
            // ignore failed operations
            return Ok(());
        };
        // on successful ROLLBACK we just remove all information about this transaction
        if let Operation::Rollback = &op {
            self.txn_started_at
                .remove(&txn_id.expect("transaction id must be set"));
            self.simple_keys_added_at.remove(&txn_id);
        }

        // on successful COMMIT we move information about current transaction keys to the "commited" state (None key in the map)
        // note, that we use end_exec_id of current COMMIT operation as AdditionMoment of moved keys
        if let Operation::Commit = &op {
            if let Some(keys) = self.simple_keys_added_at.remove(&txn_id) {
                let global = self.simple_keys_added_at.get_mut(&None).unwrap();
                for (key, _) in keys {
                    global.insert(key, end_exec_id);
                }
            }
        }

        // on successful BEGIN we record start time of the transaction
        if let Operation::Begin { .. } = &op {
            self.txn_started_at
                .insert(txn_id.expect("transaction id must be set"), start_exec_id);
        }

        // on successful INSERT put (table, key) information in the slot for current transaction and use end_exec_id as AdditionMoment
        if let Operation::SimpleInsert {
            table_name, key, ..
        } = &op
        {
            let search_key = (table_name.clone(), key.clone());
            tracing::debug!("SimpleKeysDoNotDisappear: op=SimpleInsert, key={key}");
            self.simple_keys_added_at
                .entry(txn_id)
                .and_modify(|s| {
                    s.insert(search_key.clone(), end_exec_id);
                })
                .or_insert_with(|| {
                    let mut s = HashMap::new();
                    s.insert(search_key, end_exec_id);
                    s
                });
        }

        // on successful SELECT get information about the key AdditionMoment from the "commited" state: key_exec_id
        // calculate our current ViewMoment as start_exec_id (in auto-commit mode) or start moment of the current transaction: view_exec_id
        // if we have information about key in the "commited" state and key_exec_id < view_exec_id -> then key MUST be visible
        if let Operation::SimpleSelect { table_name, key } = &op {
            let search_key = (table_name.clone(), key.clone());
            let key_exec_id = self
                .simple_keys_added_at
                .get(&None)
                .map(|s| s.get(&search_key))
                .unwrap_or(None);
            let view_exec_id = if let Some(txn_id) = txn_id {
                self.txn_started_at.get(&txn_id)
            } else {
                None
            }
            .unwrap_or(&start_exec_id);
            tracing::debug!("SimpleKeysDoNotDisappear: op=SimpleSelect, key={key}, rows={rows:?}");
            if key_exec_id.is_some() && key_exec_id.unwrap() < view_exec_id && rows.is_empty() {
                return Err(anyhow!(
                    "row disappeared: table={}, key={}, key_exec_id={:?}, view_exec_id={:?}",
                    table_name,
                    key,
                    key_exec_id,
                    view_exec_id,
                ));
            }
        }
        Ok(())
    }
}

/// Property that validates integrity check results.
/// Integrity check must either return a busy error or a single row with "ok".
pub struct IntegrityCheckProperty;

impl Property for IntegrityCheckProperty {
    fn finish_op(
        &mut self,
        step: usize,
        fiber_id: usize,
        _txn_id: Option<u64>,
        _start_exec_id: u64,
        _end_exec_id: u64,
        op: &Operation,
        result: &OpResult,
    ) -> anyhow::Result<()> {
        if !matches!(op, Operation::IntegrityCheck) {
            return Ok(());
        }

        match result {
            Err(error) => {
                // Busy errors are acceptable
                if matches!(
                    error,
                    LimboError::Busy
                        | LimboError::BusySnapshot
                        | LimboError::SchemaUpdated
                        | LimboError::SchemaConflict
                ) {
                    return Ok(());
                }
                bail!("step {step}, fiber {fiber_id}: integrity_check failed with error: {error}");
            }
            Ok(rows) => {
                if rows.len() != 1 {
                    bail!(
                        "step {step}, fiber {fiber_id}: integrity_check returned {} rows, expected 1",
                        rows.len()
                    );
                }
                let row = &rows[0];
                if row.len() != 1 {
                    bail!(
                        "step {step}, fiber {fiber_id}: integrity_check row has {} columns, expected 1",
                        row.len()
                    );
                }
                match &row[0] {
                    Value::Text(text) if text.as_str() == "ok" => Ok(()),
                    other => {
                        bail!(
                            "step {step}, fiber {fiber_id}: integrity_check returned {:?}, expected \"ok\"",
                            other
                        );
                    }
                }
            }
        }
    }
}

/// Property that records Elle history for transactional consistency analysis.
/// Tracks all Elle operations and exports history to EDN format for elle-cli analysis.
/// Writes events incrementally to the output file.
pub struct ElleHistoryRecorder {
    /// Pending transaction operations per fiber: fiber_id -> Vec<ElleOp>
    pending_txns: HashMap<usize, Vec<ElleOp>>,
    /// Output file handle for incremental writing
    output_file: std::fs::File,
    /// Counter for generating unique event indices
    index_counter: u64,
    /// Output path for reporting
    output_path: PathBuf,
    /// Count of events written
    event_count: usize,
}

impl ElleHistoryRecorder {
    pub fn new(output_path: PathBuf) -> Self {
        let file = std::fs::File::create(&output_path).expect("Failed to create Elle output file");
        Self {
            pending_txns: HashMap::new(),
            output_file: file,
            index_counter: 0,
            output_path,
            event_count: 0,
        }
    }

    /// Get the output path.
    pub fn output_path(&self) -> &PathBuf {
        &self.output_path
    }

    /// Get the number of events written.
    pub fn event_count(&self) -> usize {
        self.event_count
    }

    /// Write an event to the output file.
    fn write_event(&mut self, event_type: ElleEventType, process: usize, ops: &[ElleOp]) {
        use std::io::Write;

        let index_counter = &mut self.index_counter;
        let index = *index_counter;
        *index_counter += 1;

        let ops_str: Vec<String> = ops.iter().map(|op| op.to_edn()).collect();
        let line = format!(
            "{{:type {}, :f :txn, :value [{}], :process {}, :index {}}}\n",
            event_type,
            ops_str.join(" "),
            process,
            index
        );

        let file = &mut self.output_file;
        file.write_all(line.as_bytes())
            .expect("Failed to write Elle event");

        let count = &mut self.event_count;
        *count += 1;
    }
}

impl Property for ElleHistoryRecorder {
    fn finish_op(
        &mut self,
        _step: usize,
        fiber_id: usize,
        txn_id: Option<u64>,
        _start_exec_id: u64,
        _end_exec_id: u64,
        op: &Operation,
        result: &OpResult,
    ) -> anyhow::Result<()> {
        let pending_txns = &mut self.pending_txns;

        // Handle Begin - start tracking ops for this fiber
        match op {
            Operation::Begin { .. } => {
                if result.is_ok() {
                    pending_txns.insert(fiber_id, Vec::new());
                }
            }
            Operation::Commit => {
                // emit invoke/ok with all accumulated ops
                if result.is_ok() {
                    if let Some(ops) = pending_txns.remove(&fiber_id) {
                        if !ops.is_empty() {
                            // Record invoke with the ops (nil results for reads)
                            let invoke_ops: Vec<ElleOp> = ops
                                .iter()
                                .map(|o| match o {
                                    ElleOp::Read { key, .. } => ElleOp::Read {
                                        key: key.clone(),
                                        result: None,
                                    },
                                    other => other.clone(),
                                })
                                .collect();
                            self.write_event(ElleEventType::Invoke, fiber_id, &invoke_ops);
                            self.write_event(ElleEventType::Ok, fiber_id, &ops);
                        }
                    }
                }
            }
            Operation::Rollback => {
                // emit invoke/fail with accumulated ops
                if let Some(ops) = pending_txns.remove(&fiber_id) {
                    if !ops.is_empty() {
                        let invoke_ops: Vec<ElleOp> = ops
                            .iter()
                            .map(|o| match o {
                                ElleOp::Read { key, .. } => ElleOp::Read {
                                    key: key.clone(),
                                    result: None,
                                },
                                other => other.clone(),
                            })
                            .collect();
                        self.write_event(ElleEventType::Invoke, fiber_id, &invoke_ops);
                        self.write_event(ElleEventType::Fail, fiber_id, &ops);
                    }
                }
            }
            Operation::ElleAppend { key, value, .. } => {
                let elle_op = ElleOp::Append {
                    key: key.clone(),
                    value: *value,
                };

                if txn_id.is_some() {
                    // In a transaction - accumulate the op
                    if let Some(ops) = pending_txns.get_mut(&fiber_id) {
                        if result.is_ok() {
                            ops.push(elle_op);
                        }
                    }
                } else {
                    // Auto-commit mode - emit single-op transaction immediately
                    let ops = vec![elle_op];
                    if result.is_ok() {
                        self.write_event(ElleEventType::Invoke, fiber_id, &ops);
                        self.write_event(ElleEventType::Ok, fiber_id, &ops);
                    } else {
                        self.write_event(ElleEventType::Invoke, fiber_id, &ops);
                        self.write_event(ElleEventType::Fail, fiber_id, &ops);
                    }
                }
            }
            Operation::ElleRead { key, .. } => {
                // Parse the result to get the list values
                let read_result = if let Ok(rows) = result {
                    if rows.is_empty() {
                        // Key doesn't exist yet - empty list
                        Some(vec![])
                    } else if let Some(row) = rows.first() {
                        if let Some(Value::Text(csv_str)) = row.first() {
                            // Parse comma-separated integers like "1,2,3"
                            parse_comma_separated_ints(csv_str.as_str())
                        } else {
                            // Null or unexpected type - treat as empty list
                            Some(vec![])
                        }
                    } else {
                        Some(vec![])
                    }
                } else {
                    None // Operation failed
                };

                let elle_op = ElleOp::Read {
                    key: key.clone(),
                    result: read_result,
                };

                if txn_id.is_some() {
                    // In a transaction - accumulate the op
                    if let Some(ops) = pending_txns.get_mut(&fiber_id) {
                        if result.is_ok() {
                            ops.push(elle_op);
                        }
                    }
                } else {
                    // Auto-commit mode - emit single-op transaction immediately
                    let invoke_op = ElleOp::Read {
                        key: key.clone(),
                        result: None,
                    };
                    if result.is_ok() {
                        self.write_event(ElleEventType::Invoke, fiber_id, &[invoke_op]);
                        self.write_event(ElleEventType::Ok, fiber_id, &[elle_op]);
                    } else {
                        self.write_event(ElleEventType::Invoke, fiber_id, &[invoke_op]);
                        self.write_event(
                            ElleEventType::Fail,
                            fiber_id,
                            &[ElleOp::Read {
                                key: key.clone(),
                                result: None,
                            }],
                        );
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

/// Parse a comma-separated list of integers like "1,2,3" into a Vec<i64>.
fn parse_comma_separated_ints(s: &str) -> Option<Vec<i64>> {
    let s = s.trim();
    if s.is_empty() {
        return Some(vec![]);
    }
    let values: Result<Vec<i64>, _> = s.split(',').map(|v| v.trim().parse::<i64>()).collect();
    values.ok()
}
