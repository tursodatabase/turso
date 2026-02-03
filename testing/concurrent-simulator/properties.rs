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
    /// Called when an operation starts execution.
    /// Default implementation does nothing.
    #[allow(clippy::too_many_arguments)]
    fn init_op(
        &mut self,
        _step: usize,
        _fiber_id: usize,
        _txn_id: Option<u64>,
        _exec_id: u64,
        _op: &Operation,
    ) -> anyhow::Result<()> {
        Ok(())
    }

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

    /// Called when the simulation finishes.
    /// Default implementation does nothing.
    fn finalize(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
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

/// A buffered Elle event to be sorted and written at the end.
struct BufferedElleEvent {
    index: u64,
    event_type: ElleEventType,
    process: usize,
    ops: Vec<ElleOp>,
}

/// Pending transaction state: invoke index and accumulated operations.
struct PendingTxn {
    invoke_index: u64,
    ops: Vec<ElleOp>,
}

/// Pending auto-commit operation state: invoke index and the operation.
struct PendingAutoCommit {
    invoke_index: u64,
    op: ElleOp,
}

/// Property that records Elle history for transactional consistency checking.
/// Events are buffered in memory and sorted by index before writing to file.
pub struct ElleHistoryRecorder {
    /// Pending transactions per fiber: fiber_id -> PendingTxn
    pending_txns: HashMap<usize, PendingTxn>,
    /// Pending auto-commit operations per fiber: fiber_id -> PendingAutoCommit
    pending_auto_commits: HashMap<usize, PendingAutoCommit>,
    /// Buffered events to be sorted and written at the end
    events: Vec<BufferedElleEvent>,
    /// Counter for generating unique event indices
    index_counter: u64,
    /// Output path for the EDN file
    output_path: PathBuf,
}

impl ElleHistoryRecorder {
    pub fn new(output_path: PathBuf) -> Self {
        Self {
            pending_txns: HashMap::new(),
            pending_auto_commits: HashMap::new(),
            events: Vec::new(),
            index_counter: 0,
            output_path,
        }
    }

    /// Get the output path.
    pub fn output_path(&self) -> &PathBuf {
        &self.output_path
    }

    /// Get the number of events buffered.
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Reserve and return the next event index.
    fn next_index(&mut self) -> u64 {
        let index = self.index_counter;
        self.index_counter += 1;
        index
    }

    /// Add an event to the buffer.
    fn add_event(
        &mut self,
        index: u64,
        event_type: ElleEventType,
        process: usize,
        ops: Vec<ElleOp>,
    ) {
        self.events.push(BufferedElleEvent {
            index,
            event_type,
            process,
            ops,
        });
    }

    /// Export all buffered events to the EDN file, sorted by index.
    pub fn export(&self) -> std::io::Result<()> {
        use std::io::Write;

        let mut sorted_events: Vec<_> = self.events.iter().collect();
        sorted_events.sort_by_key(|e| e.index);

        let mut file = std::fs::File::create(&self.output_path)?;
        for event in sorted_events {
            let ops_str: Vec<String> = event.ops.iter().map(|op| op.to_edn()).collect();
            let line = format!(
                "{{:type {}, :f :txn, :value [{}], :process {}, :index {}}}\n",
                event.event_type,
                ops_str.join(" "),
                event.process,
                event.index
            );
            file.write_all(line.as_bytes())?;
        }
        Ok(())
    }
}

impl Property for ElleHistoryRecorder {
    fn init_op(
        &mut self,
        _step: usize,
        fiber_id: usize,
        txn_id: Option<u64>,
        _exec_id: u64,
        op: &Operation,
    ) -> anyhow::Result<()> {
        match op {
            Operation::Begin { .. } => {
                // Reserve invoke index and start tracking ops
                let invoke_index = self.next_index();
                self.pending_txns.insert(
                    fiber_id,
                    PendingTxn {
                        invoke_index,
                        ops: Vec::new(),
                    },
                );
            }
            Operation::ElleAppend { key, value, .. } if txn_id.is_none() => {
                // Auto-commit: reserve invoke index
                let invoke_index = self.next_index();
                self.pending_auto_commits.insert(
                    fiber_id,
                    PendingAutoCommit {
                        invoke_index,
                        op: ElleOp::Append {
                            key: key.clone(),
                            value: *value,
                        },
                    },
                );
            }
            Operation::ElleRead { key, .. } if txn_id.is_none() => {
                // Auto-commit: reserve invoke index
                let invoke_index = self.next_index();
                self.pending_auto_commits.insert(
                    fiber_id,
                    PendingAutoCommit {
                        invoke_index,
                        op: ElleOp::Read {
                            key: key.clone(),
                            result: None, // Will be filled in finish_op
                        },
                    },
                );
            }
            _ => {}
        }
        Ok(())
    }

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
        match op {
            Operation::Begin { .. } => {
                // If Begin failed, clean up pending txn
                if result.is_err() {
                    self.pending_txns.remove(&fiber_id);
                }
            }
            Operation::Commit => {
                if let Some(pending) = self.pending_txns.remove(&fiber_id) {
                    if !pending.ops.is_empty() {
                        // Invoke with nil results for reads
                        let invoke_ops: Vec<ElleOp> = pending
                            .ops
                            .iter()
                            .map(|o| match o {
                                ElleOp::Read { key, .. } => ElleOp::Read {
                                    key: key.clone(),
                                    result: None,
                                },
                                other => other.clone(),
                            })
                            .collect();
                        self.add_event(
                            pending.invoke_index,
                            ElleEventType::Invoke,
                            fiber_id,
                            invoke_ops,
                        );

                        // Ok or Fail with new index
                        let completion_index = self.next_index();
                        if result.is_ok() {
                            self.add_event(
                                completion_index,
                                ElleEventType::Ok,
                                fiber_id,
                                pending.ops,
                            );
                        } else {
                            self.add_event(
                                completion_index,
                                ElleEventType::Fail,
                                fiber_id,
                                pending.ops,
                            );
                        }
                    }
                }
            }
            Operation::Rollback => {
                if let Some(pending) = self.pending_txns.remove(&fiber_id) {
                    if !pending.ops.is_empty() {
                        // Invoke with nil results for reads
                        let invoke_ops: Vec<ElleOp> = pending
                            .ops
                            .iter()
                            .map(|o| match o {
                                ElleOp::Read { key, .. } => ElleOp::Read {
                                    key: key.clone(),
                                    result: None,
                                },
                                other => other.clone(),
                            })
                            .collect();
                        self.add_event(
                            pending.invoke_index,
                            ElleEventType::Invoke,
                            fiber_id,
                            invoke_ops,
                        );

                        // Rollback always means fail
                        let completion_index = self.next_index();
                        self.add_event(
                            completion_index,
                            ElleEventType::Fail,
                            fiber_id,
                            pending.ops,
                        );
                    }
                }
            }
            Operation::ElleAppend { key, value, .. } => {
                if txn_id.is_some() {
                    // In a transaction - accumulate the op
                    if let Some(pending) = self.pending_txns.get_mut(&fiber_id) {
                        if result.is_ok() {
                            pending.ops.push(ElleOp::Append {
                                key: key.clone(),
                                value: *value,
                            });
                        }
                    }
                } else {
                    // Auto-commit: emit invoke and ok/fail
                    if let Some(pending) = self.pending_auto_commits.remove(&fiber_id) {
                        self.add_event(
                            pending.invoke_index,
                            ElleEventType::Invoke,
                            fiber_id,
                            vec![pending.op.clone()],
                        );

                        let completion_index = self.next_index();
                        if result.is_ok() {
                            self.add_event(
                                completion_index,
                                ElleEventType::Ok,
                                fiber_id,
                                vec![pending.op],
                            );
                        } else {
                            self.add_event(
                                completion_index,
                                ElleEventType::Fail,
                                fiber_id,
                                vec![pending.op],
                            );
                        }
                    }
                }
            }
            Operation::ElleRead { key, .. } => {
                if txn_id.is_some() {
                    // In a transaction - accumulate the op with result
                    if let Some(pending) = self.pending_txns.get_mut(&fiber_id) {
                        if result.is_ok() {
                            let read_result = parse_read_result(result);
                            pending.ops.push(ElleOp::Read {
                                key: key.clone(),
                                result: read_result,
                            });
                        }
                    }
                } else {
                    // Auto-commit: emit invoke and ok/fail
                    if let Some(pending) = self.pending_auto_commits.remove(&fiber_id) {
                        // Invoke has nil result
                        self.add_event(
                            pending.invoke_index,
                            ElleEventType::Invoke,
                            fiber_id,
                            vec![pending.op],
                        );

                        let completion_index = self.next_index();
                        if result.is_ok() {
                            let read_result = parse_read_result(result);
                            self.add_event(
                                completion_index,
                                ElleEventType::Ok,
                                fiber_id,
                                vec![ElleOp::Read {
                                    key: key.clone(),
                                    result: read_result,
                                }],
                            );
                        } else {
                            self.add_event(
                                completion_index,
                                ElleEventType::Fail,
                                fiber_id,
                                vec![ElleOp::Read {
                                    key: key.clone(),
                                    result: None,
                                }],
                            );
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn finalize(&mut self) -> anyhow::Result<()> {
        // Emit :info events for any pending transactions (incomplete)
        let pending_txns: Vec<_> = self.pending_txns.drain().collect();
        for (fiber_id, pending) in pending_txns {
            if !pending.ops.is_empty() {
                // Invoke with nil results for reads
                let invoke_ops: Vec<ElleOp> = pending
                    .ops
                    .iter()
                    .map(|o| match o {
                        ElleOp::Read { key, .. } => ElleOp::Read {
                            key: key.clone(),
                            result: None,
                        },
                        other => other.clone(),
                    })
                    .collect();
                self.add_event(
                    pending.invoke_index,
                    ElleEventType::Invoke,
                    fiber_id,
                    invoke_ops,
                );

                // Info for incomplete transaction
                let info_index = self.next_index();
                self.add_event(info_index, ElleEventType::Info, fiber_id, pending.ops);
            }
        }

        // Emit :info events for any pending auto-commit operations (incomplete)
        let pending_auto: Vec<_> = self.pending_auto_commits.drain().collect();
        for (fiber_id, pending) in pending_auto {
            self.add_event(
                pending.invoke_index,
                ElleEventType::Invoke,
                fiber_id,
                vec![pending.op.clone()],
            );
            let info_index = self.next_index();
            self.add_event(info_index, ElleEventType::Info, fiber_id, vec![pending.op]);
        }

        self.export()?;
        Ok(())
    }
}

/// Parse the read result from query rows.
fn parse_read_result(result: &OpResult) -> Option<Vec<i64>> {
    if let Ok(rows) = result {
        if rows.is_empty() {
            Some(vec![])
        } else if let Some(row) = rows.first() {
            if let Some(value) = row.first() {
                match value {
                    Value::Text(csv_str) => parse_comma_separated_ints(csv_str.as_str()),
                    Value::Null => Some(vec![]),
                    _ => Some(vec![]),
                }
            } else {
                Some(vec![])
            }
        } else {
            Some(vec![])
        }
    } else {
        None
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
