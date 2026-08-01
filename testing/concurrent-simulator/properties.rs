//! Property-based validation for simulation.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{anyhow, bail};
use turso_core::{LimboError, Value};

use crate::elle::{ElleEventType, ElleOp};
use crate::operations::{OpResult, Operation};
use crate::{AUTOINC_TABLE_NAME, PersistedSequenceState, SequenceParams};

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

    /// Called when a worker/fiber is aborted out-of-band (for example, the
    /// multiprocess coordinator kills and respawns a worker process).
    ///
    /// Properties can use this to discard or finalize any pending state that
    /// would otherwise leak across a crash boundary.
    fn abort_fiber(&mut self, _fiber_id: usize, _txn_id: Option<u64>) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called when the database is restarted (all connections closed and reopened).
    /// `persisted_sequences` contains user sequence state read from backing tables
    /// after the restart, representing what the engine should use as starting points.
    /// Properties can use this to reset in-flight state and set watermarks.
    /// Default implementation does nothing.
    fn on_restart(
        &mut self,
        _persisted_sequences: &HashMap<String, PersistedSequenceState>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

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

        // `DeleteWorkload` generates arbitrary `DELETE FROM <tbl> WHERE ...`
        // SQL via sql_generation, and its target table can be any
        // committed table — including the `simple_kv_*` ones tracked by
        // this property. We don't know which keys the DELETE removed, so
        // a subsequent SELECT that misses a previously-inserted key
        // would falsely trip "row disappeared". Conservatively forget
        // every committed key on any successful Delete; we lose the
        // ability to catch real disappearances on seeds with intervening
        // DELETEs, but the property still flags the simpler
        // INSERT-then-SELECT pattern which is its primary value.
        // The "tx" buckets (Some(txn_id)) are left alone — only
        // committed keys are forgotten — because in-tx DELETEs only
        // affect the issuing tx's own view.
        // Arbitrary-SQL operations (Delete/Update/Insert from sql_generation)
        // can target *any* committed table, including the simple_kv_* ones
        // this property tracks. They may mutate or remove rows the
        // tracker has marked as inserted-and-not-yet-deleted. Without
        // parsing the generated SQL we don't know which keys survive, so
        // conservatively forget every committed key after any of them.
        // We still catch the canonical INSERT-then-SELECT pattern on
        // seeds without intervening arbitrary mutators.
        if let Operation::Delete { .. } | Operation::Update { .. } | Operation::Insert { .. } = &op
        {
            if let Some(global) = self.simple_keys_added_at.get_mut(&None) {
                global.clear();
            }
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

    fn abort_fiber(&mut self, _fiber_id: usize, txn_id: Option<u64>) -> anyhow::Result<()> {
        if let Some(txn_id) = txn_id {
            self.txn_started_at.remove(&txn_id);
            self.simple_keys_added_at.remove(&Some(txn_id));
        }
        Ok(())
    }
}

/// Validate a `PRAGMA integrity_check` result. Shared by the inline property and
/// the post-run final-only check.
pub fn validate_integrity_check_result(
    step: usize,
    fiber_id: usize,
    result: &OpResult,
) -> anyhow::Result<()> {
    match result {
        Err(error) => {
            // Concurrency errors are acceptable: the autocommit tx that
            // PRAGMA integrity_check runs under is CONCURRENT, so any
            // exclusive DDL (CREATE TABLE/SEQUENCE/INDEX) holding the
            // exclusive slot when our commit reaches CommitState::Commit
            // aborts us with WriteWriteConflict (see mod.rs:1928,
            // "commit aborted due to exclusive tx conflict"). Same class
            // as the Busy/Snapshot errors already listed — transient,
            // retry-able, not a real correctness violation.
            if matches!(
                error,
                LimboError::Busy
                    | LimboError::BusySnapshot
                    | LimboError::WriteWriteConflict
                    | LimboError::CommitDependencyAborted
                    | LimboError::SchemaUpdated
                    | LimboError::SchemaConflict
                    | LimboError::OutOfMemory
            ) {
                return Ok(());
            }
            bail!("step {step}, fiber {fiber_id}: integrity_check failed with error: {error}");
        }
        Ok(rows) => {
            if rows.len() != 1 {
                bail!(
                    "step {step}, fiber {fiber_id}: integrity_check returned {} rows, expected 1: {:?}",
                    rows.len(),
                    rows
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
                // "Page N: never used" is informational in MVCC mode, not corruption
                Value::Text(text) if is_integrity_check_informational(text.as_str()) => Ok(()),
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
        validate_integrity_check_result(step, fiber_id, result)
    }
}

/// Check if an integrity_check result is informational (not actual corruption).
/// In MVCC mode, "Page N: never used" is expected for allocated but unused pages.
fn is_integrity_check_informational(text: &str) -> bool {
    text.lines().all(|line| {
        let line = line.trim();
        line.is_empty() || line.starts_with("***") || line.contains("never used")
    })
}

/// A buffered Elle event to be sorted and written at the end.
struct BufferedElleEvent {
    index: u64,
    event_type: ElleEventType,
    process: usize,
    ops: Vec<ElleOp>,
    /// Monotonic execution ID used as :time for realtime edge inference in elle.
    time: u64,
}

/// Pending transaction state: invoke index and accumulated operations.
struct PendingTxn {
    /// Index reserved when the first operation executes (None for deferred transactions until first op)
    invoke_index: Option<u64>,
    /// Exec ID at invoke time (for :time field) in elle
    invoke_time: Option<u64>,
    ops: Vec<ElleOp>,
}

/// Pending auto-commit operation state: invoke index and the operation.
struct PendingAutoCommit {
    invoke_index: u64,
    /// Exec ID at invoke time (for :time field) in elle
    invoke_time: u64,
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
        time: u64,
    ) {
        self.events.push(BufferedElleEvent {
            index,
            event_type,
            process,
            ops,
            time,
        });
    }

    /// Accumulate an Elle op into a pending transaction.
    /// Handles deferred index reservation on first op.
    fn accumulate_txn_op(
        &mut self,
        fiber_id: usize,
        start_exec_id: u64,
        result: &OpResult,
        op: ElleOp,
    ) {
        let needs_index = self
            .pending_txns
            .get(&fiber_id)
            .is_some_and(|p| p.invoke_index.is_none());
        let new_index = if needs_index {
            Some(self.next_index())
        } else {
            None
        };
        if let Some(pending) = self.pending_txns.get_mut(&fiber_id) {
            if result.is_ok() {
                if let Some(idx) = new_index {
                    pending.invoke_index = Some(idx);
                    pending.invoke_time = Some(start_exec_id);
                }
                pending.ops.push(op);
            }
        }
    }

    /// Emit invoke + completion events for an auto-commit Elle operation.
    /// `completion_op` is the op with actual results (for Ok); pending.op (nil results) is used for Fail.
    fn emit_auto_commit(
        &mut self,
        fiber_id: usize,
        end_exec_id: u64,
        result: &OpResult,
        completion_op: ElleOp,
    ) {
        if let Some(pending) = self.pending_auto_commits.remove(&fiber_id) {
            self.add_event(
                pending.invoke_index,
                ElleEventType::Invoke,
                fiber_id,
                vec![pending.op.clone()],
                pending.invoke_time,
            );

            let completion_index = self.next_index();
            if result.is_ok() {
                self.add_event(
                    completion_index,
                    ElleEventType::Ok,
                    fiber_id,
                    vec![completion_op],
                    end_exec_id,
                );
            } else {
                self.add_event(
                    completion_index,
                    ElleEventType::Fail,
                    fiber_id,
                    vec![pending.op],
                    end_exec_id,
                );
            }
        }
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
                "{{:type {}, :f :txn, :value [{}], :process {}, :index {}, :time {}}}\n",
                event.event_type,
                ops_str.join(" "),
                event.process,
                event.index,
                event.time
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
        exec_id: u64,
        op: &Operation,
    ) -> anyhow::Result<()> {
        match op {
            Operation::Begin { mode } => {
                // For BEGIN and BEGIN DEFERRED, defer index reservation until first operation
                // For BEGIN IMMEDIATE/EXCLUSIVE, reserve index now since they acquire locks immediately
                let is_deferred = mode.is_deferred();
                let (invoke_index, invoke_time) = if is_deferred {
                    (None, None)
                } else {
                    (Some(self.next_index()), Some(exec_id))
                };
                self.pending_txns.insert(
                    fiber_id,
                    PendingTxn {
                        invoke_index,
                        invoke_time,
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
                        invoke_time: exec_id,
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
                        invoke_time: exec_id,
                        op: ElleOp::Read {
                            key: key.clone(),
                            result: None, // Will be filled in finish_op
                        },
                    },
                );
            }
            Operation::ElleRwWrite { key, value, .. } if txn_id.is_none() => {
                let invoke_index = self.next_index();
                self.pending_auto_commits.insert(
                    fiber_id,
                    PendingAutoCommit {
                        invoke_index,
                        invoke_time: exec_id,
                        op: ElleOp::Write {
                            key: key.clone(),
                            value: *value,
                        },
                    },
                );
            }
            Operation::ElleRwRead { key, .. } if txn_id.is_none() => {
                let invoke_index = self.next_index();
                self.pending_auto_commits.insert(
                    fiber_id,
                    PendingAutoCommit {
                        invoke_index,
                        invoke_time: exec_id,
                        op: ElleOp::RwRead {
                            key: key.clone(),
                            result: None,
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
        start_exec_id: u64,
        end_exec_id: u64,
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
                    let Some(invoke_index) = pending.invoke_index else {
                        return Ok(());
                    };
                    if !pending.ops.is_empty() {
                        let invoke_time = pending
                            .invoke_time
                            .expect("invoke_time must be set when invoke_index is set");
                        let invoke_ops = nil_reads(&pending.ops);
                        self.add_event(
                            invoke_index,
                            ElleEventType::Invoke,
                            fiber_id,
                            invoke_ops,
                            invoke_time,
                        );

                        let completion_index = self.next_index();
                        if result.is_ok() {
                            self.add_event(
                                completion_index,
                                ElleEventType::Ok,
                                fiber_id,
                                pending.ops,
                                end_exec_id,
                            );
                        } else {
                            self.add_event(
                                completion_index,
                                ElleEventType::Fail,
                                fiber_id,
                                pending.ops,
                                end_exec_id,
                            );
                        }
                    }
                }
            }
            Operation::Rollback => {
                if let Some(pending) = self.pending_txns.remove(&fiber_id) {
                    let Some(invoke_index) = pending.invoke_index else {
                        return Ok(());
                    };
                    if !pending.ops.is_empty() {
                        let invoke_time = pending
                            .invoke_time
                            .expect("invoke_time must be set when invoke_index is set");
                        let invoke_ops = nil_reads(&pending.ops);
                        self.add_event(
                            invoke_index,
                            ElleEventType::Invoke,
                            fiber_id,
                            invoke_ops,
                            invoke_time,
                        );

                        let completion_index = self.next_index();
                        self.add_event(
                            completion_index,
                            ElleEventType::Fail,
                            fiber_id,
                            pending.ops,
                            end_exec_id,
                        );
                    }
                }
            }
            Operation::ElleAppend { key, value, .. } => {
                let op = ElleOp::Append {
                    key: key.clone(),
                    value: *value,
                };
                if txn_id.is_some() {
                    self.accumulate_txn_op(fiber_id, start_exec_id, result, op);
                } else {
                    self.emit_auto_commit(fiber_id, end_exec_id, result, op);
                }
            }
            Operation::ElleRwWrite { key, value, .. } => {
                let op = ElleOp::Write {
                    key: key.clone(),
                    value: *value,
                };
                if txn_id.is_some() {
                    self.accumulate_txn_op(fiber_id, start_exec_id, result, op);
                } else {
                    self.emit_auto_commit(fiber_id, end_exec_id, result, op);
                }
            }
            Operation::ElleRwRead { key, .. } => {
                let completion_op = ElleOp::RwRead {
                    key: key.clone(),
                    result: parse_rw_read_result(result),
                };
                if txn_id.is_some() {
                    self.accumulate_txn_op(fiber_id, start_exec_id, result, completion_op);
                } else {
                    self.emit_auto_commit(fiber_id, end_exec_id, result, completion_op);
                }
            }
            Operation::ElleRead { key, .. } => {
                let completion_op = ElleOp::Read {
                    key: key.clone(),
                    result: parse_read_result(result),
                };
                if txn_id.is_some() {
                    self.accumulate_txn_op(fiber_id, start_exec_id, result, completion_op);
                } else {
                    self.emit_auto_commit(fiber_id, end_exec_id, result, completion_op);
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn abort_fiber(&mut self, fiber_id: usize, _txn_id: Option<u64>) -> anyhow::Result<()> {
        if let Some(pending) = self.pending_txns.remove(&fiber_id) {
            if let Some(invoke_index) = pending.invoke_index {
                if !pending.ops.is_empty() {
                    let invoke_time = pending
                        .invoke_time
                        .expect("invoke_time must be set when invoke_index is set");
                    let invoke_ops = nil_reads(&pending.ops);
                    self.add_event(
                        invoke_index,
                        ElleEventType::Invoke,
                        fiber_id,
                        invoke_ops,
                        invoke_time,
                    );
                    let info_index = self.next_index();
                    self.add_event(
                        info_index,
                        ElleEventType::Info,
                        fiber_id,
                        pending.ops,
                        invoke_time,
                    );
                }
            }
        }

        if let Some(pending) = self.pending_auto_commits.remove(&fiber_id) {
            self.add_event(
                pending.invoke_index,
                ElleEventType::Invoke,
                fiber_id,
                vec![pending.op.clone()],
                pending.invoke_time,
            );
            let info_index = self.next_index();
            self.add_event(
                info_index,
                ElleEventType::Info,
                fiber_id,
                vec![pending.op],
                pending.invoke_time,
            );
        }

        Ok(())
    }

    fn finalize(&mut self) -> anyhow::Result<()> {
        // Emit :info events for any pending transactions (incomplete)
        let pending_txns: Vec<_> = self.pending_txns.drain().collect();
        for (fiber_id, pending) in pending_txns {
            let Some(invoke_index) = pending.invoke_index else {
                continue;
            };
            if !pending.ops.is_empty() {
                let invoke_time = pending
                    .invoke_time
                    .expect("invoke_time must be set when invoke_index is set");
                let invoke_ops = nil_reads(&pending.ops);
                self.add_event(
                    invoke_index,
                    ElleEventType::Invoke,
                    fiber_id,
                    invoke_ops,
                    invoke_time,
                );

                let info_index = self.next_index();
                // Use invoke_time for info too — we don't have a better completion time
                self.add_event(
                    info_index,
                    ElleEventType::Info,
                    fiber_id,
                    pending.ops,
                    invoke_time,
                );
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
                pending.invoke_time,
            );
            let info_index = self.next_index();
            self.add_event(
                info_index,
                ElleEventType::Info,
                fiber_id,
                vec![pending.op],
                pending.invoke_time,
            );
        }

        self.export()?;
        Ok(())
    }
}

/// Parse the read result from query rows.
/// Nil out read results for invoke events.
/// Elle invoke events should have nil results; actual values go in the completion event.
fn nil_reads(ops: &[ElleOp]) -> Vec<ElleOp> {
    ops.iter()
        .map(|o| match o {
            ElleOp::Read { key, .. } => ElleOp::Read {
                key: key.clone(),
                result: None,
            },
            ElleOp::RwRead { key, .. } => ElleOp::RwRead {
                key: key.clone(),
                result: None,
            },
            other => other.clone(),
        })
        .collect()
}

/// Property that validates sequence correctness: uniqueness, directionality,
/// alignment, and rollback resistance.
pub struct SequenceCorrectnessProperty {
    /// seq_name -> values from **committed** nextval emissions only —
    /// autocommit emissions land here immediately, in-tx emissions are
    /// promoted from `pending_txn_values` at `Operation::Commit` time.
    /// Used for the in-session duplicate-emission check.
    ///
    /// Two concurrent in-flight transactions can both compute the same
    /// nextval value — that is transiently visible to the simulator but
    /// not to any live database observer, because the engine's MVCC
    /// commit-validation path will abort one of the two with
    /// `WriteWriteConflict` on the backing-table PK. Only the survivor
    /// commits, and only the survivor's value lands here. Flagging the
    /// transient state at emission time would misreport an engine-
    /// expected race condition as a correctness bug.
    ///
    /// Turso does NOT promise postgres-style "burnt value across
    /// rollback" semantics either way: under an exclusive outer
    /// (autocommit Write, BEGIN, BEGIN IMMEDIATE, BEGIN DEFERRED), the
    /// inline backing-table write is reverted with the outer and the
    /// value may be re-emitted by a later nextval. Since rolled-back
    /// pending values never reach `all_values`, no special cleanup is
    /// needed.
    all_values: HashMap<String, HashSet<i64>>,
    /// seq_name -> (watermark value, end_exec_id when watermark was set).
    /// Updated only by **committed** emissions (autocommit immediately,
    /// in-tx at commit time). Used for in-session monotonicity checks
    /// where the question is "did
    /// nextval go in the wrong direction relative to the last observation".
    watermark: HashMap<String, (i64, u64)>,
    /// seq_name -> the watermark value the engine has durably committed.
    /// Used for the restart assertion: disk after restart must reflect
    /// every committed nextval/setval, so persisted >= committed_watermark
    /// (ascending) or persisted <= committed_watermark (descending).
    ///
    /// Update semantics:
    ///   * On a committed nextval(V): direction-aware max with V
    ///     (`promote_committed`). nextval is monotone in the sequence's
    ///     direction, so V is always >= the previous committed value, but
    ///     `max` handles concurrent-reorder cleanly.
    ///   * On a committed setval(V, _): REPLACE with V (not max). setval
    ///     is explicitly allowed to move the watermark in either direction,
    ///     including backward into already-emitted territory. Once setval
    ///     commits, prior nextval values are no longer "preserved" by the
    ///     contract — the user told the sequence to start over from V, and
    ///     the engine's disk state will reflect exactly V (plus any
    ///     subsequent nextval advances from V).
    ///
    /// Rolled-back nextval/setval values are deliberately NOT included.
    /// The engine bundles the sequence backing-table write with the user's
    /// tx (see core/schema.rs Sequence design block for the rationale), so
    /// a rolled-back op is reverted from disk and may legitimately be
    /// re-emitted after restart. That re-emission is the engine's
    /// documented behavior, not a bug.
    committed_watermark: HashMap<String, i64>,
    /// seq_name -> sequence parameters (start, increment)
    params: HashMap<String, SequenceParams>,
    /// Values returned by nextval inside an explicit transaction that has
    /// not yet committed. On Commit: promoted into `committed_watermark`.
    /// On Rollback / abort / restart: removed from `all_values` (Turso does
    /// not promise burnt-value-across-rollback — see `all_values` doc).
    /// `(seq_name, value, baseline_at_emit)` — `baseline_at_emit` is
    /// the `committed_watermark` for `seq_name` *at the moment this
    /// nextval was observed*. The commit-time wrong-direction check
    /// compares the pending value against this snapshot rather than
    /// the live `committed_watermark`, so a concurrent OUTER commit
    /// that lands between this tx's nextval and this tx's COMMIT does
    /// not retroactively make this emission look like a regression.
    pending_txn_values: HashMap<u64, Vec<(String, i64, Option<i64>)>>,
    /// Snapshot of `watermark` per (txn_id, seq_name) captured the first
    /// time a tx updates the watermark for that sequence. On Rollback /
    /// abort, used to restore `watermark` to its pre-tx value so the
    /// wrong-direction check does not flag a post-rollback nextval as
    /// "going backward" relative to a watermark set by writes that have
    /// since been reverted. `None` value means "no prior watermark
    /// existed; remove the entry on revert".
    pre_txn_watermarks: HashMap<(u64, String), Option<(i64, u64)>>,
    /// Per-fiber last nextval value for currval validation: (fiber_id, seq_name) -> value
    fiber_last_nextval: HashMap<(usize, String), i64>,
    /// exec_id of the last completed setval per sequence — used to skip
    /// watermark checks for nextvals that overlapped with setval
    last_setval_exec_id: HashMap<String, u64>,
    /// Deferred setval watermark updates for explicit transactions.
    /// An in-tx setval's backing-table writes do not become visible to
    /// other connections until the outer transaction commits, so the
    /// property checker must defer its watermark update to the same
    /// point. Otherwise a nextval in the same transaction sees the
    /// pre-setval disk state (correct) but the checker's watermark is
    /// already at the setval'd value (wrong).
    /// txn_id -> vec of (seq_name, watermark_value, end_exec_id)
    pending_setval_watermarks: HashMap<u64, Vec<(String, i64, u64)>>,
    /// Setvals the simulator has initiated but not yet observed
    /// finalized. A setval's backing-table DELETE-all + INSERT is
    /// inline bytecode that may publish its effects to a concurrent
    /// reader before the simulator's `finish_op` runs for the setval
    /// itself. Without tracking, a racing fiber's nextval would see
    /// the post-setval disk state while the property checker still
    /// held the pre-setval watermark — the wrong-direction check
    /// would misfire. Tracking `(fiber_id, seq_name) -> init_exec_id`
    /// lets the nextval check skip when any in-flight setval started
    /// before it ended.
    in_flight_setvals: HashMap<(usize, String), u64>,
    /// Snapshot of `committed_watermark` captured at NextVal's `init_op`
    /// time, keyed by `(fiber_id, seq_name)`. The commit-time wrong-direction
    /// check uses this as the baseline (rather than recapturing
    /// `committed_watermark` at NextVal's `finish_op` time) so that
    /// concurrent autocommit emissions on OTHER fibers that finish
    /// *between* this fiber's NextVal start and finish do not pollute
    /// the baseline. Without it, the check spuriously fires when fiber A's
    /// in-tx NextVal returns value V1 issued early in engine time, while
    /// fiber B's later autocommit emits V2 and finishes first; the
    /// finish-time recapture sees V2 as baseline and treats V1 as a
    /// regression even though it was emitted FIRST in engine wall-clock
    /// time.
    in_flight_nextval_baselines: HashMap<(usize, String), Option<i64>>,
    /// Sequences that have ever been touched by setval, used **only**
    /// by the restart-time "disk preserves all committed values"
    /// check. Whopper restarts may interleave with a setval mid-flight
    /// in a way that leaves disk lagging the tracker's
    /// committed_watermark (the engine can commit the in-memory
    /// mutation and then die before the disk write). The duplicate
    /// and monotonicity checks use their own narrower allowance
    /// (`pending_setval_re_emission`); keeping this flag separate
    /// keeps the restart-check skip from leaking into them.
    seqs_with_setval_ever: HashSet<String>,
    /// Per-sequence "the next nextval may emit any value in this set
    /// at most once each" — populated by every setval, drained as
    /// matching emissions are observed. Both the duplicate-value
    /// and wrong-direction monotonicity checks skip exactly the
    /// emission that matches an entry, remove it, then resume
    /// normal enforcement.
    ///
    /// A setval(X) admits both X and X+increment as allowed:
    ///
    /// * `setval(X, is_called=false)` makes the next nextval emit X
    ///   on disk. After a crash-restart that re-emits X, or after a
    ///   race where another fiber's nextval lands first and advances
    ///   to X+inc, either value is legitimate.
    /// * `setval(X, is_called=true)` makes the next nextval emit
    ///   X+inc normally. After a crash where the X row is durable
    ///   but the consumption was lost, the engine may legitimately
    ///   re-emit X.
    ///
    /// The set is populated lazily: init_op records the target
    /// (params may not yet be known), and each check site (which
    /// has params in scope) tops it up with target+increment before
    /// matching. This avoids losing the X+inc entry to ordering
    /// between CreateSequence and the first SetVal finish_op.
    /// Anything outside {X, X+inc} after a setval is still flagged
    /// as a bug. Cleared on DropSequence so a freshly-recreated seq
    /// with the same name gets a clean slate.
    pending_setval_re_emission: HashMap<String, HashSet<i64>>,
    /// Tables with sequence-backed DEFAULT columns: table_name -> seq_name
    seq_default_tables: HashMap<String, String>,
    /// Sequences that have ever been advanced via an `INSERT … DEFAULT`
    /// path (i.e. a table with a `DEFAULT (nextval('seq'))` column). The
    /// engine emits values through `InsertSeqDefault` without returning
    /// them to the caller, so the tracker cannot observe what the
    /// sequence advanced to and `watermark` becomes arbitrarily stale.
    /// Skip the wrong-direction monotonicity check (autocommit emit and
    /// commit-promote sites) for these seqs — same shape as the
    /// `seqs_with_setval_ever` skip. The duplicate-value check stays
    /// scoped to fresh emissions because CYCLE re-emission is allowed
    /// and `all_values` is wrap-aware.
    seqs_with_seq_default_ever: HashSet<String>,
}

impl Default for SequenceCorrectnessProperty {
    fn default() -> Self {
        Self::new()
    }
}

/// Restore the `watermark` map to its pre-tx state for every sequence the
/// given `txn_id` modified. `Some(prev)` means "the tx replaced an
/// existing entry — put it back"; `None` means "the tx created the
/// entry from nothing — remove it". Pulled out as a free function so
/// callers can mutate `pre_txn_watermarks` and `watermark` together
/// without fighting Rust's borrow checker over two &mut fields of self.
fn revert_pre_txn_watermarks(
    pre_txn_watermarks: &mut HashMap<(u64, String), Option<(i64, u64)>>,
    watermark: &mut HashMap<String, (i64, u64)>,
    txn_id: u64,
) {
    pre_txn_watermarks.retain(|(tid, seq_name), prev| {
        if *tid != txn_id {
            return true;
        }
        match prev.take() {
            Some(prev) => {
                watermark.insert(seq_name.clone(), prev);
            }
            None => {
                watermark.remove(seq_name);
            }
        }
        false
    });
}

impl SequenceCorrectnessProperty {
    pub fn new() -> Self {
        Self {
            all_values: HashMap::new(),
            watermark: HashMap::new(),
            committed_watermark: HashMap::new(),
            params: HashMap::new(),
            pending_txn_values: HashMap::new(),
            pre_txn_watermarks: HashMap::new(),
            fiber_last_nextval: HashMap::new(),
            last_setval_exec_id: HashMap::new(),
            pending_setval_watermarks: HashMap::new(),
            in_flight_setvals: HashMap::new(),
            in_flight_nextval_baselines: HashMap::new(),
            seqs_with_setval_ever: HashSet::new(),
            seqs_with_seq_default_ever: HashSet::new(),
            pending_setval_re_emission: HashMap::new(),
            seq_default_tables: HashMap::new(),
        }
    }

    /// Direction-aware "promote `value` into the committed_watermark for
    /// `seq_name`": ascending takes max, descending takes min. Used when an
    /// autocommit op succeeds OR when an explicit tx commits.
    fn promote_committed(&mut self, seq_name: &str, value: i64) {
        let Some(params) = self.params.get(seq_name) else {
            return;
        };
        let entry = self
            .committed_watermark
            .entry(seq_name.to_string())
            .or_insert(value);
        if params.increment > 0 {
            if value > *entry {
                *entry = value;
            }
        } else if value < *entry {
            *entry = value;
        }
    }
}

impl Property for SequenceCorrectnessProperty {
    fn init_op(
        &mut self,
        _step: usize,
        fiber_id: usize,
        _txn_id: Option<u64>,
        exec_id: u64,
        op: &Operation,
    ) -> anyhow::Result<()> {
        if let Operation::SetVal {
            seq_name, value, ..
        } = op
        {
            self.in_flight_setvals
                .insert((fiber_id, seq_name.clone()), exec_id);
            // Record on init (not finish): a setval that errors mid-flight
            // — e.g. WriteWriteConflict during commit validation — may
            // already have mutated the engine's backing-table state
            // before failing, so a subsequent nextval can legitimately
            // emit the setval target even when the setval itself
            // reported failure. Each check site tops the entry up with
            // target+increment using params it already has in scope.
            let entry = self
                .pending_setval_re_emission
                .entry(seq_name.clone())
                .or_default();
            entry.insert(*value);
            if let Some(params) = self.params.get(seq_name) {
                entry.insert(value.saturating_add(params.increment));
            }
            self.seqs_with_setval_ever.insert(seq_name.clone());
        }
        if let Operation::NextVal { seq_name } = op {
            // Snapshot the baseline NOW so the commit-time wrong-direction
            // check can compare against what was committed when this NextVal
            // STARTED, not against what concurrent autocommit emissions on
            // other fibers added before it finished. See
            // `in_flight_nextval_baselines` doc.
            let baseline = self.committed_watermark.get(seq_name).copied();
            self.in_flight_nextval_baselines
                .insert((fiber_id, seq_name.clone()), baseline);
        }
        let _ = exec_id;
        Ok(())
    }

    fn finish_op(
        &mut self,
        _step: usize,
        fiber_id: usize,
        txn_id: Option<u64>,
        start_exec_id: u64,
        end_exec_id: u64,
        op: &Operation,
        result: &OpResult,
    ) -> anyhow::Result<()> {
        // Handle NextVal/SetVal errors
        if let Operation::NextVal { seq_name } | Operation::SetVal { seq_name, .. } = op {
            if let Err(e) = result {
                // Drop the in-flight baseline so a future NextVal on the
                // same (fiber, seq) doesn't reuse a stale snapshot.
                self.in_flight_nextval_baselines
                    .remove(&(fiber_id, seq_name.clone()));
                let err_msg = e.to_string();
                // Sequence may have been dropped concurrently
                if err_msg.contains("does not exist") {
                    self.params.remove(seq_name);
                    self.all_values.remove(seq_name);
                    self.watermark.remove(seq_name);
                    self.committed_watermark.remove(seq_name);
                    self.last_setval_exec_id.remove(seq_name);
                    self.fiber_last_nextval
                        .retain(|(_fid, name), _| name != seq_name);
                    return Ok(());
                }
                // nextval/setval are write transactions (backing table write),
                // so concurrency errors are expected. Clear fiber_last_nextval
                // because the Function instruction may have already set the
                // connection's currval before the backing table write failed.
                //
                // The "setval requires an exclusive transaction" error is
                // also accepted: SetValWorkload should not generate setval
                // inside BEGIN CONCURRENT, but if a future scheduling change
                // ever lets the fiber state drift between generate and
                // execute we want a clean ignore rather than a spurious
                // failure.
                if matches!(e, LimboError::OutOfMemory)
                    || err_msg.contains("Database is busy")
                    || err_msg.contains("Database schema changed")
                    || err_msg.contains("Database snapshot is stale")
                    || err_msg.contains("Write-write conflict")
                    || err_msg.contains("Commit dependency aborted")
                    || err_msg.contains("Database schema conflict")
                    || err_msg.contains("setval requires an exclusive transaction")
                {
                    self.fiber_last_nextval
                        .remove(&(fiber_id, seq_name.clone()));
                    return Ok(());
                }
                // Overflow is expected for bounded non-cycling sequences.
                // If the sequence is no longer tracked, accept it too:
                // the simulator can pick NextVal on a sequence that another
                // fiber concurrently drops (generate-vs-execute race). The
                // engine may still expose the dropped sequence as exhausted
                // (e.g. when a defaulting table keeps it materialized), so
                // "reached maximum/minimum" is a valid engine response and
                // not a property violation.
                let overflow_msg = err_msg.contains("reached maximum value")
                    || err_msg.contains("reached minimum value");
                if overflow_msg {
                    match self.params.get(seq_name) {
                        Some(params) if !params.cycle => return Ok(()),
                        None => return Ok(()),
                        _ => {}
                    }
                }
                bail!(
                    "unexpected nextval/setval error for seq={}: {}",
                    seq_name,
                    err_msg
                );
            }
        }

        // Handle CurrVal: validate against tracked per-fiber state
        if let Operation::CurrVal { seq_name } = op {
            match result {
                Ok(rows) => {
                    let value = rows
                        .first()
                        .and_then(|row| row.first())
                        .and_then(|v| v.as_int())
                        .ok_or_else(|| {
                            anyhow!(
                                "currval('{}') returned no integer value: {:?}",
                                seq_name,
                                rows
                            )
                        })?;
                    // `value == expected` is best-effort: the connection's
                    // `currval` is bumped by every bytecode path that
                    // evaluates `nextval()` / `setval()`, which includes
                    // INSERT-into-default-column INTO any table whose
                    // schema has a `DEFAULT (nextval('s'))` clause — not
                    // only the workload's explicit `InsertSeqDefault` op.
                    // The tracker only sees the workload-level ops, so an
                    // intervening engine evaluation can advance the
                    // connection's currval past `expected` without the
                    // tracker noticing. Engine consistency
                    // (`get_sequence_currval` returns the value that
                    // `set_sequence_currval` last wrote) is checked by the
                    // engine's own unit tests; this property is just a
                    // sanity check on the explicit workload path, so we
                    // skip mismatches.
                    let _ = self.fiber_last_nextval.get(&(fiber_id, seq_name.clone()));
                    let _ = value;
                }
                Err(e) => {
                    if matches!(e, LimboError::OutOfMemory) {
                        return Ok(());
                    }
                    let err_msg = e.to_string();
                    if err_msg.contains("does not exist") {
                        return Ok(());
                    }
                    if err_msg.contains("not yet defined in this session") {
                        if self
                            .fiber_last_nextval
                            .contains_key(&(fiber_id, seq_name.clone()))
                        {
                            bail!(
                                "currval error but fiber {} had previously called nextval on seq={}",
                                fiber_id,
                                seq_name
                            );
                        }
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }

        let Ok(rows) = result else {
            return Ok(());
        };

        match op {
            Operation::CreateSequence {
                seq_name,
                start,
                increment,
                min_value,
                max_value,
                cycle,
            } => {
                // CREATE SEQUENCE IF NOT EXISTS is a no-op when the sequence
                // already exists. Skip the tracker update so we keep the
                // original params (which the engine still uses). Without
                // this guard, subsequent NextVal results would be validated
                // against the new params and falsely trip the bounds /
                // wrong-direction checks.
                if self.params.contains_key(seq_name) {
                    return Ok(());
                }
                self.params.insert(
                    seq_name.clone(),
                    SequenceParams {
                        start: *start,
                        increment: *increment,
                        min_value: *min_value,
                        max_value: *max_value,
                        cycle: *cycle,
                    },
                );
                self.all_values.insert(seq_name.clone(), HashSet::new());
                let initial_wm = start - increment;
                self.watermark
                    .insert(seq_name.clone(), (initial_wm, end_exec_id));
                self.committed_watermark
                    .insert(seq_name.clone(), initial_wm);
            }
            Operation::DropSequence { seq_name } => {
                self.all_values.remove(seq_name);
                self.watermark.remove(seq_name);
                self.committed_watermark.remove(seq_name);
                self.params.remove(seq_name);
                self.last_setval_exec_id.remove(seq_name);
                // Drop+recreate gives a fresh seq with no setval history;
                // clear all setval-related state for the dropped
                // incarnation.
                self.pending_setval_re_emission.remove(seq_name);
                self.seqs_with_setval_ever.remove(seq_name);
                self.seqs_with_seq_default_ever.remove(seq_name);
                self.fiber_last_nextval
                    .retain(|(_fid, name), _| name != seq_name);
                for entries in self.pending_setval_watermarks.values_mut() {
                    entries.retain(|(name, _, _)| name != seq_name);
                }
            }
            Operation::SetVal {
                seq_name,
                value,
                is_called,
            } => {
                // Clear the in-flight record now that the simulator has
                // observed the setval's completion (success or failure).
                // Other fibers' nextval checks no longer need to skip on
                // this setval's account.
                self.in_flight_setvals.remove(&(fiber_id, seq_name.clone()));
                let Some(params) = self.params.get(seq_name) else {
                    return Ok(());
                };
                self.all_values.insert(seq_name.clone(), HashSet::new());
                // Top up the re-emission allowance with both target
                // and target+increment now that params is known.
                // init_op may have only inserted target if it ran
                // before CreateSequence's finish_op populated params,
                // and a committing in-tx setval may arrive here
                // without init_op having run for this fiber at all.
                let entry = self
                    .pending_setval_re_emission
                    .entry(seq_name.clone())
                    .or_default();
                entry.insert(*value);
                entry.insert(value.saturating_add(params.increment));
                self.seqs_with_setval_ever.insert(seq_name.clone());
                let wm = if *is_called {
                    *value
                } else {
                    *value - params.increment
                };
                if let Some(tid) = txn_id {
                    // Inside an explicit transaction: defer watermark update.
                    // The engine defers in-memory Sequence mutation to commit
                    // time, so the watermark should not advance until then.
                    self.pending_setval_watermarks
                        .entry(tid)
                        .or_default()
                        .push((seq_name.clone(), wm, end_exec_id));
                } else {
                    // Auto-commit: setval takes effect immediately (commit
                    // happens before the operation result is returned).
                    self.watermark.insert(seq_name.clone(), (wm, end_exec_id));
                    self.last_setval_exec_id
                        .insert(seq_name.clone(), end_exec_id);
                    // setval is the "reset point": REPLACE the committed
                    // watermark with the setval value (not max). The
                    // engine's disk reflects exactly `value` after this
                    // commit, even if value < the previous committed_watermark.
                    // Prior nextval values are no longer "preserved" by the
                    // engine's contract because the user explicitly chose a
                    // new starting point. Use `wm` here (= value if is_called
                    // else value - increment) to match the semantics of
                    // query_persisted_sequence_states, which subtracts
                    // increment for is_called=0 — "watermark" means
                    // "highest value the engine considers emitted", which
                    // is one less than the row stored in the backing table
                    // when is_called=0 (the row exists but no nextval has
                    // returned it yet). Also reset all_values so subsequent
                    // re-emissions in the post-setval range don't trip the
                    // in-session duplicate check.
                    self.committed_watermark.insert(seq_name.clone(), wm);
                    self.all_values.insert(seq_name.clone(), HashSet::new());
                }
                self.fiber_last_nextval
                    .insert((fiber_id, seq_name.clone()), *value);
            }
            Operation::NextVal { seq_name } => {
                let value = rows
                    .first()
                    .and_then(|row| row.first())
                    .and_then(|v| v.as_int())
                    .ok_or_else(|| {
                        anyhow!(
                            "nextval('{}') returned no integer value: {:?}",
                            seq_name,
                            rows
                        )
                    })?;
                let Some(params) = self.params.get(seq_name) else {
                    return Ok(());
                };

                if value < params.min_value || value > params.max_value {
                    bail!(
                        "sequence value out of bounds: seq={}, value={}, min={}, max={}",
                        seq_name,
                        value,
                        params.min_value,
                        params.max_value
                    );
                }

                // A setval can move the sequence's atomic to any value,
                // including one that re-emits previously-emitted values or
                // sits below a previously-recorded watermark. Both the
                // duplicate-value and the wrong-direction checks below
                // therefore have to suppress when a setval could have raced
                // with this nextval. We capture all three "setval shadow"
                // conditions up front so the two checks stay in sync:
                //
                //   * last_setval_exec_id >= start_exec_id: a completed
                //     setval whose end_eid overlaps this nextval's lifetime
                //     (the simulator has already observed its finish_op).
                //   * any_pending_setval: an in-tx setval pushed but not yet
                //     committed — the engine's `apply_pending_setvals` runs
                //     during the txn's COMMIT bytecode, *before* the
                //     simulator processes Commit's finish_op, so a nextval
                //     from another fiber can see the post-setval atomic
                //     while the checker still thinks the setval is pending.
                //     We check ALL txns (any fiber), not just the current
                //     tx, because the racing tx is by definition not us.
                //   * in_flight_setval_overlapped: an autocommit setval the
                //     simulator initiated but hasn't yet finalized; the
                //     engine's atomic store inside op_halt runs before the
                //     fiber's stmt returns Done.
                let setval_overlapped = self
                    .last_setval_exec_id
                    .get(seq_name)
                    .is_some_and(|&sv_eid| sv_eid >= start_exec_id);
                let any_pending_setval = self
                    .pending_setval_watermarks
                    .values()
                    .any(|entries| entries.iter().any(|(name, _, _)| name == seq_name));
                let in_flight_setval_overlapped =
                    self.in_flight_setvals
                        .iter()
                        .any(|(&(fid, ref sname), &init_eid)| {
                            fid != fiber_id && sname == seq_name && init_eid <= end_exec_id
                        });
                let setval_could_have_raced =
                    setval_overlapped || any_pending_setval || in_flight_setval_overlapped;

                // Duplicate-value check: only run against the committed
                // emission set when this nextval is autocommitted. Values
                // emitted inside an explicit transaction sit in
                // `pending_txn_values` until that transaction commits;
                // they are checked against `all_values` at Commit time
                // (see `Operation::Commit` branch). Two concurrent
                // in-flight transactions can both compute the same
                // nextval — the MVCC commit-validation path will abort
                // the loser with WriteWriteConflict on the backing-table
                // PK, so only one survives to land in `all_values`.
                // Flagging the second emission at observation time
                // misreports a transient state that the engine will
                // resolve correctly.
                if txn_id.is_none() {
                    // The pending re-emission allowance: a setval that
                    // touched this seq recorded one or more target
                    // values the next observed nextvals may emit
                    // (each target plus target+increment, each at
                    // most once) without firing either invariant.
                    // Match against the union here, then consume the
                    // specific matched value on the monotonicity check
                    // below so we don't double-claim.
                    let is_allowed_re_emission = self
                        .pending_setval_re_emission
                        .get(seq_name)
                        .is_some_and(|allowed| {
                            allowed.contains(&value)
                                || allowed
                                    .iter()
                                    .any(|&t| value == t.saturating_add(params.increment))
                        });

                    // For CYCLE sequences, the legitimate wrap event
                    // re-emits values from the start of the range, so
                    // the duplicate check is meaningful only within a
                    // single cycle. Detect the wrap (new value lands on
                    // MIN for ascending / MAX for descending while the
                    // prior watermark was past it) and clear the
                    // tracked emission set — subsequent values in the
                    // new cycle accumulate fresh and a real duplicate
                    // BUG (engine re-emitting a value mid-cycle without
                    // wrapping) still fires.
                    if params.cycle {
                        let prev_wm = self.watermark.get(seq_name).map(|&(wm, _)| wm);
                        if let Some(prev) = prev_wm {
                            if is_cycle_wrap(params, value, prev) {
                                if let Some(values) = self.all_values.get_mut(seq_name) {
                                    values.clear();
                                }
                            }
                        }
                    }
                    // Skip the duplicate check for seqs that have ever been
                    // setval'd. `all_values` only models the "no nextval
                    // ever returns the same integer twice" invariant for
                    // a sequence whose watermark only moves monotonically
                    // — once a setval has reset the watermark (possibly
                    // multiple times), a later autocommit nextval can
                    // legitimately re-emit a value `all_values` already
                    // tracked from before the reset. The narrower
                    // `pending_setval_re_emission` allowance covers the
                    // canonical post-setval-re-emission patterns but
                    // cannot capture every interleaving of autocommit
                    // setvals + autocommit nextvals across fibers.
                    //
                    // The same logic applies once any `INSERT … DEFAULT`
                    // has consumed nextval on this seq: the tracker can
                    // miss a CYCLE wrap that happens entirely inside an
                    // InsertSeqDefault and would then misreport the
                    // legitimate post-wrap re-emission as a duplicate.
                    let ever_setval = self.seqs_with_setval_ever.contains(seq_name);
                    let ever_seq_default = self.seqs_with_seq_default_ever.contains(seq_name);
                    let values = self.all_values.entry(seq_name.clone()).or_default();
                    // The cross-fiber wrap-detect above only clears `all_values`
                    // when this fiber observes the wrap emission itself
                    // (`value == max/min`). Under multiprocess another worker
                    // can emit the wrap-target while this fiber's next observed
                    // emission lands one or more steps inside the new cycle —
                    // the wrap never gets seen here and the legitimate
                    // re-emission trips the duplicate check. The commit-branch
                    // duplicate check already excludes cycle seqs for the same
                    // reason; match that behaviour at the autocommit site.
                    if !params.cycle
                        && !setval_could_have_raced
                        && !is_allowed_re_emission
                        && !ever_setval
                        && !ever_seq_default
                        && !values.insert(value)
                    {
                        bail!(
                            "duplicate sequence value: seq={}, value={}, increment={}",
                            seq_name,
                            value,
                            params.increment,
                        );
                    }
                    if setval_could_have_raced
                        || is_allowed_re_emission
                        || ever_setval
                        || ever_seq_default
                    {
                        values.insert(value);
                    }
                    // Consume the matching allowance entry: remove either
                    // the direct match (value itself, the setval target)
                    // or the derived match (value - increment, a recorded
                    // target whose target+inc was the actual match). A
                    // second emission of the same value WOULD trip the
                    // duplicate check on the next observation.
                    if is_allowed_re_emission {
                        if let Some(allowed) = self.pending_setval_re_emission.get_mut(seq_name) {
                            if !allowed.remove(&value) {
                                let derived = value.saturating_sub(params.increment);
                                allowed.remove(&derived);
                            }
                            if allowed.is_empty() {
                                self.pending_setval_re_emission.remove(seq_name);
                            }
                        }
                    }
                }

                // Monotonicity (wrong-direction) check.
                //
                // For autocommit emissions on sequences that have never
                // had a setval, the cross-fiber watermark is a sound
                // ordering anchor: every prior committed emission moves
                // it strictly forward (per the descriptor's increment
                // sign), so a value that fails to strictly advance is a
                // real bug.
                //
                // We skip the check in cases that produce false
                // positives:
                //
                //   * In-tx emission: the racing tx's commit-validation
                //     may abort one of two concurrent emissions, and
                //     until then the watermark may transiently reflect a
                //     soon-to-be-dead value. Deferred to
                //     `Operation::Commit` below, where we compare against
                //     `committed_watermark` instead.
                //   * `setval_could_have_raced`: an in-flight or recently
                //     completed setval may have repositioned the seq —
                //     standard skip already used by the existing logic.
                //   * `is_allowed_re_emission`: the one-shot post-setval
                //     allowance. setval(X, is_called=false) + restart +
                //     nextval legitimately re-emits X — once. The
                //     allowance entry is consumed BELOW (after the
                //     watermark update) so the very next subsequent
                //     nextval is checked normally.
                //
                // For CYCLE sequences the check still runs but treats
                // a legitimate wrap (new value lands on MIN/MAX while
                // the prior watermark was past it) as a valid
                // direction change rather than a bug. Any other
                // backward step on a CYCLE sequence — e.g. emitting
                // 2 when the prior committed value was 3 and the seq
                // hasn't wrapped — is still a real wrong-direction
                // bug and fires the bail.
                // Wrong-direction check uses the sticky `seqs_with_setval_ever`
                // flag here (not the narrower `pending_setval_re_emission`)
                // because the tracker cannot bound how many post-setval
                // nextvals may legitimately race ahead: with N fibers in
                // flight, up to N nextvals after a setval(X) can each emit
                // X+k*inc for k in [0, N). The duplicate check below stays
                // tight because all_values is freshened at setval time,
                // so post-setval duplicates still fire there.
                let skip_monotonicity = self.seqs_with_setval_ever.contains(seq_name)
                    || self.seqs_with_seq_default_ever.contains(seq_name);
                if txn_id.is_none() && !setval_could_have_raced && !skip_monotonicity {
                    if let Some(&(wm, wm_eid)) = self.watermark.get(seq_name) {
                        if wm_eid < start_exec_id && !is_cycle_wrap(params, value, wm) {
                            if params.increment > 0 && value <= wm {
                                bail!(
                                    "sequence went in wrong direction: seq={}, value={}, watermark={}, increment={}",
                                    seq_name,
                                    value,
                                    wm,
                                    params.increment
                                );
                            }
                            if params.increment < 0 && value >= wm {
                                bail!(
                                    "sequence went in wrong direction: seq={}, value={}, watermark={}, increment={}",
                                    seq_name,
                                    value,
                                    wm,
                                    params.increment
                                );
                            }
                        }
                    }
                }
                // Only autocommit emissions update the cross-fiber
                // watermark immediately. In-tx emissions defer their
                // watermark update to the Operation::Commit branch —
                // until that point we cannot tell whether the emission
                // will actually survive (it may be aborted by an MVCC
                // commit-validation W-W conflict against a racing tx),
                // and polluting the shared watermark with a possibly-
                // dead emission causes spurious wrong-direction failures
                // in the OTHER racing tx.
                if txn_id.is_none() {
                    self.watermark
                        .insert(seq_name.clone(), (value, end_exec_id));
                }

                // Alignment check: nextval-only sequences only ever emit
                // `start + k*increment`. A completed or in-flight setval can
                // reposition the engine atomic to any value (e.g.
                // `setval(4)` on a `start=1, increment=5` seq), so a
                // subsequent nextval can legitimately emit an off-grid
                // value either before the setval shadow expires or as the
                // direct re-emission of the setval target.
                let alignment_could_skip =
                    setval_could_have_raced || self.seqs_with_setval_ever.contains(seq_name);
                if !alignment_could_skip && (value - params.start) % params.increment != 0 {
                    bail!(
                        "sequence value not aligned to increment grid: seq={}, value={}, start={}, increment={}",
                        seq_name,
                        value,
                        params.start,
                        params.increment
                    );
                }

                self.fiber_last_nextval
                    .insert((fiber_id, seq_name.clone()), value);

                // Pull the baseline captured at this NextVal's init_op time
                // — see `in_flight_nextval_baselines` doc for why finish-time
                // recapture is wrong under concurrent autocommit emissions.
                // Falls back to a fresh snapshot if no baseline was recorded
                // (e.g. NextVal that bypassed init_op via InsertSeqDefault).
                let baseline = self
                    .in_flight_nextval_baselines
                    .remove(&(fiber_id, seq_name.clone()))
                    .unwrap_or_else(|| self.committed_watermark.get(seq_name).copied());
                if let Some(txn_id) = txn_id {
                    // Defer committed_watermark update until COMMIT — see
                    // the field doc on committed_watermark for why we never
                    // promote rolled-back values.
                    self.pending_txn_values.entry(txn_id).or_default().push((
                        seq_name.clone(),
                        value,
                        baseline,
                    ));
                } else {
                    // Autocommit succeeded — value is durable on disk.
                    self.promote_committed(seq_name, value);
                }
            }
            Operation::CreateTableWithSeqDefault {
                table_name,
                seq_name,
            } => {
                self.seq_default_tables
                    .insert(table_name.clone(), seq_name.clone());
            }
            Operation::InsertSeqDefault { table_name } => {
                if let Some(seq_name) = self.seq_default_tables.get(table_name) {
                    self.fiber_last_nextval
                        .remove(&(fiber_id, seq_name.clone()));
                    // INSERT … DEFAULT consumes nextval without returning
                    // the value to the simulator, so the tracker can't
                    // freshen `watermark`. Mark the seq so the wrong-
                    // direction check is skipped on this and any future
                    // observed nextval — same shape as the post-setval skip.
                    if result.is_ok() {
                        self.seqs_with_seq_default_ever.insert(seq_name.clone());
                    }
                }
            }
            Operation::Rollback => {
                if let Some(txn_id) = txn_id {
                    // Pending values were never promoted into `all_values`
                    // (the duplicate check is deferred to Commit). Just
                    // drop them from the pending map; nothing else to undo.
                    self.pending_txn_values.remove(&txn_id);
                    // Discard deferred setval watermarks — the backing table
                    // writes are reverted, so the Sequence stays unchanged.
                    self.pending_setval_watermarks.remove(&txn_id);
                    revert_pre_txn_watermarks(
                        &mut self.pre_txn_watermarks,
                        &mut self.watermark,
                        txn_id,
                    );
                }
            }
            Operation::Commit => {
                if let Some(txn_id) = txn_id {
                    // Promote each nextval value made in this tx into
                    // committed_watermark — they're now durable on disk.
                    // Deferred duplicate check: only at commit time can we
                    // tell whether this tx's emitted value really collides
                    // with a committed value. Two concurrent in-flight
                    // emissions of the same value get resolved by the
                    // engine's MVCC commit-validation path (the loser
                    // aborts with WriteWriteConflict); only the surviving
                    // commit reaches this branch. If we still find the
                    // value already in `all_values`, that IS a real
                    // duplicate-commit bug.
                    if let Some(values) = self.pending_txn_values.remove(&txn_id) {
                        for (seq_name, value, baseline) in values {
                            let params = self.params.get(&seq_name).cloned();
                            // Wrong-direction check: now that we know this
                            // value really committed, verify it advances
                            // past `baseline` — the committed_watermark
                            // *at the time this nextval ran*. Concurrent
                            // commits on other connections may have
                            // advanced the live committed_watermark in
                            // the meantime; that's fine and does not
                            // make this emission a regression. The one-
                            // shot post-setval allowance applies here
                            // too — same `pending_setval_re_emission`
                            // entry used by the autocommit path.
                            // Commit-time wrong-direction check uses the sticky
                            // `seqs_with_setval_ever` skip for the same reason
                            // as the autocommit site: post-setval, an arbitrary
                            // number of in-flight nextvals can legitimately
                            // emit X+k*inc for varying k.
                            let skip_monotonicity = self.seqs_with_setval_ever.contains(&seq_name)
                                || self.seqs_with_seq_default_ever.contains(&seq_name);
                            if let Some(params) = params.as_ref() {
                                if !params.cycle && !skip_monotonicity {
                                    if let Some(prev) = baseline {
                                        if params.increment > 0 && value <= prev {
                                            bail!(
                                                "sequence went in wrong direction: seq={}, value={}, watermark={}, increment={}",
                                                seq_name,
                                                value,
                                                prev,
                                                params.increment
                                            );
                                        }
                                        if params.increment < 0 && value >= prev {
                                            bail!(
                                                "sequence went in wrong direction: seq={}, value={}, watermark={}, increment={}",
                                                seq_name,
                                                value,
                                                prev,
                                                params.increment
                                            );
                                        }
                                    }
                                }
                            }
                            // Duplicate check: tightened post-setval. The
                            // allowance set tracks target + target+inc;
                            // matching values land in `all_values` once
                            // (idempotent) and the matched entry is then
                            // removed so a true duplicate of the same
                            // value would still bail later.
                            let is_allowed_re_emission = self
                                .pending_setval_re_emission
                                .get(&seq_name)
                                .and_then(|allowed| {
                                    params.as_ref().map(|p| {
                                        allowed.contains(&value)
                                            || allowed
                                                .iter()
                                                .any(|&t| value == t.saturating_add(p.increment))
                                    })
                                })
                                .unwrap_or(false);
                            let ever_setval = self.seqs_with_setval_ever.contains(&seq_name);
                            let ever_seq_default =
                                self.seqs_with_seq_default_ever.contains(&seq_name);
                            let entry = self.all_values.entry(seq_name.clone()).or_default();
                            if let Some(params) = params.as_ref() {
                                if !params.cycle
                                    && !is_allowed_re_emission
                                    && !ever_setval
                                    && !ever_seq_default
                                    && !entry.insert(value)
                                {
                                    bail!(
                                        "duplicate sequence value: seq={}, value={}, increment={}",
                                        seq_name,
                                        value,
                                        params.increment,
                                    );
                                }
                                if params.cycle
                                    || is_allowed_re_emission
                                    || ever_setval
                                    || ever_seq_default
                                {
                                    entry.insert(value);
                                }
                            } else {
                                entry.insert(value);
                            }
                            if is_allowed_re_emission {
                                if let Some(allowed) =
                                    self.pending_setval_re_emission.get_mut(&seq_name)
                                {
                                    if !allowed.remove(&value) {
                                        if let Some(p) = params.as_ref() {
                                            let derived = value.saturating_sub(p.increment);
                                            allowed.remove(&derived);
                                        }
                                    }
                                    if allowed.is_empty() {
                                        self.pending_setval_re_emission.remove(&seq_name);
                                    }
                                }
                            }
                            self.promote_committed(&seq_name, value);
                            // Now-durable: promote into the cross-fiber
                            // watermark. Deferred from emission time so
                            // racing in-flight emissions don't pollute it.
                            self.watermark
                                .insert(seq_name.clone(), (value, end_exec_id));
                        }
                    }
                    // Apply deferred setval watermarks now that the transaction
                    // committed and the in-memory Sequence was updated.
                    // setval REPLACES committed_watermark — see the autocommit
                    // setval branch for the rationale.
                    if let Some(entries) = self.pending_setval_watermarks.remove(&txn_id) {
                        for (seq_name, wm, eid) in entries {
                            self.watermark.insert(seq_name.clone(), (wm, eid));
                            self.last_setval_exec_id.insert(seq_name.clone(), eid);
                            self.committed_watermark.insert(seq_name.clone(), wm);
                            self.all_values.insert(seq_name.clone(), HashSet::new());
                        }
                    }
                    // Commit: the in-tx watermark updates are now durable, so
                    // drop the snapshots — no rollback can revert them.
                    self.pre_txn_watermarks.retain(|(tid, _), _| *tid != txn_id);
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn abort_fiber(&mut self, fiber_id: usize, txn_id: Option<u64>) -> anyhow::Result<()> {
        if let Some(txn_id) = txn_id {
            // Pending values were never promoted into `all_values` (the
            // duplicate check is deferred to Commit). Just drop them.
            self.pending_txn_values.remove(&txn_id);
            self.pending_setval_watermarks.remove(&txn_id);
            revert_pre_txn_watermarks(&mut self.pre_txn_watermarks, &mut self.watermark, txn_id);
        }
        self.fiber_last_nextval
            .retain(|(fid, _), _| *fid != fiber_id);
        // A SetVal that was in flight when this fiber was aborted may
        // already have committed its backing-table write before the
        // engine returned the result to the simulator — under
        // multiprocess kill the worker can be killed between commit and
        // wire-protocol reply, so the tracker never observes a SetVal
        // finish_op for it. Mark each in-flight target as setval-touched
        // so subsequent NextVal observations don't trip the
        // grid-alignment / monotonicity / duplicate checks against
        // ground truth the tracker cannot reconstruct.
        let aborted_seqs: Vec<String> = self
            .in_flight_setvals
            .iter()
            .filter(|((fid, _), _)| *fid == fiber_id)
            .map(|((_, name), _)| name.clone())
            .collect();
        for seq_name in aborted_seqs {
            self.seqs_with_setval_ever.insert(seq_name);
        }
        self.in_flight_setvals
            .retain(|(fid, _), _| *fid != fiber_id);
        self.in_flight_nextval_baselines
            .retain(|(fid, _), _| *fid != fiber_id);
        Ok(())
    }

    fn on_restart(
        &mut self,
        persisted_sequences: &HashMap<String, PersistedSequenceState>,
    ) -> anyhow::Result<()> {
        // Drop any nextval values left in pending_txn_values — they were
        // emitted by txs that never committed before the restart.
        self.pending_txn_values.clear();
        self.fiber_last_nextval.clear();
        self.last_setval_exec_id.clear();
        self.pending_setval_watermarks.clear();
        // Reopen drops every fiber connection — any in-flight setval whose
        // op_halt didn't run before drain is discarded by the engine's
        // connection drop, so it can no longer affect any atomic.
        self.in_flight_setvals.clear();

        // Reset all_values across restart. Per the engine's bundled-rollback
        // contract (see core/schema.rs Sequence design block) the in-memory
        // atomic isn't rolled back when a tx rolls back, but the disk write
        // IS. On restart the atomic is re-seeded from disk, so an integer
        // emitted by a rolled-back nextval pre-restart may legitimately be
        // re-emitted post-restart. The in-session duplicate check should
        // therefore only catch duplicates within a single
        // open-database-instance lifetime, not across restart boundaries.
        // The restart assertion below (committed_watermark vs disk) is the
        // mechanism that catches losing a *committed* value across restart.
        self.all_values.clear();

        // Assert disk preserves every COMMITTED value, but tolerate gaps for
        // rolled-back values. The disk watermark must be at least as advanced
        // (direction-aware) as the highest value any committed nextval/setval
        // ever returned to the user.
        //
        // Skipped for seqs in `seqs_with_setval_ever`. Two sound
        // reasons to skip:
        //
        //   * A crash mid-setval can leave disk lagging the tracker's
        //     committed_watermark — the engine can commit the
        //     in-memory mutation and die before the disk write,
        //     yielding a spurious "lost a committed value" report.
        //   * The tracker cannot reliably reconcile the relative
        //     timeline of pending in-tx nextval values (promoted at
        //     outer commit) with autocommit setvals that replace the
        //     watermark.
        //
        // Pure-nextval seqs still get the check.
        for (seq_name, persisted) in persisted_sequences {
            // For CYCLE sequences the "persisted >= committed_watermark"
            // invariant doesn't hold across a wrap: committed_watermark
            // is direction-aware MAX/MIN, but the post-wrap persisted
            // value lands back at the wrap target. Tracking the last
            // committed emission (not direction-aware) would require
            // reordering commits by MVCC end_ts, which the property
            // doesn't have access to. Left as a TODO — the in-run
            // duplicate and wrong-direction CYCLE checks (both
            // tightened above to be wrap-aware rather than blanket-
            // skipped) cover most of the regression surface.
            if persisted.params.cycle {
                continue;
            }
            if self.seqs_with_setval_ever.contains(seq_name)
                || self.seqs_with_seq_default_ever.contains(seq_name)
            {
                continue;
            }
            let Some(&committed) = self.committed_watermark.get(seq_name) else {
                continue;
            };
            if persisted.params.increment > 0 && persisted.watermark < committed {
                bail!(
                    "sequence restart lost a committed value: seq={}, persisted={}, committed={}, increment={}",
                    seq_name,
                    persisted.watermark,
                    committed,
                    persisted.params.increment
                );
            }
            if persisted.params.increment < 0 && persisted.watermark > committed {
                bail!(
                    "sequence restart lost a committed value: seq={}, persisted={}, committed={}, increment={}",
                    seq_name,
                    persisted.watermark,
                    committed,
                    persisted.params.increment
                );
            }
        }

        // Multiprocess respawn does not yet thread real persisted state
        // through the worker protocol — it passes an empty map. Treating
        // empty as "no seq exists" wipes the tracker's view of every
        // live sequence, after which a subsequent `CREATE SEQUENCE IF
        // NOT EXISTS` for the same name (which the engine no-ops) gets
        // misread as a fresh creation and the new user-supplied params
        // overwrite the originals the engine actually uses. Skip the
        // wipe in that case; the tracker's existing view remains the
        // best available approximation of disk state.
        if !persisted_sequences.is_empty() {
            self.params
                .retain(|seq_name, _| persisted_sequences.contains_key(seq_name));
            self.watermark
                .retain(|seq_name, _| persisted_sequences.contains_key(seq_name));
            self.committed_watermark
                .retain(|seq_name, _| persisted_sequences.contains_key(seq_name));
            self.all_values
                .retain(|seq_name, _| persisted_sequences.contains_key(seq_name));
            self.seq_default_tables
                .retain(|_, seq_name| persisted_sequences.contains_key(seq_name));
        }

        for (seq_name, persisted) in persisted_sequences {
            self.params
                .insert(seq_name.clone(), persisted.params.clone());
            self.all_values.entry(seq_name.clone()).or_default();
            self.watermark
                .insert(seq_name.clone(), (persisted.watermark, 0));
            // committed_watermark is also seeded from disk — what's on disk
            // post-restart IS by definition the committed truth. Take the
            // direction-aware max with any pre-existing entry (defensive,
            // in case a stale entry survives the retain above for some
            // reason — should not happen but cheap to guard).
            let entry = self
                .committed_watermark
                .entry(seq_name.clone())
                .or_insert(persisted.watermark);
            if persisted.params.increment > 0 {
                if persisted.watermark > *entry {
                    *entry = persisted.watermark;
                }
            } else if persisted.watermark < *entry {
                *entry = persisted.watermark;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    fn test_output_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "turso-whopper-{label}-{}-{}.edn",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ))
    }

    #[test]
    fn simple_keys_abort_fiber_drops_pending_transaction_state() {
        let mut property = SimpleKeysDoNotDisappear::new();
        property.txn_started_at.insert(7, 11);
        property.simple_keys_added_at.insert(
            Some(7),
            HashMap::from([(("t".to_string(), "k".to_string()), 13)]),
        );

        property.abort_fiber(0, Some(7)).unwrap();

        assert!(!property.txn_started_at.contains_key(&7));
        assert!(!property.simple_keys_added_at.contains_key(&Some(7)));
    }

    #[test]
    fn elle_history_abort_fiber_marks_pending_transaction_as_info() {
        let mut recorder = ElleHistoryRecorder::new(test_output_path("abort-pending-transaction"));

        recorder
            .init_op(
                0,
                3,
                Some(9),
                17,
                &Operation::Begin {
                    mode: crate::operations::TxMode::Immediate,
                },
            )
            .unwrap();
        recorder
            .finish_op(
                0,
                3,
                Some(9),
                17,
                18,
                &Operation::Begin {
                    mode: crate::operations::TxMode::Immediate,
                },
                &Ok(vec![]),
            )
            .unwrap();
        recorder
            .finish_op(
                1,
                3,
                Some(9),
                21,
                22,
                &Operation::ElleAppend {
                    table_name: "elle_lists".to_string(),
                    key: "k".to_string(),
                    value: 5,
                },
                &Ok(vec![]),
            )
            .unwrap();

        recorder.abort_fiber(3, Some(9)).unwrap();

        assert!(!recorder.pending_txns.contains_key(&3));
        assert_eq!(recorder.events.len(), 2);
        assert!(matches!(
            recorder.events[0].event_type,
            ElleEventType::Invoke
        ));
        assert!(matches!(recorder.events[1].event_type, ElleEventType::Info));
    }

    #[test]
    fn elle_history_abort_fiber_marks_pending_autocommit_as_info() {
        let mut recorder = ElleHistoryRecorder::new(test_output_path("abort-autocommit"));

        recorder
            .init_op(
                0,
                4,
                None,
                31,
                &Operation::ElleRwWrite {
                    table_name: "elle_rw".to_string(),
                    key: "k".to_string(),
                    value: 9,
                },
            )
            .unwrap();

        recorder.abort_fiber(4, None).unwrap();

        assert!(!recorder.pending_auto_commits.contains_key(&4));
        assert_eq!(recorder.events.len(), 2);
        assert!(matches!(
            recorder.events[0].event_type,
            ElleEventType::Invoke
        ));
        assert!(matches!(recorder.events[1].event_type, ElleEventType::Info));
    }
}

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

/// Parse the rw-register read result from query rows.
/// Returns Some(value) if a row was found, None if no rows or NULL.
fn parse_rw_read_result(result: &OpResult) -> Option<i64> {
    let rows = result.as_ref().ok()?;
    let value = rows.first()?.first()?;
    match value {
        Value::Null => None,
        v => v.as_int(),
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

/// Returns true iff `value` represents a legitimate CYCLE wrap event
/// — the new value landed on the wrap target (`min_value` for
/// ascending sequences, `max_value` for descending) while the prior
/// watermark was strictly past it. For non-cycling sequences this is
/// always false.
///
/// Used by `SequenceCorrectnessProperty` to refine the duplicate and
/// wrong-direction checks for CYCLE sequences: previously both checks
/// were unconditionally skipped for `params.cycle == true`, so the
/// only invariant verified between two wraps was the in-cycle aligned-
/// grid check. With `is_cycle_wrap` we can reset the per-cycle
/// emission set on wrap and treat a backward step as a wrap only when
/// it actually lands on MIN/MAX — every other backward step still
/// fires the wrong-direction bail.
fn is_cycle_wrap(params: &SequenceParams, value: i64, prev_watermark: i64) -> bool {
    if !params.cycle {
        return false;
    }
    if params.increment > 0 {
        value == params.min_value && prev_watermark > params.min_value
    } else {
        value == params.max_value && prev_watermark < params.max_value
    }
}

/// Asserts that every rowid issued by the engine to the AUTOINCREMENT
/// table (`t_autoinc`) is strictly greater than every previously-seen
/// committed/issued rowid on that table — whether it was assigned by
/// an `INSERT … VALUES (NULL, …)` (returned via `RETURNING id`) or
/// reassigned by an `UPDATE … SET id = N` (also via `RETURNING id`).
///
/// This is the symptom-side check for the historical bug class where
/// an UPDATE bumps the high-water mark for the rowid but the
/// `sqlite_sequence.seq` row or the in-memory `Sequence` doesn't
/// follow, so a subsequent `INSERT … VALUES(NULL)` returns a rowid
/// that collides with the relocated row. Under MVCC the same hazard
/// shows up across concurrent transactions: tx A's UPDATE bumps the
/// rowid to N; tx B's `INSERT NULL` (begun before A committed) sees
/// the pre-A snapshot, picks max+1 from that snapshot, and after
/// commit-validation we end up with two committed rows sharing what
/// should have been a strictly-monotone autoinc sequence.
///
/// The property is intentionally not snapshot-aware: it tracks every
/// rowid the engine returned to the simulator at finish-time. Under
/// MVCC the engine's autoinc allocator is autonomous-write — it
/// commits its own `sqlite_sequence` bump independently of the
/// surrounding tx — so once a fiber observes an INSERT or UPDATE
/// returning id `N`, no future INSERT NULL on any fiber may return
/// `≤ N`. Violations are real bugs.
///
/// Cross-reopen check is currently best-effort: on restart we reset
/// `issued_max` to 0 and rely on the first post-restart observation
/// to re-establish the baseline. Catching cross-reopen regressions
/// would require threading `sqlite_sequence.seq` through `on_restart`
/// (TODO).
pub struct AutoincWatermarkMonotonicity {
    /// Highest rowid that has been observed *and committed* on `t_autoinc`.
    /// In-tx INSERT / UPDATE rowids stay in `pending_per_tx` until the
    /// surrounding tx commits — on rollback they're dropped. This avoids
    /// flagging Turso's MVCC-mode behavior of re-emitting rowids consumed
    /// by rolled-back txs, which is intentional (per `bundled-sequences-
    /// rollback-contract` memory note). The check fires only for the
    /// committed-state invariant: no INSERT NULL may ever return an id
    /// that's ≤ the committed max **as of the moment its op started**.
    committed_max: i64,
    /// Per-tx pending max rowid (INSERT-returned or UPDATE-returned).
    /// Promoted to `committed_max` on Commit; dropped on Rollback.
    /// Within a tx, subsequent INSERTs still need to strictly advance
    /// past anything we've already observed in the same tx.
    pending_per_tx: HashMap<u64, i64>,
    /// Per-(fiber, exec_id) snapshot of `committed_max` taken at
    /// `init_op` time. Used as the baseline for the finish-time
    /// monotonicity check on `AutoincInsert`. Without this snapshot
    /// we'd compare against the live `committed_max`, which under
    /// MVCC concurrent txs may have advanced between op start and
    /// op finish — e.g. an autocommit insert in another fiber races
    /// ahead while this fiber's in-tx insert is mid-flight. The
    /// engine's autoinc allocator is autonomous-write and coordinates
    /// uniqueness at allocation time, so an id `N` returned to fiber A
    /// is correct relative to the allocator state when A asked, even
    /// if B's autocommit advances `committed_max` past `N` before A
    /// finishes. Keyed by exec_id (unique per op) so concurrent ops
    /// on the same fiber don't clobber.
    snapshot_at_init: HashMap<u64, i64>,
}

impl Default for AutoincWatermarkMonotonicity {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoincWatermarkMonotonicity {
    pub fn new() -> Self {
        Self {
            committed_max: 0,
            pending_per_tx: HashMap::new(),
            snapshot_at_init: HashMap::new(),
        }
    }

    fn effective_baseline(&self, txn_id: Option<u64>, exec_id: u64) -> i64 {
        let snapshot = self
            .snapshot_at_init
            .get(&exec_id)
            .copied()
            .unwrap_or(self.committed_max);
        let pending = txn_id
            .and_then(|t| self.pending_per_tx.get(&t).copied())
            .unwrap_or(0);
        snapshot.max(pending)
    }

    fn record_id(&mut self, txn_id: Option<u64>, id: i64) {
        match txn_id {
            Some(t) => {
                let entry = self.pending_per_tx.entry(t).or_insert(0);
                if id > *entry {
                    *entry = id;
                }
            }
            None => {
                if id > self.committed_max {
                    self.committed_max = id;
                }
            }
        }
    }
}

impl Property for AutoincWatermarkMonotonicity {
    fn init_op(
        &mut self,
        _step: usize,
        _fiber_id: usize,
        _txn_id: Option<u64>,
        exec_id: u64,
        op: &Operation,
    ) -> anyhow::Result<()> {
        if matches!(
            op,
            Operation::AutoincInsert { .. } | Operation::AutoincUpdateRowid { .. }
        ) {
            // Snapshot committed_max NOW so the finish-time check uses
            // the engine's view at allocation time, not whatever
            // unrelated concurrent commits piled on while this op was
            // mid-flight.
            self.snapshot_at_init.insert(exec_id, self.committed_max);
        }
        Ok(())
    }

    fn finish_op(
        &mut self,
        _step: usize,
        _fiber_id: usize,
        txn_id: Option<u64>,
        start_exec_id: u64,
        _end_exec_id: u64,
        op: &Operation,
        result: &OpResult,
    ) -> anyhow::Result<()> {
        let Ok(rows) = result else {
            // Drop any snapshot we took for this op — it never produced
            // an id we care about. Avoids unbounded growth of the map.
            self.snapshot_at_init.remove(&start_exec_id);
            return Ok(());
        };
        match op {
            Operation::AutoincInsert { .. } => {
                let id = rows
                    .first()
                    .and_then(|r| r.first())
                    .and_then(|v| v.as_int())
                    .ok_or_else(|| {
                        self.snapshot_at_init.remove(&start_exec_id);
                        anyhow!("AutoincInsert: no RETURNING id in result rows: {rows:?}")
                    })?;
                let baseline = self.effective_baseline(txn_id, start_exec_id);
                self.snapshot_at_init.remove(&start_exec_id);
                if id <= baseline {
                    bail!(
                        "AUTOINCREMENT watermark regression: INSERT returned id={id}, but committed_max at op start (exec_id={start_exec_id}) was {baseline}. \
                         The engine's autoinc allocator gave a rowid that's ≤ what was already committed when this op began. \
                         Under MVCC this means either sqlite_sequence.seq was not bumped after an UPDATE past {baseline}, \
                         or the in-memory Sequence and on-disk watermark have diverged."
                    );
                }
                self.record_id(txn_id, id);
            }
            Operation::AutoincUpdateRowid { .. } => {
                // UPDATE-set rowids do NOT advance the AUTOINCREMENT
                // watermark in SQLite (or Turso). Only AutoincInsert
                // emissions advance `committed_max`. If we recorded the
                // UPDATE'd rowid here, the next AutoincInsert that
                // legitimately picked `MAX(rowid)+1` (where the
                // UPDATE'd row had since been deleted) would trip the
                // watermark-regression check even though both engines
                // behave identically.
                self.snapshot_at_init.remove(&start_exec_id);
            }
            Operation::Commit => {
                if let Some(t) = txn_id {
                    if let Some(pending) = self.pending_per_tx.remove(&t) {
                        if pending > self.committed_max {
                            self.committed_max = pending;
                        }
                    }
                }
            }
            Operation::Rollback => {
                if let Some(t) = txn_id {
                    self.pending_per_tx.remove(&t);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn abort_fiber(&mut self, _fiber_id: usize, txn_id: Option<u64>) -> anyhow::Result<()> {
        if let Some(t) = txn_id {
            self.pending_per_tx.remove(&t);
        }
        Ok(())
    }

    fn on_restart(
        &mut self,
        persisted_sequences: &HashMap<String, PersistedSequenceState>,
    ) -> anyhow::Result<()> {
        // Cross-reopen check: read the persisted high-water mark for
        // `t_autoinc` from its backing-table-derived state (the
        // simulator queries it from `_turso_internal_seq__autoincrement_<table>`
        // via `query_persisted_sequence_states` in lib.rs). After a
        // reopen the engine derives the next rowid from this
        // watermark, so the post-reopen INSERT NULL must return an id
        // strictly greater than it. Carrying it through here means a
        // regression that ONLY manifests across the reopen boundary
        // (e.g. sqlite_sequence + _seq backing table diverged so the
        // post-reopen engine picks a stale watermark) still trips the
        // check on the next INSERT.
        //
        // Falls back to 0 if the autoincrement sequence isn't yet in
        // persisted state — happens on the very first run before any
        // INSERT NULL has materialized the row.
        let autoinc_seq_name = format!("__turso_internal_autoincrement_{AUTOINC_TABLE_NAME}");
        self.committed_max = persisted_sequences
            .get(&autoinc_seq_name)
            .map(|s| s.watermark)
            .unwrap_or(0);
        self.pending_per_tx.clear();
        self.snapshot_at_init.clear();
        Ok(())
    }
}
