//! Property-based validation for simulation.

use std::collections::HashMap;

use anyhow::{anyhow, bail};
use turso_core::{LimboError, Value};

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
