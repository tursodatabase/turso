//! Chaotic btree workloads that bias Whopper toward non-root page balancing.
//!
//! The workload keeps one large table hot, deletes committed key bands to seed
//! the freelist, then refills those holes under savepoints with large payloads.
//! That shape is meant to exercise page-number reassignment during
//! `balance_non_root`, especially when a small page size makes splits frequent.

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::chaotic_elle::{ChaoticWorkload, ChaoticWorkloadProfile};
use crate::operations::{OpResult, Operation, TxMode};

const DEFAULT_TABLE_NAME: &str = "btree_rebalance";
const BLOCK_WIDTH: i64 = 10_000;
const FREELIST_ROWS: i64 = 384;
const WARMUP_ROWS: i64 = 512;
const DELETE_BAND_WIDTH: i64 = 64;
const WARMUP_PERMUTATION_MULTIPLIER: i64 = 73;

/// Generates multi-statement btree churn transactions.
pub struct BtreeRebalanceProfile {
    table_name: String,
    block_counter: AtomicI64,
}

impl BtreeRebalanceProfile {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            block_counter: AtomicI64::new(0),
        }
    }
}

impl Default for BtreeRebalanceProfile {
    fn default() -> Self {
        Self::new(DEFAULT_TABLE_NAME)
    }
}

struct BtreeRebalanceWorkload {
    ops: Vec<Operation>,
    index: usize,
}

impl BtreeRebalanceWorkload {
    fn new(ops: Vec<Operation>) -> Self {
        Self { ops, index: 0 }
    }
}

impl ChaoticWorkload for BtreeRebalanceWorkload {
    fn next(&mut self, result: Option<OpResult>) -> Option<Operation> {
        if let Some(Err(e)) = &result {
            tracing::trace!("btree rebalance workload stopped after operation error: {e}");
            return None;
        }

        let op = self.ops.get(self.index).cloned()?;
        self.index += 1;
        Some(op)
    }
}

impl ChaoticWorkloadProfile for BtreeRebalanceProfile {
    fn generate(&self, mut rng: ChaCha8Rng, fiber_id: usize) -> Box<dyn ChaoticWorkload> {
        let block = self.block_counter.fetch_add(1, Ordering::Relaxed);
        let block_base = (block * 32 + (fiber_id % 32) as i64) * BLOCK_WIDTH;
        let freelist_table_name = format!("{}_freelist", self.table_name);
        let savepoint_name = format!("btree_sp_{fiber_id}");
        let mut ops = Vec::new();

        push_setup_ops(&mut ops, &self.table_name, &freelist_table_name);
        push_seed_freelist_tx(&mut ops, &freelist_table_name, block_base, &mut rng);
        push_warmup_tx(&mut ops, &self.table_name, block_base, &mut rng);
        push_free_freelist_tx(&mut ops, &freelist_table_name, block_base);
        push_churn_tx(
            &mut ops,
            &self.table_name,
            block_base,
            &savepoint_name,
            &mut rng,
        );

        ops.push(Operation::IntegrityCheck);

        Box::new(BtreeRebalanceWorkload::new(ops))
    }
}

fn push_setup_ops(ops: &mut Vec<Operation>, table_name: &str, freelist_table_name: &str) {
    ops.push(Operation::Execute {
        sql: "PRAGMA cache_size = 12".to_string(),
    });
    ops.push(Operation::Execute {
        sql: format!(
            "CREATE TABLE IF NOT EXISTS {freelist_table_name} (\
             id INTEGER PRIMARY KEY, \
             pad BLOB NOT NULL)"
        ),
    });
    ops.push(Operation::Execute {
        sql: format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (\
             id INTEGER PRIMARY KEY, \
             k INTEGER NOT NULL, \
             pad BLOB NOT NULL, \
             tag TEXT NOT NULL)"
        ),
    });
    ops.push(Operation::Execute {
        sql: format!("CREATE INDEX IF NOT EXISTS idx_{table_name}_k ON {table_name}(k)"),
    });
    ops.push(Operation::Execute {
        sql: format!("CREATE INDEX IF NOT EXISTS idx_{table_name}_tag_k ON {table_name}(tag, k)"),
    });
}

fn push_seed_freelist_tx(
    ops: &mut Vec<Operation>,
    freelist_table_name: &str,
    block_base: i64,
    rng: &mut ChaCha8Rng,
) {
    ops.push(Operation::Begin {
        mode: TxMode::Immediate,
    });

    for ordinal in 0..FREELIST_ROWS {
        let id = block_base + ordinal;
        let payload_len = random_payload_len(rng);
        ops.push(Operation::Insert {
            sql: format!(
                "INSERT OR REPLACE INTO {freelist_table_name}(id, pad) \
                 VALUES ({id}, zeroblob({payload_len}))"
            ),
        });
    }

    ops.push(Operation::Commit);
}

fn push_warmup_tx(
    ops: &mut Vec<Operation>,
    table_name: &str,
    block_base: i64,
    rng: &mut ChaCha8Rng,
) {
    ops.push(Operation::Begin {
        mode: TxMode::Immediate,
    });

    for ordinal in 0..WARMUP_ROWS {
        let id = permuted_block_id(block_base, ordinal);
        let payload_len = random_payload_len(rng);
        ops.push(insert_op(table_name, id, payload_len, rng));
    }

    ops.push(Operation::Commit);
}

fn push_free_freelist_tx(ops: &mut Vec<Operation>, freelist_table_name: &str, block_base: i64) {
    let end = block_base + FREELIST_ROWS - 1;
    ops.push(Operation::Begin {
        mode: TxMode::Immediate,
    });
    ops.push(Operation::Delete {
        sql: format!("DELETE FROM {freelist_table_name} WHERE id BETWEEN {block_base} AND {end}"),
    });
    ops.push(Operation::Commit);
}

fn push_churn_tx(
    ops: &mut Vec<Operation>,
    table_name: &str,
    block_base: i64,
    savepoint_name: &str,
    rng: &mut ChaCha8Rng,
) {
    ops.push(Operation::Begin {
        mode: TxMode::Immediate,
    });

    let band_count = rng.random_range(2..=4);
    let max_first_band = (WARMUP_ROWS / DELETE_BAND_WIDTH) - 1 - ((band_count - 1) * 2);
    let first_band = rng.random_range(0..=max_first_band);
    for band_idx in 0..band_count {
        let start_ordinal = (first_band + band_idx * 2) * DELETE_BAND_WIDTH;
        let start = block_base + start_ordinal;
        let end = start + DELETE_BAND_WIDTH - 1;
        ops.push(Operation::Delete {
            sql: format!("DELETE FROM {table_name} WHERE id BETWEEN {start} AND {end}"),
        });
    }

    ops.push(Operation::Commit);
    ops.push(Operation::Begin {
        mode: TxMode::Immediate,
    });
    ops.push(Operation::Savepoint {
        name: savepoint_name.to_string(),
    });

    for band_idx in 0..band_count {
        let start_ordinal = (first_band + band_idx * 2) * DELETE_BAND_WIDTH;
        for ordinal in start_ordinal..start_ordinal + DELETE_BAND_WIDTH {
            let id = block_base + ordinal;
            let payload_len = random_payload_len(rng);
            ops.push(insert_op(table_name, id, payload_len, rng));
        }
    }

    let update_count = rng.random_range(2..=5);
    for _ in 0..update_count {
        let start_ordinal = rng.random_range(0..WARMUP_ROWS - 32);
        let start = block_base + start_ordinal;
        let end = start + rng.random_range(8..32);
        let tag = rng.random_range(0..64);
        let payload_len = random_payload_len(rng);
        let delta = rng.random_range(1..4096);
        ops.push(Operation::Update {
            sql: format!(
                "UPDATE {table_name} \
                 SET k = k + {delta}, tag = 'tag_{tag}', pad = zeroblob({payload_len}) \
                 WHERE id BETWEEN {start} AND {end}"
            ),
        });
    }

    if rng.random_bool(0.35) {
        ops.push(Operation::RollbackToSavepoint {
            name: savepoint_name.to_string(),
        });
    }
    ops.push(Operation::ReleaseSavepoint {
        name: savepoint_name.to_string(),
    });

    if rng.random_bool(0.85) {
        ops.push(Operation::Commit);
    } else {
        ops.push(Operation::Rollback);
    }
}

fn permuted_block_id(block_base: i64, ordinal: i64) -> i64 {
    block_base + ((ordinal * WARMUP_PERMUTATION_MULTIPLIER) % WARMUP_ROWS)
}

fn insert_op(table_name: &str, id: i64, payload_len: usize, rng: &mut ChaCha8Rng) -> Operation {
    let tag = rng.random_range(0..64);
    let k = rng.random_range(0..1_000_000);
    Operation::Insert {
        sql: format!(
            "INSERT OR REPLACE INTO {table_name}(id, k, pad, tag) \
             VALUES ({id}, {k}, zeroblob({payload_len}), 'tag_{tag}')"
        ),
    }
}

fn random_payload_len(rng: &mut ChaCha8Rng) -> usize {
    if rng.random_bool(0.2) {
        rng.random_range(6 * 1024..16 * 1024)
    } else {
        rng.random_range(700..6 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn generated_workload_contains_savepoint_churn() {
        let profile = BtreeRebalanceProfile::default();
        let mut workload = profile.generate(ChaCha8Rng::seed_from_u64(17), 3);
        let mut ops = Vec::new();
        let mut result = None;

        while let Some(op) = workload.next(result.take()) {
            ops.push(op);
            result = Some(Ok(Vec::new()));
        }

        assert!(
            ops.iter()
                .any(|op| matches!(op, Operation::Savepoint { .. }))
        );
        assert!(
            ops.iter()
                .any(|op| matches!(op, Operation::ReleaseSavepoint { .. }))
        );
        assert!(
            ops.iter()
                .filter(|op| matches!(op, Operation::Insert { .. }))
                .count()
                > (FREELIST_ROWS + WARMUP_ROWS) as usize
        );
        assert!(ops.iter().any(|op| matches!(op, Operation::Delete { .. })));
        assert!(ops.iter().any(|op| matches!(op, Operation::IntegrityCheck)));
    }
}
