//! Chaotic MVCC workloads that stress schema churn around checkpointing.

use rand::Rng;
use rand_chacha::ChaCha8Rng;

use crate::AUTOINC_TABLE_NAME;
use crate::chaotic_elle::{ChaoticWorkload, ChaoticWorkloadProfile};
use crate::operations::{OpResult, Operation};

const HIGH_CHECKPOINT_THRESHOLD: i64 = 1_000_000;

/// Broad MVCC checkpoint/schema churn profile.
///
/// This deliberately stays at the SQL/operation layer: it creates and drops
/// indexes, changes MVCC checkpoint pressure, scans schema, and performs DML
/// against an indexed table while Whopper's normal scheduler and randomized
/// yield injector choose the interleavings.
pub struct MvccCheckpointSchemaChurnProfile {
    index_slots: usize,
}

impl MvccCheckpointSchemaChurnProfile {
    pub fn new() -> Self {
        Self { index_slots: 8 }
    }

    fn index_name(&self, rng: &mut ChaCha8Rng) -> String {
        let slot = rng.random_range(0..self.index_slots);
        format!("whopper_mvcc_payload_idx_{slot}")
    }

    fn checkpoint_threshold(value: i64) -> Operation {
        Operation::Select {
            sql: format!("PRAGMA mvcc_checkpoint_threshold = {value}"),
        }
    }

    fn create_payload_index(index_name: String) -> Operation {
        Operation::CreateIndex {
            sql: format!(
                "CREATE INDEX IF NOT EXISTS {index_name} ON {AUTOINC_TABLE_NAME}(payload)"
            ),
            index_name,
            table_name: AUTOINC_TABLE_NAME.to_string(),
        }
    }

    fn drop_payload_index(index_name: String) -> Operation {
        Operation::DropIndex {
            sql: format!("DROP INDEX IF EXISTS {index_name}"),
            index_name,
        }
    }

    fn insert_payload(rng: &mut ChaCha8Rng, fiber_id: usize) -> Operation {
        Operation::AutoincInsert {
            payload: format!(
                "mvcc_schema_churn_{fiber_id}_{}",
                rng.random_range(0..1_000_000_000u32)
            ),
        }
    }

    fn update_payload(rng: &mut ChaCha8Rng) -> Operation {
        let suffix = rng.random_range(0..1_000_000u32);
        Operation::Update {
            sql: format!(
                "UPDATE {AUTOINC_TABLE_NAME} \
                 SET payload = payload || '_u{suffix}' \
                 WHERE id IN (SELECT id FROM {AUTOINC_TABLE_NAME} ORDER BY id DESC LIMIT 1)"
            ),
        }
    }

    fn delete_recent_row(rng: &mut ChaCha8Rng) -> Operation {
        Operation::AutoincDelete {
            id: rng.random_range(1..10_000i64),
        }
    }

    fn schema_scan() -> Operation {
        Operation::Select {
            sql: format!(
                "SELECT name, rootpage FROM sqlite_schema \
                 WHERE tbl_name = '{AUTOINC_TABLE_NAME}' ORDER BY name"
            ),
        }
    }

    fn payload_probe() -> Operation {
        Operation::Select {
            sql: format!(
                "SELECT id FROM {AUTOINC_TABLE_NAME} \
                 WHERE payload >= 'mvcc_schema_churn' ORDER BY payload LIMIT 8"
            ),
        }
    }
}

impl Default for MvccCheckpointSchemaChurnProfile {
    fn default() -> Self {
        Self::new()
    }
}

struct MvccCheckpointSchemaChurnWorkload {
    ops: Vec<Operation>,
    index: usize,
}

impl MvccCheckpointSchemaChurnWorkload {
    fn new(ops: Vec<Operation>) -> Self {
        Self { ops, index: 0 }
    }
}

impl ChaoticWorkload for MvccCheckpointSchemaChurnWorkload {
    fn next(&mut self, result: Option<OpResult>) -> Option<Operation> {
        if let Some(Err(e)) = &result {
            tracing::trace!("mvcc checkpoint/schema churn operation error: {}", e);
            return None;
        }

        let op = self.ops.get(self.index).cloned();
        self.index += usize::from(op.is_some());
        op
    }
}

impl ChaoticWorkloadProfile for MvccCheckpointSchemaChurnProfile {
    fn generate(&self, mut rng: ChaCha8Rng, fiber_id: usize) -> Box<dyn ChaoticWorkload> {
        let index_name = self.index_name(&mut rng);
        let ops = match rng.random_range(0..6u32) {
            // Frequent auto-checkpoint candidates. If one yields before the
            // checkpoint lock, other fibers can change schema underneath it.
            0 => vec![
                Self::checkpoint_threshold(0),
                Self::insert_payload(&mut rng, fiber_id),
                Self::update_payload(&mut rng),
                Self::schema_scan(),
            ],
            // Create an index under checkpoint pressure, then leave later
            // indexed rows for another checkpoint to collect.
            1 => vec![
                Self::checkpoint_threshold(0),
                Self::create_payload_index(index_name),
                Self::checkpoint_threshold(HIGH_CHECKPOINT_THRESHOLD),
                Self::insert_payload(&mut rng, fiber_id),
                Self::update_payload(&mut rng),
                Self::schema_scan(),
            ],
            // Drop/recreate across threshold changes to exercise stale index
            // identity, rootpage reuse, and row-version filtering.
            2 => vec![
                Self::checkpoint_threshold(HIGH_CHECKPOINT_THRESHOLD),
                Self::drop_payload_index(index_name.clone()),
                Self::insert_payload(&mut rng, fiber_id),
                Self::checkpoint_threshold(0),
                Self::create_payload_index(index_name),
                Self::payload_probe(),
            ],
            // Keep indexed DML hot while checkpointing is aggressive.
            3 => vec![
                Self::checkpoint_threshold(0),
                Self::create_payload_index(index_name),
                Self::insert_payload(&mut rng, fiber_id),
                Self::insert_payload(&mut rng, fiber_id),
                Self::payload_probe(),
            ],
            // Schema churn with data changes on both sides.
            4 => vec![
                Self::checkpoint_threshold(0),
                Self::create_payload_index(index_name.clone()),
                Self::insert_payload(&mut rng, fiber_id),
                Self::drop_payload_index(index_name.clone()),
                Self::create_payload_index(index_name),
                Self::schema_scan(),
            ],
            // Cheap observer sequence that keeps connections refreshing schema
            // and reading through any currently live payload indexes.
            _ => vec![
                Self::schema_scan(),
                Self::payload_probe(),
                Self::checkpoint_threshold(0),
                Self::insert_payload(&mut rng, fiber_id),
                Self::delete_recent_row(&mut rng),
            ],
        };

        Box::new(MvccCheckpointSchemaChurnWorkload::new(ops))
    }
}
