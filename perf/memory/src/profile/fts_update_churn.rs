use super::{Phase, Profile, WorkItem};

const SEED_ROWS_PER_INDEX: usize = 2_000;
const SEED_BATCH_SIZE: usize = 500;
const TERMS: [&str; 8] = [
    "database",
    "transaction",
    "storage",
    "index",
    "replication",
    "search",
    "rust",
    "sqlite",
];

/// Repeated updates to fixed-size, full-text-indexed tables.
///
/// The FTS MVCC contract permits one active writer per index. To measure four
/// concurrent writers without turning expected same-index conflicts into
/// benchmark failures, each connection owns a separate table and index. Every
/// table retains a bounded live document set while accumulating FTS and MVCC
/// update churn.
pub struct FtsUpdateChurn {
    iterations: usize,
    batch_size: usize,
    current_iteration: usize,
    phase: InternalPhase,
    configured_connections: usize,
    seed_connection: usize,
    seed_offset: usize,
}

enum InternalPhase {
    CreateSchema,
    Seed,
    Run,
}

impl FtsUpdateChurn {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            batch_size,
            current_iteration: 0,
            phase: InternalPhase::CreateSchema,
            configured_connections: 0,
            seed_connection: 0,
            seed_offset: 0,
        }
    }
}

impl Profile for FtsUpdateChurn {
    fn name(&self) -> &str {
        "fts-update-churn"
    }

    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        match self.phase {
            InternalPhase::CreateSchema => {
                self.configured_connections = connections.max(1);
                self.phase = InternalPhase::Seed;

                let mut items = Vec::with_capacity(self.configured_connections * 2);
                for connection in 0..self.configured_connections {
                    items.push(WorkItem {
                        sql: format!(
                            "CREATE TABLE fts_update_docs_{connection} (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"
                        ),
                        params: vec![],
                    });
                    items.push(WorkItem {
                        sql: format!(
                            "CREATE INDEX fts_update_docs_{connection}_idx ON fts_update_docs_{connection} USING fts (body)"
                        ),
                        params: vec![],
                    });
                }
                (Phase::Setup, vec![items])
            }
            InternalPhase::Seed => {
                let remaining = SEED_ROWS_PER_INDEX - self.seed_offset;
                let batch = remaining.min(SEED_BATCH_SIZE);
                let mut items = Vec::with_capacity(batch);
                for i in 0..batch {
                    let id = self.seed_offset + i;
                    let term = TERMS[(id + self.seed_connection * 3) % TERMS.len()];
                    items.push(WorkItem {
                        sql: format!(
                            "INSERT INTO fts_update_docs_{} (id, body) VALUES (?, ?)",
                            self.seed_connection
                        ),
                        params: vec![
                            turso::Value::Integer(id as i64),
                            turso::Value::Text(format!("{term} initial document {id}")),
                        ],
                    });
                }

                self.seed_offset += batch;
                if self.seed_offset >= SEED_ROWS_PER_INDEX {
                    self.seed_connection += 1;
                    self.seed_offset = 0;
                    if self.seed_connection >= self.configured_connections {
                        self.phase = InternalPhase::Run;
                    }
                }
                (Phase::Setup, vec![items])
            }
            InternalPhase::Run => {
                if self.current_iteration >= self.iterations {
                    return (Phase::Done, vec![]);
                }

                let mut batches = Vec::with_capacity(self.configured_connections);
                for connection in 0..self.configured_connections {
                    let mut items = Vec::with_capacity(self.batch_size);
                    for item in 0..self.batch_size {
                        let id = (self.current_iteration * self.batch_size
                            + item * 31
                            + connection * 17)
                            % SEED_ROWS_PER_INDEX;
                        let term =
                            TERMS[(self.current_iteration + item + connection) % TERMS.len()];
                        items.push(WorkItem {
                            sql: format!(
                                "UPDATE fts_update_docs_{connection} SET body = ? WHERE id = ?"
                            ),
                            params: vec![
                                turso::Value::Text(format!(
                                    "{term} updated generation {} document {id}",
                                    self.current_iteration
                                )),
                                turso::Value::Integer(id as i64),
                            ],
                        });
                    }
                    batches.push(items);
                }

                self.current_iteration += 1;
                (Phase::Run, batches)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn four_connections_update_distinct_indexes() {
        let mut profile = FtsUpdateChurn::new(1, 3);
        let (phase, schema_batches) = profile.next_batch(4);
        assert_eq!(phase, Phase::Setup);
        assert_eq!(schema_batches[0].len(), 8);

        loop {
            let (phase, batches) = profile.next_batch(4);
            if phase == Phase::Run {
                assert_eq!(batches.len(), 4);
                for (connection, batch) in batches.iter().enumerate() {
                    assert_eq!(batch.len(), 3);
                    assert!(batch.iter().all(|item| {
                        item.sql
                            .starts_with(&format!("UPDATE fts_update_docs_{connection} "))
                    }));
                }
                break;
            }
        }
    }
}
