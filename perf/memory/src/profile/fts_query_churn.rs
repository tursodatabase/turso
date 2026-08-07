use super::{Phase, Profile, WorkItem};

const SEED_ROWS: usize = 2_000;
const QUERY_TERMS: [&str; 8] = [
    "database",
    "transaction",
    "storage",
    "index",
    "replication",
    "search",
    "rust",
    "sqlite",
];

/// Repeated full-text queries over a fixed FTS index.
///
/// Multiple connections intentionally query the same index. This makes the
/// profile sensitive to snapshot reader and immutable index cache
/// multiplication without introducing writer conflicts.
pub struct FtsQueryChurn {
    iterations: usize,
    batch_size: usize,
    current_iteration: usize,
    phase: InternalPhase,
    seed_offset: usize,
}

enum InternalPhase {
    CreateSchema,
    Seed,
    Run,
}

impl FtsQueryChurn {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            batch_size,
            current_iteration: 0,
            phase: InternalPhase::CreateSchema,
            seed_offset: 0,
        }
    }
}

impl Profile for FtsQueryChurn {
    fn name(&self) -> &str {
        "fts-query-churn"
    }

    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        match self.phase {
            InternalPhase::CreateSchema => {
                self.phase = InternalPhase::Seed;
                (
                    Phase::Setup,
                    vec![vec![
                        WorkItem {
                            sql: "CREATE TABLE fts_query_docs (id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL)".to_string(),
                            params: vec![],
                        },
                        WorkItem {
                            sql: "CREATE INDEX fts_query_docs_idx ON fts_query_docs USING fts (title, body)".to_string(),
                            params: vec![],
                        },
                    ]],
                )
            }
            InternalPhase::Seed => {
                let remaining = SEED_ROWS - self.seed_offset;
                let batch = remaining.min(500);
                let mut items = Vec::with_capacity(batch);
                for i in 0..batch {
                    let id = self.seed_offset + i;
                    let primary_term = QUERY_TERMS[id % QUERY_TERMS.len()];
                    let secondary_term = QUERY_TERMS[(id * 5 + 3) % QUERY_TERMS.len()];
                    items.push(WorkItem {
                        sql: "INSERT INTO fts_query_docs (id, title, body) VALUES (?, ?, ?)"
                            .to_string(),
                        params: vec![
                            turso::Value::Integer(id as i64),
                            turso::Value::Text(format!("{primary_term} document {id}")),
                            turso::Value::Text(format!(
                                "{primary_term} {secondary_term} benchmark payload {id}"
                            )),
                        ],
                    });
                }
                self.seed_offset += batch;
                if self.seed_offset >= SEED_ROWS {
                    self.phase = InternalPhase::Run;
                }
                (Phase::Setup, vec![items])
            }
            InternalPhase::Run => {
                if self.current_iteration >= self.iterations {
                    return (Phase::Done, vec![]);
                }

                let connection_count = connections.max(1);
                let mut batches = Vec::with_capacity(connection_count);
                for connection in 0..connection_count {
                    let mut items = Vec::with_capacity(self.batch_size);
                    for item in 0..self.batch_size {
                        let term =
                            QUERY_TERMS[(self.current_iteration + connection * 3 + item * 5)
                                % QUERY_TERMS.len()];
                        items.push(WorkItem {
                            sql: format!(
                                "SELECT id FROM fts_query_docs WHERE fts_match(title, body, '{term}') ORDER BY id LIMIT 50"
                            ),
                            params: vec![],
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
    fn four_connections_query_the_same_index() {
        let mut profile = FtsQueryChurn::new(1, 3);

        loop {
            let (phase, batches) = profile.next_batch(4);
            if phase == Phase::Run {
                assert_eq!(batches.len(), 4);
                assert!(batches.iter().all(|batch| batch.len() == 3));
                assert!(batches.iter().flatten().all(|item| {
                    item.sql.contains("FROM fts_query_docs")
                        && item.sql.contains("fts_match(title, body")
                }));
                break;
            }
        }
    }
}
