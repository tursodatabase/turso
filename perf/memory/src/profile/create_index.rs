use super::{Phase, Profile, WorkItem};

pub struct CreateIndex {
    iterations: usize,
    batch_size: usize,
    phase: InternalPhase,
    seed_offset: usize,
}

enum InternalPhase {
    CreateTable,
    Seed,
    Run,
    Done,
}

impl CreateIndex {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            batch_size,
            phase: InternalPhase::CreateTable,
            seed_offset: 0,
        }
    }

    fn seed_rows(&self) -> usize {
        self.iterations * self.batch_size
    }
}

impl Profile for CreateIndex {
    fn name(&self) -> &str {
        "create-index"
    }

    fn next_batch(&mut self, _connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        match self.phase {
            InternalPhase::CreateTable => {
                self.phase = InternalPhase::Seed;
                (
                    Phase::Setup,
                    vec![vec![WorkItem {
                        sql: "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, data TEXT NOT NULL, value REAL)".to_string(),
                        params: vec![],
                    }]],
                )
            }
            InternalPhase::Seed => {
                let seed_rows = self.seed_rows();
                if self.seed_offset >= seed_rows {
                    self.phase = InternalPhase::Run;
                    return self.next_batch(_connections);
                }

                let remaining = seed_rows - self.seed_offset;
                let batch = remaining.min(self.batch_size.max(1));
                let mut items = Vec::with_capacity(batch);
                for i in 0..batch {
                    let id = self.seed_offset + i;
                    items.push(WorkItem {
                        sql: "INSERT INTO bench (id, data, value) VALUES (?, ?, ?)".to_string(),
                        params: vec![
                            turso::Value::Integer(id as i64),
                            turso::Value::Text(format!("seed_{id:012}")),
                            turso::Value::Real((id % 10_000) as f64 * 0.5),
                        ],
                    });
                }
                self.seed_offset += batch;
                (Phase::Setup, vec![items])
            }
            InternalPhase::Run => {
                self.phase = InternalPhase::Done;
                (
                    Phase::Run,
                    vec![vec![WorkItem {
                        sql: "CREATE INDEX idx_bench_data_value ON bench(data, value)".to_string(),
                        params: vec![],
                    }]],
                )
            }
            InternalPhase::Done => (Phase::Done, vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_index_seeds_iterations_times_batch_size_rows() {
        let mut profile = CreateIndex::new(2, 3);

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Setup);
        assert_eq!(batches[0].len(), 1);

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Setup);
        assert_eq!(batches[0].len(), 3);

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Setup);
        assert_eq!(batches[0].len(), 3);

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Run);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(
            batches[0][0].sql,
            "CREATE INDEX idx_bench_data_value ON bench(data, value)"
        );

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Done);
        assert!(batches.is_empty());
    }
}
