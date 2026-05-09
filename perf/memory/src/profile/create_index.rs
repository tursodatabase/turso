use super::{Phase, Profile, WorkItem};

/// CREATE INDEX on a populated table — the workload that motivated the
/// MVCC commit-log refactor (`MVCC_COMMIT_IMPL_STEPS_V2.md`). Exercises a
/// single transaction whose log frame contains every index entry.
///
/// `--batch-size N` controls the table size (rows seeded during Setup).
/// `--iterations K` controls how many CREATE-INDEX commits to run during
/// the measured Run phase. Each Run iteration is a self-contained
/// transaction `BEGIN[ CONCURRENT]; DROP INDEX IF EXISTS idx_val; CREATE
/// INDEX idx_val ON t(val); COMMIT`. The first iteration's DROP is a
/// no-op; later iterations let dhat sample the per-commit peak across
/// multiple CREATE-INDEX bursts.
pub struct CreateIndex {
    iterations: usize,
    rows: usize,
    table_created: bool,
    populate_cursor: usize,
    iter_done: usize,
}

const POPULATE_CHUNK: usize = 1000;

impl CreateIndex {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            rows: batch_size,
            table_created: false,
            populate_cursor: 0,
            iter_done: 0,
        }
    }
}

impl Profile for CreateIndex {
    fn name(&self) -> &str {
        "create-index"
    }

    fn wraps_run_in_tx(&self) -> bool {
        // MVCC's `BEGIN CONCURRENT` rejects DDL ("requires an exclusive
        // transaction"), and autocommit is what `create_index_benchmark.rs`
        // and real CREATE INDEX usage do. Each WorkItem runs as its own
        // implicit tx.
        false
    }

    fn next_batch(&mut self, _connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        if !self.table_created {
            self.table_created = true;
            return (
                Phase::Setup,
                vec![vec![WorkItem {
                    sql: "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)"
                        .to_string(),
                    params: vec![],
                }]],
            );
        }

        if self.populate_cursor < self.rows {
            let end = (self.populate_cursor + POPULATE_CHUNK).min(self.rows);
            let mut items = Vec::with_capacity(end - self.populate_cursor);
            for j in self.populate_cursor..end {
                // Pseudo-random val so index keys are not pre-sorted; matches
                // the canonical `core/benches/create_index_benchmark.rs` mix.
                let val: i64 = (j as i64).wrapping_mul(2_654_435_761) & 0x7fff_ffff;
                // 100-char payload: 8-char prefix + 92-char zero-padded id.
                // Constant width makes per-row record size predictable so the
                // total bytes logged on CREATE INDEX commit scale linearly
                // with the table size, which is what we want to compare across
                // baseline and current runs.
                let payload = format!("payload_{j:0>92}");
                items.push(WorkItem {
                    sql: "INSERT INTO t (id, val, payload) VALUES (?, ?, ?)".to_string(),
                    params: vec![
                        turso::Value::Integer(j as i64),
                        turso::Value::Integer(val),
                        turso::Value::Text(payload),
                    ],
                });
            }
            self.populate_cursor = end;
            return (Phase::Setup, vec![items]);
        }

        if self.iter_done < self.iterations {
            self.iter_done += 1;
            return (
                Phase::Run,
                vec![vec![
                    WorkItem {
                        sql: "DROP INDEX IF EXISTS idx_val".to_string(),
                        params: vec![],
                    },
                    WorkItem {
                        sql: "CREATE INDEX idx_val ON t(val)".to_string(),
                        params: vec![],
                    },
                ]],
            );
        }

        (Phase::Done, vec![])
    }
}
