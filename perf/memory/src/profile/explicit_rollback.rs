use super::{Phase, Profile, WorkItem};

/// Mirrors `create-index`'s shape but exits via `ROLLBACK` instead of
/// `COMMIT`. Used to measure Step 1 of `MVCC_COMMIT_MEMORY_NEXT.md`:
/// the rollback path's clone of `tx.write_set`.
///
/// Each Run iteration is a single transaction that inserts `batch_size`
/// rows, then the wrapping `BEGIN .. ROLLBACK` (driven by `main.rs` via
/// `run_terminator()`) discards them. Iterations are independent — every
/// rollback returns the table to empty before the next iteration.
pub struct ExplicitRollback {
    iterations: usize,
    rows_per_iter: usize,
    table_created: bool,
    iter_done: usize,
}

impl ExplicitRollback {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            rows_per_iter: batch_size,
            table_created: false,
            iter_done: 0,
        }
    }
}

impl Profile for ExplicitRollback {
    fn name(&self) -> &str {
        "explicit-rollback"
    }

    fn run_terminator(&self) -> &str {
        "ROLLBACK"
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

        if self.iter_done >= self.iterations {
            return (Phase::Done, vec![]);
        }
        self.iter_done += 1;

        let mut items = Vec::with_capacity(self.rows_per_iter);
        for j in 0..self.rows_per_iter {
            // Same row shape as `create-index`: pseudo-random val, fixed-width
            // 100-char payload. Keeps per-row record size predictable so
            // measurements compare cleanly against the canonical workload.
            let val: i64 = (j as i64).wrapping_mul(2_654_435_761) & 0x7fff_ffff;
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
        (Phase::Run, vec![items])
    }
}
