use super::{Phase, Profile, WorkItem};

/// Insert N rows then DELETE all of them inside a single transaction,
/// then commit. Exercises the `is_delete` sidecar bit and the
/// `Some(TxTimestampOrID::TxID)` end-write path that Step 4 of
/// `MVCC_COMMIT_MEMORY_NEXT.md` (PackedTsOrId) touches most.
///
/// Each Run iteration is one BEGIN/INSERT…/DELETE/COMMIT cycle; the
/// table is empty again at the start of every iteration because every
/// row is deleted before commit.
pub struct DeleteHeavy {
    iterations: usize,
    rows_per_iter: usize,
    table_created: bool,
    iter_done: usize,
}

impl DeleteHeavy {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            rows_per_iter: batch_size,
            table_created: false,
            iter_done: 0,
        }
    }
}

impl Profile for DeleteHeavy {
    fn name(&self) -> &str {
        "delete-heavy"
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

        let mut items = Vec::with_capacity(self.rows_per_iter + 1);
        for j in 0..self.rows_per_iter {
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
        // One bulk DELETE — the engine writes a tombstone version per
        // row, exercising the same end-stamping path as a per-row
        // `DELETE WHERE id = ?` would but with a single statement.
        items.push(WorkItem {
            sql: "DELETE FROM t".to_string(),
            params: vec![],
        });
        (Phase::Run, vec![items])
    }
}
