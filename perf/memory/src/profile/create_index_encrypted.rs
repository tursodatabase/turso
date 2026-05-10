use super::{Phase, Profile, WorkItem};

/// Same workload shape as `create-index`, but with encryption enabled
/// via `PRAGMA cipher` + `PRAGMA hexkey` on every connection. Used to
/// confirm Step 3 of `MVCC_COMMIT_MEMORY_NEXT.md` (chunked plaintext
/// output) doesn't regress the encrypted branch — encrypted writes
/// keep the per-tx scratch buffer because AAD construction depends on
/// `op_count` and `payload_size`, both unknown until end-of-walk.
pub struct CreateIndexEncrypted {
    iterations: usize,
    rows: usize,
    table_created: bool,
    populate_cursor: usize,
    iter_done: usize,
}

const POPULATE_CHUNK: usize = 1000;
/// Fixed key — the workload doesn't care which key is used, only that
/// the connection negotiates the encrypted page handler. Same key as
/// `tests/integration/query_processing/test_vacuum.rs::test_plain_vacuum_encrypted`.
const HEXKEY: &str = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
const CIPHER: &str = "aegis256";

impl CreateIndexEncrypted {
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

impl Profile for CreateIndexEncrypted {
    fn name(&self) -> &str {
        "create-index-encrypted"
    }

    fn wraps_run_in_tx(&self) -> bool {
        // Match the canonical `create-index`: DDL doesn't fit inside
        // `BEGIN CONCURRENT`, and autocommit is what real CREATE INDEX
        // usage does.
        false
    }

    fn connection_pragmas(&self) -> Vec<String> {
        vec![
            format!("PRAGMA cipher = '{CIPHER}'"),
            format!("PRAGMA hexkey = '{HEXKEY}'"),
        ]
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
