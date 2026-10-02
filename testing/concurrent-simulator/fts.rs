use std::sync::Arc;

use anyhow::Context;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, SqliteDialect, Value};

use crate::chaotic_elle::{ChaoticWorkload, ChaoticWorkloadProfile};
use crate::properties::{FtsResultComparisonProperty, IntegrityCheckProperty, Property};
use crate::workloads::{
    FTS_SIM_INDEX, FTS_SIM_TABLE, FTS_SIM_WORDS, FtsMatchWorkload, fts_sim_schema,
};
use crate::{FiberState, OpResult, Operation, SchemaBias, TxMode, Whopper, WhopperOpts};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FtsProfile {
    Merge,
    Snapshots,
    Recovery,
}

impl FtsProfile {
    pub fn from_mode(mode: &str) -> Option<Self> {
        match mode {
            "fts-merge" => Some(Self::Merge),
            "fts-snapshots" => Some(Self::Snapshots),
            "fts-recovery" => Some(Self::Recovery),
            _ => None,
        }
    }

    pub fn options(self, enable_mvcc: bool) -> WhopperOpts {
        let mut schema = fts_sim_schema();
        for id in 0..16 {
            schema.push((
                FTS_SIM_TABLE.to_owned(),
                format!(
                    "INSERT INTO {FTS_SIM_TABLE} VALUES ({id}, 'alpha {}')",
                    FTS_SIM_WORDS[1 + id as usize % 7]
                ),
            ));
        }
        WhopperOpts {
            enable_mvcc,
            experimental_mvcc_passive_checkpoint: enable_mvcc,
            fts_profile: Some(self),
            schema_bias: SchemaBias {
                num_tables_range: 0..=0,
                ..SchemaBias::default()
            },
            elle_tables: schema,
            workloads: vec![(1, Box::new(FtsMatchWorkload))],
            properties: vec![
                Box::new(FtsResultComparisonProperty),
                Box::new(IntegrityCheckProperty),
            ],
            chaotic_profiles: vec![(
                0.9,
                "fts",
                Box::new(FtsWorkloadProfile {
                    profile: self,
                    enable_mvcc,
                }),
            )],
            checkpoint_probe_probability: 0.0,
            disable_mvcc_auto_checkpoint: enable_mvcc && self == Self::Recovery,
            ..WhopperOpts::default()
        }
    }
}

struct FtsWorkloadProfile {
    profile: FtsProfile,
    enable_mvcc: bool,
}

impl ChaoticWorkloadProfile for FtsWorkloadProfile {
    fn generate(&self, mut rng: ChaCha8Rng, fiber_id: usize) -> Box<dyn ChaoticWorkload> {
        let mut operations = vec![Operation::Execute {
            sql: format!("PRAGMA fts_merge_threshold = {}", [0, 2, 4][fiber_id % 3]),
        }];
        let mut result_comparisons = Vec::new();
        if self.profile == FtsProfile::Snapshots && fiber_id == 0 {
            operations.push(Operation::Begin {
                mode: if self.enable_mvcc {
                    TxMode::Concurrent
                } else {
                    TxMode::Deferred
                },
            });
            let word = FTS_SIM_WORDS[rng.random_range(0..FTS_SIM_WORDS.len())];
            let expected_op_index = operations.len();
            operations.push(Operation::Select {
                sql: format!(
                    "SELECT id, body FROM {FTS_SIM_TABLE} \
                     WHERE (' '||body||' ') LIKE '% {word} %' ORDER BY id"
                ),
            });
            for _ in 0..rng.random_range(24..48) {
                result_comparisons.push((operations.len(), expected_op_index));
                operations.push(Operation::Select {
                    sql: format!(
                        "SELECT id, body FROM {FTS_SIM_TABLE} \
                         WHERE fts_match(body, '{word}') ORDER BY id"
                    ),
                });
            }
            operations.push(Operation::Commit);
        } else {
            operations.push(Operation::Begin {
                mode: if self.enable_mvcc {
                    TxMode::Concurrent
                } else {
                    TxMode::Immediate
                },
            });
            let id = rng.random_range(0..32);
            let old_word = FTS_SIM_WORDS[rng.random_range(0..FTS_SIM_WORDS.len())];
            let new_word = FTS_SIM_WORDS[rng.random_range(0..FTS_SIM_WORDS.len())];
            operations.push(Operation::Execute {
                sql: format!(
                    "INSERT OR REPLACE INTO {FTS_SIM_TABLE} VALUES ({}, 'hotel {old_word}')",
                    32 + fiber_id
                ),
            });
            let expected_op_index = operations.len();
            operations.push(Operation::Select {
                sql: format!("SELECT id, body FROM {FTS_SIM_TABLE} ORDER BY id"),
            });
            let savepoint = format!("fts_sp_{fiber_id}");
            operations.push(Operation::Savepoint {
                name: savepoint.clone(),
            });
            if rng.random_bool(0.5) {
                operations.push(Operation::Execute {
                    sql: format!("DELETE FROM {FTS_SIM_TABLE} WHERE id = {id}"),
                });
                operations.push(Operation::Execute {
                    sql: format!("INSERT INTO {FTS_SIM_TABLE} VALUES ({id}, 'alpha {old_word}')"),
                });
            } else {
                operations.push(Operation::Execute {
                    sql: format!(
                        "INSERT OR REPLACE INTO {FTS_SIM_TABLE} VALUES ({id}, 'alpha {old_word}')"
                    ),
                });
            }
            operations.push(Operation::Execute {
                sql: format!("OPTIMIZE INDEX {FTS_SIM_INDEX}"),
            });
            operations.push(Operation::Execute {
                sql: format!(
                    "UPDATE {FTS_SIM_TABLE} SET body = 'bravo {new_word}' WHERE id = {id}"
                ),
            });
            if rng.random_bool(0.5) {
                operations.push(Operation::RollbackToSavepoint {
                    name: savepoint.clone(),
                });
                result_comparisons.push((operations.len(), expected_op_index));
                operations.push(operations[expected_op_index].clone());
            }
            operations.push(Operation::ReleaseSavepoint { name: savepoint });
            for &word in FTS_SIM_WORDS {
                operations.push(Operation::CompareFtsResults {
                    word: word.to_owned(),
                });
            }
            operations.push(if rng.random_bool(0.2) {
                Operation::Rollback
            } else {
                Operation::Commit
            });
            if rng.random_bool(0.5) {
                operations.push(Operation::WalCheckpoint {
                    mode: "PASSIVE".to_owned(),
                });
            }
        }
        for &word in FTS_SIM_WORDS {
            operations.push(Operation::CompareFtsResults {
                word: word.to_owned(),
            });
        }
        Box::new(FtsWorkload {
            results: vec![None; operations.len()],
            operations,
            result_comparisons,
            next_op_index: 0,
        })
    }
}

struct FtsWorkload {
    operations: Vec<Operation>,
    results: Vec<Option<Vec<Vec<Value>>>>,
    result_comparisons: Vec<(usize, usize)>,
    next_op_index: usize,
}

impl ChaoticWorkload for FtsWorkload {
    fn next(&mut self, result: Option<OpResult>) -> Option<Operation> {
        match result {
            Some(Err(_)) => return None,
            Some(Ok(rows)) => {
                let completed_op_index = self.next_op_index - 1;
                for &(actual_op_index, expected_op_index) in &self.result_comparisons {
                    if actual_op_index == completed_op_index {
                        assert_eq!(
                            Some(&rows),
                            self.results[expected_op_index].as_ref(),
                            "FTS results changed in an old reader or after savepoint rollback: {}",
                            self.operations[completed_op_index].sql()
                        );
                    }
                }
                self.results[completed_op_index] = Some(rows);
            }
            None => {}
        }
        let op = self.operations.get(self.next_op_index)?.clone();
        self.next_op_index += 1;
        Some(op)
    }
}

impl Whopper {
    pub(crate) fn step_fts_faults(&mut self, fiber_idx: usize) -> anyhow::Result<()> {
        if self.fts_profile != Some(FtsProfile::Recovery)
            || !self.context.fibers[fiber_idx]
                .statement
                .borrow()
                .as_ref()
                .is_some_and(|stmt| stmt.is_busy())
        {
            return Ok(());
        }
        let op = self.context.fibers[fiber_idx].current_op.as_ref();
        let writes = matches!(
            op,
            Some(Operation::Execute { .. } | Operation::Commit | Operation::WalCheckpoint { .. })
        );
        if writes && self.rng.random_bool(0.02) {
            self.check_fts_recovery_copy().with_context(|| {
                format!(
                    "FTS recovery copy: seed={} step={} fiber={fiber_idx}",
                    self.seed, self.current_step
                )
            })?;
            self.stats.fts_recovery_copies += 1;
        }
        if writes && self.rng.random_bool(0.01) {
            let fiber = &mut self.context.fibers[fiber_idx];
            fiber.statement.borrow_mut().take().unwrap().reset()?;
            for property in &self.properties {
                property
                    .lock()
                    .unwrap()
                    .abort_fiber(fiber_idx, fiber.txn_id)?;
            }
            fiber.rows.clear();
            fiber.execution_id = None;
            fiber.chaotic_workload = None;
            fiber.last_chaotic_result = None;
            fiber.current_op = if fiber.connection.get_auto_commit() {
                fiber.state = FiberState::Idle;
                fiber.txn_id = None;
                None
            } else {
                let execution_id = self.context.state.gen_execution_id();
                fiber
                    .statement
                    .replace(Some(fiber.connection.prepare("ROLLBACK")?));
                fiber.execution_id = Some(execution_id);
                for property in &self.properties {
                    property.lock().unwrap().init_op(
                        self.current_step,
                        fiber_idx,
                        fiber.txn_id,
                        execution_id,
                        &Operation::Rollback,
                    )?;
                }
                Some(Operation::Rollback)
            };
            self.stats.fts_canceled_statements += 1;
        }
        Ok(())
    }

    fn check_fts_recovery_copy(&self) -> anyhow::Result<Vec<Vec<Value>>> {
        let files = self.io.db_file_bytes();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("recovered.db");
        for (suffix, bytes) in files {
            let file = if suffix == ".db" {
                path.clone()
            } else {
                directory.path().join(format!("recovered.db{suffix}"))
            };
            std::fs::write(file, bytes)?;
        }
        let io = Arc::new(turso_core::PlatformIO::new()?);
        let database = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new()
                .with_index_method(true)
                .with_experimental_mvcc_passive_checkpoint(
                    self.experimental_mvcc_passive_checkpoint,
                ),
            None,
            Arc::new(SqliteDialect),
        )?;
        let connection = database.connect()?;
        check_fts_connection(&connection, self.context.enable_mvcc)?;
        let rows = query(
            &connection,
            &format!("SELECT id, body FROM {FTS_SIM_TABLE} ORDER BY id"),
        )?;
        connection.close()?;
        Ok(rows)
    }

    pub(crate) fn check_fts_after_reopen(&self) -> anyhow::Result<()> {
        if self.fts_profile.is_some() {
            check_fts_connection(&self.context.fibers[0].connection, self.context.enable_mvcc)?;
        }
        Ok(())
    }
}

fn check_fts_connection(connection: &Arc<Connection>, enable_mvcc: bool) -> anyhow::Result<()> {
    connection.execute(if enable_mvcc {
        "BEGIN CONCURRENT"
    } else {
        "BEGIN DEFERRED"
    })?;
    for &word in FTS_SIM_WORDS {
        let op = Operation::CompareFtsResults {
            word: word.to_owned(),
        };
        let rows = query(connection, &op.sql())?;
        FtsResultComparisonProperty.finish_op(0, 0, None, 0, 0, &op, &Ok(rows))?;
    }
    let op = Operation::IntegrityCheck;
    let rows = query(connection, &op.sql())?;
    IntegrityCheckProperty.finish_op(0, 0, None, 0, 0, &op, &Ok(rows))?;
    connection.execute("COMMIT")?;
    Ok(())
}

fn query(connection: &Arc<Connection>, sql: &str) -> anyhow::Result<Vec<Vec<Value>>> {
    let mut stmt = connection.prepare(sql)?;
    let mut rows = Vec::new();
    for _ in 0..1_000_000 {
        match stmt.step()? {
            turso_core::StepResult::Row => {
                rows.push(stmt.row().unwrap().get_values().cloned().collect())
            }
            turso_core::StepResult::Done => return Ok(rows),
            turso_core::StepResult::IO | turso_core::StepResult::Yield => {
                stmt.get_pager().io.step()?
            }
            other => anyhow::bail!("FTS query did not finish: {other:?}: {sql}"),
        }
    }
    anyhow::bail!("FTS query did not finish after 1000000 steps: {sql}")
}

pub(crate) struct FtsCacheOverride(bool);

impl FtsCacheOverride {
    pub(crate) fn new(disabled: bool) -> Self {
        if disabled {
            turso_core::index_method::fts::set_fts_retained_cache_bytes_for_test(Some(0));
        }
        Self(disabled)
    }
}

impl Drop for FtsCacheOverride {
    fn drop(&mut self) {
        if self.0 {
            turso_core::index_method::fts::set_fts_retained_cache_bytes_for_test(None);
        }
    }
}

impl crate::Stats {
    pub(crate) fn record_fts_op(&mut self, op: &Operation) {
        match op {
            Operation::Execute { sql } if sql == &format!("OPTIMIZE INDEX {FTS_SIM_INDEX}") => {
                self.fts_optimize_statements += 1
            }
            Operation::Select { sql } if sql.contains("fts_match") => self.fts_old_view_reads += 1,
            Operation::RollbackToSavepoint { .. } => self.fts_savepoint_rollbacks += 1,
            Operation::Commit => self.fts_commits += 1,
            Operation::WalCheckpoint { .. } => self.fts_checkpoints += 1,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    #[test]
    fn recovery_copy_keeps_committed_rows_and_excludes_uncommitted_rows() {
        for enable_mvcc in [false, true] {
            let whopper =
                Whopper::new(FtsProfile::Recovery.options(enable_mvcc).with_seed(811)).unwrap();
            let connection = &whopper.context.fibers[0].connection;
            connection
                .execute("INSERT INTO fts_docs VALUES (71, 'alpha hotel')")
                .unwrap();
            let committed_rows =
                query(connection, "SELECT id, body FROM fts_docs ORDER BY id").unwrap();
            connection
                .execute(if enable_mvcc {
                    "BEGIN CONCURRENT"
                } else {
                    "BEGIN IMMEDIATE"
                })
                .unwrap();
            connection
                .execute("DELETE FROM fts_docs WHERE id = 0")
                .unwrap();
            connection
                .execute("INSERT INTO fts_docs VALUES (72, 'bravo hotel')")
                .unwrap();
            connection.execute("OPTIMIZE INDEX fts_docs_fts").unwrap();
            let rows = whopper.check_fts_recovery_copy().unwrap();
            assert_eq!(rows, committed_rows);
            assert!(!connection.get_auto_commit());
            connection.execute("ROLLBACK").unwrap();
            assert_eq!(whopper.check_fts_recovery_copy().unwrap(), committed_rows);
        }
    }

    #[test]
    fn old_reader_keeps_fts_rows_after_another_connection_commits() {
        for enable_mvcc in [false, true] {
            let whopper =
                Whopper::new(FtsProfile::Snapshots.options(enable_mvcc).with_seed(813)).unwrap();
            let _cache_override = FtsCacheOverride::new(true);
            let reader = &whopper.context.fibers[0].connection;
            let writer = &whopper.context.fibers[1].connection;
            reader
                .execute(if enable_mvcc {
                    "BEGIN CONCURRENT"
                } else {
                    "BEGIN DEFERRED"
                })
                .unwrap();
            let expected = query(
                reader,
                "SELECT id, body FROM fts_docs WHERE (' '||body||' ') LIKE '% alpha %' ORDER BY id",
            )
            .unwrap();
            assert!(expected.iter().any(|row| row[0].as_int() == Some(0)));
            writer
                .execute(if enable_mvcc {
                    "BEGIN CONCURRENT"
                } else {
                    "BEGIN IMMEDIATE"
                })
                .unwrap();
            writer
                .execute("UPDATE fts_docs SET body = 'bravo hotel' WHERE id = 0")
                .unwrap();
            writer.execute("OPTIMIZE INDEX fts_docs_fts").unwrap();
            writer.execute("COMMIT").unwrap();
            let sql = "SELECT id, body FROM fts_docs WHERE fts_match(body, 'alpha') ORDER BY id";
            assert_eq!(query(reader, sql).unwrap(), expected);
            reader.execute("COMMIT").unwrap();
            assert!(
                !query(reader, sql)
                    .unwrap()
                    .iter()
                    .any(|row| row[0].as_int() == Some(0))
            );
        }
    }

    #[test]
    fn reopen_reports_fts_result_check_failures_from_unfinished_statements() {
        let mut whopper = Whopper::new(FtsProfile::Merge.options(false).with_seed(812)).unwrap();
        let fiber = &mut whopper.context.fibers[0];
        fiber.current_op = Some(Operation::CompareFtsResults {
            word: "alpha".to_owned(),
        });
        fiber.execution_id = Some(1);
        fiber.statement.replace(Some(
            fiber.connection.prepare("SELECT 1, 1, NULL, NULL").unwrap(),
        ));
        let error = whopper.reopen().unwrap_err();
        assert!(error.to_string().contains("row count difference Some(1)"));
    }

    #[test]
    fn profiles_exercise_fts_in_wal_and_mvcc() {
        for profile in [
            FtsProfile::Merge,
            FtsProfile::Snapshots,
            FtsProfile::Recovery,
        ] {
            for enable_mvcc in [false, true] {
                let opts = profile
                    .options(enable_mvcc)
                    .with_seed(8940 + profile as u64)
                    .with_max_steps(6000);
                let mut whopper = Whopper::new(opts).unwrap();
                whopper.run().unwrap();
                let connection = &whopper.context.fibers[0].connection;
                assert_eq!(
                    query(connection, "PRAGMA journal_mode").unwrap(),
                    vec![vec![Value::build_text(if enable_mvcc {
                        "mvcc"
                    } else {
                        "wal"
                    })]]
                );
                assert!(
                    whopper.stats.fts_checks > 0,
                    "{profile:?} mvcc={enable_mvcc}"
                );
                assert!(
                    whopper.stats.fts_optimize_statements > 0,
                    "{profile:?} mvcc={enable_mvcc}"
                );
                assert!(
                    whopper.stats.fts_commits > 0,
                    "{profile:?} mvcc={enable_mvcc}"
                );
                assert!(
                    whopper.stats.fts_savepoint_rollbacks > 0,
                    "{profile:?} mvcc={enable_mvcc}"
                );
                if profile == FtsProfile::Snapshots {
                    assert!(whopper.stats.fts_old_view_reads > 0);
                }
                if profile == FtsProfile::Recovery {
                    assert!(whopper.stats.fts_recovery_copies > 0);
                    assert!(whopper.stats.fts_canceled_statements > 0);
                }
            }
        }
    }

    #[test]
    fn old_reader_workload_rejects_changed_rows() {
        let mut workload = FtsWorkloadProfile {
            profile: FtsProfile::Snapshots,
            enable_mvcc: false,
        }
        .generate(ChaCha8Rng::seed_from_u64(7), 0);
        assert!(workload.next(None).is_some());
        assert!(workload.next(Some(Ok(vec![]))).is_some());
        assert!(workload.next(Some(Ok(vec![]))).is_some());
        assert!(
            workload
                .next(Some(Ok(vec![vec![Value::from_i64(7)]])))
                .is_some()
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            workload.next(Some(Ok(vec![vec![Value::from_i64(11)]])))
        }));
        assert!(result.is_err());
    }
}
