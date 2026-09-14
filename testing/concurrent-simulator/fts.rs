use std::sync::Arc;

use anyhow::Context;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, SqliteDialect, Value};

use crate::chaotic_elle::{ChaoticWorkload, ChaoticWorkloadProfile};
use crate::properties::{FtsSelfDifferentialProperty, IntegrityCheckProperty, Property};
use crate::workloads::{
    FTS_SIM_INDEX, FTS_SIM_TABLE, FTS_SIM_TOKENS, FtsMatchWorkload, fts_sim_schema,
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

    pub fn options(self) -> WhopperOpts {
        let mut schema = fts_sim_schema();
        for id in 0..16 {
            schema.push((
                FTS_SIM_TABLE.to_owned(),
                format!(
                    "INSERT INTO {FTS_SIM_TABLE} VALUES ({id}, 'alpha {}')",
                    FTS_SIM_TOKENS[1 + id as usize % 7]
                ),
            ));
        }
        WhopperOpts {
            enable_mvcc: true,
            experimental_mvcc_passive_checkpoint: true,
            fts_profile: Some(self),
            schema_bias: SchemaBias {
                num_tables_range: 0..=0,
                ..SchemaBias::default()
            },
            elle_tables: schema,
            workloads: vec![(1, Box::new(FtsMatchWorkload))],
            properties: vec![
                Box::new(FtsSelfDifferentialProperty),
                Box::new(IntegrityCheckProperty),
            ],
            chaotic_profiles: vec![(0.9, "fts", Box::new(self))],
            checkpoint_probe_probability: 0.0,
            disable_mvcc_auto_checkpoint: self == Self::Recovery,
            ..WhopperOpts::default()
        }
    }
}

impl ChaoticWorkloadProfile for FtsProfile {
    fn generate(&self, mut rng: ChaCha8Rng, fiber_id: usize) -> Box<dyn ChaoticWorkload> {
        let mut ops = vec![Operation::Execute {
            sql: format!("PRAGMA fts_merge_threshold = {}", [0, 2, 4][fiber_id % 3]),
        }];
        let mut compare_results = Vec::new();
        if *self == Self::Snapshots && fiber_id == 0 {
            ops.push(Operation::Begin {
                mode: TxMode::Concurrent,
            });
            let token = FTS_SIM_TOKENS[rng.random_range(0..FTS_SIM_TOKENS.len())];
            let baseline = ops.len();
            ops.push(Operation::Select {
                sql: format!(
                    "SELECT id, body FROM {FTS_SIM_TABLE} \
                     WHERE (' '||body||' ') LIKE '% {token} %' ORDER BY id"
                ),
            });
            for _ in 0..rng.random_range(24..48) {
                compare_results.push((ops.len(), baseline));
                ops.push(Operation::Select {
                    sql: format!(
                        "SELECT id, body FROM {FTS_SIM_TABLE} \
                         WHERE fts_match(body, '{token}') ORDER BY id"
                    ),
                });
            }
            ops.push(Operation::Commit);
        } else {
            ops.push(Operation::Begin {
                mode: TxMode::Concurrent,
            });
            let id = rng.random_range(0..32);
            let old_token = FTS_SIM_TOKENS[rng.random_range(0..FTS_SIM_TOKENS.len())];
            let new_token = FTS_SIM_TOKENS[rng.random_range(0..FTS_SIM_TOKENS.len())];
            ops.push(Operation::Execute {
                sql: format!(
                    "INSERT OR REPLACE INTO {FTS_SIM_TABLE} VALUES ({}, 'hotel {old_token}')",
                    32 + fiber_id
                ),
            });
            let baseline = ops.len();
            ops.push(Operation::Select {
                sql: format!("SELECT id, body FROM {FTS_SIM_TABLE} ORDER BY id"),
            });
            let savepoint = format!("fts_sp_{fiber_id}");
            ops.push(Operation::Savepoint {
                name: savepoint.clone(),
            });
            if rng.random_bool(0.5) {
                ops.push(Operation::Execute {
                    sql: format!("DELETE FROM {FTS_SIM_TABLE} WHERE id = {id}"),
                });
                ops.push(Operation::Execute {
                    sql: format!("INSERT INTO {FTS_SIM_TABLE} VALUES ({id}, 'alpha {old_token}')"),
                });
            } else {
                ops.push(Operation::Execute {
                    sql: format!(
                        "INSERT OR REPLACE INTO {FTS_SIM_TABLE} VALUES ({id}, 'alpha {old_token}')"
                    ),
                });
            }
            ops.push(Operation::Execute {
                sql: format!("OPTIMIZE INDEX {FTS_SIM_INDEX}"),
            });
            ops.push(Operation::Execute {
                sql: format!(
                    "UPDATE {FTS_SIM_TABLE} SET body = 'bravo {new_token}' WHERE id = {id}"
                ),
            });
            if rng.random_bool(0.5) {
                ops.push(Operation::RollbackToSavepoint {
                    name: savepoint.clone(),
                });
                compare_results.push((ops.len(), baseline));
                ops.push(ops[baseline].clone());
            }
            ops.push(Operation::ReleaseSavepoint { name: savepoint });
            for &token in FTS_SIM_TOKENS {
                ops.push(Operation::FtsMatchDifferential {
                    token: token.to_owned(),
                });
            }
            ops.push(if rng.random_bool(0.2) {
                Operation::Rollback
            } else {
                Operation::Commit
            });
            if rng.random_bool(0.5) {
                ops.push(Operation::WalCheckpoint {
                    mode: "PASSIVE".to_owned(),
                });
            }
        }
        for &token in FTS_SIM_TOKENS {
            ops.push(Operation::FtsMatchDifferential {
                token: token.to_owned(),
            });
        }
        Box::new(FtsWorkload {
            results: vec![None; ops.len()],
            ops,
            compare_results,
            index: 0,
        })
    }
}

struct FtsWorkload {
    ops: Vec<Operation>,
    results: Vec<Option<Vec<Vec<Value>>>>,
    compare_results: Vec<(usize, usize)>,
    index: usize,
}

impl ChaoticWorkload for FtsWorkload {
    fn next(&mut self, result: Option<OpResult>) -> Option<Operation> {
        match result {
            Some(Err(_)) => return None,
            Some(Ok(rows)) => {
                let completed = self.index - 1;
                for &(check, baseline) in &self.compare_results {
                    if check == completed {
                        assert_eq!(
                            Some(&rows),
                            self.results[baseline].as_ref(),
                            "FTS snapshot/rollback changed results: {}",
                            self.ops[completed].sql()
                        );
                    }
                }
                self.results[completed] = Some(rows);
            }
            None => {}
        }
        let op = self.ops.get(self.index)?.clone();
        self.index += 1;
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
            self.check_fts_crash_recovery().with_context(|| {
                format!(
                    "FTS crash snapshot: seed={} step={} fiber={fiber_idx}",
                    self.seed, self.current_step
                )
            })?;
            self.stats.fts_crash_checks += 1;
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
            self.stats.fts_abandoned_statements += 1;
        }
        Ok(())
    }

    fn check_fts_crash_recovery(&self) -> anyhow::Result<Vec<Vec<Value>>> {
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
                .with_experimental_mvcc_passive_checkpoint(true),
            None,
            Arc::new(SqliteDialect),
        )?;
        let connection = database.connect()?;
        check_fts_connection(&connection)?;
        let rows = query(
            &connection,
            &format!("SELECT id, body FROM {FTS_SIM_TABLE} ORDER BY id"),
        )?;
        connection.close()?;
        Ok(rows)
    }

    pub(crate) fn check_fts_after_reopen(&self) -> anyhow::Result<()> {
        if self.fts_profile.is_some() {
            check_fts_connection(&self.context.fibers[0].connection)?;
        }
        Ok(())
    }
}

fn check_fts_connection(connection: &Arc<Connection>) -> anyhow::Result<()> {
    connection.execute("BEGIN CONCURRENT")?;
    for &token in FTS_SIM_TOKENS {
        let op = Operation::FtsMatchDifferential {
            token: token.to_owned(),
        };
        let rows = query(connection, &op.sql())?;
        FtsSelfDifferentialProperty.finish_op(0, 0, None, 0, 0, &op, &Ok(rows))?;
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
            other => anyhow::bail!("FTS verification did not complete: {other:?}: {sql}"),
        }
    }
    anyhow::bail!("FTS verification exceeded its step budget: {sql}")
}

pub(crate) struct FtsCacheBudget(bool);

impl FtsCacheBudget {
    pub(crate) fn new(disabled: bool) -> Self {
        if disabled {
            turso_core::index_method::fts::set_fts_retained_cache_bytes_for_test(Some(0));
        }
        Self(disabled)
    }
}

impl Drop for FtsCacheBudget {
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
                self.fts_optimizes += 1
            }
            Operation::Select { sql } if sql.contains("fts_match") => self.fts_snapshot_reads += 1,
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
    fn crash_snapshot_keeps_committed_rows_and_excludes_uncommitted_rows() {
        let whopper = Whopper::new(FtsProfile::Recovery.options().with_seed(811)).unwrap();
        let connection = &whopper.context.fibers[0].connection;
        connection
            .execute("INSERT INTO fts_docs VALUES (71, 'alpha hotel')")
            .unwrap();
        connection.execute("BEGIN CONCURRENT").unwrap();
        connection
            .execute("DELETE FROM fts_docs WHERE id = 0")
            .unwrap();
        connection
            .execute("INSERT INTO fts_docs VALUES (72, 'bravo hotel')")
            .unwrap();
        connection.execute("OPTIMIZE INDEX fts_docs_fts").unwrap();
        let rows = whopper.check_fts_crash_recovery().unwrap();
        assert!(rows.iter().any(|row| row[0].as_int() == Some(0)));
        assert!(rows.iter().any(|row| row[0].as_int() == Some(71)));
        assert!(!rows.iter().any(|row| row[0].as_int() == Some(72)));
        assert!(!connection.get_auto_commit());
        connection.execute("ROLLBACK").unwrap();
        assert_eq!(whopper.check_fts_crash_recovery().unwrap(), rows);
    }

    #[test]
    fn reopen_returns_fts_property_failures_from_drained_statements() {
        let mut whopper = Whopper::new(FtsProfile::Merge.options().with_seed(812)).unwrap();
        let fiber = &mut whopper.context.fibers[0];
        fiber.current_op = Some(Operation::FtsMatchDifferential {
            token: "alpha".to_owned(),
        });
        fiber.execution_id = Some(1);
        fiber.statement.replace(Some(
            fiber.connection.prepare("SELECT 1, 1, NULL, NULL").unwrap(),
        ));
        let error = whopper.reopen().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("multiplicity difference Some(1)")
        );
    }

    #[test]
    fn profiles_exercise_fts_with_mvcc() {
        for profile in [
            FtsProfile::Merge,
            FtsProfile::Snapshots,
            FtsProfile::Recovery,
        ] {
            let opts = profile
                .options()
                .with_seed(8940 + profile as u64)
                .with_max_steps(6000);
            let mut whopper = Whopper::new(opts).unwrap();
            whopper.run().unwrap();
            assert!(whopper.stats.fts_checks > 0, "{profile:?}");
            assert!(whopper.stats.fts_optimizes > 0, "{profile:?}");
            assert!(whopper.stats.fts_commits > 0, "{profile:?}");
            assert!(whopper.stats.fts_savepoint_rollbacks > 0, "{profile:?}");
            if profile == FtsProfile::Snapshots {
                assert!(whopper.stats.fts_snapshot_reads > 0);
            }
            if profile == FtsProfile::Recovery {
                assert!(whopper.stats.fts_crash_checks > 0);
                assert!(whopper.stats.fts_abandoned_statements > 0);
            }
        }
    }

    #[test]
    fn snapshot_workload_rejects_changed_rows() {
        let mut workload = FtsProfile::Snapshots.generate(ChaCha8Rng::seed_from_u64(7), 0);
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
