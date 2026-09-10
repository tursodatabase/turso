use rand::{Rng, seq::IndexedRandom};
use rand_chacha::ChaCha8Rng;
use turso_core::{LimboError, Value};

use crate::chaotic_elle::{ChaoticWorkload, ChaoticWorkloadProfile};
use crate::operations::{OpResult, Operation, TxMode};
use crate::properties::Property;
use crate::workloads::{FTS_SIM_TABLE, FTS_SIM_TOKENS};

pub struct FtsRollbackProfile {
    pub check_ranking: bool,
}

impl ChaoticWorkloadProfile for FtsRollbackProfile {
    fn generate(&self, mut rng: ChaCha8Rng, fiber_id: usize) -> Box<dyn ChaoticWorkload> {
        let words = FTS_SIM_TOKENS
            .choose_multiple(&mut rng, 3)
            .collect::<Vec<_>>();
        let phrase = format!("{} {}", words[0], words[1]);
        if fiber_id != 0 {
            let mut ops = vec![Operation::Begin {
                mode: TxMode::Concurrent,
            }];
            for _ in 0..rng.random_range(2..=6) {
                ops.push(Operation::FtsMatchDifferential {
                    token: phrase.clone(),
                    check_ranking: self.check_ranking,
                });
            }
            ops.push(Operation::Commit);
            return Box::new(FtsSnapshotReader {
                ops: ops.into_iter(),
                snapshot: None,
            });
        }
        let first_id = 1000;
        let second_id = first_id + 1;
        let original = format!("{phrase} {}", words[2]);
        let reversed = format!("{} {}", words[1], words[0]);
        let baseline = [Some(original.clone()), None];
        let mut expected = [Some(reversed.clone()), Some(phrase.clone())];
        let mut ops = vec![
            Operation::Begin {
                mode: TxMode::Concurrent,
            },
            Operation::Execute {
                sql: format!(
                    "INSERT OR REPLACE INTO {FTS_SIM_TABLE} VALUES ({first_id}, '{original}')"
                ),
            },
            Operation::Execute {
                sql: format!("DELETE FROM {FTS_SIM_TABLE} WHERE id = {second_id}"),
            },
            Operation::Commit,
            Operation::Begin {
                mode: TxMode::Concurrent,
            },
            Operation::Savepoint {
                name: "fts_outer".into(),
            },
            Operation::Execute {
                sql: format!(
                    "UPDATE {FTS_SIM_TABLE} SET body = '{reversed}' WHERE id = {first_id}"
                ),
            },
            Operation::Execute {
                sql: format!("INSERT INTO {FTS_SIM_TABLE} VALUES ({second_id}, '{phrase}')"),
            },
        ];
        let checks = |ops: &mut Vec<Operation>, bodies: &[Option<String>; 2], scenario_complete| {
            ops.push(Operation::FtsMatchDifferential {
                token: phrase.clone(),
                check_ranking: self.check_ranking,
            });
            ops.push(Operation::FtsCheckRows {
                first_id,
                bodies: bodies.clone(),
                scenario_complete,
            });
        };
        checks(&mut ops, &expected, false);
        ops.push(Operation::FtsOptimize);
        checks(&mut ops, &expected, false);
        ops.extend([
            Operation::Savepoint {
                name: "fts_inner".into(),
            },
            Operation::Execute {
                sql: format!("DELETE FROM {FTS_SIM_TABLE} WHERE id = {first_id}"),
            },
            Operation::Execute {
                sql: format!(
                    "UPDATE {FTS_SIM_TABLE} SET body = '{}' WHERE id = {second_id}",
                    words[2]
                ),
            },
        ]);
        let before_inner = expected.clone();
        expected = [None, Some(words[2].to_string())];
        checks(&mut ops, &expected, false);
        ops.push(Operation::FtsOptimize);
        checks(&mut ops, &expected, false);
        if rng.random_bool(0.75) {
            ops.push(Operation::RollbackToSavepoint {
                name: "fts_inner".into(),
            });
            expected = before_inner;
            checks(&mut ops, &expected, false);
        }
        if rng.random_bool(0.75) {
            ops.push(Operation::RollbackToSavepoint {
                name: "fts_outer".into(),
            });
            checks(&mut ops, &baseline, false);
            ops.push(Operation::Execute {
                sql: format!(
                    "UPDATE {FTS_SIM_TABLE} SET body = '{reversed}' WHERE id = {first_id}"
                ),
            });
            checks(&mut ops, &[Some(reversed.clone()), None], false);
            ops.push(Operation::FtsOptimize);
            checks(&mut ops, &[Some(reversed), None], false);
            ops.push(Operation::RollbackToSavepoint {
                name: "fts_outer".into(),
            });
            expected.clone_from(&baseline);
        } else {
            ops.push(Operation::ReleaseSavepoint {
                name: "fts_inner".into(),
            });
        }
        ops.push(Operation::ReleaseSavepoint {
            name: "fts_outer".into(),
        });
        checks(&mut ops, &expected, false);
        ops.push(Operation::FtsOptimize);
        checks(&mut ops, &expected, false);
        if rng.random_bool(0.75) {
            ops.push(Operation::Rollback);
            expected = baseline;
        } else {
            ops.push(Operation::Commit);
        }
        checks(&mut ops, &expected, true);
        Box::new(FtsRollbackWorkload {
            ops: ops.into_iter(),
        })
    }
}

struct FtsSnapshotReader {
    ops: std::vec::IntoIter<Operation>,
    snapshot: Option<Vec<Vec<Value>>>,
}

impl ChaoticWorkload for FtsSnapshotReader {
    fn next(&mut self, result: Option<OpResult>) -> Option<Operation> {
        match result {
            Some(Err(_)) => return None,
            Some(Ok(rows)) if !rows.is_empty() => {
                if let Some(snapshot) = &self.snapshot {
                    assert_eq!(
                        &rows, snapshot,
                        "FTS results changed inside a reader snapshot"
                    );
                } else {
                    self.snapshot = Some(rows);
                }
            }
            _ => {}
        }
        self.ops.next()
    }
}

struct FtsRollbackWorkload {
    ops: std::vec::IntoIter<Operation>,
}

impl ChaoticWorkload for FtsRollbackWorkload {
    fn next(&mut self, result: Option<OpResult>) -> Option<Operation> {
        if matches!(result, Some(Err(_))) {
            return None;
        }
        self.ops.next()
    }
}

pub struct FtsRollbackProperty;

impl Property for FtsRollbackProperty {
    fn finish_op(
        &mut self,
        step: usize,
        fiber_id: usize,
        _txn_id: Option<u64>,
        _start_exec_id: u64,
        _end_exec_id: u64,
        op: &Operation,
        result: &OpResult,
    ) -> anyhow::Result<()> {
        let rows = match result {
            Ok(rows) => rows,
            Err(
                LimboError::Busy
                | LimboError::BusySnapshot
                | LimboError::WriteWriteConflict
                | LimboError::CommitDependencyAborted
                | LimboError::OutOfMemory,
            ) => return Ok(()),
            Err(error) => anyhow::bail!(
                "step {step} fiber {fiber_id}: FTS rollback operation {op:?} failed: {error}"
            ),
        };
        let Operation::FtsCheckRows {
            first_id, bodies, ..
        } = op
        else {
            return Ok(());
        };
        let expected = bodies
            .iter()
            .enumerate()
            .filter_map(|(offset, body)| {
                body.as_ref().map(|body| {
                    vec![
                        Value::from_i64(first_id + offset as i64),
                        Value::build_text(body.clone()),
                    ]
                })
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            rows == &expected,
            "step {step} fiber {fiber_id}: FTS rollback rows disagree: expected {expected:?}, got {rows:?}"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn every_optimize_has_matching_checks_before_and_after() {
        for seed in 0..100 {
            let mut plan = FtsRollbackProfile {
                check_ranking: false,
            }
            .generate(ChaCha8Rng::seed_from_u64(seed), 0);
            let mut ops = Vec::new();
            let mut result = None;
            while let Some(op) = plan.next(result.take()) {
                ops.push(op);
                result = Some(Ok(vec![]));
            }
            let mut optimizes = 0;
            for (i, op) in ops.iter().enumerate() {
                if !matches!(op, Operation::FtsOptimize) {
                    continue;
                }
                optimizes += 1;
                assert!(matches!(ops[i - 2], Operation::FtsMatchDifferential { .. }));
                assert!(matches!(ops[i + 1], Operation::FtsMatchDifferential { .. }));
                let Operation::FtsCheckRows { bodies: before, .. } = &ops[i - 1] else {
                    panic!("missing row check before OPTIMIZE");
                };
                let Operation::FtsCheckRows { bodies: after, .. } = &ops[i + 2] else {
                    panic!("missing row check after OPTIMIZE");
                };
                assert_eq!(before, after);
            }
            assert!(optimizes >= 3);
        }
    }

    #[test]
    #[should_panic(expected = "FTS results changed inside a reader snapshot")]
    fn reader_rejects_a_changed_snapshot() {
        let mut reader = FtsRollbackProfile {
            check_ranking: false,
        }
        .generate(ChaCha8Rng::seed_from_u64(7), 1);
        reader.next(None);
        reader.next(Some(Ok(vec![])));
        reader.next(Some(Ok(vec![vec![Value::Null]])));
        reader.next(Some(Ok(vec![vec![Value::build_text("1000")]])));
    }

    #[test]
    fn failed_operation_abandons_savepoint_sequence() {
        let profile = FtsRollbackProfile {
            check_ranking: false,
        };
        let mut workload = profile.generate(ChaCha8Rng::seed_from_u64(7), 0);
        assert!(matches!(workload.next(None), Some(Operation::Begin { .. })));
        assert!(workload.next(Some(Err(LimboError::OutOfMemory))).is_none());
    }

    #[test]
    fn row_check_rejects_unrolled_back_insert_and_wrong_body() {
        let op = Operation::FtsCheckRows {
            first_id: 1000,
            bodies: [Some("alpha bravo".into()), None],
            scenario_complete: false,
        };
        let row = vec![Value::from_i64(1000), Value::build_text("alpha bravo")];
        let check = |rows| FtsRollbackProperty.finish_op(0, 0, None, 0, 0, &op, &Ok(rows));
        assert!(check(vec![row.clone()]).is_ok());
        assert!(check(vec![]).is_err());
        assert!(
            check(vec![vec![
                Value::from_i64(1000),
                Value::build_text("bravo alpha")
            ]])
            .is_err()
        );
        assert!(
            check(vec![
                row,
                vec![Value::from_i64(1001), Value::build_text("alpha bravo")]
            ])
            .is_err()
        );
    }
}
