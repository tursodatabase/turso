use rand_chacha::{ChaCha8Rng, rand_core::SeedableRng};
use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, IO, OpenFlags, SqliteDialect, Value};
use turso_whopper::chaotic_elle::ChaoticWorkloadProfile;
use turso_whopper::chaotic_fts::{FtsRollbackProfile, FtsRollbackProperty};
use turso_whopper::operations::Operation;
use turso_whopper::properties::{FtsSelfDifferentialProperty, IntegrityCheckProperty, Property};
use turso_whopper::workloads::{fts_sim_schema, fts_sim_workloads};
use turso_whopper::{IOFaultConfig, SimulatorIO, Whopper, WhopperOpts};

#[test]
#[ignore = "manual FTS/MVCC baseline; not enabled in CI"]
fn fts_match_seeds_replay() {
    let run = |seed| {
        let mut whopper = Whopper::new(WhopperOpts {
            seed: Some(seed),
            max_connections: 6,
            max_steps: 12_000,
            enable_mvcc: true,
            elle_tables: fts_sim_schema(),
            workloads: fts_sim_workloads(false),
            properties: vec![
                Box::new(IntegrityCheckProperty),
                Box::new(FtsSelfDifferentialProperty),
            ],
            ..WhopperOpts::default()
        })
        .unwrap();
        whopper.run().unwrap();
        assert!(whopper.stats.fts_checks > 0 && whopper.stats.fts_phrase_checks > 0);
        assert!(whopper.stats.fts_optimizes > 0);
        eprintln!("seed={seed}: {:?}", whopper.stats);
        (whopper.stats.clone(), whopper.db_file_bytes())
    };
    let (first, files) = run(0xF75);
    let (second, replay) = run(0xF75);
    assert_eq!(first.fts_checks, second.fts_checks);
    assert_eq!(first.fts_phrase_checks, second.fts_phrase_checks);
    assert!(!files.is_empty());
    assert_eq!(files, replay);
    for seed in [0xF7501, 0xF7502] {
        run(seed);
    }
}

#[test]
#[ignore = "manual FTS/MVCC baseline; not enabled in CI"]
fn fts_rollback_interleavings() {
    let mut whopper = Whopper::new(WhopperOpts {
        seed: Some(3957),
        max_connections: 6,
        max_steps: 18_000,
        enable_mvcc: true,
        elle_tables: fts_sim_schema(),
        workloads: vec![],
        chaotic_profiles: vec![(
            1.0,
            "fts-rollback",
            Box::new(FtsRollbackProfile {
                check_ranking: false,
            }),
        )],
        properties: vec![
            Box::new(IntegrityCheckProperty),
            Box::new(FtsSelfDifferentialProperty),
            Box::new(FtsRollbackProperty),
        ],
        ..WhopperOpts::default()
    })
    .unwrap();
    whopper.run().unwrap();
    let stats = &whopper.stats;
    eprintln!("{stats:?}");
    assert!(stats.fts_rollback_scenarios > 0);
    assert!(stats.fts_optimizes > 0 && stats.fts_row_checks > 0);
    assert!(stats.savepoint_rollbacks > 0 && stats.savepoint_releases > 0);
    assert!(stats.commits > 0 && stats.rollbacks > 0);
}

#[test]
#[ignore = "manual FTS/MVCC baseline; not enabled in CI"]
fn fts_rollback_all_savepoint_and_transaction_outcomes() {
    let profile = FtsRollbackProfile {
        check_ranking: false,
    };
    let mut covered = [false; 8];
    for seed in 0..512 {
        let mut plan = profile.generate(ChaCha8Rng::seed_from_u64(seed), 0);
        let mut ops = Vec::new();
        let mut result = None;
        while let Some(op) = plan.next(result.take()) {
            ops.push(op);
            result = Some(Ok(vec![]));
        }
        let inner = ops
            .iter()
            .any(|op| matches!(op, Operation::RollbackToSavepoint { name } if name == "fts_inner"));
        let outer = ops
            .iter()
            .any(|op| matches!(op, Operation::RollbackToSavepoint { name } if name == "fts_outer"));
        let rollback = ops.iter().any(|op| matches!(op, Operation::Rollback));
        let combination =
            usize::from(inner) | (usize::from(outer) << 1) | (usize::from(rollback) << 2);
        if covered[combination] {
            continue;
        }
        let io = Arc::new(SimulatorIO::new(
            false,
            ChaCha8Rng::seed_from_u64(seed),
            IOFaultConfig::default(),
        ));
        let db = open(&io, &format!("fts-outcomes-{seed}"));
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        assert!(db.get_mv_store().is_some());
        for (_, sql) in fts_sim_schema() {
            conn.execute(sql).unwrap();
        }
        for (step, op) in ops.iter().enumerate() {
            let rows = execute(&conn, &io, &op.sql());
            FtsRollbackProperty
                .finish_op(step, 0, None, 0, 0, op, &Ok(rows.clone()))
                .unwrap();
            FtsSelfDifferentialProperty
                .finish_op(step, 0, None, 0, 0, op, &Ok(rows))
                .unwrap();
        }
        assert!(matches!(
            ops.last(),
            Some(Operation::FtsCheckRows {
                scenario_complete: true,
                ..
            })
        ));
        eprintln!(
            "seed={seed}: inner_rollback={inner}, outer_rollback={outer}, full_rollback={rollback}"
        );
        covered[combination] = true;
        if covered.iter().all(|covered| *covered) {
            break;
        }
    }
    assert!(
        covered.iter().all(|covered| *covered),
        "missing outcomes: {covered:?}"
    );
}

#[test]
#[ignore = "manual FTS/MVCC baseline; not enabled in CI"]
fn fts_snapshot_merge_savepoint_and_recovery() {
    snapshot_merge_savepoint_and_recovery(false);
}

#[test]
#[ignore = "manual FTS/MVCC ranking baseline; currently fails on main; not enabled in CI"]
fn fts_ranking_snapshot_merge_savepoint_and_recovery() {
    snapshot_merge_savepoint_and_recovery(true);
}

fn snapshot_merge_savepoint_and_recovery(check_ranking: bool) {
    let io = Arc::new(SimulatorIO::new(
        false,
        ChaCha8Rng::seed_from_u64(0xF7502),
        IOFaultConfig::default(),
    ));
    let name = format!("fts-snapshot-{check_ranking}");
    let db = open(&io, &name);
    let writer = db.connect().unwrap();
    writer.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    assert!(db.get_mv_store().is_some());
    for (_, sql) in fts_sim_schema() {
        writer.execute(sql).unwrap();
    }
    writer.execute("INSERT INTO fts_docs VALUES (1, 'alpha bravo hotel'), (2, 'alpha bravo'), (3, 'bravo alpha'), (4, 'delta')").unwrap();
    let reader = db.connect().unwrap();
    reader.execute("BEGIN CONCURRENT").unwrap();
    let check = |conn: &Arc<turso_core::Connection>, expected: &str| {
        let op = Operation::FtsMatchDifferential {
            token: "alpha bravo".into(),
            check_ranking,
        };
        let rows = execute(conn, &io, &op.sql());
        assert_eq!(rows[0][4].to_string(), expected);
        FtsSelfDifferentialProperty
            .finish_op(0, 0, None, 0, 0, &op, &Ok(rows))
            .unwrap();
    };
    let initial = if check_ranking { "2,1" } else { "1,2" };
    check(&reader, initial);
    for sql in [
        "BEGIN CONCURRENT",
        "DELETE FROM fts_docs WHERE id = 2",
        "UPDATE fts_docs SET body = 'delta' WHERE id = 1",
        "INSERT INTO fts_docs VALUES (5, 'alpha bravo')",
        "OPTIMIZE INDEX fts_docs_fts",
        "COMMIT",
    ] {
        writer.execute(sql).unwrap();
    }
    check(&reader, initial);
    check(&writer, "5");
    for sql in [
        "BEGIN CONCURRENT",
        "SAVEPOINT edit",
        "DELETE FROM fts_docs WHERE id = 5",
        "ROLLBACK TO edit",
        "RELEASE edit",
    ] {
        writer.execute(sql).unwrap();
    }
    check(&writer, "5");
    writer
        .execute("INSERT INTO fts_docs VALUES (6, 'alpha bravo')")
        .unwrap();
    writer.execute("ROLLBACK").unwrap();
    check(&writer, "5");
    reader.execute("COMMIT").unwrap();
    drop(reader);
    drop(writer);
    drop(db);
    let recovered = open(&io, &name);
    assert!(recovered.get_mv_store().is_some());
    check(&recovered.connect().unwrap(), "5");
}

fn open(io: &Arc<SimulatorIO>, name: &str) -> Arc<Database> {
    let path = std::env::current_dir()
        .unwrap()
        .join(format!("{name}-{}.db", std::process::id()));
    Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn execute(conn: &Arc<turso_core::Connection>, io: &SimulatorIO, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                rows.push(stmt.row().unwrap().get_values().cloned().collect())
            }
            turso_core::StepResult::IO => io.step().unwrap(),
            turso_core::StepResult::Yield => {}
            turso_core::StepResult::Done => return rows,
            other => panic!("unexpected {other:?}"),
        }
    }
}
