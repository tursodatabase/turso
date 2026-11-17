use std::time::Instant;

use turso_sync_engine::Result;

use crate::harness::Remote;

/// Synthetic reproduction of the large-update pull workload in context.md.
/// Run from the repository root:
/// cargo test --offline --config 'profile.dev.package.turso_core.opt-level=2'
///   -p turso_sync_engine --test mvcc_logical_pull large_update_pull_scaling
///   -- --ignored --nocapture --test-threads=1
/// Defaults to 1k, 2k, 4k, 10k, 20k, and 40k updates.
/// PULL_REPRO_SIZES=20000,40000 selects only the larger batches.
/// Compares normal automatic checkpointing with an additional checkpoint after
/// every pull, using two replicas of the same remote and identical updates.
#[test]
#[ignore = "manual performance reproduction: 50k rows with large update pulls"]
fn large_update_pull_scaling() -> Result<()> {
    let total_rows = std::env::var("PULL_REPRO_TOTAL_ROWS")
        .unwrap_or_else(|_| "50000".to_string())
        .parse::<usize>()
        .expect("PULL_REPRO_TOTAL_ROWS must be an integer");
    let sizes = std::env::var("PULL_REPRO_SIZES")
        .unwrap_or_else(|_| "1000,2000,4000,10000,20000,40000".to_string())
        .split(',')
        .map(|size| size.parse::<usize>().expect("invalid PULL_REPRO_SIZES"))
        .collect::<Vec<_>>();
    let cases = std::env::var("PULL_REPRO_CASES")
        .unwrap_or_else(|_| "plain,indexed,generated,generated_indexed".to_string());
    let repeats = std::env::var("PULL_REPRO_REPEATS")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<usize>()
        .expect("PULL_REPRO_REPEATS must be an integer");
    assert!(repeats > 0);

    println!("case,rows,repeat,checkpoint_policy,remote_update_seconds,pull_seconds,checkpoint_seconds,cycle_seconds,log_bytes_before_pull,log_bytes_after_pull,log_bytes_after_checkpoint");
    for case in cases.split(',') {
        assert!(matches!(
            case,
            "plain" | "indexed" | "generated" | "generated_indexed"
        ));
        let generated = case.contains("generated");
        let indexed = case.contains("indexed");
        let mut columns = vec![
            "id INTEGER PRIMARY KEY".to_string(),
            "payload TEXT".to_string(),
            "version INTEGER".to_string(),
        ];
        for column in 0..7 {
            columns.push(format!("c{column} INTEGER"));
        }
        if generated {
            for column in 0..23 {
                columns.push(format!(
                    "g{column} TEXT AS (CAST(c{} AS TEXT)) VIRTUAL",
                    column % 7
                ));
            }
        }
        // Only the remote disables automatic checkpoints: the in-process server
        // serves one log generation and must retain its history. Both replicas
        // keep the default automatic checkpoint threshold.
        let mut setup = vec![
            "PRAGMA mvcc_checkpoint_threshold=-1".to_string(),
            format!("CREATE TABLE items ({})", columns.join(", ")),
        ];
        if indexed {
            for index in 0..5 {
                setup.push(format!("CREATE INDEX items_{index} ON items (c{index})"));
            }
        }
        setup.push(format!(
            "WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x < {total_rows}) \
             INSERT INTO items (id,payload,version,c0,c1,c2,c3,c4,c5,c6) \
             SELECT x,printf('%01400d',x),0,x,x+1,x+2,x+3,x+4,x+5,x+6 FROM seq"
        ));
        eprintln!("setting up {case}: {total_rows} rows");
        let remote = Remote::new(&setup.iter().map(String::as_str).collect::<Vec<_>>())?;
        // Bootstrap both before any updates, because bootstrapping checkpoints
        // the remote log and the transport only serves one log generation.
        let replicas = [remote.bootstrap_replica()?, remote.bootstrap_replica()?];
        for (size_index, &size) in sizes.iter().enumerate() {
            assert!(size > 0);
            assert!(size <= total_rows);
            for repeat in 0..repeats {
                // Match writebacks committed in transactions of at most 10k rows.
                // Four indexed values stay unchanged, but full-row sync replay
                // deletes and reinserts their index entries as well.
                let start = Instant::now();
                for first in (1..=size).step_by(10000) {
                    let last = (first + 9999).min(size);
                    remote.execute_transaction(&[&format!(
                        "UPDATE items SET version=version+1,c0=c0+1 WHERE id BETWEEN {first} AND {last}"
                    )])?;
                }
                let remote_seconds = start.elapsed().as_secs_f64();
                // Alternate which policy is measured first to reduce ordering
                // effects from system load, temperature, and cached file reads.
                for order in 0..replicas.len() {
                    let policy_index = (size_index + repeat + order) % replicas.len();
                    let replica = &replicas[policy_index];
                    let policy = if policy_index == 0 {
                        "automatic"
                    } else {
                        "after_pull"
                    };
                    let log_before_pull = replica.logical_log_bytes()?;
                    eprintln!("pulling {case}: rows={size}, repeat={}, checkpoint_policy={policy}, log_bytes_before_pull={log_before_pull}", repeat + 1);
                    let start = Instant::now();
                    replica.pull()?;
                    let pull_seconds = start.elapsed().as_secs_f64();
                    let log_after_pull = replica.logical_log_bytes()?;
                    let checkpoint_seconds = if policy_index == 1 {
                        let start = Instant::now();
                        replica.checkpoint()?;
                        start.elapsed().as_secs_f64()
                    } else {
                        0.0
                    };
                    let log_after_checkpoint = replica.logical_log_bytes()?;
                    let cycle_seconds = pull_seconds + checkpoint_seconds;
                    println!("{case},{size},{},{policy},{remote_seconds:.6},{pull_seconds:.6},{checkpoint_seconds:.6},{cycle_seconds:.6},{log_before_pull},{log_after_pull},{log_after_checkpoint}", repeat + 1);
                }
            }
        }
        let expected = remote.snapshot()?;
        for replica in &replicas {
            assert_eq!(replica.snapshot()?, expected);
        }
    }
    Ok(())
}
