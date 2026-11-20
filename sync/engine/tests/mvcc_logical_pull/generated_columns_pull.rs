use std::sync::Arc;
use std::time::Instant;

use turso_sync_engine::Result;

use crate::harness::{database_opts, InProcessServer, Remote, Replica};

/// Port of reproducer.mjs using the real sync engine and an in-process transport.
/// Unlike the earlier CAST-only workload, this uses JSON expressions, generated
/// column dependencies, a TEXT primary key, and partial/expression indexes.
///
/// cargo test --locked --offline --profile bench-profile -p turso_sync_engine
///   --test mvcc_logical_pull generated_columns_pull -- --ignored --nocapture
///   --test-threads=1
///
/// GENCOL_REPRO_CASES=full,no-indexes,no-generated,plain
/// GENCOL_REPRO_ROWS=50000 GENCOL_REPRO_UPDATE_ROWS=10000
/// GENCOL_REPRO_VALUE_COLUMNS=11 GENCOL_REPRO_ROUNDS=2
/// `plain` is an extra control without generated columns or explicit indexes.
/// All variants retain the PRIMARY KEY and UNIQUE constraints in the reproducer.
/// GENCOL_REPRO_ID_FORMAT=padded is a diagnostic control: zero-pad the numeric
/// part of TEXT primary keys so their text order agrees with insertion order.
#[test]
#[ignore = "manual reproduction: JSON generated columns and large MVCC pulls"]
fn generated_columns_pull() -> Result<()> {
    let total_rows = setting("GENCOL_REPRO_ROWS", 50000);
    let update_rows = setting("GENCOL_REPRO_UPDATE_ROWS", 10000);
    let value_columns = setting("GENCOL_REPRO_VALUE_COLUMNS", 11);
    let rounds = setting("GENCOL_REPRO_ROUNDS", 2);
    let cases = std::env::var("GENCOL_REPRO_CASES")
        .unwrap_or_else(|_| "full,no-indexes,no-generated,plain".to_string());
    let id_format = std::env::var("GENCOL_REPRO_ID_FORMAT").unwrap_or_else(|_| "raw".to_string());
    assert!(matches!(id_format.as_str(), "raw" | "padded"));
    assert!(total_rows > 0 && update_rows > 0 && update_rows <= total_rows);
    assert!(value_columns >= 2 && rounds > 0);

    println!("variant,total_rows,update_rows,value_columns,round,remote_update_seconds,connect_seconds,pull_seconds,empty_pull_seconds,checkpoint_seconds");
    eprintln!("TEXT primary key format: {id_format}");
    for variant in cases.split(',') {
        assert!(matches!(
            variant,
            "full" | "no-indexes" | "no-generated" | "plain"
        ));
        let generated = matches!(variant, "full" | "no-indexes");
        let indexed = matches!(variant, "full" | "no-generated");
        let mut setup = schema(generated, indexed, value_columns);
        // The test transport serves a single log generation. Replicas retain
        // automatic checkpoints and also checkpoint after each pull, as in JS.
        setup.insert(0, "PRAGMA mvcc_checkpoint_threshold=-1".to_string());
        eprintln!("setting up {variant}: {total_rows} rows, {value_columns} JSON columns");
        let start = Instant::now();
        let remote = Remote::new(&setup.iter().map(String::as_str).collect::<Vec<_>>())?;
        seed(&remote, total_rows, value_columns, id_format == "padded")?;
        eprintln!("seeded {variant} in {:.3}s", start.elapsed().as_secs_f64());

        let dir = Arc::new(tempfile::tempdir()?);
        let start = Instant::now();
        let replica = Replica::open(&dir, InProcessServer::new(remote.clone()), database_opts())?;
        replica.pull()?;
        replica.checkpoint()?;
        drop(replica);
        eprintln!(
            "bootstrapped {variant} in {:.3}s; replica_bytes={}",
            start.elapsed().as_secs_f64(),
            std::fs::metadata(dir.path().join("replica.db"))?.len(),
        );

        for round in 1..=rounds {
            eprintln!("updating {variant}: round={round}, rows={update_rows}");
            let start = Instant::now();
            remote.execute_transaction(&[&format!(
                "UPDATE items SET c2 = {} \
                 WHERE rowid IN (SELECT rowid FROM items ORDER BY rowid LIMIT {update_rows})",
                random_value(&format!("'round-{round}-'")),
            )])?;
            let remote_seconds = start.elapsed().as_secs_f64();

            let start = Instant::now();
            let replica = Replica::open_existing(
                &dir,
                InProcessServer::new(remote.clone()),
                database_opts(),
            )?;
            let connect_seconds = start.elapsed().as_secs_f64();
            eprintln!("pulling {variant}: round={round}");
            let start = Instant::now();
            replica.pull()?;
            let pull_seconds = start.elapsed().as_secs_f64();
            // The transport sends all pending frames in one response. A second
            // pull verifies the same empty-pull cycle used by the JS loop.
            let start = Instant::now();
            replica.pull()?;
            let empty_pull_seconds = start.elapsed().as_secs_f64();
            let start = Instant::now();
            replica.checkpoint()?;
            let checkpoint_seconds = start.elapsed().as_secs_f64();
            println!("{variant},{total_rows},{update_rows},{value_columns},{round},{remote_seconds:.6},{connect_seconds:.6},{pull_seconds:.6},{empty_pull_seconds:.6},{checkpoint_seconds:.6}");

            // Check full contents, including generated values, outside timing.
            // Reopen as well so checkpoint/recovery errors cannot be hidden by
            // the connection that just applied the changes.
            drop(replica);
            if round == rounds {
                let reopened = Replica::open_existing(
                    &dir,
                    InProcessServer::new(remote.clone()),
                    database_opts(),
                )?;
                assert_eq!(reopened.snapshot()?, remote.snapshot()?);
                eprintln!("verified {variant}: schema and all rows match after reopen");
            }
        }
    }
    Ok(())
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name)
        .map(|value| value.parse::<usize>().expect(name))
        .unwrap_or(default)
}

fn schema(generated: bool, indexed: bool, value_columns: usize) -> Vec<String> {
    let mut columns = [
        "id TEXT PRIMARY KEY",
        "row_number INTEGER NOT NULL DEFAULT 0",
        "created_at INTEGER NOT NULL",
        "modified_at INTEGER NOT NULL",
        "created_by INTEGER NOT NULL",
        "modified_by INTEGER NOT NULL",
        "deleted_at INTEGER",
        "deleted_by INTEGER",
        "deleted_from TEXT",
        "restored_by INTEGER",
        "row_rank TEXT UNIQUE",
    ]
    .map(str::to_string)
    .to_vec();
    for column in 1..=value_columns {
        columns.push(format!("c{column} TEXT"));
    }
    if generated {
        for column in 1..=value_columns {
            columns.push(format!(
                "c{column}_filter BLOB GENERATED ALWAYS AS (CASE \
                 WHEN json_type(c{column}, '$.fsv') IS NOT NULL THEN json_extract(c{column}, '$.fsv') \
                 ELSE json_extract(c{column}, '$.v') END) VIRTUAL"
            ));
            columns.push(format!(
                "c{column}_sort BLOB GENERATED ALWAYS AS (CASE \
                 WHEN json_type(c{column}, '$.ssv') IS NOT NULL THEN json_extract(c{column}, '$.ssv') \
                 WHEN json_type(c{column}, '$.fsv') IS NOT NULL THEN json_extract(c{column}, '$.fsv') \
                 ELSE json_extract(c{column}, '$.v') END) VIRTUAL"
            ));
        }
        columns.push("search TEXT GENERATED ALWAYS AS (CAST(c1_sort AS TEXT)) VIRTUAL".to_string());
    }
    let mut statements = vec![format!("CREATE TABLE items ({})", columns.join(", "))];
    if indexed {
        statements.extend([
            "CREATE UNIQUE INDEX items_active_row_number ON items (row_number) WHERE deleted_at IS NULL",
            "CREATE INDEX items_active_row_rank ON items (row_rank) WHERE deleted_at IS NULL",
            "CREATE INDEX items_deleted ON items (deleted_at DESC, id ASC) WHERE deleted_at IS NOT NULL",
            "CREATE INDEX items_row_metadata ON items (row_number, id, created_by, created_at, modified_by, modified_at) WHERE deleted_at IS NULL",
        ].map(str::to_string));
        if generated {
            statements.push(
                "CREATE INDEX items_search ON items (search, row_rank, deleted_at IS NULL)"
                    .to_string(),
            );
        }
    }
    statements
}

fn seed(remote: &Remote, rows: usize, value_columns: usize, padded_ids: bool) -> Result<()> {
    let names = (1..=value_columns)
        .map(|column| format!("c{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let values = (0..value_columns)
        .map(|_| random_value("'seed-'"))
        .collect::<Vec<_>>()
        .join(", ");
    // Match the JS batch size: larger statements hit the u16 instruction limit.
    for first in (1..=rows).step_by(50) {
        let row_sql = (first..=(first + 49).min(rows))
            .map(|row| {
                let id = if padded_ids {
                    format!("printf('r%010d', {row})")
                } else {
                    format!("'r' || {row}")
                };
                format!("({id}, {row}, 0, 0, 1, 1, printf('%010d', {row}), {values})")
            })
            .collect::<Vec<_>>()
            .join(", ");
        remote.execute_transaction(&[&format!(
            "INSERT INTO items (id, row_number, created_at, modified_at, created_by, modified_by, row_rank, {names}) VALUES {row_sql}"
        )])?;
    }
    Ok(())
}

fn random_value(prefix: &str) -> String {
    format!(
        "json_object('v', {prefix} || hex(randomblob(40)), 'fsv', abs(random()) % 1000000, \
         'ssv', hex(randomblob(12)))"
    )
}
