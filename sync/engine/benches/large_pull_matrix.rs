//! Larger logical-pull workloads, using the same transport as mvcc_logical_pull.
//!
//! cargo bench --locked --offline --profile bench-profile -p turso_sync_engine
//!   --bench large_pull_matrix -- --ignored
//!
//! Defaults: 24 scenarios, 100k/250k rows, 10k–200k updates, three rounds, and
//! both checkpoint policies. Cases vary one factor from the full/raw/11-column
//! baseline rather than taking the Cartesian product of every setting.
//!
//! PULL_MATRIX_CASES=scaling-250k-200k-padded,key-composite selects scenarios.
//! PULL_MATRIX_ROUNDS=2 changes rounds per fixture. For a small validation run,
//! PULL_MATRIX_ROWS=200 PULL_MATRIX_UPDATES=120 overrides every scenario's size.
//! CLI --sample-count changes the number of independent fixtures per scenario.
//! This manual matrix is ignored by default, including in cargo test --all-targets.
//!
//! Divan reports the entire scenario, including setup and verification. The CSV
//! on stderr reports individual pending pulls, excluding setup, connection,
//! remote writes, empty pulls and explicit checkpoints. Automatic checkpoints
//! inside a pull remain included. First and subsequent pulls have separate labels.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use turso_sync_engine::Result;

#[allow(dead_code, unused_imports)]
#[path = "../tests/mvcc_logical_pull/harness/mod.rs"]
mod harness;

use harness::{database_opts, InProcessServer, Remote, Replica};

#[derive(Clone, Copy, Debug)]
enum Key {
    Raw,
    Padded,
    Permuted,
    ReversePadded,
    Integer,
    Composite,
}

#[derive(Clone, Copy, Debug)]
enum Layout {
    Full,
    NoIndexes,
    NoGenerated,
    Plain,
}

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    rows: usize,
    updates: usize,
    key: Key,
    layout: Layout,
    columns: usize,
    update_column: usize,
    mixed_json: bool,
    tail: bool,
}

fn schema(case: Case) -> Vec<String> {
    let mut columns = vec![match case.key {
        Key::Integer => "id INTEGER PRIMARY KEY",
        Key::Composite => "tenant INTEGER NOT NULL, id TEXT NOT NULL",
        _ => "id TEXT PRIMARY KEY",
    }
    .to_string()];
    columns.extend(
        [
            "row_number INTEGER NOT NULL",
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
        .map(str::to_string),
    );
    for column in 1..=case.columns {
        columns.push(format!("c{column} TEXT"));
    }
    let generated = matches!(case.layout, Layout::Full | Layout::NoIndexes);
    if generated {
        for column in 1..=case.columns {
            columns.push(format!("c{column}_filter BLOB AS (CASE WHEN json_type(c{column}, '$.fsv') IS NOT NULL THEN json_extract(c{column}, '$.fsv') ELSE json_extract(c{column}, '$.v') END) VIRTUAL"));
            columns.push(format!("c{column}_sort BLOB AS (CASE WHEN json_type(c{column}, '$.ssv') IS NOT NULL THEN json_extract(c{column}, '$.ssv') WHEN json_type(c{column}, '$.fsv') IS NOT NULL THEN json_extract(c{column}, '$.fsv') ELSE json_extract(c{column}, '$.v') END) VIRTUAL"));
        }
        columns.push("search TEXT AS (CAST(c1_sort AS TEXT)) VIRTUAL".to_string());
    }
    if matches!(case.key, Key::Composite) {
        columns.push("PRIMARY KEY (tenant, id)".to_string());
    }
    let mut statements = vec![
        // Keep the remote's one log generation available to the transport.
        "PRAGMA mvcc_checkpoint_threshold=-1".to_string(),
        format!("CREATE TABLE items ({})", columns.join(", ")),
    ];
    if matches!(case.layout, Layout::Full | Layout::NoGenerated) {
        statements.extend([
            "CREATE UNIQUE INDEX items_active_row_number ON items(row_number) WHERE deleted_at IS NULL",
            "CREATE INDEX items_active_row_rank ON items(row_rank) WHERE deleted_at IS NULL",
            "CREATE INDEX items_deleted ON items(deleted_at DESC, id ASC) WHERE deleted_at IS NOT NULL",
            "CREATE INDEX items_row_metadata ON items(row_number, id, created_by, created_at, modified_by, modified_at) WHERE deleted_at IS NULL",
        ].map(str::to_string));
        if generated {
            statements.push(
                "CREATE INDEX items_search ON items(search, row_rank, deleted_at IS NULL)"
                    .to_string(),
            );
        }
    }
    statements
}

fn seed(remote: &Remote, case: Case) -> Result<()> {
    let names = (1..=case.columns)
        .map(|column| format!("c{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let tenant = if matches!(case.key, Key::Composite) {
        "tenant, "
    } else {
        ""
    };
    // Small statements avoid the u16 instruction limit even for 24 JSON columns.
    for first in (1..=case.rows).step_by(25) {
        let values = (first..=(first + 24).min(case.rows))
            .map(|row| {
                let id = match case.key {
                    Key::Raw => format!("'r{row}'"),
                    Key::Padded | Key::Composite => format!("'r{row:010}'"),
                    // Odd multiplication modulo 2^32 is a bijection: key order is
                    // scattered without random seeds, collisions or extra sorting.
                    Key::Permuted => {
                        format!("'r{:010}'", (row as u64 * 2_654_435_761) % (1u64 << 32))
                    }
                    Key::ReversePadded => format!("'r{:010}'", case.rows + 1 - row),
                    Key::Integer => row.to_string(),
                };
                let tenant = if matches!(case.key, Key::Composite) {
                    format!("{}, ", row % 8)
                } else {
                    String::new()
                };
                let values = (1..=case.columns)
                    .map(|column| json_value(&row.to_string(), column, case.mixed_json))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({tenant}{id}, {row}, 0, 0, 1, 1, '{row:010}', {values})")
            })
            .collect::<Vec<_>>()
            .join(", ");
        remote.execute_transaction(&[&format!("INSERT INTO items ({tenant}id, row_number, created_at, modified_at, created_by, modified_by, row_rank, {names}) VALUES {values}")])?;
    }
    Ok(())
}

fn json_value(row: &str, column: usize, mixed: bool) -> String {
    let value = format!("'v', printf('%080d', {row} + {column})");
    let filter = format!("'fsv', ({row} + {column}) % 1000000");
    let sort = format!("'ssv', printf('%024d', {row} + {column})");
    if mixed {
        format!("CASE ({row} + {column}) % 3 WHEN 0 THEN json_object({value}) WHEN 1 THEN json_object({value}, {filter}) ELSE json_object({value}, {filter}, {sort}) END")
    } else {
        format!("json_object({value}, {filter}, {sort})")
    }
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name)
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("invalid {name}"))
        })
        .unwrap_or(default)
}

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}
