use anyhow::{Context, Result};
use indicatif::ProgressBar;
use turso::Connection;

use crate::generators::{GenState, Generator, Val};
use crate::workload::{
    needed_pools, post_seed_statements, table_statements, yaml_sql_literal, RowsSpec, Workload,
};

/// Apply the workload's schema and seed data to the database.
pub async fn setup_database(
    conn: &Connection,
    workload: &Workload,
    gen: &mut GenState,
    progress: &ProgressBar,
) -> Result<()> {
    for stmt in table_statements(workload)? {
        conn.execute(&stmt, ())
            .await
            .with_context(|| format!("setup: {stmt}"))?;
    }

    // Re-seeding must start from a clean slate or unique keys from a prior
    // seed collide. Children are listed after their parents in `schema`, so
    // delete in reverse order to respect foreign keys.
    if let Some(schema) = &workload.schema {
        for table in schema.tables.iter().rev() {
            conn.execute(&format!("DELETE FROM {}", table.name), ())
                .await
                .with_context(|| format!("setup: clearing table {}", table.name))?;
        }
    }

    for spec in &workload.data {
        match &spec.rows {
            RowsSpec::Literal(rows) => {
                // Explicit literal rows; column sets may differ row to row.
                for row in rows {
                    let mut cols = Vec::new();
                    let mut values = Vec::new();
                    for (col, value) in row {
                        cols.push(col.as_str().context("literal row column name")?.to_string());
                        values.push(yaml_sql_literal(value));
                    }
                    let sql = format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        spec.table,
                        cols.join(", "),
                        values.join(", ")
                    );
                    conn.execute(&sql, ())
                        .await
                        .with_context(|| format!("seeding table {}", spec.table))?;
                    progress.inc(1);
                }
            }
            RowsSpec::Count(count) => {
                let mut columns = Vec::new();
                for (name, gen_spec) in &spec.columns {
                    let name = name.as_str().context("data column name")?.to_string();
                    let generator = Generator::parse(gen_spec)
                        .with_context(|| format!("table {}, column {name}", spec.table))?;
                    columns.push((name, generator));
                }
                let column_names = columns
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let placeholders = (0..columns.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "INSERT INTO {} ({column_names}) VALUES ({placeholders})",
                    spec.table
                );
                let mut stmt = conn
                    .prepare_cached(&sql)
                    .await
                    .with_context(|| format!("preparing seed insert for table {}", spec.table))?;

                conn.execute("BEGIN", ()).await?;
                for _ in 0..*count {
                    let mut params: Vec<turso::Value> = Vec::with_capacity(columns.len());
                    for (name, generator) in &columns {
                        let pool_key = format!("{}.{name}", spec.table);
                        let value = gen.eval(generator, &pool_key).unwrap_or(Val::Null);
                        params.push((&value).into());
                        if gen.pools.has(&pool_key) {
                            gen.pools.add(&pool_key, value);
                        }
                    }
                    stmt.execute(params)
                        .await
                        .with_context(|| format!("seeding table {}", spec.table))?;
                    progress.inc(1);
                }
                conn.execute("COMMIT", ()).await?;
            }
        }
    }

    for stmt in post_seed_statements(workload) {
        conn.execute(&stmt, ())
            .await
            .with_context(|| format!("setup: {stmt}"))?;
    }
    Ok(())
}

/// Hydrate entity pools from a database seeded by an earlier run (--no-setup).
pub async fn hydrate_pools(
    conn: &Connection,
    workload: &Workload,
    gen: &mut GenState,
) -> Result<()> {
    for key in needed_pools(workload) {
        let Some((table, column)) = key.split_once('.') else {
            continue;
        };
        let mut rows = conn
            .query(&format!("SELECT {column} FROM {table} LIMIT 100000"), ())
            .await
            .with_context(|| format!("hydrating pool {key}"))?;
        while let Some(row) = rows.next().await? {
            let value = match row.get_value(0)? {
                turso::Value::Integer(n) => Val::Int(n),
                turso::Value::Real(r) => Val::Real(r),
                turso::Value::Text(s) => Val::Text(s),
                turso::Value::Null | turso::Value::Blob(_) => continue,
            };
            gen.pools.add(&key, value);
        }
    }
    Ok(())
}
